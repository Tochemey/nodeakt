/*
 * MIT License
 *
 * Copyright (c) 2026 GoAkt Team
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import {
  connect as connectTcp,
  createServer as createTcpServer,
  type Server,
  type Socket,
  type TcpSocketConnectOpts,
} from "node:net";
import {
  connect as connectTls,
  createServer as createTlsServer,
  type ConnectionOptions as TlsConnectionOptions,
  type TlsOptions,
} from "node:tls";
import { timedSignal } from "./clock";
import {
  MAX_NAME_BYTES,
  MAX_PACKET_BYTES,
  MAX_SYNC_EXCHANGE_BYTES,
  MAX_SYNC_MESSAGE_BYTES,
  MESSAGE_ACK,
  MESSAGE_GOSSIP,
  MESSAGE_NACK,
  MESSAGE_PING,
  MESSAGE_PING_REQ,
  MESSAGE_SYNC_REQUEST,
  MESSAGE_SYNC_RESPONSE,
  PROTOCOL_VERSION,
  ROLE_PACKET,
  ROLE_STREAM,
} from "./wire";

/**
 * One ordered synchronization byte stream between addressed membership endpoints.
 *
 * Reads expose arbitrary carrier chunks, not complete protocol messages. Callers
 * frame synchronization messages with a four-byte, big-endian payload length;
 * the transport validates those frames incrementally while leaving message
 * decoding to membership synchronization code.
 *
 * @internal Maintainers should keep this transport-neutral so membership code
 * can be tested without opening sockets.
 */
export interface MembershipStream {
  /**
   * Logical `host:port` identity declared by the remote transport handshake.
   *
   * This is a protocol identity, not the socket's observed source address and
   * is not cryptographically authenticated unless the configured TLS policy
   * establishes that relationship externally.
   */
  readonly remoteAddress: string;

  /**
   * Returns the next available carrier chunk as newly owned bytes.
   *
   * Chunk boundaries need not match synchronization frame boundaries.
   * `undefined` means the peer ended the readable side cleanly, or this stream
   * was locally closed. A timeout, carrier failure, malformed/incomplete frame,
   * or exchange-byte limit violation rejects with {@link MembershipTransportError}.
   */
  read(): Promise<Uint8Array | undefined>;

  /**
   * Queues synchronization bytes after incrementally validating frame structure.
   *
   * Concurrent calls are serialized. The input is not copied; callers must not
   * mutate or reuse its backing storage until the returned promise settles.
   * The promise resolves when Node reports the socket write complete, not when
   * the peer consumes the bytes. Invalid framing, queue overflow, timeout, and
   * carrier failures reject with {@link MembershipTransportError}.
   */
  write(bytes: Uint8Array): Promise<void>;

  /**
   * Idempotently stops new writes and half-closes the local socket.
   *
   * This does not wait for the peer or return a completion signal.
   */
  close(): void;
}

/**
 * Inbound callbacks installed while a transport endpoint is started.
 *
 * @internal Handler failures intentionally terminate only the affected
 * connection and must not mutate transport-wide lifecycle state.
 */
export interface TransportHandlers {
  /**
   * Handles one complete, envelope-validated packet from a declared identity.
   *
   * `bytes` is owned by the transport invocation and is not reused internally.
   * Returning a promise applies backpressure to subsequent packets on the same
   * connection. Throwing or rejecting closes only that inbound connection.
   */
  packet(from: string, bytes: Uint8Array): void | Promise<void>;

  /**
   * Handles one stream-role connection after its handshake is validated.
   *
   * Handler completion does not automatically close the stream. Throwing or
   * rejecting closes that stream connection and leaves the listener running.
   */
  stream(from: string, stream: MembershipStream): void | Promise<void>;
}

/**
 * Packet-and-stream seam consumed by probe and synchronization protocol code.
 *
 * @internal Implementations must preserve packet framing and stream opacity.
 */
export interface MembershipTransport {
  /**
   * Logical `host:port` address encoded into outbound handshakes.
   *
   * It may differ from the local bind address and may change from port zero to
   * the assigned port while the listener is being bound.
   */
  readonly address: string;

  /**
   * Installs inbound handlers and resolves once the endpoint can accept peers.
   *
   * Implementations define whether repeated starts replace handlers; callers
   * must not assume a stopped transport can be restarted.
   */
  start(handlers: TransportHandlers): Promise<void>;

  /**
   * Quiesces the endpoint and releases listener and connection resources.
   *
   * Completion means transport-owned resources have been asked to close; it
   * does not imply remote peers observed a graceful shutdown.
   */
  stop(): Promise<void>;

  /**
   * Sends one packet-role membership envelope to a logical peer.
   *
   * Implementations may pool the underlying connection. Resolution indicates
   * completion of the local write, not remote handling or acknowledgement.
   */
  packet(to: string, bytes: Uint8Array): Promise<void>;

  /**
   * Opens a dedicated stream-role connection to a logical peer.
   *
   * The returned stream is intended for one bounded synchronization exchange
   * and is not drawn from the packet connection pool.
   */
  stream(to: string): Promise<MembershipStream>;
}

/**
 * TLS material shared by the listener and outbound membership sockets.
 *
 * @internal Certificate validation remains strict for outbound connections.
 */
export interface MembershipTlsOptions {
  /** PEM certificate chain presented by both listener and outbound sockets. */
  readonly cert: string | Buffer;

  /** PEM private key corresponding to {@link cert}, used in both directions. */
  readonly key: string | Buffer;

  /**
   * Optional PEM CA bundle used for outbound server verification and, when
   * {@link requestCert} is true, inbound client-certificate verification.
   */
  readonly ca?: string | Buffer;

  /**
   * Requires inbound clients to present a certificate accepted by the listener.
   *
   * Outbound server certificates are always verified regardless of this flag.
   */
  readonly requestCert?: boolean;

  /**
   * Optional TLS SNI and hostname-verification name for every outbound peer.
   *
   * When omitted, Node derives verification behavior from the dialed host.
   */
  readonly servername?: string;
}

/**
 * Bind, queue, pool, and timeout settings for {@link TcpMembershipTransport}.
 *
 * @internal Values are validated eagerly so runtime paths can assume positive
 * limits and a syntactically valid bind endpoint.
 */
export interface TcpMembershipTransportOptions {
  /**
   * Hostname or IP passed to `Server.listen`; it is not normalized or resolved
   * by the transport.
   */
  readonly host: string;

  /** Listener port in `0..65535`; zero requests an ephemeral port. */
  readonly port: number;

  /**
   * Host placed in the logical identity when peers cannot dial {@link host}.
   *
   * The transport does not verify reachability or DNS resolution.
   */
  readonly advertiseHost?: string;

  /**
   * Enables TLS for every inbound and outbound transport connection.
   *
   * Plain TCP and TLS peers cannot be mixed by one transport instance.
   */
  readonly tls?: MembershipTlsOptions;

  /**
   * Maximum logical destinations retained in the packet LRU, including
   * connections that are still dialing or handshaking.
   */
  readonly maxPacketConnections?: number;

  /**
   * Per-connection ceiling for unread inbound bytes and pending outbound bytes.
   *
   * Read and write accounting are independent; exceeding either side destroys
   * or rejects work on that connection rather than applying socket backpressure.
   */
  readonly maxQueuedBytes?: number;

  /**
   * Deadline for outbound TCP connection establishment or the complete TLS
   * handshake. It does not include the transport-role handshake write.
   */
  readonly connectTimeoutMs?: number;

  /**
   * Per-wait deadline while reading an inbound transport handshake, packet
   * prefix, or packet body. Progress starts a fresh wait and deadline.
   */
  readonly readTimeoutMs?: number;

  /**
   * Deadline for each queued socket write, including transport handshakes.
   *
   * A timeout destroys the connection and causes that write to reject.
   */
  readonly writeTimeoutMs?: number;

  /**
   * Per-read inactivity deadline for synchronization streams.
   *
   * This is not an overall exchange deadline; each successful read starts the
   * next call with a fresh timeout.
   */
  readonly exchangeTimeoutMs?: number;
}

/**
 * Stable failure categories emitted by membership transports.
 *
 * `"address"` covers local option and logical-address validation; `"connect"`
 * and `"tls"` distinguish carrier setup failures; `"protocol"` denotes invalid
 * framing or role use; `"queue"` denotes a configured byte ceiling; read/write
 * codes distinguish carrier errors from deadlines; lifecycle codes distinguish
 * an explicit stop from other invalid state transitions.
 *
 * @internal Callers use these values to separate retryable carrier failures
 * from malformed protocol input and local lifecycle mistakes.
 */
export type TransportErrorCode =
  | "address"
  | "connect"
  | "lifecycle"
  | "protocol"
  | "queue"
  | "read"
  | "read_timeout"
  | "stopped"
  | "tls"
  | "write"
  | "write_timeout";

/**
 * Typed carrier, framing, timeout, or lifecycle failure.
 *
 * @internal Preserve the original `cause` whenever adapting a Node socket
 * error so diagnostics retain its system error code and stack.
 */
export class MembershipTransportError extends Error {
  /** Stable machine-readable category suitable for branching and metrics. */
  readonly code: TransportErrorCode;

  /**
   * Logical peer or local advertised address associated with the failure.
   *
   * It is absent when no identity has been decoded yet.
   */
  readonly peer?: string;

  /**
   * Creates a transport error without discarding an underlying carrier error.
   *
   * `cause`, when supplied, is attached through the standard `Error` cause
   * option. The constructor does not reinterpret the category or peer.
   *
   * @internal Prefer {@link transportError} when adapting an unknown caught
   * value so an existing `MembershipTransportError` is not wrapped again.
   */
  constructor(code: TransportErrorCode, message: string, peer?: string, cause?: unknown) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = "MembershipTransportError";
    this.code = code;
    if (peer !== undefined) {
      this.peer = peer;
    }
  }
}

/**
 * Fixed eight-byte role prefaces: ASCII `NAKT`, protocol version, role, and two
 * reserved zero bytes. The identity length and identity follow separately.
 *
 * @internal Keep these byte layouts synchronized with {@link connectionRole}
 * and the wire protocol specification.
 */
const PREFACE_PACKET = Uint8Array.of(0x4e, 0x41, 0x4b, 0x54, PROTOCOL_VERSION, ROLE_PACKET, 0, 0);
const PREFACE_STREAM = Uint8Array.of(0x4e, 0x41, 0x4b, 0x54, PROTOCOL_VERSION, ROLE_STREAM, 0, 0);

/**
 * Default number of logical destinations retained in the packet LRU.
 *
 * @internal The limit counts pending connection attempts as well as established
 * packet sockets.
 */
export const TCP_PACKET_POOL_SIZE = 32;

/**
 * Default ceiling for each connection's unread-byte queue and pending-write
 * queue.
 *
 * @internal The two queues are accounted independently. Keep this above the
 * largest legal stream frame so a frame can be buffered without configuration
 * changes.
 */
export const TCP_MAX_QUEUED_BYTES = 256 * 1024;

/**
 * Default deadline, in milliseconds, for TCP connect or TLS establishment.
 *
 * @internal Transport handshake writes use the write timeout instead.
 */
export const TCP_CONNECT_TIMEOUT_MS = 1_000;

/**
 * Default per-operation socket read, socket write, and stream-read deadline in
 * milliseconds.
 *
 * @internal This is an inactivity/per-operation limit, not a lifetime limit for
 * a connection or synchronization exchange.
 */
export const TCP_IO_TIMEOUT_MS = 5_000;

/** Shared UTF-8 codec for logical transport identities. */
const encoder = new TextEncoder();

/**
 * Strict UTF-8 decoder used for untrusted identities; malformed sequences are
 * rejected rather than replaced.
 */
const decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * Parsed dial endpoint for a logical membership address.
 *
 * @internal `host` never includes IPv6 brackets; `port` is in `1..65535`.
 */
interface Endpoint {
  /** Hostname, IPv4 literal, or unbracketed IPv6 literal passed to Node. */
  readonly host: string;

  /** Nonzero TCP port validated for outbound dialing. */
  readonly port: number;
}

/**
 * Adapts an unknown failure to a transport category while preserving an
 * already-categorized transport error unchanged.
 *
 * @internal `message`, `peer`, and `cause` are used only when wrapping is
 * necessary, so callers must categorize the boundary where the error arose.
 */
function transportError(
  code: TransportErrorCode,
  message: string,
  peer?: string,
  cause?: unknown,
): MembershipTransportError {
  return cause instanceof MembershipTransportError
    ? cause
    : new MembershipTransportError(code, message, peer, cause);
}

/**
 * Resolves an optional positive-integer setting against its default.
 *
 * @throws {MembershipTransportError} With code `"address"` when the resolved value
 * is zero, negative, fractional, infinite, or outside the safe-integer range.
 *
 * @internal `field` is diagnostic text and does not identify a wire field.
 */
function positiveInteger(value: number | undefined, fallback: number, field: string): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new MembershipTransportError("address", `${field} must be a positive integer`);
  }

  return resolved;
}

/**
 * Parses the transport's textual `host:port` identity into a dial endpoint.
 *
 * Bracketed IPv6 (`[::1]:9000`) is accepted and returned without brackets.
 * Non-bracketed input is split at the final colon, so IPv4, hostnames, and raw
 * IPv6-plus-port representations are accepted when the suffix is a valid port.
 *
 * @throws {MembershipTransportError} With code `"address"` for malformed syntax,
 * an empty host, or a port outside `1..65535`.
 *
 * @internal Parsing validates representation only; it performs no DNS lookup.
 */
/** Canonical decimal port text; rejects aliases such as `0x50`, `1e3`, or padded digits. */
const PORT_TEXT = /^[0-9]{1,5}$/;

/** Builds the shared diagnostic for any malformed textual `host:port` representation. */
function invalidAddress(address: string): MembershipTransportError {
  return new MembershipTransportError("address", `invalid membership address: ${address}`, address);
}

function parseAddress(address: string): Endpoint {
  let host: string;
  let portText: string;
  if (address.startsWith("[")) {
    const close = address.indexOf("]");
    if (close < 2 || address[close + 1] !== ":") {
      throw invalidAddress(address);
    }

    host = address.slice(1, close);
    portText = address.slice(close + 2);
  } else {
    const colon = address.lastIndexOf(":");
    if (colon <= 0) {
      throw invalidAddress(address);
    }

    host = address.slice(0, colon);
    portText = address.slice(colon + 1);
  }

  if (!PORT_TEXT.test(portText)) {
    throw invalidAddress(address);
  }

  const port = Number(portText);
  if (port < 1 || port > 65_535 || host.length === 0) {
    throw invalidAddress(address);
  }

  return { host, port };
}

/**
 * Formats a host and port as the transport's logical address representation.
 *
 * Hosts containing a colon are bracketed unless already bracketed. Other host
 * text is preserved verbatim; validation is intentionally performed elsewhere.
 *
 * @internal
 */
function formatAddress(host: string, port: number): string {
  return `${host.includes(":") && !host.startsWith("[") ? `[${host}]` : host}:${port}`;
}

/**
 * Encodes a local advertised identity for the length-prefixed handshake.
 *
 * @returns Newly allocated UTF-8 bytes that no longer depend on the input
 * string.
 *
 * @throws {MembershipTransportError} With code `"address"` if the identity is
 * empty in UTF-8, exceeds the one-byte length field, or contains NUL.
 *
 * @internal This validates identity encoding, not `host:port` syntax; constructor
 * and bind state establish that separately.
 */
function encodeIdentity(address: string): Uint8Array {
  if (address.includes("\0")) {
    throw new MembershipTransportError("address", "transport address contains NUL", address);
  }

  const bytes = encoder.encode(address);
  if (bytes.length < 1 || bytes.length > MAX_NAME_BYTES) {
    throw new MembershipTransportError(
      "address",
      `transport address must be 1..${MAX_NAME_BYTES} UTF-8 bytes`,
      address,
    );
  }

  return bytes;
}

/**
 * Decodes and validates an untrusted handshake identity.
 *
 * @returns The peer-declared address text after strict UTF-8 and `host:port`
 * validation.
 *
 * @throws {MembershipTransportError} With code `"protocol"` for invalid UTF-8,
 * empty/NUL text, and with code `"address"` when decoded address syntax is
 * invalid.
 *
 * @internal The result is not compared with the socket source address.
 */
function decodeIdentity(bytes: Uint8Array): string {
  let value: string;
  try {
    value = decoder.decode(bytes);
  } catch (cause) {
    throw transportError("protocol", "transport identity is invalid UTF-8", undefined, cause);
  }

  if (value.length === 0 || value.includes("\0")) {
    throw new MembershipTransportError("protocol", "transport identity is invalid");
  }

  parseAddress(value);
  return value;
}

/**
 * Validates the common membership envelope and its legality for a connection
 * role.
 *
 * The envelope is `[version, type, uint16 payloadLength, payload...]`, with the
 * length encoded big-endian by `DataView`. Packet roles permit probe and gossip
 * messages; stream roles permit only synchronization request/response messages.
 *
 * @throws {MembershipTransportError} With code `"protocol"` for a version, length,
 * message-type, or role mismatch.
 *
 * @internal Callers establish minimum byte length before invoking this helper.
 */
function validateEnvelope(bytes: Uint8Array, role: 1 | 2): void {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes[0] !== PROTOCOL_VERSION || view.getUint16(2) + 4 !== bytes.length) {
    throw new MembershipTransportError("protocol", "invalid membership message envelope");
  }

  const type = bytes[1] as number;
  const legalPacket =
    type === MESSAGE_PING ||
    type === MESSAGE_PING_REQ ||
    type === MESSAGE_ACK ||
    type === MESSAGE_NACK ||
    type === MESSAGE_GOSSIP;
  const legalStream = type === MESSAGE_SYNC_REQUEST || type === MESSAGE_SYNC_RESPONSE;
  if (role === ROLE_PACKET ? !legalPacket : !legalStream) {
    throw new MembershipTransportError("protocol", "message type is illegal for connection role");
  }
}

/**
 * Copies one validated packet envelope behind its two-byte big-endian carrier
 * length.
 *
 * @returns A newly allocated frame; subsequent mutation of `bytes` cannot alter
 * the queued packet.
 *
 * @throws {MembershipTransportError} With code `"protocol"` for illegal packet
 * size, envelope shape, version, or message type.
 *
 * @internal
 */
function framePacket(bytes: Uint8Array): Uint8Array {
  if (bytes.length < 4 || bytes.length > MAX_PACKET_BYTES) {
    throw new MembershipTransportError("protocol", "invalid packet frame length");
  }

  validateEnvelope(bytes, ROLE_PACKET);
  const framed = new Uint8Array(2 + bytes.length);
  const view = new DataView(framed.buffer);
  view.setUint16(0, bytes.length);

  framed.set(bytes, 2);
  return framed;
}

/**
 * Builds the initial role-and-identity handshake bytes for a connection.
 *
 * @returns A newly allocated concatenation of the fixed preface, one-byte
 * identity length, and identity. Inputs are copied.
 *
 * @internal The identity length must already fit one byte.
 */
function handshake(preface: Uint8Array, identity: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(preface.length + 1 + identity.length);
  bytes.set(preface);
  bytes[8] = identity.length;
  bytes.set(identity, 9);
  return bytes;
}

/** Compares byte sequences by value without assuming shared backing storage. */
function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return (
    left.length === right.length && left.every((byte, index): boolean => byte === right[index])
  );
}

/**
 * Maps an exact eight-byte handshake preface to packet or stream role.
 *
 * @throws {MembershipTransportError} With code `"protocol"` for wrong magic,
 * version, role, reserved bytes, or length.
 *
 * @internal
 */
function connectionRole(preface: Uint8Array): 1 | 2 {
  if (equalBytes(preface, PREFACE_PACKET)) {
    return ROLE_PACKET;
  }

  if (equalBytes(preface, PREFACE_STREAM)) {
    return ROLE_STREAM;
  }

  throw new MembershipTransportError("protocol", "invalid membership role preface");
}

/**
 * Serializes writes to one socket and bounds bytes waiting for their turn.
 *
 * @internal This queue sits above Node's socket buffer. It does not copy input
 * arrays, so callers retain mutation responsibility until each write settles.
 */
class SocketWriter {
  /** Socket receiving all serialized writes. */
  readonly #socket: Socket;

  /** Logical peer attached to generated diagnostics. */
  readonly #peer: string;

  /** Maximum total input bytes represented by unsettled `write` calls. */
  readonly #maximumQueued: number;

  /** Per-socket-write completion deadline in milliseconds. */
  readonly #timeout: number;

  /** Bytes represented by queued or currently executing writes. */
  #queued = 0;

  /** Settlement chain that preserves invocation order after failures. */
  #tail = Promise.resolve();

  /** Local admission flag; closing does not itself end or destroy the socket. */
  #closed = false;

  /**
   * Attaches an ordered writer to an existing socket.
   *
   * @internal The constructor installs no socket listeners and assumes limits
   * were validated by the owning transport.
   */
  constructor(socket: Socket, peer: string, maximumQueued: number, timeout: number) {
    this.#socket = socket;
    this.#peer = peer;
    this.#maximumQueued = maximumQueued;
    this.#timeout = timeout;
  }

  /**
   * Idempotently rejects future writes without cancelling already queued work.
   *
   * @internal Socket lifecycle remains the owner's responsibility.
   */
  close(): void {
    this.#closed = true;
  }

  /**
   * Admits bytes to the ordered write chain.
   *
   * @returns A promise for this specific socket write. Queue accounting is
   * released when it settles, regardless of outcome.
   *
   * @throws {MembershipTransportError} By rejection when the writer/socket is
   * closed, the queue ceiling would be exceeded, the write times out, or Node
   * reports a carrier failure.
   *
   * @internal The bytes are retained by reference until the operation executes.
   */
  write(bytes: Uint8Array): Promise<void> {
    if (this.#closed || this.#socket.destroyed || !this.#socket.writable) {
      return Promise.reject(
        new MembershipTransportError("write", "connection is not writable", this.#peer),
      );
    }

    if (this.#queued + bytes.length > this.#maximumQueued) {
      return Promise.reject(
        new MembershipTransportError("queue", "connection write queue limit exceeded", this.#peer),
      );
    }

    this.#queued += bytes.length;
    const operation = this.#tail.then((): Promise<void> => this.#writeNow(bytes));
    this.#tail = operation.catch((): void => undefined);
    return operation.finally((): void => {
      this.#queued -= bytes.length;
    });
  }

  /**
   * Performs one admitted Node socket write with a completion deadline.
   *
   * @internal Timeout destroys the socket with the same error used to reject.
   * A successful callback means Node accepted/flushed the local write according
   * to its socket contract; it is not a remote acknowledgement.
   */
  #writeNow(bytes: Uint8Array): Promise<void> {
    return new Promise<void>((resolve, reject): void => {
      const deadline = timedSignal(this.#timeout);
      let settled = false;
      const finish = (error?: MembershipTransportError): void => {
        if (settled) {
          return;
        }

        settled = true;
        deadline.dispose();
        deadline.signal.removeEventListener("abort", timeout);
        if (error === undefined) {
          resolve();
        } else {
          reject(error);
        }
      };

      const timeout = (): void => {
        const error = new MembershipTransportError(
          "write_timeout",
          "connection write timed out",
          this.#peer,
        );
        this.#socket.destroy(error);
        finish(error);
      };

      deadline.signal.addEventListener("abort", timeout, { once: true });
      this.#socket.write(bytes, (cause?: Error | null): void => {
        finish(
          cause == null
            ? undefined
            : transportError("write", "connection write failed", this.#peer, cause),
        );
      });
    });
  }
}

/**
 * Buffers socket data and serves exact-length or currently-available reads.
 *
 * @internal Incoming Node buffers are retained until consumed. Returned arrays
 * are copies, so consumers cannot mutate queued carrier storage.
 */
class ByteReader {
  /** Logical peer label used in read diagnostics. */
  readonly #peer: string;

  /** Maximum bytes that may remain unread across retained chunks. */
  readonly #maximumQueued: number;

  /** FIFO of unconsumed slices received from the socket. */
  #chunks: Buffer[] = [];

  /** Total unconsumed bytes represented by {@link #chunks}. */
  #queued = 0;

  /** Whether the readable carrier emitted `end` or `close`. */
  #ended = false;

  /** Most recent carrier or queue-limit error, if any. */
  #error: Error | undefined;

  /** Resolver for the sole outstanding wait, cleared before invocation. */
  #wake: (() => void) | undefined;

  /**
   * Starts buffering data and terminal events from a socket.
   *
   * Queue overflow records a `"queue"` error and destroys the socket. The
   * reader intentionally does not pause/resume the socket, so the configured
   * ceiling is a hard failure boundary rather than backpressure.
   *
   * @internal Consumers must issue reads sequentially; only one pending waiter
   * is stored.
   */
  constructor(socket: Socket, peer: string, maximumQueued: number) {
    this.#peer = peer;
    this.#maximumQueued = maximumQueued;
    socket.on("data", (chunk: Buffer): void => {
      if (this.#queued + chunk.length > this.#maximumQueued) {
        this.#error = new MembershipTransportError(
          "queue",
          "connection read queue limit exceeded",
          peer,
        );
        socket.destroy(this.#error);
      } else {
        this.#chunks.push(chunk);
        this.#queued += chunk.length;
      }

      this.#notify();
    });

    socket.on("end", (): void => {
      this.#ended = true;
      this.#notify();
    });

    socket.on("close", (): void => {
      this.#ended = true;
      this.#notify();
    });

    socket.on("error", (error: Error): void => {
      this.#error = error;
      this.#notify();
    });
  }

  /**
   * Reads exactly `length` bytes as a newly allocated array.
   *
   * @param timeoutMs Inactivity deadline, or `undefined` to wait indefinitely
   * for a persistent idle connection.
   * @param allowEof When true, a terminal carrier with no buffered bytes returns
   * `undefined`; EOF after a partial result remains a protocol error.
   * @returns Owned bytes, or `undefined` only for the allowed clean-EOF case.
   *
   * @throws {MembershipTransportError} For invalid requested length, queue/carrier
   * failure, timeout, or EOF with an incomplete exact-length read.
   *
   * @internal A zero-length request succeeds immediately with an empty array.
   */
  read(length: number, timeoutMs: number | undefined, allowEof?: false): Promise<Uint8Array>;
  read(
    length: number,
    timeoutMs: number | undefined,
    allowEof: boolean,
  ): Promise<Uint8Array | undefined>;
  async read(
    length: number,
    timeoutMs: number | undefined,
    allowEof = false,
  ): Promise<Uint8Array | undefined> {
    if (!Number.isSafeInteger(length) || length < 0 || length > this.#maximumQueued) {
      throw new MembershipTransportError("protocol", "invalid read length", this.#peer);
    }

    const available = await this.#waitFor((): boolean => this.#queued >= length, timeoutMs);
    if (!available) {
      if (allowEof && this.#queued === 0) {
        return undefined;
      }

      throw new MembershipTransportError(
        "protocol",
        "connection ended with an incomplete frame",
        this.#peer,
      );
    }

    return this.#consume(length);
  }

  /**
   * Reads all bytes currently buffered, waiting when the queue is empty.
   *
   * @returns Newly allocated bytes, or `undefined` when the carrier reaches EOF
   * before any bytes become available.
   *
   * @throws {MembershipTransportError} For carrier/queue failure or inactivity
   * timeout.
   *
   * @internal Data arriving in later turns remains for a subsequent call.
   */
  async readSome(timeoutMs: number): Promise<Uint8Array | undefined> {
    const available = await this.#waitFor((): boolean => this.#queued > 0, timeoutMs);
    return available ? this.#consume(this.#queued) : undefined;
  }

  /**
   * Removes exactly `length` buffered bytes and copies them into owned storage.
   *
   * @internal The caller must establish that enough bytes are queued.
   */
  #consume(length: number): Uint8Array {
    const result = new Uint8Array(length);
    let offset = 0;

    while (offset < length) {
      const chunk = this.#chunks[0] as Buffer;
      const count = Math.min(chunk.length, length - offset);
      result.set(chunk.subarray(0, count), offset);
      offset += count;
      this.#queued -= count;

      if (count === chunk.length) {
        this.#chunks.shift();
      } else {
        this.#chunks[0] = chunk.subarray(count);
      }
    }

    return result;
  }

  /**
   * Waits until a queue predicate succeeds or the carrier terminates.
   *
   * @returns `true` when ready and `false` on terminal EOF before readiness.
   *
   * @internal A fresh abort signal is created for the entire wait loop; an
   * `undefined` timeout waits solely on carrier events. Carrier errors take
   * precedence when observed before EOF.
   */
  async #waitFor(ready: () => boolean, timeoutMs: number | undefined): Promise<boolean> {
    const deadline = timeoutMs === undefined ? undefined : timedSignal(timeoutMs);
    try {
      while (!ready()) {
        if (this.#error !== undefined) {
          throw transportError("read", "connection read failed", this.#peer, this.#error);
        }

        if (this.#ended) {
          return false;
        }

        await this.#wait(deadline?.signal);
      }

      return true;
    } finally {
      deadline?.dispose();
    }
  }

  /**
   * Suspends one read until a socket event wakes it or the deadline aborts.
   *
   * @internal There can be only one waiter. Timeout clears the stored resolver
   * but does not destroy the socket.
   */
  #wait(signal: AbortSignal | undefined): Promise<void> {
    return new Promise<void>((resolve, reject): void => {
      if (signal === undefined) {
        this.#wake = resolve;
        return;
      }

      const aborted = (): void => {
        this.#wake = undefined;
        reject(
          new MembershipTransportError("read_timeout", "connection read timed out", this.#peer),
        );
      };

      signal.addEventListener("abort", aborted, { once: true });
      this.#wake = (): void => {
        signal.removeEventListener("abort", aborted);
        resolve();
      };
    });
  }

  /**
   * Wakes and clears the current read waiter, if one exists.
   *
   * @internal Socket event listeners call this after updating reader state.
   */
  #notify(): void {
    const wake = this.#wake;
    this.#wake = undefined;
    wake?.();
  }
}

/**
 * Lazily initialized packet-pool entry shared by concurrent sends to one
 * destination.
 *
 * @internal The entry is inserted before dialing completes. `socket` and
 * `writer` become available before the role handshake write; callers must await
 * `ready` before writing packets.
 */
interface PacketConnection {
  /** Dialed socket once carrier establishment succeeds. */
  socket?: Socket;

  /** Ordered writer associated with {@link socket}. */
  writer?: SocketWriter;

  /** Resolves after dialing and the packet-role handshake write complete. */
  ready: Promise<void>;
}

/**
 * Incrementally validates length-prefixed synchronization envelopes while
 * preserving arbitrary stream chunk boundaries.
 *
 * @internal The validator is observational: it copies only partial frame state
 * for validation and does not transform bytes sent to or returned from callers.
 */
class StreamFrameValidator {
  /** Four-byte big-endian frame-length prefix under construction. */
  #prefix = new Uint8Array(4);

  /** Number of prefix bytes currently accumulated. */
  #prefixBytes = 0;

  /** Declared envelope buffer under construction, absent between frames. */
  #message: Uint8Array | undefined;

  /** Number of bytes copied into the current envelope. */
  #messageBytes = 0;

  /**
   * Whether all pushed bytes end exactly on a frame boundary.
   *
   * @internal `true` also describes the initial state; it does not mean any
   * frame has been observed.
   */
  get complete(): boolean {
    return this.#prefixBytes === 0 && this.#message === undefined;
  }

  /**
   * Incorporates bytes into framing state and validates each completed envelope.
   *
   * One call may contain partial, single, or multiple frames. Completed frame
   * storage is discarded immediately after envelope validation.
   *
   * @throws {MembershipTransportError} With code `"protocol"` for an invalid frame
   * length or a non-synchronization envelope.
   *
   * @internal State after an exception must be treated as unusable; stream
   * callers close the associated connection.
   */
  push(bytes: Uint8Array): void {
    let offset = 0;
    while (offset < bytes.length) {
      if (this.#message === undefined) {
        const count = Math.min(4 - this.#prefixBytes, bytes.length - offset);
        this.#prefix.set(bytes.subarray(offset, offset + count), this.#prefixBytes);
        this.#prefixBytes += count;
        offset += count;
        if (this.#prefixBytes < 4) {
          return;
        }

        const length = new DataView(this.#prefix.buffer).getUint32(0);
        if (length < 4 || length > MAX_SYNC_MESSAGE_BYTES) {
          throw new MembershipTransportError("protocol", "invalid stream frame length");
        }

        this.#message = new Uint8Array(length);
        this.#messageBytes = 0;
      }

      const message = this.#message as Uint8Array;
      const count = Math.min(message.length - this.#messageBytes, bytes.length - offset);
      message.set(bytes.subarray(offset, offset + count), this.#messageBytes);
      this.#messageBytes += count;
      offset += count;
      if (this.#messageBytes === message.length) {
        validateEnvelope(message, ROLE_STREAM);
        this.#prefixBytes = 0;
        this.#message = undefined;
        this.#messageBytes = 0;
      }
    }
  }
}

/**
 * Dedicated synchronization connection with framing and aggregate byte limits.
 *
 * @internal Each instance owns one socket and is intentionally excluded from
 * the packet LRU. Read and write exchange budgets are tracked independently.
 */
class TcpMembershipStream implements MembershipStream {
  /** Peer-declared or dial-target logical identity for this connection. */
  readonly remoteAddress: string;

  /** Dedicated carrier socket, ended by {@link close}. */
  readonly #socket: Socket;

  /** Buffered reader shared with inbound handshake parsing when applicable. */
  readonly #reader: ByteReader;

  /** Ordered writer for outbound stream chunks. */
  readonly #writer: SocketWriter;

  /** Per-call stream read inactivity deadline in milliseconds. */
  readonly #timeout: number;

  /** Total carrier bytes returned by successful reads. */
  #readBytes = 0;

  /** Total carrier bytes included in successful writes. */
  #writtenBytes = 0;

  /** Whether local close admission has occurred. */
  #closed = false;

  /** Incremental validator for bytes received from the peer. */
  readonly #readFrames = new StreamFrameValidator();

  /** Incremental validator for bytes submitted by the local caller. */
  readonly #writtenFrames = new StreamFrameValidator();

  /**
   * Wraps an already-established, stream-role connection.
   *
   * @internal The role handshake is consumed before inbound construction and
   * written before outbound construction. This constructor takes lifecycle
   * ownership of `socket` but does not install transport-wide tracking.
   */
  constructor(
    socket: Socket,
    remoteAddress: string,
    reader: ByteReader,
    writer: SocketWriter,
    timeout: number,
  ) {
    this.remoteAddress = remoteAddress;
    this.#socket = socket;
    this.#reader = reader;
    this.#writer = writer;
    this.#timeout = timeout;
  }

  /**
   * Returns the next available carrier chunk after exchange-budget and
   * incremental-frame validation.
   *
   * @returns Newly allocated bytes, or `undefined` after clean EOF/local close.
   * The returned chunk can contain any number or fraction of framed messages.
   *
   * @throws {MembershipTransportError} For read timeout/failure, aggregate inbound
   * bytes beyond `MAX_SYNC_EXCHANGE_BYTES`, or malformed/incomplete framing.
   *
   * @internal Protocol and exchange-limit failures close the stream. A read
   * timeout or underlying read failure is propagated without an explicit local
   * close here.
   */
  async read(): Promise<Uint8Array | undefined> {
    if (this.#closed) {
      return undefined;
    }

    const bytes = await this.#reader.readSome(this.#timeout);
    if (bytes === undefined) {
      if (!this.#readFrames.complete) {
        this.close();
        throw new MembershipTransportError(
          "protocol",
          "stream ended with an incomplete frame",
          this.remoteAddress,
        );
      }

      return undefined;
    }

    if (this.#readBytes + bytes.length > MAX_SYNC_EXCHANGE_BYTES) {
      this.close();
      throw new MembershipTransportError(
        "protocol",
        "stream exchange limit exceeded",
        this.remoteAddress,
      );
    }

    try {
      this.#readFrames.push(bytes);
    } catch (error) {
      this.close();
      throw error;
    }

    this.#readBytes += bytes.length;
    return bytes;
  }

  /**
   * Validates and queues a caller-provided stream chunk in invocation order.
   *
   * @returns A promise that resolves after the underlying local socket write.
   *
   * @throws {MembershipTransportError} For aggregate outbound bytes beyond
   * `MAX_SYNC_EXCHANGE_BYTES`, malformed framing, queue overflow, write timeout,
   * or carrier failure.
   *
   * @internal Framing/limit failures close the stream before rejection. Bytes
   * count toward the exchange budget only after the socket write succeeds and
   * remain caller-owned until the promise settles.
   */
  async write(bytes: Uint8Array): Promise<void> {
    if (this.#writtenBytes + bytes.length > MAX_SYNC_EXCHANGE_BYTES) {
      this.close();
      throw new MembershipTransportError(
        "protocol",
        "stream exchange limit exceeded",
        this.remoteAddress,
      );
    }

    try {
      this.#writtenFrames.push(bytes);
    } catch (error) {
      this.close();
      throw error;
    }

    // Reserve the budget before awaiting so concurrent writes cannot both pass
    // the limit check; a failed carrier write releases the reservation.
    this.#writtenBytes += bytes.length;
    try {
      await this.#writer.write(bytes);
    } catch (error) {
      this.#writtenBytes -= bytes.length;
      throw error;
    }
  }

  /**
   * Idempotently closes local admission and half-closes the socket.
   *
   * @internal Already queued writer operations are not cancelled by
   * `SocketWriter.close`; `socket.end()` controls their carrier disposition.
   */
  close(): void {
    if (this.#closed) {
      return;
    }

    this.#closed = true;
    this.#writer.close();
    if (!this.#socket.destroyed) {
      this.#socket.end();
    }
  }
}

/**
 * TCP/TLS implementation of the membership packet-and-stream transport.
 *
 * Each carrier begins with a fixed role preface and a peer-declared logical
 * `host:port` identity. Packet-role sockets are reused through a destination
 * LRU and carry two-byte-length-prefixed envelopes. Stream-role sockets are
 * dedicated to one bounded synchronization exchange whose messages use
 * four-byte length prefixes.
 *
 * @internal Listener failures quiesce all sockets and are surfaced by later
 * operations and by `stop`, rather than being silently swallowed.
 */
export class TcpMembershipTransport implements MembershipTransport {
  /** Constructor options retained for deferred listener and socket creation. */
  readonly #options: TcpMembershipTransportOptions;

  /** UTF-8 bytes of the current advertised address used in new handshakes. */
  #identity: Uint8Array;

  /** Maximum number of destination entries retained by the packet LRU. */
  readonly #maxPool: number;

  /** Independent per-reader and per-writer queued-byte ceiling. */
  readonly #maxQueued: number;

  /** TCP/TLS establishment deadline in milliseconds. */
  readonly #connectTimeout: number;

  /** Per-wait inbound handshake and packet read deadline in milliseconds. */
  readonly #readTimeout: number;

  /** Per-write socket completion deadline in milliseconds. */
  readonly #writeTimeout: number;

  /** Per-call synchronization-stream read deadline in milliseconds. */
  readonly #exchangeTimeout: number;

  /** Accepted and outbound sockets not yet observed closed. */
  readonly #sockets = new Set<Socket>();

  /** Access-ordered destination map for pending and ready packet connections. */
  readonly #packetPool = new Map<string, PacketConnection>();

  /** Advertised logical identity, updated after an ephemeral bind. */
  #address: string;

  /** Listener while bound, including the pre-handler state created by `bind`. */
  #server: Server | undefined;

  /** First handler set installed by `start`, absent before start/after stop. */
  #handlers: TransportHandlers | undefined;

  /** Shared listener-startup operation once `start` is called. */
  #startPromise: Promise<void> | undefined;

  /** Shared teardown operation once `stop` is called. */
  #stopPromise: Promise<void> | undefined;

  /** Sticky post-bind listener failure surfaced by public operations. */
  #serverFailure: MembershipTransportError | undefined;

  /** Terminal lifecycle flag set by stop or listener failure. */
  #stopped = false;

  /**
   * Creates an unbound TCP/TLS transport and validates local configuration.
   *
   * The initial address uses `advertiseHost ?? host` and the configured port;
   * when the port is zero, a successful bind replaces it with the assigned
   * port. Construction performs no DNS lookup, bind, or certificate parsing.
   *
   * @throws {MembershipTransportError} With code `"address"` for invalid bind or
   * positive-integer settings, or `"tls"` for an incomplete certificate/key
   * pair.
   *
   * @internal Prefer {@link TcpMembershipTransport.bind} when consumers need the
   * final address before installing handlers.
   */
  constructor(options: TcpMembershipTransportOptions) {
    if (
      options.host.length === 0 ||
      !Number.isInteger(options.port) ||
      options.port < 0 ||
      options.port > 65_535
    ) {
      throw new MembershipTransportError("address", "invalid bind host or port");
    }

    if ((options.tls?.cert === undefined) !== (options.tls?.key === undefined)) {
      throw new MembershipTransportError(
        "tls",
        "TLS certificate and key must be configured together",
      );
    }

    this.#options = options;
    this.#address = formatAddress(options.advertiseHost ?? options.host, options.port);
    this.#identity = encodeIdentity(this.#address);
    this.#maxPool = positiveInteger(
      options.maxPacketConnections,
      TCP_PACKET_POOL_SIZE,
      "pool size",
    );
    this.#maxQueued = positiveInteger(
      options.maxQueuedBytes,
      TCP_MAX_QUEUED_BYTES,
      "queued byte limit",
    );
    this.#connectTimeout = positiveInteger(
      options.connectTimeoutMs,
      TCP_CONNECT_TIMEOUT_MS,
      "connect timeout",
    );
    this.#readTimeout = positiveInteger(options.readTimeoutMs, TCP_IO_TIMEOUT_MS, "read timeout");
    this.#writeTimeout = positiveInteger(
      options.writeTimeoutMs,
      TCP_IO_TIMEOUT_MS,
      "write timeout",
    );
    this.#exchangeTimeout = positiveInteger(
      options.exchangeTimeoutMs,
      TCP_IO_TIMEOUT_MS,
      "exchange timeout",
    );
  }

  /**
   * Creates and binds a transport before returning.
   *
   * This resolves an ephemeral port so {@link address} can be passed into SWIM
   * state before handlers are installed. Connections arriving before `start`
   * are rejected. The returned transport still requires `start`.
   *
   * @returns A bound transport whose advertised identity contains the actual
   * listener port.
   *
   * @throws {MembershipTransportError} For option validation, TLS listener setup,
   * or bind failure.
   *
   * @internal
   */
  static async bind(options: TcpMembershipTransportOptions): Promise<TcpMembershipTransport> {
    const transport = new TcpMembershipTransport(options);
    await transport.#bind();
    return transport;
  }

  /**
   * Logical address advertised in outbound handshakes and membership records.
   *
   * The representation is `host:port`, with IPv6 hosts bracketed by the
   * formatter. `advertiseHost` affects only this identity, not listener binding.
   *
   * @internal
   */
  get address(): string {
    return this.#address;
  }

  /**
   * Number of accepted and outbound sockets not yet observed closed.
   *
   * This includes packet and stream roles, handshakes in progress, and sockets
   * awaiting close notification; it excludes the listener itself.
   *
   * @internal
   */
  get activeConnections(): number {
    return this.#sockets.size;
  }

  /**
   * Number of logical destinations currently retained in the packet LRU.
   *
   * Entries still dialing/handshaking count. The value is therefore not a count
   * of established sockets and can temporarily describe failed work until its
   * rejection cleanup runs.
   *
   * @internal
   */
  get pooledPacketConnections(): number {
    return this.#packetPool.size;
  }

  /**
   * Installs inbound handlers and ensures the listener is bound.
   *
   * The first call owns the handler set. Concurrent or later calls return the
   * same startup promise and ignore replacement handlers. A bind failure clears
   * the handlers so a later call may retry; an explicit stop is terminal.
   *
   * @returns A shared promise that resolves when listener binding completes.
   *
   * @throws {MembershipTransportError} By rejection for a sticky listener failure,
   * restart after stop, TLS listener setup failure, or bind failure.
   *
   * @internal Repeated calls retain the first handler set.
   */
  start(handlers: TransportHandlers): Promise<void> {
    if (this.#serverFailure !== undefined) {
      return Promise.reject(this.#serverFailure);
    }

    if (this.#stopped) {
      return Promise.reject(
        new MembershipTransportError(
          "lifecycle",
          "cannot restart a stopped transport",
          this.#address,
        ),
      );
    }

    if (this.#handlers !== undefined) {
      return this.#startPromise as Promise<void>;
    }

    this.#handlers = handlers;
    const starting = this.#bind().catch((error: unknown): never => {
      this.#handlers = undefined;
      throw error;
    });

    this.#startPromise = starting;
    return starting;
  }

  /**
   * Tears down the listener and all transport-tracked connections.
   *
   * Stop is terminal and may be called before, during, or after start. It waits
   * for an in-flight startup attempt, destroys tracked sockets, clears the
   * packet pool and handlers, and waits for a listening server's close callback.
   *
   * @returns One shared teardown promise for all calls.
   *
   * @throws {MembershipTransportError} By rejection after cleanup when the listener
   * previously failed post-bind.
   *
   * @internal Repeated calls share one teardown promise. A post-bind listener
   * failure is reported after resources have been released.
   */
  stop(): Promise<void> {
    if (this.#stopPromise !== undefined) {
      return this.#stopPromise;
    }

    this.#stopped = true;
    const stopping = (async (): Promise<void> => {
      await this.#startPromise?.catch((): void => undefined);
      this.#closeConnections();
      const server = this.#server;
      this.#server = undefined;
      if (server?.listening) {
        await new Promise<void>((resolve): void => {
          server.close((): void => resolve());
        });
      }

      this.#handlers = undefined;
      if (this.#serverFailure !== undefined) {
        throw this.#serverFailure;
      }
    })();
    this.#stopPromise = stopping;
    return stopping;
  }

  /**
   * Validates and sends one packet through the destination's pooled connection.
   *
   * The envelope is copied into a two-byte-length-prefixed frame before the
   * asynchronous dial/write path, so caller mutation after invocation cannot
   * change the transmitted packet. Concurrent sends to one destination share
   * connection readiness and are serialized by its writer.
   *
   * @returns A promise for completion of the local framed socket write.
   *
   * @throws {MembershipTransportError} For stopped/failed lifecycle, invalid target
   * or envelope, connect/TLS failure, queue overflow, or write failure/timeout.
   * Any connection-path failure discards that destination's matching pool entry.
   *
   * @internal Invalid envelopes are rejected before dialing.
   */
  async packet(to: string, bytes: Uint8Array): Promise<void> {
    this.#assertOperational(to);
    const framed = framePacket(bytes);
    const connection = this.#packetConnection(to);

    try {
      await connection.ready;
      await (connection.writer as SocketWriter).write(framed);
    } catch (error) {
      this.#discardPacket(to, connection);
      throw error;
    }
  }

  /**
   * Opens and handshakes a dedicated synchronization-stream connection.
   *
   * The target is parsed and dialed, then the local stream-role preface and
   * identity are written before this method resolves. The connection is tracked
   * for transport shutdown but never inserted into the packet LRU.
   *
   * @returns A stream with independent inbound/outbound exchange byte budgets.
   *
   * @throws {MembershipTransportError} For stopped/failed lifecycle, invalid target,
   * connect/TLS failure, or handshake write failure/timeout.
   *
   * @internal Stream payload bytes remain opaque after frame validation.
   */
  async stream(to: string): Promise<MembershipStream> {
    this.#assertOperational(to);
    const { socket, reader, writer } = await this.#openConnection(to);

    try {
      await writer.write(handshake(PREFACE_STREAM, this.#identity));
      return new TcpMembershipStream(socket, to, reader, writer, this.#exchangeTimeout);
    } catch (error) {
      socket.destroy();
      throw error;
    }
  }

  #assertOperational(peer: string): void {
    if (this.#serverFailure !== undefined) {
      throw this.#serverFailure;
    }

    if (this.#stopped) {
      throw new MembershipTransportError("stopped", "transport is stopped", peer);
    }
  }

  #packetConnection(to: string): PacketConnection {
    const pooled = this.#packetPool.get(to);
    if (pooled !== undefined && pooled.socket?.destroyed !== true) {
      this.#packetPool.delete(to);
      this.#packetPool.set(to, pooled);
      return pooled;
    }

    if (pooled !== undefined) {
      this.#packetPool.delete(to);
    }

    const opened = this.#openPacket(to);
    this.#packetPool.set(to, opened);
    this.#evictPackets();
    return opened;
  }

  #discardPacket(to: string, connection: PacketConnection): void {
    if (this.#packetPool.get(to) === connection) {
      this.#packetPool.delete(to);
    }

    connection.writer?.close();
    connection.socket?.destroy();
  }

  #openPacket(to: string): PacketConnection {
    const connection: PacketConnection = {
      ready: Promise.resolve(),
    };

    connection.ready = this.#openConnection(to).then(async ({ socket, writer }): Promise<void> => {
      connection.socket = socket;
      connection.writer = writer;
      await writer.write(handshake(PREFACE_PACKET, this.#identity));
    });

    void connection.ready.catch((): void => undefined);
    return connection;
  }

  #evictPackets(): void {
    while (this.#packetPool.size > this.#maxPool) {
      const oldest = this.#packetPool.entries().next().value as [string, PacketConnection];

      this.#packetPool.delete(oldest[0]);
      void oldest[1].ready
        .finally((): void => {
          oldest[1].writer?.close();
          oldest[1].socket?.destroy();
        })
        .catch((): void => undefined);
    }
  }

  #closeConnections(): void {
    for (const connection of this.#packetPool.values()) {
      connection.writer?.close();
    }

    this.#packetPool.clear();

    for (const socket of this.#sockets) {
      socket.destroy();
    }

    this.#sockets.clear();
  }

  async #bind(): Promise<void> {
    if (this.#server !== undefined) {
      return;
    }

    const tls = this.#options.tls;
    const accepted = (socket: Socket): void => this.#accept(socket);
    let server: Server;
    try {
      server =
        tls === undefined
          ? createTcpServer({ allowHalfOpen: true }, accepted)
          : createTlsServer(
              {
                cert: tls.cert,
                key: tls.key,
                ca: tls.ca,
                requestCert: tls.requestCert === true,
                rejectUnauthorized: tls.requestCert === true,
                allowHalfOpen: true,
              } satisfies TlsOptions,
              accepted,
            );
    } catch (cause) {
      throw transportError("tls", "invalid TLS listener configuration", this.#address, cause);
    }

    this.#server = server;
    server.on("connection", (socket: Socket): void => this.#track(socket));
    await new Promise<void>((resolve, reject): void => {
      const failed = (cause: Error): void => {
        this.#server = undefined;
        reject(
          transportError(
            tls === undefined ? "connect" : "tls",
            "membership listener failed to bind",
            this.#address,
            cause,
          ),
        );
      };

      server.once("error", failed);
      server.listen(this.#options.port, this.#options.host, (): void => {
        server.removeListener("error", failed);
        server.on("error", (cause: Error): void => this.#handleServerFailure(server, cause));
        const bound = server.address() as Exclude<ReturnType<Server["address"]>, string | null>;

        if (this.#options.port === 0) {
          this.#address = formatAddress(
            this.#options.advertiseHost ?? this.#options.host,
            bound.port,
          );
          const identity = encodeIdentity(this.#address);
          this.#identity = identity;
        }

        resolve();
      });
    });
  }

  #handleServerFailure(server: Server, cause: Error): void {
    if (this.#server !== server) {
      return;
    }

    this.#serverFailure = transportError(
      "lifecycle",
      "membership listener failed after binding",
      this.#address,
      cause,
    );
    this.#stopped = true;
    this.#server = undefined;
    this.#handlers = undefined;
    this.#closeConnections();

    if (server.listening) {
      server.close();
    }
  }

  #accept(socket: Socket): void {
    if (this.#stopped || this.#handlers === undefined) {
      socket.destroy();
      return;
    }

    this.#track(socket);
    const reader = new ByteReader(socket, "inbound", this.#maxQueued);
    void this.#readHandshake(socket, reader).catch((error: unknown): void => {
      socket.destroy(error instanceof Error ? error : undefined);
    });
  }

  async #readHandshake(socket: Socket, reader: ByteReader): Promise<void> {
    const preface = await reader.read(8, this.#readTimeout);
    const role = connectionRole(preface);
    const length = (await reader.read(1, this.#readTimeout))[0] as number;

    if (length === 0) {
      throw new MembershipTransportError("protocol", "empty transport identity");
    }

    const from = decodeIdentity(await reader.read(length, this.#readTimeout));

    if (role === ROLE_PACKET) {
      await this.#servePacketConnection(socket, reader, from);
      return;
    }

    await this.#serveStreamConnection(socket, reader, from);
  }

  async #servePacketConnection(socket: Socket, reader: ByteReader, from: string): Promise<void> {
    try {
      await this.#readPackets(socket, reader, from);
    } finally {
      socket.destroy();
    }
  }

  async #serveStreamConnection(socket: Socket, reader: ByteReader, from: string): Promise<void> {
    const writer = new SocketWriter(socket, from, this.#maxQueued, this.#writeTimeout);
    const membershipStream = new TcpMembershipStream(
      socket,
      from,
      reader,
      writer,
      this.#exchangeTimeout,
    );

    try {
      await this.#handlers?.stream(from, membershipStream);
    } catch (error) {
      membershipStream.close();
      throw error;
    }
  }

  async #readPackets(socket: Socket, reader: ByteReader, from: string): Promise<void> {
    while (!socket.destroyed) {
      // A packet connection is persistent: waiting for the next frame is not
      // bounded by the read timeout. Once a frame starts, its remaining bytes
      // must arrive within the timeout.
      const first = await reader.read(1, undefined, true);
      if (first === undefined) {
        return;
      }

      const second = await reader.read(1, this.#readTimeout);
      const length = ((first[0] as number) << 8) | (second[0] as number);
      if (length < 4 || length > MAX_PACKET_BYTES) {
        throw new MembershipTransportError("protocol", "invalid packet frame length", from);
      }

      const bytes = await reader.read(length, this.#readTimeout);
      validateEnvelope(bytes, ROLE_PACKET);
      await this.#handlers?.packet(from, bytes);
    }
  }

  async #openConnection(
    peer: string,
  ): Promise<{ socket: Socket; reader: ByteReader; writer: SocketWriter }> {
    const socket = await this.#dial(peer);
    return {
      socket,
      reader: new ByteReader(socket, peer, this.#maxQueued),
      writer: new SocketWriter(socket, peer, this.#maxQueued, this.#writeTimeout),
    };
  }

  #dial(peer: string): Promise<Socket> {
    const endpoint = parseAddress(peer);
    return new Promise<Socket>((resolve, reject): void => {
      const deadline = timedSignal(this.#connectTimeout);
      const tls = this.#options.tls;
      const dialFailure = (cause: unknown): MembershipTransportError =>
        transportError(
          tls === undefined ? "connect" : "tls",
          "membership connection failed",
          peer,
          cause,
        );
      let socket: Socket;

      try {
        socket = this.#createSocket(endpoint);
      } catch (cause) {
        deadline.dispose();
        reject(dialFailure(cause));
        return;
      }

      let settled = false;
      const connectedEvent = tls === undefined ? "connect" : "secureConnect";
      const finish = (error?: MembershipTransportError): void => {
        if (settled) {
          return;
        }

        settled = true;
        deadline.dispose();
        deadline.signal.removeEventListener("abort", timeout);
        socket.removeListener(connectedEvent, connected);
        socket.removeListener("error", failed);
        if (error !== undefined) {
          socket.destroy();
          reject(error);
          return;
        }

        // A dial that completes after stop must not outlive the transport.
        if (this.#stopped) {
          socket.destroy();
          reject(new MembershipTransportError("stopped", "transport is stopped", peer));
          return;
        }

        this.#track(socket);
        resolve(socket);
      };

      const connected = (): void => finish();
      const failed = (cause: Error): void => finish(dialFailure(cause));
      const timeout = (): void =>
        finish(new MembershipTransportError("connect", "membership connection timed out", peer));
      deadline.signal.addEventListener("abort", timeout, { once: true });
      socket.once(connectedEvent, connected);
      socket.once("error", failed);
    });
  }

  #createSocket(endpoint: Endpoint): Socket {
    const tcpOptions: TcpSocketConnectOpts = {
      host: endpoint.host,
      port: endpoint.port,
    };

    const tls = this.#options.tls;

    if (tls === undefined) {
      return connectTcp(tcpOptions);
    }

    const tlsOptions: TlsConnectionOptions = {
      ...tcpOptions,
      cert: tls.cert,
      key: tls.key,
      ca: tls.ca,
      rejectUnauthorized: true,
    };

    if (tls.servername !== undefined) {
      tlsOptions.servername = tls.servername;
    }

    return connectTls(tlsOptions);
  }

  #track(socket: Socket): void {
    if (this.#sockets.has(socket)) {
      return;
    }

    this.#sockets.add(socket);
    socket.on("error", (): void => undefined);
    socket.once("close", (): void => {
      this.#sockets.delete(socket);
      for (const [peer, connection] of this.#packetPool) {
        if (connection.socket === socket) {
          connection.writer?.close();
          this.#packetPool.delete(peer);
        }
      }
    });
  }
}
