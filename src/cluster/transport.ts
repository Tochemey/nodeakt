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

/** One ordered byte stream between addressed endpoints. */
export interface ClusterStream {
  readonly remoteAddress: string;
  read(): Promise<Uint8Array | undefined>;
  write(bytes: Uint8Array): Promise<void>;
  close(): void;
}

/** Inbound callbacks installed while a transport endpoint is started. */
export interface TransportHandlers {
  packet(from: string, bytes: Uint8Array): void | Promise<void>;
  stream(from: string, stream: ClusterStream): void | Promise<void>;
}

/** Packet-and-stream seam consumed by probe and synchronization protocol code. */
export interface ClusterTransport {
  readonly address: string;
  start(handlers: TransportHandlers): Promise<void>;
  stop(): Promise<void>;
  packet(to: string, bytes: Uint8Array): Promise<void>;
  stream(to: string): Promise<ClusterStream>;
}

export interface ClusterTlsOptions {
  readonly cert: string | Buffer;
  readonly key: string | Buffer;
  readonly ca?: string | Buffer;
  readonly requestCert?: boolean;
  readonly servername?: string;
}

export interface TcpClusterTransportOptions {
  readonly host: string;
  readonly port: number;
  readonly advertiseHost?: string;
  readonly tls?: ClusterTlsOptions;
  readonly maxPacketConnections?: number;
  readonly maxQueuedBytes?: number;
  readonly connectTimeoutMs?: number;
  readonly readTimeoutMs?: number;
  readonly writeTimeoutMs?: number;
  readonly exchangeTimeoutMs?: number;
}

export type TransportErrorCode =
  | "address"
  | "connect"
  | "lifecycle"
  | "protocol"
  | "queue"
  | "read_timeout"
  | "stopped"
  | "tls"
  | "write"
  | "write_timeout";

/** A typed carrier, framing, timeout, or lifecycle failure. */
export class ClusterTransportError extends Error {
  readonly code: TransportErrorCode;
  readonly peer?: string;

  constructor(code: TransportErrorCode, message: string, peer?: string, cause?: unknown) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = "ClusterTransportError";
    this.code = code;
    if (peer !== undefined) {
      this.peer = peer;
    }
  }
}

const PREFACE_PACKET = Uint8Array.of(0x4e, 0x41, 0x4b, 0x54, 1, 1, 0, 0);
const PREFACE_STREAM = Uint8Array.of(0x4e, 0x41, 0x4b, 0x54, 1, 2, 0, 0);
const MAX_PACKET_BYTES = 1400;
const MAX_STREAM_FRAME_BYTES = 65_539;
const MAX_STREAM_EXCHANGE_BYTES = 1_048_576;
const DEFAULT_POOL_SIZE = 32;
const DEFAULT_QUEUED_BYTES = 256 * 1024;
const DEFAULT_CONNECT_TIMEOUT_MS = 1000;
const DEFAULT_IO_TIMEOUT_MS = 5000;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

interface Endpoint {
  readonly host: string;
  readonly port: number;
}

function transportError(
  code: TransportErrorCode,
  message: string,
  peer?: string,
  cause?: unknown,
): ClusterTransportError {
  return cause instanceof ClusterTransportError
    ? cause
    : new ClusterTransportError(code, message, peer, cause);
}

function positiveInteger(value: number | undefined, fallback: number, field: string): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new ClusterTransportError("address", `${field} must be a positive integer`);
  }
  return resolved;
}

function parseAddress(address: string): Endpoint {
  let host: string;
  let portText: string;
  if (address.startsWith("[")) {
    const close = address.indexOf("]");
    if (close < 2 || address[close + 1] !== ":") {
      throw new ClusterTransportError("address", `invalid cluster address: ${address}`, address);
    }
    host = address.slice(1, close);
    portText = address.slice(close + 2);
  } else {
    const colon = address.lastIndexOf(":");
    if (colon <= 0) {
      throw new ClusterTransportError("address", `invalid cluster address: ${address}`, address);
    }
    host = address.slice(0, colon);
    portText = address.slice(colon + 1);
  }
  const port = Number(portText);
  if (!Number.isInteger(port) || port < 1 || port > 65_535 || host.length === 0) {
    throw new ClusterTransportError("address", `invalid cluster address: ${address}`, address);
  }
  return { host, port };
}

function formatAddress(host: string, port: number): string {
  return `${host.includes(":") && !host.startsWith("[") ? `[${host}]` : host}:${port}`;
}

function encodeIdentity(address: string): Uint8Array {
  if (address.includes("\0")) {
    throw new ClusterTransportError("address", "transport address contains NUL", address);
  }
  const bytes = encoder.encode(address);
  if (bytes.length < 1 || bytes.length > 255) {
    throw new ClusterTransportError(
      "address",
      "transport address must be 1..255 UTF-8 bytes",
      address,
    );
  }
  return bytes;
}

function decodeIdentity(bytes: Uint8Array): string {
  let value: string;
  try {
    value = decoder.decode(bytes);
  } catch (cause) {
    throw transportError("protocol", "transport identity is invalid UTF-8", undefined, cause);
  }
  if (value.length === 0 || value.includes("\0")) {
    throw new ClusterTransportError("protocol", "transport identity is invalid");
  }
  parseAddress(value);
  return value;
}

function validateEnvelope(bytes: Uint8Array, role: 1 | 2): void {
  const maximum = role === 1 ? MAX_PACKET_BYTES : MAX_STREAM_FRAME_BYTES;
  if (bytes.length < 4 || bytes.length > maximum) {
    throw new ClusterTransportError(
      "protocol",
      `invalid ${role === 1 ? "packet" : "stream"} frame length`,
    );
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes[0] !== 1 || view.getUint16(2) + 4 !== bytes.length) {
    throw new ClusterTransportError("protocol", "invalid cluster message envelope");
  }
  const type = bytes[1] as number;
  if (role === 1 ? type < 1 || type > 5 : type !== 0x10 && type !== 0x11) {
    throw new ClusterTransportError("protocol", "message type is illegal for connection role");
  }
}

function frame(bytes: Uint8Array, role: 1 | 2): Uint8Array {
  validateEnvelope(bytes, role);
  const prefix = role === 1 ? 2 : 4;
  const framed = new Uint8Array(prefix + bytes.length);
  const view = new DataView(framed.buffer);
  if (role === 1) {
    view.setUint16(0, bytes.length);
  } else {
    view.setUint32(0, bytes.length);
  }
  framed.set(bytes, prefix);
  return framed;
}

function handshake(preface: Uint8Array, identity: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(preface.length + 1 + identity.length);
  bytes.set(preface);
  bytes[8] = identity.length;
  bytes.set(identity, 9);
  return bytes;
}

function timedSignal(milliseconds: number): AbortSignal {
  return AbortSignal.timeout(milliseconds);
}

class SocketWriter {
  readonly #socket: Socket;
  readonly #peer: string;
  readonly #maximumQueued: number;
  readonly #timeout: number;
  #queued = 0;
  #tail = Promise.resolve();
  #closed = false;

  constructor(socket: Socket, peer: string, maximumQueued: number, timeout: number) {
    this.#socket = socket;
    this.#peer = peer;
    this.#maximumQueued = maximumQueued;
    this.#timeout = timeout;
  }

  close(): void {
    this.#closed = true;
  }

  write(bytes: Uint8Array): Promise<void> {
    if (this.#closed || this.#socket.destroyed || !this.#socket.writable) {
      return Promise.reject(
        new ClusterTransportError("write", "connection is not writable", this.#peer),
      );
    }
    if (this.#queued + bytes.length > this.#maximumQueued) {
      return Promise.reject(
        new ClusterTransportError("queue", "connection write queue limit exceeded", this.#peer),
      );
    }
    this.#queued += bytes.length;
    const operation = this.#tail.then((): Promise<void> => this.#writeNow(bytes));
    this.#tail = operation.catch((): void => undefined);
    return operation.finally((): void => {
      this.#queued -= bytes.length;
    });
  }

  #writeNow(bytes: Uint8Array): Promise<void> {
    return new Promise<void>((resolve, reject): void => {
      const signal = timedSignal(this.#timeout);
      let settled = false;
      const finish = (error?: ClusterTransportError): void => {
        if (settled) {
          return;
        }
        settled = true;
        signal.removeEventListener("abort", timeout);
        if (error === undefined) {
          resolve();
        } else {
          reject(error);
        }
      };
      const timeout = (): void => {
        const error = new ClusterTransportError(
          "write_timeout",
          "connection write timed out",
          this.#peer,
        );
        this.#socket.destroy(error);
        finish(error);
      };
      signal.addEventListener("abort", timeout, { once: true });
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

class ByteReader {
  readonly #peer: string;
  readonly #maximumQueued: number;
  #chunks: Buffer[] = [];
  #queued = 0;
  #ended = false;
  #error: Error | undefined;
  #wake: (() => void) | undefined;

  constructor(socket: Socket, peer: string, maximumQueued: number) {
    this.#peer = peer;
    this.#maximumQueued = maximumQueued;
    socket.on("data", (chunk: Buffer): void => {
      if (this.#queued + chunk.length > this.#maximumQueued) {
        this.#error = new ClusterTransportError(
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

  get queued(): number {
    return this.#queued;
  }

  async read(length: number, timeoutMs: number, allowEof = false): Promise<Uint8Array | undefined> {
    if (!Number.isSafeInteger(length) || length < 0 || length > this.#maximumQueued) {
      throw new ClusterTransportError("protocol", "invalid read length", this.#peer);
    }
    const signal = timedSignal(timeoutMs);
    while (this.#queued < length) {
      if (this.#error !== undefined) {
        throw transportError("read_timeout", "connection read failed", this.#peer, this.#error);
      }
      if (this.#ended) {
        if (allowEof && this.#queued === 0) {
          return undefined;
        }
        throw new ClusterTransportError(
          "protocol",
          "connection ended with an incomplete frame",
          this.#peer,
        );
      }
      await new Promise<void>((resolve, reject): void => {
        const aborted = (): void => {
          this.#wake = undefined;
          reject(
            new ClusterTransportError("read_timeout", "connection read timed out", this.#peer),
          );
        };
        signal.addEventListener("abort", aborted, { once: true });
        this.#wake = (): void => {
          signal.removeEventListener("abort", aborted);
          resolve();
        };
      });
    }
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

  async readSome(timeoutMs: number): Promise<Uint8Array | undefined> {
    while (this.#queued === 0) {
      if (this.#error !== undefined) {
        throw transportError("read_timeout", "connection read failed", this.#peer, this.#error);
      }
      if (this.#ended) {
        return undefined;
      }
      const signal = timedSignal(timeoutMs);
      await new Promise<void>((resolve, reject): void => {
        const aborted = (): void => {
          this.#wake = undefined;
          reject(
            new ClusterTransportError("read_timeout", "connection read timed out", this.#peer),
          );
        };
        signal.addEventListener("abort", aborted, { once: true });
        this.#wake = (): void => {
          signal.removeEventListener("abort", aborted);
          resolve();
        };
      });
    }
    return this.read(this.#queued, timeoutMs) as Promise<Uint8Array>;
  }

  #notify(): void {
    const wake = this.#wake;
    this.#wake = undefined;
    wake?.();
  }
}

interface PacketConnection {
  socket?: Socket;
  writer?: SocketWriter;
  readonly ready: Promise<void>;
}

class StreamFrameValidator {
  #prefix = new Uint8Array(4);
  #prefixBytes = 0;
  #message: Uint8Array | undefined;
  #messageBytes = 0;

  get complete(): boolean {
    return this.#prefixBytes === 0 && this.#message === undefined;
  }

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
        if (length < 4 || length > MAX_STREAM_FRAME_BYTES) {
          throw new ClusterTransportError("protocol", "invalid stream frame length");
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
        validateEnvelope(message, 2);
        this.#prefixBytes = 0;
        this.#message = undefined;
        this.#messageBytes = 0;
      }
    }
  }
}

class TcpClusterStream implements ClusterStream {
  readonly remoteAddress: string;
  readonly #socket: Socket;
  readonly #reader: ByteReader;
  readonly #writer: SocketWriter;
  readonly #timeout: number;
  #readBytes = 0;
  #writtenBytes = 0;
  #closed = false;
  readonly #readFrames = new StreamFrameValidator();
  readonly #writtenFrames = new StreamFrameValidator();

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

  async read(): Promise<Uint8Array | undefined> {
    if (this.#closed) {
      return undefined;
    }
    const bytes = await this.#reader.readSome(this.#timeout);
    if (bytes === undefined) {
      if (!this.#readFrames.complete) {
        this.close();
        throw new ClusterTransportError(
          "protocol",
          "stream ended with an incomplete frame",
          this.remoteAddress,
        );
      }
      return undefined;
    }
    if (this.#readBytes + bytes.length > MAX_STREAM_EXCHANGE_BYTES) {
      this.close();
      throw new ClusterTransportError(
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

  async write(bytes: Uint8Array): Promise<void> {
    if (this.#writtenBytes + bytes.length > MAX_STREAM_EXCHANGE_BYTES) {
      this.close();
      throw new ClusterTransportError(
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
    await this.#writer.write(bytes);
    this.#writtenBytes += bytes.length;
  }

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
 * TCP implementation of the cluster transport. Packet connections are pooled
 * by logical destination; stream connections are one exchange each.
 */
export class TcpClusterTransport implements ClusterTransport {
  readonly #options: TcpClusterTransportOptions;
  #identity: Uint8Array;
  readonly #maxPool: number;
  readonly #maxQueued: number;
  readonly #connectTimeout: number;
  readonly #readTimeout: number;
  readonly #writeTimeout: number;
  readonly #exchangeTimeout: number;
  readonly #sockets = new Set<Socket>();
  readonly #packetPool = new Map<string, PacketConnection>();
  #address: string;
  #server: Server | undefined;
  #handlers: TransportHandlers | undefined;
  #startPromise: Promise<void> | undefined;
  #stopPromise: Promise<void> | undefined;
  #stopped = false;

  constructor(options: TcpClusterTransportOptions) {
    if (
      options.host.length === 0 ||
      !Number.isInteger(options.port) ||
      options.port < 0 ||
      options.port > 65_535
    ) {
      throw new ClusterTransportError("address", "invalid bind host or port");
    }
    if ((options.tls?.cert === undefined) !== (options.tls?.key === undefined)) {
      throw new ClusterTransportError("tls", "TLS certificate and key must be configured together");
    }
    this.#options = options;
    this.#address = formatAddress(options.advertiseHost ?? options.host, options.port);
    this.#identity = encodeIdentity(this.#address);
    this.#maxPool = positiveInteger(options.maxPacketConnections, DEFAULT_POOL_SIZE, "pool size");
    this.#maxQueued = positiveInteger(
      options.maxQueuedBytes,
      DEFAULT_QUEUED_BYTES,
      "queued byte limit",
    );
    this.#connectTimeout = positiveInteger(
      options.connectTimeoutMs,
      DEFAULT_CONNECT_TIMEOUT_MS,
      "connect timeout",
    );
    this.#readTimeout = positiveInteger(
      options.readTimeoutMs,
      DEFAULT_IO_TIMEOUT_MS,
      "read timeout",
    );
    this.#writeTimeout = positiveInteger(
      options.writeTimeoutMs,
      DEFAULT_IO_TIMEOUT_MS,
      "write timeout",
    );
    this.#exchangeTimeout = positiveInteger(
      options.exchangeTimeoutMs,
      DEFAULT_IO_TIMEOUT_MS,
      "exchange timeout",
    );
  }

  /**
   * Binds before returning, resolving an ephemeral port so `address` can be
   * passed to the SWIM constructor. Call `start` afterwards to install handlers.
   */
  static async bind(options: TcpClusterTransportOptions): Promise<TcpClusterTransport> {
    const transport = new TcpClusterTransport(options);
    await transport.#bind();
    return transport;
  }

  get address(): string {
    return this.#address;
  }

  /** Current live sockets, exposed for operational diagnostics. */
  get activeConnections(): number {
    return this.#sockets.size;
  }

  /** Current destinations retained in the packet LRU. */
  get pooledPacketConnections(): number {
    return this.#packetPool.size;
  }

  start(handlers: TransportHandlers): Promise<void> {
    if (this.#stopped) {
      return Promise.reject(
        new ClusterTransportError("lifecycle", "cannot restart a stopped transport", this.#address),
      );
    }
    if (this.#handlers !== undefined) {
      return this.#startPromise ?? Promise.resolve();
    }
    this.#handlers = handlers;
    const starting = this.#bind().catch((error: unknown): never => {
      this.#handlers = undefined;
      throw error;
    });
    this.#startPromise = starting;
    return starting;
  }

  stop(): Promise<void> {
    if (this.#stopPromise !== undefined) {
      return this.#stopPromise;
    }
    this.#stopped = true;
    const stopping = (async (): Promise<void> => {
      await this.#startPromise?.catch((): void => undefined);
      for (const connection of this.#packetPool.values()) {
        connection.writer?.close();
      }
      this.#packetPool.clear();
      for (const socket of this.#sockets) {
        socket.destroy();
      }
      this.#sockets.clear();
      const server = this.#server;
      this.#server = undefined;
      if (server?.listening) {
        await new Promise<void>((resolve): void => {
          server.close((): void => resolve());
        });
      }
      this.#handlers = undefined;
    })();
    this.#stopPromise = stopping;
    return stopping;
  }

  async packet(to: string, bytes: Uint8Array): Promise<void> {
    if (this.#stopped) {
      throw new ClusterTransportError("stopped", "transport is stopped", to);
    }
    const framed = frame(bytes, 1);
    let connection = this.#packetPool.get(to);
    if (connection === undefined || connection.socket?.destroyed === true) {
      if (connection !== undefined) {
        this.#packetPool.delete(to);
      }
      connection = this.#openPacket(to);
      this.#packetPool.set(to, connection);
      this.#evictPackets();
    } else {
      this.#packetPool.delete(to);
      this.#packetPool.set(to, connection);
    }
    try {
      await connection.ready;
      await (connection.writer as SocketWriter).write(framed);
    } catch (error) {
      if (this.#packetPool.get(to) === connection) {
        this.#packetPool.delete(to);
      }
      connection.socket?.destroy();
      throw error;
    }
  }

  async stream(to: string): Promise<ClusterStream> {
    if (this.#stopped) {
      throw new ClusterTransportError("stopped", "transport is stopped", to);
    }
    const socket = await this.#dial(to);
    const reader = new ByteReader(socket, to, this.#maxQueued);
    const writer = new SocketWriter(socket, to, this.#maxQueued, this.#writeTimeout);
    try {
      await writer.write(handshake(PREFACE_STREAM, this.#identity));
      return new TcpClusterStream(socket, to, reader, writer, this.#exchangeTimeout);
    } catch (error) {
      socket.destroy();
      throw error;
    }
  }

  #openPacket(to: string): PacketConnection {
    const connection: PacketConnection = {
      ready: Promise.resolve(),
    };
    const ready = this.#dial(to).then(async (connected): Promise<void> => {
      connection.socket = connected;
      connection.writer = new SocketWriter(connected, to, this.#maxQueued, this.#writeTimeout);
      await connection.writer.write(handshake(PREFACE_PACKET, this.#identity));
    });
    Object.assign(connection, { ready });
    void ready.catch((): void => undefined);
    return connection;
  }

  #evictPackets(): void {
    while (this.#packetPool.size > this.#maxPool) {
      const oldest = this.#packetPool.entries().next().value as
        | [string, PacketConnection]
        | undefined;
      if (oldest === undefined) {
        return;
      }
      this.#packetPool.delete(oldest[0]);
      void oldest[1].ready
        .finally((): void => {
          oldest[1].writer?.close();
          oldest[1].socket?.destroy();
        })
        .catch((): void => undefined);
    }
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
    server.on("error", (): void => undefined);
    await new Promise<void>((resolve, reject): void => {
      const failed = (cause: Error): void => {
        this.#server = undefined;
        reject(
          transportError(
            tls === undefined ? "connect" : "tls",
            "cluster listener failed to bind",
            this.#address,
            cause,
          ),
        );
      };
      server.once("error", failed);
      server.listen(this.#options.port, this.#options.host, (): void => {
        server.removeListener("error", failed);
        const bound = server.address();
        if (bound === null || typeof bound === "string") {
          server.close();
          this.#server = undefined;
          reject(new ClusterTransportError("address", "listener returned no TCP address"));
          return;
        }
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
    const preface = (await reader.read(8, this.#readTimeout)) as Uint8Array;
    const packet = preface.every((byte, index): boolean => byte === PREFACE_PACKET[index]);
    const stream = preface.every((byte, index): boolean => byte === PREFACE_STREAM[index]);
    if (!packet && !stream) {
      throw new ClusterTransportError("protocol", "invalid cluster role preface");
    }
    const length = ((await reader.read(1, this.#readTimeout)) as Uint8Array)[0] as number;
    if (length === 0) {
      throw new ClusterTransportError("protocol", "empty transport identity");
    }
    const from = decodeIdentity((await reader.read(length, this.#readTimeout)) as Uint8Array);
    if (packet) {
      await this.#readPackets(socket, reader, from);
      return;
    }
    const writer = new SocketWriter(socket, from, this.#maxQueued, this.#writeTimeout);
    const clusterStream = new TcpClusterStream(socket, from, reader, writer, this.#exchangeTimeout);
    try {
      await this.#handlers?.stream(from, clusterStream);
    } catch (error) {
      clusterStream.close();
      throw error;
    }
  }

  async #readPackets(socket: Socket, reader: ByteReader, from: string): Promise<void> {
    while (!socket.destroyed) {
      const prefix = await reader.read(2, this.#readTimeout, true);
      if (prefix === undefined) {
        return;
      }
      const length = new DataView(prefix.buffer, prefix.byteOffset, 2).getUint16(0);
      if (length < 4 || length > MAX_PACKET_BYTES) {
        throw new ClusterTransportError("protocol", "invalid packet frame length", from);
      }
      const bytes = (await reader.read(length, this.#readTimeout)) as Uint8Array;
      validateEnvelope(bytes, 1);
      await this.#handlers?.packet(from, bytes);
    }
  }

  #dial(peer: string): Promise<Socket> {
    const endpoint = parseAddress(peer);
    return new Promise<Socket>((resolve, reject): void => {
      const signal = timedSignal(this.#connectTimeout);
      const tcpOptions: TcpSocketConnectOpts = {
        host: endpoint.host,
        port: endpoint.port,
      };
      const tls = this.#options.tls;
      let socket: Socket;
      if (tls === undefined) {
        socket = connectTcp(tcpOptions);
      } else {
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
        socket = connectTls(tlsOptions);
      }
      let settled = false;
      const finish = (error?: ClusterTransportError): void => {
        if (settled) {
          return;
        }
        settled = true;
        signal.removeEventListener("abort", timeout);
        socket.removeListener(tls === undefined ? "connect" : "secureConnect", connected);
        socket.removeListener("error", failed);
        if (error === undefined) {
          this.#track(socket);
          resolve(socket);
        } else {
          socket.destroy();
          reject(error);
        }
      };
      const connected = (): void => finish();
      const failed = (cause: Error): void =>
        finish(
          transportError(
            tls === undefined ? "connect" : "tls",
            "cluster connection failed",
            peer,
            cause,
          ),
        );
      const timeout = (): void =>
        finish(new ClusterTransportError("connect", "cluster connection timed out", peer));
      signal.addEventListener("abort", timeout, { once: true });
      socket.once(tls === undefined ? "connect" : "secureConnect", connected);
      socket.once("error", failed);
    });
  }

  #track(socket: Socket): void {
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
