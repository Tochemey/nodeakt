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

import type { KvTransport } from "./kv/ports";
import {
  type DataEnvelope,
  ERROR_INTERNAL,
  ERROR_UNAVAILABLE,
  type Hello,
  KIND_ASK,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "./net/envelope";
import { LANE_CONTROL } from "./net/frame";
import { Peer } from "./net/peer";
import { NetServer } from "./net/server";
import { REVISION_CURRENT, type Session } from "./net/session";
import type { TlsConfig } from "./net/tls";

/** System name announced in the carrier handshake; both ends of a cluster share it. */
const KV_SYSTEM_NAME: string = "nodeakt-kv";

/** Largest carrier frame, in bytes; the carrier splits a larger message across frames. */
const KV_MAX_FRAME_SIZE: number = 16 * 1024 * 1024;

/**
 * Largest logical message, in bytes. It comfortably exceeds a fragment chunk
 * (`FRAGMENT_CHUNK_BYTES`, 256 KiB), so a whole chunk rides one request and large
 * transfers use the carrier's dedicated lane rather than being refused.
 */
const KV_MAX_MESSAGE_SIZE: number = 16 * 1024 * 1024;

/** Initial per-lane flow-control credits, in bytes. */
const KV_INITIAL_CREDITS: number = 16 * 1024 * 1024;

/** Concurrent large transfers a session admits. */
const KV_MAX_LARGE_TRANSFERS: number = 4;

/** Grace, in milliseconds, allowed for in-flight work to settle when closing the listener. */
const KV_CLOSE_GRACE_MS: number = 1_000;

/** Error name reported when the store's inbound handler rejects a request. */
const KV_HANDLER_ERROR_NAME: string = "KvHandlerError";

/**
 * The carrier DATA kind for a request that expects a correlated reply.
 *
 * At the carrier level {@link KIND_ASK} means only "this DATA frame expects a
 * REPLY", the request/response selector the store's addressed request maps onto.
 * It is unrelated to the actor ask pattern: the store is actor-blind, and the
 * reply is driven by the frame's expects-reply flag and correlation id, not by
 * this byte. Naming it here keeps that meaning local to the store.
 */
const KV_REQUEST_KIND: number = KIND_ASK;

/** Construction parameters for a {@link KvNetTransport}. */
export interface KvNetTransportOptions {
  /** Bind host, also advertised to peers as part of this node's data endpoint. */
  readonly host: string;
  /** Bind port; zero binds an ephemeral port, resolved by {@link KvNetTransport.start}. */
  readonly port: number;
  /** Carrier system name; defaults to a shared cluster constant. */
  readonly systemName?: string;
  /** TLS material; when set, the listener and every dial use it, and plaintext is refused. */
  readonly tls?: TlsConfig;
}

/**
 * The store's {@link KvTransport} over the `src/net` carrier.
 *
 * The store frames its own protocol bytes, so this adapter needs only an
 * addressed request that returns a correlated reply, plus an inbound handler.
 * The carrier supplies exactly that, with correlation, connection pooling,
 * reconnect, flow control, TLS, and large transfers already solved, and it is
 * blind to actors and to the store alike.
 *
 * Outbound, one {@link Peer} per target member is cached by the `host:port`
 * identity the store dials; a request wraps the store bytes in the carrier's
 * envelope and awaits the correlated reply's payload, which rejects on the
 * deadline, exactly the "no delivery guarantee, surfaced as a timeout" contract
 * the store is written against. Inbound, one listener hands each request's bytes
 * and the dialer's advertised endpoint to the store, then answers with the
 * store's response bytes, or a carrier error when the store has no handler or
 * its handler rejects.
 */
export class KvNetTransport implements KvTransport {
  /** The local carrier handshake, its port resolved to the bound one after start. */
  #local: Hello;
  /** TLS material applied to the listener and every dial, or undefined for plaintext. */
  readonly #tls: TlsConfig | undefined;
  /** One pooled peer per target `host:port`, created on first use. */
  readonly #peers: Map<string, Peer> = new Map<string, Peer>();
  /** The inbound listener, bound by start and shut down by close. */
  #server: NetServer | undefined;
  /** The store's inbound dispatch, installed through listen. */
  #handler: ((from: string, body: Uint8Array) => Promise<Uint8Array>) | undefined;
  /** Set by close so later requests reject instead of dialing. */
  #closed: boolean = false;

  /** @param options The bind endpoint, optional carrier system name, and optional TLS. */
  constructor(options: KvNetTransportOptions) {
    this.#tls = options.tls;
    this.#local = {
      revision: REVISION_CURRENT,
      systemName: options.systemName ?? KV_SYSTEM_NAME,
      host: options.host,
      port: options.port,
      lane: LANE_CONTROL,
      compression: 0,
      maxFrameSize: KV_MAX_FRAME_SIZE,
      maxMessageSize: KV_MAX_MESSAGE_SIZE,
      initialCredits: KV_INITIAL_CREDITS,
      maxLargeTransfers: KV_MAX_LARGE_TRANSFERS,
    };
  }

  /** This node's bound data endpoint as `host:port`, valid after {@link start}. */
  get address(): string {
    return formatHostPort(this.#local.host, this.#local.port);
  }

  /**
   * Count of pooled outbound peers, one per distinct target dialed. Lets a test
   * observe that repeated requests to one member reuse a single peer.
   *
   * @internal
   */
  get openPeers(): number {
    return this.#peers.size;
  }

  /**
   * Binds the inbound listener and resolves the advertised port to the bound one.
   * Awaited once at boot before the node answers requests.
   */
  async start(): Promise<void> {
    const listenerOptions: { host: string; port: number; local: Hello } = {
      host: this.#local.host,
      port: this.#local.port,
      local: this.#local,
    };
    const server: NetServer = await NetServer.listen(
      this.#tls === undefined ? listenerOptions : { ...listenerOptions, tls: this.#tls },
      {
        onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
          this.#serve(session, envelope, correlation);
        },
      },
    );
    this.#server = server;
    this.#local = { ...this.#local, port: server.address.port };
  }

  /**
   * Sends `body` to the member at `to` and resolves with the reply payload, or
   * rejects when the deadline elapses or the carrier cannot deliver.
   */
  async request(to: string, body: Uint8Array, deadlineMs: number): Promise<Uint8Array> {
    if (this.#closed) {
      throw new Error("kv transport is closed");
    }

    // The carrier's ref slots (to, uid, sender, senderUid, typeRef) exist to
    // address an actor. The store frames its own addressing inside the payload,
    // so it leaves every ref empty and the carrier moves the bytes untouched.
    const envelope: DataEnvelope = {
      kind: KV_REQUEST_KIND,
      to: "",
      uid: "",
      sender: "",
      senderUid: "",
      timeout: deadlineMs,
      serializerId: SERIALIZER_BINARY,
      typeRef: "",
      payload: body,
    };
    const reply: ReplyEnvelope = await this.#peerFor(to).ask(envelope, deadlineMs);
    return reply.payload;
  }

  /** Installs the store's inbound dispatch; its resolved bytes become each reply. */
  listen(handler: (from: string, body: Uint8Array) => Promise<Uint8Array>): void {
    this.#handler = handler;
  }

  /** Closes every pooled peer and the listener; later requests reject. */
  async close(): Promise<void> {
    this.#closed = true;
    for (const peer of this.#peers.values()) {
      peer.close();
    }

    this.#peers.clear();
    if (this.#server !== undefined) {
      await this.#server.shutdown(KV_CLOSE_GRACE_MS);
      this.#server = undefined;
    }
  }

  /** Returns the pooled peer for `to`, dialing lazily on first use. */
  #peerFor(to: string): Peer {
    const existing: Peer | undefined = this.#peers.get(to);
    if (existing !== undefined) {
      return existing;
    }

    const { host, port }: { host: string; port: number } = parseHostPort(to);
    const peer: Peer = new Peer(
      host,
      port,
      this.#local,
      {},
      this.#tls === undefined ? {} : { tls: this.#tls },
    );
    this.#peers.set(to, peer);
    return peer;
  }

  /** Answers one inbound request through the store's handler, or a carrier error. */
  #serve(session: Session, envelope: DataEnvelope, correlation: number): void {
    const handler: ((from: string, body: Uint8Array) => Promise<Uint8Array>) | undefined =
      this.#handler;
    if (handler === undefined) {
      session.replyError(correlation, {
        code: ERROR_UNAVAILABLE,
        sentinel: 0,
        name: KV_HANDLER_ERROR_NAME,
        message: "kv transport has no inbound handler",
      });
      return;
    }

    const remote: Hello = session.remote as Hello;
    const from: string = formatHostPort(remote.host, remote.port);
    handler(from, envelope.payload).then(
      (response: Uint8Array): void => {
        session.reply(correlation, {
          serializerId: SERIALIZER_BINARY,
          typeRef: "",
          payload: response,
        });
      },
      (error: unknown): void => {
        session.replyError(correlation, {
          code: ERROR_INTERNAL,
          sentinel: 0,
          name: KV_HANDLER_ERROR_NAME,
          message: String(error),
        });
      },
    );
  }
}

/** Joins a host and port into `host:port`, bracketing an IPv6 literal for clarity. */
export function formatHostPort(host: string, port: number): string {
  return host.includes(":") ? `[${host}]:${port}` : `${host}:${port}`;
}

/**
 * Splits a `host:port` identity into its parts, unwrapping a bracketed IPv6 host.
 *
 * @throws {Error} If the identity has no port or the port is out of range.
 */
export function parseHostPort(address: string): { host: string; port: number } {
  const lastColon: number = address.lastIndexOf(":");
  if (lastColon <= 0 || lastColon === address.length - 1) {
    throw new Error(`kv transport address is not host:port: ${address}`);
  }

  // Number() would also accept hex, exponential, and whitespace-padded forms, so
  // the port segment must be plain decimal digits before it is read as a number.
  const portText: string = address.slice(lastColon + 1);
  const port: number = Number(portText);
  if (!/^[0-9]+$/.test(portText) || port < 1 || port > 65_535) {
    throw new Error(`kv transport address has an invalid port: ${address}`);
  }

  const rawHost: string = address.slice(0, lastColon);
  const host: string =
    rawHost.startsWith("[") && rawHost.endsWith("]") ? rawHost.slice(1, -1) : rawHost;
  return { host, port };
}
