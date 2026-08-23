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

import { type AddressInfo, connect, createServer, type Server, type Socket } from "node:net";
import { expect, vi } from "vitest";
import { FramedConn, type OutboundFrame } from "../../src/_net/conn";
import { decodeHello, encodeHello, type Hello } from "../../src/_net/envelope";
import {
  FRAME_ERROR,
  FRAME_HELLO_ACK,
  type FrameHeader,
  LANE_CONTROL,
  ProtocolError,
} from "../../src/_net/frame";
import type { Peer } from "../../src/_net/peer";
import { NetServer, type NetServerHandlers, type NetServerOptions } from "../../src/_net/server";
import {
  negotiateHello,
  Session,
  type SessionHandlers,
  type SessionOptions,
} from "../../src/_net/session";
import type { TimerHandle, Timers } from "../../src/_net/timers";
import { ByteReader, ByteWriter } from "../../src/_net/values";

/**
 * Shared transport-test scaffolding: the HELLO fixture, resource
 * tracking with one cleanup call for `afterEach`, and the common ways
 * of standing up servers, raw listeners, sockets, and sessions.
 */

export const MIB: number = 1024 * 1024;
export const EMPTY: Uint8Array = new Uint8Array(0);

export function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve: () => void): void => {
    setTimeout(resolve, ms);
  });
}

/** One scheduled deadline on the manual clock. */
interface ManualEntry {
  readonly at: number;
  readonly fn: () => void;
  canceled: boolean;
}

/**
 * A hand-cranked transport clock: `now` only moves when a test calls
 * {@link ManualTimers.advance}, and due deadlines fire in time order
 * as it passes them. Deadline tests drive this instead of racing the
 * real clock.
 */
export class ManualTimers implements Timers {
  private _now: number = 0;
  private readonly _entries: ManualEntry[] = [];

  now(): number {
    return this._now;
  }

  schedule(delayMs: number, fn: () => void): TimerHandle {
    const entry: ManualEntry = { at: this._now + delayMs, fn, canceled: false };
    this._entries.push(entry);
    return {
      cancel: (): void => {
        entry.canceled = true;
      },
    };
  }

  /** Moves the clock forward, firing every deadline it passes. */
  advance(ms: number): void {
    const target: number = this._now + ms;
    for (;;) {
      let next: ManualEntry | null = null;
      for (const entry of this._entries) {
        if (entry.canceled || entry.at > target) {
          continue;
        }

        if (next === null || entry.at < next.at) {
          next = entry;
        }
      }

      if (next === null) {
        break;
      }

      this._entries.splice(this._entries.indexOf(next), 1);
      this._now = next.at;
      next.fn();
    }

    this._now = target;
  }
}

export function hello(overrides: Partial<Hello> = {}): Hello {
  return {
    revision: 4,
    systemName: "orders",
    host: "127.0.0.1",
    port: 0,
    lane: LANE_CONTROL,
    compression: 0,
    maxFrameSize: 16 * MIB,
    maxMessageSize: 16 * MIB,
    initialCredits: 16 * MIB,
    maxLargeTransfers: 4,
    ...overrides,
  };
}

const servers: NetServer[] = [];
const listeners: Server[] = [];
const sockets: Socket[] = [];
const sessions: Session[] = [];
const peers: Peer[] = [];

export function trackServer(server: NetServer): NetServer {
  servers.push(server);
  return server;
}

export function trackSocket(socket: Socket): Socket {
  sockets.push(socket);
  return socket;
}

export function trackSession(session: Session): Session {
  sessions.push(session);
  return session;
}

export function trackPeer(peer: Peer): Peer {
  peers.push(peer);
  return peer;
}

/** Tears down everything tracked since the last call. */
export async function cleanupNet(): Promise<void> {
  for (const peer of peers) {
    peer.close();
  }

  for (const session of sessions) {
    session.destroy();
  }

  for (const socket of sockets) {
    socket.destroy();
  }

  for (const listener of listeners) {
    listener.close();
  }

  for (const server of servers) {
    await server.shutdown(-1);
  }

  peers.length = 0;
  sessions.length = 0;
  sockets.length = 0;
  listeners.length = 0;
  servers.length = 0;
}

/** Starts a tracked NetServer on port zero with the fixture HELLO. */
export async function startServer(
  options: Partial<NetServerOptions> = {},
  handlers: NetServerHandlers = {},
): Promise<NetServer> {
  const server: NetServer = await NetServer.listen(
    { local: hello({ systemName: "server" }), ...options },
    handlers,
  );
  return trackServer(server);
}

/** Starts a tracked raw TCP listener; returns its port. */
export async function startRawListener(onSocket: (socket: Socket) => void): Promise<number> {
  const listener: Server = createServer(onSocket);
  listeners.push(listener);
  await new Promise<void>((resolve): void => {
    listener.listen(0, "127.0.0.1", resolve);
  });
  return (listener.address() as AddressInfo).port;
}

/** Opens a tracked client socket to a local port. */
export function dialSocket(port: number): Socket {
  return trackSocket(connect(port, "127.0.0.1"));
}

/** Dials a tracked session to a local port with the fixture HELLO. */
export async function dialSession(
  port: number,
  local: Hello = hello({ systemName: "client" }),
  handlers: SessionHandlers = {},
  options: SessionOptions = {},
): Promise<Session> {
  const session: Session = await Session.dial(dialSocket(port), local, handlers, options);
  return trackSession(session);
}

/** A raw framed endpoint for playing a misbehaving or scripted peer. */
export class RawPeer {
  readonly frames: { header: FrameHeader; body: Uint8Array }[] = [];
  readonly conn: FramedConn;
  closed: boolean = false;

  constructor(socket: Socket, maxFrameSize: number = MIB) {
    this.conn = new FramedConn(
      socket,
      {
        onFrame: (header: FrameHeader, body: Uint8Array): void => {
          this.frames.push({ header, body: Uint8Array.from(body) });
        },
        onViolation: (): void => {},
        onClose: (): void => {
          this.closed = true;
        },
      },
      { maxFrameSize },
    );
  }

  send(frame: OutboundFrame): void {
    this.conn.send(frame);
  }

  sendHello(type: number, parameters: Hello): void {
    const writer: ByteWriter = new ByteWriter();
    encodeHello(writer, parameters);
    this.conn.send({
      type,
      flags: 0,
      lane: parameters.lane,
      correlation: 0,
      body: Uint8Array.from(writer.bytes()),
    });
  }

  /** Runs `fn` once the first frame (the dialer's HELLO) arrives. */
  onFirstFrame(fn: () => void): void {
    const poll: NodeJS.Timeout = setInterval((): void => {
      if (this.frames.length === 0) {
        return;
      }

      clearInterval(poll);
      fn();
    }, 5);
  }
}

/**
 * Dials a real session against a scripted acceptor that completes the
 * handshake and then does only what the test tells it to.
 */
export async function dialScripted(
  overrides: Partial<Hello>,
  handlers: SessionHandlers = {},
  options: SessionOptions = {},
): Promise<{ client: Session; peer: RawPeer }> {
  let scripted: RawPeer | null = null;
  const port: number = await startRawListener((socket: Socket): void => {
    const raw: RawPeer = new RawPeer(socket);
    scripted = raw;
    raw.onFirstFrame((): void => {
      const request: Hello = decodeHello(new ByteReader(raw.frames[0]?.body ?? EMPTY));
      raw.sendHello(
        FRAME_HELLO_ACK,
        negotiateHello(hello({ systemName: "raw", ...overrides }), request),
      );
    });
  });

  const client: Session = await Session.dial(
    dialSocket(port),
    hello({ systemName: "client", ...overrides }),
    handlers,
    options,
  );
  trackSession(client);
  if (scripted === null) {
    throw new Error("the scripted peer never saw the connection");
  }

  return { client, peer: scripted };
}

/**
 * Dials a scripted acceptor, injects the given frames, and asserts
 * the client dies with a connection-scoped {@link ProtocolError}
 * whose message contains `message`. Callers add any frame-level
 * assertions of their own on the returned raw peer.
 */
export async function expectProtocolDeath(
  overrides: Partial<Hello>,
  frames: { type: number; body: Uint8Array | null; flags?: number; correlation?: number }[],
  message: string,
): Promise<{ client: Session; peer: RawPeer; closeError: Error }> {
  let closeError: Error | null = null;
  const { client, peer } = await dialScripted(overrides, {
    onClose: (_session: Session, error: Error | null): void => {
      closeError = error;
    },
  });
  for (const frame of frames) {
    peer.send({
      type: frame.type,
      flags: frame.flags ?? 0,
      lane: LANE_CONTROL,
      correlation: frame.correlation ?? 0,
      body: frame.body,
    });
  }

  await vi.waitFor((): void => {
    expect(closeError).toBeInstanceOf(ProtocolError);
  });
  expect((closeError as Error | null)?.message).toContain(message);
  // Every connection-scoped violation is answered with a final ERROR
  // notice before the socket closes.
  await vi.waitFor((): void => {
    const notice: { header: FrameHeader } | undefined = peer.frames.find(
      (frame): boolean => frame.header.type === FRAME_ERROR,
    );
    expect(notice?.header.correlation).toBe(0);
  });
  return { client, peer, closeError: closeError as unknown as Error };
}

/** A connected pair of tracked raw sockets over a loopback listener. */
export async function socketPair(): Promise<{ client: Socket; server: Socket }> {
  const listener: Server = createServer();
  listeners.push(listener);
  await new Promise<void>((resolve): void => {
    listener.listen(0, "127.0.0.1", resolve);
  });
  const port: number = (listener.address() as AddressInfo).port;
  const accepted: Promise<Socket> = new Promise<Socket>((resolve): void => {
    listener.once("connection", resolve);
  });
  const client: Socket = connect(port, "127.0.0.1");
  await new Promise<void>((resolve): void => {
    client.once("connect", resolve);
  });
  const server: Socket = await accepted;
  trackSocket(client);
  trackSocket(server);
  return { client, server };
}
