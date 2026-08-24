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

import { type AddressInfo, createServer, type Server, type Socket } from "node:net";
import { createServer as createTlsServer } from "node:tls";
import { type DataEnvelope, ERROR_UNAVAILABLE, type Hello } from "./envelope";
import { Session, type SessionHandlers, type SessionOptions } from "./session";
import { defaultTimers, type TimerHandle, type Timers } from "./timers";
import type { TlsConfig } from "./tls";

/**
 * NetServer owns exactly the listener lifecycle: bind, accept, hand
 * each connection to a {@link Session} for its handshake, track
 * accounting, and drain on shutdown. Everything after accept is the
 * session's business, and everything above frames is the owner's.
 *
 * @internal
 */

/** TCP keep-alive delay applied to every accepted socket. */
const KEEP_ALIVE_MS: number = 15_000;

/** Default idle reclaim for server-side connections: 20 minutes. */
const DEFAULT_CONN_IDLE_MS: number = 20 * 60 * 1000;

export interface NetServerOptions {
  /** Bind host; every interface by default. */
  readonly host?: string;
  /** Bind port; zero picks a free one. */
  readonly port?: number;
  /**
   * The local HELLO parameters answered to every dialer. The lane
   * field is ignored: the acceptor echoes each dialer's lane. A zero
   * port is advertised as the bound port once listening.
   */
  readonly local: Hello;
  /** Refuses connections beyond this count; zero means unlimited. */
  readonly maxConnections?: number;
  /**
   * Listen over TLS with this material. All or nothing: a TLS
   * listener accepts only TLS, so a plaintext dialer fails its
   * carrier handshake before a single frame crosses. Everything above
   * the socket is byte-identical either way.
   */
  readonly tls?: TlsConfig;
  /** Per-session settings; `connIdleMs` defaults to 20 minutes here. */
  readonly session?: SessionOptions;
  readonly timers?: Timers;
}

export interface NetServerHandlers {
  /** An accepted session finished its handshake. */
  onSession?(session: Session): void;
  /** An inbound DATA envelope on any session; see {@link SessionHandlers}. */
  onData?(session: Session, envelope: DataEnvelope, correlation: number): void;
  /** An opened session closed; fires exactly once per session. */
  onSessionClose?(session: Session, error: Error | null): void;
  /**
   * A listener-level failure after bind, such as an accept error at
   * file-descriptor exhaustion. The listener stays up; without this
   * handler the failure is swallowed, never thrown.
   */
  onError?(error: Error): void;
}

/** A bound host and port pair. */
export interface BoundAddress {
  readonly host: string;
  readonly port: number;
}

export class NetServer {
  private readonly _listener: Server;
  private readonly _options: NetServerOptions;
  private readonly _handlers: NetServerHandlers;
  private readonly _sessions: Set<Session> = new Set<Session>();
  private readonly _handshaking: Set<Socket> = new Set<Socket>();
  private readonly _timers: Timers;
  private _accepted: number = 0;
  private _shutdown: Promise<void> | null = null;
  private _drainResolve: (() => void) | null = null;
  private _address: BoundAddress = { host: "", port: 0 };

  /** The HELLO answered to dialers, its ephemeral port resolved to the
   * bound one once listening. */
  private _local: Hello;

  private constructor(options: NetServerOptions, handlers: NetServerHandlers) {
    this._options = options;
    this._local = options.local;
    this._handlers = handlers;
    this._timers = options.timers ?? defaultTimers;
    const tls: TlsConfig | undefined = options.tls;
    if (tls === undefined) {
      this._listener = createServer((socket: Socket): void => this.onConnection(socket));
      return;
    }

    // The TLS listener accepts a connection only once its carrier
    // handshake settled, so everything downstream of accept is
    // byte-identical to plaintext. A client whose carrier handshake
    // fails (a plaintext dialer, a refused client certificate) never
    // reaches accept; the failure is reported, not thrown, because a
    // misconfigured client must not take the listener down.
    const listener: Server = createTlsServer(
      {
        cert: tls.cert,
        key: tls.key,
        ca: tls.ca,
        requestCert: tls.requestCert === true,
        rejectUnauthorized: true,
      },
      (socket: Socket): void => this.onConnection(socket),
    );
    listener.on("tlsClientError", (error: Error): void => {
      this._handlers.onError?.(error);
    });
    this._listener = listener;
  }

  /** Binds and starts accepting; resolves once listening. */
  static listen(options: NetServerOptions, handlers: NetServerHandlers = {}): Promise<NetServer> {
    const server: NetServer = new NetServer(options, handlers);
    return new Promise<NetServer>(
      (resolve: (server: NetServer) => void, reject: (error: Error) => void): void => {
        server._listener.once("error", reject);
        server._listener.listen(options.port ?? 0, options.host, (): void => {
          server._listener.removeListener("error", reject);
          // A later listener error, such as an accept failure at fd
          // exhaustion, must never be an unhandled 'error' emission:
          // that would kill the process over a transient condition.
          server._listener.on("error", (error: Error): void => {
            server._handlers.onError?.(error);
          });
          const bound: AddressInfo = server._listener.address() as AddressInfo;
          server._address = { host: bound.address, port: bound.port };
          // An ephemeral local port advertises the bound one, so the
          // HELLO a session answers names an endpoint a peer can dial.
          if (options.local.port === 0) {
            server._local = { ...options.local, port: bound.port };
          }

          resolve(server);
        });
      },
    );
  }

  /** The bound address, useful when listening on port zero. */
  get address(): BoundAddress {
    return this._address;
  }

  /** Sessions currently alive, handshaking ones included. */
  get activeConnections(): number {
    return this._sessions.size + this._handshaking.size;
  }

  /** Connections accepted since the server started. */
  get acceptedConnections(): number {
    return this._accepted;
  }

  private onConnection(socket: Socket): void {
    /* v8 ignore next 4 -- accept-versus-shutdown race: a connection
       already queued when the listener closed still emits. */
    if (this._shutdown !== null) {
      socket.destroy();
      return;
    }

    const limit: number = this._options.maxConnections ?? 0;
    if (limit > 0 && this.activeConnections >= limit) {
      socket.destroy();
      return;
    }

    this._accepted += 1;
    this._handshaking.add(socket);
    /* v8 ignore next 3 -- runtimes without socket keep-alive skip it. */
    if (typeof socket.setKeepAlive === "function") {
      socket.setKeepAlive(true, KEEP_ALIVE_MS);
    }

    const sessionOptions: SessionOptions = {
      connIdleMs: DEFAULT_CONN_IDLE_MS,
      ...this._options.session,
    };

    const handlers: SessionHandlers = {
      onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
        this._handlers.onData?.(session, envelope, correlation);
      },
      onClose: (session: Session, error: Error | null): void => {
        this._sessions.delete(session);
        this._handlers.onSessionClose?.(session, error);
        this.checkDrained();
      },
    };

    Session.accept(socket, this._local, handlers, sessionOptions).then(
      (session: Session): void => {
        this._handshaking.delete(socket);
        /* v8 ignore next 5 -- handshake-versus-shutdown race: the
           handshake settled in the same turn shutdown began. */
        if (this._shutdown !== null) {
          session.destroy();
          this.checkDrained();
          return;
        }

        this._sessions.add(session);
        this._handlers.onSession?.(session);
      },
      (): void => {
        // The handshake failed; the session already tore the socket
        // down and the dialer saw why.
        this._handshaking.delete(socket);
        this.checkDrained();
      },
    );
  }

  /**
   * Stops the server. The listener closes first, every session is
   * told the server is going away with a connection-scoped ERROR,
   * and `graceMs` bounds the wait for teardown: positive waits up to
   * that long then destroys stragglers, zero waits indefinitely,
   * negative destroys everything now. Idempotent.
   */
  shutdown(graceMs: number): Promise<void> {
    if (this._shutdown !== null) {
      return this._shutdown;
    }

    this._shutdown = this.runShutdown(graceMs);
    return this._shutdown;
  }

  private runShutdown(graceMs: number): Promise<void> {
    this._listener.close();

    // A connection still mid-handshake serves no traffic and cannot
    // be told anything meaningful; destroying it now keeps shutdown
    // from waiting on the handshake deadline.
    for (const socket of this._handshaking) {
      socket.destroy();
    }

    if (graceMs < 0) {
      for (const session of this._sessions) {
        session.destroy();
      }
    }

    if (graceMs >= 0) {
      for (const session of this._sessions) {
        session.closeWith(ERROR_UNAVAILABLE, "server is shutting down");
      }
    }

    const drained: Promise<void> = new Promise<void>((resolve: () => void): void => {
      this._drainResolve = resolve;
      this.checkDrained();
    });
    if (graceMs <= 0) {
      return drained;
    }

    return new Promise<void>((resolve: () => void): void => {
      const timer: TimerHandle = this._timers.schedule(graceMs, (): void => {
        for (const session of this._sessions) {
          session.destroy();
        }
      });

      void drained.then((): void => {
        timer.cancel();
        resolve();
      });
    });
  }

  private checkDrained(): void {
    if (this._drainResolve === null || this.activeConnections > 0) {
      return;
    }

    const resolve: () => void = this._drainResolve;
    this._drainResolve = null;
    resolve();
  }
}
