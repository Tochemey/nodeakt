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

import type { Socket } from "node:net";
import {
  ChunkReassembler,
  DEFAULT_CHUNK_SIZE,
  REASSEMBLY_COMPLETE,
  REASSEMBLY_REJECT,
  REASSEMBLY_VIOLATION,
  type ReassemblyOutcome,
  splitLogicalFrame,
} from "./chunk";
import { type ConnCounters, ErrConnClosed, FramedConn, type OutboundFrame } from "./conn";
import { CreditWindow } from "./credit";
import {
  type DataEnvelope,
  decodeDataEnvelope,
  decodeErrorBody,
  decodeHello,
  decodeReplyEnvelope,
  ERROR_BAD_REQUEST,
  ERROR_INTERNAL,
  ERROR_PROTOCOL,
  type ErrorBody,
  encodeDataEnvelope,
  encodeErrorBody,
  encodeHello,
  encodeReplyEnvelope,
  type Hello,
  type ReplyEnvelope,
} from "./envelope";
import {
  FLAG_EXPECTS_REPLY,
  FRAME_CHUNK,
  FRAME_CREDIT,
  FRAME_DATA,
  FRAME_ERROR,
  FRAME_HEADER_SIZE,
  FRAME_HELLO,
  FRAME_HELLO_ACK,
  FRAME_PING,
  FRAME_PONG,
  FRAME_REPLY,
  FRAME_TABLE,
  type FrameHeader,
  MIN_MAX_FRAME_SIZE,
  ProtocolError,
} from "./frame";
import {
  decodeTableBody,
  encodeTableBody,
  InboundTables,
  OutboundTables,
  type TableAnnouncement,
} from "./table";
import { defaultTimers, type TimerHandle, type Timers } from "./timers";
import { ByteReader, ByteWriter } from "./values";

/**
 * Session is the connection engine over one framed connection: it
 * runs the handshake, negotiates the effective parameters, owns
 * liveness, idle reclaim, credits, chunking, and string tables,
 * enforces the protocol's connection-scoped rules, and hands decoded
 * envelopes to its owner.
 *
 * A session begins with {@link Session.dial} or
 * {@link Session.accept}; both settle once HELLO and HELLO_ACK have
 * crossed, and reject with a typed error when the peer misbehaves,
 * refuses, or never answers. Teardown ordering is fixed: mark
 * closing, flush what was admitted (a final connection-scoped ERROR
 * included) within the write-timeout budget, then destroy the
 * socket; the owner's `onClose` fires exactly once, after the last
 * frame that will ever be dispatched, and a locally initiated close
 * is never reported as a peer failure.
 *
 * @internal
 */

/** Close reason when the handshake deadline expires. */
export const ErrHandshakeTimeout: Error = new Error("tcp handshake timed out");

/** Close reason when two consecutive liveness probes go unanswered. */
export const ErrLivenessTimeout: Error = new Error("tcp peer stopped answering liveness probes");

/** Close reason when the connection idle timeout reclaims a session. */
export const ErrIdleReclaim: Error = new Error("tcp connection reclaimed after sitting idle");

/** Refusal when the outbound admission budget is full. */
export const ErrBackpressure: Error = new Error("tcp outbound admission budget is full");

/** Rejection when an ask's local timeout expires before a reply. */
export const ErrAskTimeout: Error = new Error("tcp ask timed out");

/** Refusal when a message exceeds the negotiated size limits. */
export const ErrMessageTooLarge: Error = new Error(
  "tcp message exceeds the negotiated size limits",
);

/** A failure the peer reported in an ERROR frame. */
export class PeerError extends Error {
  /** The wire error code, one of the ERROR_* constants. */
  readonly code: number;
  /** Zero for none, else one plus the peer's sentinel index. */
  readonly sentinel: number;
  /** The wire error name when the sentinel is zero. */
  readonly errorName: string;

  constructor(code: number, message: string, sentinel: number = 0, errorName: string = "") {
    super(message);
    this.name = "PeerError";
    this.code = code;
    this.sentinel = sentinel;
    this.errorName = errorName;
  }
}

/** Consecutive unanswered probes that kill a connection. */
const LIVENESS_MISS_LIMIT: number = 2;

/** Teardown flush budget when no write timeout is configured. */
const CLOSE_DRAIN_GRACE_MS: number = 5000;

/** The capability revisions that gate each negotiated feature. */
const REVISION_CHUNKING: number = 2;
const REVISION_TABLES: number = 3;
const REVISION_CREDITS: number = 4;

/** The highest capability revision this transport implements: what an
 * owner should advertise, so it can never lag or outrun the wire. */
export const REVISION_CURRENT: number = REVISION_CREDITS;

/**
 * Deadline for repaying grant bytes still below the batch threshold.
 * Batching keeps grant traffic negligible, but a remainder held
 * forever could park the peer's queue with no repayment ever due, so
 * whatever is owed flushes within this bound.
 */
const GRANT_FLUSH_MS: number = 50;

const DEFAULT_HANDSHAKE_TIMEOUT_MS: number = 10_000;
const DEFAULT_READ_IDLE_MS: number = 10_000;
const DEFAULT_WRITE_TIMEOUT_MS: number = 10_000;

export interface SessionOptions {
  /** Handshake deadline; zero disables (tests only). */
  readonly handshakeTimeoutMs?: number;
  /** Probe interval; zero disables liveness probing. */
  readonly readIdleMs?: number;
  /** Idle reclaim measured from the last inbound frame; zero disables. */
  readonly connIdleMs?: number;
  /** Write-progress bound handed to the framed connection. */
  readonly writeTimeoutMs?: number;
  /**
   * The local logical-frame threshold above which sends are chunked,
   * clamped to the negotiated frame size at handshake time.
   */
  readonly chunkSize?: number;
  readonly timers?: Timers;
}

/**
 * The owner's callbacks. The session consumes every frame type
 * itself, so what reaches the owner is decoded envelopes and the
 * close; `onClose` fires exactly once per opened session.
 */
export interface SessionHandlers {
  /**
   * An inbound DATA envelope. `correlation` is nonzero for an ask,
   * which the owner settles with {@link Session.reply} or
   * {@link Session.replyError}; zero for a tell, watch, or unwatch.
   * The envelope's payload may alias a receive buffer, so an owner
   * that retains it must copy.
   */
  onData?(session: Session, envelope: DataEnvelope, correlation: number): void;
  onClose?(session: Session, error: Error | null): void;
  /**
   * The kernel accepted the last frame of a message sent with a
   * confirmation token; see {@link Session.tell}. Must not throw.
   */
  onWrote?(session: Session, token: unknown): void;
}

/** Session lifecycle states, integers so state checks stay branch-cheap. */
type SessionState = 0 | 1 | 2 | 3;
const STATE_HANDSHAKING: SessionState = 0;
const STATE_OPEN: SessionState = 1;
const STATE_CLOSING: SessionState = 2;
const STATE_CLOSED: SessionState = 3;

/** One in-flight ask, keyed by its correlation id. */
interface PendingEntry {
  readonly resolve: (reply: ReplyEnvelope) => void;
  readonly reject: (error: Error) => void;
  readonly timer: TimerHandle | null;
}

export class Session {
  private readonly _conn: FramedConn;
  private readonly _timers: Timers;
  private readonly _handlers: SessionHandlers;
  private readonly _local: Hello;
  private readonly _dialer: boolean;
  private readonly _handshakeTimeoutMs: number;
  private readonly _readIdleMs: number;
  private readonly _connIdleMs: number;
  private readonly _writeTimeoutMs: number;
  private readonly _scratch: ByteWriter = new ByteWriter(256);

  private readonly _pending: Map<number, PendingEntry> = new Map<number, PendingEntry>();
  private _state: SessionState = STATE_HANDSHAKING;
  private _remote: Hello | null = null;
  private _effective: Hello | null = null;
  private _lane: number;
  private _correlation: number;
  private _chunkSize: number;
  /** The negotiated capability revision; zero until the handshake. */
  private _revision: number = 0;
  /** Replaced with the negotiated caps once the handshake settles. */
  private _reassembler: ChunkReassembler = new ChunkReassembler(MIN_MAX_FRAME_SIZE, 1);
  private _window: CreditWindow | null = null;
  /**
   * The outbound admission cap: the negotiated credit budget, falling
   * back to the locally configured one when the peer advertised zero,
   * so a remote cannot switch off this side's memory bound.
   */
  private _admissionCap: number = 0;
  private _outTables: OutboundTables | null = null;
  private _inTables: InboundTables | null = null;
  private _lastInbound: number;
  private _probeOutstanding: boolean = false;
  private _livenessMisses: number = 0;
  private _handshakeTimer: TimerHandle | null = null;
  private _livenessTimer: TimerHandle | null = null;
  private _reclaimTimer: TimerHandle | null = null;
  private _forceCloseTimer: TimerHandle | null = null;
  private _grantTimer: TimerHandle | null = null;
  private _closeError: Error | null = null;
  private _opened: boolean = false;
  private _openResolve: ((session: Session) => void) | null = null;
  private _openReject: ((error: Error) => void) | null = null;

  private constructor(
    socket: Socket,
    dialer: boolean,
    local: Hello,
    handlers: SessionHandlers,
    options: SessionOptions,
  ) {
    this._dialer = dialer;
    this._local = local;
    this._handlers = handlers;
    this._timers = options.timers ?? defaultTimers;
    this._handshakeTimeoutMs = options.handshakeTimeoutMs ?? DEFAULT_HANDSHAKE_TIMEOUT_MS;
    this._readIdleMs = options.readIdleMs ?? DEFAULT_READ_IDLE_MS;
    this._connIdleMs = options.connIdleMs ?? 0;
    this._writeTimeoutMs = options.writeTimeoutMs ?? DEFAULT_WRITE_TIMEOUT_MS;
    this._lane = local.lane;
    this._lastInbound = this._timers.now();
    this._conn = new FramedConn(
      socket,
      {
        onFrame: (header: FrameHeader, body: Uint8Array): void => this.onFrame(header, body),
        onViolation: (error: ProtocolError): void => this.failProtocol(error.message),
        onClose: (error: Error | null): void => this.onConnClose(error),
        onWrote: (token: unknown): void => this._handlers.onWrote?.(this, token),
      },
      { maxFrameSize: 0, writeTimeoutMs: this._writeTimeoutMs, timers: this._timers },
    );
    this._chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE;
    // Correlation ids are parity-split: the dialer allocates odd ids,
    // the acceptor even ones, so inbound chunk-group keys from the
    // two directions can never collide.
    this._correlation = dialer ? -1 : 0;
  }

  /**
   * Opens the dialer side: sends HELLO on the given socket and
   * settles once HELLO_ACK arrives. Rejects with {@link PeerError}
   * when the peer answers with an ERROR, {@link ErrHandshakeTimeout}
   * when it never answers, and the underlying failure otherwise.
   */
  static dial(
    socket: Socket,
    local: Hello,
    handlers: SessionHandlers = {},
    options: SessionOptions = {},
  ): Promise<Session> {
    const session: Session = new Session(socket, true, local, handlers, options);
    return session.start();
  }

  /** Opens the acceptor side: awaits HELLO and answers HELLO_ACK. */
  static accept(
    socket: Socket,
    local: Hello,
    handlers: SessionHandlers = {},
    options: SessionOptions = {},
  ): Promise<Session> {
    const session: Session = new Session(socket, false, local, handlers, options);
    return session.start();
  }

  private start(): Promise<Session> {
    const opened: Promise<Session> = new Promise<Session>(
      (resolve: (session: Session) => void, reject: (error: Error) => void): void => {
        this._openResolve = resolve;
        this._openReject = reject;
      },
    );

    if (this._handshakeTimeoutMs > 0) {
      this._handshakeTimer = this._timers.schedule(this._handshakeTimeoutMs, (): void => {
        this._closeError = this._closeError ?? ErrHandshakeTimeout;
        this._conn.destroy(ErrHandshakeTimeout);
      });
    }

    if (this._dialer) {
      this.sendHello(FRAME_HELLO, this._local);
    }

    return opened;
  }

  /** The peer's HELLO parameters; null until the handshake settles. */
  get remote(): Hello | null {
    return this._remote;
  }

  /**
   * The negotiated parameter set; null until the handshake settles
   * and again once the session is closing, which is also what gates
   * every send.
   */
  get effective(): Hello | null {
    return this._effective;
  }

  get closed(): boolean {
    return this._state === STATE_CLOSED;
  }

  /** The connection's negotiated lane byte. */
  get lane(): number {
    return this._lane;
  }

  /** Bytes accepted for sending and not yet handed to the kernel, frames parked awaiting credit included. */
  get outstandingBytes(): number {
    return this.heldBytes();
  }

  /** The connection's cumulative frame and byte totals, still readable once it has closed. */
  counters(): ConnCounters {
    return this._conn.counters();
  }

  /** Allocates the next correlation id on this side's parity. */
  nextCorrelation(): number {
    this._correlation += 2;
    return this._correlation;
  }

  /** Sends one frame; the connection owns the body from here on. */
  send(frame: OutboundFrame): Error | null {
    if (this._state !== STATE_OPEN) {
      return ErrConnClosed;
    }

    return this._conn.send(frame);
  }

  /**
   * Sends a fire-and-forget DATA envelope: a tell, watch, or unwatch.
   * Refused with {@link ErrBackpressure} when the admission budget is
   * full; the owner records the dead letter. A `wrote` token comes
   * back through {@link SessionHandlers.onWrote} once the kernel
   * accepts the message's last frame, the confirmation point tell
   * redelivery (see Peer) builds on.
   */
  tell(envelope: DataEnvelope, wrote?: unknown): Error | null {
    const effective: Hello | null = this._effective;
    if (effective === null) {
      return ErrConnClosed;
    }

    const held: number = this.heldBytes();
    const body: Uint8Array = this.encodeData(envelope);
    if (!this.admit(held, body.length, this._admissionCap)) {
      return ErrBackpressure;
    }

    return this.sendLogical(effective, FRAME_DATA, 0, 0, body, wrote);
  }

  /**
   * Sends an ask and settles with the peer's answer: a REPLY resolves,
   * a request-scoped ERROR rejects with {@link PeerError}, the local
   * timeout rejects with {@link ErrAskTimeout} (a late reply is then
   * dropped by design), and connection loss rejects with the close
   * reason. A non-positive timeout waits until the connection ends.
   *
   * An envelope carrying no timeout of its own is stamped with the
   * ask's, so the receiver always sees the remaining budget.
   */
  ask(envelope: DataEnvelope, timeoutMs: number): Promise<ReplyEnvelope> {
    const effective: Hello | null = this._effective;
    if (effective === null) {
      return Promise.reject(ErrConnClosed);
    }

    const stamped: DataEnvelope =
      envelope.timeout === 0 && timeoutMs > 0 ? { ...envelope, timeout: timeoutMs } : envelope;
    const held: number = this.heldBytes();
    const body: Uint8Array = this.encodeData(stamped);
    if (!this.admit(held, body.length, this._admissionCap)) {
      return Promise.reject(ErrBackpressure);
    }

    const correlation: number = this.nextCorrelation();
    return new Promise<ReplyEnvelope>(
      (resolve: (reply: ReplyEnvelope) => void, reject: (error: Error) => void): void => {
        const timer: TimerHandle | null =
          timeoutMs > 0
            ? this._timers.schedule(timeoutMs, (): void => {
                this.takePending(correlation)?.reject(ErrAskTimeout);
              })
            : null;
        this._pending.set(correlation, { resolve, reject, timer });
        const refused: Error | null = this.sendLogical(
          effective,
          FRAME_DATA,
          FLAG_EXPECTS_REPLY,
          correlation,
          body,
        );
        if (refused !== null) {
          this.takePending(correlation)?.reject(refused);
        }
      },
    );
  }

  /** Answers an inbound ask with a REPLY envelope. */
  reply(correlation: number, envelope: ReplyEnvelope): Error | null {
    const effective: Hello | null = this._effective;
    if (effective === null) {
      return ErrConnClosed;
    }

    const held: number = this.heldBytes();
    const body: Uint8Array = this.encodeReply(envelope);
    if (!this.admit(held, body.length, this._admissionCap)) {
      return ErrBackpressure;
    }

    return this.sendLogical(effective, FRAME_REPLY, 0, correlation, body);
  }

  /**
   * Sends a DATA or REPLY, splitting into CHUNK frames when the
   * logical frame exceeds the chunk size. Refused with
   * {@link ErrMessageTooLarge} when it exceeds the negotiated message
   * cap, or the frame cap on a connection that cannot chunk. A
   * chunked ask or reply keys its group by the frame's correlation; a
   * chunked tell allocates a fresh group id.
   */
  private sendLogical(
    effective: Hello,
    type: number,
    flags: number,
    correlation: number,
    body: Uint8Array,
    wrote?: unknown,
  ): Error | null {
    const logical: number = FRAME_HEADER_SIZE + body.length;
    if (logical > effective.maxMessageSize) {
      return ErrMessageTooLarge;
    }

    if (effective.revision < REVISION_CHUNKING || logical <= this._chunkSize) {
      if (body.length > effective.maxFrameSize) {
        return ErrMessageTooLarge;
      }

      const frame: OutboundFrame =
        wrote === undefined
          ? { type, flags, lane: this._lane, correlation, body }
          : { type, flags, lane: this._lane, correlation, body, wrote };
      // DATA spends the credit window; a whole REPLY is exempt so an
      // answer can never deadlock behind the asker's own window.
      if (type === FRAME_DATA) {
        return this.sendWindowed(frame);
      }

      return this._conn.send(frame);
    }

    // Chunk sends cannot be refused mid-group: an open session's
    // connection reports death only through its close event, and
    // frames accepted before it die with the connection, which the
    // receiver's teardown and the peer's redelivery already cover.
    // The splitter carries the token on the last chunk, so a chunked
    // message confirms when its last frame does.
    const group: number = correlation !== 0 ? correlation : this.nextCorrelation();
    const inner: OutboundFrame =
      wrote === undefined
        ? { type, flags, lane: this._lane, correlation, body }
        : { type, flags, lane: this._lane, correlation, body, wrote };
    const chunks: OutboundFrame[] = splitLogicalFrame(inner, this._chunkSize, group);
    for (const chunk of chunks) {
      this.sendWindowed(chunk);
    }

    return null;
  }

  /**
   * Sends one windowed frame through the credit window when the
   * connection negotiated one; queued frames report success, since
   * the window owns delivering them as grants arrive.
   */
  private sendWindowed(frame: OutboundFrame): Error | null {
    const window: CreditWindow | null = this._window;
    if (window === null) {
      return this._conn.send(frame);
    }

    return window.send(frame);
  }

  /**
   * Settles an inbound ask with a request-scoped ERROR. Error frames
   * are exempt from admission so a failure can always be answered.
   */
  replyError(correlation: number, body: ErrorBody): Error | null {
    if (this._state !== STATE_OPEN) {
      return ErrConnClosed;
    }

    return this.sendControl(FRAME_ERROR, correlation, this.encodeError(body));
  }

  /**
   * Sends one exempt frame on the connection lane: control traffic
   * rides outside admission and the credit window so teardown,
   * liveness, and repayment can always speak.
   */
  private sendControl(type: number, correlation: number, body: Uint8Array | null): Error | null {
    return this._conn.send({ type, flags: 0, lane: this._lane, correlation, body });
  }

  private encodeData(envelope: DataEnvelope): Uint8Array {
    this._scratch.reset();
    encodeDataEnvelope(this._scratch, envelope, this._outTables ?? undefined);
    return this._scratch.bytes().slice();
  }

  private encodeReply(envelope: ReplyEnvelope): Uint8Array {
    this._scratch.reset();
    encodeReplyEnvelope(this._scratch, envelope, this._outTables ?? undefined);
    return this._scratch.bytes().slice();
  }

  /**
   * The admission budget: bytes accepted and not yet flushed to the
   * kernel, frames parked awaiting credit included, capped at the
   * negotiated window so a dead or slow peer bounds memory instead of
   * growing a queue. An empty pipe admits one message of any size,
   * mirroring the window's own allowance, so a message larger than an
   * undersized peer's whole window is bounded by the message cap
   * instead of wedging forever. Callers sample {@link heldBytes}
   * before encoding, so the table announcements a message triggers
   * never count against the message itself. Control frames bypass
   * admission so teardown and liveness can always speak. The cap is
   * the peer-negotiated budget with a local fallback (see
   * `_admissionCap`); only an explicit local zero disables it.
   */
  private admit(held: number, bodyLength: number, cap: number): boolean {
    return cap <= 0 || held === 0 || held + bodyLength + FRAME_HEADER_SIZE <= cap;
  }

  /** Bytes accepted for sending and not yet flushed to the kernel. */
  private heldBytes(): number {
    return this._conn.outstandingBytes + (this._window?.queuedBytes ?? 0);
  }

  private takePending(correlation: number): PendingEntry | undefined {
    const entry: PendingEntry | undefined = this._pending.get(correlation);
    if (entry === undefined) {
      return undefined;
    }

    this._pending.delete(correlation);
    entry.timer?.cancel();
    return entry;
  }

  private failAllPending(error: Error): void {
    for (const entry of this._pending.values()) {
      entry.timer?.cancel();
      entry.reject(error);
    }

    this._pending.clear();
  }

  /** Graceful close: flush what was admitted, FIN, then destroy. */
  close(): void {
    this.teardown(null, null);
  }

  /**
   * Graceful close announced to the peer: a connection-scoped ERROR
   * with the given code and message is flushed first. The close is
   * still locally initiated, so the owner sees a clean close.
   */
  closeWith(code: number, message: string): void {
    this.teardown({ code, message }, null);
  }

  /** Immediate teardown; nothing is flushed. */
  destroy(): void {
    this._conn.destroy(this._closeError);
  }

  /**
   * Answers a connection-scoped protocol violation: the peer gets a
   * best-effort ERROR naming it, then the connection dies with a
   * {@link ProtocolError}.
   */
  private failProtocol(message: string): void {
    this.teardown({ code: ERROR_PROTOCOL, message }, new ProtocolError(message));
  }

  private teardown(notice: { code: number; message: string } | null, error: Error | null): void {
    if (this._state === STATE_CLOSING || this._state === STATE_CLOSED) {
      return;
    }

    this._state = STATE_CLOSING;
    this._effective = null;
    this._closeError = this._closeError ?? error;
    this.failAllPending(this._closeError ?? ErrConnClosed);
    this._reassembler.clear();
    this._window?.clear();
    // A closing session repays nothing; the peer's window dies with
    // the connection anyway.
    this._grantTimer?.cancel();
    this._grantTimer = null;
    if (notice !== null) {
      this.sendControl(
        FRAME_ERROR,
        0,
        this.encodeError({ code: notice.code, sentinel: 0, name: "", message: notice.message }),
      );
    }

    this._conn.end();
    const grace: number = this._writeTimeoutMs > 0 ? this._writeTimeoutMs : CLOSE_DRAIN_GRACE_MS;
    this._forceCloseTimer = this._timers.schedule(grace, (): void => {
      this._conn.destroy(this._closeError);
    });
  }

  private onConnClose(error: Error | null): void {
    this._state = STATE_CLOSED;
    this._effective = null;
    this.cancelTimers();
    this._reassembler.clear();
    this._window?.clear();
    const reported: Error | null = this._closeError ?? error;
    this.failAllPending(reported ?? ErrConnClosed);
    if (!this._opened) {
      // The connection reports its close exactly once, and the only
      // path that clears the rejecter also flips `_opened`, so it is
      // always still here on a close before open.
      const reject: (error: Error) => void = this._openReject as (error: Error) => void;
      this._openReject = null;
      this._openResolve = null;
      reject(reported ?? ErrConnClosed);
      return;
    }

    this._handlers.onClose?.(this, reported);
  }

  private cancelTimers(): void {
    for (const timer of [
      this._handshakeTimer,
      this._livenessTimer,
      this._reclaimTimer,
      this._forceCloseTimer,
      this._grantTimer,
    ]) {
      timer?.cancel();
    }

    this._handshakeTimer = null;
    this._livenessTimer = null;
    this._reclaimTimer = null;
    this._forceCloseTimer = null;
    this._grantTimer = null;
  }

  // The handshake.

  private sendHello(type: number, hello: Hello): void {
    this._scratch.reset();
    encodeHello(this._scratch, hello);
    this._conn.send({
      type,
      flags: 0,
      lane: hello.lane,
      correlation: 0,
      body: this._scratch.bytes().slice(),
    });
  }

  private onFrame(header: FrameHeader, body: Uint8Array): void {
    if (this._state === STATE_HANDSHAKING) {
      this.onHandshakeFrame(header, body);
      return;
    }

    if (this._state !== STATE_OPEN) {
      return;
    }

    this._lastInbound = this._timers.now();
    this._probeOutstanding = false;
    this._livenessMisses = 0;

    if (header.lane !== this._lane) {
      this.failLaneMismatch(header.lane);
      return;
    }

    switch (header.type) {
      case FRAME_PING:
        this.sendControl(FRAME_PONG, header.correlation, null);
        return;
      case FRAME_PONG:
        return;
      case FRAME_HELLO:
      case FRAME_HELLO_ACK:
        this.failProtocol("unexpected handshake frame on an open connection");
        return;
      case FRAME_DATA:
        this.accrueCredit(FRAME_HEADER_SIZE + header.length);
        this.onDataFrame(header, body);
        return;
      case FRAME_REPLY:
        this.onReplyFrame(header, body);
        return;
      case FRAME_CHUNK:
        this.accrueCredit(FRAME_HEADER_SIZE + header.length);
        this.onChunkFrame(header, body);
        return;
      case FRAME_CREDIT:
        this.onCreditFrame(body);
        return;
      case FRAME_TABLE:
        this.onTableFrame(body);
        return;
      case FRAME_ERROR: {
        if (header.correlation !== 0) {
          this.onRequestError(header, body);
          return;
        }

        this.closeFromPeerError(body);
        return;
      }
    }
  }

  /** The peer's last words: a connection-scoped ERROR before it closes. */
  private closeFromPeerError(body: Uint8Array): void {
    const peer: ErrorBody = decodeErrorBody(new ByteReader(body));
    this._closeError = new PeerError(peer.code, peer.message, peer.sentinel, peer.name);
    this._conn.destroy(this._closeError);
  }

  private failLaneMismatch(lane: number): void {
    this.failProtocol(`frame lane 0x${lane.toString(16)} does not match the connection lane`);
  }

  /**
   * An inbound DATA envelope. A malformed body is request-scoped: an
   * ask is answered with a badRequest ERROR and a tell is dropped,
   * the connection living on either way; only a table ref this
   * connection has not negotiated poisons the stream. A throwing
   * owner handler is request-scoped the same way, since the failure
   * is attributable to one message, not to the stream.
   */
  private onDataFrame(header: FrameHeader, body: Uint8Array): void {
    let envelope: DataEnvelope;
    try {
      envelope = decodeDataEnvelope(new ByteReader(body), this._inTables ?? undefined);
    } catch (thrown) {
      if (thrown instanceof ProtocolError) {
        this.failProtocol(thrown.message);
        return;
      }

      if ((header.flags & FLAG_EXPECTS_REPLY) !== 0) {
        this.replyError(header.correlation, {
          code: ERROR_BAD_REQUEST,
          sentinel: 0,
          name: "",
          // The envelope decoder throws only Error subclasses.
          message: (thrown as Error).message,
        });
      }

      return;
    }

    try {
      this._handlers.onData?.(this, envelope, header.correlation);
    } catch (thrown) {
      if ((header.flags & FLAG_EXPECTS_REPLY) !== 0) {
        this.replyError(header.correlation, {
          code: ERROR_INTERNAL,
          sentinel: 0,
          name: "",
          message: thrown instanceof Error ? thrown.message : String(thrown),
        });
      }
    }
  }

  /**
   * One CHUNK of a logical frame. A soft reject settles the message
   * on both sides: the sender gets a request-scoped ERROR, and a
   * local ask waiting on the rejected group (a chunked reply) fails
   * now instead of at its timeout. Violations poison the stream.
   */
  private onChunkFrame(header: FrameHeader, body: Uint8Array): void {
    if (this._revision < REVISION_CHUNKING) {
      this.failProtocol(`CHUNK requires capability revision ${REVISION_CHUNKING}`);
      return;
    }

    const outcome: ReassemblyOutcome = this._reassembler.push(header, body);
    switch (outcome.kind) {
      case REASSEMBLY_VIOLATION:
        this.failProtocol(outcome.reason);
        return;
      case REASSEMBLY_REJECT: {
        this.replyError(header.correlation, {
          code: ERROR_BAD_REQUEST,
          sentinel: 0,
          name: "",
          message: outcome.reason,
        });
        this.takePending(header.correlation)?.reject(ErrMessageTooLarge);
        return;
      }
      case REASSEMBLY_COMPLETE:
        this.onLogicalFrame(outcome.header as FrameHeader, outcome.body as Uint8Array);
        return;
      default:
        return;
    }
  }

  /** Routes a reassembled logical frame as if it had arrived whole. */
  private onLogicalFrame(header: FrameHeader, body: Uint8Array): void {
    if (header.lane !== this._lane) {
      this.failLaneMismatch(header.lane);
      return;
    }

    if (header.type === FRAME_DATA) {
      this.onDataFrame(header, body);
      return;
    }

    if (header.type === FRAME_REPLY) {
      this.onReplyFrame(header, body);
      return;
    }

    this.failProtocol(`unexpected chunked frame type 0x${header.type.toString(16)}`);
  }

  /**
   * A REPLY settling one of our asks; a late one is dropped silently.
   * A malformed reply is request-scoped and fails only its ask, but a
   * table ref that cannot resolve poisons the stream, so it fails the
   * ask and the connection both.
   */
  private onReplyFrame(header: FrameHeader, body: Uint8Array): void {
    const entry: PendingEntry | undefined = this.takePending(header.correlation);
    if (entry === undefined) {
      return;
    }

    try {
      entry.resolve(decodeReplyEnvelope(new ByteReader(body), this._inTables ?? undefined));
    } catch (thrown) {
      // The envelope decoder throws only Error subclasses.
      const failure: Error = thrown as Error;
      entry.reject(failure);
      if (failure instanceof ProtocolError) {
        this.failProtocol(failure.message);
      }
    }
  }

  /** A request-scoped ERROR settling one of our asks. */
  private onRequestError(header: FrameHeader, body: Uint8Array): void {
    const entry: PendingEntry | undefined = this.takePending(header.correlation);
    if (entry === undefined) {
      return;
    }

    let peer: ErrorBody;
    try {
      peer = decodeErrorBody(new ByteReader(body));
    } catch (thrown) {
      // The peer's error itself is garbage; reject with the decode
      // failure rather than inventing a code the peer never sent.
      // The error-body decoder throws only Error subclasses.
      entry.reject(thrown as Error);
      return;
    }

    entry.reject(new PeerError(peer.code, peer.message, peer.sentinel, peer.name));
  }

  // Credits.

  /**
   * Records the wire cost of one inbound windowed frame and flushes a
   * batched CREDIT grant to the peer when one falls due. Grants are
   * exempt from admission and the window, so repayment always flows.
   * A remainder below the batch is bounded by a deadline: without it,
   * repaying less than a batch and then going quiet would leave the
   * peer's parked frames waiting on a grant that never comes.
   */
  private accrueCredit(cost: number): void {
    const window: CreditWindow | null = this._window;
    if (window === null) {
      return;
    }

    const due: number = window.accrue(cost);
    if (due === 0) {
      if (this._grantTimer === null) {
        this._grantTimer = this._timers.schedule(GRANT_FLUSH_MS, (): void => {
          this.flushOwedGrant();
        });
      }

      return;
    }

    this._grantTimer?.cancel();
    this._grantTimer = null;
    this.sendCredit(due);
  }

  /** The grant deadline fired: repay whatever is still owed. */
  private flushOwedGrant(): void {
    this._grantTimer = null;
    // The timer is armed only while something is owed on an open
    // window: a batch flush cancels it and so does every teardown
    // path, so the balance here is always nonzero.
    const window: CreditWindow = this._window as CreditWindow;
    this.sendCredit(window.takeOwed());
  }

  private sendCredit(due: number): void {
    this._scratch.reset();
    this._scratch.writeUvarint(due);
    this.sendControl(FRAME_CREDIT, 0, this._scratch.bytes().slice());
  }

  /**
   * A peer grant replenishing the send window. A CREDIT below
   * revision 4 or with a malformed count poisons flow-control state,
   * so both are connection-scoped; on a connection that negotiated no
   * window a grant means nothing and is dropped.
   */
  private onCreditFrame(body: Uint8Array): void {
    if (this._revision < REVISION_CREDITS) {
      this.failProtocol(`CREDIT requires capability revision ${REVISION_CREDITS}`);
      return;
    }

    let count: number;
    try {
      count = new ByteReader(body).readUvarint();
    } catch {
      this.failProtocol("malformed CREDIT grant");
      return;
    }

    this._window?.grant(count);
  }

  // String tables.

  /**
   * Announces one freshly assigned table id to the peer, before any
   * ref uses it: the interner calls this mid-encode, so the frame is
   * accepted ahead of the envelope that triggered it. Announcements
   * ride outside the credit window, which can only move them earlier.
   */
  private sendTable(announcement: TableAnnouncement): void {
    // A fresh writer, not _scratch: the interner fires mid-encode,
    // while _scratch still holds the half-built envelope that
    // triggered this announcement.
    const writer: ByteWriter = new ByteWriter(64);
    encodeTableBody(writer, announcement);
    this.sendControl(FRAME_TABLE, 0, writer.bytes());
  }

  /**
   * A peer registration for this direction's inbound tables. Every
   * install rule violation, and TABLE itself below revision 3, is
   * connection-scoped: a table the two sides disagree on corrupts
   * every later message.
   */
  private onTableFrame(body: Uint8Array): void {
    // Tables exist exactly when the negotiated revision reaches them,
    // so the null check is the revision gate.
    const tables: InboundTables | null = this._inTables;
    if (tables === null) {
      this.failProtocol(`TABLE requires capability revision ${REVISION_TABLES}`);
      return;
    }

    try {
      tables.install(decodeTableBody(new ByteReader(body)));
    } catch (thrown) {
      // Table decode and install throw only Error subclasses.
      this.failProtocol((thrown as Error).message);
    }
  }

  /**
   * Attaches an owner-resolved handle to the inbound table entry for
   * an interned path, so steady-state delivery can skip resolution;
   * false when the path is not interned on this connection, in which
   * case there is nothing to pin a cache to.
   */
  cachePathHandle(path: string, handle: unknown): boolean {
    return this._inTables?.cacheHandle(path, handle) ?? false;
  }

  /** The handle cached for an interned inbound path, if any. */
  pathHandle(path: string): unknown {
    return this._inTables?.handleOf(path);
  }

  private onHandshakeFrame(header: FrameHeader, body: Uint8Array): void {
    if (header.type === FRAME_ERROR && header.correlation === 0) {
      this.closeFromPeerError(body);
      return;
    }

    const expected: number = this._dialer ? FRAME_HELLO_ACK : FRAME_HELLO;
    if (header.type !== expected) {
      this.failProtocol(`expected ${this._dialer ? "HELLO_ACK" : "HELLO"}`);
      return;
    }

    let remote: Hello;
    try {
      remote = decodeHello(new ByteReader(body));
    } catch {
      this.failProtocol("invalid HELLO payload");
      return;
    }

    if (remote.revision < 1) {
      this.failProtocol("capability revision below baseline");
      return;
    }

    this._remote = remote;
    const effective: Hello = negotiateHello(this._local, remote);
    if (!this._dialer) {
      this.sendHello(FRAME_HELLO_ACK, effective);
    }

    this._lane = effective.lane;
    this._effective = effective;
    this._revision = effective.revision;
    this._chunkSize = Math.min(this._chunkSize, effective.maxFrameSize);
    this._reassembler = new ChunkReassembler(effective.maxMessageSize, effective.maxLargeTransfers);
    // A peer advertising zero credits opts out of flow control, but
    // it must not switch off this side's memory bound: admission then
    // falls back to the locally configured budget.
    this._admissionCap =
      effective.initialCredits > 0 ? effective.initialCredits : this._local.initialCredits;
    if (effective.revision >= REVISION_TABLES) {
      this._outTables = new OutboundTables((announcement: TableAnnouncement): void =>
        this.sendTable(announcement),
      );
      this._inTables = new InboundTables();
    }

    if (effective.revision >= REVISION_CREDITS && effective.initialCredits > 0) {
      this._window = new CreditWindow(
        effective.initialCredits,
        (frame: OutboundFrame): Error | null => this._conn.send(frame),
      );
    }

    this._conn.setMaxFrameSize(effective.maxFrameSize);
    this._handshakeTimer?.cancel();
    this._handshakeTimer = null;
    this._state = STATE_OPEN;
    this._opened = true;
    this._lastInbound = this._timers.now();
    this.armLiveness();
    this.armReclaim();

    const resolve: ((session: Session) => void) | null = this._openResolve;
    this._openResolve = null;
    this._openReject = null;
    if (resolve !== null) {
      resolve(this);
    }
  }

  // Liveness and idle reclaim.

  private armLiveness(): void {
    if (this._readIdleMs <= 0 || this._state !== STATE_OPEN) {
      return;
    }

    this._livenessTimer = this._timers.schedule(this._readIdleMs, (): void => {
      this.onLivenessDeadline();
    });
  }

  private onLivenessDeadline(): void {
    if (this._state !== STATE_OPEN) {
      return;
    }

    const elapsed: number = this._timers.now() - this._lastInbound;
    if (elapsed < this._readIdleMs) {
      this._livenessTimer = this._timers.schedule(this._readIdleMs - elapsed, (): void => {
        this.onLivenessDeadline();
      });
      return;
    }

    if (this._probeOutstanding) {
      this._livenessMisses += 1;
      if (this._livenessMisses >= LIVENESS_MISS_LIMIT) {
        this._closeError = ErrLivenessTimeout;
        this._conn.destroy(ErrLivenessTimeout);
        return;
      }
    }

    if (!this._probeOutstanding) {
      const refused: Error | null = this.sendControl(FRAME_PING, this.nextCorrelation(), null);
      // A probe the peer never received must not count against it.
      this._probeOutstanding = refused === null;
    }

    this.armLiveness();
  }

  private armReclaim(): void {
    if (this._connIdleMs <= 0 || this._state !== STATE_OPEN) {
      return;
    }

    this._reclaimTimer = this._timers.schedule(this._connIdleMs, (): void => {
      if (this._state !== STATE_OPEN) {
        return;
      }

      const elapsed: number = this._timers.now() - this._lastInbound;
      if (elapsed >= this._connIdleMs) {
        this._closeError = ErrIdleReclaim;
        this._conn.destroy(ErrIdleReclaim);
        return;
      }

      this.armReclaim();
    });
  }

  private encodeError(body: ErrorBody): Uint8Array {
    this._scratch.reset();
    encodeErrorBody(this._scratch, body);
    return this._scratch.bytes().slice();
  }
}

/**
 * The negotiation rule, identical on both sides: pairwise minima on
 * the numeric fields and the revision, lane identity from the remote
 * side (the dialer picks the lane and the acceptor echoes it), and
 * compression only when both sides proposed the same codec. Identity
 * fields carry the local side, which is what an acceptor answers in
 * HELLO_ACK. Every size field is floored so a peer's advertisement
 * cannot produce an unusable effective set: `maxFrameSize` at the
 * protocol floor, `maxMessageSize` at the effective frame size, and
 * `maxLargeTransfers` at one.
 */
export function negotiateHello(local: Hello, remote: Hello): Hello {
  const frameSize: number = Math.max(
    Math.min(local.maxFrameSize, remote.maxFrameSize),
    MIN_MAX_FRAME_SIZE,
  );
  return {
    revision: Math.min(local.revision, remote.revision),
    systemName: local.systemName,
    host: local.host,
    port: local.port,
    lane: remote.lane,
    compression: local.compression === remote.compression ? remote.compression : 0,
    maxFrameSize: frameSize,
    maxMessageSize: Math.max(Math.min(local.maxMessageSize, remote.maxMessageSize), frameSize),
    initialCredits: Math.min(local.initialCredits, remote.initialCredits),
    maxLargeTransfers: Math.max(Math.min(local.maxLargeTransfers, remote.maxLargeTransfers), 1),
  };
}
