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

import { connect, type Socket } from "node:net";
import { DEFAULT_CHUNK_SIZE } from "./chunk";
import { ErrConnClosed } from "./conn";
import {
  type DataEnvelope,
  type Hello,
  KIND_UNWATCH,
  KIND_WATCH,
  type ReplyEnvelope,
} from "./envelope";
import { LANE_CONTROL, LANE_LARGE, MAX_ORDINARY_LANES, ordinaryLane } from "./frame";
import { HeadQueue } from "./queue";
import { Session, type SessionOptions } from "./session";
import { defaultTimers, type Timers } from "./timers";

/**
 * Peer owns the outbound side for one remote system: the lane set,
 * lazy single-flight dialing with capped backoff, tell redelivery,
 * and generation-checked teardown.
 *
 * Lanes split traffic so one kind can never delay another: watches
 * and unwatches ride the control lane, a message whose payload
 * reaches the large threshold rides the large lane where chunked
 * transfers belong, and everything else picks an ordinary lane by a
 * stable hash of the target path, so per-actor order holds no matter
 * how many ordinary lanes are configured. Each lane is one TCP
 * connection, dialed on first use and shared by every concurrent
 * sender.
 *
 * A failed lane arms exponential backoff, starting at one second and
 * doubling per consecutive failure up to the cap; while the window is
 * open a use fails fast with {@link ErrDialBackoff} instead of
 * queueing, and the next use after it redials, so recovery is
 * use-driven and nothing polls in the background.
 *
 * A tell admitted to a session confirms when the kernel accepts its
 * last frame. Tells still unconfirmed when the connection dies are
 * redelivered in order, exactly once, on the lane's next session; a
 * tell that dies twice dead-letters. An ask is never silently
 * retried: its pending entry fails with the connection and the caller
 * decides.
 *
 * @internal
 */

/** Refusal once the peer has been closed. */
export const ErrPeerClosed: Error = new Error("tcp peer is closed");

/** Fast refusal while a lane's reconnect backoff window is open. */
export const ErrDialBackoff: Error = new Error("tcp peer is backing off between dial attempts");

/** Sends per tell before it dead-letters: the send plus one redelivery. */
const MAX_TELL_ATTEMPTS: number = 2;

const DEFAULT_DIAL_TIMEOUT_MS: number = 5000;
const DEFAULT_BACKOFF_INITIAL_MS: number = 1000;
const DEFAULT_BACKOFF_CAP_MS: number = 30_000;

export interface PeerOptions {
  /** Ordinary lanes for user messages; 1 to 254, default 1. */
  readonly ordinaryLanes?: number;
  /** Bounds one dial, TCP connect and handshake together. */
  readonly dialTimeoutMs?: number;
  /** First reconnect backoff; doubles per consecutive failure. */
  readonly backoffInitialMs?: number;
  /** The backoff ceiling. */
  readonly backoffCapMs?: number;
  /** Payload bytes at which a message rides the large lane. */
  readonly largeThreshold?: number;
  readonly timers?: Timers;
  /** Per-session settings for every lane this peer dials. */
  readonly session?: SessionOptions;
}

export interface PeerHandlers {
  /** An inbound DATA envelope on any of this peer's lanes. */
  onData?(session: Session, envelope: DataEnvelope, correlation: number): void;
  /** A tell this peer could not deliver, with the reason. */
  onDeadLetter?(envelope: DataEnvelope, reason: Error): void;
  /** A lane's session closed; the lane redials on next use. */
  onLaneClose?(lane: number, error: Error | null): void;
}

/**
 * One tell in flight: kept until the kernel confirms its write. The
 * record itself is the confirmation token the session hands back, so
 * a tell costs one allocation and no closure.
 */
interface TellRecord {
  readonly envelope: DataEnvelope;
  attempts: number;
}

interface LaneState {
  readonly byte: number;
  session: Session | null;
  dialing: Promise<Session> | null;
  /** The wait armed by the last failure; zero when healthy. */
  backoffMs: number;
  /** Earliest allowed redial on the transport clock. */
  notBefore: number;
  /** Admitted tells awaiting kernel confirmation, in send order. */
  readonly unconfirmed: HeadQueue<TellRecord>;
  /** Tells that died unconfirmed, waiting for the next session. */
  readonly redeliver: TellRecord[];
}

function laneState(byte: number): LaneState {
  return {
    byte,
    session: null,
    dialing: null,
    backoffMs: 0,
    notBefore: 0,
    unconfirmed: new HeadQueue<TellRecord>(),
    redeliver: [],
  };
}

/** FNV-1a over the target path; stable so per-actor order holds. */
function pathHash(path: string): number {
  let hash: number = 0x811c9dc5;
  for (let i = 0; i < path.length; i++) {
    hash ^= path.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }

  return hash >>> 0;
}

/* v8 ignore next 4 -- every rejection this peer sees is already an
   Error; the wrap only guards a foreign throw. */
function toError(thrown: unknown): Error {
  return thrown instanceof Error ? thrown : new Error(String(thrown));
}

export class Peer {
  private readonly _host: string;
  private readonly _port: number;
  private readonly _local: Hello;
  private readonly _handlers: PeerHandlers;
  private readonly _timers: Timers;
  private readonly _sessionOptions: SessionOptions;
  private readonly _dialTimeoutMs: number;
  private readonly _backoffInitialMs: number;
  private readonly _backoffCapMs: number;
  private readonly _largeThreshold: number;

  private readonly _control: LaneState;
  private readonly _large: LaneState;
  private readonly _ordinary: LaneState[];
  private readonly _lanes: LaneState[];

  private _generation: number = 0;
  private _closed: boolean = false;

  constructor(
    host: string,
    port: number,
    local: Hello,
    handlers: PeerHandlers = {},
    options: PeerOptions = {},
  ) {
    const ordinary: number = options.ordinaryLanes ?? 1;
    if (!Number.isInteger(ordinary) || ordinary < 1 || ordinary > MAX_ORDINARY_LANES) {
      throw new TypeError(`ordinary lane count out of range: ${ordinary}`);
    }

    this._host = host;
    this._port = port;
    this._local = local;
    this._handlers = handlers;
    this._timers = options.timers ?? defaultTimers;
    this._sessionOptions = options.session ?? {};
    this._dialTimeoutMs = options.dialTimeoutMs ?? DEFAULT_DIAL_TIMEOUT_MS;
    this._backoffInitialMs = options.backoffInitialMs ?? DEFAULT_BACKOFF_INITIAL_MS;
    this._backoffCapMs = options.backoffCapMs ?? DEFAULT_BACKOFF_CAP_MS;
    this._largeThreshold =
      options.largeThreshold ?? options.session?.chunkSize ?? DEFAULT_CHUNK_SIZE;

    this._control = laneState(LANE_CONTROL);
    this._large = laneState(LANE_LARGE);
    this._ordinary = [];
    for (let i = 0; i < ordinary; i++) {
      this._ordinary.push(laneState(ordinaryLane(i)));
    }

    this._lanes = [this._control, ...this._ordinary, this._large];
  }

  get closed(): boolean {
    return this._closed;
  }

  /**
   * Sends a fire-and-forget envelope; every undeliverable outcome,
   * immediate or after the single redelivery, reaches
   * {@link PeerHandlers.onDeadLetter}. A lane whose session is open
   * sends synchronously, the steady-state path; the promise machinery
   * exists only for the dial. A lane mid-teardown fails fast like a
   * dial in backoff does: the tell dead-letters now instead of
   * queueing behind a session that is already dying.
   */
  tell(envelope: DataEnvelope): void {
    if (this._closed) {
      this.deadLetter(envelope, ErrPeerClosed);
      return;
    }

    const lane: LaneState = this.laneFor(envelope);
    const record: TellRecord = { envelope, attempts: 0 };
    const session: Session | null = lane.session;
    if (session !== null && session.effective !== null) {
      this.attempt(lane, session, record);
      return;
    }

    void this.deliver(lane, record);
  }

  /**
   * Sends an ask on the lane the envelope routes to and settles with
   * the peer's answer. Never retried: a dial failure, backoff window,
   * backpressure, timeout, or connection loss rejects and the caller
   * decides.
   */
  async ask(envelope: DataEnvelope, timeoutMs: number): Promise<ReplyEnvelope> {
    const session: Session = await this.acquire(this.laneFor(envelope));
    return session.ask(envelope, timeoutMs);
  }

  /**
   * Closes every lane, bumps the generation so a dial still in flight
   * parks itself, clears backoff state, and dead-letters every tell
   * still waiting on a session. Terminal: a closed peer refuses all
   * further use.
   */
  close(): void {
    if (this._closed) {
      return;
    }

    this._closed = true;
    this._generation += 1;
    for (const lane of this._lanes) {
      lane.dialing = null;
      lane.backoffMs = 0;
      lane.notBefore = 0;
      for (const record of lane.redeliver.splice(0)) {
        this.deadLetter(record.envelope, ErrPeerClosed);
      }

      lane.session?.close();
    }
  }

  // Lane selection.

  private laneFor(envelope: DataEnvelope): LaneState {
    if (envelope.kind === KIND_WATCH || envelope.kind === KIND_UNWATCH) {
      return this._control;
    }

    if (envelope.payload.length >= this._largeThreshold) {
      return this._large;
    }

    if (this._ordinary.length === 1) {
      return this._ordinary[0] as LaneState;
    }

    return this._ordinary[pathHash(envelope.to) % this._ordinary.length] as LaneState;
  }

  // Dialing.

  /**
   * The lane's session, dialing one when needed. Single-flight:
   * concurrent callers share the dial in progress. Rejects with
   * {@link ErrDialBackoff} while the lane's backoff window is open.
   */
  private acquire(lane: LaneState): Promise<Session> {
    if (this._closed) {
      return Promise.reject(ErrPeerClosed);
    }

    const session: Session | null = lane.session;
    if (session !== null && !session.closed) {
      return Promise.resolve(session);
    }

    if (lane.dialing !== null) {
      return lane.dialing;
    }

    if (lane.backoffMs > 0 && this._timers.now() < lane.notBefore) {
      return Promise.reject(ErrDialBackoff);
    }

    const dialing: Promise<Session> = this.dial(lane, this._generation);
    lane.dialing = dialing;
    return dialing;
  }

  private async dial(lane: LaneState, generation: number): Promise<Session> {
    try {
      const socket: Socket = connect(this._port, this._host);
      const session: Session = await Session.dial(
        socket,
        { ...this._local, lane: lane.byte },
        {
          onData: (from: Session, envelope: DataEnvelope, correlation: number): void => {
            this._handlers.onData?.(from, envelope, correlation);
          },
          onClose: (from: Session, error: Error | null): void => {
            this.onLaneClose(lane, from, error);
          },
          onWrote: (_from: Session, token: unknown): void => {
            this.confirm(lane, token as TellRecord);
          },
        },
        // The dial timeout bounds connect and handshake together.
        { ...this._sessionOptions, handshakeTimeoutMs: this._dialTimeoutMs },
      );
      if (generation !== this._generation) {
        // The peer closed while this dial was in flight; park it.
        session.destroy();
        throw ErrPeerClosed;
      }

      lane.session = session;
      lane.backoffMs = 0;
      lane.notBefore = 0;
      this.flushRedeliveries(lane, session);
      return session;
    } catch (thrown) {
      if (generation === this._generation) {
        this.armBackoff(lane);
      }

      throw toError(thrown);
    } finally {
      lane.dialing = null;
    }
  }

  private armBackoff(lane: LaneState): void {
    lane.backoffMs =
      lane.backoffMs === 0
        ? this._backoffInitialMs
        : Math.min(lane.backoffMs * 2, this._backoffCapMs);
    lane.notBefore = this._timers.now() + lane.backoffMs;
  }

  // Tell delivery and redelivery.

  private async deliver(lane: LaneState, record: TellRecord): Promise<void> {
    let session: Session;
    try {
      session = await this.acquire(lane);
    } catch (thrown) {
      this.deadLetter(record.envelope, toError(thrown));
      return;
    }

    this.attempt(lane, session, record);
  }

  /**
   * One send of one tell: the record waits in the lane's unconfirmed
   * list until the kernel accepts its last frame, and a refusal dead
   * letters it, since every refusal an open session can give (size
   * caps, admission) is final.
   */
  private attempt(lane: LaneState, session: Session, record: TellRecord): void {
    record.attempts += 1;
    lane.unconfirmed.push(record);
    const refused: Error | null = session.tell(record.envelope, record);
    if (refused !== null) {
      // A refusal is synchronous, so the record is still the newest
      // entry and nothing can have confirmed it in between.
      lane.unconfirmed.pop();
      this.deadLetter(record.envelope, refused);
    }
  }

  /**
   * The kernel accepted a tell's last frame. Confirmations arrive in
   * write order, so the confirmed record is the oldest unconfirmed
   * one; anything else means the close report raced this confirmation
   * and already parked the record for redelivery.
   */
  private confirm(lane: LaneState, record: TellRecord): void {
    /* v8 ignore start -- the mismatch arm is a write success racing
       the close report: the record is already parked for redelivery,
       so purge it there; a kernel-accepted tell is never resent. */
    if (lane.unconfirmed.peek() !== record) {
      const index: number = lane.redeliver.indexOf(record);
      if (index !== -1) {
        lane.redeliver.splice(index, 1);
      }

      return;
    }
    /* v8 ignore stop */

    lane.unconfirmed.shift();
  }

  /** Replays the tells the last session took down, in send order. */
  private flushRedeliveries(lane: LaneState, session: Session): void {
    for (const record of lane.redeliver.splice(0)) {
      this.attempt(lane, session, record);
    }
  }

  private onLaneClose(lane: LaneState, session: Session, error: Error | null): void {
    if (lane.session === session) {
      lane.session = null;
    }

    const reason: Error = error ?? ErrConnClosed;
    for (
      let record: TellRecord | undefined = lane.unconfirmed.shift();
      record !== undefined;
      record = lane.unconfirmed.shift()
    ) {
      if (this._closed) {
        this.deadLetter(record.envelope, reason);
        continue;
      }

      // The attempts cap catches a redelivered tell whose second
      // session also died between admit and kernel write, a race no
      // test can force; without it a flapping peer would replay the
      // same tell forever.
      /* v8 ignore next 4 */
      if (record.attempts >= MAX_TELL_ATTEMPTS) {
        this.deadLetter(record.envelope, reason);
        continue;
      }

      lane.redeliver.push(record);
    }

    this._handlers.onLaneClose?.(lane.byte, error);
  }

  private deadLetter(envelope: DataEnvelope, reason: Error): void {
    this._handlers.onDeadLetter?.(envelope, reason);
  }
}
