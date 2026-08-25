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

import { BroadcastQueue } from "./broadcast";
import type { Clock, ClockTimer } from "./clock";
import { BASE_PROTOCOL_PERIOD_MS, Probe, type ProbeFailure } from "./probe";
import type { Random } from "./random";
import { type SuspicionExpiry, SuspicionManager } from "./suspicion";
import {
  AntiEntropy,
  applyRemoteTruth,
  cancelSupersededSuspicion,
  fanoutLeave,
  join as joinSeeds,
  LEAVE_DRAIN_MS,
  leaveTargets,
  respondToSync,
  SyncActivity,
  type SyncOptions,
} from "./sync";
import type { MembershipTransport } from "./transport";
import {
  type ApplyResult,
  IncarnationExhaustedError,
  isProbeEligibleState,
  type MemberRecord,
  MembershipCapacityError,
  type MembershipEvent,
  type MembershipView,
  type ReapOperation,
  MembershipView as View,
} from "./view";
import {
  MAX_METADATA_BYTES,
  type MembershipUpdate,
  STATE_ALIVE,
  STATE_DEAD,
  STATE_LEFT,
  STATE_SUSPECT,
} from "./wire";

/**
 * Lifecycle of one single-use engine. `new` may retry failed start; `leaving`
 * still accepts inbound packets during drain; `stopped` is terminal.
 *
 * @internal
 */
export type SwimLifecycle = "new" | "starting" | "started" | "leaving" | "stopping" | "stopped";
/** Public lifecycle operation names captured by {@link SwimLifecycleError}. @internal */
export type SwimOperation = "start" | "join" | "leave" | "stop";

/**
 * An operation was requested from a lifecycle state where it cannot be honored.
 *
 * @internal
 */
export class SwimLifecycleError extends Error {
  /** Operation rejected before it could create protocol side effects. */
  readonly operation: SwimOperation;
  /** Exact lifecycle state observed by validation. */
  readonly state: SwimLifecycle;

  /** Captures an invalid operation/state pair in stable machine-readable fields. */
  constructor(operation: SwimOperation, state: SwimLifecycle) {
    super(`cannot ${operation} SWIM engine while it is ${state}`);
    this.name = "SwimLifecycleError";
    this.operation = operation;
    this.state = state;
  }
}

/** Construction dependencies and immutable local identity for one composed engine. @internal */
export interface SwimOptions {
  /** Canonical identity used both as membership name and transport address. */
  readonly address: string;
  /** Initial opaque metadata; defensively copied and bounded by `MAX_METADATA_BYTES`. */
  readonly metadata: Uint8Array;
  /** Shared packet/stream transport exclusively listener-owned and eventually closed by this engine. */
  readonly transport: MembershipTransport;
  /** Monotonic injected clock; numeric times and delays are interpreted as milliseconds. */
  readonly clock: Clock;
  /** Random source shared by probe sequencing/selection and anti-entropy peer choice. */
  readonly random: Random;
  /** Synchronous observer invoked by the view in mutation order; exceptions may propagate. */
  readonly onEvent?: (event: MembershipEvent) => void;
}

/** Externally settled promise pair used to publish a shared lifecycle promise early. */
interface Deferred {
  /** Promise handed to every caller sharing the operation. */
  readonly promise: Promise<void>;
  /** Resolves the shared promise. */
  readonly resolve: () => void;
  /** Rejects the shared promise. */
  readonly reject: (reason: unknown) => void;
}

/**
 * Creates a promise whose settlement is owned by the caller, so the shared
 * promise field can be installed before synchronous work that may reenter
 * the public API through the view's synchronous event callback.
 */
function deferred(): Deferred {
  let resolve!: () => void;
  let reject!: (reason: unknown) => void;
  const promise: Promise<void> = new Promise<void>(
    (res: () => void, rej: (reason: unknown) => void): void => {
      resolve = res;
      reject = rej;
    },
  );

  return { promise, resolve, reject };
}

/**
 * Composed membership engine and sole owner of the shared transport
 * listener. It coordinates detection, suspicion, dissemination, sync,
 * anti-entropy, retention, and lifecycle behavior.
 *
 * @internal
 */
export class Swim {
  /** Canonical local transport and membership identity. */
  readonly #address: string;
  /** Defensive copy used only to construct incarnation-zero alive truth. */
  readonly #metadata: Uint8Array;
  /** Sole transport resource owned by this composed engine. */
  readonly #transport: MembershipTransport;
  /** Shared source of protocol time, deadlines, and retention timers. */
  readonly #clock: Clock;
  /** Authoritative retained membership table for this engine. */
  readonly #view: MembershipView;
  /** Shared dissemination queue for all newly accepted local knowledge. */
  readonly #broadcasts: BroadcastQueue = new BroadcastQueue();
  /** Owns suspect-to-dead timers and confirmation acceleration. */
  readonly #suspicion: SuspicionManager;
  /** Owns packet probing/gossip but not the shared transport listener. */
  readonly #probe: Probe;
  /** Stable dependency bundle used by join, inbound sync, and anti-entropy. */
  readonly #syncOptions: SyncOptions;
  /** Periodic non-overlapping push-pull scheduler. */
  readonly #antiEntropy: AntiEntropy;
  /** One terminal-record retention timer per canonical member identity. */
  readonly #reapTimers: Map<string, ClockTimer> = new Map<string, ClockTimer>();
  /** Current single-use engine lifecycle. */
  #state: SwimLifecycle = "new";
  /** Promise shared by callers observing the in-progress initial start. */
  #startPromise: Promise<void> | undefined;
  /** Promise shared by repeated leave calls through terminal completion. */
  #leavePromise: Promise<void> | undefined;
  /** Promise shared by callers observing an in-progress forced stop. */
  #stopPromise: Promise<void> | undefined;
  /** Idempotent resolver allowing `stop` to shorten an active leave drain. */
  #finishDrain: (() => void) | undefined;
  /** Sticky marker distinguishing successful/attempted graceful leave from plain stop. */
  #left: boolean = false;

  /**
   * Validates identity/metadata, defensively captures options, and wires all
   * protocol components around a fresh empty view. Construction performs no I/O.
   *
   * @throws {RangeError} If transport identity differs or metadata exceeds the wire limit.
   */
  constructor(options: SwimOptions) {
    if (options.transport.address !== options.address) {
      throw new RangeError("transport address must match the SWIM self address");
    }

    if (options.metadata.length > MAX_METADATA_BYTES) {
      throw new RangeError(`metadata cannot exceed ${MAX_METADATA_BYTES} bytes`);
    }

    this.#address = options.address;
    this.#metadata = Uint8Array.from(options.metadata);
    this.#transport = options.transport;
    this.#clock = options.clock;
    this.#view = new View(options.address, options.onEvent);
    this.#suspicion = new SuspicionManager(options.clock, (expiry): void => {
      this.#expireSuspicion(expiry);
    });

    this.#probe = new Probe({
      view: this.#view,
      broadcasts: this.#broadcasts,
      clock: options.clock,
      random: options.random,
      suspicion: this.#suspicion,
      transport: options.transport,
      manageTransport: false,
      callbacks: {
        updates: (updates): void => this.#applyUpdates(updates),
        suspect: (failure): boolean => this.#applyProbeFailure(failure),
      },
    });

    this.#syncOptions = {
      view: this.#view,
      broadcasts: this.#broadcasts,
      clock: options.clock,
      random: options.random,
      transport: options.transport,
      stateChangeTime: (): bigint => this.#stateChangeTime(),
      activity: new SyncActivity(),
      outboundAllowed: (): boolean => this.#state === "started",
      inboundAllowed: (): boolean =>
        this.#state === "starting" || this.#state === "started" || this.#state === "leaving",
      acceptUpdate: (update): boolean => !(this.#shieldsSelf() && update.member === this.#address),
      callbacks: {
        applied: (update): void => {
          this.#truthApplied(update);
          this.#armRemoteSuspicion(update);
        },
        confirmSuspicion: (update): void => {
          this.#confirmSuspicion(update);
        },
        selfRefuted: (): void => {
          this.#probe.selfRefute();
        },
      },
    };

    this.#antiEntropy = new AntiEntropy(this.#syncOptions);
  }

  /** Current lifecycle state; transitions occur synchronously before asynchronous work starts. */
  get lifecycle(): SwimLifecycle {
    return this.#state;
  }

  /** Returns defensive snapshots of all retained records in the view's deterministic order. */
  members(): readonly MemberRecord[] {
    return this.#view.members();
  }

  /** Returns a defensive local-record snapshot, or `undefined` before initial installation. */
  self(): MemberRecord | undefined {
    return this.#view.self();
  }

  /**
   * Installs incarnation-zero local alive truth, enqueues it, binds the sole
   * transport listener, then starts probe and anti-entropy in that order.
   * Concurrent starts share a promise; a failed start returns lifecycle to `new`
   * but leaves the already-installed local truth intact.
   *
   * @throws {SwimLifecycleError} If invoked outside `new`, `starting`, or `started`.
   * @throws Any transport or probe startup error.
   */
  start(): Promise<void> {
    if (this.#state === "started") {
      return Promise.resolve();
    }

    if (this.#state === "starting") {
      return this.#startPromise as Promise<void>;
    }

    if (this.#state !== "new") {
      return Promise.reject(new SwimLifecycleError("start", this.#state));
    }

    // The shared promise is installed before any synchronous work: the view's
    // synchronous `joined` event fires inside applyLocal, and a reentrant
    // start() from that callback must observe the promise, not undefined.
    const starting: Deferred = deferred();
    this.#startPromise = starting.promise;
    this.#state = "starting";
    try {
      const alive: MembershipUpdate = {
        state: STATE_ALIVE,
        selfOriginated: true,
        incarnation: 0,
        stateChangeTime: this.#stateChangeTime(),
        member: this.#address,
        reporter: "",
        metadata: this.#metadata,
      };

      // A retry after a failed start finds the record already installed and
      // enqueued; only a first attempt applies and disseminates it.
      const installed: ApplyResult = this.#view.applyLocal(alive, this.#clock.now());
      if (installed.kind === "applied") {
        this.#broadcasts.enqueue(installed.record, this.#view.aliveOrSuspectCount());
      }
    } catch (error) {
      this.#state = "new";
      starting.reject(error);
      return starting.promise;
    }

    this.#transport
      .start({
        packet: (from, bytes): void => {
          this.#probe.receivePacket(from, bytes);
        },
        stream: (_from, stream): void => {
          void respondToSync(this.#syncOptions, stream).catch((): void => stream.close());
        },
      })
      .then(async (): Promise<void> => {
        if (this.#state !== "starting") {
          return;
        }

        await this.#probe.start();
        if (this.#state !== "starting") {
          return;
        }

        this.#antiEntropy.start();
        this.#state = "started";
      })
      .catch((error: unknown): never => {
        if (this.#state === "starting") {
          this.#state = "new";
        }

        throw error;
      })
      .then(starting.resolve, starting.reject);
    return starting.promise;
  }

  /**
   * Attempts a complete push-pull exchange with every distinct non-self seed
   * in caller order. Resolves only after all attempted seeds settle.
   *
   * @throws {SwimLifecycleError} Unless lifecycle is exactly `started`.
   * @throws {JoinError} When every attempted seed fails.
   */
  async join(seeds: readonly string[]): Promise<void> {
    if (this.#state !== "started") {
      throw new SwimLifecycleError("join", this.#state);
    }

    await joinSeeds(this.#syncOptions, seeds);
  }

  /**
   * Idempotently transitions from `started` to terminal graceful shutdown:
   * stop new work, apply/enqueue left truth, fan it out, drain inbound traffic,
   * then stop probe and transport. Repeated calls share the leave promise.
   *
   * @throws {SwimLifecycleError} Unless started, already leaving, or stopped after leave.
   * @throws Any unexpected shutdown error; per-target fanout errors are ignored.
   */
  leave(): Promise<void> {
    if (this.#state === "leaving") {
      return this.#leavePromise as Promise<void>;
    }

    if (this.#state === "stopped" && this.#left) {
      return this.#leavePromise as Promise<void>;
    }

    if (this.#state !== "started") {
      return Promise.reject(new SwimLifecycleError("leave", this.#state));
    }

    // Installed before the synchronous `left` event fires inside #beginLeave,
    // so a reentrant leave() observes the shared promise, not undefined.
    const leaving: Deferred = deferred();
    this.#leavePromise = leaving.promise;
    try {
      const update: MembershipUpdate = this.#beginLeave();
      this.#completeLeave(update).then(leaving.resolve, leaving.reject);
    } catch (error) {
      leaving.reject(error);
    }

    return leaving.promise;
  }

  /**
   * Forces terminal shutdown without creating left truth. It cancels protocol
   * timers immediately, waits out an in-progress start/leave as needed, and is
   * idempotent once stopping or stopped.
   */
  stop(): Promise<void> {
    if (this.#state === "stopped") {
      return Promise.resolve();
    }

    if (this.#state === "stopping") {
      return this.#stopPromise as Promise<void>;
    }

    const wasLeaving: boolean = this.#state === "leaving";
    const starting: Promise<void> | undefined =
      this.#state === "starting" ? this.#startPromise : undefined;
    this.#prepareStop();
    const stopping: Promise<void> = this.#completeStop(starting, wasLeaving);
    this.#stopPromise = stopping;
    return stopping;
  }

  /**
   * Performs the synchronous half of graceful leave. New outbound work and
   * timers stop before left truth is installed and enqueued.
   */
  #beginLeave(): MembershipUpdate {
    this.#state = "leaving";
    this.#left = true;
    this.#antiEntropy.stop();
    this.#probe.pause();
    this.#suspicion.cancelAll();
    this.#cancelReaps();

    // Reaching started requires the local alive record to have been installed.
    const current: MemberRecord | undefined = this.#view.self();
    if (current === undefined) {
      throw new Error("cannot begin leave: the local member record was never installed");
    }

    const update: MembershipUpdate = {
      state: STATE_LEFT,
      selfOriginated: true,
      incarnation: current.incarnation,
      stateChangeTime: this.#stateChangeTime(),
      member: current.member,
      reporter: "",
      metadata: new Uint8Array(0),
    };

    this.#applyLocalTruth(update);
    return update;
  }

  /** Runs ordered final fanout, optional drain, and resource shutdown for one left record. */
  async #completeLeave(update: MembershipUpdate): Promise<void> {
    await fanoutLeave(this.#transport, update, leaveTargets(this.#view));
    if (this.#state !== "stopping") {
      await this.#drainLeave();
    }

    await this.#shutdown();
  }

  /**
   * Keeps inbound transport handling alive for `LEAVE_DRAIN_MS`. The stored
   * resolver permits forced stop to finish the delay early and safely once.
   */
  #drainLeave(): Promise<void> {
    return new Promise<void>((resolve): void => {
      let timer: ClockTimer;
      const finish: () => void = (): void => {
        this.#finishDrain = undefined;
        this.#clock.cancel(timer);
        resolve();
      };

      this.#finishDrain = finish;
      timer = this.#clock.schedule(LEAVE_DRAIN_MS, finish);
    });
  }

  /** Synchronously enters `stopping`, cancels future work, and releases an active drain. */
  #prepareStop(): void {
    this.#state = "stopping";
    this.#antiEntropy.stop();
    this.#suspicion.cancelAll();
    this.#cancelReaps();
    this.#finishDrain?.();
  }

  /**
   * Waits for startup cleanup or graceful-leave ownership before shutting down,
   * then establishes the terminal `stopped` state. The terminal state is set
   * even when shutdown rejects: by then every resource has been asked to close,
   * and a caller polling the lifecycle must never observe `stopping` forever.
   */
  async #completeStop(starting: Promise<void> | undefined, wasLeaving: boolean): Promise<void> {
    try {
      await starting?.catch((): void => undefined);
      if (wasLeaving) {
        await this.#leavePromise?.catch((): void => undefined);
      } else {
        await this.#shutdown();
      }
    } finally {
      this.#state = "stopped";
    }
  }

  /**
   * Stops probe-owned work before closing the shared transport and marks
   * terminal state. Timers armed by traffic accepted during the leave drain are
   * cancelled last so nothing survives into `stopped`.
   */
  async #shutdown(): Promise<void> {
    await this.#probe.stop();
    await this.#transport.stop();
    this.#suspicion.cancelAll();
    this.#cancelReaps();
    this.#state = "stopped";
  }

  /** Reads the injected clock's Unix epoch milliseconds as a bigint origin time. */
  #stateChangeTime(): bigint {
    return BigInt(this.#clock.epochMilliseconds());
  }

  /** Reports whether remote truth about self must be ignored to protect terminal local truth. */
  #shieldsSelf(): boolean {
    return this.#state === "leaving" || this.#state === "stopping" || this.#state === "stopped";
  }

  /** Applies decoded updates synchronously in wire order. */
  #applyUpdates(updates: readonly MembershipUpdate[]): void {
    for (const update of updates) {
      this.#applyUpdate(update);
    }
  }

  /**
   * Merges one remote update through the shared ingestion pipeline. During
   * leave and shutdown, remote truth about self is ignored so terminal local
   * truth cannot be disturbed or refuted after the engine stopped probing.
   */
  #applyUpdate(update: MembershipUpdate): void {
    if (this.#shieldsSelf() && update.member === this.#address) {
      return;
    }

    try {
      applyRemoteTruth(this.#syncOptions, update);
    } catch (error) {
      // A wire-legal record can still be unapplicable: a self accusation at the
      // maximum incarnation cannot be answered, and a new identity cannot exceed
      // table capacity. The record is dropped so one bad update cannot abort the
      // rest of its packet or destroy the connection that carried it; the sync
      // path rejects the same conditions during preflight instead.
      if (error instanceof IncarnationExhaustedError || error instanceof MembershipCapacityError) {
        return;
      }

      throw error;
    }
  }

  /**
   * Confirms an equal-incarnation suspect record from a distinct accuser and,
   * when the local timer accepted the evidence, re-disseminates the corroborating
   * record. Without re-dissemination the corroboration would die here: the queue
   * rejects equal-precedence truth, remote confirmation counts would stall at the
   * original accuser, and Lifeguard deadline decay would never engage.
   */
  #confirmSuspicion(update: MembershipUpdate): void {
    if (this.#suspicion.confirm(update.member, update.incarnation, update.reporter)) {
      this.#broadcasts.enqueueConfirmation(update, this.#view.aliveOrSuspectCount());
    }
  }

  /**
   * Arms a local suspicion timer for suspect truth learned from another node,
   * so every member that knows of a suspicion owns an expiry deadline and dead
   * declaration cannot depend on the original accuser staying alive. Locally
   * detected failures arm their timer on the probe path with the failed
   * probe's captured period instead.
   */
  #armRemoteSuspicion(update: MembershipUpdate): void {
    if (update.state !== STATE_SUSPECT || update.member === this.#address) {
      return;
    }

    this.#suspicion.start({
      member: update.member,
      incarnation: update.incarnation,
      reporter: update.reporter,
      memberCount: this.#view.aliveOrSuspectCount(),
      effectivePeriod: BASE_PROTOCOL_PERIOD_MS * this.#probe.scale,
    });
  }

  /**
   * Revalidates detector evidence against lifecycle and incarnation, applies
   * locally reported suspect truth, and returns whether suspicion timing may
   * start. A target that was already suspect still returns `true`: a failed
   * probe must arm a local suspicion timer even when another node reported the
   * suspicion first, otherwise only the original reporter could ever expire it.
   */
  #applyProbeFailure(failure: ProbeFailure): boolean {
    if (this.#state !== "started") {
      return false;
    }

    const current: MemberRecord | undefined = this.#view.get(failure.target);
    if (
      current === undefined ||
      current.incarnation !== failure.incarnation ||
      !isProbeEligibleState(current.state)
    ) {
      return false;
    }

    const suspect: MembershipUpdate = {
      state: STATE_SUSPECT,
      selfOriginated: false,
      incarnation: failure.incarnation,
      stateChangeTime: this.#stateChangeTime(),
      member: failure.target,
      reporter: this.#address,
      metadata: new Uint8Array(0),
    };

    const result: ApplyResult = this.#view.apply(
      suspect,
      this.#clock.now(),
      this.#stateChangeTime(),
    );
    if (result.kind === "applied") {
      this.#broadcasts.enqueue(result.record, this.#view.aliveOrSuspectCount());
      this.#truthApplied(result.record);
      return true;
    }

    if (result.kind === "confirmed") {
      this.#confirmSuspicion(suspect);
      return true;
    }

    // An ignored result with retained suspect truth means this node is already
    // a recorded reporter; the local timer must still exist.
    const retained: MemberRecord | undefined = this.#view.get(failure.target);
    return retained?.state === STATE_SUSPECT && retained.incarnation === failure.incarnation;
  }

  /** Converts an on-time expiry for the exact current suspect incarnation into local dead truth. */
  #expireSuspicion(expiry: SuspicionExpiry): void {
    if (this.#state !== "started") {
      return;
    }

    const current: MemberRecord | undefined = this.#view.get(expiry.member);
    if (
      current === undefined ||
      current.state !== STATE_SUSPECT ||
      current.incarnation !== expiry.incarnation
    ) {
      return;
    }

    const dead: MembershipUpdate = {
      state: STATE_DEAD,
      selfOriginated: false,
      incarnation: expiry.incarnation,
      stateChangeTime: this.#stateChangeTime(),
      member: expiry.member,
      reporter: "",
      metadata: new Uint8Array(0),
    };

    this.#applyLocalTruth(dead);
  }

  /**
   * Applies engine-generated truth, disseminates it, and runs maintenance.
   * Returns `true` only when the view accepted a new record.
   */
  #applyLocalTruth(update: MembershipUpdate): boolean {
    const result: ApplyResult = this.#view.applyLocal(update, this.#clock.now());
    if (result.kind !== "applied") {
      return false;
    }

    this.#broadcasts.enqueue(result.record, this.#view.aliveOrSuspectCount());
    this.#truthApplied(result.record);
    return true;
  }

  /** Runs post-enqueue suspicion cancellation before terminal-record retention maintenance. */
  #truthApplied(update: MembershipUpdate): void {
    this.#cancelSupersededSuspicion(update);
    this.#updateRetention(update);
  }

  /** Applies the shared suspicion-supersession rule against the owned manager. */
  #cancelSupersededSuspicion(update: MembershipUpdate): void {
    cancelSupersededSuspicion(update, (member, incarnation): void => {
      this.#suspicion.cancelThrough(member, incarnation);
    });
  }

  /** Arms retention for dead/left truth and cancels retention when truth becomes nonterminal. */
  #updateRetention(update: MembershipUpdate): void {
    if (update.state === STATE_DEAD || update.state === STATE_LEFT) {
      this.#scheduleReap(update.member);
    } else {
      this.#cancelReap(update.member);
    }
  }

  /**
   * Replaces any prior member timer with the view's current generation-bound
   * reap operation, scheduled at its absolute injected-clock due time.
   */
  #scheduleReap(member: string): void {
    this.#cancelReap(member);
    // Terminal truth is retained before this method is called.
    const operation: ReapOperation | undefined = this.#view.reapOperation(member);
    if (operation === undefined) {
      throw new Error(`cannot schedule a reap for ${member} without retained terminal truth`);
    }

    const timer: ClockTimer = this.#clock.schedule(
      Math.max(0, operation.dueAt - this.#clock.now()),
      (): void => {
        if (this.#reapTimers.get(member) !== timer) {
          return;
        }

        this.#reapTimers.delete(member);
        this.#view.reap(operation);
      },
    );
    this.#reapTimers.set(member, timer);
  }

  /** Cancels and forgets the owned retention timer for one member, if present. */
  #cancelReap(member: string): void {
    const timer: ClockTimer | undefined = this.#reapTimers.get(member);
    if (timer !== undefined) {
      this.#clock.cancel(timer);
      this.#reapTimers.delete(member);
    }
  }

  /** Cancels every owned terminal-retention timer during leave or forced stop. */
  #cancelReaps(): void {
    for (const timer of this.#reapTimers.values()) {
      this.#clock.cancel(timer);
    }

    this.#reapTimers.clear();
  }
}
