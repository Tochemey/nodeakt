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

import type { Actor } from "./actor";
import { ActorRef, type IsolateRoute } from "./actor.ref";
import type { ActorSystem } from "./actor.system";
import { type Behavior, BehaviorStack } from "./behavior.stack";
import { Context } from "./context";
import {
  ActorInitializationError,
  ActorNotFoundError,
  ErrDead,
  ErrInvalidActorName,
  ErrInvalidReentrancyMode,
  ErrInvalidTimeout,
  ErrReentrancyDisabled,
  ErrReentrancyInFlightLimit,
  ErrRequestTimeout,
  ErrReservedName,
  ErrStashBufferEmpty,
  ErrUndefinedActor,
  ErrUnhandled,
} from "./errors";
import type { Mailbox } from "./mailbox";
// biome-ignore format: one line per import
import {
  ActorChildCreated,
  ActorPassivated,
  ActorReinstated,
  ActorRestarted,
  ActorStarted,
  ActorStopped,
  ActorSuspended,
  Panicking,
  PanicSignal,
  PoisonPill,
  PostStart,
  RequestReply,
  RuntimeCommand,
  Terminated,
} from "./messages";
import {
  defaultPassivationStrategy,
  MessagesCountBasedStrategy,
  type PassivationStrategy,
} from "./passivation";
import { addressOf, isValidActorName, newPathAt, type Path } from "./path";
import { PidTree } from "./pid.tree";
import { type PipeTask, runPipe } from "./pipe";
import type { PipeOptions } from "./pipe.options";
import {
  cancelAsk,
  createAskContext,
  createReceiveContext,
  createRequestContext,
  type ReceiveContext,
} from "./receive.context";
import {
  completedRequest,
  isValidReentrancyMode,
  ReentrancyState,
  type RequestCall,
  RequestHandle,
  type RequestOptions,
} from "./reentrancy";
import { isSystemName, noSenderName } from "./reserved";
import type { SpawnOptions } from "./spawn.options";
import { Stash } from "./stash";
import {
  backoffDelay,
  defaultSupervisor,
  OneForAllStrategy,
  RestartDirective,
  ResumeDirective,
  StopDirective,
  type Supervisor,
} from "./supervisor";
import { UnboundedMailbox } from "./unbounded.mailbox";

/**
 * Maximum number of messages an actor processes per event-loop turn. Once
 * the quota is reached, the receive loop yields with `setImmediate` so I/O
 * and other actors get their share of the loop; processing resumes on the
 * next turn. Without this cap, a flooded mailbox drained on the microtask
 * queue would starve the entire process.
 *
 * The value balances throughput against event-loop lag: each yield costs
 * one full trip around the event loop, so small quotas tax every message,
 * while the quota bounds how long a burst can monopolize the loop. At
 * typical per-message costs this ceiling keeps a turn around a hundred
 * microseconds.
 */
const THROUGHPUT = 2048;

/** Actors whose receive loop is due to run. One queue for the process;
 * idle actors hold no scheduler closure. */
const ready: PID[] = [];
let armed = false;
let yieldTurn = false;

function arm(later: boolean): void {
  if (later) {
    yieldTurn = true;
  }

  if (armed) {
    return;
  }

  armed = true;
  if (later) {
    setImmediate(pump);
  } else {
    queueMicrotask(pump);
  }
}

function pump(): void {
  armed = false;
  const later = yieldTurn;
  yieldTurn = false;
  const n = ready.length;

  for (let i = 0; i < n; i++) {
    (ready[i] as PID).dispatch();
  }

  // Compact without allocating: splice would materialize the removed
  // elements as a fresh array on every pump.
  if (ready.length === n) {
    ready.length = 0;
  } else {
    ready.copyWithin(0, n);
    ready.length -= n;
  }

  if (ready.length > 0) {
    arm(later);
  }
}

/** A promise-based delay whose timer never keeps the process alive on its
 * own. */
function pause(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms).unref();
  });
}

/**
 * PID is the process identity of a running actor: the runtime handle that
 * owns the actor instance, its mailbox, and its behavior state, and the
 * only way to interact with the actor.
 *
 * Messages sent with {@link tell} are enqueued into the actor's mailbox
 * and processed strictly one at a time by the current behavior; when a
 * behavior returns a promise, the next message is not dequeued until it
 * settles. This is what makes an actor's internal state safe without
 * synchronization.
 *
 * A PID is created by the actor system when an actor is spawned.
 * Developers send messages with {@link tell}, spawn children, watch
 * peers, and inspect identity; behavior switching and stashing belong
 * on {@link ReceiveContext}.
 */
export class PID {
  private readonly _actor: Actor;
  private readonly _mailbox: Mailbox;
  private readonly _path: Path;
  private readonly _actorSystem: ActorSystem;

  /** Created on first behavior switch; most actors never need one. */
  private _behaviorStack: BehaviorStack | null = null;

  /** Created on first stash; most actors never need one. */
  private _stash: Stash | null = null;

  /**
   * The tree recording this hierarchy's parent, child, and watch
   * relationships. Shared by every actor in the hierarchy; created lazily
   * for top-level actors, inherited by children.
   */
  private _tree: PidTree | null = null;

  /** The parent that spawned this actor; null for top-level actors. */
  private _parent: PID | null = null;

  private _running = false;
  private _processing = false;
  private _stopping = false;
  private _suspended = false;
  private _restarting = false;

  /** Whether this PID is registered in the hierarchy tree. */
  private _inTree = false;

  private _shutdown: Promise<void> | null = null;

  /** Created on first idle wait; only shutdown and restart ever park. */
  private _idleResolvers: Array<() => void> | null = null;

  /** Created on first {@link onStopped} registration; invoked once when
   * shutdown completes. */
  private _stopListeners: Array<() => void> | null = null;

  /** The transport of a PID whose actor lives on another isolate; null
   * for every local actor, so the hot paths pay one predictable null
   * check. Set once by the runtime's routed-handle factory, never for
   * an actor of this isolate. */
  private _route: IsolateRoute | null = null;

  private _latestActivity = 0;
  private _processedCount = 0;
  private _restartCount = 0;
  private _consecutiveFaults = 0;
  private _lastFaultAt = 0;
  private readonly _supervisor: Supervisor;
  private readonly _passivationStrategy: PassivationStrategy;

  /** The actor's `receive` method, cached at construction so the receive
   * loop loads it from the PID's own shape instead of performing a
   * megamorphic property load across every actor class. */
  private readonly _receive: Actor["receive"];

  /** The message budget of a count-based passivation strategy, 0 when the
   * strategy is not count based; checked per message, so the strategy
   * type is resolved once here instead of once per message. */
  private readonly _maxMessages: number;

  /** Whether an active time-based passivation schedule consults
   * {@link latestActivity}; the drain-end clock read is skipped
   * otherwise. Toggled by the passivation scheduler. */
  private _trackActivity = false;

  /** The actor's request bookkeeping; null unless the actor was spawned
   * with reentrancy enabled. */
  private readonly _reentrancy: ReentrancyState | null;

  /**
   * Runtime plumbing: the actor system mints a PID as it spawns an
   * actor, wiring the instance to its mailbox, supervisor, and tree.
   * Developers receive PIDs from `spawn` and `ctx.self`; they never
   * construct one, because a hand-built PID sits in no tree and drives
   * no receive loop.
   *
   * @internal
   */
  constructor(
    actor: Actor,
    path: Path,
    actorSystem: ActorSystem,
    options?: SpawnOptions,
    parent?: PID,
    tree?: PidTree,
  ) {
    this._actor = actor;
    this._path = path;
    this._actorSystem = actorSystem;
    this._mailbox = options?.mailbox ?? new UnboundedMailbox();
    this._supervisor = options?.supervisor ?? defaultSupervisor;
    this._passivationStrategy = options?.passivationStrategy ?? defaultPassivationStrategy;
    this._parent = parent ?? null;
    this._tree = tree ?? null;
    this._receive = actor.receive;

    const strategy = this._passivationStrategy;
    this._maxMessages = strategy instanceof MessagesCountBasedStrategy ? strategy.maxMessages : 0;

    const reentrancy = options?.reentrancy;
    if (reentrancy === undefined || reentrancy.mode === "off") {
      this._reentrancy = null;
    } else if (!isValidReentrancyMode(reentrancy.mode)) {
      throw ErrInvalidReentrancyMode;
    } else {
      this._reentrancy = new ReentrancyState(reentrancy.mode, reentrancy.maxInFlight ?? 0);
    }
  }

  /** Returns the actor's unique identifier, its canonical path string. */
  id(): string {
    return this._path.toString();
  }

  /** Returns the actor's name, the last segment of its path. */
  name(): string {
    return this._path.name();
  }

  /**
   * Reports whether this PID and `other` designate the same actor name
   * and location. The {@link Path.uid | uid} is not compared.
   */
  equals(other: PID): boolean {
    return this._path.equals(other.path());
  }

  /** Returns the underlying actor implementation. */
  actor(): Actor {
    return this._actor;
  }

  /**
   * Returns the implementation's constructor name. Object-literal actors
   * report `"Object"`.
   */
  kind(): string {
    return this._actor.constructor.name;
  }

  /** Returns the actor's path. */
  path(): Path {
    return this._path;
  }

  /** Returns the actor system this actor belongs to. */
  actorSystem(): ActorSystem {
    return this._actorSystem;
  }

  /** Reports whether the actor has been started and can receive messages:
   * it is neither stopping nor suspended. */
  isRunning(): boolean {
    return this._running && !this._stopping && !this._suspended;
  }

  /**
   * Reports whether the actor is suspended due to a fault: it holds its
   * state but accepts and processes no message until it is restarted or
   * reinstated. A suspended actor retains its queued messages and stash
   * until it is restarted, reinstated, or stopped.
   */
  isSuspended(): boolean {
    return this._suspended;
  }

  /** Returns the number of times the actor has been restarted. */
  restartCount(): number {
    return this._restartCount;
  }

  /**
   * Reports whether the actor has nothing to do: no message is being
   * processed, its mailbox is empty, nothing waits in its stash, and no
   * request awaits a reply. A stashed message counts as pending work
   * because stopping the actor would drop it, and an in-flight request
   * counts because its continuation still needs a turn. Runtime plumbing
   * for the passivation scheduler, which verifies an actor is genuinely
   * idle before stopping it.
   *
   * @internal
   */
  isIdle(): boolean {
    return (
      !this._processing &&
      this.mailboxEmpty() &&
      (this._stash === null || this._stash.isEmpty()) &&
      (this._reentrancy === null || this._reentrancy.inFlight === 0)
    );
  }

  /**
   * Returns the timestamp, in milliseconds since the epoch, of the actor's
   * latest activity: its start, restart, reinstatement, or the completion
   * of its latest mailbox drain. Used by the passivation runtime, which
   * only consults it once the actor is idle; the clock is deliberately
   * not read per message.
   */
  latestActivity(): number {
    return this._latestActivity;
  }

  /** Returns the number of messages the actor has processed since it
   * started. */
  processedCount(): number {
    return this._processedCount;
  }

  /** Returns the number of messages waiting in the actor's stash
   * buffer. */
  stashSize(): number {
    return this._stash?.len() ?? 0;
  }

  /** Returns the passivation strategy configured for this actor. */
  passivationStrategy(): PassivationStrategy {
    return this._passivationStrategy;
  }

  /**
   * Starts the actor: runs its `preStart` hook, registers it in the
   * hierarchy, announces `PostStart` to it, and begins accepting
   * messages. Starting an already running actor is a no-op.
   *
   * Runtime plumbing for spawn; developers never start a PID by hand.
   *
   * @throws An {@link ActorInitializationError} when `preStart` fails;
   * the underlying failure is available as its `cause` and the actor
   * never receives a message.
   *
   * @internal
   */
  async start(): Promise<void> {
    if (this._running) {
      return;
    }

    await this.runPreStart();

    this._running = true;
    this._latestActivity = Date.now();
    this.tree().addNode(this._parent, this);

    if (this.canEmit()) {
      this._actorSystem.publishEvent(new ActorStarted(this._path.toString(), Date.now()));
    }

    // PostStart is enqueued before anyone else can send, so it is the
    // very first message every actor processes. Runtime actors start
    // while the system itself is still starting and receive no
    // announcement.
    if (this._actorSystem.isRunning()) {
      this._actorSystem.noSender().tell(this, new PostStart());
    }
  }

  /**
   * Stops the actor gracefully: no new message is accepted, the messages
   * already in the mailbox are processed, `postStop` runs, and the
   * actor's resources are released. An error thrown by `postStop` is
   * logged and does not prevent the stop.
   *
   * Repeated calls return the same promise; shutting down an actor that
   * never started is a no-op. An actor owned by another isolate cannot
   * be stopped through its handle: the rejection is loud instead of a
   * silent no-op that would differ from local behavior.
   */
  shutdown(): Promise<void> {
    if (this._route !== null) {
      return Promise.reject(
        new TypeError("an actor owned by another isolate cannot be stopped through its handle"),
      );
    }

    if (this._shutdown !== null) {
      return this._shutdown;
    }

    if (!this._running) {
      return Promise.resolve();
    }

    this._shutdown = this.doShutdown();
    return this._shutdown;
  }

  private async doShutdown(): Promise<void> {
    this._stopping = true;

    // The receive loop may still be draining the backlog accepted before
    // the stopping flag was set, possibly parked on an async behavior or
    // between two setImmediate batches. Those messages are owed processing
    // before postStop runs, so park here until the loop empties the
    // mailbox and calls notifyIdle. When the processing flag is clear the
    // mailbox is already empty: enqueue always arms the loop
    // synchronously, new messages are rejected once the stopping flag is
    // set, and nothing can interleave between this check and the await on
    // a single thread.
    if (this._processing) {
      await this.awaitIdle();
    }

    // Children cannot outlive their parent: stop them before this actor
    // runs its own cleanup.
    const children = this.tree().children(this);
    if (children.length > 0) {
      await Promise.all(children.map((child) => child.shutdown()));
    }

    await this.runPostStop();

    this._mailbox.dispose();
    this._stash?.dispose();
    this._stash = null;
    this._behaviorStack?.reset();
    this._running = false;
    this._stopping = false;
    this._suspended = false;

    // Capture the watchers before the tree forgets this actor, then
    // deliver the death notifications.
    const watchers = this.tree().watchers(this);
    this.tree().deleteNode(this);
    this._parent = null;

    if (watchers.length > 0) {
      const terminated = new Terminated(this._path.toString());

      for (const watcher of watchers) {
        this.tell(watcher, terminated);
      }
    }

    // A dead PID must not pin the hierarchy: drop the tree reference so a
    // retained handle to this actor keeps nothing else alive.
    this._tree = null;

    if (this.canEmit()) {
      this._actorSystem.publishEvent(new ActorStopped(this._path.toString(), Date.now()));
    }

    const listeners = this._stopListeners;
    if (listeners !== null) {
      this._stopListeners = null;
      for (const listener of listeners) {
        listener();
      }
    }
  }

  /**
   * Stops the actor because it stayed idle past its passivation strategy.
   * It is a graceful stop like {@link shutdown}: the mailbox drains,
   * children stop, `postStop` runs, and watchers receive `Terminated`. An
   * {@link ActorPassivated} event marks the stop as idle-triggered,
   * published ahead of the {@link ActorStopped} the stop itself emits.
   * Runtime plumbing for the passivation scheduler.
   *
   * @internal
   */
  passivate(): Promise<void> {
    // A count-based budget can trip passivate again while the graceful
    // stop already begun by the first trip is still draining the backlog;
    // the stopping flag keeps that from publishing a second event.
    if (!this._stopping && this.canEmit()) {
      this._actorSystem.publishEvent(new ActorPassivated(this._path.toString(), Date.now()));
    }

    return this.shutdown();
  }

  /**
   * Restarts the actor: its children are stopped, its behavior, stash,
   * and counters are reset, `postStop` and `preStart` run again, and the
   * messages still queued in its mailbox are processed by the restarted
   * actor. A suspended actor is re-initialized without the
   * `postStop` teardown step.
   *
   * @throws The {@link ErrDead} sentinel when the actor is stopping,
   * already restarting, or has never started, and when a shutdown wins
   * the race against the restart.
   * @throws An {@link ActorInitializationError} when `preStart` fails; the
   * actor is then left suspended, so the restart can be attempted again.
   */
  async restart(): Promise<void> {
    if (this._stopping || !this._running || this._restarting) {
      throw ErrDead;
    }

    this._restarting = true;

    try {
      const wasSuspended = this._suspended;
      this._suspended = true;

      // Wait out the receive loop; it exits after the in-flight message.
      if (this._processing) {
        await this.awaitIdle();
      }

      // A shutdown may have started while parked above; it owns the
      // teardown, so the restart must back off.
      if (this._stopping) {
        throw ErrDead;
      }

      // Children belong to the actor that spawned them: stop them and
      // let the restarted actor respawn what it needs.
      const children = this.tree().children(this);
      if (children.length > 0) {
        await Promise.all(children.map((child) => child.shutdown()));
      }

      if (!wasSuspended) {
        await this.runPostStop();
      }

      this._behaviorStack?.reset();
      this._stash?.dispose();
      this._stash = null;
      this._processedCount = 0;

      // On failure the actor stays suspended: it holds no fresh state
      // yet, but another restart attempt remains possible.
      await this.runPreStart();

      this._suspended = false;
      this._restartCount++;
      this._latestActivity = Date.now();

      if (this.canEmit()) {
        this._actorSystem.publishEvent(new ActorRestarted(this._path.toString(), Date.now()));
      }

      if (!this.mailboxEmpty()) {
        this.schedule();
      }
    } finally {
      this._restarting = false;
    }
  }

  /**
   * Revives a suspended actor, letting it process messages again. Pass
   * the actor's `PID` to reinstate a handle the caller already holds, or
   * a `string` to reinstate a child of this actor by the name it was
   * spawned under. Reinstating an actor that is not suspended is a no-op.
   *
   * @param target - The suspended actor's PID, or the name of a
   * suspended child of this actor.
   *
   * @throws The {@link ErrDead} sentinel when a name is given and this
   * actor is not running.
   * @throws {@link ActorNotFoundError} when a name is given and this
   * actor has no child under it.
   */
  reinstate(target: PID | string): void {
    if (typeof target === "string") {
      if (!this.isRunning()) {
        throw ErrDead;
      }

      const cid = this._tree?.child(this, target);
      if (cid === undefined) {
        throw new ActorNotFoundError(target);
      }

      cid.doReinstate();
      return;
    }

    target.doReinstate();
  }

  /**
   * Sends a message asynchronously to the target PID, fire and forget.
   * This actor is recorded as the sender, which the receiving behavior
   * sees as `ctx.sender`.
   *
   * To send from outside an actor, send on behalf of the system's
   * NoSender actor: `system.noSender().tell(target, message)`.
   *
   * @param to - The PID of the receiving actor.
   * @param message - The message to deliver.
   *
   * @returns `null` when the message was accepted, {@link ErrDead} when
   * the target is not running, or the mailbox error when it rejected the
   * message (for example a bounded mailbox at capacity).
   */
  tell(to: PID, message: unknown): Error | null {
    const route = to._route;
    if (route !== null) {
      return route.tell(to._path, message, this);
    }

    return to.doReceive(createReceiveContext(message, to, this));
  }

  /**
   * Sends a message to `to` and waits for {@link ReceiveContext.response}.
   * This actor is recorded as the sender.
   *
   * To ask from outside an actor, ask on behalf of the system's NoSender
   * actor: `system.noSender().ask(target, message, timeout)`.
   *
   * Do not ask an actor that is processing this call: the target cannot
   * reply until its current message finishes, and asking `this` from
   * `receive` would never complete.
   *
   * @param to - The PID of the receiving actor.
   * @param message - The message to deliver.
   * @param timeout - How long to wait, in milliseconds. Must be
   * positive. The wait is a lower bound with coarse expiry: an
   * unanswered ask is rejected between one and two timeout periods, so
   * sending never has to read the clock.
   *
   * @returns A promise of the value passed to
   * {@link ReceiveContext.response}. It rejects with the {@link ErrDead}
   * sentinel when the target is not running, the {@link ErrInvalidTimeout}
   * sentinel when `timeout` is not a positive duration, the
   * {@link ErrRequestTimeout} sentinel when no reply arrives in time, and
   * the mailbox rejection error when delivery fails.
   */
  ask(to: PID, message: unknown, timeout: number): Promise<unknown> {
    const route = to._route;
    if (route !== null) {
      return route.ask(to._path, message, timeout, this);
    }

    if (!to.isRunning()) {
      to.deadletter(this, message, ErrDead);
      return Promise.reject(ErrDead);
    }

    if (timeout <= 0) {
      return Promise.reject(ErrInvalidTimeout);
    }

    const { ctx, reply } = createAskContext(message, to, this, timeout);

    // The running check above already vetted the target, so the context
    // goes straight into the mailbox instead of through doReceive.
    const err = to._mailbox.enqueue(ctx);
    if (err !== null) {
      cancelAsk(ctx);
      to.deadletter(this, message, err);
      return Promise.reject(err);
    }

    to.schedule();
    return reply;
  }

  /**
   * Delivers a message to this actor and routes the response into
   * callbacks instead of a promise: `resolve` receives the value passed
   * to `ReceiveContext.response`, `reject` the failure, or the coarse
   * expiry when `timeout` is positive. Runtime plumbing for the
   * receiving side of a transport, where the response must travel back
   * as an envelope rather than settle a local promise.
   *
   * @returns `null` when the message entered the mailbox, the
   * {@link ErrDead} sentinel when this actor is not running, or the
   * mailbox rejection error. On a failure the message is routed to
   * dead letters here and neither callback runs.
   *
   * @internal
   */
  deliverAsk(
    message: unknown,
    sender: PID,
    timeout: number,
    resolve: (value: unknown) => void,
    reject: (reason: Error) => void,
  ): Error | null {
    if (!this.isRunning()) {
      this.deadletter(sender, message, ErrDead);
      return ErrDead;
    }

    const ctx = createRequestContext(message, this, sender, timeout, resolve, reject);
    const err = this._mailbox.enqueue(ctx);
    if (err !== null) {
      cancelAsk(ctx);
      this.deadletter(sender, message, err);
      return err;
    }

    this.schedule();
    return null;
  }

  /**
   * Enqueues a delivery another actor received into this actor's
   * mailbox: the context is re-attached so this actor becomes the
   * receiver, while the message, the original sender, and any reply
   * channel stay intact. Runtime plumbing for routers, which forward
   * deliveries without processing them; the forwarding actor must be
   * done with the context, because after this call the context belongs
   * to this actor's mailbox.
   *
   * @returns `null` when the delivery was accepted, the {@link ErrDead}
   * sentinel when this actor is not running, or the mailbox rejection
   * error. A failed redelivery is routed to dead letters carrying the
   * original sender; a pending reply channel is left for the caller to
   * reject.
   *
   * @internal
   */
  redeliver(ctx: ReceiveContext): Error | null {
    ctx.retarget(this);
    return this.doReceive(ctx);
  }

  /**
   * Sends a message to `to` and returns a handle whose continuation runs
   * on this actor's own turn once the reply arrives. Runtime plumbing for
   * `ReceiveContext.request`: requests belong to a running actor's turn,
   * so issue them from a receive context.
   *
   * @internal
   */
  request(to: PID, message: unknown, options?: RequestOptions): RequestCall {
    const route = to._route;
    if (route !== null) {
      return route.request(to._path, message, this, options);
    }

    const opened = this.openRequest(options);
    if (opened instanceof Error) {
      return completedRequest(opened);
    }

    if (!to.isRunning()) {
      to.deadletter(this, message, ErrDead);
      return completedRequest(ErrDead);
    }

    // The handle severs its delivery link the moment the outcome is
    // decided at the source, so a cancel can never touch a context the
    // target's loop has already recycled.
    const handle = opened;
    const ctx = createRequestContext(
      message,
      to,
      this,
      options?.timeout ?? 0,
      (reply) => {
        handle.detachDelivery();
        this.deliverRequestReply(handle, reply, null);
      },
      (error) => {
        handle.detachDelivery();
        this.deliverRequestReply(handle, undefined, error);
      },
    );
    handle.attachDelivery(ctx);

    const err = to._mailbox.enqueue(ctx);
    if (err !== null) {
      cancelAsk(ctx);
      to.deadletter(this, message, err);
      return completedRequest(err);
    }

    this.admitRequest(handle);
    to.schedule();
    return handle;
  }

  /**
   * Runs `task` off this actor's message processing and delivers its
   * resolution value to `to` as an ordinary message, with this actor
   * recorded as the sender. The call returns immediately: this actor
   * keeps processing messages while the task runs, and delivery happens
   * on the target's own turn through the normal send path.
   *
   * The task is a promise, or a thunk returning one; the thunk receives
   * an `AbortSignal` that fires when the pipe's timeout expires. A pipe
   * never throws and a failing pipe delivers nothing: when the target
   * is not running when the task settles, the result becomes a dead
   * letter carrying the failing reason; a rejected task is logged and
   * routed to dead letters with the rejection as the reason; and a pipe
   * whose timeout expires first dead-letters with {@link ErrPipeTimeout}.
   * To receive a failure as a message instead, map the rejection before
   * piping: `task.catch((err) => new LoadFailed(err))`.
   *
   * The rejection handler is attached before the call returns, so a
   * piped task can never produce an unhandled rejection warning.
   *
   * Stopping this actor does not cancel the task: the promise is
   * already running, and its result is still delivered when it settles.
   * A task that must stop with the pipe observes the timeout's abort
   * signal.
   *
   * @param to - The PID of the actor receiving the result.
   * @param task - The promise, or a thunk returning it.
   * @param options - The pipe configuration; `timeout` is the deadline
   * in milliseconds, with no deadline when omitted or non-positive.
   */
  pipeTo(to: PID, task: PipeTask, options?: PipeOptions): void {
    runPipe(this, to, null, task, options);
  }

  /**
   * Pipes like {@link pipeTo}, addressing the target by name instead of
   * by handle: when the task settles, the name is resolved among the
   * running top-level actors and the result is delivered to whoever
   * holds it then. When no running top-level actor holds the name, the
   * result becomes a dead letter carrying an {@link ActorNotFoundError}.
   *
   * @param actorName - The name of the top-level actor receiving the
   * result.
   * @param task - The promise, or a thunk returning it.
   * @param options - The pipe configuration; `timeout` is the deadline
   * in milliseconds, with no deadline when omitted or non-positive.
   */
  pipeToName(actorName: string, task: PipeTask, options?: PipeOptions): void {
    runPipe(this, null, actorName, task, options);
  }

  /**
   * Runs request admission for this actor and mints the handle of one
   * request whose delivery the caller performs: reentrancy must be
   * enabled, the mode valid, and the in-flight cap not reached. The
   * returned handle is not yet counted; call {@link admitRequest} once
   * delivery has been accepted. Runtime plumbing shared by
   * {@link request} and the transport layer.
   *
   * @returns The open handle, or the typed sentinel refusing admission.
   *
   * @internal
   */
  openRequest(options?: RequestOptions): RequestHandle | Error {
    const state = this._reentrancy;
    if (state === null) {
      return ErrReentrancyDisabled;
    }

    const mode = options?.mode ?? state.mode;
    if (mode === "off") {
      return ErrReentrancyDisabled;
    }

    if (!isValidReentrancyMode(mode)) {
      return ErrInvalidReentrancyMode;
    }

    if (state.maxInFlight !== 0 && state.inFlight >= state.maxInFlight) {
      return ErrReentrancyInFlightLimit;
    }

    return new RequestHandle(this, mode === "stashNonReentrant");
  }

  /**
   * Commits an admitted request into this actor's in-flight bookkeeping
   * once its delivery has been accepted; the count drops again when the
   * handle settles on this actor's turn. Runtime plumbing shared by
   * {@link request} and the transport layer.
   *
   * @internal
   */
  admitRequest(handle: RequestHandle): void {
    const state = this._reentrancy as ReentrancyState;
    state.inFlight++;

    if (handle.stashMode) {
      state.paused++;
    }
  }

  /**
   * Routes a request outcome through this actor's mailbox so the
   * continuation runs on its own turn. When the actor can no longer run
   * a turn the request is abandoned. Runtime plumbing for the request
   * machinery.
   *
   * @internal
   */
  deliverRequestReply(handle: RequestHandle, reply: unknown, error: Error | null): void {
    const err = this.doReceive(
      createReceiveContext(new RequestReply(handle, reply, error), this, this),
    );
    if (err !== null) {
      handle.abandon();
    }
  }

  /** Settles one request on this actor's turn: fixes the in-flight
   * bookkeeping, resumes stashed messages when the last stash-mode
   * request completes, and runs the continuation. */
  private completeRequest(msg: RequestReply): void {
    if (!msg.handle.settle(msg.reply, msg.error)) {
      return;
    }

    const state = this._reentrancy as ReentrancyState;
    state.inFlight--;

    if (msg.handle.stashMode) {
      state.paused--;

      if (state.paused === 0) {
        this.unstashAll();
      }
    }

    msg.handle.run();
  }

  /**
   * Creates and starts an actor as a child of this one. The child's path
   * descends from this actor's path, the child receives `PostStart` as
   * its first message, and it is stopped automatically when this actor
   * shuts down.
   *
   * When a child already holds the given name it is returned as is, even
   * while suspended or stopping: a name is occupied until its actor has
   * fully stopped, so a live actor is never silently replaced.
   *
   * @param name - The child's name, unique among this actor's children.
   * @param actor - The actor implementation to run.
   * @param options - The mailbox and passivation configuration; both have
   * defaults.
   *
   * @returns The child's PID.
   *
   * @throws The {@link ErrDead} sentinel when this actor is not running.
   * @throws The {@link ErrReservedName} sentinel when the name carries
   * the runtime's reserved prefix.
   * @throws The {@link ErrInvalidActorName} sentinel when the name is
   * empty, longer than 255 characters, or violates the name syntax.
   * @throws An {@link ActorInitializationError} when the child's
   * `preStart` fails; the underlying failure is available as its `cause`
   * and the child is not registered.
   */
  async spawnChild(name: string, actor: Actor, options?: SpawnOptions): Promise<PID> {
    if (!this.isRunning()) {
      throw ErrDead;
    }

    if (isSystemName(name)) {
      throw ErrReservedName;
    }

    if (!isValidActorName(name)) {
      throw ErrInvalidActorName;
    }

    const existing = this.tree().child(this, name);
    if (existing !== undefined) {
      return existing;
    }

    const path = newPathAt(name, addressOf(this._path), this._path);

    const child = new PID(actor, path, this._actorSystem, options, this, this.tree());

    await child.start();
    this._actorSystem.schedulePassivation(child);

    if (this.canEmit()) {
      this._actorSystem.publishEvent(
        new ActorChildCreated(path.toString(), this._path.toString(), Date.now()),
      );
    }

    return child;
  }

  /**
   * Returns the running child registered under `name`.
   *
   * @throws The {@link ErrDead} sentinel when this actor is not running.
   * @throws {@link ActorNotFoundError} when no running child holds the name.
   */
  child(name: string): PID {
    if (!this.isRunning()) {
      throw ErrDead;
    }

    const cid = this._tree?.child(this, name);
    if (cid === undefined || !cid.isRunning()) {
      throw new ActorNotFoundError(cid?.path().toString() ?? name);
    }

    return cid;
  }

  /**
   * Returns the parent that spawned this actor, or `undefined` for a
   * root actor that has no parent.
   */
  parent(): PID | undefined {
    return this._parent ?? undefined;
  }

  /** Returns this actor's running children. */
  children(): PID[] {
    if (this._tree === null) {
      return [];
    }

    return this._tree.children(this).filter((cid) => cid.isRunning());
  }

  /**
   * Stops the given child after it finishes its current message.
   *
   * @throws The {@link ErrDead} sentinel when this actor is not running.
   * @throws The {@link ErrUndefinedActor} sentinel when `cid` is the
   * NoSender sentinel.
   * @throws {@link ActorNotFoundError} when `cid` is not a child of this
   * actor, or is neither running nor suspended.
   */
  async stop(cid: PID): Promise<void> {
    if (!this.isRunning()) {
      throw ErrDead;
    }

    if (cid.name() === noSenderName) {
      throw ErrUndefinedActor;
    }

    if (!cid.isRunning() && !cid.isSuspended()) {
      throw new ActorNotFoundError(cid.path().toString());
    }

    const known = this._tree?.child(this, cid.name());
    if (known === undefined || !known.equals(cid)) {
      throw new ActorNotFoundError(cid.path().toString());
    }

    await cid.shutdown();
  }

  /**
   * Registers this actor to receive a {@link Terminated} message when
   * `cid` stops. Watching an actor that is not running is a no-op:
   * death watch observes a future stop, so watch actors while they are
   * alive.
   */
  watch(cid: PID): void {
    const route = cid._route;
    if (route !== null) {
      route.watch(cid._path, this);
      return;
    }

    if (!cid.isRunning()) {
      return;
    }

    cid.tree().addWatcher(cid, this);
  }

  /** Cancels a {@link watch} registration on `cid`. */
  unWatch(cid: PID): void {
    const route = cid._route;
    if (route !== null) {
      route.unwatch(cid._path, this);
      return;
    }

    cid._tree?.removeWatcher(cid, this);
  }

  /**
   * Registers a listener invoked once this actor's shutdown has fully
   * completed: children stopped, `postStop` run, watchers notified. A
   * restart is not a stop and never fires it. Runtime plumbing for the
   * bookkeeping that must observe an actor's end without being an
   * actor, such as freeing a placed top-level name; developers watch
   * actors with {@link watch} instead.
   *
   * @internal
   */
  onStopped(listener: () => void): void {
    if (this._stopListeners === null) {
      this._stopListeners = [];
    }

    this._stopListeners.push(listener);
  }

  /**
   * Replaces the actor's current behavior with the given one. Runtime
   * plumbing for `ReceiveContext.become`: switching behavior belongs to
   * the actor itself, so call it from a receive context, never on a PID
   * held from outside.
   *
   * @internal
   */
  setBehavior(behavior: Behavior): void {
    const stack = this.behaviorStack();
    stack.reset();
    stack.push(behavior);
  }

  /**
   * Pushes a behavior on top of the current one. Runtime plumbing for
   * `ReceiveContext.becomeStacked`.
   *
   * @internal
   */
  setBehaviorStacked(behavior: Behavior): void {
    this.behaviorStack().push(behavior);
  }

  /**
   * Reverts the actor to its default `receive` behavior. Runtime
   * plumbing for `ReceiveContext.unBecome`.
   *
   * @internal
   */
  resetBehavior(): void {
    this._behaviorStack?.reset();
  }

  /**
   * Removes the topmost stacked behavior. Runtime plumbing for
   * `ReceiveContext.unBecomeStacked`.
   *
   * @internal
   */
  unsetBehaviorStacked(): void {
    this._behaviorStack?.pop();
  }

  /**
   * Adds the given receive context to the actor's stash buffer, creating
   * the buffer on first use. Runtime plumbing for `ReceiveContext.stash`:
   * the stash belongs to the actor itself.
   *
   * @internal
   */
  stash(ctx: ReceiveContext): Error | null {
    this._stash ??= new Stash();
    return this._stash.stash(ctx);
  }

  /**
   * Re-delivers the oldest stashed message to the mailbox. Runtime
   * plumbing for `ReceiveContext.unstash`.
   *
   * @returns {@link ErrStashBufferEmpty} when nothing is stashed.
   *
   * @internal
   */
  unstash(): Error | null {
    const ctx = this._stash?.unstash();
    if (ctx === undefined) {
      return ErrStashBufferEmpty;
    }

    return this.doReceive(ctx);
  }

  /**
   * Re-delivers all stashed messages to the mailbox in their original
   * arrival order, oldest first. The batch joins the mailbox tail:
   * messages already queued run before it, messages arriving afterwards
   * run behind it. Runtime plumbing for `ReceiveContext.unstashAll`.
   *
   * @internal
   */
  unstashAll(): Error | null {
    if (this._stash === null) {
      return null;
    }

    for (let ctx = this._stash.unstash(); ctx !== undefined; ctx = this._stash.unstash()) {
      const err = this.doReceive(ctx);
      if (err !== null) {
        return err;
      }
    }

    return null;
  }

  /** Enqueues a receive context and schedules the receive loop. */
  private doReceive(ctx: ReceiveContext): Error | null {
    if (!this.isRunning()) {
      this.deadletter(ctx.sender, ctx.message, ErrDead);
      return ErrDead;
    }

    const err = this._mailbox.enqueue(ctx);
    if (err !== null) {
      this.deadletter(ctx.sender, ctx.message, err);
      return err;
    }

    this.schedule();
    return null;
  }

  /**
   * Publishes a {@link Deadletter} for a message this actor could not
   * receive. Runtime announcements and internal commands are skipped:
   * they are the runtime talking to itself, and their loss is already
   * handled where they are sent.
   */
  private deadletter(sender: PID | undefined, message: unknown, err: Error): void {
    this._actorSystem.toDeadletter(sender?.path().toString(), this._path.toString(), message, err);
  }

  /**
   * Returns an {@link ActorRef} addressing this actor, pinned to its
   * current incarnation: once this actor stops, the ref is dead even
   * when a new actor reuses the name. Runtime plumbing for the
   * transport layer; developers interact with actors through PIDs.
   *
   * @internal
   */
  ref(): ActorRef {
    return new ActorRef(this._actorSystem, this._path, this._route);
  }

  /**
   * Registers `watcher` to receive a {@link Terminated} message when
   * this actor stops, without consulting the watcher's own routing:
   * the receiving-side half of a cross-isolate watch, where the
   * watcher is a routed handle and the registration must land in this
   * local actor's tree. Watching a non-running actor is a no-op,
   * mirroring {@link watch}.
   *
   * @internal
   */
  addWatcher(watcher: PID): void {
    if (!this.isRunning()) {
      return;
    }

    this.tree().addWatcher(this, watcher);
  }

  /** Cancels an {@link addWatcher} registration; unknown watchers and
   * stopped actors are a no-op.
   *
   * @internal
   */
  removeWatcher(watcher: PID): void {
    this._tree?.removeWatcher(this, watcher);
  }

  /**
   * Marks this PID as the handle of an actor owned by another isolate:
   * every send, watch, and request routes through the given transport
   * instead of this isolate's tree. Runtime plumbing for the routed
   * handle factory; never call it on a local actor's PID.
   *
   * @internal
   */
  setRoute(route: IsolateRoute): void {
    this._route = route;
  }

  /**
   * Reports whether this PID is the routed handle of an actor owned by
   * another isolate. A routed actor's liveness is not synchronously
   * knowable on this isolate, so local state checks do not apply to it.
   * Runtime plumbing.
   *
   * @internal
   */
  isRouted(): boolean {
    return this._route !== null;
  }

  /**
   * Routes the given delivery to dead letters as unhandled. Runtime
   * plumbing for `ReceiveContext.unhandled`.
   *
   * @internal
   */
  unhandled(ctx: ReceiveContext): void {
    this.deadletter(ctx.sender, ctx.message, ErrUnhandled);
  }

  /**
   * Arms the receive loop unless it is already running. The first batch is
   * scheduled on the microtask queue for low latency; subsequent batches
   * yield through `setImmediate` (see {@link THROUGHPUT}).
   */
  private schedule(): void {
    if (this._processing) {
      return;
    }

    this._processing = true;
    ready.push(this);
    arm(false);
  }

  /**
   * Runs the receive loop. Shared-scheduler entry; not part of the
   * developer API.
   *
   * @internal
   */
  dispatch(): void {
    this.processLoop(0);
  }

  /**
   * Drains the mailbox one message at a time through the current behavior,
   * yielding to the event loop after {@link THROUGHPUT} messages. The
   * loop stays synchronous while behaviors do, so a drain of synchronous
   * messages allocates nothing; when a behavior returns a promise the
   * loop parks on it and resumes from its settlement.
   */
  private processLoop(processed: number): void {
    for (;;) {
      const ctx = this._mailbox.dequeue();
      if (ctx === undefined) {
        // The drain is complete: this is the moment idleness starts, so
        // the clock is read once here, only for actors whose passivation
        // consults it, instead of once per message.
        if (this._trackActivity) {
          this._latestActivity = Date.now();
        }

        this._processing = false;
        this.notifyIdle();
        return;
      }

      // The runtime consumes its own commands here; a behavior never
      // sees them. A RequestReply completes an in-flight request, a pill
      // begins the graceful stop (the backlog the mailbox had already
      // accepted is still drained while new sends are rejected), and a
      // Panicking message is a failing child's supervision request.
      if (ctx.message instanceof RuntimeCommand) {
        this._processedCount++;
        const msg = ctx.message;

        if (msg instanceof RequestReply) {
          this.completeRequest(msg);
        } else if (msg instanceof PoisonPill) {
          void this.shutdown();
        } else {
          this.handlePanicking(msg as Panicking);
        }

        continue;
      }

      // While a stash-mode request is in flight, user messages wait in
      // the stash and are re-delivered in order once the last one
      // completes; only runtime commands flow. Stashing counts toward
      // the turn quota so a flooded paused actor still yields.
      const reentrancy = this._reentrancy;
      if (reentrancy !== null && reentrancy.paused !== 0) {
        this.stash(ctx);

        processed++;
        if (processed >= THROUGHPUT) {
          ready.push(this);
          arm(true);
          return;
        }

        continue;
      }

      let result: unknown;

      try {
        const behavior = this._behaviorStack?.peek();
        result = behavior === undefined ? this._receive.call(this._actor, ctx) : behavior(ctx);
      } catch (err) {
        this.handleFault(err, ctx);
        result = undefined;
      }

      // Park the loop on the pending behavior; the parking closures live
      // in a separate method so this loop body captures nothing and a
      // synchronous drain allocates nothing per message.
      if (result instanceof Promise) {
        this.parkOn(result, ctx, processed);
        return;
      }

      if (!this.postReceive()) {
        return;
      }

      ctx.recycle();

      processed++;
      if (processed >= THROUGHPUT) {
        ready.push(this);
        arm(true);
        return;
      }
    }
  }

  /** Parks the receive loop on a pending behavior and resumes it from
   * the settlement, routing a rejection through fault handling. */
  private parkOn(pending: Promise<unknown>, ctx: ReceiveContext, processed: number): void {
    pending.then(
      () => this.resumeLoop(ctx, processed),
      (err: unknown) => {
        this.handleFault(err, ctx);
        this.resumeLoop(ctx, processed);
      },
    );
  }

  /** Continues the receive loop after an asynchronous behavior settles. */
  private resumeLoop(ctx: ReceiveContext, processed: number): void {
    if (!this.postReceive()) {
      return;
    }

    ctx.recycle();

    const next = processed + 1;
    if (next >= THROUGHPUT) {
      ready.push(this);
      arm(true);
      return;
    }

    this.processLoop(next);
  }

  /**
   * Bookkeeping after a behavior settles: counts the message, halts the
   * loop when a fault suspended the actor, and applies a count-based
   * passivation budget. Returns whether the loop may continue.
   */
  private postReceive(): boolean {
    this._processedCount++;

    // A fault may have suspended the actor: halt the loop until it is
    // restarted or reinstated, leaving queued messages in the mailbox.
    if (this._suspended) {
      this._latestActivity = Date.now();
      this._processing = false;
      this.notifyIdle();
      return false;
    }

    if (this._maxMessages !== 0 && this._processedCount >= this._maxMessages) {
      void this.passivate();
    }

    return true;
  }

  /**
   * Reacts to a failure thrown by the behavior: resolves the directive
   * from the actor's supervisor, resumes in place, or suspends the actor
   * and hands the decision to its parent.
   */
  private handleFault(err: unknown, ctx: ReceiveContext): void {
    const directive = this._supervisor.directive(err);

    // The failure is transient: keep the actor's state and move on.
    if (directive === ResumeDirective) {
      return;
    }

    this.suspend(err instanceof Error ? err.message : String(err));

    // Without a directive or a parent there is nobody to decide; the
    // actor stays suspended until it is restarted or reinstated.
    if (directive === undefined || this._parent === null) {
      return;
    }

    const parent = this._parent;
    parent.doReceive(
      createReceiveContext(
        new Panicking(
          this,
          err,
          ctx.message,
          directive,
          this._supervisor.strategy(),
          this._supervisor,
          Date.now(),
        ),
        parent,
        this,
      ),
    );
  }

  /** Applies a failing child's supervision directive. */
  private handlePanicking(msg: Panicking): void {
    const includeSiblings = msg.strategy === OneForAllStrategy;

    if (msg.directive === StopDirective) {
      void this.stopGroup(msg.cid, includeSiblings);
      return;
    }

    if (msg.directive === RestartDirective) {
      this.handleRestartDirective(msg.cid, msg.supervisor, includeSiblings);
      return;
    }

    // EscalateDirective: surface the failure to this actor's own behavior
    // as a PanicSignal; the failing child stays suspended.
    const reason = msg.error instanceof Error ? msg.error : new Error(String(msg.error));
    this.doReceive(createReceiveContext(new PanicSignal(reason), this, msg.cid));
  }

  /** Returns the actors a directive applies to: the failing child alone,
   * or the child and its siblings under a one-for-all strategy. */
  private supervisionGroup(cid: PID, includeSiblings: boolean): PID[] {
    return includeSiblings ? [cid, ...this.tree().siblings(cid)] : [cid];
  }

  /** Stops a failing child, and its siblings under a one-for-all
   * strategy. */
  private async stopGroup(cid: PID, includeSiblings: boolean): Promise<void> {
    const group = this.supervisionGroup(cid, includeSiblings);
    await Promise.all(group.map((member) => member.shutdown()));
  }

  /**
   * Restarts a failing child, and its siblings under a one-for-all
   * strategy, honoring the supervisor's restart budget and backoff. When
   * the budget is exhausted the whole group is left suspended.
   */
  private handleRestartDirective(cid: PID, supervisor: Supervisor, includeSiblings: boolean): void {
    const group = this.supervisionGroup(cid, includeSiblings);

    // The reset window is backoff's resetAfter when configured, the retry
    // timeout otherwise.
    const resetAfter = supervisor.backoffResetAfter();
    const window = resetAfter > 0 ? resetAfter : supervisor.timeout();

    // Bump every group member so a one-for-all group exhausts its budget
    // even when the faults alternate between siblings; the faulty child's
    // count drives the decisions below.
    let faults = 0;

    for (const member of group) {
      const count = member.recordFault(window);

      if (member === cid) {
        faults = count;
      }
    }

    // The budget only applies within a positive window: without one the
    // counter never resets, and a handful of faults spread over days
    // would eventually suspend an otherwise healthy actor.
    const maxRetries = supervisor.maxRetries();
    if (maxRetries > 0 && window > 0 && faults > maxRetries) {
      for (const member of group) {
        if (member !== cid && member.isRunning()) {
          member.suspend("restart budget exhausted");
        }
      }

      return;
    }

    const delay = backoffDelay(faults, supervisor.initialDelay(), supervisor.maxDelay());

    for (const member of group) {
      void this.restartChild(member, supervisor, delay);
    }
  }

  /**
   * Restarts one member of a supervised group after waiting out the
   * backoff delay, retrying up to the supervisor's budget when the
   * restart itself keeps failing.
   */
  private async restartChild(cid: PID, supervisor: Supervisor, delay: number): Promise<void> {
    if (delay > 0) {
      await pause(delay);

      // A long delay can outlive the supervising parent; restarting then
      // would resurrect the child into a torn-down hierarchy.
      if (!this.isRunning()) {
        return;
      }
    }

    const bounded = supervisor.maxRetries() > 0 && supervisor.timeout() > 0;
    const attempts = bounded ? supervisor.maxRetries() : 1;
    const pace = supervisor.initialDelay() > 0 ? supervisor.initialDelay() : supervisor.timeout();

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        await cid.restart();
        return;
      } catch (err) {
        if (attempt === attempts) {
          this._actorSystem.logger().error("actor failed to restart", {
            actor: cid.path().toString(),
            error: err,
          });
          return;
        }

        await pause(pace);
      }
    }
  }

  /**
   * Updates the actor's consecutive fault counter and returns the new
   * count. A previous fault older than the reset window resets the
   * counter first; a non-positive window means it never resets.
   */
  private recordFault(window: number): number {
    const now = Date.now();

    if (window > 0 && this._lastFaultAt > 0 && now - this._lastFaultAt > window) {
      this._consecutiveFaults = 0;
    }

    this._lastFaultAt = now;
    this._consecutiveFaults++;
    return this._consecutiveFaults;
  }

  /** Puts the actor in suspension: it holds its state but accepts and
   * processes no message. The reason is carried on the published
   * {@link ActorSuspended} event. */
  private suspend(reason: string): void {
    this._suspended = true;

    if (this.canEmit()) {
      this._actorSystem.publishEvent(new ActorSuspended(this._path.toString(), reason, Date.now()));
    }
  }

  /** Lifts a suspension and resumes processing of the queued messages. */
  private doReinstate(): void {
    if (!this._suspended) {
      return;
    }

    this._suspended = false;
    this._latestActivity = Date.now();

    if (this.canEmit()) {
      this._actorSystem.publishEvent(new ActorReinstated(this._path.toString(), Date.now()));
    }

    if (!this.mailboxEmpty()) {
      this.schedule();
    }
  }

  /** Reports whether this actor should publish lifecycle events: it must
   * be a user actor, not one of the runtime's own, and the event stream
   * must have a subscriber. The subscriber check keeps an idle stream from
   * paying to build events nobody reads. */
  private canEmit(): boolean {
    return !isSystemName(this._path.name()) && this._actorSystem.hasEventSubscribers();
  }

  /** Parks the caller until the receive loop reports idle. */
  private awaitIdle(): Promise<void> {
    return new Promise<void>((resolve) => {
      this._idleResolvers ??= [];
      this._idleResolvers.push(resolve);
    });
  }

  /** Wakes the parked shutdown or restart once the receive loop exits. */
  private notifyIdle(): void {
    const resolvers = this._idleResolvers;
    if (resolvers === null) {
      return;
    }

    this._idleResolvers = null;

    for (const resolve of resolvers) {
      resolve();
    }
  }

  /**
   * Runs the actor's `preStart` hook.
   *
   * @throws An {@link ActorInitializationError} carrying the hook's
   * failure as its `cause`.
   */
  private async runPreStart(): Promise<void> {
    try {
      const result: unknown = this._actor.preStart(this.lifecycleContext());
      if (result instanceof Promise) {
        await result;
      }
    } catch (err) {
      throw new ActorInitializationError(this.name(), err);
    }
  }

  /** Runs the actor's `postStop` hook; a failure is logged, never
   * thrown. */
  private async runPostStop(): Promise<void> {
    try {
      const result: unknown = this._actor.postStop(this.lifecycleContext());
      if (result instanceof Promise) {
        await result;
      }
    } catch (err) {
      this._actorSystem.logger().error("postStop failed", {
        actor: this._path.toString(),
        error: err,
      });
    }
  }

  private behaviorStack(): BehaviorStack {
    this._behaviorStack ??= new BehaviorStack();
    return this._behaviorStack;
  }

  private lifecycleContext(): Context {
    return new Context(this._actorSystem, this.name());
  }

  private mailboxEmpty(): boolean {
    return this._mailbox.isEmpty();
  }

  /** Returns the hierarchy's shared tree, creating it for a top-level
   * actor on first use. */
  private tree(): PidTree {
    this._tree ??= new PidTree();
    return this._tree;
  }

  /**
   * Turns the drain-end activity clock on or off. Runtime plumbing for
   * the passivation scheduler: only a time-based schedule consults
   * {@link latestActivity}, so the clock is read only while one is
   * active. Enabling counts as activity, so a schedule attached to a
   * long-running actor starts from a fresh timestamp.
   *
   * @internal
   */
  setActivityTracking(enabled: boolean): void {
    this._trackActivity = enabled;

    if (enabled) {
      this._latestActivity = Date.now();
    }
  }

  /** @internal */
  isInTree(): boolean {
    return this._inTree;
  }

  /** @internal */
  setInTree(value: boolean): void {
    this._inTree = value;
  }
}
