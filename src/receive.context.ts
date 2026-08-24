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
import type { ActorSystem } from "./actor.system";
import type { Behavior } from "./behavior.stack";
import { ErrRequestTimeout } from "./errors";
import type { PID } from "./pid";
import type { PipeTask } from "./pipe";
import type { PipeOptions } from "./pipe.options";
import type { RequestCall, RequestOptions } from "./reentrancy";
import type { ScheduleOptions } from "./schedule.options";
import type { SpawnOptions } from "./spawn.options";

/**
 * ReceiveContext wraps a message delivered to an actor and gives the
 * receiving behavior the tools to act on it: reading the message,
 * sending, asking, spawning children, switching behavior, and stashing.
 *
 * A context created by the runtime is always attached to the receiving
 * actor via {@link self}. Actor-facing methods require that attachment
 * and throw when called on a detached context.
 */
export class ReceiveContext {
  private _message: unknown;
  private _self: PID | undefined;
  private _sender: PID | undefined;

  protected constructor(message?: unknown, self?: PID, sender?: PID) {
    this._message = message;
    this._self = self;
    this._sender = sender;
  }

  /** Re-attaches a recycled context to a fresh delivery. */
  protected attach(message: unknown, self: PID, sender: PID): void {
    this._message = message;
    this._self = self;
    this._sender = sender;
  }

  /**
   * Re-attaches this delivery to a new receiving actor, keeping the
   * message, the original sender, and any reply channel intact: the
   * seam by which a router hands a live delivery to a routee, so an
   * ask answered by the routee still settles the original promise.
   * Runtime plumbing for {@link PID.redeliver}.
   *
   * @internal
   */
  retarget(self: PID): void {
    this._self = self;
  }

  /**
   * Returns the context to its pool once the receive loop is done with
   * it. A no-op on a plain delivery; ask deliveries recycle themselves
   * when their reply has been settled. Runtime plumbing called after the
   * behavior has fully finished with the context.
   *
   * @internal
   */
  recycle(): void {}

  /**
   * Constructs a receive context. Runtime plumbing for mailbox delivery
   * and tests; developers receive contexts from `receive`, they do not
   * construct them.
   *
   * @internal
   */
  static create(message?: unknown, self?: PID, sender?: PID): ReceiveContext {
    return new ReceiveContext(message, self, sender);
  }

  /**
   * The message being delivered.
   *
   * The type is intentionally `unknown`: define messages as classes and
   * narrow with `instanceof`, the TypeScript analog of a type switch.
   * Each branch gets the fully typed message.
   *
   * ```ts
   * const msg = ctx.message;
   *
   * if (msg instanceof Greet) {
   *   console.log(`Hello ${msg.name}`);
   * } else if (msg instanceof Stop) {
   *   // typed as Stop here
   * }
   * ```
   */
  get message(): unknown {
    return this._message;
  }

  /** The PID of the actor receiving the message, or `undefined` when the
   * context is not attached to an actor. */
  get self(): PID | undefined {
    return this._self;
  }

  /**
   * The PID of the actor that sent the message. A message sent from
   * outside an actor carries the system's NoSender PID, comparable with
   * identity: `ctx.sender === system.noSender()`. It is `undefined` only
   * on a detached context created without a sender.
   */
  get sender(): PID | undefined {
    return this._sender;
  }

  /** The actor system hosting the receiving actor. */
  actorSystem(): ActorSystem {
    return this.pid().actorSystem();
  }

  /**
   * Replaces the actor's current behavior with the given one. The new
   * behavior handles every subsequent message until it is changed again or
   * reverted with {@link unBecome}.
   */
  become(behavior: Behavior): void {
    this.pid().setBehavior(behavior);
  }

  /**
   * Pushes the given behavior on top of the current one, so the previous
   * behavior can be restored later with {@link unBecomeStacked}.
   */
  becomeStacked(behavior: Behavior): void {
    this.pid().setBehaviorStacked(behavior);
  }

  /** Reverts the actor to its default `receive` behavior, discarding every
   * behavior installed with {@link become} or {@link becomeStacked}. */
  unBecome(): void {
    this.pid().resetBehavior();
  }

  /** Removes the topmost stacked behavior, restoring the one beneath it. */
  unBecomeStacked(): void {
    this.pid().unsetBehaviorStacked();
  }

  /**
   * Adds the current message to the actor's stash buffer, to be
   * re-delivered later by {@link unstash} or {@link unstashAll}.
   *
   * @throws The `ErrMailboxDisposed` sentinel when the stash buffer has
   * been disposed.
   */
  stash(): void {
    this.check(this.pid().stash(this));
  }

  /**
   * Re-delivers the oldest stashed message to the actor's mailbox.
   *
   * @throws The `ErrStashBufferEmpty` sentinel when nothing is stashed.
   * @throws The mailbox rejection error when re-delivery fails, for
   * example the `ErrDead` sentinel while the actor is stopping.
   */
  unstash(): void {
    this.check(this.pid().unstash());
  }

  /**
   * Re-delivers all stashed messages to the actor's mailbox in their
   * original arrival order, oldest first. The batch joins the mailbox
   * tail: messages already queued run before it, and messages arriving
   * afterwards run behind it.
   *
   * @throws The mailbox rejection error when a re-delivery fails, for
   * example the `ErrDead` sentinel while the actor is stopping.
   */
  unstashAll(): void {
    this.check(this.pid().unstashAll());
  }

  /**
   * Sends a message asynchronously to `to`. This actor is recorded as
   * the sender.
   *
   * @throws The error returned by {@link PID#tell} when delivery fails,
   * for example {@link ErrDead} when the target is not running.
   */
  tell(to: PID, message: unknown): void {
    this.check(this.pid().tell(to, message));
  }

  /**
   * Sends a message to `to` and waits for {@link response}. The call
   * parks until the reply arrives or `timeout` milliseconds elapse.
   *
   * @throws The {@link ErrDead} sentinel when the target is not running.
   * @throws The {@link ErrInvalidTimeout} sentinel when `timeout` is not
   * a positive duration.
   * @throws The {@link ErrRequestTimeout} sentinel when no reply arrives
   * in time.
   * @throws The mailbox rejection error when delivery fails.
   */
  ask(to: PID, message: unknown, timeout: number): Promise<unknown> {
    return this.pid().ask(to, message, timeout);
  }

  /**
   * Sends a message to `to` and returns immediately with a
   * {@link RequestCall} handle, letting this actor keep processing
   * according to its {@link ReentrancyMode} while the reply is in
   * flight. Register the continuation with `onReply`; it runs on this
   * actor's own turn, serialized with its message processing.
   *
   * Unlike `ask`, a request never parks the receive loop, so an actor
   * in `allowAll` mode can safely request itself or sit in a call cycle
   * (A requests B, B requests A).
   *
   * The actor must be spawned with a reentrancy configuration; a failure
   * to admit or deliver the request returns an already completed handle
   * whose continuation runs with the error: {@link ErrReentrancyDisabled}
   * without one or with mode `off`, {@link ErrInvalidReentrancyMode} for
   * an unknown mode, {@link ErrReentrancyInFlightLimit} past the
   * in-flight cap, {@link ErrDead} when the target is not running, and
   * the mailbox rejection error when delivery fails.
   */
  request(to: PID, message: unknown, options?: RequestOptions): RequestCall {
    return this.pid().request(to, message, options);
  }

  /**
   * Runs `task` off this actor's message processing and delivers its
   * resolution value to `to` as an ordinary message, with this actor
   * recorded as the sender. The call returns immediately; the actor
   * keeps processing messages while the task runs, and delivery happens
   * on the target's own turn, the same guarantee `onReply` gives.
   *
   * The task is a promise, or a thunk returning one; the thunk receives
   * an `AbortSignal` that fires when the pipe's timeout expires. A pipe
   * never throws and a failing pipe delivers nothing: a dead target, a
   * rejected task, and an expired timeout all route to dead letters
   * carrying the failing reason. To receive a failure as a message
   * instead, map the rejection before piping:
   * `task.catch((err) => new LoadFailed(err))`.
   *
   * Stopping this actor does not cancel the task: the promise is
   * already running, and its result is still delivered when it settles.
   *
   * ```ts
   * if (ctx.message instanceof Load) {
   *   ctx.pipeTo(ctx.self as PID, fetchOrder(ctx.message.id));
   * }
   *
   * if (ctx.message instanceof Order) {
   *   // the fetched result, delivered like any other message
   * }
   * ```
   */
  pipeTo(to: PID, task: PipeTask, options?: PipeOptions): void {
    this.pid().pipeTo(to, task, options);
  }

  /**
   * Pipes like {@link pipeTo}, addressing the target by name instead of
   * by handle: when the task settles, the name is resolved among the
   * running top-level actors and the result is delivered to whoever
   * holds it then. When no running top-level actor holds the name, the
   * result becomes a dead letter carrying an `ActorNotFoundError`.
   */
  pipeToName(actorName: string, task: PipeTask, options?: PipeOptions): void {
    this.pid().pipeToName(actorName, task, options);
  }

  /**
   * Delivers `message` to `to` repeatedly, every `interval`
   * milliseconds, the first delivery one interval from now. The
   * schedule is owned by this actor: when this actor fully stops, the
   * schedule is cancelled with it, so no timer cleanup is needed in
   * `postStop`. A restart is not a stop and keeps the schedule running.
   * This actor is recorded as the sender unless `opts.sender` overrides
   * it.
   *
   * Give the schedule a reference through `opts.reference` to cancel,
   * pause, or resume it later; references live in one flat, system-wide
   * namespace.
   *
   * @returns A promise that settles once the schedule is registered. It
   * rejects with the {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running, the {@link ErrInvalidInterval} sentinel when
   * `interval` is not a positive number, the
   * {@link ErrScheduleAlreadyExists} sentinel when the reference is
   * already held by another schedule, and the {@link ErrDead} sentinel
   * when the target has already stopped.
   */
  schedule(message: unknown, to: PID, interval: number, opts?: ScheduleOptions): Promise<void> {
    const self: PID = this.pid();
    return self.actorSystem().scheduleFrom(self, message, to, interval, opts);
  }

  /**
   * Delivers `message` to `to` once, `delay` milliseconds from now. The
   * schedule is owned by this actor: when this actor fully stops before
   * the delay elapses, nothing fires. This actor is recorded as the
   * sender unless `opts.sender` overrides it, so a message scheduled to
   * `ctx.self` arrives as a tick from the actor to itself.
   *
   * @returns A promise that settles once the schedule is registered. It
   * rejects with the {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running, the {@link ErrInvalidInterval} sentinel when
   * `delay` is not a positive number, the
   * {@link ErrScheduleAlreadyExists} sentinel when the reference is
   * already held by another schedule, and the {@link ErrDead} sentinel
   * when the target has already stopped.
   */
  scheduleOnce(message: unknown, to: PID, delay: number, opts?: ScheduleOptions): Promise<void> {
    const self: PID = this.pid();
    return self.actorSystem().scheduleOnceFrom(self, message, to, delay, opts);
  }

  /**
   * Cancels the schedule held under `reference`, whichever actor or
   * code created it: references live in one flat, system-wide
   * namespace.
   *
   * @returns A promise that settles once the schedule is cancelled. It
   * rejects with the {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running and the {@link ErrScheduleNotFound} sentinel
   * when no schedule holds the reference.
   */
  cancelSchedule(reference: string): Promise<void> {
    return this.pid().actorSystem().cancelSchedule(reference);
  }

  /**
   * Pauses the schedule held under `reference`; a paused schedule fires
   * nothing until it is resumed. Pausing a paused schedule is a no-op.
   *
   * @returns A promise that settles once the schedule is paused. It
   * rejects with the {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running and the {@link ErrScheduleNotFound} sentinel
   * when no schedule holds the reference.
   */
  pauseSchedule(reference: string): Promise<void> {
    return this.pid().actorSystem().pauseSchedule(reference);
  }

  /**
   * Resumes the schedule held under `reference`. Resuming a schedule
   * that is not paused is a no-op.
   *
   * @returns A promise that settles once the schedule is resumed. It
   * rejects with the {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running and the {@link ErrScheduleNotFound} sentinel
   * when no schedule holds the reference.
   */
  resumeSchedule(reference: string): Promise<void> {
    return this.pid().actorSystem().resumeSchedule(reference);
  }

  /**
   * Completes an {@link PID#ask} with `value`. The first call
   * wins; later calls are ignored. A no-op when this message was not
   * delivered by ask: a plain delivery carries no reply channel, so the
   * fire-and-forget path allocates none.
   *
   * Respond before the behavior finishes: contexts are managed by the
   * runtime and may be reused once `receive` has fully settled, so do
   * not retain one beyond the current message.
   */
  response(_value: unknown): void {}

  /**
   * Creates and starts a child of the receiving actor.
   *
   * @returns The child's PID.
   */
  spawn(name: string, actor: Actor, options?: SpawnOptions): Promise<PID> {
    return this.pid().spawnChild(name, actor, options);
  }

  /** Returns this actor's running children. */
  children(): PID[] {
    return this.pid().children();
  }

  /**
   * Returns the running child registered under `name`.
   *
   * @throws {@link ErrDead} when this actor is not running.
   * @throws {@link ActorNotFoundError} when no running child holds the name.
   */
  child(name: string): PID {
    return this.pid().child(name);
  }

  /**
   * Stops the given child after it finishes its current message.
   *
   * @throws {@link ErrDead} when this actor is not running.
   * @throws {@link ErrUndefinedActor} when `child` is the NoSender sentinel.
   * @throws {@link ActorNotFoundError} when `child` is not a live child of
   * this actor.
   */
  stop(child: PID): Promise<void> {
    return this.pid().stop(child);
  }

  /**
   * Begins a graceful stop of the receiving actor. Already queued
   * messages are still processed; children are shut down with it.
   *
   * Fire-and-forget: do not await this from `receive`. Awaiting the
   * actor's own shutdown from inside its receive loop would never
   * complete, because shutdown waits for the loop to go idle.
   */
  shutdown(): void {
    void this.pid().shutdown();
  }

  /** Registers this actor to receive {@link Terminated} when `cid` stops. */
  watch(cid: PID): void {
    this.pid().watch(cid);
  }

  /** Cancels a {@link watch} registration on `cid`. */
  unWatch(cid: PID): void {
    this.pid().unWatch(cid);
  }

  /**
   * Resolves a top-level actor by name on the remote node at
   * `host:port`, with the contract of `ActorSystem.remoteLookup`: the
   * promise resolves with the remote actor's PID, or undefined when no
   * running top-level actor holds the name there. Remote spawn is a
   * system-level operation: reach it through the system,
   * `actorSystem().remoteSpawn`.
   */
  remoteLookup(host: string, port: number, name: string): Promise<PID | undefined> {
    return this.actorSystem().remoteLookup(host, port, name);
  }

  /**
   * Restarts the named top-level actor on the remote node at
   * `host:port`, with the contract of `ActorSystem.remoteReSpawn`: the
   * promise resolves with the restarted actor's PID.
   */
  remoteReSpawn(host: string, port: number, name: string): Promise<PID> {
    return this.actorSystem().remoteReSpawn(host, port, name);
  }

  /**
   * Stops the named top-level actor on the remote node at `host:port`
   * gracefully, with the contract of `ActorSystem.remoteStop`.
   */
  remoteStop(host: string, port: number, name: string): Promise<void> {
    return this.actorSystem().remoteStop(host, port, name);
  }

  /**
   * Marks the current message as unhandled: it is routed to the
   * system's dead letters carrying `ErrUnhandled` as the reason, and
   * processing continues normally. Prefer this over throwing when
   * unknown messages are expected; a throw engages supervision.
   */
  unhandled(): void {
    this.pid().unhandled(this);
  }

  /**
   * Forwards the current message to `to`, preserving the original sender.
   * The receiving behavior sees `ctx.sender` as whoever sent the message
   * here, not this actor.
   *
   * @throws The error returned by {@link PID#tell} when delivery fails.
   */
  forward(to: PID): void {
    const sender = this._sender;
    if (sender === undefined) {
      throw new Error("the receive context is not attached to an actor");
    }

    this.check(sender.tell(to, this._message));
  }

  private pid(): PID {
    if (this._self === undefined) {
      throw new Error("the receive context is not attached to an actor");
    }

    return this._self;
  }

  private check(err: Error | null): void {
    if (err !== null) {
      throw err;
    }
  }
}

/** One generation of pending asks: an intrusive doubly-linked list. */
interface AskGeneration {
  head: AskContext | null;
  tail: AskContext | null;
}

/**
 * The pending asks that share one timeout duration, in two generations.
 * A fresh ask joins the young generation; when the shared timer fires,
 * the old generation, pending for at least one full period by then,
 * expires, and the young generation becomes the old one. An unanswered
 * ask is therefore rejected between one and two periods after it was
 * sent, and sending never reads the clock: one unreferenced timer per
 * duration replaces a timer object and a timestamp per ask.
 */
interface AskList {
  young: AskGeneration;
  old: AskGeneration;
  timer: NodeJS.Timeout | null;
  readonly duration: number;
}

/** One pending-ask list per distinct timeout duration. Applications use
 * a handful of fixed timeouts, so the map stays tiny. */
const askLists = new Map<number, AskList>();

/** The most recently used duration and its list. Applications typically
 * ask with one fixed timeout, so this cache spares the map lookup. */
let lastDuration = -1;
let lastList: AskList | null = null;

/** Settled ask contexts ready for reuse, so a steady stream of asks
 * stops allocating contexts. Bounded so a burst cannot pin memory. */
const askPool: AskContext[] = [];

/** Upper bound on pooled ask contexts. Sized for steady request flows;
 * a deep pool would feed aged objects into large bursts, whose stores
 * pay generational write barriers that fresh allocations avoid. */
const ASK_POOL_LIMIT = 256;

/** Expires the list's old generation, promotes the young one, and keeps
 * the timer running while asks remain pending. */
function sweepAsks(list: AskList): void {
  list.timer = null;

  const dying = list.old;
  for (let ctx = dying.head; ctx !== null; ctx = dying.head) {
    ctx.unlink();
    ctx.expire();
  }

  // Swap the generation objects rather than moving entries: each pending
  // context keeps pointing at the generation that holds it.
  list.old = list.young;
  list.young = dying;

  if (list.old.head !== null) {
    armAskTimer(list);
  }
}

/** Arms the list's shared timer for one full period. */
function armAskTimer(list: AskList): void {
  const timer = setTimeout(sweepAsks, list.duration, list);
  timer.unref();
  list.timer = timer;
}

/**
 * The context of one ask delivery: a receive context extended with the
 * reply channel and its place in the pending-ask list. Keeping this
 * state on a subclass means a plain tell never carries or allocates any
 * of it. The class never leaves this module.
 *
 * @internal
 */
class AskContext extends ReceiveContext {
  /** Completes the ask; null once the ask has been settled, so the first
   * response wins and a response after a timeout is ignored. */
  reply: ((value: unknown) => void) | null;

  /** Rejects the ask on timeout; nulled together with {@link reply}. */
  fail: ((reason: Error) => void) | null;

  /** Intrusive links into the pending generation of this ask's
   * duration. */
  gen: AskGeneration | null = null;
  prev: AskContext | null = null;
  next: AskContext | null = null;

  constructor(
    message: unknown,
    self: PID,
    sender: PID,
    reply: (value: unknown) => void,
    fail: (reason: Error) => void,
  ) {
    super(message, self, sender);
    this.reply = reply;
    this.fail = fail;
  }

  /** Re-attaches a pooled context to a fresh ask. */
  rearm(
    message: unknown,
    self: PID,
    sender: PID,
    reply: (value: unknown) => void,
    fail: (reason: Error) => void,
  ): void {
    this.attach(message, self, sender);
    this.reply = reply;
    this.fail = fail;
  }

  /** Returns this context to the pool once its reply has been settled; a
   * still pending ask stays out of the pool and is reclaimed by the
   * garbage collector after its timeout instead. */
  override recycle(): void {
    if (this.reply === null && this.gen === null && askPool.length < ASK_POOL_LIMIT) {
      askPool.push(this);
    }
  }

  override response(value: unknown): void {
    const reply = this.reply;
    if (reply === null) {
      return;
    }

    this.reply = null;
    this.fail = null;
    this.unlink();
    reply(value);
  }

  /** Rejects the ask with the timeout sentinel. Only the sweep calls
   * this, and only for pending asks: a settled ask left its generation
   * when it was answered or cancelled. */
  expire(): void {
    const fail = this.fail as (reason: Error) => void;
    this.reply = null;
    this.fail = null;
    fail(ErrRequestTimeout);
  }

  /** Removes this ask from its pending generation in O(1). A context
   * that was never enlisted, a request without a timeout for example,
   * has no generation and nothing to remove. */
  unlink(): void {
    const gen = this.gen;
    if (gen === null) {
      return;
    }

    const prev = this.prev;
    const next = this.next;

    if (prev !== null) {
      prev.next = next;
    } else {
      gen.head = next;
    }

    if (next !== null) {
      next.prev = prev;
    } else {
      gen.tail = prev;
    }

    this.gen = null;
    this.prev = null;
    this.next = null;
  }
}

/**
 * Constructs a receive context. Runtime plumbing for mailbox delivery
 * and tests; developers receive contexts from `receive`, they do not
 * construct them.
 *
 * @internal
 */
export function createReceiveContext(message?: unknown, self?: PID, sender?: PID): ReceiveContext {
  return ReceiveContext.create(message, self, sender);
}

/** Capture slots for {@link captureResolvers}: a shared executor spares
 * a closure allocation per ask. */
let capturedResolve: ((value: unknown) => void) | null = null;
let capturedReject: ((reason: Error) => void) | null = null;

function captureResolvers(
  resolve: (value: unknown) => void,
  reject: (reason: Error) => void,
): void {
  capturedResolve = resolve;
  capturedReject = reject;
}

/**
 * Constructs a receive context that can complete an {@link PID#ask} and
 * registers it with the timeout generations for `timeout`.
 *
 * @internal
 */
export function createAskContext(
  message: unknown,
  self: PID,
  sender: PID,
  timeout: number,
): { ctx: ReceiveContext; reply: Promise<unknown> } {
  const reply = new Promise<unknown>(captureResolvers);
  const resolve = capturedResolve as (value: unknown) => void;
  const reject = capturedReject as (reason: Error) => void;
  return { ctx: createRequestContext(message, self, sender, timeout, resolve, reject), reply };
}

/**
 * Constructs a receive context whose {@link ReceiveContext.response}
 * routes into the given callbacks, registering it with the timeout
 * generations when `timeout` is positive. Runtime plumbing shared by ask
 * and in-actor requests.
 *
 * @internal
 */
export function createRequestContext(
  message: unknown,
  self: PID,
  sender: PID,
  timeout: number,
  resolve: (value: unknown) => void,
  reject: (reason: Error) => void,
): ReceiveContext {
  let ctx = askPool.pop();
  if (ctx === undefined) {
    ctx = new AskContext(message, self, sender, resolve, reject);
  } else {
    ctx.rearm(message, self, sender, resolve, reject);
  }

  if (timeout > 0) {
    enlist(ctx, timeout);
  }

  return ctx;
}

/** Appends the context to the young generation of its duration and makes
 * sure the shared timer is running. */
function enlist(ctx: AskContext, timeout: number): void {
  let list: AskList;
  if (timeout === lastDuration) {
    list = lastList as AskList;
  } else {
    let found = askLists.get(timeout);
    if (found === undefined) {
      found = {
        young: { head: null, tail: null },
        old: { head: null, tail: null },
        timer: null,
        duration: timeout,
      };
      askLists.set(timeout, found);
    }

    list = found;
    lastDuration = timeout;
    lastList = list;
  }

  const gen = list.young;
  const tail = gen.tail;
  if (tail === null) {
    gen.head = ctx;
  } else {
    ctx.prev = tail;
    tail.next = ctx;
  }

  gen.tail = ctx;
  ctx.gen = gen;

  if (list.timer === null) {
    armAskTimer(list);
  }
}

/**
 * Abandons an ask whose delivery failed before it entered a mailbox: the
 * context leaves its pending generation and a late response or expiry
 * becomes a no-op. The reply promise is left unsettled; the caller
 * reports the delivery error instead.
 *
 * Only contexts minted by {@link createAskContext} reach this; the
 * parameter is typed as the base class because the ask subclass does not
 * leave this module.
 *
 * @internal
 */
export function cancelAsk(ctx: ReceiveContext): void {
  const ask = ctx as AskContext;
  ask.reply = null;
  ask.fail = null;
  ask.unlink();
}

/**
 * Rejects the reply channel of a delivery: the pending ask or request
 * completes with `err` instead of a response. Reports whether there was
 * a live reply channel to reject; a plain tell, and an ask already
 * settled or expired, report `false` and are left untouched. Runtime
 * plumbing for routers, which refuse some deliveries they never
 * process.
 *
 * @internal
 */
export function rejectAsk(ctx: ReceiveContext, err: Error): boolean {
  if (!(ctx instanceof AskContext)) {
    return false;
  }

  const fail = ctx.fail;
  if (fail === null) {
    return false;
  }

  ctx.reply = null;
  ctx.fail = null;
  ctx.unlink();
  fail(err);
  return true;
}
