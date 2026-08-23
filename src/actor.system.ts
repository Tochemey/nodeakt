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
import { ActorRef } from "./actor.ref";
import type { ActorSystemOptions } from "./actor.system.options";
import {
  DeadletterActor,
  DeadlettersCountRequest,
  eventsTopic,
  SendDeadletter,
} from "./deadletter";
import { discardLogger } from "./discard.logger";
import {
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrInvalidActorName,
  ErrInvalidActorSystemName,
  ErrNameRequired,
  ErrReservedName,
} from "./errors";
import { EventStream, type StreamSubscriber } from "./eventstream";
import { defaultLogger } from "./json.logger";
import type { Logger } from "./logger";
import { Deadletter, PostStart, RuntimeCommand, Terminated } from "./messages";
import { NoSender } from "./no.sender";
import { PassivationManager } from "./passivation.manager";
import { isValidActorName, newPathAt, type Path, type PathAddress, parsePath } from "./path";
import { PID } from "./pid";
import { PidTree } from "./pid.tree";
import type { Placement } from "./placement";
import { Props } from "./props";
import {
  deadletterName,
  isSystemName,
  noSenderName,
  rootGuardianName,
  systemGuardianName,
  userGuardianName,
} from "./reserved";
import { RootGuardian } from "./root.guardian";
import { createRouter } from "./router";
import type { RouterOptions } from "./router.options";
import type { ScheduleOptions } from "./schedule.options";
import { Scheduler } from "./scheduler";
import type { SpawnOptions } from "./spawn.options";
import { SystemGuardian } from "./system.guardian";
import { UserGuardian } from "./user.guardian";

/** The node endpoint of a single-node actor system. */
const HOST = "127.0.0.1";
const PORT = 0;

/** How long a dead-letter count query waits, in milliseconds; the
 * dead-letter actor answers from memory, so this never fires in
 * practice. */
const DEADLETTERS_COUNT_TIMEOUT = 5_000;

/**
 * Actor system names must start with an alphanumeric character and may
 * contain alphanumerics, '-' or '_'. Stricter than actor names: no dots.
 */
const SYSTEM_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9-_]*$/;

/**
 * ActorSystem is the runtime that hosts and manages actors: one logical
 * system per machine, owning the actor tree, the guardian hierarchy, and
 * the passivation scheduler.
 *
 * Create one, `start` it, `spawn` actors, and `stop` it on the way out.
 * Every actor lives under one of the guardians: runtime actors under the
 * system guardian, actors created with {@link spawn} under the user
 * guardian, both children of the root guardian.
 */
export class ActorSystem {
  private readonly _name: string;
  private readonly _address: PathAddress;
  private readonly _tree = new PidTree();
  private readonly _passivation = new PassivationManager();
  private readonly _scheduler = new Scheduler();
  private readonly _logger: Logger;
  private readonly _events: EventStream;

  private _rootGuardian: PID | null = null;
  private _systemGuardian: PID | null = null;
  private _userGuardian: PID | null = null;
  private _noSender: PID | null = null;
  private _deadletter: PID | null = null;
  private _started = false;
  private _stopping = false;

  /** The pool seam: null until the first `Props` spawn boots it on the
   * main isolate, or until the worker runtime attaches the facade
   * implementation. */
  private _placement: Placement | null = null;

  /** Single-flight guard of the lazy placement boot. */
  private _placementBoot: Promise<Placement> | null = null;

  /** Whether this system booted the placement itself and must tear it
   * down on stop; facades never own theirs. */
  private _ownsPlacement = false;

  private readonly _parallelism: number | undefined;

  /**
   * Creates an actor system with the given name.
   *
   * @param name - The system name; it must start with an alphanumeric
   * character and contain only alphanumerics, `-` or `_`.
   * @param options - The system configuration; every setting has a
   * default.
   *
   * @throws The {@link ErrNameRequired} sentinel when the name is empty.
   * @throws The {@link ErrInvalidActorSystemName} sentinel when the name
   * violates the syntax rules.
   */
  constructor(name: string, options?: ActorSystemOptions) {
    if (name.length === 0) {
      throw ErrNameRequired;
    }

    if (!SYSTEM_NAME_PATTERN.test(name)) {
      throw ErrInvalidActorSystemName;
    }

    this._name = name;
    this._address = { system: name, host: HOST, port: PORT };
    this._logger = options?.logger ?? defaultLogger;
    this._parallelism = options?.parallelism;
    this._events = new EventStream((err) => {
      this._logger.error("event subscriber failed", { error: err });
    });
  }

  /** Returns the actor system name. */
  name(): string {
    return this._name;
  }

  /** Returns the logger the runtime reports through. */
  logger(): Logger {
    return this._logger;
  }

  /**
   * Subscribes to the system's runtime events. The subscriber receives
   * every event published on the system's events topic and narrows with
   * `instanceof`, the same type switch used for `ctx.message`:
   *
   * ```ts
   * system.subscribe((event) => {
   *   if (event instanceof Deadletter) {
   *     console.warn(`dead letter for ${event.receiver}`);
   *   }
   * });
   * ```
   *
   * `Deadletter` is the first event kind; later runtime events flow
   * through the same subscription. Subscribing the same function twice
   * is a no-op.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running.
   */
  subscribe(subscriber: StreamSubscriber): void {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    this._events.subscribe(subscriber, eventsTopic);
  }

  /**
   * Cancels a {@link subscribe} registration; unknown subscribers are
   * ignored.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running.
   */
  unsubscribe(subscriber: StreamSubscriber): void {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    this._events.unsubscribe(subscriber, eventsTopic);
  }

  /**
   * Returns an {@link ActorRef} addressing the given canonical path
   * string, for example `nodeakt://sys@127.0.0.1:0/parent/child`. The
   * ref carries no incarnation: it addresses whatever lives at the path
   * when a message is sent. Runtime plumbing for the transport layer;
   * developers resolve actors with `actorOf`.
   *
   * @throws A `TypeError` when the string is not a well-formed path.
   *
   * @internal
   */
  refOf(address: string): ActorRef {
    return new ActorRef(this, parsePath(address));
  }

  /**
   * Resolves a path to the live PID registered under it, walking the
   * name chain from the top-level actor down. Returns undefined when
   * the path belongs to another node, the system is not running, or any
   * segment is unregistered. Incarnation is not compared here; the
   * caller decides how strict identity is. Runtime plumbing for
   * {@link ActorRef}.
   *
   * @internal
   */
  resolvePath(path: Path): PID | undefined {
    const guardian = this._userGuardian;
    if (guardian === null) {
      return undefined;
    }

    if (path.system() !== this._name || path.host() !== HOST || path.port() !== PORT) {
      return undefined;
    }

    const names: string[] = [];
    for (let segment: Path | undefined = path; segment !== undefined; segment = segment.parent()) {
      names.push(segment.name());
    }

    let current = this._tree.child(guardian, names[names.length - 1] as string);
    for (let i = names.length - 2; current !== undefined && i >= 0; i--) {
      current = this._tree.child(current, names[i] as string);
    }

    return current;
  }

  /**
   * Routes one undeliverable message to the dead-letter actor, unless
   * it is a runtime announcement or an internal command: those are the
   * runtime talking to itself, and skipping `SendDeadletter` keeps a
   * failing dead-letter delivery from feeding on itself. Runtime
   * plumbing for the send paths.
   *
   * @internal
   */
  toDeadletter(sender: string | undefined, receiver: string, message: unknown, err: Error): void {
    if (
      message instanceof RuntimeCommand ||
      message instanceof PostStart ||
      message instanceof Terminated ||
      message instanceof SendDeadletter
    ) {
      return;
    }

    const deadletter = this._deadletter;
    if (deadletter === null) {
      return;
    }

    (this._noSender as PID).tell(
      deadletter,
      new SendDeadletter(new Deadletter(sender, receiver, message, Date.now(), err.message)),
    );
  }

  /**
   * Reports whether anything is subscribed to the runtime event stream.
   * Lifecycle publishers consult it first so an idle stream costs only
   * this check, never the work of building an event nobody reads.
   * Runtime plumbing for the PID lifecycle; developers observe events by
   * subscribing.
   *
   * @internal
   */
  hasEventSubscribers(): boolean {
    return this._events.subscribersCount(eventsTopic) > 0;
  }

  /**
   * Publishes one runtime event to every subscriber of the event stream.
   * Runtime plumbing for the PID lifecycle; developers observe events by
   * subscribing.
   *
   * @internal
   */
  publishEvent(event: unknown): void {
    this._events.publish(eventsTopic, event);
  }

  /**
   * Asks the dead-letter actor how many dead letters the given receiver
   * has accumulated, by canonical path string, or the system-wide total
   * when no receiver is given. Counts live in the dead-letter actor and
   * reset when the system stops.
   *
   * Runtime plumbing behind the dead-letter subsystem's own bookkeeping
   * and tests; developers observe dead letters by subscribing to the
   * event stream, not by polling a count.
   *
   * @returns A promise of the count. It rejects with the
   * {@link ErrActorSystemNotStarted} sentinel when the system is not
   * running.
   *
   * @internal
   */
  async deadlettersCount(receiver?: string): Promise<number> {
    const deadletter = this._deadletter;
    if (!this.isRunning() || deadletter === null) {
      throw ErrActorSystemNotStarted;
    }

    const count = await this.noSender().ask(
      deadletter,
      new DeadlettersCountRequest(receiver),
      DEADLETTERS_COUNT_TIMEOUT,
    );
    return count as number;
  }

  /**
   * Returns the PID of the dead-letter actor, the sink of unhandled
   * messages, or null while the system is not running. Runtime plumbing
   * for the send paths; developers subscribe through {@link subscribe}.
   *
   * @internal
   */
  deadletterPid(): PID | null {
    return this._deadletter;
  }

  /** Reports whether the system has been started and is not stopping. */
  isRunning(): boolean {
    return this._started && !this._stopping;
  }

  /**
   * Starts the actor system: the guardian hierarchy is created and the
   * system begins accepting spawns. Starting a running system is a no-op.
   *
   * @throws An `ActorInitializationError` when a guardian fails to
   * initialize, in which case the system did not start.
   */
  async start(): Promise<void> {
    if (this._started) {
      return;
    }

    // Runtime actors rely on the long-lived default strategy and never
    // passivate. The tree records that the system and user guardians are
    // children of the root guardian and that the NoSender actor is a
    // child of the system guardian; guardians are supervision parents
    // only, so they never appear in the path of an actor beneath them.
    this._rootGuardian = await this.configPID(rootGuardianName, new RootGuardian(), null);

    this._systemGuardian = await this.configPID(
      systemGuardianName,
      new SystemGuardian(),
      this._rootGuardian,
    );

    this._noSender = await this.configPID(noSenderName, new NoSender(), this._systemGuardian);

    this._deadletter = await this.configPID(
      deadletterName,
      new DeadletterActor(this._events),
      this._systemGuardian,
    );

    this._userGuardian = await this.configPID(
      userGuardianName,
      new UserGuardian(),
      this._rootGuardian,
    );

    this._started = true;
  }

  /**
   * Stops the actor system gracefully: every actor is shut down through
   * the guardian hierarchy (mailboxes drain, `postStop` hooks run) and
   * the passivation scheduler is released. Stopping a stopped system is a
   * no-op; a stopped system can be started again.
   */
  async stop(): Promise<void> {
    if (!this._started || this._stopping) {
      return;
    }

    this._stopping = true;

    // The pool goes first, while this system still serves: workers
    // drain their own actors and their final dead letters still reach
    // this stream. A facade never owns its placement; the worker
    // runtime tears that down.
    const placement = this._placement;
    this._placement = null;
    this._placementBoot = null;
    if (placement !== null && this._ownsPlacement) {
      this._ownsPlacement = false;
      await placement.stop();
    }

    this._scheduler.stop();
    this._passivation.stop();
    await this._rootGuardian?.shutdown();

    this._events.close();
    this._tree.reset();
    this._rootGuardian = null;
    this._systemGuardian = null;
    this._userGuardian = null;
    this._noSender = null;
    this._deadletter = null;
    this._started = false;
    this._stopping = false;
  }

  /**
   * Returns the PID of the NoSender actor, the runtime actor representing
   * an anonymous or absent sender. Use it to send messages from outside an
   * actor: `system.noSender().tell(target, message)`; the receiving
   * behavior then sees this PID as `ctx.sender`.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when the system
   * is not started.
   */
  noSender(): PID {
    if (this._noSender === null) {
      throw ErrActorSystemNotStarted;
    }

    return this._noSender;
  }

  /**
   * Delivers `message` to `pid` repeatedly, every `interval`
   * milliseconds, the first delivery one interval from now. The
   * schedule runs until it is cancelled or the system stops; delivery
   * goes through the normal send path, so a tick to an actor that has
   * stopped by fire time becomes a dead letter like any other
   * undeliverable send. Every tick is an independent send: nothing
   * suppresses a tick because the previous message is still queued.
   *
   * The message carries the sender from `opts.sender`, or the system's
   * NoSender actor by default. Give the schedule a reference through
   * `opts.reference` to cancel, pause, or resume it later.
   *
   * @returns A promise that settles once the schedule is registered. It
   * rejects with the {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running, the {@link ErrInvalidInterval} sentinel when
   * `interval` is not a positive number, the
   * {@link ErrScheduleAlreadyExists} sentinel when the reference is
   * already held by another schedule, and the {@link ErrDead} sentinel
   * when the target has already stopped.
   */
  async schedule(
    message: unknown,
    pid: PID,
    interval: number,
    opts?: ScheduleOptions,
  ): Promise<void> {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    this._scheduler.schedule(
      message,
      pid,
      interval,
      opts?.sender ?? this.noSender(),
      null,
      opts?.reference,
    );
  }

  /**
   * Delivers `message` to `pid` once, `delay` milliseconds from now.
   * Delivery goes through the normal send path, so a target that has
   * stopped by fire time receives nothing and the message becomes a
   * dead letter.
   *
   * The message carries the sender from `opts.sender`, or the system's
   * NoSender actor by default. Give the schedule a reference through
   * `opts.reference` to cancel, pause, or resume it before it fires.
   *
   * @returns A promise that settles once the schedule is registered. It
   * rejects with the {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running, the {@link ErrInvalidInterval} sentinel when
   * `delay` is not a positive number, the
   * {@link ErrScheduleAlreadyExists} sentinel when the reference is
   * already held by another schedule, and the {@link ErrDead} sentinel
   * when the target has already stopped.
   */
  async scheduleOnce(
    message: unknown,
    pid: PID,
    delay: number,
    opts?: ScheduleOptions,
  ): Promise<void> {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    this._scheduler.scheduleOnce(
      message,
      pid,
      delay,
      opts?.sender ?? this.noSender(),
      null,
      opts?.reference,
    );
  }

  /**
   * Cancels the schedule held under `reference`: nothing fires after
   * the returned promise settles.
   *
   * @returns A promise that settles once the schedule is cancelled. It
   * rejects with the {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running and the {@link ErrScheduleNotFound} sentinel
   * when no schedule holds the reference.
   */
  async cancelSchedule(reference: string): Promise<void> {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    this._scheduler.cancel(reference);
  }

  /**
   * Pauses the schedule held under `reference`; a paused schedule fires
   * nothing until it is resumed. Pausing a paused schedule is a no-op.
   * A paused one-shot keeps the delay it had left; a paused repeating
   * schedule fires one full interval after it is resumed.
   *
   * @returns A promise that settles once the schedule is paused. It
   * rejects with the {@link ErrActorSystemNotStarted} sentinel when the
   * system is not running and the {@link ErrScheduleNotFound} sentinel
   * when no schedule holds the reference.
   */
  async pauseSchedule(reference: string): Promise<void> {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    this._scheduler.pause(reference);
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
  async resumeSchedule(reference: string): Promise<void> {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    this._scheduler.resume(reference);
  }

  /**
   * Registers a repeating schedule owned by `owner`: it is cancelled
   * when that actor fully stops, and the delivered messages carry the
   * owner as sender unless `opts.sender` overrides it. Runtime plumbing
   * for `ReceiveContext.schedule`.
   *
   * @internal
   */
  async scheduleFrom(
    owner: PID,
    message: unknown,
    pid: PID,
    interval: number,
    opts?: ScheduleOptions,
  ): Promise<void> {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    this._scheduler.schedule(message, pid, interval, opts?.sender ?? owner, owner, opts?.reference);
  }

  /**
   * Registers a one-shot schedule owned by `owner`: it is cancelled
   * when that actor fully stops, and the delivered message carries the
   * owner as sender unless `opts.sender` overrides it. Runtime plumbing
   * for `ReceiveContext.scheduleOnce`.
   *
   * @internal
   */
  async scheduleOnceFrom(
    owner: PID,
    message: unknown,
    pid: PID,
    delay: number,
    opts?: ScheduleOptions,
  ): Promise<void> {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    this._scheduler.scheduleOnce(
      message,
      pid,
      delay,
      opts?.sender ?? owner,
      owner,
      opts?.reference,
    );
  }

  /**
   * Hooks an actor into the system's passivation scheduler according to
   * the actor's own strategy. Runtime plumbing for the spawn paths;
   * strategies other than time based involve no scheduling.
   *
   * @internal
   */
  schedulePassivation(pid: PID): void {
    this._passivation.register(pid, pid.passivationStrategy());
  }

  /**
   * Creates and starts an actor under the given unique name.
   *
   * The actor becomes a child of the user guardian and receives a
   * {@link PostStart} message before anything else.
   *
   * @param name - The actor's name, unique within the system.
   * @param actor - The actor implementation to run.
   * @param options - The mailbox and passivation configuration; both have
   * defaults.
   *
   * @returns The PID of the started actor.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when the system
   * is not running.
   * @throws The {@link ErrReservedName} sentinel when the name carries
   * the runtime's reserved prefix.
   * @throws The {@link ErrInvalidActorName} sentinel when the name is
   * empty, longer than 255 characters, or violates the name syntax.
   * @throws The {@link ErrActorAlreadyExists} sentinel when the name is
   * still held by an actor that has not fully stopped, including a
   * suspended or currently stopping one.
   * @throws An `ActorInitializationError` when the actor's `preStart`
   * fails; the underlying failure is available as its `cause` and the
   * actor is not registered.
   */
  async spawn(name: string, actor: Actor | Props, options?: SpawnOptions): Promise<PID> {
    if (!this.isRunning() || this._userGuardian === null) {
      throw ErrActorSystemNotStarted;
    }

    if (isSystemName(name)) {
      throw ErrReservedName;
    }

    if (!isValidActorName(name)) {
      throw ErrInvalidActorName;
    }

    // Props is construction as data: the placement decides which
    // isolate builds the actor, booting the pool on first use.
    if (actor instanceof Props) {
      const placement = await this.ensurePlacement();
      return placement.place(name, actor, options);
    }

    // Occupancy is registration in the tree: a suspended or stopping
    // actor still holds its name until it has fully stopped.
    const existing = this._tree.child(this._userGuardian, name);
    if (existing !== undefined) {
      throw ErrActorAlreadyExists;
    }

    // With a pool active, top-level names are unique across every
    // isolate, so an instance spawn claims its name with the control
    // plane and releases it when the actor stops.
    const placement = this._placement;
    if (placement !== null) {
      const refused = await placement.claim(name);
      if (refused !== null) {
        throw refused;
      }
    }

    let pid: PID;
    try {
      pid = await this.configPID(name, actor, this._userGuardian, options);
    } catch (err) {
      placement?.free(name);
      throw err;
    }

    if (placement !== null) {
      pid.onStopped(() => {
        placement.free(name);
      });
    }

    this.schedulePassivation(pid);
    return pid;
  }

  /**
   * Creates and starts a router: an actor owning a pool of `poolSize`
   * identical routees built from `routees`, forwarding every message it
   * receives to them according to the routing strategy, round robin
   * when none is configured. The router is a real actor: it lives under
   * the given unique name like any {@link spawn}, its routees are its
   * children and it supervises them, and stopping it stops the pool.
   *
   * The router never processes a user message itself. A delivery is
   * handed to the routee live, so the routee sees the original sender
   * as `ctx.sender` and its reply to an ask settles the asker's promise
   * directly, bypassing the router; an ask through a fan-out router is
   * rejected with the {@link ErrFanOutAsk} sentinel, because a
   * broadcast has no single answer.
   *
   * Two management messages are consumed by the router itself:
   * `GetRoutees`, answered with a `Routees` message listing the live
   * routees, and `AdjustRouterPoolSize`, which grows or shrinks the
   * pool in place. A routee that fails while processing is handled by
   * the routee directive chosen at spawn time, `stop` by default, so
   * the pool shrinks; a dead routee leaves the rotation, and once no
   * routee is left every subsequent send becomes a dead letter until
   * `AdjustRouterPoolSize` restores capacity.
   *
   * ```ts
   * const router = await system.spawnRouter("workers", 8, Props.create(Worker), {
   *   strategy: "roundRobin",
   * });
   *
   * system.noSender().tell(router, new Job(42));
   * ```
   *
   * @param name - The router's name, unique within the system.
   * @param poolSize - How many routees to start with; a positive
   * integer.
   * @param routees - How to construct each routee, as `Props`.
   * @param options - The routing strategy, the routing key extractor of
   * a consistent-hash router, and the routee directive.
   *
   * @returns A promise of the router's PID. It rejects with everything
   * {@link spawn} rejects with, a `TypeError` when `routees` is not a
   * `Props`, and the {@link ErrInvalidPoolSize},
   * {@link ErrInvalidRoutingStrategy}, {@link ErrRoutingKeyRequired},
   * and {@link ErrInvalidRouteeDirective} sentinels when the
   * configuration is invalid.
   */
  async spawnRouter(
    name: string,
    poolSize: number,
    routees: Props,
    options?: RouterOptions,
  ): Promise<PID> {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    return this.spawn(name, createRouter(poolSize, routees, options));
  }

  /**
   * Boots the pool seam on first use: dynamically imported so the
   * runtime's pool machinery never loads, let alone boots, in a
   * program that spawns only instances.
   */
  private ensurePlacement(): Promise<Placement> {
    if (this._placement !== null) {
      return Promise.resolve(this._placement);
    }

    this._placementBoot ??= this.bootPlacement().catch((err: unknown) => {
      // A failed boot must not poison every later spawn: the next
      // Props spawn retries from scratch.
      this._placementBoot = null;
      throw err;
    });
    return this._placementBoot;
  }

  private async bootPlacement(): Promise<Placement> {
    const { systemPlacement } = await import("./system.placement");
    const placement = await systemPlacement(this, {
      parallelism: this._parallelism,
      quiet: this._logger === discardLogger,
    });
    this._placement = placement;
    this._ownsPlacement = true;
    return placement;
  }

  /**
   * Hands this system its placement seam: the worker runtime attaches
   * the facade implementation at boot, so spawns and lookups on a
   * worker facade go through the control plane. Runtime plumbing.
   *
   * @internal
   */
  attachPlacement(placement: Placement): void {
    this._placement = placement;
  }

  /**
   * Returns the running top-level actors, so a placement booting after
   * instance spawns can claim their names with the control plane.
   * Runtime plumbing.
   *
   * @internal
   */
  topLevelActors(): PID[] {
    const guardian = this._userGuardian;
    if (guardian === null) {
      return [];
    }

    return this._tree.children(guardian).filter((pid) => pid.isRunning());
  }

  /**
   * Resolves a top-level actor by name: one created with {@link spawn}.
   *
   * Actors deeper in the hierarchy are reached through their parent, not
   * by bare name, and the runtime's own actors are not resolvable.
   *
   * @param name - The actor name to look up.
   *
   * @returns The running actor's PID, or `undefined` when no running
   * top-level actor holds the name.
   */
  actorOf(name: string): PID | undefined {
    if (this._userGuardian === null) {
      return undefined;
    }

    const pid = this._tree.child(this._userGuardian, name);
    if (pid === undefined || !pid.isRunning()) {
      return this._placement?.find(name);
    }

    return pid;
  }

  /**
   * Returns the address of a top-level actor: a path with no parent
   * segment. An actor's path chain begins at its top-level ancestor;
   * the guardian layer above is supervision structure recorded in the
   * tree, never part of an address.
   */
  private actorAddress(name: string): Path {
    return newPathAt(name, this._address);
  }

  /** Constructs, starts, and registers one actor in the tree under the
   * given parent. Bypasses the user-facing spawn checks, so guardians
   * can carry reserved names. */
  private async configPID(
    name: string,
    actor: Actor,
    parent: PID | null,
    options?: SpawnOptions,
  ): Promise<PID> {
    const pid = new PID(
      actor,
      this.actorAddress(name),
      this,
      options,
      parent ?? undefined,
      this._tree,
    );

    await pid.start();
    return pid;
  }
}
