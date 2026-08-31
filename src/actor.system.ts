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

import { availableParallelism } from "node:os";
import type { Actor } from "./actor";
import { ActorRef, type IsolateRoute } from "./actor.ref";
import type { ActorSystemOptions } from "./actor.system.options";
import { translateClusterEvent } from "./cluster.events";
import type { ClusterOptions } from "./cluster.options";
import { CLUSTER_EVENT_TOPIC, type ClusterEvent, ClusterEventType } from "./clustering.events";
import { ClusterNode, type ClusterNodeOptions } from "./clustering.host";
import { ClusterPlacement } from "./clustering.placement";
import type { RecreateRecipe } from "./clustering.recreate";
import { ClusterRegistry, type PlacementRecord } from "./clustering.registry";
import {
  NodeDeparted,
  type RelocationMember,
  RelocationSweep,
  Relocator,
} from "./clustering.relocator";
import { ClusterResolver } from "./clustering.resolver";
import { STRATEGY_ROUND_ROBIN, selectOwner } from "./clustering.strategy";
import { formatHostPort } from "./clustering.transport";
import {
  DeadletterActor,
  DeadlettersCountRequest,
  eventsTopic,
  SendDeadletter,
} from "./deadletter";
import { discardLogger } from "./discard.logger";
import {
  ActorNotFoundError,
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrClusteringDisabled,
  ErrClusterRequiresRemote,
  ErrClusterRequiresRoutableHost,
  ErrInvalidActorName,
  ErrInvalidActorSystemName,
  ErrNameRequired,
  ErrRemotingDisabled,
  ErrReservedName,
} from "./errors";
import { EventStream, type StreamSubscriber } from "./eventstream";
import type { Extension } from "./extension/extension";
import { ExtensionRegistry } from "./extension/registry";
import type { Logger } from "./logger";
import { Deadletter, PostStart, RuntimeCommand, Terminated } from "./messages";
import { MetricRegistry } from "./metric.registry";
import { NoSender } from "./no.sender";
import type { MetricsOptions } from "./observability/metric.options";
import {
  emptyMetricsSnapshot,
  type IsolateGauges,
  type IsolateMetrics,
  type MetricsSnapshot,
  mergeMetrics,
} from "./observability/metric.snapshot";
import { PassivationManager } from "./passivation.manager";
import { isValidActorName, newPathAt, type Path, type PathAddress, parsePath } from "./path";
import { PID } from "./pid";
import { PidTree } from "./pid.tree";
import type { Placement } from "./placement";
import { Props } from "./props";
import type { RemoteOptions } from "./remote.options";
import { Remoting } from "./remoting";
import {
  deadletterName,
  isSystemName,
  noSenderName,
  relocatorName,
  rootGuardianName,
  systemGuardianName,
  userGuardianName,
} from "./reserved";
import { RootGuardian } from "./root.guardian";
import { createRouter } from "./router";
import type { RouterOptions } from "./router.options";
import { startupBanner } from "./runtime.info";
import type { ScheduleOptions } from "./schedule.options";
import { Scheduler } from "./scheduler";
import type { SpawnOnOptions, SpawnOptions } from "./spawn.options";
import { SystemGuardian } from "./system.guardian";
import { systemPlacement } from "./system.placement";
import { defaultLogger } from "./text.logger";
import { UserGuardian } from "./user.guardian";

/** The node endpoint of a system with remoting disabled: the loopback
 * host and an unbound port, so single-node paths stay stable and a
 * remote endpoint, when enabled, overrides both. */
const HOST: string = "127.0.0.1";
const PORT: number = 0;

/** The stable gossip port a clustered node uses when its cluster options name
 * none, the shared default every node agrees on without configuration. */
const CLUSTER_GOSSIP_PORT: number = 7946;

/** How many times a singleton create re-attempts the coordinator claim when it
 * loses the race yet cannot yet resolve the winner: the winner is momentarily gone
 * (it stopped between the loss and the resolve) so the name is free to reclaim.
 * A small bound, since each retry needs the state to flip again to keep failing. */
const SINGLETON_CLAIM_ATTEMPTS: number = 3;

/** How often the coordinator's relocation actor sweeps for records still naming a
 * departed node, the backstop that retries a recreate the `NodeLeft` pass could not
 * place. Long enough that a settled cluster's sweep is cheap. */
const RELOCATION_SWEEP_INTERVAL_MS: number = 30_000;

/** The hosts that name no dialable interface: an empty host and the IPv4 and IPv6
 * unspecified addresses a node may bind but must not advertise, in the spellings a
 * bracket-stripped, lower-cased host normalizes to. */
const WILDCARD_HOSTS: ReadonlySet<string> = new Set([
  "",
  "0.0.0.0",
  "::",
  "::0",
  "0:0:0:0:0:0:0:0",
  "0000:0000:0000:0000:0000:0000:0000:0000",
]);

/** Whether a host is a wildcard or empty, so not an address a peer can dial back:
 * a node that binds one must advertise a concrete address instead. It strips an
 * IPv6 literal's brackets and lower-cases before matching, so bracketed and
 * mixed-case spellings of the unspecified address are caught too. */
function isWildcardHost(host: string): boolean {
  const normalized: string = host
    .trim()
    .toLowerCase()
    .replace(/^\[(.*)\]$/, "$1");
  return WILDCARD_HOSTS.has(normalized);
}

/**
 * Builds the cluster node's options from the public cluster options, deriving the
 * remoting address the node advertises from the bound remoting endpoint, binding
 * the gossip and data endpoints to that same remoting host, and defaulting the
 * gossip port. A standalone mapping so its defaulting is unit-testable without
 * binding a socket. `remotingHost` and `remotingPort` are the system's bound
 * remoting endpoint, so the caller passes them after remoting has started.
 *
 * @internal
 */
export function clusterNodeOptions(
  cluster: ClusterOptions,
  remotingHost: string,
  remotingPort: number,
  events: EventStream,
  logger?: Logger,
): ClusterNodeOptions {
  return {
    discovery: cluster.discovery,
    host: remotingHost,
    gossipPort: cluster.gossipPort ?? CLUSTER_GOSSIP_PORT,
    remotingAddress: formatHostPort(remotingHost, remotingPort),
    events,
    ...(logger !== undefined ? { logger } : {}),
    ...(cluster.dataPort !== undefined ? { dataPort: cluster.dataPort } : {}),
    ...(cluster.bootstrapTimeout !== undefined ? { bootDeadlineMs: cluster.bootstrapTimeout } : {}),
    ...(cluster.partitionCount !== undefined ? { partitionCount: cluster.partitionCount } : {}),
    ...(cluster.replicaCount !== undefined ? { replicaCount: cluster.replicaCount } : {}),
    ...(cluster.writeQuorum !== undefined ? { writeQuorum: cluster.writeQuorum } : {}),
    ...(cluster.minimumMemberQuorum !== undefined
      ? { minimumMemberQuorum: cluster.minimumMemberQuorum }
      : {}),
  };
}

/** The fallback ask/request deadline when a system configures none: a
 * generous default so a caller that omits its own timeout still never
 * waits unbounded. */
const DEFAULT_ASK_TIMEOUT_MS: number = 5_000;

/** The environment variable overriding the detected capacity: an
 * operational escape hatch for machines whose usable CPU count the
 * runtime misreads (container quotas, shared hosts). At `1` the system
 * never boots workers; the variable is never part of the API. */
const capacityVariable = "NODEAKT_PARALLELISM";

/** Detects the system's capacity: how many isolates it may run, the
 * machine's `os.availableParallelism()` unless the environment
 * overrides it, clamped to `[1, machine]`. An override that is not an
 * integer is ignored. */
function detectCapacity(): number {
  const machine: number = availableParallelism();
  const raw: string | undefined = process.env[capacityVariable];
  if (raw === undefined || raw === "") {
    return machine;
  }

  const override: number = Number(raw);
  if (!Number.isInteger(override)) {
    return machine;
  }

  return Math.max(1, Math.min(override, machine));
}

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
  /** The node address every actor's path carries. Mutable so an
   * ephemeral remoting port resolves to the bound port at start. */
  private _address: PathAddress;
  private readonly _tree = new PidTree();
  private readonly _passivation = new PassivationManager();
  private readonly _scheduler = new Scheduler();
  private readonly _logger: Logger;
  private readonly _events: EventStream;

  /** The isolate's metrics, or null when metrics were not enabled; PIDs
   * cache this reference and read it on the message hot path. */
  private readonly _metricsRegistry: MetricRegistry | null;

  /** The fallback deadline for an ask or request whose own timeout is
   * omitted or non-positive; always a positive duration. */
  private readonly _askTimeout: number;

  /** The services installed at construction, keyed by identifier; empty
   * when the system was created without any. */
  private readonly _extensions: ExtensionRegistry;

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

  /** How many isolates the system may run, detected at start. */
  private _capacity: number = 1;

  /** The remoting configuration, or undefined for a single-node system. */
  private readonly _remoteOptions: RemoteOptions | undefined;

  /** The remoting layer once started, or null while remoting is disabled
   * or the system is stopped. */
  private _remoting: Remoting | null = null;

  /** The cluster configuration, or undefined for an unclustered system. */
  private readonly _clusterOptions: ClusterOptions | undefined;

  /** The cluster node once started, or null while clustering is disabled or
   * the system is stopped. */
  private _clusterNode: ClusterNode | null = null;

  /** The distributed registry over the cluster node, or null while clustering
   * is disabled or the system is stopped. */
  private _clusterRegistry: ClusterRegistry | null = null;

  /** The cluster placement, typed for the chosen-node placement `spawnOn` drives,
   * or null while clustering is disabled or the system is stopped. */
  private _clusterPlacement: ClusterPlacement | null = null;

  /** The relocation actor once clustering has started, or null otherwise; the
   * coordinator's driver for recreating a departed node's actors. */
  private _relocator: PID | null = null;

  /** The subscriber bridging cluster runtime events onto the public stream and the
   * relocation actor, kept to unsubscribe on stop; null while clustering is off. */
  private _clusterBridge: StreamSubscriber | null = null;

  /** A worker facade's route for paths of other nodes; null on the
   * main isolate, whose remoting seam serves them instead. */
  private _foreignResolver: ((path: Path) => PID | undefined) | null = null;

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
   * @throws The `ErrInvalidExtensionId` sentinel when an extension
   * reports an identifier that violates the identifier syntax rules, and
   * the `ErrExtensionAlreadyExists` sentinel when two of them report the
   * same identifier.
   */
  constructor(name: string, options?: ActorSystemOptions) {
    if (name.length === 0) {
      throw ErrNameRequired;
    }

    if (!SYSTEM_NAME_PATTERN.test(name)) {
      throw ErrInvalidActorSystemName;
    }

    const askTimeout: number = options?.askTimeout ?? DEFAULT_ASK_TIMEOUT_MS;
    if (!Number.isInteger(askTimeout) || askTimeout <= 0) {
      throw new RangeError(`askTimeout must be a positive integer, got ${askTimeout}`);
    }

    this._name = name;
    this._remoteOptions = options?.remote;
    this._clusterOptions = options?.cluster;
    // Clustering needs a remoting endpoint for cross-node actor messages, so a
    // clustered system with no remote is rejected rather than silently made
    // unreachable; the remoting host must also be a concrete peer-routable address.
    if (this._clusterOptions !== undefined) {
      if (this._remoteOptions === undefined) {
        throw ErrClusterRequiresRemote;
      }

      if (isWildcardHost(this._remoteOptions.advertisedHost ?? this._remoteOptions.host)) {
        throw ErrClusterRequiresRoutableHost;
      }
    }

    this._askTimeout = askTimeout;
    this._extensions = new ExtensionRegistry(options?.extensions);
    this._address = {
      system: name,
      host: this._remoteOptions?.advertisedHost ?? this._remoteOptions?.host ?? HOST,
      port: this._remoteOptions?.port ?? PORT,
    };

    this._logger = options?.logger ?? defaultLogger;
    this._events = new EventStream((err) => {
      this._logger.error("event subscriber failed", { error: err });
    });
    this._metricsRegistry =
      options?.metrics?.enabled === true
        ? new MetricRegistry(this._events, options.metrics.processingDuration === true)
        : null;
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
   * Returns the extension installed under the given identifier, or
   * `undefined` when the system carries none under it.
   *
   * The type parameter names the concrete service for the caller; the
   * system stores plain {@link Extension} values, so ask for the type the
   * identifier was installed with.
   *
   * ```ts
   * const store = system.extension<EventStore>("eventStore");
   * await store?.append("orders", event);
   * ```
   *
   * An actor reaches the same instance through `ctx.extension(...)`, on
   * both a lifecycle `Context` and a `ReceiveContext`.
   *
   * @param id - The identifier the extension was installed under.
   */
  extension<T extends Extension>(id: string): T | undefined {
    return this._extensions.get<T>(id);
  }

  /** Returns every extension installed on the system, in the order they
   * were given, so a caller can report what a running system depends on. */
  extensions(): Extension[] {
    return this._extensions.all();
  }

  /** The fallback deadline, in milliseconds, an `ask` or `request`
   * without its own positive timeout is bounded by. Always positive, so
   * no reply-bearing call ever waits unbounded.
   *
   * @internal
   */
  askTimeout(): number {
    return this._askTimeout;
  }

  /**
   * Returns the host the system's node is reachable at: the configured
   * remoting host, or the loopback address when remoting is disabled.
   */
  host(): string {
    return this._address.host;
  }

  /**
   * Returns the port the system's node listens on: the bound remoting
   * port (resolved from an ephemeral `0`), or `0` when remoting is
   * disabled.
   */
  port(): number {
    return this._address.port;
  }

  /** The running cluster node, or null when clustering is off or the system is
   * stopped.
   *
   * @internal
   */
  clusterNode(): ClusterNode | null {
    return this._clusterNode;
  }

  /** The distributed cluster registry over the node, or null when clustering is
   * off or the system is stopped.
   *
   * @internal
   */
  clusterRegistry(): ClusterRegistry | null {
    return this._clusterRegistry;
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

    if (
      path.system() !== this._name ||
      path.host() !== this._address.host ||
      path.port() !== this._address.port
    ) {
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

  /**
   * Returns this isolate's metrics registry, or null when metrics were
   * not enabled. A PID caches the reference at construction and reads it
   * on the message hot path. Runtime plumbing for the message loop.
   *
   * @internal
   */
  metricRegistry(): MetricRegistry | null {
    return this._metricsRegistry;
  }

  /**
   * Returns the metrics configuration to enable on a worker isolate, or
   * null when metrics are off, so every isolate counts its own actors for
   * the machine-wide snapshot. Runtime plumbing for the worker boot.
   *
   * @internal
   */
  metricsConfig(): MetricsOptions | null {
    const registry = this._metricsRegistry;
    return registry === null ? null : { enabled: true, processingDuration: registry.timing };
  }

  /**
   * Returns this isolate's raw metrics contribution, or null when metrics
   * are off: the registry's counters plus the live-actor gauges this
   * isolate reads from its own tree. The main isolate merges these across
   * every isolate; a worker answers a metrics request with them.
   *
   * @internal
   */
  isolateMetrics(): IsolateMetrics | null {
    const registry = this._metricsRegistry;
    return registry === null ? null : registry.isolateMetrics(this.isolateGauges());
  }

  /**
   * Returns a snapshot of the runtime's own metrics: actor counts,
   * message throughput, mailbox depth, and dead letters. The snapshot is
   * plain readonly data an adapter maps onto its backend from outside the
   * runtime, which takes on no metrics dependency of its own.
   *
   * A system that never enabled metrics answers a valid, zeroed snapshot,
   * so an adapter can be wired unconditionally.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when the system
   * is not running.
   */
  async collectMetrics(): Promise<MetricsSnapshot> {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    const mine = this.isolateMetrics();
    if (mine === null) {
      return emptyMetricsSnapshot(this._name);
    }

    const placement = this._placement;
    const workers = placement === null ? [] : await placement.collectMetrics();
    return mergeMetrics(this._name, mine, workers);
  }

  /**
   * Reads the live-actor gauges for a metrics snapshot: how many user
   * actors are alive, how many of those are suspended, and their mailbox
   * depth. A suspended actor is alive and is counted, so its retained
   * backlog still shows up in the mailbox depth.
   *
   * @internal
   */
  private isolateGauges(): IsolateGauges {
    let active = 0;
    let suspended = 0;
    let mailboxTotalDepth = 0;
    let mailboxMaxDepth = 0;

    // collectMetrics only reaches here on a running system, so the user
    // guardian is set; every user actor is a descendant of it.
    const guardian = this._userGuardian as PID;
    for (const pid of this._tree.descendants(guardian)) {
      active++;
      if (pid.isSuspended()) {
        suspended++;
      }

      const depth = pid.mailboxSize();
      mailboxTotalDepth += depth;
      if (depth > mailboxMaxDepth) {
        mailboxMaxDepth = depth;
      }
    }

    return { active, suspended, mailboxTotalDepth, mailboxMaxDepth };
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

    this._logger.info("actor system starting", startupBanner(this._name));
    try {
      await this.bootstrap();
    } catch (error: unknown) {
      this._logger.error("actor system failed to start", { name: this._name, error });
      throw error;
    }

    this._logger.info("actor system started", { name: this._name });
  }

  /**
   * The boot sequence run under {@link start}'s logging: capacity detection,
   * remoting, the guardian tree, and clustering.
   */
  private async bootstrap(): Promise<void> {
    // Capacity is a boot fact: detected once here, consulted by the
    // placement when the first Props spawn boots the pool, so the
    // system provisions for every core the machine gives it.
    this._capacity = detectCapacity();

    // Remoting binds its endpoint before any actor exists, so every actor's path
    // advertises the reachable node address. An ephemeral port resolves to the
    // bound one here. A single-node system loads the seam but binds nothing, so it
    // pays no runtime cost. A bind failure rejects, leaving the system unstarted.
    let boundRemoting: Remoting | null = null;
    if (this._remoteOptions !== undefined) {
      boundRemoting = await Remoting.start(this, this._remoteOptions);
      this._remoting = boundRemoting;
      this._address = {
        system: this._name,
        host: this._remoteOptions.advertisedHost ?? this._remoteOptions.host,
        port: boundRemoting.port,
      };
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

    // Clustering starts last, once the actor machinery is attached: the ready flag
    // a peer consults before selecting this node for a placement is gossiped only
    // from here, so a peer can never pick a member that cannot yet host an actor.
    // The constructor requires remoting when clustering, so the listener is bound
    // above; a cluster boot failure releases it, since a system that never reached
    // "started" can never close it through stop(), while the cluster node releases
    // its own sockets as it fails.
    if (this._clusterOptions !== undefined) {
      await this.attachCluster(boundRemoting as Remoting);
    }

    this._started = true;

    // Kick the relocation actor once the system is running so it arms its recurring
    // sweep even if no node ever departs; sent after "started" so it can schedule.
    const relocator: PID | null = this._relocator;
    if (relocator !== null) {
      this.noSender().tell(relocator, new RelocationSweep());
    }
  }

  /**
   * Starts clustering on the bound remoting, wiring the cluster node, its registry
   * and placement, and the relocation actor. A boot failure releases the remoting
   * listener the constructor required, since a system that never reached "started"
   * cannot close it through {@link stop}. Called last in {@link start}, once the
   * actor machinery is attached, so the ready flag a peer consults before placing on
   * this node is gossiped only when it can host an actor.
   */
  private async attachCluster(remoting: Remoting): Promise<void> {
    try {
      const clusterNode: ClusterNode = await ClusterNode.start(
        clusterNodeOptions(
          this._clusterOptions as ClusterOptions,
          this.host(),
          this.port(),
          this._events,
          this._logger,
        ),
      );
      this._clusterNode = clusterNode;
      const registry: ClusterRegistry = new ClusterRegistry(clusterNode);
      this._clusterRegistry = registry;

      const placement: ClusterPlacement = this.buildClusterPlacement(
        clusterNode,
        registry,
        remoting,
      );
      this._placement = placement;
      this._clusterPlacement = placement;
      this._ownsPlacement = true;

      await this.startRelocation(clusterNode, registry, placement);
    } catch (error: unknown) {
      // Only remoting is released here. The one cluster step that can fail is
      // ClusterNode.start, which closes its own gossip and data sockets as it rejects,
      // so a failure leaves no cluster listener bound; nothing after it, the placement
      // wiring or the relocator spawn, throws, so the node never outlives a failed
      // attach. A future fallible step added after the node starts must release it.
      this._remoting = null;
      await remoting.stop();
      throw error;
    }
  }

  /** Builds the cluster placement over `clusterNode`, wiring its resolver for
   * location-transparent lookup and its remote seam for spawning and recreating on
   * other nodes. The worker pool it wraps boots lazily on the first placement. */
  private buildClusterPlacement(
    clusterNode: ClusterNode,
    registry: ClusterRegistry,
    remoting: Remoting,
  ): ClusterPlacement {
    const resolver: ClusterResolver = new ClusterResolver({
      registry,
      handleFor: remoting.handleFor.bind(remoting),
      remotingAddressOf: clusterNode.remotingAddressOf.bind(clusterNode),
      self: clusterNode.address,
      systemName: this._name,
    });

    return new ClusterPlacement({
      registry,
      node: clusterNode.address,
      resolver,
      remote: {
        spawn: remoting.remoteSpawn.bind(remoting),
        recreate: remoting.remoteRecreate.bind(remoting),
        remotingAddressOf: clusterNode.remotingAddressOf.bind(clusterNode),
      },
      relocationDefault: (this._clusterOptions as ClusterOptions).relocation ?? true,
      bootInner: (onRelease: (name: string) => void): Promise<Placement> =>
        systemPlacement(this, {
          capacity: this._capacity,
          quiet: this._logger === discardLogger,
          onRelease,
        }),
    });
  }

  /** Spawns the relocation actor as a system actor and bridges the cluster runtime's
   * events onto the public stream and into it, so a departure the runtime reports
   * drives the coordinator's recovery. */
  private async startRelocation(
    clusterNode: ClusterNode,
    registry: ClusterRegistry,
    placement: ClusterPlacement,
  ): Promise<void> {
    const relocator: Relocator = new Relocator({
      scanPlacements: (): Promise<PlacementRecord[]> => registry.scanPlacements(),
      free: (name: string, deadOwner: string): Promise<void> =>
        registry.freeActorIf(name, deadOwner),
      recreate: placement.relocateTo.bind(placement),
      self: clusterNode.address,
      coordinator: (): string => clusterNode.coordinator(),
      members: (): readonly RelocationMember[] => clusterNode.members(),
      publish: (event: unknown): void => this.publishEvent(event),
      now: (): number => Date.now(),
      sweepIntervalMs: RELOCATION_SWEEP_INTERVAL_MS,
    });

    const relocatorPid: PID = await this.configPID(
      relocatorName,
      relocator,
      this._systemGuardian as PID,
    );
    this._relocator = relocatorPid;

    const bridge: StreamSubscriber = this.onClusterEvent.bind(this);
    this._clusterBridge = bridge;
    this._events.subscribe(bridge, CLUSTER_EVENT_TOPIC);
  }

  /**
   * Handles one cluster runtime event: re-publishes it on the public stream in its
   * `instanceof` form, and drives the relocation actor on a departure so the
   * coordinator recreates the departed node's actors. Subscribed to the cluster
   * topic, and callable directly to drive a recovery in a test.
   *
   * @internal
   */
  onClusterEvent(event: unknown): void {
    const clusterEvent: ClusterEvent = event as ClusterEvent;
    this.publishEvent(translateClusterEvent(clusterEvent, Date.now()));
    const relocator: PID | null = this._relocator;
    if (clusterEvent.type === ClusterEventType.nodeLeft && relocator !== null) {
      this.noSender().tell(relocator, new NodeDeparted(clusterEvent.address));
    }
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
    this._logger.info("actor system stopping", { name: this._name });
    try {
      await this.teardown();
    } catch (error: unknown) {
      this._logger.error("actor system failed to stop", { name: this._name, error });
      throw error;
    }

    this._logger.info("actor system stopped", { name: this._name });
  }

  /**
   * The teardown sequence run under {@link stop}'s logging: remoting, a graceful
   * cluster leave, then the placement, scheduler, passivation, and guardians.
   */
  private async teardown(): Promise<void> {
    // Remoting closes first, while the system still serves: every
    // connection is torn down at once, so in-flight remote asks fail
    // cleanly and no listener outlives the system.
    const remoting: Remoting | null = this._remoting;
    this._remoting = null;
    if (remoting !== null) {
      await remoting.stop();
    }

    // The cluster node leaves gracefully after remoting closes: it drains its
    // partitions to survivors and departs membership, so a clean shutdown is not
    // mistaken for a crash and does not churn the survivors' placement. A node
    // alone in its view has no peer to hand off to and departs at once, so this
    // never stalls a single-node system. The rest of teardown runs in a finally, so
    // a drain that reports an error still tears the system down and resets its
    // state rather than wedging it; the leave error then surfaces from stop().
    // Detach the cluster event bridge before the node departs, so no membership
    // event drives a relocation into a system that is tearing down. The relocation
    // actor itself stops with the guardian hierarchy below.
    const bridge: StreamSubscriber | null = this._clusterBridge;
    this._clusterBridge = null;
    this._relocator = null;
    if (bridge !== null) {
      this._events.unsubscribe(bridge, CLUSTER_EVENT_TOPIC);
    }

    const clusterNode: ClusterNode | null = this._clusterNode;
    this._clusterNode = null;
    this._clusterRegistry = null;
    this._clusterPlacement = null;
    try {
      if (clusterNode !== null) {
        await clusterNode.leave();
      }
    } finally {
      // The pool goes first, while this system still serves: workers drain their
      // own actors and their final dead letters still reach this stream. A facade
      // never owns its placement; the worker runtime tears that down.
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

    this._logger.debug("spawning actor", { actor: name });

    // Props is construction as data: the placement decides which
    // isolate builds the actor, booting the pool on first use.
    if (actor instanceof Props) {
      const placement = await this.ensurePlacement();
      const placed: PID = await placement.place(name, actor, options);
      this._logger.info("actor spawned", { actor: name });
      return placed;
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
    this._logger.info("actor spawned", { actor: name });
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
   * Places an actor on a node the options' strategy selects and returns its
   * handle: a live PID when the strategy lands on this node, a routed handle to
   * the owning node otherwise. The name is unique cluster-wide, so a lost race is
   * refused with {@link ErrActorAlreadyExists} rather than handed a second holder,
   * exactly like {@link spawn}; a caller that wants the existing actor resolves it
   * with {@link actorOf}.
   *
   * The strategy is a closed set, `roundRobin` by default: `roundRobin` spreads
   * placements by a cluster-wide counter, `random` picks a uniformly random
   * member, `local` places on the calling node, and `leastLoad` picks the member
   * owning the fewest actors. Every strategy treats live, ready members as equal
   * candidates and falls back to this node when no other is ready.
   *
   * Construction crosses as a recipe, so the actor's class must be registered
   * with `registerActor` on every node and its `Props` arguments must be plain
   * data, exactly as for a plain distributed spawn.
   *
   * @param name - The top-level name the actor holds, unique across the cluster.
   * @param props - The actor's construction as data.
   * @param options - The placement strategy and the data spawn options.
   *
   * @returns The placed actor's handle, local or routed.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when the system is not
   * running.
   * @throws The {@link ErrReservedName} sentinel for a reserved name, and the
   * {@link ErrInvalidActorName} sentinel for an invalid one.
   * @throws The {@link ErrClusteringDisabled} sentinel when this system was
   * created without a `cluster` configuration.
   * @throws The {@link ErrActorAlreadyExists} sentinel when the name is held
   * anywhere in the cluster, and the remote spawn failure otherwise.
   */
  async spawnOn(name: string, props: Props, options?: SpawnOnOptions): Promise<PID> {
    if (!this.isRunning() || this._userGuardian === null) {
      throw ErrActorSystemNotStarted;
    }

    if (isSystemName(name)) {
      throw ErrReservedName;
    }

    if (!isValidActorName(name)) {
      throw ErrInvalidActorName;
    }

    const placement: ClusterPlacement | null = this._clusterPlacement;
    const clusterNode: ClusterNode | null = this._clusterNode;
    const registry: ClusterRegistry | null = this._clusterRegistry;
    if (placement === null || clusterNode === null || registry === null) {
      throw ErrClusteringDisabled;
    }

    const owner: string = await selectOwner({
      strategy: options?.strategy ?? STRATEGY_ROUND_ROBIN,
      members: clusterNode.members(),
      self: clusterNode.address,
      registry,
      random: Math.random,
    });
    return placement.placeOn(name, props, owner, options);
  }

  /**
   * Creates the one cluster-wide instance of `name`, hosted on the coordinator,
   * and returns a handle to it. Unlike {@link spawn} and {@link spawnOn}, creating
   * a singleton is idempotent: the first caller claims the name and the actor is
   * built on the coordinator, and every later caller, on this node or any other,
   * receives a handle to that same instance rather than the {@link ErrActorAlreadyExists}
   * a duplicate spawn is refused with. When callers race with different `props`, the
   * winner's `props` win. A singleton is always relocatable, so it is recreated on a
   * surviving node when its host departs.
   *
   * The coordinator is the oldest live member, the one node every view agrees on, so
   * a singleton's location is predictable and it moves only when that node departs.
   * At-most-one rests on the same cluster-wide name claim every spawn uses, so a
   * `spawnSingleton(name)` and a `spawn(name)` anywhere in the cluster contend for the
   * one name: whichever creates it first holds it, and the other is a duplicate.
   *
   * @param name - The top-level name the single instance holds, unique across the cluster.
   * @param props - The actor's construction as data.
   * @param options - The data spawn options; the relocation flag is forced on.
   *
   * @returns The singleton's handle, local when this node is its host, routed otherwise.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when the system is not running.
   * @throws The {@link ErrReservedName} sentinel for a reserved name, and the
   * {@link ErrInvalidActorName} sentinel for an invalid one.
   * @throws The {@link ErrClusteringDisabled} sentinel when this system was created
   * without a `cluster` configuration.
   */
  async spawnSingleton(name: string, props: Props, options?: SpawnOptions): Promise<PID> {
    if (!this.isRunning() || this._userGuardian === null) {
      throw ErrActorSystemNotStarted;
    }

    if (isSystemName(name)) {
      throw ErrReservedName;
    }

    if (!isValidActorName(name)) {
      throw ErrInvalidActorName;
    }

    const placement: ClusterPlacement | null = this._clusterPlacement;
    const clusterNode: ClusterNode | null = this._clusterNode;
    if (placement === null || clusterNode === null || this._clusterRegistry === null) {
      throw ErrClusteringDisabled;
    }

    const singletonOptions: SpawnOptions = {
      ...options,
      relocatable: true,
      singleton: true,
    };

    for (let attempt: number = 0; attempt < SINGLETON_CLAIM_ATTEMPTS; attempt++) {
      const coordinator: string = clusterNode.coordinator();
      try {
        return await placement.placeOn(name, props, coordinator, singletonOptions);
      } catch (err: unknown) {
        if (err !== ErrActorAlreadyExists) {
          throw err;
        }

        // Lost the claim: a live instance already holds the name, so hand the caller
        // a handle to it rather than the duplicate error. It is the local instance
        // when this node hosts it, else a routed handle once the resolver view warms.
        // A winner that vanished between the loss and the resolve leaves the name free
        // to reclaim on the next pass.
        const existing: PID | undefined =
          this.actorOf(name) ?? (await placement.resolveActor(name));
        if (existing !== undefined) {
          return existing;
        }
      }
    }

    throw ErrActorAlreadyExists;
  }

  /**
   * Recreates a departed node's actor on this node from its rebuilt `Props`, gated
   * on the placement still naming `deadOwner`, and resolves whether this node took
   * the name. The relocation control request lands here after rebuilding the
   * actor's construction from the shipped recipe.
   *
   * @throws The {@link ErrClusteringDisabled} sentinel when this system is not
   * clustered, so a relocate to a node without a placement is a clean failure.
   *
   * @internal
   */
  recreatePlaced(
    name: string,
    props: Props,
    options: SpawnOptions,
    deadOwner: string,
  ): Promise<boolean> {
    const placement: ClusterPlacement | null = this._clusterPlacement;
    if (placement === null) {
      return Promise.reject(ErrClusteringDisabled);
    }

    return placement.recreate(name, props, options, deadOwner);
  }

  /**
   * Boots the pool seam on first use, so a program that spawns only
   * instances never pays for workers.
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
    const placement = await systemPlacement(this, {
      capacity: this._capacity,
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
   * Resolves a top-level actor by name, awaiting the cluster registry when the name
   * lives on another node. Unlike the synchronous {@link actorOf}, which reads a warm
   * local view and returns `undefined` for a cluster name it has not learned yet, this
   * warms the view from the registry first, so the routed handle to a name placed
   * anywhere in the cluster resolves on the first call rather than after a cold miss.
   * A local actor or an unclustered system resolves without any network round trip.
   *
   * @param name - The actor name to look up.
   *
   * @returns The actor's PID, local or routed, or `undefined` when no running actor
   * holds the name anywhere in the cluster.
   */
  async actorOfAsync(name: string): Promise<PID | undefined> {
    const local: PID | undefined = this.actorOf(name);
    if (local !== undefined) {
      return local;
    }

    return this._clusterPlacement?.resolveActor(name);
  }

  /**
   * Resolves a top-level actor by name on the remote node at
   * `host:port` and returns its PID. The handle sends, asks, and
   * watches over the network exactly as a local PID does; liveness
   * across nodes is not synchronously knowable, so `isRunning()` on it
   * reports false and `watch` is the way to observe its stop.
   *
   * @param host - The remote node's host.
   * @param port - The remote node's port.
   * @param name - The top-level actor name to resolve there.
   *
   * @returns The remote actor's PID, or `undefined` when no running
   * top-level actor holds the name on that node.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when this
   * system is not running.
   * @throws The {@link ErrRemotingDisabled} sentinel when this system
   * was created without a `remote` configuration.
   * @throws The dial or transport failure when the node cannot be
   * reached, and the {@link ErrRequestTimeout} sentinel when it does
   * not answer in time.
   */
  async remoteLookup(host: string, port: number, name: string): Promise<PID | undefined> {
    return this.requireRemoting().remoteLookup(host, port, name);
  }

  /**
   * Spawns a top-level actor on the remote node at `host:port` and
   * returns its PID. Construction crosses by name: the actor class must
   * be registered with `registerActor` on both nodes, under a name no
   * other registered class shares on the receiving one, and the `Props`
   * arguments must be plain data. Spawn options that are live objects
   * are refused exactly as they are for a placed spawn; `reentrancy`,
   * being data, travels.
   *
   * @param host - The remote node's host.
   * @param port - The remote node's port.
   * @param name - The top-level name the actor will hold there.
   * @param props - The actor's construction as data.
   * @param options - The spawn options; only data options may travel.
   *
   * @returns The remote actor's PID once its `preStart` resolved there.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when this
   * system is not running.
   * @throws The {@link ErrRemotingDisabled} sentinel when this system
   * was created without a `remote` configuration.
   * @throws An {@link ActorNotRegisteredError} when the class is not
   * registered locally, or not resolvable by name on the remote node.
   * @throws The remote spawn failure otherwise, sentinel identity
   * preserved: {@link ErrActorAlreadyExists} for a held name, an
   * `ActorInitializationError` for a failing `preStart`, and friends.
   */
  async remoteSpawn(
    host: string,
    port: number,
    name: string,
    props: Props,
    options?: SpawnOptions,
  ): Promise<PID> {
    return this.requireRemoting().remoteSpawn(host, port, name, props, options);
  }

  /**
   * Recreates a departed node's actor on the node at `host:port` from its recipe,
   * gated on the record still naming `deadOwner` there, resolving whether that node
   * took the name. The runtime seam the relocation actor ships a remote recreate
   * through; a build failure or an unclustered target settles the promise with the
   * error.
   *
   * @throws The {@link ErrRemotingDisabled} sentinel when this system was created
   * without a `remote` configuration.
   *
   * @internal
   */
  async remoteRecreate(
    host: string,
    port: number,
    name: string,
    recipe: RecreateRecipe,
    singleton: boolean,
    deadOwner: string,
  ): Promise<boolean> {
    return this.requireRemoting().remoteRecreate(host, port, name, recipe, singleton, deadOwner);
  }

  /**
   * Restarts the named top-level actor on the remote node at
   * `host:port` and returns its PID. The actor re-initializes in place
   * through its lifecycle hooks: same path, same incarnation, fresh
   * state; watchers receive nothing, because a restart is not a stop.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when this
   * system is not running.
   * @throws The {@link ErrRemotingDisabled} sentinel when this system
   * was created without a `remote` configuration.
   * @throws An {@link ActorNotFoundError} when no running top-level
   * actor holds the name on that node.
   */
  async remoteReSpawn(host: string, port: number, name: string): Promise<PID> {
    return this.requireRemoting().remoteReSpawn(host, port, name);
  }

  /**
   * Stops the named top-level actor on the remote node at `host:port`
   * gracefully: its mailbox drains, `postStop` runs, and its watchers
   * receive `Terminated`. A name no running actor holds there is
   * already stopped, so the call succeeds idempotently.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when this
   * system is not running.
   * @throws The {@link ErrRemotingDisabled} sentinel when this system
   * was created without a `remote` configuration.
   */
  async remoteStop(host: string, port: number, name: string): Promise<void> {
    return this.requireRemoting().remoteStop(host, port, name);
  }

  /**
   * Adopts the node's advertised address on a worker facade, before
   * the system starts, so every isolate mints and resolves the same
   * canonical paths. Runtime plumbing for the worker entry; the main
   * isolate's address comes from its own remote configuration instead.
   *
   * @internal
   */
  adoptAddress(host: string, port: number): void {
    // Adoption after start would split the path space between paths
    // already minted and paths to come; misuse must be loud.
    if (this.isRunning()) {
      throw new Error("an address cannot be adopted on a running system");
    }

    this._address = { system: this._name, host, port };
  }

  /**
   * The handle that carries an envelope naming another node's path
   * onward, or undefined when the path belongs to this node or nothing
   * here can route it. Runtime plumbing for the isolate transport: an
   * envelope arriving from a worker isolate can name a foreign path (a
   * placed actor answering a remote sender). On the main isolate the
   * remoting seam serves it; on a worker facade the attached resolver
   * routes it to the main isolate, whose own fallback carries it over
   * the wire, so the hop count is placement's concern and never the
   * sender's.
   *
   * @internal
   */
  remoteHandle(path: Path): PID | undefined {
    return this._remoting?.handleFor(path) ?? this._foreignResolver?.(path);
  }

  /**
   * Hands a worker facade its foreign-path resolver: the worker
   * runtime attaches one that routes another node's paths to the main
   * isolate. Runtime plumbing; the main isolate resolves through its
   * remoting seam instead and never attaches one.
   *
   * @internal
   */
  attachForeignResolver(resolver: (path: Path) => PID | undefined): void {
    this._foreignResolver = resolver;
  }

  /**
   * The route to the worker isolate owning a placed top-level name, or
   * undefined when no placement holds it away from this isolate.
   * Runtime plumbing for the remoting seam, which mints delivery
   * handles around inbound paths itself and needs only the route.
   *
   * @internal
   */
  placedRouteOf(name: string): IsolateRoute | undefined {
    return this._placement?.routeOf(name);
  }

  /**
   * Restarts a top-level actor this node placed on a worker isolate,
   * in place: same PID, same incarnation, fresh state. Runtime
   * plumbing for the remote control endpoint; rejects with
   * {@link ActorNotFoundError} when no placement holds the name.
   *
   * @internal
   */
  respawnPlaced(name: string): Promise<void> {
    const placement: Placement | null = this._placement;
    if (placement === null) {
      return Promise.reject(new ActorNotFoundError(name));
    }

    return placement.respawn(name);
  }

  /**
   * Stops a top-level actor this node placed on a worker isolate,
   * gracefully and idempotently. Runtime plumbing for the remote
   * control endpoint.
   *
   * @internal
   */
  stopPlaced(name: string): Promise<void> {
    const placement: Placement | null = this._placement;
    if (placement === null) {
      return Promise.resolve();
    }

    return placement.stopActor(name);
  }

  /** Returns the live remoting layer, or throws the reason there is
   * none: an unstarted system, or one without a remote configuration. */
  private requireRemoting(): Remoting {
    if (!this.isRunning()) {
      throw ErrActorSystemNotStarted;
    }

    if (this._remoting === null) {
      throw ErrRemotingDisabled;
    }

    return this._remoting;
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
