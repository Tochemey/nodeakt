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

import { type ClusterEvent, type ClusterEventSink, ClusterEventType } from "./clustering.events";
import { discardLogger } from "./discard.logger";
import { AntiEntropy } from "./kv/anti.entropy";
import {
  DEFAULT_PARTITION_COUNT,
  DEFAULT_REPLICA_COUNT,
  DEFAULT_WRITE_QUORUM,
  DRAIN_POLL_INTERVAL_MS,
  JANITOR_INTERVAL_MS,
  LEAVE_DRAIN_TIMEOUT_MS,
  REPAIR_INTERVAL_MS,
  STABLE_VIEW_QUIET_MS,
  TABLE_PUSH_INTERVAL_MS,
} from "./kv/constants";
import { Coordinator } from "./kv/coordinator";
import { Engine } from "./kv/engine";
import { isLiveValue } from "./kv/entry";
import { ClusterUnavailableError, KvProtocolError } from "./kv/errors";
import { FragmentTransfer } from "./kv/fragment";
import type {
  ClusterMember,
  ClusterView,
  Entry,
  KvTransport,
  ScanEntry,
  WriteOp,
  WriteResult,
} from "./kv/ports";
import { Recovery } from "./kv/recovery";
import { Replicator } from "./kv/replication";
import { KeepMajorityResolver, type SplitBrainStrategy } from "./kv/resolver";
import { PartitionRing } from "./kv/ring";
import type { PartitionPlacement, RoutingTable } from "./kv/routing.table";
import {
  MSG_ENTRIES_REQUEST,
  MSG_FRAGMENT_PUSH,
  MSG_FRAGMENT_REQUEST,
  MSG_KEY_VERSIONS_REQUEST,
  MSG_PEEK_REQUEST,
  MSG_READ_REQUEST,
  MSG_REPLICATE,
  MSG_SYNC_DIGEST,
  MSG_TABLE,
  MSG_WRITE_REQUEST,
  messageType,
} from "./kv/wire";
import type { Logger } from "./logger";

/** A scheduled callback that can be cancelled before it fires. @internal */
export interface ClusterTimer {
  /** Prevents the callback from firing; idempotent once fired or already cancelled. */
  cancel(): void;
}

/** Epoch time and callback scheduling the periodic work of one node runs on. @internal */
export interface ClusterClock {
  /** Current epoch time in milliseconds, for expiry and last-write-wins order. */
  now(): number;
  /** Schedules `callback` to run once after at least `delayMs` milliseconds. */
  schedule(delayMs: number, callback: () => void): ClusterTimer;
}

/** Answers one inbound request with its response bytes, mirroring a store server. */
type InboundHandler = (from: string, body: Uint8Array) => Promise<Uint8Array> | Uint8Array;

/** One anti-entropy comparison to run: a partition this node primaries against a replica. */
interface RepairTarget {
  readonly partition: number;
  readonly peer: string;
}

/**
 * Request message types the canonical dispatch hands to the replication layer:
 * forwarded writes and reads, peeks, backup replication, and fragment moves.
 */
const REPLICATOR_TYPES: readonly number[] = [
  MSG_WRITE_REQUEST,
  MSG_READ_REQUEST,
  MSG_PEEK_REQUEST,
  MSG_REPLICATE,
  MSG_FRAGMENT_REQUEST,
  MSG_FRAGMENT_PUSH,
];

/** Request message types the canonical dispatch hands to the anti-entropy responder. */
const ANTI_ENTROPY_TYPES: readonly number[] = [
  MSG_SYNC_DIGEST,
  MSG_KEY_VERSIONS_REQUEST,
  MSG_ENTRIES_REQUEST,
];

/** Construction parameters for a {@link Cluster}. @internal */
export interface ClusterOptions {
  /** Who is in the cluster, oldest first, as the membership adapter reports it. */
  readonly view: ClusterView;
  /** Addressed request/response carrier the store frames its own bytes over. */
  readonly transport: KvTransport;
  /** Epoch time and the scheduler the periodic work runs on; injected for deterministic tests. */
  readonly clock: ClusterClock;
  /** Immutable partition count shared by every node; defaults to the cluster constant. */
  readonly partitionCount?: number;
  /** Intended replica set size including the primary; defaults to the cluster constant. */
  readonly replicaCount?: number;
  /** Acknowledgments, counting the primary, a synchronous write awaits; defaults to the constant. */
  readonly writeQuorum?: number;
  /** Minimum member quorum enabling the split-brain resolver; one, the default, disables it. */
  readonly minimumMemberQuorum?: number;
  /** Receives this node's cluster lifecycle events; omitted when nothing observes them. */
  readonly events?: ClusterEventSink;
  /** Logger the store reports routing and recovery activity through; drops everything by default. */
  readonly logger?: Logger;
}

/**
 * The clustering engine for one node.
 *
 * It constructs and wires the store's pieces, the {@link Engine}, the
 * {@link Coordinator}, the {@link Replicator}, the {@link Recovery} orchestrator,
 * and an {@link AntiEntropy}, over the two ports the store defines: a
 * {@link ClusterView} for who is in the cluster and a {@link KvTransport} for
 * addressed request and response. It never learns of actors, sockets, or membership
 * internals directly; the adapters behind those ports do, so the same engine runs unchanged
 * against real sockets and against the simulation harness.
 *
 * Membership is the only topology signal. On every membership change the engine
 * recomputes the coordinator, which is simply the oldest live member, so when the
 * coordinator leaves the next-oldest node takes over on the same event with no
 * election. The coordinator recomputes and pushes the routing table; every node,
 * coordinator or follower, installs the table it holds into the {@link Replicator}
 * and hands it to {@link Recovery}. A single {@link listen} handler is the one
 * authoritative router from a message kind to its server, replacing the per-test
 * routers the store's unit tests hand-rolled.
 *
 * Three periodic timers run off the injected clock: the coordinator re-pushes the
 * table so a node that missed a push heals, the janitor reaps expired entries, and
 * anti-entropy reconciles one of this node's partitions against a replica per tick.
 *
 * A split-brain resolver decides, on every membership change, whether this half of
 * a partitioned cluster keeps serving. It compares the members this node currently
 * reaches against the size the cluster held before the change, a size the node
 * records only once the view has been quiet for a settling window, so a minority
 * that has lost the larger half stops while the majority serves on. A stopped half
 * refuses reads and writes and relinquishes its coordinator duty until the view
 * heals. The resolver is disabled by default, correct for a single node that cannot
 * fork, and enabled by a member quorum above one.
 *
 * It is cluster-runtime infrastructure the actor system runs, not a surface an
 * application uses directly.
 *
 * @internal
 */
export class Cluster {
  /** Membership view, oldest first, and the source of this node's identity. */
  readonly #view: ClusterView;
  /** This node's canonical identity, compared against each partition's primary. */
  readonly #self: string;
  /** Addressed request/response carrier shared by every server this node runs. */
  readonly #transport: KvTransport;
  /** Epoch time and scheduler the periodic timers run on. */
  readonly #clock: ClusterClock;
  /** Immutable partition count every ring and table this node builds covers. */
  readonly #partitionCount: number;
  /** Intended replica set size including the primary, for ring backups and placement. */
  readonly #replicaCount: number;
  /** Local store engine backing the primary write path and the backup merge. */
  readonly #engine: Engine;
  /** Oldest-member coordinator authority over the routing table. */
  readonly #coordinator: Coordinator;
  /** Node-level router over the partition primaries. */
  readonly #replicator: Replicator;
  /** Crash reconcile, refill, and drain orchestrator. */
  readonly #recovery: Recovery;
  /** Bucketed-digest reconciler this node runs against its replicas on a timer. */
  readonly #antiEntropy: AntiEntropy;
  /** Reads a partition this node does not own, without merging, for a cluster-wide scan. */
  readonly #transfer: FragmentTransfer;
  /** Decides whether this half of a partitioned cluster keeps serving. */
  readonly #resolver: SplitBrainStrategy;
  /** Canonical route from a message type to the server that answers it. */
  readonly #routes: Map<number, InboundHandler> = new Map<number, InboundHandler>();
  /** One live periodic timer per loop, cancelled on stop; each loop reschedules its own slot. */
  readonly #timers: ClusterTimer[] = [];
  /** The routing table currently installed, or `undefined` before the first one. */
  #table: RoutingTable | undefined;
  /** Whether this half serves operations; false once the resolver stops it, true once it heals. */
  #serving: boolean = true;
  /** Member names of the last quiet, serving view; the split-brain majority denominator. */
  #lastStable: readonly string[] = [];
  /** Epoch time the resolver last stopped this half, for measuring how long a rejoin was away. */
  #stoppedAt: number = 0;
  /** Pending timer that promotes the current view to {@link #lastStable}; rearmed on each change. */
  #stableTimer: ClusterTimer | undefined;
  /** Receives this node's lifecycle events, or `undefined` when nothing observes them. */
  readonly #events: ClusterEventSink | undefined;
  /** Logger for routing and recovery activity, tagged with its component. */
  readonly #log: Logger;
  /** Member names at the last event diff, for detecting joins, departures, and coordinator change. */
  #previousMembers: readonly string[] = [];
  /** Departed members awaiting the repair of their partitions before `node-left` is reported. */
  #pendingLeft: string[] = [];
  /** Backstop timer that reports pending departures if a repair wedges and never settles. */
  #leftBackstop: ClusterTimer | undefined;
  /** The anti-entropy comparisons this node cycles through, recomputed on each table. */
  #repairTargets: readonly RepairTarget[] = [];
  /** Rolling index into {@link #repairTargets}, advanced one per repair tick. */
  #repairCursor: number = 0;
  /** Unsubscribes this node from membership change; set by {@link start}. */
  #unsubscribe: (() => void) | undefined;

  /** @param options The two ports, the clock, and the optional sizing constants. */
  constructor(options: ClusterOptions) {
    const partitionCount: number = options.partitionCount ?? DEFAULT_PARTITION_COUNT;
    const replicaCount: number = options.replicaCount ?? DEFAULT_REPLICA_COUNT;
    const writeQuorum: number = options.writeQuorum ?? DEFAULT_WRITE_QUORUM;
    this.#view = options.view;
    this.#self = options.view.self;
    this.#transport = options.transport;
    this.#clock = options.clock;
    this.#partitionCount = partitionCount;
    this.#replicaCount = replicaCount;
    this.#engine = new Engine(options.view.self, partitionCount, (): number => this.#clock.now());
    this.#recovery = new Recovery(this.#engine, options.transport, options.view, { replicaCount });
    this.#replicator = new Replicator(options.view.self, this.#engine, options.transport, {
      replicaCount,
      writeQuorum,
      isRecovering: (partition: number): boolean => this.#recovery.isRecovering(partition),
    });
    this.#coordinator = new Coordinator(
      options.view,
      options.transport,
      partitionCount,
      (): readonly number[] => this.#engine.heldPartitions(),
    );
    this.#antiEntropy = new AntiEntropy(this.#engine, options.transport);
    this.#transfer = new FragmentTransfer(this.#engine, options.transport);
    this.#resolver = new KeepMajorityResolver(options.minimumMemberQuorum);
    this.#events = options.events;
    this.#log = options.logger ?? discardLogger;
    this.#installRoutes();
  }

  /**
   * Installs the canonical inbound handler, subscribes to membership change, and
   * arms the periodic timers. Formation is driven by the first membership event,
   * as in production, where the view first fills once the join has merged the
   * cluster.
   */
  async start(): Promise<void> {
    this.#transport.listen(
      (from: string, body: Uint8Array): Promise<Uint8Array> => this.#dispatch(from, body),
    );
    this.#unsubscribe = this.#view.onChange((): void => this.#onView());
    this.#everyTick(TABLE_PUSH_INTERVAL_MS, (): void => this.#rebalance(false));
    this.#everyTick(JANITOR_INTERVAL_MS, (): void => {
      this.#engine.sweep(this.#clock.now());
    });
    this.#everyTick(REPAIR_INTERVAL_MS, (): void => this.#repairTick());
  }

  /** Cancels the timers, unsubscribes from membership, and releases the transport. */
  async stop(): Promise<void> {
    for (const timer of this.#timers) {
      timer.cancel();
    }

    this.#timers.length = 0;
    this.#stableTimer?.cancel();
    this.#stableTimer = undefined;
    this.#leftBackstop?.cancel();
    this.#leftBackstop = undefined;
    this.#unsubscribe?.();
    this.#unsubscribe = undefined;
    await this.#transport.stop();
  }

  /**
   * Submits a mutation and resolves with its outcome, routing to the partition
   * primary. Rejects with {@link ClusterUnavailableError} while the split-brain
   * resolver has stopped this half of a partitioned cluster.
   */
  write(op: WriteOp): Promise<WriteResult> {
    if (!this.#serving) {
      return Promise.reject(new ClusterUnavailableError());
    }

    return this.#replicator.write(op);
  }

  /**
   * Reads the live value for `key`, or `undefined`, routing to the partition
   * primary. Rejects with {@link ClusterUnavailableError} while the split-brain
   * resolver has stopped this half of a partitioned cluster.
   */
  read(key: string): Promise<Entry | undefined> {
    if (!this.#serving) {
      return Promise.reject(new ClusterUnavailableError());
    }

    return this.#replicator.read(key);
  }

  /**
   * Reads every live key and value across the cluster by reading each partition
   * from its primary. A partition this node primaries is read from the local store;
   * one it does not is read from the primary without merging, so the scan observes
   * remote state without disturbing its own. The reads run concurrently. Rejects
   * with {@link ClusterUnavailableError} while the split-brain resolver has stopped
   * this half, and yields nothing before the node holds a routing table.
   */
  async scan(): Promise<ScanEntry[]> {
    if (!this.#serving) {
      throw new ClusterUnavailableError();
    }

    const table: RoutingTable | undefined = this.#table;
    if (table === undefined) {
      return [];
    }

    const nowMs: number = this.#clock.now();
    const partitions: number[] = Array.from(
      { length: table.partitionCount },
      (_unused: unknown, id: number): number => id,
    );
    const pages: ScanEntry[][] = await Promise.all(
      partitions.map(
        (partition: number): Promise<ScanEntry[]> => this.#scanPartition(partition, table, nowMs),
      ),
    );
    return pages.flat();
  }

  /** Whether this half currently serves operations, false once the resolver has stopped it. @internal */
  get serving(): boolean {
    return this.#serving;
  }

  /** How many partitions this node currently holds a fragment of. @internal */
  get heldPartitionCount(): number {
    return this.#engine.heldPartitions().length;
  }

  /**
   * The version and per-partition primaries of the routing table this node has
   * installed, or `undefined` before it holds one. Two nodes that agree on this
   * hold the same table; it is how a test reads convergence. @internal
   */
  get routingSignature(): { version: bigint; primaries: readonly string[] } | undefined {
    const table: RoutingTable | undefined = this.#table;
    if (table === undefined) {
      return undefined;
    }

    const primaries: string[] = [];
    for (let id: number = 0; id < table.partitionCount; id += 1) {
      primaries.push(table.primary(id));
    }

    return { version: table.version, primaries };
  }

  /**
   * Resolves once this node holds no partition, or `timeoutMs` has elapsed. A
   * graceful leave awaits this after announcing itself draining, so it departs
   * only once its fragments have moved to their new owners. The timeout is the
   * backstop for a drain that cannot finish; its remainder is recovered as a crash.
   *
   * @internal
   */
  async awaitDrained(timeoutMs: number): Promise<void> {
    // A node alone in its view has no peer to hand its partitions to, so it can
    // never drain to zero; it departs at once rather than spin out the backstop.
    if (this.#view.members().length <= 1) {
      return;
    }

    const deadline: number = this.#clock.now() + timeoutMs;
    while (this.#engine.heldPartitions().length > 0 && this.#clock.now() < deadline) {
      await this.#delay(DRAIN_POLL_INTERVAL_MS);
    }
  }

  /** Populates the route map once at construction. */
  #installRoutes(): void {
    const toReplicator: InboundHandler = (from: string, body: Uint8Array): Promise<Uint8Array> =>
      this.#replicator.receive(from, body);
    const toAntiEntropy: InboundHandler = (from: string, body: Uint8Array): Promise<Uint8Array> =>
      this.#antiEntropy.receive(from, body);
    this.#routes.set(
      MSG_TABLE,
      (from: string, body: Uint8Array): Uint8Array => this.#serveTable(from, body),
    );
    for (const type of REPLICATOR_TYPES) {
      this.#routes.set(type, toReplicator);
    }

    for (const type of ANTI_ENTROPY_TYPES) {
      this.#routes.set(type, toAntiEntropy);
    }
  }

  /**
   * Routes one inbound request to the server that owns its kind and answers with
   * that server's bytes. It is `async` so a synchronous throw, from an undecodable
   * envelope or an unrouted kind, surfaces as a rejection the carrier turns into an
   * error reply rather than an uncaught error.
   */
  async #dispatch(from: string, body: Uint8Array): Promise<Uint8Array> {
    const handler: InboundHandler | undefined = this.#routes.get(messageType(body));
    if (handler === undefined) {
      throw new KvProtocolError("clustering received an unroutable message");
    }

    return handler(from, body);
  }

  /** Adopts a pushed table on this follower, installs it, and returns the report. */
  #serveTable(from: string, body: Uint8Array): Uint8Array {
    const response: Uint8Array = this.#coordinator.receive(from, body);
    this.#applyLocalTable();
    return response;
  }

  /**
   * Reacts to a membership change: re-runs the split-brain verdict against the
   * last stable view, emits the lifecycle events the change produced, then, only
   * while this half still serves, recomputes and pushes the routing table.
   */
  #onView(): void {
    this.#applyResolver();
    this.#emitMembershipEvents();
    this.#rebalance(true);
  }

  /**
   * Emits the membership transitions since the last change: a `node-joined` for
   * each new peer, a `coordinator-changed` when the oldest member differs, and a
   * queued `node-left` for each departed peer, held behind the repair gate. A node
   * reports only its peers, never itself, so a subscriber hears about the rest of
   * the cluster and not its own arrival or exit. Self stays in the tracked set for
   * coordinator detection, since the oldest member may be this node. A no-op when
   * nothing observes the events.
   */
  #emitMembershipEvents(): void {
    if (this.#events === undefined) {
      return;
    }

    const current: string[] = this.#view.members().map((each: ClusterMember): string => each.name);
    const previous: Set<string> = new Set(this.#previousMembers);
    const present: Set<string> = new Set(current);
    for (const name of current) {
      if (name !== this.#self && !previous.has(name)) {
        this.#emit({ type: ClusterEventType.nodeJoined, address: name });
      }
    }

    for (const name of this.#previousMembers) {
      if (name !== this.#self && !present.has(name)) {
        this.#pendingLeft.push(name);
      }
    }

    if (this.#pendingLeft.length > 0 && this.#leftBackstop === undefined) {
      this.#leftBackstop = this.#clock.schedule(LEAVE_DRAIN_TIMEOUT_MS, (): void =>
        this.#releaseLeft(),
      );
    }

    const previousCoordinator: string | undefined = this.#previousMembers[0];
    const currentCoordinator: string | undefined = current[0];
    if (currentCoordinator !== undefined && currentCoordinator !== previousCoordinator) {
      this.#emit({ type: ClusterEventType.coordinatorChanged, coordinator: currentCoordinator });
    }

    this.#previousMembers = current;
  }

  /**
   * Reports every departure whose repair epoch has closed, cancelling the backstop.
   * The routing table this node adopts settles its recovery moves before this
   * runs, so a `node-left` reaches a consumer only once the departed node's
   * partitions have been promoted from their backups.
   */
  #releaseLeft(): void {
    this.#leftBackstop?.cancel();
    this.#leftBackstop = undefined;
    const pending: readonly string[] = this.#pendingLeft;
    this.#pendingLeft = [];
    this.#reportDepartures(pending);
  }

  /**
   * Emits a `node-left` for each name still absent from the current view. A node
   * that departed and rejoined inside its repair window is a member again by the
   * time its gate opens, so its departure is dropped rather than reported against
   * a node that is present.
   */
  #reportDepartures(names: readonly string[]): void {
    const present: Set<string> = new Set(
      this.#view.members().map((each: ClusterMember): string => each.name),
    );
    for (const name of names) {
      if (!present.has(name)) {
        this.#emit({ type: ClusterEventType.nodeLeft, address: name });
      }
    }
  }

  /** Hands one event to the sink, if one is watching. */
  #emit(event: ClusterEvent): void {
    this.#events?.(event);
  }

  /**
   * Recomputes whether this half keeps serving and rearms the quiet-window advance
   * of the stable baseline. The verdict weighs the members this node currently
   * reaches against the size the cluster held before the change, so a partitioned
   * minority stops while the majority serves on. A stop stamps the moment for a
   * later rejoin to measure its absence against; a heal reseeds when that absence
   * outran the tombstone window.
   */
  #applyResolver(): void {
    const reachable: Set<string> = new Set(
      this.#view.members().map((each: ClusterMember): string => each.name),
    );
    const survives: boolean = this.#resolver.survives(reachable, this.#lastStable);
    if (survives && !this.#serving) {
      this.#onHeal();
    }

    if (!survives && this.#serving) {
      this.#stoppedAt = this.#clock.now();
    }

    this.#serving = survives;
    this.#armStable();
  }

  /**
   * Reacts to this half rejoining a healthy cluster. A node the resolver stopped
   * kept serving nothing but held its now-stale fragments; if it was away longer
   * than the tombstone lifetime, the deletes that would veto its stale keys have
   * been reaped elsewhere, so it discards every fragment rather than let the
   * healed cluster resurrect a reaped key through a read gather or anti-entropy.
   *
   * Dropping without an immediate re-pull is safe: the table it adopts next keeps
   * each partition it still owns fragmented with the majority owner that stayed,
   * because the coordinator retains a live previous owner. Reads therefore gather
   * across both owners and the majority answers, conditional writes are refused
   * while a partition is fragmented, and the majority streams its fragment back
   * under drain-on-demotion. So the node serves correct reads and refills from a
   * live owner, never an empty local copy, and without the stale keys it dropped.
   */
  #onHeal(): void {
    if (!Recovery.shouldReseed(this.#clock.now() - this.#stoppedAt)) {
      return;
    }

    for (const partition of this.#engine.heldPartitions()) {
      this.#engine.drop(partition);
    }
  }

  /**
   * Restarts the timer that promotes the current view to {@link #lastStable}. Any
   * further membership change cancels and rearms it, so the baseline records only
   * a view that has held unchanged across the quiet window.
   */
  #armStable(): void {
    this.#stableTimer?.cancel();
    this.#stableTimer = this.#clock.schedule(STABLE_VIEW_QUIET_MS, (): void => {
      this.#promoteStable();
    });
  }

  /**
   * Records the current members as the split-brain baseline, but only while this
   * half serves, so a stopped minority never adopts its shrunken size as the
   * denominator and can never vote itself back into a majority.
   */
  #promoteStable(): void {
    if (!this.#serving) {
      return;
    }

    this.#lastStable = this.#view.members().map((each: ClusterMember): string => each.name);
  }

  /**
   * Recomputes and pushes the table when coordinator, then installs what this node
   * holds. A rebalance driven by a membership change (`announce`) reports its start
   * and completion; the periodic re-push, which heals a missed push without moving
   * ownership, stays quiet.
   */
  #rebalance(announce: boolean): void {
    // A half the resolver has stopped relinquishes its coordinator duty: it pushes
    // no table and leaves the surviving majority to own the topology.
    if (!this.#serving) {
      return;
    }

    const coordinating: boolean = announce && this.#coordinator.isCoordinator();
    if (coordinating) {
      this.#emit({ type: ClusterEventType.rebalanceStarted });
    }

    // Membership is the only topology signal: recompute and push the table when
    // this node is coordinator, then install what it holds. A rebalance settles
    // even when a push fails, so this chain does not reject in practice; the next
    // membership change, or the periodic re-push, heals a missed push.
    void this.#coordinator.rebalance().then((): void => {
      this.#applyLocalTable();
      if (coordinating) {
        this.#emit({ type: ClusterEventType.rebalanceCompleted });
      }
    });
  }

  /**
   * Installs the table this node currently holds into the replicator, hands it to
   * recovery for crash reconcile and refill, and drains any partition this node was
   * demoted from but still holds. A no-op before the node holds any table or when
   * the held version is not newer than the installed one.
   */
  #applyLocalTable(): void {
    // A half the resolver has stopped owns no topology, so it installs no table
    // and drives no recovery even when a stale rebalance continuation or an inbound
    // push reaches it after the stop.
    if (!this.#serving) {
      return;
    }

    const table: RoutingTable | undefined = this.#coordinator.currentTable();
    if (table === undefined) {
      return;
    }

    if (this.#table !== undefined && table.version <= this.#table.version) {
      return;
    }

    const ring: PartitionRing = new PartitionRing(this.#ringMembers(), this.#partitionCount);
    this.#replicator.install(table, ring);
    this.#table = table;
    this.#log.info("routing table updated", {
      version: table.version.toString(),
      partitions: this.#partitionCount,
      primaried: this.#engine.heldPartitions().length,
    });
    this.#repairTargets = this.#computeRepairTargets(table, ring);
    this.#repairCursor = 0;
    // This table reflects the departures observed up to now, so its recovery is the
    // repair epoch those departures wait on: claim them and report each once this
    // table's own crash reconcile and refill finish, not merely the next table's.
    const claimed: readonly string[] = this.#pendingLeft;
    this.#pendingLeft = [];
    this.#leftBackstop?.cancel();
    this.#leftBackstop = undefined;
    void this.#recovery.onTable(table, ring).then((): void => this.#reportDepartures(claimed));
    void this.#recovery.drain(table, ring);
  }

  /**
   * The partitions this node primaries paired with each of their replicas, the
   * comparisons the repair timer cycles through. Empty when this node primaries no
   * partition, or when every one it primaries has no replica.
   */
  #computeRepairTargets(table: RoutingTable, ring: PartitionRing): readonly RepairTarget[] {
    const targets: RepairTarget[] = [];
    for (let partition: number = 0; partition < table.partitionCount; partition += 1) {
      const placement: PartitionPlacement = table.placement(partition, ring, this.#replicaCount);
      if (placement.primary !== this.#self) {
        continue;
      }

      for (const peer of placement.backups) {
        targets.push({ partition, peer });
      }
    }

    return targets;
  }

  /**
   * The live entries of `partition`, read from the local store when this node
   * primaries it, otherwise read from its primary without merging. A tombstone or
   * expired entry is dropped, so only present state is reported.
   *
   * This reads the primary alone, unlike a point read, which gathers across a
   * partition's owners while it recovers. A scan is a best-effort observation of
   * remote state, so a partition whose primary is still refilling from its backups
   * may under-report until recovery settles; a point read of a specific key stays
   * complete throughout. Scanning the primary keeps a cluster-wide sweep to one
   * request per partition rather than one per owner.
   */
  async #scanPartition(
    partition: number,
    table: RoutingTable,
    nowMs: number,
  ): Promise<ScanEntry[]> {
    const primary: string = table.primary(partition);
    const entries: readonly Entry[] =
      primary === this.#self
        ? this.#engine.snapshot(partition)
        : await this.#transfer.collect(partition, primary);
    const live: ScanEntry[] = [];
    for (const entry of entries) {
      if (isLiveValue(entry, nowMs)) {
        live.push({ key: entry.key, value: entry.value as Uint8Array });
      }
    }

    return live;
  }

  /** Runs the next anti-entropy comparison in the cycle, or nothing when there are none. */
  #repairTick(): void {
    if (!this.#serving) {
      return;
    }

    const targets: readonly RepairTarget[] = this.#repairTargets;
    if (targets.length === 0) {
      return;
    }

    const target: RepairTarget = targets[this.#repairCursor % targets.length] as RepairTarget;
    this.#repairCursor += 1;
    void this.#antiEntropy.sync(target.partition, target.peer);
  }

  /**
   * Arms one self-rescheduling periodic timer. Its slot in {@link #timers} is
   * overwritten on each reschedule and cancelled on stop, so exactly one timer per
   * loop is ever live.
   */
  #everyTick(delayMs: number, work: () => void): void {
    const slot: number = this.#timers.length;
    const tick: () => void = (): void => {
      work();
      this.#timers[slot] = this.#clock.schedule(delayMs, tick);
    };
    this.#timers.push(this.#clock.schedule(delayMs, tick));
  }

  /** Resolves after `delayMs` on the injected clock, the wait between drain polls. */
  #delay(delayMs: number): Promise<void> {
    return new Promise<void>((resolve: () => void): void => {
      this.#clock.schedule(delayMs, resolve);
    });
  }

  /** The member names this node builds its ring from: the live members not draining. */
  #ringMembers(): string[] {
    const members: readonly ClusterMember[] = this.#view.members();
    const active: string[] = [];
    for (const each of members) {
      if (each.draining) {
        continue;
      }

      active.push(each.name);
    }

    // A cluster where every member is draining cannot demote the last node, so it
    // falls back to the full set rather than leaving the ring without any owner.
    return active.length > 0 ? active : members.map((each: ClusterMember): string => each.name);
  }
}
