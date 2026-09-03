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

import { Cluster, type ClusterClock, type ClusterOptions, type ClusterTimer } from "./clustering";
import { CLUSTER_EVENT_TOPIC, type ClusterEvent, type ClusterEventSink } from "./clustering.events";
import { appendRemotingAddress, encodeNodeMetadata } from "./clustering.metadata";
import { KvNetTransport } from "./clustering.transport";
import { SwimClusterView } from "./clustering.view";
import { discardLogger } from "./discard.logger";
import { type BootstrapOptions, type BootstrapResult, bootstrap } from "./discovery/bootstrap";
import type { DiscoveryProvider } from "./discovery/provider";
import type { EventStream } from "./eventstream";
import { LEAVE_DRAIN_TIMEOUT_MS } from "./kv/constants";
import type { ClusterMember, Entry, ScanEntry, WriteOp, WriteResult } from "./kv/ports";
import type { Logger } from "./logger";
import { wallClock } from "./membership/clock";
import { createRandom } from "./membership/random";
import { Swim } from "./membership/swim";
import { TcpMembershipTransport } from "./membership/transport";
import type { MemberRecord } from "./membership/view";
import { STATE_ALIVE, STATE_DEAD, STATE_LEFT, STATE_SUSPECT } from "./membership/wire";
import type { ClusterCounters, ClusterMetrics } from "./observability/metric.snapshot";

/** Bind host used when a node names none; a multi-host deployment must set a peer-reachable address. */
const DEFAULT_BIND_HOST: string = "127.0.0.1";

/**
 * The production clustering clock: epoch time for last-write-wins order and
 * expiry, and `setTimeout` scheduling for the store's periodic work.
 *
 * `now()` reads Unix epoch milliseconds because the store's time-to-live is an
 * absolute epoch deadline and the hybrid clock, which is monotone by
 * construction, tolerates the wall clock stepping. Scheduling rides `setTimeout`,
 * whose relative delays are unaffected by a wall-clock adjustment.
 *
 * @internal
 */
export const systemClusterClock: ClusterClock = {
  now(): number {
    return Date.now();
  },
  schedule(delayMs: number, callback: () => void): ClusterTimer {
    const handle: ReturnType<typeof setTimeout> = setTimeout(callback, delayMs);
    return {
      cancel(): void {
        clearTimeout(handle);
      },
    };
  },
};

/** Construction parameters for a {@link ClusterNode}. @internal */
export interface ClusterNodeOptions {
  /** How this node finds seed peers at boot; consulted once, never for topology after. */
  readonly discovery: DiscoveryProvider;
  /** Bind and advertised host for both endpoints; defaults to loopback. */
  readonly host?: string;
  /** The actor remoting endpoint peers reach this node's actors at, advertised in metadata so routing can dial it; omitted by a node that runs no actor remoting. */
  readonly remotingAddress?: string;
  /** Key/value data endpoint port; zero binds an ephemeral port. */
  readonly dataPort?: number;
  /** Membership gossip port; zero binds an ephemeral port. */
  readonly gossipPort?: number;
  /** Immutable partition count shared by every node; defaults to the cluster constant. */
  readonly partitionCount?: number;
  /** Intended replica set size including the primary; defaults to the cluster constant. */
  readonly replicaCount?: number;
  /** Acknowledgments a synchronous write awaits; defaults to the cluster constant. */
  readonly writeQuorum?: number;
  /** Minimum member quorum enabling the split-brain resolver; one, the default, disables it. */
  readonly minimumMemberQuorum?: number;
  /** Budget to reach a seed before anchoring a fresh cluster; defaults to the discovery constant. */
  readonly bootDeadlineMs?: number;
  /** Epoch time and scheduler the store runs on; defaults to {@link systemClusterClock}, injected for tests. */
  readonly clock?: ClusterClock;
  /** Immutable process start time deciding coordinator order; defaults to the wall clock at boot. */
  readonly startedAt?: number;
  /** Event stream this node's cluster lifecycle events publish onto, under {@link CLUSTER_EVENT_TOPIC}. */
  readonly events?: EventStream;
  /** Logger the cluster reports through; drops everything by default. Membership and the store tag their entries under their own component. */
  readonly logger?: Logger;
}

/**
 * One running cluster node: the outer shell that wires the store's two injected
 * ports to real gossip and real sockets.
 *
 * A node runs two endpoints. The membership engine gossips on its own address,
 * and the key/value store answers on a separate data endpoint; the data endpoint
 * is the node's cluster identity, advertised through membership metadata so every
 * peer can dial it. Over those it constructs the {@link SwimClusterView} adapter,
 * the {@link KvNetTransport} adapter, and the sim-tested {@link Cluster} engine,
 * then boots: it subscribes the engine to membership, starts gossip, uses
 * discovery once to reach a seed or anchor a fresh cluster, and marks itself
 * ready. After boot nothing is polled; every topology change arrives as a
 * membership event the engine reacts to.
 *
 * It is cluster-runtime infrastructure the actor system runs; an application
 * drives the actor system, not this node directly.
 *
 * @internal
 */
export class ClusterNode {
  /** The sim-tested store engine over the two adapters below. */
  readonly #cluster: Cluster;
  /** The membership engine gossiping this node's presence and metadata. */
  readonly #swim: Swim;
  /** The store's membership view over {@link #swim}, re-emitted on every event. */
  readonly #view: SwimClusterView;
  /** This node's key/value data endpoint, its cluster identity. */
  readonly #address: string;
  /** The actor remoting endpoint advertised for routing, empty when the node carries none. */
  readonly #remotingAddress: string;
  /** This node's membership gossip endpoint, the seed address peers join at. */
  readonly #gossipAddress: string;
  /** Immutable process start time deciding this node's coordinator order. */
  readonly #startedAt: number;
  /** Whether boot reached a seed peer, false when this node anchored a fresh cluster. */
  #joined: boolean = false;
  /** Guards a graceful leave against a second concurrent or repeated call. */
  #leaving: boolean = false;
  /** Guards teardown so a repeated stop, or a stop after a leave, is a quiet no-op. */
  #stopped: boolean = false;

  private constructor(
    cluster: Cluster,
    swim: Swim,
    view: SwimClusterView,
    address: string,
    gossipAddress: string,
    startedAt: number,
    remotingAddress: string,
  ) {
    this.#cluster = cluster;
    this.#swim = swim;
    this.#view = view;
    this.#address = address;
    this.#gossipAddress = gossipAddress;
    this.#startedAt = startedAt;
    this.#remotingAddress = remotingAddress;
  }

  /**
   * Binds both endpoints, wires the engine over them, and boots the node into the
   * cluster: it subscribes to membership, starts gossip, runs boot-only discovery
   * to reach a seed or anchor a fresh cluster, then marks itself ready. Resolves
   * with the running node.
   */
  static async start(options: ClusterNodeOptions): Promise<ClusterNode> {
    const host: string = options.host ?? DEFAULT_BIND_HOST;
    const startedAt: number = options.startedAt ?? Date.now();
    const clock: ClusterClock = options.clock ?? systemClusterClock;

    const transport: KvNetTransport = new KvNetTransport({ host, port: options.dataPort ?? 0 });
    await transport.start();
    const address: string = transport.address;

    // The data endpoint is bound; from here every failure must release it, so the
    // rest of the boot runs under cleanup that stops what was started.
    let membershipTransport: TcpMembershipTransport;
    try {
      membershipTransport = await TcpMembershipTransport.bind({
        host,
        port: options.gossipPort ?? 0,
      });
    } catch (error: unknown) {
      await transport.stop();
      throw error;
    }

    const gossipAddress: string = membershipTransport.address;

    // Both endpoints are bound but no node yet owns them, so wiring the engine,
    // which can throw while encoding metadata or constructing the membership
    // engine, runs under cleanup that stops both rather than orphaning a listener.
    let node: ClusterNode;
    try {
      // The view reads the membership snapshot and the engine reacts to gossip
      // events; the two reference each other only through closures invoked after
      // start, so the mutual capture is safe.
      let swim!: Swim;
      const view: SwimClusterView = new SwimClusterView(address, (): readonly MemberRecord[] =>
        swim.members(),
      );
      swim = new Swim({
        address: gossipAddress,
        metadata: appendRemotingAddress(
          encodeNodeMetadata({ startedAt, ready: false, draining: false, address }),
          options.remotingAddress ?? "",
        ),
        transport: membershipTransport,
        clock: wallClock,
        random: createRandom(),
        onEvent: (): void => view.publish(),
        logger: (options.logger ?? discardLogger).with({ logger: "membership" }),
      });

      const cluster: Cluster = new Cluster(clusterOptions(view, transport, clock, options));
      node = new ClusterNode(
        cluster,
        swim,
        view,
        address,
        gossipAddress,
        startedAt,
        options.remotingAddress ?? "",
      );
    } catch (error: unknown) {
      await membershipTransport.stop();
      await transport.stop();
      throw error;
    }

    // From here the node owns both endpoints, so a boot failure releases them
    // through its own stop rather than the raw transports.
    try {
      await node.#boot(options);
    } catch (error: unknown) {
      await node.stop();
      throw error;
    }

    return node;
  }

  /** This node's key/value data endpoint, the identity peers dial and the store keys on. */
  get address(): string {
    return this.#address;
  }

  /** This node's membership gossip endpoint, the seed address other nodes join at. */
  get gossipAddress(): string {
    return this.#gossipAddress;
  }

  /** Whether this node joined a seed peer, false when it anchored a fresh cluster. */
  get joined(): boolean {
    return this.#joined;
  }

  /** The present cluster members, oldest first, as this node sees them. */
  members(): readonly ClusterMember[] {
    return this.#view.members();
  }

  /** The cluster coordinator this node currently sees: the oldest present member,
   * the one node every view agrees on, the anchor a singleton is pinned to. */
  coordinator(): string {
    return this.#view.coordinator();
  }

  /**
   * This node's cluster metrics section: a count of the members it knows by
   * membership state and whether it is the coordinator, read from the
   * membership state at collection, joined with the transition `counters` the
   * caller has derived from the node's cluster events. Never on the message path.
   *
   * @internal
   */
  metrics(counters: ClusterCounters): ClusterMetrics {
    const tally: number[] = [0, 0, 0, 0];
    const records: readonly MemberRecord[] = this.#swim.members();
    for (const record of records) {
      const state: number = record.state;
      tally[state] = (tally[state] as number) + 1;
    }

    return {
      members: records.length,
      alive: tally[STATE_ALIVE] as number,
      suspect: tally[STATE_SUSPECT] as number,
      dead: tally[STATE_DEAD] as number,
      left: tally[STATE_LEFT] as number,
      isCoordinator: this.#view.coordinator() === this.#address,
      coordinatorChanges: counters.coordinatorChanges,
      relocationsTotal: counters.relocationsTotal,
    };
  }

  /**
   * The actor remoting endpoint the member named by `dataAddress` advertises, or
   * `undefined` when no present member carries that data address or the member
   * advertises none. Routing consults this to turn a placement's owning member
   * into an address it can deliver actor messages to.
   */
  remotingAddressOf(dataAddress: string): string | undefined {
    return this.#view.remotingAddressOf(dataAddress);
  }

  /** Submits a mutation and resolves with its outcome; routes to the partition primary. */
  write(op: WriteOp): Promise<WriteResult> {
    return this.#cluster.write(op);
  }

  /** Reads the live value for `key`, or `undefined`; routes to the partition primary. */
  read(key: string): Promise<Entry | undefined> {
    return this.#cluster.read(key);
  }

  /** Reads every live key and value across the cluster; see {@link Cluster.scan}. */
  scan(): Promise<ScanEntry[]> {
    return this.#cluster.scan();
  }

  /**
   * Gracefully leaves the cluster: announces this node draining so the coordinator
   * hands its partitions to new owners, waits for that handoff to finish, then
   * departs membership and stops. The wait is bounded by {@link LEAVE_DRAIN_TIMEOUT_MS};
   * any partition still held past it is recovered by the survivors as a crash.
   */
  async leave(): Promise<void> {
    if (this.#leaving || this.#stopped) {
      return;
    }

    this.#leaving = true;
    try {
      this.#swim.updateMetadata(
        appendRemotingAddress(
          encodeNodeMetadata({
            startedAt: this.#startedAt,
            ready: true,
            draining: true,
            address: this.#address,
          }),
          this.#remotingAddress,
        ),
      );
      await this.#cluster.awaitDrained(LEAVE_DRAIN_TIMEOUT_MS);
      await this.#swim.leave();
    } finally {
      // Release both endpoints whether the drain and departure succeeded or not,
      // so a failed leave never leaks the listeners a hard stop would have closed.
      await this.stop();
    }
  }

  /**
   * Stops the node: halts the store's timers, closes the data endpoint, and leaves
   * gossip. This is an abrupt stop, not a graceful drain; survivors recover this
   * node's partitions from their backups as they would after a crash. Idempotent,
   * and both endpoints are always stopped even if one throws, so a failure in one
   * never strands the other.
   */
  async stop(): Promise<void> {
    if (this.#stopped) {
      return;
    }

    this.#stopped = true;
    await Promise.allSettled([this.#cluster.stop(), this.#swim.stop()]);
  }

  /**
   * Subscribes the engine to membership and arms its timers, starts gossip so this
   * node's presence disseminates, runs boot-only discovery, then re-announces
   * itself ready. The engine subscribes before gossip starts, so the first
   * membership event, self on start and then each peer on join, drives formation.
   */
  async #boot(options: ClusterNodeOptions): Promise<void> {
    await this.#cluster.start();
    await this.#swim.start();
    const result: BootstrapResult = await bootstrap(this.#bootstrapOptions(options));
    this.#joined = result.joined;
    this.#swim.updateMetadata(
      appendRemotingAddress(
        encodeNodeMetadata({
          startedAt: this.#startedAt,
          ready: true,
          draining: false,
          address: this.#address,
        }),
        this.#remotingAddress,
      ),
    );
  }

  /** Builds the boot-only discovery options, wiring the join callback to the membership engine. */
  #bootstrapOptions(options: ClusterNodeOptions): BootstrapOptions {
    const join: (seeds: readonly string[]) => Promise<boolean> = (
      seeds: readonly string[],
    ): Promise<boolean> =>
      this.#swim.join(seeds).then(
        (): boolean => true,
        (): boolean => false,
      );
    return {
      provider: options.discovery,
      join,
      ...(options.bootDeadlineMs !== undefined ? { bootDeadlineMs: options.bootDeadlineMs } : {}),
    };
  }
}

/** Assembles the engine's options, forwarding only the sizing knobs a caller set. */
function clusterOptions(
  view: SwimClusterView,
  transport: KvNetTransport,
  clock: ClusterClock,
  options: ClusterNodeOptions,
): ClusterOptions {
  const stream: EventStream | undefined = options.events;
  const events: ClusterEventSink | undefined =
    stream === undefined
      ? undefined
      : (event: ClusterEvent): void => stream.publish(CLUSTER_EVENT_TOPIC, event);
  return {
    view,
    transport,
    clock,
    logger: (options.logger ?? discardLogger).with({ logger: "kv" }),
    ...(options.partitionCount !== undefined ? { partitionCount: options.partitionCount } : {}),
    ...(options.replicaCount !== undefined ? { replicaCount: options.replicaCount } : {}),
    ...(options.writeQuorum !== undefined ? { writeQuorum: options.writeQuorum } : {}),
    ...(options.minimumMemberQuorum !== undefined
      ? { minimumMemberQuorum: options.minimumMemberQuorum }
      : {}),
    ...(events !== undefined ? { events } : {}),
  };
}
