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

/**
 * Departure and recovery: the data movement that keeps a partition whole as
 * nodes join, leave, and die.
 *
 * Every move here is one of three kinds, all built on the same paged fragment
 * transfer and throttled through one {@link MoveScheduler} so a rebalance never
 * saturates the link:
 *
 * **Crash recovery.** When a node dies, nothing moves off it; its keys already
 * live on the surviving replicas. The routing table drops the dead node and names
 * one survivor the new primary, so the partition looks unfragmented again. But
 * that survivor may be missing an acknowledged write: with write quorum two every
 * acknowledged write was on the dead primary and at least one backup, and that
 * backup need not be the one promoted, so trusting the new primary alone would
 * drop the write and only unioning every survivor recovers it. So a partition this
 * node is promoted to, whose previous primary died, is marked recovering: reads
 * gather across this node and its backups and conditional writes are refused (the
 * router consults {@link Recovery.isRecovering}), while the new primary pulls each
 * backup's fragment under last write wins. Backups are pure ring geometry, and
 * removing the dead node only appends a fresh backup at the end of the clockwise
 * walk, so every surviving holder is still among the backups and the pull misses
 * no survivor. The mark is set synchronously, before {@link Recovery.onTable}
 * yields, so a read cannot slip in against the primary before its window is up;
 * the pull clears it, and reads collapse back to the fast local path.
 *
 * **Refill.** A departure leaves a partition short a replica, and the ring names a
 * fresh backup to restore the count. The primary streams its fragment to each
 * backup the ring has newly added, so the partition is back to full replication
 * and survives the next failure. A backup that already held the partition is not
 * refilled.
 *
 * **Graceful drain.** A leaving node stays a member, advertises `draining`, and is
 * demoted to a previous owner of each partition it held while a replacement takes
 * over. It then streams each such fragment to the new primary and drops its copy
 * once the receiver acknowledges, so its next ownership report shows the partition
 * empty and the coordinator prunes it. Only then does it leave membership. A drain
 * that does not finish within its backstop is recovered as a crash.
 *
 * **Stale rejoin.** A node absent longer than the tombstone lifetime must discard
 * its fragments and re-seed from the current owner rather than merge, because the
 * tombstones that would veto its stale keys have been reaped. {@link
 * Recovery.shouldReseed} is the cutoff and {@link Recovery.reseed} the mechanism.
 *
 * This class carries only the mechanisms. The schedule that drives them, the
 * membership metadata that marks a node draining, and the timing of a rejoin all
 * live in the clustering layer.
 *
 * @internal
 */

import { DEFAULT_REPLICA_COUNT, TOMBSTONE_TTL_MS } from "./constants";
import type { Engine } from "./engine";
import { FragmentTransfer } from "./fragment";
import { MovePriority, MoveScheduler } from "./move.scheduler";
import type { ClusterMember, ClusterView, KvTransport } from "./ports";
import type { PartitionRing } from "./ring";
import type { PartitionPlacement, RoutingTable } from "./routing.table";

/** A partition this node has just been promoted to recover, and its placement. */
interface CrashPromotion {
  /** Partition id whose previous primary died and left this node in charge. */
  readonly partition: number;
  /** Owners and backups of the partition under the new table. */
  readonly placement: PartitionPlacement;
}

/** Tuning for a {@link Recovery}, defaulted from the store's constants. @internal */
export interface RecoveryOptions {
  /** Intended replica set size including the primary, for ring backups. */
  readonly replicaCount?: number;
  /** Maximum fragment moves this node runs at once. */
  readonly maxConcurrentMoves?: number;
}

/**
 * Drives departure and recovery for one node: crash reconcile, background refill,
 * and graceful drain, all through one throttled move scheduler.
 *
 * @internal
 */
export class Recovery {
  /** This node's canonical identity, compared against each partition's primary. */
  readonly #self: string;

  /** Membership view, read to tell a dead previous primary from a live one. */
  readonly #view: ClusterView;

  /** Local engine, whose fragment a drain or reseed drops once it is handed off. */
  readonly #engine: Engine;

  /** Paged transfer used to pull from a backup and push to a new owner. */
  readonly #transfer: FragmentTransfer;

  /** Priority queue bounding how many fragment moves run at once. */
  readonly #scheduler: MoveScheduler;

  /** Intended replica set size including the primary, for ring backups. */
  readonly #replicaCount: number;

  /** Partitions this node is still reconciling after a crash promotion. */
  readonly #recovering: Set<number> = new Set();

  /** The table last reacted to, or `undefined` before the first one. */
  #table: RoutingTable | undefined;

  /** The ring last reacted to, whose backups a refill diffs against. */
  #ring: PartitionRing | undefined;

  /**
   * @param engine Local store engine; its merge is the reconcile target and its
   * fragment is what a drain hands off.
   * @param transport Carrier for pulling and pushing fragments.
   * @param view Membership view whose `self` names this node.
   */
  constructor(
    engine: Engine,
    transport: KvTransport,
    view: ClusterView,
    options: RecoveryOptions = {},
  ) {
    this.#self = view.self;
    this.#view = view;
    this.#engine = engine;
    this.#transfer = new FragmentTransfer(engine, transport);
    this.#scheduler = new MoveScheduler(options.maxConcurrentMoves);
    this.#replicaCount = options.replicaCount ?? DEFAULT_REPLICA_COUNT;
  }

  /**
   * Whether `partition`'s promoted primary is still reconciling, so a read must
   * gather across its backups and a conditional write must be refused. The router
   * is given this as its `isRecovering` predicate.
   */
  isRecovering(partition: number): boolean {
    return this.#recovering.has(partition);
  }

  /**
   * Reacts to a newly installed routing table: marks and reconciles every
   * partition this node has just been promoted to, and refills a fresh backup for
   * every partition this node primaries. The recovering marks are set
   * synchronously, before this method yields, so a read cannot reach a promoted
   * primary before its window is up; the returned promise settles once every move
   * has finished, for deterministic drivers.
   */
  onTable(table: RoutingTable, ring: PartitionRing): Promise<void> {
    const previousTable: RoutingTable | undefined = this.#table;
    const previousRing: PartitionRing | undefined = this.#ring;
    this.#table = table;
    this.#ring = ring;
    const promotions: CrashPromotion[] = [];
    const refills: number[] = [];
    for (let partition: number = 0; partition < table.partitionCount; partition += 1) {
      const placement: PartitionPlacement = table.placement(partition, ring, this.#replicaCount);
      if (placement.primary !== this.#self) {
        continue;
      }

      if (this.#isCrashPromotion(previousTable, partition, placement)) {
        this.#recovering.add(partition);
        promotions.push({ partition, placement });
        continue;
      }

      refills.push(partition);
    }

    return this.#applyMoves(promotions, refills, previousRing, ring);
  }

  /**
   * Streams every fragment this node still owns but no longer primaries to the new
   * primary, dropping each once the receiver acknowledges so its next ownership
   * report shows the partition empty. Resolves to whether every fragment drained;
   * a `false` leaves the rest to crash recovery once the leave backstop fires. The
   * copy is dropped only when this node is no longer a backup of the partition, so
   * a replica keeps the data it is meant to hold.
   *
   * This is the migration path for any demotion, a graceful leave or a join that
   * moved primacy to another node, not only a departure: the clustering layer runs
   * it whenever this node has been demoted from a partition it still holds, so the
   * new primary receives the data and the previous owner is pruned. The boolean is
   * consulted only when the node is leaving, to decide when it is safe to go.
   */
  async drain(table: RoutingTable, ring: PartitionRing): Promise<boolean> {
    const partitions: number[] = [];
    for (let partition: number = 0; partition < table.partitionCount; partition += 1) {
      const owners: readonly string[] = table.owners(partition);
      if (owners[owners.length - 1] !== this.#self && owners.includes(this.#self)) {
        partitions.push(partition);
      }
    }

    const drained: boolean[] = await Promise.all(
      partitions.map(
        (partition: number): Promise<boolean> =>
          this.#scheduler.submit(
            MovePriority.drain,
            (): Promise<boolean> => this.#drainOne(partition, table, ring),
          ),
      ),
    );
    return drained.every((done: boolean): boolean => done);
  }

  /**
   * Whether a node absent for `awayMs` must re-seed instead of merging, because
   * the tombstones that would veto its stale keys have been reaped.
   */
  static shouldReseed(awayMs: number): boolean {
    return awayMs > TOMBSTONE_TTL_MS;
  }

  /**
   * Discards this node's fragment of `partition` and pulls a fresh copy from
   * `from`, for a rejoin past the tombstone window that must not merge its stale
   * keys back in.
   */
  async reseed(partition: number, from: string): Promise<void> {
    this.#engine.drop(partition);
    await this.#transfer.pull(partition, from);
  }

  /** Runs every promotion's reconcile-then-refill and every plain refill together. */
  async #applyMoves(
    promotions: readonly CrashPromotion[],
    refills: readonly number[],
    previousRing: PartitionRing | undefined,
    ring: PartitionRing,
  ): Promise<void> {
    await Promise.all([
      ...promotions.map(
        (promotion: CrashPromotion): Promise<void> => this.#recover(promotion, previousRing, ring),
      ),
      ...refills.map(
        (partition: number): Promise<void> => this.#refill(partition, previousRing, ring),
      ),
    ]);
  }

  /**
   * Pulls `promotion`'s fragment from every surviving backup under last write
   * wins, clears the recovering flag once the union lands, then refills a fresh
   * backup. An unreachable backup is skipped by the transfer; the merge is
   * idempotent, so a later pass converges what one missed.
   */
  async #recover(
    promotion: CrashPromotion,
    previousRing: PartitionRing | undefined,
    ring: PartitionRing,
  ): Promise<void> {
    try {
      await Promise.all(
        promotion.placement.backups.map(
          (backup: string): Promise<void> =>
            this.#scheduler.submit(
              MovePriority.restoreReplication,
              (): Promise<void> => this.#transfer.pull(promotion.partition, backup),
            ),
        ),
      );
    } finally {
      this.#recovering.delete(promotion.partition);
    }

    await this.#refill(promotion.partition, previousRing, ring);
  }

  /** Pushes this node's fragment of `partition` to each backup the ring newly added. */
  async #refill(
    partition: number,
    previousRing: PartitionRing | undefined,
    ring: PartitionRing,
  ): Promise<void> {
    const fresh: readonly string[] = this.#freshBackups(partition, previousRing, ring);
    await Promise.all(
      fresh.map(
        (backup: string): Promise<boolean> =>
          this.#scheduler.submit(
            MovePriority.restoreReplication,
            (): Promise<boolean> => this.#transfer.push(partition, backup),
          ),
      ),
    );
  }

  /** The backups of `partition` the ring holds now but did not in `previousRing`. */
  #freshBackups(
    partition: number,
    previousRing: PartitionRing | undefined,
    ring: PartitionRing,
  ): readonly string[] {
    if (previousRing === undefined) {
      return [];
    }

    const before: Set<string> = new Set(previousRing.backups(partition, this.#replicaCount));
    return ring
      .backups(partition, this.#replicaCount)
      .filter((backup: string): boolean => !before.has(backup));
  }

  /** Pushes one demoted fragment to its new primary and drops it on acknowledgment. */
  async #drainOne(partition: number, table: RoutingTable, ring: PartitionRing): Promise<boolean> {
    const pushed: boolean = await this.#transfer.push(partition, table.primary(partition));
    if (pushed && !ring.backups(partition, this.#replicaCount).includes(this.#self)) {
      this.#engine.drop(partition);
    }

    return pushed;
  }

  /**
   * Whether `partition` names this node the sole primary because its previous
   * primary died. A fragmented partition is excluded, since the router already
   * gathers its reads across the owners list; a partition this node already
   * primaried, or one from the very first table, has nothing to recover.
   */
  #isCrashPromotion(
    previous: RoutingTable | undefined,
    partition: number,
    placement: PartitionPlacement,
  ): boolean {
    if (placement.primary !== this.#self || placement.owners.length > 1) {
      return false;
    }

    if (previous === undefined) {
      return false;
    }

    const priorPrimary: string = previous.primary(partition);
    if (priorPrimary === this.#self) {
      return false;
    }

    return !this.#isLive(priorPrimary);
  }

  /** Whether `name` is still a live member, so its departure was not a death. */
  #isLive(name: string): boolean {
    return this.#view.members().some((member: ClusterMember): boolean => member.name === name);
  }
}
