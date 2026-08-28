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
 * The versioned routing table: who owns each partition, authored by one
 * coordinator and carried to every node.
 *
 * A partition's entry is an owners list with the primary last and any previous
 * owners before it, exactly the shape {@link PartitionRing} assignment feeds and
 * the wire codec carries. Backups are ring geometry, not versioned authority, so
 * they are read from the ring on demand rather than stored here; {@link
 * RoutingTable.placement} joins the two into the view the engine acts on.
 *
 * Two builders shape a table. {@link RoutingTable.initial} is the fresh,
 * unfragmented assignment. {@link RoutingTable.evolve} folds a new ring and the
 * members' ownership reports into the next table: a demoted owner is retained as
 * a previous owner so reads still reach it, then pruned once it has died or its
 * report shows the partition drained. The version is a monotone counter, and
 * {@link RoutingTable.supersedes} is how a node keeps the newer of two tables.
 *
 * A gracefully leaving member is excluded from the ring so it takes no new
 * primary or backup, but it stays in the live set {@link RoutingTable.evolve}
 * retains against, so it remains a previous owner and keeps answering reads until
 * its fragments have drained. That split, ring for assignment and a wider live
 * set for retention, is why `evolve` takes the live members separately.
 *
 * @internal
 */

import { DEFAULT_REPLICA_COUNT } from "./constants";
import type { PartitionRing } from "./ring";
import type { PartitionOwners, RoutingTableWire } from "./wire";

/**
 * A member's report of the partitions for which it still holds entries.
 *
 * The coordinator collects one per member and feeds them to {@link
 * RoutingTable.evolve}. A partition a previous owner omits has drained and the
 * owner is pruned; a partition it lists is retained.
 *
 * @internal
 */
export interface OwnershipReport {
  /** Reporting member's canonical identity. */
  readonly node: string;
  /** Partition ids the member still holds entries for. */
  readonly partitions: readonly number[];
}

/**
 * The full placement of one partition: its owners from the table and its
 * backups from the ring, joined for the engine and replication.
 *
 * @internal
 */
export interface PartitionPlacement {
  /** Owners list, primary last, previous owners before it. */
  readonly owners: readonly string[];
  /** The single node that accepts writes: the last owner. */
  readonly primary: string;
  /** Owners before the primary, which still serve reads during a move. */
  readonly previousOwners: readonly string[];
  /** Replica members from ring geometry, excluding the primary. */
  readonly backups: readonly string[];
}

/**
 * Aggregates ownership reports into the set of partitions each member holds.
 *
 * A member that never reported is absent from the map, which {@link
 * RoutingTable.evolve} reads as "no evidence to prune," so a freshly demoted
 * owner is retained until its first report actually shows the partition gone.
 */
function aggregateReports(reports: readonly OwnershipReport[]): Map<string, Set<number>> {
  const held: Map<string, Set<number>> = new Map();
  for (const report of reports) {
    let partitions: Set<number> | undefined = held.get(report.node);
    if (partitions === undefined) {
      partitions = new Set();
      held.set(report.node, partitions);
    }

    for (const id of report.partitions) {
      partitions.add(id);
    }
  }

  return held;
}

/**
 * An immutable, version-numbered assignment of owners to partitions.
 *
 * @internal
 */
export class RoutingTable {
  /** Monotone table version; a member keeps the higher of two tables. */
  readonly #version: bigint;

  /** Owners per partition, indexed by id, each non-empty with the primary last. */
  readonly #owners: readonly (readonly string[])[];

  /**
   * Wraps a version and an owners-per-partition list.
   *
   * @param version Non-negative monotone table version.
   * @param owners One owners list per partition, each with at least the primary.
   * The array is retained by reference and must not be mutated afterward.
   * @throws {RangeError} If `version` is negative or any partition has no owner.
   */
  constructor(version: bigint, owners: readonly (readonly string[])[]) {
    if (version < 0n) {
      throw new RangeError("table version must not be negative");
    }

    for (const list of owners) {
      if (list.length === 0) {
        throw new RangeError("a partition must have at least one owner");
      }
    }

    this.#version = version;
    this.#owners = owners;
  }

  /** Monotone table version. */
  get version(): bigint {
    return this.#version;
  }

  /** Number of partitions this table assigns. */
  get partitionCount(): number {
    return this.#owners.length;
  }

  /**
   * Owners of partition `id`, primary last.
   *
   * @throws {RangeError} If `id` is not an integer in `[0, partitionCount)`.
   */
  owners(id: number): readonly string[] {
    this.#requireId(id);
    return this.#owners[id] as readonly string[];
  }

  /**
   * The node that accepts writes for partition `id`: the last owner.
   *
   * @throws {RangeError} If `id` is out of range.
   */
  primary(id: number): string {
    const owners: readonly string[] = this.owners(id);
    return owners[owners.length - 1] as string;
  }

  /**
   * Owners of partition `id` before the primary, which still serve reads while
   * a move drains. Empty on a settled partition.
   *
   * @throws {RangeError} If `id` is out of range.
   */
  previousOwners(id: number): readonly string[] {
    return this.owners(id).slice(0, -1);
  }

  /**
   * Whether partition `id` has more than one owner, the transient state a move
   * produces and the rebalancer resolves.
   *
   * @throws {RangeError} If `id` is out of range.
   */
  isFragmented(id: number): boolean {
    return this.owners(id).length > 1;
  }

  /** Whether this table is newer than `other` and should replace it. */
  supersedes(other: RoutingTable): boolean {
    return this.#version > other.#version;
  }

  /**
   * The full placement of partition `id`: its owners from this table joined with
   * its backups from `ring`.
   *
   * @param id Partition index in `[0, partitionCount)`.
   * @param ring Ring whose geometry supplies the backups.
   * @param replicaCount Intended replica set size including the primary.
   * @throws {RangeError} If `id` is out of range for this table or the ring.
   */
  placement(
    id: number,
    ring: PartitionRing,
    replicaCount: number = DEFAULT_REPLICA_COUNT,
  ): PartitionPlacement {
    const owners: readonly string[] = this.owners(id);
    return {
      owners,
      primary: owners[owners.length - 1] as string,
      previousOwners: owners.slice(0, -1),
      backups: ring.backups(id, replicaCount),
    };
  }

  /** This table as the codec's wire shape. */
  toWire(): RoutingTableWire {
    const partitions: PartitionOwners[] = this.#owners.map(
      (owners: readonly string[]): PartitionOwners => ({ owners }),
    );
    return { version: this.#version, partitions };
  }

  /**
   * Rebuilds a table from its wire shape.
   *
   * @throws {RangeError} If the version is negative or a partition has no owner.
   */
  static fromWire(wire: RoutingTableWire): RoutingTable {
    const owners: (readonly string[])[] = wire.partitions.map(
      (partition: PartitionOwners): readonly string[] => partition.owners,
    );
    return new RoutingTable(wire.version, owners);
  }

  /**
   * The fresh, unfragmented table for `ring`: every partition has its ring
   * primary as sole owner.
   */
  static initial(ring: PartitionRing, version: bigint): RoutingTable {
    const owners: string[][] = new Array<string[]>(ring.partitionCount);
    for (let id: number = 0; id < ring.partitionCount; id += 1) {
      owners[id] = [ring.primary(id)];
    }

    return new RoutingTable(version, owners);
  }

  /**
   * The next table after a membership change: each partition takes `ring`'s new
   * primary as its last owner, keeping any previous owner that is still live and
   * has not reported the partition drained.
   *
   * A previous owner is dropped when it is not in `live` (it died or was reaped)
   * or when its report lists partitions but not this one (it drained). Absent a
   * report, a demoted owner is kept so reads still reach its data. A draining
   * member is absent from `ring` yet present in `live`, so it is demoted from
   * primary but retained as a previous owner until its report shows it drained.
   *
   * @param previous The table being replaced, or `undefined` for the first one.
   * @param ring Assignment for the new member set, from which draining members
   * are already excluded so they take no new primary or backup.
   * @param reports The members' ownership reports.
   * @param version Version for the new table, above every version seen.
   * @param live The members eligible to remain owners, which includes draining
   * members that the ring omits. Defaults to the ring's members, so a caller that
   * has no draining members need not pass it.
   * @throws {RangeError} If `previous` covers a different partition count.
   */
  static evolve(
    previous: RoutingTable | undefined,
    ring: PartitionRing,
    reports: readonly OwnershipReport[],
    version: bigint,
    live: ReadonlySet<string> = new Set(ring.members()),
  ): RoutingTable {
    if (previous !== undefined && previous.partitionCount !== ring.partitionCount) {
      throw new RangeError("partition count must not change across table versions");
    }

    const held: Map<string, Set<number>> = aggregateReports(reports);
    const owners: string[][] = new Array<string[]>(ring.partitionCount);
    for (let id: number = 0; id < ring.partitionCount; id += 1) {
      owners[id] = evolveOwners(previous, ring, held, live, id);
    }

    return new RoutingTable(version, owners);
  }

  /** Fails unless `id` indexes a partition of this table. */
  #requireId(id: number): void {
    if (!Number.isSafeInteger(id) || id < 0 || id >= this.#owners.length) {
      throw new RangeError(`partition id must be an integer in [0, ${this.#owners.length})`);
    }
  }
}

/** Owners list for one partition under {@link RoutingTable.evolve}. */
function evolveOwners(
  previous: RoutingTable | undefined,
  ring: PartitionRing,
  held: Map<string, Set<number>>,
  live: ReadonlySet<string>,
  id: number,
): string[] {
  const primary: string = ring.primary(id);
  const retained: string[] = [];
  if (previous !== undefined) {
    for (const owner of previous.owners(id)) {
      if (isRetained(owner, primary, held, live, id)) {
        retained.push(owner);
      }
    }
  }

  retained.push(primary);
  return retained;
}

/** Whether a previous owner survives into the next table for partition `id`. */
function isRetained(
  owner: string,
  primary: string,
  held: Map<string, Set<number>>,
  live: ReadonlySet<string>,
  id: number,
): boolean {
  if (owner === primary) {
    return false;
  }

  if (!live.has(owner)) {
    return false;
  }

  const report: Set<number> | undefined = held.get(owner);
  return report === undefined || report.has(id);
}
