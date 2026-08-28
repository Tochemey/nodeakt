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
 * Consistent hashing with bounded loads for partition ownership.
 *
 * Each member occupies {@link RING_POINTS_PER_MEMBER} virtual positions derived
 * from its name. Each partition id hashes to a position, and a clockwise walk
 * from that position assigns the partition to the first member whose current
 * load is still below `partitionCount / memberCount * LOAD_FACTOR`. The load
 * bound is what keeps the distribution even; consistent hashing alone does not.
 *
 * The ring is an immutable snapshot of one member set and one partition count.
 * Rebuild it when either input changes. Assignment is a pure function of those
 * inputs: the same names in any order produce the same primaries and backups.
 * This module does not know about draining, previous owners, or table versions;
 * those belong to the routing table built on top of this assignment.
 *
 * @internal
 */

import {
  DEFAULT_PARTITION_COUNT,
  DEFAULT_REPLICA_COUNT,
  LOAD_FACTOR,
  RING_POINTS_PER_MEMBER,
} from "./constants";
import { fnv1a32, hash32, mix32 } from "./hash";

/**
 * One virtual node on the hash ring.
 *
 * `point` is the unsigned 32-bit hash of `` `${owner}#${vnode}` `` for
 * `vnode` in `[0, RING_POINTS_PER_MEMBER)`. Two members that hash to the same
 * point are ordered by {@link compareRingPoints} on `owner`, never by the
 * order they were inserted. Changing that tie-break would let two nodes with
 * the same member set compute different assignments.
 *
 * @internal
 */
export interface RingPoint {
  /** Unsigned 32-bit ring coordinate in `[0, 2^32)`. */
  readonly point: number;
  /** Canonical member name that owns this virtual node. */
  readonly owner: string;
}

/**
 * Bounded-load ceiling used while assigning primaries.
 *
 * A member whose current load is at or above this value is skipped. The
 * comparison uses the load before the partition is taken, so a member just
 * below the ceiling may still accept one more partition.
 *
 * @param partitionCount Positive partition count of the ring being built.
 * @param memberCount Positive number of distinct members on that ring.
 * @returns `partitionCount / memberCount * LOAD_FACTOR`, not necessarily an
 * integer.
 */
function averageLoad(partitionCount: number, memberCount: number): number {
  return (partitionCount / memberCount) * LOAD_FACTOR;
}

/**
 * Reused four-byte scratch for {@link partitionPoint}.
 *
 * Assigning a full ring hashes every partition id, so a fresh buffer per call
 * would allocate one throwaway array per partition. A module-scoped scratch is
 * safe because hashing is synchronous and single-threaded: {@link fnv1a32}
 * reads every byte before the next `partitionPoint` call can overwrite them,
 * and it never mutates its input.
 */
const scratchBytes: Uint8Array = new Uint8Array(4);
const scratchView: DataView = new DataView(scratchBytes.buffer);

/**
 * Maps a partition id onto the unsigned 32-bit ring.
 *
 * The id is hashed as four big-endian bytes, not as a decimal string, so
 * `10` and `100` do not share a prefix bias and the mapping is independent of
 * how the id would be printed.
 *
 * @param id Non-negative partition id; only the low 32 bits are hashed.
 * @returns Mixed FNV-1a hash used as a ring coordinate.
 */
function partitionPoint(id: number): number {
  scratchView.setUint32(0, id >>> 0);
  return mix32(fnv1a32(scratchBytes));
}

/**
 * Lowest index whose `point` is greater than or equal to `hash`.
 *
 * When every point is strictly less than `hash`, the result is
 * `points.length`. Callers wrap with modulo so that walk starts at index 0.
 * The ring must be sorted by {@link compareRingPoints} or the search is wrong.
 *
 * @param points Non-empty ring, sorted ascending.
 * @param hash Unsigned 32-bit coordinate to search for.
 * @returns An index in `[0, points.length]`.
 */
function startIndex(points: readonly RingPoint[], hash: number): number {
  let lo: number = 0;
  let hi: number = points.length;
  while (lo < hi) {
    const mid: number = (lo + hi) >>> 1;
    if ((points[mid] as RingPoint).point < hash) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }

  return lo;
}

/**
 * Total order on ring positions: unsigned `point` ascending, then `owner`
 * lexicographically.
 *
 * Equal points from different members must not depend on insertion order, or
 * two nodes with the same member set would compute different assignments.
 *
 * @returns Negative if `left` precedes `right`, zero if they are equal, and
 * positive if `left` follows `right`.
 * @internal
 */
export function compareRingPoints(left: RingPoint, right: RingPoint): number {
  if (left.point !== right.point) {
    return left.point < right.point ? -1 : 1;
  }

  if (left.owner === right.owner) {
    return 0;
  }

  return left.owner < right.owner ? -1 : 1;
}

/**
 * Requires a positive integer that remains exact in JavaScript arithmetic.
 *
 * @throws {RangeError} If `value` is not a positive safe integer.
 */
function requirePositiveInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${field} must be a positive integer`);
  }
}

/**
 * Requires a partition id in the half-open range of this ring.
 *
 * @throws {RangeError} If `id` is not an integer in `[0, partitionCount)`.
 */
function requirePartitionId(id: number, partitionCount: number): void {
  if (!Number.isSafeInteger(id) || id < 0 || id >= partitionCount) {
    throw new RangeError(`partition id must be an integer in [0, ${partitionCount})`);
  }
}

/**
 * Copies `members` after rejecting an empty list, a blank name, or a duplicate.
 *
 * The copy is what the ring retains. Mutating the caller's array afterward
 * cannot change assignment. Names are the canonical cluster identities; this
 * function does not normalize case, Unicode, or host syntax.
 *
 * @returns A new array of the same names in the same order.
 * @throws {RangeError} If the list is empty, a name is empty, or a name repeats.
 */
function uniqueMembers(members: readonly string[]): string[] {
  if (members.length === 0) {
    throw new RangeError("members must not be empty");
  }

  const seen: Set<string> = new Set();
  const copy: string[] = [];
  for (const member of members) {
    if (member.length === 0) {
      throw new RangeError("member name must not be empty");
    }

    if (seen.has(member)) {
      throw new RangeError("member names must be unique");
    }

    seen.add(member);
    copy.push(member);
  }

  return copy;
}

/**
 * Builds and sorts the virtual-node ring for `members`.
 *
 * Each name contributes {@link RING_POINTS_PER_MEMBER} points. The sort uses
 * {@link compareRingPoints}, so the resulting order is independent of the
 * input permutation.
 *
 * @param members Non-empty unique names, already validated.
 * @returns Newly allocated sorted points; callers must not mutate it after the
 * ring is published.
 */
function buildPoints(members: readonly string[]): RingPoint[] {
  const points: RingPoint[] = [];
  for (const owner of members) {
    for (let vnode = 0; vnode < RING_POINTS_PER_MEMBER; vnode += 1) {
      points.push({ point: hash32(`${owner}#${vnode}`), owner });
    }
  }

  points.sort(compareRingPoints);
  return points;
}

/**
 * Assigns a primary owner to every partition id in `[0, partitionCount)`.
 *
 * For each partition the walk starts at {@link startIndex} of that partition's
 * {@link partitionPoint} and proceeds clockwise. A candidate is accepted when
 * its current load is strictly below {@link averageLoad}. If every member is
 * already at the ceiling, the first clockwise member is used anyway so the
 * assignment is total; with `LOAD_FACTOR` 1.25 that backstop is not reached at
 * the intended partition-to-member ratios.
 *
 * @param members Distinct names that own the `points`.
 * @param points Sorted virtual-node ring for those members.
 * @param partitionCount Positive partition count, equal to the result length.
 * @returns Newly allocated array of length `partitionCount`; index `i` is the
 * primary for partition `i`.
 */
function assignPrimaries(
  members: readonly string[],
  points: readonly RingPoint[],
  partitionCount: number,
): string[] {
  const ceiling: number = averageLoad(partitionCount, members.length);
  const loads: Map<string, number> = new Map();
  for (const member of members) {
    loads.set(member, 0);
  }

  const primaries: string[] = new Array(partitionCount);
  const pointCount: number = points.length;
  for (let id = 0; id < partitionCount; id += 1) {
    const start: number = startIndex(points, partitionPoint(id));
    let owner: string = (points[start % pointCount] as RingPoint).owner;
    for (let step = 0; step < pointCount; step += 1) {
      const candidate: string = (points[(start + step) % pointCount] as RingPoint).owner;
      if ((loads.get(candidate) as number) < ceiling) {
        owner = candidate;
        break;
      }
    }

    loads.set(owner, (loads.get(owner) as number) + 1);
    primaries[id] = owner;
  }

  return primaries;
}

/**
 * Immutable partition-to-member assignment for one member set.
 *
 * {@link primary} is the bounded-load owner and, on a stable cluster, the only
 * node that accepts writes for that partition. {@link backups} are the next
 * distinct members clockwise from the partition's ring position, excluding the
 * primary; they are ring geometry, not the previous-owner list the routing
 * table maintains during a move.
 *
 * @internal
 */
export class PartitionRing {
  /**
   * Distinct member names in caller order, copied at construction.
   *
   * Used to detect the single-member case for backups; assignment itself reads
   * {@link #points}, not this sequence.
   */
  readonly #members: readonly string[];

  /** Virtual nodes sorted by {@link compareRingPoints}, never mutated after construction. */
  readonly #points: readonly RingPoint[];

  /** Primary owner of partition `i` at index `i`. Length is the partition count. */
  readonly #primaries: readonly string[];

  /**
   * Builds the assignment for `members` over `partitionCount` partitions.
   *
   * @param members Non-empty unique canonical member names.
   * @param partitionCount Positive partition count; defaults to
   * {@link DEFAULT_PARTITION_COUNT}. The cluster treats this as immutable after
   * formation because changing it re-hashes every key.
   * @throws {RangeError} If `members` is empty, contains a blank or duplicate
   * name, or `partitionCount` is not a positive safe integer.
   */
  constructor(members: readonly string[], partitionCount: number = DEFAULT_PARTITION_COUNT) {
    requirePositiveInteger(partitionCount, "partition count");
    this.#members = uniqueMembers(members);
    this.#points = buildPoints(this.#members);
    this.#primaries = assignPrimaries(this.#members, this.#points, partitionCount);
  }

  /**
   * Number of partitions this snapshot assigns.
   *
   * Equal to the `partitionCount` passed to the constructor.
   */
  get partitionCount(): number {
    return this.#primaries.length;
  }

  /**
   * Bounded-load primary owner of partition `id`.
   *
   * @param id Partition index in `[0, partitionCount)`.
   * @returns The member that accepts writes for this partition on a stable cluster.
   * @throws {RangeError} If `id` is not an integer in that range.
   */
  primary(id: number): string {
    requirePartitionId(id, this.#primaries.length);
    return this.#primaries[id] as string;
  }

  /**
   * Replica members for partition `id`, excluding the primary.
   *
   * Walks clockwise from the partition's ring position and collects the next
   * `replicaCount - 1` distinct names that are not the primary. The result is
   * shorter when fewer other members exist. Replica count `1` and a
   * single-member ring both yield an empty list. This walk is not
   * load-bounded: backups follow ring order, not remaining capacity.
   *
   * @param id Partition index in `[0, partitionCount)`.
   * @param replicaCount Positive intended replica set size, including the
   * primary; defaults to {@link DEFAULT_REPLICA_COUNT}.
   * @returns A newly allocated list, possibly empty, never containing the
   * primary and never containing duplicates.
   * @throws {RangeError} If `id` is out of range or `replicaCount` is not a
   * positive safe integer.
   */
  backups(id: number, replicaCount: number = DEFAULT_REPLICA_COUNT): readonly string[] {
    requirePartitionId(id, this.#primaries.length);
    requirePositiveInteger(replicaCount, "replica count");
    const want: number = replicaCount - 1;
    if (want === 0 || this.#members.length === 1) {
      return [];
    }

    const primary: string = this.#primaries[id] as string;
    const chosen: string[] = [];
    const seen: Set<string> = new Set([primary]);
    const start: number = startIndex(this.#points, partitionPoint(id));
    const pointCount: number = this.#points.length;
    for (let step = 0; step < pointCount && chosen.length < want; step += 1) {
      const candidate: string = (this.#points[(start + step) % pointCount] as RingPoint).owner;
      if (seen.has(candidate)) {
        continue;
      }

      seen.add(candidate);
      chosen.push(candidate);
    }

    return chosen;
  }

  /**
   * Primary owner of every partition, indexed by partition id.
   *
   * The returned array is the ring's own storage and must not be mutated. It
   * is the snapshot tests and the routing table use to compare assignments.
   *
   * @returns An array of length {@link partitionCount}. Index `i` is the
   * primary of partition `i`.
   */
  primaries(): readonly string[] {
    return this.#primaries;
  }

  /**
   * The distinct member names this ring was built from, in caller order.
   *
   * The returned array is the ring's own storage and must not be mutated. The
   * routing table reads it to prune owners that are no longer live members: a
   * member absent from this set has left or died and cannot remain an owner.
   *
   * @returns The validated, deduplicated member names.
   */
  members(): readonly string[] {
    return this.#members;
  }
}
