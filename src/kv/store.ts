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
 * The node-local store: the partitions this node actually holds data for, keyed
 * by partition id.
 *
 * A partition materializes on its first write, so the store carries only the
 * node's share of the key space, not one empty partition per id. The store
 * routes a key to its partition, drives the janitor sweep, and yields a
 * partition's entries in cooperative chunks. It knows nothing of who owns which
 * partition: routing and replication live above it.
 *
 * @internal
 */

import { JANITOR_PARTITIONS_PER_SWEEP, SCAN_YIELD_EVERY } from "./constants";
import type { DigestLanes } from "./entry";
import { partitionId } from "./hash";
import { Partition } from "./partition";
import type { Entry } from "./ports";

/** Yields the current macrotask so a long walk never starves the event loop. */
function yieldToEventLoop(): Promise<void> {
  return new Promise<void>((resolve: () => void): void => {
    setTimeout(resolve, 0);
  });
}

/**
 * The partitioned in-memory store for one node.
 *
 * @internal
 */
export class Store {
  /** Immutable partition count shared by every node; sets the key mapping. */
  readonly #partitionCount: number;

  /** Materialized partitions this node holds, keyed by partition id. */
  readonly #partitions: Map<number, Partition> = new Map();

  /** Rolling start index for the round-robin janitor sample. */
  #sweepCursor: number = 0;

  /**
   * Creates a store over a fixed partition count.
   *
   * @throws {RangeError} If `partitionCount` is not a positive safe integer.
   */
  constructor(partitionCount: number) {
    if (!Number.isSafeInteger(partitionCount) || partitionCount <= 0) {
      throw new RangeError("partition count must be a positive integer");
    }

    this.#partitionCount = partitionCount;
  }

  /** Number of partitions this node currently holds data for. */
  get partitionsHeld(): number {
    return this.#partitions.size;
  }

  /** Partition id a key maps onto, the same on every node. */
  partitionFor(key: string): number {
    return partitionId(key, this.#partitionCount);
  }

  /** The live, unexpired value for `key`, or `undefined`. See {@link Partition.get}. */
  get(key: string, nowMs: number): Entry | undefined {
    return this.#partitions.get(this.partitionFor(key))?.get(key, nowMs);
  }

  /** The raw stored entry for `key`, tombstone or expired included, or `undefined`. */
  peek(key: string): Entry | undefined {
    return this.#partitions.get(this.partitionFor(key))?.peek(key);
  }

  /**
   * Every stored entry for partition `id`, tombstones included, or `[]` when the
   * node does not hold the partition. Reconcile and fragment transfer read this.
   */
  snapshot(id: number): Entry[] {
    const partition: Partition | undefined = this.#partitions.get(id);
    return partition === undefined ? [] : [...partition.entries()];
  }

  /** Merges `entry` into its partition under last write wins. See {@link Partition.apply}. */
  apply(entry: Entry): boolean {
    return this.#ensure(this.partitionFor(entry.key)).apply(entry);
  }

  /** The digest for partition `id`, or `undefined` when the node does not hold it. */
  digest(id: number): DigestLanes | undefined {
    return this.#partitions.get(id)?.digest();
  }

  /**
   * Reaps a round-robin sample of at most `JANITOR_PARTITIONS_PER_SWEEP` held
   * partitions and returns how many entries were removed. Sampling keeps a sweep
   * cheap on a node holding many partitions; the cursor advances so every
   * partition is visited across successive sweeps. `clustering.ts` drives this
   * on `JANITOR_INTERVAL_MS`.
   */
  sweep(nowMs: number): number {
    const held: Partition[] = [...this.#partitions.values()];
    if (held.length === 0) {
      return 0;
    }

    const count: number = Math.min(held.length, JANITOR_PARTITIONS_PER_SWEEP);
    let reaped: number = 0;
    for (let step: number = 0; step < count; step += 1) {
      reaped += (held[(this.#sweepCursor + step) % held.length] as Partition).reap(nowMs);
    }

    this.#sweepCursor = (this.#sweepCursor + count) % held.length;
    return reaped;
  }

  /**
   * Yields partition `id`'s entries in arrays of at most `batchSize`, awaiting
   * the event loop between batches so a large partition never blocks it. Yields
   * nothing when the node does not hold the partition. Callers page fragment
   * transfers and scans over this without materializing the whole partition.
   */
  async *iterate(id: number, batchSize: number = SCAN_YIELD_EVERY): AsyncGenerator<Entry[]> {
    const partition: Partition | undefined = this.#partitions.get(id);
    if (partition === undefined) {
      return;
    }

    const size: number = Math.max(1, batchSize);
    let batch: Entry[] = [];
    for (const entry of partition.entries()) {
      batch.push(entry);
      if (batch.length >= size) {
        yield batch;
        batch = [];
        await yieldToEventLoop();
      }
    }

    if (batch.length > 0) {
      yield batch;
    }
  }

  /** Returns the partition for `id`, creating an empty one on first write. */
  #ensure(id: number): Partition {
    const existing: Partition | undefined = this.#partitions.get(id);
    if (existing !== undefined) {
      return existing;
    }

    const created: Partition = new Partition();
    this.#partitions.set(id, created);
    return created;
  }
}
