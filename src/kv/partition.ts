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
 * One partition's local fragment: the key to entry map plus a digest kept in
 * step with it.
 *
 * Merges are last write wins, so the fragment converges regardless of the order
 * entries arrive from writes, replication, a drain, or a reconcile. The digest
 * is a commutative XOR of every stored entry's contribution, maintained on each
 * mutation, so two replicas that hold the same entries compare equal in one
 * integer pair without scanning. Reads filter tombstones and expired entries;
 * only the janitor's {@link reap} physically removes them, so a read never
 * mutates the digest.
 *
 * @internal
 */

import type { DigestLanes } from "./entry";
import {
  entryContribution,
  isExpired,
  isLiveValue,
  isReapableTombstone,
  repairBucket,
  supersedes,
} from "./entry";
import type { Entry, KeyVersion } from "./ports";

/**
 * The mutable fragment for one partition.
 *
 * @internal
 */
export class Partition {
  /** Live map of key to its most recent stored entry, tombstones included. */
  readonly #entries: Map<string, Entry> = new Map();

  /** High lane of the rolling digest over every entry in {@link #entries}. */
  #hi: number = 0;

  /** Low lane of the rolling digest over every entry in {@link #entries}. */
  #lo: number = 0;

  /** Count of stored entries, tombstones and expired-but-unreaped included. */
  get size(): number {
    return this.#entries.size;
  }

  /** Snapshot of the current digest as two unsigned 32-bit lanes. */
  digest(): DigestLanes {
    return { hi: this.#hi >>> 0, lo: this.#lo >>> 0 };
  }

  /**
   * The raw stored entry for `key`, tombstone or expired included, or
   * `undefined` when the key was never written. The write pipeline and
   * reconcile use this to compare timestamps; readers use {@link get}.
   */
  peek(key: string): Entry | undefined {
    return this.#entries.get(key);
  }

  /**
   * The live, unexpired value for `key`, or `undefined` when it is absent, a
   * tombstone, or expired. Expiry is lazy: an expired entry is invisible here
   * from the instant it expires, before the janitor reaps it.
   */
  get(key: string, nowMs: number): Entry | undefined {
    const entry: Entry | undefined = this.#entries.get(key);
    if (entry === undefined) {
      return undefined;
    }

    return isLiveValue(entry, nowMs) ? entry : undefined;
  }

  /**
   * Merges `entry` under last write wins and keeps the digest in step. Returns
   * `true` when the fragment changed, `false` when an equal or newer entry was
   * already present, which makes a repeated transfer a no-op.
   */
  apply(entry: Entry): boolean {
    const existing: Entry | undefined = this.#entries.get(entry.key);
    if (existing !== undefined && !supersedes(entry, existing)) {
      return false;
    }

    if (existing !== undefined) {
      this.#toggle(existing);
    }

    this.#entries.set(entry.key, entry);
    this.#toggle(entry);
    return true;
  }

  /**
   * Removes reapable tombstones and expired entries, updating the digest, and
   * returns how many were removed. Deleting the current key during iteration is
   * safe for a `Map`.
   */
  reap(nowMs: number): number {
    let reaped: number = 0;
    for (const entry of this.#entries.values()) {
      if (isReapableTombstone(entry, nowMs) || isExpired(entry, nowMs)) {
        this.#entries.delete(entry.key);
        this.#toggle(entry);
        reaped += 1;
      }
    }

    return reaped;
  }

  /**
   * A live iterator over every stored entry, tombstones and expired-but-unreaped
   * included, for transfer, reconcile, and scan. It reflects the fragment as it
   * is walked.
   */
  entries(): IterableIterator<Entry> {
    return this.#entries.values();
  }

  /**
   * The rolling digest split across `bucketCount` repair sub-buckets: each
   * entry's contribution is XORed into its key's bucket. Two replicas that differ
   * in one key differ in exactly one bucket, so anti-entropy narrows a mismatch to
   * the buckets it must inspect in one pass over the fragment.
   */
  bucketDigests(bucketCount: number): DigestLanes[] {
    const hi: number[] = new Array<number>(bucketCount).fill(0);
    const lo: number[] = new Array<number>(bucketCount).fill(0);
    for (const entry of this.#entries.values()) {
      const bucket: number = repairBucket(entry.key, bucketCount);
      const contribution: DigestLanes = entryContribution(entry);
      hi[bucket] = ((hi[bucket] as number) ^ contribution.hi) >>> 0;
      lo[bucket] = ((lo[bucket] as number) ^ contribution.lo) >>> 0;
    }

    const digests: DigestLanes[] = [];
    for (let bucket: number = 0; bucket < bucketCount; bucket += 1) {
      digests.push({ hi: hi[bucket] as number, lo: lo[bucket] as number });
    }

    return digests;
  }

  /**
   * The key and last-write-wins order of every entry, tombstones included, whose
   * repair bucket is in `buckets`. Anti-entropy exchanges these to learn which
   * side holds the newer version of each key without shipping the values.
   */
  keyVersions(buckets: ReadonlySet<number>, bucketCount: number): KeyVersion[] {
    const versions: KeyVersion[] = [];
    for (const entry of this.#entries.values()) {
      if (buckets.has(repairBucket(entry.key, bucketCount))) {
        versions.push({ key: entry.key, timestamp: entry.timestamp });
      }
    }

    return versions;
  }

  /** The stored entries, tombstones included, for the keys in `keys` this fragment holds. */
  entriesFor(keys: ReadonlySet<string>): Entry[] {
    const found: Entry[] = [];
    for (const key of keys) {
      const entry: Entry | undefined = this.#entries.get(key);
      if (entry !== undefined) {
        found.push(entry);
      }
    }

    return found;
  }

  /** XORs the entry's contribution into the digest; a second call undoes it. */
  #toggle(entry: Entry): void {
    const contribution: DigestLanes = entryContribution(entry);
    this.#hi = (this.#hi ^ contribution.hi) >>> 0;
    this.#lo = (this.#lo ^ contribution.lo) >>> 0;
  }
}
