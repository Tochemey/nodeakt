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

import { describe, expect, it } from "vitest";
import { REPAIR_BUCKETS, TOMBSTONE_TTL_MS } from "../../src/kv/constants";
import { type DigestLanes, repairBucket } from "../../src/kv/entry";
import { Partition } from "../../src/kv/partition";
import type { Entry, HybridTime } from "../../src/kv/ports";

function at(wallMs: number, logical: number = 0, node: string = "n1"): HybridTime {
  return { wallMs, logical, node };
}

function value(key: string, timestamp: HybridTime, overrides: Partial<Entry> = {}): Entry {
  return {
    key,
    value: new Uint8Array([1]),
    timestamp,
    sequence: 1n,
    expiresAt: undefined,
    deleted: false,
    ...overrides,
  };
}

function tombstone(key: string, timestamp: HybridTime): Entry {
  return { key, value: undefined, timestamp, sequence: 1n, expiresAt: undefined, deleted: true };
}

const ZERO: DigestLanes = { hi: 0, lo: 0 };

describe("Partition apply and read", () => {
  it("stores a new entry and reads it back", () => {
    const partition: Partition = new Partition();
    expect(partition.apply(value("k", at(1)))).toBe(true);
    expect(partition.size).toBe(1);
    expect(partition.get("k", 0)?.timestamp).toEqual(at(1));
  });

  it("overwrites an older entry and rejects an older or equal write", () => {
    const partition: Partition = new Partition();
    partition.apply(value("k", at(1)));
    expect(partition.apply(value("k", at(2)))).toBe(true);
    expect(partition.get("k", 0)?.timestamp.wallMs).toBe(2);
    expect(partition.apply(value("k", at(1)))).toBe(false);
    expect(partition.apply(value("k", at(2)))).toBe(false);
    expect(partition.size).toBe(1);
    expect(partition.get("k", 0)?.timestamp.wallMs).toBe(2);
  });

  it("hides tombstones, expired, and absent keys from get but keeps peek raw", () => {
    const partition: Partition = new Partition();
    partition.apply(tombstone("gone", at(1)));
    partition.apply(value("stale", at(1), { expiresAt: 10 }));
    partition.apply(value("live", at(1)));
    expect(partition.get("gone", 0)).toBeUndefined();
    expect(partition.get("stale", 10)).toBeUndefined();
    expect(partition.get("missing", 0)).toBeUndefined();
    expect(partition.get("live", 0)?.key).toBe("live");
    expect(partition.peek("gone")?.deleted).toBe(true);
    expect(partition.peek("stale")?.expiresAt).toBe(10);
    expect(partition.peek("missing")).toBeUndefined();
  });
});

describe("Partition digest", () => {
  it("starts empty, tracks the current content, and is commutative", () => {
    const partition: Partition = new Partition();
    expect(partition.digest()).toEqual(ZERO);

    partition.apply(value("a", at(1)));
    const afterA: DigestLanes = partition.digest();
    expect(afterA).not.toEqual(ZERO);

    partition.apply(value("b", at(1)));
    partition.apply(value("c", at(1)));
    const built: DigestLanes = partition.digest();

    const other: Partition = new Partition();
    other.apply(value("c", at(1)));
    other.apply(value("a", at(1)));
    other.apply(value("b", at(1)));
    expect(other.digest()).toEqual(built);
  });

  it("changes on overwrite and is unchanged by an idempotent re-apply", () => {
    const partition: Partition = new Partition();
    partition.apply(value("k", at(1)));
    const one: DigestLanes = partition.digest();
    partition.apply(value("k", at(2)));
    const two: DigestLanes = partition.digest();
    expect(two).not.toEqual(one);
    partition.apply(value("k", at(2)));
    expect(partition.digest()).toEqual(two);
  });

  it("returns to empty once every entry is reaped", () => {
    const partition: Partition = new Partition();
    partition.apply(value("x", at(1), { expiresAt: 10 }));
    partition.apply(value("y", at(1), { expiresAt: 10 }));
    expect(partition.digest()).not.toEqual(ZERO);
    expect(partition.reap(10)).toBe(2);
    expect(partition.digest()).toEqual(ZERO);
  });
});

describe("Partition reap", () => {
  it("removes reapable tombstones and expired entries and updates the digest", () => {
    const partition: Partition = new Partition();
    partition.apply(value("live", at(1)));
    partition.apply(value("stale", at(1), { expiresAt: 10 }));
    partition.apply(tombstone("old", at(1_000)));
    partition.apply(tombstone("older", at(1_000)));

    const before: DigestLanes = partition.digest();
    // At a small clock only "stale" has expired; both tombstones are still young.
    expect(partition.reap(20)).toBe(1);
    expect(partition.peek("stale")).toBeUndefined();
    expect(partition.size).toBe(3);

    const past: number = 1_000 + TOMBSTONE_TTL_MS;
    expect(partition.reap(past)).toBe(2);
    expect(partition.peek("old")).toBeUndefined();
    expect(partition.peek("older")).toBeUndefined();
    expect(partition.peek("live")?.key).toBe("live");
    expect(partition.size).toBe(1);
    expect(partition.digest()).not.toEqual(before);
  });

  it("reaps nothing when every entry is live and young", () => {
    const partition: Partition = new Partition();
    partition.apply(value("live", at(1)));
    partition.apply(tombstone("young", at(1_000)));
    expect(partition.reap(1_001)).toBe(0);
    expect(partition.size).toBe(2);
  });
});

describe("Partition entries", () => {
  it("iterates every stored entry including tombstones", () => {
    const partition: Partition = new Partition();
    partition.apply(value("a", at(1)));
    partition.apply(tombstone("b", at(1)));
    const keys: string[] = [...partition.entries()].map((entry: Entry): string => entry.key);
    expect(keys).toEqual(["a", "b"]);
  });
});

/** A key whose repair bucket differs from `bucket`, for splitting keys across buckets. */
function keyOutsideBucket(bucket: number): string {
  for (let index: number = 1; index < 10_000; index += 1) {
    const key: string = `key-${index}`;
    if (repairBucket(key, REPAIR_BUCKETS) !== bucket) {
      return key;
    }
  }

  throw new Error("no key outside the bucket");
}

describe("Partition anti-entropy", () => {
  it("splits the digest across buckets and matches an identical fragment", () => {
    const first: Partition = new Partition();
    const second: Partition = new Partition();
    first.apply(value("x", at(1)));
    second.apply(value("x", at(1)));
    const digests: DigestLanes[] = first.bucketDigests(REPAIR_BUCKETS);
    expect(digests).toHaveLength(REPAIR_BUCKETS);
    expect(digests).toEqual(second.bucketDigests(REPAIR_BUCKETS));

    const bucket: number = repairBucket("x", REPAIR_BUCKETS);
    expect(digests[bucket]).not.toEqual(ZERO);
    const others: DigestLanes[] = digests.filter(
      (_: DigestLanes, index: number): boolean => index !== bucket,
    );
    expect(others.every((lane: DigestLanes): boolean => lane.hi === 0 && lane.lo === 0)).toBe(true);
  });

  it("returns key versions only for the requested buckets", () => {
    const partition: Partition = new Partition();
    const held: string = "key-0";
    const heldBucket: number = repairBucket(held, REPAIR_BUCKETS);
    const excluded: string = keyOutsideBucket(heldBucket);
    partition.apply(value(held, at(1)));
    partition.apply(value(excluded, at(2)));
    const keys: string[] = partition
      .keyVersions(new Set([heldBucket]), REPAIR_BUCKETS)
      .map((version): string => version.key);
    expect(keys).toEqual([held]);
  });

  it("returns entries only for the keys it holds", () => {
    const partition: Partition = new Partition();
    partition.apply(value("here", at(1)));
    const found: Entry[] = partition.entriesFor(new Set(["here", "absent"]));
    expect(found.map((entry: Entry): string => entry.key)).toEqual(["here"]);
  });
});
