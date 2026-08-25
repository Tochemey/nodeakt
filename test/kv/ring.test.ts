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
import {
  DEFAULT_PARTITION_COUNT,
  DEFAULT_REPLICA_COUNT,
  LOAD_FACTOR,
} from "../../src/kv/constants";
import { compareRingPoints, PartitionRing, type RingPoint } from "../../src/kv/ring";

function members(count: number): string[] {
  return Array.from({ length: count }, (_: unknown, index: number): string => `n${index}`);
}

function loadsOf(ring: PartitionRing): number[] {
  const counts: Map<string, number> = new Map();
  for (const owner of ring.primaries()) {
    counts.set(owner, (counts.get(owner) ?? 0) + 1);
  }

  return [...counts.values()];
}

function moved(before: readonly string[], after: readonly string[]): number {
  let count: number = 0;
  for (let index = 0; index < before.length; index += 1) {
    if (before[index] !== after[index]) {
      count += 1;
    }
  }

  return count;
}

describe("ring point order", () => {
  it("orders by unsigned point then by owner name", () => {
    const low: RingPoint = { point: 1, owner: "b" };
    const high: RingPoint = { point: 2, owner: "a" };
    expect(compareRingPoints(low, high)).toBe(-1);
    expect(compareRingPoints(high, low)).toBe(1);
    expect(compareRingPoints({ point: 1, owner: "a" }, { point: 1, owner: "b" })).toBe(-1);
    expect(compareRingPoints({ point: 1, owner: "b" }, { point: 1, owner: "a" })).toBe(1);
    expect(compareRingPoints(low, { point: 1, owner: "b" })).toBe(0);
  });
});

describe("partition ring", () => {
  it("gives every partition to the sole member and has no backups", () => {
    const ring: PartitionRing = new PartitionRing(["solo"]);
    expect(ring.partitionCount).toBe(DEFAULT_PARTITION_COUNT);
    expect(new Set(ring.primaries())).toEqual(new Set(["solo"]));
    expect(ring.primary(0)).toBe("solo");
    expect(ring.backups(0)).toEqual([]);
    expect(ring.backups(0, 1)).toEqual([]);
  });

  it("assigns independently of the order members are supplied", () => {
    const forward: PartitionRing = new PartitionRing(["c", "a", "b"], 32);
    const shuffled: PartitionRing = new PartitionRing(["b", "c", "a"], 32);
    expect(forward.primaries()).toEqual(shuffled.primaries());
    expect(forward.backups(7, 3)).toEqual(shuffled.backups(7, 3));
  });

  it("rejects empty, duplicate, or blank members and a non-positive partition count", () => {
    expect((): PartitionRing => new PartitionRing([])).toThrow(RangeError);
    expect((): PartitionRing => new PartitionRing(["a", "a"])).toThrow(RangeError);
    expect((): PartitionRing => new PartitionRing([""])).toThrow(RangeError);
    expect((): PartitionRing => new PartitionRing(["a"], 0)).toThrow(RangeError);
    expect((): PartitionRing => new PartitionRing(["a"], 1.5)).toThrow(RangeError);
  });

  it("rejects an out-of-range partition id and a non-positive replica count", () => {
    const ring: PartitionRing = new PartitionRing(["a", "b"], 8);
    expect((): string => ring.primary(-1)).toThrow(RangeError);
    expect((): string => ring.primary(8)).toThrow(RangeError);
    expect((): string => ring.primary(0.5)).toThrow(RangeError);
    expect((): readonly string[] => ring.backups(0, 0)).toThrow(RangeError);
    expect((): readonly string[] => ring.backups(8, 2)).toThrow(RangeError);
  });

  it("keeps backups distinct from the primary and from each other", () => {
    const ring: PartitionRing = new PartitionRing(["a", "b", "c", "d"], 64);
    for (let id = 0; id < ring.partitionCount; id += 1) {
      const primary: string = ring.primary(id);
      const backups: readonly string[] = ring.backups(id, DEFAULT_REPLICA_COUNT);
      expect(backups).not.toContain(primary);
      expect(new Set(backups).size).toBe(backups.length);
      expect(backups.length).toBe(2);
    }

    const pair: PartitionRing = new PartitionRing(["a", "b"], 16);
    expect(pair.backups(0, 5)).toEqual(pair.backups(0, 2));
    expect(pair.backups(0, 5).length).toBe(1);
  });

  it.each([3, 5, 10, 50])(
    "bounds load at %s members so no node exceeds the ceiling and none is idle",
    (count: number) => {
      const ring: PartitionRing = new PartitionRing(members(count), DEFAULT_PARTITION_COUNT);
      const loads: number[] = loadsOf(ring);
      expect(loads.length).toBe(count);
      const ceiling: number = (DEFAULT_PARTITION_COUNT / count) * LOAD_FACTOR;
      const min: number = Math.min(...loads);
      const max: number = Math.max(...loads);
      expect(max).toBeLessThanOrEqual(Math.ceil(ceiling));
      expect(min).toBeGreaterThan(0);
    },
  );

  it.each([3, 5, 10, 50])(
    "moves a bounded fraction of partitions when a %s-member ring gains one node",
    (count: number) => {
      const names: string[] = members(count);
      const before: PartitionRing = new PartitionRing(names, DEFAULT_PARTITION_COUNT);
      const after: PartitionRing = new PartitionRing(
        [...names, `n${count}`],
        DEFAULT_PARTITION_COUNT,
      );
      const changed: number = moved(before.primaries(), after.primaries());
      const bound: number = (2 * LOAD_FACTOR * DEFAULT_PARTITION_COUNT) / (count + 1);
      expect(changed).toBeGreaterThan(0);
      expect(changed).toBeLessThanOrEqual(Math.ceil(bound));
    },
  );

  it("skips a member already at the bounded-load ceiling", () => {
    const ring: PartitionRing = new PartitionRing(["a", "b"], 4);
    const loads: number[] = loadsOf(ring);
    expect(Math.max(...loads)).toBeLessThanOrEqual(Math.ceil((4 / 2) * LOAD_FACTOR));
    expect(loads.reduce((sum: number, load: number): number => sum + load, 0)).toBe(4);
  });
});
