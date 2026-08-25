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
import { PartitionRing } from "../../src/kv/ring";
import {
  type OwnershipReport,
  type PartitionPlacement,
  RoutingTable,
} from "../../src/kv/routingtable";
import type { RoutingTableWire } from "../../src/kv/wire";

/** A live member that is not the primary of partition `id`. */
function nonPrimary(ring: PartitionRing, id: number): string {
  const primary: string = ring.primary(id);
  return ring.members().find((member: string): boolean => member !== primary) as string;
}

/** A live member that is the primary of neither `a` nor `b`. */
function nonPrimaryBoth(ring: PartitionRing, a: number, b: number): string {
  const primaryA: string = ring.primary(a);
  const primaryB: string = ring.primary(b);
  return ring
    .members()
    .find((member: string): boolean => member !== primaryA && member !== primaryB) as string;
}

/** A single-owner table over the ring's own primaries. */
function settledTable(ring: PartitionRing, version: bigint): RoutingTable {
  return RoutingTable.initial(ring, version);
}

describe("RoutingTable construction", () => {
  it("rejects a negative version and an ownerless partition", () => {
    expect((): RoutingTable => new RoutingTable(-1n, [["a"]])).toThrow(RangeError);
    expect((): RoutingTable => new RoutingTable(1n, [["a"], []])).toThrow(RangeError);
  });

  it("exposes version and partition count", () => {
    const table: RoutingTable = new RoutingTable(7n, [["a"], ["b", "c"]]);
    expect(table.version).toBe(7n);
    expect(table.partitionCount).toBe(2);
  });
});

describe("RoutingTable accessors", () => {
  const table: RoutingTable = new RoutingTable(1n, [["a"], ["prev", "primary"]]);

  it("reads owners, primary, previous owners, and fragmentation", () => {
    expect(table.owners(0)).toEqual(["a"]);
    expect(table.primary(1)).toBe("primary");
    expect(table.previousOwners(1)).toEqual(["prev"]);
    expect(table.previousOwners(0)).toEqual([]);
    expect(table.isFragmented(0)).toBe(false);
    expect(table.isFragmented(1)).toBe(true);
  });

  it("rejects an out-of-range partition id", () => {
    expect((): readonly string[] => table.owners(-1)).toThrow(RangeError);
    expect((): readonly string[] => table.owners(1.5)).toThrow(RangeError);
    expect((): readonly string[] => table.owners(2)).toThrow(RangeError);
  });
});

describe("RoutingTable version order", () => {
  it("supersedes only a strictly older table", () => {
    const older: RoutingTable = new RoutingTable(1n, [["a"]]);
    const newer: RoutingTable = new RoutingTable(2n, [["a"]]);
    expect(newer.supersedes(older)).toBe(true);
    expect(older.supersedes(newer)).toBe(false);
    expect(older.supersedes(new RoutingTable(1n, [["a"]]))).toBe(false);
  });
});

describe("RoutingTable wire round-trip", () => {
  it("survives a trip through the wire shape", () => {
    const table: RoutingTable = new RoutingTable(9n, [["a"], ["b", "c"], ["d"]]);
    const wire: RoutingTableWire = table.toWire();
    expect(wire.version).toBe(9n);
    const back: RoutingTable = RoutingTable.fromWire(wire);
    expect(back.version).toBe(9n);
    expect(back.owners(1)).toEqual(["b", "c"]);
    expect(back.partitionCount).toBe(3);
  });
});

describe("RoutingTable.placement", () => {
  it("joins table owners with ring backups", () => {
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], 6);
    const primary: string = ring.primary(0);
    const previous: string = nonPrimary(ring, 0);
    const table: RoutingTable = new RoutingTable(1n, [
      [previous, primary],
      ["A"],
      ["A"],
      ["A"],
      ["A"],
      ["A"],
    ]);

    const placement: PartitionPlacement = table.placement(0, ring, 3);
    expect(placement.owners).toEqual([previous, primary]);
    expect(placement.primary).toBe(primary);
    expect(placement.previousOwners).toEqual([previous]);
    expect(placement.backups).toEqual(ring.backups(0, 3));
    expect(table.placement(0, ring).backups).toEqual(ring.backups(0));
  });
});

describe("RoutingTable.initial", () => {
  it("gives every partition its ring primary as sole owner", () => {
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], 6);
    const table: RoutingTable = RoutingTable.initial(ring, 3n);
    expect(table.version).toBe(3n);
    expect(table.partitionCount).toBe(6);
    for (let id = 0; id < 6; id += 1) {
      expect(table.owners(id)).toEqual([ring.primary(id)]);
      expect(table.isFragmented(id)).toBe(false);
    }
  });
});

describe("RoutingTable.evolve", () => {
  it("builds the initial table when there is no previous", () => {
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], 6);
    const evolved: RoutingTable = RoutingTable.evolve(undefined, ring, [], 1n);
    expect(evolved.owners(0)).toEqual([ring.primary(0)]);
    expect(evolved.isFragmented(0)).toBe(false);
  });

  it("retains a demoted owner by default and leaves a settled partition alone", () => {
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], 6);
    const other: string = nonPrimary(ring, 0);
    const previousOwners: string[][] = Array.from(
      { length: 6 },
      (_: unknown, id: number): string[] => (id === 0 ? [other] : [ring.primary(id)]),
    );
    const previous: RoutingTable = new RoutingTable(1n, previousOwners);

    const next: RoutingTable = RoutingTable.evolve(previous, ring, [], 2n);
    expect(next.version).toBe(2n);
    expect(next.owners(0)).toEqual([other, ring.primary(0)]);
    expect(next.isFragmented(0)).toBe(true);
    expect(next.owners(1)).toEqual([ring.primary(1)]);
    expect(next.isFragmented(1)).toBe(false);
  });

  it("prunes an owner that has died out of the member set", () => {
    const before: PartitionRing = new PartitionRing(["A", "B", "C"], 6);
    const previous: RoutingTable = settledTable(before, 1n);
    const dead: string = before.primary(0);
    const survivors: string[] = ["A", "B", "C"].filter((name: string): boolean => name !== dead);
    const after: PartitionRing = new PartitionRing(survivors, 6);

    const next: RoutingTable = RoutingTable.evolve(previous, after, [], 2n);
    for (let id = 0; id < 6; id += 1) {
      expect(next.owners(id)).not.toContain(dead);
    }
  });

  it("prunes on an empty report and retains on a holding report", () => {
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], 6);
    const drained: string = nonPrimary(ring, 0);
    const holder: string = nonPrimary(ring, 1);
    const owners: string[][] = [
      [drained, ring.primary(0)],
      [holder, ring.primary(1)],
      [ring.primary(2)],
      [ring.primary(3)],
      [ring.primary(4)],
      [ring.primary(5)],
    ];
    const previous: RoutingTable = new RoutingTable(1n, owners);
    const reports: OwnershipReport[] = [
      { node: drained, partitions: [] },
      { node: holder, partitions: [1] },
    ];

    const next: RoutingTable = RoutingTable.evolve(previous, ring, reports, 2n);
    expect(next.owners(0)).toEqual([ring.primary(0)]);
    expect(next.owners(1)).toEqual([holder, ring.primary(1)]);
  });

  it("unions repeated reports from one node when deciding retention", () => {
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], 6);
    const owner: string = nonPrimaryBoth(ring, 0, 1);
    const owners: string[][] = [
      [owner],
      [owner],
      [ring.primary(2)],
      [ring.primary(3)],
      [ring.primary(4)],
      [ring.primary(5)],
    ];
    const previous: RoutingTable = new RoutingTable(1n, owners);
    const reports: OwnershipReport[] = [
      { node: owner, partitions: [0] },
      { node: owner, partitions: [1] },
    ];

    const next: RoutingTable = RoutingTable.evolve(previous, ring, reports, 2n);
    expect(next.owners(0)).toEqual([owner, ring.primary(0)]);
    expect(next.owners(1)).toEqual([owner, ring.primary(1)]);
  });

  it("drops a previous owner that has become the new primary", () => {
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], 6);
    const previous: RoutingTable = settledTable(ring, 1n);
    const next: RoutingTable = RoutingTable.evolve(previous, ring, [], 2n);
    for (let id = 0; id < 6; id += 1) {
      expect(next.owners(id)).toEqual([ring.primary(id)]);
    }
  });

  it("rejects a previous table with a different partition count", () => {
    const ring: PartitionRing = new PartitionRing(["A", "B"], 6);
    const previous: RoutingTable = new RoutingTable(1n, [["A"], ["B"]]);
    expect((): RoutingTable => RoutingTable.evolve(previous, ring, [], 2n)).toThrow(RangeError);
  });
});
