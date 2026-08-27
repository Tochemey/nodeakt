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
import { Coordinator } from "../../src/kv/coordinator";
import { KvProtocolError } from "../../src/kv/errors";
import type { ClusterMember, KvTransport } from "../../src/kv/ports";
import { PartitionRing } from "../../src/kv/ring";
import { RoutingTable } from "../../src/kv/routing.table";
import { decodeMessage, encodeMessage, type KvMessage } from "../../src/kv/wire";
import { member, SimCluster, SimFabric, settle } from "./sim";

const PARTITIONS: number = 128;

/** Every partition id, the set a fresh node reports holding before any drain. */
function allPartitions(): number[] {
  return Array.from({ length: PARTITIONS }, (_unused: unknown, id: number): number => id);
}

/** A node under test: its view, its coordinator, and its mutable held-partition set. */
interface Node {
  readonly name: string;
  readonly view: SimCluster;
  readonly coordinator: Coordinator;
  readonly held: Set<number>;
}

/** Builds a node whose transport routes inbound table pushes into its coordinator. */
function makeNode(fabric: SimFabric, name: string, members: readonly ClusterMember[]): Node {
  const view: SimCluster = new SimCluster(name, members);
  const held: Set<number> = new Set<number>(allPartitions());
  const transport: KvTransport = fabric.transport(name);
  const coordinator: Coordinator = new Coordinator(view, transport, PARTITIONS, (): number[] => [
    ...held,
  ]);
  transport.listen(
    (from: string, body: Uint8Array): Promise<Uint8Array> =>
      Promise.resolve(coordinator.receive(from, body)),
  );
  return { name, view, coordinator, held };
}

/** Builds one node per member, all sharing the same starting membership. */
function makeCluster(fabric: SimFabric, members: readonly ClusterMember[]): Map<string, Node> {
  const nodes: Map<string, Node> = new Map<string, Node>();
  for (const entry of members) {
    nodes.set(entry.name, makeNode(fabric, entry.name, members));
  }

  return nodes;
}

/** Looks a node up by name, failing loudly on a test wiring mistake. */
function nodeOf(nodes: Map<string, Node>, name: string): Node {
  const node: Node | undefined = nodes.get(name);
  if (node === undefined) {
    throw new Error(`no node named ${name}`);
  }

  return node;
}

/** The owners list of every partition, for comparing what two nodes hold. */
function ownersOf(coordinator: Coordinator): string[][] {
  const table: RoutingTable | undefined = coordinator.currentTable();
  if (table === undefined) {
    return [];
  }

  const result: string[][] = [];
  for (let id: number = 0; id < PARTITIONS; id += 1) {
    result.push([...table.owners(id)]);
  }

  return result;
}

const THREE: ClusterMember[] = [member("A", 10), member("B", 20), member("C", 30)];

describe("Coordinator election", () => {
  it("makes the oldest live member the coordinator", () => {
    const nodes: Map<string, Node> = makeCluster(new SimFabric(1), THREE);
    expect(nodeOf(nodes, "A").coordinator.isCoordinator()).toBe(true);
    expect(nodeOf(nodes, "B").coordinator.isCoordinator()).toBe(false);
    expect(nodeOf(nodes, "C").coordinator.isCoordinator()).toBe(false);
  });

  it("rejects a non-positive partition count", () => {
    const fabric: SimFabric = new SimFabric(1);
    const view: SimCluster = new SimCluster("A", THREE);
    expect(
      (): Coordinator => new Coordinator(view, fabric.transport("A"), 0, (): number[] => []),
    ).toThrow(RangeError);
  });

  it("is not the coordinator over an empty view and rebalances to nothing", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const lonely: Coordinator = new Coordinator(
      new SimCluster("A", []),
      fabric.transport("A"),
      PARTITIONS,
      (): number[] => [],
    );
    expect(lonely.isCoordinator()).toBe(false);
    await settle(fabric, lonely.rebalance());
    expect(lonely.currentTable()).toBeUndefined();
  });
});

describe("Coordinator push", () => {
  it("pushes an initial table that every member adopts", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const nodes: Map<string, Node> = makeCluster(fabric, THREE);
    await settle(fabric, nodeOf(nodes, "A").coordinator.rebalance());
    const owners: string[][] = ownersOf(nodeOf(nodes, "A").coordinator);
    expect(nodeOf(nodes, "A").coordinator.currentTable()?.version).toBe(1n);
    expect(ownersOf(nodeOf(nodes, "B").coordinator)).toEqual(owners);
    expect(ownersOf(nodeOf(nodes, "C").coordinator)).toEqual(owners);
    expect(nodeOf(nodes, "C").coordinator.currentTable()?.version).toBe(1n);
  });

  it("does nothing when a non-coordinator rebalances", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const nodes: Map<string, Node> = makeCluster(fabric, THREE);
    await settle(fabric, nodeOf(nodes, "B").coordinator.rebalance());
    expect(nodeOf(nodes, "A").coordinator.currentTable()).toBeUndefined();
    expect(nodeOf(nodes, "B").coordinator.currentTable()).toBeUndefined();
  });

  it("keeps the version on an unchanged membership and bumps it on a join", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const nodes: Map<string, Node> = makeCluster(fabric, THREE);
    const a: Coordinator = nodeOf(nodes, "A").coordinator;
    await settle(fabric, a.rebalance());
    expect(a.currentTable()?.version).toBe(1n);
    await settle(fabric, a.rebalance());
    expect(a.currentTable()?.version).toBe(1n);

    const four: ClusterMember[] = [...THREE, member("D", 40)];
    const d: Node = makeNode(fabric, "D", four);
    for (const name of ["A", "B", "C"]) {
      nodeOf(nodes, name).view.set(four);
    }

    await settle(fabric, a.rebalance());
    expect(a.currentTable()?.version).toBe(2n);
    expect(ownersOf(d.coordinator)).toEqual(ownersOf(a));
  });

  it("bumps the version when a departure reassigns a partition to a survivor", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const nodes: Map<string, Node> = makeCluster(fabric, [member("A", 10), member("B", 20)]);
    const a: Coordinator = nodeOf(nodes, "A").coordinator;
    await settle(fabric, a.rebalance());
    nodeOf(nodes, "A").view.set([member("A", 10)]);
    await settle(fabric, a.rebalance());
    const table: RoutingTable = a.currentTable() as RoutingTable;
    expect(table.version).toBe(2n);
    for (let id: number = 0; id < PARTITIONS; id += 1) {
      expect(table.owners(id)).toEqual(["A"]);
    }
  });

  it("completes a rebalance when a member is unreachable", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const nodes: Map<string, Node> = makeCluster(fabric, THREE);
    fabric.partitionBoth("A", "C");
    await settle(fabric, nodeOf(nodes, "A").coordinator.rebalance());
    expect(nodeOf(nodes, "A").coordinator.currentTable()?.version).toBe(1n);
    expect(nodeOf(nodes, "B").coordinator.currentTable()?.version).toBe(1n);
    expect(nodeOf(nodes, "C").coordinator.currentTable()).toBeUndefined();
  });

  it("ignores a malformed or unexpected push response", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: Node = makeNode(fabric, "A", THREE);
    const b: KvTransport = fabric.transport("B");
    b.listen(async (): Promise<Uint8Array> => new Uint8Array([0xff, 0xff]));
    const c: KvTransport = fabric.transport("C");
    c.listen(async (): Promise<Uint8Array> => encodeMessage({ kind: "read-request", key: "k" }));
    await settle(fabric, a.coordinator.rebalance());
    expect(a.coordinator.currentTable()?.version).toBe(1n);
  });

  it("rejects a rebalance over an invalid member set", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const view: SimCluster = new SimCluster("A", [member("A", 10), member("A", 20)]);
    const coordinator: Coordinator = new Coordinator(
      view,
      fabric.transport("A"),
      PARTITIONS,
      (): number[] => [],
    );
    await expect(coordinator.rebalance()).rejects.toThrow(/unique/);
  });
});

describe("Coordinator report-driven pruning", () => {
  it("prunes a demoted owner once its report shows the partition drained", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const nodes: Map<string, Node> = makeCluster(fabric, THREE);
    const a: Coordinator = nodeOf(nodes, "A").coordinator;
    await settle(fabric, a.rebalance());

    const four: ClusterMember[] = [...THREE, member("D", 40)];
    nodes.set("D", makeNode(fabric, "D", four));
    for (const name of ["A", "B", "C"]) {
      nodeOf(nodes, name).view.set(four);
    }

    await settle(fabric, a.rebalance());

    let fragment: number = -1;
    let previousOwner: string = "";
    let primary: string = "";
    const table: RoutingTable = a.currentTable() as RoutingTable;
    for (let id: number = 0; id < PARTITIONS; id += 1) {
      const owners: readonly string[] = table.owners(id);
      if (owners.length === 2) {
        fragment = id;
        previousOwner = owners[0] as string;
        primary = owners[1] as string;
        break;
      }
    }

    expect(fragment).toBeGreaterThanOrEqual(0);

    nodeOf(nodes, previousOwner).held.delete(fragment);
    await settle(fabric, a.rebalance());
    await settle(fabric, a.rebalance());
    expect(a.currentTable()?.owners(fragment)).toEqual([primary]);
  });
});

describe("Coordinator draining-aware demotion", () => {
  it("excludes a draining member from primacy but keeps it as a previous owner", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const nodes: Map<string, Node> = makeCluster(fabric, THREE);
    const a: Coordinator = nodeOf(nodes, "A").coordinator;
    await settle(fabric, a.rebalance());

    const draining: ClusterMember[] = [
      member("A", 10),
      member("B", 20),
      member("C", 30, { draining: true }),
    ];
    for (const name of ["A", "B", "C"]) {
      nodeOf(nodes, name).view.set(draining);
    }

    await settle(fabric, a.rebalance());

    const table: RoutingTable = a.currentTable() as RoutingTable;
    let retained: number = 0;
    for (let id: number = 0; id < PARTITIONS; id += 1) {
      const owners: readonly string[] = table.owners(id);
      expect(owners[owners.length - 1]).not.toBe("C");
      if (owners.includes("C")) {
        retained += 1;
        expect(owners.length).toBeGreaterThan(1);
      }
    }

    expect(retained).toBeGreaterThan(0);
  });

  it("prunes a draining member once it reports its partitions drained", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const nodes: Map<string, Node> = makeCluster(fabric, THREE);
    const a: Coordinator = nodeOf(nodes, "A").coordinator;
    await settle(fabric, a.rebalance());

    const draining: ClusterMember[] = [
      member("A", 10),
      member("B", 20),
      member("C", 30, { draining: true }),
    ];
    for (const name of ["A", "B", "C"]) {
      nodeOf(nodes, name).view.set(draining);
    }

    await settle(fabric, a.rebalance());
    nodeOf(nodes, "C").held.clear();
    await settle(fabric, a.rebalance());
    await settle(fabric, a.rebalance());

    const table: RoutingTable = a.currentTable() as RoutingTable;
    for (let id: number = 0; id < PARTITIONS; id += 1) {
      expect(table.owners(id)).not.toContain("C");
    }
  });

  it("still assigns primaries when every member is draining", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const everyone: ClusterMember[] = [
      member("A", 10, { draining: true }),
      member("B", 20, { draining: true }),
    ];
    const nodes: Map<string, Node> = makeCluster(fabric, everyone);
    const a: Coordinator = nodeOf(nodes, "A").coordinator;
    await settle(fabric, a.rebalance());

    const table: RoutingTable = a.currentTable() as RoutingTable;
    const primaries: Set<string> = new Set<string>();
    for (let id: number = 0; id < PARTITIONS; id += 1) {
      primaries.add(table.primary(id));
    }

    expect(primaries).toEqual(new Set(["A", "B"]));
  });
});

describe("Coordinator table intake", () => {
  it("adopts a table only from the believed coordinator and keeps the higher version", () => {
    const fabric: SimFabric = new SimFabric(1);
    const b: Node = makeNode(fabric, "B", THREE);
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], PARTITIONS);
    const report: { node: string } = b.coordinator.handleTable("A", RoutingTable.initial(ring, 1n));
    expect(b.coordinator.currentTable()?.version).toBe(1n);
    expect(report.node).toBe("B");

    b.coordinator.handleTable("C", RoutingTable.initial(ring, 5n));
    expect(b.coordinator.currentTable()?.version).toBe(1n);

    b.coordinator.handleTable("A", RoutingTable.initial(ring, 1n));
    expect(b.coordinator.currentTable()?.version).toBe(1n);

    b.coordinator.handleTable("A", RoutingTable.initial(ring, 2n));
    expect(b.coordinator.currentTable()?.version).toBe(2n);
  });

  it("applies a pushed table through receive and answers with an ownership report", () => {
    const fabric: SimFabric = new SimFabric(1);
    const b: Node = makeNode(fabric, "B", THREE);
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], PARTITIONS);
    const push: Uint8Array = encodeMessage({
      kind: "table",
      table: RoutingTable.initial(ring, 3n).toWire(),
    });
    const response: KvMessage = decodeMessage(b.coordinator.receive("A", push));
    expect(response.kind).toBe("ownership-report");
    expect(b.coordinator.currentTable()?.version).toBe(3n);
  });

  it("rejects a non-table message in receive", () => {
    const fabric: SimFabric = new SimFabric(1);
    const b: Node = makeNode(fabric, "B", THREE);
    const bytes: Uint8Array = encodeMessage({ kind: "read-request", key: "k" });
    expect((): Uint8Array => b.coordinator.receive("A", bytes)).toThrow(KvProtocolError);
  });

  it("seeds a new coordinator's version above the highest it has held", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const b: Node = makeNode(fabric, "B", THREE);
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], PARTITIONS);
    b.coordinator.handleTable("A", RoutingTable.initial(ring, 5n));
    expect(b.coordinator.currentTable()?.version).toBe(5n);

    b.view.set([member("B", 20), member("C", 30)]);
    expect(b.coordinator.isCoordinator()).toBe(true);
    await settle(fabric, b.coordinator.rebalance());
    expect(b.coordinator.currentTable()?.version).toBe(6n);
  });
});

describe("Coordinator catch-up on a stale resume", () => {
  it("adopts a newer table a member reveals instead of forking a stale version", async () => {
    const fabric: SimFabric = new SimFabric(4);
    const nodes: Map<string, Node> = makeCluster(fabric, THREE);
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], PARTITIONS);
    // B and C already hold a newer table than A ever authored, as if A had been
    // partitioned away while the majority advanced the table; C is a step ahead of B.
    nodeOf(nodes, "B").coordinator.handleTable("A", RoutingTable.initial(ring, 5n));
    nodeOf(nodes, "C").coordinator.handleTable("A", RoutingTable.initial(ring, 6n));

    await settle(fabric, nodeOf(nodes, "A").coordinator.rebalance());

    // A does not fork a v1: it adopts the newest revealed table and everyone agrees.
    expect(nodeOf(nodes, "A").coordinator.currentTable()?.version).toBe(6n);
    expect(nodeOf(nodes, "B").coordinator.currentTable()?.version).toBe(6n);
    expect(nodeOf(nodes, "C").coordinator.currentTable()?.version).toBe(6n);
  });

  it("answers a same-version fork with its own table so the pusher converges", () => {
    const fabric: SimFabric = new SimFabric(5);
    const b: Node = makeNode(fabric, "B", THREE);
    // B holds a fork at version 2: every partition owned by B, unlike the ring layout.
    const forked: RoutingTable = new RoutingTable(
      2n,
      Array.from({ length: PARTITIONS }, (): string[] => ["B"]),
    );
    b.coordinator.handleTable("A", forked);
    expect(b.coordinator.currentTable()?.version).toBe(2n);

    // A pushes a different table at the same version; B answers with its own table,
    // not a report, and keeps what it holds so the pusher can adopt and re-author.
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], PARTITIONS);
    const push: Uint8Array = encodeMessage({
      kind: "table",
      table: RoutingTable.initial(ring, 2n).toWire(),
    });
    const response: KvMessage = decodeMessage(b.coordinator.receive("A", push));
    expect(response.kind).toBe("table");
    expect(b.coordinator.currentTable()?.version).toBe(2n);
  });

  it("answers with a report, not its table, when the pusher is ahead but not believed", () => {
    const fabric: SimFabric = new SimFabric(6);
    const b: Node = makeNode(fabric, "B", THREE);
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], PARTITIONS);
    b.coordinator.handleTable("A", RoutingTable.initial(ring, 2n));

    // C is not the believed coordinator, so its newer push is not adopted; B must
    // not answer with its older table, which would drag a real coordinator backward.
    const push: Uint8Array = encodeMessage({
      kind: "table",
      table: RoutingTable.initial(ring, 5n).toWire(),
    });
    const response: KvMessage = decodeMessage(b.coordinator.receive("C", push));
    expect(response.kind).toBe("ownership-report");
    expect(b.coordinator.currentTable()?.version).toBe(2n);
  });

  it("answers with a report when it holds no table to reveal", () => {
    const fabric: SimFabric = new SimFabric(7);
    const b: Node = makeNode(fabric, "B", THREE);
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], PARTITIONS);

    // A push from a node B does not believe is coordinator is not adopted, and B
    // holds nothing to reveal, so it simply reports.
    const push: Uint8Array = encodeMessage({
      kind: "table",
      table: RoutingTable.initial(ring, 4n).toWire(),
    });
    const response: KvMessage = decodeMessage(b.coordinator.receive("C", push));
    expect(response.kind).toBe("ownership-report");
    expect(b.coordinator.currentTable()).toBeUndefined();
  });
});
