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
import { Engine } from "../../src/kv/engine";
import {
  ClusterUnavailableError,
  KvProtocolError,
  KvQuorumError,
  PartitionRebalancingError,
} from "../../src/kv/errors";
import { partitionId } from "../../src/kv/hash";
import type { Entry, KvTransport, WriteResult } from "../../src/kv/ports";
import { PrimaryBackup, type ReplicationMode } from "../../src/kv/primary.backup";
import { Replicator, type ReplicatorOptions } from "../../src/kv/replication";
import { PartitionRing } from "../../src/kv/ring";
import { RoutingTable } from "../../src/kv/routing.table";
import { decodeMessage, encodeMessage } from "../../src/kv/wire";
import { flush, SimFabric, settle } from "./sim";

const PARTITIONS: number = 8;

function bytes(...values: number[]): Uint8Array {
  return new Uint8Array(values);
}

/** A node under test: its local engine and its router. */
interface RepNode {
  readonly name: string;
  readonly engine: Engine;
  readonly replicator: Replicator;
}

/** Builds one router over `table`, wired to the fabric under `name`. */
function makeNode(
  fabric: SimFabric,
  name: string,
  table: RoutingTable,
  ring: PartitionRing,
  options: ReplicatorOptions = {},
): RepNode {
  const engine: Engine = new Engine(name, PARTITIONS, (): number => 1_000);
  const transport: KvTransport = fabric.transport(name);
  const replicator: Replicator = new Replicator(name, engine, transport, options);
  replicator.install(table, ring);
  transport.listen(
    (from: string, body: Uint8Array): Promise<Uint8Array> => replicator.receive(from, body),
  );
  return { name, engine, replicator };
}

/** Builds one router per member over one shared initial table, all wired to the fabric. */
function makeCluster(
  fabric: SimFabric,
  members: readonly string[],
  options: ReplicatorOptions = {},
): { nodes: Map<string, RepNode>; ring: PartitionRing } {
  const ring: PartitionRing = new PartitionRing(members, PARTITIONS);
  const table: RoutingTable = RoutingTable.initial(ring, 1n);
  const nodes: Map<string, RepNode> = new Map<string, RepNode>();
  for (const name of members) {
    nodes.set(name, makeNode(fabric, name, table, ring, options));
  }

  return { nodes, ring };
}

/** A copy of `ring`'s initial table with one partition's owners overridden. */
function fragmentedTable(
  ring: PartitionRing,
  partition: number,
  owners: readonly string[],
): RoutingTable {
  const perPartition: string[][] = [];
  for (let id: number = 0; id < PARTITIONS; id += 1) {
    perPartition.push(id === partition ? [...owners] : [ring.primary(id)]);
  }

  return new RoutingTable(1n, perPartition);
}

/** A live entry for `key` stamped at `wallMs`, or a tombstone when `value` is undefined. */
function entryAt(
  key: string,
  value: Uint8Array | undefined,
  wallMs: number,
  expiresAt?: number,
): Entry {
  return {
    key,
    value,
    timestamp: { wallMs, logical: 0, node: "peer" },
    sequence: 1n,
    expiresAt,
    deleted: value === undefined,
  };
}

/** Looks a node up by name, failing loudly on a wiring mistake. */
function nodeOf(nodes: Map<string, RepNode>, name: string): RepNode {
  const node: RepNode | undefined = nodes.get(name);
  if (node === undefined) {
    throw new Error(`no node named ${name}`);
  }

  return node;
}

/** A key whose partition's ring primary is `primary`. */
function keyFor(ring: PartitionRing, primary: string): string {
  for (let index: number = 0; index < 10_000; index += 1) {
    const key: string = `key-${index}`;
    if (ring.primary(partitionId(key, PARTITIONS)) === primary) {
      return key;
    }
  }

  throw new Error(`no key maps to ${primary}`);
}

/** Drains every pending timer and microtask so fire-and-forget repair settles. */
async function quiesce(fabric: SimFabric): Promise<void> {
  for (let turn: number = 0; turn < 100 && fabric.clock.pending > 0; turn += 1) {
    await flush();
    fabric.clock.runNext();
  }

  await flush();
}

describe("Replicator local primary", () => {
  it("writes and reads on a single node without any backup acks", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes }: { nodes: Map<string, RepNode> } = makeCluster(fabric, ["A"]);
    const a: Replicator = nodeOf(nodes, "A").replicator;
    const result: WriteResult = await settle(
      fabric,
      a.write({ kind: "put", key: "k", value: bytes(1), condition: "none" }),
    );
    expect(result.applied).toBe(true);
    expect((await settle(fabric, a.read("k")))?.value).toEqual(bytes(1));
  });

  it("replicates a synchronous write to the backup before acknowledging", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes, ring } = makeCluster(fabric, ["A", "B"]);
    const key: string = keyFor(ring, "A");
    const result: WriteResult = await settle(
      fabric,
      nodeOf(nodes, "A").replicator.write({ kind: "put", key, value: bytes(7), condition: "none" }),
    );
    expect(result.applied).toBe(true);
    expect((await nodeOf(nodes, "B").engine.read(key))?.value).toEqual(bytes(7));
  });

  it("returns a rejected conditional write without replicating", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes, ring } = makeCluster(fabric, ["A", "B"]);
    const key: string = keyFor(ring, "A");
    const a: Replicator = nodeOf(nodes, "A").replicator;
    const first: WriteResult = await settle(
      fabric,
      a.write({ kind: "put", key, value: bytes(1), condition: "nx" }),
    );
    expect(first.applied).toBe(true);
    const second: WriteResult = await settle(
      fabric,
      a.write({ kind: "put", key, value: bytes(2), condition: "nx" }),
    );
    expect(second).toEqual({ applied: false, reason: "nx" });
  });
});

describe("Replicator remote routing", () => {
  it("forwards a write to the primary, which replicates back to the sender", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes, ring } = makeCluster(fabric, ["A", "B"]);
    const key: string = keyFor(ring, "A");
    const result: WriteResult = await settle(
      fabric,
      nodeOf(nodes, "B").replicator.write({ kind: "put", key, value: bytes(5), condition: "none" }),
    );
    expect(result.applied).toBe(true);
    expect((await nodeOf(nodes, "A").engine.read(key))?.value).toEqual(bytes(5));
    expect((await nodeOf(nodes, "B").engine.read(key))?.value).toEqual(bytes(5));
  });

  it("forwards a read to the primary", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes, ring } = makeCluster(fabric, ["A", "B"]);
    const key: string = keyFor(ring, "A");
    await settle(
      fabric,
      nodeOf(nodes, "A").replicator.write({ kind: "put", key, value: bytes(3), condition: "none" }),
    );
    const entry: Entry | undefined = await settle(fabric, nodeOf(nodes, "B").replicator.read(key));
    expect(entry?.value).toEqual(bytes(3));
  });

  it("rejects a router operation before a table is installed", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const engine: Engine = new Engine("A", PARTITIONS, (): number => 1_000);
    const replicator: Replicator = new Replicator("A", engine, fabric.transport("A"));
    await expect(
      replicator.write({ kind: "put", key: "k", value: bytes(1), condition: "none" }),
    ).rejects.toThrow(ClusterUnavailableError);
    await expect(replicator.read("k")).rejects.toThrow(ClusterUnavailableError);
  });

  it("rejects a forwarded response of the wrong kind", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const table: RoutingTable = RoutingTable.initial(ring, 1n);
    const engine: Engine = new Engine("A", PARTITIONS, (): number => 1_000);
    const replicator: Replicator = new Replicator("A", engine, fabric.transport("A"));
    replicator.install(table, ring);
    const key: string = keyFor(ring, "B");
    fabric
      .transport("B")
      .listen(
        async (): Promise<Uint8Array> => encodeMessage({ kind: "read-response", entry: undefined }),
      );
    await expect(
      settle(fabric, replicator.write({ kind: "put", key, value: bytes(1), condition: "none" })),
    ).rejects.toThrow(KvProtocolError);
  });

  it("rejects a forwarded read response of the wrong kind", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const table: RoutingTable = RoutingTable.initial(ring, 1n);
    const engine: Engine = new Engine("A", PARTITIONS, (): number => 1_000);
    const replicator: Replicator = new Replicator("A", engine, fabric.transport("A"));
    replicator.install(table, ring);
    const key: string = keyFor(ring, "B");
    fabric
      .transport("B")
      .listen(
        async (): Promise<Uint8Array> =>
          encodeMessage({ kind: "write-response", result: { applied: false, reason: "nx" } }),
      );
    await expect(settle(fabric, replicator.read(key))).rejects.toThrow(KvProtocolError);
  });

  it("rejects an unexpected inbound message", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes } = makeCluster(fabric, ["A"]);
    const unexpected: Uint8Array = encodeMessage({
      kind: "ownership-report",
      report: { node: "A", partitions: [] },
    });
    await expect(nodeOf(nodes, "A").replicator.receive("B", unexpected)).rejects.toThrow(
      KvProtocolError,
    );
  });
});

describe("Replicator quorum", () => {
  it("fails a synchronous write when the backup is unreachable", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes, ring } = makeCluster(fabric, ["A", "B"]);
    const key: string = keyFor(ring, "A");
    fabric.partitionBoth("A", "B");
    await expect(
      settle(
        fabric,
        nodeOf(nodes, "A").replicator.write({
          kind: "put",
          key,
          value: bytes(1),
          condition: "none",
        }),
      ),
    ).rejects.toThrow(KvQuorumError);
  });

  it("acknowledges a background write despite an unreachable backup", async () => {
    const options: ReplicatorOptions = { mode: "async" satisfies ReplicationMode };
    const fabric: SimFabric = new SimFabric(1);
    const { nodes, ring } = makeCluster(fabric, ["A", "B"], options);
    const key: string = keyFor(ring, "A");
    fabric.partitionBoth("A", "B");
    const result: WriteResult = await settle(
      fabric,
      nodeOf(nodes, "A").replicator.write({ kind: "put", key, value: bytes(1), condition: "none" }),
    );
    expect(result.applied).toBe(true);
  });

  it("does not count a non-acknowledgment toward the quorum", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes, ring } = makeCluster(fabric, ["A", "B", "C"], { writeQuorum: 3 });
    const key: string = keyFor(ring, "A");
    fabric.transport("B").listen(async (): Promise<Uint8Array> => new Uint8Array([0xff, 0xff]));
    fabric
      .transport("C")
      .listen(
        async (): Promise<Uint8Array> => encodeMessage({ kind: "read-response", entry: undefined }),
      );
    await expect(
      settle(
        fabric,
        nodeOf(nodes, "A").replicator.write({
          kind: "put",
          key,
          value: bytes(1),
          condition: "none",
        }),
      ),
    ).rejects.toThrow(KvQuorumError);
  });
});

describe("Replicator rebalance gate", () => {
  it("refuses conditional writes on a fragmented partition and allows unconditional ones", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const key: string = "gate-key";
    const partition: number = partitionId(key, PARTITIONS);
    const owners: string[][] = [];
    for (let id: number = 0; id < PARTITIONS; id += 1) {
      owners.push([ring.primary(id)]);
    }

    owners[partition] = ["B", "A"];
    const table: RoutingTable = new RoutingTable(1n, owners);
    const engine: Engine = new Engine("A", PARTITIONS, (): number => 1_000);
    const replicator: Replicator = new Replicator("A", engine, fabric.transport("A"), {
      writeQuorum: 1,
    });
    replicator.install(table, ring);

    await expect(
      replicator.write({ kind: "put", key, value: bytes(1), condition: "nx" }),
    ).rejects.toThrow(PartitionRebalancingError);
    await expect(
      replicator.write({ kind: "put", key, value: bytes(1), condition: "xx" }),
    ).rejects.toThrow(PartitionRebalancingError);
    await expect(
      replicator.write({ kind: "cas", key, expected: bytes(1), value: bytes(2) }),
    ).rejects.toThrow(PartitionRebalancingError);
    await expect(replicator.write({ kind: "incr", key, delta: 1n })).rejects.toThrow(
      PartitionRebalancingError,
    );

    const put: WriteResult = await settle(
      fabric,
      replicator.write({ kind: "put", key, value: bytes(9), condition: "none" }),
    );
    expect(put.applied).toBe(true);
    const removed: WriteResult = await settle(fabric, replicator.write({ kind: "delete", key }));
    expect(removed.applied).toBe(true);
  });
});

describe("Replicator recovery window", () => {
  /** A two-node cluster where only A treats a partition as still recovering. */
  function recoveringCluster(
    fabric: SimFabric,
    recovering: Set<number>,
  ): { a: RepNode; b: RepNode; ring: PartitionRing } {
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const table: RoutingTable = RoutingTable.initial(ring, 1n);
    const a: RepNode = makeNode(fabric, "A", table, ring, {
      isRecovering: (partition: number): boolean => recovering.has(partition),
    });
    const b: RepNode = makeNode(fabric, "B", table, ring);
    return { a, b, ring };
  }

  it("gathers a primary-local read across the backups while recovering", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const recovering: Set<number> = new Set<number>();
    const { a, b, ring } = recoveringCluster(fabric, recovering);
    const key: string = keyFor(ring, "A");
    const partition: number = partitionId(key, PARTITIONS);
    b.engine.merge(entryAt(key, bytes(9), 2_000));

    expect(await settle(fabric, a.replicator.read(key))).toBeUndefined();
    recovering.add(partition);
    expect((await settle(fabric, a.replicator.read(key)))?.value).toEqual(bytes(9));
  });

  it("gathers a forwarded read through a recovering primary", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const recovering: Set<number> = new Set<number>();
    const { b, ring } = recoveringCluster(fabric, recovering);
    const key: string = keyFor(ring, "A");
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    recovering.add(partitionId(key, PARTITIONS));

    expect((await settle(fabric, b.replicator.read(key)))?.value).toEqual(bytes(9));
  });

  it("refuses a conditional write on a recovering partition", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const recovering: Set<number> = new Set<number>();
    const { a, ring } = recoveringCluster(fabric, recovering);
    const key: string = keyFor(ring, "A");
    recovering.add(partitionId(key, PARTITIONS));

    await expect(
      a.replicator.write({ kind: "put", key, value: bytes(1), condition: "nx" }),
    ).rejects.toThrow(PartitionRebalancingError);
  });

  it("refuses a forwarded conditional write at a recovering primary", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const recovering: Set<number> = new Set<number>();
    const { b, ring } = recoveringCluster(fabric, recovering);
    const key: string = keyFor(ring, "A");
    recovering.add(partitionId(key, PARTITIONS));

    await expect(
      settle(fabric, b.replicator.write({ kind: "put", key, value: bytes(1), condition: "nx" })),
    ).rejects.toThrow(PartitionRebalancingError);
  });
});

describe("Replicator table changes", () => {
  it("rejects a non-positive write quorum", () => {
    const fabric: SimFabric = new SimFabric(1);
    const engine: Engine = new Engine("A", PARTITIONS, (): number => 1_000);
    expect(
      (): Replicator => new Replicator("A", engine, fabric.transport("A"), { writeQuorum: 0 }),
    ).toThrow(RangeError);
  });

  it("refreshes group replicas when a new table is installed", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes, ring } = makeCluster(fabric, ["A", "B"]);
    const a: Replicator = nodeOf(nodes, "A").replicator;
    const key: string = keyFor(ring, "A");
    await settle(fabric, a.write({ kind: "put", key, value: bytes(1), condition: "none" }));
    a.install(RoutingTable.initial(ring, 2n), ring);
    const result: WriteResult = await settle(
      fabric,
      a.write({ kind: "put", key, value: bytes(2), condition: "none" }),
    );
    expect(result.applied).toBe(true);
    expect((await nodeOf(nodes, "B").engine.read(key))?.value).toEqual(bytes(2));
  });
});

describe("Replicator fragmented read", () => {
  const KEY: string = "frag";
  const PARTITION: number = partitionId(KEY, PARTITIONS);
  const RING: PartitionRing = new PartitionRing(["A", "B", "C"], PARTITIONS);

  it("returns the highest-timestamp version and repairs the stale owner", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const table: RoutingTable = fragmentedTable(RING, PARTITION, ["B", "A"]);
    const a: RepNode = makeNode(fabric, "A", table, RING);
    const b: RepNode = makeNode(fabric, "B", table, RING);
    a.engine.merge(entryAt(KEY, bytes(2), 2_000));
    b.engine.merge(entryAt(KEY, bytes(1), 1_000));
    expect((await settle(fabric, a.replicator.read(KEY)))?.value).toEqual(bytes(2));
    await quiesce(fabric);
    expect(b.engine.peek(KEY)?.value).toEqual(bytes(2));
  });

  it("falls back to a previous owner and repairs the new primary", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const table: RoutingTable = fragmentedTable(RING, PARTITION, ["B", "A"]);
    const a: RepNode = makeNode(fabric, "A", table, RING);
    const b: RepNode = makeNode(fabric, "B", table, RING);
    b.engine.merge(entryAt(KEY, bytes(1), 1_000));
    expect((await settle(fabric, a.replicator.read(KEY)))?.value).toEqual(bytes(1));
    await quiesce(fabric);
    expect(a.engine.peek(KEY)?.value).toEqual(bytes(1));
  });

  it("returns undefined when a tombstone is the newest version", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const table: RoutingTable = fragmentedTable(RING, PARTITION, ["B", "A"]);
    const a: RepNode = makeNode(fabric, "A", table, RING);
    const b: RepNode = makeNode(fabric, "B", table, RING);
    a.engine.merge(entryAt(KEY, undefined, 2_000));
    b.engine.merge(entryAt(KEY, bytes(1), 1_000));
    expect(await settle(fabric, a.replicator.read(KEY))).toBeUndefined();
  });

  it("returns undefined when the newest version has expired", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const table: RoutingTable = fragmentedTable(RING, PARTITION, ["B", "A"]);
    const a: RepNode = makeNode(fabric, "A", table, RING);
    makeNode(fabric, "B", table, RING);
    a.engine.merge(entryAt(KEY, bytes(1), 1_000, 500));
    expect(await settle(fabric, a.replicator.read(KEY))).toBeUndefined();
  });

  it("returns undefined when no owner holds the key", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const table: RoutingTable = fragmentedTable(RING, PARTITION, ["B", "A"]);
    const a: RepNode = makeNode(fabric, "A", table, RING);
    makeNode(fabric, "B", table, RING);
    expect(await settle(fabric, a.replicator.read(KEY))).toBeUndefined();
  });

  it("tolerates an unreachable owner during the gather", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const table: RoutingTable = fragmentedTable(RING, PARTITION, ["B", "A"]);
    const a: RepNode = makeNode(fabric, "A", table, RING);
    makeNode(fabric, "B", table, RING);
    a.engine.merge(entryAt(KEY, bytes(5), 1_000));
    fabric.partitionBoth("A", "B");
    expect((await settle(fabric, a.replicator.read(KEY)))?.value).toEqual(bytes(5));
    // The read repair to the unreachable owner fails silently in the background.
    await quiesce(fabric);
  });

  it("ignores malformed and unexpected peek responses", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const table: RoutingTable = fragmentedTable(RING, PARTITION, ["B", "C", "A"]);
    const a: RepNode = makeNode(fabric, "A", table, RING);
    a.engine.merge(entryAt(KEY, bytes(3), 1_000));
    fabric.transport("B").listen(async (): Promise<Uint8Array> => new Uint8Array([0xff]));
    fabric
      .transport("C")
      .listen(async (): Promise<Uint8Array> => encodeMessage({ kind: "replicate-ack" }));
    expect((await settle(fabric, a.replicator.read(KEY)))?.value).toEqual(bytes(3));
  });
});

describe("Replicator primary rebalance gate", () => {
  it("refuses a forwarded conditional write the primary sees as fragmented", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const key: string = "gate2";
    const partition: number = partitionId(key, PARTITIONS);
    const ring: PartitionRing = new PartitionRing(["A", "B", "C"], PARTITIONS);
    const tableA: RoutingTable = fragmentedTable(ring, partition, ["B"]);
    const tableB: RoutingTable = fragmentedTable(ring, partition, ["C", "B"]);
    const a: RepNode = makeNode(fabric, "A", tableA, ring);
    makeNode(fabric, "B", tableB, ring);
    await expect(
      settle(fabric, a.replicator.write({ kind: "put", key, value: bytes(1), condition: "nx" })),
    ).rejects.toThrow(PartitionRebalancingError);
  });
});

describe("Replicator fragment intake", () => {
  it("merges a pushed fragment chunk and acknowledges it", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { nodes } = makeCluster(fabric, ["A"]);
    const a: RepNode = nodeOf(nodes, "A");
    const key: string = "pushed";
    const partition: number = partitionId(key, PARTITIONS);
    const push: Uint8Array = encodeMessage({
      kind: "fragment-push",
      chunk: { partitionId: partition, final: true, entries: [entryAt(key, bytes(4), 1_000)] },
    });
    const response: Uint8Array = await a.replicator.receive("B", push);
    expect(decodeMessage(response).kind).toBe("fragment-ack");
    expect(a.engine.peek(key)?.value).toEqual(bytes(4));
  });
});

describe("PrimaryBackup reconcile", () => {
  function loneGroup(
    fabric: SimFabric,
    partition: number,
  ): { group: PrimaryBackup; engine: Engine } {
    const engine: Engine = new Engine("A", PARTITIONS, (): number => 1_000);
    const group: PrimaryBackup = new PrimaryBackup(
      partition,
      engine,
      fabric.transport("A"),
      2,
      "sync",
    );
    return { group, engine };
  }

  it("pulls and merges a peer's fragment", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const key: string = "recon";
    const partition: number = partitionId(key, PARTITIONS);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const b: RepNode = makeNode(fabric, "B", RoutingTable.initial(ring, 1n), ring);
    b.engine.merge(entryAt(key, bytes(9), 1_000));
    const { group, engine } = loneGroup(fabric, partition);
    await settle(fabric, group.reconcile(["B"]));
    expect(engine.peek(key)?.value).toEqual(bytes(9));
  });

  it("skips an unreachable peer", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { group, engine } = loneGroup(fabric, partitionId("k", PARTITIONS));
    await settle(fabric, group.reconcile(["Z"]));
    expect(engine.peek("k")).toBeUndefined();
  });

  it("ignores malformed and unexpected reconcile responses", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const { group, engine } = loneGroup(fabric, partitionId("k", PARTITIONS));
    fabric.transport("Y").listen(async (): Promise<Uint8Array> => new Uint8Array([0xff]));
    fabric
      .transport("Z")
      .listen(async (): Promise<Uint8Array> => encodeMessage({ kind: "replicate-ack" }));
    await settle(fabric, group.reconcile(["Y", "Z"]));
    expect(engine.peek("k")).toBeUndefined();
  });
});
