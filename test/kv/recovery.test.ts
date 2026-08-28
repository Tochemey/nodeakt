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
import { TOMBSTONE_TTL_MS } from "../../src/kv/constants";
import { Engine } from "../../src/kv/engine";
import { KvProtocolError } from "../../src/kv/errors";
import { FragmentTransfer } from "../../src/kv/fragment";
import { partitionId } from "../../src/kv/hash";
import type { Entry, KvTransport } from "../../src/kv/ports";
import { Recovery } from "../../src/kv/recovery";
import { PartitionRing } from "../../src/kv/ring";
import { RoutingTable } from "../../src/kv/routing.table";
import { decodeMessage, encodeMessage, type KvMessage } from "../../src/kv/wire";
import { member, SimCluster, SimFabric, settle } from "./sim";

const PARTITIONS: number = 8;

function bytes(...values: number[]): Uint8Array {
  return new Uint8Array(values);
}

/** A live entry for `key` stamped at `wallMs`. */
function entryAt(key: string, value: Uint8Array, wallMs: number): Entry {
  return {
    key,
    value,
    timestamp: { wallMs, logical: 0, node: "peer" },
    sequence: 1n,
    expiresAt: undefined,
    deleted: false,
  };
}

/** A node that can serve a fragment pull and a gather peek, for a recovering peer. */
interface PeerNode {
  readonly engine: Engine;
  readonly transport: KvTransport;
}

/** Builds a peer that answers the RPCs a recovering primary sends it. */
function peer(fabric: SimFabric, name: string): PeerNode {
  const engine: Engine = new Engine(name, PARTITIONS, (): number => 1_000);
  const transport: KvTransport = fabric.transport(name);
  const transfer: FragmentTransfer = new FragmentTransfer(engine, transport);
  transport.listen(async (_from: string, body: Uint8Array): Promise<Uint8Array> => {
    const message: KvMessage = decodeMessage(body);
    if (message.kind === "fragment-request") {
      return encodeMessage({
        kind: "fragment-chunk",
        chunk: transfer.servePage(message.partitionId, message.afterKey),
      });
    }

    if (message.kind === "fragment-push") {
      transfer.applyChunk(message.chunk);
      return encodeMessage({ kind: "fragment-ack" });
    }

    if (message.kind === "peek-request") {
      return encodeMessage({ kind: "read-response", entry: engine.peek(message.key) });
    }

    if (message.kind === "replicate") {
      engine.merge(message.entry);
      return encodeMessage({ kind: "replicate-ack" });
    }

    throw new KvProtocolError("peer received an unexpected message");
  });
  return { engine, transport };
}

/** `count` distinct keys that all map to `partition`. */
function keysIn(partition: number, count: number): string[] {
  const keys: string[] = [];
  for (let index: number = 0; keys.length < count && index < 100_000; index += 1) {
    const key: string = `key-${index}`;
    if (partitionId(key, PARTITIONS) === partition) {
      keys.push(key);
    }
  }

  return keys;
}

/** The lowest key whose partition `who` is the ring primary of. */
function keyPrimariedBy(ring: PartitionRing, who: string): { key: string; partition: number } {
  for (let index: number = 0; index < 100_000; index += 1) {
    const key: string = `key-${index}`;
    const partition: number = partitionId(key, PARTITIONS);
    if (ring.primary(partition) === who) {
      return { key, partition };
    }
  }

  throw new Error(`no partition is primaried by ${who}`);
}

/** A table whose every partition is solely owned by `owner`. */
function ownedBy(owner: string, version: bigint): RoutingTable {
  return new RoutingTable(
    version,
    Array.from({ length: PARTITIONS }, (): string[] => [owner]),
  );
}

/** A view over A and B, with `dead` never a member so its departure reads as a death. */
function twoLive(): SimCluster {
  return new SimCluster("A", [member("A", 10), member("B", 20)]);
}

describe("Recovery crash reconcile", () => {
  it("recovers a write that lived only on a non-promoted survivor", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const { key, partition } = keyPrimariedBy(ring, "A");
    const a: PeerNode = peer(fabric, "A");
    const b: PeerNode = peer(fabric, "B");
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    const recovery: Recovery = new Recovery(a.engine, a.transport, twoLive());

    await settle(fabric, recovery.onTable(ownedBy("D", 1n), ring));
    await settle(fabric, recovery.onTable(RoutingTable.initial(ring, 2n), ring));

    expect(a.engine.peek(key)?.value).toEqual(bytes(9));
    expect(recovery.isRecovering(partition)).toBe(false);
  });

  it("marks a promoted partition recovering until its reconcile lands", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const { partition } = keyPrimariedBy(ring, "A");
    const a: PeerNode = peer(fabric, "A");
    peer(fabric, "B");
    const recovery: Recovery = new Recovery(a.engine, a.transport, twoLive(), { replicaCount: 3 });
    await settle(fabric, recovery.onTable(ownedBy("D", 1n), ring));

    const pass: Promise<void> = recovery.onTable(RoutingTable.initial(ring, 2n), ring);
    expect(recovery.isRecovering(partition)).toBe(true);
    await settle(fabric, pass);
    expect(recovery.isRecovering(partition)).toBe(false);
  });

  it("clears the flag even when a surviving backup is unreachable", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const { key, partition } = keyPrimariedBy(ring, "A");
    const a: PeerNode = peer(fabric, "A");
    const b: PeerNode = peer(fabric, "B");
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    fabric.partitionBoth("A", "B");
    const recovery: Recovery = new Recovery(a.engine, a.transport, twoLive(), { replicaCount: 3 });

    await settle(fabric, recovery.onTable(ownedBy("D", 1n), ring));
    await settle(fabric, recovery.onTable(RoutingTable.initial(ring, 2n), ring));

    expect(recovery.isRecovering(partition)).toBe(false);
    expect(a.engine.peek(key)).toBeUndefined();
  });
});

describe("Recovery non-crash tables", () => {
  it("recovers nothing from the very first table", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const { key, partition } = keyPrimariedBy(ring, "A");
    const a: PeerNode = peer(fabric, "A");
    const b: PeerNode = peer(fabric, "B");
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    const recovery: Recovery = new Recovery(a.engine, a.transport, twoLive(), { replicaCount: 3 });

    await settle(fabric, recovery.onTable(RoutingTable.initial(ring, 1n), ring));

    expect(recovery.isRecovering(partition)).toBe(false);
    expect(a.engine.peek(key)).toBeUndefined();
  });

  it("does not recover a partition it already primaried", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const { key, partition } = keyPrimariedBy(ring, "A");
    const a: PeerNode = peer(fabric, "A");
    const b: PeerNode = peer(fabric, "B");
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    const recovery: Recovery = new Recovery(a.engine, a.transport, twoLive(), { replicaCount: 3 });

    await settle(fabric, recovery.onTable(RoutingTable.initial(ring, 1n), ring));
    await settle(fabric, recovery.onTable(RoutingTable.initial(ring, 2n), ring));

    expect(recovery.isRecovering(partition)).toBe(false);
    expect(a.engine.peek(key)).toBeUndefined();
  });

  it("does not recover when the previous primary is still a live member", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const { key, partition } = keyPrimariedBy(ring, "A");
    const a: PeerNode = peer(fabric, "A");
    const b: PeerNode = peer(fabric, "B");
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    const recovery: Recovery = new Recovery(a.engine, a.transport, twoLive(), { replicaCount: 3 });

    await settle(fabric, recovery.onTable(ownedBy("B", 1n), ring));
    await settle(fabric, recovery.onTable(RoutingTable.initial(ring, 2n), ring));

    expect(recovery.isRecovering(partition)).toBe(false);
    expect(a.engine.peek(key)).toBeUndefined();
  });

  it("does not recover a fragmented partition or one it does not primary", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const mine: number = keyPrimariedBy(ring, "A").partition;
    const theirs: number = keyPrimariedBy(ring, "B").partition;
    const a: PeerNode = peer(fabric, "A");
    peer(fabric, "B");
    const recovery: Recovery = new Recovery(a.engine, a.transport, twoLive(), { replicaCount: 3 });
    const owners: string[][] = Array.from({ length: PARTITIONS }, (): string[] => ["A"]);
    owners[mine] = ["B", "A"];
    const fragmented: RoutingTable = new RoutingTable(2n, owners);

    await settle(fabric, recovery.onTable(ownedBy("D", 1n), ring));
    await settle(fabric, recovery.onTable(fragmented, ring));

    expect(recovery.isRecovering(mine)).toBe(false);
    expect(recovery.isRecovering(theirs)).toBe(false);
  });
});

/** A view naming `self`, with `other` a second live member; neither is used for drain. */
function pair(self: string, other: string): SimCluster {
  return new SimCluster(self, [member(self, 10), member(other, 20)]);
}

/** A table whose `partition` is owned by `[previous, primary]` and the rest by their ring primary. */
function demoting(
  ring: PartitionRing,
  partition: number,
  previous: string,
  primary: string,
): RoutingTable {
  const owners: string[][] = Array.from(
    { length: PARTITIONS },
    (_unused: unknown, id: number): string[] => [ring.primary(id)],
  );
  owners[partition] = [previous, primary];
  return new RoutingTable(2n, owners);
}

describe("Recovery graceful drain", () => {
  it("streams a demoted fragment to the new primary and drops it", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["P"], PARTITIONS);
    const l: PeerNode = peer(fabric, "L");
    const p: PeerNode = peer(fabric, "P");
    const key: string = "drain-key";
    const partition: number = partitionId(key, PARTITIONS);
    l.engine.merge(entryAt(key, bytes(7), 1_000));
    const table: RoutingTable = demoting(ring, partition, "L", "P");
    const recovery: Recovery = new Recovery(l.engine, l.transport, pair("L", "P"), {
      replicaCount: 2,
    });

    const drained: boolean = await settle(fabric, recovery.drain(table, ring));
    expect(drained).toBe(true);
    expect(p.engine.peek(key)?.value).toEqual(bytes(7));
    expect(l.engine.peek(key)).toBeUndefined();
  });

  it("keeps the fragment when this node stays a backup", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["L", "P"], PARTITIONS);
    const { key, partition } = keyPrimariedBy(ring, "P");
    const l: PeerNode = peer(fabric, "L");
    peer(fabric, "P");
    l.engine.merge(entryAt(key, bytes(7), 1_000));
    const table: RoutingTable = demoting(ring, partition, "L", "P");
    const recovery: Recovery = new Recovery(l.engine, l.transport, pair("L", "P"), {
      replicaCount: 2,
    });

    const drained: boolean = await settle(fabric, recovery.drain(table, ring));
    expect(drained).toBe(true);
    expect(l.engine.peek(key)?.value).toEqual(bytes(7));
  });

  it("reports an incomplete drain when the new primary is unreachable", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const ring: PartitionRing = new PartitionRing(["P"], PARTITIONS);
    const l: PeerNode = peer(fabric, "L");
    peer(fabric, "P");
    const key: string = "drain-key";
    const partition: number = partitionId(key, PARTITIONS);
    l.engine.merge(entryAt(key, bytes(7), 1_000));
    const table: RoutingTable = demoting(ring, partition, "L", "P");
    fabric.partitionBoth("L", "P");
    const recovery: Recovery = new Recovery(l.engine, l.transport, pair("L", "P"), {
      replicaCount: 2,
    });

    const drained: boolean = await settle(fabric, recovery.drain(table, ring));
    expect(drained).toBe(false);
    expect(l.engine.peek(key)?.value).toEqual(bytes(7));
  });
});

describe("Recovery refill", () => {
  it("streams the fragment to a backup the ring newly adds", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const one: PartitionRing = new PartitionRing(["A"], PARTITIONS);
    const two: PartitionRing = new PartitionRing(["A", "B"], PARTITIONS);
    const { key, partition } = keyPrimariedBy(two, "A");
    const a: PeerNode = peer(fabric, "A");
    const b: PeerNode = peer(fabric, "B");
    a.engine.merge(entryAt(key, bytes(4), 1_000));
    const recovery: Recovery = new Recovery(a.engine, a.transport, pair("A", "B"), {
      replicaCount: 2,
    });

    await settle(fabric, recovery.onTable(RoutingTable.initial(one, 1n), one));
    await settle(fabric, recovery.onTable(RoutingTable.initial(two, 2n), two));

    expect(b.engine.peek(key)?.value).toEqual(bytes(4));
    expect(recovery.isRecovering(partition)).toBe(false);
  });
});

describe("Recovery stale rejoin", () => {
  it("reseeds only past the tombstone window", () => {
    expect(Recovery.shouldReseed(TOMBSTONE_TTL_MS + 1)).toBe(true);
    expect(Recovery.shouldReseed(TOMBSTONE_TTL_MS)).toBe(false);
    expect(Recovery.shouldReseed(0)).toBe(false);
  });

  it("discards its stale fragment and re-seeds from the current owner", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const partition: number = partitionId("key-0", PARTITIONS);
    const [staleKey, freshKey] = keysIn(partition, 2) as [string, string];
    const a: PeerNode = peer(fabric, "A");
    const b: PeerNode = peer(fabric, "B");
    a.engine.merge(entryAt(staleKey, bytes(1), 1_000));
    b.engine.merge(entryAt(freshKey, bytes(2), 3_000));
    const recovery: Recovery = new Recovery(a.engine, a.transport, pair("A", "B"));

    await settle(fabric, recovery.reseed(partition, "B"));

    expect(a.engine.peek(staleKey)).toBeUndefined();
    expect(a.engine.peek(freshKey)?.value).toEqual(bytes(2));
  });
});
