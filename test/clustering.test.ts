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

import { afterEach, describe, expect, it } from "vitest";
import {
  Cluster,
  type ClusterClock,
  type ClusterOptions,
  type ClusterTimer,
} from "../src/clustering";
import { type ClusterEvent, ClusterEventType } from "../src/clustering.events";
import {
  DRAIN_POLL_INTERVAL_MS,
  LEAVE_DRAIN_TIMEOUT_MS,
  REPAIR_INTERVAL_MS,
  STABLE_VIEW_QUIET_MS,
  TABLE_PUSH_INTERVAL_MS,
  TOMBSTONE_TTL_MS,
} from "../src/kv/constants";
import { ClusterUnavailableError } from "../src/kv/errors";
import type { ClusterMember, Entry, KvTransport, ScanEntry, WriteResult } from "../src/kv/ports";
import { decodeMessage, encodeMessage, type KvMessage, MessageKind } from "../src/kv/wire";
import {
  flush,
  member,
  type SimClock,
  SimCluster,
  SimFabric,
  type SimTimer,
  settle,
} from "./kv/sim";

/** A cluster node under test: its scripted membership view and the engine over it. */
interface Node {
  readonly view: SimCluster;
  readonly cluster: Cluster;
}

/** Clusters started by a test, closed after it regardless of outcome. */
const opened: Cluster[] = [];

afterEach(async (): Promise<void> => {
  await Promise.all(opened.map((cluster: Cluster): Promise<void> => cluster.stop()));
  opened.length = 0;
});

/** Adapts the harness clock to the {@link ClusterClock} port the engine schedules on. */
function clusterClock(sim: SimClock): ClusterClock {
  return {
    now: (): number => sim.now(),
    schedule: (delayMs: number, callback: () => void): ClusterTimer => {
      const timer: SimTimer = sim.schedule(delayMs, callback);
      return { cancel: (): void => sim.cancel(timer) };
    },
  };
}

/** Builds and starts a node over `fabric`, wired to the shared simulation clock. */
function build(fabric: SimFabric, self: string, members: readonly ClusterMember[]): Node {
  const view: SimCluster = new SimCluster(self, members);
  const options: ClusterOptions = {
    view,
    transport: fabric.transport(self),
    clock: clusterClock(fabric.clock),
    partitionCount: 8,
  };
  const cluster: Cluster = new Cluster(options);
  opened.push(cluster);
  cluster.start();
  return { view, cluster };
}

describe("Cluster formation and routing", () => {
  it("forms a cluster and serves a cross-node write and read through the real dispatch", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const members: readonly ClusterMember[] = [
      member("a:1", 1),
      member("b:1", 2),
      member("c:1", 3),
    ];
    const a: Node = build(fabric, "a:1", members);
    const b: Node = build(fabric, "b:1", members);
    const c: Node = build(fabric, "c:1", members);

    // The first membership event forms the cluster: the oldest node becomes
    // coordinator, computes the routing table, and pushes it to the others.
    for (const node of [a, b, c]) {
      node.view.set(members);
    }

    await settle(fabric, flush(300));

    const applied: WriteResult = await settle(
      fabric,
      a.cluster.write({
        kind: "put",
        key: "greeting",
        value: Uint8Array.of(1, 2, 3),
        condition: "none",
      }),
    );
    expect(applied.applied).toBe(true);

    const entry: Entry | undefined = await settle(fabric, c.cluster.read("greeting"));
    expect(Array.from(entry?.value ?? new Uint8Array())).toEqual([1, 2, 3]);
  });

  it("does not reinstall the table when an unchanged membership event repeats", async () => {
    const fabric: SimFabric = new SimFabric(2);
    const members: readonly ClusterMember[] = [member("a:1", 1), member("b:1", 2)];
    const a: Node = build(fabric, "a:1", members);
    const b: Node = build(fabric, "b:1", members);

    for (const node of [a, b]) {
      node.view.set(members);
    }

    await settle(fabric, flush(300));

    // A second, identical membership event recomputes the same table at the same
    // version, so every node skips a redundant install and still routes writes.
    for (const node of [a, b]) {
      node.view.set(members);
    }

    await settle(fabric, flush(300));

    const applied: WriteResult = await settle(
      fabric,
      a.cluster.write({ kind: "put", key: "k", value: Uint8Array.of(9), condition: "none" }),
    );
    expect(applied.applied).toBe(true);
  });

  it("rejects an inbound message it cannot route", async () => {
    const fabric: SimFabric = new SimFabric(3);
    const members: readonly ClusterMember[] = [member("a:1", 1)];
    const a: Node = build(fabric, "a:1", members);
    a.view.set(members);
    await settle(fabric, flush(100));

    const client: KvTransport = fabric.transport("client:1");
    // A read-response is a reply kind, never an inbound request, so the router
    // has no server for it and the request fails.
    const unroutable: Uint8Array = encodeMessage({
      kind: MessageKind.readResponse,
      entry: undefined,
    });
    await expect(settle(fabric, client.request("a:1", unroutable, 1_000))).rejects.toThrow();
    await client.stop();
  });

  it("routes an anti-entropy digest request to the responder", async () => {
    const fabric: SimFabric = new SimFabric(4);
    const members: readonly ClusterMember[] = [member("a:1", 1)];
    const a: Node = build(fabric, "a:1", members);
    a.view.set(members);
    await settle(fabric, flush(100));

    const client: KvTransport = fabric.transport("client:1");
    const digest: Uint8Array = encodeMessage({
      kind: MessageKind.syncDigest,
      partitionId: 0,
      digest: { hi: 0, lo: 0 },
    });
    const response: Uint8Array = await settle(fabric, client.request("a:1", digest, 5_000));
    const decoded: KvMessage = decodeMessage(response);
    expect(decoded.kind).toBe(MessageKind.bucketDigests);
    await client.stop();
  });

  it("defaults the partition count and sizing when only the ports are given", async () => {
    const fabric: SimFabric = new SimFabric(5);
    const view: SimCluster = new SimCluster("solo:1", [member("solo:1", 1)]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("solo:1"),
      clock: clusterClock(fabric.clock),
    });
    cluster.start();
    await expect(cluster.stop()).resolves.toBeUndefined();
  });
});

describe("Cluster periodic timers", () => {
  it("runs the janitor, re-push, and repair timers on a lone node and keeps serving", async () => {
    const fabric: SimFabric = new SimFabric(6);
    const members: readonly ClusterMember[] = [member("solo:1", 1)];
    const solo: Node = build(fabric, "solo:1", members);
    solo.view.set(members);
    await settle(fabric, flush(100));

    // A lone node primaries every partition with no replica, so the repair tick
    // finds no target; advancing past every interval still fires the janitor and
    // the coordinator re-push without error.
    fabric.clock.advanceBy(TABLE_PUSH_INTERVAL_MS + 1);
    await flush(100);

    const applied: WriteResult = await settle(
      fabric,
      solo.cluster.write({ kind: "put", key: "k", value: Uint8Array.of(7), condition: "none" }),
    );
    expect(applied.applied).toBe(true);
    const entry: Entry | undefined = await settle(fabric, solo.cluster.read("k"));
    expect(Array.from(entry?.value ?? new Uint8Array())).toEqual([7]);
  });

  it("runs an anti-entropy comparison against a replica on the repair tick", async () => {
    const fabric: SimFabric = new SimFabric(7);
    const members: readonly ClusterMember[] = [
      member("a:1", 1),
      member("b:1", 2),
      member("c:1", 3),
    ];
    const a: Node = build(fabric, "a:1", members);
    const b: Node = build(fabric, "b:1", members);
    const c: Node = build(fabric, "c:1", members);
    for (const node of [a, b, c]) {
      node.view.set(members);
    }

    await settle(fabric, flush(300));
    await settle(
      fabric,
      a.cluster.write({
        kind: "put",
        key: "greeting",
        value: Uint8Array.of(1, 2, 3),
        condition: "none",
      }),
    );

    // The repair tick reconciles one of this node's partitions against a replica;
    // advancing past the interval fires it and the comparison runs to convergence.
    fabric.clock.advanceBy(REPAIR_INTERVAL_MS + 1);
    await settle(fabric, flush(200));

    const entry: Entry | undefined = await settle(fabric, c.cluster.read("greeting"));
    expect(Array.from(entry?.value ?? new Uint8Array())).toEqual([1, 2, 3]);
  });
});

describe("Cluster ring membership", () => {
  it("falls back to the full member set for the ring when every member is draining", async () => {
    const fabric: SimFabric = new SimFabric(8);
    const draining: readonly ClusterMember[] = [member("solo:1", 1, { draining: true })];
    const view: SimCluster = new SimCluster("solo:1", draining);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("solo:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 4,
    });
    opened.push(cluster);
    cluster.start();
    view.set(draining);
    await settle(fabric, flush(100));

    // The node still forms a table over the fallback ring and serves a write.
    const applied: WriteResult = await settle(
      fabric,
      cluster.write({ kind: "put", key: "k", value: Uint8Array.of(1), condition: "none" }),
    );
    expect(applied.applied).toBe(true);
  });
});

describe("Cluster split-brain resolution", () => {
  it("stops the minority half and refuses operations until the view heals", async () => {
    const fabric: SimFabric = new SimFabric(9);
    const members: readonly ClusterMember[] = [
      member("a:1", 1),
      member("b:1", 2),
      member("c:1", 3),
    ];
    const view: SimCluster = new SimCluster("a:1", members);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("a:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 8,
      minimumMemberQuorum: 2,
    });
    opened.push(cluster);
    cluster.start();

    // The full view becomes the split-brain baseline once it has held unchanged
    // across the quiet window.
    view.set(members);
    fabric.clock.advanceBy(STABLE_VIEW_QUIET_MS + 1);
    await flush(50);
    expect(cluster.serving).toBe(true);

    // A partition strands this node alone: one of three cannot reach a majority
    // of the last stable size, so it stops serving and refuses reads and writes.
    view.set([member("a:1", 1)]);
    await flush(50);
    expect(cluster.serving).toBe(false);
    await expect(
      cluster.write({ kind: "put", key: "k", value: Uint8Array.of(1), condition: "none" }),
    ).rejects.toBeInstanceOf(ClusterUnavailableError);
    await expect(cluster.read("k")).rejects.toBeInstanceOf(ClusterUnavailableError);
    await expect(cluster.scan()).rejects.toBeInstanceOf(ClusterUnavailableError);

    // While stopped, the quiet timer and the repair tick both no-op, so the
    // baseline never shrinks to one and the half cannot vote itself a majority.
    fabric.clock.advanceBy(STABLE_VIEW_QUIET_MS + 1);
    await flush(50);
    expect(cluster.serving).toBe(false);

    // The partition heals: reaching the majority again, the node resumes serving.
    view.set(members);
    await flush(50);
    expect(cluster.serving).toBe(true);
  });

  it("keeps serving through any view when the resolver is disabled by default", async () => {
    const fabric: SimFabric = new SimFabric(10);
    const view: SimCluster = new SimCluster("solo:1", [member("solo:1", 1)]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("solo:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 4,
    });
    opened.push(cluster);
    cluster.start();

    view.set([member("solo:1", 1)]);
    fabric.clock.advanceBy(STABLE_VIEW_QUIET_MS + 1);
    await flush(50);

    // With the default member quorum of one the resolver never stops a half, so a
    // lone node that has lost every peer keeps serving rather than fork-fearing.
    view.set([]);
    await flush(50);
    expect(cluster.serving).toBe(true);
  });

  it("discards its stale fragments when it rejoins past the tombstone window", async () => {
    const fabric: SimFabric = new SimFabric(11);
    const members: readonly ClusterMember[] = [
      member("a:1", 1),
      member("b:1", 2),
      member("c:1", 3),
    ];
    const view: SimCluster = new SimCluster("a:1", [member("a:1", 1)]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("a:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 8,
      minimumMemberQuorum: 2,
    });
    opened.push(cluster);
    cluster.start();

    // Form as a lone node and write a key, so this node holds a fragment.
    view.set([member("a:1", 1)]);
    await settle(fabric, flush(200));
    await settle(
      fabric,
      cluster.write({ kind: "put", key: "k", value: Uint8Array.of(5), condition: "none" }),
    );
    expect(cluster.heldPartitionCount).toBeGreaterThan(0);

    // The cluster grows to three and holds that size long enough to become the
    // baseline, then a partition strands this node alone so the resolver stops it.
    view.set(members);
    fabric.clock.advanceBy(STABLE_VIEW_QUIET_MS + 1);
    await flush(50);
    view.set([member("a:1", 1)]);
    await flush(50);
    expect(cluster.serving).toBe(false);
    expect(cluster.heldPartitionCount).toBeGreaterThan(0);

    // It stays away past the tombstone lifetime, so its held data is now stale.
    fabric.clock.advanceBy(TOMBSTONE_TTL_MS + 1_000);
    await flush(50);

    // On heal it discards every stale fragment rather than merge a reaped key back.
    view.set(members);
    await flush(50);
    expect(cluster.serving).toBe(true);
    expect(cluster.heldPartitionCount).toBe(0);
  });

  it("installs no table from a push while the resolver has it stopped", async () => {
    const fabric: SimFabric = new SimFabric(20);
    const pair: readonly ClusterMember[] = [member("a:1", 1), member("b:1", 2)];
    // a:1 stays the coordinator in the majority; b:1's own view will shrink to
    // itself so its resolver stops it, while the fabric keeps a:1 able to reach it.
    const a: SimCluster = new SimCluster("a:1", pair);
    const coordinator: Cluster = new Cluster({
      view: a,
      transport: fabric.transport("a:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 8,
      minimumMemberQuorum: 2,
    });
    const b: SimCluster = new SimCluster("b:1", pair);
    const stopped: Cluster = new Cluster({
      view: b,
      transport: fabric.transport("b:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 8,
      minimumMemberQuorum: 2,
    });
    opened.push(coordinator, stopped);
    coordinator.start();
    stopped.start();
    a.set(pair);
    b.set(pair);
    fabric.clock.advanceBy(STABLE_VIEW_QUIET_MS + 1);
    await settle(fabric, flush(200));

    // b:1 loses sight of the majority and stops; a:1 still sees and can reach it.
    b.set([member("b:1", 2)]);
    await flush(50);
    expect(stopped.serving).toBe(false);

    // a:1 re-authors and pushes the table to b:1, which must ignore it while stopped
    // rather than install a topology it no longer owns.
    a.set(pair);
    await settle(fabric, flush(200));
    expect(stopped.serving).toBe(false);
    expect(stopped.heldPartitionCount).toBe(0);
  });

  it("reconverges the table when the oldest node heals from a stopped minority", async () => {
    const fabric: SimFabric = new SimFabric(21);
    const members: readonly ClusterMember[] = [
      member("a:1", 1),
      member("b:1", 2),
      member("c:1", 3),
    ];
    const make = (self: string): Node => {
      const view: SimCluster = new SimCluster(self, members);
      const cluster: Cluster = new Cluster({
        view,
        transport: fabric.transport(self),
        clock: clusterClock(fabric.clock),
        partitionCount: 8,
        minimumMemberQuorum: 2,
      });
      opened.push(cluster);
      cluster.start();
      return { view, cluster };
    };
    const a: Node = make("a:1");
    const b: Node = make("b:1");
    const c: Node = make("c:1");
    for (const node of [a, b, c]) {
      node.view.set(members);
    }

    fabric.clock.advanceBy(STABLE_VIEW_QUIET_MS + 1);
    await settle(fabric, flush(300));

    // Partition the oldest node into a stopped minority; the majority keeps serving
    // and authors a new table version while it is away.
    fabric.partitionBoth("a:1", "b:1");
    fabric.partitionBoth("a:1", "c:1");
    a.view.set([member("a:1", 1)]);
    b.view.set([member("b:1", 2), member("c:1", 3)]);
    c.view.set([member("b:1", 2), member("c:1", 3)]);
    await settle(fabric, flush(300));
    expect(a.cluster.serving).toBe(false);
    expect(b.cluster.serving).toBe(true);
    await settle(
      fabric,
      b.cluster.write({ kind: "put", key: "during", value: Uint8Array.of(2), condition: "none" }),
    );
    const splitVersion: bigint = b.cluster.routingSignature?.version as bigint;

    // Heal: the oldest node returns and resumes as coordinator holding a stale table.
    fabric.partitionBoth("a:1", "b:1", false);
    fabric.partitionBoth("a:1", "c:1", false);
    for (const node of [a, b, c]) {
      node.view.set(members);
    }

    await settle(fabric, flush(600));

    // It authors above the version the majority reached rather than forking a rival
    // table at or below it, and all three nodes converge on that one table.
    expect(a.cluster.serving).toBe(true);
    const signature: { version: bigint; primaries: readonly string[] } | undefined =
      a.cluster.routingSignature;
    expect(signature).toBeDefined();
    expect((signature?.version as bigint) > splitVersion).toBe(true);
    expect(b.cluster.routingSignature).toEqual(signature);
    expect(c.cluster.routingSignature).toEqual(signature);
  });
});

describe("Cluster drain wait", () => {
  it("resolves at once when the node holds no partition", async () => {
    const fabric: SimFabric = new SimFabric(12);
    const view: SimCluster = new SimCluster("solo:1", [member("solo:1", 1)]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("solo:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 4,
    });
    opened.push(cluster);
    cluster.start();

    await expect(cluster.awaitDrained(1_000)).resolves.toBeUndefined();
  });

  it("resolves at the backstop when its partitions never drain", async () => {
    const fabric: SimFabric = new SimFabric(13);
    // A draining peer leaves a:1 the sole assignable owner, so a:1 primaries every
    // partition, yet the two-member view keeps awaitDrained polling rather than
    // short-circuiting the way a node alone in its view does.
    const members: readonly ClusterMember[] = [
      member("a:1", 1),
      member("b:1", 2, { draining: true }),
    ];
    const view: SimCluster = new SimCluster("a:1", members);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("a:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 4,
    });
    opened.push(cluster);
    cluster.start();
    view.set(members);
    await settle(fabric, flush(100));
    await settle(
      fabric,
      cluster.write({ kind: "put", key: "k", value: Uint8Array.of(1), condition: "none" }),
    );
    expect(cluster.heldPartitionCount).toBeGreaterThan(0);

    // a:1 has no reachable peer to hand its partitions to, so the wait polls until
    // the backstop elapses and returns with the data still held.
    await settle(fabric, cluster.awaitDrained(DRAIN_POLL_INTERVAL_MS * 3));
    expect(cluster.heldPartitionCount).toBeGreaterThan(0);
  });
});

describe("Cluster scan", () => {
  it("gathers live entries from every partition primary across the cluster", async () => {
    const fabric: SimFabric = new SimFabric(30);
    const members: readonly ClusterMember[] = [
      member("a:1", 1),
      member("b:1", 2),
      member("c:1", 3),
    ];
    const a: Node = build(fabric, "a:1", members);
    const b: Node = build(fabric, "b:1", members);
    const c: Node = build(fabric, "c:1", members);
    for (const node of [a, b, c]) {
      node.view.set(members);
    }

    await settle(fabric, flush(300));

    // Keys spread across the partitions, some primaried by this node and some by
    // its peers, so the scan reads both the local store and remote primaries.
    const keys: readonly string[] = ["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"];
    for (const key of keys) {
      await settle(
        fabric,
        a.cluster.write({ kind: "put", key, value: Uint8Array.of(key.length), condition: "none" }),
      );
    }

    // Deleting one key leaves a tombstone the scan must skip, reporting present
    // state rather than the merge history.
    await settle(fabric, a.cluster.write({ kind: "delete", key: "k9" }));

    const entries: ScanEntry[] = await settle(fabric, a.cluster.scan());
    const scanned: string[] = entries.map((entry: ScanEntry): string => entry.key).sort();
    expect(scanned).toEqual(["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8"]);
  });

  it("scans nothing before the node holds a routing table", async () => {
    const fabric: SimFabric = new SimFabric(31);
    const view: SimCluster = new SimCluster("solo:1", [member("solo:1", 1)]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("solo:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 4,
    });
    opened.push(cluster);
    cluster.start();

    // No membership event has formed a table yet, so the scan yields nothing and
    // the node has no routing signature to compare.
    expect(await cluster.scan()).toEqual([]);
    expect(cluster.routingSignature).toBeUndefined();
  });
});

describe("Cluster lifecycle events", () => {
  it("emits coordinator and rebalance events as a node forms", async () => {
    const fabric: SimFabric = new SimFabric(40);
    const captured: ClusterEvent[] = [];
    const view: SimCluster = new SimCluster("a:1", [member("a:1", 1)]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("a:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 8,
      events: (event: ClusterEvent): void => {
        captured.push(event);
      },
    });
    opened.push(cluster);
    cluster.start();
    view.set([member("a:1", 1)]);
    await settle(fabric, flush(200));

    const types: string[] = captured.map((event: ClusterEvent): string => event.type);
    expect(types).toContain(ClusterEventType.coordinatorChanged);
    expect(types).toContain(ClusterEventType.rebalanceStarted);
    expect(types).toContain(ClusterEventType.rebalanceCompleted);
  });

  it("reports only its peers joining and leaving, never itself", async () => {
    const fabric: SimFabric = new SimFabric(43);
    const captured: ClusterEvent[] = [];
    const view: SimCluster = new SimCluster("a:1", [member("a:1", 1), member("b:1", 2)]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("a:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 8,
      events: (event: ClusterEvent): void => {
        captured.push(event);
      },
    });
    opened.push(cluster);
    cluster.start();
    view.set([member("a:1", 1), member("b:1", 2)]);
    await settle(fabric, flush(200));

    // Forming, this node announces its peer b:1 joining but never publishes a join
    // for itself, so a subscriber hears about the rest of the cluster, not itself.
    const joins: ClusterEvent[] = captured.filter(
      (event: ClusterEvent): boolean => event.type === ClusterEventType.nodeJoined,
    );
    expect(joins).toContainEqual({ type: ClusterEventType.nodeJoined, address: "b:1" });
    expect(joins).not.toContainEqual({ type: ClusterEventType.nodeJoined, address: "a:1" });

    // Even if this node falls out of its own view, it publishes no departure for
    // itself, and the repair backstop releases nothing on its behalf.
    captured.length = 0;
    view.set([member("b:1", 2)]);
    fabric.clock.advanceBy(LEAVE_DRAIN_TIMEOUT_MS + 1);
    await flush(50);
    const departures: ClusterEvent[] = captured.filter(
      (event: ClusterEvent): boolean => event.type === ClusterEventType.nodeLeft,
    );
    expect(departures).not.toContainEqual({ type: ClusterEventType.nodeLeft, address: "a:1" });
  });

  it("reports a departure only after the repair epoch closes", async () => {
    const fabric: SimFabric = new SimFabric(41);
    const captured: ClusterEvent[] = [];
    const view: SimCluster = new SimCluster("a:1", [member("a:1", 1), member("b:1", 2)]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("a:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 8,
      events: (event: ClusterEvent): void => {
        captured.push(event);
      },
    });
    opened.push(cluster);
    cluster.start();
    view.set([member("a:1", 1), member("b:1", 2)]);
    await settle(fabric, flush(200));
    captured.length = 0;

    // The coordinator installs a table that repairs the departed node's partitions;
    // the departure is reported once that recovery has settled, not before.
    view.set([member("a:1", 1)]);
    await settle(fabric, flush(300));
    expect(
      captured.filter((event: ClusterEvent): boolean => event.type === ClusterEventType.nodeLeft),
    ).toEqual([{ type: ClusterEventType.nodeLeft, address: "b:1" }]);
  });

  it("reports a departure through the backstop when no repair settles it", async () => {
    const fabric: SimFabric = new SimFabric(42);
    const captured: ClusterEvent[] = [];
    // This node is a follower behind the older a:1, and neither a:1 nor c:1 is a
    // real node, so the departure of c:1 installs no table on this follower.
    const view: SimCluster = new SimCluster("b:1", [
      member("a:1", 1),
      member("b:1", 2),
      member("c:1", 3),
    ]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("b:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 8,
      events: (event: ClusterEvent): void => {
        captured.push(event);
      },
    });
    opened.push(cluster);
    cluster.start();
    view.set([member("a:1", 1), member("b:1", 2), member("c:1", 3)]);
    await flush(50);
    captured.length = 0;

    view.set([member("a:1", 1), member("b:1", 2)]);
    await flush(50);
    // No table install repairs c:1's partitions here, so the departure waits.
    expect(
      captured.filter((event: ClusterEvent): boolean => event.type === ClusterEventType.nodeLeft),
    ).toEqual([]);

    // The backstop reports it regardless once the repair budget elapses.
    fabric.clock.advanceBy(LEAVE_DRAIN_TIMEOUT_MS + 1);
    await flush(50);
    expect(
      captured.filter((event: ClusterEvent): boolean => event.type === ClusterEventType.nodeLeft),
    ).toEqual([{ type: ClusterEventType.nodeLeft, address: "c:1" }]);
  });

  it("drops a departure whose node rejoins before its repair epoch closes", async () => {
    const fabric: SimFabric = new SimFabric(44);
    const captured: ClusterEvent[] = [];
    const view: SimCluster = new SimCluster("a:1", [member("a:1", 1), member("b:1", 2)]);
    const cluster: Cluster = new Cluster({
      view,
      transport: fabric.transport("a:1"),
      clock: clusterClock(fabric.clock),
      partitionCount: 8,
      events: (event: ClusterEvent): void => {
        captured.push(event);
      },
    });
    opened.push(cluster);
    cluster.start();
    view.set([member("a:1", 1), member("b:1", 2)]);
    await settle(fabric, flush(200));
    captured.length = 0;

    // b:1 departs and rejoins before the repair epoch that would report it closes,
    // so it is a live member again when the gate opens and no `node-left` is emitted
    // for it, only the `node-joined` of its return.
    view.set([member("a:1", 1)]);
    view.set([member("a:1", 1), member("b:1", 2)]);
    await settle(fabric, flush(300));
    fabric.clock.advanceBy(LEAVE_DRAIN_TIMEOUT_MS + 1);
    await settle(fabric, flush(50));

    expect(
      captured.filter((event: ClusterEvent): boolean => event.type === ClusterEventType.nodeLeft),
    ).toEqual([]);
    expect(captured).toContainEqual({ type: ClusterEventType.nodeJoined, address: "b:1" });
  });
});
