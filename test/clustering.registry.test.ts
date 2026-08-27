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
import { Cluster, type ClusterClock, type ClusterTimer } from "../src/clustering";
import { ClusterRegistry } from "../src/clustering.registry";
import {
  flush,
  member,
  type SimClock,
  SimCluster,
  SimFabric,
  type SimTimer,
  settle,
} from "./kv/sim";

/** Clusters started by a test, stopped after it regardless of outcome. */
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

/** Builds, starts, and forms a lone-node cluster that primaries every partition. */
async function loneCluster(fabric: SimFabric): Promise<Cluster> {
  const view: SimCluster = new SimCluster("solo:1", [member("solo:1", 1)]);
  const cluster: Cluster = new Cluster({
    view,
    transport: fabric.transport("solo:1"),
    clock: clusterClock(fabric.clock),
    partitionCount: 8,
  });
  opened.push(cluster);
  cluster.start();
  view.set([member("solo:1", 1)]);
  await settle(fabric, flush(100));
  return cluster;
}

describe("ClusterRegistry", () => {
  it("claims a name once and rejects a duplicate claim", async () => {
    const fabric: SimFabric = new SimFabric(20);
    const cluster: Cluster = await loneCluster(fabric);
    const registry: ClusterRegistry = new ClusterRegistry(cluster);

    expect(await settle(fabric, registry.claimActorName("worker", "node-a"))).toBe(true);
    expect(await settle(fabric, registry.claimActorName("worker", "node-b"))).toBe(false);
    expect(await settle(fabric, registry.getActor("worker"))).toBe("node-a");
    expect(await settle(fabric, registry.actorExists("worker"))).toBe(true);
  });

  it("reports an unknown name as absent", async () => {
    const fabric: SimFabric = new SimFabric(21);
    const cluster: Cluster = await loneCluster(fabric);
    const registry: ClusterRegistry = new ClusterRegistry(cluster);

    expect(await settle(fabric, registry.getActor("missing"))).toBeUndefined();
    expect(await settle(fabric, registry.actorExists("missing"))).toBe(false);
  });

  it("overwrites with putActor and clears with removeActor", async () => {
    const fabric: SimFabric = new SimFabric(22);
    const cluster: Cluster = await loneCluster(fabric);
    const registry: ClusterRegistry = new ClusterRegistry(cluster);

    await settle(fabric, registry.putActor("svc", "first"));
    await settle(fabric, registry.putActor("svc", "second"));
    expect(await settle(fabric, registry.getActor("svc"))).toBe("second");

    await settle(fabric, registry.removeActor("svc"));
    expect(await settle(fabric, registry.getActor("svc"))).toBeUndefined();
    expect(await settle(fabric, registry.actorExists("svc"))).toBe(false);
  });

  it("increments a shared round-robin counter from one", async () => {
    const fabric: SimFabric = new SimFabric(23);
    const cluster: Cluster = await loneCluster(fabric);
    const registry: ClusterRegistry = new ClusterRegistry(cluster);

    expect(await settle(fabric, registry.nextRoundRobinValue("rr"))).toBe(1n);
    expect(await settle(fabric, registry.nextRoundRobinValue("rr"))).toBe(2n);
    expect(await settle(fabric, registry.nextRoundRobinValue("rr"))).toBe(3n);
  });

  it("holds a lease once and grants it again only after it expires", async () => {
    const fabric: SimFabric = new SimFabric(24);
    const cluster: Cluster = await loneCluster(fabric);
    const registry: ClusterRegistry = new ClusterRegistry(cluster);

    expect(await settle(fabric, registry.claimOnce("singleton", "owner-a", 1_000))).toBe(true);
    expect(await settle(fabric, registry.claimOnce("singleton", "owner-b", 1_000))).toBe(false);

    // Past the lease the claim lapses, so the next caller wins it afresh.
    fabric.clock.advanceBy(1_001);
    expect(await settle(fabric, registry.claimOnce("singleton", "owner-b", 1_000))).toBe(true);
    expect(await settle(fabric, registry.getActor("singleton"))).toBe("owner-b");
  });

  it("finds actors by host and ignores values that are not an address", async () => {
    const fabric: SimFabric = new SimFabric(25);
    const cluster: Cluster = await loneCluster(fabric);
    const registry: ClusterRegistry = new ClusterRegistry(cluster);

    await settle(fabric, registry.putActor("a1", "10.0.0.1:7000"));
    await settle(fabric, registry.putActor("a2", "10.0.0.1:7001"));
    await settle(fabric, registry.putActor("b1", "10.0.0.2:7000"));
    await settle(fabric, registry.putActor("odd", "not-an-address"));

    const onFirst: string[] = (await settle(fabric, registry.actorsByHost("10.0.0.1"))).sort();
    expect(onFirst).toEqual(["a1", "a2"]);
    expect(await settle(fabric, registry.countActorsByHost("10.0.0.2"))).toBe(1);
    expect(await settle(fabric, registry.actorsByHost("10.0.0.9"))).toEqual([]);
  });
});
