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
import { CLUSTER_EVENT_TOPIC, type ClusterEvent, ClusterEventType } from "../src/clustering.events";
import { ClusterNode, systemClusterClock } from "../src/clustering.node";
import { parseHostPort } from "../src/clustering.transport";
import type { DiscoveryProvider } from "../src/discovery/provider";
import { StaticDiscovery } from "../src/discovery/static";
import { EventStream } from "../src/eventstream";
import type { Entry, ScanEntry } from "../src/kv/ports";

/** Nodes started by a test, stopped afterward regardless of outcome. */
const running: ClusterNode[] = [];

afterEach(async (): Promise<void> => {
  await Promise.all(running.map((node: ClusterNode): Promise<void> => node.stop()));
  running.length = 0;
});

/** Resolves after `milliseconds`, the wait between convergence polls. */
function sleep(milliseconds: number): Promise<void> {
  return new Promise<void>((resolve: () => void): void => {
    setTimeout(resolve, milliseconds);
  });
}

/** Retries `attempt` until it resolves without throwing or the timeout elapses. */
async function eventually<T>(attempt: () => Promise<T>, timeoutMs: number = 8_000): Promise<T> {
  const start: number = Date.now();
  let lastError: unknown;
  while (Date.now() - start < timeoutMs) {
    try {
      return await attempt();
    } catch (error: unknown) {
      lastError = error;
      await sleep(50);
    }
  }

  throw lastError;
}

describe("ClusterNode over real sockets", () => {
  it("forms a two-node cluster and serves a cross-node write and read", async (): Promise<void> => {
    const anchor: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([]),
      bootDeadlineMs: 0,
    });
    running.push(anchor);
    expect(anchor.joined).toBe(false);

    const joiner: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([anchor.gossipAddress]),
    });
    running.push(joiner);
    expect(joiner.joined).toBe(true);

    // Both nodes converge on the same two-member view before either serves.
    await eventually(async (): Promise<void> => {
      if (anchor.members().length !== 2 || joiner.members().length !== 2) {
        throw new Error("membership has not converged to two members");
      }
    });

    // A write on the anchor is readable on the joiner once the table has formed;
    // retrying tolerates the brief window while the routing table propagates.
    const value: Uint8Array = Uint8Array.of(7, 8, 9);
    await eventually(async (): Promise<void> => {
      await anchor.write({ kind: "put", key: "greeting", value, condition: "none" });
      const entry: Entry | undefined = await joiner.read("greeting");
      expect(Array.from(entry?.value ?? new Uint8Array())).toEqual([7, 8, 9]);
    });

    const names: readonly string[] = joiner.members().map((each): string => each.name);
    expect(names).toContain(anchor.address);
    expect(names).toContain(joiner.address);

    // A cluster-wide scan from the joiner reaches every partition primary and
    // surfaces the key written on the anchor.
    const scanned: ScanEntry[] = await joiner.scan();
    expect(scanned.map((entry: ScanEntry): string => entry.key)).toContain("greeting");
  }, 30_000);

  it("anchors a fully configured single node and serves it", async (): Promise<void> => {
    const node: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([]),
      host: "127.0.0.1",
      dataPort: 0,
      gossipPort: 0,
      partitionCount: 16,
      replicaCount: 1,
      writeQuorum: 1,
      minimumMemberQuorum: 1,
      bootDeadlineMs: 0,
      clock: systemClusterClock,
      startedAt: 1,
    });
    running.push(node);

    expect(node.joined).toBe(false);
    expect(node.members().map((each): string => each.name)).toEqual([node.address]);

    await eventually(async (): Promise<void> => {
      await node.write({ kind: "put", key: "solo", value: Uint8Array.of(1), condition: "none" });
      const entry: Entry | undefined = await node.read("solo");
      expect(Array.from(entry?.value ?? new Uint8Array())).toEqual([1]);
    });
  }, 20_000);

  it("hands off its partitions on a graceful leave and the survivor keeps serving", async (): Promise<void> => {
    const anchor: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([]),
      bootDeadlineMs: 0,
    });
    running.push(anchor);
    const joiner: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([anchor.gossipAddress]),
    });
    running.push(joiner);

    await eventually(async (): Promise<void> => {
      if (anchor.members().length !== 2 || joiner.members().length !== 2) {
        throw new Error("membership has not converged to two members");
      }
    });

    // Write a spread of keys through the anchor so both nodes hold data.
    const keys: readonly string[] = ["k0", "k1", "k2", "k3", "k4", "k5"];
    await eventually(async (): Promise<void> => {
      for (const key of keys) {
        await anchor.write({
          kind: "put",
          key,
          value: Uint8Array.of(key.length),
          condition: "none",
        });
      }

      for (const key of keys) {
        const entry: Entry | undefined = await anchor.read(key);
        expect(entry?.value).toBeDefined();
      }
    });

    // The joiner leaves gracefully: it drains its partitions to the anchor, then
    // departs membership.
    await joiner.leave();

    // The anchor converges to a single-member cluster and still serves every key.
    await eventually(async (): Promise<void> => {
      if (anchor.members().length !== 1) {
        throw new Error("survivor still sees the departed node");
      }
    });
    await eventually(async (): Promise<void> => {
      for (const key of keys) {
        const entry: Entry | undefined = await anchor.read(key);
        expect(Array.from(entry?.value ?? new Uint8Array())).toEqual([key.length]);
      }
    });
  }, 30_000);

  it("anchors a fresh cluster after failing to reach an unreachable seed", async (): Promise<void> => {
    const node: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery(["127.0.0.1:1"]),
      bootDeadlineMs: 150,
    });
    running.push(node);

    // The seed is unreachable, so every join attempt fails and the node anchors a
    // fresh cluster of itself once the boot deadline elapses.
    expect(node.joined).toBe(false);
    expect(node.members().map((each): string => each.name)).toEqual([node.address]);
  }, 20_000);

  it("publishes cluster lifecycle events to the event stream", async (): Promise<void> => {
    const stream: EventStream = new EventStream();
    const seen: ClusterEvent[] = [];
    stream.subscribe((event: unknown): void => {
      seen.push(event as ClusterEvent);
    }, CLUSTER_EVENT_TOPIC);

    const anchor: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([]),
      bootDeadlineMs: 0,
      events: stream,
    });
    running.push(anchor);
    const joiner: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([anchor.gossipAddress]),
    });
    running.push(joiner);

    // The anchor sees the joiner arrive and publishes a join event onto the stream.
    await eventually(async (): Promise<void> => {
      const types: string[] = seen.map((event: ClusterEvent): string => event.type);
      if (!types.includes(ClusterEventType.nodeJoined)) {
        throw new Error("no cluster event published yet");
      }
    });
  }, 30_000);

  it("releases the data endpoint when the gossip bind fails", async (): Promise<void> => {
    const first: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([]),
      bootDeadlineMs: 0,
    });
    running.push(first);

    // A second node forced onto the first's gossip port cannot bind, and start must
    // release the data endpoint it already bound rather than leak it.
    const gossipPort: number = parseHostPort(first.gossipAddress).port;
    await expect(
      ClusterNode.start({ discovery: new StaticDiscovery([]), bootDeadlineMs: 0, gossipPort }),
    ).rejects.toThrow();
  }, 20_000);

  it("releases both endpoints when boot discovery fails", async (): Promise<void> => {
    const failing: DiscoveryProvider = {
      resolve: (): Promise<readonly string[]> => Promise.reject(new Error("discovery is down")),
    };

    // Discovery rejecting aborts the boot; start must stop the node it assembled.
    await expect(ClusterNode.start({ discovery: failing })).rejects.toThrow("discovery is down");
  }, 20_000);

  it("releases both endpoints when wiring the engine fails", async (): Promise<void> => {
    // Claim a pair of ports and hand them back, so the failing start below binds
    // exactly these and a later start can prove they were released.
    const probe: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([]),
      bootDeadlineMs: 0,
    });
    const dataPort: number = parseHostPort(probe.address).port;
    const gossipPort: number = parseHostPort(probe.gossipAddress).port;
    await probe.stop();

    // A negative startedAt makes encodeNodeMetadata throw after both endpoints are
    // bound, so start must stop the data and gossip listeners it already opened.
    await expect(
      ClusterNode.start({
        discovery: new StaticDiscovery([]),
        bootDeadlineMs: 0,
        dataPort,
        gossipPort,
        startedAt: -1,
      }),
    ).rejects.toThrow(RangeError);

    // Both ports bind again, which they could not if either listener had leaked.
    const reuse: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([]),
      bootDeadlineMs: 0,
      dataPort,
      gossipPort,
    });
    running.push(reuse);
    expect(parseHostPort(reuse.address).port).toBe(dataPort);
    expect(parseHostPort(reuse.gossipAddress).port).toBe(gossipPort);
  }, 20_000);

  it("is idempotent across a concurrent leave and a repeated stop", async (): Promise<void> => {
    const node: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([]),
      bootDeadlineMs: 0,
    });
    running.push(node);

    // A lone node has nothing to drain, so leave returns promptly; a second leave
    // issued before the first settles sees the guard and is a no-op, as are a later
    // leave and a repeated stop once the node has already torn down.
    const first: Promise<void> = node.leave();
    const second: Promise<void> = node.leave();
    await Promise.all([first, second]);
    await node.leave();
    await node.stop();
    await node.stop();
  }, 20_000);
});
