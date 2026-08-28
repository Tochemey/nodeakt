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
import { ActorSystem, clusterNodeOptions } from "../src/actor.system";
import { ClusterNode, type ClusterNodeOptions } from "../src/clustering.node";
import { StaticDiscovery } from "../src/discovery/static";
import { ErrClusterRequiresRemote, ErrClusterRequiresRoutableHost } from "../src/errors";
import { EventStream } from "../src/eventstream";
import type { ClusterMember } from "../src/kv/ports";

describe("clusterNodeOptions", () => {
  it("binds the remoting host and defaults the gossip port from a minimal option", () => {
    const events: EventStream = new EventStream((): void => {});
    const options: ClusterNodeOptions = clusterNodeOptions(
      { discovery: new StaticDiscovery([]) },
      "10.0.0.7",
      2552,
      events,
    );

    expect(options.host).toBe("10.0.0.7");
    expect(options.gossipPort).toBe(7946);
    expect(options.remotingAddress).toBe("10.0.0.7:2552");
    expect(options.events).toBe(events);
    expect(options.dataPort).toBeUndefined();
    expect(options.bootDeadlineMs).toBeUndefined();
    expect(options.partitionCount).toBeUndefined();
    expect(options.replicaCount).toBeUndefined();
    expect(options.writeQuorum).toBeUndefined();
    expect(options.minimumMemberQuorum).toBeUndefined();
  });

  it("passes every explicitly set option through and brackets an IPv6 remoting host", () => {
    const events: EventStream = new EventStream((): void => {});
    const options: ClusterNodeOptions = clusterNodeOptions(
      {
        discovery: new StaticDiscovery([]),
        gossipPort: 7000,
        dataPort: 8000,
        bootstrapTimeout: 0,
        partitionCount: 16,
        replicaCount: 3,
        writeQuorum: 2,
        minimumMemberQuorum: 2,
      },
      "::1",
      2552,
      events,
    );

    expect(options.host).toBe("::1");
    expect(options.gossipPort).toBe(7000);
    expect(options.dataPort).toBe(8000);
    expect(options.bootDeadlineMs).toBe(0);
    expect(options.partitionCount).toBe(16);
    expect(options.replicaCount).toBe(3);
    expect(options.writeQuorum).toBe(2);
    expect(options.minimumMemberQuorum).toBe(2);
    expect(options.remotingAddress).toBe("[::1]:2552");
  });
});

describe("ActorSystem clustering", () => {
  /** Systems started by a test, stopped afterward regardless of outcome. */
  const running: ActorSystem[] = [];

  afterEach(async (): Promise<void> => {
    await Promise.all(running.map((system: ActorSystem): Promise<void> => system.stop()));
    running.length = 0;
  });

  it("refuses a cluster configuration without remoting", () => {
    expect(
      (): ActorSystem =>
        new ActorSystem("orders", { cluster: { discovery: new StaticDiscovery([]) } }),
    ).toThrow(ErrClusterRequiresRemote);
  });

  it("starts a clustered node, advertises its remoting endpoint, and unwinds on stop", async (): Promise<void> => {
    const system: ActorSystem = new ActorSystem("orders", {
      remote: { host: "127.0.0.1", port: 0 },
      cluster: {
        discovery: new StaticDiscovery([]),
        gossipPort: 0,
        dataPort: 0,
        bootstrapTimeout: 0,
        replicaCount: 1,
        writeQuorum: 1,
        minimumMemberQuorum: 1,
      },
    });
    running.push(system);
    await system.start();

    const node: ClusterNode | null = system.clusterNode();
    if (node === null) {
      throw new Error("expected the cluster node to be started");
    }

    expect(system.clusterRegistry()).not.toBeNull();
    expect(node.members().map((member: ClusterMember): string => member.name)).toContain(
      node.address,
    );
    expect(node.remotingAddressOf(node.address)).toBe(`127.0.0.1:${system.port()}`);

    await system.stop();
    expect(system.clusterNode()).toBeNull();
    expect(system.clusterRegistry()).toBeNull();
  }, 20_000);

  it("releases the remoting listener when the cluster node fails to start", async (): Promise<void> => {
    const gossipPort: number = 7947;
    const remotingPort: number = 7948;
    // Occupy the gossip port so the system under test collides on it and its
    // ClusterNode.start rejects after remoting has already bound.
    const blocker: ClusterNode = await ClusterNode.start({
      discovery: new StaticDiscovery([]),
      gossipPort,
      bootDeadlineMs: 0,
    });

    try {
      const system: ActorSystem = new ActorSystem("orders", {
        remote: { host: "127.0.0.1", port: remotingPort },
        cluster: {
          discovery: new StaticDiscovery([]),
          gossipPort,
          dataPort: 0,
          bootstrapTimeout: 0,
          replicaCount: 1,
          writeQuorum: 1,
          minimumMemberQuorum: 1,
        },
      });
      running.push(system);

      await expect(system.start()).rejects.toThrow();
      expect(system.clusterNode()).toBeNull();
      expect(system.clusterRegistry()).toBeNull();

      // Prove the bound remoting listener was released: a second system binds the
      // same fixed remoting port, which would fail with EADDRINUSE if it leaked.
      const reuser: ActorSystem = new ActorSystem("ordersreuser", {
        remote: { host: "127.0.0.1", port: remotingPort },
      });
      running.push(reuser);
      await reuser.start();
      expect(reuser.port()).toBe(remotingPort);
    } finally {
      await blocker.stop();
    }
  }, 20_000);
});

describe("ActorSystem advertised host", () => {
  /** Systems started by a test, stopped afterward regardless of outcome. */
  const running: ActorSystem[] = [];

  afterEach(async (): Promise<void> => {
    await Promise.all(running.map((system: ActorSystem): Promise<void> => system.stop()));
    running.length = 0;
  });

  it("advertises a configured advertised host while binding the bind host", async (): Promise<void> => {
    const system: ActorSystem = new ActorSystem("orders", {
      remote: { host: "127.0.0.1", advertisedHost: "10.0.0.7", port: 0 },
    });
    running.push(system);
    await system.start();

    expect(system.host()).toBe("10.0.0.7");
    expect(system.port()).toBeGreaterThan(0);
  }, 20_000);

  it("refuses a clustered system whose advertised host is a wildcard", () => {
    expect(
      (): ActorSystem =>
        new ActorSystem("orders", {
          remote: { host: "0.0.0.0", port: 0 },
          cluster: { discovery: new StaticDiscovery([]) },
        }),
    ).toThrow(ErrClusterRequiresRoutableHost);
  });

  it("accepts a wildcard bind host when a concrete advertised host is given", () => {
    expect(
      (): ActorSystem =>
        new ActorSystem("orders", {
          remote: { host: "0.0.0.0", advertisedHost: "10.0.0.7", port: 0 },
          cluster: { discovery: new StaticDiscovery([]) },
        }),
    ).not.toThrow();
  });
});
