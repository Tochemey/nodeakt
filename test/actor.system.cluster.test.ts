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

import { afterEach, describe, expect, it, vi } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem, clusterNodeOptions } from "../src/actor.system";
import { NodeJoined } from "../src/cluster.events";
import { type Companion, decodeCompanion, encodeCompanion } from "../src/clustering.companion";
import { ClusterEventType } from "../src/clustering.events";
import { ClusterNode, type ClusterNodeOptions } from "../src/clustering.host";
import type { ClusterRegistry } from "../src/clustering.registry";
import { discardLogger } from "../src/discard.logger";
import { StaticDiscovery } from "../src/discovery/static";
import {
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrClusteringDisabled,
  ErrClusterRequiresRemote,
  ErrClusterRequiresRoutableHost,
  ErrInvalidActorName,
  ErrReservedName,
} from "../src/errors";
import { EventStream } from "../src/eventstream";
import type { ClusterMember } from "../src/kv/ports";
import type { MetricsSnapshot } from "../src/observability/metric.snapshot";
import { LongLivedStrategy } from "../src/passivation";
import type { PID } from "../src/pid";
import { Props } from "../src/props";
import { registerActor } from "../src/registration";

/** The module a placed recipe imports to rebuild {@link Registered} on its owner. */
const registeredModule: string = new URL("./fixtures/registered.actor.mjs", import.meta.url).href;

/** A placeable actor registered under the fixture the cluster placement ships. */
class Registered implements Actor {
  constructor(readonly prefix: string) {}

  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

registerActor(Registered, registeredModule);

/** A registered actor whose preStart always throws, so a placement build fails
 * with an initialization error rather than a duplicate. */
class AlwaysFails implements Actor {
  preStart(): void {
    throw new Error("preStart always fails");
  }

  receive(): void {}

  postStop(): void {}
}

registerActor(AlwaysFails, "file:///always-fails.actor.ts");

/** Starts a single-node, single-isolate clustered system and tracks it for teardown.
 * `relocation` overrides the cluster's relocation default when given. */
async function startSolo(running: ActorSystem[], relocation?: boolean): Promise<ActorSystem> {
  const system: ActorSystem = new ActorSystem("orders", {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0 },
    cluster: {
      discovery: new StaticDiscovery([]),
      gossipPort: 0,
      dataPort: 0,
      bootstrapTimeout: 0,
      replicaCount: 1,
      writeQuorum: 1,
      minimumMemberQuorum: 1,
      ...(relocation !== undefined ? { relocation } : {}),
    },
  });
  running.push(system);
  await system.start();
  return system;
}

/** Polls until `check` holds or the deadline elapses, for a fire-and-forget effect. */
async function eventually(check: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline: number = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) {
      return;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 20);
    });
  }

  throw new Error("condition was not met within the timeout");
}

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

  // A generous hook budget: tearing down two clustered systems (each draining and
  // departing membership) is slower under coverage instrumentation.
  afterEach(async (): Promise<void> => {
    await Promise.all(running.map((system: ActorSystem): Promise<void> => system.stop()));
    running.length = 0;
    vi.unstubAllEnvs();
  }, 30_000);

  it("refuses a cluster configuration without remoting", () => {
    expect(
      (): ActorSystem =>
        new ActorSystem("orders", { cluster: { discovery: new StaticDiscovery([]) } }),
    ).toThrow(ErrClusterRequiresRemote);
  });

  it("reports this node's membership view in the metrics snapshot", async (): Promise<void> => {
    const system: ActorSystem = new ActorSystem("orders", {
      logger: discardLogger,
      remote: { host: "127.0.0.1", port: 0 },
      metrics: { enabled: true },
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

    await eventually(async (): Promise<boolean> => {
      const snapshot: MetricsSnapshot = await system.collectMetrics();
      return snapshot.cluster?.alive === 1;
    }, 5_000);

    const snapshot: MetricsSnapshot = await system.collectMetrics();
    expect(snapshot.cluster).toBeDefined();
    expect(snapshot.cluster?.members).toBe(1);
    expect(snapshot.cluster?.alive).toBe(1);
    expect(snapshot.cluster?.suspect).toBe(0);
    expect(snapshot.cluster?.dead).toBe(0);
    expect(snapshot.cluster?.left).toBe(0);
    expect(snapshot.cluster?.isCoordinator).toBe(true);
  }, 30_000);

  it("records a clustered placement at the node's data address and frees it on stop", async (): Promise<void> => {
    // One isolate, so the placement lands on the main isolate and its own stop
    // drives the pool release that deletes the registry record.
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = new ActorSystem("orders", {
      logger: discardLogger,
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
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (node === null || registry === null) {
      throw new Error("expected the cluster node and registry to be started");
    }

    const pid: PID = await system.spawn("worker", Props.create(Registered, "w"));
    expect(await registry.getActor("worker")).toBe(node.address);

    // actorOf resolves a local name directly, and a name no node holds reads as
    // absent through the resolver behind the placement.
    expect(system.actorOf("worker")).not.toBeUndefined();
    expect(system.actorOf("ghost")).toBeUndefined();

    await pid.shutdown();
    await eventually(
      async (): Promise<boolean> => (await registry.getActor("worker")) === undefined,
      10_000,
    );
    expect(await registry.getActor("worker")).toBeUndefined();
  }, 30_000);

  it("refuses spawnOn before start and on a system without clustering", async (): Promise<void> => {
    const unstarted: ActorSystem = new ActorSystem("orders", { logger: discardLogger });
    await expect(unstarted.spawnOn("worker", Props.create(Registered, "w"))).rejects.toBe(
      ErrActorSystemNotStarted,
    );

    const local: ActorSystem = new ActorSystem("orders", { logger: discardLogger });
    running.push(local);
    await local.start();
    await expect(local.spawnOn("worker", Props.create(Registered, "w"))).rejects.toBe(
      ErrClusteringDisabled,
    );
  });

  it("places a spawnOn actor on the only member and validates its name", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = new ActorSystem("orders", {
      logger: discardLogger,
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
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (node === null || registry === null) {
      throw new Error("expected the cluster node and registry to be started");
    }

    await expect(system.spawnOn("NodeAktReserved", Props.create(Registered, "x"))).rejects.toBe(
      ErrReservedName,
    );
    await expect(system.spawnOn("bad name", Props.create(Registered, "x"))).rejects.toBe(
      ErrInvalidActorName,
    );

    // The default round-robin strategy resolves to this node, the only ready
    // member, so the actor is placed locally and answers.
    const chosen: PID = await system.spawnOn("sequencer", Props.create(Registered, "rr"));
    expect(await system.noSender().ask(chosen, "hi", 5000)).toBe("rr:hi");
    expect(await registry.getActor("sequencer")).toBe(node.address);

    // The local strategy lands here explicitly too.
    const here: PID = await system.spawnOn("local-one", Props.create(Registered, "lo"), {
      strategy: "local",
    });
    expect(await system.noSender().ask(here, "yo", 5000)).toBe("lo:yo");
  }, 30_000);

  it("refuses spawnSingleton before start and on a system without clustering", async (): Promise<void> => {
    const unstarted: ActorSystem = new ActorSystem("orders", { logger: discardLogger });
    await expect(unstarted.spawnSingleton("seq", Props.create(Registered, "s"))).rejects.toBe(
      ErrActorSystemNotStarted,
    );

    const local: ActorSystem = new ActorSystem("orders", { logger: discardLogger });
    running.push(local);
    await local.start();
    await expect(local.spawnSingleton("seq", Props.create(Registered, "s"))).rejects.toBe(
      ErrClusteringDisabled,
    );
  });

  it("hosts one singleton on the coordinator, idempotently, sharing the spawn namespace", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);
    const node: ClusterNode | null = system.clusterNode();
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (node === null || registry === null) {
      throw new Error("expected the cluster node and registry to be started");
    }

    // The sole member is the coordinator every view agrees on.
    expect(node.coordinator()).toBe(node.address);

    await expect(
      system.spawnSingleton("NodeAktReserved", Props.create(Registered, "x")),
    ).rejects.toBe(ErrReservedName);
    await expect(system.spawnSingleton("bad name", Props.create(Registered, "x"))).rejects.toBe(
      ErrInvalidActorName,
    );

    // First create: built on the coordinator (this node), findable, and it answers.
    const first: PID = await system.spawnSingleton("sequencer", Props.create(Registered, "one"));
    expect(await system.noSender().ask(first, "hi", 5000)).toBe("one:hi");
    expect(await registry.getActor("sequencer")).toBe(node.address);

    // A singleton is cluster infrastructure: it is forced long-lived so it
    // never passivates out from under the cluster.
    expect(first.passivationStrategy()).toBeInstanceOf(LongLivedStrategy);

    // Idempotent: a later create hands back the SAME instance rather than a duplicate
    // error, and the loser's props are ignored (the winner's "one" answers, not "two").
    const again: PID = await system.spawnSingleton("sequencer", Props.create(Registered, "two"));
    expect(again.path().uid()).toBe(first.path().uid());
    expect(await system.noSender().ask(again, "yo", 5000)).toBe("one:yo");

    // Singletons share the name space: a plain spawn of the name is a duplicate.
    await expect(system.spawn("sequencer", Props.create(Registered, "three"))).rejects.toBe(
      ErrActorAlreadyExists,
    );
  }, 30_000);

  it("surfaces the duplicate when a singleton's name is held by an unreachable owner", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (registry === null) {
      throw new Error("expected the cluster registry to be started");
    }

    // A phantom holder no present member matches: the claim keeps losing and the
    // winner never resolves, so the idempotent path exhausts and reports the duplicate.
    await registry.claimActorName("ghost", "10.9.9.9:1");
    await expect(system.spawnSingleton("ghost", Props.create(Registered, "g"))).rejects.toBe(
      ErrActorAlreadyExists,
    );
  }, 30_000);

  it("propagates a build failure rather than treating it as a lost race", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);

    await expect(system.spawnSingleton("boom", Props.create(AlwaysFails))).rejects.toSatisfy(
      (err: unknown): boolean => err instanceof Error && err !== ErrActorAlreadyExists,
    );
  }, 30_000);

  it("stores a companion recipe for a relocatable actor and removes it on stop", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (registry === null) {
      throw new Error("expected the cluster registry to be started");
    }

    // Relocation is on by default, so a plain spawn stores its recipe beside the
    // placement, ready for a survivor to rebuild it, and marks it not a singleton.
    const pid: PID = await system.spawn("worker", Props.create(Registered, "w"));
    const stored: Uint8Array | undefined = await registry.getCompanion("worker");
    if (stored === undefined) {
      throw new Error("expected a companion recipe for the relocatable actor");
    }

    const companion: Companion = decodeCompanion(stored);
    expect(companion.singleton).toBe(false);
    expect(companion.recipe.actor).toBe("Registered");
    expect(companion.recipe.args).toEqual(["w"]);

    // Its stop frees the name and its companion together.
    await pid.shutdown();
    await eventually(
      async (): Promise<boolean> => (await registry.getCompanion("worker")) === undefined,
      10_000,
    );
  }, 30_000);

  it("stores no companion for an actor opted out of relocation", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (registry === null) {
      throw new Error("expected the cluster registry to be started");
    }

    await system.spawn("bound", Props.create(Registered, "b"), { relocatable: false });
    expect(await registry.getCompanion("bound")).toBeUndefined();
  }, 30_000);

  it("honors a cluster relocation:false default and a per-actor opt-in over it", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running, false);
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (registry === null) {
      throw new Error("expected the cluster registry to be started");
    }

    // Default off: an ordinary spawn is node-bound, storing no recipe.
    await system.spawn("bound", Props.create(Registered, "b"));
    expect(await registry.getCompanion("bound")).toBeUndefined();

    // A per-actor opt-in wins over the system default.
    await system.spawn("mobile", Props.create(Registered, "m"), { relocatable: true });
    expect(await registry.getCompanion("mobile")).toBeDefined();
  }, 30_000);

  it("marks a singleton's companion so recovery re-pins it to the coordinator", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (registry === null) {
      throw new Error("expected the cluster registry to be started");
    }

    await system.spawnSingleton("sequencer", Props.create(Registered, "s"));
    const stored: Uint8Array | undefined = await registry.getCompanion("sequencer");
    if (stored === undefined) {
      throw new Error("expected a companion recipe for the singleton");
    }

    expect(decodeCompanion(stored).singleton).toBe(true);
  }, 30_000);

  it("resolves a local actor with actorOfAsync and reads a ghost as absent", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);
    await system.spawn("worker", Props.create(Registered, "w"));

    // A local actor resolves without a network round trip; a name no node holds warms
    // the view, finds nothing, and reads as absent.
    expect(await system.actorOfAsync("worker")).toBeDefined();
    expect(await system.actorOfAsync("ghost")).toBeUndefined();
  }, 30_000);

  it("actorOfAsync reads absent on a system without clustering", async (): Promise<void> => {
    const local: ActorSystem = new ActorSystem("orders", { logger: discardLogger });
    running.push(local);
    await local.start();

    expect(await local.actorOfAsync("nobody")).toBeUndefined();
  });

  it("recreates a departed node's actor on the coordinator when a node leaves", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);
    const node: ClusterNode | null = system.clusterNode();
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (node === null || registry === null) {
      throw new Error("expected the cluster node and registry to be started");
    }

    // Seed a companion and placement naming a node that is not a member: an orphan
    // the coordinator must relocate. The companion is written first so the sweep
    // never sees the placement without its recipe and frees it as non-relocatable.
    const dead: string = "10.9.9.9:9999";
    await registry.putCompanion(
      "orphan",
      encodeCompanion({
        recipe: { module: registeredModule, actor: "Registered", args: ["re"] },
        singleton: false,
      }),
    );
    await registry.putActor("orphan", dead);

    // Drive the coordinator's recovery through a departure event.
    system.onClusterEvent({ type: ClusterEventType.nodeLeft, address: dead });

    // The only survivor is this node, so the orphan is recreated here and answers.
    await eventually(async (): Promise<boolean> => system.actorOf("orphan") !== undefined, 10_000);
    expect(await registry.getActor("orphan")).toBe(node.address);
    const pid: PID | undefined = system.actorOf("orphan");
    if (pid === undefined) {
      throw new Error("expected the recreated actor");
    }

    expect(await system.noSender().ask(pid, "hi", 5000)).toBe("re:hi");
  }, 30_000);

  it("re-publishes a non-departure cluster event without driving relocation", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);
    const seen: unknown[] = [];
    system.subscribe((event: unknown): void => {
      seen.push(event);
    });

    system.onClusterEvent({ type: ClusterEventType.nodeJoined, address: "peer:1" });

    expect(seen.some((event: unknown): boolean => event instanceof NodeJoined)).toBe(true);
  }, 30_000);

  it("frees a departed node's non-relocatable actor rather than recreating it", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system: ActorSystem = await startSolo(running);
    const registry: ClusterRegistry | null = system.clusterRegistry();
    if (registry === null) {
      throw new Error("expected the cluster registry to be started");
    }

    // A placement with no companion is non-relocatable: recovery frees its name.
    const dead: string = "10.9.9.9:9998";
    await registry.putActor("bound", dead);
    system.onClusterEvent({ type: ClusterEventType.nodeLeft, address: dead });

    await eventually(
      async (): Promise<boolean> => (await registry.getActor("bound")) === undefined,
      10_000,
    );
  }, 30_000);

  it("ships a recreate to a survivor over remoting, moving the record there", async (): Promise<void> => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const coordinator: ActorSystem = await startSolo(running);
    const survivor: ActorSystem = await startSolo(running);
    const survivorNode: ClusterNode | null = survivor.clusterNode();
    const survivorRegistry: ClusterRegistry | null = survivor.clusterRegistry();
    if (survivorNode === null || survivorRegistry === null) {
      throw new Error("expected the survivor's cluster node and registry to be started");
    }

    // Seed the survivor's registry with a record naming a dead node, so its
    // compare-and-set applies when the recreate lands.
    await survivorRegistry.putActor("moved", "dead:1");

    const placed: boolean = await coordinator.remoteRecreate(
      survivor.host(),
      survivor.port(),
      "moved",
      { actor: "Registered", args: ["mv"] },
      false,
      "dead:1",
    );

    expect(placed).toBe(true);
    expect(survivor.actorOf("moved")).toBeDefined();
    expect(await survivorRegistry.getActor("moved")).toBe(survivorNode.address);
  }, 30_000);

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
