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
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import {
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrInvalidActorName,
  ErrInvalidActorSystemName,
  ErrNameRequired,
  ErrReservedName,
} from "../src/errors";
import { PostStart } from "../src/messages";
import { LongLivedStrategy, TimeBasedStrategy } from "../src/passivation";
import type { PID } from "../src/pid";
import type { ReceiveContext } from "../src/receive.context";
import { noSenderName, rootGuardianName, userGuardianName } from "../src/reserved";
import { StopDirective, Supervisor } from "../src/supervisor";

class Collector implements Actor {
  readonly received: unknown[] = [];
  stopped = 0;

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    this.received.push(ctx.message);
  }

  postStop(): void {
    this.stopped++;
  }
}

describe("ActorSystem constructor", () => {
  it("validates the system name", () => {
    expect(() => new ActorSystem("")).toThrow(ErrNameRequired);
    expect(() => new ActorSystem("bad name")).toThrow(ErrInvalidActorSystemName);
    expect(() => new ActorSystem("-bad")).toThrow(ErrInvalidActorSystemName);
    expect(() => new ActorSystem("no.dots")).toThrow(ErrInvalidActorSystemName);
    expect(new ActorSystem("sys-1_a").name()).toBe("sys-1_a");
  });

  it("validates and defaults the askTimeout", () => {
    expect(() => new ActorSystem("sys", { askTimeout: 0 })).toThrow(RangeError);
    expect(() => new ActorSystem("sys", { askTimeout: -1 })).toThrow(RangeError);
    expect(() => new ActorSystem("sys", { askTimeout: 1.5 })).toThrow(RangeError);
    expect(new ActorSystem("sys", { askTimeout: 250 }).askTimeout()).toBe(250);
    // Omitted, it defaults to a positive bound rather than no timeout.
    expect(new ActorSystem("sys").askTimeout()).toBeGreaterThan(0);
  });
});

describe("ActorSystem lifecycle", () => {
  it("starts the guardian hierarchy", async () => {
    const system = new ActorSystem("sys");
    expect(system.isRunning()).toBe(false);

    await system.start();
    expect(system.isRunning()).toBe(true);

    // The runtime's own actors are alive but never resolvable by name.
    expect(system.noSender().isRunning()).toBe(true);
    expect(system.noSender().path().toString()).toBe(`nodeakt://sys@127.0.0.1:0/${noSenderName}`);
    expect(system.actorOf(rootGuardianName)).toBeUndefined();
    expect(system.actorOf(userGuardianName)).toBeUndefined();

    await system.stop();
  });

  it("start on a started system and stop on a stopped one are no-ops", async () => {
    const system = new ActorSystem("sys");

    await system.stop();
    expect(system.isRunning()).toBe(false);

    await system.start();
    const noSender = system.noSender();
    await system.start();
    expect(system.noSender()).toBe(noSender);

    await system.stop();
  });

  it("stops every actor on stop and can start again", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const actor = new Collector();
    const pid = await system.spawn("worker", actor);

    await system.stop();
    expect(system.isRunning()).toBe(false);
    expect(pid.isRunning()).toBe(false);
    expect(actor.stopped).toBe(1);
    expect(system.actorOf("worker")).toBeUndefined();

    await system.start();
    const again = await system.spawn("worker", new Collector());
    expect(again.isRunning()).toBe(true);
    await system.stop();
  });
});

describe("ActorSystem spawn", () => {
  it("rejects spawning before start", async () => {
    const system = new ActorSystem("sys");
    await expect(system.spawn("worker", new Collector())).rejects.toBe(ErrActorSystemNotStarted);
  });

  it("spawns actors under the user guardian and delivers PostStart first", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const actor = new Collector();
    const pid = await system.spawn("worker", actor);
    expect(pid.isRunning()).toBe(true);

    // A top-level actor's path has no parent segment; the guardian layer
    // above it is supervision structure recorded in the tree.
    expect(pid.path().toString()).toBe("nodeakt://sys@127.0.0.1:0/worker");
    expect(pid.path().parent()).toBeUndefined();

    system.noSender().tell(pid, "job");
    await expect.poll(() => actor.received.length).toBe(2);
    expect(actor.received[0]).toBeInstanceOf(PostStart);
    expect(actor.received[1]).toBe("job");

    await system.stop();
  });

  it("rejects duplicate and reserved names", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    await system.spawn("worker", new Collector());
    await expect(system.spawn("worker", new Collector())).rejects.toBe(ErrActorAlreadyExists);
    await expect(system.spawn("NodeAktEvil", new Collector())).rejects.toBe(ErrReservedName);

    await system.stop();
  });

  it("rejects invalid actor names", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    for (const bad of ["", "-bad", "has space", "a".repeat(256)]) {
      await expect(system.spawn(bad, new Collector()), bad).rejects.toBe(ErrInvalidActorName);
    }

    await system.stop();
  });

  it("resolves running top-level actors by name with actorOf", async () => {
    const system = new ActorSystem("sys");
    expect(system.actorOf("worker")).toBeUndefined();

    await system.start();

    const pid = await system.spawn("worker", new Collector());
    expect(system.actorOf("worker")).toBe(pid);
    expect(system.actorOf("missing")).toBeUndefined();

    // Actors deeper in the hierarchy are not resolvable by bare name;
    // they are reached through their parent.
    const nested = await pid.spawnChild("nested", new Collector());
    expect(nested.isRunning()).toBe(true);
    expect(system.actorOf("nested")).toBeUndefined();

    await pid.shutdown();
    expect(system.actorOf("worker")).toBeUndefined();

    await system.stop();
  });

  it("starts the NoSender actor with the system and stops it on stop", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const noSender = system.noSender();
    expect(noSender.isRunning()).toBe(true);
    expect(noSender.name()).toBe(noSenderName);

    await system.stop();
    expect(noSender.isRunning()).toBe(false);
  });

  it("noSender throws when the system is not started", async () => {
    const system = new ActorSystem("sys");
    expect(() => system.noSender()).toThrow(ErrActorSystemNotStarted);

    await system.start();
    await system.stop();
    expect(() => system.noSender()).toThrow(ErrActorSystemNotStarted);
  });

  it("the NoSender actor ignores messages told to it", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const noSender = system.noSender();
    expect(noSender.tell(noSender, "ignored")).toBeNull();

    await expect.poll(() => noSender.processedCount()).toBe(1);
    expect(noSender.isRunning()).toBe(true);

    await system.stop();
  });

  it("noSender is the sender actors see for messages sent from outside", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const senders: Array<PID | undefined> = [];
    const actor: Actor = {
      preStart(): void {},
      receive(ctx: ReceiveContext): void {
        senders.push(ctx.sender);
      },
      postStop(): void {},
    };

    const pid = await system.spawn("worker", actor);
    system.noSender().tell(pid, "hello");

    // Both the PostStart announcement and the outside message carry the
    // NoSender actor's PID.
    await expect.poll(() => senders.length).toBe(2);
    expect(senders[0]).toBe(system.noSender());
    expect(senders[1]).toBe(system.noSender());

    await system.stop();
  });

  it("passivates idle actors through the system scheduler", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const actor = new Collector();
    const pid = await system.spawn("worker", actor, {
      passivationStrategy: new TimeBasedStrategy(40),
    });

    await expect.poll(() => pid.isRunning(), { timeout: 2000 }).toBe(false);
    await expect.poll(() => actor.stopped).toBe(1);
    expect(system.actorOf("worker")).toBeUndefined();

    // The name is free again for a fresh spawn.
    const again = await system.spawn("worker", new Collector());
    expect(again.isRunning()).toBe(true);

    await system.stop();
  });

  it("defaults spawned actors to a long-lived strategy", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const pid = await system.spawn("worker", new Collector());
    expect(pid.passivationStrategy()).toBeInstanceOf(LongLivedStrategy);

    await system.stop();
  });

  it("keeps a suspended actor's name occupied", async () => {
    class Known extends Error {}

    class Faulty extends Collector {
      override receive(ctx: ReceiveContext): void {
        if (ctx.message === "boom") {
          throw new Error("boom");
        }

        super.receive(ctx);
      }
    }

    const system = new ActorSystem("sys");
    await system.start();

    // No directive matches the thrown error, so the fault suspends the
    // actor instead of stopping it.
    const pid = await system.spawn("worker", new Faulty(), {
      supervisor: new Supervisor({ directives: [[Known, StopDirective]] }),
    });

    system.noSender().tell(pid, "boom");
    await expect.poll(() => pid.isSuspended()).toBe(true);

    // Suspended is not stopped: the name stays taken while actorOf, which
    // only resolves running actors, hides the actor.
    await expect(system.spawn("worker", new Collector())).rejects.toBe(ErrActorAlreadyExists);
    expect(system.actorOf("worker")).toBeUndefined();

    await system.stop();
  });
});

describe("ActorSystem hierarchy", () => {
  it("delivers PostStart to children spawned with spawnChild", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const parent = await system.spawn("parent", new Collector());
    const childActor = new Collector();
    const child = await parent.spawnChild("worker", childActor);

    system.noSender().tell(child, "job");
    await expect.poll(() => childActor.received.length).toBe(2);
    expect(childActor.received[0]).toBeInstanceOf(PostStart);
    expect(childActor.received[1]).toBe("job");

    await system.stop();
  });

  it("passivates idle children through the system scheduler", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const parent = await system.spawn("parent", new Collector());
    const childActor = new Collector();
    const child = await parent.spawnChild("worker", childActor, {
      passivationStrategy: new TimeBasedStrategy(40),
    });

    await expect.poll(() => child.isRunning(), { timeout: 2000 }).toBe(false);
    await expect.poll(() => childActor.stopped).toBe(1);
    expect(parent.isRunning()).toBe(true);
    expect(parent.children()).toEqual([]);

    await system.stop();
  });

  it("keeps same-named grandchildren under different parents distinct", async () => {
    const system = new ActorSystem("sys");
    await system.start();

    const alice = await system.spawn("alice", new Collector());
    const bob = await system.spawn("bob", new Collector());
    const aliceChild = await alice.spawnChild("child", new Collector());
    const bobChild = await bob.spawnChild("child", new Collector());
    const aliceGrand = await aliceChild.spawnChild("g", new Collector());
    const bobGrandActor = new Collector();
    const bobGrand = await bobChild.spawnChild("g", bobGrandActor);

    // Both grandchildren coexist in the one shared tree under distinct
    // canonical paths.
    expect(aliceGrand.path().toString()).toBe("nodeakt://sys@127.0.0.1:0/alice/child/g");
    expect(bobGrand.path().toString()).toBe("nodeakt://sys@127.0.0.1:0/bob/child/g");
    expect(aliceGrand.isRunning()).toBe(true);
    expect(bobGrand.isRunning()).toBe(true);
    expect(aliceChild.children()).toEqual([aliceGrand]);
    expect(bobChild.children()).toEqual([bobGrand]);

    system.noSender().tell(bobGrand, "job");
    await expect.poll(() => bobGrandActor.received.filter((m) => m === "job")).toEqual(["job"]);

    // Stopping one branch leaves the other's grandchild untouched.
    await alice.shutdown();
    expect(aliceGrand.isRunning()).toBe(false);
    expect(bobGrand.isRunning()).toBe(true);

    await system.stop();
  });
});
