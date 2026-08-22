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
import type { Actor } from "../src/actor/actor";
import { ActorSystem } from "../src/actor/actor.system";
import { PoisonPill, Terminated } from "../src/actor/messages";
import { newPath } from "../src/actor/path";
import { PID } from "../src/actor/pid";
import type { ReceiveContext } from "../src/actor/receive.context";
import { StopDirective, Supervisor } from "../src/actor/supervisor";
import {
  ActorInitializationError,
  ErrDead,
  ErrInvalidActorName,
  ErrReservedName,
} from "../src/errors/errors";

// A real but never started system: standalone PIDs receive no PostStart
// announcement because the NoSender actor does not exist.
const system = new ActorSystem("sys");

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

let nextName = 0;

async function startPid(actor: Actor): Promise<PID> {
  const pid = new PID(actor, newPath(`actor-${nextName++}`, "sys", "127.0.0.1", 0), system);
  await pid.start();
  return pid;
}

// The sending side for messages told from outside an actor. It never
// starts: tell only checks the target's state.
const external = new PID(new Collector(), newPath("external", "sys", "127.0.0.1", 0), system);

describe("PID spawnChild", () => {
  it("spawns a running child under the parent's path", async () => {
    const parent = await startPid(new Collector());
    const child_actor = new Collector();
    const child = await parent.spawnChild("worker", child_actor);

    expect(child.isRunning()).toBe(true);
    expect(child.name()).toBe("worker");
    expect(child.path().parent()?.equals(parent.path())).toBe(true);
    expect(parent.children()).toEqual([child]);

    external.tell(child, "job");
    await expect.poll(() => child_actor.received).toEqual(["job"]);
  });

  it("returns the existing child when the name is already taken", async () => {
    const parent = await startPid(new Collector());
    const child = await parent.spawnChild("worker", new Collector());
    const again = await parent.spawnChild("worker", new Collector());

    expect(again).toBe(child);
    expect(parent.children()).toHaveLength(1);
  });

  it("returns a suspended child instead of spawning a replacement", async () => {
    class Known extends Error {}

    class Faulty extends Collector {
      override receive(ctx: ReceiveContext): void {
        if (ctx.message === "boom") {
          throw new Error("boom");
        }

        super.receive(ctx);
      }
    }

    const parent = await startPid(new Collector());
    const childActor = new Faulty();
    // No directive matches the thrown error, so the fault suspends the
    // child instead of stopping it.
    const child = await parent.spawnChild("worker", childActor, {
      supervisor: new Supervisor({ directives: [[Known, StopDirective]] }),
    });

    external.tell(child, "boom");
    external.tell(child, "queued");
    await expect.poll(() => child.isSuspended()).toBe(true);

    // The queued backlog keeps the suspended child from looking idle.
    expect(child.isIdle()).toBe(false);

    // The suspended child still owns its name and its place in the tree,
    // but children() only lists running actors.
    const again = await parent.spawnChild("worker", new Collector());
    expect(again).toBe(child);
    expect(parent.children()).toEqual([]);

    parent.reinstate(child);
    expect(child.isRunning()).toBe(true);
    expect(parent.children()).toEqual([child]);
    await expect.poll(() => childActor.received).toEqual(["queued"]);
  });

  it("rejects reserved and invalid names and dead parents", async () => {
    const parent = await startPid(new Collector());
    await expect(parent.spawnChild("NodeAktWorker", new Collector())).rejects.toBe(ErrReservedName);
    await expect(parent.spawnChild("", new Collector())).rejects.toBe(ErrInvalidActorName);
    await expect(parent.spawnChild("bad name", new Collector())).rejects.toBe(ErrInvalidActorName);

    await parent.shutdown();
    await expect(parent.spawnChild("worker", new Collector())).rejects.toBe(ErrDead);
  });

  it("does not register a child whose preStart fails", async () => {
    class Failing extends Collector {
      override preStart(): void {
        throw new Error("boom");
      }
    }

    const parent = await startPid(new Collector());
    await expect(parent.spawnChild("worker", new Failing())).rejects.toBeInstanceOf(
      ActorInitializationError,
    );
    expect(parent.children()).toHaveLength(0);

    // The name stays available for a healthy spawn.
    const child = await parent.spawnChild("worker", new Collector());
    expect(child.isRunning()).toBe(true);
  });

  it("stops its children when the parent shuts down", async () => {
    const parent = await startPid(new Collector());
    const childActor = new Collector();
    const grandChildActor = new Collector();
    const child = await parent.spawnChild("child", childActor);
    const grandChild = await child.spawnChild("grandchild", grandChildActor);

    await parent.shutdown();

    expect(child.isRunning()).toBe(false);
    expect(grandChild.isRunning()).toBe(false);
    expect(childActor.stopped).toBe(1);
    expect(grandChildActor.stopped).toBe(1);
  });

  it("frees the child's name once the child stops on its own", async () => {
    const parent = await startPid(new Collector());
    const first = await parent.spawnChild("worker", new Collector());

    await first.shutdown();
    expect(parent.children()).toHaveLength(0);

    const second = await parent.spawnChild("worker", new Collector());
    expect(second).not.toBe(first);
    expect(second.isRunning()).toBe(true);
  });
});

describe("PID watch and unWatch", () => {
  it("delivers Terminated to watchers when the watched actor stops", async () => {
    const watcherActor = new Collector();
    const watcher = await startPid(watcherActor);
    const watched = await startPid(new Collector());

    watcher.watch(watched);
    await watched.shutdown();

    await expect.poll(() => watcherActor.received.length).toBe(1);
    const msg = watcherActor.received[0];
    expect(msg).toBeInstanceOf(Terminated);
    expect((msg as Terminated).actorPath).toBe(watched.path().toString());
  });

  it("does not notify after unWatch", async () => {
    const watcherActor = new Collector();
    const watcher = await startPid(watcherActor);
    const watched = await startPid(new Collector());

    watcher.watch(watched);
    watcher.unWatch(watched);
    await watched.shutdown();

    external.tell(watcher, "probe");
    await expect.poll(() => watcherActor.received).toEqual(["probe"]);
  });

  it("watching an already stopped actor is a no-op", async () => {
    const watcherActor = new Collector();
    const watcher = await startPid(watcherActor);
    const watched = await startPid(new Collector());
    await watched.shutdown();

    watcher.watch(watched);

    // No notification is delivered; the probe is the only message.
    external.tell(watcher, "probe");
    await expect.poll(() => watcherActor.received).toEqual(["probe"]);
  });
});

describe("PID tree release", () => {
  it("reports no children for an actor without a tree", () => {
    const fresh = new PID(new Collector(), newPath("fresh", "sys", "127.0.0.1", 0), system);
    expect(fresh.children()).toEqual([]);
  });

  it("releases the tree reference once stopped", async () => {
    const parent = await startPid(new Collector());
    const child = await parent.spawnChild("worker", new Collector());

    await child.shutdown();

    // A retained handle to a dead actor must not pin the hierarchy.
    expect((child as unknown as { _tree: unknown })._tree).toBeNull();
    expect(child.children()).toEqual([]);
    expect(parent.children()).toEqual([]);
  });

  it("unWatch on a stopped actor is a no-op", async () => {
    const watcher = await startPid(new Collector());
    const watched = await startPid(new Collector());
    await watched.shutdown();

    watcher.unWatch(watched);
    expect(watcher.isRunning()).toBe(true);
  });
});

describe("PoisonPill", () => {
  it("stops the actor after draining its backlog, without reaching receive", async () => {
    const actor = new Collector();
    const pid = await startPid(actor);

    external.tell(pid, 1);
    external.tell(pid, new PoisonPill());
    external.tell(pid, 2);

    await expect.poll(() => pid.isRunning()).toBe(false);
    expect(actor.received).toEqual([1, 2]);
    expect(actor.stopped).toBe(1);
    expect(external.tell(pid, 3)).toBe(ErrDead);
  });
});
