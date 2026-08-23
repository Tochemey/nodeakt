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
import { ErrDead } from "../src/errors";
import { Terminated } from "../src/messages";
import { newPath } from "../src/path";
import { PID } from "../src/pid";
import { createReceiveContext, type ReceiveContext } from "../src/receive.context";

const system = new ActorSystem("sys");

class Collector implements Actor {
  readonly received: unknown[] = [];
  readonly senders: Array<unknown> = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    this.received.push(ctx.message);
    this.senders.push(ctx.sender);
  }

  postStop(): void {}
}

let nextName = 0;

async function startPid(actor: Actor, name?: string): Promise<PID> {
  const pid = new PID(actor, newPath(name ?? `actor-${nextName++}`, "sys", "127.0.0.1", 0), system);
  await pid.start();
  return pid;
}

const external = new PID(new Collector(), newPath("external", "sys", "127.0.0.1", 0), system);

describe("ReceiveContext actor API", () => {
  it("tells another actor from receive, recording the sender", async () => {
    const targetActor = new Collector();
    const target = await startPid(targetActor, "target");

    class Sender implements Actor {
      preStart(): void {}
      receive(ctx: ReceiveContext): void {
        ctx.tell(target, ctx.message);
      }
      postStop(): void {}
    }

    const sender = await startPid(new Sender(), "sender");
    external.tell(sender, "hello");

    await expect.poll(() => targetActor.received).toEqual(["hello"]);
    expect(targetActor.senders[0]).toBe(sender);
  });

  it("spawns, looks up, and stops a child from receive", async () => {
    class Parent implements Actor {
      child: PID | undefined;
      lookedUp: PID | undefined;
      kids: PID[] = [];

      preStart(): void {}

      async receive(ctx: ReceiveContext): Promise<void> {
        if (ctx.message === "spawn") {
          this.child = await ctx.spawn("worker", new Collector());
          this.lookedUp = ctx.child("worker");
          this.kids = ctx.children();
          return;
        }

        if (ctx.message === "stop" && this.child !== undefined) {
          await ctx.stop(this.child);
        }
      }

      postStop(): void {}
    }

    const parentActor = new Parent();
    const parent = await startPid(parentActor, "parent");
    external.tell(parent, "spawn");
    await expect.poll(() => parentActor.child !== undefined).toBe(true);

    expect(parentActor.lookedUp).toBe(parentActor.child);
    expect(parentActor.kids).toEqual([parentActor.child]);
    expect(parentActor.child?.parent()).toBe(parent);
    expect(parentActor.child?.isRunning()).toBe(true);

    external.tell(parent, "stop");
    await expect.poll(() => parentActor.child?.isRunning()).toBe(false);
    expect(parent.children()).toEqual([]);
  });

  it("watches and unWatches from receive", async () => {
    class Watcher implements Actor {
      readonly received: unknown[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message instanceof PID) {
          ctx.watch(ctx.message);
          return;
        }

        if (ctx.message === "unwatch" && ctx.sender !== undefined) {
          ctx.unWatch(ctx.sender);
          return;
        }

        this.received.push(ctx.message);
      }

      postStop(): void {}
    }

    const watcherActor = new Watcher();
    const watcher = await startPid(watcherActor, "watcher");
    const watched = await startPid(new Collector(), "watched");

    external.tell(watcher, watched);
    await expect.poll(() => watcher.processedCount()).toBe(1);

    await watched.shutdown();
    await expect.poll(() => watcherActor.received.length).toBe(1);
    expect(watcherActor.received[0]).toBeInstanceOf(Terminated);
  });

  it("unWatches from receive so a later stop is not observed", async () => {
    class Watcher implements Actor {
      readonly received: unknown[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message instanceof PID) {
          ctx.watch(ctx.message);
          ctx.unWatch(ctx.message);
          return;
        }

        this.received.push(ctx.message);
      }

      postStop(): void {}
    }

    const watcherActor = new Watcher();
    const watcher = await startPid(watcherActor, "unwatcher");
    const watched = await startPid(new Collector(), "unwatched");

    external.tell(watcher, watched);
    await expect.poll(() => watcher.processedCount()).toBe(1);

    await watched.shutdown();
    await expect.poll(() => watcher.isIdle()).toBe(true);
    expect(watcherActor.received).toEqual([]);
  });

  it("forwards the current message, preserving the original sender", async () => {
    const sinkActor = new Collector();
    const sink = await startPid(sinkActor, "sink");

    class Forwarder implements Actor {
      preStart(): void {}
      receive(ctx: ReceiveContext): void {
        ctx.forward(sink);
      }
      postStop(): void {}
    }

    const forwarder = await startPid(new Forwarder(), "forwarder");
    const origin = await startPid(new Collector(), "origin");

    origin.tell(forwarder, "relay");
    await expect.poll(() => sinkActor.received).toEqual(["relay"]);
    expect(sinkActor.senders[0]).toBe(origin);
  });

  it("throws when forwarding from a senderless context", async () => {
    const target = await startPid(new Collector(), "fwd-target");
    expect(() => createReceiveContext("x").forward(target)).toThrow("not attached");
  });

  it("shuts the receiving actor down without awaiting from receive", async () => {
    class Suicidal implements Actor {
      preStart(): void {}
      receive(ctx: ReceiveContext): void {
        ctx.shutdown();
      }
      postStop(): void {}
    }

    const pid = await startPid(new Suicidal(), "suicidal");
    external.tell(pid, "die");
    await expect.poll(() => pid.isRunning()).toBe(false);
  });

  it("exposes the actor system from an attached context", async () => {
    class SysAware implements Actor {
      seen: ActorSystem | undefined;
      preStart(): void {}
      receive(ctx: ReceiveContext): void {
        this.seen = ctx.actorSystem();
      }
      postStop(): void {}
    }

    const actor = new SysAware();
    const pid = await startPid(actor, "sys-aware");
    external.tell(pid, "ping");
    await expect.poll(() => actor.seen).toBe(system);
  });

  it("throws when tell targets a dead actor", async () => {
    const dead = await startPid(new Collector(), "dead");
    await dead.shutdown();

    class Sender implements Actor {
      err: unknown;
      preStart(): void {}
      receive(ctx: ReceiveContext): void {
        try {
          ctx.tell(dead, "nope");
        } catch (err) {
          this.err = err;
        }
      }
      postStop(): void {}
    }

    const actor = new Sender();
    const pid = await startPid(actor, "teller");
    external.tell(pid, "go");
    await expect.poll(() => actor.err).toBe(ErrDead);
  });
});
