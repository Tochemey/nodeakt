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
import {
  LongLivedStrategy,
  MessagesCountBasedStrategy,
  type PassivationStrategy,
  TimeBasedStrategy,
} from "../src/actor/passivation";
import { PassivationManager } from "../src/actor/passivation.manager";
import { newPath } from "../src/actor/path";
import { PID } from "../src/actor/pid";
import type { ReceiveContext } from "../src/actor/receive.context";
import { ErrDead } from "../src/errors/errors";

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

function makePid(actor: Actor, strategy?: PassivationStrategy): PID {
  return new PID(
    actor,
    newPath(`actor-${nextName++}`, "sys", "127.0.0.1", 0),
    system,
    strategy === undefined ? undefined : { passivationStrategy: strategy },
  );
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// The sending side for messages told from outside an actor. It never
// starts: tell only checks the target's state.
const external = makePid(new Collector());

describe("passivation strategies", () => {
  it("validate their inputs", () => {
    expect(() => new TimeBasedStrategy(0)).toThrow(RangeError);
    expect(() => new TimeBasedStrategy(-5)).toThrow(RangeError);
    expect(() => new MessagesCountBasedStrategy(0)).toThrow(RangeError);
    expect(() => new MessagesCountBasedStrategy(1.5)).toThrow(RangeError);
  });
});

describe("PassivationManager", () => {
  it("passivates an idle actor after its timeout", async () => {
    const manager = new PassivationManager();
    const actor = new Collector();
    const pid = makePid(actor);
    await pid.start();

    manager.register(pid, new TimeBasedStrategy(40));

    await expect.poll(() => pid.isRunning(), { timeout: 2000 }).toBe(false);
    expect(actor.stopped).toBe(1);
    expect(external.tell(pid, "late")).toBe(ErrDead);

    manager.stop();
  });

  it("does not passivate an actor that keeps receiving messages", async () => {
    const manager = new PassivationManager();
    const actor = new Collector();
    const pid = makePid(actor);
    await pid.start();

    manager.register(pid, new TimeBasedStrategy(60));

    const ping = setInterval(() => external.tell(pid, "ping"), 15);
    await pause(200);
    expect(pid.isRunning()).toBe(true);
    clearInterval(ping);

    // Once the actor goes quiet, the lazy deadline catches up with it.
    await expect.poll(() => pid.isRunning(), { timeout: 2000 }).toBe(false);
    expect(actor.stopped).toBe(1);

    manager.stop();
  });

  it("unregister cancels a pending passivation", async () => {
    const manager = new PassivationManager();
    const actor = new Collector();
    const pid = makePid(actor);
    await pid.start();

    manager.register(pid, new TimeBasedStrategy(40));
    manager.unregister(pid);

    await pause(120);
    expect(pid.isRunning()).toBe(true);
    expect(actor.stopped).toBe(0);

    manager.stop();
    await pid.shutdown();
  });

  it("does not schedule long-lived actors", async () => {
    const manager = new PassivationManager();
    const actor = new Collector();
    const pid = makePid(actor, new LongLivedStrategy());
    await pid.start();

    manager.register(pid, pid.passivationStrategy());

    await pause(100);
    expect(pid.isRunning()).toBe(true);

    manager.stop();
    await pid.shutdown();
  });

  it("stop cancels every pending passivation", async () => {
    const manager = new PassivationManager();
    const actor = new Collector();
    const pid = makePid(actor);
    await pid.start();

    manager.register(pid, new TimeBasedStrategy(40));
    manager.stop();

    await pause(120);
    expect(pid.isRunning()).toBe(true);

    await pid.shutdown();
  });
});

describe("message-count passivation", () => {
  it("passivates the actor after it processed maxMessages", async () => {
    const actor = new Collector();
    const pid = makePid(actor, new MessagesCountBasedStrategy(3));
    await pid.start();

    for (let i = 0; i < 3; i++) {
      expect(external.tell(pid, i)).toBeNull();
    }

    await expect.poll(() => pid.isRunning(), { timeout: 2000 }).toBe(false);
    expect(actor.received).toEqual([0, 1, 2]);
    expect(actor.stopped).toBe(1);
    expect(external.tell(pid, "late")).toBe(ErrDead);
  });

  it("drains messages enqueued before the threshold was crossed", async () => {
    const actor = new Collector();
    const pid = makePid(actor, new MessagesCountBasedStrategy(3));
    await pid.start();

    // All five are enqueued before processing begins; the threshold is
    // crossed mid-drain and the backlog still completes.
    for (let i = 0; i < 5; i++) {
      expect(external.tell(pid, i)).toBeNull();
    }

    await expect.poll(() => pid.isRunning(), { timeout: 2000 }).toBe(false);
    expect(actor.received).toEqual([0, 1, 2, 3, 4]);
    expect(actor.stopped).toBe(1);
  });
});

describe("PassivationManager scheduling", () => {
  async function startPid(actor: Actor): Promise<PID> {
    const pid = makePid(actor);
    await pid.start();
    return pid;
  }

  it("re-arms the shared timer for each earlier deadline", async () => {
    const manager = new PassivationManager();
    const slow = await startPid(new Collector());
    const mid = await startPid(new Collector());
    const fast = await startPid(new Collector());

    // Latest-first registration, so every registration re-arms the timer.
    manager.register(slow, new TimeBasedStrategy(240));
    manager.register(mid, new TimeBasedStrategy(140));
    manager.register(fast, new TimeBasedStrategy(40));

    await expect.poll(() => fast.isRunning(), { timeout: 2000 }).toBe(false);
    expect(mid.isRunning()).toBe(true);
    expect(slow.isRunning()).toBe(true);

    await expect.poll(() => mid.isRunning(), { timeout: 2000 }).toBe(false);
    await expect.poll(() => slow.isRunning(), { timeout: 2000 }).toBe(false);

    manager.stop();
  });

  it("drains due entries in deadline order through the shared heap", async () => {
    const manager = new PassivationManager();
    const pids = await Promise.all([
      startPid(new Collector()),
      startPid(new Collector()),
      startPid(new Collector()),
      startPid(new Collector()),
    ]);
    const timeouts = [40, 200, 120, 300];

    for (let i = 0; i < pids.length; i++) {
      manager.register(
        pids[i] as PID,
        new TimeBasedStrategy(timeouts[i] as number),
      );
    }

    for (const pid of pids) {
      await expect.poll(() => pid.isRunning(), { timeout: 2000 }).toBe(false);
    }

    manager.stop();
  });

  it("unregister removes an entry from the middle of the schedule", async () => {
    const manager = new PassivationManager();
    const fast = await startPid(new Collector());
    const kept = await startPid(new Collector());
    const third = await startPid(new Collector());
    const fourth = await startPid(new Collector());

    manager.register(fast, new TimeBasedStrategy(40));
    manager.register(kept, new TimeBasedStrategy(120));
    manager.register(third, new TimeBasedStrategy(200));
    manager.register(fourth, new TimeBasedStrategy(300));

    manager.unregister(kept);

    await expect.poll(() => fast.isRunning(), { timeout: 2000 }).toBe(false);
    await expect.poll(() => third.isRunning(), { timeout: 2000 }).toBe(false);
    expect(kept.isRunning()).toBe(true);

    manager.stop();
    await kept.shutdown();
    await fourth.shutdown();
  });

  it("prunes an entry whose actor already stopped", async () => {
    const manager = new PassivationManager();
    const dead = await startPid(new Collector());
    const alive = await startPid(new Collector());

    manager.register(dead, new TimeBasedStrategy(40));
    manager.register(alive, new TimeBasedStrategy(140));
    await dead.shutdown();

    // The timer fires for the stopped actor, prunes its entry, and keeps
    // scheduling the remaining one.
    await expect.poll(() => alive.isRunning(), { timeout: 2000 }).toBe(false);

    manager.stop();
  });

  it("grants a busy actor a fresh window instead of passivating it", async () => {
    class Busy implements Actor {
      done = 0;

      preStart(): void {}

      async receive(): Promise<void> {
        await pause(140);
        this.done++;
      }

      postStop(): void {}
    }

    const manager = new PassivationManager();
    const actor = new Busy();
    const pid = await startPid(actor);

    manager.register(pid, new TimeBasedStrategy(50));
    external.tell(pid, "work");

    // The deadline passes while the message is still being processed.
    await pause(80);
    expect(pid.isRunning()).toBe(true);

    await expect.poll(() => pid.isRunning(), { timeout: 2000 }).toBe(false);
    expect(actor.done).toBe(1);

    manager.stop();
  });

  it("keeps a suspended actor scheduled instead of pruning it", async () => {
    class Faulty extends Collector {
      override receive(ctx: ReceiveContext): void {
        if (ctx.message === "boom") {
          throw new Error("boom");
        }

        super.receive(ctx);
      }
    }

    const manager = new PassivationManager();
    const actor = new Faulty();
    const pid = makePid(actor);
    await pid.start();

    manager.register(pid, new TimeBasedStrategy(50));

    // A fault with no parent to decide the directive suspends the actor.
    external.tell(pid, "boom");
    await expect.poll(() => pid.isSuspended()).toBe(true);

    // The schedule is kept paused, not pruned, while suspended.
    await pause(120);
    expect(pid.isSuspended()).toBe(true);

    // Once reinstated, the kept schedule passivates the idle actor.
    pid.reinstate(pid);
    await expect.poll(() => pid.isRunning(), { timeout: 2000 }).toBe(false);
    expect(actor.stopped).toBe(1);

    manager.stop();
  });

  it("unregister tolerates an entry that is missing from the heap", async () => {
    const manager = new PassivationManager();
    const pid = await startPid(new Collector());

    manager.register(pid, new TimeBasedStrategy(60_000));

    // Force the inconsistent state the guard protects against: the entry
    // is tracked but claims not to be enqueued.
    const internals = manager as unknown as {
      entries: Map<string, { index: number }>;
    };
    for (const entry of internals.entries.values()) {
      entry.index = -1;
    }

    expect(() => manager.unregister(pid)).not.toThrow();

    manager.stop();
    await pid.shutdown();
  });
});
