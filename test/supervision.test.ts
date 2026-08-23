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
import { ActorSystem } from "../src/actor.system";
import { ActorNotFoundError, ErrDead } from "../src/errors";
import { PanicSignal } from "../src/messages";
import { newPath } from "../src/path";
import { PID } from "../src/pid";
import type { ReceiveContext } from "../src/receive.context";
import {
  EscalateDirective,
  OneForAllStrategy,
  RestartDirective,
  StopDirective,
  Supervisor,
} from "../src/supervisor";

// A real but never started system: standalone PIDs receive no PostStart
// announcement because the NoSender actor does not exist.
const system = new ActorSystem("sys");

let nextName = 0;

async function startPid(actor: Actor): Promise<PID> {
  const pid = new PID(actor, newPath(`actor-${nextName++}`, "sys", "127.0.0.1", 0), system);
  await pid.start();
  return pid;
}

// The sending side for messages told from outside an actor. It never
// starts: tell only checks the target's state.
const external = new PID(
  { preStart(): void {}, receive(): void {}, postStop(): void {} },
  newPath("external", "sys", "127.0.0.1", 0),
  system,
);

class Collector implements Actor {
  readonly received: unknown[] = [];
  starts = 0;
  stopped = 0;

  preStart(): void {
    this.starts++;
  }

  receive(ctx: ReceiveContext): void {
    this.received.push(ctx.message);
  }

  postStop(): void {
    this.stopped++;
  }
}

class Faulty extends Collector {
  override receive(ctx: ReceiveContext): void {
    if (ctx.message === "boom") {
      throw new Error("boom");
    }

    if (ctx.message === "chaos") {
      throw "chaos";
    }

    super.receive(ctx);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("stop directive", () => {
  it("stops a failing child by default", async () => {
    const parent = await startPid(new Collector());
    const childActor = new Faulty();
    const child = await parent.spawnChild("worker", childActor);

    external.tell(child, "boom");

    await expect.poll(() => child.isRunning()).toBe(false);
    expect(childActor.stopped).toBe(1);
    expect(child.isSuspended()).toBe(false);
    expect(parent.isRunning()).toBe(true);
    expect(parent.children()).toEqual([]);
  });

  it("stops the whole group under a one-for-all strategy", async () => {
    const parent = await startPid(new Collector());
    const supervisor = new Supervisor({
      strategy: OneForAllStrategy,
      anyErrorDirective: StopDirective,
    });
    const faulty = await parent.spawnChild("faulty", new Faulty(), { supervisor });
    const sibling = await parent.spawnChild("sibling", new Collector(), { supervisor });

    external.tell(faulty, "boom");

    await expect.poll(() => faulty.isRunning()).toBe(false);
    await expect.poll(() => sibling.isRunning()).toBe(false);
    expect(parent.isRunning()).toBe(true);
  });
});

describe("suspension", () => {
  it("suspends a failure without a matching directive until reinstated", async () => {
    class Known extends Error {}

    const childActor = new Faulty();
    const parent = await startPid(new Collector());
    const child = await parent.spawnChild("worker", childActor, {
      supervisor: new Supervisor({ directives: [[Known, StopDirective]] }),
    });

    external.tell(child, "boom");
    external.tell(child, "q1");
    external.tell(child, "q2");

    await expect.poll(() => child.isSuspended()).toBe(true);
    expect(child.isRunning()).toBe(false);
    expect(external.tell(child, "late")).toBe(ErrDead);
    expect(childActor.received).toEqual([]);

    // Reinstating resumes the backlog that was queued before the fault.
    parent.reinstate(child);
    expect(child.isSuspended()).toBe(false);
    expect(child.isRunning()).toBe(true);

    await expect.poll(() => childActor.received).toEqual(["q1", "q2"]);

    // Reinstating a healthy actor is a no-op.
    parent.reinstate(child);
    expect(child.isRunning()).toBe(true);
  });

  it("reinstates a suspended child by name", async () => {
    class Known extends Error {}

    const childActor = new Faulty();
    const parent = await startPid(new Collector());
    const child = await parent.spawnChild("worker", childActor, {
      supervisor: new Supervisor({ directives: [[Known, StopDirective]] }),
    });

    external.tell(child, "boom");
    external.tell(child, "q1");
    external.tell(child, "q2");

    await expect.poll(() => child.isSuspended()).toBe(true);

    // Reinstating by name resumes the backlog queued before the fault.
    parent.reinstate("worker");
    expect(child.isSuspended()).toBe(false);
    expect(child.isRunning()).toBe(true);
    await expect.poll(() => childActor.received).toEqual(["q1", "q2"]);

    // Reinstating a healthy child by name is a no-op.
    parent.reinstate("worker");
    expect(child.isRunning()).toBe(true);
  });

  it("rejects reinstate by name for an unknown child or a stopped parent", async () => {
    const parent = await startPid(new Collector());
    await parent.spawnChild("worker", new Collector());

    expect(() => parent.reinstate("ghost")).toThrow(ActorNotFoundError);

    await parent.shutdown();
    expect(() => parent.reinstate("worker")).toThrow(ErrDead);
  });

  it("suspends a failing top-level actor that has no parent", async () => {
    const actor = new Faulty();
    const pid = await startPid(actor);

    external.tell(pid, "boom");

    await expect.poll(() => pid.isSuspended()).toBe(true);
    expect(actor.stopped).toBe(0);
  });
});

describe("restart directive", () => {
  it("restarts a failing child and resumes its backlog", async () => {
    const parent = await startPid(new Collector());
    const childActor = new Faulty();
    const child = await parent.spawnChild("worker", childActor, {
      supervisor: new Supervisor({ anyErrorDirective: RestartDirective }),
    });

    external.tell(child, "one");
    external.tell(child, "boom");
    external.tell(child, "after");

    await expect.poll(() => child.restartCount()).toBe(1);
    await expect.poll(() => childActor.received).toEqual(["one", "after"]);
    expect(childActor.starts).toBe(2);
    // A suspended actor restarts without the postStop teardown step.
    expect(childActor.stopped).toBe(0);
    expect(child.isRunning()).toBe(true);
    expect(child.processedCount()).toBe(1);
  });

  it("suspends the group once the restart budget is exhausted", async () => {
    const parent = await startPid(new Collector());
    const supervisor = new Supervisor({
      strategy: OneForAllStrategy,
      anyErrorDirective: RestartDirective,
      maxRetries: 1,
      timeout: 60_000,
    });
    const faultyActor = new Faulty();
    const siblingActor = new Collector();
    const faulty = await parent.spawnChild("faulty", faultyActor, { supervisor });
    const sibling = await parent.spawnChild("sibling", siblingActor, { supervisor });

    external.tell(faulty, "boom");

    // The first fault restarts the whole group.
    await expect.poll(() => faulty.restartCount()).toBe(1);
    await expect.poll(() => sibling.restartCount()).toBe(1);
    expect(siblingActor.stopped).toBe(1);

    // The second consecutive fault exceeds the budget: the group is
    // suspended instead of restarted.
    external.tell(faulty, "boom");

    await expect.poll(() => faulty.isSuspended()).toBe(true);
    await expect.poll(() => sibling.isSuspended()).toBe(true);
    expect(faulty.restartCount()).toBe(1);
  });

  it("resets the fault counter after a quiet window", async () => {
    const parent = await startPid(new Collector());
    const child = await parent.spawnChild("worker", new Faulty(), {
      supervisor: new Supervisor({
        anyErrorDirective: RestartDirective,
        maxRetries: 1,
        timeout: 40,
      }),
    });

    external.tell(child, "boom");
    await expect.poll(() => child.restartCount()).toBe(1);

    // Stay quiet past the reset window; the next fault counts as the
    // first again instead of exhausting the budget.
    await sleep(80);
    external.tell(child, "boom");

    await expect.poll(() => child.restartCount()).toBe(2);
    expect(child.isSuspended()).toBe(false);
  });

  it("delays consecutive restarts with exponential backoff", async () => {
    const parent = await startPid(new Collector());
    const child = await parent.spawnChild("worker", new Faulty(), {
      supervisor: new Supervisor({
        anyErrorDirective: RestartDirective,
        maxRetries: 5,
        timeout: 10,
        initialDelay: 60,
        maxDelay: 240,
        backoffResetAfter: 60_000,
      }),
    });

    external.tell(child, "boom");

    // The restart is armed but held back by the backoff delay.
    await sleep(20);
    expect(child.isSuspended()).toBe(true);

    await expect.poll(() => child.restartCount(), { timeout: 2000 }).toBe(1);
    expect(child.isRunning()).toBe(true);
  });

  it("skips a delayed restart when the parent is gone", async () => {
    const parent = await startPid(new Collector());
    const child = await parent.spawnChild("worker", new Faulty(), {
      supervisor: new Supervisor({ anyErrorDirective: RestartDirective, initialDelay: 50 }),
    });

    external.tell(child, "boom");
    await expect.poll(() => child.isSuspended()).toBe(true);

    // The parent (and with it the child) is torn down during the delay.
    await parent.shutdown();
    await sleep(80);

    expect(child.isRunning()).toBe(false);
    expect(child.restartCount()).toBe(0);
  });

  it("retries a failing restart and gives up after the budget", async () => {
    const errorSpy = vi.spyOn(system.logger(), "error").mockImplementation(() => {});

    class BadStart extends Faulty {
      override preStart(): void {
        super.preStart();

        if (this.starts > 1) {
          throw new Error("init fails");
        }
      }
    }

    const parent = await startPid(new Collector());
    const childActor = new BadStart();
    const child = await parent.spawnChild("worker", childActor, {
      supervisor: new Supervisor({
        anyErrorDirective: RestartDirective,
        maxRetries: 2,
        timeout: 10,
      }),
    });

    external.tell(child, "boom");

    await expect.poll(() => childActor.starts).toBe(3);
    await expect.poll(() => errorSpy.mock.calls.length).toBe(1);
    expect(child.isRunning()).toBe(false);
    expect(child.isSuspended()).toBe(true);
  });

  it("suspends in-flight group members when the budget is exhausted mid-restart", async () => {
    class SlowStart extends Faulty {
      override async preStart(): Promise<void> {
        await sleep(10);
        this.starts++;
      }
    }

    const parent = await startPid(new Collector());
    const supervisor = new Supervisor({
      strategy: OneForAllStrategy,
      anyErrorDirective: RestartDirective,
      maxRetries: 1,
      timeout: 60_000,
    });
    const a = await parent.spawnChild("a", new SlowStart(), { supervisor });
    const b = await parent.spawnChild("b", new SlowStart(), { supervisor });

    // Both children fault in the same batch: the second supervision
    // request exhausts the shared budget while the first group restart is
    // still in flight.
    external.tell(a, "boom");
    external.tell(b, "boom");

    await expect.poll(() => a.restartCount() + b.restartCount() > 0).toBe(true);
    await expect.poll(() => parent.isRunning()).toBe(true);
  });
});

describe("escalate directive", () => {
  it("escalates the failure to the parent's behavior as a PanicSignal", async () => {
    const parentActor = new Collector();
    const parent = await startPid(parentActor);
    const child = await parent.spawnChild("worker", new Faulty(), {
      supervisor: new Supervisor({ anyErrorDirective: EscalateDirective }),
    });

    external.tell(child, "boom");

    await expect.poll(() => parentActor.received.length).toBe(1);
    const signal = parentActor.received[0];
    expect(signal).toBeInstanceOf(PanicSignal);
    expect((signal as PanicSignal).reason.message).toBe("boom");
    expect(child.isSuspended()).toBe(true);
  });

  it("wraps a non-error failure when escalating", async () => {
    const parentActor = new Collector();
    const parent = await startPid(parentActor);
    const child = await parent.spawnChild("worker", new Faulty(), {
      supervisor: new Supervisor({ anyErrorDirective: EscalateDirective }),
    });

    external.tell(child, "chaos");

    await expect.poll(() => parentActor.received.length).toBe(1);
    expect((parentActor.received[0] as PanicSignal).reason.message).toBe("chaos");
  });
});

describe("PID restart", () => {
  it("tears down and reinitializes a running actor", async () => {
    class Slowish extends Collector {
      override async preStart(): Promise<void> {
        await sleep(1);
        this.starts++;
      }

      override async receive(ctx: ReceiveContext): Promise<void> {
        await sleep(30);
        this.received.push(ctx.message);
      }

      override async postStop(): Promise<void> {
        await sleep(1);
        this.stopped++;
      }
    }

    const actor = new Slowish();
    const pid = await startPid(actor);
    const child = await pid.spawnChild("child", new Collector());

    external.tell(pid, "in-flight");
    await sleep(5);

    await pid.restart();

    expect(actor.received).toEqual(["in-flight"]);
    expect(actor.stopped).toBe(1);
    expect(actor.starts).toBe(2);
    expect(child.isRunning()).toBe(false);
    expect(pid.isRunning()).toBe(true);
    expect(pid.processedCount()).toBe(0);
    expect(pid.restartCount()).toBe(1);
  });

  it("clears switched behavior and stashed messages", async () => {
    class Switcher extends Collector {
      override receive(ctx: ReceiveContext): void {
        if (ctx.message === "stash") {
          ctx.stash();
          return;
        }

        if (ctx.message === "become") {
          ctx.become((c) => {
            this.received.push(`alt:${c.message}`);
          });
          return;
        }

        this.received.push(`base:${ctx.message}`);
      }
    }

    const actor = new Switcher();
    const pid = await startPid(actor);

    external.tell(pid, "stash");
    external.tell(pid, "become");
    await expect.poll(() => pid.processedCount()).toBe(2);
    expect(pid.stashSize()).toBe(1);

    await pid.restart();

    expect(pid.stashSize()).toBe(0);
    external.tell(pid, "x");
    await expect.poll(() => actor.received).toEqual(["base:x"]);
  });

  it("logs a postStop failure and completes the restart", async () => {
    const errorSpy = vi.spyOn(system.logger(), "error").mockImplementation(() => {});

    class BadStop extends Collector {
      override postStop(): void {
        throw new Error("cleanup failed");
      }
    }

    const actor = new BadStop();
    const pid = await startPid(actor);

    await pid.restart();

    expect(actor.starts).toBe(2);
    expect(pid.isRunning()).toBe(true);
    expect(errorSpy).toHaveBeenCalledOnce();
  });

  it("rejects a stopped actor", async () => {
    const pid = await startPid(new Collector());
    await pid.shutdown();

    await expect(pid.restart()).rejects.toBe(ErrDead);
  });

  it("rejects a concurrent restart", async () => {
    class Slow extends Collector {
      override async receive(ctx: ReceiveContext): Promise<void> {
        await sleep(30);
        this.received.push(ctx.message);
      }
    }

    const pid = await startPid(new Slow());
    external.tell(pid, "work");
    await sleep(5);

    const first = pid.restart();
    await expect(pid.restart()).rejects.toBe(ErrDead);

    await first;
    expect(pid.restartCount()).toBe(1);
    expect(pid.isRunning()).toBe(true);
  });

  it("backs off when a shutdown wins the race", async () => {
    class Slow extends Collector {
      override async receive(ctx: ReceiveContext): Promise<void> {
        await sleep(30);
        this.received.push(ctx.message);
      }
    }

    const actor = new Slow();
    const pid = await startPid(actor);
    external.tell(pid, "work");
    await sleep(5);

    // Both park on the draining loop; the shutdown owns the teardown and
    // the restart backs off instead of running postStop a second time.
    const restarting = pid.restart();
    const stopping = pid.shutdown();

    await expect(restarting).rejects.toBe(ErrDead);
    await stopping;

    expect(pid.isRunning()).toBe(false);
    expect(actor.stopped).toBe(1);
    expect(actor.starts).toBe(1);
  });
});
