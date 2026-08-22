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
import type { IsolateRoute } from "../src/actor/actor.ref";
import { ActorSystem } from "../src/actor/actor.system";
import { Deadletter, PostStart } from "../src/actor/messages";
import { newPath, newPathAt } from "../src/actor/path";
import { PID } from "../src/actor/pid";
import type { ReceiveContext } from "../src/actor/receive.context";
import { completedRequest } from "../src/actor/reentrancy";
import { Scheduler } from "../src/actor/scheduler";
import {
  ErrActorSystemNotStarted,
  ErrDead,
  ErrInvalidInterval,
  ErrScheduleAlreadyExists,
  ErrScheduleNotFound,
} from "../src/errors/errors";
import { routedPid } from "../src/runtime/routed.pid";

class Tick {}

class Remind {}

class Start {
  constructor(readonly to: PID) {}
}

class Collector implements Actor {
  readonly received: unknown[] = [];
  readonly senders: (PID | undefined)[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.received.push(ctx.message);
    this.senders.push(ctx.sender);
  }

  postStop(): void {}
}

async function startedSystem(): Promise<ActorSystem> {
  const system: ActorSystem = new ActorSystem("sys");
  await system.start();
  return system;
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve: () => void) => setTimeout(resolve, ms));
}

describe("ActorSystem scheduling", () => {
  it("delivers a one-shot after its delay, exactly once", async () => {
    const system: ActorSystem = await startedSystem();
    const actor: Collector = new Collector();
    const pid: PID = await system.spawn("target", actor);

    await system.scheduleOnce(new Tick(), pid, 30);
    expect(actor.received.length).toBe(0);

    await expect.poll(() => actor.received.length, { timeout: 2000 }).toBe(1);
    expect(actor.received[0]).toBeInstanceOf(Tick);
    expect(actor.senders[0]).toBe(system.noSender());

    await pause(120);
    expect(actor.received.length).toBe(1);

    await system.stop();
  });

  it("delivers a repeating schedule every interval", async () => {
    const system: ActorSystem = await startedSystem();
    const actor: Collector = new Collector();
    const pid: PID = await system.spawn("target", actor);

    await system.schedule(new Tick(), pid, 20, { reference: "heartbeat" });

    await expect.poll(() => actor.received.length >= 3, { timeout: 2000 }).toBe(true);
    expect(actor.received[0]).toBeInstanceOf(Tick);

    await system.stop();
  });

  it("rejects a delay or interval that is not a positive number", async () => {
    const system: ActorSystem = await startedSystem();
    const pid: PID = await system.spawn("target", new Collector());

    await expect(system.scheduleOnce(new Tick(), pid, 0)).rejects.toBe(ErrInvalidInterval);
    await expect(system.schedule(new Tick(), pid, -5)).rejects.toBe(ErrInvalidInterval);
    await expect(system.schedule(new Tick(), pid, Number.NaN)).rejects.toBe(ErrInvalidInterval);

    await system.stop();
  });

  it("rejects every call while the system is not running", async () => {
    const system: ActorSystem = new ActorSystem("sys");
    const started: ActorSystem = await startedSystem();
    const pid: PID = await started.spawn("target", new Collector());

    await expect(system.schedule(new Tick(), pid, 20)).rejects.toBe(ErrActorSystemNotStarted);
    await expect(system.scheduleOnce(new Tick(), pid, 20)).rejects.toBe(ErrActorSystemNotStarted);
    await expect(system.cancelSchedule("ref")).rejects.toBe(ErrActorSystemNotStarted);
    await expect(system.pauseSchedule("ref")).rejects.toBe(ErrActorSystemNotStarted);
    await expect(system.resumeSchedule("ref")).rejects.toBe(ErrActorSystemNotStarted);

    await started.stop();
  });

  it("rejects a target that already stopped at registration time", async () => {
    const system: ActorSystem = await startedSystem();
    const pid: PID = await system.spawn("target", new Collector());
    await pid.shutdown();

    await expect(system.scheduleOnce(new Tick(), pid, 20)).rejects.toBe(ErrDead);

    await system.stop();
  });

  it("rejects a duplicate reference and frees it on cancel", async () => {
    const system: ActorSystem = await startedSystem();
    const pid: PID = await system.spawn("target", new Collector());

    await system.schedule(new Tick(), pid, 5_000, { reference: "job" });
    await expect(system.scheduleOnce(new Tick(), pid, 5_000, { reference: "job" })).rejects.toBe(
      ErrScheduleAlreadyExists,
    );

    await system.cancelSchedule("job");
    await system.schedule(new Tick(), pid, 5_000, { reference: "job" });

    await system.stop();
  });

  it("cancels a schedule so no further tick fires", async () => {
    const system: ActorSystem = await startedSystem();
    const actor: Collector = new Collector();
    const pid: PID = await system.spawn("target", actor);

    await system.schedule(new Tick(), pid, 20, { reference: "heartbeat" });
    await expect.poll(() => actor.received.length >= 1, { timeout: 2000 }).toBe(true);

    await system.cancelSchedule("heartbeat");
    const delivered: number = actor.received.length;

    await pause(120);
    expect(actor.received.length).toBe(delivered);

    await expect(system.cancelSchedule("heartbeat")).rejects.toBe(ErrScheduleNotFound);

    await system.stop();
  });

  it("only fires the schedules that were not cancelled", async () => {
    const system: ActorSystem = await startedSystem();
    const actor: Collector = new Collector();
    const pid: PID = await system.spawn("target", actor);

    await system.scheduleOnce("a", pid, 60, { reference: "a" });
    await system.scheduleOnce("b", pid, 30, { reference: "b" });
    await system.scheduleOnce("c", pid, 90, { reference: "c" });
    await system.cancelSchedule("b");

    await expect.poll(() => actor.received.length, { timeout: 2000 }).toBe(2);
    expect(actor.received).toEqual(["a", "c"]);

    await system.stop();
  });

  it("pauses and resumes a repeating schedule", async () => {
    const system: ActorSystem = await startedSystem();
    const actor: Collector = new Collector();
    const pid: PID = await system.spawn("target", actor);

    await system.schedule(new Tick(), pid, 20, { reference: "heartbeat" });
    await expect.poll(() => actor.received.length >= 1, { timeout: 2000 }).toBe(true);

    await system.pauseSchedule("heartbeat");
    const delivered: number = actor.received.length;

    await pause(120);
    expect(actor.received.length).toBe(delivered);

    // Pausing a paused schedule is a no-op.
    await system.pauseSchedule("heartbeat");

    await system.resumeSchedule("heartbeat");
    await expect.poll(() => actor.received.length > delivered, { timeout: 2000 }).toBe(true);

    await system.stop();
  });

  it("freezes the remaining delay of a paused one-shot", async () => {
    const system: ActorSystem = await startedSystem();
    const actor: Collector = new Collector();
    const pid: PID = await system.spawn("target", actor);

    await system.scheduleOnce(new Tick(), pid, 40, { reference: "reminder" });
    await system.pauseSchedule("reminder");

    await pause(120);
    expect(actor.received.length).toBe(0);

    await system.resumeSchedule("reminder");
    await expect.poll(() => actor.received.length, { timeout: 2000 }).toBe(1);

    await system.stop();
  });

  it("treats resuming a schedule that is not paused as a no-op", async () => {
    const system: ActorSystem = await startedSystem();
    const actor: Collector = new Collector();
    const pid: PID = await system.spawn("target", actor);

    await system.schedule(new Tick(), pid, 20, { reference: "heartbeat" });
    await system.resumeSchedule("heartbeat");

    await expect.poll(() => actor.received.length >= 2, { timeout: 2000 }).toBe(true);

    await system.stop();
  });

  it("keeps the heap ordered through interleaved registrations and cancels", async () => {
    const system: ActorSystem = await startedSystem();
    const actor: Collector = new Collector();
    const pid: PID = await system.spawn("target", actor);

    // The push order shapes the deadline heap so the cancels below
    // exercise both sift-down paths: replacing a middle entry whose
    // subtree is already ordered, and replacing the root when the
    // smaller child is on the right.
    const delays: number[] = [100, 200, 110, 400, 500, 120, 130];
    for (const delay of delays) {
      await system.scheduleOnce(`${delay}`, pid, delay, { reference: `r${delay}` });
    }

    await system.cancelSchedule("r200");
    await system.cancelSchedule("r100");
    await system.cancelSchedule("r500");

    await expect.poll(() => actor.received.length, { timeout: 2000 }).toBe(4);
    expect([...actor.received].sort()).toEqual(["110", "120", "130", "400"]);

    await system.stop();
  });

  it("rejects owned registrations while the system is not running", async () => {
    const system: ActorSystem = new ActorSystem("sys");
    const started: ActorSystem = await startedSystem();
    const pid: PID = await started.spawn("target", new Collector());

    await expect(system.scheduleFrom(pid, new Tick(), pid, 20)).rejects.toBe(
      ErrActorSystemNotStarted,
    );
    await expect(system.scheduleOnceFrom(pid, new Tick(), pid, 20)).rejects.toBe(
      ErrActorSystemNotStarted,
    );

    await started.stop();
  });

  it("dead-letters a tick to a stopped target and drops the repeating schedule", async () => {
    const system: ActorSystem = await startedSystem();
    const actor: Collector = new Collector();
    const pid: PID = await system.spawn("target", actor);

    const deadletters: Deadletter[] = [];
    system.subscribe((event: unknown) => {
      if (event instanceof Deadletter) {
        deadletters.push(event);
      }
    });

    await system.schedule(new Tick(), pid, 20, { reference: "heartbeat" });
    await expect.poll(() => actor.received.length >= 1, { timeout: 2000 }).toBe(true);
    await pid.shutdown();

    await expect.poll(() => deadletters.length >= 1, { timeout: 2000 }).toBe(true);
    expect(deadletters[0]?.receiver).toBe(pid.path().toString());

    // The schedule died with its target: the reference is gone.
    await expect
      .poll(() => system.cancelSchedule("heartbeat").catch((err: Error) => err), { timeout: 2000 })
      .toBe(ErrScheduleNotFound);

    await system.stop();
  });

  it("cancels every schedule when the system stops", async () => {
    const system: ActorSystem = await startedSystem();
    const pid: PID = await system.spawn("target", new Collector());

    await system.schedule(new Tick(), pid, 5_000, { reference: "job" });
    await system.stop();

    await system.start();
    await expect(system.cancelSchedule("job")).rejects.toBe(ErrScheduleNotFound);
    await system.stop();
  });
});

describe("ReceiveContext scheduling", () => {
  it("schedules a one-shot to self with the actor as sender", async () => {
    const system: ActorSystem = await startedSystem();

    class Reminder implements Actor {
      readonly received: unknown[] = [];
      readonly senders: (PID | undefined)[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message instanceof PostStart) {
          return;
        }

        if (ctx.message instanceof Start) {
          void ctx.scheduleOnce(new Remind(), ctx.self as PID, 20, { reference: "remind" });
          return;
        }

        this.received.push(ctx.message);
        this.senders.push(ctx.sender);
      }

      postStop(): void {}
    }

    const actor: Reminder = new Reminder();
    const pid: PID = await system.spawn("reminder", actor);
    system.noSender().tell(pid, new Start(pid));

    await expect.poll(() => actor.received.length, { timeout: 2000 }).toBe(1);
    expect(actor.received[0]).toBeInstanceOf(Remind);
    expect(actor.senders[0]).toBe(pid);

    await system.stop();
  });

  it("cancels an actor's schedules when the actor stops", async () => {
    const system: ActorSystem = await startedSystem();
    const collector: Collector = new Collector();
    const target: PID = await system.spawn("collector", collector);

    class Publisher implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message instanceof Start) {
          // One referenced and one anonymous schedule: both must die
          // with this actor.
          void ctx.schedule(new Tick(), ctx.message.to, 20, { reference: "ticker" });
          void ctx.schedule(new Tick(), ctx.message.to, 20);
        }
      }

      postStop(): void {}
    }

    const publisher: PID = await system.spawn("publisher", new Publisher());
    system.noSender().tell(publisher, new Start(target));

    await expect.poll(() => collector.received.length >= 2, { timeout: 2000 }).toBe(true);
    await publisher.shutdown();

    const delivered: number = collector.received.length;
    await pause(120);
    expect(collector.received.length).toBe(delivered);

    await expect(system.cancelSchedule("ticker")).rejects.toBe(ErrScheduleNotFound);

    await system.stop();
  });

  it("addresses schedules in the flat namespace from any actor", async () => {
    const system: ActorSystem = await startedSystem();
    const collector: Collector = new Collector();
    const target: PID = await system.spawn("collector", collector);

    class Canceller implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message instanceof Tick) {
          void ctx.pauseSchedule("system-job");
          void ctx.resumeSchedule("system-job");
          void ctx.cancelSchedule("system-job");
        }
      }

      postStop(): void {}
    }

    const canceller: PID = await system.spawn("canceller", new Canceller());
    await system.schedule(new Remind(), target, 5_000, { reference: "system-job" });

    system.noSender().tell(canceller, new Tick());

    await expect
      .poll(() => system.cancelSchedule("system-job").catch((err: Error) => err), {
        timeout: 2000,
      })
      .toBe(ErrScheduleNotFound);

    await system.stop();
  });
});

// These exercise the Scheduler directly, the way passivation.test drives
// the PassivationManager, to reach receiver states an actor spawned through
// a started system cannot stably hold: a suspended local actor and a routed
// handle to another isolate.
describe("Scheduler receiver states", () => {
  // A real but unstarted system: standalone PIDs receive no PostStart, and
  // their dead letters are a safe no-op because no dead-letter actor exists.
  const system: ActorSystem = new ActorSystem("sched");

  let nextName = 0;

  function makePid(actor: Actor): PID {
    return new PID(actor, newPath(`actor-${nextName++}`, "sched", "127.0.0.1", 0), system);
  }

  // The sender for schedules told from outside an actor. It never starts;
  // tell only checks the receiver's state.
  const external: PID = makePid(new Collector());

  it("keeps a suspended receiver's repeating schedule instead of dropping it", async () => {
    class Faulty extends Collector {
      override receive(ctx: ReceiveContext): void {
        if (ctx.message === "boom") {
          throw new Error("boom");
        }

        super.receive(ctx);
      }
    }

    const scheduler: Scheduler = new Scheduler();
    const actor: Faulty = new Faulty();
    const pid: PID = makePid(actor);
    await pid.start();

    scheduler.schedule(new Tick(), pid, 20, external, null, "hb");

    // A fault with no parent to decide the directive suspends the actor.
    external.tell(pid, "boom");
    await expect.poll(() => pid.isSuspended()).toBe(true);

    // Ticks fire while it is suspended: each dead-letters, but the schedule
    // is kept, not pruned, because a suspended actor may be reinstated.
    await pause(80);
    expect(pid.isSuspended()).toBe(true);

    // Reinstated, the kept schedule delivers again with no re-registration.
    const before: number = actor.received.length;
    pid.reinstate(pid);
    await expect.poll(() => actor.received.length > before, { timeout: 2000 }).toBe(true);
    expect(actor.received.at(-1)).toBeInstanceOf(Tick);

    scheduler.stop();
  });

  it("registers and repeatedly fires a routed receiver", async () => {
    const calls: string[] = [];
    const route: IsolateRoute = {
      workerId: 3,
      tell: (to, message) => {
        calls.push(`${to.name()}:${String(message)}`);
        return null;
      },
      ask: (to, message) => Promise.resolve(`${to.name()}:${String(message)}`),
      request: () => completedRequest(ErrDead),
      watch: () => {},
      unwatch: () => {},
    };
    const handle: PID = routedPid(
      system,
      newPathAt("far", { system: "sched", host: "127.0.0.1", port: 0 }, undefined, "1"),
      route,
    );

    const scheduler: Scheduler = new Scheduler();

    // A routed handle reports isRunning() false locally, yet registration
    // must accept it and every tick must fire through the route.
    scheduler.schedule("tick", handle, 20, external, null, "routed");
    await expect.poll(() => calls.length >= 2, { timeout: 2000 }).toBe(true);
    expect(calls[0]).toBe("far:tick");

    scheduler.cancel("routed");
    const delivered: number = calls.length;
    await pause(80);
    expect(calls.length).toBe(delivered);

    scheduler.stop();
  });
});
