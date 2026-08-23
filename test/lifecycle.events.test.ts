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

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { discardLogger } from "../src/discard.logger";
import {
  ActorChildCreated,
  ActorPassivated,
  ActorReinstated,
  ActorRestarted,
  ActorStarted,
  ActorStopped,
  ActorSuspended,
} from "../src/messages";
import { MessagesCountBasedStrategy } from "../src/passivation";
import type { PID } from "../src/pid";
import type { ReceiveContext } from "../src/receive.context";
import { RestartDirective, StopDirective, Supervisor } from "../src/supervisor";

/** A quiet actor that records nothing and never fails. */
class Quiet implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** Throws an `Error` on "boom" and a raw string on "chaos" to exercise
 * both branches of the suspension reason. */
class Faulty implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message === "boom") {
      throw new Error("kaboom");
    }

    if (ctx.message === "chaos") {
      // A non-Error throw exercises the stringified suspension reason.
      const raw: string = "havoc";
      throw raw;
    }
  }

  postStop(): void {}
}

describe("lifecycle events", () => {
  let system: ActorSystem;
  let events: unknown[];

  function only<T>(kind: new (...args: never[]) => T): T[] {
    return events.filter((event): event is T => event instanceof kind);
  }

  beforeEach(async () => {
    system = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
    events = [];
    system.subscribe((event) => {
      events.push(event);
    });
  });

  afterEach(async () => {
    await system.stop();
  });

  it("publishes ActorStarted when an actor starts", async () => {
    const before: number = Date.now();
    const pid: PID = await system.spawn("greeter", new Quiet());

    const started: ActorStarted[] = only(ActorStarted);
    expect(started).toHaveLength(1);
    expect((started[0] as ActorStarted).actorPath).toBe(pid.path().toString());
    expect((started[0] as ActorStarted).timestamp).toBeGreaterThanOrEqual(before);

    // A top-level spawn is not reported as a child creation: its parent is
    // the user guardian, a runtime actor that stays silent.
    expect(only(ActorChildCreated)).toHaveLength(0);
  });

  it("publishes ActorChildCreated alongside the child's ActorStarted", async () => {
    const parent: PID = await system.spawn("parent", new Quiet());
    events.length = 0;

    const child: PID = await parent.spawnChild("child", new Quiet());

    expect(only(ActorStarted).map((event) => event.actorPath)).toEqual([child.path().toString()]);

    const created: ActorChildCreated[] = only(ActorChildCreated);
    expect(created).toHaveLength(1);
    expect((created[0] as ActorChildCreated).actorPath).toBe(child.path().toString());
    expect((created[0] as ActorChildCreated).parent).toBe(parent.path().toString());
    expect((created[0] as ActorChildCreated).timestamp).toBeGreaterThan(0);
  });

  it("publishes ActorStopped when an actor stops", async () => {
    const pid: PID = await system.spawn("mortal", new Quiet());
    events.length = 0;

    await pid.shutdown();

    const stopped: ActorStopped[] = only(ActorStopped);
    expect(stopped).toHaveLength(1);
    expect((stopped[0] as ActorStopped).actorPath).toBe(pid.path().toString());
    expect(only(ActorPassivated)).toHaveLength(0);
  });

  it("publishes ActorPassivated ahead of ActorStopped, and only once", async () => {
    // A budget of three leaves the actor alive after PostStart. Four more
    // messages carry it past the budget with a backlog still queued: the
    // trip announces passivation once, and the messages draining behind the
    // graceful stop must not announce it again.
    const pid: PID = await system.spawn("idle", new Quiet(), {
      passivationStrategy: new MessagesCountBasedStrategy(3),
    });
    events.length = 0;

    for (const message of ["a", "b", "c", "d"]) {
      system.noSender().tell(pid, message);
    }

    await expect.poll(() => pid.isRunning()).toBe(false);

    expect(only(ActorPassivated)).toHaveLength(1);
    expect(only(ActorStopped)).toHaveLength(1);
    expect((only(ActorPassivated)[0] as ActorPassivated).actorPath).toBe(pid.path().toString());

    // Passivation precedes the stop it triggers.
    const kinds: string[] = events.map((event) => (event as object).constructor.name);
    expect(kinds.indexOf("ActorPassivated")).toBeLessThan(kinds.indexOf("ActorStopped"));
  });

  it("publishes ActorRestarted when a supervisor restarts a child", async () => {
    const parent: PID = await system.spawn("host", new Quiet());
    const child: PID = await parent.spawnChild("worker", new Faulty(), {
      supervisor: new Supervisor({ anyErrorDirective: RestartDirective }),
    });
    events.length = 0;

    system.noSender().tell(child, "boom");
    await expect.poll(() => child.restartCount()).toBe(1);

    const restarted: ActorRestarted[] = only(ActorRestarted);
    expect(restarted).toHaveLength(1);
    expect((restarted[0] as ActorRestarted).actorPath).toBe(child.path().toString());
  });

  it("publishes ActorSuspended with the error message and ActorReinstated on revival", async () => {
    const parent: PID = await system.spawn("ward", new Quiet());
    const child: PID = await parent.spawnChild("faulty", new Faulty(), {
      // No directive matches a plain Error, so the child stays suspended.
      supervisor: new Supervisor({ directives: [[RangeError, StopDirective]] }),
    });
    events.length = 0;

    system.noSender().tell(child, "boom");
    await expect.poll(() => child.isSuspended()).toBe(true);

    const suspended: ActorSuspended[] = only(ActorSuspended);
    expect(suspended).toHaveLength(1);
    expect((suspended[0] as ActorSuspended).actorPath).toBe(child.path().toString());
    expect((suspended[0] as ActorSuspended).reason).toBe("kaboom");

    parent.reinstate(child);
    await expect.poll(() => child.isSuspended()).toBe(false);

    const reinstated: ActorReinstated[] = only(ActorReinstated);
    expect(reinstated).toHaveLength(1);
    expect((reinstated[0] as ActorReinstated).actorPath).toBe(child.path().toString());
  });

  it("carries the stringified value when a non-Error is thrown", async () => {
    const parent: PID = await system.spawn("guard", new Quiet());
    const child: PID = await parent.spawnChild("wild", new Faulty(), {
      supervisor: new Supervisor({ directives: [[RangeError, StopDirective]] }),
    });
    events.length = 0;

    system.noSender().tell(child, "chaos");
    await expect.poll(() => child.isSuspended()).toBe(true);

    expect((only(ActorSuspended)[0] as ActorSuspended).reason).toBe("havoc");
  });
});
