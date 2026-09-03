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
import type { Actor } from "../../src/actor";
import { ActorSystem } from "../../src/actor.system";
import { discardLogger } from "../../src/discard.logger";
import { ErrActorSystemNotStarted } from "../../src/errors";
import { MessagesCountBasedStrategy } from "../../src/passivation";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { RestartDirective, StopDirective, Supervisor } from "../../src/supervisor";

/** A quiet actor that records nothing and never fails. */
class Quiet implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** Throws on "boom" to drive restart and suspension paths. */
class Faulty implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message === "boom") {
      throw new Error("kaboom");
    }
  }

  postStop(): void {}
}

/** Parks on a gate so messages pile up in its mailbox until released. */
class Blocking implements Actor {
  private readonly gate: Promise<void>;
  private open: () => void = () => {};

  constructor() {
    this.gate = new Promise<void>((resolve) => {
      this.open = resolve;
    });
  }

  preStart(): void {}

  async receive(): Promise<void> {
    await this.gate;
  }

  release(): void {
    this.open();
  }

  postStop(): void {}
}

describe("ActorSystem.collectMetrics", () => {
  it("rejects when the system is not running", async () => {
    const system: ActorSystem = new ActorSystem("sys", {
      logger: discardLogger,
      metrics: { enabled: true },
    });

    await expect(system.collectMetrics()).rejects.toBe(ErrActorSystemNotStarted);

    await system.start();
    await system.stop();

    await expect(system.collectMetrics()).rejects.toBe(ErrActorSystemNotStarted);
  });

  it("returns a zeroed snapshot when metrics are disabled", async () => {
    const system: ActorSystem = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
    await system.spawn("greeter", new Quiet());

    const snapshot = await system.collectMetrics();
    expect(snapshot.system).toBe("sys");
    expect(snapshot.actors.startedTotal).toBe(0);
    expect(snapshot.messages.processedTotal).toBe(0);
    expect(snapshot.deadlettersTotal).toBe(0);
    expect(snapshot.remoting).toBeUndefined();
    expect(snapshot.cluster).toBeUndefined();

    await system.stop();
  });

  it("answers a metrics request with null when metrics are disabled", async () => {
    const system: ActorSystem = new ActorSystem("sys", { logger: discardLogger });
    await system.start();

    // A worker isolate answers the main isolate's metrics request with its
    // raw contribution; one that never enabled metrics has none to give.
    expect(system.isolateMetrics()).toBeNull();

    await system.stop();
  });

  it("carries neither a remoting nor a cluster section on a local system", async () => {
    const system: ActorSystem = new ActorSystem("sys", {
      logger: discardLogger,
      metrics: { enabled: true },
    });
    await system.start();

    const snapshot = await system.collectMetrics();
    expect(snapshot.remoting).toBeUndefined();
    expect(snapshot.cluster).toBeUndefined();

    await system.stop();
  });

  describe("with metrics enabled", () => {
    let system: ActorSystem;

    beforeEach(async () => {
      system = new ActorSystem("sys", { logger: discardLogger, metrics: { enabled: true } });
      await system.start();
    });

    afterEach(async () => {
      await system.stop();
    });

    it("counts started actors", async () => {
      await system.spawn("a", new Quiet());
      await system.spawn("b", new Quiet());

      expect((await system.collectMetrics()).actors.startedTotal).toBe(2);
    });

    it("counts stopped actors", async () => {
      const pid: PID = await system.spawn("mortal", new Quiet());
      await pid.shutdown();

      expect((await system.collectMetrics()).actors.stoppedTotal).toBe(1);
    });

    it("counts restarted actors", async () => {
      const parent: PID = await system.spawn("host", new Quiet());
      const child: PID = await parent.spawnChild("worker", new Faulty(), {
        supervisor: new Supervisor({ anyErrorDirective: RestartDirective }),
      });

      system.noSender().tell(child, "boom");
      await expect.poll(() => child.restartCount()).toBe(1);

      expect((await system.collectMetrics()).actors.restartedTotal).toBe(1);
    });

    it("counts passivated actors", async () => {
      const pid: PID = await system.spawn("idle", new Quiet(), {
        passivationStrategy: new MessagesCountBasedStrategy(3),
      });

      for (const message of ["a", "b", "c", "d"]) {
        system.noSender().tell(pid, message);
      }

      await expect.poll(() => pid.isRunning()).toBe(false);

      const snapshot = await system.collectMetrics();
      expect(snapshot.actors.passivatedTotal).toBe(1);
      expect(snapshot.actors.stoppedTotal).toBe(1);
    });

    it("counts dead letters", async () => {
      const pid: PID = await system.spawn("gone", new Quiet());
      await pid.shutdown();

      expect(system.noSender().tell(pid, "witness")).not.toBeNull();

      await expect.poll(async () => (await system.collectMetrics()).deadlettersTotal).toBe(1);
    });

    it("reports the active and suspended gauges over live actors", async () => {
      await system.spawn("a", new Quiet());
      const parent: PID = await system.spawn("host", new Quiet());
      const child: PID = await parent.spawnChild("faulty", new Faulty(), {
        // No directive matches a plain Error, so the child stays suspended and alive.
        supervisor: new Supervisor({ directives: [[RangeError, StopDirective]] }),
      });

      system.noSender().tell(child, "boom");
      await expect.poll(() => child.isSuspended()).toBe(true);

      const snapshot = await system.collectMetrics();
      // a, host, and the suspended faulty child are all alive.
      expect(snapshot.actors.active).toBe(3);
      expect(snapshot.actors.suspended).toBe(1);
    });

    it("reports mailbox depth for a backlogged actor", async () => {
      const blocking = new Blocking();
      const pid: PID = await system.spawn("busy", blocking);

      for (let i = 0; i < 5; i++) {
        system.noSender().tell(pid, i);
      }

      await expect.poll(() => pid.mailboxSize()).toBeGreaterThan(0);

      const snapshot = await system.collectMetrics();
      expect(snapshot.mailbox.totalDepth).toBeGreaterThan(0);
      expect(snapshot.mailbox.maxDepth).toBeGreaterThan(0);
      expect(snapshot.mailbox.maxDepth).toBeLessThanOrEqual(snapshot.mailbox.totalDepth);

      // Release the gate so the mailbox drains and the actor stops cleanly.
      blocking.release();
    });

    it("grows the processed total as messages are handled", async () => {
      const pid: PID = await system.spawn("counter", new Quiet());
      const before: number = (await system.collectMetrics()).messages.processedTotal;

      for (let i = 0; i < 50; i++) {
        system.noSender().tell(pid, i);
      }

      await expect.poll(() => pid.processedCount()).toBeGreaterThanOrEqual(50);
      const snapshot = await system.collectMetrics();
      expect(snapshot.messages.processedTotal).toBeGreaterThanOrEqual(before + 50);
      // The latency histogram is absent until processing-duration timing is on.
      expect(snapshot.messages.processingDurationMs).toBeUndefined();
    });
  });

  describe("with processing-duration timing", () => {
    let system: ActorSystem;

    beforeEach(async () => {
      system = new ActorSystem("sys", {
        logger: discardLogger,
        metrics: { enabled: true, processingDuration: true },
      });
      await system.start();
    });

    afterEach(async () => {
      await system.stop();
    });

    it("accumulates a processing-duration histogram", async () => {
      const pid: PID = await system.spawn("timed", new Quiet());

      for (let i = 0; i < 20; i++) {
        system.noSender().tell(pid, i);
      }

      await expect.poll(() => pid.processedCount()).toBeGreaterThanOrEqual(20);

      const histogram = (await system.collectMetrics()).messages.processingDurationMs;
      expect(histogram).toBeDefined();
      const data = histogram as NonNullable<typeof histogram>;
      expect(data.count).toBeGreaterThanOrEqual(20);
      expect(data.sum).toBeGreaterThanOrEqual(0);

      // Buckets are cumulative, so the unbounded last one holds every sample.
      const last = data.buckets[data.buckets.length - 1] as (typeof data.buckets)[number];
      expect(last.count).toBe(data.count);
    });
  });
});

describe("PID.metrics", () => {
  let system: ActorSystem;

  beforeEach(async () => {
    system = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
  });

  afterEach(async () => {
    await system.stop();
  });

  it("mirrors the actor's own numbers, without requiring the metrics option", async () => {
    const parent: PID = await system.spawn("parent", new Quiet());
    await parent.spawnChild("one", new Quiet());
    await parent.spawnChild("two", new Quiet());

    const metrics = parent.metrics();
    expect(metrics.path).toBe(parent.path().toString());
    expect(metrics.processedCount).toBe(parent.processedCount());
    expect(metrics.restartCount).toBe(parent.restartCount());
    expect(metrics.stashSize).toBe(parent.stashSize());
    expect(metrics.childrenCount).toBe(2);
    expect(metrics.lastActivity).toBe(parent.latestActivity());
    expect(metrics.suspended).toBe(false);
    expect(metrics.mailboxSize).toBeGreaterThanOrEqual(0);
  });

  it("reports a suspended actor as suspended", async () => {
    const parent: PID = await system.spawn("ward", new Quiet());
    const child: PID = await parent.spawnChild("faulty", new Faulty(), {
      // No directive matches a plain Error, so the child stays suspended.
      supervisor: new Supervisor({ directives: [[RangeError, StopDirective]] }),
    });

    system.noSender().tell(child, "boom");
    await expect.poll(() => child.isSuspended()).toBe(true);

    expect(child.metrics().suspended).toBe(true);
  });
});
