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

/**
 * Showcase: send a message later, or on a repeat, without holding a timer.
 *
 * `scheduleOnce` delivers once after a delay; `schedule` delivers on an
 * interval. A `reference` in the options names the schedule so it can be
 * cancelled, paused, and resumed. The same calls on `ReceiveContext` bind
 * a schedule to the scheduling actor, so it dies with the actor and needs
 * no cleanup in `postStop`.
 *
 * Every tick is an ordinary send: it reaches the actor's mailbox on the
 * actor's own turn, and a tick to a stopped actor becomes a dead letter
 * like any other undeliverable message.
 *
 * Run: make scheduling
 */

import type { Actor, PID } from "../../src/index";
import { ActorSystem, ErrScheduleNotFound, type ReceiveContext, TextLogger } from "../../src/index";

// protocol

/** A repeating tick delivered to the worker. */
class Heartbeat {}

/** A one-shot reminder delivered to the worker. */
class Reminder {}

/** Ask the worker how many heartbeats it has seen. */
class Count {}

/** Driver -> ephemeral: arm a tick to yourself. */
class ArmSelfTick {}

/** The self-scheduled tick; it must never fire once the actor has stopped. */
class SelfTick {}

class Worker implements Actor {
  private beats = 0;

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message: unknown = ctx.message;

    if (message instanceof Heartbeat) {
      this.beats++;
      ctx.logger().info(`  heartbeat ${this.beats}`);
      return;
    }

    if (message instanceof Reminder) {
      ctx.logger().info("  reminder fired (one-shot)");
      return;
    }

    if (message instanceof Count) {
      ctx.response(this.beats);
    }
  }

  postStop(): void {}
}

/**
 * Schedules a tick to itself through `ReceiveContext`, so the schedule is
 * owned by this actor. Stopping the actor cancels it; there is no timer to
 * clear in `postStop`.
 */
class Ephemeral implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof ArmSelfTick) {
      // A far-future self-tick that never fires: the actor stops first,
      // and its own schedules are cancelled with it.
      void ctx.scheduleOnce(new SelfTick(), ctx.self as PID, 60_000, {
        reference: "ephemeral-tick",
      });
      return;
    }

    if (ctx.message instanceof SelfTick) {
      ctx.logger().info("  self-tick fired (this should not happen)");
    }
  }

  postStop(): void {}
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve: () => void) => setTimeout(resolve, ms));
}

// --- driver ---------------------------------------------------------------

const logger = new TextLogger({ level: "debug" });
const system: ActorSystem = new ActorSystem("scheduling", {
  logger,
});
await system.start();

const worker: PID = await system.spawn("worker", new Worker());

// One-shot: a single reminder, 30ms out.
logger.info("scheduleOnce: a reminder in 30ms");
await system.scheduleOnce(new Reminder(), worker, 30);

// Repeating: a heartbeat every 20ms, addressable as "heartbeat".
logger.info('schedule: a heartbeat every 20ms, reference "heartbeat"');
await system.schedule(new Heartbeat(), worker, 20, { reference: "heartbeat" });
await sleep(70);

// Pause it: nothing fires while paused.
logger.info("pauseSchedule: hold the heartbeat");
await system.pauseSchedule("heartbeat");
await sleep(70);

// Resume it: heartbeats come back, one interval after the resume.
logger.info("resumeSchedule: let it beat again");
await system.resumeSchedule("heartbeat");
await sleep(70);

// Cancel it for good.
logger.info("cancelSchedule: stop the heartbeat");
await system.cancelSchedule("heartbeat");
await sleep(40);

const beats: number = (await system.noSender().ask(worker, new Count(), 1_000)) as number;
logger.info(`total heartbeats delivered: ${beats}`);

// Ownership: a schedule created inside an actor dies with the actor.
logger.info("\nownership: an actor's own schedule is cancelled when it stops");
const ephemeral: PID = await system.spawn("ephemeral", new Ephemeral());
system.noSender().tell(ephemeral, new ArmSelfTick());
await sleep(10);
await ephemeral.shutdown();

// The reference is gone: stopping the actor already cancelled its schedule.
try {
  await system.cancelSchedule("ephemeral-tick");
  logger.info("  schedule still present (unexpected)");
} catch (err: unknown) {
  if (err === ErrScheduleNotFound) {
    logger.info("  the ephemeral actor's schedule was cancelled with it");
  }
}

await system.stop();
