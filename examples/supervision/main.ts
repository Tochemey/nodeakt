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
 * Showcase: let it crash, then recover.
 *
 * Job 13 throws. The supervisor restarts the worker with backoff: `preStart`
 * runs again (fresh state) and the queued jobs drain. The "completed"
 * counter resetting to 1 is the restart, not a patch.
 *
 * Run: make supervision
 */

import type { Actor, Context } from "../../src/index";
import {
  ActorSystem,
  type ReceiveContext,
  RestartDirective,
  Supervisor,
  TextLogger,
} from "../../src/index";

/** A unit of work; job 13 is unlucky and makes the worker throw. */
class Job {
  constructor(readonly n: number) {}
}

/** The failure the supervisor knows how to recover from. */
class Unlucky extends Error {}

class Worker implements Actor {
  private completed = 0;

  preStart(ctx: Context): void {
    // Initialize state here, not in a field or the constructor: a restart
    // reuses the instance and re-runs preStart, so this is what makes the
    // recovered actor start from a clean slate.
    this.completed = 0;
    ctx.actorSystem().logger().info("worker started with fresh state");
  }

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Job) {
      if (ctx.message.n === 13) {
        throw new Unlucky();
      }

      this.completed++;
      ctx
        .logger()
        .info(`  did job ${ctx.message.n} (completed ${this.completed} since last start)`);
    }
  }

  postStop(): void {}
}

const logger = new TextLogger({ level: "debug" });
const system = new ActorSystem("supervision", { logger });
await system.start();

const worker = await system.spawn("worker", new Worker(), {
  supervisor: new Supervisor({
    directives: [[Unlucky, RestartDirective]],
    maxRetries: 3,
    initialDelay: 50,
    maxDelay: 200,
  }),
});

const outside = system.noSender();
for (const n of [1, 2, 13, 3, 4]) {
  outside.tell(worker, new Job(n));
}

// Give the restart (delayed by backoff) time to run and drain the backlog.
await new Promise<void>((resolve) => {
  setTimeout(resolve, 500);
});

await system.stop();
