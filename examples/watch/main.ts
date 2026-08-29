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
 * Showcase: death is a message.
 *
 * A sentinel `watch`es a worker on `PostStart`. When the worker stops, the
 * runtime delivers `Terminated` as an ordinary message on the sentinel's
 * own turn. No polling, no shared flag.
 *
 * Run: make watch
 */

import type { Actor, PID } from "../../src/index";
import {
  ActorSystem,
  PostStart,
  type ReceiveContext,
  Terminated,
  TextLogger,
} from "../../src/index";

/** An actor that does nothing but exist until it is stopped. */
class Worker implements Actor {
  preStart(): void {}
  receive(): void {}
  postStop(): void {}
}

class Sentinel implements Actor {
  constructor(private readonly target: PID) {}

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      // Watch while the target is alive; death watch reports a future stop.
      ctx.watch(this.target);
      ctx.logger().info(`sentinel is watching "${this.target.name()}"`);
      return;
    }

    if (ctx.message instanceof Terminated) {
      ctx.logger().info(`sentinel saw "${ctx.message.actorPath}" terminate`);
    }
  }

  postStop(): void {}
}

const logger = new TextLogger({ level: "debug" });
const system = new ActorSystem("watch", { logger });
await system.start();

const worker = await system.spawn("worker", new Worker());
await system.spawn("sentinel", new Sentinel(worker));

const settle = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

await settle(30); // let the sentinel start and register its watch
await worker.shutdown(); // stop the watched actor
await settle(30); // let the Terminated notification arrive

await system.stop();
