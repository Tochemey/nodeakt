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
 * Showcase: actors own their state and process messages one at a time.
 *
 * Spawns a greeter, tells it two greetings, then asks how many it handled.
 * `count` is mutated only inside `receive`, so it needs no lock.
 *
 * Run: make helloworld
 */

import type { Actor } from "../../src/index";
import { ActorSystem, PostStart, type ReceiveContext, TextLogger } from "../../src/index";

/** A greeting to print. Messages are plain classes; the receiver narrows
 * them with `instanceof`. */
class Greet {
  constructor(readonly name: string) {}
}

/** A question: "how many greetings have you handled so far?" */
class HowMany {}

class Greeter implements Actor {
  /** Private actor state. Safe without synchronization: only this actor's
   * own `receive` ever reads or writes it. */
  private count = 0;

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message = ctx.message;

    // Every actor receives PostStart as its first message. Nothing to do
    // here, so we let it fall through.
    if (message instanceof PostStart) {
      return;
    }

    if (message instanceof Greet) {
      this.count++;
      ctx.logger().info(`Hello, ${message.name}!`);
      return;
    }

    if (message instanceof HowMany) {
      // Answer the pending `ask`. The value travels back to the caller.
      ctx.response(this.count);
    }
  }

  postStop(): void {}
}

const logger = new TextLogger({
  level: "debug",
});

const system = new ActorSystem("hello", {
  logger: logger,
});
await system.start();

const greeter = await system.spawn("greeter", new Greeter());

// From outside any actor, send on behalf of the system's NoSender actor.
const outside = system.noSender();
outside.tell(greeter, new Greet("Ada"));
outside.tell(greeter, new Greet("Alan"));

// `ask` waits for the actor to answer with `ctx.response`, up to the timeout.
const total = (await outside.ask(greeter, new HowMany(), 1_000)) as number;
logger.info(`Greeted ${total} people.`);

await system.stop();
