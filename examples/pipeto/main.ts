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
 * Showcase: deliver an async result as a message.
 *
 * `ctx.pipeTo` runs a task off the actor's turn and delivers its result
 * like any other message. The loader stays free between the Load and the
 * Order: it pipes the fetch to itself, keeps processing, and receives the
 * Order later. A rejected task never reaches the actor; it becomes a dead
 * letter instead.
 *
 * Run: make pipeto
 */

import type { Actor, PID } from "../../src/index";
import { ActorSystem, Deadletter, type ReceiveContext, TextLogger } from "../../src/index";

// protocol

/** Driver -> loader: fetch the order with this id. */
class Load {
  constructor(readonly id: number) {}
}

/** The fetched result, delivered to the loader like any other message. */
class Order {
  constructor(
    readonly id: number,
    readonly total: number,
  ) {}
}

/** Driver -> loader: how many orders have landed so far? */
class Count {}

// a stand-in for real async work (a DB read, an HTTP call)

function fetchOrder(id: number, signal: AbortSignal): Promise<Order> {
  return new Promise<Order>((resolve, reject) => {
    // The odd id is the one that fails, to show the dead-letter path.
    const timer: NodeJS.Timeout = setTimeout(() => {
      if (id % 2 === 1) {
        reject(new Error(`order ${id} not found`));
        return;
      }

      resolve(new Order(id, id * 100));
    }, 20);

    // Stop the work if the pipe's deadline fires first.
    signal.addEventListener("abort", () => {
      clearTimeout(timer);
      reject(new Error("aborted"));
    });
  });
}

class Loader implements Actor {
  private landed = 0;

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message: unknown = ctx.message;

    if (message instanceof Load) {
      // Returns at once. The task runs off this turn; its Order result is
      // delivered back here on a later turn, with this actor as sender.
      const self: PID = ctx.self as PID;
      ctx.pipeTo(self, (signal: AbortSignal) => fetchOrder(message.id, signal), {
        timeout: 1_000,
      });
      return;
    }

    if (message instanceof Order) {
      this.landed++;
      ctx.logger().info(`  order ${message.id} landed: total ${message.total}`);
      return;
    }

    if (message instanceof Count) {
      ctx.response(this.landed);
    }
  }

  postStop(): void {}
}

// --- driver ---------------------------------------------------------------

const logger = new TextLogger({ level: "debug" });
const system: ActorSystem = new ActorSystem("pipeto", {
  logger,
});
await system.start();

// A failed pipe becomes a dead letter; subscribe to see it.
system.subscribe((event: unknown) => {
  if (event instanceof Deadletter) {
    logger.info(`  dead letter for ${event.receiver}: ${event.reason}`);
  }
});

const loader: PID = await system.spawn("loader", new Loader());
const outside: PID = system.noSender();

// Two even ids succeed; the odd id fails and dead-letters.
outside.tell(loader, new Load(2));
outside.tell(loader, new Load(3));
outside.tell(loader, new Load(4));

// Asked immediately: the loader answers 0 because no fetch has settled yet.
const early: number = (await outside.ask(loader, new Count(), 1_000)) as number;
logger.info(`orders landed before any fetch settled: ${early}`);

// Let the fetches settle and their results (or dead letters) arrive.
await new Promise<void>((resolve) => {
  setTimeout(resolve, 60);
});

const total: number = (await outside.ask(loader, new Count(), 1_000)) as number;
logger.info(`orders landed after fetches settled: ${total}`);

await system.stop();
