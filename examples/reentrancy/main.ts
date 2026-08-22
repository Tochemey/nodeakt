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
 * Showcase: ask without freezing.
 *
 * `ctx.request` returns a handle immediately; `onReply` runs later on this
 * actor's own turn. The driver fires two Computes, then asks Status: the
 * aggregator answers "2 in flight" because it was never parked.
 *
 * Run: make reentrancy
 */

import type { Actor, PID } from "../../src/index";
import { ActorSystem, type ReceiveContext } from "../../src/index";

// --- protocol -------------------------------------------------------------

/** Aggregator -> backend: fetch a value for this key. */
class Fetch {
  constructor(readonly key: string) {}
}

/** Backend -> aggregator: the value. */
class Value {
  constructor(
    readonly key: string,
    readonly value: number,
  ) {}
}

/** Driver -> aggregator: compute the value for this key (via the backend). */
class Compute {
  constructor(readonly key: string) {}
}

/** Driver -> aggregator: how many requests are in flight right now? */
class Status {}

// --- actors ---------------------------------------------------------------

/** A plain request/response service. It answers immediately; no blocking. */
class Backend implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Fetch) {
      ctx.response(new Value(ctx.message.key, ctx.message.key.length * 10));
    }
  }

  postStop(): void {}
}

class Aggregator implements Actor {
  private pending = 0;

  constructor(private readonly backend: PID) {}

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message = ctx.message;

    if (message instanceof Compute) {
      this.pending++;

      // Non-parking request: returns at once. The actor stays free to
      // handle the next message while the reply is in flight.
      const call = ctx.request(this.backend, new Fetch(message.key));
      call.onReply((reply, error) => {
        // This continuation runs on the aggregator's own turn.
        this.pending--;
        if (error !== null) {
          console.log(`  ${message.key} failed: ${error.message}`);
          return;
        }

        const value = reply as Value;
        console.log(`  ${value.key} = ${value.value} (${this.pending} still in flight)`);
      });
      return;
    }

    if (message instanceof Status) {
      // Answered even while requests are outstanding: never blocked.
      ctx.response(this.pending);
    }
  }

  postStop(): void {}
}

// --- driver ---------------------------------------------------------------

const system = new ActorSystem("reentrancy");
await system.start();

const backend = await system.spawn("backend", new Backend());
const aggregator = await system.spawn("aggregator", new Aggregator(backend), {
  reentrancy: { mode: "allowAll" },
});

const outside = system.noSender();
outside.tell(aggregator, new Compute("alpha"));
outside.tell(aggregator, new Compute("bravo"));

// Asked right after firing two requests; answered without waiting for them.
const inFlight = (await outside.ask(aggregator, new Status(), 1_000)) as number;
console.log(`status while computing: ${inFlight} in flight`);

// Let the backend replies land and their continuations run.
await new Promise<void>((resolve) => {
  setTimeout(resolve, 50);
});

await system.stop();
