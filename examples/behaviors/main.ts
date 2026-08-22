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
 * Showcase: an actor is a state machine.
 *
 * `become` swaps the message handler at runtime. State lives in which
 * behavior is active, not in flags. `unBecome` from yellow returns to
 * the default `receive` (red), closing the cycle.
 *
 * Run: make behaviors
 */

import type { Actor } from "../../src/index";
import { ActorSystem, type ReceiveContext } from "../../src/index";

/** Advances the light by one step. */
class Tick {}

class TrafficLight implements Actor {
  preStart(): void {}

  /** The default behavior is the RED state. */
  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Tick) {
      console.log("🔴 red    -> 🟢 green");
      ctx.become(this.green);
    }
  }

  private green = (ctx: ReceiveContext): void => {
    if (ctx.message instanceof Tick) {
      console.log("🟢 green  -> 🟡 yellow");
      ctx.become(this.yellow);
    }
  };

  private yellow = (ctx: ReceiveContext): void => {
    if (ctx.message instanceof Tick) {
      console.log("🟡 yellow -> 🔴 red");
      // Revert to the default `receive`, which is the red state.
      ctx.unBecome();
    }
  };

  postStop(): void {}
}

const system = new ActorSystem("traffic");
await system.start();

const light = await system.spawn("light", new TrafficLight());
const outside = system.noSender();

// Two full cycles: red -> green -> yellow -> red -> green -> yellow -> red.
for (let i = 0; i < 6; i++) {
  outside.tell(light, new Tick());
}

// Let the six queued Ticks drain before stopping.
await new Promise<void>((resolve) => {
  setTimeout(resolve, 50);
});

await system.stop();
