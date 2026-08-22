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
 * Showcase: defer work until the actor is ready.
 *
 * Work that arrives before initialization is `stash`ed. On Ready, `become`
 * plus `unstashAll` replays the backlog in arrival order. No lock, no wait.
 *
 * Run: make stash
 */

import type { Actor } from "../../src/index";
import { ActorSystem, type ReceiveContext } from "../../src/index";

/** A piece of work to perform once ready. */
class Work {
  constructor(readonly job: string) {}
}

/** Signals that initialization has finished. */
class Ready {}

class Loader implements Actor {
  preStart(): void {}

  /** Initial behavior: not ready, so stash any work that arrives. */
  receive(ctx: ReceiveContext): void {
    const message = ctx.message;

    if (message instanceof Work) {
      console.log(`  stashing "${message.job}" (not ready yet)`);
      ctx.stash();
      return;
    }

    if (message instanceof Ready) {
      console.log("initialized; switching to serving and replaying the backlog");
      ctx.become(this.serving);
      ctx.unstashAll(); // re-deliver the stashed Work to `serving`, in order
    }
  }

  /** Ready behavior: process work immediately. */
  private serving = (ctx: ReceiveContext): void => {
    if (ctx.message instanceof Work) {
      console.log(`  processing "${ctx.message.job}"`);
    }
  };

  postStop(): void {}
}

const system = new ActorSystem("stash");
await system.start();

const loader = await system.spawn("loader", new Loader());
const outside = system.noSender();

// These arrive before the actor is ready: they get stashed.
outside.tell(loader, new Work("a"));
outside.tell(loader, new Work("b"));

// Now it is ready: the two stashed jobs replay and run.
outside.tell(loader, new Ready());

await new Promise<void>((resolve) => {
  setTimeout(resolve, 50);
});

await system.stop();
