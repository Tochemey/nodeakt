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

import type { Actor } from "../../src/actor.js";
import type { Context } from "../../src/context.js";
import type { ReceiveContext } from "../../src/receive.context.js";
import { registerActor, registerMessage } from "../../src/registration.js";

/** Where a {@link Worker} answers from, reported by its `host()` reply so a caller
 * can see which node currently hosts it. */
export class WhereAreYou {}

/** A greeting a {@link Worker} echoes back with its region prefix. */
export class Greet {
  constructor(readonly who: string) {}
}

// The ask messages cross nodes when a caller reaches a worker on another node, so
// they are registered to keep `instanceof` intact over the wire.
registerMessage(WhereAreYou);
registerMessage(Greet);

/**
 * A relocatable clustered actor. Its `region` is construction data, so it travels in
 * the actor's recipe: when the node hosting it departs, the coordinator recreates the
 * worker on a survivor from that same recipe, its region intact, on a fresh incarnation.
 * A worker that must recover accumulated state would reload it here in `preStart` from
 * its own source of truth; this one is stateless beyond its region, so a fresh start is
 * all it needs.
 */
export class Worker implements Actor {
  #host: string = "";

  constructor(readonly region: string) {}

  preStart(ctx: Context): void {
    // The host is read at each (re)start, so after a relocation the worker reports the
    // survivor it now runs on rather than the node that departed.
    this.#host = ctx.actorSystem().host();
  }

  receive(ctx: ReceiveContext): void {
    const message: unknown = ctx.message;
    if (message instanceof Greet) {
      ctx.response(`${this.region}:${message.who}`);
      return;
    }

    if (message instanceof WhereAreYou) {
      ctx.response(this.#host);
      return;
    }
  }

  postStop(): void {}
}

registerActor(Worker);
