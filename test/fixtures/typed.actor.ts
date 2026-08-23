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

import type { Actor } from "../../src/actor";
import { PostStart } from "../../src/messages";
import type { ReceiveContext } from "../../src/receive.context";
import { registerActor, registerMessage } from "../../src/registration";

/**
 * A registered request message. Its `instanceof` must survive the hop to
 * the worker isolate, which only holds when the worker's transport reads
 * from the same registry these module-scope `registerMessage` lines
 * populate.
 */
export class Ping {
  constructor(readonly n: number) {}
}

/** A registered reply message, crossing the boundary the other way. */
export class Pong {
  constructor(readonly n: number) {}
}

registerMessage(Ping);
registerMessage(Pong);

/** Replies to a registered {@link Ping} with a registered {@link Pong},
 * so a typed round trip exercises decode on the worker and decode on the
 * main isolate both. */
export class PingReplier implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    if (ctx.message instanceof Ping) {
      ctx.response(new Pong(ctx.message.n + 1));
    }
  }

  postStop(): void {}
}

registerActor(PingReplier);
