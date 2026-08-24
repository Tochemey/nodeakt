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

/** The child half of the deep-family fixture: introduces itself to
 * whoever the parent points it at and answers asks, so a remote node
 * ends up holding the child's own handle. */
class Junior {
  preStart() {}

  receive(ctx) {
    const message = ctx.message;
    if (typeof message === "object" && message !== null && "greet" in message) {
      ctx.tell(message.greet, "hi-from-junior");
      return;
    }

    if (message === "who") {
      ctx.response("junior-here");
    }
  }

  postStop() {}
}

/** Test fixture: a placed parent that spawns a child on demand and has
 * the child introduce itself to the requester, so tests can prove a
 * child of a placed actor is reachable from other nodes. */
export class DeepParent {
  preStart() {}

  async receive(ctx) {
    if (ctx.message !== "delegate") {
      return;
    }

    if (this.junior === undefined) {
      this.junior = await ctx.self.spawnChild("junior", new Junior());
    }

    ctx.tell(this.junior, { greet: ctx.sender });
  }

  postStop() {}
}
