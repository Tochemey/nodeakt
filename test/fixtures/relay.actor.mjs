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

/** Test fixture: proves a spawn option travels. On "go" it requests
 * itself, which only a reentrant actor may do, and answers the ask
 * with the request's outcome; without reentrancy the request completes
 * with the admission failure and the answer says so. */
export class Relay {
  preStart() {}

  receive(ctx) {
    const message = ctx.message;
    if (message === "go") {
      const asked = ctx;
      ctx.request(ctx.self, "inner", { timeout: 1000 }).onReply((reply, error) => {
        asked.response(error === null ? `ok:${reply}` : `refused:${error.message}`);
      });
      return;
    }

    if (message === "inner") {
      ctx.response("pong");
    }
  }

  postStop() {}
}
