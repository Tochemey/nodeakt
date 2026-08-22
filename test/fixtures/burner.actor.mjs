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

/** Test fixture: burns CPU on demand, so tests can measure whether
 * actors on different isolates genuinely run in parallel. `burn:ms`
 * spins for a wall-clock duration (right for proving concurrency);
 * `work:n` runs a fixed number of iterations (right for measuring
 * speedup, since heterogeneous cores finish fixed work at different
 * speeds). */
export class Burner {
  preStart() {}

  receive(ctx) {
    const message = ctx.message;
    if (typeof message !== "string") {
      return;
    }

    if (message.startsWith("burn:")) {
      const ms = Number(message.slice(5));
      const end = Date.now() + ms;
      while (Date.now() < end) {
        // Busy loop: this is the point.
      }

      ctx.response(ms);
      return;
    }

    if (message.startsWith("work:")) {
      let n = Number(message.slice(5));
      let acc = 1;
      while (n-- > 0) {
        acc = (acc * 48271 + n) % 2147483647;
      }

      ctx.response(acc);
    }
  }

  postStop() {}
}
