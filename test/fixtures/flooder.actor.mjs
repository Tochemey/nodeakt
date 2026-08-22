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

/** The tell-flood target: answers the wave barrier, swallows
 * everything else as fast as the loop can hand it over. */
class Sink {
  preStart() {}

  receive(ctx) {
    if (ctx.message === "done") {
      ctx.response("done");
    }
  }

  postStop() {}
}

/** The ask-flood target: answers every ask. */
class AskSink {
  preStart() {}

  receive(ctx) {
    if (typeof ctx.message === "number") {
      ctx.response(ctx.message);
    }
  }

  postStop() {}
}

/** Test fixture: measures its own isolate's local processing rate and
 * reports messages per second, without any message crossing a port.
 * `flood:n` tells a child in bounded waves (an ask barrier drains each
 * wave before the next); `askflood:n` runs sequential ask round trips
 * against a child. */
export class Flooder {
  preStart() {}

  async receive(ctx) {
    const message = ctx.message;
    if (typeof message !== "string") {
      return;
    }

    const self = ctx.self;

    if (message.startsWith("flood:")) {
      const total = Number(message.slice(6));
      if (this.sink === undefined) {
        this.sink = await self.spawnChild("sink", new Sink());
      }

      const wave = 100000;
      let remaining = total;
      const started = Date.now();
      while (remaining > 0) {
        const batch = Math.min(wave, remaining);
        for (let i = 0; i < batch; i++) {
          self.tell(this.sink, i);
        }

        await self.ask(this.sink, "done", 300000);
        remaining -= batch;
      }

      const elapsed = Math.max(1, Date.now() - started);
      ctx.response(Math.round((total / elapsed) * 1000));
      return;
    }

    if (message.startsWith("askflood:")) {
      const total = Number(message.slice(9));
      if (this.askSink === undefined) {
        this.askSink = await self.spawnChild("asksink", new AskSink());
      }

      const started = Date.now();
      for (let i = 0; i < total; i++) {
        await self.ask(this.askSink, i, 300000);
      }

      const elapsed = Math.max(1, Date.now() - started);
      ctx.response(Math.round((total / elapsed) * 1000));
    }
  }

  postStop() {}
}
