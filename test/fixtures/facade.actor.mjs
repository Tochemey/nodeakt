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

/** The target the facade spawns on its own isolate. */
class Cell {
  preStart() {}

  receive(ctx) {
    if (typeof ctx.message === "string") {
      ctx.response(`cell:${ctx.message}`);
    }
  }

  postStop() {}
}

/** Test fixture: exercises the worker facade's own actor system from
 * inside a worker. "spawn|name" spawns a top-level instance through
 * the facade (claiming the name pool-wide); "find|name|payload" looks
 * a top-level name up through the facade's actorOf and asks it,
 * wherever it lives. */
export class Facade {
  preStart() {}

  async receive(ctx) {
    const message = ctx.message;
    if (typeof message !== "string") {
      return;
    }

    const system = ctx.actorSystem();

    if (message.startsWith("spawn|")) {
      const name = message.slice(6);
      try {
        await system.spawn(name, new Cell());
        ctx.response("spawned");
      } catch (err) {
        ctx.response(`refused:${err.message}`);
      }

      return;
    }

    if (message.startsWith("find|")) {
      const [, name, payload] = message.split("|");
      const pid = system.actorOf(name);
      if (pid === undefined) {
        ctx.response("missing");
        return;
      }

      try {
        const reply = await ctx.self.ask(pid, payload, 10000);
        ctx.response({ ok: true, reply });
      } catch (err) {
        ctx.response({ ok: false, error: err.message });
      }
    }
  }

  postStop() {}
}
