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

import { type Actor, PostStart, type ReceiveContext, registerActor } from "../../src/index";

const GREETINGS: Record<string, string> = {
  en: "Hello",
  fr: "Bonjour",
  es: "Hola",
  de: "Hallo",
};

/**
 * A greeter configured by a constructor argument. `Props.create` captures
 * that construction as data, so the runtime can build this actor wherever
 * it places it.
 */
export class Greeter implements Actor {
  constructor(private readonly lang: string) {}

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    if (typeof ctx.message === "string") {
      const hello = GREETINGS[this.lang] ?? "Hello";
      ctx.response(`${hello}, ${ctx.message}!`);
    }
  }

  postStop(): void {}
}

// Records where this actor's code lives, so any isolate can rebuild it from
// a recipe. Once, at module scope; the module URL is inferred from here.
registerActor(Greeter);
