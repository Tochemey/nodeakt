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

import { describe, expect, it } from "vitest";
import type { Actor } from "../src/actor/actor";
import { Props } from "../src/actor/props";

class Quiet implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

class Loud implements Actor {
  constructor(
    readonly lang: string,
    readonly excited: boolean,
  ) {}

  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

describe("Props", () => {
  it("captures a class without constructor arguments", () => {
    const props = Props.create(Quiet);

    expect(props.type()).toBe(Quiet);
    expect(props.args()).toEqual([]);
  });

  it("captures constructor arguments in order", () => {
    const props = Props.create(Loud, "fr", true);

    expect(props.type()).toBe(Loud);
    expect(props.args()).toEqual(["fr", true]);
  });

  it("rejects mismatched constructor arguments at compile time", () => {
    // @ts-expect-error the constructor requires two arguments
    Props.create(Loud);
    // @ts-expect-error the first argument must be a string
    Props.create(Loud, 42, true);
    // @ts-expect-error a class whose instances are not actors is not spawnable
    Props.create(class NotAnActor {});

    expect(true).toBe(true);
  });
});
