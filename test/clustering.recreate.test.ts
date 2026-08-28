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
import type { Actor } from "../src/actor";
import { recipeToSpawn } from "../src/clustering.recreate";
import { serializePassivation, TimeBasedStrategy } from "../src/passivation";
import { Props } from "../src/props";
import { registerActor } from "../src/registration";
import type { SpawnOptions } from "../src/spawn.options";

/** A registered actor a recreate rebuilds by name. */
class Widget implements Actor {
  constructor(readonly tag: string) {}

  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

registerActor(Widget, "file:///widget.recreate.actor.ts");

describe("recipeToSpawn", () => {
  it("rebuilds props and the data options, forcing relocatable and the singleton marker", () => {
    const { props, options }: { props: Props; options: SpawnOptions } = recipeToSpawn(
      {
        actor: "Widget",
        args: ["w"],
        reentrancy: { mode: "allowAll" },
        passivation: serializePassivation(new TimeBasedStrategy(100)),
      },
      true,
    );

    expect(props).toBeInstanceOf(Props);
    expect(options.reentrancy).toEqual({ mode: "allowAll" });
    expect(options.passivationStrategy).toBeInstanceOf(TimeBasedStrategy);
    expect(options.relocatable).toBe(true);
    expect(options.singleton).toBe(true);
  });

  it("rebuilds a bare recipe with no arguments or data options", () => {
    const { options }: { props: Props; options: SpawnOptions } = recipeToSpawn(
      { actor: "Widget" },
      false,
    );

    expect(options.reentrancy).toBeUndefined();
    expect(options.passivationStrategy).toBeUndefined();
    expect(options.relocatable).toBe(true);
    expect(options.singleton).toBe(false);
  });

  it("throws for a class not registered on this node", () => {
    expect(() => recipeToSpawn({ actor: "Unknown" }, false)).toThrow('actor class "Unknown"');
  });
});
