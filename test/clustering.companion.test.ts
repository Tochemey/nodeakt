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
import { type Companion, decodeCompanion, encodeCompanion } from "../src/clustering.companion";
import { ByteWriter, encodeValue } from "../src/net/values";
import type { ActorRecipe } from "../src/protocol";

/** The bytes an arbitrary value encodes to, for crafting corrupt companion records. */
function bytesOf(value: unknown): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, value);
  return writer.bytes().slice();
}

describe("companion codec", () => {
  it("round-trips a full recipe and its singleton flag", () => {
    const recipe: ActorRecipe = {
      module: "file:///worker.mjs",
      actor: "Worker",
      args: ["region", 3, true],
      reentrancy: { mode: "allowAll" },
      relocatable: true,
    };

    const decoded: Companion = decodeCompanion(encodeCompanion({ recipe, singleton: true }));

    expect(decoded.singleton).toBe(true);
    expect(decoded.recipe.module).toBe("file:///worker.mjs");
    expect(decoded.recipe.actor).toBe("Worker");
    expect(decoded.recipe.args).toEqual(["region", 3, true]);
    expect(decoded.recipe.reentrancy).toEqual({ mode: "allowAll" });
  });

  it("round-trips a bare recipe with no arguments", () => {
    const decoded: Companion = decodeCompanion(
      encodeCompanion({ recipe: { module: "m", actor: "A" }, singleton: false }),
    );

    expect(decoded.singleton).toBe(false);
    expect(decoded.recipe.args).toBeUndefined();
  });

  it("rejects bytes that do not decode to an object", () => {
    expect(() => decodeCompanion(bytesOf(42))).toThrow("not an object");
  });

  it("rejects a record missing its singleton flag", () => {
    expect(() =>
      decodeCompanion(bytesOf({ recipe: { module: "m", actor: "A" }, singleton: 1 })),
    ).toThrow("no singleton flag");
  });

  it("rejects a record whose recipe is not an object", () => {
    expect(() => decodeCompanion(bytesOf({ recipe: 5, singleton: true }))).toThrow("no recipe");
  });

  it("rejects a recipe missing its module or actor", () => {
    expect(() => decodeCompanion(bytesOf({ recipe: { actor: "A" }, singleton: true }))).toThrow(
      "module or actor",
    );
  });

  it("rejects a recipe whose arguments are not an array", () => {
    expect(() =>
      decodeCompanion(bytesOf({ recipe: { module: "m", actor: "A", args: 5 }, singleton: true })),
    ).toThrow("malformed arguments");
  });
});
