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
import { createRandom, type Random, randomSeed, SeededRandom } from "../../src/membership/random";

function choices(source: Random): readonly number[] {
  return Array.from({ length: 20 }, (): number => source.integer(17));
}

describe("seeded random source", () => {
  it("reproduces a stable xorshift32 sequence and reports its seed", () => {
    const source = new SeededRandom(1);
    expect(source.seed).toBe(1);
    expect(source.next()).toBe(270_369 / 0x1_0000_0000);
    expect(source.next()).toBe(67_634_689 / 0x1_0000_0000);
    expect(choices(new SeededRandom(0x1234_5678))).toEqual(choices(new SeededRandom(0x1234_5678)));
  });

  it("handles zero and the full uint32 integer range deterministically", () => {
    const first = new SeededRandom(0);
    const second = new SeededRandom(0);
    expect(first.integer(0x1_0000_0000)).toBe(second.integer(0x1_0000_0000));
    expect(first.next()).toBe(second.next());
  });

  it("keeps integer choices inside every requested half-open range", () => {
    const source = new SeededRandom(99);
    expect(Array.from({ length: 100 }, (): number => source.integer(1))).toEqual(
      Array.from({ length: 100 }, (): number => 0),
    );
    for (const maximum of [2, 3, 255, 65_537, 0x1_0000_0000]) {
      for (let count = 0; count < 100; count += 1) {
        const value = source.integer(maximum);
        expect(Number.isInteger(value)).toBe(true);
        expect(value).toBeGreaterThanOrEqual(0);
        expect(value).toBeLessThan(maximum);
      }
    }
  });

  it("retries samples in the modulo-bias rejection interval", () => {
    const source = new SeededRandom(1);
    const value = source.integer(0x8000_0001);

    expect(value).toBeGreaterThanOrEqual(0);
    expect(value).toBeLessThan(0x8000_0001);
  });

  it("picks values and shuffles a copy without loss", () => {
    const values = ["a", "b", "c", "d", "e"] as const;
    const first = new SeededRandom(42);
    const second = new SeededRandom(42);

    expect(first.pick(values)).toBe(second.pick(values));
    const shuffled = first.shuffle(values);
    expect(shuffled).toEqual(second.shuffle(values));
    expect(shuffled).not.toBe(values);
    expect([...shuffled].sort()).toEqual([...values]);
    expect(values).toEqual(["a", "b", "c", "d", "e"]);
  });

  it("validates seeds, bounds, and empty picks", () => {
    for (const seed of [-1, 0x1_0000_0000, 1.5, Number.NaN]) {
      expect((): SeededRandom => new SeededRandom(seed)).toThrow(RangeError);
    }
    const source = new SeededRandom(1);
    for (const maximum of [0, -1, 1.5, 0x1_0000_0001, Number.NaN]) {
      expect((): number => source.integer(maximum)).toThrow(RangeError);
    }
    expect((): never => source.pick([])).toThrow(RangeError);
  });
});

describe("entropy boundary", () => {
  it("creates valid reported seeds and ready-to-inject sources", () => {
    const seed = randomSeed();
    expect(Number.isInteger(seed)).toBe(true);
    expect(seed).toBeGreaterThanOrEqual(0);
    expect(seed).toBeLessThan(0x1_0000_0000);

    const source = createRandom();
    expect(source.seed).toBeGreaterThanOrEqual(0);
    expect(source.next()).toBeGreaterThanOrEqual(0);
    expect(source.next()).toBeLessThan(1);
  });
});
