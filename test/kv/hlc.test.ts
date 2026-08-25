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
import { compareHybrid, HybridClock } from "../../src/kv/hlc";
import type { HybridTime } from "../../src/kv/ports";

/** A physical clock whose reading the test sets by hand. */
function fixedClock(start: number): { read: () => number; set: (value: number) => void } {
  let current: number = start;
  return {
    read: (): number => current,
    set: (value: number): void => {
      current = value;
    },
  };
}

describe("compareHybrid", () => {
  it("orders by wall time, then logical, then node, and reports equality", () => {
    expect(
      compareHybrid({ wallMs: 1, logical: 0, node: "a" }, { wallMs: 2, logical: 0, node: "a" }),
    ).toBe(-1);
    expect(
      compareHybrid({ wallMs: 2, logical: 0, node: "a" }, { wallMs: 1, logical: 0, node: "a" }),
    ).toBe(1);
    expect(
      compareHybrid({ wallMs: 1, logical: 0, node: "a" }, { wallMs: 1, logical: 1, node: "a" }),
    ).toBe(-1);
    expect(
      compareHybrid({ wallMs: 1, logical: 1, node: "a" }, { wallMs: 1, logical: 0, node: "a" }),
    ).toBe(1);
    expect(
      compareHybrid({ wallMs: 1, logical: 0, node: "a" }, { wallMs: 1, logical: 0, node: "b" }),
    ).toBe(-1);
    expect(
      compareHybrid({ wallMs: 1, logical: 0, node: "b" }, { wallMs: 1, logical: 0, node: "a" }),
    ).toBe(1);
    expect(
      compareHybrid({ wallMs: 1, logical: 0, node: "a" }, { wallMs: 1, logical: 0, node: "a" }),
    ).toBe(0);
  });
});

describe("HybridClock", () => {
  it("rejects an empty node name", () => {
    expect((): HybridClock => new HybridClock("", (): number => 0)).toThrow(RangeError);
  });

  it("stamps its own node and starts at the physical reading", () => {
    const clock: HybridClock = new HybridClock("n1", (): number => 1_000);
    const first: HybridTime = clock.now();
    expect(first).toEqual({ wallMs: 1_000, logical: 0, node: "n1" });
  });

  it("bumps the logical counter while physical time is stuck", () => {
    const physical: { read: () => number; set: (value: number) => void } = fixedClock(1_000);
    const clock: HybridClock = new HybridClock("n1", physical.read);
    const a: HybridTime = clock.now();
    const b: HybridTime = clock.now();
    const c: HybridTime = clock.now();
    expect(a).toEqual({ wallMs: 1_000, logical: 0, node: "n1" });
    expect(b).toEqual({ wallMs: 1_000, logical: 1, node: "n1" });
    expect(c).toEqual({ wallMs: 1_000, logical: 2, node: "n1" });
    expect(compareHybrid(a, b)).toBe(-1);
    expect(compareHybrid(b, c)).toBe(-1);
  });

  it("resets the logical counter when physical time advances", () => {
    const physical: { read: () => number; set: (value: number) => void } = fixedClock(1_000);
    const clock: HybridClock = new HybridClock("n1", physical.read);
    clock.now();
    clock.now();
    physical.set(2_000);
    expect(clock.now()).toEqual({ wallMs: 2_000, logical: 0, node: "n1" });
  });

  it("never moves wall time backward when the physical clock steps back", () => {
    const physical: { read: () => number; set: (value: number) => void } = fixedClock(2_000);
    const clock: HybridClock = new HybridClock("n1", physical.read);
    const forward: HybridTime = clock.now();
    physical.set(500);
    const afterStepBack: HybridTime = clock.now();
    expect(afterStepBack).toEqual({ wallMs: 2_000, logical: 1, node: "n1" });
    expect(compareHybrid(forward, afterStepBack)).toBe(-1);
  });

  it("adopts a higher remote wall and orders after it", () => {
    const clock: HybridClock = new HybridClock("n1", (): number => 1_000);
    clock.now();
    const merged: HybridTime = clock.update({ wallMs: 5_000, logical: 7, node: "n2" });
    expect(merged).toEqual({ wallMs: 5_000, logical: 8, node: "n1" });
  });

  it("takes the larger logical plus one when both walls equal the merged wall", () => {
    const physical: { read: () => number; set: (value: number) => void } = fixedClock(1_000);
    const clock: HybridClock = new HybridClock("n1", physical.read);
    clock.now();
    clock.now();
    const merged: HybridTime = clock.update({ wallMs: 1_000, logical: 5, node: "n2" });
    expect(merged).toEqual({ wallMs: 1_000, logical: 6, node: "n1" });
  });

  it("keeps its own wall and bumps when the remote is behind", () => {
    const physical: { read: () => number; set: (value: number) => void } = fixedClock(3_000);
    const clock: HybridClock = new HybridClock("n1", physical.read);
    clock.now();
    clock.now();
    const merged: HybridTime = clock.update({ wallMs: 1_000, logical: 9, node: "n2" });
    expect(merged).toEqual({ wallMs: 3_000, logical: 2, node: "n1" });
  });

  it("resets logical when physical time is ahead of both walls on merge", () => {
    const physical: { read: () => number; set: (value: number) => void } = fixedClock(1_000);
    const clock: HybridClock = new HybridClock("n1", physical.read);
    clock.now();
    physical.set(9_000);
    const merged: HybridTime = clock.update({ wallMs: 2_000, logical: 4, node: "n2" });
    expect(merged).toEqual({ wallMs: 9_000, logical: 0, node: "n1" });
  });

  it("stays strictly monotonic across interleaved local and remote events", () => {
    const physical: { read: () => number; set: (value: number) => void } = fixedClock(1_000);
    const clock: HybridClock = new HybridClock("n1", physical.read);
    const seen: HybridTime[] = [clock.now(), clock.now()];
    seen.push(clock.update({ wallMs: 500, logical: 3, node: "n2" }));
    physical.set(1_500);
    seen.push(clock.now());
    seen.push(clock.update({ wallMs: 4_000, logical: 1, node: "n2" }));
    for (let index = 1; index < seen.length; index += 1) {
      expect(compareHybrid(seen[index - 1] as HybridTime, seen[index] as HybridTime)).toBe(-1);
    }
  });

  it("rejects a non-finite physical reading rather than corrupting its state", () => {
    expect((): HybridTime => new HybridClock("n1", (): number => Number.NaN).now()).toThrow(
      RangeError,
    );
    expect(
      (): HybridTime => new HybridClock("n1", (): number => Number.POSITIVE_INFINITY).now(),
    ).toThrow(RangeError);
  });
});
