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
import { KeepMajorityResolver, type SplitBrainStrategy } from "../../src/kv/resolver";

/** An enabled keep-majority strategy; the numeric quorum only toggles it on. */
const enabled: KeepMajorityResolver = new KeepMajorityResolver(2);

function reach(...names: string[]): Set<string> {
  return new Set<string>(names);
}

describe("KeepMajorityResolver disabled", () => {
  it("keeps every half when the quorum is one", () => {
    const off: KeepMajorityResolver = new KeepMajorityResolver(1);
    expect(off.survives(reach("a"), ["a", "b", "c", "d", "e"])).toBe(true);
    expect(off.survives(reach(), ["a", "b"])).toBe(true);
  });

  it("keeps every half when there is no stable baseline yet", () => {
    expect(enabled.survives(reach("a"), [])).toBe(true);
  });

  it("defaults to the disabled quorum", () => {
    const preset: SplitBrainStrategy = new KeepMajorityResolver();
    expect(preset.survives(reach("a"), ["a", "b", "c"])).toBe(true);
  });
});

describe("KeepMajorityResolver majority", () => {
  it("keeps a strict majority and stops a strict minority", () => {
    const stable: string[] = ["old", "a", "b", "c", "d"];
    expect(enabled.survives(reach("old", "a", "b"), stable)).toBe(true);
    expect(enabled.survives(reach("c", "d"), stable)).toBe(false);
  });

  it("counts only members that were in the last stable view", () => {
    const stable: string[] = ["a", "b", "c"];
    expect(enabled.survives(reach("a", "joined", "other"), stable)).toBe(false);
    expect(enabled.survives(reach("a", "b", "joined"), stable)).toBe(true);
  });
});

describe("KeepMajorityResolver even split", () => {
  it("keeps the half that reaches the oldest member and stops the other", () => {
    const stable: string[] = ["old", "a", "b", "c"];
    expect(enabled.survives(reach("old", "a"), stable)).toBe(true);
    expect(enabled.survives(reach("b", "c"), stable)).toBe(false);
  });
});

describe("KeepMajorityResolver construction", () => {
  it("rejects a non-positive or non-integer quorum", () => {
    expect((): KeepMajorityResolver => new KeepMajorityResolver(0)).toThrow(RangeError);
    expect((): KeepMajorityResolver => new KeepMajorityResolver(-1)).toThrow(RangeError);
    expect((): KeepMajorityResolver => new KeepMajorityResolver(1.5)).toThrow(RangeError);
  });
});
