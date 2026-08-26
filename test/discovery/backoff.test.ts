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
import { ExponentialBackoff } from "../../src/discovery/backoff";

/** Collects `count` successive delays from a backoff. */
function delays(backoff: ExponentialBackoff, count: number): number[] {
  const out: number[] = [];
  for (let index: number = 0; index < count; index += 1) {
    out.push(backoff.nextDelay());
  }

  return out;
}

describe("ExponentialBackoff growth", () => {
  it("grows the ceiling exponentially and clamps it to the maximum", () => {
    const backoff: ExponentialBackoff = new ExponentialBackoff({
      baseDelayMs: 100,
      maxDelayMs: 1_000,
      multiplier: 2,
      random: (): number => 1,
    });
    expect(delays(backoff, 6)).toEqual([100, 200, 400, 800, 1_000, 1_000]);
  });

  it("applies full jitter as a fraction of the ceiling", () => {
    const half: ExponentialBackoff = new ExponentialBackoff({
      baseDelayMs: 100,
      maxDelayMs: 1_000,
      multiplier: 2,
      random: (): number => 0.5,
    });
    expect(delays(half, 4)).toEqual([50, 100, 200, 400]);

    const zero: ExponentialBackoff = new ExponentialBackoff({
      baseDelayMs: 100,
      random: (): number => 0,
    });
    expect(delays(zero, 3)).toEqual([0, 0, 0]);
  });
});

describe("ExponentialBackoff defaults", () => {
  it("is a valid production policy with no options", () => {
    const backoff: ExponentialBackoff = new ExponentialBackoff();
    const delay: number = backoff.nextDelay();
    expect(delay).toBeGreaterThanOrEqual(0);
    expect(delay).toBeLessThanOrEqual(250);
  });
});

describe("ExponentialBackoff construction", () => {
  it("rejects a base delay that is not a positive integer", () => {
    expect((): ExponentialBackoff => new ExponentialBackoff({ baseDelayMs: 0 })).toThrow(
      RangeError,
    );
    expect((): ExponentialBackoff => new ExponentialBackoff({ baseDelayMs: 1.5 })).toThrow(
      RangeError,
    );
  });

  it("rejects a maximum below the base or not an integer", () => {
    expect(
      (): ExponentialBackoff => new ExponentialBackoff({ baseDelayMs: 100, maxDelayMs: 50 }),
    ).toThrow(RangeError);
    expect(
      (): ExponentialBackoff => new ExponentialBackoff({ baseDelayMs: 100, maxDelayMs: 100.5 }),
    ).toThrow(RangeError);
  });

  it("rejects a multiplier below one or not finite", () => {
    expect((): ExponentialBackoff => new ExponentialBackoff({ multiplier: 0.5 })).toThrow(
      RangeError,
    );
    expect(
      (): ExponentialBackoff => new ExponentialBackoff({ multiplier: Number.POSITIVE_INFINITY }),
    ).toThrow(RangeError);
  });
});
