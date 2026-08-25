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

import { afterEach, describe, expect, it, vi } from "vitest";
import { WallClock, wallClock } from "../../src/membership/clock";

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("wall clock", () => {
  it("reports wall time and exposes a shared implementation", () => {
    expect(new WallClock().now()).toBeGreaterThan(0);
    expect(wallClock.now()).toBeGreaterThan(0);
  });

  it("schedules callbacks and marks completed timers active", () => {
    vi.useFakeTimers();
    const clock = new WallClock();
    const callback = vi.fn();
    const timer = clock.schedule(25, callback);

    vi.advanceTimersByTime(24);
    expect(callback).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(callback).toHaveBeenCalledOnce();
    expect(timer.cancelled).toBe(false);
  });

  it("cancels idempotently before and after deadlines", () => {
    vi.useFakeTimers();
    const clock = new WallClock();
    const cancelled = vi.fn();
    const timer = clock.schedule(10, cancelled);

    clock.cancel(timer);
    clock.cancel(timer);
    vi.advanceTimersByTime(20);
    expect(cancelled).not.toHaveBeenCalled();
    expect(timer.cancelled).toBe(true);

    const completed = clock.schedule(1, vi.fn());
    vi.advanceTimersByTime(1);
    expect((): void => {
      clock.cancel(completed);
      clock.cancel(completed);
    }).not.toThrow();
  });

  it("unrefs native handles and clears them only once", () => {
    const unref = vi.fn();
    const handle = { unref } as unknown as ReturnType<typeof setTimeout>;
    vi.spyOn(globalThis, "setTimeout").mockReturnValue(handle);
    const clear = vi.spyOn(globalThis, "clearTimeout").mockImplementation((): void => {});
    const clock = new WallClock();

    const timer = clock.schedule(10, vi.fn());
    expect(unref).toHaveBeenCalledOnce();
    clock.cancel(timer);
    clock.cancel(timer);
    expect(clear).toHaveBeenCalledOnce();
    expect(clear).toHaveBeenCalledWith(handle);
  });

  it("guards a cancelled callback that was already queued", () => {
    let queued: (() => void) | undefined;
    const handle = { unref: vi.fn() } as unknown as ReturnType<typeof setTimeout>;
    vi.spyOn(globalThis, "setTimeout").mockImplementation(((
      callback: () => void,
    ): ReturnType<typeof setTimeout> => {
      queued = callback;
      return handle;
    }) as typeof setTimeout);
    vi.spyOn(globalThis, "clearTimeout").mockImplementation((): void => {});
    const callback = vi.fn();
    const clock = new WallClock();
    const timer = clock.schedule(10, callback);

    clock.cancel(timer);
    queued?.();
    expect(callback).not.toHaveBeenCalled();
  });

  it("supports timer handles without an unref method", () => {
    const handle = 7 as unknown as ReturnType<typeof setTimeout>;
    vi.spyOn(globalThis, "setTimeout").mockReturnValue(handle);
    const clock = new WallClock();

    expect(clock.schedule(10, vi.fn()).cancelled).toBe(false);
  });

  it("rejects invalid delays", () => {
    const clock = new WallClock();
    expect((): void => {
      clock.schedule(-1, vi.fn());
    }).toThrow(RangeError);
    expect((): void => {
      clock.schedule(Number.POSITIVE_INFINITY, vi.fn());
    }).toThrow(RangeError);
  });
});
