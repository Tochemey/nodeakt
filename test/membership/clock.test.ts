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

import { afterEach, describe, expect, it, type Mock, type MockInstance, vi } from "vitest";
import {
  type ClockTimer,
  type Deadline,
  timedSignal,
  WallClock,
  wallClock,
} from "../../src/membership/clock";

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
    const clock: WallClock = new WallClock();
    const callback: Mock<() => void> = vi.fn();
    const timer: ClockTimer = clock.schedule(25, callback);

    vi.advanceTimersByTime(24);
    expect(callback).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(callback).toHaveBeenCalledOnce();
    expect(timer.cancelled).toBe(false);
  });

  it("chains scheduled callbacks beyond the host timer ceiling", () => {
    vi.useFakeTimers();
    const clock: WallClock = new WallClock();
    const callback: Mock<() => void> = vi.fn();
    const ceiling: number = 2 ** 31 - 1;
    clock.schedule(ceiling + 5, callback);

    vi.advanceTimersByTime(ceiling);
    expect(callback).not.toHaveBeenCalled();
    vi.advanceTimersByTime(4);
    expect(callback).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(callback).toHaveBeenCalledOnce();
  });

  it("cancels idempotently before and after deadlines", () => {
    vi.useFakeTimers();
    const clock: WallClock = new WallClock();
    const cancelled: Mock<() => void> = vi.fn();
    const timer: ClockTimer = clock.schedule(10, cancelled);

    clock.cancel(timer);
    clock.cancel(timer);
    vi.advanceTimersByTime(20);
    expect(cancelled).not.toHaveBeenCalled();
    expect(timer.cancelled).toBe(true);

    const completed: ClockTimer = clock.schedule(1, vi.fn());
    vi.advanceTimersByTime(1);
    expect((): void => {
      clock.cancel(completed);
      clock.cancel(completed);
    }).not.toThrow();
  });

  it("unrefs native handles and clears them only once", () => {
    const unref: Mock<() => void> = vi.fn();
    const handle: ReturnType<typeof setTimeout> = { unref } as unknown as ReturnType<
      typeof setTimeout
    >;
    vi.spyOn(globalThis, "setTimeout").mockReturnValue(handle);
    const clear: MockInstance = vi
      .spyOn(globalThis, "clearTimeout")
      .mockImplementation((): void => {});
    const clock: WallClock = new WallClock();

    const timer: ClockTimer = clock.schedule(10, vi.fn());
    expect(unref).toHaveBeenCalledOnce();
    clock.cancel(timer);
    clock.cancel(timer);
    expect(clear).toHaveBeenCalledOnce();
    expect(clear).toHaveBeenCalledWith(handle);
  });

  it("guards a cancelled callback that was already queued", () => {
    let queued: (() => void) | undefined;
    const handle: ReturnType<typeof setTimeout> = { unref: vi.fn() } as unknown as ReturnType<
      typeof setTimeout
    >;
    vi.spyOn(globalThis, "setTimeout").mockImplementation(((
      callback: () => void,
    ): ReturnType<typeof setTimeout> => {
      queued = callback;
      return handle;
    }) as typeof setTimeout);
    vi.spyOn(globalThis, "clearTimeout").mockImplementation((): void => {});
    const callback: Mock<() => void> = vi.fn();
    const clock: WallClock = new WallClock();
    const timer: ClockTimer = clock.schedule(10, callback);

    clock.cancel(timer);
    queued?.();
    expect(callback).not.toHaveBeenCalled();
  });

  it("supports timer handles without an unref method", () => {
    const handle: ReturnType<typeof setTimeout> = 7 as unknown as ReturnType<typeof setTimeout>;
    vi.spyOn(globalThis, "setTimeout").mockReturnValue(handle);
    const clock: WallClock = new WallClock();

    expect(clock.schedule(10, vi.fn()).cancelled).toBe(false);
  });

  it("rejects invalid delays", () => {
    const clock: WallClock = new WallClock();
    expect((): void => {
      clock.schedule(-1, vi.fn());
    }).toThrow(RangeError);
    expect((): void => {
      clock.schedule(Number.POSITIVE_INFINITY, vi.fn());
    }).toThrow(RangeError);
  });
});

describe("timed signal", () => {
  it("aborts at the deadline and clears a disposed timer idempotently", () => {
    vi.useFakeTimers();
    const deadline: Deadline = timedSignal(25);
    vi.advanceTimersByTime(24);
    expect(deadline.signal.aborted).toBe(false);
    vi.advanceTimersByTime(1);
    expect(deadline.signal.aborted).toBe(true);

    const disposed: Deadline = timedSignal(10);
    disposed.dispose();
    disposed.dispose();
    vi.advanceTimersByTime(20);
    expect(disposed.signal.aborted).toBe(false);
  });

  it("chains deadlines beyond the host timer ceiling instead of clamping", () => {
    vi.useFakeTimers();
    const ceiling: number = 2 ** 31 - 1;
    const deadline: Deadline = timedSignal(ceiling + 6);
    vi.advanceTimersByTime(ceiling);
    expect(deadline.signal.aborted).toBe(false);
    vi.advanceTimersByTime(5);
    expect(deadline.signal.aborted).toBe(false);
    vi.advanceTimersByTime(1);
    expect(deadline.signal.aborted).toBe(true);
  });
});
