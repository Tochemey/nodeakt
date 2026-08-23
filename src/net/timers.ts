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

/**
 * The transport's timer facility: a coarse clock and cancelable
 * deadline scheduling. The interface is deliberately tiny so an owner
 * can substitute another implementation (a shared timer wheel, a fake
 * clock in tests) without the transport importing anything.
 *
 * @internal
 */

/** A scheduled deadline; cancel is idempotent. */
export interface TimerHandle {
  cancel(): void;
}

export interface Timers {
  /**
   * The current time in coarse milliseconds. Calls within one
   * microtask burst share a single clock read, which is what the
   * per-frame paths need: many frames arriving together stamp the
   * same instant without one clock call each.
   */
  now(): number;

  /** Runs `fn` once after `delayMs`; never keeps the process alive. */
  schedule(delayMs: number, fn: () => void): TimerHandle;
}

class DefaultTimers implements Timers {
  private _cached: number = 0;
  private _fresh: boolean = false;

  now(): number {
    if (!this._fresh) {
      this._cached = Date.now();
      this._fresh = true;
      queueMicrotask((): void => {
        this._fresh = false;
      });
    }

    return this._cached;
  }

  schedule(delayMs: number, fn: () => void): TimerHandle {
    const timer: NodeJS.Timeout = setTimeout(fn, delayMs);
    if (typeof timer.unref === "function") {
      timer.unref();
    }

    return {
      cancel(): void {
        clearTimeout(timer);
      },
    };
  }
}

/** The shared default implementation. */
export const defaultTimers: Timers = new DefaultTimers();
