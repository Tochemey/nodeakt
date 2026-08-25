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

import { performance } from "node:perf_hooks";

/**
 * An opaque scheduled callback owned by the {@link Clock} that created it.
 *
 * A handle is an identity token, not a promise that the callback is still
 * pending. Callers must pass it only to the creating clock.
 *
 * @internal
 */
export interface ClockTimer {
  /**
   * Whether {@link Clock.cancel} has made the callback ineligible to run.
   *
   * Completion alone does not set this flag: a callback that ran normally
   * remains uncancelled.
   */
  readonly cancelled: boolean;
}

/**
 * Time and one-shot timer dependency used by membership protocol code.
 *
 * All values are milliseconds in one clock-local time domain. `now()` and
 * timer deadlines share one monotonic domain used for every protocol deadline;
 * `epochMilliseconds()` supplies the separate Unix wall-time domain carried in
 * wire records but never used for ordering or expiry.
 *
 * @internal
 */
export interface Clock {
  /**
   * Reads the current clock-local timestamp in milliseconds.
   *
   * Protocol arithmetic expects finite safe-integer readings that never move
   * backward. Suspicion deadlines, probe windows, and retention all assume this
   * monotonic guarantee.
   */
  now(): number;

  /**
   * Reads Unix wall time in milliseconds for transported state-change stamps.
   *
   * This value may step in either direction when the system clock is adjusted;
   * the protocol never uses it for ordering, deadlines, or retention.
   */
  epochMilliseconds(): number;

  /**
   * Enqueues a one-shot callback for no earlier than `delayMs` milliseconds
   * after the current clock reading.
   *
   * A zero delay is still queued rather than invoked inline. The returned
   * handle remains owned by this clock.
   *
   * @param delayMs Finite, non-negative delay in milliseconds.
   * @param callback Callback to invoke at most once unless cancelled first.
   * @returns A handle that can be passed to {@link cancel}.
   * @throws {RangeError} If the implementation rejects the delay.
   */
  schedule(delayMs: number, callback: () => void): ClockTimer;

  /**
   * Prevents a timer owned by this clock from invoking its callback.
   *
   * Cancellation is idempotent, including after normal completion. Passing a
   * handle from another clock is outside the contract.
   *
   * @param timer Handle returned by this clock's {@link schedule}.
   */
  cancel(timer: ClockTimer): void;
}

/** Mutable bookkeeping for one host `setTimeout` registration. */
interface WallTimer extends ClockTimer {
  /** Records explicit cancellation independently of completion. */
  cancelled: boolean;
  /** Host handle while queued; cleared after firing or cancellation. */
  handle: ReturnType<typeof setTimeout> | undefined;
}

/**
 * Longest delay Node honors in one `setTimeout`; larger values are clamped by
 * the host to 1 ms, so they must be chained instead.
 */
const MAX_HOST_TIMER_DELAY_MS = 2 ** 31 - 1;

/**
 * Enforces the delay domain accepted by {@link WallClock.schedule}.
 *
 * @throws {RangeError} If `delayMs` is negative, infinite, or `NaN`.
 */
function validateDelay(delayMs: number): void {
  if (!Number.isFinite(delayMs) || delayMs < 0) {
    throw new RangeError("delay must be a finite non-negative number");
  }
}

/**
 * Allows a Node-compatible timer to stop keeping the process alive.
 *
 * Browser-style numeric handles and object handles without `unref` are left
 * unchanged.
 */
function unref(handle: ReturnType<typeof setTimeout>): void {
  if (typeof handle === "object" && handle !== null && "unref" in handle) {
    (handle as { unref(): void }).unref();
  }
}

/**
 * Host-backed clock using monotonic `performance.now()` readings, Unix
 * `Date.now()` epoch stamps, and one-shot `setTimeout` timers.
 *
 * Timers are unreferenced when the host exposes Node's `unref()`, so a pending
 * membership timer alone does not keep the process alive. Protocol components
 * should accept a {@link Clock} dependency so tests can substitute virtual
 * time.
 *
 * @internal
 */
export class WallClock implements Clock {
  /**
   * Reads monotonic host time in whole milliseconds from `performance.now()`.
   *
   * The reading never moves backward while the process runs, so protocol
   * deadlines survive system-clock adjustments.
   */
  now(): number {
    return Math.trunc(performance.now());
  }

  /**
   * Reads Unix wall time in milliseconds from `Date.now()`.
   *
   * The value can move backward or forward when the system clock is adjusted;
   * it is carried in wire records only.
   */
  epochMilliseconds(): number {
    return Date.now();
  }

  /**
   * Registers a one-shot callback on the host timer queue.
   *
   * The callback is never called inline. Cancellation also guards against a
   * host callback that was already queued when `clearTimeout` ran. Exceptions
   * thrown by `callback` propagate from the host timer turn. Delays beyond the
   * host's 32-bit signed-millisecond ceiling are chained across successive host
   * timers instead of being silently clamped to 1 ms.
   *
   * @param delayMs Finite, non-negative delay in milliseconds.
   * @param callback Callback to invoke once after the host delay.
   * @returns A handle owned by this clock; normal completion leaves its
   * `cancelled` flag false.
   * @throws {RangeError} If `delayMs` is negative, infinite, or `NaN`.
   */
  schedule(delayMs: number, callback: () => void): ClockTimer {
    validateDelay(delayMs);
    const timer: WallTimer = { cancelled: false, handle: undefined };
    this.#arm(timer, delayMs, callback);
    return timer;
  }

  /** Arms one host timer slice, chaining any remainder beyond the host ceiling. */
  #arm(timer: WallTimer, remainingMs: number, callback: () => void): void {
    const slice = Math.min(remainingMs, MAX_HOST_TIMER_DELAY_MS);
    timer.handle = setTimeout((): void => {
      if (timer.cancelled) {
        return;
      }

      const remainder = remainingMs - slice;
      if (remainder > 0) {
        this.#arm(timer, remainder, callback);
        return;
      }

      timer.handle = undefined;
      callback();
    }, slice);
    unref(timer.handle);
  }

  /**
   * Cancels a handle created by a `WallClock`.
   *
   * The first call marks the handle cancelled and clears a pending host timer;
   * later calls have no effect. Cancelling after completion still marks the
   * handle but has no host timer to clear.
   *
   * @param timer Handle returned by a compatible `WallClock`.
   */
  cancel(timer: ClockTimer): void {
    const wallTimer = timer as WallTimer;
    if (wallTimer.cancelled) {
      return;
    }

    wallTimer.cancelled = true;
    if (wallTimer.handle !== undefined) {
      clearTimeout(wallTimer.handle);
      wallTimer.handle = undefined;
    }
  }
}

/**
 * Shared stateless wall-clock dependency for production membership engines.
 *
 * Timer ownership remains per returned handle; sharing this instance does not
 * couple otherwise independent timers.
 *
 * @internal
 */
export const wallClock: Clock = new WallClock();

/**
 * One-shot deadline whose backing timer is released as soon as a caller
 * disposes it, rather than lingering until it fires.
 *
 * @internal The signal does not cancel I/O work by itself; each caller owns
 * timeout cleanup and any required resource destruction.
 */
export interface Deadline {
  /** Signal aborted when the millisecond deadline elapses before disposal. */
  readonly signal: AbortSignal;

  /** Idempotently clears the pending timer so a settled operation frees it at once. */
  dispose(): void;
}

/**
 * Creates a one-shot abort deadline for a positive millisecond duration.
 *
 * This module owns the ambient host timer so protocol code stays free of
 * direct `setTimeout` use.
 *
 * @internal Unlike `AbortSignal.timeout`, the underlying timer is cleared on
 * {@link Deadline.dispose}, so fast operations do not leave it pending.
 */
export function timedSignal(milliseconds: number): Deadline {
  const controller = new AbortController();
  const handle = setTimeout((): void => controller.abort(), milliseconds);
  unref(handle);
  return {
    signal: controller.signal,
    dispose(): void {
      clearTimeout(handle);
    },
  };
}
