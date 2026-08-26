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
 * A retry-delay policy: the wait before each successive attempt.
 *
 * The one operation a retry loop needs, kept minimal so a caller can substitute
 * a deterministic policy in a test. {@link ExponentialBackoff} is the production
 * implementation.
 */
export interface Backoff {
  /**
   * The delay, in whole milliseconds, to wait before the next attempt, and then
   * advances the sequence so the following call returns the next delay.
   */
  nextDelay(): number;
}

/** Default first delay before jitter, in milliseconds. */
const DEFAULT_BASE_DELAY_MS: number = 250;

/** Default ceiling the un-jittered delay is clamped to, in milliseconds. */
const DEFAULT_MAX_DELAY_MS: number = 5_000;

/** Default growth factor applied per attempt. */
const DEFAULT_MULTIPLIER: number = 2;

/** Configuration for an {@link ExponentialBackoff}. */
export interface BackoffOptions {
  /** First delay before jitter; defaults to 250 ms. Must be a positive integer. */
  readonly baseDelayMs?: number;
  /** Ceiling the growing delay is clamped to; defaults to 5000 ms. Must be at least the base. */
  readonly maxDelayMs?: number;
  /** Growth factor per attempt; defaults to 2. Must be at least 1. */
  readonly multiplier?: number;
  /**
   * Source of a jitter sample in `[0, 1)`; defaults to `Math.random`. Injected so
   * a test is deterministic and so a caller can share one seeded source.
   */
  readonly random?: () => number;
}

/**
 * Capped exponential backoff with full jitter.
 *
 * The un-jittered ceiling grows as `base * multiplier ** attempt`, clamped to a
 * maximum, and the delay returned is a uniform sample in `[0, ceiling]`. Full
 * jitter is what keeps a synchronized cold start, every node retrying the same
 * seeds on the same schedule, from becoming a thundering herd: each node's waits
 * are independently randomized while still growing under sustained failure. The
 * sequence is stateful, so one instance backs one retry loop; construct another
 * for an independent loop.
 */
export class ExponentialBackoff implements Backoff {
  /** First delay before jitter. */
  readonly #baseDelayMs: number;
  /** Ceiling the growing delay is clamped to. */
  readonly #maxDelayMs: number;
  /** Growth factor per attempt. */
  readonly #multiplier: number;
  /** Jitter sample source in `[0, 1)`. */
  readonly #random: () => number;
  /** How many delays have been produced, the exponent applied to the multiplier. */
  #attempt: number = 0;

  /**
   * @param options Base delay, ceiling, multiplier, and jitter source; every
   * field defaults, so `new ExponentialBackoff()` is a valid production policy.
   * @throws {RangeError} If the base is not a positive integer, the ceiling is
   * below the base, or the multiplier is below one.
   */
  constructor(options: BackoffOptions = {}) {
    const baseDelayMs: number = options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS;
    const maxDelayMs: number = options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS;
    const multiplier: number = options.multiplier ?? DEFAULT_MULTIPLIER;
    if (!Number.isSafeInteger(baseDelayMs) || baseDelayMs < 1) {
      throw new RangeError("backoff base delay must be a positive integer of milliseconds");
    }

    if (!Number.isSafeInteger(maxDelayMs) || maxDelayMs < baseDelayMs) {
      throw new RangeError("backoff max delay must be an integer at least the base delay");
    }

    if (!Number.isFinite(multiplier) || multiplier < 1) {
      throw new RangeError("backoff multiplier must be at least one");
    }

    this.#baseDelayMs = baseDelayMs;
    this.#maxDelayMs = maxDelayMs;
    this.#multiplier = multiplier;
    this.#random = options.random ?? Math.random;
  }

  /**
   * The next jittered delay in whole milliseconds, a uniform sample in
   * `[0, ceiling]` where the ceiling grows exponentially up to the maximum.
   */
  nextDelay(): number {
    const grown: number = this.#baseDelayMs * this.#multiplier ** this.#attempt;
    const ceiling: number = Math.min(grown, this.#maxDelayMs);
    this.#attempt += 1;
    return Math.round(this.#random() * ceiling);
  }
}
