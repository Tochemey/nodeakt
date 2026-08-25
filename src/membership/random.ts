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

import { randomBytes } from "node:crypto";

/** Number of distinct unsigned 32-bit values and exclusive seed upper bound. */
const UINT32_RANGE = 0x1_0000_0000;
/** Nonzero xorshift32 state substituted for the absorbing all-zero state. */
const ZERO_SEED_STATE = 0x6d2b_79f5;

/**
 * Randomness dependency used for membership protocol choices.
 *
 * Implementations define whether a sequence is reproducible. Methods consume
 * source state, so call order is part of deterministic behavior.
 *
 * @internal
 */
export interface Random {
  /**
   * Draws a floating-point sample in the half-open interval `[0, 1)`.
   *
   * @returns A value greater than or equal to zero and strictly less than one.
   */
  next(): number;

  /**
   * Draws a uniformly distributed integer from `[0, maximumExclusive)`.
   *
   * @param maximumExclusive Positive integer upper bound, at most `2^32`.
   * @returns An integer greater than or equal to zero and below the bound.
   * @throws {RangeError} If the bound is not an integer in `[1, 2^32]`.
   */
  integer(maximumExclusive: number): number;

  /**
   * Draws one collection element with equal probability per index.
   *
   * Repeated references at different indices therefore receive proportionally
   * more probability.
   *
   * @param values Non-empty collection to sample without mutating.
   * @returns The element stored at the selected index.
   * @throws {RangeError} If `values` is empty.
   */
  pick<T>(values: readonly T[]): T;

  /**
   * Produces a uniformly distributed permutation using Fisher-Yates.
   *
   * @param values Collection whose elements are copied by reference.
   * @returns A new mutable array; the input and its ordering are unchanged.
   */
  shuffle<T>(values: readonly T[]): T[];
}

/**
 * Validates the externally visible seed rather than internal generator state.
 *
 * @throws {RangeError} If `seed` is not an unsigned 32-bit integer.
 */
function validateSeed(seed: number): void {
  if (!Number.isInteger(seed) || seed < 0 || seed >= UINT32_RANGE) {
    throw new RangeError("seed must be an unsigned 32-bit integer");
  }
}

/**
 * Validates a bound that can be sampled from one unsigned 32-bit draw.
 *
 * @throws {RangeError} If the bound is not an integer in `[1, 2^32]`.
 */
function validateMaximum(maximumExclusive: number): void {
  if (
    !Number.isInteger(maximumExclusive) ||
    maximumExclusive <= 0 ||
    maximumExclusive > UINT32_RANGE
  ) {
    throw new RangeError("maximum must be an integer from 1 through 2^32");
  }
}

/**
 * Reproducible xorshift32 source for protocol choices and deterministic tests.
 *
 * This generator is fast and statistically adequate for peer ordering; it is
 * not cryptographically secure. Equal seeds and equal method-call sequences
 * produce equal results. Seed zero is preserved publicly but mapped to a fixed
 * nonzero internal state because zero is absorbing for xorshift32.
 *
 * @internal
 */
export class SeededRandom implements Random {
  /** Original unsigned 32-bit seed, including zero before state substitution. */
  readonly seed: number;

  /** Current nonzero unsigned xorshift32 state, advanced by each draw. */
  #state: number;

  /**
   * Creates a source at the beginning of the sequence identified by `seed`.
   *
   * @param seed Unsigned 32-bit integer in `[0, 2^32)`.
   * @throws {RangeError} If `seed` is fractional, non-finite, or outside the
   * unsigned 32-bit range.
   */
  constructor(seed: number) {
    validateSeed(seed);
    this.seed = seed;
    this.#state = seed === 0 ? ZERO_SEED_STATE : seed;
  }

  /**
   * Advances xorshift32 once and returns the resulting unsigned word.
   *
   * @returns An integer in `[0, 2^32)`.
   */
  #uint32(): number {
    let value = this.#state;
    value ^= value << 13;
    value ^= value >>> 17;
    value ^= value << 5;
    this.#state = value >>> 0;
    return this.#state;
  }

  /**
   * Converts the next unsigned word to a reproducible fraction.
   *
   * @returns One of `2^32` evenly spaced values in `[0, 1)`.
   */
  next(): number {
    return this.#uint32() / UINT32_RANGE;
  }

  /**
   * Draws a reproducible integer without modulo bias.
   *
   * Values in the incomplete low-end residue are rejected before applying
   * modulo, so this method may consume more than one generator word.
   *
   * @param maximumExclusive Positive integer upper bound, at most `2^32`.
   * @returns A uniform integer in `[0, maximumExclusive)`.
   * @throws {RangeError} If the bound is not an integer in `[1, 2^32]`.
   */
  integer(maximumExclusive: number): number {
    validateMaximum(maximumExclusive);
    const rejectedBelow = UINT32_RANGE % maximumExclusive;
    let value = this.#uint32();
    while (value < rejectedBelow) {
      value = this.#uint32();
    }

    return value % maximumExclusive;
  }

  /**
   * Selects an element by a reproducible uniform index draw.
   *
   * @param values Non-empty collection to sample without mutating.
   * @returns The element at the selected index.
   * @throws {RangeError} If `values` is empty.
   */
  pick<T>(values: readonly T[]): T {
    if (values.length === 0) {
      throw new RangeError("cannot pick from an empty collection");
    }

    return values[this.integer(values.length)] as T;
  }

  /**
   * Applies an in-place Fisher-Yates shuffle to a shallow copy.
   *
   * The empty and single-element cases consume no random words.
   *
   * @param values Collection to copy and permute.
   * @returns A new mutable array containing the same element references.
   */
  shuffle<T>(values: readonly T[]): T[] {
    const shuffled = Array.from(values);
    for (let index = shuffled.length - 1; index > 0; index -= 1) {
      const other = this.integer(index + 1);
      const value = shuffled[index] as T;
      shuffled[index] = shuffled[other] as T;
      shuffled[other] = value;
    }

    return shuffled;
  }
}

/**
 * Draws an unsigned 32-bit seed from Node's cryptographic entropy source.
 *
 * Reading four bytes as big-endian affects only numeric representation, not
 * entropy. This function is nondeterministic and does not create generator
 * state.
 *
 * @returns An integer in `[0, 2^32)`.
 * @throws If the platform cryptographic entropy source fails.
 * @internal
 */
export function randomSeed(): number {
  return randomBytes(4).readUInt32BE(0);
}

/**
 * Creates an independently seeded xorshift32 source from platform entropy.
 *
 * The returned source becomes deterministic once created; its public
 * {@link SeededRandom.seed} can be recorded to reproduce the sequence.
 *
 * @returns A new source initialized at the start of its sampled seed sequence.
 * @throws If the platform cryptographic entropy source fails.
 * @internal
 */
export function createRandom(): SeededRandom {
  return new SeededRandom(randomSeed());
}
