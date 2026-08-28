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
 * Partition hashing: FNV-1a over UTF-8 bytes, finished with a 32-bit avalanche.
 *
 * The mix is required. Raw FNV-1a has weak low-bit diffusion and the partition
 * modulo reads exactly those bits; after mixing, any positive partition count
 * distributes evenly and a prime count is not required. UTF-8 bytes, not UTF-16
 * code units, so a key hashes identically across runtimes and a supplementary
 * character does not hash as two surrogates.
 *
 * @internal
 */

/** FNV-1a 32-bit offset basis. */
const FNV_OFFSET: number = 0x811c9dc5;

/** FNV-1a 32-bit prime. */
const FNV_PRIME: number = 0x01000193;

/** Shared UTF-8 encoder; hashing never mutates its output. */
const encoder: TextEncoder = new TextEncoder();

/**
 * FNV-1a 32-bit over the given bytes, without the avalanche mix.
 *
 * @internal
 */
export function fnv1a32(bytes: Uint8Array): number {
  let hash: number = FNV_OFFSET;
  for (let index = 0; index < bytes.length; index += 1) {
    hash ^= bytes[index] as number;
    hash = Math.imul(hash, FNV_PRIME);
  }

  return hash >>> 0;
}

/**
 * MurmurHash3 32-bit finalizer. Diffuses entropy into the low bits that a
 * power-of-two modulo would otherwise read unchanged.
 *
 * @internal
 */
export function mix32(hash: number): number {
  let mixed: number = hash >>> 0;
  mixed ^= mixed >>> 16;
  mixed = Math.imul(mixed, 0x85ebca6b);
  mixed ^= mixed >>> 13;
  mixed = Math.imul(mixed, 0xc2b2ae35);
  mixed ^= mixed >>> 16;
  return mixed >>> 0;
}

/**
 * Mixes FNV-1a of `value`'s UTF-8 bytes into an unsigned 32-bit hash.
 *
 * @internal
 */
export function hash32(value: string): number {
  return mix32(fnv1a32(encoder.encode(value)));
}

/**
 * Maps `key` onto `[0, partitionCount)`.
 *
 * @throws {RangeError} If `partitionCount` is not a positive safe integer.
 * @internal
 */
export function partitionId(key: string, partitionCount: number): number {
  if (!Number.isSafeInteger(partitionCount) || partitionCount <= 0) {
    throw new RangeError("partition count must be a positive integer");
  }

  return hash32(key) % partitionCount;
}
