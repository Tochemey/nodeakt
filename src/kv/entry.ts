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
 * Pure predicates and the digest contribution for one stored {@link Entry}.
 *
 * Nothing here holds state or reads a clock: callers pass the current time in,
 * so the same entry and the same instant always yield the same answer. The
 * digest contribution folds key and timestamp, plus the tombstone flag as a
 * guard, because last write wins makes the timestamp identify the content: two
 * entries that share a key and a timestamp are the same write, so equal
 * contributions mean equal content.
 *
 * @internal
 */

import { TOMBSTONE_TTL_MS } from "./constants";
import { hash32, mix32 } from "./hash";
import { compareHybrid } from "./hlc";
import type { Entry } from "./ports";

/**
 * A 64-bit value carried as two unsigned 32-bit lanes. Partition digests XOR
 * these lanes with `^`, which stays on 32-bit integers and never allocates a
 * `bigint` on the write path.
 *
 * @internal
 */
export interface DigestLanes {
  /** High 32 bits. */
  readonly hi: number;
  /** Low 32 bits. */
  readonly lo: number;
}

/**
 * Whether `candidate` wins last write wins against `incumbent`. Equal
 * timestamps do not win, so re-applying an identical entry is a no-op and every
 * transfer stays idempotent.
 *
 * @internal
 */
export function supersedes(candidate: Entry, incumbent: Entry): boolean {
  return compareHybrid(candidate.timestamp, incumbent.timestamp) > 0;
}

/** Whether the entry carries an absolute expiry that `nowMs` has reached. @internal */
export function isExpired(entry: Entry, nowMs: number): boolean {
  return entry.expiresAt !== undefined && entry.expiresAt <= nowMs;
}

/** Whether the entry is a live value: not a tombstone and not expired. @internal */
export function isLiveValue(entry: Entry, nowMs: number): boolean {
  return !entry.deleted && !isExpired(entry, nowMs);
}

/**
 * Whether a tombstone has outlived `TOMBSTONE_TTL_MS` and may be reaped. The age
 * is measured from the delete's wall time, the same clock every replica reads,
 * so reaping never races ahead of the retention window.
 *
 * @internal
 */
export function isReapableTombstone(entry: Entry, nowMs: number): boolean {
  return entry.deleted && nowMs - entry.timestamp.wallMs >= TOMBSTONE_TTL_MS;
}

/** Decorrelates the repair bucket from the partition hash so the two are independent. */
const BUCKET_SEED: number = 0x2545f491;

/**
 * The repair sub-bucket a key falls in, in `[0, bucketCount)`. Anti-entropy XORs
 * each entry's contribution into its key's bucket, so two replicas that differ in
 * one key differ in one bucket. A second seed decorrelates this from the
 * partition hash, and it reads UTF-8 bytes through {@link hash32} so every node
 * assigns a key the same bucket.
 *
 * @internal
 */
export function repairBucket(key: string, bucketCount: number): number {
  return mix32((hash32(key) ^ BUCKET_SEED) >>> 0) % bucketCount;
}

/** Decorrelates the high lane from the low lane so they are not one permutation. */
const HI_SEED: number = 0x9e3779b9;

/** Separates a tombstone from a value that somehow shared its timestamp. */
const DELETED_SALT: number = 0x27d4eb2f;

/**
 * The entry's contribution to its partition's rolling digest, folding key,
 * timestamp, and the tombstone flag into two well-diffused 32-bit lanes.
 *
 * @internal
 */
export function entryContribution(entry: Entry): DigestLanes {
  const keyHash: number = hash32(entry.key);
  const wallLow: number = entry.timestamp.wallMs >>> 0;
  const wallHigh: number = Math.floor(entry.timestamp.wallMs / 0x1_0000_0000) >>> 0;
  const logical: number = entry.timestamp.logical >>> 0;
  const nodeHash: number = hash32(entry.timestamp.node);
  const salt: number = entry.deleted ? DELETED_SALT : 0;
  const lo: number = mix32(mix32(mix32((keyHash ^ wallLow ^ salt) >>> 0) ^ logical) ^ nodeHash);
  const hi: number = mix32(
    mix32(mix32((keyHash ^ HI_SEED) >>> 0) ^ wallHigh) ^ ((nodeHash ^ logical) >>> 0),
  );
  return { hi: hi >>> 0, lo: lo >>> 0 };
}
