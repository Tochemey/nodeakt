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
 * Internal operational constants for the partitioned key/value store.
 *
 * Public clustering configuration is limited to partition count, replica count,
 * replication mode, read and write quorums, and minimum member quorum. Every
 * value here is an internal control, exported so tests can assert the contract.
 *
 * @internal
 */

/**
 * Default partition count, sized at roughly ten times a moderate maximum node
 * count. Immutable after cluster formation, because changing it re-hashes every
 * key. Need not be prime: the avalanche finisher already diffuses the low bits
 * that the modulo reads.
 *
 * @internal
 */
export const DEFAULT_PARTITION_COUNT: number = 512;

/**
 * Bounded-load ceiling multiplier applied to `partitionCount / memberCount`.
 * A member already at or above this load is skipped during the clockwise walk.
 *
 * @internal
 */
export const LOAD_FACTOR: number = 1.25;

/** Virtual ring positions derived from each member name. @internal */
export const RING_POINTS_PER_MEMBER: number = 20;

/** Default replica count: the primary plus two backups. @internal */
export const DEFAULT_REPLICA_COUNT: number = 3;

/**
 * Default write quorum. A write is acknowledged only after a second replica
 * holds it, so one node loss cannot drop an acknowledged write.
 *
 * @internal
 */
export const DEFAULT_WRITE_QUORUM: number = 2;

/**
 * Default read quorum. The primary is the single writer and is authoritative
 * after reconcile, so a primary-local read is sufficient.
 *
 * @internal
 */
export const DEFAULT_READ_QUORUM: number = 1;

/**
 * Default minimum member quorum. `1` disables the split-brain resolver, which
 * is correct for single-node and development clusters. Keep-majority production
 * deployments derive their threshold from the last stable size, not this number.
 *
 * @internal
 */
export const DEFAULT_MEMBER_QUORUM: number = 1;

/**
 * How long, in milliseconds, the membership view must hold unchanged before its
 * size becomes the split-brain baseline. A partition is judged against the size
 * the cluster held before it began, so the baseline advances only once a genuine,
 * sustained topology change has settled, never off a transient flap.
 *
 * @internal
 */
export const STABLE_VIEW_QUIET_MS: number = 15_000;

/**
 * Tombstone retention, in milliseconds, and the stale-rejoin cutoff. A node
 * absent longer than this must discard its fragments and re-seed rather than
 * merge, because the tombstones that would veto its stale keys are gone.
 *
 * @internal
 */
export const TOMBSTONE_TTL_MS: number = 600_000;

/** Interval, in milliseconds, between per-partition anti-entropy ticks. @internal */
export const REPAIR_INTERVAL_MS: number = 10_000;

/** Sub-digests per partition when anti-entropy escalates past the rolling checksum. @internal */
export const REPAIR_BUCKETS: number = 64;

/**
 * Graceful-leave handoff backstop, in milliseconds. After this the leaver
 * departs regardless; remaining data is recovered as if the node had crashed.
 *
 * @internal
 */
export const LEAVE_DRAIN_TIMEOUT_MS: number = 30_000;

/** Interval, in milliseconds, at which a graceful leave polls for its partitions to finish draining. @internal */
export const DRAIN_POLL_INTERVAL_MS: number = 100;

/** Interval, in milliseconds, between coordinator table re-pushes that heal a missed push. @internal */
export const TABLE_PUSH_INTERVAL_MS: number = 60_000;

/** Per-RPC deadline, in milliseconds. @internal */
export const REQUEST_TIMEOUT_MS: number = 5_000;

/** Budget, in milliseconds, for the initial routing table plus fragment intake. @internal */
export const BOOTSTRAP_TIMEOUT_MS: number = 10_000;

/** Maximum bytes in one fragment-move chunk. @internal */
export const FRAGMENT_CHUNK_BYTES: number = 262_144;

/** Maximum entries returned in one scan page. @internal */
export const SCAN_PAGE_SIZE: number = 256;

/** Entries walked inside a partition between yields to the event loop. @internal */
export const SCAN_YIELD_EVERY: number = 1_024;

/** Interval, in milliseconds, between lazy TTL janitor sweeps. @internal */
export const JANITOR_INTERVAL_MS: number = 30_000;

/**
 * Partitions reaped per janitor sweep, sampled round-robin so a sweep costs the
 * same regardless of how many partitions the node holds. Reaping late only
 * delays memory reclaim: lazy expiry already hides an expired entry from reads,
 * and the tombstone age check refuses to reap before `TOMBSTONE_TTL_MS`.
 *
 * @internal
 */
export const JANITOR_PARTITIONS_PER_SWEEP: number = 64;

/** Maximum UTF-8 byte length of a key. @internal */
export const MAX_KEY_BYTES: number = 1_024;

/** Maximum byte length of a value. @internal */
export const MAX_VALUE_BYTES: number = 1_048_576;

/** Only protocol version the store's codec emits and accepts. @internal */
export const PROTOCOL_VERSION: number = 1;

/** Maximum UTF-8 byte length of a canonical member identity on the wire. @internal */
export const MAX_NAME_BYTES: number = 255;

/** Maximum owners recorded for one partition; a wire-safety bound on the list. @internal */
export const MAX_OWNERS_PER_PARTITION: number = 16;

/** Maximum partitions a single routing-table message may describe. @internal */
export const MAX_WIRE_PARTITIONS: number = 1_048_576;

/** Maximum entries a single fragment chunk may carry. @internal */
export const MAX_CHUNK_ENTRIES: number = 1_048_576;

/**
 * Maximum fragment moves a node runs at once, across drain, refill, and crash
 * reconcile. Capping concurrency keeps a rebalance from saturating the link; the
 * moves that do not fit queue and run as slots free, highest priority first.
 *
 * @internal
 */
export const MAX_CONCURRENT_MOVES: number = 4;
