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
 * The single-node engine: the full operation set served against the local store
 * through one serialized pipeline per partition.
 *
 * A write enters its partition's pipeline, where the condition is evaluated, a
 * fresh hybrid timestamp and a per-partition sequence number are assigned, and
 * the entry is merged into the store, one operation at a time. Because Node has
 * no preemption, that critical section is already atomic; the pipeline serves
 * two other ends. It orders operations on a partition so the sequence numbers
 * are monotone, and it is the structure the replication layer extends to wait on
 * backups after the local decision, without holding the decision across the wait.
 *
 * Reads take the fast path: a read is answered from the store directly, since on
 * a single node the local fragment is authoritative and already consistent.
 *
 * @internal
 */

import { REPAIR_BUCKETS } from "./constants";
import { PutCondition, RejectionReason, WriteKind } from "./discriminants";
import type { DigestLanes } from "./entry";
import { isLiveValue } from "./entry";
import { HybridClock } from "./hlc";
import type {
  CompareAndSetOp,
  Entry,
  IncrementOp,
  KeyVersion,
  PutOp,
  WriteApplied,
  WriteOp,
  WriteResult,
} from "./ports";
import { Store } from "./store";

/** Reads a stored counter, requiring the invariant eight-byte encoding. */
function readCounter(value: Uint8Array): bigint {
  if (value.length !== 8) {
    throw new TypeError("increment target is not an eight-byte counter");
  }

  return new DataView(value.buffer, value.byteOffset, value.byteLength).getBigInt64(0);
}

/** Encodes a signed counter as eight big-endian bytes. */
function encodeCounter(value: bigint): Uint8Array {
  const bytes: Uint8Array = new Uint8Array(8);
  new DataView(bytes.buffer).setBigInt64(0, value);
  return bytes;
}

/** Byte-for-byte equality of two payloads. */
function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) {
    return false;
  }

  for (let index: number = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }

  return true;
}

/**
 * One partition's serialized pipeline: a promise chain that runs tasks in
 * submission order and a monotone per-partition sequence counter.
 *
 * A task's rejection settles that task's caller but never breaks the chain, so a
 * failed operation does not stall the partition.
 *
 * @internal
 */
export class PartitionPipeline {
  /** Resolves after the last queued task settles; the next task chains onto it. */
  #tail: Promise<unknown> = Promise.resolve();

  /** Last sequence number handed out for this partition. */
  #sequence: bigint = 0n;

  /**
   * Queues `task` after every task already submitted and returns its result.
   *
   * Advancing the chain over a swallowed copy is what keeps a failed task from
   * stalling the partition, and it also marks the returned promise handled.
   * Callers therefore must await the result: a rejection is delivered to an
   * awaiter but is deliberately not raised as an unhandled rejection, so a
   * misused fire-and-forget write cannot crash the host process.
   */
  run<T>(task: () => T): Promise<T> {
    const result: Promise<T> = this.#tail.then(task);
    this.#tail = result.catch((): undefined => undefined);
    return result;
  }

  /** The next per-partition sequence number, starting at one. */
  nextSequence(): bigint {
    this.#sequence += 1n;
    return this.#sequence;
  }
}

/**
 * The node-local operation server for the partitioned store.
 *
 * @internal
 */
export class Engine {
  /** The fragments this node holds, one partition at a time. */
  readonly #store: Store;

  /** Hybrid clock stamping every local write with its last-write-wins order. */
  readonly #clock: HybridClock;

  /** Injected physical clock in epoch milliseconds, for expiry and the hybrid clock. */
  readonly #physicalNow: () => number;

  /** Serialized pipelines, created on first write to a partition. */
  readonly #pipelines: Map<number, PartitionPipeline> = new Map();

  /**
   * Creates an engine for one node over a fixed partition count.
   *
   * @param node Non-empty canonical identity stamped onto every write.
   * @param partitionCount Immutable positive partition count.
   * @param physicalNow Current epoch time in milliseconds.
   * @throws {RangeError} If `node` is empty or `partitionCount` is not positive.
   */
  constructor(node: string, partitionCount: number, physicalNow: () => number) {
    this.#store = new Store(partitionCount);
    this.#clock = new HybridClock(node, physicalNow);
    this.#physicalNow = physicalNow;
  }

  /** The live, unexpired value for `key`, or `undefined`. The fast read path. */
  async read(key: string): Promise<Entry | undefined> {
    return this.#store.get(key, this.#physicalNow());
  }

  /** Partition id `key` maps onto, the same mapping every node uses. */
  partitionFor(key: string): number {
    return this.#store.partitionFor(key);
  }

  /** The ids of the partitions this node currently holds, for its ownership report. */
  heldPartitions(): readonly number[] {
    return this.#store.heldPartitions();
  }

  /** Reaps expired entries across a sample of held partitions and returns the count removed. */
  sweep(nowMs: number): number {
    return this.#store.sweep(nowMs);
  }

  /** Current injected epoch time in milliseconds, for expiry decisions above the store. */
  now(): number {
    return this.#physicalNow();
  }

  /**
   * The raw stored entry for `key`, tombstone or expired included, or
   * `undefined`. A cross-owner read compares raw timestamps, so it peeks rather
   * than reading and filters the winner itself.
   */
  peek(key: string): Entry | undefined {
    return this.#store.peek(key);
  }

  /** Every stored entry for `partition`, tombstones included, for reconcile. */
  snapshot(partition: number): Entry[] {
    return this.#store.snapshot(partition);
  }

  /** The rolling digest of `partition`, all-zero when this node holds no data for it. */
  partitionDigest(partition: number): DigestLanes {
    return this.#store.digest(partition) ?? { hi: 0, lo: 0 };
  }

  /** The per-repair-bucket digests of `partition`, for anti-entropy escalation. */
  bucketDigests(partition: number): DigestLanes[] {
    return this.#store.bucketDigests(partition, REPAIR_BUCKETS);
  }

  /** Key and last-write-wins order of every entry of `partition` in the given repair `buckets`. */
  keyVersions(partition: number, buckets: ReadonlySet<number>): KeyVersion[] {
    return this.#store.keyVersions(partition, buckets, REPAIR_BUCKETS);
  }

  /** The stored entries for `keys` in `partition`, tombstones included, for an anti-entropy pull. */
  entriesFor(partition: number, keys: ReadonlySet<string>): Entry[] {
    return this.#store.entriesFor(partition, keys);
  }

  /**
   * Merges a peer's already-stamped entry into the local store under last write
   * wins. This is the backup intake path: the entry keeps the timestamp and
   * sequence the primary assigned, so it is never restamped or resequenced.
   */
  merge(entry: Entry): void {
    this.#store.apply(entry);
  }

  /**
   * Discards this node's fragment of `partition`, for a handoff the receiver has
   * acknowledged or a stale rejoin that must re-seed. See {@link Store.drop}.
   */
  drop(partition: number): void {
    this.#store.drop(partition);
  }

  /** Submits `op` to its partition's pipeline and resolves with the outcome. */
  write(op: WriteOp): Promise<WriteResult> {
    const pipeline: PartitionPipeline = this.#pipelineFor(this.#store.partitionFor(op.key));
    return pipeline.run((): WriteResult => this.#apply(op, pipeline));
  }

  /** Returns the partition's pipeline, creating it on first use. */
  #pipelineFor(id: number): PartitionPipeline {
    const existing: PartitionPipeline | undefined = this.#pipelines.get(id);
    if (existing !== undefined) {
      return existing;
    }

    const created: PartitionPipeline = new PartitionPipeline();
    this.#pipelines.set(id, created);
    return created;
  }

  /** Evaluates `op` against current state and applies it, inside the pipeline. */
  #apply(op: WriteOp, pipeline: PartitionPipeline): WriteResult {
    const nowMs: number = this.#physicalNow();
    const current: Entry | undefined = this.#liveValue(op.key, nowMs);
    if (op.kind === WriteKind.put) {
      return this.#applyPut(op, current, nowMs, pipeline);
    }

    if (op.kind === WriteKind.delete) {
      return this.#commit(op.key, undefined, undefined, pipeline);
    }

    if (op.kind === WriteKind.increment) {
      return this.#applyIncrement(op, current, pipeline);
    }

    return this.#applyCompareAndSet(op, current, pipeline);
  }

  /** The live, unexpired value entry for `key`, or `undefined`. */
  #liveValue(key: string, nowMs: number): Entry | undefined {
    const existing: Entry | undefined = this.#store.peek(key);
    return existing !== undefined && isLiveValue(existing, nowMs) ? existing : undefined;
  }

  /** Applies a put after checking its presence condition. */
  #applyPut(
    op: PutOp,
    current: Entry | undefined,
    nowMs: number,
    pipeline: PartitionPipeline,
  ): WriteResult {
    if (op.condition === PutCondition.ifAbsent && current !== undefined) {
      return { applied: false, reason: RejectionReason.ifAbsent };
    }

    if (op.condition === PutCondition.ifPresent && current === undefined) {
      return { applied: false, reason: RejectionReason.ifPresent };
    }

    const expiresAt: number | undefined = op.ttlMs !== undefined ? nowMs + op.ttlMs : undefined;
    return this.#commit(op.key, op.value, expiresAt, pipeline);
  }

  /** Adds `delta` to the current counter, treating an absent key as zero. */
  #applyIncrement(
    op: IncrementOp,
    current: Entry | undefined,
    pipeline: PartitionPipeline,
  ): WriteApplied {
    const base: bigint = current === undefined ? 0n : readCounter(current.value as Uint8Array);
    const next: bigint = BigInt.asIntN(64, base + op.delta);
    return this.#commit(op.key, encodeCounter(next), current?.expiresAt, pipeline);
  }

  /** Applies a compare-and-set only when the live payload equals `expected`. */
  #applyCompareAndSet(
    op: CompareAndSetOp,
    current: Entry | undefined,
    pipeline: PartitionPipeline,
  ): WriteResult {
    if (current === undefined || !bytesEqual(current.value as Uint8Array, op.expected)) {
      return { applied: false, reason: RejectionReason.compareAndSet };
    }

    return this.#commit(op.key, op.value, current.expiresAt, pipeline);
  }

  /**
   * Stamps a fresh timestamp and sequence, merges the entry, and reports it. A
   * `value` of `undefined` writes a tombstone, keeping the delete invariant.
   */
  #commit(
    key: string,
    value: Uint8Array | undefined,
    expiresAt: number | undefined,
    pipeline: PartitionPipeline,
  ): WriteApplied {
    const entry: Entry = {
      key,
      value,
      timestamp: this.#clock.now(),
      sequence: pipeline.nextSequence(),
      expiresAt,
      deleted: value === undefined,
    };
    this.#store.apply(entry);
    return { applied: true, entry };
  }
}
