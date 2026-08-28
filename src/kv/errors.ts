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
 * Typed errors raised by the partitioned key/value store.
 *
 * Callers compare with `instanceof`. Condition failures (`NX`, `XX`, compare-and-set)
 * are not errors: they return as a rejected write result. These classes cover
 * the cases that cannot be answered from local store state.
 *
 * @internal
 */

/**
 * A conditional write was refused because its partition has more than one owner.
 *
 * `clustering.ts` retries this error with backoff inside the caller's timeout
 * budget. Unconditional reads and writes are still served from the owners list
 * while the partition is fragmented.
 *
 * @internal
 */
export class PartitionRebalancingError extends Error {
  /** Partition whose owners list currently has more than one member. */
  readonly partitionId: number;

  /** Captures the fragmented partition in a stable diagnostic shape. */
  constructor(partitionId: number) {
    super(`partition ${partitionId} is rebalancing`);
    this.name = "PartitionRebalancingError";
    this.partitionId = partitionId;
  }
}

/**
 * The local node is not serving operations: the split-brain resolver stopped
 * this half, or the configured member quorum is not met.
 *
 * @internal
 */
export class ClusterUnavailableError extends Error {
  /** Creates a stable typed error for a node that must not accept operations. */
  constructor() {
    super("cluster is unavailable");
    this.name = "ClusterUnavailableError";
  }
}

/** Deadline whose expiration is represented by {@link KvTimeoutError}. @internal */
export type KvTimeoutPhase = "request" | "bootstrap";

/**
 * An injected or wall-clock deadline elapsed before the store could finish an
 * RPC or complete bootstrap intake.
 *
 * @internal
 */
export class KvTimeoutError extends Error {
  /** Operation segment whose timer won the settlement race. */
  readonly phase: KvTimeoutPhase;
  /** Configured relative deadline in milliseconds, not an absolute timestamp. */
  readonly timeoutMs: number;

  /** Captures the elapsed phase and configured duration in a stable diagnostic shape. */
  constructor(phase: KvTimeoutPhase, timeoutMs: number) {
    super(`kv ${phase} timed out after ${timeoutMs}ms`);
    this.name = "KvTimeoutError";
    this.phase = phase;
    this.timeoutMs = timeoutMs;
  }
}

/** Quorum role whose configured count was not reached. @internal */
export type KvQuorumKind = "read" | "write";

/**
 * A read or write could not gather the configured quorum of replica responses.
 *
 * @internal
 */
export class KvQuorumError extends Error {
  /** Whether the unmet count was a read quorum or a write quorum. */
  readonly kind: KvQuorumKind;
  /** Replica responses collected before the deadline. */
  readonly got: number;
  /** Configured quorum that had to be met. */
  readonly need: number;

  /** Captures the unmet quorum in a stable diagnostic shape. */
  constructor(kind: KvQuorumKind, got: number, need: number) {
    super(`${kind} quorum ${need} not met (got ${got})`);
    this.name = "KvQuorumError";
    this.kind = kind;
    this.got = got;
    this.need = need;
  }
}

/** Size-limited field rejected by {@link KvLimitError}. @internal */
export type KvLimitField = "key" | "value";

/**
 * A key or value exceeded its byte budget before it was stored or transmitted.
 *
 * @internal
 */
export class KvLimitError extends Error {
  /** Which field exceeded its budget. */
  readonly field: KvLimitField;
  /** Observed size in bytes. */
  readonly size: number;
  /** Inclusive maximum permitted size in bytes. */
  readonly max: number;

  /** Captures the oversized field in a stable diagnostic shape. */
  constructor(field: KvLimitField, size: number, max: number) {
    super(`${field} length ${size} exceeds ${max} bytes`);
    this.name = "KvLimitError";
    this.field = field;
    this.size = size;
    this.max = max;
  }
}

/**
 * Raised for malformed, truncated, oversized, or noncanonical store protocol
 * data. Decode paths must reject before allocating from an untrusted length.
 *
 * @internal
 */
export class KvProtocolError extends Error {
  /** Creates an error with the stable `KvProtocolError` name and supplied diagnostic. */
  constructor(message: string) {
    super(message);
    this.name = "KvProtocolError";
  }
}
