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
 * The contract between the key/value package and the rest of the runtime.
 *
 * The package is actor-blind, membership-blind, and transport-blind. It reaches
 * the outside world only through {@link ClusterView} and {@link KvTransport}.
 * {@link ReplicationGroup} is the durability interface: the v1 implementation is
 * primary/backup with quorum acknowledgment and reconcile; a consensus
 * implementation can replace it without changing anything above it.
 *
 * @internal
 */

/**
 * Hybrid logical clock reading used as the last-write-wins order.
 *
 * `wallMs` tracks physical time; `logical` breaks ties at equal wall time on
 * the same node; `node` breaks ties across nodes so the outcome is deterministic.
 *
 * @internal
 */
export interface HybridTime {
  /** Physical time in Unix epoch milliseconds. */
  readonly wallMs: number;
  /** Per-node causal counter, reset when `wallMs` advances past the last observation. */
  readonly logical: number;
  /** Canonical cluster identity of the writing node. */
  readonly node: string;
}

/**
 * One stored record, including tombstones.
 *
 * A tombstone has `deleted` set and no value. Last write wins compares
 * tombstones against live writes by {@link HybridTime} in both directions.
 *
 * @internal
 */
export interface Entry {
  /** UTF-8 key. Byte length is bounded by `MAX_KEY_BYTES`. */
  readonly key: string;
  /** Payload bytes, or `undefined` when this record is a tombstone. */
  readonly value: Uint8Array | undefined;
  /** Last-write-wins order assigned at the primary. */
  readonly timestamp: HybridTime;
  /** Per-partition monotone sequence used for backup FIFO, not for promotion. */
  readonly sequence: bigint;
  /** Absolute Unix expiry in milliseconds, or `undefined` when the entry does not expire. */
  readonly expiresAt: number | undefined;
  /** Whether this record is a delete tombstone rather than a live value. */
  readonly deleted: boolean;
}

/**
 * Unconditional or predicate put. `nx` fails when an unexpired live key exists;
 * `xx` fails when the key is absent or a tombstone. Optional `ttlMs` becomes an
 * absolute `expiresAt` at the primary.
 *
 * @internal
 */
export interface PutOp {
  readonly kind: "put";
  readonly key: string;
  readonly value: Uint8Array;
  readonly condition: "none" | "nx" | "xx";
  readonly ttlMs?: number;
}

/**
 * Delete that writes a tombstone rather than an absence, so anti-entropy cannot
 * resurrect the key.
 *
 * @internal
 */
export interface DeleteOp {
  readonly kind: "delete";
  readonly key: string;
}

/**
 * Atomic integer increment of the current value interpreted as a signed 64-bit
 * counter. Missing keys start at zero before applying `delta`. `delta` is a
 * `bigint` so the full signed 64-bit range stays exact, matching the counter it
 * mutates; a `number` would lose precision past 2^53.
 *
 * @internal
 */
export interface IncrementOp {
  readonly kind: "incr";
  readonly key: string;
  readonly delta: bigint;
}

/**
 * Compare-and-set: apply `value` only when the live unexpired payload equals
 * `expected` byte-for-byte.
 *
 * @internal
 */
export interface CompareAndSetOp {
  readonly kind: "cas";
  readonly key: string;
  readonly expected: Uint8Array;
  readonly value: Uint8Array;
}

/**
 * A key paired with the last-write-wins order of its stored entry, exchanged
 * during anti-entropy so two replicas learn which side holds the newer version
 * without shipping the values themselves.
 *
 * @internal
 */
export interface KeyVersion {
  /** UTF-8 key of the entry. */
  readonly key: string;
  /** Last-write-wins order of the entry the holder stores for `key`. */
  readonly timestamp: HybridTime;
}

/** Mutation submitted through {@link ReplicationGroup.propose}. @internal */
export type WriteOp = PutOp | DeleteOp | IncrementOp | CompareAndSetOp;

/** Why a conditional write declined to mutate. @internal */
export type ConditionFailure = "nx" | "xx" | "cas";

/** Successful mutation; `entry` is the record stored at the primary. @internal */
export interface WriteApplied {
  readonly applied: true;
  readonly entry: Entry;
}

/** Conditional write that evaluated false; the store is unchanged. @internal */
export interface WriteRejected {
  readonly applied: false;
  readonly reason: ConditionFailure;
}

/** Outcome of {@link ReplicationGroup.propose} when the group accepted the RPC. @internal */
export type WriteResult = WriteApplied | WriteRejected;

/**
 * What the store needs to know about who is in the cluster.
 *
 * `clustering.ts` adapts the membership engine into this view. `startedAt`,
 * `ready`, and `draining` travel in membership metadata; this package never
 * inspects those bytes itself.
 *
 * @internal
 */
export interface ClusterView {
  /** This node's canonical cluster identity, the address {@link KvTransport} dials. */
  readonly self: string;

  /**
   * Live members in a stable order, oldest `startedAt` first, ties broken by
   * name. The coordinator is therefore `members()[0]` when the list is non-empty.
   */
  members(): readonly ClusterMember[];

  /**
   * Subscribes to membership change. The listener receives the same snapshot
   * `members()` would then return. Returns an unsubscribe function.
   */
  onChange(listener: (members: readonly ClusterMember[]) => void): () => void;
}

/**
 * One live member as the store sees it.
 *
 * @internal
 */
export interface ClusterMember {
  /** Canonical identity, the address {@link KvTransport} dials to reach this member. */
  readonly name: string;
  /** Immutable process start time in epoch milliseconds; decides the coordinator. */
  readonly startedAt: number;
  /** Whether the node has completed its initial fragment intake. */
  readonly ready: boolean;
  /** Whether the node has announced a graceful leave and is draining fragments. */
  readonly draining: boolean;
}

/**
 * Addressed request/response with no ordering or delivery guarantee.
 *
 * Bodies are opaque protocol bytes. Loss, delay, duplication, and partition
 * are all permitted; the store must be correct under all of them.
 *
 * @internal
 */
export interface KvTransport {
  /**
   * Sends `body` to `to` and waits for one response or until `deadlineMs`
   * milliseconds have elapsed.
   */
  request(to: string, body: Uint8Array, deadlineMs: number): Promise<Uint8Array>;

  /**
   * Installs the inbound handler. The handler's resolved bytes become the
   * response to that request.
   */
  listen(handler: (from: string, body: Uint8Array) => Promise<Uint8Array>): void;

  /** Releases carrier resources. Subsequent requests must reject. */
  close(): Promise<void>;
}

/**
 * The replication authority for one partition.
 *
 * The v1 implementation is primary/backup with quorum acknowledgment and
 * survivor reconcile. A later consensus implementation would satisfy the same
 * methods without changing the ring, the routing table, the transport, or the
 * client API.
 *
 * @internal
 */
export interface ReplicationGroup {
  /** Submits a mutation, including conditional writes, to the group's leader. */
  propose(op: WriteOp): Promise<WriteResult>;

  /** Reads the current live, unexpired record, or `undefined` when absent. */
  read(key: string): Promise<Entry | undefined>;

  /**
   * Unions this replica with `peers` under last write wins so a newly promoted
   * primary holds every acknowledged write that survived the failure.
   */
  reconcile(peers: readonly string[]): Promise<void>;

  /** Installs the current owners list after a routing-table change. */
  memberChange(owners: readonly string[]): void;
}
