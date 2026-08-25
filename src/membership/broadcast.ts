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

import { compareMembershipUpdates } from "./view";
import {
  copyMembershipUpdate,
  type MembershipUpdate,
  membershipUpdateSize,
  STATE_ALIVE,
  UPDATE_LIST_MAX_RECORDS,
} from "./wire";

/** Wire-format record cap re-exported for queue consumers. @internal */
export { UPDATE_LIST_MAX_RECORDS } from "./wire";

/** Scale factor in `4 * ceil(log10(N + 1))` accepted-send budgets. @internal */
export const RETRANSMIT_MULTIPLIER = 4;

/**
 * Detached diagnostic snapshot of one member's queued truth and send budget.
 *
 * The update, including metadata, is copied and may be mutated by the recipient.
 *
 * @internal
 */
export interface BroadcastSnapshot {
  /** Detached copy of the currently queued canonical truth. */
  readonly update: MembershipUpdate;

  /** Whether an alive self-defense sorts before all ordinary queued truth. */
  readonly priority: boolean;

  /** Accepted-send budget captured when this truth generation was enqueued. */
  readonly initialRemaining: number;

  /** Accepted destination sends still available before this generation is removed. */
  readonly remaining: number;

  /** `initialRemaining - remaining` for the current generation. */
  readonly transmissions: number;
}

/** Byte/count constraints and optional suspect-buddy truth for one side-effect-free pack. @internal */
export interface BroadcastPackOptions {
  /** Non-negative safe-integer bytes charged per selected record beyond its encoded size. */
  readonly perRecordOverhead?: number;

  /**
   * Candidate mandatory-first suspect record.
   *
   * It is included only if one record is allowed and it fits the byte budget. Input metadata
   * remains caller-owned and is copied into the selection.
   */
  readonly buddy?: MembershipUpdate;

  /** Non-negative record cap, defaulting to and never exceeding the unsigned-byte limit. */
  readonly maxRecords?: number;
}

/**
 * Frozen, single-acknowledgement result of a queue selection.
 *
 * The object and update array are frozen, but each copied update's metadata remains mutable
 * and detached from queue state. Only the originating queue recognizes the object identity.
 *
 * @internal
 */
export interface BroadcastSelection {
  /** Included buddy first, followed by detached queued updates in scheduling order. */
  readonly updates: readonly MembershipUpdate[];

  /** Sum in bytes of encoded record sizes plus configured overhead; never exceeds the budget. */
  readonly bytes: number;
}

/** Queue-owned state for one canonical member identity and one truth generation. */
interface QueuedBroadcast {
  /** Queue-owned copy of the membership truth. */
  readonly update: MembershipUpdate;

  /** Validated encoded membership-record size in bytes, cached at enqueue time. */
  readonly encodedBytes: number;

  /** Alive self-defense ordering flag. */
  readonly priority: boolean;

  /** Accepted-send budget assigned to this generation. */
  readonly initialRemaining: number;

  /** Monotonic insertion order used as the final scheduling tie-breaker. */
  readonly order: number;

  /** Monotonic identity preventing stale selections from charging replacement truth. */
  readonly generation: number;

  /** Accepted destination sends remaining; decremented only by successful acknowledgement. */
  remaining: number;
}

/** Generation guard for one queued record included in a selection. */
interface IncludedQueuedRecord {
  /** Canonical member identity/address used to look up current queued truth. */
  readonly member: string;

  /** Queue generation that must still match when the selection is acknowledged. */
  readonly generation: number;
}

/** Queue-private acknowledgement metadata associated with a selection object identity. */
interface SelectionReceipt {
  /** Queued generations eligible to consume one accepted-send budget unit. */
  readonly included: readonly IncludedQueuedRecord[];
}

/** Validates a configuration value as a non-negative safe integer. */
function assertNonnegativeInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${field} must be a non-negative safe integer`);
  }
}

/**
 * Orders queue records by self-defense priority, fewest prior transmissions, then insertion.
 *
 * A negative result places `left` before `right`; comparison has no side effects.
 */
function compareQueued(left: QueuedBroadcast, right: QueuedBroadcast): number {
  if (left.priority !== right.priority) {
    return left.priority ? -1 : 1;
  }

  const leftTransmissions = left.initialRemaining - left.remaining;
  const rightTransmissions = right.initialRemaining - right.remaining;
  if (leftTransmissions !== rightTransmissions) {
    return leftTransmissions - rightTransmissions;
  }

  return left.order - right.order;
}

/** Tests whether two records represent the same precedence truth generation. */
function truthMatches(left: MembershipUpdate, right: MembershipUpdate): boolean {
  return left.incarnation === right.incarnation && left.state === right.state;
}

/**
 * Transmit-limited membership broadcast queue. Selection is side-effect free;
 * callers explicitly acknowledge each destination's local transport outcome.
 * The queue owns copies of metadata and keeps at most one precedence-maximal
 * truth generation per canonical advertised member identity/address.
 *
 * @internal
 */
export class BroadcastQueue {
  /** Current queued truth indexed by canonical advertised member identity/address. */
  readonly #queued = new Map<string, QueuedBroadcast>();

  /** Single-use receipts keyed by exact selection object identity without retaining selections. */
  readonly #receipts = new WeakMap<BroadcastSelection, SelectionReceipt>();

  /** Reused scratch ordering buffer; valid only within one `pack` call. */
  readonly #scratch: QueuedBroadcast[] = [];

  /** Next insertion-order value; relevant only among equally transmitted records. */
  #nextOrder = 0;

  /** Next truth-generation guard used to make stale acknowledgements harmless. */
  #nextGeneration = 0;

  /** Number of distinct member identities with queued truth. */
  get size(): number {
    return this.#queued.size;
  }

  /**
   * Enqueues new truth or supersedes lower-precedence truth for the same member identity.
   *
   * Returns `false` without validating `aliveOrSuspectCount` or mutating state when queued
   * truth has equal or greater precedence. Otherwise the update and metadata are copied, and
   * the count is captured in `4 * ceil(log10(max(1, count) + 1))`. Priority applies only when
   * requested for an alive update.
   *
   * @throws {ProtocolError} If `incoming` is not wire-encodable.
   * @throws {RangeError} If `aliveOrSuspectCount` is not a non-negative safe integer.
   */
  enqueue(
    incoming: MembershipUpdate,
    aliveOrSuspectCount: number,
    prioritySelfDefense = false,
  ): boolean {
    const encodedBytes = membershipUpdateSize(incoming);
    const current = this.#queued.get(incoming.member);
    if (current !== undefined && compareMembershipUpdates(incoming, current.update) <= 0) {
      return false;
    }

    assertNonnegativeInteger(aliveOrSuspectCount, "alive-or-suspect count");
    const memberCount = Math.max(1, aliveOrSuspectCount);
    const remaining = RETRANSMIT_MULTIPLIER * Math.ceil(Math.log10(memberCount + 1));
    const update = copyMembershipUpdate(incoming);
    this.#queued.set(update.member, {
      update,
      encodedBytes,
      priority: prioritySelfDefense && update.state === STATE_ALIVE,
      initialRemaining: remaining,
      remaining,
      order: this.#nextOrder,
      generation: this.#nextGeneration,
    });

    this.#nextOrder += 1;
    this.#nextGeneration += 1;
    return true;
  }

  /** Returns a detached snapshot for a member identity, or `undefined` when it is not queued. */
  get(member: string): BroadcastSnapshot | undefined {
    const record = this.#queued.get(member);
    if (record === undefined) {
      return undefined;
    }

    return {
      update: copyMembershipUpdate(record.update),
      priority: record.priority,
      initialRemaining: record.initialRemaining,
      remaining: record.remaining,
      transmissions: record.initialRemaining - record.remaining,
    };
  }

  /**
   * Selects whole records within byte and count budgets without changing queue counters.
   *
   * A fitting buddy is copied first and suppresses any queued record with the same member,
   * regardless of truth. It consumes queued retransmission budget on acknowledgement only when
   * its incarnation and state match the current queued generation. Oversized queued records are
   * skipped so later records can fit. The returned selection is recognized once by this queue.
   *
   * @throws {RangeError} If a budget/option is not a non-negative safe integer or `maxRecords`
   * exceeds 255.
   * @throws {ProtocolError} If a supplied buddy is not wire-encodable.
   */
  pack(byteBudget: number, options: BroadcastPackOptions = {}): BroadcastSelection {
    assertNonnegativeInteger(byteBudget, "byte budget");
    const perRecordOverhead = options.perRecordOverhead ?? 0;
    const maxRecords = options.maxRecords ?? UPDATE_LIST_MAX_RECORDS;
    assertNonnegativeInteger(perRecordOverhead, "per-record overhead");
    assertNonnegativeInteger(maxRecords, "maximum record count");
    if (maxRecords > UPDATE_LIST_MAX_RECORDS) {
      throw new RangeError(`maximum record count cannot exceed ${UPDATE_LIST_MAX_RECORDS}`);
    }

    const updates: MembershipUpdate[] = [];
    const included: IncludedQueuedRecord[] = [];
    let bytes = 0;
    const buddy = options.buddy;
    let buddyMember: string | undefined;
    if (buddy !== undefined && maxRecords > 0) {
      const buddyBytes = membershipUpdateSize(buddy) + perRecordOverhead;
      if (buddyBytes <= byteBudget) {
        const buddyCopy = copyMembershipUpdate(buddy);
        updates.push(buddyCopy);
        bytes = buddyBytes;
        buddyMember = buddyCopy.member;
        const queuedBuddy = this.#queued.get(buddyCopy.member);
        if (queuedBuddy !== undefined && truthMatches(queuedBuddy.update, buddyCopy)) {
          included.push({
            member: queuedBuddy.update.member,
            generation: queuedBuddy.generation,
          });
        }
      }
    }

    const ordered = this.#scratch;
    ordered.length = 0;
    for (const record of this.#queued.values()) {
      ordered.push(record);
    }

    ordered.sort(compareQueued);
    for (const record of ordered) {
      if (updates.length >= maxRecords) {
        break;
      }

      if (record.update.member === buddyMember) {
        continue;
      }

      const recordBytes = record.encodedBytes + perRecordOverhead;
      if (bytes + recordBytes > byteBudget) {
        continue;
      }

      updates.push(copyMembershipUpdate(record.update));
      included.push({ member: record.update.member, generation: record.generation });
      bytes += recordBytes;
    }

    const selection: BroadcastSelection = Object.freeze({
      updates: Object.freeze(updates),
      bytes,
    });

    this.#receipts.set(selection, { included });
    return selection;
  }

  /**
   * Completes one destination/send attempt for a selection produced by this queue.
   *
   * Returns `false` for foreign or already acknowledged objects. A recognized selection is
   * consumed and returns `true` regardless of `accepted`. Rejection leaves budgets unchanged;
   * acceptance decrements each still-current included generation once and removes it at zero.
   * Buddy-only truth and generations superseded after packing are never charged.
   */
  acknowledge(selection: BroadcastSelection, accepted: boolean): boolean {
    const receipt = this.#receipts.get(selection);
    if (receipt === undefined) {
      return false;
    }

    this.#receipts.delete(selection);
    if (!accepted) {
      return true;
    }

    for (const included of receipt.included) {
      const current = this.#queued.get(included.member);
      if (current === undefined || current.generation !== included.generation) {
        continue;
      }

      current.remaining -= 1;
      if (current.remaining === 0) {
        this.#queued.delete(included.member);
      }
    }

    return true;
  }
}
