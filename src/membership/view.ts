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

import {
  bytesEqual,
  copyMembershipUpdate,
  MAX_MEMBERS,
  type MemberState,
  type MembershipUpdate,
  STATE_ALIVE,
  STATE_DEAD,
  STATE_LEFT,
  STATE_SUSPECT,
} from "./wire";

/** Duration, in local monotonic milliseconds, that dead and left records remain retained. @internal */
export const TERMINAL_RETENTION_MS: number = 30_000;

/** Largest unsigned 32-bit incarnation representable by the wire protocol. @internal */
export const MAX_INCARNATION: number = 0xffffffff;

/**
 * Detached membership snapshot annotated with when this view accepted the truth.
 *
 * Its metadata bytes are copied from internal storage and may be mutated by the recipient.
 *
 * @internal
 */
export interface MemberRecord extends MembershipUpdate {
  /** Local monotonic timestamp in milliseconds, clamped not to decrease within this view. */
  readonly appliedAt: number;
}

/**
 * Consumer-visible transition categories; suspect transitions and unchanged metadata are silent.
 *
 * @internal
 */
export type MembershipEventType = "joined" | "left" | "dead" | "updated";

/**
 * One synchronous observable transition with detached before/after snapshots.
 *
 * Callback recipients own their copies, including both metadata arrays.
 *
 * @internal
 */
export interface MembershipEvent {
  /** Transition derived from previous state and, for `updated`, alive metadata bytes. */
  readonly type: MembershipEventType;

  /** Detached snapshot of the newly accepted truth. */
  readonly member: MemberRecord;

  /** Detached superseded truth, or `undefined` when this identity was not retained. */
  readonly previous: MemberRecord | undefined;
}

/**
 * Immutable compare-and-delete token for a future terminal-record removal.
 *
 * State, incarnation, and deadline guard against a stale timer deleting revived truth.
 *
 * @internal
 */
export interface ReapOperation {
  /** Canonical member identity/address to remove if every guard still matches. */
  readonly member: string;

  /** Dead or left state that must still be retained at execution time. */
  readonly state: typeof STATE_DEAD | typeof STATE_LEFT;

  /** Unsigned incarnation that must still match at execution time. */
  readonly incarnation: number;

  /** Absolute local monotonic deadline in milliseconds derived from original application time. */
  readonly dueAt: number;
}

/**
 * Exhaustive result of processing one update, including non-replacing suspect confirmations.
 *
 * Any exposed update or record is detached from the view's stored metadata.
 *
 * @internal
 */
export type ApplyResult =
  | {
      /** Newer precedence truth replaced or created the retained record. */
      readonly kind: "applied";

      /** Detached snapshot of the newly stored record. */
      readonly record: MemberRecord;

      /** Transition returned to the caller, or `undefined` for a silent state change. */
      readonly event: MembershipEvent | undefined;
    }
  | {
      /** Equal suspect truth added a distinct reporter without replacing the retained record. */
      readonly kind: "confirmed";

      /** Canonical identity/address of the suspected member. */
      readonly member: string;

      /** Incarnation for which the distinct reporter was accepted. */
      readonly incarnation: number;

      /** Canonical identity/address of the newly accepted accuser. */
      readonly reporter: string;

      /** Number of distinct confirmations after the original accuser, including this one. */
      readonly confirmationCount: number;
    }
  | {
      /** The local member replaced an applicable accusation with a higher-incarnation alive truth. */
      readonly kind: "refuted";

      /** Detached copy of the incoming suspect, dead, or left accusation. */
      readonly accusation: MembershipUpdate;

      /** Detached snapshot of the newly retained alive defense. */
      readonly record: MemberRecord;

      /** Transition returned to the caller, or `undefined` when refutation is not observable. */
      readonly event: MembershipEvent | undefined;
    }
  | {
      /** Stale, duplicate, or otherwise inapplicable truth produced no mutation or callback. */
      readonly kind: "ignored";
    };

/**
 * The {@link ApplyResult} kinds as named values, so dissemination code keys on a
 * shared name rather than a bare literal each. The union above keeps the
 * literals as its source of truth; `satisfies` validates each against it.
 *
 * @internal
 */
export const ApplyKind = {
  applied: "applied",
  confirmed: "confirmed",
  refuted: "refuted",
  ignored: "ignored",
} as const satisfies Record<string, ApplyResult["kind"]>;

/** Raised when self-refutation would overflow the wire's unsigned 32-bit incarnation. @internal */
export class IncarnationExhaustedError extends Error {
  /** Maximum of the accusation and retained incarnation that could not be incremented. */
  readonly incarnation: number;

  /** Creates a stable typed error for an incarnation that cannot be safely advanced. */
  constructor(incarnation: number) {
    super(`incarnation ${incarnation} cannot be incremented`);
    this.name = "IncarnationExhaustedError";
    this.incarnation = incarnation;
  }
}

/** Raised before insertion when the retained table already contains 1,024 identities. @internal */
export class MembershipCapacityError extends Error {
  /** Creates a stable typed error describing the fixed synchronization-table limit. */
  constructor() {
    super(`membership table cannot exceed ${MAX_MEMBERS} retained members`);
    this.name = "MembershipCapacityError";
  }
}

/** Internal owned record; unlike exposed snapshots, its metadata and confirmations are mutable state. */
interface StoredRecord {
  /** Retained wire state used in precedence comparisons. */
  readonly state: MemberState;

  /** Retained protocol provenance flag. */
  readonly selfOriginated: boolean;

  /** Retained unsigned incarnation used as primary precedence. */
  readonly incarnation: number;

  /** Origin timestamp transported as truth but not used for local precedence. */
  readonly stateChangeTime: bigint;

  /** Canonical identity/address and map key for this record. */
  readonly member: string;

  /** Original suspect accuser, or empty outside suspect state. */
  readonly reporter: string;

  /** View-owned copy of opaque alive metadata. */
  readonly metadata: Uint8Array;

  /** Nondecreasing local monotonic application time in milliseconds. */
  readonly appliedAt: number;

  /** Distinct suspect accusers for the currently retained suspect incarnation. */
  readonly confirmations: Set<string>;
}

/** Common detached result shape for record replacement and self-refutation. */
interface ReplacementResult<K extends "applied" | "refuted"> {
  /** Literal operation kind retained for the caller's discriminated union. */
  readonly kind: K;

  /** Detached snapshot of the replacement record. */
  readonly record: MemberRecord;

  /** Detached observable transition, if replacement semantics produce one. */
  readonly event: MembershipEvent | undefined;
}

/** Allocates an independent byte-for-byte copy of mutable metadata. */
function copyBytes(bytes: Uint8Array): Uint8Array {
  return Uint8Array.from(bytes);
}

/** Creates a caller-owned record snapshot with detached metadata. */
function snapshot(record: StoredRecord): MemberRecord {
  return {
    state: record.state,
    selfOriginated: record.selfOriginated,
    incarnation: record.incarnation,
    stateChangeTime: record.stateChangeTime,
    member: record.member,
    reporter: record.reporter,
    metadata: copyBytes(record.metadata),
    appliedAt: record.appliedAt,
  };
}

/**
 * Compares membership truth by incarnation first, then numeric state precedence.
 *
 * Returns a positive number when `left` supersedes `right`, zero for equal precedence,
 * and a negative number otherwise. Metadata, reporter, provenance, and timestamps are ignored.
 *
 * @internal
 */
export function compareMembershipUpdates(
  left: Pick<MembershipUpdate, "incarnation" | "state">,
  right: Pick<MembershipUpdate, "incarnation" | "state">,
): number {
  if (left.incarnation !== right.incarnation) {
    return left.incarnation > right.incarnation ? 1 : -1;
  }

  return left.state === right.state ? 0 : left.state > right.state ? 1 : -1;
}

/** Narrows dead and left states, which are retained only through the terminal grace window. @internal */
export function isTerminalState(
  state: MemberState,
): state is typeof STATE_DEAD | typeof STATE_LEFT {
  return state === STATE_DEAD || state === STATE_LEFT;
}

/** Narrows alive and suspect states, the only states eligible for active probing. @internal */
export function isProbeEligibleState(
  state: MemberState,
): state is typeof STATE_ALIVE | typeof STATE_SUSPECT {
  return state === STATE_ALIVE || state === STATE_SUSPECT;
}

/**
 * Derives a consumer-visible transition for a replacement.
 *
 * Suspect transitions and alive replacements with byte-identical metadata intentionally
 * return `undefined`; revival from dead or left is reported as `joined`. A terminal
 * replacement is observable only once per departure: terminal truth superseding truth
 * that was already terminal, or arriving for an identity with no retained record, is
 * silent so consumers cannot receive duplicate `dead`/`left` notifications.
 */
function eventType(
  previous: StoredRecord | undefined,
  next: StoredRecord,
): MembershipEventType | undefined {
  if (next.state === STATE_ALIVE) {
    if (previous === undefined || isTerminalState(previous.state)) {
      return "joined";
    }

    if (previous.state === STATE_ALIVE && !bytesEqual(previous.metadata, next.metadata)) {
      return "updated";
    }

    return undefined;
  }

  if (isTerminalState(next.state)) {
    if (previous === undefined || isTerminalState(previous.state)) {
      return undefined;
    }

    return next.state === STATE_DEAD ? "dead" : "left";
  }

  return undefined;
}

/**
 * Pure membership-table state machine. It owns no clock or timers; callers pass
 * local time and execute guarded reap operations when their clock says they are due.
 * Incoming and outgoing metadata are copied, and event callbacks run synchronously
 * after storage mutation with an additional detached event copy.
 *
 * @internal
 */
export class MembershipView {
  /** Canonical advertised identity/address of this process. */
  readonly #selfName: string;

  /** Optional synchronous callback; called only for consumer-visible transitions. */
  readonly #onEvent: ((event: MembershipEvent) => void) | undefined;

  /** View-owned truth indexed by canonical advertised member identity/address. */
  readonly #records: Map<string, StoredRecord> = new Map<string, StoredRecord>();

  /** Greatest local application time accepted, used to prevent timestamp regression. */
  #lastAppliedAt: number = Number.NEGATIVE_INFINITY;

  /**
   * Retained alive-plus-suspect record count, maintained on every replacement.
   *
   * The count feeds retransmit and suspicion sizing on every applied update, so
   * it is kept incrementally instead of scanning the table per call. Reaping
   * removes only terminal records and therefore never changes it.
   */
  #eligibleCount: number = 0;

  /**
   * Creates an empty local view for one stable advertised identity/address.
   *
   * The optional event sink is invoked synchronously and receives metadata copies that
   * cannot mutate retained truth.
   */
  constructor(selfName: string, onEvent?: (event: MembershipEvent) => void) {
    this.#selfName = selfName;
    this.#onEvent = onEvent;
  }

  /** Number of retained identities, including dead and left records awaiting reaping. */
  get size(): number {
    return this.#records.size;
  }

  /** Stable canonical advertised identity/address supplied at construction. */
  get selfName(): string {
    return this.#selfName;
  }

  /** Returns a detached snapshot for an advertised identity/address, or `undefined` if absent. */
  get(name: string): MemberRecord | undefined {
    const record: StoredRecord | undefined = this.#records.get(name);
    return record === undefined ? undefined : snapshot(record);
  }

  /**
   * Returns the retained state and incarnation for an identity, or `undefined` if absent.
   *
   * This is the cheap read for hot selection paths: no snapshot or metadata copy is made.
   */
  stateOf(name: string): { readonly state: MemberState; readonly incarnation: number } | undefined {
    const record: StoredRecord | undefined = this.#records.get(name);
    if (record === undefined) {
      return undefined;
    }

    return { state: record.state, incarnation: record.incarnation };
  }

  /**
   * Invokes `callback` with each retained member name and state in map insertion order.
   *
   * No snapshots or metadata copies are made; the callback must not mutate this view.
   */
  eachMember(callback: (member: string, state: MemberState) => void): void {
    for (const record of this.#records.values()) {
      callback(record.member, record.state);
    }
  }

  /** Returns a detached snapshot for `selfName`, or `undefined` before local truth is retained. */
  self(): MemberRecord | undefined {
    return this.get(this.#selfName);
  }

  /** Returns detached snapshots of all records in map insertion order, including terminal truth. */
  members(): readonly MemberRecord[] {
    return Array.from(this.#records.values(), snapshot);
  }

  /** Returns wire-ready copies of all retained truth, excluding only local `appliedAt` state. */
  updates(): readonly MembershipUpdate[] {
    return Array.from(
      this.#records.values(),
      (record: StoredRecord): MembershipUpdate => copyMembershipUpdate(record),
    );
  }

  /** Counts retained alive and suspect records; dead and left records are excluded. */
  aliveOrSuspectCount(): number {
    return this.#eligibleCount;
  }

  /**
   * Returns a new array of distinct accusers for the retained suspect incarnation.
   *
   * The original reporter appears first; absent and non-suspect members return an empty array.
   */
  confirmationReporters(member: string): readonly string[] {
    const record: StoredRecord | undefined = this.#records.get(member);
    return record?.state === STATE_SUSPECT ? Array.from(record.confirmations) : [];
  }

  /**
   * Reports whether retained truth may be gossiped at local monotonic time `now`.
   *
   * Alive and suspect truth is always eligible. Terminal truth is eligible strictly before
   * `appliedAt + TERMINAL_RETENTION_MS`; absence and exact expiry are false.
   */
  isGossipEligible(name: string, now: number): boolean {
    const record: StoredRecord | undefined = this.#records.get(name);
    if (record === undefined) {
      return false;
    }

    return (
      isProbeEligibleState(record.state) ||
      (isTerminalState(record.state) && now < record.appliedAt + TERMINAL_RETENTION_MS)
    );
  }

  /**
   * Reports whether `apply` would treat `update` as an applicable self-accusation and
   * answer it with a generated higher-incarnation alive defense.
   *
   * This is the single home of the self-refutation applicability rule. Non-alive truth
   * about `selfName` is applicable when its incarnation is at least the retained one (or
   * no record is retained). An alive claim about `selfName` is also an accusation when
   * local truth is alive and the claim carries a higher incarnation, or the same
   * incarnation with different metadata: adopting it would silently replace local self
   * truth, so it is answered with a re-announcement instead. An equal-incarnation echo
   * with identical metadata is not an accusation. It reads no clock and never mutates
   * the view.
   */
  wouldRefute(update: MembershipUpdate): boolean {
    if (update.member !== this.#selfName) {
      return false;
    }

    const current: StoredRecord | undefined = this.#records.get(update.member);
    if (update.state !== STATE_ALIVE) {
      return current === undefined || update.incarnation >= current.incarnation;
    }

    if (current === undefined || current.state !== STATE_ALIVE) {
      return false;
    }

    if (update.incarnation > current.incarnation) {
      return true;
    }

    return (
      update.incarnation === current.incarnation && !bytesEqual(update.metadata, current.metadata)
    );
  }

  /**
   * Applies one remotely observed update under incarnation/state precedence.
   *
   * Applicable self-accusations, including remote alive claims about `selfName` that
   * would otherwise replace local self truth, are refuted before normal replacement. For
   * that path, `selfStateChangeTime` becomes the generated alive record's origin
   * timestamp. Terminal truth about an identity with no retained record is ignored: a
   * departure for a member this view never knew, or already reaped, must not resurrect a
   * record, restart retention, or earn fresh dissemination budget. Accepted metadata is
   * copied, `now` is clamped to the view's last application time, and observable
   * callbacks run synchronously after mutation.
   *
   * @throws {IncarnationExhaustedError} If self-refutation cannot increment incarnation.
   * @throws {MembershipCapacityError} If accepting a new identity would exceed `MAX_MEMBERS`.
   */
  apply(
    incoming: MembershipUpdate,
    now: number,
    selfStateChangeTime: bigint = incoming.stateChangeTime,
  ): ApplyResult {
    const current: StoredRecord | undefined = this.#records.get(incoming.member);
    if (this.wouldRefute(incoming)) {
      return this.#refute(incoming, current, now, selfStateChangeTime);
    }

    if (current === undefined && isTerminalState(incoming.state)) {
      return { kind: ApplyKind.ignored };
    }

    if (current !== undefined && compareMembershipUpdates(incoming, current) <= 0) {
      return this.#confirmationOrIgnored(incoming, current);
    }

    return this.#replace(incoming, current, now, ApplyKind.applied);
  }

  /**
   * Applies locally originated truth without interpreting non-alive self truth as an accusation.
   *
   * This is the path for startup metadata and graceful leave. Precedence, copying, capacity,
   * synchronous events, and `now` clamping are otherwise identical to `apply`.
   *
   * @throws {MembershipCapacityError} If accepting a new identity would exceed `MAX_MEMBERS`.
   */
  applyLocal(update: MembershipUpdate, now: number): ApplyResult {
    const current: StoredRecord | undefined = this.#records.get(update.member);
    if (current !== undefined && compareMembershipUpdates(update, current) <= 0) {
      return this.#confirmationOrIgnored(update, current);
    }

    return this.#replace(update, current, now, ApplyKind.applied);
  }

  /** Creates a guarded reap token for terminal truth, or `undefined` if absent/nonterminal. */
  reapOperation(name: string): ReapOperation | undefined {
    const record: StoredRecord | undefined = this.#records.get(name);
    if (record === undefined || !isTerminalState(record.state)) {
      return undefined;
    }

    return {
      member: record.member,
      state: record.state,
      incarnation: record.incarnation,
      dueAt: record.appliedAt + TERMINAL_RETENTION_MS,
    };
  }

  /**
   * Returns new guarded reap tokens whose deadlines are at or before monotonic `now`.
   *
   * This method does not remove records or emit events.
   */
  dueReaps(now: number): readonly ReapOperation[] {
    const due: ReapOperation[] = [];
    for (const record of this.#records.values()) {
      if (!isTerminalState(record.state)) {
        continue;
      }

      const operation: ReapOperation | undefined = this.reapOperation(record.member);
      if (operation !== undefined && now >= operation.dueAt) {
        due.push(operation);
      }
    }

    return due;
  }

  /**
   * Executes a previously described reap only when the exact terminal truth and
   * its original deadline are still current, making stale timer callbacks harmless.
   *
   * Returns `true` only when a record was deleted. It does not check the current clock and
   * emits no event; callers are responsible for invoking it no earlier than `dueAt`.
   */
  reap(operation: ReapOperation): boolean {
    const current: StoredRecord | undefined = this.#records.get(operation.member);
    if (
      current === undefined ||
      current.state !== operation.state ||
      current.incarnation !== operation.incarnation ||
      current.appliedAt + TERMINAL_RETENTION_MS !== operation.dueAt
    ) {
      return false;
    }

    this.#records.delete(operation.member);
    return true;
  }

  /**
   * Builds and stores a higher-incarnation alive defense against applicable self truth.
   *
   * Preserves metadata only from a currently alive self record and returns detached copies.
   */
  #refute(
    accusation: MembershipUpdate,
    current: StoredRecord | undefined,
    now: number,
    stateChangeTime: bigint,
  ): ApplyResult {
    const base: number = Math.max(accusation.incarnation, current?.incarnation ?? 0);
    if (base >= MAX_INCARNATION) {
      throw new IncarnationExhaustedError(base);
    }

    const alive: MembershipUpdate = {
      state: STATE_ALIVE,
      selfOriginated: true,
      incarnation: base + 1,
      stateChangeTime,
      member: this.#selfName,
      reporter: "",
      metadata: current?.state === STATE_ALIVE ? copyBytes(current.metadata) : new Uint8Array(0),
    };

    const result: ReplacementResult<"refuted"> = this.#replace(
      alive,
      current,
      now,
      ApplyKind.refuted,
    );

    return {
      kind: ApplyKind.refuted,
      accusation: copyMembershipUpdate(accusation),
      record: result.record,
      event: result.event,
    };
  }

  /**
   * Records a distinct equal-incarnation suspect reporter or reports the update as ignored.
   *
   * Confirmation mutates only the reporter set; it does not replace truth, advance `appliedAt`,
   * or emit an event.
   */
  #confirmationOrIgnored(incoming: MembershipUpdate, current: StoredRecord): ApplyResult {
    if (
      incoming.state !== STATE_SUSPECT ||
      current.state !== STATE_SUSPECT ||
      incoming.incarnation !== current.incarnation ||
      current.confirmations.has(incoming.reporter)
    ) {
      return { kind: ApplyKind.ignored };
    }

    current.confirmations.add(incoming.reporter);

    return {
      kind: ApplyKind.confirmed,
      member: incoming.member,
      incarnation: incoming.incarnation,
      reporter: incoming.reporter,
      confirmationCount: current.confirmations.size - 1,
    };
  }

  /**
   * Replaces retained truth, resets suspicion confirmations, and emits any derived event.
   *
   * Incoming metadata is copied into storage. The returned record and event share one
   * detached snapshot per identity, while the callback receives independent metadata
   * copies, so no consumer can mutate the view or the callback's data. The callback runs
   * after the map update.
   */
  #replace<K extends "applied" | "refuted">(
    incoming: MembershipUpdate,
    previous: StoredRecord | undefined,
    now: number,
    kind: K,
  ): ReplacementResult<K> {
    if (previous === undefined && this.#records.size >= MAX_MEMBERS) {
      throw new MembershipCapacityError();
    }

    const previousEligible: boolean =
      previous !== undefined && isProbeEligibleState(previous.state);
    const nextEligible: boolean = isProbeEligibleState(incoming.state);
    if (previousEligible !== nextEligible) {
      this.#eligibleCount += nextEligible ? 1 : -1;
    }

    const appliedAt: number = Math.max(now, this.#lastAppliedAt);
    this.#lastAppliedAt = appliedAt;
    const confirmations: Set<string> = new Set<string>();
    if (incoming.state === STATE_SUSPECT) {
      confirmations.add(incoming.reporter);
    }

    const stored: StoredRecord = {
      state: incoming.state,
      selfOriginated: incoming.selfOriginated,
      incarnation: incoming.incarnation,
      stateChangeTime: incoming.stateChangeTime,
      member: incoming.member,
      reporter: incoming.reporter,
      metadata: copyBytes(incoming.metadata),
      appliedAt,
      confirmations,
    };

    this.#records.set(incoming.member, stored);

    const record: MemberRecord = snapshot(stored);
    const type: MembershipEventType | undefined = eventType(previous, stored);
    const event: MembershipEvent | undefined =
      type === undefined
        ? undefined
        : {
            type,
            member: record,
            previous: previous === undefined ? undefined : snapshot(previous),
          };
    if (event !== undefined) {
      this.#onEvent?.({
        type: event.type,
        member: { ...event.member, metadata: copyBytes(event.member.metadata) },
        previous:
          event.previous === undefined
            ? undefined
            : { ...event.previous, metadata: copyBytes(event.previous.metadata) },
      });
    }

    return { kind, record, event };
  }
}
