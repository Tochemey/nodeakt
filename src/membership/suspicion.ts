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

import type { Clock, ClockTimer } from "./clock";
import { MAX_INCARNATION } from "./view";

/** Lifeguard factor `α = 4` in `minimum = α × ceil(log10(n + 1)) × period`. @internal */
export const SUSPICION_MULTIPLIER: number = 4;
/** Lifeguard factor `β = 6` in `maximum = β × minimum`. @internal */
export const SUSPICION_MAX_MULTIPLIER: number = 6;
/** Lifeguard confirmation target `K`; the `K`th confirmation reaches `minimum`. @internal */
export const SUSPICION_CONFIRMATION_TARGET: number = 3;

/**
 * Immutable duration bounds captured when one suspicion starts.
 *
 * Both values are positive safe-integer milliseconds, with
 * `maximum >= minimum`.
 *
 * @internal
 */
export interface SuspicionBounds {
  /** Shortest duration, in milliseconds from the original start. */
  readonly minimum: number;
  /** Initial duration, in milliseconds, used before independent confirmations. */
  readonly maximum: number;
}

/**
 * Evidence and timing inputs used to open or replace a suspicion.
 *
 * The manager copies these primitive values; the caller retains no mutable
 * state owned by the manager.
 *
 * @internal
 */
export interface SuspicionStart {
  /** Non-empty stable identity of the member believed unreachable. */
  readonly member: string;
  /** Unsigned protocol incarnation to which this evidence applies. */
  readonly incarnation: number;
  /** Non-empty identity that originated the initial suspect evidence. */
  readonly reporter: string;
  /** Non-negative alive-or-suspect count, including self, captured at start. */
  readonly memberCount: number;
  /** Positive effective probe period, in milliseconds, captured from the failed probe. */
  readonly effectivePeriod: number;
}

/**
 * Stable protocol identity delivered when a suspicion reaches its deadline.
 *
 * It intentionally omits timing and reporter details: consumers should mark
 * dead only this member incarnation.
 *
 * @internal
 */
export interface SuspicionExpiry {
  /** Member whose active suspicion was removed at expiry. */
  readonly member: string;
  /** Exact suspected incarnation that reached its deadline. */
  readonly incarnation: number;
}

/**
 * Detached read-only snapshot of one currently active suspicion.
 *
 * Times are milliseconds in the injected clock's domain. The reporter array is
 * copied, so callers cannot mutate manager-owned confirmation state.
 *
 * @internal
 */
export interface ActiveSuspicion extends SuspicionExpiry, SuspicionBounds {
  /** Clock reading at creation, retained across all deadline reductions. */
  readonly start: number;
  /** Current absolute expiry timestamp in the same clock domain as `start`. */
  readonly deadline: number;
  /** Initial reporter, which cannot count as an independent confirmation. */
  readonly reporter: string;
  /** Accepted distinct reporter count in `[0, SUSPICION_CONFIRMATION_TARGET]`. */
  readonly confirmationCount: number;
  /** Copied identities of accepted reporters, in first-acceptance order. */
  readonly confirmationReporters: readonly string[];
}

/** Manager-owned mutable state backing one {@link ActiveSuspicion} snapshot. */
interface StoredSuspicion extends SuspicionExpiry, SuspicionBounds {
  /** Original clock-local start timestamp in milliseconds. */
  readonly start: number;
  /** Initial reporter excluded from the confirmation set. */
  readonly reporter: string;
  /** Distinct accepted reporters, owned exclusively by the manager. */
  readonly confirmationReporters: Set<string>;
  /** Current absolute clock-local deadline in milliseconds. */
  deadline: number;
  /** Generation token invalidating callbacks from cancelled or replaced timers. */
  revision: number;
  /** Currently scheduled clock handle, absent during setup and after removal. */
  timer: ClockTimer | undefined;
}

/**
 * Requires a positive integer that remains exact in JavaScript arithmetic.
 *
 * @throws {RangeError} If `value` is not a positive safe integer.
 */
function requirePositiveSafeInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${field} must be a positive safe integer`);
  }
}

/**
 * Requires a non-negative integer that remains exact in JavaScript arithmetic.
 *
 * @throws {RangeError} If `value` is not a non-negative safe integer.
 */
function requireNonNegativeSafeInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${field} must be a non-negative safe integer`);
  }
}

/**
 * Enforces the membership wire format's unsigned 32-bit incarnation range.
 *
 * @throws {RangeError} If `value` is not an integer in `[0, MAX_INCARNATION]`.
 */
function requireIncarnation(value: number): void {
  requireNonNegativeSafeInteger(value, "incarnation");
  if (value > MAX_INCARNATION) {
    throw new RangeError(`incarnation must not exceed ${MAX_INCARNATION}`);
  }
}

/**
 * Requires an exact integer timestamp or deadline.
 *
 * Negative values are permitted because injected clocks may use an arbitrary
 * epoch.
 *
 * @throws {RangeError} If `value` is not a safe integer.
 */
function requireSafeTime(value: number, field: string): void {
  if (!Number.isSafeInteger(value)) {
    throw new RangeError(`${field} must be a safe integer`);
  }
}

/**
 * Calculates Lifeguard suspicion-duration bounds in milliseconds.
 *
 * The formula is
 * `minimum = 4 × ceil(log10(max(1, memberCount) + 1)) × effectivePeriod`
 * and `maximum = 6 × minimum`. Treating zero members as one keeps the result
 * positive while preserving the first logarithmic bucket. Results must remain
 * safe integers.
 *
 * @param memberCount Non-negative safe-integer alive-or-suspect count,
 * including self.
 * @param effectivePeriod Positive safe-integer probe period in milliseconds.
 * @returns Newly allocated immutable-by-contract duration bounds.
 * @throws {RangeError} If an input is outside its documented domain or either
 * computed bound is not a positive safe integer.
 * @internal
 */
export function suspicionBounds(memberCount: number, effectivePeriod: number): SuspicionBounds {
  requireNonNegativeSafeInteger(memberCount, "member count");
  requirePositiveSafeInteger(effectivePeriod, "effective period");
  const count: number = Math.max(1, memberCount);
  const minimum: number = SUSPICION_MULTIPLIER * Math.ceil(Math.log10(count + 1)) * effectivePeriod;
  const maximum: number = SUSPICION_MAX_MULTIPLIER * minimum;
  requirePositiveSafeInteger(minimum, "minimum suspicion timeout");
  requirePositiveSafeInteger(maximum, "maximum suspicion timeout");
  return { minimum, maximum };
}

/**
 * Calculates the Lifeguard deadline for a confirmation count.
 *
 * For `0 < C < K`, the duration is
 * `maximum - floor((maximum - minimum) × ln(C + 1) / ln(K + 1))`.
 * `C = 0` uses `maximum`, while `C >= K` uses `minimum`. The returned deadline
 * is always `start + duration`: confirmations shorten the original window and
 * never restart it from confirmation time. Counts above the target are capped,
 * matching the manager's distinct-reporter cap.
 *
 * @param start Original safe-integer clock timestamp in milliseconds.
 * @param minimum Positive safe-integer minimum duration in milliseconds.
 * @param maximum Positive safe-integer maximum duration, at least `minimum`.
 * @param confirmationCount Non-negative safe-integer accepted count.
 * @param confirmationTarget Positive safe-integer `K`; defaults to the
 * canonical target.
 * @returns Absolute safe-integer deadline in the same clock domain as `start`.
 * @throws {RangeError} If an argument violates its domain, `maximum` is below
 * `minimum`, or adding the duration exceeds safe-integer precision.
 * @internal
 */
export function suspicionDeadline(
  start: number,
  minimum: number,
  maximum: number,
  confirmationCount: number,
  confirmationTarget: number = SUSPICION_CONFIRMATION_TARGET,
): number {
  requireSafeTime(start, "start");
  requirePositiveSafeInteger(minimum, "minimum suspicion timeout");
  requirePositiveSafeInteger(maximum, "maximum suspicion timeout");
  requireNonNegativeSafeInteger(confirmationCount, "confirmation count");
  requirePositiveSafeInteger(confirmationTarget, "confirmation target");
  if (maximum < minimum) {
    throw new RangeError("maximum suspicion timeout must not be less than minimum");
  }

  const count: number = Math.min(confirmationCount, confirmationTarget);
  if (count === 0) {
    const deadline: number = start + maximum;
    requireSafeTime(deadline, "suspicion deadline");
    return deadline;
  }

  if (count === confirmationTarget) {
    const deadline: number = start + minimum;
    requireSafeTime(deadline, "suspicion deadline");
    return deadline;
  }

  const decay: number =
    ((maximum - minimum) * Math.log(count + 1)) / Math.log(confirmationTarget + 1);
  const duration: number = maximum - Math.floor(decay);
  const deadline: number = start + duration;
  requireSafeTime(deadline, "suspicion deadline");
  return deadline;
}

/**
 * Copies manager-owned state into a detached public snapshot.
 *
 * @returns A new object and reporter array with no mutable aliases to storage.
 */
function snapshot(suspicion: StoredSuspicion): ActiveSuspicion {
  return {
    member: suspicion.member,
    incarnation: suspicion.incarnation,
    start: suspicion.start,
    deadline: suspicion.deadline,
    minimum: suspicion.minimum,
    maximum: suspicion.maximum,
    reporter: suspicion.reporter,
    confirmationCount: suspicion.confirmationReporters.size,
    confirmationReporters: Array.from(suspicion.confirmationReporters),
  };
}

/**
 * Owns active suspicion state and deadlines without ambient time.
 *
 * At most one suspicion is stored per member. A higher incarnation atomically
 * replaces an older one; equal or lower starts leave existing timing and
 * evidence untouched. Independent confirmations reduce the absolute deadline
 * according to {@link suspicionDeadline}. The expiry callback is the engine
 * seam for applying and broadcasting dead at the supplied incarnation.
 *
 * @internal
 */
export class SuspicionManager {
  /** Injected clock that owns every timer and defines the timestamp domain. */
  readonly #clock: Clock;
  /** Consumer callback invoked synchronously after expired state is removed. */
  readonly #onExpire: (expiry: SuspicionExpiry) => void;
  /** Manager-owned active records keyed by member identity. */
  readonly #active: Map<string, StoredSuspicion> = new Map<string, StoredSuspicion>();

  /**
   * Creates an empty manager.
   *
   * The manager retains both dependencies. `onExpire` is called at most once
   * for each accepted suspicion unless it is cancelled or replaced; exceptions
   * from it propagate to the timer turn or confirming call that triggered
   * immediate expiry.
   *
   * @param clock Source of timestamps and owner of scheduled handles.
   * @param onExpire Synchronous terminal callback for the expired identity.
   */
  constructor(clock: Clock, onExpire: (expiry: SuspicionExpiry) => void) {
    this.#clock = clock;
    this.#onExpire = onExpire;
  }

  /** Number of member identities with a currently active suspicion. */
  get size(): number {
    return this.#active.size;
  }

  /**
   * Reads an active suspicion without exposing manager-owned mutable state.
   *
   * @param member Exact member identity used as the map key.
   * @returns A detached snapshot, or `undefined` when no suspicion is active.
   */
  get(member: string): ActiveSuspicion | undefined {
    const active: StoredSuspicion | undefined = this.#active.get(member);
    return active === undefined ? undefined : snapshot(active);
  }

  /**
   * Opens a suspicion or replaces one at a lower incarnation.
   *
   * Equal and stale starts return `false` and preserve the original reporter,
   * start, confirmations, and deadline. An accepted start captures `clock.now`,
   * derives fixed bounds, installs one timer, and only then reports success. A
   * replacement cancels and invalidates the previous timer.
   *
   * @param input Identity, evidence origin, and captured timing inputs.
   * @returns `true` when a new record was installed; `false` for an equal or
   * lower incarnation already tracked for the member.
   * @throws {RangeError} If the incarnation or either identity is invalid. For
   * an otherwise accepted start, also throws if the clock reading, member
   * count, effective period, or derived timing arithmetic is invalid.
   */
  start(input: SuspicionStart): boolean {
    requireIncarnation(input.incarnation);
    if (input.member.length === 0) {
      throw new RangeError("member must not be empty");
    }

    if (input.reporter.length === 0) {
      throw new RangeError("reporter must not be empty");
    }

    const current: StoredSuspicion | undefined = this.#active.get(input.member);
    if (current !== undefined && input.incarnation <= current.incarnation) {
      return false;
    }

    const start: number = this.#clock.now();
    requireSafeTime(start, "clock time");
    const bounds: SuspicionBounds = suspicionBounds(input.memberCount, input.effectivePeriod);
    const active: StoredSuspicion = {
      member: input.member,
      incarnation: input.incarnation,
      reporter: input.reporter,
      start,
      minimum: bounds.minimum,
      maximum: bounds.maximum,
      confirmationReporters: new Set<string>(),
      deadline: suspicionDeadline(start, bounds.minimum, bounds.maximum, 0),
      revision: 0,
      timer: undefined,
    };

    if (current !== undefined) {
      this.#cancelTimer(current);
    }

    this.#active.set(input.member, active);
    this.#schedule(active);
    return true;
  }

  /**
   * Accepts one independent confirmation and reduces the active deadline.
   *
   * The original reporter, duplicate reporters, mismatched incarnations,
   * missing suspicions, and confirmations beyond the target return `false`
   * without mutation. An accepted confirmation cancels the old timer and
   * either schedules the reduced remainder or expires synchronously if the new
   * absolute deadline has already passed. Reporter strings are treated as
   * opaque identities; non-emptiness is enforced by the caller's protocol
   * boundary, not here.
   *
   * @param member Member whose active suspicion should be confirmed.
   * @param incarnation Exact incarnation that the evidence confirms.
   * @param reporter Identity of the independent evidence source.
   * @returns `true` only when the reporter was accepted, including when that
   * acceptance caused immediate expiry.
   * @throws {RangeError} If `incarnation` is outside the wire-format range or
   * recomputed deadline arithmetic is invalid.
   * @throws Any error raised by the expiry callback on immediate expiry.
   */
  confirm(member: string, incarnation: number, reporter: string): boolean {
    requireIncarnation(incarnation);
    const active: StoredSuspicion | undefined = this.#active.get(member);
    if (
      active === undefined ||
      active.incarnation !== incarnation ||
      reporter === active.reporter ||
      active.confirmationReporters.has(reporter) ||
      active.confirmationReporters.size >= SUSPICION_CONFIRMATION_TARGET
    ) {
      return false;
    }

    active.confirmationReporters.add(reporter);
    active.deadline = suspicionDeadline(
      active.start,
      active.minimum,
      active.maximum,
      active.confirmationReporters.size,
    );
    this.#cancelTimer(active);
    active.revision += 1;
    if (active.deadline <= this.#clock.now()) {
      this.#expire(active, active.revision);
      return true;
    }

    this.#schedule(active);
    return true;
  }

  /**
   * Removes a suspicion superseded by truth at the same or newer incarnation.
   *
   * A lower supplied incarnation cannot refute newer suspect evidence. On
   * success the record is removed before its timer is cancelled and stale
   * callbacks are invalidated.
   *
   * @param member Member whose suspicion may be superseded.
   * @param incarnation Incarnation of the alive or terminal truth.
   * @returns `true` when an active suspicion was removed; otherwise `false`.
   * @throws {RangeError} If `incarnation` is outside the wire-format range.
   */
  cancelThrough(member: string, incarnation: number): boolean {
    requireIncarnation(incarnation);
    const active: StoredSuspicion | undefined = this.#active.get(member);
    if (active === undefined || active.incarnation > incarnation) {
      return false;
    }

    this.#active.delete(member);
    this.#cancelTimer(active);
    active.revision += 1;
    return true;
  }

  /**
   * Removes all active suspicions and invalidates every pending callback.
   *
   * The operation is idempotent and never invokes the expiry callback.
   */
  cancelAll(): void {
    for (const active of this.#active.values()) {
      this.#cancelTimer(active);
      active.revision += 1;
    }

    this.#active.clear();
  }

  /**
   * Schedules the record's current deadline and captures its revision.
   *
   * The injected clock owns the resulting timer handle. A deadline that passed
   * between the caller's check and this reading clamps to an immediate timer
   * rather than faulting the packet-processing path with a negative delay.
   */
  #schedule(active: StoredSuspicion): void {
    const revision: number = active.revision;
    const delay: number = Math.max(0, active.deadline - this.#clock.now());
    active.timer = this.#clock.schedule(delay, (): void => {
      this.#expire(active, revision);
    });
  }

  /** Cancels and releases the timer owned by an installed active record. */
  #cancelTimer(active: StoredSuspicion): void {
    if (active.timer !== undefined) {
      this.#clock.cancel(active.timer);
      active.timer = undefined;
    }
  }

  /**
   * Commits expiry only for the current record, timer revision, and deadline.
   *
   * Stale callbacks are ignored. A callback that fires before the recorded
   * deadline reschedules the remainder instead of orphaning the suspicion,
   * because its host timer has already been consumed. Valid expiry removes
   * manager-owned state before invoking the consumer, preventing reentrant
   * observation of the expired record.
   */
  #expire(active: StoredSuspicion, revision: number): void {
    if (this.#active.get(active.member) !== active || active.revision !== revision) {
      return;
    }

    if (this.#clock.now() < active.deadline) {
      this.#schedule(active);
      return;
    }

    this.#active.delete(active.member);
    active.timer = undefined;
    this.#onExpire({ member: active.member, incarnation: active.incarnation });
  }
}
