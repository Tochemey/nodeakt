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

import { describe, expect, it } from "vitest";
import type { Clock, ClockTimer } from "../../src/membership/clock";
import {
  type SuspicionExpiry,
  SuspicionManager,
  type SuspicionStart,
  suspicionBounds,
  suspicionDeadline,
} from "../../src/membership/suspicion";
import { type ApplyResult, MembershipView } from "../../src/membership/view";
import { type MembershipUpdate, STATE_ALIVE, STATE_SUSPECT } from "../../src/membership/wire";
import { SimClock } from "./sim";

function suspect(member: string, incarnation: number, reporter: string): MembershipUpdate {
  return {
    state: STATE_SUSPECT,
    selfOriginated: false,
    incarnation,
    stateChangeTime: 0n,
    member,
    reporter,
    metadata: new Uint8Array(0),
  };
}

function alive(member: string): MembershipUpdate {
  return {
    state: STATE_ALIVE,
    selfOriginated: true,
    incarnation: 0,
    stateChangeTime: 0n,
    member,
    reporter: "",
    metadata: new Uint8Array(0),
  };
}

function start(
  manager: SuspicionManager,
  incarnation: number = 4,
  reporter: string = "original",
  memberCount: number = 1,
  effectivePeriod: number = 1_000,
): boolean {
  return manager.start({
    member: "target",
    incarnation,
    reporter,
    memberCount,
    effectivePeriod,
  });
}

class InspectableClock implements Clock {
  readonly inner: SimClock = new SimClock();
  readonly callbacks: Array<() => void> = [];

  now(): number {
    return this.inner.now();
  }

  epochMilliseconds(): number {
    return this.inner.epochMilliseconds();
  }

  schedule(delayMs: number, callback: () => void): ClockTimer {
    this.callbacks.push(callback);
    return this.inner.schedule(delayMs, callback);
  }

  cancel(timer: ClockTimer): void {
    this.inner.cancel(timer);
  }
}

describe("suspicion arithmetic", () => {
  it("uses exact worked member-count windows and the captured effective period", () => {
    expect(suspicionBounds(0, 1_000)).toEqual({ minimum: 4_000, maximum: 24_000 });
    expect(suspicionBounds(1, 1_000)).toEqual({ minimum: 4_000, maximum: 24_000 });
    expect(suspicionBounds(9, 1_000)).toEqual({ minimum: 4_000, maximum: 24_000 });
    expect(suspicionBounds(10, 1_000)).toEqual({ minimum: 8_000, maximum: 48_000 });
    expect(suspicionBounds(99, 1_000)).toEqual({ minimum: 8_000, maximum: 48_000 });
    expect(suspicionBounds(100, 1_000)).toEqual({ minimum: 12_000, maximum: 72_000 });
    expect(suspicionBounds(10, 3_000)).toEqual({ minimum: 24_000, maximum: 144_000 });
  });

  it("computes every K=3 deadline from the original start", () => {
    expect(suspicionDeadline(100, 4_000, 24_000, 0)).toBe(24_100);
    expect(suspicionDeadline(100, 4_000, 24_000, 1)).toBe(14_100);
    expect(suspicionDeadline(100, 4_000, 24_000, 2)).toBe(8_251);
    expect(suspicionDeadline(100, 4_000, 24_000, 3)).toBe(4_100);
    expect(suspicionDeadline(100, 4_000, 24_000, 99)).toBe(4_100);
  });

  it("handles an exposed K=1 edge and rejects invalid arithmetic inputs", () => {
    expect(suspicionDeadline(50, 10, 60, 0, 1)).toBe(110);
    expect(suspicionDeadline(50, 10, 60, 1, 1)).toBe(60);
    expect(() => suspicionDeadline(0, 10, 60, 0, 0)).toThrow(RangeError);
    expect(() => suspicionBounds(1, 0)).toThrow(RangeError);
    expect(() => suspicionBounds(-1, 1_000)).toThrow(RangeError);
    expect(() => suspicionDeadline(Number.NaN, 10, 60, 0)).toThrow(RangeError);
    expect(() => suspicionDeadline(0, 60, 10, 0)).toThrow(RangeError);
    expect(() => suspicionDeadline(Number.MAX_SAFE_INTEGER, 10, 60, 0)).toThrow(RangeError);
  });
});

describe("suspicion confirmations", () => {
  it("composes with view confirmations and excludes duplicate and original reporters", () => {
    const clock: SimClock = new SimClock();
    const manager: SuspicionManager = new SuspicionManager(clock, (): void => {});
    const view: MembershipView = new MembershipView("self");
    view.applyLocal(alive("self"), clock.now());
    view.applyLocal(alive("peer"), clock.now());
    view.apply(suspect("target", 4, "original"), clock.now());

    start(manager, 4, "original", view.aliveOrSuspectCount());
    for (const reporter of ["original", "second", "second", "third"]) {
      const result: ApplyResult = view.apply(suspect("target", 4, reporter), clock.now());
      if (result.kind === "confirmed") {
        manager.confirm(result.member, result.incarnation, result.reporter);
      }
    }

    expect(manager.get("target")).toMatchObject({
      confirmationCount: 2,
      confirmationReporters: ["second", "third"],
    });
    expect(view.get("target")?.reporter).toBe("original");
  });

  it("caps confirmations at three distinct later reporters", () => {
    const clock: SimClock = new SimClock();
    const manager: SuspicionManager = new SuspicionManager(clock, (): void => {});
    start(manager);

    expect(manager.confirm("target", 4, "one")).toBe(true);
    expect(manager.confirm("target", 4, "two")).toBe(true);
    expect(manager.confirm("target", 4, "three")).toBe(true);
    expect(manager.confirm("target", 4, "four")).toBe(false);
    expect(manager.confirm("target", 3, "stale")).toBe(false);
    expect(manager.get("target")).toMatchObject({
      confirmationCount: 3,
      confirmationReporters: ["one", "two", "three"],
      deadline: 4_000,
    });
  });

  it("reduces absolute deadlines without restarting from confirmation time", () => {
    const clock: SimClock = new SimClock();
    const manager: SuspicionManager = new SuspicionManager(clock, (): void => {});
    clock.advanceTo(100);
    start(manager);

    clock.advanceTo(5_000);
    manager.confirm("target", 4, "one");
    expect(manager.get("target")?.deadline).toBe(14_100);

    clock.advanceTo(6_000);
    manager.confirm("target", 4, "two");
    expect(manager.get("target")?.deadline).toBe(8_251);
  });

  it("expires immediately when a reduced absolute deadline is already past", () => {
    const clock: SimClock = new SimClock();
    const expiries: SuspicionExpiry[] = [];
    const manager: SuspicionManager = new SuspicionManager(
      clock,
      (expiry: SuspicionExpiry): void => {
        expiries.push(expiry);
      },
    );
    start(manager);

    clock.advanceTo(10_000);
    expect(manager.confirm("target", 4, "one")).toBe(true);
    expect(manager.confirm("target", 4, "two")).toBe(true);

    expect(expiries).toEqual([{ member: "target", incarnation: 4 }]);
    expect(manager.get("target")).toBeUndefined();
    expect(clock.pending).toBe(0);
  });
});

describe("suspicion lifecycle guards", () => {
  it("expires at the deterministic maximum and exposes the dead/broadcast seam", () => {
    const clock: SimClock = new SimClock();
    const expiries: SuspicionExpiry[] = [];
    const manager: SuspicionManager = new SuspicionManager(
      clock,
      (expiry: SuspicionExpiry): void => {
        expiries.push(expiry);
      },
    );
    start(manager);

    clock.advanceTo(23_999);
    expect(expiries).toEqual([]);
    clock.advanceTo(24_000);
    expect(expiries).toEqual([{ member: "target", incarnation: 4 }]);
    expect(manager.size).toBe(0);
  });

  it("cancels only through matching or newer superseding incarnations", () => {
    const clock: SimClock = new SimClock();
    const expiries: SuspicionExpiry[] = [];
    const manager: SuspicionManager = new SuspicionManager(
      clock,
      (expiry: SuspicionExpiry): void => {
        expiries.push(expiry);
      },
    );
    start(manager, 5);

    expect(manager.cancelThrough("target", 4)).toBe(false);
    expect(manager.get("target")?.incarnation).toBe(5);
    expect(manager.cancelThrough("target", 5)).toBe(true);
    expect(manager.cancelThrough("target", 6)).toBe(false);
    clock.advanceTo(100_000);
    expect(expiries).toEqual([]);
  });

  it("replaces only with a higher incarnation and preserves equal starts", () => {
    const clock: SimClock = new SimClock();
    const manager: SuspicionManager = new SuspicionManager(clock, (): void => {});
    expect(start(manager, 4, "first")).toBe(true);
    clock.advanceTo(1_000);
    expect(start(manager, 4, "equal")).toBe(false);
    expect(start(manager, 3, "older")).toBe(false);
    expect(manager.get("target")).toMatchObject({
      incarnation: 4,
      reporter: "first",
      start: 0,
      deadline: 24_000,
    });

    expect(() => start(manager, 5, "invalid", 1, 0)).toThrow(RangeError);
    expect(() => start(manager, 0x1_0000_0000, "invalid")).toThrow(RangeError);
    expect(manager.get("target")?.incarnation).toBe(4);

    expect(start(manager, 5, "newer")).toBe(true);
    expect(manager.get("target")).toMatchObject({
      incarnation: 5,
      reporter: "newer",
      start: 1_000,
      deadline: 25_000,
    });
    expect(clock.pending).toBe(1);
  });

  it("rejects empty member and reporter identities", () => {
    const clock: SimClock = new SimClock();
    const manager: SuspicionManager = new SuspicionManager(clock, (): void => {});
    const valid: SuspicionStart = {
      member: "target",
      incarnation: 1,
      reporter: "reporter",
      memberCount: 1,
      effectivePeriod: 1_000,
    };

    expect(() => manager.start({ ...valid, member: "" })).toThrow(RangeError);
    expect(() => manager.start({ ...valid, reporter: "" })).toThrow(RangeError);
  });

  it("ignores stale cancelled callbacks after reschedule, replacement, and cancellation", () => {
    const clock: InspectableClock = new InspectableClock();
    const expiries: SuspicionExpiry[] = [];
    const manager: SuspicionManager = new SuspicionManager(
      clock,
      (expiry: SuspicionExpiry): void => {
        expiries.push(expiry);
      },
    );
    start(manager, 4);
    const initial: (() => void) | undefined = clock.callbacks[0];
    if (initial === undefined) {
      throw new Error("expected initial timer callback");
    }

    manager.confirm("target", 4, "one");
    initial();
    expect(manager.get("target")?.incarnation).toBe(4);

    const reduced: (() => void) | undefined = clock.callbacks[1];
    if (reduced === undefined) {
      throw new Error("expected reduced timer callback");
    }
    start(manager, 5, "newer");
    reduced();
    expect(manager.get("target")?.incarnation).toBe(5);

    const replacement: (() => void) | undefined = clock.callbacks[2];
    if (replacement === undefined) {
      throw new Error("expected replacement timer callback");
    }
    manager.cancelThrough("target", 5);
    replacement();
    expect(expiries).toEqual([]);
  });

  it("reschedules a timer that fires before its recorded deadline", () => {
    const clock: InspectableClock = new InspectableClock();
    const expiries: SuspicionExpiry[] = [];
    const manager: SuspicionManager = new SuspicionManager(
      clock,
      (expiry: SuspicionExpiry): void => {
        expiries.push(expiry);
      },
    );
    start(manager, 4);
    const premature: (() => void) | undefined = clock.callbacks[0];
    if (premature === undefined) {
      throw new Error("expected initial timer callback");
    }

    // The host timer turn arrives before the recorded absolute deadline: the
    // remainder must be rescheduled instead of expiring or orphaning the record.
    premature();
    expect(expiries).toEqual([]);
    expect(manager.get("target")).toBeDefined();

    clock.inner.advanceBy(24_000);
    expect(expiries).toEqual([{ member: "target", incarnation: 4 }]);
  });

  it("cancels a record whose timer never armed because scheduling failed", () => {
    const clock: InspectableClock = new InspectableClock();
    let failNextSchedule: boolean = true;
    const throwingClock: Clock = {
      now: (): number => clock.now(),
      epochMilliseconds: (): number => clock.epochMilliseconds(),
      schedule: (delayMs: number, callback: () => void): ClockTimer => {
        if (failNextSchedule) {
          failNextSchedule = false;
          throw new RangeError("host timer unavailable");
        }

        return clock.schedule(delayMs, callback);
      },
      cancel: (timer: ClockTimer): void => {
        clock.cancel(timer);
      },
    };
    const manager: SuspicionManager = new SuspicionManager(throwingClock, (): void => {});

    expect((): void => {
      start(manager);
    }).toThrow("host timer unavailable");
    expect(manager.size).toBe(1);
    manager.cancelAll();
    expect(manager.size).toBe(0);
  });

  it("cancels every active timer idempotently", () => {
    const clock: SimClock = new SimClock();
    const manager: SuspicionManager = new SuspicionManager(clock, (): void => {});
    start(manager);
    manager.start({
      member: "other",
      incarnation: 1,
      reporter: "reporter",
      memberCount: 2,
      effectivePeriod: 1_000,
    });

    manager.cancelAll();
    manager.cancelAll();
    expect(manager.size).toBe(0);
    expect(clock.pending).toBe(0);
  });
});
