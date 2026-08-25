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
import {
  compareMembershipUpdates,
  IncarnationExhaustedError,
  MAX_INCARNATION,
  MembershipCapacityError,
  type MembershipEvent,
  MembershipView,
  TERMINAL_RETENTION_MS,
} from "../../src/membership/view";
import {
  MAX_MEMBERS,
  type MemberState,
  type MembershipUpdate,
  STATE_ALIVE,
  STATE_DEAD,
  STATE_LEFT,
  STATE_SUSPECT,
} from "../../src/membership/wire";

const states = [STATE_ALIVE, STATE_SUSPECT, STATE_DEAD, STATE_LEFT] as const;

function update(
  state: MemberState,
  incarnation = 7,
  member = "member",
  metadata: Uint8Array = Uint8Array.of(incarnation),
  reporter = "reporter",
): MembershipUpdate {
  return {
    state,
    selfOriginated: state === STATE_ALIVE || state === STATE_LEFT,
    incarnation,
    stateChangeTime: BigInt(incarnation * 10),
    member,
    reporter: state === STATE_SUSPECT ? reporter : "",
    metadata: state === STATE_ALIVE ? metadata : new Uint8Array(0),
  };
}

describe("membership precedence", () => {
  it("exhaustively compares every state and incarnation ordering", () => {
    for (const storedState of states) {
      for (const incomingState of states) {
        for (const delta of [-1, 0, 1]) {
          const view = new MembershipView("self");
          view.apply(update(storedState, 7), 10);
          const incoming = update(incomingState, 7 + delta);
          const result = view.apply(incoming, 20);
          const replaces = delta > 0 || (delta === 0 && incomingState > storedState);

          expect(result.kind, `${storedState}/${incomingState}/${delta}`).toBe(
            replaces ? "applied" : "ignored",
          );
          const stored = view.get("member");
          expect(stored?.state).toBe(replaces ? incomingState : storedState);
          expect(stored?.incarnation).toBe(replaces ? 7 + delta : 7);
          expect(stored?.appliedAt).toBe(replaces ? 20 : 10);
        }
      }
    }
  });

  it("converges on left in both dead/left arrival orders", () => {
    for (const order of [
      [STATE_DEAD, STATE_LEFT],
      [STATE_LEFT, STATE_DEAD],
    ] as const) {
      const view = new MembershipView("self");
      for (const state of order) {
        view.apply(update(state), 1);
      }
      expect(view.get("member")?.state).toBe(STATE_LEFT);
    }
  });

  it("exports the incarnation-first comparison helper", () => {
    expect(compareMembershipUpdates(update(STATE_ALIVE, 8), update(STATE_LEFT, 7))).toBe(1);
    expect(compareMembershipUpdates(update(STATE_DEAD), update(STATE_LEFT))).toBe(-1);
    expect(compareMembershipUpdates(update(STATE_DEAD), update(STATE_DEAD))).toBe(0);
  });

  it("keeps accepted appliedAt locally monotonic", () => {
    const view = new MembershipView("self");
    view.apply(update(STATE_ALIVE, 1), 20);
    view.apply(update(STATE_SUSPECT, 1), 10);
    view.apply(update(STATE_ALIVE, 2), 30);
    expect(view.get("member")?.appliedAt).toBe(30);

    view.apply(update(STATE_SUSPECT, 2), 25);
    expect(view.get("member")?.appliedAt).toBe(30);
  });
});

describe("membership events", () => {
  it("emits public events synchronously in local apply order", () => {
    const events: MembershipEvent[] = [];
    const view = new MembershipView("self", (event: MembershipEvent): void => {
      events.push(event);
    });

    view.apply(update(STATE_ALIVE, 1, "a", Uint8Array.of(1)), 1);
    view.apply(update(STATE_ALIVE, 2, "a", Uint8Array.of(2)), 2);
    view.apply(update(STATE_SUSPECT, 2, "a"), 3);
    view.apply(update(STATE_DEAD, 2, "a"), 4);
    view.apply(update(STATE_LEFT, 2, "a"), 5);
    view.apply(update(STATE_ALIVE, 3, "a", Uint8Array.of(3)), 6);

    expect(events.map((event: MembershipEvent): string => event.type)).toEqual([
      "joined",
      "updated",
      "dead",
      "left",
      "joined",
    ]);
    expect(events.map((event: MembershipEvent): number => event.member.appliedAt)).toEqual([
      1, 2, 4, 5, 6,
    ]);
  });

  it("does not emit updated when alive metadata is unchanged", () => {
    const events: MembershipEvent[] = [];
    const view = new MembershipView("self", (event: MembershipEvent): void => {
      events.push(event);
    });
    view.apply(update(STATE_ALIVE, 1, "a", Uint8Array.of(1)), 1);
    const result = view.apply(update(STATE_ALIVE, 2, "a", Uint8Array.of(1)), 2);

    expect(result.kind).toBe("applied");
    expect(events.map((event: MembershipEvent): string => event.type)).toEqual(["joined"]);
  });

  it("emits updated when alive metadata length changes", () => {
    const events: MembershipEvent[] = [];
    const view = new MembershipView("self", (event: MembershipEvent): void => {
      events.push(event);
    });
    view.apply(update(STATE_ALIVE, 1, "a", Uint8Array.of(1)), 1);
    view.apply(update(STATE_ALIVE, 2, "a", Uint8Array.of(1, 2)), 2);

    expect(events.map((event: MembershipEvent): string => event.type)).toEqual([
      "joined",
      "updated",
    ]);
  });

  it("isolates callback and returned event metadata from stored truth", () => {
    let callbackEvent: MembershipEvent | undefined;
    const view = new MembershipView("self", (event: MembershipEvent): void => {
      callbackEvent = event;
      event.member.metadata[0] = 91;
    });
    const result = view.apply(update(STATE_ALIVE, 1, "a", Uint8Array.of(4)), 1);
    if (result.kind !== "applied") {
      throw new Error("expected apply");
    }
    result.record.metadata[0] = 92;
    result.event?.member.metadata.fill(93);

    expect(callbackEvent?.member.metadata).toEqual(Uint8Array.of(91));
    expect(view.get("a")?.metadata).toEqual(Uint8Array.of(4));
  });
});

describe("suspicion confirmations", () => {
  it("returns confirmations once per distinct reporter without replacing truth", () => {
    const view = new MembershipView("self");
    view.apply(update(STATE_SUSPECT, 4, "a", undefined, "first"), 10);

    const confirmation = view.apply(update(STATE_SUSPECT, 4, "a", undefined, "second"), 20);
    expect(confirmation).toEqual({
      kind: "confirmed",
      member: "a",
      incarnation: 4,
      reporter: "second",
      confirmationCount: 1,
    });
    expect(view.get("a")?.reporter).toBe("first");
    expect(view.get("a")?.appliedAt).toBe(10);
    expect(view.confirmationReporters("a")).toEqual(["first", "second"]);

    expect(view.apply(update(STATE_SUSPECT, 4, "a", undefined, "second"), 30)).toEqual({
      kind: "ignored",
    });
    expect(view.apply(update(STATE_SUSPECT, 3, "a", undefined, "third"), 30)).toEqual({
      kind: "ignored",
    });
  });

  it("resets reporters when a higher-incarnation suspicion starts", () => {
    const view = new MembershipView("self");
    view.apply(update(STATE_SUSPECT, 4, "a", undefined, "first"), 1);
    view.apply(update(STATE_SUSPECT, 4, "a", undefined, "second"), 2);
    view.apply(update(STATE_SUSPECT, 5, "a", undefined, "third"), 3);

    expect(view.confirmationReporters("a")).toEqual(["third"]);
  });

  it("returns no confirmation reporters for absent and non-suspect records", () => {
    const view = new MembershipView("self");
    expect(view.confirmationReporters("missing")).toEqual([]);
    view.apply(update(STATE_ALIVE, 1, "a"), 1);
    expect(view.confirmationReporters("a")).toEqual([]);
  });
});

describe("self-refutation", () => {
  it("refutes an accusation before self has joined", () => {
    const view = new MembershipView("self");
    const result = view.apply(update(STATE_SUSPECT, 3, "self"), 10, 20n);

    expect(result.kind).toBe("refuted");
    expect(view.self()).toMatchObject({
      state: STATE_ALIVE,
      incarnation: 4,
      metadata: new Uint8Array(0),
    });
  });

  it("does not preserve metadata from a non-alive self record", () => {
    const view = new MembershipView("self");
    view.applyLocal(update(STATE_SUSPECT, 3, "self"), 1);
    view.apply(update(STATE_DEAD, 3, "self"), 2);

    expect(view.self()).toMatchObject({
      state: STATE_ALIVE,
      incarnation: 4,
      metadata: new Uint8Array(0),
    });
  });

  it("refutes equal and higher accusations above the greatest incarnation", () => {
    for (const accusation of [STATE_SUSPECT, STATE_DEAD, STATE_LEFT] as const) {
      const view = new MembershipView("self");
      view.apply(update(STATE_ALIVE, 4, "self", Uint8Array.of(8)), 1);

      const equal = view.apply(update(accusation, 4, "self"), 2, 200n);
      expect(equal.kind).toBe("refuted");
      expect(view.self()).toMatchObject({
        state: STATE_ALIVE,
        incarnation: 5,
        stateChangeTime: 200n,
        metadata: Uint8Array.of(8),
      });

      const higher = view.apply(update(accusation, 8, "self"), 3, 300n);
      expect(higher.kind).toBe("refuted");
      expect(view.self()).toMatchObject({ state: STATE_ALIVE, incarnation: 9 });
    }
  });

  it("ignores a lower-incarnation self accusation", () => {
    const view = new MembershipView("self");
    view.apply(update(STATE_ALIVE, 4, "self"), 1);
    expect(view.apply(update(STATE_DEAD, 3, "self"), 2)).toEqual({ kind: "ignored" });
    expect(view.self()?.incarnation).toBe(4);
  });

  it("allows a locally originated graceful leave without refuting it", () => {
    const view = new MembershipView("self");
    view.applyLocal(update(STATE_ALIVE, 4, "self"), 1);
    const result = view.applyLocal(update(STATE_LEFT, 4, "self"), 2);

    expect(result.kind).toBe("applied");
    expect(view.self()).toMatchObject({ state: STATE_LEFT, incarnation: 4 });
  });

  it("throws a typed overflow without changing truth", () => {
    const view = new MembershipView("self");
    view.apply(update(STATE_ALIVE, MAX_INCARNATION, "self"), 1);

    expect(() => view.apply(update(STATE_DEAD, MAX_INCARNATION, "self"), 2)).toThrow(
      IncarnationExhaustedError,
    );
    expect(view.self()).toMatchObject({ state: STATE_ALIVE, incarnation: MAX_INCARNATION });
  });
});

describe("retention and snapshots", () => {
  it("exposes empty-view diagnostics and counts only probe-eligible records", () => {
    const view = new MembershipView("self");
    expect(view.size).toBe(0);
    expect(view.selfName).toBe("self");
    expect(view.get("missing")).toBeUndefined();
    expect(view.isGossipEligible("missing", 0)).toBe(false);
    expect(view.reapOperation("missing")).toBeUndefined();

    view.apply(update(STATE_ALIVE, 1, "alive"), 1);
    view.apply(update(STATE_SUSPECT, 1, "suspect"), 1);
    view.apply(update(STATE_DEAD, 1, "dead"), 1);
    expect(view.aliveOrSuspectCount()).toBe(2);
    expect(view.reapOperation("alive")).toBeUndefined();
    expect(view.dueReaps(TERMINAL_RETENTION_MS + 1)).toHaveLength(1);
  });

  it("routes stale local truth through the duplicate guard", () => {
    const view = new MembershipView("self");
    view.applyLocal(update(STATE_ALIVE, 2, "a"), 1);

    expect(view.applyLocal(update(STATE_ALIVE, 1, "a"), 2)).toEqual({ kind: "ignored" });
  });

  it("uses an inclusive transition and exclusive terminal expiry for gossip", () => {
    const view = new MembershipView("self");
    view.apply(update(STATE_DEAD, 1, "a"), 100);

    expect(view.isGossipEligible("a", 100)).toBe(true);
    expect(view.isGossipEligible("a", 100 + TERMINAL_RETENTION_MS - 1)).toBe(true);
    expect(view.isGossipEligible("a", 100 + TERMINAL_RETENTION_MS)).toBe(false);
    expect(view.reapOperation("a")).toEqual({
      member: "a",
      state: STATE_DEAD,
      incarnation: 1,
      dueAt: 100 + TERMINAL_RETENTION_MS,
    });
    expect(view.dueReaps(100 + TERMINAL_RETENTION_MS - 1)).toEqual([]);
    expect(view.dueReaps(100 + TERMINAL_RETENTION_MS)).toEqual([
      {
        member: "a",
        state: STATE_DEAD,
        incarnation: 1,
        dueAt: 100 + TERMINAL_RETENTION_MS,
      },
    ]);
  });

  it("rejects stale reap operations after truth changes", () => {
    const view = new MembershipView("self");
    view.apply(update(STATE_DEAD, 1, "a"), 100);
    const stale = view.dueReaps(100 + TERMINAL_RETENTION_MS)[0];
    if (stale === undefined) {
      throw new Error("expected due reap");
    }
    view.apply(update(STATE_ALIVE, 2, "a"), 200);

    expect(view.reap(stale)).toBe(false);
    expect(view.get("a")?.state).toBe(STATE_ALIVE);
  });

  it("reaps only the matching terminal record and emits no event", () => {
    const events: MembershipEvent[] = [];
    const view = new MembershipView("self", (event: MembershipEvent): void => {
      events.push(event);
    });
    view.apply(update(STATE_LEFT, 1, "a"), 100);
    const operation = view.dueReaps(100 + TERMINAL_RETENTION_MS)[0];

    expect(operation === undefined ? false : view.reap(operation)).toBe(true);
    expect(view.get("a")).toBeUndefined();
    expect(events.map((event: MembershipEvent): string => event.type)).toEqual(["left"]);
    expect(operation === undefined ? false : view.reap(operation)).toBe(false);
  });

  it("rejects reap operations whose incarnation or deadline changed", () => {
    const view = new MembershipView("self");
    view.apply(update(STATE_DEAD, 2, "a"), 100);
    const operation = view.reapOperation("a");
    if (operation === undefined) {
      throw new Error("expected reap operation");
    }

    expect(view.reap({ ...operation, incarnation: 1 })).toBe(false);
    expect(view.reap({ ...operation, dueAt: operation.dueAt + 1 })).toBe(false);
    expect(view.get("a")).toBeDefined();
  });

  it("copies incoming metadata and every exposed record", () => {
    const metadata = Uint8Array.of(1, 2);
    const incoming = update(STATE_ALIVE, 1, "a", metadata);
    const view = new MembershipView("self");
    view.apply(incoming, 1);
    metadata[0] = 9;

    const getRecord = view.get("a");
    const membersRecord = view.members()[0];
    const updateRecord = view.updates()[0];
    if (getRecord === undefined || membersRecord === undefined || updateRecord === undefined) {
      throw new Error("expected snapshots");
    }
    getRecord.metadata[0] = 8;
    membersRecord.metadata[0] = 7;
    updateRecord.metadata[0] = 6;

    expect(view.get("a")?.metadata).toEqual(Uint8Array.of(1, 2));
  });

  it("enforces the retained membership capacity", () => {
    const view = new MembershipView("self");
    for (let index = 0; index < MAX_MEMBERS; index += 1) {
      view.apply(update(STATE_ALIVE, 1, `member-${index}`), index);
    }

    expect(view.size).toBe(MAX_MEMBERS);
    expect(() => view.apply(update(STATE_ALIVE, 1, "overflow"), MAX_MEMBERS)).toThrow(
      MembershipCapacityError,
    );
  });
});
