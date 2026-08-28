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
  BroadcastQueue,
  type BroadcastSelection,
  type BroadcastSnapshot,
} from "../../src/membership/broadcast";
import {
  type MemberState,
  type MembershipUpdate,
  membershipUpdateSize,
  STATE_ALIVE,
  STATE_DEAD,
  STATE_LEFT,
  STATE_SUSPECT,
} from "../../src/membership/wire";

function update(
  member: string,
  state: MemberState = STATE_ALIVE,
  incarnation: number = 1,
  metadata: Uint8Array = Uint8Array.of(incarnation),
): MembershipUpdate {
  return {
    state,
    selfOriginated: state === STATE_ALIVE || state === STATE_LEFT,
    incarnation,
    stateChangeTime: BigInt(incarnation),
    member,
    reporter: state === STATE_SUSPECT ? "reporter" : "",
    metadata: state === STATE_ALIVE ? metadata : new Uint8Array(0),
  };
}

function names(selection: { readonly updates: readonly MembershipUpdate[] }): readonly string[] {
  return selection.updates.map((item: MembershipUpdate): string => item.member);
}

describe("broadcast queue truth and budgets", () => {
  it("rejects invalid enqueue counts without replacing queued truth", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("a"), 1);

    for (const count of [-1, Number.NaN]) {
      expect(() => queue.enqueue(update("b"), count)).toThrow(RangeError);
    }
    expect(queue.size).toBe(1);
  });

  it("keeps one truth per member and refreshes only on supersession", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    expect(queue.enqueue(update("a", STATE_ALIVE, 2), 1)).toBe(true);
    const original: BroadcastSnapshot | undefined = queue.get("a");

    expect(queue.enqueue(update("a", STATE_ALIVE, 1), 99)).toBe(false);
    expect(queue.enqueue(update("a", STATE_ALIVE, 2, Uint8Array.of(9)), 99)).toBe(false);
    expect(queue.get("a")).toEqual(original);

    expect(queue.enqueue(update("a", STATE_SUSPECT, 2), 10)).toBe(true);
    expect(queue.get("a")).toMatchObject({
      update: { state: STATE_SUSPECT, incarnation: 2 },
      initialRemaining: 8,
      remaining: 8,
      transmissions: 0,
    });
  });

  it("captures the current member count for each fresh counter", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("small"), 1);
    queue.enqueue(update("large"), 10);

    expect(queue.get("small")?.initialRemaining).toBe(4);
    expect(queue.get("large")?.initialRemaining).toBe(8);
  });

  it("copies enqueue inputs and exposed snapshots", () => {
    const metadata: Uint8Array = Uint8Array.of(1, 2);
    const incoming: MembershipUpdate = update("safe", STATE_ALIVE, 1, metadata);
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(incoming, 1);
    metadata[0] = 8;

    const snapshot: BroadcastSnapshot | undefined = queue.get("safe");
    if (snapshot === undefined) {
      throw new Error("expected queued update");
    }
    snapshot.update.metadata[0] = 9;
    expect(queue.get("safe")?.update.metadata).toEqual(Uint8Array.of(1, 2));
  });
});

describe("broadcast confirmations", () => {
  function confirmation(member: string, incarnation: number, reporter: string): MembershipUpdate {
    return { ...update(member, STATE_SUSPECT, incarnation), reporter };
  }

  it("replaces equal-precedence suspect truth from a distinct accuser with a fresh budget", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(confirmation("a", 2, "first"), 1);
    const selection: BroadcastSelection = queue.pack(1_400);
    queue.acknowledge(selection, true);
    expect(queue.get("a")?.transmissions).toBe(1);

    expect(queue.enqueueConfirmation(confirmation("a", 2, "second"), 1)).toBe(true);
    expect(queue.get("a")).toMatchObject({
      update: { state: STATE_SUSPECT, incarnation: 2, reporter: "second" },
      transmissions: 0,
    });
  });

  it("rejects duplicate reporters, stale precedence, and non-suspect records", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(confirmation("a", 2, "first"), 1);

    expect(queue.enqueueConfirmation(confirmation("a", 2, "first"), 1)).toBe(false);
    expect(queue.enqueueConfirmation(confirmation("a", 1, "second"), 1)).toBe(false);
    expect(queue.enqueueConfirmation(update("a", STATE_ALIVE, 2), 1)).toBe(false);
    expect(queue.get("a")?.update.reporter).toBe("first");
  });

  it("re-enqueues a corroboration after the original budget drained", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    expect(queue.enqueueConfirmation(confirmation("a", 2, "second"), 0)).toBe(true);
    expect(queue.get("a")).toMatchObject({
      update: { reporter: "second" },
      initialRemaining: 4,
    });
  });

  it("accepts a corroboration that supersedes stale queued truth", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(confirmation("a", 1, "first"), 1);

    expect(queue.enqueueConfirmation(confirmation("a", 2, "second"), 1)).toBe(true);
    expect(queue.get("a")?.update).toMatchObject({ incarnation: 2, reporter: "second" });
  });
});

describe("broadcast packing order", () => {
  it("validates budgets, overheads, and record limits", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("a"), 1);

    for (const budget of [-1, Number.NaN]) {
      expect(() => queue.pack(budget)).toThrow(RangeError);
    }
    expect(() => queue.pack(100, { perRecordOverhead: -1 })).toThrow(RangeError);
    expect(() => queue.pack(100, { maxRecords: -1 })).toThrow(RangeError);
    expect(() => queue.pack(100, { maxRecords: 256 })).toThrow(RangeError);
  });

  it("stops at the record limit and omits a buddy that cannot fit", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("a"), 1);
    queue.enqueue(update("b"), 1);

    expect(names(queue.pack(10_000, { maxRecords: 1 }))).toEqual(["a"]);
    expect(names(queue.pack(1, { buddy: update("buddy", STATE_SUSPECT), maxRecords: 1 }))).toEqual(
      [],
    );
  });

  it("does not charge queued truth that differs from a buddy", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("target", STATE_ALIVE, 1), 1);
    const selection: BroadcastSelection = queue.pack(1_000, {
      buddy: update("target", STATE_DEAD, 1),
    });

    expect(names(selection)).toEqual(["target"]);
    queue.acknowledge(selection, true);
    expect(queue.get("target")?.remaining).toBe(4);
  });

  it("orders self-defense, fewer transmissions, then older records", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    const oldest: MembershipUpdate = update("oldest");
    const newer: MembershipUpdate = update("newer");
    const defense: MembershipUpdate = update("self", STATE_ALIVE, 3);
    queue.enqueue(oldest, 1);
    queue.enqueue(newer, 1);
    queue.enqueue(defense, 1, true);

    const first: BroadcastSelection = queue.pack(membershipUpdateSize(defense));
    expect(names(first)).toEqual(["self"]);
    queue.acknowledge(first, true);

    const second: BroadcastSelection = queue.pack(
      membershipUpdateSize(defense) + membershipUpdateSize(oldest),
    );
    expect(names(second)).toEqual(["self", "oldest"]);
    queue.acknowledge(second, true);

    expect(names(queue.pack(10_000))).toEqual(["self", "newer", "oldest"]);
  });

  it("orders a later normal record behind an earlier self-defense record", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("self"), 1, true);
    queue.enqueue(update("other"), 1);

    expect(names(queue.pack(10_000))).toEqual(["self", "other"]);
  });

  it("packs whole fitting records with overhead and skips oversized records", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    const large: MembershipUpdate = update("large", STATE_ALIVE, 1, new Uint8Array(40));
    const small: MembershipUpdate = update("s");
    queue.enqueue(large, 1);
    queue.enqueue(small, 1);
    const overhead: number = 3;
    const budget: number = membershipUpdateSize(small) + overhead;

    const selection: BroadcastSelection = queue.pack(budget, { perRecordOverhead: overhead });
    expect(names(selection)).toEqual(["s"]);
    expect(selection.bytes).toBe(budget);
    expect(queue.get("large")?.remaining).toBe(4);
    expect(queue.get("s")?.remaining).toBe(4);
  });

  it("returns copied selection metadata without changing counters", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("safe", STATE_ALIVE, 1, Uint8Array.of(3)), 1);
    const selection: BroadcastSelection = queue.pack(1_000);

    selection.updates[0]?.metadata.fill(7);
    expect(queue.get("safe")?.update.metadata).toEqual(Uint8Array.of(3));
    expect(queue.get("safe")?.remaining).toBe(4);
  });
});

describe("broadcast send acknowledgement", () => {
  it("decrements accepted sends once and leaves rejected sends unchanged", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("a"), 1);

    const rejected: BroadcastSelection = queue.pack(1_000);
    expect(queue.acknowledge(rejected, false)).toBe(true);
    expect(queue.get("a")?.remaining).toBe(4);
    expect(queue.acknowledge(rejected, true)).toBe(false);

    const accepted: BroadcastSelection = queue.pack(1_000);
    expect(queue.acknowledge(accepted, true)).toBe(true);
    expect(queue.get("a")?.remaining).toBe(3);
    expect(queue.acknowledge(accepted, true)).toBe(false);
    expect(queue.get("a")?.remaining).toBe(3);
  });

  it("counts independently selected destinations and removes at zero", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("a"), 1);

    const destinations: BroadcastSelection[] = Array.from(
      { length: 4 },
      (): ReturnType<BroadcastQueue["pack"]> => queue.pack(1_000),
    );
    for (const destination of destinations) {
      queue.acknowledge(destination, true);
    }
    expect(queue.get("a")).toBeUndefined();
    expect(queue.size).toBe(0);
  });

  it("does not charge a superseding record for a stale selection", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    queue.enqueue(update("a", STATE_ALIVE, 1), 1);
    const staleSelection: BroadcastSelection = queue.pack(1_000);
    queue.enqueue(update("a", STATE_DEAD, 1), 10);

    queue.acknowledge(staleSelection, true);
    expect(queue.get("a")).toMatchObject({
      update: { state: STATE_DEAD },
      remaining: 8,
    });
  });
});

describe("buddy packing", () => {
  it("puts the buddy first, deduplicates it, and charges it once", () => {
    const queue: BroadcastQueue = new BroadcastQueue();
    const buddy: MembershipUpdate = update("target", STATE_SUSPECT, 4);
    queue.enqueue(update("other"), 1);
    queue.enqueue(buddy, 1);

    const selection: BroadcastSelection = queue.pack(1_000, { buddy });
    expect(names(selection)).toEqual(["target", "other"]);
    expect(names(selection).filter((name: string): boolean => name === "target")).toHaveLength(1);
    expect(queue.get("target")?.remaining).toBe(4);

    queue.acknowledge(selection, true);
    expect(queue.get("target")?.remaining).toBe(3);
    expect(queue.get("other")?.remaining).toBe(3);
  });
});
