import { describe, expect, it } from "vitest";
import { ErrMailboxFull } from "../src/actor/errors";
import { BoundedPriorityMailbox } from "../src/actor/priority.mailbox";
import { byUrgency, ctx, describeMailboxContract, drain } from "./mailbox.contract";

describe("BoundedPriorityMailbox", () => {
  describeMailboxContract(() => new BoundedPriorityMailbox(16, () => false));

  it("rejects an invalid capacity", () => {
    expect(() => new BoundedPriorityMailbox(0, byUrgency)).toThrow(RangeError);
    expect(() => new BoundedPriorityMailbox(-1, byUrgency)).toThrow(RangeError);
    expect(() => new BoundedPriorityMailbox(1.5, byUrgency)).toThrow(RangeError);
  });

  it("dequeues the highest priority first", () => {
    const mb = new BoundedPriorityMailbox(8, byUrgency);
    for (const urgency of [1, 3, 2, 3, 1]) {
      mb.enqueue(ctx({ urgency }));
    }

    const urgencies = drain(mb).map((m) => (m as { urgency: number }).urgency);
    expect(urgencies).toEqual([3, 3, 2, 1, 1]);
  });

  it("returns ErrMailboxFull at capacity", () => {
    const mb = new BoundedPriorityMailbox(2, byUrgency);
    expect(mb.enqueue(ctx({ urgency: 1 }))).toBeNull();
    expect(mb.enqueue(ctx({ urgency: 2 }))).toBeNull();
    expect(mb.enqueue(ctx({ urgency: 3 }))).toBe(ErrMailboxFull);
    expect(mb.len()).toBe(2);
  });
});
