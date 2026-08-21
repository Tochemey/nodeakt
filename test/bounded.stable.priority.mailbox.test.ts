import { describe, expect, it } from "vitest";
import { ErrMailboxFull } from "../src/actor/errors";
import { BoundedStablePriorityMailbox } from "../src/actor/priority.mailbox";
import { byUrgency, ctx, describeMailboxContract, drain } from "./mailbox.contract";

describe("BoundedStablePriorityMailbox", () => {
  describeMailboxContract(() => new BoundedStablePriorityMailbox(16, () => false));

  it("rejects an invalid capacity", () => {
    expect(() => new BoundedStablePriorityMailbox(0, byUrgency)).toThrow(RangeError);
    expect(() => new BoundedStablePriorityMailbox(-3, byUrgency)).toThrow(RangeError);
  });

  it("breaks priority ties in arrival order", () => {
    const mb = new BoundedStablePriorityMailbox(8, byUrgency);
    for (const m of [
      { id: "a", urgency: 1 },
      { id: "b", urgency: 3 },
      { id: "c", urgency: 2 },
      { id: "d", urgency: 3 },
      { id: "e", urgency: 1 },
    ]) {
      mb.enqueue(ctx(m));
    }

    const ids = drain(mb).map((m) => (m as { id: string }).id);
    expect(ids).toEqual(["b", "d", "c", "a", "e"]);
  });

  it("returns ErrMailboxFull at capacity", () => {
    const mb = new BoundedStablePriorityMailbox(2, byUrgency);
    expect(mb.enqueue(ctx({ urgency: 1 }))).toBeNull();
    expect(mb.enqueue(ctx({ urgency: 2 }))).toBeNull();
    expect(mb.enqueue(ctx({ urgency: 3 }))).toBe(ErrMailboxFull);
    expect(mb.len()).toBe(2);
  });
});
