import { describe, expect, it } from "vitest";
import { UnboundedStablePriorityMailbox } from "../src/actor/priority.mailbox";
import { byUrgency, ctx, describeMailboxContract, drain } from "./mailbox.contract";

describe("UnboundedStablePriorityMailbox", () => {
  describeMailboxContract(() => new UnboundedStablePriorityMailbox(() => false));

  it("breaks priority ties in arrival order", () => {
    const mb = new UnboundedStablePriorityMailbox(byUrgency);
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

  it("is FIFO when all priorities are equal", () => {
    const mb = new UnboundedStablePriorityMailbox(() => false);
    for (let i = 0; i < 100; i++) {
      mb.enqueue(ctx(i));
    }

    expect(drain(mb)).toEqual([...Array(100).keys()]);
  });
});
