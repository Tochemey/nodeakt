import { describe, expect, it } from "vitest";
import { UnboundedPriorityMailbox } from "../src/actor/priority.mailbox";
import { byUrgency, ctx, describeMailboxContract, drain } from "./mailbox.contract";

describe("UnboundedPriorityMailbox", () => {
  describeMailboxContract(() => new UnboundedPriorityMailbox(() => false));

  it("dequeues the highest priority first", () => {
    const mb = new UnboundedPriorityMailbox(byUrgency);
    for (const urgency of [1, 3, 2, 3, 1]) {
      mb.enqueue(ctx({ urgency }));
    }

    const urgencies = drain(mb).map((m) => (m as { urgency: number }).urgency);
    expect(urgencies).toEqual([3, 3, 2, 1, 1]);
  });
});
