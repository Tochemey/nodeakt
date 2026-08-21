import { describe, expect, it } from "vitest";
import { UnboundedSegmentedMailbox } from "../src/actor/segmented.mailbox";
import { ctx, describeFifo, describeMailboxContract, drain } from "./mailbox.contract";

describe("UnboundedSegmentedMailbox", () => {
  describeMailboxContract(() => new UnboundedSegmentedMailbox());
  describeFifo(() => new UnboundedSegmentedMailbox());

  it("preserves FIFO order across many segment boundaries", () => {
    const mb = new UnboundedSegmentedMailbox();
    const total = 256 * 4 + 37;
    for (let i = 0; i < total; i++) {
      mb.enqueue(ctx(i));
    }

    expect(mb.len()).toBe(total);
    expect(drain(mb)).toEqual([...Array(total).keys()]);
  });
});
