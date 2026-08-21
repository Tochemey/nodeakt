import { describe, expect, it } from "vitest";
import { BoundedMailbox } from "../src/actor/bounded.mailbox";
import { ErrMailboxFull } from "../src/actor/errors";
import { ctx, describeFifo, describeMailboxContract, drain } from "./mailbox.contract";

describe("BoundedMailbox", () => {
  describeMailboxContract(() => new BoundedMailbox(16));
  describeFifo(() => new BoundedMailbox(1000));

  it("rejects a non-positive or fractional capacity", () => {
    expect(() => new BoundedMailbox(0)).toThrow(RangeError);
    expect(() => new BoundedMailbox(-1)).toThrow(RangeError);
    expect(() => new BoundedMailbox(1.5)).toThrow(RangeError);
  });

  it("returns ErrMailboxFull at exactly its capacity", () => {
    const mb = new BoundedMailbox(3);
    expect(mb.enqueue(ctx(1))).toBeNull();
    expect(mb.enqueue(ctx(2))).toBeNull();
    expect(mb.enqueue(ctx(3))).toBeNull();
    expect(mb.enqueue(ctx(4))).toBe(ErrMailboxFull);
    expect(mb.len()).toBe(3);

    // Freeing a slot admits a new message; order is preserved.
    expect(mb.dequeue()?.message).toBe(1);
    expect(mb.enqueue(ctx(4))).toBeNull();
    expect(drain(mb)).toEqual([2, 3, 4]);
  });

  it("wraps around its ring many times", () => {
    const mb = new BoundedMailbox(4);
    for (let i = 0; i < 100; i++) {
      expect(mb.enqueue(ctx(i))).toBeNull();
      expect(mb.dequeue()?.message).toBe(i);
    }
  });
});
