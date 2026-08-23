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
import { BoundedMailbox } from "../src/bounded.mailbox";
import { ErrMailboxFull } from "../src/errors";
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
