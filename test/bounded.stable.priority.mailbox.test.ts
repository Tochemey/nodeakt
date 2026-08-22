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
import { BoundedStablePriorityMailbox } from "../src/actor/priority.mailbox";
import { ErrMailboxFull } from "../src/errors/errors";
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
