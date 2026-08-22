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
