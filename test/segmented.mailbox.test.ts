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
import { UnboundedSegmentedMailbox } from "../src/segmented.mailbox";
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
