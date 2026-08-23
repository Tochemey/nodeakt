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
import { HeadQueue } from "../../src/_net/queue";

describe("head queue", () => {
  it("keeps FIFO order through peek, shift, and pop", () => {
    const queue: HeadQueue<number> = new HeadQueue<number>();
    expect(queue.shift()).toBeUndefined();
    expect(queue.pop()).toBeUndefined();
    expect(queue.peek()).toBeUndefined();

    queue.push(1);
    queue.push(2);
    queue.push(3);
    expect(queue.length).toBe(3);
    expect(queue.peek()).toBe(1);
    expect(queue.shift()).toBe(1);
    expect(queue.pop()).toBe(3);
    expect(queue.shift()).toBe(2);
    expect(queue.length).toBe(0);
  });

  it("resets its storage once the last item leaves either end", () => {
    const queue: HeadQueue<number> = new HeadQueue<number>();
    queue.push(1);
    queue.push(2);
    expect(queue.shift()).toBe(1);
    expect(queue.shift()).toBe(2);
    queue.push(3);
    expect(queue.peek()).toBe(3);
    expect(queue.pop()).toBe(3);
    expect(queue.shift()).toBeUndefined();
  });

  it("compacts the consumed prefix once it dominates the array", () => {
    const queue: HeadQueue<number> = new HeadQueue<number>();
    for (let i = 0; i < 130; i++) {
      queue.push(i);
    }

    for (let i = 0; i < 65; i++) {
      expect(queue.shift()).toBe(i);
    }

    // The compaction is invisible from outside; order and length hold.
    expect(queue.length).toBe(65);
    expect(queue.peek()).toBe(65);
    for (let i = 65; i < 130; i++) {
      expect(queue.shift()).toBe(i);
    }

    expect(queue.length).toBe(0);
  });

  it("clears in place", () => {
    const queue: HeadQueue<string> = new HeadQueue<string>();
    queue.push("a");
    queue.push("b");
    queue.shift();
    queue.clear();
    expect(queue.length).toBe(0);
    expect(queue.peek()).toBeUndefined();
  });
});
