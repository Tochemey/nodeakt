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
import { createReceiveContext, type ReceiveContext } from "../src/actor/receive.context";
import { Stash } from "../src/actor/stash";
import { ErrMailboxDisposed } from "../src/errors/errors";

function ctx(message: unknown): ReceiveContext {
  return createReceiveContext(message);
}

describe("Stash", () => {
  it("starts empty", () => {
    const stash = new Stash();
    expect(stash.isEmpty()).toBe(true);
    expect(stash.len()).toBe(0);
    expect(stash.unstash()).toBeUndefined();
    expect(stash.unstashAll()).toEqual([]);
  });

  it("unstashes the oldest message first", () => {
    const stash = new Stash();
    expect(stash.stash(ctx(1))).toBeNull();
    expect(stash.stash(ctx(2))).toBeNull();
    expect(stash.stash(ctx(3))).toBeNull();
    expect(stash.len()).toBe(3);

    expect(stash.unstash()?.message).toBe(1);
    expect(stash.unstash()?.message).toBe(2);
    expect(stash.len()).toBe(1);
  });

  it("unstashAll returns everything in arrival order and empties the buffer", () => {
    const stash = new Stash();
    for (let i = 0; i < 50; i++) {
      stash.stash(ctx(i));
    }

    const all = stash.unstashAll().map((c) => c.message);
    expect(all).toEqual([...Array(50).keys()]);
    expect(stash.isEmpty()).toBe(true);
    expect(stash.unstashAll()).toEqual([]);
  });

  it("is reusable after being drained", () => {
    const stash = new Stash();
    stash.stash(ctx("a"));
    stash.unstashAll();

    expect(stash.stash(ctx("b"))).toBeNull();
    expect(stash.unstash()?.message).toBe("b");
  });

  it("rejects stash and drops messages after dispose", () => {
    const stash = new Stash();
    stash.stash(ctx(1));
    stash.dispose();

    expect(stash.stash(ctx(2))).toBe(ErrMailboxDisposed);
    expect(stash.unstash()).toBeUndefined();
    expect(stash.isEmpty()).toBe(true);
  });
});
