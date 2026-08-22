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

import { describe, expect, it, vi } from "vitest";
import { EventStream } from "../src/eventstream/eventstream";

describe("EventStream", () => {
  it("delivers published events to the topic's subscribers", () => {
    const stream = new EventStream();
    const seen: unknown[] = [];
    const other: unknown[] = [];

    stream.subscribe((event) => seen.push(event), "a");
    stream.subscribe((event) => other.push(event), "b");

    stream.publish("a", 1);
    stream.publish("b", 2);
    stream.publish("empty", 3);

    expect(seen).toEqual([1]);
    expect(other).toEqual([2]);
  });

  it("subscribes a function to a topic at most once", () => {
    const stream = new EventStream();
    const seen: unknown[] = [];
    const subscriber = (event: unknown): void => {
      seen.push(event);
    };

    stream.subscribe(subscriber, "a");
    stream.subscribe(subscriber, "a");
    stream.publish("a", 1);

    expect(seen).toEqual([1]);
    expect(stream.subscribersCount("a")).toBe(1);
  });

  it("unsubscribes a function from one topic only", () => {
    const stream = new EventStream();
    const seen: string[] = [];
    const subscriber = (event: unknown): void => {
      seen.push(String(event));
    };

    stream.subscribe(subscriber, "a");
    stream.subscribe(subscriber, "b");
    stream.unsubscribe(subscriber, "a");
    stream.unsubscribe(subscriber, "unknown-topic");
    stream.unsubscribe(() => {}, "b");

    stream.publish("a", "a1");
    stream.publish("b", "b1");

    expect(seen).toEqual(["b1"]);
    expect(stream.subscribersCount("a")).toBe(0);
    expect(stream.subscribersCount("unknown-topic")).toBe(0);
  });

  it("broadcasts one event to several topics", () => {
    const stream = new EventStream();
    const seen: string[] = [];

    stream.subscribe(() => seen.push("a"), "a");
    stream.subscribe(() => seen.push("b"), "b");

    stream.broadcast("x", ["a", "b", "empty"]);

    expect(seen).toEqual(["a", "b"]);
  });

  it("hands a throwing subscriber to the error handler and keeps going", () => {
    const failures: unknown[] = [];
    const stream = new EventStream((err) => failures.push(err));
    const seen: unknown[] = [];

    stream.subscribe(() => {
      throw new Error("boom");
    }, "a");
    stream.subscribe((event) => seen.push(event), "a");

    stream.publish("a", 1);

    expect(seen).toEqual([1]);
    expect(failures).toHaveLength(1);
    expect((failures[0] as Error).message).toBe("boom");
  });

  it("swallows a throwing subscriber without an error handler", () => {
    const stream = new EventStream();
    const failing = vi.fn(() => {
      throw new Error("boom");
    });

    stream.subscribe(failing, "a");
    expect(() => stream.publish("a", 1)).not.toThrow();
    expect(failing).toHaveBeenCalledOnce();
  });

  it("close drops every subscription and the stream stays usable", () => {
    const stream = new EventStream();
    const seen: unknown[] = [];
    const subscriber = (event: unknown): void => {
      seen.push(event);
    };

    stream.subscribe(subscriber, "a");
    stream.close();
    stream.publish("a", 1);
    expect(seen).toEqual([]);

    stream.subscribe(subscriber, "a");
    stream.publish("a", 2);
    expect(seen).toEqual([2]);
  });
});
