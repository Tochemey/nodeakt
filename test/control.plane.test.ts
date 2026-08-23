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
import { ControlPlane, mainWorkerId } from "../src/control.plane";
import { ErrActorAlreadyExists } from "../src/errors";

describe("ControlPlane", () => {
  it("round-robins placement over the pool and wraps", () => {
    const plane = new ControlPlane([1, 2, 3]);

    expect([plane.place(), plane.place(), plane.place(), plane.place()]).toEqual([1, 2, 3, 1]);
  });

  it("falls back to the main isolate when the pool is empty", () => {
    const plane = new ControlPlane([]);

    expect(plane.place()).toBe(mainWorkerId);
  });

  it("keeps a valid rotation after the pool shrinks", () => {
    const plane = new ControlPlane([1, 2]);

    expect(plane.place()).toBe(1);
    plane.evict(2);

    expect(plane.place()).toBe(1);
    expect(plane.place()).toBe(1);
  });

  it("registers, resolves, and frees top-level names", () => {
    const plane = new ControlPlane([1, 2]);

    expect(plane.register("greeter", 1)).toBeNull();
    expect(plane.resolve("greeter")).toBe(1);

    expect(plane.register("greeter", 2)).toBe(ErrActorAlreadyExists);
    expect(plane.resolve("greeter")).toBe(1);

    plane.free("greeter");
    expect(plane.resolve("greeter")).toBeUndefined();
    expect(plane.register("greeter", 2)).toBeNull();
    expect(plane.resolve("greeter")).toBe(2);
  });

  it("ignores freeing an unknown name", () => {
    const plane = new ControlPlane([1]);

    expect(() => plane.free("nobody")).not.toThrow();
  });

  it("evicts a worker: names freed and returned, rotation shrunk", () => {
    const plane = new ControlPlane([1, 2]);
    plane.register("a", 1);
    plane.register("b", 2);
    plane.register("c", 1);

    const freed = plane.evict(1);

    expect(freed.sort()).toEqual(["a", "c"]);
    expect(plane.resolve("a")).toBeUndefined();
    expect(plane.resolve("c")).toBeUndefined();
    expect(plane.resolve("b")).toBe(2);
    expect(plane.workers()).toEqual([2]);
  });

  it("evicts an unknown worker as a no-op", () => {
    const plane = new ControlPlane([1]);
    plane.register("a", 1);

    expect(plane.evict(9)).toEqual([]);
    expect(plane.workers()).toEqual([1]);
    expect(plane.resolve("a")).toBe(1);
  });

  it("places on the main isolate once every pool worker died", () => {
    const plane = new ControlPlane([1]);
    plane.evict(1);

    expect(plane.workers()).toEqual([]);
    expect(plane.place()).toBe(mainWorkerId);
  });
});
