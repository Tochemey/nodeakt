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
import { MessageRegistry } from "../src/message.registry";

class Ping {
  constructor(readonly value: number) {}
}

class Pong {
  constructor(readonly value: number) {}
}

describe("MessageRegistry", () => {
  it("registers a class under its own name by default", () => {
    const registry = new MessageRegistry();
    registry.register(Ping);

    expect(registry.idOf(Ping)).toBe("Ping");
    expect(registry.classOf("Ping")).toBe(Ping);
  });

  it("registers a class under an explicit id", () => {
    const registry = new MessageRegistry();
    registry.register(Ping, "net.ping");

    expect(registry.idOf(Ping)).toBe("net.ping");
    expect(registry.classOf("net.ping")).toBe(Ping);
    expect(registry.classOf("Ping")).toBeUndefined();
  });

  it("treats re-registering the same class under the same id as a no-op", () => {
    const registry = new MessageRegistry();
    registry.register(Ping);

    expect(() => registry.register(Ping)).not.toThrow();
    expect(registry.idOf(Ping)).toBe("Ping");
  });

  it("rejects an empty id", () => {
    const registry = new MessageRegistry();

    expect(() => registry.register(Ping, "")).toThrow(TypeError);
  });

  it("rejects a second class under an already-bound id", () => {
    const registry = new MessageRegistry();
    registry.register(Ping, "shared");

    expect(() => registry.register(Pong, "shared")).toThrow(
      'message type id "shared" is already registered',
    );
  });

  it("rejects a second id for an already-registered class", () => {
    const registry = new MessageRegistry();
    registry.register(Ping);

    expect(() => registry.register(Ping, "other")).toThrow(
      'message type "Ping" is already registered as "Ping"',
    );
  });

  it("returns undefined for unknown lookups", () => {
    const registry = new MessageRegistry();

    expect(registry.idOf(Ping)).toBeUndefined();
    expect(registry.classOf("nope")).toBeUndefined();
  });
});
