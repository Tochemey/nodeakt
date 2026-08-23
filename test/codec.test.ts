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
import { Codec, decodeError, encodeError } from "../src/codec";
import { ErrDead, ErrRequestTimeout, TypeNotRegisteredError } from "../src/errors";
import { type MessageClass, MessageRegistry } from "../src/message.registry";

class Ping {
  constructor(readonly value: number) {}

  tag(): string {
    return `ping:${this.value}`;
  }
}

class Bare {
  constructor(readonly value: number) {}
}

function newCodec(): Codec {
  const registry = new MessageRegistry();
  registry.register(Ping);
  return new Codec(registry);
}

describe("Codec messages", () => {
  it("passes primitives and null through untyped", () => {
    const codec = newCodec();

    for (const value of ["hello", 42, true, null, undefined, 7n]) {
      const wire = codec.encodeMessage(value);
      expect(wire.type).toBe("");
      expect(codec.decodeMessage(wire)).toBe(value);
    }
  });

  it("passes plain objects, arrays, and null-prototype objects through untyped", () => {
    const codec = newCodec();

    for (const value of [{ a: 1 }, [1, 2, 3], Object.assign(Object.create(null), { b: 2 })]) {
      const wire = codec.encodeMessage(value);
      expect(wire.type).toBe("");
      expect(wire.data).toBe(value);
    }
  });

  it("round-trips a registered class across a structured clone", () => {
    const codec = newCodec();

    const wire = structuredClone(codec.encodeMessage(new Ping(7)));
    const restored = codec.decodeMessage(wire);

    expect(restored).toBeInstanceOf(Ping);
    expect((restored as Ping).value).toBe(7);
    expect((restored as Ping).tag()).toBe("ping:7");
  });

  it("refuses to encode an unregistered class instance", () => {
    const codec = newCodec();

    expect(() => codec.encodeMessage(new Bare(1))).toThrow(TypeNotRegisteredError);
    expect(() => codec.encodeMessage(new Bare(1))).toThrow('message type "Bare" is not registered');
  });

  it("names an anonymous class and an unreachable constructor as unknown", () => {
    const codec = newCodec();

    const anonymous = new (class {
      readonly value = 1;
    })();
    expect(() => codec.encodeMessage(Object.create(anonymous))).toThrow(TypeNotRegisteredError);

    const rootless = Object.create(Object.create(null));
    expect(() => codec.encodeMessage(rootless)).toThrow('message type "unknown"');
  });

  it("refuses to encode functions and symbols", () => {
    const codec = newCodec();

    expect(() => codec.encodeMessage(() => 1)).toThrow(TypeError);
    expect(() => codec.encodeMessage(Symbol("x"))).toThrow(
      "a symbol cannot cross an isolate boundary",
    );
  });

  it("refuses to decode an unknown type id", () => {
    const codec = newCodec();

    expect(() => codec.decodeMessage({ type: "Nope", data: {} })).toThrow(
      'message type "Nope" is not registered',
    );
  });

  it("drops __proto__ and constructor keys instead of restoring them", () => {
    const codec = newCodec();

    // JSON.parse is the one honest way to build an object whose own
    // "__proto__" key survives as a data property, exactly what a
    // hostile wire payload would carry.
    const data = JSON.parse('{"__proto__": {"polluted": true}, "constructor": "evil", "value": 3}');
    const restored = codec.decodeMessage({ type: "Ping", data }) as Ping;

    expect(restored).toBeInstanceOf(Ping);
    expect(Object.getPrototypeOf(restored)).toBe(Ping.prototype);
    expect((restored as unknown as { polluted?: boolean }).polluted).toBeUndefined();
    expect(restored.constructor).toBe(Ping);
    expect(restored.value).toBe(3);
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  });

  it("restores fields as own data properties without running prototype setters", () => {
    class Trapped {
      sprung = false;

      set trigger(_value: number) {
        this.sprung = true;
      }
    }

    const registry = new MessageRegistry();
    registry.register(Trapped);
    const codec = new Codec(registry);

    const restored = codec.decodeMessage({
      type: "Trapped",
      data: { sprung: false, trigger: 5 },
    }) as Trapped;

    expect(restored).toBeInstanceOf(Trapped);
    expect(restored.sprung).toBe(false);
    expect(Object.getOwnPropertyDescriptor(restored, "trigger")?.value).toBe(5);
  });

  it("restores a bare instance when the wire data is not an object", () => {
    const codec = newCodec();

    const restored = codec.decodeMessage({ type: "Ping", data: null });

    expect(restored).toBeInstanceOf(Ping);
    expect(Object.keys(restored as object)).toEqual([]);
  });
});

describe("Codec errors", () => {
  it("round-trips a sentinel by identity", () => {
    const deadWire = structuredClone(encodeError(ErrDead));
    expect(decodeError(deadWire)).toBe(ErrDead);

    const timeoutWire = structuredClone(encodeError(ErrRequestTimeout));
    expect(decodeError(timeoutWire)).toBe(ErrRequestTimeout);
  });

  it("round-trips an arbitrary error by name and message", () => {
    const error = new RangeError("out of range");

    const restored = decodeError(structuredClone(encodeError(error)));

    expect(restored).not.toBe(error);
    expect(restored.name).toBe("RangeError");
    expect(restored.message).toBe("out of range");
  });

  it("reconstructs an out-of-range sentinel index as a plain error", () => {
    const restored = decodeError({ sentinel: 9999, name: "", message: "" });

    expect(restored).toBeInstanceOf(Error);
    expect(restored.message).toBe("");
  });
});

describe("MessageClass shape", () => {
  it("accepts any class value", () => {
    const type: MessageClass = Ping;

    expect(type.name).toBe("Ping");
  });
});
