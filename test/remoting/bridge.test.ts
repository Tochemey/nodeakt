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
import { Codec } from "../../src/codec";
import { ErrDead, ErrRequestTimeout, TypeNotRegisteredError } from "../../src/errors";
import { MessageRegistry } from "../../src/message.registry";
import { Terminated } from "../../src/messages";
import type { ErrorBody } from "../../src/net/envelope";
import { ByteWriter } from "../../src/net/values";
import {
  decodeFailure,
  decodePayload,
  encodeFailure,
  encodePayload,
  type WirePayload,
} from "../../src/remoting.codec";

class Charge {
  constructor(
    readonly orderId: string,
    readonly amount: number,
  ) {}
}

class Unregistered {
  constructor(readonly n: number) {}
}

/** A registry and codec with the one test class registered. */
function bridge(): { codec: Codec; writer: ByteWriter } {
  const registry: MessageRegistry = new MessageRegistry();
  registry.register(Charge, "test.Charge");
  return { codec: new Codec(registry), writer: new ByteWriter() };
}

describe("remoting message bridge", () => {
  it("round-trips a registered class with its prototype restored", () => {
    const { codec, writer } = bridge();
    const wire: WirePayload = encodePayload(codec, writer, new Charge("o-1", 42));
    expect(wire.typeRef).toBe("test.Charge");

    const restored: unknown = decodePayload(codec, wire.typeRef, wire.payload);
    expect(restored).toBeInstanceOf(Charge);
    expect((restored as Charge).orderId).toBe("o-1");
    expect((restored as Charge).amount).toBe(42);
  });

  it("round-trips passthrough data under an empty type ref", () => {
    const { codec, writer } = bridge();
    const value: Record<string, unknown> = { ok: true, items: [1, 2, 3], note: "plain" };
    const wire: WirePayload = encodePayload(codec, writer, value);
    expect(wire.typeRef).toBe("");
    expect(decodePayload(codec, wire.typeRef, wire.payload)).toEqual(value);
  });

  it("round-trips the pre-registered Terminated announcement", () => {
    const { codec, writer } = bridge();
    const wire: WirePayload = encodePayload(
      codec,
      writer,
      new Terminated("nodeakt://sys@10.0.0.5:5100/greeter"),
    );
    expect(wire.typeRef).toBe("nodeakt.Terminated");

    const restored: unknown = decodePayload(codec, wire.typeRef, wire.payload);
    expect(restored).toBeInstanceOf(Terminated);
    expect((restored as Terminated).actorPath).toBe("nodeakt://sys@10.0.0.5:5100/greeter");
  });

  it("reuses the scratch writer without cross-message bleed", () => {
    const { codec, writer } = bridge();
    const first: WirePayload = encodePayload(codec, writer, new Charge("o-1", 1));
    const second: WirePayload = encodePayload(codec, writer, new Charge("longer-id", 2));

    expect((decodePayload(codec, first.typeRef, first.payload) as Charge).orderId).toBe("o-1");
    expect((decodePayload(codec, second.typeRef, second.payload) as Charge).orderId).toBe(
      "longer-id",
    );
  });

  it("refuses an unregistered class instance on the sending side", () => {
    const { codec, writer } = bridge();
    expect(() => encodePayload(codec, writer, new Unregistered(7))).toThrow(TypeNotRegisteredError);
  });

  it("refuses a type ref with no registration on the receiving side", () => {
    const { codec, writer } = bridge();
    const wire: WirePayload = encodePayload(codec, writer, new Charge("o-1", 1));

    const empty: Codec = new Codec(new MessageRegistry());
    expect(() => decodePayload(empty, wire.typeRef, wire.payload)).toThrow(TypeNotRegisteredError);
  });
});

describe("remoting failure bridge", () => {
  it("carries a sentinel error as an identity-preserving index", () => {
    const body: ErrorBody = encodeFailure(ErrDead);
    expect(body.sentinel).toBeGreaterThan(0);
    expect(body.name).toBe("");
    expect(decodeFailure(body.sentinel, body.name, body.message)).toBe(ErrDead);
    expect(decodeFailure(encodeFailure(ErrRequestTimeout).sentinel, "", "")).toBe(
      ErrRequestTimeout,
    );
  });

  it("carries any other error as its name and message", () => {
    const boom: Error = new RangeError("charge exceeds the account limit");
    const body: ErrorBody = encodeFailure(boom);
    expect(body.sentinel).toBe(0);
    expect(body.name).toBe("RangeError");

    const restored: Error = decodeFailure(body.sentinel, body.name, body.message);
    expect(restored).not.toBe(boom);
    expect(restored.name).toBe("RangeError");
    expect(restored.message).toBe("charge exceeds the account limit");
  });
});
