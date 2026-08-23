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

import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import {
  ByteReader,
  ByteWriter,
  decodeValue,
  encodeValue,
  ValueDecodeError,
} from "../../src/_net/values";

function encodeToBytes(value: unknown): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, value);
  return Uint8Array.from(writer.bytes());
}

function roundTrip(value: unknown): unknown {
  return decodeValue(new ByteReader(encodeToBytes(value)));
}

function decodeBytes(bytes: number[]): unknown {
  return decodeValue(new ByteReader(Uint8Array.from(bytes)));
}

describe("byte primitives", () => {
  it("round-trips fixed-width integers and doubles", () => {
    const writer: ByteWriter = new ByteWriter();
    writer.writeU8(0xab);
    writer.writeU32(0xdeadbeef);
    writer.writeU64(Number.MAX_SAFE_INTEGER);
    writer.writeF64(-123.456);

    const reader: ByteReader = new ByteReader(writer.bytes());
    expect(reader.readU8()).toBe(0xab);
    expect(reader.readU32()).toBe(0xdeadbeef);
    expect(reader.readU64()).toBe(Number.MAX_SAFE_INTEGER);
    expect(reader.readF64()).toBe(-123.456);
    expect(reader.remaining).toBe(0);
  });

  it("round-trips uvarints across the safe range", () => {
    const values: number[] = [0, 1, 127, 128, 300, 0xffff, 0xffffffff, 2 ** 45, 2 ** 53 - 1];
    const writer: ByteWriter = new ByteWriter();
    for (const value of values) {
      writer.writeUvarint(value);
    }

    const reader: ByteReader = new ByteReader(writer.bytes());
    for (const value of values) {
      expect(reader.readUvarint()).toBe(value);
    }
  });

  it("rejects unsafe uvarint and u64 writes", () => {
    const writer: ByteWriter = new ByteWriter();
    expect(() => writer.writeUvarint(-1)).toThrow(TypeError);
    expect(() => writer.writeUvarint(0.5)).toThrow(TypeError);
    expect(() => writer.writeUvarint(2 ** 53)).toThrow(TypeError);
    expect(() => writer.writeU64(-1)).toThrow(TypeError);
  });

  it("rejects a u64 read above the safe integer range", () => {
    const writer: ByteWriter = new ByteWriter();
    writer.writeU32(0xffffffff);
    writer.writeU32(0xffffffff);
    expect(() => new ByteReader(writer.bytes()).readU64()).toThrow(ValueDecodeError);
  });

  it("rejects an overlong uvarint", () => {
    const bytes: Uint8Array = Uint8Array.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]);
    expect(() => new ByteReader(bytes).readUvarint()).toThrow(ValueDecodeError);
  });

  it("reuses a writer across messages after reset", () => {
    const writer: ByteWriter = new ByteWriter(16);
    encodeValue(writer, "a longer first message to force growth beyond sixteen bytes");
    writer.reset();
    encodeValue(writer, 7);
    expect(decodeValue(new ByteReader(writer.bytes()))).toBe(7);
  });
});

describe("value codec vectors", () => {
  it("encodes the fixed single-byte values", () => {
    expect(Array.from(encodeToBytes(null))).toEqual([0x00]);
    expect(Array.from(encodeToBytes(undefined))).toEqual([0x01]);
    expect(Array.from(encodeToBytes(false))).toEqual([0x02]);
    expect(Array.from(encodeToBytes(true))).toEqual([0x03]);
  });

  it("encodes zigzag integers", () => {
    expect(Array.from(encodeToBytes(0))).toEqual([0x04, 0x00]);
    expect(Array.from(encodeToBytes(1))).toEqual([0x04, 0x02]);
    expect(Array.from(encodeToBytes(-1))).toEqual([0x04, 0x01]);
    expect(Array.from(encodeToBytes(300))).toEqual([0x04, 0xd8, 0x04]);
  });

  it("encodes doubles big-endian", () => {
    expect(Array.from(encodeToBytes(0.5))).toEqual([0x05, 0x3f, 0xe0, 0, 0, 0, 0, 0, 0]);
  });

  it("encodes strings with a uvarint length", () => {
    expect(Array.from(encodeToBytes("hi"))).toEqual([0x06, 0x02, 0x68, 0x69]);
    expect(Array.from(encodeToBytes(""))).toEqual([0x06, 0x00]);
  });

  it("encodes containers with a leading count", () => {
    expect(Array.from(encodeToBytes([]))).toEqual([0x0a, 0x00]);
    expect(Array.from(encodeToBytes({ a: 1 }))).toEqual([0x0b, 0x01, 0x01, 0x61, 0x04, 0x02]);
  });
});

describe("value codec round-trips", () => {
  it("round-trips numbers exactly, including the awkward ones", () => {
    const values: number[] = [
      0,
      1,
      -1,
      2147483647,
      -2147483648,
      2147483648,
      Number.MAX_SAFE_INTEGER,
      Number.MIN_SAFE_INTEGER,
      0.1,
      -123.456,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
    ];
    for (const value of values) {
      expect(roundTrip(value)).toBe(value);
    }

    expect(Number.isNaN(roundTrip(Number.NaN))).toBe(true);
    expect(Object.is(roundTrip(-0), -0)).toBe(true);
  });

  it("round-trips strings, unicode included", () => {
    const values: string[] = [
      "",
      "plain ascii",
      "🚀 rockets and 中文 mixed",
      "é".repeat(100),
      "a".repeat(1000),
    ];
    for (const value of values) {
      expect(roundTrip(value)).toBe(value);
    }
  });

  it("replaces a lone surrogate the way the platform encoder does", () => {
    expect(roundTrip("\ud800")).toBe("�");
  });

  it("round-trips bigints of any size", () => {
    const values: bigint[] = [0n, 255n, -255n, 2n ** 100n, -(2n ** 64n)];
    for (const value of values) {
      expect(roundTrip(value)).toBe(value);
    }
  });

  it("round-trips dates, invalid dates included", () => {
    const date: Date = new Date(1724371200000);
    expect(roundTrip(date)).toEqual(date);

    const invalid: Date = roundTrip(new Date(Number.NaN)) as Date;
    expect(Number.isNaN(invalid.getTime())).toBe(true);
  });

  it("round-trips buffers and every supported view", () => {
    const bytes: Uint8Array = Uint8Array.from([1, 2, 3, 255]);
    expect(roundTrip(bytes)).toEqual(bytes);

    const buffer: ArrayBuffer = new ArrayBuffer(bytes.length);
    new Uint8Array(buffer).set(bytes);
    expect(roundTrip(buffer)).toEqual(buffer);

    const ints: Int32Array = Int32Array.from([-1, 2 ** 30]);
    expect(roundTrip(ints)).toEqual(ints);

    const floats: Float64Array = Float64Array.from([0.5, -2.25]);
    expect(roundTrip(floats)).toEqual(floats);

    const view: DataView = roundTrip(new DataView(buffer)) as DataView;
    expect(new Uint8Array(view.buffer)).toEqual(bytes);
  });

  it("decodes a Buffer as a plain Uint8Array", () => {
    const decoded: unknown = roundTrip(Buffer.from([9, 8, 7]));
    expect(decoded).toBeInstanceOf(Uint8Array);
    expect(decoded).not.toBeInstanceOf(Buffer);
    expect(decoded).toEqual(Uint8Array.from([9, 8, 7]));
  });

  it("round-trips arrays, holes becoming undefined", () => {
    expect(roundTrip([1, "two", null, [3]])).toEqual([1, "two", null, [3]]);

    const sparse: unknown[] = [];
    sparse[1] = 1;
    expect(roundTrip(sparse)).toEqual([undefined, 1]);
  });

  it("round-trips objects, keeping undefined values and dropping symbol keys", () => {
    const symbol: symbol = Symbol("hidden");
    const value: Record<string | symbol, unknown> = { a: 1, b: undefined, [symbol]: "gone" };
    const decoded: Record<string, unknown> = roundTrip(value) as Record<string, unknown>;
    expect(Object.keys(decoded)).toEqual(["a", "b"]);
    expect(decoded.b).toBeUndefined();
  });

  it("round-trips maps and sets, object keys included", () => {
    const key: { id: number } = { id: 1 };
    const map: Map<unknown, unknown> = new Map<unknown, unknown>([
      [key, "entry"],
      ["plain", 2],
    ]);
    expect(roundTrip(map)).toEqual(map);

    const set: Set<unknown> = new Set<unknown>([1, "two", key]);
    expect(roundTrip(set)).toEqual(set);
  });

  it("flattens a nested class instance to its own enumerable properties", () => {
    class Point {
      constructor(readonly x: number) {}
    }

    const decoded: { p: { x: number } } = roundTrip({ p: new Point(3) }) as { p: { x: number } };
    expect(decoded.p).toEqual({ x: 3 });
    expect(Object.getPrototypeOf(decoded.p)).toBe(Object.prototype);
  });
});

describe("value codec aliasing and cycles", () => {
  it("preserves aliasing across the tree", () => {
    const shared: { n: number } = { n: 1 };
    const decoded: { a: object; b: object } = roundTrip({ a: shared, b: shared }) as {
      a: object;
      b: object;
    };
    expect(decoded.a).toBe(decoded.b);
  });

  it("survives a cyclic object and a cyclic array", () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    const decodedObject: Record<string, unknown> = roundTrip(cyclic) as Record<string, unknown>;
    expect(decodedObject.self).toBe(decodedObject);

    const loop: unknown[] = [];
    loop.push(loop);
    const decodedArray: unknown[] = roundTrip(loop) as unknown[];
    expect(decodedArray[0]).toBe(decodedArray);
  });

  it("preserves aliasing of map keys", () => {
    const shared: { id: number } = { id: 7 };
    const value: { key: object; map: Map<unknown, unknown> } = {
      key: shared,
      map: new Map<unknown, unknown>([[shared, "hit"]]),
    };
    const decoded: { key: object; map: Map<unknown, unknown> } = roundTrip(value) as {
      key: object;
      map: Map<unknown, unknown>;
    };
    expect(decoded.map.get(decoded.key)).toBe("hit");
  });
});

describe("value codec refusals", () => {
  it("refuses functions and symbols, top-level and nested", () => {
    expect(() => encodeToBytes(() => 1)).toThrow(TypeError);
    expect(() => encodeToBytes(Symbol("nope"))).toThrow(TypeError);
    expect(() => encodeToBytes({ f: () => 1 })).toThrow(TypeError);
  });

  it("refuses nesting beyond the depth limit on encode", () => {
    let deep: unknown[] = [];
    for (let i = 0; i < 1100; i++) {
      deep = [deep];
    }

    expect(() => encodeToBytes(deep)).toThrow(RangeError);
  });
});

describe("value codec hostile input", () => {
  it("rejects an unknown tag", () => {
    expect(() => decodeBytes([0x7f])).toThrow(ValueDecodeError);
  });

  it("rejects an unknown bytes subtype and a misaligned length", () => {
    expect(() => decodeBytes([0x09, 0x20, 0x00])).toThrow(ValueDecodeError);
    expect(() => decodeBytes([0x09, 0x06, 0x03, 1, 2, 3])).toThrow(ValueDecodeError);
  });

  it("rejects a declared bytes length beyond the input before allocating", () => {
    // Subtype 1 (Uint8Array) declaring two gigabytes with one byte of
    // input: the bound check must fire before the buffer is sized
    // from the attacker's number.
    const writer: ByteWriter = new ByteWriter();
    writer.writeU8(0x09);
    writer.writeU8(0x01);
    writer.writeUvarint(2 * 1024 * 1024 * 1024);
    writer.writeU8(0x7a);
    expect(() => decodeValue(new ByteReader(writer.bytes()))).toThrow(ValueDecodeError);
    expect(() => decodeValue(new ByteReader(writer.bytes()))).toThrow(/truncated input/);
  });

  it("rejects an out-of-range int and a bad back-reference", () => {
    const writer: ByteWriter = new ByteWriter();
    writer.writeU8(0x04);
    writer.writeUvarint(2 ** 33);
    expect(() => decodeValue(new ByteReader(writer.bytes()))).toThrow(ValueDecodeError);

    expect(() => decodeBytes([0x0e, 0x00])).toThrow(ValueDecodeError);
  });

  it("rejects invalid utf-8 and a hostile element count", () => {
    expect(() => decodeBytes([0x06, 0x01, 0xff])).toThrow(ValueDecodeError);
    expect(() => decodeBytes([0x0a, 0xff, 0xff, 0xff, 0xff, 0x0f])).toThrow(ValueDecodeError);
  });

  it("rejects nesting beyond the depth limit on decode", () => {
    const bytes: number[] = [];
    for (let i = 0; i < 1100; i++) {
      bytes.push(0x0a, 0x01);
    }

    bytes.push(0x00);
    expect(() => decodeBytes(bytes)).toThrow(ValueDecodeError);
  });

  it("never pollutes prototypes through crafted keys", () => {
    const evil: Record<string, unknown> = {};
    Object.defineProperty(evil, "__proto__", {
      value: { polluted: true },
      enumerable: true,
      configurable: true,
      writable: true,
    });
    Object.defineProperty(evil, "constructor", {
      value: "shadowed",
      enumerable: true,
      configurable: true,
      writable: true,
    });

    const decoded: Record<string, unknown> = roundTrip({ x: evil }) as Record<string, unknown>;
    const inner: object = decoded.x as object;
    expect(Object.getPrototypeOf(inner)).toBe(Object.prototype);
    expect(Object.keys(inner)).toEqual([]);
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  });

  it("fails cleanly on every strict prefix of a valid encoding", () => {
    const corpus: unknown[] = [
      1,
      0.5,
      "hello world",
      12345678901234n,
      new Date(1724371200000),
      Uint8Array.from([1, 2, 3]),
      [1, [2, "three"], { four: 4n }],
      new Map<unknown, unknown>([["k", new Set([1, 2])]]),
    ];
    for (const value of corpus) {
      const bytes: Uint8Array = encodeToBytes(value);
      for (let cut = 0; cut < bytes.length; cut++) {
        expect(() => decodeValue(new ByteReader(bytes.subarray(0, cut)))).toThrow(ValueDecodeError);
      }
    }
  });
});

describe("value codec randomized round-trips", () => {
  /** Deterministic 32-bit LCG so failures reproduce. */
  class Rng {
    private state: number;

    constructor(seed: number) {
      this.state = seed >>> 0;
    }

    next(): number {
      this.state = (Math.imul(this.state, 1664525) + 1013904223) >>> 0;
      return this.state / 0x100000000;
    }

    int(bound: number): number {
      return Math.floor(this.next() * bound);
    }
  }

  function randomValue(rng: Rng, depth: number): unknown {
    const leaf: boolean = depth >= 4 || rng.next() < 0.4;
    if (leaf) {
      const pick: number = rng.int(8);
      switch (pick) {
        case 0:
          return null;
        case 1:
          return undefined;
        case 2:
          return rng.next() < 0.5;
        case 3:
          return rng.int(2 ** 31) - 2 ** 30;
        case 4:
          return (rng.next() - 0.5) * 1e9;
        case 5:
          return `s${rng.int(1e9)}✓`.repeat(rng.int(4) + 1);
        case 6:
          return BigInt(rng.int(2 ** 31)) * (rng.next() < 0.5 ? -1n : 1n);
        default:
          return new Date(rng.int(2 ** 40));
      }
    }

    const pick: number = rng.int(5);
    switch (pick) {
      case 0: {
        const array: unknown[] = [];
        const count: number = rng.int(5);
        for (let i = 0; i < count; i++) {
          array.push(randomValue(rng, depth + 1));
        }

        return array;
      }
      case 1: {
        const object: Record<string, unknown> = {};
        const count: number = rng.int(5);
        for (let i = 0; i < count; i++) {
          object[`k${rng.int(1000)}`] = randomValue(rng, depth + 1);
        }

        return object;
      }
      case 2: {
        const map: Map<unknown, unknown> = new Map();
        const count: number = rng.int(4);
        for (let i = 0; i < count; i++) {
          map.set(randomValue(rng, depth + 1), randomValue(rng, depth + 1));
        }

        return map;
      }
      case 3: {
        const set: Set<unknown> = new Set();
        const count: number = rng.int(4);
        for (let i = 0; i < count; i++) {
          set.add(randomValue(rng, depth + 1));
        }

        return set;
      }
      default: {
        const bytes: Uint8Array = new Uint8Array(rng.int(16));
        for (let i = 0; i < bytes.length; i++) {
          bytes[i] = rng.int(256);
        }

        return bytes;
      }
    }
  }

  it("round-trips 250 randomized value trees", () => {
    const rng: Rng = new Rng(0x0dea11);
    for (let i = 0; i < 250; i++) {
      const value: unknown = randomValue(rng, 0);
      expect(roundTrip(value)).toEqual(value);
    }
  });
});
