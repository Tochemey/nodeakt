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
import { DEFAULT_PARTITION_COUNT } from "../../src/kv/constants";
import { fnv1a32, hash32, mix32, partitionId } from "../../src/kv/hash";

const encoder: TextEncoder = new TextEncoder();

function fnv1aUtf16(value: string): number {
  let hash: number = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }

  return hash >>> 0;
}

describe("kv partition hash", () => {
  it("matches FNV-1a 32-bit golden values over UTF-8 bytes", () => {
    expect(fnv1a32(new Uint8Array())).toBe(0x811c9dc5);
    expect(fnv1a32(encoder.encode("a"))).toBe(0xe40c292c);
    expect(fnv1a32(encoder.encode("é"))).toBe(0x1e9de8c1);
    expect(fnv1a32(encoder.encode("😀"))).toBe(0x33a29608);
  });

  it("finishes with the MurmurHash3 avalanche mix", () => {
    expect(mix32(0x811c9dc5)).toBe(0xab3e7c0b);
    expect(hash32("")).toBe(0xab3e7c0b);
    expect(hash32("a")).toBe(0x1a80b1b3);
    expect(hash32("a")).not.toBe(fnv1a32(encoder.encode("a")));
  });

  it("hashes UTF-8 bytes rather than UTF-16 code units", () => {
    expect(fnv1a32(encoder.encode("é"))).not.toBe(fnv1aUtf16("é"));
    expect(fnv1a32(encoder.encode("😀"))).not.toBe(fnv1aUtf16("😀"));
    expect(hash32("é")).toBe(0x8e4756c7);
    expect(hash32("😀")).toBe(0x3303a80a);
  });

  it("maps a key onto a partition with the mixed hash", () => {
    expect(partitionId("a", 512)).toBe(435);
    expect(partitionId("payments-42", DEFAULT_PARTITION_COUNT)).toBe(429);
    expect(partitionId("a", 1)).toBe(0);
    expect(partitionId("a", 10)).toBe(hash32("a") % 10);
  });

  it("rejects a partition count that is not a positive integer", () => {
    expect((): number => partitionId("a", 0)).toThrow(RangeError);
    expect((): number => partitionId("a", -1)).toThrow(RangeError);
    expect((): number => partitionId("a", 1.5)).toThrow(RangeError);
    expect((): number => partitionId("a", Number.NaN)).toThrow(RangeError);
    expect((): number => partitionId("a", Number.POSITIVE_INFINITY)).toThrow(RangeError);
  });

  it("spreads sequential keys across a non-prime partition count", () => {
    const count: number = 10;
    const hits: number[] = new Array(count).fill(0);
    for (let index = 0; index < 2_000; index += 1) {
      const id: number = partitionId(`key-${index}`, count);
      hits[id] = (hits[id] as number) + 1;
    }

    const expected: number = 2_000 / count;
    for (const observed of hits) {
      expect(observed).toBeGreaterThan(expected * 0.5);
      expect(observed).toBeLessThan(expected * 1.5);
    }
  });
});
