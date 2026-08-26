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
import {
  decodeNodeMetadata,
  encodeNodeMetadata,
  type NodeMetadata,
} from "../src/clustering.metadata";

/** The value an undecodable record decodes to. */
const UNDECODABLE: NodeMetadata = {
  startedAt: Number.MAX_SAFE_INTEGER,
  ready: false,
  draining: false,
  address: "",
};

describe("node metadata codec", () => {
  it("round-trips startedAt, both flags, and the data address in every combination", () => {
    const cases: readonly NodeMetadata[] = [
      { startedAt: 0, ready: false, draining: false, address: "127.0.0.1:6000" },
      { startedAt: 1_724_700_000_000, ready: true, draining: false, address: "10.0.0.5:7000" },
      { startedAt: 42, ready: false, draining: true, address: "[::1]:6000" },
      { startedAt: 7, ready: true, draining: true, address: "node-a.svc.cluster.local:6000" },
    ];
    for (const original of cases) {
      expect(decodeNodeMetadata(encodeNodeMetadata(original))).toEqual(original);
    }
  });

  it("decodes metadata too short for the fixed prefix as the youngest unaddressable member", () => {
    expect(decodeNodeMetadata(new Uint8Array(5))).toEqual(UNDECODABLE);
  });

  it("decodes metadata whose declared address runs past the bytes as unaddressable", () => {
    const encoded: Uint8Array = encodeNodeMetadata({
      startedAt: 1,
      ready: true,
      draining: false,
      address: "ab",
    });
    const truncated: Uint8Array = encoded.subarray(0, encoded.length - 1);
    expect(decodeNodeMetadata(truncated)).toEqual(UNDECODABLE);
  });

  it("rejects a startedAt that is not a non-negative safe integer", () => {
    expect(
      (): Uint8Array =>
        encodeNodeMetadata({ startedAt: -1, ready: false, draining: false, address: "a:1" }),
    ).toThrow(RangeError);
    expect(
      (): Uint8Array =>
        encodeNodeMetadata({ startedAt: 1.5, ready: false, draining: false, address: "a:1" }),
    ).toThrow(RangeError);
  });

  it("rejects an address over its byte budget", () => {
    const oversized: string = `${"h".repeat(256)}:1`;
    expect(
      (): Uint8Array =>
        encodeNodeMetadata({ startedAt: 1, ready: false, draining: false, address: oversized }),
    ).toThrow(RangeError);
  });
});
