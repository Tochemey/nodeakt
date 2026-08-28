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
  appendRemotingAddress,
  decodeNodeMetadata,
  encodeNodeMetadata,
  type NodeMetadata,
  readRemotingAddress,
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

describe("remoting-address trailing field", () => {
  const base: NodeMetadata = {
    startedAt: 42,
    ready: true,
    draining: false,
    address: "10.0.0.5:7000",
  };

  it("round-trips a remoting address appended after the store record", () => {
    const encoded: Uint8Array = appendRemotingAddress(encodeNodeMetadata(base), "10.0.0.5:2552");
    expect(readRemotingAddress(encoded)).toBe("10.0.0.5:2552");
  });

  it("leaves the store fields untouched, so a decoder that skips the field still reads them", () => {
    const encoded: Uint8Array = appendRemotingAddress(encodeNodeMetadata(base), "10.0.0.5:2552");
    expect(decodeNodeMetadata(encoded)).toEqual(base);
  });

  it("appends nothing for an empty remoting address and reads it back as empty", () => {
    const store: Uint8Array = encodeNodeMetadata(base);
    const appended: Uint8Array = appendRemotingAddress(store, "");
    expect(appended).toEqual(store);
    expect(readRemotingAddress(appended)).toBe("");
  });

  it("reads a store-only record as carrying no remoting address", () => {
    expect(readRemotingAddress(encodeNodeMetadata(base))).toBe("");
  });

  it("reads a record too short for the fixed prefix as carrying none", () => {
    expect(readRemotingAddress(new Uint8Array(5))).toBe("");
  });

  it("reads a record whose remoting field runs past the bytes as carrying none", () => {
    const encoded: Uint8Array = appendRemotingAddress(encodeNodeMetadata(base), "ab");
    const truncated: Uint8Array = encoded.subarray(0, encoded.length - 1);
    expect(readRemotingAddress(truncated)).toBe("");
  });

  it("round-trips a remoting address with multibyte characters", () => {
    const multibyte: string = "höst.example:6000";
    const encoded: Uint8Array = appendRemotingAddress(encodeNodeMetadata(base), multibyte);
    expect(readRemotingAddress(encoded)).toBe(multibyte);
  });

  it("reads a record embedded at a non-zero byte offset in a larger buffer", () => {
    const encoded: Uint8Array = appendRemotingAddress(encodeNodeMetadata(base), "10.0.0.5:2552");
    const framed: Uint8Array = new Uint8Array(encoded.length + 4);
    framed.set(encoded, 3);
    const embedded: Uint8Array = framed.subarray(3, 3 + encoded.length);
    expect(embedded.byteOffset).toBe(3);
    expect(readRemotingAddress(embedded)).toBe("10.0.0.5:2552");
    expect(decodeNodeMetadata(embedded)).toEqual(base);
  });

  it("rejects a remoting address that pushes the record past the metadata budget", () => {
    const oversized: string = "h".repeat(500);
    expect((): Uint8Array => appendRemotingAddress(encodeNodeMetadata(base), oversized)).toThrow(
      RangeError,
    );
  });
});
