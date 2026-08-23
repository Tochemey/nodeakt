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
  decodeFrameHeader,
  encodeFrameHeader,
  FLAG_EXPECTS_REPLY,
  FLAG_FIRST_CHUNK,
  FLAG_LAST_CHUNK,
  FRAME_CHUNK,
  FRAME_CREDIT,
  FRAME_DATA,
  FRAME_ERROR,
  FRAME_HEADER_SIZE,
  FRAME_HELLO,
  FRAME_HELLO_ACK,
  FRAME_PING,
  FRAME_PONG,
  FRAME_REPLY,
  FRAME_TABLE,
  type FrameHeader,
  LANE_CONTROL,
  LANE_LARGE,
  MIN_MAX_FRAME_SIZE,
  ordinaryLane,
  ProtocolError,
} from "../../src/net/frame";
import { ByteWriter } from "../../src/net/values";

const NO_LIMIT: number = 0xffffffff;

function encodeToBytes(header: FrameHeader): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeFrameHeader(writer, header);
  return Uint8Array.from(writer.bytes());
}

function roundTrip(header: FrameHeader, maxFrameSize: number = NO_LIMIT): FrameHeader {
  return decodeFrameHeader(encodeToBytes(header), maxFrameSize);
}

describe("frame header codec", () => {
  it("encodes the documented byte layout", () => {
    const bytes: Uint8Array = encodeToBytes({
      type: FRAME_DATA,
      flags: FLAG_EXPECTS_REPLY,
      lane: ordinaryLane(0),
      length: 0x01020304,
      correlation: 0x0102030405,
    });
    expect(bytes.length).toBe(FRAME_HEADER_SIZE);
    expect(Array.from(bytes)).toEqual([
      0x01, 0x03, 0x01, 0x01, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04,
      0x05,
    ]);
  });

  it("round-trips every frame type", () => {
    const types: number[] = [
      FRAME_HELLO,
      FRAME_HELLO_ACK,
      FRAME_DATA,
      FRAME_REPLY,
      FRAME_ERROR,
      FRAME_CHUNK,
      FRAME_CREDIT,
      FRAME_TABLE,
      FRAME_PING,
      FRAME_PONG,
    ];
    for (const type of types) {
      const correlated: boolean = type === FRAME_REPLY || type === FRAME_CHUNK;
      const header: FrameHeader = {
        type,
        flags: 0,
        lane: LANE_CONTROL,
        length: 42,
        correlation: correlated ? 7 : 0,
      };
      expect(roundTrip(header)).toEqual(header);
    }
  });

  it("round-trips flags, lanes, and a large correlation", () => {
    const header: FrameHeader = {
      type: FRAME_CHUNK,
      flags: FLAG_FIRST_CHUNK | FLAG_LAST_CHUNK,
      lane: LANE_LARGE,
      length: 0xffffffff,
      correlation: Number.MAX_SAFE_INTEGER,
    };
    expect(roundTrip(header)).toEqual(header);
  });

  it("maps ordinary lane indexes to bytes and bounds them", () => {
    expect(ordinaryLane(0)).toBe(0x01);
    expect(ordinaryLane(253)).toBe(0xfe);
    expect(() => ordinaryLane(254)).toThrow(RangeError);
    expect(() => ordinaryLane(-1)).toThrow(RangeError);
  });

  it("rejects a wrong version and an unknown type", () => {
    const bytes: Uint8Array = encodeToBytes({
      type: FRAME_PING,
      flags: 0,
      lane: LANE_CONTROL,
      length: 0,
      correlation: 1,
    });
    bytes[0] = 0x02;
    expect(() => decodeFrameHeader(bytes, NO_LIMIT)).toThrow(ProtocolError);

    bytes[0] = 0x01;
    bytes[1] = 0x0b;
    expect(() => decodeFrameHeader(bytes, NO_LIMIT)).toThrow(ProtocolError);
  });

  it("rejects reserved flag bits on both paths", () => {
    const header: FrameHeader = {
      type: FRAME_DATA,
      flags: 0x08,
      lane: LANE_CONTROL,
      length: 0,
      correlation: 0,
    };
    expect(() => encodeToBytes(header)).toThrow(ProtocolError);

    const bytes: Uint8Array = encodeToBytes({ ...header, flags: 0 });
    bytes[2] = 0x80;
    expect(() => decodeFrameHeader(bytes, NO_LIMIT)).toThrow(ProtocolError);
  });

  it("bounds the length and floors a zero maximum", () => {
    const header: FrameHeader = {
      type: FRAME_DATA,
      flags: 0,
      lane: LANE_CONTROL,
      length: MIN_MAX_FRAME_SIZE + 1,
      correlation: 0,
    };
    expect(() => roundTrip(header, 0)).toThrow(ProtocolError);
    expect(roundTrip(header, MIN_MAX_FRAME_SIZE + 1)).toEqual(header);
    expect(roundTrip({ ...header, length: MIN_MAX_FRAME_SIZE }, 0).length).toBe(MIN_MAX_FRAME_SIZE);
  });

  it("enforces the correlation rules on both paths", () => {
    const uncorrelated: FrameHeader[] = [
      { type: FRAME_REPLY, flags: 0, lane: LANE_CONTROL, length: 0, correlation: 0 },
      { type: FRAME_CHUNK, flags: FLAG_FIRST_CHUNK, lane: LANE_LARGE, length: 0, correlation: 0 },
      {
        type: FRAME_DATA,
        flags: FLAG_EXPECTS_REPLY,
        lane: LANE_CONTROL,
        length: 0,
        correlation: 0,
      },
    ];
    for (const header of uncorrelated) {
      expect(() => encodeToBytes(header)).toThrow(ProtocolError);
    }

    const bytes: Uint8Array = encodeToBytes({
      type: FRAME_REPLY,
      flags: 0,
      lane: LANE_CONTROL,
      length: 0,
      correlation: 1,
    });
    bytes[15] = 0x00;
    expect(() => decodeFrameHeader(bytes, NO_LIMIT)).toThrow(ProtocolError);
  });

  it("allows a connection-scoped ERROR with correlation zero", () => {
    const header: FrameHeader = {
      type: FRAME_ERROR,
      flags: 0,
      lane: LANE_CONTROL,
      length: 12,
      correlation: 0,
    };
    expect(roundTrip(header)).toEqual(header);
  });

  it("rejects a truncated header and an unsafe correlation", () => {
    expect(() => decodeFrameHeader(new Uint8Array(15), NO_LIMIT)).toThrow(ProtocolError);

    const bytes: Uint8Array = encodeToBytes({
      type: FRAME_PING,
      flags: 0,
      lane: LANE_CONTROL,
      length: 0,
      correlation: 1,
    });
    bytes.fill(0xff, 8, 16);
    expect(() => decodeFrameHeader(bytes, NO_LIMIT)).toThrow(ProtocolError);
  });
});
