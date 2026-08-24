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

import type { ByteWriter } from "./values";

/**
 * The frame header codec: every frame on the wire is this fixed
 * 16-byte header followed by `length` body bytes.
 *
 * | Bytes | Field       | Encoding                                  |
 * | ----- | ----------- | ----------------------------------------- |
 * | 0     | version     | must be {@link PROTOCOL_VERSION}          |
 * | 1     | type        | one of the FRAME_* types                  |
 * | 2     | flags       | FLAG_* bitset, reserved bits zero         |
 * | 3     | lane        | lane byte                                 |
 * | 4..7  | length      | body byte count, big-endian uint32        |
 * | 8..15 | correlation | correlation id, big-endian uint64         |
 *
 * Decoding validates before anything else looks at the frame: the
 * version, the type, the reserved flag bits, the length bound, and
 * the correlation rules. A violation here poisons the shared stream
 * state, so it throws {@link ProtocolError}, which the session
 * answers with a connection-scoped ERROR before closing.
 *
 * @internal
 */

/**
 * Thrown for a violation that makes the stream untrustworthy; always
 * connection-scoped.
 */
export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

/** The wire protocol version, the first byte of every frame. */
export const PROTOCOL_VERSION: number = 0x01;

/** The fixed frame header size in bytes. */
export const FRAME_HEADER_SIZE: number = 16;

/** Frame types. */
export const FRAME_HELLO: number = 0x01;
export const FRAME_HELLO_ACK: number = 0x02;
export const FRAME_DATA: number = 0x03;
export const FRAME_REPLY: number = 0x04;
export const FRAME_ERROR: number = 0x05;
export const FRAME_CHUNK: number = 0x06;
export const FRAME_CREDIT: number = 0x07;
export const FRAME_TABLE: number = 0x08;
export const FRAME_PING: number = 0x09;
export const FRAME_PONG: number = 0x0a;

/** Frame flag bits; the remaining bits are reserved and must be zero. */
export const FLAG_EXPECTS_REPLY: number = 1 << 0;
export const FLAG_FIRST_CHUNK: number = 1 << 1;
export const FLAG_LAST_CHUNK: number = 1 << 2;
const RESERVED_FLAGS_MASK: number = 0xf8;

/** Lane bytes: control, ordinary lanes 1..254 (index plus one), large. */
export const LANE_CONTROL: number = 0x00;
export const LANE_LARGE: number = 0xff;
export const MAX_ORDINARY_LANES: number = 254;

/**
 * The floor under every negotiated `maxFrameSize`, so a peer can never
 * advertise a limit of zero to disable length validation.
 */
export const MIN_MAX_FRAME_SIZE: number = 16 * 1024;

/** The largest frame body length the header's 32-bit field can carry. */
export const MAX_U32: number = 0xffffffff;

/**
 * One decoded frame header. `version` is not carried: decoding already
 * rejected anything but {@link PROTOCOL_VERSION}, and encoding always
 * writes it.
 */
export interface FrameHeader {
  readonly type: number;
  readonly flags: number;
  readonly lane: number;
  /** Body byte count, bounded by the negotiated maximum on decode. */
  readonly length: number;
  /** Correlation id, zero for none; a safe integer in the runtime. */
  readonly correlation: number;
}

/** Maps an ordinary lane index (0-based) to its header lane byte. */
export function ordinaryLane(index: number): number {
  if (!Number.isInteger(index) || index < 0 || index >= MAX_ORDINARY_LANES) {
    throw new RangeError(`ordinary lane index out of range: ${index}`);
  }

  return index + 1;
}

function knownFrameType(type: number): boolean {
  return type >= FRAME_HELLO && type <= FRAME_PONG;
}

/**
 * Enforces the correlation rules shared by encode and decode: REPLY,
 * CHUNK, and DATA that expects a reply must correlate; a
 * connection-scoped ERROR uses zero.
 */
function validateCorrelation(type: number, flags: number, correlation: number): void {
  const needed: boolean =
    type === FRAME_REPLY ||
    type === FRAME_CHUNK ||
    (type === FRAME_DATA && (flags & FLAG_EXPECTS_REPLY) !== 0);
  if (needed && correlation === 0) {
    throw new ProtocolError(`frame type 0x${type.toString(16)} requires a nonzero correlation`);
  }
}

/**
 * Enforces every outbound header rule: known type, reserved flags
 * zero, length in the 32-bit range, and the correlation rules.
 * Callers that queue frames validate here at accept time and encode
 * later at flush time.
 */
export function validateFrameHeader(header: FrameHeader): void {
  if (!knownFrameType(header.type)) {
    throw new ProtocolError(`unknown frame type 0x${header.type.toString(16)}`);
  }

  if ((header.flags & RESERVED_FLAGS_MASK) !== 0) {
    throw new ProtocolError(`reserved frame flags set: 0x${header.flags.toString(16)}`);
  }

  if (!Number.isInteger(header.length) || header.length < 0 || header.length > MAX_U32) {
    throw new ProtocolError(`frame length out of range: ${header.length}`);
  }

  validateCorrelation(header.type, header.flags, header.correlation);
}

/**
 * Validates and writes a frame header. The header's `length` must
 * already equal the body that will follow; the framed connection owns
 * that check because it owns the body.
 */
export function encodeFrameHeader(writer: ByteWriter, header: FrameHeader): void {
  validateFrameHeader(header);

  writer.writeU8(PROTOCOL_VERSION);
  writer.writeU8(header.type);
  writer.writeU8(header.flags);
  writer.writeU8(header.lane);
  writer.writeU32(header.length);
  writer.writeU64(header.correlation);
}

/**
 * Validates and decodes a frame header from the 16 bytes of `bytes`
 * starting at `offset`. Pure byte arithmetic, no views and no
 * allocation beyond the returned header, which is why callers pass an
 * offset instead of slicing a view per frame. A `maxFrameSize` of
 * zero is raised to {@link MIN_MAX_FRAME_SIZE}.
 *
 * @throws A {@link ProtocolError} on any violation; the caller must
 * treat it as connection-scoped.
 */
export function decodeFrameHeader(
  bytes: Uint8Array,
  maxFrameSize: number,
  offset: number = 0,
): FrameHeader {
  if (bytes.length - offset < FRAME_HEADER_SIZE) {
    throw new ProtocolError(`frame header truncated: ${bytes.length - offset} bytes`);
  }

  const version: number = bytes[offset] as number;
  if (version !== PROTOCOL_VERSION) {
    throw new ProtocolError(`unsupported protocol version 0x${version.toString(16)}`);
  }

  const type: number = bytes[offset + 1] as number;
  if (!knownFrameType(type)) {
    throw new ProtocolError(`unknown frame type 0x${type.toString(16)}`);
  }

  const flags: number = bytes[offset + 2] as number;
  if ((flags & RESERVED_FLAGS_MASK) !== 0) {
    throw new ProtocolError(`reserved frame flags set: 0x${flags.toString(16)}`);
  }

  const lane: number = bytes[offset + 3] as number;
  const length: number =
    (((bytes[offset + 4] as number) << 24) |
      ((bytes[offset + 5] as number) << 16) |
      ((bytes[offset + 6] as number) << 8) |
      (bytes[offset + 7] as number)) >>>
    0;
  const bound: number = maxFrameSize === 0 ? MIN_MAX_FRAME_SIZE : maxFrameSize;
  if (length > bound) {
    throw new ProtocolError(`frame length ${length} exceeds the maximum ${bound}`);
  }

  const high: number =
    (((bytes[offset + 8] as number) << 24) |
      ((bytes[offset + 9] as number) << 16) |
      ((bytes[offset + 10] as number) << 8) |
      (bytes[offset + 11] as number)) >>>
    0;
  const low: number =
    (((bytes[offset + 12] as number) << 24) |
      ((bytes[offset + 13] as number) << 16) |
      ((bytes[offset + 14] as number) << 8) |
      (bytes[offset + 15] as number)) >>>
    0;
  const correlation: number = high * 0x100000000 + low;
  if (!Number.isSafeInteger(correlation)) {
    throw new ProtocolError("correlation exceeds the safe integer range");
  }

  validateCorrelation(type, flags, correlation);

  return { type, flags, lane, length, correlation };
}
