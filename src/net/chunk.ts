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

import type { OutboundFrame } from "./conn";
import {
  decodeFrameHeader,
  encodeFrameHeader,
  FLAG_EXPECTS_REPLY,
  FLAG_FIRST_CHUNK,
  FLAG_LAST_CHUNK,
  FRAME_CHUNK,
  FRAME_HEADER_SIZE,
  type FrameHeader,
} from "./frame";
import { ByteReader, ByteWriter } from "./values";

/**
 * Chunking: a DATA or REPLY whose encoded logical frame (header plus
 * body) exceeds the chunk size travels as CHUNK frames sharing one
 * group correlation. Each CHUNK body is a uvarint index, a uvarint
 * total logical size on the first chunk only, and a slice of the
 * logical bytes; indexes start at zero and must be contiguous, since
 * TCP already guarantees order and a gap is corruption. Chunk bodies
 * are capped at exactly the chunk size so receive buffers stay
 * uniform. A sender failing mid-group emits a short lastChunk frame
 * with no data, which aborts the group on the receiver.
 *
 * @internal
 */

/** The default logical-frame threshold above which a sender splits. */
export const DEFAULT_CHUNK_SIZE: number = 256 * 1024;

/** Room the per-chunk uvarint prefix may need: two 8-byte uvarints. */
const CHUNK_PREFIX_MAX: number = 16;

/**
 * Splits one logical frame into CHUNK frames of at most `chunkSize`
 * body bytes each, all correlated by `groupCorrelation`. The inner
 * frame's own header rides at the start of the logical bytes, so the
 * receiver recovers it whole, its correlation included. The frame's
 * confirmation token, when it carries one, rides on the last chunk:
 * a chunked message is confirmed when its final bytes are.
 */
export function splitLogicalFrame(
  frame: OutboundFrame,
  chunkSize: number,
  groupCorrelation: number,
): OutboundFrame[] {
  if (chunkSize <= CHUNK_PREFIX_MAX) {
    throw new TypeError(`chunk size ${chunkSize} cannot hold the chunk prefix`);
  }

  if (groupCorrelation === 0) {
    throw new TypeError("a chunk group correlation must be nonzero");
  }

  const body: Uint8Array = frame.body ?? new Uint8Array(0);
  const writer: ByteWriter = new ByteWriter(FRAME_HEADER_SIZE + body.length);

  encodeFrameHeader(writer, {
    type: frame.type,
    flags: frame.flags,
    lane: frame.lane,
    length: body.length,
    correlation: frame.correlation,
  });

  writer.writeBytes(body);
  const logical: Uint8Array = writer.bytes();

  const chunks: OutboundFrame[] = [];
  const prefix: ByteWriter = new ByteWriter(CHUNK_PREFIX_MAX);
  let offset: number = 0;
  let index: number = 0;

  while (offset < logical.length) {
    prefix.reset();
    prefix.writeUvarint(index);
    if (index === 0) {
      prefix.writeUvarint(logical.length);
    }

    const room: number = chunkSize - prefix.length;
    const take: number = Math.min(room, logical.length - offset);
    const chunkBody: Uint8Array = new Uint8Array(prefix.length + take);
    chunkBody.set(prefix.bytes());
    chunkBody.set(logical.subarray(offset, offset + take), prefix.length);
    offset += take;

    let flags: number = 0;
    if (index === 0) {
      flags |= FLAG_FIRST_CHUNK | (frame.flags & FLAG_EXPECTS_REPLY);
    }

    const last: boolean = offset === logical.length;
    if (last) {
      flags |= FLAG_LAST_CHUNK;
    }

    const chunk: OutboundFrame = {
      type: FRAME_CHUNK,
      flags,
      lane: frame.lane,
      correlation: groupCorrelation,
      body: chunkBody,
    };
    chunks.push(last && frame.wrote !== undefined ? { ...chunk, wrote: frame.wrote } : chunk);
    index += 1;
  }

  return chunks;
}

/**
 * The abort frame a sender emits after a mid-group failure: lastChunk
 * with the next index and no data, forcing the receiver's group to
 * finish short and be discarded.
 */
export function abortChunkFrame(lane: number, correlation: number, index: number): OutboundFrame {
  const writer: ByteWriter = new ByteWriter(CHUNK_PREFIX_MAX);
  writer.writeUvarint(index);
  return {
    type: FRAME_CHUNK,
    flags: FLAG_LAST_CHUNK,
    lane,
    correlation,
    body: writer.bytes().slice(),
  };
}

/**
 * What one pushed chunk amounted to, as integer kinds like every
 * other wire-adjacent discriminant. Pending covers progress on a live
 * group and orphan continuations for unknown groups (the tail of an
 * aborted or rejected group must not kill the connection). Reject is
 * request-scoped: the message dies, the connection lives. Violation
 * is connection-scoped corruption.
 */
export type ReassemblyKind = 0 | 1 | 2 | 3 | 4;
export const REASSEMBLY_PENDING: ReassemblyKind = 0;
export const REASSEMBLY_ABORTED: ReassemblyKind = 1;
export const REASSEMBLY_COMPLETE: ReassemblyKind = 2;
export const REASSEMBLY_REJECT: ReassemblyKind = 3;
export const REASSEMBLY_VIOLATION: ReassemblyKind = 4;

export interface ReassemblyOutcome {
  readonly kind: ReassemblyKind;
  /** The reassembled inner header; null unless complete. */
  readonly header: FrameHeader | null;
  /** The reassembled inner body; null unless complete. */
  readonly body: Uint8Array | null;
  /** The reject or violation reason; empty otherwise. */
  readonly reason: string;
}

/** The two payload-free outcomes, shared so pushes allocate nothing. */
const PENDING: ReassemblyOutcome = {
  kind: REASSEMBLY_PENDING,
  header: null,
  body: null,
  reason: "",
};

const ABORTED: ReassemblyOutcome = {
  kind: REASSEMBLY_ABORTED,
  header: null,
  body: null,
  reason: "",
};

function reject(reason: string): ReassemblyOutcome {
  return { kind: REASSEMBLY_REJECT, header: null, body: null, reason };
}

function violation(reason: string): ReassemblyOutcome {
  return { kind: REASSEMBLY_VIOLATION, header: null, body: null, reason };
}

interface ChunkGroup {
  readonly buffer: Uint8Array;
  written: number;
  nextIndex: number;
}

/**
 * Reassembles chunk groups keyed by correlation. One exact-size
 * buffer per group, allocated from the declared total on the first
 * chunk; the soft limits (declared total over the message cap, more
 * concurrent groups than allowed) reject the message and keep the
 * connection. There is no timer: partial groups are bounded by the
 * concurrency cap times the message cap and are freed by abort,
 * violation, or {@link ChunkReassembler.clear} at teardown.
 */
export class ChunkReassembler {
  private readonly _groups: Map<number, ChunkGroup> = new Map<number, ChunkGroup>();
  private readonly _maxMessageSize: number;
  private readonly _maxGroups: number;

  constructor(maxMessageSize: number, maxGroups: number) {
    this._maxMessageSize = maxMessageSize;
    this._maxGroups = maxGroups < 1 ? 1 : maxGroups;
  }

  /** Live partial groups, for tests and diagnostics. */
  get activeGroups(): number {
    return this._groups.size;
  }

  /** Drops every partial group; called at connection teardown. */
  clear(): void {
    this._groups.clear();
  }

  push(header: FrameHeader, body: Uint8Array): ReassemblyOutcome {
    const reader: ByteReader = new ByteReader(body);
    let index: number;
    try {
      index = reader.readUvarint();
    } catch {
      return violation("chunk index is malformed");
    }

    const first: boolean = (header.flags & FLAG_FIRST_CHUNK) !== 0;
    const last: boolean = (header.flags & FLAG_LAST_CHUNK) !== 0;
    let group: ChunkGroup | undefined = this._groups.get(header.correlation);

    if (first) {
      if (group !== undefined) {
        return violation("duplicate first chunk for a live group");
      }

      let total: number;
      try {
        total = reader.readUvarint();
      } catch {
        return violation("chunk total size is malformed");
      }

      if (total <= FRAME_HEADER_SIZE) {
        return violation(`declared logical size ${total} is too small`);
      }

      if (total > this._maxMessageSize) {
        return reject(
          `logical message of ${total} bytes exceeds the ${this._maxMessageSize} byte limit`,
        );
      }

      if (this._groups.size >= this._maxGroups) {
        return reject("too many concurrent large transfers");
      }

      if (index !== 0) {
        return violation("first chunk index must be zero");
      }

      group = { buffer: new Uint8Array(total), written: 0, nextIndex: 1 };
      this._groups.set(header.correlation, group);
    }

    if (group === undefined) {
      return PENDING;
    }

    if (!first) {
      if (index !== group.nextIndex) {
        this._groups.delete(header.correlation);
        return violation(`chunk index ${index} arrived, ${group.nextIndex} expected`);
      }

      group.nextIndex += 1;
    }

    const data: Uint8Array = reader.readBytes(reader.remaining);
    if (group.written + data.length > group.buffer.length) {
      this._groups.delete(header.correlation);
      return violation("chunk data exceeds the declared total");
    }

    group.buffer.set(data, group.written);
    group.written += data.length;

    if (!last) {
      return PENDING;
    }

    this._groups.delete(header.correlation);
    if (group.written < group.buffer.length) {
      return ABORTED;
    }

    let inner: FrameHeader;
    try {
      inner = decodeFrameHeader(group.buffer, group.buffer.length);
    } catch (thrown) {
      // The frame decoder throws only Error subclasses.
      return violation((thrown as Error).message);
    }

    if (inner.length !== group.buffer.length - FRAME_HEADER_SIZE) {
      return violation("logical frame length mismatch");
    }

    return {
      kind: REASSEMBLY_COMPLETE,
      header: inner,
      body: group.buffer.subarray(FRAME_HEADER_SIZE),
      reason: "",
    };
  }
}
