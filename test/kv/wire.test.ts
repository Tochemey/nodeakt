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
  MAX_CHUNK_ENTRIES,
  MAX_KEY_BYTES,
  MAX_NAME_BYTES,
  MAX_VALUE_BYTES,
  MAX_WIRE_PARTITIONS,
  REPAIR_BUCKETS,
} from "../../src/kv/constants";
import { KvProtocolError } from "../../src/kv/errors";
import type { Entry, HybridTime, KeyVersion, WriteOp, WriteResult } from "../../src/kv/ports";
import {
  ByteReader,
  ByteWriter,
  decodeMessage,
  encodeMessage,
  type KvMessage,
  MessageKind,
  MSG_READ_REQUEST,
  messageType,
  type PartitionOwners,
} from "../../src/kv/wire";

const time: HybridTime = { wallMs: 1_700_000_000_000, logical: 3, node: "10.0.0.1:7000" };

function liveEntry(overrides: Partial<Entry> = {}): Entry {
  return {
    key: "payments-42",
    value: new Uint8Array([1, 2, 3]),
    timestamp: time,
    sequence: 42n,
    expiresAt: undefined,
    deleted: false,
    ...overrides,
  };
}

function tombstone(): Entry {
  return {
    key: "gone",
    value: undefined,
    timestamp: time,
    sequence: 7n,
    expiresAt: 1_700_000_100_000,
    deleted: true,
  };
}

function roundTrip(message: KvMessage): KvMessage {
  return decodeMessage(encodeMessage(message));
}

describe("ByteWriter and ByteReader", () => {
  it("round-trips every scalar width and raw bytes across a buffer growth", () => {
    const writer: ByteWriter = new ByteWriter(1);
    writer.u8(0xab);
    writer.u16(0xbeef);
    writer.u32(0xdeadbeef);
    writer.u64(0xffffffffffffffffn);
    writer.i64(-5n);
    writer.raw(new Uint8Array([9, 8, 7]));
    const reader: ByteReader = new ByteReader(writer.finish());
    expect(reader.u8()).toBe(0xab);
    expect(reader.u16()).toBe(0xbeef);
    expect(reader.u32()).toBe(0xdeadbeef);
    expect(reader.u64()).toBe(0xffffffffffffffffn);
    expect(reader.i64()).toBe(-5n);
    expect(reader.take(3)).toEqual(new Uint8Array([9, 8, 7]));
    expect(reader.remaining).toBe(0);
    reader.end();
  });

  it("reads from a subarray view without leaking neighboring bytes", () => {
    const backing: Uint8Array = new Uint8Array([0, 0, 0x12, 0x34, 0, 0]);
    const view: Uint8Array = backing.subarray(2, 4);
    const reader: ByteReader = new ByteReader(view);
    expect(reader.u16()).toBe(0x1234);
    reader.end();
  });

  it("fails on a read past the end", () => {
    const reader: ByteReader = new ByteReader(new Uint8Array([1]));
    expect((): number => reader.u16()).toThrow(KvProtocolError);
  });

  it("fails when bytes remain after end", () => {
    const reader: ByteReader = new ByteReader(new Uint8Array([1, 2]));
    reader.u8();
    expect((): void => reader.end()).toThrow(KvProtocolError);
  });
});

describe("message round-trips", () => {
  it("round-trips a read request", () => {
    expect(roundTrip({ kind: "read-request", key: "a" })).toEqual({
      kind: "read-request",
      key: "a",
    });
  });

  it("round-trips a present and an absent read response", () => {
    expect(roundTrip({ kind: "read-response", entry: liveEntry() })).toEqual({
      kind: "read-response",
      entry: liveEntry(),
    });
    expect(roundTrip({ kind: "read-response", entry: undefined })).toEqual({
      kind: "read-response",
      entry: undefined,
    });
  });

  it("round-trips a tombstone entry with an expiry", () => {
    const message: KvMessage = { kind: "read-response", entry: tombstone() };
    expect(roundTrip(message)).toEqual(message);
  });

  it("round-trips every write op kind", () => {
    const ops: WriteOp[] = [
      { kind: "put", key: "k", value: new Uint8Array([1]), condition: "none" },
      { kind: "put", key: "k", value: new Uint8Array([1]), condition: "nx", ttlMs: 5_000 },
      { kind: "put", key: "k", value: new Uint8Array(), condition: "xx" },
      { kind: "delete", key: "k" },
      { kind: "incr", key: "k", delta: -9_223_372_036_854_775_808n },
      { kind: "incr", key: "k", delta: 9_223_372_036_854_775_807n },
      { kind: "cas", key: "k", expected: new Uint8Array([1]), value: new Uint8Array([2]) },
    ];
    for (const op of ops) {
      expect(roundTrip({ kind: "write-request", op })).toEqual({ kind: "write-request", op });
    }
  });

  it("round-trips both write results", () => {
    const applied: WriteResult = { applied: true, entry: liveEntry() };
    const rejected: WriteResult = { applied: false, reason: "nx" };
    expect(roundTrip({ kind: "write-response", result: applied })).toEqual({
      kind: "write-response",
      result: applied,
    });
    expect(roundTrip({ kind: "write-response", result: rejected })).toEqual({
      kind: "write-response",
      result: rejected,
    });
  });

  it("round-trips a routing table", () => {
    const message: KvMessage = {
      kind: "table",
      table: {
        version: 12n,
        partitions: [{ owners: ["n1"] }, { owners: ["n2", "n1"] }, { owners: ["n3", "n2", "n1"] }],
      },
    };
    expect(roundTrip(message)).toEqual(message);
  });

  it("round-trips a fragment chunk including an empty final chunk", () => {
    const full: KvMessage = {
      kind: "fragment-chunk",
      chunk: { partitionId: 5, final: false, entries: [liveEntry(), tombstone()] },
    };
    const empty: KvMessage = {
      kind: "fragment-chunk",
      chunk: { partitionId: 5, final: true, entries: [] },
    };
    expect(roundTrip(full)).toEqual(full);
    expect(roundTrip(empty)).toEqual(empty);
  });

  it("round-trips an ownership report including an empty one", () => {
    const full: KvMessage = {
      kind: "ownership-report",
      report: { node: "10.0.0.1:7000", partitions: [0, 5, 511] },
    };
    const empty: KvMessage = {
      kind: "ownership-report",
      report: { node: "n2", partitions: [] },
    };
    expect(roundTrip(full)).toEqual(full);
    expect(roundTrip(empty)).toEqual(empty);
  });

  it("round-trips a replicate and its acknowledgment", () => {
    const replicate: KvMessage = { kind: "replicate", entry: liveEntry() };
    const ack: KvMessage = { kind: "replicate-ack" };
    expect(roundTrip(replicate)).toEqual(replicate);
    expect(roundTrip(ack)).toEqual(ack);
  });

  it("round-trips a peek request, a rebalancing reply, and a fragment request", () => {
    const peek: KvMessage = { kind: "peek-request", key: "payments-42" };
    const rebalancing: KvMessage = { kind: "rebalancing", partitionId: 511 };
    const fragment: KvMessage = { kind: "fragment-request", partitionId: 7, afterKey: undefined };
    expect(roundTrip(peek)).toEqual(peek);
    expect(roundTrip(rebalancing)).toEqual(rebalancing);
    expect(roundTrip(fragment)).toEqual(fragment);
  });

  it("round-trips a fragment request carrying a cursor key", () => {
    const fragment: KvMessage = {
      kind: "fragment-request",
      partitionId: 7,
      afterKey: "payments-42",
    };
    expect(roundTrip(fragment)).toEqual(fragment);
  });

  it("round-trips a fragment push chunk and its acknowledgment", () => {
    const push: KvMessage = {
      kind: "fragment-push",
      chunk: { partitionId: 3, final: false, entries: [liveEntry(), liveEntry({ key: "k2" })] },
    };
    const ack: KvMessage = { kind: "fragment-ack" };
    expect(roundTrip(push)).toEqual(push);
    expect(roundTrip(ack)).toEqual(ack);
  });

  it("rejects an invalid fragment cursor flag", () => {
    const framed: Uint8Array = encodeMessage({
      kind: "fragment-request",
      partitionId: 1,
      afterKey: undefined,
    });
    const corrupted: Uint8Array = framed.slice();
    corrupted[corrupted.length - 1] = 2;
    expect((): KvMessage => decodeMessage(corrupted)).toThrow(KvProtocolError);
  });

  it("rejects an out-of-range partition id in a rebalancing or fragment message", () => {
    expect(
      (): Uint8Array => encodeMessage({ kind: "rebalancing", partitionId: 0x1_0000_0000 }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({ kind: "fragment-request", partitionId: -1, afterKey: undefined }),
    ).toThrow(KvProtocolError);
  });

  it("preserves multibyte keys and node identities as UTF-8", () => {
    const message: KvMessage = {
      kind: "read-response",
      entry: liveEntry({ key: "café-😀", timestamp: { wallMs: 1, logical: 0, node: "béta:1" } }),
    };
    expect(roundTrip(message)).toEqual(message);
  });
});

describe("encode-side rejection", () => {
  it("rejects an inconsistent entry, an empty key, and oversized fields", () => {
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "read-response",
          entry: liveEntry({ deleted: true }),
        }),
    ).toThrow(KvProtocolError);
    expect((): Uint8Array => encodeMessage({ kind: "read-request", key: "" })).toThrow(
      KvProtocolError,
    );
    expect(
      (): Uint8Array => encodeMessage({ kind: "read-request", key: "k".repeat(MAX_KEY_BYTES + 1) }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "write-request",
          op: {
            kind: "put",
            key: "k",
            value: new Uint8Array(MAX_VALUE_BYTES + 1),
            condition: "none",
          },
        }),
    ).toThrow(KvProtocolError);
  });

  it("rejects a NUL or overlong node identity and an overlong owner", () => {
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "read-response",
          entry: liveEntry({ timestamp: { wallMs: 1, logical: 0, node: "a b" } }),
        }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "table",
          table: { version: 1n, partitions: [{ owners: ["n".repeat(MAX_NAME_BYTES + 1)] }] },
        }),
    ).toThrow(KvProtocolError);
  });

  it("rejects a table with an empty owners list and a chunk with a bad partition id", () => {
    expect(
      (): Uint8Array =>
        encodeMessage({ kind: "table", table: { version: 1n, partitions: [{ owners: [] }] } }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "fragment-chunk",
          chunk: { partitionId: -1, final: false, entries: [] },
        }),
    ).toThrow(KvProtocolError);
  });

  it("rejects a logical counter and a partition id beyond the unsigned 32-bit range", () => {
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "read-response",
          entry: liveEntry({ timestamp: { wallMs: 1, logical: 0x1_0000_0000, node: "n1" } }),
        }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "fragment-chunk",
          chunk: { partitionId: 0x1_0000_0000, final: false, entries: [] },
        }),
    ).toThrow(KvProtocolError);
  });

  it("rejects an out-of-range increment delta and a negative expiry", () => {
    expect(
      (): Uint8Array =>
        encodeMessage({ kind: "write-request", op: { kind: "incr", key: "k", delta: 2n ** 63n } }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({ kind: "read-response", entry: liveEntry({ expiresAt: -1 }) }),
    ).toThrow(KvProtocolError);
  });
});

describe("decode-side rejection", () => {
  it("rejects an unknown version, type, and trailing bytes", () => {
    const good: Uint8Array = encodeMessage({ kind: "read-request", key: "a" });
    const badVersion: Uint8Array = Uint8Array.from(good);
    badVersion[0] = 2;
    expect((): KvMessage => decodeMessage(badVersion)).toThrow(KvProtocolError);
    const badType: Uint8Array = Uint8Array.from(good);
    badType[1] = 0x7f;
    expect((): KvMessage => decodeMessage(badType)).toThrow(KvProtocolError);
    const trailing: Uint8Array = new Uint8Array([...good, 0]);
    expect((): KvMessage => decodeMessage(trailing)).toThrow(KvProtocolError);
  });

  it("rejects a truncated body", () => {
    const full: Uint8Array = encodeMessage({ kind: "read-response", entry: liveEntry() });
    expect((): KvMessage => decodeMessage(full.subarray(0, full.length - 2))).toThrow(
      KvProtocolError,
    );
  });

  it("rejects a key length that exceeds the remaining bytes before allocating", () => {
    // version, MSG_READ_REQUEST, then a u16 key length of 60000 with no key bytes.
    const crafted: Uint8Array = new Uint8Array([1, 0x01, 0xea, 0x60]);
    expect((): KvMessage => decodeMessage(crafted)).toThrow(KvProtocolError);
  });

  it("rejects unknown opcodes, conditions, reasons, and flags", () => {
    const unknownOpcode: Uint8Array = new Uint8Array([1, 0x03, 0x09]);
    expect((): KvMessage => decodeMessage(unknownOpcode)).toThrow(KvProtocolError);
    const badCondition: Uint8Array = new Uint8Array([1, 0x03, 0x01, 0x09, 0x00]);
    expect((): KvMessage => decodeMessage(badCondition)).toThrow(KvProtocolError);
    const badResultMarker: Uint8Array = new Uint8Array([1, 0x04, 0x09]);
    expect((): KvMessage => decodeMessage(badResultMarker)).toThrow(KvProtocolError);
    const badReason: Uint8Array = new Uint8Array([1, 0x04, 0x00, 0x09]);
    expect((): KvMessage => decodeMessage(badReason)).toThrow(KvProtocolError);
  });

  it("rejects an invalid entry flag byte and a bad presence byte", () => {
    const badPresence: Uint8Array = new Uint8Array([1, 0x02, 0x09]);
    expect((): KvMessage => decodeMessage(badPresence)).toThrow(KvProtocolError);
    const badFlags: Uint8Array = new Uint8Array([1, 0x02, 0x01, 0xf0]);
    expect((): KvMessage => decodeMessage(badFlags)).toThrow(KvProtocolError);
  });

  it("rejects invalid UTF-8 in a decoded key", () => {
    // read-request, u16 length 2, then a lone continuation byte pair.
    const crafted: Uint8Array = new Uint8Array([1, 0x01, 0x00, 0x02, 0xff, 0xfe]);
    expect((): KvMessage => decodeMessage(crafted)).toThrow(KvProtocolError);
  });

  it("rejects a table owner count of zero and a bad chunk final flag", () => {
    const zeroOwners: Uint8Array = new Uint8Array([1, 0x10, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0]);
    expect((): KvMessage => decodeMessage(zeroOwners)).toThrow(KvProtocolError);
    const badFinal: Uint8Array = new Uint8Array([1, 0x11, 0, 0, 0, 5, 0x09]);
    expect((): KvMessage => decodeMessage(badFinal)).toThrow(KvProtocolError);
  });

  it("rejects a wall time beyond the exact number range", () => {
    const writer: ByteWriter = new ByteWriter();
    writer.u8(1);
    writer.u8(0x02);
    writer.u8(1);
    writer.u8(0);
    writer.u16(1);
    writer.raw(new TextEncoder().encode("k"));
    writer.u64(0xffffffffffffffffn);
    expect((): KvMessage => decodeMessage(writer.finish())).toThrow(KvProtocolError);
  });
});

/** Builds a valid read-response entry prefix up to the sequence, for the caller to complete. */
function entryPrefix(flags: number): ByteWriter {
  const writer: ByteWriter = new ByteWriter();
  const coder: TextEncoder = new TextEncoder();
  writer.u8(1);
  writer.u8(0x02);
  writer.u8(1);
  writer.u8(flags);
  writer.u16(1);
  writer.raw(coder.encode("k"));
  writer.u64(1n);
  writer.u32(0);
  writer.u8(2);
  writer.raw(coder.encode("n1"));
  writer.u64(0n);
  return writer;
}

describe("remaining validation paths", () => {
  it("rejects malformed identities and keys on encode", () => {
    expect((): Uint8Array => encodeMessage({ kind: "read-request", key: "a\0b" })).toThrow(
      KvProtocolError,
    );
    expect((): Uint8Array => encodeMessage({ kind: "read-request", key: "\uD800" })).toThrow(
      KvProtocolError,
    );
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "table",
          table: { version: 1n, partitions: [{ owners: ["\uD800"] }] },
        }),
    ).toThrow(KvProtocolError);
  });

  it("rejects a table version beyond the unsigned 64-bit range on encode", () => {
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "table",
          table: { version: 2n ** 64n, partitions: [{ owners: ["a"] }] },
        }),
    ).toThrow(KvProtocolError);
  });

  it("rejects list counts beyond their wire bounds on encode", () => {
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "table",
          table: { version: 1n, partitions: new Array<PartitionOwners>(MAX_WIRE_PARTITIONS + 1) },
        }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "fragment-chunk",
          chunk: { partitionId: 0, final: false, entries: new Array<Entry>(MAX_CHUNK_ENTRIES + 1) },
        }),
    ).toThrow(KvProtocolError);
  });

  it("rejects a value length beyond its bound before allocating", () => {
    const writer: ByteWriter = entryPrefix(0);
    writer.u32(MAX_VALUE_BYTES + 1);
    expect((): KvMessage => decodeMessage(writer.finish())).toThrow(KvProtocolError);
  });

  it("rejects an expiry beyond the exact number range", () => {
    const writer: ByteWriter = entryPrefix(0x02);
    writer.u64(0xffffffffffffffffn);
    expect((): KvMessage => decodeMessage(writer.finish())).toThrow(KvProtocolError);
  });

  it("rejects an invalid put ttl flag and a zero-length owner name", () => {
    const badTtlFlag: Uint8Array = new Uint8Array([1, 0x03, 0x01, 0x00, 0x02]);
    expect((): KvMessage => decodeMessage(badTtlFlag)).toThrow(KvProtocolError);
    const emptyOwnerName: Uint8Array = new Uint8Array([
      1, 0x10, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0,
    ]);
    expect((): KvMessage => decodeMessage(emptyOwnerName)).toThrow(KvProtocolError);
  });

  it("rejects valid-UTF-8 bytes that decode to a NUL", () => {
    const nulKey: Uint8Array = new Uint8Array([1, 0x01, 0x00, 0x01, 0x00]);
    expect((): KvMessage => decodeMessage(nulKey)).toThrow(KvProtocolError);
  });

  it("rejects list counts beyond their wire bounds on decode", () => {
    const bigTable: Uint8Array = new Uint8Array([
      1, 0x10, 0, 0, 0, 0, 0, 0, 0, 1, 0x00, 0x10, 0x00, 0x01,
    ]);
    expect((): KvMessage => decodeMessage(bigTable)).toThrow(KvProtocolError);
    const bigChunk: Uint8Array = new Uint8Array([1, 0x11, 0, 0, 0, 0, 0, 0x00, 0x10, 0x00, 0x01]);
    expect((): KvMessage => decodeMessage(bigChunk)).toThrow(KvProtocolError);
  });

  it("rejects an ownership report with a bad partition id or oversized count on encode", () => {
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "ownership-report",
          report: { node: "n1", partitions: [0x1_0000_0000] },
        }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "ownership-report",
          report: { node: "n1", partitions: new Array<number>(MAX_WIRE_PARTITIONS + 1) },
        }),
    ).toThrow(KvProtocolError);
  });

  it("rejects an ownership report partition count beyond its bound on decode", () => {
    // version, MSG_OWNERSHIP_REPORT, node length 1 "n", then a u32 count of 0x00100001.
    const crafted: Uint8Array = new Uint8Array([1, 0x12, 1, 0x6e, 0x00, 0x10, 0x00, 0x01]);
    expect((): KvMessage => decodeMessage(crafted)).toThrow(KvProtocolError);
  });

  it("round-trips the anti-entropy messages", () => {
    const digest: KvMessage = { kind: "sync-digest", partitionId: 5, digest: { hi: 7, lo: 9 } };
    const buckets: KvMessage = {
      kind: "bucket-digests",
      partitionId: 5,
      digests: [
        { hi: 1, lo: 2 },
        { hi: 3, lo: 4 },
      ],
    };
    const inSync: KvMessage = { kind: "bucket-digests", partitionId: 5, digests: [] };
    const request: KvMessage = { kind: "key-versions-request", partitionId: 5, buckets: [0, 3, 7] };
    const versions: KvMessage = {
      kind: "key-versions",
      partitionId: 5,
      versions: [{ key: "payments-42", timestamp: { wallMs: 1, logical: 2, node: "n1" } }],
    };
    const entries: KvMessage = { kind: "entries-request", partitionId: 5, keys: ["a", "b"] };
    for (const message of [digest, buckets, inSync, request, versions, entries]) {
      expect(roundTrip(message)).toEqual(message);
    }
  });

  it("rejects anti-entropy list counts beyond their bounds on encode", () => {
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "bucket-digests",
          partitionId: 0,
          digests: new Array<{ hi: number; lo: number }>(REPAIR_BUCKETS + 1),
        }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "key-versions-request",
          partitionId: 0,
          buckets: new Array<number>(REPAIR_BUCKETS + 1),
        }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "key-versions",
          partitionId: 0,
          versions: new Array<KeyVersion>(MAX_CHUNK_ENTRIES + 1),
        }),
    ).toThrow(KvProtocolError);
    expect(
      (): Uint8Array =>
        encodeMessage({
          kind: "entries-request",
          partitionId: 0,
          keys: new Array<string>(MAX_CHUNK_ENTRIES + 1),
        }),
    ).toThrow(KvProtocolError);
  });

  it("rejects anti-entropy list counts beyond their bounds on decode", () => {
    // Each frame: version, type, u32 partition id 0, then an oversized u32 count.
    const bigDigests: Uint8Array = new Uint8Array([1, 0x0d, 0, 0, 0, 0, 0, 0, 0, 0x41]);
    const bigBuckets: Uint8Array = new Uint8Array([1, 0x0e, 0, 0, 0, 0, 0, 0, 0, 0x41]);
    const bigVersions: Uint8Array = new Uint8Array([1, 0x0f, 0, 0, 0, 0, 0x00, 0x10, 0x00, 0x01]);
    const bigKeys: Uint8Array = new Uint8Array([1, 0x13, 0, 0, 0, 0, 0x00, 0x10, 0x00, 0x01]);
    for (const frame of [bigDigests, bigBuckets, bigVersions, bigKeys]) {
      expect((): KvMessage => decodeMessage(frame)).toThrow(KvProtocolError);
    }
  });
});

describe("message type peek", () => {
  it("reads the type byte of an encoded message without decoding its body", () => {
    const encoded: Uint8Array = encodeMessage({ kind: MessageKind.readRequest, key: "k" });
    expect(messageType(encoded)).toBe(MSG_READ_REQUEST);
  });

  it("rejects an unsupported protocol version", () => {
    expect((): number => messageType(Uint8Array.of(99, MSG_READ_REQUEST))).toThrow(KvProtocolError);
  });
});
