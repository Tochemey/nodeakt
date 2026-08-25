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

/**
 * Hand-rolled binary codec for the store's protocol messages.
 *
 * The format is big-endian, length-prefixed, and self-describing through a
 * two-byte envelope of protocol version and message type. Every decode path
 * validates a length against the bytes that remain before it allocates or
 * copies, and rejects trailing bytes, so a truncated, oversized, or crafted
 * frame raises {@link KvProtocolError} instead of producing a partial value.
 * Encoders validate their inputs too, so a malformed frame is never emitted.
 *
 * There are no external dependencies: numbers are written through `DataView`,
 * strings through a shared UTF-8 coder that rejects malformed sequences.
 *
 * @internal
 */

import {
  MAX_CHUNK_ENTRIES,
  MAX_KEY_BYTES,
  MAX_NAME_BYTES,
  MAX_OWNERS_PER_PARTITION,
  MAX_VALUE_BYTES,
  MAX_WIRE_PARTITIONS,
  PROTOCOL_VERSION,
} from "./constants";
import { KvProtocolError } from "./errors";
import type {
  CompareAndSetOp,
  Entry,
  HybridTime,
  IncrementOp,
  PutOp,
  WriteOp,
  WriteResult,
} from "./ports";

/** Read request carrying a single key. @internal */
export const MSG_READ_REQUEST: number = 0x01;

/** Read response carrying an optional entry. @internal */
export const MSG_READ_RESPONSE: number = 0x02;

/** Write request carrying one mutation. @internal */
export const MSG_WRITE_REQUEST: number = 0x03;

/** Write response carrying an applied entry or a condition failure. @internal */
export const MSG_WRITE_RESPONSE: number = 0x04;

/** Coordinator push of the versioned routing table. @internal */
export const MSG_TABLE: number = 0x10;

/** One chunk of a fragment transfer during migration. @internal */
export const MSG_FRAGMENT_CHUNK: number = 0x11;

/** Largest value representable in an unsigned 64-bit field. */
const MAX_U64: bigint = 0xffffffffffffffffn;

/** Inclusive signed 64-bit bounds for increment deltas. */
const MIN_I64: bigint = -(2n ** 63n);
const MAX_I64: bigint = 2n ** 63n - 1n;

/** Shared UTF-8 encoder; its output is copied into frames, never retained. */
const encoder: TextEncoder = new TextEncoder();

/** Shared fatal UTF-8 decoder; a malformed byte sequence is a protocol error. */
const decoder: TextDecoder = new TextDecoder("utf-8", { fatal: true });

/** Raises a typed protocol error and marks the calling path as non-returning. */
function fail(message: string): never {
  throw new KvProtocolError(message);
}

/**
 * Condition byte for a put: none, only-if-absent, or only-if-present.
 */
const CONDITION_TO_BYTE: Readonly<Record<PutOp["condition"], number>> = {
  none: 0,
  nx: 1,
  xx: 2,
};

/** Inverse of {@link CONDITION_TO_BYTE}, indexed by wire byte. */
const BYTE_TO_CONDITION: readonly PutOp["condition"][] = ["none", "nx", "xx"];

/** Opcode byte for each write kind. */
const OP_PUT: number = 1;
const OP_DELETE: number = 2;
const OP_INCR: number = 3;
const OP_CAS: number = 4;

/** Condition-failure byte for a rejected conditional write, indexed from zero. */
const REASON_TO_BYTE: Readonly<Record<"nx" | "xx" | "cas", number>> = {
  nx: 0,
  xx: 1,
  cas: 2,
};

/** Inverse of {@link REASON_TO_BYTE}, indexed by wire byte. */
const BYTE_TO_REASON: readonly ("nx" | "xx" | "cas")[] = ["nx", "xx", "cas"];

/**
 * Growable big-endian byte sink.
 *
 * Capacity doubles as needed; {@link finish} returns an exactly sized copy so
 * the frame owns no slack and shares no buffer with the writer.
 *
 * @internal
 */
export class ByteWriter {
  /** Backing buffer, replaced by a larger one when capacity is exceeded. */
  #bytes: Uint8Array;

  /** View over {@link #bytes}, replaced whenever the buffer grows. */
  #view: DataView;

  /** Count of bytes written so far, and the offset of the next write. */
  #offset: number = 0;

  /**
   * Allocates a writer with an initial capacity in bytes. A capacity below one
   * is raised to one so the doubling in {@link #ensure} always makes progress.
   */
  constructor(capacity: number = 64) {
    this.#bytes = new Uint8Array(Math.max(1, capacity));
    this.#view = new DataView(this.#bytes.buffer);
  }

  /** Ensures room for `extra` more bytes, growing the buffer if needed. */
  #ensure(extra: number): void {
    const required: number = this.#offset + extra;
    if (required <= this.#bytes.length) {
      return;
    }

    let capacity: number = this.#bytes.length * 2;
    while (capacity < required) {
      capacity *= 2;
    }

    const grown: Uint8Array = new Uint8Array(capacity);
    grown.set(this.#bytes);
    this.#bytes = grown;
    this.#view = new DataView(grown.buffer);
  }

  /** Writes one unsigned byte. */
  u8(value: number): void {
    this.#ensure(1);
    this.#view.setUint8(this.#offset, value);
    this.#offset += 1;
  }

  /** Writes an unsigned 16-bit integer. */
  u16(value: number): void {
    this.#ensure(2);
    this.#view.setUint16(this.#offset, value);
    this.#offset += 2;
  }

  /** Writes an unsigned 32-bit integer. */
  u32(value: number): void {
    this.#ensure(4);
    this.#view.setUint32(this.#offset, value);
    this.#offset += 4;
  }

  /** Writes an unsigned 64-bit integer. */
  u64(value: bigint): void {
    this.#ensure(8);
    this.#view.setBigUint64(this.#offset, value);
    this.#offset += 8;
  }

  /** Writes a signed 64-bit integer in two's complement. */
  i64(value: bigint): void {
    this.#ensure(8);
    this.#view.setBigInt64(this.#offset, value);
    this.#offset += 8;
  }

  /** Appends raw bytes without a length prefix. */
  raw(bytes: Uint8Array): void {
    this.#ensure(bytes.length);
    this.#bytes.set(bytes, this.#offset);
    this.#offset += bytes.length;
  }

  /** Returns an exactly sized copy of everything written. */
  finish(): Uint8Array {
    return this.#bytes.slice(0, this.#offset);
  }
}

/**
 * Bounds-checked big-endian byte source.
 *
 * Every read first requires the bytes it needs, so a truncated frame raises
 * {@link KvProtocolError} rather than reading past the end. Length-prefixed
 * reads validate the prefix against the remaining bytes before allocating.
 *
 * @internal
 */
export class ByteReader {
  /** Immutable frame under inspection. */
  readonly #bytes: Uint8Array;

  /** View over {@link #bytes} for multi-byte reads. */
  readonly #view: DataView;

  /** Offset of the next unread byte. */
  #offset: number = 0;

  /** Wraps a frame for reading from its first byte. */
  constructor(bytes: Uint8Array) {
    this.#bytes = bytes;
    this.#view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  /** Bytes not yet consumed. */
  get remaining(): number {
    return this.#bytes.length - this.#offset;
  }

  /** Fails unless at least `count` unread bytes remain. */
  #require(count: number): void {
    if (count > this.remaining) {
      fail("unexpected end of input");
    }
  }

  /** Reads one unsigned byte. */
  u8(): number {
    this.#require(1);
    const value: number = this.#view.getUint8(this.#offset);
    this.#offset += 1;
    return value;
  }

  /** Reads an unsigned 16-bit integer. */
  u16(): number {
    this.#require(2);
    const value: number = this.#view.getUint16(this.#offset);
    this.#offset += 2;
    return value;
  }

  /** Reads an unsigned 32-bit integer. */
  u32(): number {
    this.#require(4);
    const value: number = this.#view.getUint32(this.#offset);
    this.#offset += 4;
    return value;
  }

  /** Reads an unsigned 64-bit integer. */
  u64(): bigint {
    this.#require(8);
    const value: bigint = this.#view.getBigUint64(this.#offset);
    this.#offset += 8;
    return value;
  }

  /** Reads a signed 64-bit integer. */
  i64(): bigint {
    this.#require(8);
    const value: bigint = this.#view.getBigInt64(this.#offset);
    this.#offset += 8;
    return value;
  }

  /** Reads and copies exactly `count` bytes after checking they are present. */
  take(count: number): Uint8Array {
    this.#require(count);
    const slice: Uint8Array = this.#bytes.slice(this.#offset, this.#offset + count);
    this.#offset += count;
    return slice;
  }

  /** Fails unless every byte has been consumed. */
  end(): void {
    if (this.remaining !== 0) {
      fail("trailing bytes after message");
    }
  }
}

/** Validates a non-negative safe integer usable as an unsigned 64-bit wire field. */
function assertUnsignedInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    fail(`${field} is out of range`);
  }
}

/**
 * Validates a non-negative integer that fits an unsigned 32-bit wire field.
 *
 * `DataView.setUint32` truncates a larger value silently, so a field written
 * with `u32` must be range-checked here first to keep encode and decode exact.
 */
function assertU32(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff) {
    fail(`${field} is out of range`);
  }
}

/** Validates an unsigned 64-bit bigint. */
function assertU64(value: bigint, field: string): void {
  if (value < 0n || value > MAX_U64) {
    fail(`${field} is out of range`);
  }
}

/** Encodes a canonical member identity as a `u8`-length-prefixed UTF-8 string. */
function writeName(writer: ByteWriter, name: string, field: string): void {
  if (name.includes("\0")) {
    fail(`${field} contains NUL`);
  }

  const bytes: Uint8Array = encoder.encode(name);
  if (bytes.length === 0 || bytes.length > MAX_NAME_BYTES) {
    fail(`${field} length is out of range`);
  }

  if (!name.isWellFormed()) {
    fail(`${field} is not valid Unicode`);
  }

  writer.u8(bytes.length);
  writer.raw(bytes);
}

/** Decodes a `u8`-length-prefixed UTF-8 member identity. */
function readName(reader: ByteReader, field: string): string {
  const length: number = reader.u8();
  if (length === 0) {
    fail(`${field} length is out of range`);
  }

  const bytes: Uint8Array = reader.take(length);
  return decodeUtf8(bytes, field);
}

/** Encodes a store key as a `u16`-length-prefixed UTF-8 string. */
function writeKey(writer: ByteWriter, key: string): void {
  if (key.includes("\0")) {
    fail("key contains NUL");
  }

  const bytes: Uint8Array = encoder.encode(key);
  if (bytes.length === 0 || bytes.length > MAX_KEY_BYTES) {
    fail("key length is out of range");
  }

  if (!key.isWellFormed()) {
    fail("key is not valid Unicode");
  }

  writer.u16(bytes.length);
  writer.raw(bytes);
}

/** Decodes a `u16`-length-prefixed UTF-8 key, rejecting an oversized length. */
function readKey(reader: ByteReader): string {
  const length: number = reader.u16();
  if (length === 0 || length > MAX_KEY_BYTES) {
    fail("key length is out of range");
  }

  return decodeUtf8(reader.take(length), "key");
}

/** Decodes UTF-8 bytes, turning malformed sequences and NUL into protocol errors. */
function decodeUtf8(bytes: Uint8Array, field: string): string {
  let text: string;
  try {
    text = decoder.decode(bytes);
  } catch {
    fail(`${field} is not valid UTF-8`);
  }

  if (text.includes("\0")) {
    fail(`${field} contains NUL`);
  }

  return text;
}

/** Encodes an opaque value as a `u32`-length-prefixed blob, bounded by `max`. */
function writeBlob(writer: ByteWriter, bytes: Uint8Array, max: number, field: string): void {
  if (bytes.length > max) {
    fail(`${field} length is out of range`);
  }

  writer.u32(bytes.length);
  writer.raw(bytes);
}

/** Decodes a `u32`-length-prefixed blob, validating the length before copying. */
function readBlob(reader: ByteReader, max: number, field: string): Uint8Array {
  const length: number = reader.u32();
  if (length > max) {
    fail(`${field} length is out of range`);
  }

  return reader.take(length);
}

/** Writes a hybrid timestamp: wall time, logical counter, then the node identity. */
function writeHybridTime(writer: ByteWriter, time: HybridTime): void {
  assertUnsignedInteger(time.wallMs, "wall time");
  assertU32(time.logical, "logical counter");
  writer.u64(BigInt(time.wallMs));
  writer.u32(time.logical);
  writeName(writer, time.node, "node");
}

/** Reads a hybrid timestamp, rejecting a wall time beyond exact number range. */
function readHybridTime(reader: ByteReader): HybridTime {
  const wall: bigint = reader.u64();
  if (wall > BigInt(Number.MAX_SAFE_INTEGER)) {
    fail("wall time is out of range");
  }

  const logical: number = reader.u32();
  const node: string = readName(reader, "node");
  return { wallMs: Number(wall), logical, node };
}

/** Bit set when an entry is a tombstone. */
const ENTRY_FLAG_DELETED: number = 1 << 0;

/** Bit set when an entry carries an absolute expiry. */
const ENTRY_FLAG_EXPIRES: number = 1 << 1;

/** Writes one stored entry, enforcing the tombstone-has-no-value invariant. */
function writeEntry(writer: ByteWriter, entry: Entry): void {
  if (entry.deleted !== (entry.value === undefined)) {
    fail("entry deleted flag is inconsistent with its value");
  }

  assertU64(entry.sequence, "sequence");
  let flags: number = 0;
  if (entry.deleted) {
    flags |= ENTRY_FLAG_DELETED;
  }

  if (entry.expiresAt !== undefined) {
    assertUnsignedInteger(entry.expiresAt, "expiry");
    flags |= ENTRY_FLAG_EXPIRES;
  }

  writer.u8(flags);
  writeKey(writer, entry.key);
  writeHybridTime(writer, entry.timestamp);
  writer.u64(entry.sequence);
  if (entry.expiresAt !== undefined) {
    writer.u64(BigInt(entry.expiresAt));
  }

  if (entry.value !== undefined) {
    writeBlob(writer, entry.value, MAX_VALUE_BYTES, "value");
  }
}

/** Reads one stored entry, reconstructing a tombstone as an absent value. */
function readEntry(reader: ByteReader): Entry {
  const flags: number = reader.u8();
  if ((flags & ~(ENTRY_FLAG_DELETED | ENTRY_FLAG_EXPIRES)) !== 0) {
    fail("entry flags are invalid");
  }

  const deleted: boolean = (flags & ENTRY_FLAG_DELETED) !== 0;
  const key: string = readKey(reader);
  const timestamp: HybridTime = readHybridTime(reader);
  const sequence: bigint = reader.u64();
  const expiresAt: number | undefined =
    (flags & ENTRY_FLAG_EXPIRES) !== 0 ? readSafeU64(reader, "expiry") : undefined;
  const value: Uint8Array | undefined = deleted
    ? undefined
    : readBlob(reader, MAX_VALUE_BYTES, "value");
  return { key, value, timestamp, sequence, expiresAt, deleted };
}

/** Reads a `u64` that must fit the exact number range. */
function readSafeU64(reader: ByteReader, field: string): number {
  const value: bigint = reader.u64();
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    fail(`${field} is out of range`);
  }

  return Number(value);
}

/** Writes a mutation, dispatching on its discriminant. */
function writeWriteOp(writer: ByteWriter, op: WriteOp): void {
  if (op.kind === "put") {
    writePut(writer, op);
    return;
  }

  if (op.kind === "delete") {
    writer.u8(OP_DELETE);
    writeKey(writer, op.key);
    return;
  }

  if (op.kind === "incr") {
    writeIncrement(writer, op);
    return;
  }

  writeCompareAndSet(writer, op);
}

/** Writes a put with its condition and optional TTL. */
function writePut(writer: ByteWriter, op: PutOp): void {
  writer.u8(OP_PUT);
  writer.u8(CONDITION_TO_BYTE[op.condition]);
  const hasTtl: boolean = op.ttlMs !== undefined;
  writer.u8(hasTtl ? 1 : 0);
  writeKey(writer, op.key);
  writeBlob(writer, op.value, MAX_VALUE_BYTES, "value");
  if (op.ttlMs !== undefined) {
    assertUnsignedInteger(op.ttlMs, "ttl");
    writer.u64(BigInt(op.ttlMs));
  }
}

/** Writes a signed increment delta. */
function writeIncrement(writer: ByteWriter, op: IncrementOp): void {
  if (op.delta < MIN_I64 || op.delta > MAX_I64) {
    fail("increment delta is out of range");
  }

  writer.u8(OP_INCR);
  writeKey(writer, op.key);
  writer.i64(op.delta);
}

/** Writes a compare-and-set with its expected and replacement payloads. */
function writeCompareAndSet(writer: ByteWriter, op: CompareAndSetOp): void {
  writer.u8(OP_CAS);
  writeKey(writer, op.key);
  writeBlob(writer, op.expected, MAX_VALUE_BYTES, "expected");
  writeBlob(writer, op.value, MAX_VALUE_BYTES, "value");
}

/** Reads a mutation, dispatching on the leading opcode byte. */
function readWriteOp(reader: ByteReader): WriteOp {
  const opcode: number = reader.u8();
  if (opcode === OP_PUT) {
    return readPut(reader);
  }

  if (opcode === OP_DELETE) {
    return { kind: "delete", key: readKey(reader) };
  }

  if (opcode === OP_INCR) {
    return { kind: "incr", key: readKey(reader), delta: reader.i64() };
  }

  if (opcode === OP_CAS) {
    return readCompareAndSet(reader);
  }

  return fail("unknown write opcode");
}

/** Reads a put and its optional TTL. */
function readPut(reader: ByteReader): PutOp {
  const conditionByte: number = reader.u8();
  const condition: PutOp["condition"] | undefined = BYTE_TO_CONDITION[conditionByte];
  if (condition === undefined) {
    fail("unknown put condition");
  }

  const ttlByte: number = reader.u8();
  if (ttlByte > 1) {
    fail("put ttl flag is invalid");
  }

  const key: string = readKey(reader);
  const value: Uint8Array = readBlob(reader, MAX_VALUE_BYTES, "value");
  if (ttlByte === 0) {
    return { kind: "put", key, value, condition };
  }

  return { kind: "put", key, value, condition, ttlMs: readSafeU64(reader, "ttl") };
}

/** Reads a compare-and-set. */
function readCompareAndSet(reader: ByteReader): CompareAndSetOp {
  const key: string = readKey(reader);
  const expected: Uint8Array = readBlob(reader, MAX_VALUE_BYTES, "expected");
  const value: Uint8Array = readBlob(reader, MAX_VALUE_BYTES, "value");
  return { kind: "cas", key, expected, value };
}

/** Byte marking an applied write result. */
const RESULT_APPLIED: number = 1;

/** Byte marking a rejected write result. */
const RESULT_REJECTED: number = 0;

/** Writes a write result: an applied entry, or a rejection reason. */
function writeWriteResult(writer: ByteWriter, result: WriteResult): void {
  if (result.applied) {
    writer.u8(RESULT_APPLIED);
    writeEntry(writer, result.entry);
    return;
  }

  writer.u8(RESULT_REJECTED);
  writer.u8(REASON_TO_BYTE[result.reason]);
}

/** Reads a write result. */
function readWriteResult(reader: ByteReader): WriteResult {
  const marker: number = reader.u8();
  if (marker === RESULT_APPLIED) {
    return { applied: true, entry: readEntry(reader) };
  }

  if (marker !== RESULT_REJECTED) {
    fail("write result marker is invalid");
  }

  const reason: ("nx" | "xx" | "cas") | undefined = BYTE_TO_REASON[reader.u8()];
  if (reason === undefined) {
    fail("unknown write rejection reason");
  }

  return { applied: false, reason };
}

/**
 * One partition's owners as carried in a routing-table push. The last name is
 * the primary; earlier names are previous owners retained during a move.
 *
 * @internal
 */
export interface PartitionOwners {
  /** Owners for this partition, primary last, at least one and never empty. */
  readonly owners: readonly string[];
}

/**
 * A versioned routing table as carried on the wire.
 *
 * The in-memory routing table of a later slice serializes into and out of this
 * shape; this codec knows only the bytes, not the table's behavior.
 *
 * @internal
 */
export interface RoutingTableWire {
  /** Monotone table version; a member keeps the highest it has seen. */
  readonly version: bigint;
  /** Owners per partition, indexed by partition id. */
  readonly partitions: readonly PartitionOwners[];
}

/**
 * One chunk of a fragment transfer.
 *
 * Entries are merged by last write wins at the receiver; `final` marks the last
 * chunk of the partition's transfer.
 *
 * @internal
 */
export interface FragmentChunkWire {
  /** Partition whose fragment these entries belong to. */
  readonly partitionId: number;
  /** Whether this is the final chunk of the transfer. */
  readonly final: boolean;
  /** Entries in this chunk, in no required order. */
  readonly entries: readonly Entry[];
}

/** Discriminated union of every top-level message this codec round-trips. @internal */
export type KvMessage =
  | { readonly kind: "read-request"; readonly key: string }
  | { readonly kind: "read-response"; readonly entry: Entry | undefined }
  | { readonly kind: "write-request"; readonly op: WriteOp }
  | { readonly kind: "write-response"; readonly result: WriteResult }
  | { readonly kind: "table"; readonly table: RoutingTableWire }
  | { readonly kind: "fragment-chunk"; readonly chunk: FragmentChunkWire };

/** Writes the two-byte envelope: protocol version, then message type. */
function writeEnvelope(writer: ByteWriter, type: number): void {
  writer.u8(PROTOCOL_VERSION);
  writer.u8(type);
}

/**
 * Encodes one message, envelope included.
 *
 * @throws {KvProtocolError} If any field is out of range or inconsistent.
 * @internal
 */
export function encodeMessage(message: KvMessage): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeBody(writer, message);
  return writer.finish();
}

/** Writes the envelope and body for one message. */
function encodeBody(writer: ByteWriter, message: KvMessage): void {
  if (message.kind === "read-request") {
    writeEnvelope(writer, MSG_READ_REQUEST);
    writeKey(writer, message.key);
    return;
  }

  if (message.kind === "read-response") {
    writeEnvelope(writer, MSG_READ_RESPONSE);
    writeOptionalEntry(writer, message.entry);
    return;
  }

  if (message.kind === "write-request") {
    writeEnvelope(writer, MSG_WRITE_REQUEST);
    writeWriteOp(writer, message.op);
    return;
  }

  if (message.kind === "write-response") {
    writeEnvelope(writer, MSG_WRITE_RESPONSE);
    writeWriteResult(writer, message.result);
    return;
  }

  if (message.kind === "table") {
    writeEnvelope(writer, MSG_TABLE);
    writeTable(writer, message.table);
    return;
  }

  writeEnvelope(writer, MSG_FRAGMENT_CHUNK);
  writeChunk(writer, message.chunk);
}

/** Writes a presence byte followed by the entry when present. */
function writeOptionalEntry(writer: ByteWriter, entry: Entry | undefined): void {
  if (entry === undefined) {
    writer.u8(0);
    return;
  }

  writer.u8(1);
  writeEntry(writer, entry);
}

/** Writes a routing table: version, partition count, then each owners list. */
function writeTable(writer: ByteWriter, table: RoutingTableWire): void {
  assertU64(table.version, "table version");
  if (table.partitions.length > MAX_WIRE_PARTITIONS) {
    fail("table partition count is out of range");
  }

  writer.u64(table.version);
  writer.u32(table.partitions.length);
  for (const partition of table.partitions) {
    const owners: readonly string[] = partition.owners;
    if (owners.length === 0 || owners.length > MAX_OWNERS_PER_PARTITION) {
      fail("partition owner count is out of range");
    }

    writer.u8(owners.length);
    for (const owner of owners) {
      writeName(writer, owner, "owner");
    }
  }
}

/** Writes a fragment chunk: partition, final flag, entry count, then entries. */
function writeChunk(writer: ByteWriter, chunk: FragmentChunkWire): void {
  assertU32(chunk.partitionId, "partition id");
  if (chunk.entries.length > MAX_CHUNK_ENTRIES) {
    fail("chunk entry count is out of range");
  }

  writer.u32(chunk.partitionId);
  writer.u8(chunk.final ? 1 : 0);
  writer.u32(chunk.entries.length);
  for (const entry of chunk.entries) {
    writeEntry(writer, entry);
  }
}

/**
 * Decodes one message, validating the envelope and rejecting trailing bytes.
 *
 * @throws {KvProtocolError} For an unknown version or type, a truncated body,
 * an out-of-range length, or any bytes left after the message.
 * @internal
 */
export function decodeMessage(bytes: Uint8Array): KvMessage {
  const reader: ByteReader = new ByteReader(bytes);
  const version: number = reader.u8();
  if (version !== PROTOCOL_VERSION) {
    fail("unsupported protocol version");
  }

  const message: KvMessage = decodeBody(reader, reader.u8());
  reader.end();
  return message;
}

/** Reads the body for a validated message type. */
function decodeBody(reader: ByteReader, type: number): KvMessage {
  if (type === MSG_READ_REQUEST) {
    return { kind: "read-request", key: readKey(reader) };
  }

  if (type === MSG_READ_RESPONSE) {
    return { kind: "read-response", entry: readOptionalEntry(reader) };
  }

  if (type === MSG_WRITE_REQUEST) {
    return { kind: "write-request", op: readWriteOp(reader) };
  }

  if (type === MSG_WRITE_RESPONSE) {
    return { kind: "write-response", result: readWriteResult(reader) };
  }

  if (type === MSG_TABLE) {
    return { kind: "table", table: readTable(reader) };
  }

  if (type === MSG_FRAGMENT_CHUNK) {
    return { kind: "fragment-chunk", chunk: readChunk(reader) };
  }

  return fail("unknown message type");
}

/** Reads a presence byte and the entry it guards. */
function readOptionalEntry(reader: ByteReader): Entry | undefined {
  const present: number = reader.u8();
  if (present === 0) {
    return undefined;
  }

  if (present !== 1) {
    fail("entry presence flag is invalid");
  }

  return readEntry(reader);
}

/** Reads a routing table, bounding every count before allocating. */
function readTable(reader: ByteReader): RoutingTableWire {
  const version: bigint = reader.u64();
  const count: number = reader.u32();
  if (count > MAX_WIRE_PARTITIONS) {
    fail("table partition count is out of range");
  }

  const partitions: PartitionOwners[] = [];
  for (let index = 0; index < count; index += 1) {
    const ownerCount: number = reader.u8();
    if (ownerCount === 0 || ownerCount > MAX_OWNERS_PER_PARTITION) {
      fail("partition owner count is out of range");
    }

    const owners: string[] = [];
    for (let owner = 0; owner < ownerCount; owner += 1) {
      owners.push(readName(reader, "owner"));
    }

    partitions.push({ owners });
  }

  return { version, partitions };
}

/** Reads a fragment chunk, bounding the entry count before allocating. */
function readChunk(reader: ByteReader): FragmentChunkWire {
  const partitionId: number = reader.u32();
  const finalByte: number = reader.u8();
  if (finalByte > 1) {
    fail("chunk final flag is invalid");
  }

  const count: number = reader.u32();
  if (count > MAX_CHUNK_ENTRIES) {
    fail("chunk entry count is out of range");
  }

  const entries: Entry[] = [];
  for (let index = 0; index < count; index += 1) {
    entries.push(readEntry(reader));
  }

  return { partitionId, final: finalByte === 1, entries };
}
