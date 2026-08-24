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
 * The transport's byte primitives and binary value codec.
 *
 * This module is the byte-level foundation of the wire protocol: a
 * growable {@link ByteWriter}, a bounds-checked {@link ByteReader},
 * and the tag-based encoding of plain value trees that message
 * payloads travel as. It knows nothing about actors, registries, or
 * sockets, and imports nothing outside the platform.
 *
 * The value domain matches what survives an isolate boundary today:
 * primitives, plain objects and arrays, Map, Set, Date, binary
 * buffers and views, with aliasing and cycles preserved through
 * back-references. Encoding fails on the sending side for values no
 * boundary can carry (functions and symbols); decoding never trusts
 * its input, validating every tag, length, and count before it
 * allocates.
 *
 * @internal
 */

/** Thrown by every decode path for malformed or truncated bytes. */
export class ValueDecodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ValueDecodeError";
  }
}

/** Value tags, one byte each on the wire. The tag space is append-only. */
const TAG_NULL: number = 0x00;
const TAG_UNDEFINED: number = 0x01;
const TAG_FALSE: number = 0x02;
const TAG_TRUE: number = 0x03;
const TAG_INT: number = 0x04;
const TAG_F64: number = 0x05;
const TAG_STRING: number = 0x06;
const TAG_BIGINT: number = 0x07;
const TAG_DATE: number = 0x08;
const TAG_BYTES: number = 0x09;
const TAG_ARRAY: number = 0x0a;
const TAG_OBJECT: number = 0x0b;
const TAG_MAP: number = 0x0c;
const TAG_SET: number = 0x0d;
const TAG_REF: number = 0x0e;

/**
 * Nesting bound for encode and decode alike, so a hostile or runaway
 * value tree fails with a typed error instead of exhausting the stack.
 */
const MAX_DEPTH: number = 1024;

/** Strings below this byte length try a char loop before TextDecoder. */
const ASCII_FAST_PATH_LIMIT: number = 24;

const textEncoder: TextEncoder = new TextEncoder();
const textDecoder: TextDecoder = new TextDecoder("utf-8", { fatal: true });

/**
 * The BYTES subtypes in wire order. Contents travel as raw bytes in
 * the platform's native element order; every supported runtime is
 * little-endian.
 */
interface ByteShape {
  /** Element size the byte length must divide by, 1 for byte shapes. */
  readonly size: number;
  /** Builds the decoded value over a freshly copied buffer. */
  readonly make: (bytes: Uint8Array) => object;
}

const byteShapes: readonly ByteShape[] = [
  { size: 1, make: (bytes: Uint8Array): object => bytes.buffer },
  { size: 1, make: (bytes: Uint8Array): object => bytes },
  { size: 1, make: (bytes: Uint8Array): object => new Int8Array(bytes.buffer) },
  { size: 1, make: (bytes: Uint8Array): object => new Uint8ClampedArray(bytes.buffer) },
  { size: 2, make: (bytes: Uint8Array): object => new Int16Array(bytes.buffer) },
  { size: 2, make: (bytes: Uint8Array): object => new Uint16Array(bytes.buffer) },
  { size: 4, make: (bytes: Uint8Array): object => new Int32Array(bytes.buffer) },
  { size: 4, make: (bytes: Uint8Array): object => new Uint32Array(bytes.buffer) },
  { size: 4, make: (bytes: Uint8Array): object => new Float32Array(bytes.buffer) },
  { size: 8, make: (bytes: Uint8Array): object => new Float64Array(bytes.buffer) },
  { size: 8, make: (bytes: Uint8Array): object => new BigInt64Array(bytes.buffer) },
  { size: 8, make: (bytes: Uint8Array): object => new BigUint64Array(bytes.buffer) },
  { size: 1, make: (bytes: Uint8Array): object => new DataView(bytes.buffer) },
];

/** Maps a view constructor to its BYTES subtype. */
const viewSubtypes: ReadonlyMap<object, number> = new Map<object, number>([
  [Uint8Array, 1],
  [Int8Array, 2],
  [Uint8ClampedArray, 3],
  [Int16Array, 4],
  [Uint16Array, 5],
  [Int32Array, 6],
  [Uint32Array, 7],
  [Float32Array, 8],
  [Float64Array, 9],
  [BigInt64Array, 10],
  [BigUint64Array, 11],
  [DataView, 12],
]);

/**
 * ByteWriter is a growable output buffer with the wire's integer and
 * string encodings. One writer is meant to live per connection and be
 * reused across messages: {@link ByteWriter.reset} rewinds it without
 * releasing the backing buffer, so steady-state encoding allocates
 * nothing.
 */
export class ByteWriter {
  private _bytes: Uint8Array;
  private _view: DataView;
  private _length: number = 0;

  constructor(initialCapacity: number = 256) {
    const capacity: number = initialCapacity < 16 ? 16 : initialCapacity;
    this._bytes = new Uint8Array(capacity);
    this._view = new DataView(this._bytes.buffer);
  }

  /** The number of bytes written since construction or reset. */
  get length(): number {
    return this._length;
  }

  /** Rewinds the writer, retaining the backing buffer. */
  reset(): void {
    this._length = 0;
  }

  /**
   * The written bytes as a view over the backing buffer. The view is
   * valid only until the next write or reset; callers that retain it
   * must copy.
   */
  bytes(): Uint8Array {
    return this._bytes.subarray(0, this._length);
  }

  /** Ensures room for `extra` more bytes, growing geometrically. */
  private ensure(extra: number): void {
    const needed: number = this._length + extra;
    if (needed <= this._bytes.length) {
      return;
    }

    let capacity: number = this._bytes.length * 2;
    while (capacity < needed) {
      capacity *= 2;
    }

    const grown: Uint8Array = new Uint8Array(capacity);
    grown.set(this._bytes.subarray(0, this._length));
    this._bytes = grown;
    this._view = new DataView(grown.buffer);
  }

  writeU8(value: number): void {
    this.ensure(1);
    this._bytes[this._length] = value;
    this._length += 1;
  }

  /** Writes a fixed 4-byte big-endian unsigned integer. */
  writeU32(value: number): void {
    this.ensure(4);
    this._view.setUint32(this._length, value, false);
    this._length += 4;
  }

  /**
   * Writes a fixed 8-byte big-endian unsigned integer from an ordinary
   * number, which must be a safe non-negative integer.
   */
  writeU64(value: number): void {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new TypeError(`u64 value out of range: ${value}`);
    }

    const high: number = Math.floor(value / 0x100000000);
    const low: number = value % 0x100000000;
    this.ensure(8);
    this._view.setUint32(this._length, high, false);
    this._view.setUint32(this._length + 4, low, false);
    this._length += 8;
  }

  /** Writes a fixed 8-byte big-endian IEEE 754 double. */
  writeF64(value: number): void {
    this.ensure(8);
    this._view.setFloat64(this._length, value, false);
    this._length += 8;
  }

  /**
   * Writes an unsigned LEB128 varint. The value must be a safe
   * non-negative integer; the arithmetic stays exact over the whole
   * safe range because each step subtracts the low group before an
   * exact power-of-two division.
   */
  writeUvarint(value: number): void {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new TypeError(`uvarint value out of range: ${value}`);
    }

    this.ensure(8);
    let remaining: number = value;
    while (remaining > 0x7f) {
      const low: number = remaining % 0x80;
      this._bytes[this._length] = low | 0x80;
      this._length += 1;
      remaining = (remaining - low) / 0x80;
    }

    this._bytes[this._length] = remaining;
    this._length += 1;
  }

  /** Writes a uvarint byte length followed by UTF-8 bytes. */
  writeString(value: string): void {
    const utf8: number = utf8Length(value);
    this.writeUvarint(utf8);
    this.ensure(utf8);
    if (utf8 === value.length) {
      const bytes: Uint8Array = this._bytes;
      let position: number = this._length;
      for (let i = 0; i < value.length; i++) {
        bytes[position] = value.charCodeAt(i);
        position += 1;
      }

      this._length = position;
      return;
    }

    textEncoder.encodeInto(value, this._bytes.subarray(this._length));
    this._length += utf8;
  }

  /** Appends raw bytes as they are, with no length prefix. */
  writeBytes(value: Uint8Array): void {
    this.ensure(value.length);
    this._bytes.set(value, this._length);
    this._length += value.length;
  }
}

/**
 * Counts the UTF-8 bytes of a string the way `encodeInto` will write
 * them, including the replacement character for a lone surrogate.
 */
function utf8Length(value: string): number {
  let bytes: number = 0;
  for (let i = 0; i < value.length; i++) {
    const code: number = value.charCodeAt(i);
    if (code < 0x80) {
      bytes += 1;
      continue;
    }

    if (code < 0x800) {
      bytes += 2;
      continue;
    }

    if (code >= 0xd800 && code < 0xdc00 && i + 1 < value.length) {
      const next: number = value.charCodeAt(i + 1);
      if (next >= 0xdc00 && next < 0xe000) {
        bytes += 4;
        i += 1;
        continue;
      }
    }

    bytes += 3;
  }

  return bytes;
}

/**
 * ByteReader consumes a byte buffer with the wire's integer and
 * string encodings, throwing {@link ValueDecodeError} on any
 * truncation or malformed input instead of ever reading past the end.
 */
export class ByteReader {
  private readonly _bytes: Uint8Array;
  /** Built on first fixed-width read; envelope decoding never needs it. */
  private _view: DataView | null = null;
  private _position: number = 0;

  constructor(bytes: Uint8Array) {
    this._bytes = bytes;
  }

  private view(): DataView {
    if (this._view === null) {
      this._view = new DataView(this._bytes.buffer, this._bytes.byteOffset, this._bytes.byteLength);
    }

    return this._view;
  }

  /** The current read offset. */
  get position(): number {
    return this._position;
  }

  /** The number of unread bytes. */
  get remaining(): number {
    return this._bytes.length - this._position;
  }

  private need(count: number): void {
    if (this._bytes.length - this._position < count) {
      throw new ValueDecodeError(
        `truncated input: need ${count} bytes, have ${this._bytes.length - this._position}`,
      );
    }
  }

  readU8(): number {
    this.need(1);
    const value: number = this._bytes[this._position] as number;
    this._position += 1;
    return value;
  }

  /** Reads a fixed 4-byte big-endian unsigned integer. */
  readU32(): number {
    this.need(4);
    const value: number = this.view().getUint32(this._position, false);
    this._position += 4;
    return value;
  }

  /**
   * Reads a fixed 8-byte big-endian unsigned integer into an ordinary
   * number, rejecting values above the safe integer range.
   */
  readU64(): number {
    this.need(8);
    const high: number = this.view().getUint32(this._position, false);
    const low: number = this.view().getUint32(this._position + 4, false);
    this._position += 8;
    const value: number = high * 0x100000000 + low;
    if (!Number.isSafeInteger(value)) {
      throw new ValueDecodeError("u64 value exceeds the safe integer range");
    }

    return value;
  }

  /** Reads a fixed 8-byte big-endian IEEE 754 double. */
  readF64(): number {
    this.need(8);
    const value: number = this.view().getFloat64(this._position, false);
    this._position += 8;
    return value;
  }

  /**
   * Reads an unsigned LEB128 varint of at most 8 groups, rejecting
   * values above the safe integer range.
   */
  readUvarint(): number {
    let value: number = 0;
    let scale: number = 1;
    for (let group = 0; group < 8; group++) {
      this.need(1);
      const byte: number = this._bytes[this._position] as number;
      this._position += 1;
      value += (byte & 0x7f) * scale;
      if ((byte & 0x80) === 0) {
        if (!Number.isSafeInteger(value)) {
          throw new ValueDecodeError("uvarint exceeds the safe integer range");
        }

        return value;
      }

      scale *= 0x80;
    }

    throw new ValueDecodeError("uvarint is too long");
  }

  /** Reads a uvarint byte length followed by UTF-8 bytes. */
  readString(): string {
    const length: number = this.readUvarint();
    this.need(length);
    const start: number = this._position;
    this._position += length;
    const bytes: Uint8Array = this._bytes.subarray(start, start + length);
    if (length < ASCII_FAST_PATH_LIMIT) {
      let ascii: boolean = true;
      for (let i = 0; i < length; i++) {
        if ((bytes[i] as number) >= 0x80) {
          ascii = false;
          break;
        }
      }

      if (ascii) {
        let value: string = "";
        for (let i = 0; i < length; i++) {
          value += String.fromCharCode(bytes[i] as number);
        }

        return value;
      }
    }

    try {
      return textDecoder.decode(bytes);
    } catch {
      throw new ValueDecodeError("invalid utf-8 in string");
    }
  }

  /**
   * Reads `count` raw bytes as a view over the input buffer. The view
   * is only valid while the input buffer is; callers that retain it
   * must copy.
   */
  readBytes(count: number): Uint8Array {
    this.need(count);
    const start: number = this._position;
    this._position += count;
    return this._bytes.subarray(start, start + count);
  }
}

/**
 * Encodes one value tree into the writer.
 *
 * @throws A `TypeError` for a function, a symbol, or an unsupported
 * binary object, which no boundary can carry.
 * @throws A `RangeError` when nesting exceeds the depth limit.
 */
export function encodeValue(writer: ByteWriter, value: unknown): void {
  encodeAny(writer, value, null, 0);
}

/**
 * The back-reference map is created lazily by the first container, so
 * a primitive-only payload encodes without allocating it; every
 * nested call below a container receives the shared map.
 */
function encodeAny(
  writer: ByteWriter,
  value: unknown,
  refs: Map<object, number> | null,
  depth: number,
): void {
  if (value === null) {
    writer.writeU8(TAG_NULL);
    return;
  }

  const kind: string = typeof value;
  if (kind === "undefined") {
    writer.writeU8(TAG_UNDEFINED);
    return;
  }

  if (kind === "boolean") {
    writer.writeU8(value === true ? TAG_TRUE : TAG_FALSE);
    return;
  }

  if (kind === "number") {
    encodeNumber(writer, value as number);
    return;
  }

  if (kind === "string") {
    writer.writeU8(TAG_STRING);
    writer.writeString(value as string);
    return;
  }

  if (kind === "bigint") {
    encodeBigint(writer, value as bigint);
    return;
  }

  if (kind === "object") {
    encodeObject(writer, value as object, refs ?? new Map<object, number>(), depth);
    return;
  }

  throw new TypeError(`a ${kind} cannot cross the wire`);
}

/**
 * Integers in the 32-bit signed range travel as compact zigzag
 * varints; every other number, including -0, NaN, infinities, and
 * safe integers beyond 32 bits, travels as an exact IEEE 754 double.
 */
function encodeNumber(writer: ByteWriter, value: number): void {
  if (
    Number.isInteger(value) &&
    value >= -2147483648 &&
    value <= 2147483647 &&
    !Object.is(value, -0)
  ) {
    writer.writeU8(TAG_INT);
    writer.writeUvarint(((value << 1) ^ (value >> 31)) >>> 0);
    return;
  }

  writer.writeU8(TAG_F64);
  writer.writeF64(value);
}

function encodeBigint(writer: ByteWriter, value: bigint): void {
  writer.writeU8(TAG_BIGINT);
  const negative: boolean = value < 0n;
  writer.writeU8(negative ? 1 : 0);
  let magnitude: bigint = negative ? -value : value;
  const digits: number[] = [];
  while (magnitude > 0n) {
    digits.push(Number(magnitude & 0xffn));
    magnitude >>= 8n;
  }

  writer.writeUvarint(digits.length);
  for (let i = digits.length - 1; i >= 0; i--) {
    writer.writeU8(digits[i] as number);
  }
}

function encodeObject(
  writer: ByteWriter,
  value: object,
  refs: Map<object, number>,
  depth: number,
): void {
  const seen: number | undefined = refs.get(value);
  if (seen !== undefined) {
    writer.writeU8(TAG_REF);
    writer.writeUvarint(seen);
    return;
  }

  if (depth >= MAX_DEPTH) {
    throw new RangeError("value nesting exceeds the depth limit");
  }

  refs.set(value, refs.size);

  if (Array.isArray(value)) {
    writer.writeU8(TAG_ARRAY);
    writer.writeUvarint(value.length);
    for (let i = 0; i < value.length; i++) {
      encodeAny(writer, value[i], refs, depth + 1);
    }

    return;
  }

  if (value instanceof Map) {
    writer.writeU8(TAG_MAP);
    writer.writeUvarint(value.size);
    for (const [key, entry] of value) {
      encodeAny(writer, key, refs, depth + 1);
      encodeAny(writer, entry, refs, depth + 1);
    }

    return;
  }

  if (value instanceof Set) {
    writer.writeU8(TAG_SET);
    writer.writeUvarint(value.size);
    for (const entry of value) {
      encodeAny(writer, entry, refs, depth + 1);
    }

    return;
  }

  if (value instanceof Date) {
    writer.writeU8(TAG_DATE);
    writer.writeF64(value.getTime());
    return;
  }

  if (value instanceof ArrayBuffer) {
    writer.writeU8(TAG_BYTES);
    writer.writeU8(0);
    writer.writeUvarint(value.byteLength);
    writer.writeBytes(new Uint8Array(value));
    return;
  }

  if (ArrayBuffer.isView(value)) {
    encodeView(writer, value);
    return;
  }

  if (typeof SharedArrayBuffer !== "undefined" && value instanceof SharedArrayBuffer) {
    throw new TypeError("a SharedArrayBuffer cannot cross the wire");
  }

  const keys: string[] = Object.keys(value);
  writer.writeU8(TAG_OBJECT);
  writer.writeUvarint(keys.length);
  for (const key of keys) {
    writer.writeString(key);
    encodeAny(writer, (value as Record<string, unknown>)[key], refs, depth + 1);
  }
}

function encodeView(writer: ByteWriter, value: ArrayBufferView): void {
  let subtype: number | undefined = viewSubtypes.get(value.constructor);
  if (subtype === undefined && value instanceof Uint8Array) {
    // A Uint8Array subclass (Node's Buffer, most commonly) travels as
    // its bytes and decodes as a plain Uint8Array.
    subtype = 1;
  }

  if (subtype === undefined) {
    throw new TypeError("an unsupported binary view cannot cross the wire");
  }

  writer.writeU8(TAG_BYTES);
  writer.writeU8(subtype);
  writer.writeUvarint(value.byteLength);
  writer.writeBytes(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
}

/**
 * Decodes one value tree from the reader.
 *
 * @throws A {@link ValueDecodeError} for any malformed, truncated, or
 * out-of-bounds input; decoding never throws anything else.
 */
export function decodeValue(reader: ByteReader): unknown {
  return decodeAny(reader, null, 0);
}

/**
 * The back-reference table mirrors the encoder's lazy map: the first
 * container creates it and passes it down, so a primitive-only
 * payload decodes without allocating it.
 */
function decodeAny(reader: ByteReader, refs: unknown[] | null, depth: number): unknown {
  if (depth >= MAX_DEPTH) {
    throw new ValueDecodeError("value nesting exceeds the depth limit");
  }

  const tag: number = reader.readU8();
  switch (tag) {
    case TAG_NULL:
      return null;
    case TAG_UNDEFINED:
      return undefined;
    case TAG_FALSE:
      return false;
    case TAG_TRUE:
      return true;
    case TAG_INT: {
      const zigzag: number = reader.readUvarint();
      if (zigzag > 0xffffffff) {
        throw new ValueDecodeError("int zigzag value out of range");
      }

      return (zigzag >>> 1) ^ -(zigzag & 1);
    }
    case TAG_F64:
      return reader.readF64();
    case TAG_STRING:
      return reader.readString();
    case TAG_BIGINT:
      return decodeBigint(reader);
    case TAG_DATE: {
      const date: Date = new Date(reader.readF64());
      refs?.push(date);
      return date;
    }
    case TAG_BYTES:
      return decodeBytes(reader, refs);
    case TAG_ARRAY: {
      const count: number = readCount(reader);
      const table: unknown[] = refs ?? [];
      const array: unknown[] = [];
      table.push(array);
      for (let i = 0; i < count; i++) {
        array.push(decodeAny(reader, table, depth + 1));
      }

      return array;
    }
    case TAG_OBJECT: {
      const count: number = readCount(reader);
      const table: unknown[] = refs ?? [];
      const object: Record<string, unknown> = {};
      table.push(object);
      for (let i = 0; i < count; i++) {
        const key: string = reader.readString();
        const entry: unknown = decodeAny(reader, table, depth + 1);
        if (key === "__proto__" || key === "constructor") {
          continue;
        }

        object[key] = entry;
      }

      return object;
    }
    case TAG_MAP: {
      const count: number = readCount(reader);
      const table: unknown[] = refs ?? [];
      const map: Map<unknown, unknown> = new Map();
      table.push(map);
      for (let i = 0; i < count; i++) {
        const key: unknown = decodeAny(reader, table, depth + 1);
        const entry: unknown = decodeAny(reader, table, depth + 1);
        map.set(key, entry);
      }

      return map;
    }
    case TAG_SET: {
      const count: number = readCount(reader);
      const table: unknown[] = refs ?? [];
      const set: Set<unknown> = new Set();
      table.push(set);
      for (let i = 0; i < count; i++) {
        set.add(decodeAny(reader, table, depth + 1));
      }

      return set;
    }
    case TAG_REF: {
      const index: number = reader.readUvarint();
      if (refs === null || index >= refs.length) {
        throw new ValueDecodeError(`back-reference ${index} is out of bounds`);
      }

      return refs[index];
    }
    default:
      throw new ValueDecodeError(`unknown value tag 0x${tag.toString(16).padStart(2, "0")}`);
  }
}

/**
 * Reads an element count and bounds it by the remaining input, since
 * every element costs at least one byte: a hostile count can never
 * force a large allocation.
 */
function readCount(reader: ByteReader): number {
  const count: number = reader.readUvarint();
  if (count > reader.remaining) {
    throw new ValueDecodeError(`count ${count} exceeds the remaining input`);
  }

  return count;
}

function decodeBigint(reader: ByteReader): bigint {
  const sign: number = reader.readU8();
  if (sign > 1) {
    throw new ValueDecodeError(`invalid bigint sign ${sign}`);
  }

  const length: number = reader.readUvarint();
  const digits: Uint8Array = reader.readBytes(length);
  let value: bigint = 0n;
  for (let i = 0; i < digits.length; i++) {
    value = (value << 8n) | BigInt(digits[i] as number);
  }

  return sign === 1 ? -value : value;
}

function decodeBytes(reader: ByteReader, refs: unknown[] | null): object {
  const subtype: number = reader.readU8();
  const shape: ByteShape | undefined = byteShapes[subtype];
  if (shape === undefined) {
    throw new ValueDecodeError(`unknown bytes subtype ${subtype}`);
  }

  const length: number = reader.readUvarint();
  if (length % shape.size !== 0) {
    throw new ValueDecodeError(`bytes length ${length} does not fit subtype ${subtype}`);
  }

  // Validate before allocating: the length is attacker-declared, and
  // sizing a buffer from it before the bounds check would hand a
  // hostile message a giant allocation for a few bytes of input.
  if (length > reader.remaining) {
    throw new ValueDecodeError(`truncated input: need ${length} bytes, have ${reader.remaining}`);
  }

  const copy: Uint8Array = new Uint8Array(length);
  copy.set(reader.readBytes(length));
  const value: object = shape.make(copy);
  refs?.push(value);
  return value;
}
