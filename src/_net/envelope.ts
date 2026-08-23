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

import { ProtocolError } from "./frame";
import { type ByteReader, type ByteWriter, ValueDecodeError } from "./values";

/**
 * The frame body codecs: DATA and REPLY envelopes, ERROR bodies, and
 * the HELLO handshake parameters. Everything here is opaque to the
 * transport: paths, incarnations, and type ids are plain strings,
 * payloads are bytes, and the sentinel field of an error is just a
 * number the layer above gives meaning to.
 *
 * A malformed body throws {@link ValueDecodeError} and is
 * request-scoped: it settles or dead-letters one message and the
 * connection lives on. The one exception is a table ref that cannot
 * resolve, which throws {@link ProtocolError}, because a ref into a
 * table this connection does not have poisons every later message.
 *
 * Refs intern through the connection's tables when the caller passes
 * them: the encoder asks the {@link RefInterner} for an id (zero
 * means inline) and the decoder resolves nonzero ids through the
 * {@link RefResolver}. Without them, encoding is inline-only and any
 * nonzero inbound id is the connection-scoped violation above, which
 * is exactly the contract below capability revision 3.
 *
 * @internal
 */

/**
 * Assigns table ids on the sending side; zero means encode inline.
 * The table module implements this and owns announcing each new id
 * before it is ever referenced.
 */
export interface RefInterner {
  pathId(literal: string): number;
  typeId(literal: string): number;
}

/**
 * Resolves inbound table refs on the receiving side; an unknown id
 * throws {@link ProtocolError}.
 */
export interface RefResolver {
  path(id: number): string;
  type(id: number): string;
}

/** DATA envelope kinds. */
export const KIND_TELL: number = 0;
export const KIND_ASK: number = 1;
export const KIND_WATCH: number = 2;
export const KIND_UNWATCH: number = 3;

/** Serializer ids: the binary value codec, and the reserved custom id. */
export const SERIALIZER_BINARY: number = 0;
export const SERIALIZER_CUSTOM: number = 255;

/** ERROR body codes. */
export const ERROR_PROTOCOL: number = 1;
export const ERROR_BAD_REQUEST: number = 2;
export const ERROR_UNAVAILABLE: number = 3;
export const ERROR_INTERNAL: number = 4;
export const ERROR_APPLICATION: number = 5;

/**
 * The body of a DATA frame. The correlation id is not here: it rides
 * in the frame header. `timeout` is the remaining ask budget in
 * milliseconds, zero for none, so enforcement never depends on
 * clocks agreeing across machines.
 */
export interface DataEnvelope {
  readonly kind: number;
  /** Target actor path. */
  readonly to: string;
  /** Target incarnation; empty addresses whoever lives at the path. */
  readonly uid: string;
  /** Sender actor path; empty when the send carried none. */
  readonly sender: string;
  readonly senderUid: string;
  readonly timeout: number;
  readonly serializerId: number;
  /** Registered message type id; empty for a passthrough value. */
  readonly typeRef: string;
  readonly payload: Uint8Array;
}

/** The body of a REPLY frame settling an ask. */
export interface ReplyEnvelope {
  readonly serializerId: number;
  readonly typeRef: string;
  readonly payload: Uint8Array;
}

/**
 * The body of an ERROR frame. `sentinel` is zero for none, else one
 * plus an index into the append-only sentinel list the layer above
 * maintains; `name` and `message` carry a reconstructed error when
 * the sentinel is zero.
 */
export interface ErrorBody {
  readonly code: number;
  readonly sentinel: number;
  readonly name: string;
  readonly message: string;
}

/** The HELLO and HELLO_ACK parameters, in wire field order. */
export interface Hello {
  readonly revision: number;
  readonly systemName: string;
  readonly host: string;
  readonly port: number;
  /** Same encoding as the frame header lane byte. */
  readonly lane: number;
  /** 0 none; other values reserved for negotiated codecs. */
  readonly compression: number;
  readonly maxFrameSize: number;
  readonly maxMessageSize: number;
  readonly initialCredits: number;
  readonly maxLargeTransfers: number;
}

function knownSerializer(id: number): boolean {
  return id === SERIALIZER_BINARY || id === SERIALIZER_CUSTOM;
}

/** Writes one ref: the bare id when interned, zero plus the literal
    otherwise. Callers resolve the id so path and type share one body. */
function writeRef(writer: ByteWriter, literal: string, id: number): void {
  if (id !== 0) {
    writer.writeUvarint(id);
    return;
  }

  writer.writeUvarint(0);
  writer.writeString(literal);
}

/** Guards a nonzero inbound ref on a connection with no tables. */
function resolveRef(refs: RefResolver | undefined, id: number): RefResolver {
  if (refs === undefined) {
    throw new ProtocolError(`table ref ${id} without a negotiated table`);
  }

  return refs;
}

function readPathRef(reader: ByteReader, refs: RefResolver | undefined): string {
  const id: number = reader.readUvarint();
  return id === 0 ? reader.readString() : resolveRef(refs, id).path(id);
}

function readTypeRef(reader: ByteReader, refs: RefResolver | undefined): string {
  const id: number = reader.readUvarint();
  return id === 0 ? reader.readString() : resolveRef(refs, id).type(id);
}

/** Enforces that a watch or unwatch carries no message. */
function validateWatchShape(envelope: DataEnvelope): boolean {
  if (envelope.kind !== KIND_WATCH && envelope.kind !== KIND_UNWATCH) {
    return true;
  }

  return envelope.typeRef === "" && envelope.payload.length === 0;
}

/**
 * Validates and writes a DATA body.
 *
 * @throws A `TypeError` for an envelope that violates the layout
 * rules; that is a sending-side programming error, not wire input.
 */
export function encodeDataEnvelope(
  writer: ByteWriter,
  envelope: DataEnvelope,
  refs?: RefInterner,
): void {
  if (envelope.kind < KIND_TELL || envelope.kind > KIND_UNWATCH) {
    throw new TypeError(`unknown envelope kind ${envelope.kind}`);
  }

  if (!knownSerializer(envelope.serializerId)) {
    throw new TypeError(`unknown serializer id ${envelope.serializerId}`);
  }

  if (!validateWatchShape(envelope)) {
    throw new TypeError("a watch or unwatch carries no type ref and no payload");
  }

  writer.writeU8(envelope.kind);
  writeRef(writer, envelope.to, refs === undefined ? 0 : refs.pathId(envelope.to));
  writer.writeString(envelope.uid);
  writeRef(writer, envelope.sender, refs === undefined ? 0 : refs.pathId(envelope.sender));
  writer.writeString(envelope.senderUid);
  writer.writeUvarint(envelope.timeout);
  writer.writeU8(envelope.serializerId);
  writeRef(writer, envelope.typeRef, refs === undefined ? 0 : refs.typeId(envelope.typeRef));
  writer.writeBytes(envelope.payload);
}

/**
 * Reads a DATA body; the payload is the rest of the input, returned
 * as a view over it.
 */
export function decodeDataEnvelope(reader: ByteReader, refs?: RefResolver): DataEnvelope {
  const kind: number = reader.readU8();
  if (kind > KIND_UNWATCH) {
    throw new ValueDecodeError(`unknown envelope kind ${kind}`);
  }

  const to: string = readPathRef(reader, refs);
  const uid: string = reader.readString();
  const sender: string = readPathRef(reader, refs);
  const senderUid: string = reader.readString();
  const timeout: number = reader.readUvarint();
  const serializerId: number = reader.readU8();
  if (!knownSerializer(serializerId)) {
    throw new ValueDecodeError(`unknown serializer id ${serializerId}`);
  }

  const typeRef: string = readTypeRef(reader, refs);
  const payload: Uint8Array = reader.readBytes(reader.remaining);
  const envelope: DataEnvelope = {
    kind,
    to,
    uid,
    sender,
    senderUid,
    timeout,
    serializerId,
    typeRef,
    payload,
  };
  if (!validateWatchShape(envelope)) {
    throw new ValueDecodeError("a watch or unwatch carries no type ref and no payload");
  }

  return envelope;
}

/** Validates and writes a REPLY body. */
export function encodeReplyEnvelope(
  writer: ByteWriter,
  envelope: ReplyEnvelope,
  refs?: RefInterner,
): void {
  if (!knownSerializer(envelope.serializerId)) {
    throw new TypeError(`unknown serializer id ${envelope.serializerId}`);
  }

  writer.writeU8(envelope.serializerId);
  writeRef(writer, envelope.typeRef, refs === undefined ? 0 : refs.typeId(envelope.typeRef));
  writer.writeBytes(envelope.payload);
}

/** Reads a REPLY body; the payload is the rest of the input. */
export function decodeReplyEnvelope(reader: ByteReader, refs?: RefResolver): ReplyEnvelope {
  const serializerId: number = reader.readU8();
  if (!knownSerializer(serializerId)) {
    throw new ValueDecodeError(`unknown serializer id ${serializerId}`);
  }

  const typeRef: string = readTypeRef(reader, refs);
  const payload: Uint8Array = reader.readBytes(reader.remaining);
  return { serializerId, typeRef, payload };
}

/** Validates and writes an ERROR body. */
export function encodeErrorBody(writer: ByteWriter, body: ErrorBody): void {
  if (body.code < ERROR_PROTOCOL || body.code > ERROR_APPLICATION) {
    throw new TypeError(`unknown error code ${body.code}`);
  }

  writer.writeU8(body.code);
  writer.writeUvarint(body.sentinel);
  writer.writeString(body.name);
  writer.writeString(body.message);
}

/** Reads an ERROR body. */
export function decodeErrorBody(reader: ByteReader): ErrorBody {
  const code: number = reader.readU8();
  if (code < ERROR_PROTOCOL || code > ERROR_APPLICATION) {
    throw new ValueDecodeError(`unknown error code ${code}`);
  }

  const sentinel: number = reader.readUvarint();
  const name: string = reader.readString();
  const message: string = reader.readString();
  return { code, sentinel, name, message };
}

/** Writes the HELLO parameters in wire field order. */
export function encodeHello(writer: ByteWriter, hello: Hello): void {
  writer.writeUvarint(hello.revision);
  writer.writeString(hello.systemName);
  writer.writeString(hello.host);
  writer.writeUvarint(hello.port);
  writer.writeU8(hello.lane);
  writer.writeU8(hello.compression);
  writer.writeUvarint(hello.maxFrameSize);
  writer.writeUvarint(hello.maxMessageSize);
  writer.writeUvarint(hello.initialCredits);
  writer.writeUvarint(hello.maxLargeTransfers);
}

/**
 * Reads the HELLO parameters. Trailing bytes are ignored on purpose:
 * a newer peer may append fields, and the negotiated revision governs
 * whether it may then use them.
 */
export function decodeHello(reader: ByteReader): Hello {
  const revision: number = reader.readUvarint();
  const systemName: string = reader.readString();
  const host: string = reader.readString();
  const port: number = reader.readUvarint();
  const lane: number = reader.readU8();
  const compression: number = reader.readU8();
  const maxFrameSize: number = reader.readUvarint();
  const maxMessageSize: number = reader.readUvarint();
  const initialCredits: number = reader.readUvarint();
  const maxLargeTransfers: number = reader.readUvarint();
  return {
    revision,
    systemName,
    host,
    port,
    lane,
    compression,
    maxFrameSize,
    maxMessageSize,
    initialCredits,
    maxLargeTransfers,
  };
}
