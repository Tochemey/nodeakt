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

/** Unsigned byte identifying the only wire format emitted and accepted here. @internal */
export const PROTOCOL_VERSION = 1;

/** Unsigned byte selecting a direct-probe packet body. @internal */
export const MESSAGE_PING = 0x01;

/** Unsigned byte selecting an indirect-probe request packet body. @internal */
export const MESSAGE_PING_REQ = 0x02;

/** Unsigned byte selecting a successful-probe packet body. @internal */
export const MESSAGE_ACK = 0x03;

/** Unsigned byte selecting a failed-indirect-probe packet body. @internal */
export const MESSAGE_NACK = 0x04;

/** Unsigned byte selecting a packet containing only membership gossip. @internal */
export const MESSAGE_GOSSIP = 0x05;

/** Unsigned byte selecting a full-table synchronization request chunk. @internal */
export const MESSAGE_SYNC_REQUEST = 0x10;

/** Unsigned byte selecting a full-table synchronization response chunk. @internal */
export const MESSAGE_SYNC_RESPONSE = 0x11;

/** Lowest-precedence state value: the member advertises itself as reachable. @internal */
export const STATE_ALIVE = 0;

/** State value for an unconfirmed failure accusation against a member. @internal */
export const STATE_SUSPECT = 1;

/** State value for a member declared failed by another member. @internal */
export const STATE_DEAD = 2;

/** Highest-precedence state value: a member's self-announced graceful departure. @internal */
export const STATE_LEFT = 3;

/** Transport preface role byte for bounded, datagram-like packet traffic. @internal */
export const ROLE_PACKET = 1;

/** Transport preface role byte for length-framed synchronization traffic. @internal */
export const ROLE_STREAM = 2;

/** Byte width of the version, type, and unsigned 16-bit body-length envelope. @internal */
export const MESSAGE_HEADER_BYTES = 4;

/** Byte width of a membership record before its variable UTF-8 and metadata fields. @internal */
export const UPDATE_HEADER_BYTES = 20;

/** Byte width of a sync body header before its membership records. @internal */
export const SYNC_HEADER_BYTES = 14;

/** Maximum complete packet size, including the common envelope, in bytes. @internal */
export const MAX_PACKET_BYTES = 1_400;

/** Maximum complete sync chunk accepted inside one stream frame, in bytes. @internal */
export const MAX_SYNC_MESSAGE_BYTES = 65_539;

/** Maximum aggregate sync exchange size, including each 4-byte stream prefix, in bytes. @internal */
export const MAX_SYNC_EXCHANGE_BYTES = 1_048_576;

/** Maximum distinct advertised member identities in one complete sync table. @internal */
export const MAX_MEMBERS = 1_024;

/** Maximum encoded UTF-8 bytes in any protocol identity/address field. @internal */
export const MAX_NAME_BYTES = 255;

/** Maximum membership records in one counted packet update list. @internal */
export const UPDATE_LIST_MAX_RECORDS = 255;

/** Maximum opaque metadata payload on an alive membership record, in bytes. @internal */
export const MAX_METADATA_BYTES = 512;

/**
 * Membership states represented as unsigned bytes on the wire.
 *
 * Numeric order is also equal-incarnation truth precedence: alive, suspect, dead, left.
 *
 * @internal
 */
export type MemberState =
  | typeof STATE_ALIVE
  | typeof STATE_SUSPECT
  | typeof STATE_DEAD
  | typeof STATE_LEFT;

/** Message discriminators legal on packet-role connections. @internal */
export type PacketMessageType =
  | typeof MESSAGE_PING
  | typeof MESSAGE_PING_REQ
  | typeof MESSAGE_ACK
  | typeof MESSAGE_NACK
  | typeof MESSAGE_GOSSIP;

/** Message discriminators legal on stream-role connections. @internal */
export type SyncMessageType = typeof MESSAGE_SYNC_REQUEST | typeof MESSAGE_SYNC_RESPONSE;

/**
 * One canonical membership truth record carried by packet or synchronization traffic.
 *
 * The codec requires unsigned 32-bit incarnations, unsigned 64-bit state-change times,
 * state-specific provenance, and metadata only on alive records. `metadata` remains
 * caller-owned on input; encoders read it synchronously, while decoders allocate it.
 *
 * @internal
 */
export interface MembershipUpdate {
  /** Advertised state; its numeric value defines equal-incarnation precedence. */
  readonly state: MemberState;

  /** True exactly for self-originated alive and left records; false for suspect and dead. */
  readonly selfOriginated: boolean;

  /** Unsigned 32-bit, subject-controlled version that increases when the subject refutes truth. */
  readonly incarnation: number;

  /** Unsigned 64-bit origin timestamp; transported verbatim and not used by wire precedence. */
  readonly stateChangeTime: bigint;

  /**
   * Canonical advertised member identity and gossip address (`host:port`) whose truth is carried.
   *
   * This is the protocol's stable member key, not a generic member object or transport peer.
   */
  readonly member: string;

  /** Canonical identity/address of the accuser for suspect records; empty for all other states. */
  readonly reporter: string;

  /** Opaque application bytes; at most 512 bytes for alive records and empty otherwise. */
  readonly metadata: Uint8Array;
}

/**
 * Copies a membership update, detaching only its mutable metadata bytes.
 *
 * All other fields are immutable primitives shared with the source object.
 *
 * @internal
 */
export function copyMembershipUpdate(update: MembershipUpdate): MembershipUpdate {
  return {
    state: update.state,
    selfOriginated: update.selfOriginated,
    incarnation: update.incarnation,
    stateChangeTime: update.stateChangeTime,
    member: update.member,
    reporter: update.reporter,
    metadata: Uint8Array.from(update.metadata),
  };
}

/** Fields shared by probe packets and their positive or negative responses. */
interface ProbeMessage {
  /** Unsigned 32-bit sequence assigned by the probe owner and echoed by responses. */
  readonly sequence: number;

  /** Canonical identity/address of the member that owns and correlates the probe. */
  readonly owner: string;

  /** Ordered membership records piggybacked on this packet; limited to 255 records. */
  readonly updates: readonly MembershipUpdate[];
}

/** Direct probe, optionally relayed for an indirect-probe requester. @internal */
export interface PingMessage extends ProbeMessage {
  /** Fixed discriminator selecting the direct-probe body layout. */
  readonly type: typeof MESSAGE_PING;

  /** Canonical requester identity/address to receive a relayed ACK, or empty for a direct probe. */
  readonly relay: string;
}

/** Request for a helper to probe a target. @internal */
export interface PingReqMessage extends ProbeMessage {
  /** Fixed discriminator selecting the indirect-probe-request body layout. */
  readonly type: typeof MESSAGE_PING_REQ;

  /** Canonical identity/address that the receiving helper must probe. */
  readonly target: string;
}

/** Successful probe response. @internal */
export interface AckMessage extends ProbeMessage {
  /** Fixed discriminator selecting the successful-probe body layout. */
  readonly type: typeof MESSAGE_ACK;

  /** Canonical identity/address whose direct or relayed probe succeeded. */
  readonly target: string;
}

/** Helper response showing that an indirect probe completed without an ACK. @internal */
export interface NackMessage extends ProbeMessage {
  /** Fixed discriminator selecting the negative indirect-probe body layout. */
  readonly type: typeof MESSAGE_NACK;

  /** Canonical identity/address that did not answer the helper's probe. */
  readonly target: string;

  /** Canonical identity/address of the helper reporting its completed attempt. */
  readonly helper: string;
}

/** Packet containing only piggybacked membership truth. @internal */
export interface GossipMessage {
  /** Fixed discriminator selecting the gossip-only body layout. */
  readonly type: typeof MESSAGE_GOSSIP;

  /** Ordered membership records; encoded count is an unsigned byte. */
  readonly updates: readonly MembershipUpdate[];
}

/** One chunk of a full membership-table synchronization. @internal */
export interface SyncMessage {
  /** Whether this chunk belongs to the push side or response side of an exchange. */
  readonly type: SyncMessageType;

  /** Nonzero unsigned 64-bit identifier shared by every chunk in this exchange direction. */
  readonly exchangeId: bigint;

  /** Unsigned 16-bit, zero-based canonical position; strictly less than `chunkCount`. */
  readonly chunkIndex: number;

  /** Nonzero unsigned 16-bit number of chunks in the complete exchange direction. */
  readonly chunkCount: number;

  /** Distinct-member records packed in canonical largest-prefix order for full exchanges. */
  readonly updates: readonly MembershipUpdate[];
}

/** Any message legal on a packet-role connection. @internal */
export type PacketMessage = PingMessage | PingReqMessage | AckMessage | NackMessage | GossipMessage;

/** Any decoded membership message before transport-role legality is enforced. @internal */
export type MembershipMessage = PacketMessage | SyncMessage;

/** Fully validated membership table reconstructed from one exchange direction. */
interface DecodedSyncExchange {
  /** Request/response discriminator shared by every validated input chunk. */
  readonly type: SyncMessageType;

  /** Nonzero exchange identifier shared by every validated input chunk. */
  readonly exchangeId: bigint;

  /** Newly decoded updates in canonical chunk and record order. */
  readonly updates: readonly MembershipUpdate[];
}

/** Error raised for malformed, oversized, noncanonical, or role-inappropriate protocol data. @internal */
export class ProtocolError extends Error {
  /** Creates an error with the stable `ProtocolError` name and supplied diagnostic message. */
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

/** Shared stateless UTF-8 encoder used for length validation and serialization. */
const encoder = new TextEncoder();

/** Shared fatal UTF-8 decoder; malformed byte sequences are protocol errors. */
const decoder = new TextDecoder("utf-8", { fatal: true });

/** Shared zero-length metadata value returned where no mutable bytes exist. */
const EMPTY_BYTES = new Uint8Array(0);

/** Raises a typed protocol error and marks the calling path as non-returning. */
function fail(message: string): never {
  throw new ProtocolError(message);
}

/** Shared encode/decode failure text for an unrecognized membership state byte. */
const UNKNOWN_STATE_MESSAGE = "unknown membership state";

/** Shared encode/decode failure text for metadata beyond its unsigned 16-bit budget. */
const METADATA_LENGTH_MESSAGE = "metadata length is out of range";

/** Shared encode/decode failure text for a reporter violating its state rule. */
const REPORTER_STATE_MESSAGE = "reporter is inconsistent with state";

/** Shared encode/decode failure text for metadata on a non-alive record. */
const METADATA_STATE_MESSAGE = "metadata is only legal on alive updates";

/** Shared encode/decode failure text for a provenance flag violating its state rule. */
const SELF_ORIGINATED_MESSAGE = "self-originated flag is inconsistent with state";

/** Shared encode/decode failure text for an illegal sync chunk index/count pair. */
const SYNC_CHUNK_POSITION_MESSAGE = "sync chunk position is invalid";

/** Shared encode/decode failure text for one chunk naming too many members. */
const SYNC_MEMBER_COUNT_MESSAGE = `sync member count exceeds ${MAX_MEMBERS}`;

/** Shared encode/decode failure text for a complete table naming too many members. */
const SYNC_TABLE_MESSAGE = `sync table exceeds ${MAX_MEMBERS} members`;

/** Shared encode/decode failure text for a complete packet beyond its byte cap. */
const PACKET_SIZE_MESSAGE = `packet message exceeds ${MAX_PACKET_BYTES} bytes`;

/** Adds one member identity to `names`, rejecting a duplicate advertised member. */
function addDistinctMember(names: Set<string>, member: string): void {
  if (names.has(member)) {
    fail(`duplicate sync member ${member}`);
  }

  names.add(member);
}

/** Validates an inclusive unsigned integer range representable safely as a number. */
function assertUint(value: number, maximum: number, field: string): void {
  if (!Number.isInteger(value) || value < 0 || value > maximum) {
    fail(`${field} is out of range`);
  }
}

/** Validates an unsigned 64-bit bigint, optionally excluding zero. */
function assertU64(value: bigint, field: string, nonzero = false): void {
  if (value < 0n || value > 0xffffffffffffffffn || (nonzero && value === 0n)) {
    fail(`${field} is out of range`);
  }
}

/**
 * Encodes and validates a protocol identity/address.
 *
 * Empty values are accepted only for explicitly optional fields. NUL, malformed Unicode,
 * and values exceeding the one-byte UTF-8 length field raise `ProtocolError`.
 */
function encodeName(name: string, field: string, allowEmpty = false): Uint8Array {
  if (name.includes("\0")) {
    fail(`${field} contains NUL`);
  }

  const bytes = encoder.encode(name);
  if ((!allowEmpty && bytes.length === 0) || bytes.length > MAX_NAME_BYTES) {
    fail(`${field} length is out of range`);
  }

  if (!name.isWellFormed()) {
    fail(`${field} is not valid Unicode`);
  }

  return bytes;
}

/**
 * Decodes a validated UTF-8 identity/address from an already bounds-checked byte range.
 *
 * The returned string is newly decoded. Invalid UTF-8, NUL, and illegal lengths raise
 * `ProtocolError`.
 */
function decodeName(
  bytes: Uint8Array,
  offset: number,
  length: number,
  field: string,
  allowEmpty = false,
): string {
  if ((!allowEmpty && length === 0) || length > MAX_NAME_BYTES) {
    fail(`${field} length is out of range`);
  }

  let value: string;
  try {
    value = decoder.decode(bytes.subarray(offset, offset + length));
  } catch {
    fail(`${field} is invalid UTF-8`);
  }

  if (value.includes("\0")) {
    fail(`${field} contains NUL`);
  }

  return value;
}

/** One update validated for encoding, with pre-encoded names and its exact record size. */
interface PreparedUpdate {
  /** Source update whose scalar fields and metadata are written verbatim. */
  readonly update: MembershipUpdate;

  /** Validated UTF-8 bytes for the canonical advertised member identity/address. */
  readonly member: Uint8Array;

  /** Validated UTF-8 bytes for the optional suspect reporter identity/address. */
  readonly reporter: Uint8Array;

  /** Exact complete encoded record size in bytes, including the length prefix. */
  readonly length: number;
}

/**
 * Validates all state-dependent membership invariants and pre-encodes variable names.
 *
 * Returns the encoded names and exact total record bytes without copying metadata,
 * so one validation/encoding pass can serve both sizing and writing. Throws
 * `ProtocolError` for any value that cannot be represented canonically.
 */
function prepareUpdate(update: MembershipUpdate): PreparedUpdate {
  if (
    update.state !== STATE_ALIVE &&
    update.state !== STATE_SUSPECT &&
    update.state !== STATE_DEAD &&
    update.state !== STATE_LEFT
  ) {
    fail(UNKNOWN_STATE_MESSAGE);
  }

  assertUint(update.incarnation, 0xffffffff, "incarnation");
  assertU64(update.stateChangeTime, "state-change time");
  const member = encodeName(update.member, "member");
  const reporter = encodeName(update.reporter, "reporter", true);
  if (update.metadata.length > MAX_METADATA_BYTES) {
    fail(METADATA_LENGTH_MESSAGE);
  }

  if (update.state === STATE_SUSPECT ? reporter.length === 0 : reporter.length !== 0) {
    fail(REPORTER_STATE_MESSAGE);
  }

  if (update.state === STATE_ALIVE ? false : update.metadata.length !== 0) {
    fail(METADATA_STATE_MESSAGE);
  }

  const selfRequired = update.state === STATE_ALIVE || update.state === STATE_LEFT;
  if (update.selfOriginated !== selfRequired) {
    fail(SELF_ORIGINATED_MESSAGE);
  }

  return {
    update,
    member,
    reporter,
    length: UPDATE_HEADER_BYTES + member.length + reporter.length + update.metadata.length,
  };
}

/**
 * Writes one prepared record at `offset` through the message-wide data view.
 *
 * Returns the exclusive next offset. Metadata is copied from the source update.
 */
function writePreparedUpdate(
  bytes: Uint8Array,
  view: DataView,
  offset: number,
  prepared: PreparedUpdate,
): number {
  const update = prepared.update;
  view.setUint16(offset, prepared.length - 2);
  view.setUint8(offset + 2, update.state);
  view.setUint8(offset + 3, update.selfOriginated ? 1 : 0);
  view.setUint32(offset + 4, update.incarnation);
  view.setBigUint64(offset + 8, update.stateChangeTime);
  view.setUint8(offset + 16, prepared.member.length);
  view.setUint8(offset + 17, prepared.reporter.length);
  view.setUint16(offset + 18, update.metadata.length);
  let position = offset + UPDATE_HEADER_BYTES;
  bytes.set(prepared.member, position);
  position += prepared.member.length;
  bytes.set(prepared.reporter, position);
  position += prepared.reporter.length;
  bytes.set(update.metadata, position);
  return offset + prepared.length;
}

/**
 * Returns the exact record size in bytes, including its unsigned 16-bit length prefix.
 *
 * @throws {ProtocolError} If the update violates wire ranges or state-specific invariants.
 * @internal
 */
export function membershipUpdateSize(update: MembershipUpdate): number {
  return prepareUpdate(update).length;
}

/**
 * Encodes one membership update in canonical big-endian form, including its length prefix.
 *
 * The returned bytes are newly allocated; metadata is copied into them.
 *
 * @throws {ProtocolError} If the update violates wire ranges or state-specific invariants.
 * @internal
 */
export function encodeMembershipUpdate(update: MembershipUpdate): Uint8Array {
  const prepared = prepareUpdate(update);
  const bytes = new Uint8Array(prepared.length);
  writePreparedUpdate(bytes, new DataView(bytes.buffer), 0, prepared);
  return bytes;
}

/** One decoded record and the first byte after it in its containing message. */
interface DecodedUpdate {
  /** Newly constructed update; nonempty metadata is copied from the input bytes. */
  readonly update: MembershipUpdate;

  /** Exclusive absolute byte offset of this record in the input. */
  readonly end: number;
}

/**
 * Decodes one length-prefixed record between `offset` and the exclusive `limit`.
 *
 * The caller retains the input buffer and supplies one message-wide data view over
 * it; nonempty metadata is copied before return.
 */
function decodeMembershipUpdateAt(
  bytes: Uint8Array,
  view: DataView,
  offset: number,
  limit: number,
): DecodedUpdate {
  if (limit - offset < UPDATE_HEADER_BYTES) {
    fail("membership update is truncated");
  }

  const recordLength = view.getUint16(offset);
  const totalLength = recordLength + 2;
  if (totalLength < UPDATE_HEADER_BYTES || totalLength > limit - offset) {
    fail("membership record length is inconsistent");
  }

  const end = offset + totalLength;
  const state = view.getUint8(offset + 2);
  const flags = view.getUint8(offset + 3);
  const memberLength = view.getUint8(offset + 16);
  const reporterLength = view.getUint8(offset + 17);
  const metadataLength = view.getUint16(offset + 18);
  if (state > STATE_LEFT) {
    fail(UNKNOWN_STATE_MESSAGE);
  }

  if ((flags & 0xfe) !== 0) {
    fail("reserved membership flags are set");
  }

  if (memberLength === 0) {
    fail("member name is empty");
  }

  if (metadataLength > MAX_METADATA_BYTES) {
    fail(METADATA_LENGTH_MESSAGE);
  }

  const expected = UPDATE_HEADER_BYTES + memberLength + reporterLength + metadataLength;
  if (totalLength !== expected) {
    fail("membership record fields do not match its length");
  }

  const selfOriginated = flags === 1;
  const selfRequired = state === STATE_ALIVE || state === STATE_LEFT;

  if (selfOriginated !== selfRequired) {
    fail(SELF_ORIGINATED_MESSAGE);
  }

  if (state === STATE_SUSPECT ? reporterLength === 0 : reporterLength !== 0) {
    fail(REPORTER_STATE_MESSAGE);
  }

  if (state !== STATE_ALIVE && metadataLength !== 0) {
    fail(METADATA_STATE_MESSAGE);
  }

  const memberOffset = offset + UPDATE_HEADER_BYTES;
  const reporterOffset = memberOffset + memberLength;
  const metadataOffset = reporterOffset + reporterLength;
  const member = decodeName(bytes, memberOffset, memberLength, "member");
  const reporter = decodeName(bytes, reporterOffset, reporterLength, "reporter", true);
  const metadata =
    metadataLength === 0
      ? EMPTY_BYTES
      : bytes.slice(metadataOffset, metadataOffset + metadataLength);
  return {
    update: {
      state: state as MemberState,
      selfOriginated,
      incarnation: view.getUint32(offset + 4),
      stateChangeTime: view.getBigUint64(offset + 8),
      member,
      reporter,
      metadata,
    },
    end,
  };
}

/**
 * Decodes exactly one canonical membership record from the supplied bytes.
 *
 * Nonempty metadata is copied, so later input mutation cannot affect the result.
 *
 * @throws {ProtocolError} If the record is malformed, truncated, or followed by bytes.
 * @internal
 */
export function decodeMembershipUpdate(bytes: Uint8Array): MembershipUpdate {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const decoded = decodeMembershipUpdateAt(bytes, view, 0, bytes.length);
  if (decoded.end !== bytes.length) {
    fail("trailing bytes after membership update");
  }

  return decoded.update;
}

/** Pre-encoded packet name fields and the base body size they imply. */
interface PreparedPacketNames {
  /** Length-prefixed name fields in exact write order after any probe sequence. */
  readonly names: readonly Uint8Array[];

  /** Packet body size in bytes, excluding membership updates and their count. */
  readonly baseLength: number;
}

/** Validates and encodes every packet name field once, computing the base body size. */
function preparePacketNames(message: PacketMessage): PreparedPacketNames {
  const names: Uint8Array[] = [];
  if (message.type !== MESSAGE_GOSSIP) {
    names.push(encodeName(message.owner, "probe owner"));
  }

  switch (message.type) {
    case MESSAGE_PING:
      names.push(encodeName(message.relay, "relay", true));
      break;
    case MESSAGE_NACK:
      names.push(encodeName(message.target, "target"));
      names.push(encodeName(message.helper, "helper"));
      break;
    case MESSAGE_PING_REQ:
    case MESSAGE_ACK:
      names.push(encodeName(message.target, "target"));
      break;
    case MESSAGE_GOSSIP:
      break;
  }

  let baseLength = message.type === MESSAGE_GOSSIP ? 1 : 5;
  for (const name of names) {
    baseLength += 1 + name.length;
  }

  return { names, baseLength };
}

/**
 * Returns the exact encoded byte length of `message` carrying no updates.
 *
 * This is the fixed envelope-plus-header overhead before piggybacked records,
 * letting callers compute a remaining update byte budget without encoding a
 * throwaway frame.
 *
 * @throws {ProtocolError} If any name field is invalid.
 * @internal
 */
export function packetOverheadLength(message: PacketMessage): number {
  return MESSAGE_HEADER_BYTES + preparePacketNames(message).baseLength;
}

/** Fully validated packet body: encoded names, prepared updates, and exact body size. */
interface PreparedPacketBody extends PreparedPacketNames {
  /** Prepared piggybacked records in message order. */
  readonly updates: readonly PreparedUpdate[];

  /** Complete packet body size in bytes. */
  readonly bodyLength: number;
}

/** Validates names, update count, and every update exactly once for one packet encode. */
function preparePacketBody(message: PacketMessage): PreparedPacketBody {
  const base = preparePacketNames(message);
  if (message.updates.length > UPDATE_LIST_MAX_RECORDS) {
    fail(`packet update count exceeds ${UPDATE_LIST_MAX_RECORDS}`);
  }

  const updates: PreparedUpdate[] = [];
  let bodyLength = base.baseLength;
  for (const update of message.updates) {
    const prepared = prepareUpdate(update);
    updates.push(prepared);
    bodyLength += prepared.length;
  }

  return { names: base.names, baseLength: base.baseLength, updates, bodyLength };
}

/** Writes a prepared packet body after validating its unsigned probe sequence. */
function writePacketBody(
  message: PacketMessage,
  prepared: PreparedPacketBody,
  bytes: Uint8Array,
  view: DataView,
): void {
  let offset = MESSAGE_HEADER_BYTES;
  if (message.type !== MESSAGE_GOSSIP) {
    assertUint(message.sequence, 0xffffffff, "probe sequence");
    view.setUint32(offset, message.sequence);
    offset += 4;
  }

  for (const name of prepared.names) {
    bytes[offset] = name.length;
    bytes.set(name, offset + 1);
    offset += 1 + name.length;
  }

  bytes[offset] = prepared.updates.length;
  offset += 1;
  for (const update of prepared.updates) {
    offset = writePreparedUpdate(bytes, view, offset, update);
  }
}

/** Fully validated sync chunk body: prepared distinct-member updates and exact body size. */
interface PreparedSyncBody {
  /** Prepared distinct-member records in message order. */
  readonly updates: readonly PreparedUpdate[];

  /** Complete sync chunk body size in bytes. */
  readonly bodyLength: number;
}

/** Validates one sync chunk body exactly once, including distinct-member enforcement. */
function prepareSyncBody(message: SyncMessage): PreparedSyncBody {
  assertU64(message.exchangeId, "exchange ID", true);
  assertUint(message.chunkIndex, 0xffff, "chunk index");
  assertUint(message.chunkCount, 0xffff, "chunk count");
  if (message.chunkCount === 0 || message.chunkIndex >= message.chunkCount) {
    fail(SYNC_CHUNK_POSITION_MESSAGE);
  }

  if (message.updates.length > MAX_MEMBERS) {
    fail(SYNC_MEMBER_COUNT_MESSAGE);
  }

  const names = new Set<string>();
  const updates: PreparedUpdate[] = [];
  let bodyLength = SYNC_HEADER_BYTES;
  for (const update of message.updates) {
    addDistinctMember(names, update.member);
    const prepared = prepareUpdate(update);
    updates.push(prepared);
    bodyLength += prepared.length;
  }

  return { updates, bodyLength };
}

/** Narrows a membership message by its reserved sync discriminator range. */
function isSyncMessage(message: MembershipMessage): message is SyncMessage {
  return message.type === MESSAGE_SYNC_REQUEST || message.type === MESSAGE_SYNC_RESPONSE;
}

/** Allocates one message buffer, enforcing size limits and writing the common envelope. */
function allocateMessage(
  type: number,
  bodyLength: number,
  packet: boolean,
): {
  /** Zero-filled buffer for the complete message, with its envelope written. */
  readonly bytes: Uint8Array;

  /** Data view over the complete buffer shared by every body writer. */
  readonly view: DataView;
} {
  if (bodyLength > 0xffff) {
    fail("message body exceeds the envelope limit");
  }

  const totalLength = MESSAGE_HEADER_BYTES + bodyLength;
  if (packet && totalLength > MAX_PACKET_BYTES) {
    fail(PACKET_SIZE_MESSAGE);
  }

  const bytes = new Uint8Array(totalLength);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, PROTOCOL_VERSION);
  view.setUint8(1, type);
  view.setUint16(2, bodyLength);
  return { bytes, view };
}

/**
 * Encodes one packet or sync chunk with its canonical big-endian common envelope.
 *
 * Returns a new byte array. Packet messages are capped at 1,400 complete bytes; sync
 * bodies must fit the envelope's unsigned 16-bit length.
 *
 * @throws {ProtocolError} If any field is invalid, duplicated, or exceeds protocol limits.
 * @internal
 */
export function encodeMessage(message: MembershipMessage): Uint8Array {
  if (isSyncMessage(message)) {
    const body = prepareSyncBody(message);
    const { bytes, view } = allocateMessage(message.type, body.bodyLength, false);
    view.setBigUint64(4, message.exchangeId);
    view.setUint16(12, message.chunkIndex);
    view.setUint16(14, message.chunkCount);
    view.setUint16(16, body.updates.length);
    let offset = SYNC_HEADER_BYTES + MESSAGE_HEADER_BYTES;
    for (const update of body.updates) {
      offset = writePreparedUpdate(bytes, view, offset, update);
    }

    return bytes;
  }

  const body = preparePacketBody(message);
  const { bytes, view } = allocateMessage(message.type, body.bodyLength, true);
  writePacketBody(message, body, bytes, view);
  return bytes;
}

/** Reads one length-prefixed name within `limit` and returns its value and next offset. */
function readName(
  bytes: Uint8Array,
  offset: number,
  limit: number,
  field: string,
  empty = false,
): {
  /** Decoded protocol identity/address. */
  readonly value: string;

  /** Exclusive byte offset immediately after the encoded name. */
  readonly end: number;
} {
  if (offset >= limit) {
    fail(`${field} length is truncated`);
  }

  const length = bytes[offset] as number;
  const end = offset + 1 + length;
  if (end > limit) {
    fail(`${field} is truncated`);
  }

  return { value: decodeName(bytes, offset + 1, length, field, empty), end };
}

/**
 * Decodes exactly `count` consecutive membership records up to `limit`.
 *
 * Rejects both truncation and trailing bytes, returning newly allocated update objects.
 */
function decodeUpdateList(
  bytes: Uint8Array,
  view: DataView,
  offset: number,
  limit: number,
  count: number,
): readonly MembershipUpdate[] {
  const updates: MembershipUpdate[] = [];
  for (let index = 0; index < count; index += 1) {
    const decoded = decodeMembershipUpdateAt(bytes, view, offset, limit);
    updates.push(decoded.update);
    offset = decoded.end;
  }

  if (offset !== limit) {
    fail("trailing bytes after update list");
  }

  return updates;
}

/** Decoded fields common to all probe-family packets and the next unread offset. */
interface DecodedProbeHeader {
  /** Unsigned 32-bit probe correlation sequence. */
  readonly sequence: number;

  /** Canonical identity/address of the probe owner. */
  readonly owner: string;

  /** Exclusive offset immediately after the owner field. */
  readonly end: number;
}

/** Decodes and bounds-checks the common sequence-plus-owner probe header. */
function decodeProbeHeader(
  bytes: Uint8Array,
  view: DataView,
  bodyOffset: number,
  limit: number,
): DecodedProbeHeader {
  if (limit - bodyOffset < 4) {
    fail("probe sequence is truncated");
  }

  const sequence = view.getUint32(bodyOffset);
  const owner = readName(bytes, bodyOffset + 4, limit, "probe owner");

  return { sequence, owner: owner.value, end: owner.end };
}

/** Decodes an unsigned-byte-counted update suffix and requires it to consume the body. */
function decodeCountedPacketUpdates(
  bytes: Uint8Array,
  view: DataView,
  offset: number,
  limit: number,
): readonly MembershipUpdate[] {
  if (offset >= limit) {
    fail("packet update count is truncated");
  }

  return decodeUpdateList(bytes, view, offset + 1, limit, bytes[offset] as number);
}

/** Decodes a gossip-only body whose first byte is the update count. */
function decodeGossipMessage(
  bytes: Uint8Array,
  view: DataView,
  bodyOffset: number,
  limit: number,
): GossipMessage {
  if (bodyOffset >= limit) {
    fail("gossip update count is truncated");
  }

  const count = bytes[bodyOffset] as number;
  return {
    type: MESSAGE_GOSSIP,
    updates: decodeUpdateList(bytes, view, bodyOffset + 1, limit, count),
  };
}

/** Decodes a direct or relayed ping body and its complete piggyback list. */
function decodePingMessage(
  bytes: Uint8Array,
  view: DataView,
  bodyOffset: number,
  limit: number,
): PingMessage {
  const probe = decodeProbeHeader(bytes, view, bodyOffset, limit);
  const relay = readName(bytes, probe.end, limit, "relay", true);

  return {
    type: MESSAGE_PING,
    sequence: probe.sequence,
    owner: probe.owner,
    relay: relay.value,
    updates: decodeCountedPacketUpdates(bytes, view, relay.end, limit),
  };
}

/** Decodes the shared PING_REQ/ACK body layout while preserving its discriminator. */
function decodeTargetMessage(
  type: typeof MESSAGE_PING_REQ | typeof MESSAGE_ACK,
  bytes: Uint8Array,
  view: DataView,
  bodyOffset: number,
  limit: number,
): PingReqMessage | AckMessage {
  const probe = decodeProbeHeader(bytes, view, bodyOffset, limit);
  const target = readName(bytes, probe.end, limit, "target");

  return {
    type,
    sequence: probe.sequence,
    owner: probe.owner,
    target: target.value,
    updates: decodeCountedPacketUpdates(bytes, view, target.end, limit),
  };
}

/** Decodes a negative indirect-probe body and its complete piggyback list. */
function decodeNackMessage(
  bytes: Uint8Array,
  view: DataView,
  bodyOffset: number,
  limit: number,
): NackMessage {
  const probe = decodeProbeHeader(bytes, view, bodyOffset, limit);
  const target = readName(bytes, probe.end, limit, "target");
  const helper = readName(bytes, target.end, limit, "helper");

  return {
    type: MESSAGE_NACK,
    sequence: probe.sequence,
    owner: probe.owner,
    target: target.value,
    helper: helper.value,
    updates: decodeCountedPacketUpdates(bytes, view, helper.end, limit),
  };
}

/** Dispatches a packet discriminator to its exact body decoder. */
function decodePacketBody(
  type: PacketMessageType,
  bytes: Uint8Array,
  view: DataView,
  bodyOffset: number,
  limit: number,
): PacketMessage {
  switch (type) {
    case MESSAGE_PING:
      return decodePingMessage(bytes, view, bodyOffset, limit);
    case MESSAGE_PING_REQ:
    case MESSAGE_ACK:
      return decodeTargetMessage(type, bytes, view, bodyOffset, limit);
    case MESSAGE_NACK:
      return decodeNackMessage(bytes, view, bodyOffset, limit);
    case MESSAGE_GOSSIP:
      return decodeGossipMessage(bytes, view, bodyOffset, limit);
  }
}

/**
 * Validates the common envelope against the exact input length.
 *
 * The input remains caller-owned and is not copied.
 */
function decodeEnvelope(
  bytes: Uint8Array,
  view: DataView,
): {
  /** Raw unsigned-byte discriminator; body dispatch validates whether it is known. */
  readonly type: number;

  /** Absolute offset of the first body byte. */
  readonly bodyOffset: number;

  /** Exclusive message limit, equal to the validated input length. */
  readonly limit: number;
} {
  if (bytes.length < MESSAGE_HEADER_BYTES) {
    fail("message envelope is truncated");
  }

  const version = view.getUint8(0);
  if (version !== PROTOCOL_VERSION) {
    fail(`unsupported protocol version ${version}`);
  }

  const type = view.getUint8(1);
  const bodyLength = view.getUint16(2);
  if (bodyLength + MESSAGE_HEADER_BYTES !== bytes.length) {
    fail("message body length is inconsistent");
  }

  return { type, bodyOffset: MESSAGE_HEADER_BYTES, limit: bytes.length };
}

/** Decodes one sync chunk body and rejects invalid positions, counts, and duplicate members. */
function decodeSyncBody(
  type: SyncMessageType,
  bytes: Uint8Array,
  view: DataView,
  offset: number,
): SyncMessage {
  if (bytes.length - offset < SYNC_HEADER_BYTES) {
    fail("sync header is truncated");
  }

  const exchangeId = view.getBigUint64(offset);
  const chunkIndex = view.getUint16(offset + 8);
  const chunkCount = view.getUint16(offset + 10);
  const memberCount = view.getUint16(offset + 12);
  if (exchangeId === 0n) {
    fail("exchange ID is zero");
  }

  if (chunkCount === 0 || chunkIndex >= chunkCount) {
    fail(SYNC_CHUNK_POSITION_MESSAGE);
  }

  if (memberCount > MAX_MEMBERS) {
    fail(SYNC_MEMBER_COUNT_MESSAGE);
  }

  const updates = decodeUpdateList(
    bytes,
    view,
    offset + SYNC_HEADER_BYTES,
    bytes.length,
    memberCount,
  );
  const names = new Set<string>();
  for (const update of updates) {
    addDistinctMember(names, update.member);
  }

  return { type, exchangeId, chunkIndex, chunkCount, updates };
}

/**
 * Decodes one complete message without enforcing the connection role.
 *
 * Packet limits, canonical field encodings, and exact body consumption are enforced.
 *
 * @throws {ProtocolError} If the message is malformed, unknown, or over its packet limit.
 * @internal
 */
export function decodeMessage(bytes: Uint8Array): MembershipMessage {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const envelope = decodeEnvelope(bytes, view);
  if (envelope.type >= MESSAGE_PING && envelope.type <= MESSAGE_GOSSIP) {
    if (bytes.length > MAX_PACKET_BYTES) {
      fail(PACKET_SIZE_MESSAGE);
    }

    return decodePacketBody(
      envelope.type as PacketMessageType,
      bytes,
      view,
      envelope.bodyOffset,
      envelope.limit,
    );
  }

  if (envelope.type === MESSAGE_SYNC_REQUEST || envelope.type === MESSAGE_SYNC_RESPONSE) {
    return decodeSyncBody(envelope.type, bytes, view, envelope.bodyOffset);
  }

  fail(`unknown message type 0x${envelope.type.toString(16)}`);
}

/**
 * Decodes one complete packet-role message.
 *
 * @throws {ProtocolError} If decoding fails or the bytes contain a sync message.
 * @internal
 */
export function decodePacketMessage(bytes: Uint8Array): PacketMessage {
  const message = decodeMessage(bytes);
  if (isSyncMessage(message)) {
    fail("sync message is illegal on a packet connection");
  }

  return message;
}

/**
 * Decodes one complete stream-role synchronization message.
 *
 * @throws {ProtocolError} If decoding fails or the bytes contain a packet message.
 * @internal
 */
export function decodeSyncMessage(bytes: Uint8Array): SyncMessage {
  const message = decodeMessage(bytes);
  if (message.type !== MESSAGE_SYNC_REQUEST && message.type !== MESSAGE_SYNC_RESPONSE) {
    fail("packet message is illegal on a stream connection");
  }

  return message;
}

/**
 * Splits a complete sync table into canonical largest-prefix chunks.
 * The returned values include message envelopes but not u32 stream frame prefixes.
 *
 * Input order is preserved, each output is newly allocated, and an empty table produces one
 * empty chunk. Member identities must be distinct across the complete table.
 *
 * @throws {ProtocolError} If the type, exchange ID, update fields, or table limits are invalid.
 * @internal
 */
export function encodeSyncChunks(
  type: SyncMessageType,
  exchangeId: bigint,
  updates: readonly MembershipUpdate[],
): readonly Uint8Array[] {
  if (type !== MESSAGE_SYNC_REQUEST && type !== MESSAGE_SYNC_RESPONSE) {
    fail("unknown sync message type");
  }

  assertU64(exchangeId, "exchange ID", true);
  if (updates.length > MAX_MEMBERS) {
    fail(SYNC_TABLE_MESSAGE);
  }

  const names = new Set<string>();
  const sizes: number[] = [];
  for (const update of updates) {
    addDistinctMember(names, update.member);
    sizes.push(membershipUpdateSize(update));
  }

  const chunks: MembershipUpdate[][] = [];
  let current: MembershipUpdate[] = [];
  let currentSize = MESSAGE_HEADER_BYTES + SYNC_HEADER_BYTES;
  for (let index = 0; index < updates.length; index += 1) {
    const size = sizes[index] as number;
    if (current.length > 0 && currentSize + size > MAX_SYNC_MESSAGE_BYTES) {
      chunks.push(current);
      current = [];
      currentSize = MESSAGE_HEADER_BYTES + SYNC_HEADER_BYTES;
    }

    current.push(updates[index] as MembershipUpdate);
    currentSize += size;
  }

  chunks.push(current);
  const chunkCount = chunks.length;
  const encoded = chunks.map(
    (chunk: MembershipUpdate[], chunkIndex: number): Uint8Array =>
      encodeMessage({ type, exchangeId, chunkIndex, chunkCount, updates: chunk }),
  );
  return encoded;
}

/**
 * Decodes all chunks while enforcing the aggregate framed-byte budget.
 *
 * This stage validates each chunk independently but does not yet validate exchange ordering.
 */
function decodeSyncExchangeChunks(chunks: readonly Uint8Array[]): readonly SyncMessage[] {
  let framedBytes = 0;

  return chunks.map((bytes: Uint8Array): SyncMessage => {
    framedBytes += 4 + bytes.length;
    if (framedBytes > MAX_SYNC_EXCHANGE_BYTES) {
      fail(`sync exchange exceeds ${MAX_SYNC_EXCHANGE_BYTES} framed bytes`);
    }

    return decodeSyncMessage(bytes);
  });
}

/** Appends one chunk while enforcing exchange-wide member uniqueness and capacity. */
function appendCanonicalChunkUpdates(
  chunk: SyncMessage,
  names: Set<string>,
  updates: MembershipUpdate[],
): void {
  for (const update of chunk.updates) {
    addDistinctMember(names, update.member);
    updates.push(update);
    if (updates.length > MAX_MEMBERS) {
      fail(SYNC_TABLE_MESSAGE);
    }
  }
}

/** Validates shared exchange fields, exact position, and legal empty-chunk placement. */
function validateCanonicalChunk(
  chunk: SyncMessage,
  first: SyncMessage,
  index: number,
  totalChunks: number,
): void {
  if (
    chunk.type !== first.type ||
    chunk.exchangeId !== first.exchangeId ||
    chunk.chunkCount !== first.chunkCount ||
    chunk.chunkIndex !== index
  ) {
    fail("sync chunks are reordered or mismatched");
  }

  if (chunk.updates.length === 0 && !(totalChunks === 1 && index === 0)) {
    fail("only an empty sync table may have an empty chunk");
  }
}

/** Ensures the current chunk could not have accepted the next chunk's first record. */
function validateLargestPrefix(currentBytes: Uint8Array, next: SyncMessage | undefined): void {
  const nextUpdate = next?.updates[0];
  if (
    nextUpdate !== undefined &&
    currentBytes.length + membershipUpdateSize(nextUpdate) <= MAX_SYNC_MESSAGE_BYTES
  ) {
    fail("sync chunk is not the largest prefix that fits");
  }
}

/**
 * Validates complete exchange ordering and canonical chunk boundaries, then joins updates.
 *
 * Returned updates are the decoded objects owned by `decoded`; metadata is already detached
 * from the original encoded chunks.
 */
function validateCanonicalSyncChunks(
  chunks: readonly Uint8Array[],
  decoded: readonly SyncMessage[],
): readonly MembershipUpdate[] {
  const first = decoded[0] as SyncMessage;
  if (first.chunkCount !== decoded.length) {
    fail("sync chunk count is incomplete");
  }

  const updates: MembershipUpdate[] = [];
  const names = new Set<string>();

  for (let index = 0; index < decoded.length; index += 1) {
    const chunk = decoded[index] as SyncMessage;
    validateCanonicalChunk(chunk, first, index, decoded.length);
    appendCanonicalChunkUpdates(chunk, names, updates);
    validateLargestPrefix(chunks[index] as Uint8Array, decoded[index + 1]);
  }

  return updates;
}

/**
 * Validates and joins one complete canonical sync exchange direction atomically.
 *
 * The input must contain every chunk in zero-based order, use largest-prefix packing, stay
 * within the 1 MiB framed budget, and name each member at most once. No partial result escapes.
 *
 * @throws {ProtocolError} If the exchange is empty, incomplete, mismatched, or noncanonical.
 * @internal
 */
export function decodeSyncChunks(chunks: readonly Uint8Array[]): DecodedSyncExchange {
  if (chunks.length === 0) {
    fail("sync exchange has no chunks");
  }

  const decoded = decodeSyncExchangeChunks(chunks);
  const first = decoded[0] as SyncMessage;
  const updates = validateCanonicalSyncChunks(chunks, decoded);

  return { type: first.type, exchangeId: first.exchangeId, updates };
}
