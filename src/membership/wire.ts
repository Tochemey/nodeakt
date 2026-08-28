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
export const PROTOCOL_VERSION: number = 1;

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
export const MESSAGE_HEADER_BYTES: number = 4;

/** Byte width of a membership record before its variable UTF-8 and metadata fields. @internal */
export const UPDATE_HEADER_BYTES: number = 20;

/** Byte width of a sync body header before its membership records. @internal */
export const SYNC_HEADER_BYTES: number = 14;

/** Maximum complete packet size, including the common envelope, in bytes. @internal */
export const MAX_PACKET_BYTES: number = 1_400;

/** Maximum complete sync chunk accepted inside one stream frame, in bytes. @internal */
export const MAX_SYNC_MESSAGE_BYTES: number = 65_539;

/** Maximum aggregate sync exchange size, including each 4-byte stream prefix, in bytes. @internal */
export const MAX_SYNC_EXCHANGE_BYTES: number = 1_048_576;

/** Maximum distinct advertised member identities in one complete sync table. @internal */
export const MAX_MEMBERS: number = 1_024;

/** Maximum encoded UTF-8 bytes in any protocol identity/address field. @internal */
export const MAX_NAME_BYTES: number = 255;

/** Maximum membership records in one counted packet update list. @internal */
export const UPDATE_LIST_MAX_RECORDS: number = 255;

/** Maximum opaque metadata payload on an alive membership record, in bytes. @internal */
export const MAX_METADATA_BYTES: number = 512;

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

/**
 * Compares two byte sequences by value without assuming shared backing storage.
 *
 * @internal
 */
export function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) {
    return false;
  }

  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }

  return true;
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
const encoder: TextEncoder = new TextEncoder();

/** Shared fatal UTF-8 decoder; malformed byte sequences are protocol errors. */
const decoder: TextDecoder = new TextDecoder("utf-8", { fatal: true });

/** Shared zero-length metadata value returned where no mutable bytes exist. */
const EMPTY_BYTES: Uint8Array = new Uint8Array(0);

/** Raises a typed protocol error and marks the calling path as non-returning. */
function fail(message: string): never {
  throw new ProtocolError(message);
}

/** Shared encode/decode failure text for an unrecognized membership state byte. */
const UNKNOWN_STATE_MESSAGE: string = "unknown membership state";

/** Shared encode/decode failure text for metadata beyond its unsigned 16-bit budget. */
const METADATA_LENGTH_MESSAGE: string = "metadata length is out of range";

/** Shared encode/decode failure text for a reporter violating its state rule. */
const REPORTER_STATE_MESSAGE: string = "reporter is inconsistent with state";

/** Shared encode/decode failure text for metadata on a non-alive record. */
const METADATA_STATE_MESSAGE: string = "metadata is only legal on alive updates";

/** Shared encode/decode failure text for a provenance flag violating its state rule. */
const SELF_ORIGINATED_MESSAGE: string = "self-originated flag is inconsistent with state";

/** Shared encode/decode failure text for an illegal sync chunk index/count pair. */
const SYNC_CHUNK_POSITION_MESSAGE: string = "sync chunk position is invalid";

/** Shared encode/decode failure text for one chunk naming too many members. */
const SYNC_MEMBER_COUNT_MESSAGE: string = `sync member count exceeds ${MAX_MEMBERS}`;

/** Shared encode/decode failure text for a complete table naming too many members. */
const SYNC_TABLE_MESSAGE: string = `sync table exceeds ${MAX_MEMBERS} members`;

/** Shared encode/decode failure text for a complete packet beyond its byte cap. */
const PACKET_SIZE_MESSAGE: string = `packet message exceeds ${MAX_PACKET_BYTES} bytes`;

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
function assertU64(value: bigint, field: string, nonzero: boolean = false): void {
  if (value < 0n || value > 0xffffffffffffffffn || (nonzero && value === 0n)) {
    fail(`${field} is out of range`);
  }
}

/**
 * Encodes and validates a protocol identity/address.
 *
 * Empty values are accepted only for explicitly optional fields. NUL, malformed Unicode,
 * and values exceeding the one-byte UTF-8 length field raise `ProtocolError`. This is
 * the single home of the identity rules; the transport handshake reuses it so a name
 * cannot pass one layer and fail the other.
 *
 * @internal
 */
export function encodeName(name: string, field: string, allowEmpty: boolean = false): Uint8Array {
  if (name.includes("\0")) {
    fail(`${field} contains NUL`);
  }

  const bytes: Uint8Array = encoder.encode(name);
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
 * `ProtocolError`. Shared with the transport handshake as the single identity decoder.
 *
 * @internal
 */
export function decodeName(
  bytes: Uint8Array,
  offset: number,
  length: number,
  field: string,
  allowEmpty: boolean = false,
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
  const member: Uint8Array = encodeName(update.member, "member");
  const reporter: Uint8Array = encodeName(update.reporter, "reporter", true);
  if (update.metadata.length > MAX_METADATA_BYTES) {
    fail(METADATA_LENGTH_MESSAGE);
  }

  if (update.state === STATE_SUSPECT ? reporter.length === 0 : reporter.length !== 0) {
    fail(REPORTER_STATE_MESSAGE);
  }

  if (update.state === STATE_ALIVE ? false : update.metadata.length !== 0) {
    fail(METADATA_STATE_MESSAGE);
  }

  const selfRequired: boolean = update.state === STATE_ALIVE || update.state === STATE_LEFT;
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
  const update: MembershipUpdate = prepared.update;
  view.setUint16(offset, prepared.length - 2);
  view.setUint8(offset + 2, update.state);
  view.setUint8(offset + 3, update.selfOriginated ? 1 : 0);
  view.setUint32(offset + 4, update.incarnation);
  view.setBigUint64(offset + 8, update.stateChangeTime);
  view.setUint8(offset + 16, prepared.member.length);
  view.setUint8(offset + 17, prepared.reporter.length);
  view.setUint16(offset + 18, update.metadata.length);
  let position: number = offset + UPDATE_HEADER_BYTES;
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
  const prepared: PreparedUpdate = prepareUpdate(update);
  const bytes: Uint8Array = new Uint8Array(prepared.length);
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

  const recordLength: number = view.getUint16(offset);
  const totalLength: number = recordLength + 2;
  if (totalLength < UPDATE_HEADER_BYTES || totalLength > limit - offset) {
    fail("membership record length is inconsistent");
  }

  const end: number = offset + totalLength;
  const state: number = view.getUint8(offset + 2);
  const flags: number = view.getUint8(offset + 3);
  const memberLength: number = view.getUint8(offset + 16);
  const reporterLength: number = view.getUint8(offset + 17);
  const metadataLength: number = view.getUint16(offset + 18);
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

  const expected: number = UPDATE_HEADER_BYTES + memberLength + reporterLength + metadataLength;
  if (totalLength !== expected) {
    fail("membership record fields do not match its length");
  }

  const selfOriginated: boolean = flags === 1;
  const selfRequired: boolean = state === STATE_ALIVE || state === STATE_LEFT;

  if (selfOriginated !== selfRequired) {
    fail(SELF_ORIGINATED_MESSAGE);
  }

  if (state === STATE_SUSPECT ? reporterLength === 0 : reporterLength !== 0) {
    fail(REPORTER_STATE_MESSAGE);
  }

  if (state !== STATE_ALIVE && metadataLength !== 0) {
    fail(METADATA_STATE_MESSAGE);
  }

  const memberOffset: number = offset + UPDATE_HEADER_BYTES;
  const reporterOffset: number = memberOffset + memberLength;
  const metadataOffset: number = reporterOffset + reporterLength;
  const member: string = decodeName(bytes, memberOffset, memberLength, "member");
  const reporter: string = decodeName(bytes, reporterOffset, reporterLength, "reporter", true);
  const metadata: Uint8Array =
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
  const view: DataView = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const decoded: DecodedUpdate = decodeMembershipUpdateAt(bytes, view, 0, bytes.length);
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

  let baseLength: number = message.type === MESSAGE_GOSSIP ? 1 : 5;
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
  const base: PreparedPacketNames = preparePacketNames(message);
  if (message.updates.length > UPDATE_LIST_MAX_RECORDS) {
    fail(`packet update count exceeds ${UPDATE_LIST_MAX_RECORDS}`);
  }

  const updates: PreparedUpdate[] = [];
  let bodyLength: number = base.baseLength;
  for (const update of message.updates) {
    const prepared: PreparedUpdate = prepareUpdate(update);
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
  let offset: number = MESSAGE_HEADER_BYTES;
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

  const names: Set<string> = new Set<string>();
  const updates: PreparedUpdate[] = [];
  let bodyLength: number = SYNC_HEADER_BYTES;
  for (const update of message.updates) {
    addDistinctMember(names, update.member);
    const prepared: PreparedUpdate = prepareUpdate(update);
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

  const totalLength: number = MESSAGE_HEADER_BYTES + bodyLength;
  if (packet && totalLength > MAX_PACKET_BYTES) {
    fail(PACKET_SIZE_MESSAGE);
  }

  const bytes: Uint8Array = new Uint8Array(totalLength);
  const view: DataView = new DataView(bytes.buffer);
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
    const body: PreparedSyncBody = prepareSyncBody(message);
    const { bytes, view } = allocateMessage(message.type, body.bodyLength, false);
    view.setBigUint64(4, message.exchangeId);
    view.setUint16(12, message.chunkIndex);
    view.setUint16(14, message.chunkCount);
    view.setUint16(16, body.updates.length);
    let offset: number = SYNC_HEADER_BYTES + MESSAGE_HEADER_BYTES;
    for (const update of body.updates) {
      offset = writePreparedUpdate(bytes, view, offset, update);
    }

    return bytes;
  }

  const body: PreparedPacketBody = preparePacketBody(message);
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
  empty: boolean = false,
): {
  /** Decoded protocol identity/address. */
  readonly value: string;

  /** Exclusive byte offset immediately after the encoded name. */
  readonly end: number;
} {
  if (offset >= limit) {
    fail(`${field} length is truncated`);
  }

  const length: number = bytes[offset] as number;
  const end: number = offset + 1 + length;
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
    const decoded: DecodedUpdate = decodeMembershipUpdateAt(bytes, view, offset, limit);
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

  const sequence: number = view.getUint32(bodyOffset);
  const owner: ReturnType<typeof readName> = readName(bytes, bodyOffset + 4, limit, "probe owner");

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
  return {
    type: MESSAGE_GOSSIP,
    updates: decodeCountedPacketUpdates(bytes, view, bodyOffset, limit),
  };
}

/** Decodes a direct or relayed ping body and its complete piggyback list. */
function decodePingMessage(
  bytes: Uint8Array,
  view: DataView,
  bodyOffset: number,
  limit: number,
): PingMessage {
  const probe: DecodedProbeHeader = decodeProbeHeader(bytes, view, bodyOffset, limit);
  const relay: ReturnType<typeof readName> = readName(bytes, probe.end, limit, "relay", true);

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
  const probe: DecodedProbeHeader = decodeProbeHeader(bytes, view, bodyOffset, limit);
  const target: ReturnType<typeof readName> = readName(bytes, probe.end, limit, "target");

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
  const probe: DecodedProbeHeader = decodeProbeHeader(bytes, view, bodyOffset, limit);
  const target: ReturnType<typeof readName> = readName(bytes, probe.end, limit, "target");
  const helper: ReturnType<typeof readName> = readName(bytes, target.end, limit, "helper");

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

  const version: number = view.getUint8(0);
  if (version !== PROTOCOL_VERSION) {
    fail(`unsupported protocol version ${version}`);
  }

  const type: number = view.getUint8(1);
  const bodyLength: number = view.getUint16(2);
  if (bodyLength + MESSAGE_HEADER_BYTES !== bytes.length) {
    fail("message body length is inconsistent");
  }

  return { type, bodyOffset: MESSAGE_HEADER_BYTES, limit: bytes.length };
}

/**
 * Validates the shared message envelope and the type's legality for a
 * connection role without decoding the body.
 *
 * This is the single home of the role-legality rule: the TCP carrier consults
 * it for every observed frame, and the full decoders in this module enforce
 * the same partition when a body is decoded. Adding a message type therefore
 * requires no carrier change.
 *
 * @throws {ProtocolError} For a truncated envelope, unsupported version,
 * inconsistent body length, or a message type illegal for `role`.
 * @internal
 */
export function assertEnvelopeForRole(
  bytes: Uint8Array,
  role: typeof ROLE_PACKET | typeof ROLE_STREAM,
): void {
  const view: DataView = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const envelope: ReturnType<typeof decodeEnvelope> = decodeEnvelope(bytes, view);
  const legalForPacket: boolean =
    envelope.type === MESSAGE_PING ||
    envelope.type === MESSAGE_PING_REQ ||
    envelope.type === MESSAGE_ACK ||
    envelope.type === MESSAGE_NACK ||
    envelope.type === MESSAGE_GOSSIP;
  const legalForStream: boolean =
    envelope.type === MESSAGE_SYNC_REQUEST || envelope.type === MESSAGE_SYNC_RESPONSE;
  if (role === ROLE_PACKET ? !legalForPacket : !legalForStream) {
    fail("message type is illegal for connection role");
  }
}

/**
 * Incremental splitter for the u32-length-prefixed synchronization stream
 * framing. This is the single owner of that format: the TCP carrier validates
 * inbound and outbound stream bytes through it, and synchronization protocol
 * code reassembles messages from the frames it yields, so the framing rule
 * cannot drift between two parsers.
 *
 * Each input byte is copied exactly once, into the frame under construction,
 * regardless of how the carrier splits or coalesces chunks.
 *
 * @internal
 */
export class SyncFrameAssembler {
  /** Four-byte big-endian frame-length prefix under construction. */
  readonly #prefix: Uint8Array = new Uint8Array(4);

  /** Number of prefix bytes currently accumulated. */
  #prefixBytes: number = 0;

  /** Frame buffer under construction, absent between frames. */
  #frame: Uint8Array | undefined;

  /** Number of bytes copied into the current frame. */
  #frameBytes: number = 0;

  /**
   * Whether all pushed bytes end exactly on a frame boundary.
   *
   * @internal `true` also describes the initial state; it does not mean any
   * frame has been observed.
   */
  get complete(): boolean {
    return this.#prefixBytes === 0 && this.#frame === undefined;
  }

  /** Whether the next expected byte belongs to a length prefix, not a frame body. */
  get awaitingPrefix(): boolean {
    return this.#frame === undefined;
  }

  /**
   * Incorporates carrier bytes and returns the frames they completed, in order.
   *
   * One call may complete zero, one, or many frames. Returned frames are owned
   * by the caller and never reused by the assembler.
   *
   * @throws {ProtocolError} For an out-of-range frame length; state after an
   * exception must be treated as unusable.
   */
  push(bytes: Uint8Array): readonly Uint8Array[] {
    const frames: Uint8Array[] = [];
    let offset: number = 0;
    while (offset < bytes.length) {
      if (this.#frame === undefined) {
        const count: number = Math.min(4 - this.#prefixBytes, bytes.length - offset);
        this.#prefix.set(bytes.subarray(offset, offset + count), this.#prefixBytes);
        this.#prefixBytes += count;
        offset += count;
        if (this.#prefixBytes < 4) {
          return frames;
        }

        const length: number = new DataView(this.#prefix.buffer).getUint32(0);
        if (length < 4 || length > MAX_SYNC_MESSAGE_BYTES) {
          fail("sync frame length is out of range");
        }

        this.#frame = new Uint8Array(length);
        this.#frameBytes = 0;
      }

      const frame: Uint8Array = this.#frame;
      const count: number = Math.min(frame.length - this.#frameBytes, bytes.length - offset);
      frame.set(bytes.subarray(offset, offset + count), this.#frameBytes);
      this.#frameBytes += count;
      offset += count;
      if (this.#frameBytes === frame.length) {
        frames.push(frame);
        this.#prefixBytes = 0;
        this.#frame = undefined;
        this.#frameBytes = 0;
      }
    }

    return frames;
  }
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

  const exchangeId: bigint = view.getBigUint64(offset);
  const chunkIndex: number = view.getUint16(offset + 8);
  const chunkCount: number = view.getUint16(offset + 10);
  const memberCount: number = view.getUint16(offset + 12);
  if (exchangeId === 0n) {
    fail("exchange ID is zero");
  }

  if (chunkCount === 0 || chunkIndex >= chunkCount) {
    fail(SYNC_CHUNK_POSITION_MESSAGE);
  }

  if (memberCount > MAX_MEMBERS) {
    fail(SYNC_MEMBER_COUNT_MESSAGE);
  }

  const updates: readonly MembershipUpdate[] = decodeUpdateList(
    bytes,
    view,
    offset + SYNC_HEADER_BYTES,
    bytes.length,
    memberCount,
  );
  const names: Set<string> = new Set<string>();
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
  const view: DataView = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const envelope: ReturnType<typeof decodeEnvelope> = decodeEnvelope(bytes, view);
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
  const message: MembershipMessage = decodeMessage(bytes);
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
  const message: MembershipMessage = decodeMessage(bytes);
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

  const names: Set<string> = new Set<string>();
  const sizes: number[] = [];
  for (const update of updates) {
    addDistinctMember(names, update.member);
    sizes.push(membershipUpdateSize(update));
  }

  const chunks: MembershipUpdate[][] = [];
  let current: MembershipUpdate[] = [];
  let currentSize: number = MESSAGE_HEADER_BYTES + SYNC_HEADER_BYTES;
  for (let index = 0; index < updates.length; index += 1) {
    const size: number = sizes[index] as number;
    if (current.length > 0 && currentSize + size > MAX_SYNC_MESSAGE_BYTES) {
      chunks.push(current);
      current = [];
      currentSize = MESSAGE_HEADER_BYTES + SYNC_HEADER_BYTES;
    }

    current.push(updates[index] as MembershipUpdate);
    currentSize += size;
  }

  chunks.push(current);
  const chunkCount: number = chunks.length;
  const encoded: readonly Uint8Array[] = chunks.map(
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
/**
 * Enforces the exchange-wide framed byte budget over one direction's raw chunks.
 *
 * @throws {ProtocolError} When the cumulative framed size exceeds the budget.
 * @internal
 */
export function assertSyncExchangeBudget(chunks: readonly Uint8Array[]): void {
  let framedBytes: number = 0;
  for (const bytes of chunks) {
    framedBytes += 4 + bytes.length;
    if (framedBytes > MAX_SYNC_EXCHANGE_BYTES) {
      fail(`sync exchange exceeds ${MAX_SYNC_EXCHANGE_BYTES} framed bytes`);
    }
  }
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
  const nextUpdate: MembershipUpdate | undefined = next?.updates[0];
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
  const first: SyncMessage = decoded[0] as SyncMessage;
  if (first.chunkCount !== decoded.length) {
    fail("sync chunk count is incomplete");
  }

  const updates: MembershipUpdate[] = [];
  const names: Set<string> = new Set<string>();

  for (let index = 0; index < decoded.length; index += 1) {
    const chunk: SyncMessage = decoded[index] as SyncMessage;
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

  assertSyncExchangeBudget(chunks);
  return combineSyncChunks(
    chunks,
    chunks.map((bytes: Uint8Array): SyncMessage => decodeSyncMessage(bytes)),
  );
}

/**
 * Validates and joins one exchange direction from already-decoded chunks and
 * their raw frames, so a reader that decoded each chunk on arrival does not
 * decode the exchange a second time.
 *
 * The inputs must be position-aligned: `decoded[i]` is the decoding of
 * `chunks[i]`. Canonical ordering, chunk-count consistency, cross-chunk member
 * uniqueness, and largest-prefix packing are enforced exactly as in
 * {@link decodeSyncChunks}; the caller owns the framed byte budget.
 *
 * @throws {ProtocolError} If the exchange is empty, incomplete, mismatched, or noncanonical.
 * @internal
 */
export function combineSyncChunks(
  chunks: readonly Uint8Array[],
  decoded: readonly SyncMessage[],
): DecodedSyncExchange {
  if (decoded.length === 0 || chunks.length !== decoded.length) {
    fail("sync exchange has no chunks");
  }

  const first: SyncMessage = decoded[0] as SyncMessage;
  const updates: readonly MembershipUpdate[] = validateCanonicalSyncChunks(chunks, decoded);

  return { type: first.type, exchangeId: first.exchangeId, updates };
}
