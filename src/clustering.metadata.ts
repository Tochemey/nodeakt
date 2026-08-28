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

import { MAX_METADATA_BYTES } from "./membership/wire";

/**
 * The fixed record the clustering layer carries in each node's membership
 * metadata, the one thing the opaque metadata blob means to this layer.
 *
 * A cluster node runs two endpoints: the membership engine gossips on its own
 * address, and the key/value store answers on a separate `src/net` data
 * endpoint. Membership identities and data endpoints are therefore different
 * addresses, so a node advertises its data endpoint here, in the metadata that
 * membership gossips for it. Every node then learns every other node's data
 * endpoint through ordinary gossip, and the store can address a peer by an
 * identity it can dial. This is the same pattern a service registry uses when a
 * gossip member carries its service port as a tag.
 *
 * `startedAt` and `address` are written once at boot and never change, so the
 * decoded `address` is the stable identity the store keys on and `startedAt` is
 * the stable order the coordinator relies on. `ready` and `draining` flip later
 * through `Swim.updateMetadata`, which re-announces at a higher incarnation with
 * the same `startedAt` and `address`.
 */

/** The decoded clustering fields carried in a member's opaque metadata. @internal */
export interface NodeMetadata {
  /** Immutable process start time in epoch milliseconds; decides the coordinator. */
  readonly startedAt: number;
  /** Whether the node has completed its initial fragment intake. */
  readonly ready: boolean;
  /** Whether the node has announced a graceful leave and is draining fragments. */
  readonly draining: boolean;
  /** The node's key/value data endpoint as `host:port`, the identity the store dials. */
  readonly address: string;
}

/** Byte offset of the `u64` `startedAt`. */
const STARTED_AT_OFFSET: number = 0;

/** Byte offset of the `ready` flag. */
const READY_OFFSET: number = 8;

/** Byte offset of the `draining` flag. */
const DRAINING_OFFSET: number = 9;

/** Byte offset of the `u16` address length. */
const ADDRESS_LENGTH_OFFSET: number = 10;

/** Byte offset where the UTF-8 address bytes begin; also the fixed-prefix length. */
const ADDRESS_OFFSET: number = 12;

/** Largest data address the store record admits; the data and remoting fields together must still fit {@link MAX_METADATA_BYTES}, which {@link appendRemotingAddress} enforces. */
const MAX_ADDRESS_BYTES: number = 255;

/** Byte width of the `u16` length prefix on the trailing remoting-address field. */
const REMOTING_LENGTH_SIZE: number = 2;

/** Shared UTF-8 codecs; the decoder tolerates malformed bytes rather than throwing. */
const UTF8_ENCODER: TextEncoder = new TextEncoder();
const UTF8_DECODER: TextDecoder = new TextDecoder();

/** The value an undecodable record maps to: youngest, not ready, and unaddressable. */
const UNDECODABLE: NodeMetadata = {
  startedAt: Number.MAX_SAFE_INTEGER,
  ready: false,
  draining: false,
  address: "",
};

/**
 * Encodes a node's clustering fields into the fixed-prefix metadata record.
 *
 * @throws {RangeError} If `startedAt` is not a non-negative safe integer, or the
 * address exceeds its byte budget.
 * @internal
 */
export function encodeNodeMetadata(metadata: NodeMetadata): Uint8Array {
  if (!Number.isSafeInteger(metadata.startedAt) || metadata.startedAt < 0) {
    throw new RangeError("node metadata startedAt must be a non-negative integer of milliseconds");
  }

  const addressBytes: Uint8Array = UTF8_ENCODER.encode(metadata.address);
  if (addressBytes.length > MAX_ADDRESS_BYTES) {
    throw new RangeError(`node metadata address cannot exceed ${MAX_ADDRESS_BYTES} bytes`);
  }

  const bytes: Uint8Array = new Uint8Array(ADDRESS_OFFSET + addressBytes.length);
  const view: DataView = new DataView(bytes.buffer);
  view.setBigUint64(STARTED_AT_OFFSET, BigInt(metadata.startedAt));
  bytes[READY_OFFSET] = metadata.ready ? 1 : 0;
  bytes[DRAINING_OFFSET] = metadata.draining ? 1 : 0;
  view.setUint16(ADDRESS_LENGTH_OFFSET, addressBytes.length);
  bytes.set(addressBytes, ADDRESS_OFFSET);
  return bytes;
}

/**
 * Decodes a member's metadata into its clustering fields.
 *
 * Metadata too short to be this record, or whose declared address runs past the
 * bytes, which a homogeneous cluster never produces, decodes as the youngest
 * possible unaddressable member, so an undecodable peer can never be mistaken
 * for the oldest and made coordinator, and is dropped by the view adapter for
 * having no data endpoint.
 *
 * @internal
 */
export function decodeNodeMetadata(bytes: Uint8Array): NodeMetadata {
  if (bytes.length < ADDRESS_OFFSET) {
    return UNDECODABLE;
  }

  const view: DataView = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const addressLength: number = view.getUint16(ADDRESS_LENGTH_OFFSET);
  if (bytes.length < ADDRESS_OFFSET + addressLength) {
    return UNDECODABLE;
  }

  return {
    startedAt: Number(view.getBigUint64(STARTED_AT_OFFSET)),
    ready: bytes[READY_OFFSET] !== 0,
    draining: bytes[DRAINING_OFFSET] !== 0,
    address: UTF8_DECODER.decode(bytes.subarray(ADDRESS_OFFSET, ADDRESS_OFFSET + addressLength)),
  };
}

/**
 * Appends an actor remoting endpoint after the store's fixed record, as a second
 * length-prefixed field.
 *
 * The remoting endpoint is where a node's actors are reached, a different address
 * from the data endpoint the store keys on, so a clustered actor node advertises
 * it here alongside the store's own fields. {@link decodeNodeMetadata} reads only
 * up to the data address and ignores whatever follows, so a node that carries
 * this field stays readable by the store, which never learns of it. An empty
 * endpoint appends nothing, so a node with no remoting endpoint produces exactly
 * the bytes the store record alone would.
 *
 * @throws {RangeError} If the record with the remoting endpoint appended would
 * exceed the membership metadata budget.
 * @internal
 */
export function appendRemotingAddress(metadata: Uint8Array, remotingAddress: string): Uint8Array {
  const addressBytes: Uint8Array = UTF8_ENCODER.encode(remotingAddress);
  if (addressBytes.length === 0) {
    return metadata;
  }

  const total: number = metadata.length + REMOTING_LENGTH_SIZE + addressBytes.length;
  if (total > MAX_METADATA_BYTES) {
    throw new RangeError(
      `node metadata with a remoting address cannot exceed ${MAX_METADATA_BYTES} bytes`,
    );
  }

  const bytes: Uint8Array = new Uint8Array(total);
  bytes.set(metadata, 0);
  const view: DataView = new DataView(bytes.buffer);
  view.setUint16(metadata.length, addressBytes.length);
  bytes.set(addressBytes, metadata.length + REMOTING_LENGTH_SIZE);
  return bytes;
}

/**
 * Reads the trailing remoting endpoint {@link appendRemotingAddress} wrote, or the
 * empty string when the record carries none.
 *
 * It skips past the store's fixed record and its data address to the trailing
 * field, and tolerates a short or absent field the way {@link decodeNodeMetadata}
 * tolerates a short record, so a peer that never advertised a remoting endpoint,
 * or one whose record predates this field, reads as having none rather than
 * throwing.
 *
 * @internal
 */
export function readRemotingAddress(metadata: Uint8Array): string {
  if (metadata.length < ADDRESS_OFFSET) {
    return "";
  }

  const view: DataView = new DataView(metadata.buffer, metadata.byteOffset, metadata.byteLength);
  const dataAddressLength: number = view.getUint16(ADDRESS_LENGTH_OFFSET);
  const fieldOffset: number = ADDRESS_OFFSET + dataAddressLength;
  if (metadata.length < fieldOffset + REMOTING_LENGTH_SIZE) {
    return "";
  }

  const addressLength: number = view.getUint16(fieldOffset);
  const addressStart: number = fieldOffset + REMOTING_LENGTH_SIZE;
  if (metadata.length < addressStart + addressLength) {
    return "";
  }

  return UTF8_DECODER.decode(metadata.subarray(addressStart, addressStart + addressLength));
}
