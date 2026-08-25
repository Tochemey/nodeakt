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
  decodeMembershipUpdate,
  decodeMessage,
  decodePacketMessage,
  decodeSyncChunks,
  decodeSyncMessage,
  encodeMembershipUpdate,
  encodeMessage,
  encodeSyncChunks,
  MAX_MEMBERS,
  MAX_METADATA_BYTES,
  MAX_NAME_BYTES,
  MAX_PACKET_BYTES,
  MAX_SYNC_EXCHANGE_BYTES,
  MAX_SYNC_MESSAGE_BYTES,
  MESSAGE_ACK,
  MESSAGE_GOSSIP,
  MESSAGE_NACK,
  MESSAGE_PING,
  MESSAGE_PING_REQ,
  MESSAGE_SYNC_REQUEST,
  MESSAGE_SYNC_RESPONSE,
  type MembershipUpdate,
  membershipUpdateSize,
  ProtocolError,
  STATE_ALIVE,
  STATE_DEAD,
  STATE_LEFT,
  STATE_SUSPECT,
} from "../../src/membership/wire";

const empty = new Uint8Array(0);

function alive(member = "a", metadata: Uint8Array = empty): MembershipUpdate {
  return {
    state: STATE_ALIVE,
    selfOriginated: true,
    incarnation: 0x01020304,
    stateChangeTime: 0x0102030405060708n,
    member,
    reporter: "",
    metadata,
  };
}

function updateFor(state: number): MembershipUpdate {
  if (state === STATE_ALIVE) {
    return alive("member", Uint8Array.of(1, 2));
  }
  return {
    state: state as MembershipUpdate["state"],
    selfOriginated: state === STATE_LEFT,
    incarnation: 7,
    stateChangeTime: 9n,
    member: "member",
    reporter: state === STATE_SUSPECT ? "reporter" : "",
    metadata: empty,
  };
}

function copy(bytes: Uint8Array, change: (result: Uint8Array) => void): Uint8Array {
  const result = Uint8Array.from(bytes);
  change(result);
  return result;
}

function expectProtocol(action: () => unknown): void {
  expect(action).toThrow(ProtocolError);
}

describe("membership update codec", () => {
  it("encodes the exact big-endian layout", () => {
    const bytes = encodeMembershipUpdate(alive("a", Uint8Array.of(0xaa)));
    expect(Array.from(bytes)).toEqual([
      0x00, 0x14, 0x00, 0x01, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
      0x08, 0x01, 0x00, 0x00, 0x01, 0x61, 0xaa,
    ]);
    expect(membershipUpdateSize(alive("a", Uint8Array.of(0xaa)))).toBe(22);
  });

  it("round-trips every state and copies metadata", () => {
    for (const state of [STATE_ALIVE, STATE_SUSPECT, STATE_DEAD, STATE_LEFT]) {
      const source = updateFor(state);
      const encoded = encodeMembershipUpdate(source);
      const decoded = decodeMembershipUpdate(encoded);
      expect(decoded).toEqual(source);
      if (state === STATE_ALIVE) {
        encoded[encoded.length - 1] = 99;
        expect(decoded.metadata).toEqual(Uint8Array.of(1, 2));
      }
    }
  });

  it("accepts scalar and field boundaries", () => {
    const member = "x".repeat(MAX_NAME_BYTES);
    const metadata = new Uint8Array(MAX_METADATA_BYTES).fill(0xff);
    const source: MembershipUpdate = {
      ...alive(member, metadata),
      incarnation: 0xffffffff,
      stateChangeTime: 0xffffffffffffffffn,
    };
    expect(decodeMembershipUpdate(encodeMembershipUpdate(source))).toEqual(source);
  });

  it("rejects invalid encode inputs", () => {
    const cases: MembershipUpdate[] = [
      { ...alive(), state: 9 as MembershipUpdate["state"] },
      { ...alive(), member: "" },
      { ...alive(), member: "x".repeat(MAX_NAME_BYTES + 1) },
      { ...alive(), member: "a\0b" },
      { ...alive(), member: "\ud800" },
      { ...alive(), incarnation: -1 },
      { ...alive(), incarnation: 0x1_0000_0000 },
      { ...alive(), stateChangeTime: -1n },
      { ...alive(), stateChangeTime: 0x1_0000_0000_0000_0000n },
      { ...alive(), selfOriginated: false },
      { ...updateFor(STATE_LEFT), selfOriginated: false },
      { ...updateFor(STATE_DEAD), selfOriginated: true },
      { ...updateFor(STATE_SUSPECT), selfOriginated: true },
      { ...updateFor(STATE_SUSPECT), reporter: "" },
      { ...updateFor(STATE_DEAD), reporter: "x" },
      { ...updateFor(STATE_LEFT), reporter: "x" },
      { ...updateFor(STATE_SUSPECT), metadata: Uint8Array.of(1) },
      { ...alive(), metadata: new Uint8Array(MAX_METADATA_BYTES + 1) },
    ];
    for (const value of cases) {
      expectProtocol(() => encodeMembershipUpdate(value));
    }
  });

  it("rejects malformed fixed fields before variable fields", () => {
    const valid = encodeMembershipUpdate(alive("a", Uint8Array.of(1)));
    const malformed = [
      valid.subarray(0, 19),
      copy(valid, (bytes) => {
        bytes[0] = 0;
        bytes[1] = 19;
      }),
      copy(valid, (bytes) => {
        bytes[0] = 0xff;
        bytes[1] = 0xff;
      }),
      copy(valid, (bytes) => {
        bytes[2] = 4;
      }),
      copy(valid, (bytes) => {
        bytes[3] = 0x80;
      }),
      copy(valid, (bytes) => {
        bytes[3] = 0;
      }),
      copy(valid, (bytes) => {
        bytes[16] = 0;
      }),
      copy(valid, (bytes) => {
        bytes[17] = 1;
      }),
      copy(valid, (bytes) => {
        bytes[18] = 0x02;
        bytes[19] = 0x01;
      }),
      Uint8Array.from([...valid, 0]),
    ];
    for (const bytes of malformed) {
      expectProtocol(() => decodeMembershipUpdate(bytes));
    }
  });

  it("rejects invalid UTF-8 and NUL names", () => {
    const valid = encodeMembershipUpdate(alive("a"));
    expectProtocol(() =>
      decodeMembershipUpdate(
        copy(valid, (bytes) => {
          bytes[20] = 0xff;
        }),
      ),
    );
    expectProtocol(() =>
      decodeMembershipUpdate(
        copy(valid, (bytes) => {
          bytes[20] = 0;
        }),
      ),
    );
  });

  it("rejects state-specific reporter, metadata, and provenance on decode", () => {
    const aliveBytes = encodeMembershipUpdate(alive("a", Uint8Array.of(1)));
    const suspectBytes = encodeMembershipUpdate(updateFor(STATE_SUSPECT));
    const deadBytes = encodeMembershipUpdate(updateFor(STATE_DEAD));
    const leftBytes = encodeMembershipUpdate(updateFor(STATE_LEFT));
    const cases = [
      copy(aliveBytes, (bytes) => {
        bytes[2] = STATE_DEAD;
      }),
      copy(suspectBytes, (bytes) => {
        bytes[3] = 1;
      }),
      copy(deadBytes, (bytes) => {
        bytes[3] = 1;
      }),
      copy(leftBytes, (bytes) => {
        bytes[3] = 0;
      }),
    ];
    for (const bytes of cases) {
      expectProtocol(() => decodeMembershipUpdate(bytes));
    }
  });

  it("rejects missing suspect reporters and non-alive metadata on decode", () => {
    const valid = encodeMembershipUpdate(alive("a", Uint8Array.of(1)));
    expectProtocol(() =>
      decodeMembershipUpdate(
        copy(valid, (bytes) => {
          bytes[2] = STATE_SUSPECT;
          bytes[3] = 0;
        }),
      ),
    );
    expectProtocol(() =>
      decodeMembershipUpdate(
        copy(valid, (bytes) => {
          bytes[2] = STATE_DEAD;
          bytes[3] = 0;
        }),
      ),
    );
  });
});

describe("packet message codec", () => {
  const fixtures = [
    {
      message: { type: MESSAGE_PING, sequence: 1, owner: "o", relay: "", updates: [] } as const,
      bytes: [1, 1, 0, 8, 0, 0, 0, 1, 1, 0x6f, 0, 0],
    },
    {
      message: {
        type: MESSAGE_PING_REQ,
        sequence: 1,
        owner: "o",
        target: "t",
        updates: [],
      } as const,
      bytes: [1, 2, 0, 9, 0, 0, 0, 1, 1, 0x6f, 1, 0x74, 0],
    },
    {
      message: { type: MESSAGE_ACK, sequence: 1, owner: "o", target: "t", updates: [] } as const,
      bytes: [1, 3, 0, 9, 0, 0, 0, 1, 1, 0x6f, 1, 0x74, 0],
    },
    {
      message: {
        type: MESSAGE_NACK,
        sequence: 1,
        owner: "o",
        target: "t",
        helper: "h",
        updates: [],
      } as const,
      bytes: [1, 4, 0, 11, 0, 0, 0, 1, 1, 0x6f, 1, 0x74, 1, 0x68, 0],
    },
    {
      message: { type: MESSAGE_GOSSIP, updates: [] } as const,
      bytes: [1, 5, 0, 1, 0],
    },
  ];

  it("matches exact fixtures and round-trips every packet type", () => {
    for (const fixture of fixtures) {
      const encoded = encodeMessage(fixture.message);
      expect(Array.from(encoded)).toEqual(fixture.bytes);
      expect(decodePacketMessage(encoded)).toEqual(fixture.message);
    }
  });

  it("round-trips piggyback updates and relayed pings", () => {
    const update = updateFor(STATE_SUSPECT);
    const message = {
      type: MESSAGE_PING,
      sequence: 0xffffffff,
      owner: "owner:1",
      relay: "helper:2",
      updates: [update],
    } as const;
    expect(decodeMessage(encodeMessage(message))).toEqual(message);
  });

  it("enforces the packet budget and update-count byte", () => {
    const fitting = {
      type: MESSAGE_GOSSIP,
      updates: [alive("a", new Uint8Array(MAX_METADATA_BYTES))],
    } as const;
    expect(encodeMessage(fitting).length).toBeLessThanOrEqual(MAX_PACKET_BYTES);
    expectProtocol(() =>
      encodeMessage({
        type: MESSAGE_GOSSIP,
        updates: [alive("a", new Uint8Array(MAX_METADATA_BYTES)), alive("b", new Uint8Array(900))],
      }),
    );
    expectProtocol(() =>
      encodeMessage({ type: MESSAGE_GOSSIP, updates: Array.from({ length: 256 }, () => alive()) }),
    );
    expectProtocol(() =>
      encodeMessage({
        type: MESSAGE_GOSSIP,
        updates: ["a", "b", "c"].map((member) => alive(member, new Uint8Array(MAX_METADATA_BYTES))),
      }),
    );
  });

  it("rejects malformed envelopes, reserved types, lengths, and trailing bytes", () => {
    const valid = encodeMessage({ type: MESSAGE_GOSSIP, updates: [] });
    const cases = [
      valid.subarray(0, 3),
      copy(valid, (bytes) => {
        bytes[0] = 2;
      }),
      copy(valid, (bytes) => {
        bytes[1] = 0;
      }),
      copy(valid, (bytes) => {
        bytes[1] = 6;
      }),
      copy(valid, (bytes) => {
        bytes[2] = 0;
        bytes[3] = 2;
      }),
      Uint8Array.from([...valid, 0]),
    ];
    for (const bytes of cases) {
      expectProtocol(() => decodeMessage(bytes));
    }
  });

  it("rejects truncated mandatory names, empty names, counts, and invalid names", () => {
    const ping = encodeMessage({
      type: MESSAGE_PING,
      sequence: 1,
      owner: "o",
      relay: "",
      updates: [],
    });
    const cases = [
      copy(ping, (bytes) => {
        bytes[8] = 0;
      }),
      copy(ping, (bytes) => {
        bytes[8] = 10;
      }),
      copy(ping, (bytes) => {
        bytes[9] = 0xff;
      }),
      ping.subarray(0, ping.length - 1),
    ];
    for (const original of cases) {
      const bytes =
        original.length === ping.length
          ? original
          : copy(original, (value) => {
              value[2] = 0;
              value[3] = value.length - 4;
            });
      expectProtocol(() => decodeMessage(bytes));
    }
  });

  it("rejects each truncated packet body boundary and trailing update bytes", () => {
    const malformed = [
      Uint8Array.of(1, MESSAGE_PING, 0, 3, 0, 0, 0),
      Uint8Array.of(1, MESSAGE_PING, 0, 6, 0, 0, 0, 1, 1, 0x6f),
      Uint8Array.of(1, MESSAGE_GOSSIP, 0, 0),
    ];
    for (const bytes of malformed) {
      expectProtocol(() => decodeMessage(bytes));
    }

    const withUpdate = encodeMessage({
      type: MESSAGE_GOSSIP,
      updates: [alive("a")],
    });
    withUpdate[4] = 0;
    expectProtocol(() => decodeMessage(withUpdate));
  });

  it("rejects an oversized packet before parsing its body", () => {
    const bytes = new Uint8Array(MAX_PACKET_BYTES + 1);
    const view = new DataView(bytes.buffer);
    view.setUint8(0, 1);
    view.setUint8(1, MESSAGE_GOSSIP);
    view.setUint16(2, bytes.length - 4);

    expectProtocol(() => decodeMessage(bytes));
  });

  it("rejects packet messages on the stream role", () => {
    expectProtocol(() => decodeSyncMessage(encodeMessage({ type: MESSAGE_GOSSIP, updates: [] })));
  });
});

describe("sync message codec", () => {
  it("encodes an exact empty chunk fixture", () => {
    const message = {
      type: MESSAGE_SYNC_REQUEST,
      exchangeId: 0x0102030405060708n,
      chunkIndex: 0,
      chunkCount: 1,
      updates: [],
    } as const;
    const bytes = encodeMessage(message);
    expect(Array.from(bytes)).toEqual([1, 0x10, 0, 14, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 1, 0, 0]);
    expect(decodeSyncMessage(bytes)).toEqual(message);
  });

  it("round-trips requests and responses", () => {
    for (const type of [MESSAGE_SYNC_REQUEST, MESSAGE_SYNC_RESPONSE] as const) {
      const message = {
        type,
        exchangeId: 0xffffffffffffffffn,
        chunkIndex: 1,
        chunkCount: 2,
        updates: [alive("node:1")],
      };
      expect(decodeMessage(encodeMessage(message))).toEqual(message);
    }
  });

  it("rejects invalid sync scalar fields and duplicate names", () => {
    const base = {
      type: MESSAGE_SYNC_REQUEST,
      exchangeId: 1n,
      chunkIndex: 0,
      chunkCount: 1,
      updates: [alive("a")],
    } as const;
    expectProtocol(() => encodeMessage({ ...base, exchangeId: 0n }));
    expectProtocol(() => encodeMessage({ ...base, chunkCount: 0 }));
    expectProtocol(() => encodeMessage({ ...base, chunkIndex: 1 }));
    expectProtocol(() => encodeMessage({ ...base, updates: [alive("a"), alive("a")] }));

    const valid = encodeMessage(base);
    expectProtocol(() =>
      decodeSyncMessage(
        copy(valid, (bytes) => {
          bytes.fill(0, 4, 12);
        }),
      ),
    );
    expectProtocol(() =>
      decodeSyncMessage(
        copy(valid, (bytes) => {
          bytes[14] = 0;
          bytes[15] = 0;
        }),
      ),
    );
    expectProtocol(() =>
      decodeSyncMessage(
        copy(valid, (bytes) => {
          bytes[12] = 0;
          bytes[13] = 1;
        }),
      ),
    );
  });

  it("rejects oversized sync tables and envelope bodies on encode", () => {
    const tooMany = Array.from({ length: MAX_MEMBERS + 1 }, (_, index) => alive(`n${index}`));
    expectProtocol(() =>
      encodeMessage({
        type: MESSAGE_SYNC_REQUEST,
        exchangeId: 1n,
        chunkIndex: 0,
        chunkCount: 1,
        updates: tooMany,
      }),
    );

    const oversizedBody = Array.from({ length: 130 }, (_, index) =>
      alive(`large-${index}`, new Uint8Array(MAX_METADATA_BYTES)),
    );
    expectProtocol(() =>
      encodeMessage({
        type: MESSAGE_SYNC_REQUEST,
        exchangeId: 1n,
        chunkIndex: 0,
        chunkCount: 1,
        updates: oversizedBody,
      }),
    );
  });

  it("rejects truncated headers, oversized counts, and duplicate decoded members", () => {
    expectProtocol(() => decodeSyncMessage(Uint8Array.of(1, MESSAGE_SYNC_REQUEST, 0, 0)));

    const oversizedCount = new Uint8Array(18);
    const countView = new DataView(oversizedCount.buffer);
    countView.setUint8(0, 1);
    countView.setUint8(1, MESSAGE_SYNC_REQUEST);
    countView.setUint16(2, 14);
    countView.setBigUint64(4, 1n);
    countView.setUint16(14, 1);
    countView.setUint16(16, MAX_MEMBERS + 1);
    expectProtocol(() => decodeSyncMessage(oversizedCount));

    const single = encodeMessage({
      type: MESSAGE_SYNC_REQUEST,
      exchangeId: 1n,
      chunkIndex: 0,
      chunkCount: 1,
      updates: [alive("duplicate")],
    });
    const record = single.slice(18);
    const duplicate = new Uint8Array(single.length + record.length);
    duplicate.set(single);
    duplicate.set(record, single.length);
    const duplicateView = new DataView(duplicate.buffer);
    duplicateView.setUint16(2, duplicate.length - 4);
    duplicateView.setUint16(16, 2);
    expectProtocol(() => decodeSyncMessage(duplicate));
  });

  it("rejects sync messages on the packet role", () => {
    const bytes = encodeMessage({
      type: MESSAGE_SYNC_REQUEST,
      exchangeId: 1n,
      chunkIndex: 0,
      chunkCount: 1,
      updates: [],
    });
    expectProtocol(() => decodePacketMessage(bytes));
  });

  it("chunks at the largest stream-frame prefix and rejoins atomically", () => {
    const updates = Array.from({ length: 100 }, (_, index) =>
      alive(`node-${index}-${"x".repeat(220)}`, new Uint8Array(MAX_METADATA_BYTES).fill(index)),
    );
    const chunks = encodeSyncChunks(MESSAGE_SYNC_RESPONSE, 42n, updates);
    expect(chunks.length).toBeGreaterThan(1);
    for (const chunk of chunks) {
      expect(chunk.length).toBeLessThanOrEqual(MAX_SYNC_MESSAGE_BYTES);
    }
    const joined = decodeSyncChunks(chunks);
    expect(joined.type).toBe(MESSAGE_SYNC_RESPONSE);
    expect(joined.exchangeId).toBe(42n);
    expect(joined.updates).toEqual(updates);
    const first = decodeSyncMessage(chunks[0] as Uint8Array);
    const second = decodeSyncMessage(chunks[1] as Uint8Array);
    expect(
      (chunks[0] as Uint8Array).length +
        membershipUpdateSize(second.updates[0] as MembershipUpdate),
    ).toBeGreaterThan(MAX_SYNC_MESSAGE_BYTES);
    expect(first.chunkIndex).toBe(0);
    expect(first.chunkCount).toBe(chunks.length);
  });

  it("uses one empty chunk for an empty table", () => {
    const chunks = encodeSyncChunks(MESSAGE_SYNC_REQUEST, 7n, []);
    expect(chunks).toHaveLength(1);
    expect(decodeSyncChunks(chunks)).toEqual({
      type: MESSAGE_SYNC_REQUEST,
      exchangeId: 7n,
      updates: [],
    });
  });

  it("enforces the complete-table member and duplicate limits", () => {
    const updates = Array.from({ length: 1_024 }, (_, index) => alive(`n${index}`));
    expect(
      decodeSyncChunks(encodeSyncChunks(MESSAGE_SYNC_REQUEST, 1n, updates)).updates,
    ).toHaveLength(1_024);
    expectProtocol(() =>
      encodeSyncChunks(MESSAGE_SYNC_REQUEST, 1n, [...updates, alive("overflow")]),
    );
    expectProtocol(() =>
      encodeSyncChunks(MESSAGE_SYNC_REQUEST, 1n, [alive("same"), alive("same")]),
    );
  });

  it("rejects partial, reordered, mismatched, duplicated, and noncanonical chunks", () => {
    const large = Array.from({ length: 100 }, (_, index) =>
      alive(`node-${index}-${"x".repeat(220)}`, new Uint8Array(MAX_METADATA_BYTES)),
    );
    const chunks = encodeSyncChunks(MESSAGE_SYNC_REQUEST, 3n, large);
    expect(chunks.length).toBeGreaterThan(1);
    expectProtocol(() => decodeSyncChunks([]));
    expectProtocol(() => decodeSyncChunks(chunks.slice(0, -1)));
    expectProtocol(() => decodeSyncChunks([chunks[1] as Uint8Array, chunks[0] as Uint8Array]));
    expectProtocol(() => decodeSyncChunks([chunks[0] as Uint8Array, chunks[0] as Uint8Array]));

    const decoded = chunks.map((chunk) => decodeSyncMessage(chunk));
    const mismatched = [...chunks];
    mismatched[1] = encodeMessage({ ...(decoded[1] as (typeof decoded)[number]), exchangeId: 4n });
    expectProtocol(() => decodeSyncChunks(mismatched));

    const duplicate = [...chunks];
    const second = decoded[1] as (typeof decoded)[number];
    duplicate[1] = encodeMessage({
      ...second,
      updates: [large[0] as MembershipUpdate, ...second.updates.slice(1)],
    });
    expectProtocol(() => decodeSyncChunks(duplicate));

    const noncanonical = [
      encodeMessage({
        type: MESSAGE_SYNC_REQUEST,
        exchangeId: 9n,
        chunkIndex: 0,
        chunkCount: 2,
        updates: [alive("a")],
      }),
      encodeMessage({
        type: MESSAGE_SYNC_REQUEST,
        exchangeId: 9n,
        chunkIndex: 1,
        chunkCount: 2,
        updates: [alive("b")],
      }),
    ];
    expectProtocol(() => decodeSyncChunks(noncanonical));
  });

  it("rejects invalid chunk types and over-budget framed exchanges", () => {
    expectProtocol(() => encodeSyncChunks(MESSAGE_GOSSIP as typeof MESSAGE_SYNC_REQUEST, 1n, []));

    const chunk = encodeMessage({
      type: MESSAGE_SYNC_REQUEST,
      exchangeId: 1n,
      chunkIndex: 0,
      chunkCount: 1,
      updates: [],
    });
    const count = Math.floor(MAX_SYNC_EXCHANGE_BYTES / (chunk.length + 4)) + 1;
    expectProtocol(() => decodeSyncChunks(Array.from({ length: count }, () => chunk)));
  });

  it("rejects multi-chunk empty records and aggregate tables over 1024 members", () => {
    const emptyChunks = [0, 1].map((chunkIndex) =>
      encodeMessage({
        type: MESSAGE_SYNC_REQUEST,
        exchangeId: 2n,
        chunkIndex,
        chunkCount: 2,
        updates: [],
      }),
    );
    expectProtocol(() => decodeSyncChunks(emptyChunks));

    const updates = Array.from({ length: MAX_MEMBERS + 1 }, (_, index) =>
      alive(
        `member-${index}`,
        index < 130 ? new Uint8Array(MAX_METADATA_BYTES) : new Uint8Array(0),
      ),
    );
    const split = updates.findIndex((_, index) => {
      const prefix = updates.slice(0, index + 1);
      return (
        18 + prefix.reduce((bytes, value) => bytes + membershipUpdateSize(value), 0) >
        MAX_SYNC_MESSAGE_BYTES
      );
    });
    const first = updates.slice(0, split);
    const second = updates.slice(split);
    expect(first.length).toBeGreaterThan(0);
    expect(second.length).toBeLessThanOrEqual(MAX_MEMBERS);
    const chunks = [
      encodeMessage({
        type: MESSAGE_SYNC_REQUEST,
        exchangeId: 3n,
        chunkIndex: 0,
        chunkCount: 2,
        updates: first,
      }),
      encodeMessage({
        type: MESSAGE_SYNC_REQUEST,
        exchangeId: 3n,
        chunkIndex: 1,
        chunkCount: 2,
        updates: second,
      }),
    ];
    expectProtocol(() => decodeSyncChunks(chunks));
  });
});
