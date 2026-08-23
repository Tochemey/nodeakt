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

import { afterEach, describe, expect, it, vi } from "vitest";
import {
  type DataEnvelope,
  decodeDataEnvelope,
  encodeDataEnvelope,
  KIND_ASK,
  KIND_TELL,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "../../src/_net/envelope";
import {
  FRAME_DATA,
  FRAME_REPLY,
  FRAME_TABLE,
  type FrameHeader,
  LANE_CONTROL,
  ProtocolError,
} from "../../src/_net/frame";
import type { NetServer } from "../../src/_net/server";
import type { Session } from "../../src/_net/session";
import {
  decodeTableBody,
  encodeTableBody,
  InboundTables,
  OutboundTables,
  TABLE_CAPACITY,
  TABLE_PATH,
  TABLE_TYPE,
  type TableAnnouncement,
} from "../../src/_net/table";
import { ByteReader, ByteWriter } from "../../src/_net/values";
import {
  cleanupNet,
  dialScripted,
  dialSession,
  expectProtocolDeath,
  type RawPeer,
  startServer,
} from "./helpers";

afterEach(cleanupNet);

const TARGET_PATH: string = "nodeakt://orders@10.0.0.5:5100/user/charger";
const TYPE_REF: string = "test.Fill";

function envelopeOf(kind: number, payloadLength: number): DataEnvelope {
  return {
    kind,
    to: TARGET_PATH,
    uid: "b3f2",
    sender: "",
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: TYPE_REF,
    payload: new Uint8Array(payloadLength),
  };
}

function collector(): { tables: OutboundTables; announced: TableAnnouncement[] } {
  const announced: TableAnnouncement[] = [];
  const tables: OutboundTables = new OutboundTables((announcement: TableAnnouncement): void => {
    announced.push(announcement);
  });
  return { tables, announced };
}

function tableBody(kind: number, id: number, literal: string): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeTableBody(writer, { kind, id, literal });
  return Uint8Array.from(writer.bytes());
}

describe("outbound tables", () => {
  it("assigns ids from 1 per kind and announces each exactly once", () => {
    const { tables, announced } = collector();

    expect(tables.pathId("/user/a")).toBe(1);
    expect(tables.pathId("/user/b")).toBe(2);
    expect(tables.typeId("test.A")).toBe(1);
    expect(tables.pathId("/user/a")).toBe(1);
    expect(tables.typeId("test.A")).toBe(1);

    expect(announced).toEqual([
      { kind: TABLE_PATH, id: 1, literal: "/user/a" },
      { kind: TABLE_PATH, id: 2, literal: "/user/b" },
      { kind: TABLE_TYPE, id: 1, literal: "test.A" },
    ]);
  });

  it("keeps empty literals inline", () => {
    const { tables, announced } = collector();

    expect(tables.pathId("")).toBe(0);
    expect(tables.typeId("")).toBe(0);
    expect(announced.length).toBe(0);
  });

  it("goes inline past capacity while old entries keep their ids", () => {
    const { tables, announced } = collector();
    for (let i = 1; i <= TABLE_CAPACITY; i++) {
      expect(tables.typeId(`test.T${i}`)).toBe(i);
    }

    expect(tables.typeId("test.Overflow")).toBe(0);
    expect(announced.length).toBe(TABLE_CAPACITY);
    expect(tables.typeId("test.T17")).toBe(17);
  });
});

describe("inbound tables", () => {
  it("installs and resolves both kinds independently", () => {
    const tables: InboundTables = new InboundTables();
    tables.install({ kind: TABLE_PATH, id: 1, literal: "/user/a" });
    tables.install({ kind: TABLE_TYPE, id: 1, literal: "test.A" });

    expect(tables.path(1)).toBe("/user/a");
    expect(tables.type(1)).toBe("test.A");
    expect((): string => tables.path(2)).toThrow(ProtocolError);
    expect((): string => tables.type(2)).toThrow(ProtocolError);
  });

  it("is idempotent on an identical re-registration and strict on the rest", () => {
    const tables: InboundTables = new InboundTables();
    tables.install({ kind: TABLE_PATH, id: 1, literal: "/user/a" });
    tables.install({ kind: TABLE_PATH, id: 1, literal: "/user/a" });
    expect(tables.path(1)).toBe("/user/a");

    const violations: TableAnnouncement[] = [
      { kind: TABLE_PATH, id: 1, literal: "/user/b" },
      { kind: TABLE_PATH, id: 0, literal: "/user/z" },
      { kind: TABLE_PATH, id: 2, literal: "" },
      { kind: TABLE_PATH, id: TABLE_CAPACITY + 1, literal: "/user/z" },
    ];
    for (const violation of violations) {
      expect((): void => tables.install(violation)).toThrow(ProtocolError);
    }
  });

  it("caches a handle only on an interned path", () => {
    const tables: InboundTables = new InboundTables();
    tables.install({ kind: TABLE_PATH, id: 1, literal: "/user/a" });
    const marker: object = { pid: "a" };

    expect(tables.cacheHandle("/user/a", marker)).toBe(true);
    expect(tables.handleOf("/user/a")).toBe(marker);
    expect(tables.cacheHandle("/user/unknown", marker)).toBe(false);
    expect(tables.handleOf("/user/unknown")).toBeUndefined();
  });
});

describe("table body codec", () => {
  it("round-trips a registration", () => {
    const decoded: TableAnnouncement = decodeTableBody(
      new ByteReader(tableBody(TABLE_TYPE, 42, "test.A")),
    );
    expect(decoded).toEqual({ kind: TABLE_TYPE, id: 42, literal: "test.A" });
  });

  it("rejects an unknown kind and trailing bytes", () => {
    expect((): TableAnnouncement => decodeTableBody(new ByteReader(tableBody(7, 1, "x")))).toThrow(
      ProtocolError,
    );

    const trailing: Uint8Array = new Uint8Array([...tableBody(TABLE_PATH, 1, "x"), 0]);
    expect((): TableAnnouncement => decodeTableBody(new ByteReader(trailing))).toThrow(
      ProtocolError,
    );
  });
});

describe("interned envelopes", () => {
  it("shrinks refs to ids and restores them through the receiver tables", () => {
    const { tables, announced } = collector();
    const receiver: InboundTables = new InboundTables();
    const envelope: DataEnvelope = envelopeOf(KIND_TELL, 8);

    const interned: ByteWriter = new ByteWriter();
    encodeDataEnvelope(interned, envelope, tables);
    for (const announcement of announced) {
      receiver.install(announcement);
    }

    const inline: ByteWriter = new ByteWriter();
    encodeDataEnvelope(inline, envelope);
    expect(interned.length).toBeLessThan(inline.length);

    const decoded: DataEnvelope = decodeDataEnvelope(
      new ByteReader(Uint8Array.from(interned.bytes())),
      receiver,
    );
    expect(decoded.to).toBe(TARGET_PATH);
    expect(decoded.typeRef).toBe(TYPE_REF);
    expect(decoded.sender).toBe("");
  });
});

describe("tables over the wire", () => {
  function framesOf(peer: RawPeer, type: number): { header: FrameHeader; body: Uint8Array }[] {
    return peer.frames.filter((frame): boolean => frame.header.type === type);
  }

  it("announces each ref once, before the envelope that uses it", async () => {
    const { client, peer } = await dialScripted({});
    const envelope: DataEnvelope = envelopeOf(KIND_TELL, 8);

    expect(client.tell(envelope)).toBeNull();
    expect(client.tell(envelope)).toBeNull();
    await vi.waitFor((): void => {
      expect(framesOf(peer, FRAME_DATA).length).toBe(2);
    });

    const registrations: TableAnnouncement[] = framesOf(peer, FRAME_TABLE).map(
      (frame): TableAnnouncement => decodeTableBody(new ByteReader(frame.body)),
    );
    expect(registrations).toEqual([
      { kind: TABLE_PATH, id: 1, literal: TARGET_PATH },
      { kind: TABLE_TYPE, id: 1, literal: TYPE_REF },
    ]);

    const firstData: number = peer.frames.findIndex(
      (frame): boolean => frame.header.type === FRAME_DATA,
    );
    const lastTable: number = peer.frames.reduce(
      (last: number, frame, index: number): number =>
        frame.header.type === FRAME_TABLE ? index : last,
      -1,
    );
    expect(lastTable).toBeLessThan(firstData);

    // Both envelopes ride the ids, and both restore through a
    // receiver table fed from the announcements.
    const receiver: InboundTables = new InboundTables();
    for (const registration of registrations) {
      receiver.install(registration);
    }

    const inline: ByteWriter = new ByteWriter();
    encodeDataEnvelope(inline, envelope);
    for (const frame of framesOf(peer, FRAME_DATA)) {
      expect(frame.body.length).toBeLessThan(inline.length);
      const decoded: DataEnvelope = decodeDataEnvelope(new ByteReader(frame.body), receiver);
      expect(decoded.to).toBe(TARGET_PATH);
      expect(decoded.typeRef).toBe(TYPE_REF);
    }
  });

  it("interns the reply direction independently", async () => {
    const server: NetServer = await startServer(
      {},
      {
        onData: (session: Session, _envelope: DataEnvelope, correlation: number): void => {
          session.reply(correlation, {
            serializerId: SERIALIZER_BINARY,
            typeRef: "test.FillReply",
            payload: new Uint8Array(4),
          });
        },
      },
    );
    const client: Session = await dialSession(server.address.port);

    for (let i = 0; i < 2; i++) {
      const reply: ReplyEnvelope = await client.ask(envelopeOf(KIND_ASK, 8), 2000);
      expect(reply.typeRef).toBe("test.FillReply");
    }
  });

  it("caches a resolved handle on the inbound path entry", async () => {
    const marker: object = { resolved: true };
    const cached: boolean[] = [];
    const seen: unknown[] = [];
    const server: NetServer = await startServer(
      {},
      {
        onData: (session: Session, envelope: DataEnvelope): void => {
          seen.push(session.pathHandle(envelope.to));
          cached.push(session.cachePathHandle(envelope.to, marker));
        },
      },
    );
    const client: Session = await dialSession(server.address.port);

    client.tell(envelopeOf(KIND_TELL, 8));
    client.tell(envelopeOf(KIND_TELL, 8));
    await vi.waitFor((): void => {
      expect(cached.length).toBe(2);
    });

    expect(cached).toEqual([true, true]);
    expect(seen[0]).toBeUndefined();
    expect(seen[1]).toBe(marker);
  });
});

describe("table strictness on the wire", () => {
  /** A DATA body whose target ref names a table id. */
  function rawTellTo(refId: number): Uint8Array {
    const writer: ByteWriter = new ByteWriter();
    writer.writeU8(KIND_TELL);
    writer.writeUvarint(refId);
    writer.writeString("");
    writer.writeUvarint(0);
    writer.writeString("");
    writer.writeString("");
    writer.writeUvarint(0);
    writer.writeU8(SERIALIZER_BINARY);
    writer.writeUvarint(0);
    writer.writeString("");
    return Uint8Array.from(writer.bytes());
  }

  it("dies on a ref naming an unknown id", async () => {
    await expectProtocolDeath(
      {},
      [{ type: FRAME_DATA, body: rawTellTo(9) }],
      "unknown path table ref 9",
    );
  });

  it("dies on any nonzero ref below revision 3", async () => {
    await expectProtocolDeath(
      { revision: 2 },
      [{ type: FRAME_DATA, body: rawTellTo(9) }],
      "without a negotiated table",
    );
  });

  it("dies on TABLE below revision 3", async () => {
    await expectProtocolDeath(
      { revision: 2 },
      [{ type: FRAME_TABLE, body: tableBody(TABLE_PATH, 1, "/user/a") }],
      "TABLE requires capability revision 3",
    );
  });

  it("dies on a conflicting re-registration", async () => {
    await expectProtocolDeath(
      {},
      [
        { type: FRAME_TABLE, body: tableBody(TABLE_PATH, 1, "/user/a") },
        { type: FRAME_TABLE, body: tableBody(TABLE_PATH, 1, "/user/b") },
      ],
      "re-registered with a new literal",
    );
  });

  it("dies on a malformed table kind", async () => {
    await expectProtocolDeath(
      {},
      [{ type: FRAME_TABLE, body: tableBody(7, 1, "/user/a") }],
      "unknown table kind 7",
    );
  });

  it("lives through an idempotent duplicate and resolves the ref", async () => {
    const received: DataEnvelope[] = [];
    const { client, peer } = await dialScripted(
      {},
      {
        onData: (_session: Session, envelope: DataEnvelope): void => {
          received.push(envelope);
        },
      },
    );

    const registration: Uint8Array = tableBody(TABLE_PATH, 1, "/user/a");
    peer.send({
      type: FRAME_TABLE,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 0,
      body: Uint8Array.from(registration),
    });
    peer.send({
      type: FRAME_TABLE,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 0,
      body: Uint8Array.from(registration),
    });
    peer.send({
      type: FRAME_DATA,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 0,
      body: rawTellTo(1),
    });

    await vi.waitFor((): void => {
      expect(received.length).toBe(1);
    });
    expect(received[0]?.to).toBe("/user/a");
    expect(client.closed).toBe(false);
  });

  it("fails the waiting ask and the connection on an unresolvable reply ref", async () => {
    let closeError: Error | null = null;
    const { client, peer } = await dialScripted(
      {},
      {
        onClose: (_session: Session, error: Error | null): void => {
          closeError = error;
        },
      },
    );

    const pending: Promise<ReplyEnvelope> = client.ask(envelopeOf(KIND_ASK, 4), 5000);
    await vi.waitFor((): void => {
      expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_DATA)).toBe(true);
    });

    const ask = peer.frames.find((frame): boolean => frame.header.type === FRAME_DATA);
    const reply: ByteWriter = new ByteWriter();
    reply.writeU8(SERIALIZER_BINARY);
    reply.writeUvarint(9);
    peer.send({
      type: FRAME_REPLY,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: ask?.header.correlation ?? 0,
      body: Uint8Array.from(reply.bytes()),
    });

    await expect(pending).rejects.toThrow(ProtocolError);
    await vi.waitFor((): void => {
      expect(closeError).toBeInstanceOf(ProtocolError);
    });
    expect((closeError as Error | null)?.message).toContain("unknown type table ref 9");
  });
});
