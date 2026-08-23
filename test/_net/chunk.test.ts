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

import type { Socket } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  abortChunkFrame,
  ChunkReassembler,
  REASSEMBLY_ABORTED,
  REASSEMBLY_COMPLETE,
  REASSEMBLY_PENDING,
  REASSEMBLY_REJECT,
  REASSEMBLY_VIOLATION,
  type ReassemblyOutcome,
  splitLogicalFrame,
} from "../../src/_net/chunk";
import type { OutboundFrame } from "../../src/_net/conn";
import {
  type DataEnvelope,
  decodeHello,
  KIND_ASK,
  KIND_TELL,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "../../src/_net/envelope";
import {
  FLAG_EXPECTS_REPLY,
  FLAG_FIRST_CHUNK,
  FLAG_LAST_CHUNK,
  FRAME_CHUNK,
  FRAME_DATA,
  FRAME_HEADER_SIZE,
  FRAME_HELLO_ACK,
  type FrameHeader,
  LANE_CONTROL,
} from "../../src/_net/frame";
import type { NetServer } from "../../src/_net/server";
import {
  ErrMessageTooLarge,
  negotiateHello,
  Session,
  type SessionHandlers,
} from "../../src/_net/session";
import { ByteReader, ByteWriter, decodeValue, encodeValue } from "../../src/_net/values";
import {
  cleanupNet,
  dialSession,
  dialSocket,
  EMPTY,
  hello,
  MIB,
  RawPeer,
  startRawListener,
  startServer,
} from "./helpers";

afterEach(cleanupNet);

const CHUNK_SIZE: number = 1024;

function logicalOf(payloadLength: number, correlation: number = 0): OutboundFrame {
  const body: Uint8Array = new Uint8Array(payloadLength);
  for (let i = 0; i < body.length; i++) {
    body[i] = i % 251;
  }

  return {
    type: FRAME_DATA,
    flags: correlation === 0 ? 0 : FLAG_EXPECTS_REPLY,
    lane: LANE_CONTROL,
    correlation,
    body,
  };
}

function chunkHeader(frame: OutboundFrame): FrameHeader {
  return {
    type: frame.type,
    flags: frame.flags,
    lane: frame.lane,
    length: frame.body?.length ?? 0,
    correlation: frame.correlation,
  };
}

function pushAll(reassembler: ChunkReassembler, chunks: OutboundFrame[]): ReassemblyOutcome {
  let outcome: ReassemblyOutcome = {
    kind: REASSEMBLY_PENDING,
    header: null,
    body: null,
    reason: "",
  };
  for (const chunk of chunks) {
    outcome = reassembler.push(chunkHeader(chunk), chunk.body ?? EMPTY);
  }

  return outcome;
}

describe("chunk split", () => {
  it("splits and flags a logical frame, bodies capped at the chunk size", () => {
    const frame: OutboundFrame = logicalOf(10 * 1024, 7);
    const chunks: OutboundFrame[] = splitLogicalFrame(frame, CHUNK_SIZE, 99);

    expect(chunks.length).toBeGreaterThan(9);
    for (const [i, chunk] of chunks.entries()) {
      expect(chunk.type).toBe(FRAME_CHUNK);
      expect(chunk.correlation).toBe(99);
      expect((chunk.body?.length ?? 0) <= CHUNK_SIZE).toBe(true);
      const first: boolean = (chunk.flags & FLAG_FIRST_CHUNK) !== 0;
      const expectsReply: boolean = (chunk.flags & FLAG_EXPECTS_REPLY) !== 0;
      expect(first).toBe(i === 0);
      expect(expectsReply).toBe(i === 0);
      expect((chunk.flags & FLAG_LAST_CHUNK) !== 0).toBe(i === chunks.length - 1);
    }
  });

  it("round-trips through the reassembler byte for byte", () => {
    const frame: OutboundFrame = logicalOf(10 * 1024, 7);
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
    const outcome: ReassemblyOutcome = pushAll(
      reassembler,
      splitLogicalFrame(frame, CHUNK_SIZE, 99),
    );

    expect(outcome.kind).toBe(REASSEMBLY_COMPLETE);
    expect(outcome.header?.type).toBe(FRAME_DATA);
    expect(outcome.header?.correlation).toBe(7);
    expect(outcome.header?.length).toBe(10 * 1024);
    expect(outcome.body).toEqual(frame.body);
    expect(reassembler.activeGroups).toBe(0);
  });

  it("refuses a chunk size that cannot hold the prefix", () => {
    expect(() => splitLogicalFrame(logicalOf(100), 16, 1)).toThrow(TypeError);
    expect(() => splitLogicalFrame(logicalOf(100), CHUNK_SIZE, 0)).toThrow(TypeError);
  });
});

describe("chunk reassembly rules", () => {
  it("interleaves concurrent groups", () => {
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
    const a: OutboundFrame[] = splitLogicalFrame(logicalOf(3 * 1024), CHUNK_SIZE, 11);
    const b: OutboundFrame[] = splitLogicalFrame(logicalOf(3 * 1024), CHUNK_SIZE, 12);

    const order: OutboundFrame[] = [];
    for (let i = 0; i < Math.max(a.length, b.length); i++) {
      if (i < a.length) {
        order.push(a[i] as OutboundFrame);
      }

      if (i < b.length) {
        order.push(b[i] as OutboundFrame);
      }
    }

    let completed: number = 0;
    for (const chunk of order) {
      const outcome: ReassemblyOutcome = reassembler.push(chunkHeader(chunk), chunk.body ?? EMPTY);
      if (outcome.kind === REASSEMBLY_COMPLETE) {
        completed += 1;
      }
    }

    expect(completed).toBe(2);
    expect(reassembler.activeGroups).toBe(0);
  });

  it("soft-rejects an oversize declared total and ignores its tail", () => {
    const reassembler: ChunkReassembler = new ChunkReassembler(4 * 1024, 4);
    const chunks: OutboundFrame[] = splitLogicalFrame(logicalOf(8 * 1024), CHUNK_SIZE, 5);

    const first: ReassemblyOutcome = reassembler.push(
      chunkHeader(chunks[0] as OutboundFrame),
      chunks[0]?.body ?? EMPTY,
    );
    expect(first.kind).toBe(REASSEMBLY_REJECT);

    const tail: ReassemblyOutcome = reassembler.push(
      chunkHeader(chunks[1] as OutboundFrame),
      chunks[1]?.body ?? EMPTY,
    );
    expect(tail.kind).toBe(REASSEMBLY_PENDING);
    expect(reassembler.activeGroups).toBe(0);
  });

  it("soft-rejects beyond the concurrent group cap", () => {
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 1);
    const a: OutboundFrame[] = splitLogicalFrame(logicalOf(3 * 1024), CHUNK_SIZE, 21);
    const b: OutboundFrame[] = splitLogicalFrame(logicalOf(3 * 1024), CHUNK_SIZE, 22);

    expect(reassembler.push(chunkHeader(a[0] as OutboundFrame), a[0]?.body ?? EMPTY).kind).toBe(
      REASSEMBLY_PENDING,
    );
    expect(reassembler.push(chunkHeader(b[0] as OutboundFrame), b[0]?.body ?? EMPTY).kind).toBe(
      REASSEMBLY_REJECT,
    );
  });

  it("kills the stream on sequence violations", () => {
    const violations: (() => ReassemblyOutcome)[] = [
      // Duplicate first chunk.
      (): ReassemblyOutcome => {
        const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
        const chunks: OutboundFrame[] = splitLogicalFrame(logicalOf(3 * 1024), CHUNK_SIZE, 1);
        reassembler.push(chunkHeader(chunks[0] as OutboundFrame), chunks[0]?.body ?? EMPTY);
        return reassembler.push(chunkHeader(chunks[0] as OutboundFrame), chunks[0]?.body ?? EMPTY);
      },
      // Index gap.
      (): ReassemblyOutcome => {
        const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
        const chunks: OutboundFrame[] = splitLogicalFrame(logicalOf(4 * 1024), CHUNK_SIZE, 1);
        reassembler.push(chunkHeader(chunks[0] as OutboundFrame), chunks[0]?.body ?? EMPTY);
        return reassembler.push(chunkHeader(chunks[2] as OutboundFrame), chunks[2]?.body ?? EMPTY);
      },
      // First index nonzero.
      (): ReassemblyOutcome => {
        const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
        const writer: ByteWriter = new ByteWriter();
        writer.writeUvarint(1);
        writer.writeUvarint(4 * 1024);
        const header: FrameHeader = {
          type: FRAME_CHUNK,
          flags: FLAG_FIRST_CHUNK,
          lane: LANE_CONTROL,
          length: writer.length,
          correlation: 1,
        };
        return reassembler.push(header, Uint8Array.from(writer.bytes()));
      },
      // Declared total too small.
      (): ReassemblyOutcome => {
        const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
        const writer: ByteWriter = new ByteWriter();
        writer.writeUvarint(0);
        writer.writeUvarint(FRAME_HEADER_SIZE);
        const header: FrameHeader = {
          type: FRAME_CHUNK,
          flags: FLAG_FIRST_CHUNK,
          lane: LANE_CONTROL,
          length: writer.length,
          correlation: 1,
        };
        return reassembler.push(header, Uint8Array.from(writer.bytes()));
      },
      // Malformed index.
      (): ReassemblyOutcome => {
        const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
        const header: FrameHeader = {
          type: FRAME_CHUNK,
          flags: FLAG_FIRST_CHUNK,
          lane: LANE_CONTROL,
          length: 0,
          correlation: 1,
        };
        return reassembler.push(header, EMPTY);
      },
    ];
    for (const run of violations) {
      expect(run().kind).toBe(REASSEMBLY_VIOLATION);
    }
  });

  it("frees an aborted group and accepts a fresh one under the same id", () => {
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
    const chunks: OutboundFrame[] = splitLogicalFrame(logicalOf(4 * 1024), CHUNK_SIZE, 31);
    reassembler.push(chunkHeader(chunks[0] as OutboundFrame), chunks[0]?.body ?? EMPTY);

    const abort: OutboundFrame = abortChunkFrame(LANE_CONTROL, 31, 1);
    const aborted: ReassemblyOutcome = reassembler.push(chunkHeader(abort), abort.body ?? EMPTY);
    expect(aborted.kind).toBe(REASSEMBLY_ABORTED);
    expect(reassembler.activeGroups).toBe(0);

    expect(pushAll(reassembler, splitLogicalFrame(logicalOf(2 * 1024), CHUNK_SIZE, 31)).kind).toBe(
      REASSEMBLY_COMPLETE,
    );
  });
});

describe("chunked requests end to end", () => {
  function encodePayload(value: unknown): Uint8Array {
    const writer: ByteWriter = new ByteWriter();
    encodeValue(writer, value);
    return Uint8Array.from(writer.bytes());
  }

  function bigData(kind: number, typeRef: string, payload: Uint8Array): DataEnvelope {
    return {
      kind,
      to: "nodeakt://orders@10.0.0.5:5100/user/charger",
      uid: "b3f2",
      sender: "",
      senderUid: "",
      timeout: 0,
      serializerId: SERIALIZER_BINARY,
      typeRef,
      payload,
    };
  }

  async function startBigEchoServer(): Promise<NetServer> {
    return startServer(
      {},
      {
        onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
          if (correlation === 0) {
            return;
          }

          session.reply(correlation, {
            serializerId: SERIALIZER_BINARY,
            typeRef: "test.BigReply",
            payload: Uint8Array.from(envelope.payload),
          });
        },
      },
    );
  }

  it("round-trips a chunked ask whose reply is chunked back", async () => {
    const server: NetServer = await startBigEchoServer();
    const client: Session = await dialSession(
      server.address.port,
      hello(),
      {},
      {
        chunkSize: 64 * 1024,
      },
    );

    const payload: Uint8Array = encodePayload("x".repeat(MIB));
    const reply: ReplyEnvelope = await client.ask(bigData(KIND_ASK, "test.Big", payload), 5000);
    expect(reply.payload.length).toBe(payload.length);
    expect(decodeValue(new ByteReader(reply.payload))).toBe("x".repeat(MIB));
  });

  it("delivers a chunked tell intact", async () => {
    const received: Uint8Array[] = [];
    const server: NetServer = await startServer(
      {},
      {
        onData: (_session: Session, envelope: DataEnvelope): void => {
          received.push(Uint8Array.from(envelope.payload));
        },
      },
    );
    const client: Session = await dialSession(
      server.address.port,
      hello(),
      {},
      {
        chunkSize: 32 * 1024,
      },
    );

    const payload: Uint8Array = encodePayload(new Uint8Array(300 * 1024).fill(7));
    expect(client.tell(bigData(KIND_TELL, "test.Big", payload))).toBeNull();

    await vi.waitFor(
      (): void => {
        expect(received.length).toBe(1);
      },
      { timeout: 5000 },
    );
    expect(received[0]).toEqual(payload);
  });

  it("refuses a message over the negotiated cap on the sending side", async () => {
    // The message cap must sit at or above the frame size to be a
    // legal advertisement, so the frame size drops to the floor here.
    const server: NetServer = await startServer({
      local: hello({ systemName: "server", maxFrameSize: 16 * 1024, maxMessageSize: 64 * 1024 }),
    });
    const client: Session = await dialSession(
      server.address.port,
      hello({ maxFrameSize: 16 * 1024, maxMessageSize: 64 * 1024 }),
    );

    const payload: Uint8Array = new Uint8Array(128 * 1024);
    expect(client.tell(bigData(KIND_TELL, "test.Big", payload))).toBe(ErrMessageTooLarge);
    await expect(client.ask(bigData(KIND_ASK, "test.Big", payload), 1000)).rejects.toBe(
      ErrMessageTooLarge,
    );
  });

  it("refuses a frame-oversize message when the peer cannot chunk", async () => {
    const server: NetServer = await startServer();
    const client: Session = await dialSession(
      server.address.port,
      hello({ revision: 1, maxFrameSize: 16 * 1024 }),
    );
    expect(client.effective?.revision).toBe(1);

    const payload: Uint8Array = new Uint8Array(100 * 1024);
    expect(client.tell(bigData(KIND_TELL, "test.Big", payload))).toBe(ErrMessageTooLarge);
  });

  it("allocates parity-split correlations so group keys never collide", async () => {
    let accepted: Session | null = null;
    const server: NetServer = await startServer(
      {},
      {
        onSession: (session: Session): void => {
          accepted = session;
        },
      },
    );
    const client: Session = await dialSession(server.address.port);

    expect(client.nextCorrelation() % 2).toBe(1);
    expect(client.nextCorrelation() % 2).toBe(1);
    await vi.waitFor((): void => {
      expect(accepted).not.toBeNull();
    });
    expect(((accepted as Session | null)?.nextCorrelation() as number) % 2).toBe(0);
  });

  it("settles a waiting ask when a chunked reply is rejected as oversize", async () => {
    // A scripted acceptor advertises a huge message cap, then answers
    // the ask with a chunked reply declaring a total beyond what the
    // dialer accepts; the dialer must reject the group and fail the
    // ask promptly.
    const dialerCap: number = 64 * 1024;
    const port: number = await startRawListener((socket: Socket): void => {
      const peer: RawPeer = new RawPeer(socket);
      peer.onFirstFrame((): void => {
        const request = decodeHello(new ByteReader(peer.frames[0]?.body ?? EMPTY));
        peer.sendHello(
          FRAME_HELLO_ACK,
          negotiateHello(hello({ maxMessageSize: 16 * MIB }), request),
        );
        const waitAsk: NodeJS.Timeout = setInterval((): void => {
          const ask = peer.frames.find((frame): boolean => frame.header.type === FRAME_DATA);
          if (ask === undefined) {
            return;
          }

          clearInterval(waitAsk);
          const writer: ByteWriter = new ByteWriter();
          writer.writeUvarint(0);
          writer.writeUvarint(8 * MIB);
          peer.send({
            type: FRAME_CHUNK,
            flags: FLAG_FIRST_CHUNK,
            lane: LANE_CONTROL,
            correlation: ask.header.correlation,
            body: Uint8Array.from(writer.bytes()),
          });
        }, 5);
      });
    });

    const handlers: SessionHandlers = {};
    const client: Session = await Session.dial(
      dialSocket(port),
      hello({ maxFrameSize: 16 * 1024, maxMessageSize: dialerCap }),
      handlers,
    );
    // The scripted peer ignores the negotiated minimum on purpose; the
    // dialer's own cap must still protect it.
    await expect(client.ask(bigData(KIND_ASK, "test.Big", new Uint8Array(8)), 5000)).rejects.toBe(
      ErrMessageTooLarge,
    );
    client.destroy();
  });
});
