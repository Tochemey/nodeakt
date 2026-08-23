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

import { EventEmitter } from "node:events";
import { connect, createServer, type Server, type Socket } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ChunkReassembler,
  REASSEMBLY_PENDING,
  REASSEMBLY_REJECT,
  REASSEMBLY_VIOLATION,
  type ReassemblyOutcome,
  splitLogicalFrame,
} from "../../src/_net/chunk";
import {
  ErrConnClosed,
  ErrWriteTimeout,
  FramedConn,
  type FramedConnHandlers,
  type OutboundFrame,
} from "../../src/_net/conn";
import {
  type DataEnvelope,
  decodeHello,
  decodeReplyEnvelope,
  ERROR_UNAVAILABLE,
  encodeReplyEnvelope,
  type Hello,
  KIND_ASK,
  KIND_TELL,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "../../src/_net/envelope";
import {
  encodeFrameHeader,
  FLAG_FIRST_CHUNK,
  FLAG_LAST_CHUNK,
  FRAME_CHUNK,
  FRAME_DATA,
  FRAME_ERROR,
  FRAME_HELLO,
  FRAME_HELLO_ACK,
  FRAME_PING,
  FRAME_PONG,
  FRAME_REPLY,
  type FrameHeader,
  LANE_CONTROL,
  MAX_U32,
  ProtocolError,
  validateFrameHeader,
} from "../../src/_net/frame";
import { Peer } from "../../src/_net/peer";
import type { NetServer } from "../../src/_net/server";
import {
  ErrIdleReclaim,
  ErrMessageTooLarge,
  negotiateHello,
  Session,
} from "../../src/_net/session";
import { defaultTimers, type TimerHandle } from "../../src/_net/timers";
import {
  ByteReader,
  ByteWriter,
  decodeValue,
  encodeValue,
  ValueDecodeError,
} from "../../src/_net/values";
import {
  cleanupNet,
  dialScripted,
  dialSession,
  dialSocket,
  EMPTY,
  expectProtocolDeath,
  hello,
  ManualTimers,
  MIB,
  RawPeer,
  sleep,
  startRawListener,
  startServer,
  trackPeer,
  trackSession,
  trackSocket,
} from "./helpers";

afterEach(cleanupNet);

function roundTrip(value: unknown): unknown {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, value);
  return decodeValue(new ByteReader(Uint8Array.from(writer.bytes())));
}

function envelopeOf(kind: number, payloadLength: number): DataEnvelope {
  return {
    kind,
    to: "nodeakt://orders@10.0.0.5:5100/user/charger",
    uid: "",
    sender: "",
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: "test.Edge",
    payload: new Uint8Array(payloadLength),
  };
}

describe("value codec hardening", () => {
  it("round-trips the full typed-array family", () => {
    const views: ArrayBufferView[] = [
      Int8Array.from([1, -2]),
      Uint8ClampedArray.from([3, 255]),
      Int16Array.from([-5, 6]),
      Uint16Array.from([7, 8]),
      Int32Array.from([-9, 10]),
      Uint32Array.from([11, 12]),
      Float32Array.from([1.5, -2.5]),
      Float64Array.from([3.5, -4.5]),
      BigInt64Array.from([-3n, 4n]),
      BigUint64Array.from([5n, 6n]),
    ];
    for (const view of views) {
      const decoded: unknown = roundTrip(view);
      expect(decoded).toBeInstanceOf(view.constructor);
      expect(decoded).toEqual(view);
    }

    const dataView: DataView = new DataView(Uint8Array.from([9, 8, 7]).buffer);
    const decodedView: unknown = roundTrip(dataView);
    expect(decodedView).toBeInstanceOf(DataView);
    expect(new Uint8Array((decodedView as DataView).buffer)).toEqual(Uint8Array.from([9, 8, 7]));

    const buffer: ArrayBuffer = Uint8Array.from([1, 2, 3]).buffer as ArrayBuffer;
    const decodedBuffer: unknown = roundTrip(buffer);
    expect(decodedBuffer).toBeInstanceOf(ArrayBuffer);
    expect(new Uint8Array(decodedBuffer as ArrayBuffer)).toEqual(Uint8Array.from([1, 2, 3]));
  });

  it("sizes astral pairs and lone surrogates correctly", () => {
    expect(roundTrip("a\u{1F600}b")).toBe("a\u{1F600}b");

    const writer: ByteWriter = new ByteWriter();
    writer.writeString("x\ud800y");
    writer.writeString("x\ud800");
    writer.writeString("\udc00x");
    const reader: ByteReader = new ByteReader(Uint8Array.from(writer.bytes()));
    expect(reader.readString()).toBe("x�y");
    expect(reader.readString()).toBe("x�");
    expect(reader.readString()).toBe("�x");
    expect(reader.remaining).toBe(0);
  });

  it("rejects a uvarint beyond the safe integer range", () => {
    const reader: ByteReader = new ByteReader(
      Uint8Array.from([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]),
    );
    expect((): number => reader.readUvarint()).toThrow(ValueDecodeError);
  });

  it("tracks the read position", () => {
    const reader: ByteReader = new ByteReader(Uint8Array.from([1, 2, 3]));
    expect(reader.position).toBe(0);
    reader.readU8();
    reader.readU8();
    expect(reader.position).toBe(2);
  });

  it("refuses shared memory and foreign views on the way out", () => {
    const writer: ByteWriter = new ByteWriter();
    expect((): void => encodeValue(writer, new SharedArrayBuffer(8))).toThrow(TypeError);

    class WeirdView extends Int16Array {}
    expect((): void => encodeValue(writer, new WeirdView(2))).toThrow(TypeError);
  });

  it("rejects a bigint with a corrupt sign byte", () => {
    const writer: ByteWriter = new ByteWriter();
    encodeValue(writer, 5n);
    const bytes: Uint8Array = Uint8Array.from(writer.bytes());
    bytes[1] = 2;
    expect((): unknown => decodeValue(new ByteReader(bytes))).toThrow(ValueDecodeError);
  });

  it("clamps a tiny writer capacity and grows past it", () => {
    const writer: ByteWriter = new ByteWriter(1);
    writer.writeBytes(new Uint8Array(64).fill(7));
    expect(writer.length).toBe(64);
  });
});

describe("frame validation hardening", () => {
  function headerOf(overrides: Partial<FrameHeader>): FrameHeader {
    return {
      type: FRAME_PING,
      flags: 0,
      lane: LANE_CONTROL,
      length: 0,
      correlation: 1,
      ...overrides,
    };
  }

  it("refuses unknown types, reserved flags, and bad lengths at accept time", () => {
    expect((): void => validateFrameHeader(headerOf({ type: 0x00 }))).toThrow(ProtocolError);
    expect((): void => validateFrameHeader(headerOf({ type: 0x0b }))).toThrow(ProtocolError);
    expect((): void => validateFrameHeader(headerOf({ flags: 0x80 }))).toThrow(ProtocolError);
    expect((): void => validateFrameHeader(headerOf({ length: -1 }))).toThrow(ProtocolError);
    expect((): void => validateFrameHeader(headerOf({ length: 1.5 }))).toThrow(ProtocolError);
    expect((): void => validateFrameHeader(headerOf({ length: MAX_U32 + 1 }))).toThrow(
      ProtocolError,
    );
  });
});

describe("envelope hardening", () => {
  it("rejects a reply type ref without a negotiated table", () => {
    const writer: ByteWriter = new ByteWriter();
    writer.writeU8(SERIALIZER_BINARY);
    writer.writeUvarint(5);
    expect(
      (): ReplyEnvelope => decodeReplyEnvelope(new ByteReader(Uint8Array.from(writer.bytes()))),
    ).toThrow(ProtocolError);
  });
});

describe("chunk hardening", () => {
  function chunkHeader(flags: number, length: number, correlation: number): FrameHeader {
    return { type: FRAME_CHUNK, flags, lane: LANE_CONTROL, length, correlation };
  }

  it("kills the stream on a malformed declared total", () => {
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
    const body: Uint8Array = Uint8Array.from([0x00, 0xff]);
    const outcome: ReassemblyOutcome = reassembler.push(
      chunkHeader(FLAG_FIRST_CHUNK, body.length, 1),
      body,
    );
    expect(outcome.kind).toBe(REASSEMBLY_VIOLATION);
    expect(outcome.reason).toContain("total size is malformed");
  });

  it("kills the stream when chunk data overflows the declared total", () => {
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
    const first: ByteWriter = new ByteWriter();
    first.writeUvarint(0);
    first.writeUvarint(26);
    first.writeBytes(new Uint8Array(8));
    expect(
      reassembler.push(
        chunkHeader(FLAG_FIRST_CHUNK, first.length, 2),
        Uint8Array.from(first.bytes()),
      ).kind,
    ).toBe(REASSEMBLY_PENDING);

    const next: ByteWriter = new ByteWriter();
    next.writeUvarint(1);
    next.writeBytes(new Uint8Array(20));
    const outcome: ReassemblyOutcome = reassembler.push(
      chunkHeader(0, next.length, 2),
      Uint8Array.from(next.bytes()),
    );
    expect(outcome.kind).toBe(REASSEMBLY_VIOLATION);
    expect(outcome.reason).toContain("exceeds the declared total");
  });

  it("kills the stream when the reassembled inner header is garbage", () => {
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
    const writer: ByteWriter = new ByteWriter();
    writer.writeUvarint(0);
    writer.writeUvarint(17);
    writer.writeBytes(new Uint8Array(17).fill(0xaa));
    const outcome: ReassemblyOutcome = reassembler.push(
      chunkHeader(FLAG_FIRST_CHUNK | FLAG_LAST_CHUNK, writer.length, 3),
      Uint8Array.from(writer.bytes()),
    );
    expect(outcome.kind).toBe(REASSEMBLY_VIOLATION);
  });

  it("kills the stream when the inner length disagrees with the logical size", () => {
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
    const inner: ByteWriter = new ByteWriter();
    encodeFrameHeader(inner, {
      type: FRAME_DATA,
      flags: 0,
      lane: LANE_CONTROL,
      length: 0,
      correlation: 0,
    });
    const writer: ByteWriter = new ByteWriter();
    writer.writeUvarint(0);
    writer.writeUvarint(18);
    writer.writeBytes(Uint8Array.from(inner.bytes()));
    writer.writeBytes(new Uint8Array(2));
    const outcome: ReassemblyOutcome = reassembler.push(
      chunkHeader(FLAG_FIRST_CHUNK | FLAG_LAST_CHUNK, writer.length, 4),
      Uint8Array.from(writer.bytes()),
    );
    expect(outcome.kind).toBe(REASSEMBLY_VIOLATION);
    expect(outcome.reason).toContain("length mismatch");
  });

  it("splits a bodiless frame, which no receiver may accept as a group", () => {
    const chunks = splitLogicalFrame(
      { type: FRAME_DATA, flags: 0, lane: LANE_CONTROL, correlation: 0, body: null },
      1024,
      9,
    );
    expect(chunks.length).toBe(1);

    // A bodiless logical frame is exactly one header, and a declared
    // total that small is a violation: nothing that fits one frame
    // may ever arrive chunked.
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 4);
    const chunk = chunks[0];
    const outcome: ReassemblyOutcome = reassembler.push(
      chunkHeader(chunk?.flags ?? 0, chunk?.body?.length ?? 0, 9),
      chunk?.body ?? EMPTY,
    );
    expect(outcome.kind).toBe(REASSEMBLY_VIOLATION);
    expect(outcome.reason).toContain("too small");
  });

  it("clamps a zero group cap to one", () => {
    const reassembler: ChunkReassembler = new ChunkReassembler(MIB, 0);
    const a = splitLogicalFrame(
      { type: FRAME_DATA, flags: 0, lane: LANE_CONTROL, correlation: 0, body: new Uint8Array(64) },
      32,
      11,
    );
    const b = splitLogicalFrame(
      { type: FRAME_DATA, flags: 0, lane: LANE_CONTROL, correlation: 0, body: new Uint8Array(64) },
      32,
      12,
    );
    expect(
      reassembler.push(
        chunkHeader(a[0]?.flags ?? 0, a[0]?.body?.length ?? 0, 11),
        a[0]?.body ?? EMPTY,
      ).kind,
    ).toBe(REASSEMBLY_PENDING);
    expect(
      reassembler.push(
        chunkHeader(b[0]?.flags ?? 0, b[0]?.body?.length ?? 0, 12),
        b[0]?.body ?? EMPTY,
      ).kind,
    ).toBe(REASSEMBLY_REJECT);
  });
});

describe("conn hardening", () => {
  function encodedFrame(type: number, correlation: number): Buffer {
    const writer: ByteWriter = new ByteWriter();
    encodeFrameHeader(writer, { type, flags: 0, lane: LANE_CONTROL, length: 0, correlation });
    return Buffer.from(writer.bytes().slice());
  }

  /** The smallest socket shape the framed connection needs. */
  class FakeSocket extends EventEmitter {
    readonly written: Uint8Array[] = [];

    write(bytes: Uint8Array, callback?: (error?: Error | null) => void): boolean {
      this.written.push(Uint8Array.from(bytes));
      callback?.();
      return true;
    }

    destroy(): void {
      this.emit("close");
    }

    end(): void {}
  }

  function collectHandlers(): {
    handlers: FramedConnHandlers;
    frames: FrameHeader[];
    closed: (Error | null)[];
    confirmed: unknown[];
  } {
    const frames: FrameHeader[] = [];
    const closed: (Error | null)[] = [];
    const confirmed: unknown[] = [];
    return {
      frames,
      closed,
      confirmed,
      handlers: {
        onFrame: (header: FrameHeader): void => {
          frames.push(header);
        },
        onViolation: (): void => {},
        onClose: (error: Error | null): void => {
          closed.push(error);
        },
        onWrote: (token: unknown): void => {
          confirmed.push(token);
        },
      },
    };
  }

  it("runs on a socket without platform extras", async () => {
    const fake: FakeSocket = new FakeSocket();
    const { handlers, frames } = collectHandlers();
    const conn: FramedConn = new FramedConn(fake as unknown as Socket, handlers);

    expect(conn.closed).toBe(false);
    expect(
      conn.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 7, body: null }),
    ).toBeNull();
    await Promise.resolve();
    expect(fake.written.length).toBe(1);

    fake.emit("data", encodedFrame(FRAME_PONG, 7));
    expect(frames[0]?.type).toBe(FRAME_PONG);

    conn.end();
    conn.end();
    conn.destroy();
    conn.end();
    expect(conn.closed).toBe(true);
  });

  it("re-arms the write deadline for the time the last progress bought", async () => {
    // A socket that parks its write callbacks so the test controls
    // exactly when the kernel "accepts" each buffer.
    class StallSocket extends EventEmitter {
      readonly callbacks: ((error?: Error | null) => void)[] = [];

      write(_bytes: Uint8Array, callback?: (error?: Error | null) => void): boolean {
        if (callback !== undefined) {
          this.callbacks.push(callback);
        }

        return true;
      }

      destroy(): void {
        this.emit("close");
      }

      end(): void {}
    }

    const clock: ManualTimers = new ManualTimers();
    const socket: StallSocket = new StallSocket();
    const { handlers, closed } = collectHandlers();
    const conn: FramedConn = new FramedConn(socket as unknown as Socket, handlers, {
      writeTimeoutMs: 100,
      timers: clock,
    });

    // A large body issues two writes: the header batch and the body.
    conn.send({
      type: FRAME_DATA,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 0,
      body: new Uint8Array(5000),
    });
    await Promise.resolve();
    expect(socket.callbacks.length).toBe(2);

    // The header write completes at 40; the body write stays stuck.
    clock.advance(40);
    (socket.callbacks[0] as (error?: Error | null) => void)();

    // At the 100ms deadline the last progress is 60ms old, so the
    // timer re-arms for the remainder instead of tearing down.
    clock.advance(60);
    expect(conn.closed).toBe(false);

    // No progress through the re-armed deadline: now it tears down.
    clock.advance(40);
    expect(conn.closed).toBe(true);
    expect(closed).toEqual([ErrWriteTimeout]);
  });

  it("honors a configured frames-per-write batch cap", async () => {
    const fake: FakeSocket = new FakeSocket();
    const { handlers } = collectHandlers();
    const conn: FramedConn = new FramedConn(fake as unknown as Socket, handlers, {
      batchFrames: 2,
    });

    for (let i = 0; i < 5; i++) {
      conn.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 1, body: null });
    }

    await Promise.resolve();
    // Five frames at two per write: 2 + 2 + 1.
    expect(fake.written.length).toBe(3);
    conn.destroy();
  });

  it("resumes flushing on drain after the socket reports backpressure", async () => {
    // A socket that refuses the first write (returns false, the kernel
    // buffer is full) and accepts the rest; the connection must park
    // until the drain event and then flush what was left queued.
    class ChokedSocket extends EventEmitter {
      readonly written: Uint8Array[] = [];

      write(bytes: Uint8Array, callback?: (error?: Error | null) => void): boolean {
        this.written.push(Uint8Array.from(bytes));
        callback?.();
        return this.written.length > 1;
      }

      destroy(): void {
        this.emit("close");
      }

      end(): void {}
    }

    const socket: ChokedSocket = new ChokedSocket();
    const { handlers } = collectHandlers();
    const conn: FramedConn = new FramedConn(socket as unknown as Socket, handlers, {
      batchFrames: 1,
    });

    conn.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 1, body: null });
    conn.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 2, body: null });
    await Promise.resolve();
    // The first write reported backpressure; the second frame waits.
    expect(socket.written.length).toBe(1);

    socket.emit("drain");
    await Promise.resolve();
    expect(socket.written.length).toBe(2);
    conn.destroy();
  });

  it("confirms a large frame on its own write", async () => {
    const fake: FakeSocket = new FakeSocket();
    const { handlers, confirmed } = collectHandlers();
    const conn: FramedConn = new FramedConn(fake as unknown as Socket, handlers);

    conn.send({
      type: FRAME_DATA,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 0,
      body: new Uint8Array(5000),
      wrote: "large-token",
    });
    await Promise.resolve();
    expect(confirmed).toEqual(["large-token"]);
    // Header and large body go out as two writes.
    expect(fake.written.length).toBe(2);
    conn.destroy();
  });

  it("skips confirmation when the write itself fails", async () => {
    const fake: FakeSocket = new FakeSocket();
    fake.write = (_bytes: Uint8Array, callback?: (error?: Error | null) => void): boolean => {
      callback?.(new Error("wrecked"));
      return true;
    };
    const { handlers, confirmed } = collectHandlers();
    const conn: FramedConn = new FramedConn(fake as unknown as Socket, handlers);

    conn.send({
      type: FRAME_PING,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 1,
      body: null,
      wrote: "doomed-token",
    });
    await Promise.resolve();
    expect(confirmed).toEqual([]);
    conn.destroy();
  });

  it("closes the connection when the frame handler throws", async () => {
    const fake: FakeSocket = new FakeSocket();
    const closed: (Error | null)[] = [];
    const conn: FramedConn = new FramedConn(fake as unknown as Socket, {
      onFrame: (): void => {
        throw new Error("owner exploded");
      },
      onViolation: (): void => {},
      onClose: (error: Error | null): void => {
        closed.push(error);
      },
    });

    fake.emit("data", encodedFrame(FRAME_PING, 1));
    expect(conn.closed).toBe(true);
    expect(closed[0]?.message).toBe("owner exploded");
  });

  it("wraps a non-error throw from the frame handler", async () => {
    const fake: FakeSocket = new FakeSocket();
    const closed: (Error | null)[] = [];
    new FramedConn(fake as unknown as Socket, {
      onFrame: (): void => {
        throw "owner string";
      },
      onViolation: (): void => {},
      onClose: (error: Error | null): void => {
        closed.push(error);
      },
    });

    fake.emit("data", encodedFrame(FRAME_PING, 1));
    expect(closed[0]?.message).toBe("owner string");
  });

  it("tolerates a timer implementation without unref", async () => {
    const fake: FakeSocket = new FakeSocket();
    const { handlers } = collectHandlers();
    const conn: FramedConn = new FramedConn(fake as unknown as Socket, handlers, {
      writeTimeoutMs: 5000,
    });

    vi.stubGlobal("setTimeout", (): NodeJS.Timeout => ({}) as unknown as NodeJS.Timeout);
    vi.stubGlobal("clearTimeout", (): void => {});
    conn.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 1, body: null });
    await Promise.resolve();
    vi.unstubAllGlobals();
    conn.destroy();
  });
});

describe("timers hardening", () => {
  it("caches the clock within a microtask burst", async () => {
    const first: number = defaultTimers.now();
    expect(defaultTimers.now()).toBe(first);
    await sleep(5);
    expect(defaultTimers.now()).toBeGreaterThanOrEqual(first);
  });

  it("schedules, fires, and cancels", async () => {
    let fired: number = 0;
    defaultTimers.schedule(5, (): void => {
      fired += 1;
    });
    const canceled: TimerHandle = defaultTimers.schedule(5, (): void => {
      fired += 100;
    });
    canceled.cancel();
    await sleep(30);
    expect(fired).toBe(1);
  });

  it("tolerates a runtime whose timers have no unref", () => {
    vi.stubGlobal("setTimeout", (): NodeJS.Timeout => ({}) as unknown as NodeJS.Timeout);
    vi.stubGlobal("clearTimeout", (): void => {});
    const handle: TimerHandle = defaultTimers.schedule(1, (): void => {});
    handle.cancel();
    vi.unstubAllGlobals();
  });
});

describe("session hardening", () => {
  it("refuses every send once the session is gone", async () => {
    const server: NetServer = await startServer();
    const client: Session = await dialSession(server.address.port);
    client.destroy();

    expect(client.send({ type: FRAME_PING, flags: 0, lane: 0, correlation: 1, body: null })).toBe(
      ErrConnClosed,
    );
    expect(client.tell(envelopeOf(KIND_TELL, 4))).toBe(ErrConnClosed);
    await expect(client.ask(envelopeOf(KIND_ASK, 4), 100)).rejects.toBe(ErrConnClosed);
    expect(client.reply(1, { serializerId: SERIALIZER_BINARY, typeRef: "", payload: EMPTY })).toBe(
      ErrConnClosed,
    );
    expect(
      client.replyError(1, { code: ERROR_UNAVAILABLE, sentinel: 0, name: "", message: "x" }),
    ).toBe(ErrConnClosed);
    expect(client.effective).toBeNull();
  });

  /** Reduces split chunks to the injection shape the helper takes. */
  function asInjected(chunks: OutboundFrame[]): {
    type: number;
    flags: number;
    correlation: number;
    body: Uint8Array | null;
  }[] {
    return chunks.map(
      (
        chunk: OutboundFrame,
      ): { type: number; flags: number; correlation: number; body: Uint8Array | null } => ({
        type: chunk.type,
        flags: chunk.flags,
        correlation: chunk.correlation,
        body: chunk.body,
      }),
    );
  }

  it("kills the connection on CHUNK below revision 2", async () => {
    await expectProtocolDeath(
      { revision: 1 },
      [{ type: FRAME_CHUNK, flags: FLAG_FIRST_CHUNK, correlation: 5, body: Uint8Array.from([0]) }],
      "revision 2",
    );
  });

  it("kills the connection on a chunk sequence violation", async () => {
    const chunks: OutboundFrame[] = splitLogicalFrame(
      { type: FRAME_DATA, flags: 0, lane: LANE_CONTROL, correlation: 0, body: new Uint8Array(90) },
      48,
      5,
    );
    expect(chunks.length).toBeGreaterThan(2);
    const gapped: OutboundFrame[] = [chunks[0] as OutboundFrame, chunks[2] as OutboundFrame];
    await expectProtocolDeath({}, asInjected(gapped), "expected");
  });

  it("kills the connection when a reassembled frame lies about its lane", async () => {
    // The inner frame claims lane 7; the chunks travel on the
    // connection lane, so reassembly must catch the lie.
    const chunks: OutboundFrame[] = splitLogicalFrame(
      { type: FRAME_DATA, flags: 0, lane: 7, correlation: 0, body: new Uint8Array(60) },
      48,
      6,
    );
    await expectProtocolDeath({}, asInjected(chunks), "lane");
  });

  it("kills the connection when a reassembled frame is not data or reply", async () => {
    const chunks: OutboundFrame[] = splitLogicalFrame(
      { type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 0, body: new Uint8Array(60) },
      48,
      7,
    );
    await expectProtocolDeath({}, asInjected(chunks), "unexpected chunked frame type");
  });

  it("settles an ask with the decode failure when the peer's error is garbage", async () => {
    const { client, peer } = await dialScripted({});
    const pending: Promise<ReplyEnvelope> = client.ask(envelopeOf(KIND_ASK, 4), 5000);
    await vi.waitFor((): void => {
      expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_DATA)).toBe(true);
    });

    const ask = peer.frames.find((frame): boolean => frame.header.type === FRAME_DATA);
    peer.send({
      type: FRAME_ERROR,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: ask?.header.correlation ?? 0,
      body: Uint8Array.from([0x00]),
    });

    await expect(pending).rejects.toThrow(ValueDecodeError);
    // The failure was request-scoped: the connection lives on.
    expect(client.closed).toBe(false);
  });

  it("rejects the dial when the HELLO_ACK is garbage or below baseline", async () => {
    const garbagePort: number = await startRawListener((socket: Socket): void => {
      const raw: RawPeer = new RawPeer(socket);
      raw.onFirstFrame((): void => {
        raw.send({
          type: FRAME_HELLO_ACK,
          flags: 0,
          lane: LANE_CONTROL,
          correlation: 0,
          body: Uint8Array.from([0xff]),
        });
      });
    });
    await expect(Session.dial(dialSocket(garbagePort), hello())).rejects.toThrow(ProtocolError);

    const zeroPort: number = await startRawListener((socket: Socket): void => {
      const raw: RawPeer = new RawPeer(socket);
      raw.onFirstFrame((): void => {
        raw.sendHello(FRAME_HELLO_ACK, hello({ revision: 0 }));
      });
    });
    await expect(Session.dial(dialSocket(zeroPort), hello())).rejects.toThrow(
      /revision below baseline/,
    );
  });

  it("tears down exactly once no matter how often close is called", async () => {
    const server: NetServer = await startServer();
    const closes: (Error | null)[] = [];
    const client: Session = await dialSession(server.address.port, hello(), {
      onClose: (_session: Session, error: Error | null): void => {
        closes.push(error);
      },
    });

    client.close();
    client.close();
    client.closeWith(ERROR_UNAVAILABLE, "going away");
    await vi.waitFor((): void => {
      expect(client.closed).toBe(true);
    });
    expect(closes).toEqual([null]);
  });

  it("falls back to the drain grace when no write timeout is configured", async () => {
    const server: NetServer = await startServer();
    const client: Session = await dialSession(
      server.address.port,
      hello(),
      {},
      {
        writeTimeoutMs: 0,
      },
    );
    client.close();
    await vi.waitFor((): void => {
      expect(client.closed).toBe(true);
    });
  });

  it("force-closes a half-open peer after the grace, timers quiet while closing", async () => {
    let raw: RawPeer | null = null;
    const listener: Server = createServer({ allowHalfOpen: true }, (socket: Socket): void => {
      const scripted: RawPeer = new RawPeer(socket);
      raw = scripted;
      scripted.onFirstFrame((): void => {
        const request: Hello = decodeHello(new ByteReader(scripted.frames[0]?.body ?? EMPTY));
        scripted.sendHello(
          FRAME_HELLO_ACK,
          negotiateHello(hello({ systemName: "half-open" }), request),
        );
      });
    });

    await new Promise<void>((resolve): void => {
      listener.listen(0, "127.0.0.1", resolve);
    });
    const port: number = (listener.address() as { port: number }).port;

    const client: Session = await Session.dial(
      trackSocket(connect(port, "127.0.0.1")),
      hello({ systemName: "client" }),
      {},
      { writeTimeoutMs: 150, readIdleMs: 40, connIdleMs: 60 },
    );
    trackSession(client);
    expect(raw).not.toBeNull();

    client.close();
    await vi.waitFor(
      (): void => {
        expect(client.closed).toBe(true);
      },
      { timeout: 2000 },
    );
    listener.close();
  });

  it("re-arms liveness and reclaim while traffic keeps arriving", async () => {
    // The clock is manual: deadlines fire exactly when the test
    // advances past them, so nothing here races the real clock.
    const clock: ManualTimers = new ManualTimers();
    let closeError: Error | null = null;
    const { client, peer } = await dialScripted(
      {},
      {
        onClose: (_session: Session, error: Error | null): void => {
          closeError = error;
        },
      },
      { readIdleMs: 0, connIdleMs: 200, timers: clock },
    );

    clock.advance(150);
    peer.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 3, body: null });
    await vi.waitFor((): void => {
      expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_PONG)).toBe(true);
    });

    // The reclaim deadline fires at 200 but traffic arrived at 150,
    // so it re-arms instead of closing.
    clock.advance(50);
    expect(client.closed).toBe(false);

    clock.advance(200);
    await vi.waitFor((): void => {
      expect(closeError).toBe(ErrIdleReclaim);
    });
  });

  it("reschedules a liveness probe that raced fresh traffic", async () => {
    const clock: ManualTimers = new ManualTimers();
    const { client, peer } = await dialScripted({}, {}, { readIdleMs: 100, timers: clock });

    clock.advance(60);
    peer.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 3, body: null });
    await vi.waitFor((): void => {
      expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_PONG)).toBe(true);
    });

    // The probe deadline fires at 100 but traffic arrived at 60, so
    // the client only reschedules; no probe goes out yet.
    clock.advance(40);
    expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_PING)).toBe(false);

    // Quiet through the rescheduled deadline: now the client probes.
    clock.advance(60);
    await vi.waitFor((): void => {
      expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_PING)).toBe(true);
    });
    expect(client.closed).toBe(false);
  });

  it("delivers below revision 4 without a credit window", async () => {
    const { client, peer } = await dialScripted({ revision: 3 });
    expect(client.tell(envelopeOf(KIND_TELL, 8))).toBeNull();
    await vi.waitFor((): void => {
      expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_DATA)).toBe(true);
    });
  });

  it("confirms a chunked tell once its last frame is written", async () => {
    const confirmed: unknown[] = [];
    const { client, peer } = await dialScripted(
      {},
      {
        onWrote: (_session: Session, token: unknown): void => {
          confirmed.push(token);
        },
      },
      { chunkSize: 1024 },
    );
    expect(client.tell(envelopeOf(KIND_TELL, 4000), "chunked-token")).toBeNull();

    await vi.waitFor((): void => {
      expect(confirmed).toEqual(["chunked-token"]);
    });
    expect(
      peer.frames.filter((frame): boolean => frame.header.type === FRAME_CHUNK).length,
    ).toBeGreaterThan(1);
  });

  it("silently drops an undecodable tell", async () => {
    const { client, peer } = await dialScripted({});
    peer.send({
      type: FRAME_DATA,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 0,
      body: Uint8Array.from([9, 9, 9]),
    });

    await sleep(50);
    expect(client.closed).toBe(false);
    expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_ERROR)).toBe(false);
  });

  it("speaks plain inline envelopes below revision 3", async () => {
    const { client, peer } = await dialScripted({ revision: 2 });

    // No tables: refs stay inline in both directions, and the handle
    // cache has nothing to pin to.
    expect(client.cachePathHandle("/user/a", {})).toBe(false);
    expect(client.pathHandle("/user/a")).toBeUndefined();
    expect(
      client.reply(9, { serializerId: SERIALIZER_BINARY, typeRef: "test.R", payload: EMPTY }),
    ).toBeNull();
    await vi.waitFor((): void => {
      expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_REPLY)).toBe(true);
    });

    const pending: Promise<ReplyEnvelope> = client.ask(envelopeOf(KIND_ASK, 4), 5000);
    await vi.waitFor((): void => {
      expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_DATA)).toBe(true);
    });
    const ask = peer.frames.find((frame): boolean => frame.header.type === FRAME_DATA);
    const reply: ByteWriter = new ByteWriter();
    encodeReplyEnvelope(reply, {
      serializerId: SERIALIZER_BINARY,
      typeRef: "test.EdgeReply",
      payload: Uint8Array.from([1]),
    });
    peer.send({
      type: FRAME_REPLY,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: ask?.header.correlation ?? 0,
      body: Uint8Array.from(reply.bytes()),
    });
    expect((await pending).typeRef).toBe("test.EdgeReply");
  });

  it("settles an ask request-scoped when the reply body is garbage", async () => {
    const { client, peer } = await dialScripted({});
    const pending: Promise<ReplyEnvelope> = client.ask(envelopeOf(KIND_ASK, 4), 5000);
    await vi.waitFor((): void => {
      expect(peer.frames.some((frame): boolean => frame.header.type === FRAME_DATA)).toBe(true);
    });

    const ask = peer.frames.find((frame): boolean => frame.header.type === FRAME_DATA);
    peer.send({
      type: FRAME_REPLY,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: ask?.header.correlation ?? 0,
      body: Uint8Array.from([0x07]),
    });

    await expect(pending).rejects.toThrow(ValueDecodeError);
    expect(client.closed).toBe(false);
  });

  it("kills an acceptor handshake that opens with anything but HELLO", async () => {
    const server: NetServer = await startServer();
    const raw: RawPeer = new RawPeer(dialSocket(server.address.port));
    raw.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 1, body: null });

    await vi.waitFor((): void => {
      expect(raw.frames.some((frame): boolean => frame.header.type === FRAME_ERROR)).toBe(true);
    });
  });

  it("rejects a dial answered with anything but HELLO_ACK", async () => {
    const port: number = await startRawListener((socket: Socket): void => {
      const raw: RawPeer = new RawPeer(socket);
      raw.onFirstFrame((): void => {
        raw.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 1, body: null });
      });
    });

    await expect(Session.dial(dialSocket(port), hello())).rejects.toThrow(/expected HELLO_ACK/);
  });

  it("opens with the handshake deadline disabled", async () => {
    const port: number = await startRawListener((socket: Socket): void => {
      const raw: RawPeer = new RawPeer(socket);
      raw.onFirstFrame((): void => {
        raw.sendHello(FRAME_HELLO_ACK, hello({ systemName: "acceptor" }));
      });
    });
    const client: Session = await Session.dial(
      dialSocket(port),
      hello(),
      {},
      {
        handshakeTimeoutMs: 0,
      },
    );
    trackSession(client);
    expect(client.closed).toBe(false);
  });

  it("waits indefinitely on an ask with no timeout", async () => {
    const server: NetServer = await startServer(
      {},
      {
        onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
          session.reply(correlation, {
            serializerId: SERIALIZER_BINARY,
            typeRef: "test.EdgeReply",
            payload: Uint8Array.from(envelope.payload),
          });
        },
      },
    );
    const client: Session = await dialSession(server.address.port);
    const reply: ReplyEnvelope = await client.ask(envelopeOf(KIND_ASK, 4), 0);
    expect(reply.typeRef).toBe("test.EdgeReply");
  });
});

describe("server hardening", () => {
  it("destroys connections still mid-handshake at shutdown", async () => {
    const server: NetServer = await startServer();
    dialSocket(server.address.port);
    await sleep(40);
    expect(server.activeConnections).toBe(1);

    await server.shutdown(-1);
    expect(server.activeConnections).toBe(0);
  });

  it("destroys stragglers once the shutdown grace expires", async () => {
    const server: NetServer = await startServer();
    const socket: Socket = trackSocket(
      connect({ port: server.address.port, host: "127.0.0.1", allowHalfOpen: true }),
    );
    const raw: RawPeer = new RawPeer(socket);
    raw.sendHello(FRAME_HELLO, hello({ systemName: "clingy" }));
    await vi.waitFor((): void => {
      expect(raw.frames[0]?.header.type).toBe(FRAME_HELLO_ACK);
    });
    expect(server.activeConnections).toBe(1);

    await server.shutdown(200);
    expect(server.activeConnections).toBe(0);
  });

  it("shuts down under a timer implementation without unref", async () => {
    const server: NetServer = await startServer();
    vi.stubGlobal("setTimeout", (): NodeJS.Timeout => ({}) as unknown as NodeJS.Timeout);
    const done: Promise<void> = server.shutdown(50);
    vi.unstubAllGlobals();
    await done;
    expect(server.activeConnections).toBe(0);
  });

  it("survives a listener-level error after bind and reports it", async () => {
    // An accept failure such as fd exhaustion emits 'error' on the
    // listener; with no handler attached Node would throw it as an
    // uncaught exception and kill the process.
    interface ErrorEmitter {
      readonly _listener: { emit(name: string, error: Error): boolean };
    }

    const seen: Error[] = [];
    const reporting: NetServer = await startServer(
      {},
      {
        onError: (error: Error): void => {
          seen.push(error);
        },
      },
    );
    (reporting as unknown as ErrorEmitter)._listener.emit("error", new Error("accept failed"));
    expect(seen.map((error): string => error.message)).toEqual(["accept failed"]);

    const silent: NetServer = await startServer();
    (silent as unknown as ErrorEmitter)._listener.emit("error", new Error("accept failed"));
    const client: Session = await dialSession(silent.address.port);
    expect(client.closed).toBe(false);
  });
});

describe("peer hardening", () => {
  it("refuses an ordinary lane count outside the protocol range", () => {
    for (const lanes of [0, 255, 1.5]) {
      expect((): Peer => new Peer("127.0.0.1", 1, hello(), {}, { ordinaryLanes: lanes })).toThrow(
        TypeError,
      );
    }
  });

  it("dead-letters a tell the session refuses outright", async () => {
    const server: NetServer = await startServer();
    const reasons: Error[] = [];
    const peer: Peer = trackPeer(
      new Peer(
        "127.0.0.1",
        server.address.port,
        hello({ systemName: "peer", maxFrameSize: 16 * 1024, maxMessageSize: 32 * 1024 }),
        {
          onDeadLetter: (_envelope: DataEnvelope, reason: Error): void => {
            reasons.push(reason);
          },
        },
      ),
    );

    peer.tell(envelopeOf(KIND_TELL, 40 * 1024));
    await vi.waitFor((): void => {
      expect(reasons).toEqual([ErrMessageTooLarge]);
    });
  });

  it("hands inbound envelopes on a dialed lane to its owner", async () => {
    const inbound: DataEnvelope[] = [];
    const server: NetServer = await startServer(
      {},
      {
        onSession: (session: Session): void => {
          session.tell(envelopeOf(KIND_TELL, 6));
        },
      },
    );
    const peer: Peer = trackPeer(
      new Peer("127.0.0.1", server.address.port, hello({ systemName: "peer" }), {
        onData: (_session: Session, envelope: DataEnvelope): void => {
          inbound.push(envelope);
        },
      }),
    );

    peer.tell(envelopeOf(KIND_TELL, 4));
    await vi.waitFor((): void => {
      expect(inbound.length).toBe(1);
    });
    expect(inbound[0]?.payload.length).toBe(6);
  });
});
