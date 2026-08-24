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
  ErrConnClosed,
  ErrWriteTimeout,
  FramedConn,
  type FramedConnOptions,
  type OutboundFrame,
} from "../../src/net/conn";
import {
  encodeFrameHeader,
  FLAG_EXPECTS_REPLY,
  FRAME_DATA,
  FRAME_PING,
  type FrameHeader,
  LANE_CONTROL,
  ProtocolError,
} from "../../src/net/frame";
import { ByteWriter } from "../../src/net/values";
import { cleanupNet, sleep, socketPair } from "./helpers";

afterEach(cleanupNet);

interface Recorded {
  readonly header: FrameHeader;
  readonly body: Uint8Array;
}

/** Records everything a FramedConn reports, copying aliased bodies. */
class Recorder {
  readonly frames: Recorded[] = [];
  violation: ProtocolError | null = null;
  closeError: Error | null = null;
  closeCount: number = 0;
  readonly conn: FramedConn;
  onFrameHook: ((header: FrameHeader) => void) | null = null;

  constructor(socket: Socket, options: FramedConnOptions = {}) {
    this.conn = new FramedConn(
      socket,
      {
        onFrame: (header: FrameHeader, body: Uint8Array): void => {
          this.frames.push({ header, body: Uint8Array.from(body) });
          if (this.onFrameHook !== null) {
            this.onFrameHook(header);
          }
        },
        onViolation: (error: ProtocolError): void => {
          this.violation = error;
        },
        onClose: (error: Error | null): void => {
          this.closeError = error;
          this.closeCount += 1;
        },
      },
      options,
    );
  }
}

function ping(correlation: number): OutboundFrame {
  return { type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation, body: null };
}

function data(correlation: number, body: Uint8Array): OutboundFrame {
  return {
    type: FRAME_DATA,
    flags: correlation === 0 ? 0 : FLAG_EXPECTS_REPLY,
    lane: LANE_CONTROL,
    correlation,
    body,
  };
}

describe("FramedConn round-trips", () => {
  it("delivers frames in order, both directions at once", async () => {
    const { client, server } = await socketPair();
    const sender: Recorder = new Recorder(client, { maxFrameSize: 1024 * 1024 });
    const receiver: Recorder = new Recorder(server, { maxFrameSize: 1024 * 1024 });

    expect(sender.conn.send(ping(0))).toBeNull();
    expect(sender.conn.send(data(1, Uint8Array.from([1, 2, 3])))).toBeNull();
    expect(receiver.conn.send(data(9, Uint8Array.from([9])))).toBeNull();

    await vi.waitFor((): void => {
      expect(receiver.frames.length).toBe(2);
      expect(sender.frames.length).toBe(1);
    });
    expect(receiver.frames[0]?.header.type).toBe(FRAME_PING);
    expect(receiver.frames[1]?.header.correlation).toBe(1);
    expect(Array.from(receiver.frames[1]?.body ?? [])).toEqual([1, 2, 3]);
    expect(sender.frames[0]?.header.correlation).toBe(9);
  });

  it("coalesces a burst of small frames into one socket write", async () => {
    const { client, server } = await socketPair();
    const receiver: Recorder = new Recorder(server, {});
    let writes: number = 0;
    const originalWrite: typeof client.write = client.write.bind(client);
    client.write = ((chunk: Uint8Array, callback?: () => void): boolean => {
      writes += 1;
      return originalWrite(chunk, callback);
    }) as typeof client.write;
    const sender: Recorder = new Recorder(client, {});

    for (let i = 0; i < 20; i++) {
      sender.conn.send(ping(0));
    }

    await vi.waitFor((): void => {
      expect(receiver.frames.length).toBe(20);
    });
    expect(writes).toBe(1);
    await vi.waitFor((): void => {
      expect(sender.conn.outstandingBytes).toBe(0);
    });
  });

  it("assembles a body larger than any single chunk", async () => {
    const { client, server } = await socketPair();
    const receiver: Recorder = new Recorder(server, { maxFrameSize: 1024 * 1024 });
    const sender: Recorder = new Recorder(client, { maxFrameSize: 1024 * 1024 });
    const body: Uint8Array = new Uint8Array(300 * 1024);
    for (let i = 0; i < body.length; i++) {
      body[i] = i % 251;
    }

    sender.conn.send(data(0, body));
    await vi.waitFor((): void => {
      expect(receiver.frames.length).toBe(1);
    });
    expect(receiver.frames[0]?.body).toEqual(body);
  });
});

describe("FramedConn split delivery", () => {
  it("parses frames arriving one slice at a time", async () => {
    const { client, server } = await socketPair();
    const receiver: Recorder = new Recorder(server, {});

    const writer: ByteWriter = new ByteWriter();
    encodeFrameHeader(writer, {
      type: FRAME_PING,
      flags: 0,
      lane: LANE_CONTROL,
      length: 0,
      correlation: 5,
    });
    const payload: Uint8Array = Uint8Array.from([104, 101, 108, 108, 111]);
    encodeFrameHeader(writer, {
      type: FRAME_DATA,
      flags: FLAG_EXPECTS_REPLY,
      lane: LANE_CONTROL,
      length: payload.length,
      correlation: 1,
    });
    writer.writeBytes(payload);
    const stream: Uint8Array = Uint8Array.from(writer.bytes());

    const cuts: number[] = [0, 5, 13, 16, 20, 30, 34, stream.length];
    for (let i = 1; i < cuts.length; i++) {
      client.write(stream.subarray(cuts[i - 1] as number, cuts[i] as number));
      await sleep(10);
    }

    await vi.waitFor((): void => {
      expect(receiver.frames.length).toBe(2);
    });
    expect(receiver.frames[0]?.header.correlation).toBe(5);
    expect(Array.from(receiver.frames[1]?.body ?? [])).toEqual([104, 101, 108, 108, 111]);
  });

  it("parses two frames arriving in one chunk", async () => {
    const { client, server } = await socketPair();
    const receiver: Recorder = new Recorder(server, {});

    const writer: ByteWriter = new ByteWriter();
    for (const correlation of [1, 2]) {
      encodeFrameHeader(writer, {
        type: FRAME_PING,
        flags: 0,
        lane: LANE_CONTROL,
        length: 0,
        correlation,
      });
    }

    client.write(Uint8Array.from(writer.bytes()));
    await vi.waitFor((): void => {
      expect(receiver.frames.length).toBe(2);
    });
  });
});

describe("FramedConn violations", () => {
  it("reports a bad version and stops parsing", async () => {
    const { client, server } = await socketPair();
    const receiver: Recorder = new Recorder(server, {});

    const garbage: Uint8Array = new Uint8Array(16);
    garbage[0] = 0x99;
    client.write(garbage);
    await vi.waitFor((): void => {
      expect(receiver.violation).toBeInstanceOf(ProtocolError);
    });

    const writer: ByteWriter = new ByteWriter();
    encodeFrameHeader(writer, {
      type: FRAME_PING,
      flags: 0,
      lane: LANE_CONTROL,
      length: 0,
      correlation: 0,
    });
    client.write(Uint8Array.from(writer.bytes()));
    await sleep(50);
    expect(receiver.frames.length).toBe(0);
  });

  it("reports an oversize frame without reading its body", async () => {
    const { client, server } = await socketPair();
    const receiver: Recorder = new Recorder(server, { maxFrameSize: 1024 });
    const sender: Recorder = new Recorder(client, {});

    sender.conn.send(data(0, new Uint8Array(2048)));
    await vi.waitFor((): void => {
      expect(receiver.violation).toBeInstanceOf(ProtocolError);
    });
    expect(receiver.frames.length).toBe(0);
  });
});

describe("FramedConn failure handling", () => {
  it("tears down when a write makes no progress within the timeout", async () => {
    const { client, server } = await socketPair();
    void server;
    const sender: Recorder = new Recorder(client, { writeTimeoutMs: 150 });

    for (let i = 0; i < 4; i++) {
      sender.conn.send(data(0, new Uint8Array(2 * 1024 * 1024)));
    }

    await vi.waitFor(
      (): void => {
        expect(sender.closeError).toBe(ErrWriteTimeout);
      },
      { timeout: 5000 },
    );
    expect(sender.conn.send(ping(0))).toBe(ErrConnClosed);
  });

  it("rides out backpressure when the peer resumes reading", async () => {
    const { client, server } = await socketPair();
    const sender: Recorder = new Recorder(client, {});
    const total: number = 64;
    for (let i = 0; i < total; i++) {
      sender.conn.send(data(0, new Uint8Array(16 * 1024)));
    }

    await sleep(100);
    const receiver: Recorder = new Recorder(server, { maxFrameSize: 1024 * 1024 });
    await vi.waitFor(
      (): void => {
        expect(receiver.frames.length).toBe(total);
      },
      { timeout: 5000 },
    );
    await vi.waitFor((): void => {
      expect(sender.conn.outstandingBytes).toBe(0);
    });
    expect(sender.closeError).toBeNull();
    expect(sender.closeCount).toBe(0);
  });

  it("closes exactly once when the peer vanishes mid-stream", async () => {
    const { client, server } = await socketPair();
    const sender: Recorder = new Recorder(client, {});
    const receiver: Recorder = new Recorder(server, {});

    sender.conn.send(ping(0));
    await vi.waitFor((): void => {
      expect(receiver.frames.length).toBe(1);
    });

    server.destroy();
    await vi.waitFor((): void => {
      expect(sender.closeCount).toBe(1);
      expect(receiver.closeCount).toBe(1);
    });
    expect(sender.conn.send(ping(0))).toBe(ErrConnClosed);
  });

  it("contains a throwing frame handler as a connection failure", async () => {
    const { client, server } = await socketPair();
    const sender: Recorder = new Recorder(client, {});
    const receiver: Recorder = new Recorder(server, {});
    receiver.onFrameHook = (): void => {
      throw new Error("handler boom");
    };

    sender.conn.send(ping(0));
    await vi.waitFor((): void => {
      expect(receiver.closeCount).toBe(1);
    });
    expect(receiver.closeError?.message).toBe("handler boom");
  });

  it("flushes queued frames before a graceful end", async () => {
    const { client, server } = await socketPair();
    const sender: Recorder = new Recorder(client, {});
    const receiver: Recorder = new Recorder(server, {});

    for (let i = 0; i < 10; i++) {
      sender.conn.send(ping(0));
    }

    sender.conn.end();
    expect(sender.conn.send(ping(0))).toBe(ErrConnClosed);

    await vi.waitFor((): void => {
      expect(receiver.frames.length).toBe(10);
      expect(receiver.closeCount).toBe(1);
    });
    expect(receiver.closeError).toBeNull();
  });
});
