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
import type { OutboundFrame } from "../../src/_net/conn";
import { CreditWindow } from "../../src/_net/credit";
import {
  type DataEnvelope,
  decodeHello,
  encodeDataEnvelope,
  type Hello,
  KIND_ASK,
  KIND_TELL,
  type RefInterner,
  SERIALIZER_BINARY,
} from "../../src/_net/envelope";
import {
  FRAME_CREDIT,
  FRAME_DATA,
  FRAME_HEADER_SIZE,
  FRAME_HELLO,
  FRAME_HELLO_ACK,
  FRAME_PING,
  FRAME_PONG,
  type FrameHeader,
  LANE_CONTROL,
} from "../../src/_net/frame";
import type { NetServer } from "../../src/_net/server";
import { ErrBackpressure, negotiateHello, type Session } from "../../src/_net/session";
import { ByteReader, ByteWriter } from "../../src/_net/values";
import {
  cleanupNet,
  dialScripted,
  dialSession,
  dialSocket,
  EMPTY,
  expectProtocolDeath,
  hello,
  RawPeer,
  sleep,
  startRawListener,
  startServer,
} from "./helpers";

afterEach(cleanupNet);

function frameOf(bodyLength: number): OutboundFrame {
  return {
    type: FRAME_DATA,
    flags: 0,
    lane: LANE_CONTROL,
    correlation: 0,
    body: new Uint8Array(bodyLength),
  };
}

function uvarint(value: number): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  writer.writeUvarint(value);
  return Uint8Array.from(writer.bytes());
}

function envelopeOf(kind: number, payloadLength: number): DataEnvelope {
  return {
    kind,
    to: "nodeakt://orders@10.0.0.5:5100/user/charger",
    uid: "b3f2",
    sender: "",
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: "test.Fill",
    payload: new Uint8Array(payloadLength),
  };
}

/**
 * The exact wire cost of a tell carrying `payloadLength` bytes on a
 * revision-4 connection, where nonempty refs travel as interned ids.
 */
function tellCost(payloadLength: number): number {
  const interned: RefInterner = {
    pathId: (literal: string): number => (literal === "" ? 0 : 1),
    typeId: (literal: string): number => (literal === "" ? 0 : 1),
  };
  const writer: ByteWriter = new ByteWriter();
  encodeDataEnvelope(writer, envelopeOf(KIND_TELL, payloadLength), interned);
  return FRAME_HEADER_SIZE + writer.length;
}

function dataFrames(peer: RawPeer): number {
  return peer.frames.filter((frame): boolean => frame.header.type === FRAME_DATA).length;
}

/**
 * Sends tells against a peer that never grants until admission
 * durably refuses: each round drains to the kernel first, so the
 * window empties and then the credit queue fills.
 */
async function stallWindow(
  client: Session,
  envelope: DataEnvelope,
): Promise<{ accepted: number; refusal: Error | null }> {
  let accepted: number = 0;
  let refusal: Error | null = null;
  let idle: boolean = false;
  for (let round = 0; round < 40 && !idle; round++) {
    idle = true;
    while (accepted < 5000) {
      const result: Error | null = client.tell(envelope);
      if (result !== null) {
        refusal = result;
        break;
      }

      accepted += 1;
      idle = false;
    }

    await sleep(10);
  }

  return { accepted, refusal };
}

describe("credit window", () => {
  function windowOf(capacity: number): { window: CreditWindow; written: OutboundFrame[] } {
    const written: OutboundFrame[] = [];
    const window: CreditWindow = new CreditWindow(
      capacity,
      (frame: OutboundFrame): Error | null => {
        written.push(frame);
        return null;
      },
    );
    return { window, written };
  }

  it("spends, parks, and drains strictly in order", () => {
    const { window, written } = windowOf(100);

    expect(window.send(frameOf(50))).toBeNull();
    expect(written.length).toBe(1);
    expect(window.available).toBe(34);

    window.send(frameOf(50));
    expect(written.length).toBe(1);
    expect(window.queuedBytes).toBe(66);

    // An affordable frame must still wait behind the queue head.
    window.send(frameOf(0));
    expect(written.length).toBe(1);
    expect(window.queuedBytes).toBe(82);

    window.grant(66);
    expect(written.length).toBe(3);
    expect(written[1]?.body?.length).toBe(50);
    expect(written[2]?.body?.length).toBe(0);
    expect(window.queuedBytes).toBe(0);
    expect(window.available).toBe(18);
  });

  it("holds a parked frame through a partial grant", () => {
    const { window, written } = windowOf(100);
    window.send(frameOf(50));
    window.send(frameOf(50));
    expect(written.length).toBe(1);

    window.grant(20);
    expect(written.length).toBe(1);
    expect(window.queuedBytes).toBe(66);

    window.grant(46);
    expect(written.length).toBe(2);
  });

  it("lets one frame larger than the whole window through an empty pipe", () => {
    const { window, written } = windowOf(100);

    expect(window.send(frameOf(134))).toBeNull();
    expect(written.length).toBe(1);
    expect(window.available).toBe(-50);

    // Even a tiny frame parks behind an overdrawn window.
    window.send(frameOf(4));
    expect(written.length).toBe(1);

    // Grants clamp at capacity, then the queue drains.
    window.grant(1000);
    expect(written.length).toBe(2);
    expect(window.available).toBe(80);
  });

  it("releases a parked oversize frame only once fully replenished", () => {
    const { window, written } = windowOf(100);
    window.send(frameOf(134));
    window.grant(75);
    expect(window.available).toBe(25);

    window.send(frameOf(134));
    expect(written.length).toBe(1);

    window.grant(75);
    expect(written.length).toBe(2);
    expect(window.available).toBe(-50);
  });

  it("batches owed grants at a quarter window", () => {
    const { window } = windowOf(100);

    expect(window.accrue(10)).toBe(0);
    expect(window.accrue(10)).toBe(0);
    expect(window.accrue(10)).toBe(30);
    expect(window.accrue(24)).toBe(0);
    expect(window.accrue(1)).toBe(25);
  });

  it("prices a bodiless frame at one header", () => {
    const { window, written } = windowOf(100);
    window.send({ type: FRAME_DATA, flags: 0, lane: LANE_CONTROL, correlation: 0, body: null });
    expect(written.length).toBe(1);
    expect(window.available).toBe(100 - FRAME_HEADER_SIZE);
  });

  it("drops parked frames on clear", () => {
    const { window, written } = windowOf(100);
    window.send(frameOf(80));
    window.send(frameOf(80));
    expect(window.queuedBytes).toBe(96);

    window.clear();
    expect(window.queuedBytes).toBe(0);

    window.grant(1000);
    expect(written.length).toBe(1);
  });
});

describe("credit grants over the wire", () => {
  /** Handshakes a raw sender against a counting server. */
  async function rawSender(window: number): Promise<RawPeer> {
    const server: NetServer = await startServer();
    const peer: RawPeer = new RawPeer(dialSocket(server.address.port));
    peer.sendHello(FRAME_HELLO, hello({ systemName: "raw", initialCredits: window }));
    await vi.waitFor((): void => {
      expect(peer.frames[0]?.header.type).toBe(FRAME_HELLO_ACK);
    });
    return peer;
  }

  function creditFrames(peer: RawPeer): { header: FrameHeader; body: Uint8Array }[] {
    return peer.frames.filter((frame): boolean => frame.header.type === FRAME_CREDIT);
  }

  it("flushes one batched CREDIT once a quarter window is owed", async () => {
    const window: number = 64 * 1024;
    const peer: RawPeer = await rawSender(window);

    const writer: ByteWriter = new ByteWriter();
    encodeDataEnvelope(writer, envelopeOf(KIND_TELL, 4000));
    const body: Uint8Array = Uint8Array.from(writer.bytes());
    const cost: number = FRAME_HEADER_SIZE + body.length;
    const needed: number = Math.ceil(Math.floor(window / 4) / cost);

    // One burst, well inside the repayment deadline: the threshold
    // flushes a single exact grant and cancels the pending deadline.
    for (let i = 0; i < needed; i++) {
      peer.send({
        type: FRAME_DATA,
        flags: 0,
        lane: LANE_CONTROL,
        correlation: 0,
        body: Uint8Array.from(body),
      });
    }

    await vi.waitFor((): void => {
      expect(creditFrames(peer).length).toBe(1);
    });
    expect(new ByteReader(creditFrames(peer)[0]?.body ?? EMPTY).readUvarint()).toBe(needed * cost);
  });

  it("repays a remainder below the batch within the grant deadline", async () => {
    const window: number = 64 * 1024;
    const peer: RawPeer = await rawSender(window);

    const writer: ByteWriter = new ByteWriter();
    encodeDataEnvelope(writer, envelopeOf(KIND_TELL, 100));
    const body: Uint8Array = Uint8Array.from(writer.bytes());
    peer.send({
      type: FRAME_DATA,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 0,
      body: Uint8Array.from(body),
    });

    // A single frame never reaches the quarter-window threshold, so
    // the grant that arrives can only be the deadline repayment.
    await vi.waitFor((): void => {
      expect(creditFrames(peer).length).toBe(1);
    });
    expect(new ByteReader(creditFrames(peer)[0]?.body ?? EMPTY).readUvarint()).toBe(
      FRAME_HEADER_SIZE + body.length,
    );
  });

  it("releases a parked frame once the deadline repays a sub-batch remainder", async () => {
    // The wedge this guards against: spend a little, then park a
    // frame bigger than what is left. With repayment below the batch
    // held forever, no grant would ever arrive and the parked frame
    // would wait until teardown.
    const cap: number = 600;
    const received: number[] = [];
    const server: NetServer = await startServer(
      {},
      {
        onData: (_session: Session, envelope: DataEnvelope): void => {
          received.push(envelope.payload.length);
        },
      },
    );
    const client: Session = await dialSession(
      server.address.port,
      hello({ systemName: "client", initialCredits: cap }),
    );

    const base: number = tellCost(0);
    expect(client.tell(envelopeOf(KIND_TELL, 120 - base))).toBeNull();
    await vi.waitFor((): void => {
      expect(received.length).toBe(1);
    });

    // Spent 120 of 600: the receiver owes less than the 150-byte
    // batch, and this 540-byte frame can neither afford the remaining
    // 480 nor ride the full-replenishment allowance.
    expect(client.tell(envelopeOf(KIND_TELL, 540 - base))).toBeNull();
    await vi.waitFor((): void => {
      expect(received.length).toBe(2);
    });
  });
});

describe("credit flow control end to end", () => {
  it("bounds a sender against a throttled receiver, then drains on a grant", async () => {
    const window: number = 32 * 1024;
    const { client, peer } = await dialScripted({ initialCredits: window });
    expect(client.effective?.initialCredits).toBe(window);

    const envelope: DataEnvelope = envelopeOf(KIND_TELL, 1500);
    const cost: number = tellCost(1500);
    const { accepted, refusal } = await stallWindow(client, envelope);

    // The window's worth went to the wire, another window's worth
    // parked under admission, and everything past that was refused.
    expect(refusal).toBe(ErrBackpressure);
    expect(accepted * cost).toBeLessThanOrEqual(2 * window + 2 * cost);
    expect(accepted).toBeGreaterThanOrEqual(Math.floor(window / cost));
    await vi.waitFor((): void => {
      expect(dataFrames(peer)).toBe(Math.floor(window / cost));
    });

    // Asks share the admission budget.
    await expect(client.ask(envelopeOf(KIND_ASK, 1500), 1000)).rejects.toBe(ErrBackpressure);

    peer.send({
      type: FRAME_CREDIT,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 0,
      body: uvarint(window),
    });
    await vi.waitFor((): void => {
      expect(dataFrames(peer)).toBe(accepted);
    });
  });

  it("refuses replies over the admission budget with backpressure", async () => {
    const window: number = 8 * 1024;
    const { client } = await dialScripted({ initialCredits: window });
    await stallWindow(client, envelopeOf(KIND_TELL, 1000));

    const refused: Error | null = client.reply(9, {
      serializerId: SERIALIZER_BINARY,
      typeRef: "test.Reply",
      payload: new Uint8Array(1000),
    });
    expect(refused).toBe(ErrBackpressure);
  });

  it("keeps the local admission budget when the peer advertises zero credits", async () => {
    // A remote opting out of flow control must not be able to switch
    // off this side's memory bound: admission falls back to the
    // locally configured budget.
    const port: number = await startRawListener((socket: Socket): void => {
      const raw: RawPeer = new RawPeer(socket);
      raw.onFirstFrame((): void => {
        const request: Hello = decodeHello(new ByteReader(raw.frames[0]?.body ?? EMPTY));
        raw.sendHello(
          FRAME_HELLO_ACK,
          negotiateHello(hello({ systemName: "zero", initialCredits: 0 }), request),
        );
      });
    });
    const client: Session = await dialSession(
      port,
      hello({ systemName: "client", initialCredits: 600 }),
    );
    expect(client.effective?.initialCredits).toBe(0);

    // One synchronous burst: the flush is a microtask away, so held
    // bytes climb deterministically until admission refuses.
    const results: (Error | null)[] = [];
    for (let i = 0; i < 40; i++) {
      results.push(client.tell(envelopeOf(KIND_TELL, 60)));
    }

    expect(results[0]).toBeNull();
    expect(results).toContain(ErrBackpressure);
  });

  it("disables admission only when the local budget is zero as well", async () => {
    const { client } = await dialScripted({ initialCredits: 0 });
    for (let i = 0; i < 30; i++) {
      expect(client.tell(envelopeOf(KIND_TELL, 60))).toBeNull();
    }
  });

  it("lets control frames overtake a queue stalled on credit", async () => {
    const window: number = 8 * 1024;
    const { client, peer } = await dialScripted({ initialCredits: window });

    const { accepted } = await stallWindow(client, envelopeOf(KIND_TELL, 1000));
    const delivered: number = dataFrames(peer);
    expect(accepted).toBeGreaterThan(delivered);

    peer.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 77, body: null });
    await vi.waitFor((): void => {
      const pong = peer.frames.find((frame): boolean => frame.header.type === FRAME_PONG);
      expect(pong?.header.correlation).toBe(77);
    });

    // The PONG overtook; the parked DATA is still parked.
    expect(dataFrames(peer)).toBe(delivered);
  });

  it("moves one message larger than the whole window through an empty pipe", async () => {
    const window: number = 20 * 1024;
    const { client, peer } = await dialScripted({ initialCredits: window });

    const envelope: DataEnvelope = envelopeOf(KIND_TELL, 30 * 1024);
    const cost: number = tellCost(30 * 1024);
    expect(cost).toBeGreaterThan(window);
    expect(client.tell(envelope)).toBeNull();
    await vi.waitFor((): void => {
      expect(dataFrames(peer)).toBe(1);
    });

    // A second one parks until the peer has digested the first whole.
    await vi.waitFor((): void => {
      expect(client.tell(envelope)).toBeNull();
    });
    await sleep(30);
    expect(dataFrames(peer)).toBe(1);

    peer.send({
      type: FRAME_CREDIT,
      flags: 0,
      lane: LANE_CONTROL,
      correlation: 0,
      body: uvarint(cost),
    });
    await vi.waitFor((): void => {
      expect(dataFrames(peer)).toBe(2);
    });
  });

  it("kills the connection on CREDIT below revision 4", async () => {
    await expectProtocolDeath(
      { revision: 3 },
      [{ type: FRAME_CREDIT, body: uvarint(1024) }],
      "revision 4",
    );
  });

  it("kills the connection on a malformed CREDIT count", async () => {
    await expectProtocolDeath(
      {},
      [{ type: FRAME_CREDIT, body: new Uint8Array(10).fill(0xff) }],
      "malformed CREDIT",
    );
  });
});
