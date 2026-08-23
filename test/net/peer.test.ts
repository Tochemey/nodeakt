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
  type DataEnvelope,
  decodeHello,
  encodeDataEnvelope,
  type Hello,
  KIND_ASK,
  KIND_TELL,
  KIND_WATCH,
  type RefInterner,
  SERIALIZER_BINARY,
} from "../../src/net/envelope";
import {
  FRAME_DATA,
  FRAME_HEADER_SIZE,
  FRAME_HELLO_ACK,
  LANE_CONTROL,
  LANE_LARGE,
} from "../../src/net/frame";
import { ErrDialBackoff, ErrPeerClosed, Peer, type PeerOptions } from "../../src/net/peer";
import type { NetServer } from "../../src/net/server";
import { negotiateHello, Session } from "../../src/net/session";
import { ByteReader, ByteWriter, decodeValue, encodeValue } from "../../src/net/values";
import {
  cleanupNet,
  EMPTY,
  hello,
  ManualTimers,
  RawPeer,
  sleep,
  startRawListener,
  startServer,
  trackPeer,
  trackSession,
} from "./helpers";

afterEach(cleanupNet);

const TARGET_PATH: string = "nodeakt://orders@10.0.0.5:5100/user/charger";

function seqEnvelope(seq: number, to: string = TARGET_PATH): DataEnvelope {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, seq);
  return {
    kind: KIND_TELL,
    to,
    uid: "",
    sender: "",
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: "test.Seq",
    payload: Uint8Array.from(writer.bytes()),
  };
}

function askEnvelope(to: string = TARGET_PATH): DataEnvelope {
  return { ...seqEnvelope(0, to), kind: KIND_ASK };
}

function decodeSeq(envelope: DataEnvelope): number {
  return decodeValue(new ByteReader(envelope.payload)) as number;
}

/**
 * The wire cost of one sequence tell on a revision-4 connection,
 * where the nonempty refs travel as interned ids; used to size a
 * stalling acceptor's credit window so a known share of a burst is
 * confirmed and the rest parks.
 */
function tellWireCost(): number {
  const interned: RefInterner = {
    pathId: (literal: string): number => (literal === "" ? 0 : 1),
    typeId: (literal: string): number => (literal === "" ? 0 : 1),
  };
  const writer: ByteWriter = new ByteWriter();
  encodeDataEnvelope(writer, seqEnvelope(9), interned);
  return FRAME_HEADER_SIZE + writer.length;
}

function makePeer(
  port: number,
  handlers: ConstructorParameters<typeof Peer>[3] = {},
  options: PeerOptions = {},
): Peer {
  return trackPeer(new Peer("127.0.0.1", port, hello({ systemName: "peer" }), handlers, options));
}

/** An echo server answering asks and recording tell sequence counts. */
async function startEchoServer(
  port: number,
  counts: Map<number, number>,
  lanes?: { lane: number; to: string }[],
): Promise<NetServer> {
  return startServer(
    { port, local: hello({ systemName: "server" }) },
    {
      onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
        lanes?.push({ lane: session.lane, to: envelope.to });
        if (correlation !== 0) {
          session.reply(correlation, {
            serializerId: SERIALIZER_BINARY,
            typeRef: "test.Echo",
            payload: Uint8Array.from(envelope.payload),
          });
          return;
        }

        const seq: number = decodeSeq(envelope);
        counts.set(seq, (counts.get(seq) ?? 0) + 1);
      },
    },
  );
}

/** Waits until `read()` stops changing across consecutive checks. */
async function stableCount(read: () => number): Promise<number> {
  let last: number = read();
  for (let i = 0; i < 100; i++) {
    await sleep(30);
    const next: number = read();
    if (next === last) {
      return next;
    }

    last = next;
  }

  return last;
}

describe("peer dialing", () => {
  it("dials lazily and single-flight: ten concurrent asks share one connection", async () => {
    const server: NetServer = await startEchoServer(0, new Map<number, number>());
    const peer: Peer = makePeer(server.address.port);
    expect(server.acceptedConnections).toBe(0);

    const asks: Promise<unknown>[] = [];
    for (let i = 0; i < 10; i++) {
      asks.push(peer.ask(askEnvelope(), 2000));
    }

    await Promise.all(asks);
    expect(server.acceptedConnections).toBe(1);
  });

  it("routes watches to control, big payloads to large, paths to stable ordinary lanes", async () => {
    const lanes: { lane: number; to: string }[] = [];
    const server: NetServer = await startEchoServer(0, new Map<number, number>(), lanes);
    const peer: Peer = makePeer(
      server.address.port,
      {},
      { ordinaryLanes: 3, largeThreshold: 1024 },
    );

    peer.tell({ ...seqEnvelope(0, "/user/a"), kind: KIND_WATCH, typeRef: "", payload: EMPTY });
    peer.tell({ ...seqEnvelope(1, "/user/a"), payload: new Uint8Array(2048) });
    peer.tell(seqEnvelope(2, "/user/a"));
    peer.tell(seqEnvelope(3, "/user/a"));
    peer.tell(seqEnvelope(4, "/user/b"));
    await vi.waitFor((): void => {
      expect(lanes.length).toBe(5);
    });

    const byTo = (to: string): number[] =>
      lanes.filter((entry): boolean => entry.to === to).map((entry): number => entry.lane);
    const watchLane: number = (lanes[0] as { lane: number }).lane;
    expect(watchLane).toBe(LANE_CONTROL);
    expect(lanes[1]?.lane).toBe(LANE_LARGE);

    const ordinaryA: number[] = byTo("/user/a").slice(2);
    expect(ordinaryA.length).toBe(2);
    for (const lane of ordinaryA) {
      expect(lane).toBeGreaterThanOrEqual(1);
      expect(lane).toBeLessThanOrEqual(3);
    }

    expect(ordinaryA[0]).toBe(ordinaryA[1]);
    const distinct: Set<number> = new Set(lanes.map((entry): number => entry.lane));
    expect(server.acceptedConnections).toBe(distinct.size);
  });

  it("fails fast inside the backoff window, doubling per failure", async () => {
    const parked: NetServer = await startServer({ port: 0 });
    const deadPort: number = parked.address.port;
    await parked.shutdown(-1);

    // The backoff window lives on the manual clock; only the dials
    // themselves touch the real network, and those fail immediately
    // against a closed loopback port.
    const clock: ManualTimers = new ManualTimers();
    const peer: Peer = makePeer(
      deadPort,
      {},
      { backoffInitialMs: 150, backoffCapMs: 500, dialTimeoutMs: 1000, timers: clock },
    );

    const first: unknown = await peer.ask(askEnvelope(), 1000).catch((thrown): unknown => thrown);
    expect(first).toBeInstanceOf(Error);
    expect(first).not.toBe(ErrDialBackoff);

    const second: unknown = await peer.ask(askEnvelope(), 1000).catch((thrown): unknown => thrown);
    expect(second).toBe(ErrDialBackoff);

    // Past the first window the lane redials, fails again, and arms
    // a doubled window measured from that failure.
    clock.advance(150);
    const third: unknown = await peer.ask(askEnvelope(), 1000).catch((thrown): unknown => thrown);
    expect(third).toBeInstanceOf(Error);
    expect(third).not.toBe(ErrDialBackoff);

    clock.advance(299);
    const fourth: unknown = await peer.ask(askEnvelope(), 1000).catch((thrown): unknown => thrown);
    expect(fourth).toBe(ErrDialBackoff);

    clock.advance(1);
    const fifth: unknown = await peer.ask(askEnvelope(), 1000).catch((thrown): unknown => thrown);
    expect(fifth).toBeInstanceOf(Error);
    expect(fifth).not.toBe(ErrDialBackoff);
  });

  it("redials on next use after a restart, without duplicating confirmed tells", async () => {
    const counts: Map<number, number> = new Map<number, number>();
    const deadLetters: number[] = [];
    const laneClosed: number[] = [];
    let server: NetServer = await startEchoServer(0, counts);
    const port: number = server.address.port;
    const peer: Peer = makePeer(
      port,
      {
        onDeadLetter: (envelope: DataEnvelope): void => {
          deadLetters.push(decodeSeq(envelope));
        },
        onLaneClose: (lane: number): void => {
          laneClosed.push(lane);
        },
      },
      { backoffInitialMs: 100 },
    );

    peer.tell(seqEnvelope(1));
    await vi.waitFor((): void => {
      expect(counts.get(1)).toBe(1);
    });

    await server.shutdown(-1);
    await vi.waitFor((): void => {
      expect(laneClosed.length).toBe(1);
    });

    // The server is down: this use dials, fails, and dead-letters.
    peer.tell(seqEnvelope(2));
    await vi.waitFor((): void => {
      expect(deadLetters).toContain(2);
    });

    server = await startEchoServer(port, counts);
    await sleep(150);
    peer.tell(seqEnvelope(3));
    await vi.waitFor((): void => {
      expect(counts.get(3)).toBe(1);
    });

    // The confirmed tell was never replayed onto the new session.
    expect(counts.get(1)).toBe(1);
    expect(counts.get(2)).toBeUndefined();
  });
});

describe("peer redelivery", () => {
  /**
   * A scripted first acceptor advertises a tiny credit window and
   * never grants, so tells past the window are admitted but never
   * reach the kernel; whatever replaces it sees exactly those tells
   * again.
   */
  function stallScript(socket: Socket, credits: number): RawPeer {
    const raw: RawPeer = new RawPeer(socket);
    raw.onFirstFrame((): void => {
      const request: Hello = decodeHello(new ByteReader(raw.frames[0]?.body ?? EMPTY));
      raw.sendHello(
        FRAME_HELLO_ACK,
        negotiateHello(hello({ systemName: "stall", initialCredits: credits }), request),
      );
    });
    return raw;
  }

  it("redelivers unconfirmed tells once, in order, on the next session", async () => {
    const received: number[] = [];
    // Eleven tells fit the window; the other nine park unconfirmed.
    const credits: number = tellWireCost() * 11 + 10;
    let stall: RawPeer | null = null;
    let phase: number = 0;
    const port: number = await startRawListener((socket: Socket): void => {
      phase += 1;
      if (phase === 1) {
        stall = stallScript(socket, credits);
        return;
      }

      void Session.accept(socket, hello({ systemName: "real" }), {
        onData: (_session: Session, envelope: DataEnvelope): void => {
          received.push(decodeSeq(envelope));
        },
      }).then(trackSession);
    });

    const peer: Peer = makePeer(port, {}, { backoffInitialMs: 50 });
    for (let seq = 0; seq < 20; seq++) {
      peer.tell(seqEnvelope(seq));
      await sleep(1);
    }

    const raw: RawPeer = stall as unknown as RawPeer;
    const confirmed: number = await stableCount(
      (): number => raw.frames.filter((frame): boolean => frame.header.type === FRAME_DATA).length,
    );
    expect(confirmed).toBeGreaterThan(0);
    expect(confirmed).toBeLessThan(20);

    // Kill the stalled connection; the next use dials the real
    // acceptor and must replay exactly the unconfirmed tail first.
    raw.conn.destroy(new Error("killed"));
    await sleep(30);
    peer.tell(seqEnvelope(20));

    const expected: number[] = [];
    for (let seq = confirmed; seq <= 20; seq++) {
      expected.push(seq);
    }

    await vi.waitFor((): void => {
      expect(received).toEqual(expected);
    });
  });

  it("dead-letters unconfirmed tells when closing over a live stall", async () => {
    const deadLetters: { seq: number; reason: Error }[] = [];
    const credits: number = tellWireCost() * 11 + 10;
    let stall: RawPeer | null = null;
    const port: number = await startRawListener((socket: Socket): void => {
      stall = stall ?? stallScript(socket, credits);
    });

    const peer: Peer = makePeer(port, {
      onDeadLetter: (envelope: DataEnvelope, reason: Error): void => {
        deadLetters.push({ seq: decodeSeq(envelope), reason });
      },
    });
    for (let seq = 0; seq < 20; seq++) {
      peer.tell(seqEnvelope(seq));
      await sleep(1);
    }

    const raw: RawPeer = stall as unknown as RawPeer;
    const confirmed: number = await stableCount(
      (): number => raw.frames.filter((frame): boolean => frame.header.type === FRAME_DATA).length,
    );

    // The lane's session is still up: closing the peer tears it down
    // and the tells parked behind the dead window die with it.
    peer.close();
    await vi.waitFor((): void => {
      expect(deadLetters.length).toBe(20 - confirmed);
    });
    for (const letter of deadLetters) {
      expect(letter.seq).toBeGreaterThanOrEqual(confirmed);
    }
  });

  it("dead-letters parked tells when the peer closes", async () => {
    const deadLetters: { seq: number; reason: Error }[] = [];
    const credits: number = tellWireCost() * 11 + 10;
    let stall: RawPeer | null = null;
    const port: number = await startRawListener((socket: Socket): void => {
      stall = stall ?? stallScript(socket, credits);
    });

    const peer: Peer = makePeer(port, {
      onDeadLetter: (envelope: DataEnvelope, reason: Error): void => {
        deadLetters.push({ seq: decodeSeq(envelope), reason });
      },
    });
    for (let seq = 0; seq < 20; seq++) {
      peer.tell(seqEnvelope(seq));
      await sleep(1);
    }

    const raw: RawPeer = stall as unknown as RawPeer;
    const confirmed: number = await stableCount(
      (): number => raw.frames.filter((frame): boolean => frame.header.type === FRAME_DATA).length,
    );
    raw.conn.destroy(new Error("killed"));
    await sleep(30);
    peer.close();

    await vi.waitFor((): void => {
      expect(deadLetters.length).toBe(20 - confirmed);
    });
    for (const letter of deadLetters) {
      expect(letter.reason).toBe(ErrPeerClosed);
      expect(letter.seq).toBeGreaterThanOrEqual(confirmed);
    }
  });
});

describe("peer teardown", () => {
  it("parks a dial still in flight when the peer closes mid-handshake", async () => {
    let raw: RawPeer | null = null;
    const port: number = await startRawListener((socket: Socket): void => {
      const scripted: RawPeer = new RawPeer(socket);
      raw = scripted;
      scripted.onFirstFrame((): void => {
        setTimeout((): void => {
          const request: Hello = decodeHello(new ByteReader(scripted.frames[0]?.body ?? EMPTY));
          scripted.sendHello(
            FRAME_HELLO_ACK,
            negotiateHello(hello({ systemName: "slow" }), request),
          );
        }, 150);
      });
    });

    const peer: Peer = makePeer(port, {}, { dialTimeoutMs: 2000 });
    const pending: Promise<unknown> = peer
      .ask(askEnvelope(), 3000)
      .catch((thrown): unknown => thrown);
    await sleep(30);
    peer.close();

    expect(await pending).toBe(ErrPeerClosed);
    // The handshake completes anyway; the parked dial destroys it.
    await vi.waitFor((): void => {
      expect((raw as unknown as RawPeer).closed).toBe(true);
    });
  });

  it("refuses every use once closed", async () => {
    const deadLetters: Error[] = [];
    const server: NetServer = await startServer();
    const peer: Peer = makePeer(server.address.port, {
      onDeadLetter: (_envelope: DataEnvelope, reason: Error): void => {
        deadLetters.push(reason);
      },
    });

    peer.close();
    peer.close();
    expect(peer.closed).toBe(true);

    peer.tell(seqEnvelope(1));
    expect(deadLetters).toEqual([ErrPeerClosed]);
    await expect(peer.ask(askEnvelope(), 1000)).rejects.toBe(ErrPeerClosed);
    expect(server.acceptedConnections).toBe(0);
  });
});

describe("peer chaos", () => {
  it("survives repeated kill and restart under load: asks settle, tells never triple", {
    timeout: 20_000,
  }, async () => {
    const counts: Map<number, number> = new Map<number, number>();
    const deadLetters: Set<number> = new Set<number>();
    let server: NetServer = await startEchoServer(0, counts);
    const port: number = server.address.port;
    const peer: Peer = makePeer(
      port,
      {
        onDeadLetter: (envelope: DataEnvelope): void => {
          deadLetters.add(decodeSeq(envelope));
        },
      },
      { backoffInitialMs: 40, backoffCapMs: 160, dialTimeoutMs: 1000 },
    );

    const askOutcomes: Promise<unknown>[] = [];
    let seq: number = 0;
    for (let cycle = 0; cycle < 3; cycle++) {
      for (let i = 0; i < 40; i++) {
        peer.tell(seqEnvelope(seq++));
        if (i % 4 === 0) {
          askOutcomes.push(
            peer.ask(askEnvelope(), 1000).then(
              (): string => "ok",
              (thrown): unknown => thrown,
            ),
          );
        }

        await sleep(2);
      }

      await server.shutdown(-1);
      await sleep(50);
      server = await startEchoServer(port, counts);
      await sleep(100);
    }

    // Past the last backoff window, delivery must fully recover.
    await sleep(250);
    const finalStart: number = seq;
    for (let i = 0; i < 10; i++) {
      peer.tell(seqEnvelope(seq++));
    }

    await vi.waitFor(
      (): void => {
        for (let s = finalStart; s < seq; s++) {
          expect(counts.get(s)).toBe(1);
        }
      },
      { timeout: 5000 },
    );

    // Every ask settled, each with a reply or a real error.
    const outcomes: unknown[] = await Promise.all(askOutcomes);
    for (const outcome of outcomes) {
      expect(outcome === "ok" || outcome instanceof Error).toBe(true);
    }

    // No tell was delivered more than the send plus one redelivery.
    for (const count of counts.values()) {
      expect(count).toBeLessThanOrEqual(2);
    }

    peer.close();
    await server.shutdown(-1);
    expect(server.activeConnections).toBe(0);
  });
});
