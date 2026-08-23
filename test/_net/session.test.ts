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
  decodeErrorBody,
  decodeHello,
  ERROR_UNAVAILABLE,
  type Hello,
} from "../../src/_net/envelope";
import {
  FRAME_ERROR,
  FRAME_HELLO,
  FRAME_HELLO_ACK,
  FRAME_PING,
  type FrameHeader,
  LANE_CONTROL,
  MIN_MAX_FRAME_SIZE,
  ordinaryLane,
} from "../../src/_net/frame";
import type { NetServer } from "../../src/_net/server";
import {
  ErrHandshakeTimeout,
  ErrIdleReclaim,
  ErrLivenessTimeout,
  negotiateHello,
  PeerError,
  Session,
} from "../../src/_net/session";
import { ByteReader, ByteWriter } from "../../src/_net/values";
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

describe("handshake and negotiation", () => {
  it("opens both sides and negotiates pairwise minima", async () => {
    const server: NetServer = await startServer({
      local: hello({ systemName: "server", maxFrameSize: MIB, initialCredits: 8 * MIB }),
    });
    const client: Session = await dialSession(
      server.address.port,
      hello({
        systemName: "client",
        lane: ordinaryLane(0),
        maxFrameSize: 2 * MIB,
        maxLargeTransfers: 2,
      }),
    );

    expect(client.effective?.maxFrameSize).toBe(MIB);
    expect(client.effective?.initialCredits).toBe(8 * MIB);
    expect(client.effective?.maxLargeTransfers).toBe(2);
    expect(client.effective?.revision).toBe(4);
    expect(client.lane).toBe(ordinaryLane(0));
    expect(client.remote?.systemName).toBe("server");
    expect(server.activeConnections).toBe(1);
    expect(server.acceptedConnections).toBe(1);
  });

  it("floors the negotiated frame size", () => {
    const negotiated: Hello = negotiateHello(
      hello({ maxFrameSize: 1024 }),
      hello({ maxFrameSize: 512 }),
    );
    expect(negotiated.maxFrameSize).toBe(MIN_MAX_FRAME_SIZE);
  });

  it("floors every negotiated size so an advertisement cannot wedge the set", () => {
    // A message cap below the frame size would refuse every send even
    // for frames the negotiated frame size permits; a zero large-
    // transfer cap would refuse every chunk group.
    const negotiated: Hello = negotiateHello(
      hello({ maxMessageSize: 0, maxLargeTransfers: 0 }),
      hello({ maxMessageSize: 512, maxLargeTransfers: 9 }),
    );
    expect(negotiated.maxMessageSize).toBe(negotiated.maxFrameSize);
    expect(negotiated.maxLargeTransfers).toBe(1);
  });

  it("falls back to no compression on a codec mismatch", () => {
    expect(negotiateHello(hello({ compression: 1 }), hello({ compression: 0 })).compression).toBe(
      0,
    );
    expect(negotiateHello(hello({ compression: 1 }), hello({ compression: 1 })).compression).toBe(
      1,
    );
  });

  it("times out a peer that connects and never speaks", async () => {
    const server: NetServer = await startServer({ session: { handshakeTimeoutMs: 100 } });
    dialSocket(server.address.port);

    await vi.waitFor((): void => {
      expect(server.acceptedConnections).toBe(1);
      expect(server.activeConnections).toBe(0);
    });
  });

  it("answers garbage first bytes with a connection-scoped ERROR", async () => {
    const server: NetServer = await startServer();
    const socket: Socket = dialSocket(server.address.port);
    const peer: RawPeer = new RawPeer(socket);
    socket.write(Uint8Array.from([0x99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]));

    await vi.waitFor((): void => {
      expect(peer.frames.length).toBe(1);
      expect(peer.closed).toBe(true);
    });
    expect(peer.frames[0]?.header.type).toBe(FRAME_ERROR);
    expect(decodeErrorBody(new ByteReader(peer.frames[0]?.body ?? EMPTY)).message).toContain(
      "version",
    );
    await vi.waitFor((): void => {
      expect(server.activeConnections).toBe(0);
    });
  });

  it("rejects a first frame that is not HELLO", async () => {
    const server: NetServer = await startServer();
    const peer: RawPeer = new RawPeer(dialSocket(server.address.port));
    peer.send({ type: FRAME_PING, flags: 0, lane: LANE_CONTROL, correlation: 1, body: null });

    await vi.waitFor((): void => {
      expect(peer.frames.length).toBe(1);
      expect(peer.closed).toBe(true);
    });
    expect(peer.frames[0]?.header.type).toBe(FRAME_ERROR);
    expect(decodeErrorBody(new ByteReader(peer.frames[0]?.body ?? EMPTY)).message).toContain(
      "HELLO",
    );
  });

  it("surfaces a peer's handshake refusal as a typed dial error", async () => {
    const port: number = await startRawListener((accepted: Socket): void => {
      const acceptor: RawPeer = new RawPeer(accepted);
      acceptor.onFirstFrame((): void => {
        const writer: ByteWriter = new ByteWriter();
        writer.writeU8(ERROR_UNAVAILABLE);
        writer.writeUvarint(0);
        writer.writeString("");
        writer.writeString("no capacity");
        acceptor.send({
          type: FRAME_ERROR,
          flags: 0,
          lane: LANE_CONTROL,
          correlation: 0,
          body: Uint8Array.from(writer.bytes()),
        });
      });
    });

    await expect(Session.dial(dialSocket(port), hello())).rejects.toSatisfy(
      (error: unknown): boolean => error instanceof PeerError && error.message === "no capacity",
    );
  });

  it("rejects the dial when the peer vanishes mid-handshake", async () => {
    const port: number = await startRawListener((accepted: Socket): void => {
      accepted.destroy();
    });

    await expect(Session.dial(dialSocket(port), hello())).rejects.toBeInstanceOf(Error);
  });

  it("rejects the dial on handshake timeout against a silent peer", async () => {
    const port: number = await startRawListener((): void => {});

    await expect(
      Session.dial(dialSocket(port), hello(), {}, { handshakeTimeoutMs: 100 }),
    ).rejects.toBe(ErrHandshakeTimeout);
  });
});

describe("liveness and idle", () => {
  it("keeps a quiet connection alive through ping and pong", async () => {
    const server: NetServer = await startServer();
    const closes: (Error | null)[] = [];
    await dialSession(
      server.address.port,
      hello({ systemName: "client" }),
      {
        onClose: (_session: Session, error: Error | null): void => {
          closes.push(error);
        },
      },
      { readIdleMs: 40 },
    );

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 400);
    });
    expect(closes.length).toBe(0);
    expect(server.activeConnections).toBe(1);
  });

  it("tears down a peer that stops answering probes", async () => {
    let acceptor: RawPeer | null = null;
    const port: number = await startRawListener((accepted: Socket): void => {
      const peer: RawPeer = new RawPeer(accepted);
      acceptor = peer;
      peer.onFirstFrame((): void => {
        const request: Hello = decodeHello(new ByteReader(peer.frames[0]?.body ?? EMPTY));
        peer.sendHello(FRAME_HELLO_ACK, negotiateHello(hello(), request));
        // From here on: total silence, no PONGs.
      });
    });

    const closes: (Error | null)[] = [];
    await Session.dial(
      dialSocket(port),
      hello({ systemName: "client" }),
      {
        onClose: (_session: Session, error: Error | null): void => {
          closes.push(error);
        },
      },
      { readIdleMs: 50 },
    );

    await vi.waitFor(
      (): void => {
        expect(closes).toEqual([ErrLivenessTimeout]);
      },
      { timeout: 2000 },
    );
    const probes: number = (acceptor as RawPeer | null | undefined)?.frames.filter(
      (frame: { header: FrameHeader }): boolean => frame.header.type === FRAME_PING,
    ).length as number;
    expect(probes).toBeGreaterThanOrEqual(1);
  });

  it("reclaims a server-side connection that goes fully idle", async () => {
    const serverCloses: (Error | null)[] = [];
    const server: NetServer = await startServer(
      { session: { connIdleMs: 120, readIdleMs: 0 } },
      {
        onSessionClose: (_session: Session, error: Error | null): void => {
          serverCloses.push(error);
        },
      },
    );

    const clientCloses: (Error | null)[] = [];
    await dialSession(
      server.address.port,
      hello({ systemName: "client" }),
      {
        onClose: (_session: Session, error: Error | null): void => {
          clientCloses.push(error);
        },
      },
      { readIdleMs: 0 },
    );

    await vi.waitFor(
      (): void => {
        expect(serverCloses).toEqual([ErrIdleReclaim]);
        expect(clientCloses.length).toBe(1);
      },
      { timeout: 2000 },
    );
    expect(server.activeConnections).toBe(0);
  });

  it("kills a connection on a lane mismatch after open", async () => {
    const server: NetServer = await startServer();
    const closes: (Error | null)[] = [];
    const client: Session = await dialSession(
      server.address.port,
      hello({ systemName: "client" }),
      {
        onClose: (_session: Session, error: Error | null): void => {
          closes.push(error);
        },
      },
    );

    // Frames must carry the negotiated lane; forging another one is a
    // violation the server answers and kills.
    client.send({
      type: FRAME_PING,
      flags: 0,
      lane: 0x07,
      correlation: client.nextCorrelation(),
      body: null,
    });

    await vi.waitFor((): void => {
      expect(closes.length).toBe(1);
    });
    expect(closes[0]).toBeInstanceOf(PeerError);
  });
});

describe("server lifecycle", () => {
  it("refuses connections beyond the cap", async () => {
    const server: NetServer = await startServer({ maxConnections: 1 });
    await dialSession(server.address.port);

    await expect(
      Session.dial(dialSocket(server.address.port), hello(), {}, { handshakeTimeoutMs: 300 }),
    ).rejects.toBeInstanceOf(Error);
    expect(server.activeConnections).toBe(1);
  });

  it("shuts down gracefully, telling every session why", async () => {
    const server: NetServer = await startServer();
    const closes: Error[] = [];
    await dialSession(server.address.port, hello({ systemName: "client" }), {
      onClose: (_session: Session, error: Error | null): void => {
        if (error !== null) {
          closes.push(error);
        }
      },
    });

    await server.shutdown(2000);
    await vi.waitFor((): void => {
      expect(closes.length).toBe(1);
    });
    expect(closes[0]).toBeInstanceOf(PeerError);
    expect((closes[0] as PeerError).code).toBe(ERROR_UNAVAILABLE);
    expect(server.activeConnections).toBe(0);

    await expect(
      Session.dial(dialSocket(server.address.port), hello(), {}, { handshakeTimeoutMs: 300 }),
    ).rejects.toBeInstanceOf(Error);
  });

  it("destroys everything immediately on a negative grace", async () => {
    const server: NetServer = await startServer();
    const closes: (Error | null)[] = [];
    await dialSession(server.address.port, hello({ systemName: "client" }), {
      onClose: (_session: Session, error: Error | null): void => {
        closes.push(error);
      },
    });

    await server.shutdown(-1);
    expect(server.activeConnections).toBe(0);
    await vi.waitFor((): void => {
      expect(closes.length).toBe(1);
    });
  });

  it("rejects a post-open handshake frame as a violation", async () => {
    const server: NetServer = await startServer();
    const closes: (Error | null)[] = [];
    const client: Session = await dialSession(
      server.address.port,
      hello({ systemName: "client" }),
      {
        onClose: (_session: Session, error: Error | null): void => {
          closes.push(error);
        },
      },
    );

    client.send({ type: FRAME_HELLO, flags: 0, lane: client.lane, correlation: 0, body: null });
    await vi.waitFor((): void => {
      expect(closes.length).toBe(1);
    });
    expect(closes[0]).toBeInstanceOf(PeerError);
    expect(server.activeConnections).toBe(0);
  });
});
