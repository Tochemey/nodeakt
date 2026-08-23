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
  encodeDataEnvelope,
  encodeHello,
  KIND_TELL,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "../../src/_net/envelope";
import {
  encodeFrameHeader,
  FRAME_ERROR,
  FRAME_HELLO,
  FRAME_HELLO_ACK,
  LANE_CONTROL,
} from "../../src/_net/frame";
import type { NetServer } from "../../src/_net/server";
import type { Session, SessionOptions } from "../../src/_net/session";
import { ByteWriter } from "../../src/_net/values";
import {
  cleanupNet,
  dialSession,
  dialSocket,
  hello,
  RawPeer,
  sleep,
  startRawListener,
  startServer,
} from "./helpers";

afterEach(cleanupNet);

/**
 * The malformed-bytes soak: seeded random garbage, bit-flipped valid
 * streams, and hostile well-formed headers thrown at both ends of the
 * transport. The property under test is survival, not any particular
 * refusal: no crash, no hang, every poisoned connection dies with a
 * typed error, and the endpoint keeps serving clean traffic after.
 * The generator is seeded, so every run replays the same bytes.
 */

/** Deterministic xorshift32; the soak must replay identically. */
function rng(seed: number): () => number {
  let state: number = seed >>> 0;
  return (): number => {
    state ^= (state << 13) >>> 0;
    state ^= state >>> 17;
    state ^= (state << 5) >>> 0;
    state >>>= 0;
    return state;
  };
}

function randomBytes(next: () => number, length: number): Uint8Array {
  const bytes: Uint8Array = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    bytes[i] = next() & 0xff;
  }

  return bytes;
}

/** Session settings that reap poisoned connections fast. */
const REAP_FAST: SessionOptions = {
  handshakeTimeoutMs: 200,
  readIdleMs: 100,
  connIdleMs: 250,
  writeTimeoutMs: 500,
};

/** One valid HELLO frame followed by one valid DATA frame, as bytes. */
function validStream(): Uint8Array {
  const writer: ByteWriter = new ByteWriter(512);
  const body: ByteWriter = new ByteWriter(256);
  encodeHello(body, hello({ systemName: "soak" }));
  encodeFrameHeader(writer, {
    type: FRAME_HELLO,
    flags: 0,
    lane: LANE_CONTROL,
    length: body.length,
    correlation: 0,
  });
  writer.writeBytes(body.bytes());

  body.reset();
  const envelope: DataEnvelope = {
    kind: KIND_TELL,
    to: "/user/soak",
    uid: "u1",
    sender: "",
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: "soak.Poke",
    payload: Uint8Array.from([1, 2, 3]),
  };
  encodeDataEnvelope(body, envelope);
  encodeFrameHeader(writer, {
    type: 0x03,
    flags: 0,
    lane: LANE_CONTROL,
    length: body.length,
    correlation: 0,
  });
  writer.writeBytes(body.bytes());
  return Uint8Array.from(writer.bytes());
}

/** A 16-byte header that passes no validation but has a true length. */
function wildHeader(next: () => number, bodyLength: number): Uint8Array {
  const header: Uint8Array = new Uint8Array(16);
  header[0] = next() % 4 === 0 ? next() & 0xff : 0x01;
  header[1] = next() & 0xff;
  header[2] = next() & 0xff;
  header[3] = next() & 0xff;
  header[4] = (bodyLength >>> 24) & 0xff;
  header[5] = (bodyLength >>> 16) & 0xff;
  header[6] = (bodyLength >>> 8) & 0xff;
  header[7] = bodyLength & 0xff;
  for (let i = 8; i < 16; i++) {
    header[i] = next() & 0x0f;
  }

  return header;
}

/** Fires garbage at a socket in a few random-sized writes, then FIN. */
function fireAndEnd(socket: Socket, next: () => number, payload: Uint8Array): Promise<void> {
  return new Promise<void>((resolve: () => void): void => {
    socket.on("error", (): void => {});
    socket.on("close", resolve);
    // Consume whatever the endpoint answers: a paused socket never
    // processes the peer's FIN, and the round would sit on the
    // fallback timer instead of the close event.
    socket.resume();
    let offset: number = 0;
    while (offset < payload.length) {
      const take: number = Math.min(1 + (next() % 512), payload.length - offset);
      socket.write(payload.subarray(offset, offset + take));
      offset += take;
    }

    socket.end();
    setTimeout((): void => {
      socket.destroy();
      resolve();
    }, 400);
  });
}

/** Proves the server still serves clean traffic end to end. */
async function assertStillServes(server: NetServer): Promise<void> {
  const client: Session = await dialSession(server.address.port);
  const reply: ReplyEnvelope = await client.ask(
    {
      kind: 1,
      to: "/user/echo",
      uid: "",
      sender: "",
      senderUid: "",
      timeout: 0,
      serializerId: SERIALIZER_BINARY,
      typeRef: "soak.Echo",
      payload: Uint8Array.from([9]),
    },
    2000,
  );
  expect(reply.typeRef).toBe("soak.EchoReply");
  client.close();
}

async function startEchoServer(): Promise<NetServer> {
  return startServer(
    { session: REAP_FAST },
    {
      onData: (session: Session, _envelope: DataEnvelope, correlation: number): void => {
        if (correlation === 0) {
          return;
        }

        session.reply(correlation, {
          serializerId: SERIALIZER_BINARY,
          typeRef: "soak.EchoReply",
          payload: Uint8Array.from([9]),
        });
      },
    },
  );
}

describe("malformed-bytes soak", () => {
  it("survives pure garbage thrown at the accept path", { timeout: 30_000 }, async () => {
    const server: NetServer = await startEchoServer();
    const next: () => number = rng(0xa11ce);

    for (let round = 0; round < 24; round++) {
      const socket: Socket = dialSocket(server.address.port);
      await fireAndEnd(socket, next, randomBytes(next, 64 + (next() % 2048)));
    }

    await vi.waitFor((): void => {
      expect(server.activeConnections).toBe(0);
    });
    await assertStillServes(server);
  });

  it("survives bit-flipped valid streams", { timeout: 30_000 }, async () => {
    const server: NetServer = await startEchoServer();
    const next: () => number = rng(0xbadc0de);
    const clean: Uint8Array = validStream();

    for (let round = 0; round < 32; round++) {
      const mutated: Uint8Array = Uint8Array.from(clean);
      const flips: number = 1 + (next() % 4);
      for (let i = 0; i < flips; i++) {
        const at: number = next() % mutated.length;
        mutated[at] = (mutated[at] as number) ^ (1 << (next() % 8));
      }

      const socket: Socket = dialSocket(server.address.port);
      await fireAndEnd(socket, next, mutated);
    }

    await vi.waitFor((): void => {
      expect(server.activeConnections).toBe(0);
    });
    await assertStillServes(server);
  });

  it("survives well-formed headers with hostile fields", { timeout: 30_000 }, async () => {
    const server: NetServer = await startEchoServer();
    const next: () => number = rng(0x5eed);

    for (let round = 0; round < 24; round++) {
      const writer: ByteWriter = new ByteWriter(1024);
      const frames: number = 1 + (next() % 4);
      for (let i = 0; i < frames; i++) {
        const body: Uint8Array = randomBytes(next, next() % 128);
        writer.writeBytes(wildHeader(next, body.length));
        writer.writeBytes(body);
      }

      const socket: Socket = dialSocket(server.address.port);
      await fireAndEnd(socket, next, Uint8Array.from(writer.bytes()));
    }

    await vi.waitFor((): void => {
      expect(server.activeConnections).toBe(0);
    });
    await assertStillServes(server);
  });

  it("rejects every dial against a garbage-spewing acceptor", { timeout: 30_000 }, async () => {
    const next: () => number = rng(0xd1a1);
    const port: number = await startRawListener((socket: Socket): void => {
      socket.on("error", (): void => {});
      socket.write(randomBytes(next, 64 + (next() % 1024)));
    });

    for (let round = 0; round < 12; round++) {
      let failure: Error | null = null;
      try {
        await dialSession(port, hello({ systemName: "soak-dialer" }), {}, REAP_FAST);
      } catch (thrown) {
        failure = thrown as Error;
      }

      expect(failure).toBeInstanceOf(Error);
    }
  });

  it("kills an open session fed post-handshake garbage, typed", { timeout: 30_000 }, async () => {
    const server: NetServer = await startEchoServer();
    const next: () => number = rng(0xfeed);

    for (let round = 0; round < 8; round++) {
      const socket: Socket = dialSocket(server.address.port);
      socket.on("error", (): void => {});
      const raw: RawPeer = new RawPeer(socket);
      raw.sendHello(FRAME_HELLO, hello({ systemName: "soak-raw" }));
      await vi.waitFor((): void => {
        expect(raw.frames.some((frame): boolean => frame.header.type === FRAME_HELLO_ACK)).toBe(
          true,
        );
      });

      socket.write(randomBytes(next, 64 + (next() % 512)));
      // The open session answers corruption with a connection-scoped
      // ERROR before it closes; seeing either proves a typed death.
      await vi.waitFor((): void => {
        const answered: boolean = raw.frames.some(
          (frame): boolean => frame.header.type === FRAME_ERROR,
        );
        expect(answered || raw.closed).toBe(true);
      });
      socket.destroy();
      await sleep(10);
    }

    await vi.waitFor((): void => {
      expect(server.activeConnections).toBe(0);
    });
    await assertStillServes(server);
  });
});
