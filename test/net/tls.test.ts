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

import { readFileSync } from "node:fs";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  type DataEnvelope,
  KIND_ASK,
  KIND_TELL,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "../../src/net/envelope";
import { Peer } from "../../src/net/peer";
import type { NetServer } from "../../src/net/server";
import type { Session } from "../../src/net/session";
import type { TlsConfig } from "../../src/net/tls";
import { ByteReader, ByteWriter, decodeValue, encodeValue } from "../../src/net/values";
import { cleanupNet, dialSession, hello, startServer, trackPeer } from "./helpers";

/**
 * TLS is carrier configuration underneath the unchanged protocol: the
 * frames, handshake, negotiation, chunking, and credits are
 * byte-identical over a TLS socket. These tests run the protocol's
 * load-bearing behaviors over TLS to prove exactly that, and pin the
 * all-or-nothing rule: a mixed pair or an untrusted certificate fails
 * its carrier handshake and surfaces as the dial or accept failure it
 * is, with no frame ever crossing.
 */

function pem(name: string): Buffer {
  return readFileSync(new URL(`./tls/${name}`, import.meta.url));
}

const CA: Buffer = pem("ca.pem");
const NODE_CERT: Buffer = pem("node.pem");
const NODE_KEY: Buffer = pem("node.key");
const OTHER_CERT: Buffer = pem("other.pem");
const OTHER_KEY: Buffer = pem("other.key");

/** The fixture material both sides of a trusted pair use. */
const TLS: TlsConfig = { cert: NODE_CERT, key: NODE_KEY, ca: CA };

const TARGET: string = "nodeakt://orders@127.0.0.1:5100/user/charger";

function tellEnvelope(payload: Uint8Array, to: string = TARGET): DataEnvelope {
  return {
    kind: KIND_TELL,
    to,
    uid: "",
    sender: "",
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: "test.Blob",
    payload,
  };
}

function askEnvelope(payload: Uint8Array): DataEnvelope {
  return { ...tellEnvelope(payload), kind: KIND_ASK };
}

function numberPayload(n: number): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, n);
  return Uint8Array.from(writer.bytes());
}

/** An echo acceptor over TLS: answers asks, collects tell payloads. */
async function startTlsEcho(
  received: Uint8Array[],
  overrides: Partial<Parameters<typeof startServer>[0]> = {},
): Promise<NetServer> {
  return startServer(
    { tls: TLS, ...overrides },
    {
      onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
        if (correlation !== 0) {
          session.reply(correlation, {
            serializerId: SERIALIZER_BINARY,
            typeRef: "test.Echo",
            payload: Uint8Array.from(envelope.payload),
          });
          return;
        }

        received.push(Uint8Array.from(envelope.payload));
      },
    },
  );
}

function tlsPeer(port: number, tls: TlsConfig, chunkSize?: number): Peer {
  return trackPeer(
    new Peer(
      "127.0.0.1",
      port,
      hello({ systemName: "client" }),
      {},
      { tls, session: chunkSize === undefined ? {} : { chunkSize } },
    ),
  );
}

afterEach(cleanupNet);

describe("the protocol over TLS", () => {
  it("runs handshake, asks, and tells unchanged over a TLS carrier", async () => {
    const received: Uint8Array[] = [];
    const server: NetServer = await startTlsEcho(received);
    const peer: Peer = tlsPeer(server.address.port, TLS);

    const reply: ReplyEnvelope = await peer.ask(askEnvelope(numberPayload(41)), 2000);
    expect(decodeValue(new ByteReader(reply.payload))).toBe(41);

    peer.tell(tellEnvelope(numberPayload(7)));
    await vi.waitFor((): void => {
      expect(received.length).toBe(1);
    });
    expect(decodeValue(new ByteReader(received[0] as Uint8Array))).toBe(7);
    expect(server.activeConnections).toBeGreaterThan(0);
  });

  it("chunks a large message over TLS exactly as over plaintext", async () => {
    const received: Uint8Array[] = [];
    // Both sides chunk at 1 KiB so the 64 KiB payload crosses as CHUNK
    // frames in each direction and reassembles behind the TLS carrier.
    const server: NetServer = await startTlsEcho(received, {
      tls: TLS,
      session: { chunkSize: 1024 },
    });
    const peer: Peer = tlsPeer(server.address.port, TLS, 1024);

    const big: Uint8Array = new Uint8Array(64 * 1024);
    for (let i: number = 0; i < big.length; i++) {
      big[i] = i & 0xff;
    }

    const writer: ByteWriter = new ByteWriter();
    encodeValue(writer, big);
    const reply: ReplyEnvelope = await peer.ask(askEnvelope(Uint8Array.from(writer.bytes())), 5000);
    expect(decodeValue(new ByteReader(reply.payload))).toEqual(big);
  });

  it("repays credits over TLS so traffic past the window keeps flowing", async () => {
    const received: Uint8Array[] = [];
    // A window this small exhausts after a handful of asks: the tenth
    // settling at all proves CREDIT grants flow back over the TLS
    // carrier, since an unrepaid window would park it forever.
    const server: NetServer = await startTlsEcho(received, {
      tls: TLS,
      local: hello({ systemName: "server", initialCredits: 4096 }),
    });
    const peer: Peer = tlsPeer(server.address.port, TLS);

    const payload: Uint8Array = new Uint8Array(1024).fill(7);
    const writer: ByteWriter = new ByteWriter();
    encodeValue(writer, payload);
    for (let i: number = 0; i < 10; i++) {
      const reply: ReplyEnvelope = await peer.ask(
        askEnvelope(Uint8Array.from(writer.bytes())),
        2000,
      );
      expect(decodeValue(new ByteReader(reply.payload))).toEqual(payload);
    }
  });
});

describe("the all-or-nothing rule", () => {
  it("refuses a plaintext dialer on a TLS listener", async () => {
    const errors: Error[] = [];
    const server: NetServer = await startServer(
      { tls: TLS },
      {
        onError: (error: Error): void => {
          errors.push(error);
        },
      },
    );

    await expect(
      dialSession(
        server.address.port,
        hello({ systemName: "plain" }),
        {},
        {
          handshakeTimeoutMs: 1000,
        },
      ),
    ).rejects.toBeInstanceOf(Error);
    await vi.waitFor((): void => {
      expect(errors.length).toBeGreaterThan(0);
    });
    expect(server.activeConnections).toBe(0);
  });

  it("refuses a TLS dialer on a plaintext listener", async () => {
    const server: NetServer = await startServer();
    const peer: Peer = tlsPeer(server.address.port, TLS);

    await expect(peer.ask(askEnvelope(numberPayload(1)), 2000)).rejects.toBeInstanceOf(Error);
    // The listener saw only carrier garbage; its session dies without
    // ever opening, a beat after the dialer's own failure.
    await vi.waitFor((): void => {
      expect(server.activeConnections).toBe(0);
    });
  });

  it("refuses a certificate from an authority the dialer does not trust", async () => {
    const server: NetServer = await startServer({
      tls: { cert: OTHER_CERT, key: OTHER_KEY },
    });
    const peer: Peer = tlsPeer(server.address.port, TLS);

    await expect(peer.ask(askEnvelope(numberPayload(1)), 2000)).rejects.toBeInstanceOf(Error);
  });
});

describe("mutual TLS", () => {
  it("verifies the client certificate and serves the round trip", async () => {
    const received: Uint8Array[] = [];
    const server: NetServer = await startTlsEcho(received, {
      tls: { ...TLS, requestCert: true },
    });
    const peer: Peer = tlsPeer(server.address.port, TLS);

    const reply: ReplyEnvelope = await peer.ask(askEnvelope(numberPayload(9)), 2000);
    expect(decodeValue(new ByteReader(reply.payload))).toBe(9);
  });

  it("refuses a dialer that presents no certificate", async () => {
    const errors: Error[] = [];
    const server: NetServer = await startServer(
      { tls: { ...TLS, requestCert: true } },
      {
        onError: (error: Error): void => {
          errors.push(error);
        },
      },
    );

    // Verify-only material: the dialer trusts the listener but cannot
    // answer its certificate demand.
    const peer: Peer = tlsPeer(server.address.port, { ca: CA });
    await expect(peer.ask(askEnvelope(numberPayload(1)), 2000)).rejects.toBeInstanceOf(Error);
    await vi.waitFor((): void => {
      expect(errors.length).toBeGreaterThan(0);
    });
    expect(server.activeConnections).toBe(0);
  });
});
