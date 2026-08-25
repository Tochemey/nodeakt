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
import { connect, type Socket } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { afterEach, describe, expect, it } from "vitest";
import { wallClock } from "../../src/cluster/clock";
import { SeededRandom } from "../../src/cluster/random";
import { Swim } from "../../src/cluster/swim";
import {
  ClusterTransportError,
  TcpClusterTransport,
  type TransportHandlers,
} from "../../src/cluster/transport";
import {
  encodeMessage,
  MESSAGE_GOSSIP,
  MESSAGE_SYNC_REQUEST,
  MESSAGE_SYNC_RESPONSE,
} from "../../src/cluster/wire";

const transports = new Set<TcpClusterTransport>();
const noop: TransportHandlers = {
  packet: (): void => undefined,
  stream: (_from, stream): void => stream.close(),
};

function track(transport: TcpClusterTransport): TcpClusterTransport {
  transports.add(transport);
  return transport;
}

async function endpoint(
  handlers: TransportHandlers = noop,
  options: Partial<ConstructorParameters<typeof TcpClusterTransport>[0]> = {},
): Promise<TcpClusterTransport> {
  const transport = track(
    await TcpClusterTransport.bind({
      host: "127.0.0.1",
      port: 0,
      ...options,
    }),
  );
  await transport.start(handlers);
  return transport;
}

function gossip(): Uint8Array {
  return encodeMessage({ type: MESSAGE_GOSSIP, updates: [] });
}

function sync(type: typeof MESSAGE_SYNC_REQUEST | typeof MESSAGE_SYNC_RESPONSE): Uint8Array {
  return encodeMessage({
    type,
    exchangeId: 1n,
    chunkIndex: 0,
    chunkCount: 1,
    updates: [],
  });
}

function framed(bytes: Uint8Array): Uint8Array {
  const result = new Uint8Array(4 + bytes.length);
  new DataView(result.buffer).setUint32(0, bytes.length);
  result.set(bytes, 4);
  return result;
}

function raw(address: string): Promise<Socket> {
  const colon = address.lastIndexOf(":");
  return new Promise<Socket>((resolve, reject): void => {
    const socket = connect(Number(address.slice(colon + 1)), address.slice(0, colon));
    socket.once("connect", (): void => resolve(socket));
    socket.once("error", reject);
  });
}

function closed(socket: Socket): Promise<void> {
  if (socket.destroyed) {
    return Promise.resolve();
  }
  return new Promise<void>((resolve): void => {
    socket.once("close", (): void => resolve());
  });
}

function pem(name: string): Buffer {
  return readFileSync(new URL(`../net/tls/${name}`, import.meta.url));
}

afterEach(async () => {
  await Promise.all(Array.from(transports, (transport) => transport.stop()));
  transports.clear();
});

describe("TcpClusterTransport", () => {
  it("binds port zero, maps logical packet identity, reuses packets, and evicts LRU peers", async () => {
    const received: Array<{ from: string; bytes: Uint8Array }> = [];
    const b = await endpoint({
      packet: (from, bytes): void => {
        received.push({ from, bytes });
      },
      stream: noop.stream,
    });
    const c = await endpoint();
    const a = await endpoint(noop, { maxPacketConnections: 1 });

    expect(a.address).not.toMatch(/:0$/);
    await a.packet(b.address, gossip());
    await a.packet(b.address, gossip());
    expect(a.pooledPacketConnections).toBe(1);
    expect(received).toHaveLength(2);
    expect(received[0]?.from).toBe(a.address);
    expect(b.activeConnections).toBe(1);

    await a.packet(c.address, gossip());
    expect(a.pooledPacketConnections).toBe(1);
    await delay(10);
    expect(b.activeConnections).toBe(0);
  });

  it("keeps stream framing opaque while validating roles and coexisting with packets", async () => {
    let packetFrom = "";
    const server = await endpoint({
      packet: (from): void => {
        packetFrom = from;
      },
      stream: async (_from, stream): Promise<void> => {
        const request = await stream.read();
        expect(request).toEqual(framed(sync(MESSAGE_SYNC_REQUEST)));
        await stream.write(framed(sync(MESSAGE_SYNC_RESPONSE)));
        stream.close();
      },
    });
    const client = await endpoint();

    await client.packet(server.address, gossip());
    const stream = await client.stream(server.address);
    await stream.write(framed(sync(MESSAGE_SYNC_REQUEST)));
    expect(await stream.read()).toEqual(framed(sync(MESSAGE_SYNC_RESPONSE)));
    stream.close();
    expect(packetFrom).toBe(client.address);
    expect(client.pooledPacketConnections).toBe(1);
  });

  it("closes malformed, oversized, slow, and half-open handshakes", async () => {
    const server = await endpoint(noop, { readTimeoutMs: 25 });

    const malformed = await raw(server.address);
    malformed.write(Uint8Array.of(0x42, 0x41, 0x44, 0x21, 1, 1, 0, 0));
    await closed(malformed);

    const oversized = await raw(server.address);
    oversized.write(
      Uint8Array.of(0x4e, 0x41, 0x4b, 0x54, 1, 1, 0, 0, 3, 0x78, 0x3a, 0x31, 0x05, 0x79),
    );
    await closed(oversized);

    const slow = await raw(server.address);
    slow.write(Uint8Array.of(0x4e));
    await closed(slow);

    const halfOpen = await raw(server.address);
    halfOpen.write(Uint8Array.of(0x4e, 0x41, 0x4b));
    halfOpen.end();
    await closed(halfOpen);
  });

  it("validates before allocation and rejects outbound role and size violations", async () => {
    const server = await endpoint();
    const client = await endpoint();
    const oversized = new Uint8Array(1401);
    oversized[0] = 1;
    new DataView(oversized.buffer).setUint16(2, 1397);

    await expect(client.packet(server.address, oversized)).rejects.toMatchObject({
      code: "protocol",
    });
    await expect(client.packet(server.address, sync(MESSAGE_SYNC_REQUEST))).rejects.toBeInstanceOf(
      ClusterTransportError,
    );
    expect(client.pooledPacketConnections).toBe(0);
  });

  it("bounds queued writes and cumulative stream exchange bytes", async () => {
    const server = await endpoint({
      packet: noop.packet,
      stream: async (_from, stream): Promise<void> => {
        while ((await stream.read()) !== undefined) {
          // Drain a legal exchange until the peer closes it.
        }
      },
    });
    const queued = await endpoint(noop, { maxQueuedBytes: 8 });
    await expect(queued.packet(server.address, gossip())).rejects.toMatchObject({ code: "queue" });

    const client = await endpoint();
    const stream = await client.stream(server.address);
    const message = new Uint8Array(65_539);
    message[0] = 1;
    message[1] = MESSAGE_SYNC_REQUEST;
    new DataView(message.buffer).setUint16(2, 65_535);
    const bytes = framed(message);
    for (let index = 0; index < 15; index += 1) {
      await stream.write(bytes);
    }
    await expect(stream.write(bytes)).rejects.toMatchObject({ code: "protocol" });
  });

  it("starts and stops idempotently and cannot restart", async () => {
    const transport = track(
      new TcpClusterTransport({
        host: "127.0.0.1",
        port: 0,
      }),
    );
    await Promise.all([transport.start(noop), transport.start(noop)]);
    expect(transport.address).not.toMatch(/:0$/);
    await Promise.all([transport.stop(), transport.stop()]);
    await expect(transport.start(noop)).rejects.toMatchObject({ code: "lifecycle" });
  });

  it("carries trusted and mutual TLS and rejects mismatched carriers and trust", async () => {
    const tls = {
      cert: pem("node.pem"),
      key: pem("node.key"),
      ca: pem("ca.pem"),
    };
    let received = false;
    const server = await endpoint(
      {
        packet: (): void => {
          received = true;
        },
        stream: noop.stream,
      },
      { tls },
    );
    const client = await endpoint(noop, { tls });
    await client.packet(server.address, gossip());
    expect(received).toBe(true);

    let mutualReceived = false;
    const mutual = await endpoint(
      {
        packet: (): void => {
          mutualReceived = true;
        },
        stream: noop.stream,
      },
      { tls: { ...tls, requestCert: true } },
    );
    await client.packet(mutual.address, gossip());
    expect(mutualReceived).toBe(true);

    const untrusted = await endpoint(noop, {
      tls: {
        cert: pem("other.pem"),
        key: pem("other.key"),
        ca: pem("other-ca.pem"),
      },
    });
    await expect(untrusted.stream(server.address)).rejects.toMatchObject({ code: "tls" });

    const plaintext = await endpoint();
    await expect(client.stream(plaintext.address)).rejects.toMatchObject({ code: "tls" });
  });

  it("joins real-socket SWIM engines with post-bind port-zero identities", async () => {
    const aTransport = track(
      await TcpClusterTransport.bind({
        host: "127.0.0.1",
        port: 0,
      }),
    );
    const bTransport = track(
      await TcpClusterTransport.bind({
        host: "127.0.0.1",
        port: 0,
      }),
    );
    const a = new Swim({
      address: aTransport.address,
      metadata: new Uint8Array(0),
      transport: aTransport,
      clock: wallClock,
      random: new SeededRandom(42),
    });
    const b = new Swim({
      address: bTransport.address,
      metadata: new Uint8Array(0),
      transport: bTransport,
      clock: wallClock,
      random: new SeededRandom(43),
    });
    await Promise.all([a.start(), b.start()]);
    await b.join([aTransport.address]);
    expect(a.members().some((member) => member.member === bTransport.address)).toBe(true);
    expect(b.members().some((member) => member.member === aTransport.address)).toBe(true);
    await Promise.all([a.stop(), b.stop()]);
  });
});
