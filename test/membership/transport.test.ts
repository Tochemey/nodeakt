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
import { readFileSync } from "node:fs";
import { connect, createServer, type Server, type Socket } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { wallClock } from "../../src/membership/clock";
import { SeededRandom } from "../../src/membership/random";
import { Swim } from "../../src/membership/swim";
import {
  MembershipTransportError,
  TcpMembershipTransport,
  type TransportHandlers,
} from "../../src/membership/transport";
import {
  encodeMessage,
  MESSAGE_ACK,
  MESSAGE_GOSSIP,
  MESSAGE_NACK,
  MESSAGE_PING,
  MESSAGE_PING_REQ,
  MESSAGE_SYNC_REQUEST,
  MESSAGE_SYNC_RESPONSE,
} from "../../src/membership/wire";

const transports = new Set<TcpMembershipTransport>();
const servers = new Set<Server>();
const serverSockets = new Set<Socket>();
const noop: TransportHandlers = {
  packet: (): void => undefined,
  stream: (_from, stream): void => stream.close(),
};

type WriteCallback = (error?: Error | null) => void;

class FakeSocket extends EventEmitter {
  connects = true;
  destroyed = false;
  writable = true;
  writeBehavior: (callback: WriteCallback) => void = (callback): void => callback();

  write(_bytes: Uint8Array, callback: WriteCallback): boolean {
    this.writeBehavior(callback);
    return true;
  }

  destroy(error?: Error): this {
    if (this.destroyed) {
      return this;
    }

    this.destroyed = true;
    this.writable = false;
    if (error !== undefined) {
      this.emit("error", error);
    }
    this.emit("close");
    return this;
  }

  end(): this {
    return this.destroy();
  }
}

class FakeServer extends EventEmitter {
  listening = false;
  readonly accepted: (socket: Socket) => void;

  constructor(accepted: (socket: Socket) => void) {
    super();
    this.accepted = accepted;
  }

  address(): { address: string; family: string; port: number } {
    return { address: "127.0.0.1", family: "IPv4", port: 12345 };
  }

  listen(_port: number, _host: string, callback: () => void): this {
    this.listening = true;
    callback();
    return this;
  }

  close(callback?: () => void): this {
    this.listening = false;
    callback?.();
    return this;
  }
}

function track(transport: TcpMembershipTransport): TcpMembershipTransport {
  transports.add(transport);
  return transport;
}

async function endpoint(
  handlers: TransportHandlers = noop,
  options: Partial<ConstructorParameters<typeof TcpMembershipTransport>[0]> = {},
): Promise<TcpMembershipTransport> {
  const transport = track(
    await TcpMembershipTransport.bind({
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

function envelope(type: number): Uint8Array {
  return Uint8Array.of(1, type, 0, 0);
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

function packetFrame(bytes: Uint8Array): Uint8Array {
  const result = new Uint8Array(2 + bytes.length);
  new DataView(result.buffer).setUint16(0, bytes.length);
  result.set(bytes, 2);
  return result;
}

function transportHandshake(role: 1 | 2, address: string): Uint8Array {
  const identity = new TextEncoder().encode(address);
  return Uint8Array.of(0x4e, 0x41, 0x4b, 0x54, 1, role, 0, 0, identity.length, ...identity);
}

function raw(address: string): Promise<Socket> {
  const colon = address.lastIndexOf(":");
  return new Promise<Socket>((resolve, reject): void => {
    const socket = connect(Number(address.slice(colon + 1)), address.slice(0, colon));
    socket.once("connect", (): void => resolve(socket));
    socket.once("error", reject);
  });
}

function listen(server: Server): Promise<string> {
  servers.add(server);
  return new Promise<string>((resolve, reject): void => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", (): void => {
      server.removeListener("error", reject);
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("test server returned no TCP address"));
        return;
      }
      resolve(`127.0.0.1:${address.port}`);
    });
  });
}

async function carrier(accept: (socket: Socket) => void): Promise<string> {
  return listen(
    createServer({ allowHalfOpen: true }, (socket): void => {
      serverSockets.add(socket);
      socket.on("error", (): void => undefined);
      socket.once("close", (): void => {
        serverSockets.delete(socket);
      });
      accept(socket);
    }),
  );
}

function closed(socket: Socket): Promise<void> {
  if (socket.destroyed) {
    return Promise.resolve();
  }
  return new Promise<void>((resolve): void => {
    socket.once("close", (): void => resolve());
  });
}

async function waitUntil(read: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (read()) {
      return;
    }
    await delay(5);
  }
}

function pem(name: string): Buffer {
  return readFileSync(new URL(`../net/tls/${name}`, import.meta.url));
}

afterEach(async () => {
  vi.doUnmock("node:net");
  vi.doUnmock("node:tls");
  vi.resetModules();
  await Promise.all(Array.from(transports, (transport) => transport.stop()));
  transports.clear();
  for (const socket of serverSockets) {
    socket.destroy();
  }
  serverSockets.clear();
  await Promise.all(
    Array.from(
      servers,
      (server): Promise<void> =>
        new Promise<void>((resolve): void => {
          server.close((): void => resolve());
        }),
    ),
  );
  servers.clear();
});

describe("TcpMembershipTransport", () => {
  it("validates configuration, identities, and IPv4 and IPv6 peer addresses", async () => {
    for (const options of [
      { host: "", port: 1 },
      { host: "127.0.0.1", port: -1 },
      { host: "127.0.0.1", port: 1.5 },
      { host: "127.0.0.1", port: 65_536 },
    ]) {
      expect(() => new TcpMembershipTransport(options)).toThrowError(
        expect.objectContaining({ code: "address" }),
      );
    }

    expect(
      () =>
        new TcpMembershipTransport({
          host: "127.0.0.1",
          port: 1,
          tls: { cert: "certificate" } as never,
        }),
    ).toThrowError(expect.objectContaining({ code: "tls" }));

    for (const options of [
      { maxPacketConnections: 0 },
      { maxQueuedBytes: 1.5 },
      { connectTimeoutMs: 0 },
      { readTimeoutMs: 0 },
      { writeTimeoutMs: 0 },
      { exchangeTimeoutMs: 0 },
    ]) {
      expect(
        () => new TcpMembershipTransport({ host: "127.0.0.1", port: 1, ...options }),
      ).toThrowError(expect.objectContaining({ code: "address" }));
    }

    expect(
      () =>
        new TcpMembershipTransport({
          host: "127.0.0.1",
          port: 1,
          advertiseHost: "bad\0host",
        }),
    ).toThrowError(expect.objectContaining({ code: "address", peer: "bad\0host:1" }));
    expect(
      () =>
        new TcpMembershipTransport({
          host: "127.0.0.1",
          port: 1,
          advertiseHost: "x".repeat(256),
        }),
    ).toThrowError(expect.objectContaining({ code: "address" }));

    const ipv6 = new TcpMembershipTransport({
      host: "::1",
      port: 1,
      advertiseHost: "2001:db8::1",
      maxPacketConnections: 1,
      maxQueuedBytes: 1,
      connectTimeoutMs: 1,
      readTimeoutMs: 1,
      writeTimeoutMs: 1,
      exchangeTimeoutMs: 1,
    });
    expect(ipv6.address).toBe("[2001:db8::1]:1");

    const client = await endpoint();
    for (const address of [
      "missing-port",
      ":1",
      "[::1",
      "[::1]x",
      "host:0",
      "host:x",
      "host:65536",
    ]) {
      await expect(client.stream(address)).rejects.toMatchObject({
        code: "address",
        peer: address,
      });
    }
  });

  it("accepts every packet role type and rejects malformed envelopes before dialing", async () => {
    const received: number[] = [];
    const server = await endpoint({
      packet: (_from, bytes): void => {
        received.push(bytes[1] as number);
      },
      stream: noop.stream,
    });
    const client = await endpoint();

    for (const type of [
      MESSAGE_PING,
      MESSAGE_PING_REQ,
      MESSAGE_ACK,
      MESSAGE_NACK,
      MESSAGE_GOSSIP,
    ]) {
      await client.packet(server.address, envelope(type));
    }
    await waitUntil((): boolean => received.length === 5);
    expect(received).toEqual([
      MESSAGE_PING,
      MESSAGE_PING_REQ,
      MESSAGE_ACK,
      MESSAGE_NACK,
      MESSAGE_GOSSIP,
    ]);

    for (const bytes of [
      new Uint8Array(3),
      Uint8Array.of(2, MESSAGE_GOSSIP, 0, 0),
      Uint8Array.of(1, MESSAGE_GOSSIP, 0, 1),
      envelope(255),
    ]) {
      await expect(client.packet(server.address, bytes)).rejects.toMatchObject({
        code: "protocol",
      });
    }
  });

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
    await waitUntil((): boolean => received.length === 2);
    expect(a.pooledPacketConnections).toBe(1);
    expect(received).toHaveLength(2);
    expect(received[0]?.from).toBe(a.address);
    expect(b.activeConnections).toBe(1);

    await a.packet(c.address, gossip());
    expect(a.pooledPacketConnections).toBe(1);
    await waitUntil((): boolean => b.activeConnections === 0);
    expect(b.activeConnections).toBe(0);
  });

  it("rejects connections before handlers are installed and removes closed pooled sockets", async () => {
    const unstarted = track(
      await TcpMembershipTransport.bind({
        host: "127.0.0.1",
        port: 0,
      }),
    );
    const premature = await raw(unstarted.address);
    await closed(premature);
    await unstarted.start(noop);

    const closingAddress = await carrier((socket): void => {
      socket.once("data", (): void => {
        setTimeout((): void => {
          socket.destroy();
        }, 5);
      });
    });
    const client = await endpoint();
    await client.packet(closingAddress, gossip());
    expect(client.pooledPacketConnections).toBe(1);
    await waitUntil((): boolean => client.pooledPacketConnections === 0);
    expect(client.pooledPacketConnections).toBe(0);
  });

  it("settles concurrent packet failures and rejected LRU eviction independently", async () => {
    const client = await endpoint(noop, { maxPacketConnections: 1 });
    const first = client.packet("127.0.0.1:1", gossip());
    const sameConnection = client.packet("127.0.0.1:1", gossip());
    const replacement = client.packet("127.0.0.1:2", gossip());
    const results = await Promise.allSettled([first, sameConnection, replacement]);

    expect(results.every((result): boolean => result.status === "rejected")).toBe(true);
    expect(client.pooledPacketConnections).toBe(0);
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

  it("rejects invalid inbound identities, queue overflow, and packet framing", async () => {
    const server = await endpoint(noop, { maxQueuedBytes: 32, readTimeoutMs: 100 });
    const invalidHandshakes = [
      Uint8Array.of(0x4e, 0x41, 0x4b, 0x54, 1, 1, 0, 0, 0),
      Uint8Array.of(0x4e, 0x41, 0x4b, 0x54, 1, 1, 0, 0, 1, 0xff),
      Uint8Array.of(0x4e, 0x41, 0x4b, 0x54, 1, 1, 0, 0, 3, 0xef, 0xbb, 0xbf),
      transportHandshake(1, "x\0:1"),
      transportHandshake(1, "missing-port"),
    ];

    for (const bytes of invalidHandshakes) {
      const socket = await raw(server.address);
      socket.write(bytes);
      await closed(socket);
    }

    for (const length of [3, 1_401]) {
      const socket = await raw(server.address);
      socket.write(transportHandshake(1, "127.0.0.1:1"));
      const prefix = new Uint8Array(2);
      new DataView(prefix.buffer).setUint16(0, length);
      socket.write(prefix);
      await closed(socket);
    }

    const malformedEnvelope = await raw(server.address);
    malformedEnvelope.write(transportHandshake(1, "127.0.0.1:1"));
    malformedEnvelope.write(packetFrame(Uint8Array.of(2, MESSAGE_GOSSIP, 0, 0)));
    await closed(malformedEnvelope);

    const constrained = await endpoint(noop, { maxQueuedBytes: 8 });
    const overflowing = await raw(constrained.address);
    overflowing.write(new Uint8Array(9));
    await closed(overflowing);

    const tooSmallToHandshake = await endpoint(noop, { maxQueuedBytes: 4 });
    const rejectedBeforeRead = await raw(tooSmallToHandshake.address);
    await closed(rejectedBeforeRead);
  });

  it("consumes fragmented IPv6 handshakes and packet frames and handles clean EOF", async () => {
    const received: Array<{ from: string; bytes: Uint8Array }> = [];
    const server = await endpoint({
      packet: (from, bytes): void => {
        received.push({ from, bytes });
      },
      stream: noop.stream,
    });
    const socket = await raw(server.address);
    const identity = "[::1]:1";
    const preface = transportHandshake(1, identity);
    const packet = packetFrame(gossip());

    socket.write(preface.subarray(0, 3));
    socket.write(preface.subarray(3, 10));
    socket.write(preface.subarray(10));
    socket.write(packet.subarray(0, 1));
    socket.write(packet.subarray(1, 4));
    socket.write(packet.subarray(4));
    await waitUntil((): boolean => received.length === 1);
    socket.end();
    await closed(socket);

    expect(received).toEqual([{ from: identity, bytes: gossip() }]);
  });

  it("isolates packet and stream handler failures to their connections", async () => {
    const server = await endpoint({
      packet: (): never => {
        throw new Error("packet handler failed");
      },
      stream: (): never => {
        throw new Error("stream handler failed");
      },
    });
    const packetSocket = await raw(server.address);
    packetSocket.write(transportHandshake(1, "127.0.0.1:1"));
    packetSocket.write(packetFrame(gossip()));
    await closed(packetSocket);

    const client = await endpoint();
    const stream = await client.stream(server.address);
    expect(await stream.read()).toBeUndefined();
    stream.close();
    await waitUntil((): boolean => server.activeConnections === 0);

    const nonErrorServer = await endpoint({
      packet: noop.packet,
      stream: (): never => {
        throw 42;
      },
    });
    const nonErrorStream = await client.stream(nonErrorServer.address);
    expect(await nonErrorStream.read()).toBeUndefined();
  });

  it("distinguishes socket read failures from read timeouts and preserves their cause", async () => {
    let accepted: Socket | undefined;
    const failingServer = createServer({ allowHalfOpen: true }, (socket): void => {
      accepted = socket;
      serverSockets.add(socket);
      socket.on("error", (): void => undefined);
      socket.once("close", (): void => {
        serverSockets.delete(socket);
      });
    });
    const address = await listen(failingServer);
    const client = await endpoint(noop, { exchangeTimeoutMs: 1_000 });
    const stream = await client.stream(address);

    accepted?.resetAndDestroy();

    await expect(stream.read()).rejects.toMatchObject({
      code: "read",
      cause: expect.any(Error),
    });
    stream.close();

    const stalledServer = createServer({ allowHalfOpen: true }, (socket): void => {
      serverSockets.add(socket);
      socket.on("error", (): void => undefined);
      socket.once("close", (): void => {
        serverSockets.delete(socket);
      });
    });
    const stalledAddress = await listen(stalledServer);
    const impatientClient = await endpoint(noop, { exchangeTimeoutMs: 20 });
    const stalledStream = await impatientClient.stream(stalledAddress);

    const timeoutError = await stalledStream.read().catch((error: unknown): unknown => error);
    expect(timeoutError).toMatchObject({ code: "read_timeout" });
    expect((timeoutError as Error).cause).toBeUndefined();
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
      MembershipTransportError,
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

  it("validates fragmented inbound stream frames and incomplete EOF", async () => {
    const response = framed(sync(MESSAGE_SYNC_RESPONSE));
    const address = await carrier((socket): void => {
      socket.once("data", (): void => {
        socket.write(response.subarray(0, 2));
        setTimeout((): void => {
          socket.write(response.subarray(2, 7));
          setTimeout((): void => {
            socket.write(response.subarray(7));
            socket.end();
          }, 5);
        }, 5);
      });
    });
    const client = await endpoint();
    const stream = await client.stream(address);

    expect(await stream.read()).toEqual(response.subarray(0, 2));
    expect(await stream.read()).toEqual(response.subarray(2, 7));
    expect(await stream.read()).toEqual(response.subarray(7));
    expect(await stream.read()).toBeUndefined();
    stream.close();
    stream.close();
    expect(await stream.read()).toBeUndefined();

    const incompleteAddress = await carrier((socket): void => {
      socket.once("data", (): void => {
        socket.end(Uint8Array.of(0, 0));
      });
    });
    const incomplete = await client.stream(incompleteAddress);
    expect(await incomplete.read()).toEqual(Uint8Array.of(0, 0));
    await expect(incomplete.read()).rejects.toMatchObject({ code: "protocol" });
  });

  it("rejects invalid inbound and outbound stream frames", async () => {
    const payloads = [
      Uint8Array.of(0, 0, 0, 3),
      framed(Uint8Array.of(2, MESSAGE_SYNC_RESPONSE, 0, 0)),
      framed(envelope(MESSAGE_GOSSIP)),
    ];
    const client = await endpoint();

    for (const payload of payloads) {
      const address = await carrier((socket): void => {
        socket.once("data", (): void => {
          socket.write(payload);
        });
      });
      const stream = await client.stream(address);
      await expect(stream.read()).rejects.toMatchObject({ code: "protocol" });
    }

    const sink = await carrier((): void => undefined);
    for (const payload of payloads) {
      const stream = await client.stream(sink);
      await expect(stream.write(payload)).rejects.toMatchObject({ code: "protocol" });
    }
  });

  it("bounds cumulative inbound stream bytes and rejects writes after close", async () => {
    const message = new Uint8Array(65_539);
    message[0] = 1;
    message[1] = MESSAGE_SYNC_RESPONSE;
    new DataView(message.buffer).setUint16(2, 65_535);
    const bytes = framed(message);
    const address = await carrier((socket): void => {
      socket.once("data", (): void => {
        for (let index = 0; index < 16; index += 1) {
          socket.write(bytes);
        }
      });
    });
    const client = await endpoint(noop, { maxQueuedBytes: 2 * 1024 * 1024 });
    const stream = await client.stream(address);
    let readError: unknown;

    while (readError === undefined) {
      await stream.read().catch((error: unknown): void => {
        readError = error;
      });
    }
    expect(readError).toMatchObject({ code: "protocol" });

    const writable = await client.stream(await carrier((): void => undefined));
    writable.close();
    await expect(writable.write(framed(sync(MESSAGE_SYNC_REQUEST)))).rejects.toMatchObject({
      code: "write",
    });
  });

  it("starts and stops idempotently and cannot restart", async () => {
    const transport = track(
      new TcpMembershipTransport({
        host: "127.0.0.1",
        port: 0,
      }),
    );
    await Promise.all([transport.start(noop), transport.start(noop)]);
    expect(transport.address).not.toMatch(/:0$/);
    await Promise.all([transport.stop(), transport.stop()]);
    await expect(transport.start(noop)).rejects.toMatchObject({ code: "lifecycle" });
    await expect(transport.packet("127.0.0.1:1", gossip())).rejects.toMatchObject({
      code: "stopped",
    });
    await expect(transport.stream("127.0.0.1:1")).rejects.toMatchObject({ code: "stopped" });
  });

  it("surfaces bind and dial failures and can retry a failed start", async () => {
    const occupied = createServer();
    const occupiedAddress = await listen(occupied);
    const port = Number(occupiedAddress.slice(occupiedAddress.lastIndexOf(":") + 1));
    const transport = track(new TcpMembershipTransport({ host: "127.0.0.1", port }));

    await expect(transport.start(noop)).rejects.toMatchObject({
      code: "connect",
      cause: expect.any(Error),
    });
    await expect(transport.start(noop)).rejects.toMatchObject({ code: "connect" });
    await expect(transport.stop()).resolves.toBeUndefined();

    const client = await endpoint();
    await expect(client.stream("127.0.0.1:1")).rejects.toMatchObject({
      code: "connect",
      cause: expect.any(Error),
    });
    await expect(client.packet("missing-port", gossip())).rejects.toMatchObject({
      code: "address",
    });
    expect(client.pooledPacketConnections).toBe(0);

    await new Promise<void>((resolve): void => {
      occupied.close((): void => resolve());
    });
    servers.delete(occupied);
    const fixed = await endpoint(noop, { port });
    expect(fixed.address).toBe(`127.0.0.1:${port}`);
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
    const client = await endpoint(noop, { tls: { ...tls, servername: "localhost" } });
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

    const occupied = createServer();
    const occupiedAddress = await listen(occupied);
    const occupiedPort = Number(occupiedAddress.slice(occupiedAddress.lastIndexOf(":") + 1));
    const blockedTls = track(
      new TcpMembershipTransport({
        host: "127.0.0.1",
        port: occupiedPort,
        tls,
      }),
    );
    await expect(blockedTls.start(noop)).rejects.toMatchObject({ code: "tls" });
  });

  it("quiesces mocked listeners after post-bind failures and rejects stale accepts", async () => {
    const fakeServers: FakeServer[] = [];
    vi.doMock("node:net", () => ({
      connect: (): FakeSocket => new FakeSocket(),
      createServer: (
        _options: { allowHalfOpen: boolean },
        accepted: (socket: Socket) => void,
      ): FakeServer => {
        const server = new FakeServer(accepted);
        fakeServers.push(server);
        return server;
      },
    }));
    const { TcpMembershipTransport: MockTransport } = await import(
      "../../src/membership/transport"
    );
    const beforeStart = await MockTransport.bind({ host: "127.0.0.1", port: 0 });
    const premature = new FakeSocket();
    fakeServers[0]?.accepted(premature as unknown as Socket);
    expect(premature.destroyed).toBe(true);
    await beforeStart.start(noop);
    await beforeStart.stop();
    const afterStop = new FakeSocket();
    fakeServers[0]?.accepted(afterStop as unknown as Socket);
    expect(afterStop.destroyed).toBe(true);

    const failed = new MockTransport({ host: "127.0.0.1", port: 0 });
    await failed.start(noop);
    const failedServer = fakeServers[1] as FakeServer;
    const cause = new Error("listener failed");
    failedServer.emit("error", cause);
    failedServer.emit("error", new Error("stale listener failure"));
    await expect(failed.start(noop)).rejects.toMatchObject({ code: "lifecycle", cause });
    await expect(failed.packet("127.0.0.1:1", gossip())).rejects.toMatchObject({
      code: "lifecycle",
    });
    await expect(failed.stop()).rejects.toMatchObject({ code: "lifecycle" });

    const alreadyClosed = new MockTransport({ host: "127.0.0.1", port: 0 });
    await alreadyClosed.start(noop);
    const closedServer = fakeServers[2] as FakeServer;
    closedServer.listening = false;
    closedServer.emit("error", new Error("closed listener failed"));
    await expect(alreadyClosed.stop()).rejects.toMatchObject({ code: "lifecycle" });
  });

  it("adapts mocked socket creation, connection, write-error, and timeout failures", async () => {
    const sockets: Array<FakeSocket | Error> = [];
    vi.doMock("node:net", () => ({
      connect: (): FakeSocket => {
        const next = sockets.shift();
        if (next instanceof Error) {
          throw next;
        }

        const socket = next as FakeSocket;
        queueMicrotask((): void => {
          if (socket.connects && !socket.destroyed) {
            socket.emit("connect");
          }
        });
        return socket;
      },
      createServer: (
        _options: { allowHalfOpen: boolean },
        accepted: (socket: Socket) => void,
      ): FakeServer => new FakeServer(accepted),
    }));
    const { MembershipTransportError: MockTransportError, TcpMembershipTransport: MockTransport } =
      await import("../../src/membership/transport");
    const client = new MockTransport({
      host: "127.0.0.1",
      port: 0,
      connectTimeoutMs: 5,
      writeTimeoutMs: 5,
    });
    await client.start(noop);

    const creationCause = new Error("socket creation failed");
    sockets.push(creationCause);
    await expect(client.stream("127.0.0.1:1")).rejects.toMatchObject({
      code: "connect",
      cause: creationCause,
    });

    const connecting = new FakeSocket();
    connecting.connects = false;
    sockets.push(connecting);
    await expect(client.stream("127.0.0.1:1")).rejects.toMatchObject({ code: "connect" });

    const delayedCallback = new FakeSocket();
    delayedCallback.writeBehavior = (callback): void => {
      setTimeout((): void => callback(), 20);
    };
    sockets.push(delayedCallback);
    await expect(client.stream("127.0.0.1:1")).rejects.toMatchObject({
      code: "write_timeout",
    });
    await delay(25);

    const writeCause = new Error("write failed");
    const writeFailure = new FakeSocket();
    writeFailure.writeBehavior = (callback): void => callback(writeCause);
    sockets.push(writeFailure);
    await expect(client.stream("127.0.0.1:1")).rejects.toMatchObject({
      code: "write",
      cause: writeCause,
    });

    const existing = new MockTransportError("write", "existing transport failure");
    const existingFailure = new FakeSocket();
    existingFailure.writeBehavior = (callback): void => callback(existing);
    sockets.push(existingFailure);
    await expect(client.stream("127.0.0.1:1")).rejects.toBe(existing);

    const pooled = new FakeSocket();
    sockets.push(pooled);
    await client.packet("127.0.0.1:1", gossip());
    pooled.destroyed = true;
    pooled.writable = false;
    sockets.push(new FakeSocket());
    await client.packet("127.0.0.1:1", gossip());

    const duplicateSignal = new FakeSocket();
    vi.spyOn(duplicateSignal, "removeListener").mockReturnValue(duplicateSignal);
    sockets.push(duplicateSignal);
    const stream = await client.stream("127.0.0.1:1");
    duplicateSignal.emit("error", new Error("late duplicate carrier signal"));
    stream.close();
    await client.stop();
  });

  it("adapts mocked TLS listener and outbound socket construction failures", async () => {
    vi.doMock("node:tls", () => ({
      connect: (): never => {
        throw new Error("invalid TLS connection options");
      },
      createServer: (): never => {
        throw new Error("invalid TLS listener options");
      },
    }));
    const { TcpMembershipTransport: MockTransport } = await import(
      "../../src/membership/transport"
    );
    const tls = { cert: "certificate", key: "private key" };
    const listener = new MockTransport({ host: "127.0.0.1", port: 0, tls });
    await expect(listener.start(noop)).rejects.toMatchObject({ code: "tls" });

    vi.doMock("node:tls", () => ({
      connect: (): never => {
        throw new Error("invalid TLS connection options");
      },
      createServer: (_options: object, accepted: (socket: Socket) => void): FakeServer =>
        new FakeServer(accepted),
    }));
    vi.resetModules();
    const { TcpMembershipTransport: DialTransport } = await import(
      "../../src/membership/transport"
    );
    const client = new DialTransport({ host: "127.0.0.1", port: 0, tls });
    await client.start(noop);
    await expect(client.stream("127.0.0.1:1")).rejects.toMatchObject({ code: "tls" });
    await client.stop();
  });

  it("joins real-socket SWIM engines with post-bind port-zero identities", async () => {
    const aTransport = track(
      await TcpMembershipTransport.bind({
        host: "127.0.0.1",
        port: 0,
      }),
    );
    const bTransport = track(
      await TcpMembershipTransport.bind({
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
