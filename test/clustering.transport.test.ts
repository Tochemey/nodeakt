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
import { afterEach, describe, expect, it } from "vitest";
import { formatHostPort, KvNetTransport, parseHostPort } from "../src/clustering.transport";
import type { TlsConfig } from "../src/net/tls";

/** Reads a PEM fixture shared with the carrier's own TLS tests. */
function pem(name: string): Buffer {
  return readFileSync(new URL(`./net/tls/${name}`, import.meta.url));
}

/** A trusted mutual-TLS pair, the same fixture material the carrier tests use. */
const TLS: TlsConfig = { cert: pem("node.pem"), key: pem("node.key"), ca: pem("ca.pem") };

/** Transports opened by a test, closed after it regardless of outcome. */
const opened: KvNetTransport[] = [];

function track(transport: KvNetTransport): KvNetTransport {
  opened.push(transport);
  return transport;
}

afterEach(async (): Promise<void> => {
  await Promise.all(opened.map((transport: KvNetTransport): Promise<void> => transport.close()));
  opened.length = 0;
});

/** Starts a listening transport whose handler records callers and transforms bodies. */
async function startServer(
  transform: (body: Uint8Array) => Uint8Array,
  tls?: TlsConfig,
): Promise<{ transport: KvNetTransport; callers: string[] }> {
  const transport: KvNetTransport = track(
    new KvNetTransport(
      tls === undefined ? { host: "127.0.0.1", port: 0 } : { host: "127.0.0.1", port: 0, tls },
    ),
  );
  const callers: string[] = [];
  transport.listen((from: string, body: Uint8Array): Promise<Uint8Array> => {
    callers.push(from);
    return Promise.resolve(transform(body));
  });
  await transport.start();
  return { transport, callers };
}

/** Starts a client transport bound to an ephemeral port so it advertises a real endpoint. */
async function startClient(tls?: TlsConfig): Promise<KvNetTransport> {
  const transport: KvNetTransport = track(
    new KvNetTransport(
      tls === undefined ? { host: "127.0.0.1", port: 0 } : { host: "127.0.0.1", port: 0, tls },
    ),
  );
  await transport.start();
  return transport;
}

describe("KvNetTransport request and response", () => {
  it("round-trips a request and reports the caller's advertised endpoint", async () => {
    const server: { transport: KvNetTransport; callers: string[] } = await startServer(
      (body: Uint8Array): Uint8Array => Uint8Array.from(body).reverse(),
    );
    const client: KvNetTransport = await startClient();

    const response: Uint8Array = await client.request(
      server.transport.address,
      Uint8Array.of(1, 2, 3),
      5_000,
    );
    expect(Array.from(response)).toEqual([3, 2, 1]);
    expect(server.callers).toEqual([client.address]);
  });

  it("reuses one pooled peer across requests to the same member", async () => {
    const server: { transport: KvNetTransport; callers: string[] } = await startServer(
      (body: Uint8Array): Uint8Array => body,
    );
    const client: KvNetTransport = await startClient();

    const first: Uint8Array = await client.request(
      server.transport.address,
      Uint8Array.of(1),
      5_000,
    );
    const second: Uint8Array = await client.request(
      server.transport.address,
      Uint8Array.of(2),
      5_000,
    );
    expect([Array.from(first), Array.from(second)]).toEqual([[1], [2]]);
    expect(server.callers).toHaveLength(2);
    expect(client.openPeers).toBe(1);
  });

  it("carries a payload larger than a fragment chunk over the large-transfer lane", async () => {
    const server: { transport: KvNetTransport; callers: string[] } = await startServer(
      (body: Uint8Array): Uint8Array => body,
    );
    const client: KvNetTransport = await startClient();

    const large: Uint8Array = new Uint8Array(512 * 1024);
    large.fill(7);
    large[0] = 1;
    large[large.length - 1] = 2;
    const response: Uint8Array = await client.request(server.transport.address, large, 10_000);
    expect(response.length).toBe(large.length);
    expect([response[0], response[response.length - 1]]).toEqual([1, 2]);
  });

  it("round-trips over TLS", async () => {
    const server: { transport: KvNetTransport; callers: string[] } = await startServer(
      (body: Uint8Array): Uint8Array => body,
      TLS,
    );
    const client: KvNetTransport = await startClient(TLS);

    const response: Uint8Array = await client.request(
      server.transport.address,
      Uint8Array.of(9),
      5_000,
    );
    expect(Array.from(response)).toEqual([9]);
  });
});

describe("KvNetTransport failure handling", () => {
  it("rejects when the store's handler rejects", async () => {
    const server: KvNetTransport = track(new KvNetTransport({ host: "127.0.0.1", port: 0 }));
    server.listen((): Promise<Uint8Array> => Promise.reject(new Error("handler boom")));
    await server.start();
    const client: KvNetTransport = await startClient();

    await expect(client.request(server.address, Uint8Array.of(1), 5_000)).rejects.toBeInstanceOf(
      Error,
    );
  });

  it("rejects when the peer has no inbound handler installed", async () => {
    const server: KvNetTransport = track(new KvNetTransport({ host: "127.0.0.1", port: 0 }));
    await server.start();
    const client: KvNetTransport = await startClient();

    await expect(client.request(server.address, Uint8Array.of(1), 5_000)).rejects.toBeInstanceOf(
      Error,
    );
  });

  it("rejects a request to an address with no listener", async () => {
    const client: KvNetTransport = await startClient();
    await expect(client.request("127.0.0.1:1", Uint8Array.of(1), 1_000)).rejects.toBeInstanceOf(
      Error,
    );
  });

  it("rejects a request after close", async () => {
    const client: KvNetTransport = await startClient();
    await client.close();
    await expect(client.request("127.0.0.1:6000", Uint8Array.of(1), 5_000)).rejects.toThrow(
      /closed/,
    );
  });

  it("closes cleanly when it was never started", async () => {
    const transport: KvNetTransport = new KvNetTransport({ host: "127.0.0.1", port: 0 });
    await expect(transport.close()).resolves.toBeUndefined();
  });
});

describe("host:port parsing", () => {
  it("parses an IPv4 and a bracketed IPv6 address", () => {
    expect(parseHostPort("127.0.0.1:6000")).toEqual({ host: "127.0.0.1", port: 6000 });
    expect(parseHostPort("[::1]:6000")).toEqual({ host: "::1", port: 6000 });
  });

  it("rejects a malformed or out-of-range address", () => {
    expect((): { host: string; port: number } => parseHostPort("nohost")).toThrow();
    expect((): { host: string; port: number } => parseHostPort("host:")).toThrow();
    expect((): { host: string; port: number } => parseHostPort(":6000")).toThrow();
    expect((): { host: string; port: number } => parseHostPort("host:0")).toThrow();
    expect((): { host: string; port: number } => parseHostPort("host:70000")).toThrow();
    expect((): { host: string; port: number } => parseHostPort("host:notaport")).toThrow();
    expect((): { host: string; port: number } => parseHostPort("host:6e3")).toThrow();
    expect((): { host: string; port: number } => parseHostPort("host:0x10")).toThrow();
    expect((): { host: string; port: number } => parseHostPort("host: 6000")).toThrow();
  });

  it("formats IPv4 plainly and brackets IPv6", () => {
    expect(formatHostPort("127.0.0.1", 6000)).toBe("127.0.0.1:6000");
    expect(formatHostPort("::1", 6000)).toBe("[::1]:6000");
  });
});
