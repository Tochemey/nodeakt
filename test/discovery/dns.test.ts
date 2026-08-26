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

import { promises as dnsPromises } from "node:dns";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  DnsDiscovery,
  DnsRecordType,
  type DnsResolver,
  type DnsSrvRecord,
} from "../../src/discovery/dns";

/** A resolver whose named lookups are scripted; the rest reject as unexpectedly called. */
function fakeResolver(overrides: Partial<DnsResolver>): DnsResolver {
  const unused: () => Promise<never> = (): Promise<never> => Promise.reject(new Error("unused"));
  return {
    resolveSrv: overrides.resolveSrv ?? unused,
    resolve4: overrides.resolve4 ?? unused,
    resolve6: overrides.resolve6 ?? unused,
  };
}

/** A coded DNS rejection like the ones `node:dns` raises. */
function codedError(code: string): Error {
  const error: Error & { code?: string } = new Error(code);
  error.code = code;
  return error;
}

afterEach((): void => {
  vi.restoreAllMocks();
});

describe("DnsDiscovery SRV mode", () => {
  it("maps each SRV target and port to a seed by default", async () => {
    const resolver: DnsResolver = fakeResolver({
      resolveSrv: (): Promise<readonly DnsSrvRecord[]> =>
        Promise.resolve([
          { name: "pod-a", port: 6000 },
          { name: "pod-b", port: 6001 },
        ]),
    });
    const discovery: DnsDiscovery = new DnsDiscovery({ hostname: "svc.local", resolver });
    expect(await discovery.resolve()).toEqual(["pod-a:6000", "pod-b:6001"]);
  });

  it("honors an explicit SRV record type", async () => {
    const resolver: DnsResolver = fakeResolver({
      resolveSrv: (): Promise<readonly DnsSrvRecord[]> =>
        Promise.resolve([{ name: "pod-a", port: 6000 }]),
    });
    const discovery: DnsDiscovery = new DnsDiscovery({
      hostname: "svc.local",
      recordType: DnsRecordType.srv,
      resolver,
    });
    expect(await discovery.resolve()).toEqual(["pod-a:6000"]);
  });

  it("drops a trailing root dot from a fully qualified SRV target", async () => {
    const resolver: DnsResolver = fakeResolver({
      resolveSrv: (): Promise<readonly DnsSrvRecord[]> =>
        Promise.resolve([{ name: "pod-a.svc.cluster.local.", port: 6000 }]),
    });
    const discovery: DnsDiscovery = new DnsDiscovery({ hostname: "svc.local", resolver });
    expect(await discovery.resolve()).toEqual(["pod-a.svc.cluster.local:6000"]);
  });
});

describe("DnsDiscovery address mode", () => {
  it("attaches the configured port to each IPv4 A address", async () => {
    const resolver: DnsResolver = fakeResolver({
      resolve4: (): Promise<readonly string[]> => Promise.resolve(["10.0.0.1", "10.0.0.2"]),
    });
    const discovery: DnsDiscovery = new DnsDiscovery({
      hostname: "svc.local",
      recordType: DnsRecordType.address,
      port: 7000,
      resolver,
    });
    expect(await discovery.resolve()).toEqual(["10.0.0.1:7000", "10.0.0.2:7000"]);
  });

  it("brackets each IPv6 AAAA address before attaching the port", async () => {
    const resolver: DnsResolver = fakeResolver({
      resolve6: (): Promise<readonly string[]> => Promise.resolve(["::1", "fe80::2"]),
    });
    const discovery: DnsDiscovery = new DnsDiscovery({
      hostname: "svc.local",
      recordType: DnsRecordType.address6,
      port: 9000,
      resolver,
    });
    expect(await discovery.resolve()).toEqual(["[::1]:9000", "[fe80::2]:9000"]);
  });
});

describe("DnsDiscovery construction", () => {
  it("rejects a blank hostname", () => {
    expect((): DnsDiscovery => new DnsDiscovery({ hostname: "   " })).toThrow(TypeError);
  });

  it("rejects an address mode without a port", () => {
    expect(
      (): DnsDiscovery =>
        new DnsDiscovery({ hostname: "svc.local", recordType: DnsRecordType.address }),
    ).toThrow(RangeError);
    expect(
      (): DnsDiscovery =>
        new DnsDiscovery({ hostname: "svc.local", recordType: DnsRecordType.address6 }),
    ).toThrow(RangeError);
  });

  it("rejects an out-of-range or non-integer address-mode port", () => {
    const address: typeof DnsRecordType.address = DnsRecordType.address;
    expect(
      (): DnsDiscovery => new DnsDiscovery({ hostname: "svc.local", recordType: address, port: 0 }),
    ).toThrow(RangeError);
    expect(
      (): DnsDiscovery =>
        new DnsDiscovery({ hostname: "svc.local", recordType: address, port: 70_000 }),
    ).toThrow(RangeError);
    expect(
      (): DnsDiscovery =>
        new DnsDiscovery({ hostname: "svc.local", recordType: address, port: 1.5 }),
    ).toThrow(RangeError);
  });
});

describe("DnsDiscovery resolution-failure handling", () => {
  it("resolves empty for any coded DNS failure so the boot sequence retries", async () => {
    const codes: readonly string[] = ["ENOTFOUND", "ENODATA", "ESERVFAIL", "ETIMEOUT"];
    for (const code of codes) {
      const resolver: DnsResolver = fakeResolver({
        resolveSrv: (): Promise<readonly DnsSrvRecord[]> => Promise.reject(codedError(code)),
      });
      const discovery: DnsDiscovery = new DnsDiscovery({ hostname: "svc.local", resolver });
      expect(await discovery.resolve()).toEqual([]);
    }
  });

  it("propagates a rejection that carries no code", async () => {
    const resolver: DnsResolver = fakeResolver({
      resolveSrv: (): Promise<readonly DnsSrvRecord[]> => Promise.reject(new Error("boom")),
    });
    const discovery: DnsDiscovery = new DnsDiscovery({ hostname: "svc.local", resolver });
    await expect(discovery.resolve()).rejects.toThrow("boom");
  });

  it("propagates a null rejection", async () => {
    const resolver: DnsResolver = fakeResolver({
      resolveSrv: (): Promise<readonly DnsSrvRecord[]> => Promise.reject(null),
    });
    const discovery: DnsDiscovery = new DnsDiscovery({ hostname: "svc.local", resolver });
    await expect(discovery.resolve()).rejects.toBeNull();
  });

  it("propagates a non-object rejection", async () => {
    const resolver: DnsResolver = fakeResolver({
      resolveSrv: (): Promise<readonly DnsSrvRecord[]> => Promise.reject("plain string failure"),
    });
    const discovery: DnsDiscovery = new DnsDiscovery({ hostname: "svc.local", resolver });
    await expect(discovery.resolve()).rejects.toBe("plain string failure");
  });
});

describe("DnsDiscovery default node:dns resolver", () => {
  it("queries node:dns for SRV records when no resolver is injected", async () => {
    vi.spyOn(dnsPromises, "resolveSrv").mockResolvedValue([
      { name: "pod-a", port: 6000, priority: 1, weight: 1 },
    ]);
    const discovery: DnsDiscovery = new DnsDiscovery({ hostname: "svc.local" });
    expect(await discovery.resolve()).toEqual(["pod-a:6000"]);
    expect(dnsPromises.resolveSrv).toHaveBeenCalledWith("svc.local");
  });

  it("queries node:dns for A records when no resolver is injected", async () => {
    vi.spyOn(dnsPromises, "resolve4").mockResolvedValue(["10.0.0.1"]);
    const discovery: DnsDiscovery = new DnsDiscovery({
      hostname: "svc.local",
      recordType: DnsRecordType.address,
      port: 7000,
    });
    expect(await discovery.resolve()).toEqual(["10.0.0.1:7000"]);
    expect(dnsPromises.resolve4).toHaveBeenCalledWith("svc.local");
  });

  it("queries node:dns for AAAA records when no resolver is injected", async () => {
    vi.spyOn(dnsPromises, "resolve6").mockResolvedValue(["::1"]);
    const discovery: DnsDiscovery = new DnsDiscovery({
      hostname: "svc.local",
      recordType: DnsRecordType.address6,
      port: 9000,
    });
    expect(await discovery.resolve()).toEqual(["[::1]:9000"]);
    expect(dnsPromises.resolve6).toHaveBeenCalledWith("svc.local");
  });
});
