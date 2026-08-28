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

import { describe, expect, it } from "vitest";
import { ClusterRegistry, type RegistryStore } from "../src/clustering.registry";
import { ClusterResolver } from "../src/clustering.resolver";
import type { Entry, ScanEntry, WriteResult } from "../src/kv/ports";
import type { Path } from "../src/path";
import type { PID } from "../src/pid";

const SELF: string = "10.0.0.1:8080";
const SYSTEM: string = "orders";
/** Owner data address to the remoting endpoint its actors are reached at. */
const REMOTING: Map<string, string> = new Map([
  ["10.0.0.2:8080", "10.0.0.2:2552"],
  ["10.0.0.3:8080", "10.0.0.3:2552"],
  // A peer advertising a malformed remoting endpoint in its gossiped metadata.
  ["10.0.0.5:8080", "garbage"],
]);

const UTF8: TextEncoder = new TextEncoder();

function entryOf(key: string, value: Uint8Array): Entry {
  return {
    key,
    value,
    timestamp: { wallMs: 0, logical: 0, node: "test" },
    sequence: 0n,
    expiresAt: undefined,
    deleted: false,
  };
}

/** A registry store whose reads answer a name's owner from an in-memory map. */
class ReadStore implements RegistryStore {
  readonly owners: Map<string, string> = new Map();
  throwOnRead: boolean = false;
  reads: number = 0;

  write(): Promise<WriteResult> {
    return Promise.reject(new Error("the resolver never writes"));
  }

  read(key: string): Promise<Entry | undefined> {
    this.reads++;
    if (this.throwOnRead) {
      return Promise.reject(new Error("read wedged"));
    }

    const owner: string | undefined = this.owners.get(key);
    return Promise.resolve(owner === undefined ? undefined : entryOf(key, UTF8.encode(owner)));
  }

  scan(): Promise<ScanEntry[]> {
    return Promise.resolve([]);
  }
}

/** A resolver over the fake store, plus the paths it asked the remoting seam to handle. */
interface Harness {
  readonly resolver: ClusterResolver;
  readonly store: ReadStore;
  readonly handled: Path[];
}

function harness(): Harness {
  const store: ReadStore = new ReadStore();
  const handled: Path[] = [];
  const resolver: ClusterResolver = new ClusterResolver({
    registry: new ClusterRegistry(store),
    handleFor: (path: Path): PID | undefined => {
      handled.push(path);
      return { path: (): Path => path } as unknown as PID;
    },
    remotingAddressOf: (owner: string): string | undefined => REMOTING.get(owner),
    self: SELF,
    systemName: SYSTEM,
  });
  return { resolver, store, handled };
}

describe("ClusterResolver.find", () => {
  it("reads cold as undefined and warms the view in the background", async () => {
    const h: Harness = harness();
    h.store.owners.set("worker", "10.0.0.2:8080");

    // The first lookup misses and never blocks on the read it starts.
    expect(h.resolver.find("worker")).toBeUndefined();
    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 0);
    });

    // Once the view is warm the lookup resolves to a handle at the owner's
    // remoting endpoint.
    const handle: PID | undefined = h.resolver.find("worker");
    expect(handle).not.toBeUndefined();
    expect(h.handled).toHaveLength(1);
    const path: Path = h.handled[0] as Path;
    expect(path.name()).toBe("worker");
    expect(path.host()).toBe("10.0.0.2");
    expect(path.port()).toBe(2552);
    expect(path.system()).toBe(SYSTEM);
  });

  it("hands back nothing for a name owned by this node", async () => {
    const h: Harness = harness();
    h.store.owners.set("here", SELF);
    await h.resolver.resolve("here");

    expect(h.resolver.find("here")).toBeUndefined();
    expect(h.handled).toEqual([]);
  });

  it("hands back nothing when the owner is no longer a present member", async () => {
    const h: Harness = harness();
    // An owner with no remoting endpoint: departed, or never advertised one.
    h.store.owners.set("gone", "10.0.0.9:8080");
    await h.resolver.resolve("gone");

    expect(h.resolver.find("gone")).toBeUndefined();
    expect(h.handled).toEqual([]);
  });

  it("re-resolves a departed owner's entry so a relocation to a survivor is reached", async () => {
    const h: Harness = harness();
    // A warm entry to an owner that then departs, so it no longer advertises an endpoint.
    h.store.owners.set("worker", "10.0.0.9:8080");
    await h.resolver.resolve("worker");
    expect(h.resolver.find("worker")).toBeUndefined();

    // The actor relocates to a reachable survivor; the lookups' own re-resolve heals
    // the view to its new owner with no explicit resolve call.
    h.store.owners.set("worker", "10.0.0.2:8080");
    let handle: PID | undefined;
    for (let attempt: number = 0; attempt < 20 && handle === undefined; attempt++) {
      handle = h.resolver.find("worker");
      await new Promise<void>((resolve): void => {
        setTimeout(resolve, 5);
      });
    }

    expect(handle?.path().port()).toBe(2552);
  });

  it("resolves each present owner to its own remoting endpoint", async () => {
    const h: Harness = harness();
    h.store.owners.set("a", "10.0.0.2:8080");
    h.store.owners.set("b", "10.0.0.3:8080");
    await h.resolver.resolve("a");
    await h.resolver.resolve("b");

    expect(h.resolver.find("a")?.path().port()).toBe(2552);
    expect(h.resolver.find("b")?.path().host()).toBe("10.0.0.3");
  });

  it("stays total when a peer advertises a malformed remoting endpoint", async () => {
    const h: Harness = harness();
    h.store.owners.set("worker", "10.0.0.5:8080");
    await h.resolver.resolve("worker");

    // A corrupt endpoint collapses to "not reachable" rather than throwing out of
    // the synchronous lookup.
    expect(() => h.resolver.find("worker")).not.toThrow();
    expect(h.resolver.find("worker")).toBeUndefined();
    expect(h.handled).toEqual([]);
  });

  it("re-resolves a self-owned entry so a re-home to another node is picked up", async () => {
    const h: Harness = harness();
    h.store.owners.set("worker", SELF);
    await h.resolver.resolve("worker");
    // Owned here per the view, so the lookup is absent and drops the stale entry.
    expect(h.resolver.find("worker")).toBeUndefined();

    // The name re-homes to a remote node; the self branch's own re-resolve, driven
    // by the lookups, heals the view with no explicit resolve call.
    h.store.owners.set("worker", "10.0.0.2:8080");
    let handle: PID | undefined;
    for (let attempt: number = 0; attempt < 20 && handle === undefined; attempt++) {
      handle = h.resolver.find("worker");
      await new Promise<void>((resolve): void => {
        setTimeout(resolve, 5);
      });
    }

    expect(handle?.path().port()).toBe(2552);
  });
});

describe("ClusterResolver.resolve", () => {
  it("drops a view entry when no owner holds the name", async () => {
    const h: Harness = harness();
    h.store.owners.set("worker", "10.0.0.2:8080");
    await h.resolver.resolve("worker");
    expect(h.resolver.find("worker")).not.toBeUndefined();

    // The name is freed cluster-wide, so a re-resolve clears the stale entry.
    h.store.owners.delete("worker");
    await h.resolver.resolve("worker");
    expect(h.resolver.find("worker")).toBeUndefined();
  });

  it("collapses a burst of reads for one cold name into a single read", async () => {
    const h: Harness = harness();
    h.store.owners.set("worker", "10.0.0.2:8080");

    await Promise.all([
      h.resolver.resolve("worker"),
      h.resolver.resolve("worker"),
      h.resolver.resolve("worker"),
    ]);

    expect(h.store.reads).toBe(1);
  });

  it("leaves the view untouched when the read cannot reach its partition", async () => {
    const h: Harness = harness();
    h.store.owners.set("worker", "10.0.0.2:8080");
    h.store.throwOnRead = true;

    await h.resolver.resolve("worker");

    expect(h.resolver.find("worker")).toBeUndefined();
  });
});
