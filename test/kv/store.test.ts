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
import type { DigestLanes } from "../../src/kv/entry";
import type { Entry, HybridTime } from "../../src/kv/ports";
import { Store } from "../../src/kv/store";

function at(wallMs: number, logical: number = 0, node: string = "n1"): HybridTime {
  return { wallMs, logical, node };
}

function value(key: string, timestamp: HybridTime, overrides: Partial<Entry> = {}): Entry {
  return {
    key,
    value: new Uint8Array([1]),
    timestamp,
    sequence: 1n,
    expiresAt: undefined,
    deleted: false,
    ...overrides,
  };
}

async function drain(source: AsyncGenerator<Entry[]>): Promise<Entry[][]> {
  const batches: Entry[][] = [];
  for await (const batch of source) {
    batches.push(batch);
  }

  return batches;
}

describe("Store construction", () => {
  it("rejects a non-positive partition count", () => {
    expect((): Store => new Store(0)).toThrow(RangeError);
    expect((): Store => new Store(1.5)).toThrow(RangeError);
  });
});

describe("Store routing", () => {
  it("materializes only the partitions it holds data for", () => {
    const store: Store = new Store(512);
    const keys: string[] = ["alpha", "bravo", "charlie", "delta"];
    for (const key of keys) {
      store.apply(value(key, at(1)));
    }

    const distinct: number = new Set(keys.map((key: string): number => store.partitionFor(key)))
      .size;
    expect(store.partitionsHeld).toBe(distinct);
  });

  it("reuses a partition on a second write to the same key", () => {
    const store: Store = new Store(1);
    expect(store.apply(value("k", at(1)))).toBe(true);
    expect(store.apply(value("k", at(2)))).toBe(true);
    expect(store.partitionsHeld).toBe(1);
    expect(store.get("k", 0)?.timestamp.wallMs).toBe(2);
  });
});

describe("Store read", () => {
  it("reads a live value, hides expired, and reports absence", () => {
    const store: Store = new Store(1);
    store.apply(value("live", at(1)));
    store.apply(value("stale", at(1), { expiresAt: 10 }));
    expect(store.get("live", 0)?.key).toBe("live");
    expect(store.get("stale", 10)).toBeUndefined();
    expect(store.get("missing", 0)).toBeUndefined();
    expect(store.peek("stale")?.expiresAt).toBe(10);
    expect(store.peek("missing")).toBeUndefined();
  });
});

describe("Store digest", () => {
  it("exposes a held partition's digest and nothing for an absent one", () => {
    const store: Store = new Store(1);
    store.apply(value("k", at(1)));
    const digest: DigestLanes | undefined = store.digest(0);
    expect(digest).not.toEqual({ hi: 0, lo: 0 });
    expect(store.digest(999)).toBeUndefined();
  });
});

describe("Store sweep", () => {
  it("reaps expired entries across held partitions and advances the cursor", () => {
    const store: Store = new Store(1);
    store.apply(value("live", at(1)));
    store.apply(value("stale", at(1), { expiresAt: 10 }));
    expect(store.peek("stale")?.key).toBe("stale");

    expect(store.sweep(10)).toBe(1);
    expect(store.peek("stale")).toBeUndefined();
    expect(store.get("live", 0)?.key).toBe("live");
    expect(store.sweep(10)).toBe(0);
  });

  it("is a no-op on an empty store", () => {
    const store: Store = new Store(4);
    expect(store.sweep(1_000)).toBe(0);
  });
});

describe("Store iterate", () => {
  it("yields nothing for a partition the node does not hold", async () => {
    const store: Store = new Store(1);
    expect(await drain(store.iterate(0))).toEqual([]);
  });

  it("chunks a partition into full batches with no trailing remainder", async () => {
    const store: Store = new Store(1);
    for (const key of ["a", "b", "c", "d"]) {
      store.apply(value(key, at(1)));
    }

    const batches: Entry[][] = await drain(store.iterate(0, 2));
    expect(batches.map((batch: Entry[]): number => batch.length)).toEqual([2, 2]);
    expect(batches.flat().map((entry: Entry): string => entry.key)).toEqual(["a", "b", "c", "d"]);
  });

  it("emits a trailing partial batch and defaults to a single batch", async () => {
    const store: Store = new Store(1);
    for (const key of ["a", "b", "c"]) {
      store.apply(value(key, at(1)));
    }

    expect(
      (await drain(store.iterate(0, 2))).map((batch: Entry[]): number => batch.length),
    ).toEqual([2, 1]);
    const whole: Entry[][] = await drain(store.iterate(0));
    expect(whole.length).toBe(1);
    expect(whole[0]?.length).toBe(3);
  });
});
