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
import { Engine, PartitionPipeline } from "../../src/kv/engine";
import type { WriteResult } from "../../src/kv/ports";

/** A physical clock the test advances by hand. */
function clockAt(start: number): { now: () => number; set: (value: number) => void } {
  let current: number = start;
  return {
    now: (): number => current,
    set: (value: number): void => {
      current = value;
    },
  };
}

function bytes(...values: number[]): Uint8Array {
  return new Uint8Array(values);
}

function counter(value: Uint8Array | undefined): bigint {
  const view: DataView = new DataView(
    (value as Uint8Array).buffer,
    (value as Uint8Array).byteOffset,
    8,
  );
  return view.getBigInt64(0);
}

function newEngine(now: () => number = (): number => 1_000): Engine {
  return new Engine("n1", 8, now);
}

describe("PartitionPipeline", () => {
  it("runs queued tasks in submission order", async () => {
    const pipeline: PartitionPipeline = new PartitionPipeline();
    const order: number[] = [];
    const a: Promise<number> = pipeline.run((): number => {
      order.push(1);
      return 1;
    });
    const b: Promise<number> = pipeline.run((): number => {
      order.push(2);
      return 2;
    });
    expect(await Promise.all([a, b])).toEqual([1, 2]);
    expect(order).toEqual([1, 2]);
  });

  it("survives a task that throws and keeps serving the next", async () => {
    const pipeline: PartitionPipeline = new PartitionPipeline();
    const failed: Promise<never> = pipeline.run((): never => {
      throw new Error("boom");
    });
    await expect(failed).rejects.toThrow("boom");
    expect(await pipeline.run((): number => 42)).toBe(42);
  });

  it("hands out monotone sequence numbers from one", () => {
    const pipeline: PartitionPipeline = new PartitionPipeline();
    expect(pipeline.nextSequence()).toBe(1n);
    expect(pipeline.nextSequence()).toBe(2n);
  });
});

describe("Engine construction", () => {
  it("delegates validation to the clock and the store", () => {
    expect((): Engine => new Engine("", 8, (): number => 0)).toThrow(RangeError);
    expect((): Engine => new Engine("n1", 0, (): number => 0)).toThrow(RangeError);
  });
});

describe("Engine put", () => {
  it("writes an unconditional value and reads it back", async () => {
    const engine: Engine = newEngine();
    const result: WriteResult = await engine.write({
      kind: "put",
      key: "k",
      value: bytes(1, 2),
      condition: "none",
    });
    expect(result.applied).toBe(true);
    expect((await engine.read("k"))?.value).toEqual(bytes(1, 2));
  });

  it("stamps the node, a monotone sequence, and reports the entry", async () => {
    const engine: Engine = newEngine();
    const first: WriteResult = await engine.write({
      kind: "put",
      key: "k",
      value: bytes(1),
      condition: "none",
    });
    const second: WriteResult = await engine.write({
      kind: "put",
      key: "k",
      value: bytes(2),
      condition: "none",
    });
    expect(first.applied && first.entry.timestamp.node).toBe("n1");
    expect(first.applied && first.entry.sequence).toBe(1n);
    expect(second.applied && second.entry.sequence).toBe(2n);
  });

  it("honours nx: rejects over a live key, applies when absent", async () => {
    const engine: Engine = newEngine();
    await engine.write({ kind: "put", key: "k", value: bytes(1), condition: "none" });
    expect(await engine.write({ kind: "put", key: "k", value: bytes(2), condition: "nx" })).toEqual(
      {
        applied: false,
        reason: "nx",
      },
    );
    expect(
      (await engine.write({ kind: "put", key: "fresh", value: bytes(9), condition: "nx" })).applied,
    ).toBe(true);
  });

  it("honours xx: rejects when absent, applies over a live key", async () => {
    const engine: Engine = newEngine();
    expect(await engine.write({ kind: "put", key: "k", value: bytes(1), condition: "xx" })).toEqual(
      {
        applied: false,
        reason: "xx",
      },
    );
    await engine.write({ kind: "put", key: "k", value: bytes(1), condition: "none" });
    expect(
      (await engine.write({ kind: "put", key: "k", value: bytes(2), condition: "xx" })).applied,
    ).toBe(true);
  });

  it("turns a ttl into an absolute expiry that a later read honours", async () => {
    const clock: { now: () => number; set: (value: number) => void } = clockAt(1_000);
    const engine: Engine = new Engine("n1", 8, clock.now);
    await engine.write({ kind: "put", key: "k", value: bytes(1), condition: "none", ttlMs: 500 });
    expect((await engine.read("k"))?.value).toEqual(bytes(1));
    clock.set(1_500);
    expect(await engine.read("k")).toBeUndefined();
  });
});

describe("Engine delete", () => {
  it("writes a tombstone that hides the key and frees nx", async () => {
    const engine: Engine = newEngine();
    await engine.write({ kind: "put", key: "k", value: bytes(1), condition: "none" });
    const removed: WriteResult = await engine.write({ kind: "delete", key: "k" });
    expect(removed.applied && removed.entry.deleted).toBe(true);
    expect(await engine.read("k")).toBeUndefined();
    expect(await engine.write({ kind: "put", key: "k", value: bytes(2), condition: "xx" })).toEqual(
      { applied: false, reason: "xx" },
    );
    expect(
      (await engine.write({ kind: "put", key: "k", value: bytes(2), condition: "nx" })).applied,
    ).toBe(true);
  });
});

describe("Engine increment", () => {
  it("counts up from an absent key and accumulates", async () => {
    const engine: Engine = newEngine();
    const first: WriteResult = await engine.write({ kind: "incr", key: "c", delta: 5n });
    expect(first.applied && counter(first.entry.value)).toBe(5n);
    const second: WriteResult = await engine.write({ kind: "incr", key: "c", delta: -2n });
    expect(second.applied && counter(second.entry.value)).toBe(3n);
  });

  it("wraps at the signed 64-bit boundary", async () => {
    const engine: Engine = newEngine();
    await engine.write({ kind: "incr", key: "c", delta: 9_223_372_036_854_775_807n });
    const wrapped: WriteResult = await engine.write({ kind: "incr", key: "c", delta: 1n });
    expect(wrapped.applied && counter(wrapped.entry.value)).toBe(-9_223_372_036_854_775_808n);
  });

  it("preserves an existing expiry across increments", async () => {
    const clock: { now: () => number; set: (value: number) => void } = clockAt(1_000);
    const engine: Engine = new Engine("n1", 8, clock.now);
    await engine.write({
      kind: "put",
      key: "c",
      value: new Uint8Array(8),
      condition: "none",
      ttlMs: 500,
    });
    await engine.write({ kind: "incr", key: "c", delta: 7n });
    expect(counter((await engine.read("c"))?.value)).toBe(7n);
    clock.set(1_500);
    expect(await engine.read("c")).toBeUndefined();
  });

  it("rejects a promise when the target is not an eight-byte counter", async () => {
    const engine: Engine = newEngine();
    await engine.write({ kind: "put", key: "c", value: bytes(1, 2, 3), condition: "none" });
    await expect(engine.write({ kind: "incr", key: "c", delta: 1n })).rejects.toThrow(TypeError);
  });
});

describe("Engine compare-and-set", () => {
  it("applies on an exact match", async () => {
    const engine: Engine = newEngine();
    await engine.write({ kind: "put", key: "k", value: bytes(1, 2, 3), condition: "none" });
    const swapped: WriteResult = await engine.write({
      kind: "cas",
      key: "k",
      expected: bytes(1, 2, 3),
      value: bytes(9),
    });
    expect(swapped.applied).toBe(true);
    expect((await engine.read("k"))?.value).toEqual(bytes(9));
  });

  it("rejects on an absent key, a length mismatch, and a byte mismatch", async () => {
    const engine: Engine = newEngine();
    expect(
      await engine.write({ kind: "cas", key: "k", expected: bytes(1), value: bytes(2) }),
    ).toEqual({ applied: false, reason: "cas" });

    await engine.write({ kind: "put", key: "k", value: bytes(1, 2, 3), condition: "none" });
    expect(
      await engine.write({ kind: "cas", key: "k", expected: bytes(1, 2), value: bytes(9) }),
    ).toEqual({ applied: false, reason: "cas" });
    expect(
      await engine.write({ kind: "cas", key: "k", expected: bytes(1, 2, 4), value: bytes(9) }),
    ).toEqual({ applied: false, reason: "cas" });
    expect((await engine.read("k"))?.value).toEqual(bytes(1, 2, 3));
  });
});

describe("Engine read", () => {
  it("returns undefined for a key that was never written", async () => {
    expect(await newEngine().read("missing")).toBeUndefined();
  });

  it("serializes concurrent writes to one key by submission order", async () => {
    const engine: Engine = newEngine();
    const results: WriteResult[] = await Promise.all([
      engine.write({ kind: "put", key: "k", value: bytes(1), condition: "none" }),
      engine.write({ kind: "put", key: "k", value: bytes(2), condition: "none" }),
    ]);
    const sequences: (bigint | false)[] = results.map(
      (result: WriteResult): bigint | false => result.applied && result.entry.sequence,
    );
    expect(sequences).toEqual([1n, 2n]);
    expect((await engine.read("k"))?.value).toEqual(bytes(2));
  });
});
