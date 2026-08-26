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
import { MovePriority, MoveScheduler } from "../../src/kv/move.scheduler";
import { flush } from "./sim";

/** A promise whose settlement the test controls. */
interface Gate {
  readonly promise: Promise<void>;
  readonly open: () => void;
}

function gate(): Gate {
  let open!: () => void;
  const promise: Promise<void> = new Promise<void>((resolve: () => void): void => {
    open = resolve;
  });
  return { promise, open };
}

describe("MoveScheduler concurrency", () => {
  it("runs at most the configured number of moves at once", async () => {
    const scheduler: MoveScheduler = new MoveScheduler(2);
    const started: number[] = [];
    const gates: Gate[] = [];
    const run = (id: number): (() => Promise<void>) => {
      return (): Promise<void> => {
        started.push(id);
        const held: Gate = gate();
        gates[id] = held;
        return held.promise;
      };
    };
    const done: Promise<void>[] = [0, 1, 2, 3].map(
      (id: number): Promise<void> => scheduler.submit(MovePriority.rebalance, run(id)),
    );

    expect(started).toEqual([0, 1]);
    (gates[0] as Gate).open();
    await flush();
    expect(started).toEqual([0, 1, 2]);
    (gates[1] as Gate).open();
    await flush();
    expect(started).toEqual([0, 1, 2, 3]);
    (gates[2] as Gate).open();
    (gates[3] as Gate).open();
    await flush();
    await Promise.all(done);
    expect(started).toEqual([0, 1, 2, 3]);
  });

  it("defaults to a concurrency cap when none is given", async () => {
    const scheduler: MoveScheduler = new MoveScheduler();
    expect(
      await scheduler.submit(MovePriority.drain, (): Promise<number> => Promise.resolve(7)),
    ).toBe(7);
  });
});

describe("MoveScheduler priority", () => {
  it("starts a higher-priority move before a lower-priority one", async () => {
    const scheduler: MoveScheduler = new MoveScheduler(1);
    const started: string[] = [];
    const held: Gate = gate();
    const blocking: Promise<void> = scheduler.submit(MovePriority.rebalance, (): Promise<void> => {
      started.push("block");
      return held.promise;
    });
    const low: Promise<void> = scheduler.submit(MovePriority.rebalance, (): Promise<void> => {
      started.push("low");
      return Promise.resolve();
    });
    const high: Promise<void> = scheduler.submit(
      MovePriority.restoreReplication,
      (): Promise<void> => {
        started.push("high");
        return Promise.resolve();
      },
    );

    expect(started).toEqual(["block"]);
    held.open();
    await flush();
    await Promise.all([blocking, low, high]);
    expect(started).toEqual(["block", "high", "low"]);
  });
});

describe("MoveScheduler failure", () => {
  it("rejects the caller and still frees the slot", async () => {
    const scheduler: MoveScheduler = new MoveScheduler(1);
    const started: string[] = [];
    const failing: Promise<void> = scheduler.submit(MovePriority.drain, (): Promise<void> => {
      started.push("fail");
      return Promise.reject(new Error("boom"));
    });
    const next: Promise<number> = scheduler.submit(MovePriority.drain, (): Promise<number> => {
      started.push("next");
      return Promise.resolve(9);
    });

    await expect(failing).rejects.toThrow("boom");
    await flush();
    expect(started).toEqual(["fail", "next"]);
    expect(await next).toBe(9);
  });

  it("rejects a non-positive concurrency cap", () => {
    expect((): MoveScheduler => new MoveScheduler(0)).toThrow(RangeError);
    expect((): MoveScheduler => new MoveScheduler(-1)).toThrow(RangeError);
    expect((): MoveScheduler => new MoveScheduler(1.5)).toThrow(RangeError);
  });
});
