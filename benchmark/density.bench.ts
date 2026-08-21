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

import { afterAll, describe, expect, it } from "vitest";
import type { Actor } from "../src/actor/actor";
import { ActorSystem } from "../src/actor/actor.system";

/**
 * Resident density of idle actors: how much heap one spawned, started,
 * empty actor occupies after PostStart has drained and GC has run.
 *
 * This is not a throughput number. Millions of light actors on one
 * machine is a memory-shape constraint; cores do not change it.
 */

const COUNT = 100_000;

class Empty implements Actor {
  preStart(): void {}
  receive(): void {}
  postStop(): void {}
}

const gc = (globalThis as { gc?: () => void }).gc;

describe("idle actor density", () => {
  let system: ActorSystem | undefined;

  afterAll(async () => {
    await system?.stop();
  });

  it("reports heap bytes per idle empty actor", async () => {
    system = new ActorSystem("density");
    await system.start();

    gc?.();
    const before = process.memoryUsage();

    const batch = 1_000;
    for (let i = 0; i < COUNT; i += batch) {
      const n = Math.min(batch, COUNT - i);
      const spawned = new Array<Promise<unknown>>(n);
      for (let j = 0; j < n; j++) {
        spawned[j] = system.spawn(`a${i + j}`, new Empty());
      }
      await Promise.all(spawned);
    }

    gc?.();
    const after = process.memoryUsage();
    const heap = after.heapUsed - before.heapUsed;
    const rss = after.rss - before.rss;
    const heapPer = heap / COUNT;
    const rssPer = rss / COUNT;

    const report = {
      count: COUNT,
      heapBytesPerActor: Math.round(heapPer),
      rssBytesPerActor: Math.round(rssPer),
      heapMB: Math.round(heap / 1024 / 1024),
      rssMB: Math.round(rss / 1024 / 1024),
      millionActorsHeapGB: Number(((heapPer * 1_000_000) / 1024 / 1024 / 1024).toFixed(2)),
      millionActorsRssGB: Number(((rssPer * 1_000_000) / 1024 / 1024 / 1024).toFixed(2)),
      gcExposed: typeof gc === "function",
    };

    console.log(JSON.stringify(report, null, 2));

    expect(heapPer, "idle actor heap target is ≤ 512 bytes").toBeLessThanOrEqual(512);
  });
});
