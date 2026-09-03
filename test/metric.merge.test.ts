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
import {
  DURATION_BUCKETS_MS,
  type IsolateMetrics,
  mergeMetrics,
} from "../src/observability/metric.snapshot";

function isolate(overrides: Partial<IsolateMetrics> = {}): IsolateMetrics {
  return {
    timing: false,
    startedTotal: 0,
    stoppedTotal: 0,
    restartedTotal: 0,
    passivatedTotal: 0,
    deadlettersTotal: 0,
    processedTotal: 0,
    active: 0,
    suspended: 0,
    mailboxTotalDepth: 0,
    mailboxMaxDepth: 0,
    durationCount: 0,
    durationSum: 0,
    durationBuckets: new Array<number>(DURATION_BUCKETS_MS.length).fill(0),
    ...overrides,
  };
}

describe("mergeMetrics", () => {
  it("sums counters and gauges, and takes the deepest mailbox across isolates", () => {
    const main = isolate({ startedTotal: 2, processedTotal: 10, active: 3, mailboxMaxDepth: 3 });
    // The first worker is deeper than main (max moves up), the second is
    // shallower (max stays), exercising both sides of the comparison.
    const deep = isolate({ startedTotal: 1, processedTotal: 5, active: 2, mailboxMaxDepth: 7 });
    const shallow = isolate({ startedTotal: 4, processedTotal: 1, active: 1, mailboxMaxDepth: 2 });

    const snapshot = mergeMetrics("sys", main, [deep, shallow]);
    expect(snapshot.isolates).toBe(3);
    expect(snapshot.actors.startedTotal).toBe(7);
    expect(snapshot.actors.active).toBe(6);
    expect(snapshot.messages.processedTotal).toBe(16);
    expect(snapshot.mailbox.maxDepth).toBe(7);
  });

  it("takes dead letters from the main isolate alone", () => {
    // A worker forwards its dead letters to main, which already counts
    // them, so the worker's own count must not be summed in again.
    const main = isolate({ deadlettersTotal: 5 });
    const worker = isolate({ deadlettersTotal: 99 });

    expect(mergeMetrics("sys", main, [worker]).deadlettersTotal).toBe(5);
  });

  it("skips a worker that dropped out of the collection", () => {
    const main = isolate({ active: 1 });
    const worker = isolate({ active: 2 });

    const snapshot = mergeMetrics("sys", main, [worker, null]);
    expect(snapshot.isolates).toBe(2);
    expect(snapshot.actors.active).toBe(3);
  });

  it("omits the histogram when timing is off", () => {
    expect(mergeMetrics("sys", isolate(), []).messages.processingDurationMs).toBeUndefined();
  });

  it("adds histogram buckets and builds the cumulative distribution when timing is on", () => {
    const firstBucket = (count: number): number[] => {
      const buckets = new Array<number>(DURATION_BUCKETS_MS.length).fill(0);
      buckets[0] = count;
      return buckets;
    };

    const main = isolate({
      timing: true,
      durationCount: 1,
      durationSum: 0.5,
      durationBuckets: firstBucket(1),
    });
    const worker = isolate({
      timing: true,
      durationCount: 2,
      durationSum: 1,
      durationBuckets: firstBucket(2),
    });

    const histogram = mergeMetrics("sys", main, [worker]).messages.processingDurationMs;
    expect(histogram).toBeDefined();
    const data = histogram as NonNullable<typeof histogram>;
    expect(data.count).toBe(3);
    expect(data.sum).toBeCloseTo(1.5, 5);
    // Cumulative: the summed 3 samples in the first bucket carry through.
    expect(data.buckets[0]?.count).toBe(3);
    expect(data.buckets[data.buckets.length - 1]?.count).toBe(3);
  });
});
