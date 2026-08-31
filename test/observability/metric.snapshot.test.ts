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
  emptyMetricsSnapshot,
  type MetricsSnapshot,
} from "../../src/observability/metric.snapshot";

describe("emptyMetricsSnapshot", () => {
  it("returns a zeroed but valid snapshot under the given system name", () => {
    const before: number = Date.now();
    const snapshot: MetricsSnapshot = emptyMetricsSnapshot("orders");

    expect(snapshot.system).toBe("orders");
    expect(snapshot.isolates).toBe(1);
    expect(snapshot.collectedAt).toBeGreaterThanOrEqual(before);
    expect(snapshot.actors).toEqual({
      active: 0,
      suspended: 0,
      startedTotal: 0,
      stoppedTotal: 0,
      restartedTotal: 0,
      passivatedTotal: 0,
    });
    expect(snapshot.messages.processedTotal).toBe(0);
    expect(snapshot.mailbox).toEqual({ totalDepth: 0, maxDepth: 0 });
    expect(snapshot.deadlettersTotal).toBe(0);
  });
});
