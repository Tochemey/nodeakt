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
import { CoordinatorChanged, RelocationCompleted } from "../src/cluster.events";
import { eventsTopic } from "../src/deadletter";
import { EventStream } from "../src/eventstream";
import {
  ActorPassivated,
  ActorRestarted,
  ActorStarted,
  ActorStopped,
  ActorSuspended,
  Deadletter,
} from "../src/messages";
import { MetricRegistry } from "../src/metric.registry";
import type { IsolateGauges } from "../src/observability/metric.snapshot";

const noGauges: IsolateGauges = {
  active: 0,
  suspended: 0,
  mailboxTotalDepth: 0,
  mailboxMaxDepth: 0,
};

describe("MetricRegistry", () => {
  it("derives lifecycle counters from the event stream", () => {
    const events: EventStream = new EventStream();
    const registry: MetricRegistry = new MetricRegistry(events, false);

    events.publish(eventsTopic, new ActorStarted("/a", 1));
    events.publish(eventsTopic, new ActorStarted("/b", 2));
    events.publish(eventsTopic, new ActorStopped("/a", 3));
    events.publish(eventsTopic, new ActorRestarted("/b", 4));
    events.publish(eventsTopic, new ActorPassivated("/b", 5));
    events.publish(eventsTopic, new Deadletter(undefined, "/x", "m", 6, "boom"));

    const metrics = registry.isolateMetrics(noGauges);
    expect(metrics.startedTotal).toBe(2);
    expect(metrics.stoppedTotal).toBe(1);
    expect(metrics.restartedTotal).toBe(1);
    expect(metrics.passivatedTotal).toBe(1);
    expect(metrics.deadlettersTotal).toBe(1);
    expect(metrics.timing).toBe(false);
  });

  it("ignores events it does not track", () => {
    const events: EventStream = new EventStream();
    const registry: MetricRegistry = new MetricRegistry(events, false);

    events.publish(eventsTopic, new ActorSuspended("/a", "why", 1));
    events.publish(eventsTopic, { not: "an event" });

    const metrics = registry.isolateMetrics(noGauges);
    expect(metrics.startedTotal).toBe(0);
    expect(metrics.deadlettersTotal).toBe(0);
  });

  it("counts cluster transitions from the cluster events on the stream", () => {
    const events: EventStream = new EventStream();
    const registry: MetricRegistry = new MetricRegistry(events, false);

    expect(registry.clusterCounters()).toEqual({ coordinatorChanges: 0, relocationsTotal: 0 });

    events.publish(eventsTopic, new CoordinatorChanged("10.0.0.1:7946", 1));
    events.publish(eventsTopic, new CoordinatorChanged("10.0.0.2:7946", 2));
    events.publish(eventsTopic, new RelocationCompleted("10.0.0.1:7946", ["a", "b", "c"], 3));
    events.publish(eventsTopic, new RelocationCompleted("10.0.0.3:7946", [], 4));

    expect(registry.clusterCounters()).toEqual({ coordinatorChanges: 2, relocationsTotal: 3 });
  });

  it("reports the processed counter it is handed on the hot path", () => {
    const events: EventStream = new EventStream();
    const registry: MetricRegistry = new MetricRegistry(events, false);

    registry.processed += 3;

    expect(registry.isolateMetrics(noGauges).processedTotal).toBe(3);
  });

  it("carries the live-actor gauges the caller reads from the tree", () => {
    const events: EventStream = new EventStream();
    const registry: MetricRegistry = new MetricRegistry(events, false);

    const metrics = registry.isolateMetrics({
      active: 4,
      suspended: 1,
      mailboxTotalDepth: 9,
      mailboxMaxDepth: 5,
    });

    expect(metrics.active).toBe(4);
    expect(metrics.suspended).toBe(1);
    expect(metrics.mailboxTotalDepth).toBe(9);
    expect(metrics.mailboxMaxDepth).toBe(5);
  });

  it("records non-cumulative duration buckets when timing is on", () => {
    const events: EventStream = new EventStream();
    const registry: MetricRegistry = new MetricRegistry(events, true);

    // 0.01 ms lands in the first bucket (<= 0.05), 3 ms in the <= 5 bucket
    // (index 6), and 10000 ms past every finite bound in the last (index 14).
    registry.recordDuration(0.01);
    registry.recordDuration(3);
    registry.recordDuration(10_000);

    const metrics = registry.isolateMetrics(noGauges);
    expect(metrics.timing).toBe(true);
    expect(metrics.durationCount).toBe(3);
    expect(metrics.durationSum).toBeCloseTo(10_003.01, 5);
    expect(metrics.durationBuckets[0]).toBe(1);
    expect(metrics.durationBuckets[6]).toBe(1);
    expect(metrics.durationBuckets[14]).toBe(1);
    expect(metrics.durationBuckets[1]).toBe(0);
  });
});
