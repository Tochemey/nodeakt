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

import { afterAll, describe, it } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import type { PID } from "../src/pid";
import { printReport, runScenario, type Scenario, type ScenarioReport } from "./harness";

/**
 * Isolates the cost the throughput counter adds to the message hot path.
 *
 * Both scenarios run the same single-pair `tell` workload; the only
 * difference is whether metrics are enabled, so the gap between them is
 * the per-message counter increment and the guard that precedes it. The
 * "off" number must track the plain single-pair `tell` baseline: enabling
 * metrics is what an operator pays, and not enabling them must cost
 * nothing.
 */

/** Messages per benchmark operation. */
const BATCH = 10_000;

/** The message sent in every benchmark; reused so the hot path measures
 * dispatch, not message construction. */
class Ping {}

const msg = new Ping();

/**
 * A receiver that resolves a promise once it has processed the number of
 * messages a batch announced through {@link expect}.
 */
class CountingActor implements Actor {
  private remaining = 0;
  private resolveDone: (() => void) | null = null;

  expect(count: number): Promise<void> {
    this.remaining = count;
    return new Promise((resolve) => {
      this.resolveDone = resolve;
    });
  }

  preStart(): void {}

  receive(): void {
    this.remaining--;

    if (this.remaining === 0) {
      const resolve = this.resolveDone as () => void;
      this.resolveDone = null;
      resolve();
    }
  }

  postStop(): void {}
}

/** A sender whose behavior is never exercised; it only lends its PID. */
class SilentActor implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

const offSystem = new ActorSystem("bench-metrics-off");
await offSystem.start();

const onSystem = new ActorSystem("bench-metrics-on", { metrics: { enabled: true } });
await onSystem.start();

const timingSystem = new ActorSystem("bench-metrics-timing", {
  metrics: { enabled: true, processingDuration: true },
});
await timingSystem.start();

afterAll(async () => {
  await offSystem.stop();
  await onSystem.stop();
  await timingSystem.stop();
});

const offSender: PID = await offSystem.spawn("sender", new SilentActor());
const offCounting = new CountingActor();
const offReceiver: PID = await offSystem.spawn("receiver", offCounting);

const onSender: PID = await onSystem.spawn("sender", new SilentActor());
const onCounting = new CountingActor();
const onReceiver: PID = await onSystem.spawn("receiver", onCounting);

const timingSender: PID = await timingSystem.spawn("sender", new SilentActor());
const timingCounting = new CountingActor();
const timingReceiver: PID = await timingSystem.spawn("receiver", timingCounting);

const scenarios: Scenario[] = [
  {
    name: "metrics off",
    batch: BATCH,
    op: async () => {
      const done = offCounting.expect(BATCH);

      for (let i = 0; i < BATCH; i++) {
        offSender.tell(offReceiver, msg);
      }

      await done;
    },
  },
  {
    name: "metrics on (no timing)",
    batch: BATCH,
    op: async () => {
      const done = onCounting.expect(BATCH);

      for (let i = 0; i < BATCH; i++) {
        onSender.tell(onReceiver, msg);
      }

      await done;
    },
  },
  {
    name: "metrics on (timing)",
    batch: BATCH,
    op: async () => {
      const done = timingCounting.expect(BATCH);

      for (let i = 0; i < BATCH; i++) {
        timingSender.tell(timingReceiver, msg);
      }

      await done;
    },
  },
];

describe("metrics", () => {
  it("measures the throughput-counter hot-path cost", { timeout: 300_000 }, async () => {
    const reports: ScenarioReport[] = [];

    for (const scenario of scenarios) {
      reports.push(await runScenario(scenario));
    }

    printReport(reports, "metrics  ·  throughput-counter hot-path cost");
  });
});
