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

/**
 * Showcase: the runtime reports on itself, and you own the reporter.
 *
 * Turn metrics on, drive one busy worker with a burst of messages, and read
 * `collectMetrics()` on a timer. The reporter here is a plain function over
 * the returned snapshot: no vendor SDK, no dependency the runtime pulled in.
 * `processingDuration` adds the latency histogram, and the reporter turns it
 * into an average and a couple of percentiles.
 *
 * Run: make metrics
 */

import type {
  Actor,
  ActorMetrics,
  HistogramData,
  MetricsSnapshot,
  PID,
  ReceiveContext,
} from "../../src/index";
import { ActorSystem, LongLivedStrategy, PostStart, TextLogger } from "../../src/index";

/** A unit of work: spin the CPU for `iterations` before counting the message,
 * so the worker's processing time is real and varies message to message. */
class Work {
  constructor(readonly iterations: number) {}
}

class Worker implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message = ctx.message;

    if (message instanceof PostStart) {
      return;
    }

    if (message instanceof Work) {
      this.spin(message.iterations);
    }
  }

  postStop(): void {}

  /** Busy-work standing in for whatever an actor actually does; its cost is
   * what the processing-duration histogram measures. */
  private spin(iterations: number): void {
    let sink = 0;
    for (let i = 0; i < iterations; i++) {
      sink += Math.sqrt(i);
    }

    if (sink < 0) {
      throw new Error("unreachable: keeps the loop from being optimized away");
    }
  }
}

const TOTAL: number = 40_000;
const REPORT_MS: number = 200;

/** Resolves after `ms`, so the reporter can tick while the backlog drains. */
function delay(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

/** The iterations one message spins: mostly light, with a scattering of
 * medium and heavy messages so the latency histogram has a spread to show. */
function iterationsFor(index: number): number {
  if (index % 100 === 0) {
    return 1_500_000;
  }

  if (index % 20 === 0) {
    return 300_000;
  }

  return 50_000;
}

/** Reads a percentile off the cumulative histogram: the upper bound of the
 * first bucket whose running count reaches the target rank. This is exactly
 * how a backend would consume the buckets. */
function quantileMs(histogram: HistogramData, q: number): string {
  const target: number = q * histogram.count;
  for (const bucket of histogram.buckets) {
    if (bucket.count >= target) {
      return bucket.leMs === Number.POSITIVE_INFINITY ? ">1000ms" : `${bucket.leMs}ms`;
    }
  }

  return ">1000ms";
}

/** The periodic reporter: everything below is code you own, mapping the plain
 * snapshot onto one log line. A real adapter would record these onto its
 * backend's counters and gauges instead of printing them. */
function report(logger: TextLogger, snapshot: MetricsSnapshot): void {
  const parts: string[] = [
    `active=${snapshot.actors.active}`,
    `processed=${snapshot.messages.processedTotal}`,
    `mailbox=${snapshot.mailbox.totalDepth}`,
    `maxDepth=${snapshot.mailbox.maxDepth}`,
    `deadletters=${snapshot.deadlettersTotal}`,
  ];

  const histogram: HistogramData | undefined = snapshot.messages.processingDurationMs;
  if (histogram !== undefined && histogram.count > 0) {
    const avg: number = histogram.sum / histogram.count;
    parts.push(`avg=${avg.toFixed(3)}ms`);
    parts.push(`p50=${quantileMs(histogram, 0.5)}`);
    parts.push(`p95=${quantileMs(histogram, 0.95)}`);
  }

  logger.info(parts.join("  "));
}

const logger: TextLogger = new TextLogger({ level: "info" });

const system: ActorSystem = new ActorSystem("metrics", {
  logger: logger,
  // `enabled` keeps the counters and gauges; `processingDuration` adds the
  // latency histogram, at the cost of timing every message. Leave the second
  // off in production unless you need latency, not just liveness.
  metrics: { enabled: true, processingDuration: true },
});
await system.start();

// Long-lived so the worker cannot passivate on an idle window mid-run: the
// default is time-based passivation, and we want this actor for the whole demo.
const worker: PID = await system.spawn("worker", new Worker(), {
  passivationStrategy: new LongLivedStrategy(),
});
const outside: PID = system.noSender();

// Pull a snapshot on a timer and hand it to the reporter. This is the whole
// integration: the runtime pulls in nothing, you scrape on your own schedule.
const reporter: NodeJS.Timeout = setInterval(() => {
  void system.collectMetrics().then((snapshot: MetricsSnapshot) => report(logger, snapshot));
}, REPORT_MS);

logger.info(`enqueuing ${TOTAL} messages, then watching the backlog drain`);

// Queue the whole burst up front. `tell` only enqueues, so this returns long
// before the worker has caught up: the mailbox jumps to the full backlog.
for (let i = 0; i < TOTAL; i++) {
  outside.tell(worker, new Work(iterationsFor(i)));
}

// Let the reporter tick while the worker drains, until the mailbox is empty.
while (worker.metrics().mailboxSize > 0) {
  await delay(REPORT_MS);
}

clearInterval(reporter);

const final: MetricsSnapshot = await system.collectMetrics();
logger.info("final fleet snapshot:");
report(logger, final);

// One actor's own numbers, read on demand: no metrics option required, never a
// fleet time series. Handy for a health check or a debug command.
const own: ActorMetrics = worker.metrics();
logger.info(
  `worker: processed=${own.processedCount} mailbox=${own.mailboxSize} restarts=${own.restartCount}`,
);

await system.stop();
