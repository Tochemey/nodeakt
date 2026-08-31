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
 * Inclusive upper bounds, in milliseconds, of the processing-duration
 * histogram buckets. The last is unbounded, so every sample lands in a
 * bucket. Internal, not a tuning knob; shared by the registry that fills
 * the buckets and the merge that reports them.
 *
 * @internal
 */
export const DURATION_BUCKETS_MS: readonly number[] = [
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  25,
  50,
  100,
  250,
  500,
  1000,
  Number.POSITIVE_INFINITY,
];

/** Fleet-wide actor counts: live gauges and monotonic lifecycle counters. */
export interface ActorFleetMetrics {
  /** How many actors are alive right now, suspended ones included. */
  readonly active: number;

  /** How many live actors are currently suspended. */
  readonly suspended: number;

  /** How many actors have started over the system's life. */
  readonly startedTotal: number;

  /** How many actors have fully stopped over the system's life. */
  readonly stoppedTotal: number;

  /** How many actor restarts have happened over the system's life. */
  readonly restartedTotal: number;

  /** How many actors have been idle-passivated over the system's life. */
  readonly passivatedTotal: number;
}

/** One cumulative bucket of the processing-duration histogram. */
export interface HistogramBucket {
  /** The inclusive upper bound in milliseconds; `Infinity` for the last. */
  readonly leMs: number;

  /** The cumulative count of samples at or below {@link leMs}. */
  readonly count: number;
}

/** A processing-duration distribution: total samples, total time, and buckets. */
export interface HistogramData {
  /** The number of timed messages. */
  readonly count: number;

  /** The total processing time in milliseconds across those messages. */
  readonly sum: number;

  /** The cumulative buckets, ordered by ascending upper bound. */
  readonly buckets: readonly HistogramBucket[];
}

/** Message throughput across the fleet, and processing latency when timed. */
export interface MessageMetrics {
  /** The monotonic count of messages processed across the fleet. */
  readonly processedTotal: number;

  /** The processing-duration distribution; present only when timing is on. */
  readonly processingDurationMs?: HistogramData;
}

/** Mailbox depth across the fleet. */
export interface MailboxMetrics {
  /** The summed mailbox depth over all live actors. */
  readonly totalDepth: number;

  /** The depth of the single deepest mailbox. */
  readonly maxDepth: number;
}

/**
 * MetricsSnapshot is a point-in-time, machine-wide view of the runtime's
 * own state: the calling isolate merged with every worker isolate. It is
 * plain readonly data with no methods and no vendor types, so an adapter
 * can map it onto any backend from outside the core.
 */
export interface MetricsSnapshot {
  /** The actor system name. */
  readonly system: string;

  /** When the snapshot was taken, in milliseconds since the epoch. */
  readonly collectedAt: number;

  /** How many isolates contributed to the snapshot. */
  readonly isolates: number;

  /** Fleet-wide actor counts. */
  readonly actors: ActorFleetMetrics;

  /** Message throughput and latency. */
  readonly messages: MessageMetrics;

  /** Mailbox depth across the fleet. */
  readonly mailbox: MailboxMetrics;

  /** The count of dead letters over the system's life. */
  readonly deadlettersTotal: number;
}

/**
 * ActorMetrics is one actor's numbers, read on demand. It is
 * introspection for a single actor, a health check or a debug command,
 * never a fleet time series.
 */
export interface ActorMetrics {
  /** The actor's canonical path string. */
  readonly path: string;

  /** Messages this actor has processed since its last (re)start. */
  readonly processedCount: number;

  /** How many times this actor has restarted. */
  readonly restartCount: number;

  /** The actor's current mailbox depth. */
  readonly mailboxSize: number;

  /** The number of messages currently stashed. */
  readonly stashSize: number;

  /** The actor's current number of children. */
  readonly childrenCount: number;

  /**
   * The clock read at the end of the actor's last mailbox drain, in ms
   * since the epoch, for an actor whose passivation strategy tracks
   * idleness. For an actor that never passivates the drain-end clock is
   * not read, so this stays the time the actor started.
   */
  readonly lastActivity: number;

  /** Whether the actor is currently suspended. */
  readonly suspended: boolean;
}

/**
 * The live-actor gauges one isolate reads for a snapshot: how many actors
 * are alive and suspended, and their mailbox depth. The counters come
 * from the registry; these come from walking the live actors.
 *
 * @internal
 */
export interface IsolateGauges {
  readonly active: number;
  readonly suspended: number;
  readonly mailboxTotalDepth: number;
  readonly mailboxMaxDepth: number;
}

/**
 * IsolateMetrics is one isolate's raw contribution to a snapshot: its
 * counters, its live-actor gauges, and its processing-duration buckets
 * in non-cumulative form so they add across isolates. It crosses the
 * control plane from a worker, so it is plain structured-cloneable data.
 *
 * @internal
 */
export interface IsolateMetrics {
  readonly timing: boolean;
  readonly startedTotal: number;
  readonly stoppedTotal: number;
  readonly restartedTotal: number;
  readonly passivatedTotal: number;
  readonly deadlettersTotal: number;
  readonly processedTotal: number;
  readonly active: number;
  readonly suspended: number;
  readonly mailboxTotalDepth: number;
  readonly mailboxMaxDepth: number;
  readonly durationCount: number;
  readonly durationSum: number;

  /** Non-cumulative per-bucket counts, aligned to {@link DURATION_BUCKETS_MS}. */
  readonly durationBuckets: readonly number[];
}

/**
 * Merges one machine's isolates into a snapshot: counters and gauges sum,
 * the deepest mailbox takes the max, and the histogram buckets add. A
 * `null` worker is one that dropped out of the collection (a dead
 * isolate) and contributes nothing. Dead letters come from the main
 * isolate alone: a worker forwards its dead-letter events to the main
 * isolate, which already counts them, so summing would double-count.
 *
 * @internal
 */
export function mergeMetrics(
  system: string,
  main: IsolateMetrics,
  workers: readonly (IsolateMetrics | null)[],
): MetricsSnapshot {
  let startedTotal = main.startedTotal;
  let stoppedTotal = main.stoppedTotal;
  let restartedTotal = main.restartedTotal;
  let passivatedTotal = main.passivatedTotal;
  let processedTotal = main.processedTotal;
  let active = main.active;
  let suspended = main.suspended;
  let mailboxTotalDepth = main.mailboxTotalDepth;
  let mailboxMaxDepth = main.mailboxMaxDepth;
  let durationCount = main.durationCount;
  let durationSum = main.durationSum;
  let isolates = 1;

  const buckets: number[] = [...main.durationBuckets];

  for (const worker of workers) {
    if (worker === null) {
      continue;
    }

    startedTotal += worker.startedTotal;
    stoppedTotal += worker.stoppedTotal;
    restartedTotal += worker.restartedTotal;
    passivatedTotal += worker.passivatedTotal;
    processedTotal += worker.processedTotal;
    active += worker.active;
    suspended += worker.suspended;
    mailboxTotalDepth += worker.mailboxTotalDepth;
    if (worker.mailboxMaxDepth > mailboxMaxDepth) {
      mailboxMaxDepth = worker.mailboxMaxDepth;
    }

    durationCount += worker.durationCount;
    durationSum += worker.durationSum;
    for (let i = 0; i < buckets.length; i++) {
      buckets[i] = (buckets[i] as number) + (worker.durationBuckets[i] as number);
    }

    isolates++;
  }

  return {
    system,
    collectedAt: Date.now(),
    isolates,
    actors: { active, suspended, startedTotal, stoppedTotal, restartedTotal, passivatedTotal },
    messages: main.timing
      ? {
          processedTotal,
          processingDurationMs: cumulativeHistogram(buckets, durationCount, durationSum),
        }
      : { processedTotal },
    mailbox: { totalDepth: mailboxTotalDepth, maxDepth: mailboxMaxDepth },
    deadlettersTotal: main.deadlettersTotal,
  };
}

/** Turns non-cumulative bucket counts into the cumulative distribution. */
function cumulativeHistogram(
  rawBuckets: readonly number[],
  count: number,
  sum: number,
): HistogramData {
  const buckets: HistogramBucket[] = [];
  let cumulative = 0;

  for (let i = 0; i < DURATION_BUCKETS_MS.length; i++) {
    cumulative += rawBuckets[i] as number;
    buckets.push({ leMs: DURATION_BUCKETS_MS[i] as number, count: cumulative });
  }

  return { count, sum, buckets };
}

/**
 * Builds a zeroed but valid snapshot under the given system name, the
 * answer a system that never enabled metrics gives to a collection so an
 * adapter can be wired unconditionally.
 *
 * @internal
 */
export function emptyMetricsSnapshot(system: string): MetricsSnapshot {
  return {
    system,
    collectedAt: Date.now(),
    isolates: 1,
    actors: {
      active: 0,
      suspended: 0,
      startedTotal: 0,
      stoppedTotal: 0,
      restartedTotal: 0,
      passivatedTotal: 0,
    },
    messages: {
      processedTotal: 0,
    },
    mailbox: {
      totalDepth: 0,
      maxDepth: 0,
    },
    deadlettersTotal: 0,
  };
}
