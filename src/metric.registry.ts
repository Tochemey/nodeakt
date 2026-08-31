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

import { eventsTopic } from "./deadletter";
import type { EventStream, StreamSubscriber } from "./eventstream";
import {
  ActorPassivated,
  ActorRestarted,
  ActorStarted,
  ActorStopped,
  Deadletter,
} from "./messages";
import {
  DURATION_BUCKETS_MS,
  type IsolateGauges,
  type IsolateMetrics,
} from "./observability/metric.snapshot";

/** The concrete class of a runtime event, used to route it to its counter. */
type EventClass = new (...args: never[]) => object;

/**
 * MetricRegistry holds one isolate's metrics: the lifecycle counters it
 * derives from the event stream, the monotonic count of messages its
 * actors have processed, and, when timing is on, the distribution of how
 * long those messages took. It is framework state, constructed only when
 * metrics are enabled, and it never calls user code.
 *
 * @internal
 */
export class MetricRegistry {
  /**
   * The monotonic count of messages processed across this isolate's
   * actors. A PID increments it in place on the message hot path, and it
   * is read when a snapshot is collected.
   */
  processed = 0;

  /** Whether messages are timed into the processing-duration histogram;
   * fixed at construction so the hot-path guard is a stable branch. */
  readonly timing: boolean;

  private _startedTotal = 0;
  private _stoppedTotal = 0;
  private _restartedTotal = 0;
  private _passivatedTotal = 0;
  private _deadlettersTotal = 0;

  /** Non-cumulative bucket counts for the processing-duration histogram;
   * written in place on the timing hot path. */
  private readonly _durationBuckets: Uint32Array = new Uint32Array(DURATION_BUCKETS_MS.length);
  private _durationSum = 0;
  private _durationCount = 0;

  /** Routes an event to the counter it bumps, by the event's class. */
  private readonly _dispatch: Map<EventClass, () => void>;

  constructor(events: EventStream, timing: boolean) {
    this.timing = timing;
    this._dispatch = new Map<EventClass, () => void>([
      [ActorStarted, (): void => void this._startedTotal++],
      [ActorStopped, (): void => void this._stoppedTotal++],
      [ActorRestarted, (): void => void this._restartedTotal++],
      [ActorPassivated, (): void => void this._passivatedTotal++],
      [Deadletter, (): void => void this._deadlettersTotal++],
    ]);

    const onEvent: StreamSubscriber = (event: unknown): void => {
      const bump = this._dispatch.get((event as { constructor: EventClass }).constructor);
      if (bump !== undefined) {
        bump();
      }
    };

    events.subscribe(onEvent, eventsTopic);
  }

  /**
   * Records one message's processing duration, in milliseconds, into the
   * histogram. Called on the message hot path only when timing is on.
   */
  recordDuration(ms: number): void {
    this._durationSum += ms;
    this._durationCount++;

    const buckets = this._durationBuckets;
    for (let i = 0; i < DURATION_BUCKETS_MS.length; i++) {
      if (ms <= (DURATION_BUCKETS_MS[i] as number)) {
        buckets[i] = (buckets[i] as number) + 1;
        return;
      }
    }
  }

  /**
   * Returns this isolate's raw contribution to a snapshot: the counters
   * it keeps and the live-actor gauges the caller has read from the tree.
   * The main isolate merges these across every isolate.
   */
  isolateMetrics(gauges: IsolateGauges): IsolateMetrics {
    return {
      timing: this.timing,
      startedTotal: this._startedTotal,
      stoppedTotal: this._stoppedTotal,
      restartedTotal: this._restartedTotal,
      passivatedTotal: this._passivatedTotal,
      deadlettersTotal: this._deadlettersTotal,
      processedTotal: this.processed,
      active: gauges.active,
      suspended: gauges.suspended,
      mailboxTotalDepth: gauges.mailboxTotalDepth,
      mailboxMaxDepth: gauges.mailboxMaxDepth,
      durationCount: this._durationCount,
      durationSum: this._durationSum,
      durationBuckets: Array.from(this._durationBuckets),
    };
  }
}
