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
 * MetricsOptions turns on the actor system's built-in metrics. It is
 * absent by default, in which case the system maintains no metrics state:
 * there is no subscription and no counter, and the message hot path is
 * untouched.
 */
export interface MetricsOptions {
  /** Turns on the metrics registry and its counters. */
  readonly enabled: true;

  /**
   * Also times every message through `receive` and accumulates a
   * processing-duration histogram. Off by default: timing reads a
   * monotonic clock around every message, which measurably lowers peak
   * throughput. Turn it on for latency, not just liveness.
   */
  readonly processingDuration?: boolean;
}
