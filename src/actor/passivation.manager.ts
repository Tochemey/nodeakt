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

import { type PassivationStrategy, TimeBasedStrategy } from "./passivation";
import type { PID } from "./pid";

/** Scheduling metadata for one time-based participant. */
interface Entry {
  readonly pid: PID;
  readonly timeout: number;
  /** Absolute timestamp of the next passivation attempt. */
  deadline: number;
  /** Position in the heap; -1 when not enqueued. */
  index: number;
}

/**
 * PassivationManager centralizes passivation scheduling for one actor
 * system.
 *
 * Time-based participants are tracked in a min-heap ordered by absolute
 * deadline (latest activity plus timeout), and a single shared timer is
 * armed for the earliest deadline. When it fires, each due participant is
 * re-checked against its actual latest activity: an actor that was active
 * in the meantime is rescheduled, and only a genuinely idle one is
 * passivated. Message processing therefore never touches a timer; the
 * per-message cost lives entirely in the PID as one timestamp write.
 *
 * Message-count strategies involve no scheduling: the PID passivates
 * itself when its processed count crosses the threshold. Long-lived
 * actors are never registered.
 *
 * The shared timer is unreferenced, so a pending passivation never keeps
 * the process alive on its own.
 *
 * @internal
 */
export class PassivationManager {
  private readonly entries = new Map<PID, Entry>();
  private readonly heap: Entry[] = [];
  private timer: NodeJS.Timeout | null = null;
  private timerDeadline = Number.POSITIVE_INFINITY;

  /**
   * Hooks an actor into the scheduler using its strategy. Registering an
   * already registered actor replaces its schedule. Only time-based
   * strategies are scheduled; any other strategy removes the actor from
   * the scheduler.
   */
  register(pid: PID, strategy: PassivationStrategy): void {
    this.drop(pid);

    if (!(strategy instanceof TimeBasedStrategy)) {
      pid.setActivityTracking(false);
      return;
    }

    pid.setActivityTracking(true);

    const entry: Entry = {
      pid,
      timeout: strategy.timeout,
      deadline: Date.now() + strategy.timeout,
      index: -1,
    };

    this.entries.set(pid, entry);
    this.push(entry);
    this.arm();
  }

  /** Removes an actor from any passivation bookkeeping. */
  unregister(pid: PID): void {
    this.drop(pid);
    pid.setActivityTracking(false);
    this.arm();
  }

  /** Cancels all pending passivations and releases the timer. */
  stop(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    this.timerDeadline = Number.POSITIVE_INFINITY;
    this.heap.length = 0;
    this.entries.clear();
  }

  private drop(pid: PID): void {
    const entry = this.entries.get(pid);
    if (entry === undefined) {
      return;
    }

    this.remove(entry);
    this.entries.delete(pid);
  }

  /** Fires due participants and re-arms the timer for the next deadline. */
  private onTimer(): void {
    this.timer = null;
    this.timerDeadline = Number.POSITIVE_INFINITY;
    const now = Date.now();

    for (;;) {
      const root = this.heap[0];
      if (root === undefined || root.deadline > now) {
        break;
      }

      const entry = this.pop();

      if (!entry.pid.isRunning()) {
        // A suspended actor may be restarted or reinstated: keep its
        // schedule paused instead of dropping it.
        if (entry.pid.isSuspended()) {
          entry.deadline = now + entry.timeout;
          this.push(entry);
          continue;
        }

        this.entries.delete(entry.pid);
        continue;
      }

      const due = entry.pid.latestActivity() + entry.timeout;
      if (due > now) {
        // The actor was active since the deadline was set: reschedule.
        entry.deadline = due;
        this.push(entry);
        continue;
      }

      if (!entry.pid.isIdle()) {
        // Messages are in flight right now: grant a fresh window.
        entry.deadline = now + entry.timeout;
        this.push(entry);
        continue;
      }

      this.entries.delete(entry.pid);
      void entry.pid.shutdown();
    }

    this.arm();
  }

  /** Arms the shared timer for the earliest deadline in the heap. */
  private arm(): void {
    const root = this.heap[0];

    if (root === undefined) {
      if (this.timer !== null) {
        clearTimeout(this.timer);
        this.timer = null;
      }

      this.timerDeadline = Number.POSITIVE_INFINITY;
      return;
    }

    if (this.timer !== null && root.deadline >= this.timerDeadline) {
      return;
    }

    if (this.timer !== null) {
      clearTimeout(this.timer);
    }

    this.timerDeadline = root.deadline;
    this.timer = setTimeout(() => this.onTimer(), Math.max(0, root.deadline - Date.now()));
    this.timer.unref();
  }

  private push(entry: Entry): void {
    entry.index = this.heap.length;
    this.heap.push(entry);
    this.up(entry.index);
  }

  private pop(): Entry {
    const root = this.heap[0] as Entry;
    const last = this.heap.pop() as Entry;

    if (this.heap.length > 0 && last !== root) {
      this.heap[0] = last;
      last.index = 0;
      this.down(0);
    }

    root.index = -1;
    return root;
  }

  private remove(entry: Entry): void {
    const i = entry.index;
    if (i < 0) {
      return;
    }

    const last = this.heap.pop() as Entry;

    if (i < this.heap.length && last !== entry) {
      this.heap[i] = last;
      last.index = i;
      this.down(i);
      this.up(i);
    }

    entry.index = -1;
  }

  private up(j: number): void {
    while (j > 0) {
      const i = (j - 1) >> 1;
      if ((this.heap[j] as Entry).deadline >= (this.heap[i] as Entry).deadline) {
        break;
      }

      this.swap(i, j);
      j = i;
    }
  }

  private down(i: number): void {
    const n = this.heap.length;

    for (;;) {
      const left = 2 * i + 1;
      if (left >= n) {
        break;
      }

      let child = left;
      const right = left + 1;
      if (right < n && (this.heap[right] as Entry).deadline < (this.heap[left] as Entry).deadline) {
        child = right;
      }

      if ((this.heap[child] as Entry).deadline >= (this.heap[i] as Entry).deadline) {
        break;
      }

      this.swap(i, child);
      i = child;
    }
  }

  private swap(i: number, j: number): void {
    const a = this.heap[i] as Entry;
    const b = this.heap[j] as Entry;
    this.heap[i] = b;
    this.heap[j] = a;
    a.index = j;
    b.index = i;
  }
}
