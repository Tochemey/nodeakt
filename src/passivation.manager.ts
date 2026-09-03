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

import { TimeBasedStrategy } from "./passivation";
import type { PID } from "./pid";

/**
 * The idle window of a scheduled actor. Only time-based actors are ever
 * scheduled, so the strategy behind a scheduled actor is always one.
 */
function timeoutOf(pid: PID): number {
  return (pid.passivationStrategy() as TimeBasedStrategy).timeout;
}

/**
 * PassivationManager centralizes passivation scheduling for one actor
 * system.
 *
 * Time-based participants sit in a min-heap ordered by absolute deadline
 * (latest activity plus timeout), and a single shared timer is armed for
 * the earliest deadline. When it fires, each due participant is
 * re-checked against its actual latest activity: an actor that was active
 * in the meantime is rescheduled, and only a genuinely idle one is
 * passivated. Message processing therefore never touches a timer; the
 * per-message cost lives entirely in the PID as one timestamp write at
 * the end of each drain, made only while the actor is scheduled.
 *
 * The heap is two parallel arrays (the actor and its deadline) and each
 * actor remembers its own slot, so scheduling an actor costs two array
 * slots and one integer field on the PID: no entry object and no lookup
 * table, which is what keeps a large fleet of idle actors cheap to hold.
 * The idle window is read back from the actor's own strategy when it is
 * due.
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
  /** The scheduled actors, heap-ordered by {@link deadlines}. */
  private readonly heap: PID[] = [];

  /** Each slot's absolute timestamp of the next passivation attempt. */
  private readonly deadlines: number[] = [];

  private timer: NodeJS.Timeout | null = null;
  private timerDeadline: number = Number.POSITIVE_INFINITY;

  /**
   * Hooks an actor into the scheduler under its own strategy. Registering
   * an already registered actor replaces its schedule. Only a time-based
   * strategy is scheduled; any other strategy removes the actor from the
   * scheduler.
   */
  register(pid: PID): void {
    this.remove(pid);

    const strategy = pid.passivationStrategy();
    if (!(strategy instanceof TimeBasedStrategy)) {
      return;
    }

    this.push(pid, Date.now() + strategy.timeout);
    this.arm();
  }

  /** Removes an actor from any passivation bookkeeping. */
  unregister(pid: PID): void {
    this.remove(pid);
    this.arm();
  }

  /** Cancels all pending passivations and releases the timer. */
  stop(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    this.timerDeadline = Number.POSITIVE_INFINITY;
    for (const pid of this.heap) {
      pid.setPassivationSlot(-1);
    }

    this.heap.length = 0;
    this.deadlines.length = 0;
  }

  /** Fires due participants and re-arms the timer for the next deadline. */
  private onTimer(): void {
    this.timer = null;
    this.timerDeadline = Number.POSITIVE_INFINITY;
    const now: number = Date.now();

    while (this.heap.length > 0 && (this.deadlines[0] as number) <= now) {
      const pid: PID = this.pop();
      const timeout: number = timeoutOf(pid);

      if (!pid.isRunning()) {
        // A suspended actor may be restarted or reinstated: keep its
        // schedule paused instead of dropping it.
        if (pid.isSuspended()) {
          this.push(pid, now + timeout);
        }

        continue;
      }

      const due: number = pid.latestActivity() + timeout;
      if (due > now) {
        // The actor was active since the deadline was set: reschedule.
        this.push(pid, due);
        continue;
      }

      if (!pid.isIdle()) {
        // Messages are in flight right now: grant a fresh window.
        this.push(pid, now + timeout);
        continue;
      }

      void pid.passivate();
    }

    this.arm();
  }

  /** Arms the shared timer for the earliest deadline in the heap. */
  private arm(): void {
    if (this.heap.length === 0) {
      if (this.timer !== null) {
        clearTimeout(this.timer);
        this.timer = null;
      }

      this.timerDeadline = Number.POSITIVE_INFINITY;
      return;
    }

    const deadline: number = this.deadlines[0] as number;
    if (this.timer !== null && deadline >= this.timerDeadline) {
      return;
    }

    if (this.timer !== null) {
      clearTimeout(this.timer);
    }

    this.timerDeadline = deadline;
    this.timer = setTimeout(() => this.onTimer(), Math.max(0, deadline - Date.now()));
    this.timer.unref();
  }

  private push(pid: PID, deadline: number): void {
    const slot: number = this.heap.length;
    this.heap.push(pid);
    this.deadlines.push(deadline);
    pid.setPassivationSlot(slot);
    this.up(slot);
  }

  /** Removes and returns the root, the actor with the earliest deadline. */
  private pop(): PID {
    const root: PID = this.heap[0] as PID;
    this.removeAt(0);
    return root;
  }

  private remove(pid: PID): void {
    const slot: number = pid.passivationSlot();
    if (slot < 0) {
      return;
    }

    this.removeAt(slot);
  }

  /** Vacates a slot: the last element moves into it and settles in either direction. */
  private removeAt(slot: number): void {
    const removed: PID = this.heap[slot] as PID;
    removed.setPassivationSlot(-1);

    const last: number = this.heap.length - 1;
    const lastPid: PID = this.heap.pop() as PID;
    const lastDeadline: number = this.deadlines.pop() as number;
    if (slot === last) {
      return;
    }

    this.heap[slot] = lastPid;
    this.deadlines[slot] = lastDeadline;
    lastPid.setPassivationSlot(slot);
    this.down(slot);
    this.up(slot);
  }

  private up(j: number): void {
    const deadlines: number[] = this.deadlines;
    while (j > 0) {
      const i: number = (j - 1) >> 1;
      if ((deadlines[j] as number) >= (deadlines[i] as number)) {
        break;
      }

      this.swap(i, j);
      j = i;
    }
  }

  private down(i: number): void {
    const deadlines: number[] = this.deadlines;
    const n: number = this.heap.length;

    for (;;) {
      const left: number = 2 * i + 1;
      if (left >= n) {
        break;
      }

      let child: number = left;
      const right: number = left + 1;
      if (right < n && (deadlines[right] as number) < (deadlines[left] as number)) {
        child = right;
      }

      if ((deadlines[child] as number) >= (deadlines[i] as number)) {
        break;
      }

      this.swap(i, child);
      i = child;
    }
  }

  private swap(i: number, j: number): void {
    const heap: PID[] = this.heap;
    const deadlines: number[] = this.deadlines;

    const a: PID = heap[i] as PID;
    const b: PID = heap[j] as PID;
    heap[i] = b;
    heap[j] = a;
    a.setPassivationSlot(j);
    b.setPassivationSlot(i);

    const deadline: number = deadlines[i] as number;
    deadlines[i] = deadlines[j] as number;
    deadlines[j] = deadline;
  }
}
