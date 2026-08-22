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

import {
  ErrDead,
  ErrInvalidInterval,
  ErrScheduleAlreadyExists,
  ErrScheduleNotFound,
} from "../errors/errors";
import type { PID } from "./pid";

/** Scheduling metadata for one registered schedule. */
interface Entry {
  /** The caller-supplied reference; null for an anonymous schedule. */
  readonly reference: string | null;
  /** Whether the schedule repeats after each delivery. */
  readonly repeats: boolean;
  readonly message: unknown;
  readonly receiver: PID;
  readonly sender: PID;
  /** The one-shot delay or the repeating period, in milliseconds. */
  readonly interval: number;
  /** The actor whose stop cancels this schedule; null for a schedule
   * owned by the system alone. */
  readonly owner: PID | null;
  /** Absolute timestamp of the next delivery. */
  deadline: number;
  /** Position in the heap; -1 when not enqueued. */
  index: number;
  paused: boolean;
  /** Delay left on a paused one-shot, frozen when it was paused. */
  remaining: number;
}

/**
 * Scheduler centralizes delayed and repeating message delivery for one
 * actor system.
 *
 * Schedules are tracked in a min-heap ordered by absolute deadline, and
 * a single shared timer is armed for the earliest one. When it fires,
 * each due schedule delivers its message through the normal send path,
 * so an undeliverable tick becomes a dead letter like any other send
 * and a delivery counts as activity for passivation. A repeating
 * schedule then re-enters the heap at its next deadline; a one-shot is
 * discarded. Registration and cancellation never touch message
 * processing: the cost of a schedule lives entirely in this class.
 *
 * The shared timer is unreferenced, so pending schedules never keep the
 * process alive on their own.
 *
 * @internal
 */
export class Scheduler {
  private readonly references = new Map<string, Entry>();
  private readonly owned = new Map<PID, Set<Entry>>();
  private readonly heap: Entry[] = [];
  private timer: NodeJS.Timeout | null = null;
  private timerDeadline: number = Number.POSITIVE_INFINITY;

  /**
   * Registers a repeating schedule: `message` is delivered to `receiver`
   * every `interval` milliseconds, the first time one interval from now.
   *
   * @throws The {@link ErrInvalidInterval} sentinel when `interval` is
   * not a positive number.
   * @throws The {@link ErrScheduleAlreadyExists} sentinel when
   * `reference` is already held by another schedule.
   * @throws The {@link ErrDead} sentinel when the receiver has already
   * stopped.
   */
  schedule(
    message: unknown,
    receiver: PID,
    interval: number,
    sender: PID,
    owner: PID | null,
    reference?: string,
  ): void {
    this.register(message, receiver, interval, sender, owner, reference, true);
  }

  /**
   * Registers a one-shot schedule: `message` is delivered to `receiver`
   * once, `delay` milliseconds from now.
   *
   * @throws The {@link ErrInvalidInterval} sentinel when `delay` is not
   * a positive number.
   * @throws The {@link ErrScheduleAlreadyExists} sentinel when
   * `reference` is already held by another schedule.
   * @throws The {@link ErrDead} sentinel when the receiver has already
   * stopped.
   */
  scheduleOnce(
    message: unknown,
    receiver: PID,
    delay: number,
    sender: PID,
    owner: PID | null,
    reference?: string,
  ): void {
    this.register(message, receiver, delay, sender, owner, reference, false);
  }

  /**
   * Cancels the schedule held under `reference`: nothing fires after
   * this call returns.
   *
   * @throws The {@link ErrScheduleNotFound} sentinel when no schedule
   * holds the reference.
   */
  cancel(reference: string): void {
    const entry: Entry = this.lookup(reference);
    this.discard(entry);
    this.arm();
  }

  /**
   * Pauses the schedule held under `reference`. Pausing a paused
   * schedule is a no-op. A paused one-shot keeps the delay it had left;
   * a paused repeating schedule fires one full interval after it is
   * resumed.
   *
   * @throws The {@link ErrScheduleNotFound} sentinel when no schedule
   * holds the reference.
   */
  pause(reference: string): void {
    const entry: Entry = this.lookup(reference);
    if (entry.paused) {
      return;
    }

    entry.paused = true;
    if (!entry.repeats) {
      entry.remaining = Math.max(0, entry.deadline - Date.now());
    }

    this.remove(entry);
    this.arm();
  }

  /**
   * Resumes the schedule held under `reference`. Resuming a schedule
   * that is not paused is a no-op.
   *
   * @throws The {@link ErrScheduleNotFound} sentinel when no schedule
   * holds the reference.
   */
  resume(reference: string): void {
    const entry: Entry = this.lookup(reference);
    if (!entry.paused) {
      return;
    }

    entry.paused = false;
    entry.deadline = Date.now() + (entry.repeats ? entry.interval : entry.remaining);
    this.push(entry);
    this.arm();
  }

  /** Cancels every schedule and releases the timer. */
  stop(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    this.timerDeadline = Number.POSITIVE_INFINITY;
    this.heap.length = 0;
    this.references.clear();
    this.owned.clear();
  }

  private lookup(reference: string): Entry {
    const entry: Entry | undefined = this.references.get(reference);
    if (entry === undefined) {
      throw ErrScheduleNotFound;
    }

    return entry;
  }

  private register(
    message: unknown,
    receiver: PID,
    interval: number,
    sender: PID,
    owner: PID | null,
    reference: string | undefined,
    repeats: boolean,
  ): void {
    if (!Number.isFinite(interval) || interval <= 0) {
      throw ErrInvalidInterval;
    }

    if (reference !== undefined && this.references.has(reference)) {
      throw ErrScheduleAlreadyExists;
    }

    // A routed receiver's liveness is not knowable on this isolate; an
    // undeliverable tick is dead-lettered by the isolate that owns it.
    if (!receiver.isRouted() && !receiver.isRunning()) {
      throw ErrDead;
    }

    const entry: Entry = {
      reference: reference ?? null,
      repeats,
      message,
      receiver,
      sender,
      interval,
      owner,
      deadline: Date.now() + interval,
      index: -1,
      paused: false,
      remaining: 0,
    };

    if (entry.reference !== null) {
      this.references.set(entry.reference, entry);
    }

    if (owner !== null) {
      this.own(owner, entry);
    }

    this.push(entry);
    this.arm();
  }

  /** Fires due schedules and re-arms the timer for the next deadline. */
  private onTimer(): void {
    this.timer = null;
    this.timerDeadline = Number.POSITIVE_INFINITY;
    const now: number = Date.now();

    for (;;) {
      const root: Entry | undefined = this.heap[0];
      if (root === undefined || root.deadline > now) {
        break;
      }

      const entry: Entry = this.pop();

      // The normal send path handles an undeliverable tick: a stopped
      // receiver routes the message to dead letters.
      entry.sender.tell(entry.receiver, entry.message);

      if (!entry.repeats) {
        this.discard(entry);
        continue;
      }

      // A suspended receiver may be restarted or reinstated, and a routed
      // receiver's liveness is unknowable here: both keep their schedule.
      // A fully stopped local receiver never comes back, so its schedule
      // is dropped instead of dead-lettering forever.
      if (
        !entry.receiver.isRouted() &&
        !entry.receiver.isRunning() &&
        !entry.receiver.isSuspended()
      ) {
        this.discard(entry);
        continue;
      }

      entry.deadline = now + entry.interval;
      this.push(entry);
    }

    this.arm();
  }

  /** Records `entry` against the actor whose stop cancels it. */
  private own(owner: PID, entry: Entry): void {
    let set: Set<Entry> | undefined = this.owned.get(owner);
    if (set === undefined) {
      set = new Set<Entry>();
      this.owned.set(owner, set);
      owner.onStopped(() => {
        this.dropOwner(owner);
      });
    }

    set.add(entry);
  }

  /** Cancels every schedule owned by a fully stopped actor. */
  private dropOwner(owner: PID): void {
    const set: Set<Entry> | undefined = this.owned.get(owner);
    if (set === undefined) {
      return;
    }

    this.owned.delete(owner);
    for (const entry of set) {
      this.remove(entry);
      if (entry.reference !== null) {
        this.references.delete(entry.reference);
      }
    }

    this.arm();
  }

  /** Removes `entry` from every structure that tracks it. */
  private discard(entry: Entry): void {
    this.remove(entry);

    if (entry.reference !== null) {
      this.references.delete(entry.reference);
    }

    if (entry.owner !== null) {
      this.owned.get(entry.owner)?.delete(entry);
    }
  }

  /** Arms the shared timer for the earliest deadline in the heap. */
  private arm(): void {
    const root: Entry | undefined = this.heap[0];

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
    const i: number = entry.index;
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
      const i: number = (j - 1) >> 1;
      if ((this.heap[j] as Entry).deadline >= (this.heap[i] as Entry).deadline) {
        break;
      }

      this.swap(i, j);
      j = i;
    }
  }

  private down(i: number): void {
    const n: number = this.heap.length;

    for (;;) {
      const left: number = 2 * i + 1;
      if (left >= n) {
        break;
      }

      let child: number = left;
      const right: number = left + 1;
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
