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

import { ErrMailboxDisposed, ErrMailboxFull } from "../errors/errors";
import type { Mailbox } from "./mailbox";
import type { ReceiveContext } from "./receive.context";

/**
 * PriorityFunc decides the relative priority of two messages.
 *
 * Returns `true` when `msg1` should be processed before `msg2`.
 */
export type PriorityFunc = (msg1: unknown, msg2: unknown) => boolean;

/**
 * Shared binary-heap core behind the four priority mailboxes.
 *
 * The heap is kept in a flat array. In stable mode a parallel array of
 * monotonic sequence numbers breaks ties between messages the priority
 * function ranks equally, preserving FIFO order among them; unstable mode
 * skips the extra comparisons. `capacity` is `Infinity` for the unbounded
 * variants.
 */
abstract class PriorityMailboxBase implements Mailbox {
  private readonly items: ReceiveContext[] = [];
  /** Insertion sequence per heap slot; null when ordering is unstable. */
  private readonly seqs: number[] | null;
  private readonly priorityFunc: PriorityFunc;
  private readonly capacity: number;
  private seq = 0;
  private disposed = false;

  protected constructor(priorityFunc: PriorityFunc, capacity: number, stable: boolean) {
    this.priorityFunc = priorityFunc;
    this.capacity = capacity;
    this.seqs = stable ? [] : null;
  }

  enqueue(msg: ReceiveContext): Error | null {
    if (this.disposed) {
      return ErrMailboxDisposed;
    }

    if (this.items.length >= this.capacity) {
      return ErrMailboxFull;
    }

    this.items.push(msg);
    this.seqs?.push(this.seq++);
    this.siftUp(this.items.length - 1);
    return null;
  }

  dequeue(): ReceiveContext | undefined {
    const n = this.items.length;
    if (n === 0) {
      return undefined;
    }

    const top = this.items[0] as ReceiveContext;
    const last = this.items.pop() as ReceiveContext;
    const lastSeq = this.seqs === null ? 0 : (this.seqs.pop() as number);
    if (n > 1) {
      this.items[0] = last;
      if (this.seqs !== null) {
        this.seqs[0] = lastSeq;
      }

      this.siftDown(0);
    }

    return top;
  }

  isEmpty(): boolean {
    return this.items.length === 0;
  }

  len(): number {
    return this.items.length;
  }

  dispose(): void {
    this.disposed = true;
    this.items.length = 0;
    if (this.seqs !== null) {
      this.seqs.length = 0;
    }
  }

  /** True when slot `i` outranks slot `j`. */
  private less(i: number, j: number): boolean {
    const a = (this.items[i] as ReceiveContext).message;
    const b = (this.items[j] as ReceiveContext).message;

    if (this.priorityFunc(a, b)) {
      return true;
    }

    if (this.seqs === null) {
      return false;
    }

    if (this.priorityFunc(b, a)) {
      return false;
    }

    // Neither strictly outranks the other: fall back to arrival order.
    return (this.seqs[i] as number) < (this.seqs[j] as number);
  }

  private swap(i: number, j: number): void {
    const items = this.items;
    const tmp = items[i] as ReceiveContext;
    items[i] = items[j] as ReceiveContext;
    items[j] = tmp;

    if (this.seqs !== null) {
      const seqs = this.seqs;
      const tmpSeq = seqs[i] as number;
      seqs[i] = seqs[j] as number;
      seqs[j] = tmpSeq;
    }
  }

  private siftUp(j: number): void {
    while (j > 0) {
      const i = (j - 1) >> 1; // parent
      if (!this.less(j, i)) {
        break;
      }

      this.swap(i, j);
      j = i;
    }
  }

  private siftDown(i: number): void {
    const n = this.items.length;
    for (;;) {
      const left = 2 * i + 1;
      if (left >= n) {
        break;
      }

      let child = left;
      const right = left + 1;
      if (right < n && this.less(right, left)) {
        child = right;
      }

      if (!this.less(child, i)) {
        break;
      }

      this.swap(i, child);
      i = child;
    }
  }
}

/** Validates a bounded mailbox capacity. */
function checkCapacity(capacity: number): void {
  if (!Number.isInteger(capacity) || capacity <= 0) {
    throw new RangeError(`mailbox capacity must be a positive integer, got ${capacity}`);
  }
}

/**
 * UnboundedPriorityMailbox dequeues the highest-priority message first, as
 * decided by the given {@link PriorityFunc}.
 *
 * Ordering among messages of equal priority is unspecified. Use
 * {@link UnboundedStablePriorityMailbox} when FIFO tiebreaking is required.
 * `enqueue` and `dequeue` are O(log n); the mailbox is unbounded, so the
 * heap grows with the backlog.
 */
export class UnboundedPriorityMailbox extends PriorityMailboxBase {
  constructor(priorityFunc: PriorityFunc) {
    super(priorityFunc, Number.POSITIVE_INFINITY, false);
  }
}

/**
 * UnboundedStablePriorityMailbox is a priority mailbox that preserves FIFO
 * ordering among messages of equal priority.
 *
 * Each message is stamped with a monotonic arrival sequence used to break
 * ties the priority function cannot decide. Slightly slower than
 * {@link UnboundedPriorityMailbox} (up to two priority calls per
 * comparison); the mailbox is unbounded.
 */
export class UnboundedStablePriorityMailbox extends PriorityMailboxBase {
  constructor(priorityFunc: PriorityFunc) {
    super(priorityFunc, Number.POSITIVE_INFINITY, true);
  }
}

/**
 * BoundedPriorityMailbox caps the number of buffered messages; when full,
 * `enqueue` returns {@link ErrMailboxFull} and the message is discarded
 * rather than stored, so the dispatcher can route it to the dead-letter
 * stream.
 *
 * Ordering among messages of equal priority is unspecified. Use
 * {@link BoundedStablePriorityMailbox} when FIFO tiebreaking is required.
 */
export class BoundedPriorityMailbox extends PriorityMailboxBase {
  /** @throws RangeError when `capacity` is not a positive integer. */
  constructor(capacity: number, priorityFunc: PriorityFunc) {
    checkCapacity(capacity);
    super(priorityFunc, capacity, false);
  }
}

/**
 * BoundedStablePriorityMailbox combines the capacity ceiling of
 * {@link BoundedPriorityMailbox} with the FIFO tiebreaking of
 * {@link UnboundedStablePriorityMailbox}.
 */
export class BoundedStablePriorityMailbox extends PriorityMailboxBase {
  /** @throws RangeError when `capacity` is not a positive integer. */
  constructor(capacity: number, priorityFunc: PriorityFunc) {
    checkCapacity(capacity);
    super(priorityFunc, capacity, true);
  }
}
