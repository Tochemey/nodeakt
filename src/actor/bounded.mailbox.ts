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
 * BoundedMailbox is a bounded FIFO mailbox backed by a fixed-size ring
 * buffer.
 *
 * Characteristics
 * - Bounded capacity: memory is fixed at construction time and never grows.
 * - Non-blocking: `enqueue` never waits. When the ring is full it returns
 *   {@link ErrMailboxFull}; the dispatcher should route the rejected message
 *   to the dead-letter stream.
 * - O(1) `enqueue` and `dequeue`, no per-message allocation.
 * - FIFO: messages are dequeued in arrival order.
 */
export class BoundedMailbox implements Mailbox {
  private readonly buffer: (ReceiveContext | null)[];
  private readonly capacity: number;
  /** Index of the next message to dequeue; always in [0, capacity). */
  private head = 0;
  private count = 0;
  private disposed = false;

  /**
   * Creates a bounded mailbox holding at most `capacity` messages.
   *
   * @throws RangeError when `capacity` is not a positive integer.
   */
  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new RangeError(`mailbox capacity must be a positive integer, got ${capacity}`);
    }

    this.capacity = capacity;
    this.buffer = new Array<ReceiveContext | null>(capacity).fill(null);
  }

  enqueue(msg: ReceiveContext): Error | null {
    if (this.disposed) {
      return ErrMailboxDisposed;
    }

    if (this.count === this.capacity) {
      return ErrMailboxFull;
    }

    let write = this.head + this.count;
    if (write >= this.capacity) {
      write -= this.capacity;
    }

    this.buffer[write] = msg;
    this.count++;
    return null;
  }

  dequeue(): ReceiveContext | undefined {
    if (this.count === 0) {
      return undefined;
    }

    const msg = this.buffer[this.head] as ReceiveContext;
    this.buffer[this.head] = null;
    this.head++;
    if (this.head === this.capacity) {
      this.head = 0;
    }

    this.count--;
    return msg;
  }

  isEmpty(): boolean {
    return this.count === 0;
  }

  len(): number {
    return this.count;
  }

  dispose(): void {
    this.disposed = true;
    this.buffer.fill(null);
    this.head = 0;
    this.count = 0;
  }
}
