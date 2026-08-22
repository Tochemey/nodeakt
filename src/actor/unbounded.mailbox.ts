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

import { ErrMailboxDisposed } from "../errors/errors";
import type { Mailbox } from "./mailbox";
import type { ReceiveContext } from "./receive.context";

/**
 * Rings at or below this capacity are released when the mailbox drains,
 * so an idle mailbox holds no buffer; rebuilding one this small costs a
 * single tiny allocation. A larger ring records a proven backlog and
 * survives the drain, so a busy actor never re-grows its buffer between
 * bursts.
 */
const RELEASE_CAPACITY = 64;

/**
 * UnboundedMailbox is the default mailbox: an unbounded FIFO queue backed
 * by a growable ring buffer.
 *
 * Characteristics
 * - O(1) `enqueue` and `dequeue`. The ring is allocated on first enqueue
 *   (capacity 1) and grows by doubling only while there is a backlog.
 *   The first drain releases a small ring so an actor that went idle
 *   after start-up holds no buffer; later drains keep the ring, and a
 *   large ring always survives for the next burst.
 * - Unbounded: if producers outpace the consumer, memory grows without
 *   limit. Apply higher-level backpressure if needed, or use
 *   `BoundedMailbox` for a hard ceiling.
 * - FIFO: messages are dequeued in arrival order.
 * - A drained slot is cleared immediately, so a processed message never
 *   lingers in the buffer.
 */
export class UnboundedMailbox implements Mailbox {
  private buffer: Array<ReceiveContext | null> | null = null;
  private mask = 0;
  private head = 0;
  private size = 0;
  private disposed = false;

  /** Whether the mailbox has ever drained. The first drain releases a
   * small ring, so an actor that only ever processed its start-up
   * message holds no buffer; an actor that drains again is recurrent
   * and keeps the ring instead of rebuilding it every cycle. */
  private drained = false;

  enqueue(msg: ReceiveContext): Error | null {
    if (this.disposed) {
      return ErrMailboxDisposed;
    }

    if (this.buffer === null) {
      this.buffer = [null];
      this.mask = 0;
    } else if (this.size === this.buffer.length) {
      this.grow();
    }

    this.buffer[(this.head + this.size) & this.mask] = msg;
    this.size++;
    return null;
  }

  dequeue(): ReceiveContext | undefined {
    if (this.size === 0 || this.buffer === null) {
      return undefined;
    }

    const i = this.head;
    const msg = this.buffer[i] as ReceiveContext;
    this.buffer[i] = null;
    this.head = (i + 1) & this.mask;
    this.size--;

    if (this.size === 0) {
      this.head = 0;

      if (!this.drained) {
        this.drained = true;

        if (this.buffer.length <= RELEASE_CAPACITY) {
          this.buffer = null;
          this.mask = 0;
        }
      }
    }

    return msg;
  }

  isEmpty(): boolean {
    return this.size === 0;
  }

  len(): number {
    return this.size;
  }

  dispose(): void {
    this.disposed = true;
    this.buffer = null;
    this.mask = 0;
    this.head = 0;
    this.size = 0;
  }

  /** Doubles the ring, unrolling the wrapped backlog into the new buffer
   * in arrival order. */
  private grow(): void {
    const old = this.buffer as Array<ReceiveContext | null>;
    const next = new Array<ReceiveContext | null>(old.length * 2).fill(null);

    for (let i = 0; i < this.size; i++) {
      next[i] = old[(this.head + i) & this.mask] as ReceiveContext;
    }

    this.buffer = next;
    this.mask = next.length - 1;
    this.head = 0;
  }
}
