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

const SEGMENT_SIZE = 256;

/** A fixed-size block of messages, linked into a list as the queue grows. */
class Segment {
  readonly data: (ReceiveContext | null)[] = new Array<ReceiveContext | null>(SEGMENT_SIZE).fill(
    null,
  );
  read = 0;
  write = 0;
  next: Segment | null = null;
}

/**
 * UnboundedSegmentedMailbox is an unbounded FIFO mailbox that stores
 * messages in fixed-size array segments connected in a singly-linked list,
 * combining the cache locality of ring buffers with the growth
 * characteristics of linked queues.
 *
 * Characteristics
 * - Amortized O(1) `enqueue` and `dequeue` with no per-message allocation:
 *   a segment is allocated once per {@link SEGMENT_SIZE} messages, and a
 *   drained segment is recycled as a spare. An actor whose backlog fits in
 *   one segment runs allocation-free indefinitely.
 * - Unbounded: pair it with upstream throttling or a bounded mailbox when
 *   a hard memory ceiling is required.
 * - FIFO: dequeue order matches enqueue order across segment boundaries.
 *
 * Prefer this mailbox for high-throughput, bursty fan-in actors
 * (aggregators, routers, ingestion and telemetry sinks) where deep
 * backlogs come and go: growth is incremental, one segment at a time,
 * and a drained mailbox hands its memory back instead of holding a
 * doubled ring at its high-water capacity the way
 * {@link UnboundedMailbox} does.
 */
export class UnboundedSegmentedMailbox implements Mailbox {
  private head: Segment;
  private tail: Segment;
  /** One recycled segment kept to make burst growth allocation-free. */
  private spare: Segment | null = null;
  private size = 0;
  private disposed = false;

  constructor() {
    const first = new Segment();
    this.head = first;
    this.tail = first;
  }

  enqueue(msg: ReceiveContext): Error | null {
    if (this.disposed) {
      return ErrMailboxDisposed;
    }

    let tail = this.tail;
    if (tail.write === SEGMENT_SIZE) {
      const seg = this.spare ?? new Segment();
      this.spare = null;
      tail.next = seg;
      this.tail = seg;
      tail = seg;
    }

    tail.data[tail.write] = msg;
    tail.write++;
    this.size++;
    return null;
  }

  dequeue(): ReceiveContext | undefined {
    let seg = this.head;
    while (seg.read === seg.write) {
      const next = seg.next;
      if (next === null) {
        return undefined;
      }

      // Recycle the drained segment as the spare.
      seg.read = 0;
      seg.write = 0;
      seg.next = null;
      this.spare ??= seg;
      this.head = next;
      seg = next;
    }

    const msg = seg.data[seg.read] as ReceiveContext;
    seg.data[seg.read] = null;
    seg.read++;
    this.size--;

    // Fully drained tail segment: rewind in place so a steady-state actor
    // keeps reusing the same slots instead of rolling over to new segments.
    if (seg.read === seg.write && seg === this.tail) {
      seg.read = 0;
      seg.write = 0;
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
    const empty = new Segment();
    this.head = empty;
    this.tail = empty;
    this.spare = null;
    this.size = 0;
  }
}
