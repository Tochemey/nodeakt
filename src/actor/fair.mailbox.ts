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
import { UnboundedMailbox } from "./unbounded.mailbox";

/**
 * SenderKeyFunc derives the fairness key of a message, typically the
 * sender's identity. Messages sharing a key share a sub-queue and stay
 * FIFO-ordered relative to each other.
 */
export type SenderKeyFunc = (msg: ReceiveContext) => string;

/** The default fairness key: the sender's canonical path, with one
 * shared key for messages that carry no sender. */
function senderPathKey(msg: ReceiveContext): string {
  return msg.sender?.path().toString() ?? "";
}

/** A per-sender sub-queue, intrusively linked into the round-robin ring. */
class SenderBox {
  readonly queue = new UnboundedMailbox();
  /** Next box in the active ring; null when last or inactive. */
  nextActive: SenderBox | null = null;
  active = false;

  constructor(readonly key: string) {}
}

/**
 * UnboundedFairMailbox enforces fairness across independent senders by
 * giving each sender its own private sub-queue.
 *
 * Messages from the same sender remain FIFO-ordered, while the consumer
 * drains sub-queues in round-robin order (one message per turn) so a chatty
 * sender cannot starve quieter peers. Well suited to multi-tenant actors,
 * protocol handlers serving many clients, or any actor that must offer
 * predictable latency under bursty load. When raw throughput of a single
 * hot sender matters more than fairness, prefer {@link UnboundedMailbox}.
 *
 * A sender's sub-queue is created on first use and dropped as soon as it
 * drains, so memory tracks the set of senders with pending messages rather
 * than every sender ever seen.
 */
export class UnboundedFairMailbox implements Mailbox {
  private readonly senders = new Map<string, SenderBox>();
  private readonly senderKeyOf: SenderKeyFunc;
  private activeHead: SenderBox | null = null;
  private activeTail: SenderBox | null = null;
  private size = 0;
  private disposed = false;

  /**
   * @param senderKeyOf - Derives the fairness key of each message; when
   * omitted, the sender's canonical path is the key and messages without
   * a sender share one sub-queue.
   */
  constructor(senderKeyOf: SenderKeyFunc = senderPathKey) {
    this.senderKeyOf = senderKeyOf;
  }

  enqueue(msg: ReceiveContext): Error | null {
    if (this.disposed) {
      return ErrMailboxDisposed;
    }

    const key = this.senderKeyOf(msg);
    let box = this.senders.get(key);
    if (box === undefined) {
      box = new SenderBox(key);
      this.senders.set(key, box);
    }

    box.queue.enqueue(msg);
    this.size++;

    if (!box.active) {
      box.active = true;
      this.pushActive(box);
    }

    return null;
  }

  dequeue(): ReceiveContext | undefined {
    const box = this.popActive();
    if (box === null) {
      return undefined;
    }

    // Invariant: a box in the active ring always has a pending message.
    const msg = box.queue.dequeue() as ReceiveContext;
    this.size--;

    if (box.queue.isEmpty()) {
      box.active = false;
      this.senders.delete(box.key);
    } else {
      this.pushActive(box);
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
    this.senders.clear();
    this.activeHead = null;
    this.activeTail = null;
    this.size = 0;
  }

  private pushActive(box: SenderBox): void {
    box.nextActive = null;
    if (this.activeTail === null) {
      this.activeHead = box;
    } else {
      this.activeTail.nextActive = box;
    }

    this.activeTail = box;
  }

  private popActive(): SenderBox | null {
    const box = this.activeHead;
    if (box === null) {
      return null;
    }

    this.activeHead = box.nextActive;
    if (this.activeHead === null) {
      this.activeTail = null;
    }

    box.nextActive = null;
    return box;
  }
}
