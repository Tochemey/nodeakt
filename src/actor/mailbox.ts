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

import type { ReceiveContext } from "./receive.context";

/**
 * A message queue owned by a single actor.
 *
 * Every actor drains its mailbox one message at a time, which is what
 * guarantees single-threaded message processing per actor. The default
 * contract is FIFO: messages are dequeued in the order they were enqueued.
 * Implementations may deliberately relax this (for example a priority
 * mailbox), and must document it when they do.
 *
 * A mailbox is not shared between actors; implementations may therefore
 * assume a single consumer.
 */
export interface Mailbox {
  /**
   * Adds a message to the mailbox.
   *
   * @param msg - The receive context wrapping the message to deliver.
   * @returns `null` when the message was accepted, or an `Error` describing
   * why it was rejected (for example, the mailbox is bounded and full, or
   * has already been disposed).
   */
  enqueue(msg: ReceiveContext): Error | null;

  /**
   * Removes and returns the next message to process.
   *
   * @returns The next receive context, or `undefined` when the mailbox
   * is empty.
   */
  dequeue(): ReceiveContext | undefined;

  /**
   * Reports whether the mailbox currently holds no messages.
   *
   * Equivalent to `len() === 0`.
   */
  isEmpty(): boolean;

  /**
   * Returns the number of messages currently in the mailbox.
   */
  len(): number;

  /**
   * Releases the mailbox and any resources it holds.
   *
   * Called when the owning actor stops. After disposal the mailbox accepts
   * no new messages: `enqueue` returns an `Error` and `dequeue` returns
   * `undefined`. Disposing an already-disposed mailbox is a no-op.
   */
  dispose(): void;
}
