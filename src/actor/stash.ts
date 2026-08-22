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
import { UnboundedMailbox } from "./unbounded.mailbox";

/**
 * Stash is a side buffer where an actor parks messages it cannot handle in
 * its current behavior, to be re-delivered later.
 *
 * The canonical use is with behavior switching: an actor that is, say,
 * waiting for an initialization result stashes every business message it
 * receives meanwhile, then, after switching back to its ready behavior,
 * unstashes them all so they are processed in their original arrival
 * order. Re-delivered messages join the mailbox tail: anything already
 * queued at that moment runs before them, and messages arriving after the
 * unstash run behind them.
 *
 * The buffer is unbounded FIFO. Re-delivery is the owner's job: the actor
 * runtime feeds what `unstash`/`unstashAll` return back into message
 * processing.
 *
 * @internal
 */
export class Stash {
  private readonly buffer = new UnboundedMailbox();

  /**
   * Adds a message to the stash buffer.
   *
   * @returns `null` when the message was stashed, or an `Error` when the
   * buffer has been disposed.
   */
  stash(ctx: ReceiveContext): Error | null {
    return this.buffer.enqueue(ctx);
  }

  /**
   * Removes and returns the oldest stashed message, or `undefined` when the
   * buffer is empty.
   */
  unstash(): ReceiveContext | undefined {
    return this.buffer.dequeue();
  }

  /**
   * Removes and returns all stashed messages in their original arrival
   * order (oldest first). The buffer is empty afterwards.
   */
  unstashAll(): ReceiveContext[] {
    const out: ReceiveContext[] = [];
    for (let ctx = this.buffer.dequeue(); ctx !== undefined; ctx = this.buffer.dequeue()) {
      out.push(ctx);
    }

    return out;
  }

  /** Reports whether the buffer holds no messages. */
  isEmpty(): boolean {
    return this.buffer.isEmpty();
  }

  /** Returns the number of stashed messages. */
  len(): number {
    return this.buffer.len();
  }

  /**
   * Releases the buffer, dropping any stashed messages. Afterwards `stash`
   * returns an `Error` and `unstash` returns `undefined`.
   */
  dispose(): void {
    this.buffer.dispose();
  }
}
