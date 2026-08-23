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

import type { OutboundFrame } from "./conn";
import { FRAME_HEADER_SIZE } from "./frame";
import { HeadQueue } from "./queue";

/**
 * CreditWindow is one connection's end-to-end flow control: the
 * outbound send window the peer granted us, and the ledger of grants
 * we owe the peer for what it has sent us. It never touches the wire
 * itself; the owner hands it a write callback for released frames and
 * flushes a CREDIT frame whenever {@link CreditWindow.accrue} says a
 * batched grant is due.
 *
 * Outbound, every windowed frame costs its wire size (header plus
 * body). A frame is written immediately when nothing is already
 * waiting and either the window can afford it or the window is fully
 * replenished; the latter lets a single frame larger than the whole
 * window through an empty pipe, so an undersized peer still makes
 * progress. Everything else waits in arrival order and drains as
 * grants arrive. Exempt frames never enter the window: the owner
 * writes them directly, which is what lets control traffic overtake
 * a stalled queue.
 *
 * Inbound, owed bytes accumulate per received windowed frame and are
 * released in batches of a quarter window, so grant traffic stays
 * negligible while the sender can never stall more than a quarter of
 * its window short.
 *
 * @internal
 */

/** One frame waiting for window, with its precomputed wire cost. */
interface WindowEntry {
  readonly frame: OutboundFrame;
  readonly cost: number;
}

export class CreditWindow {
  /** The full window size; `available` never exceeds it. */
  readonly capacity: number;

  private readonly _write: (frame: OutboundFrame) => Error | null;
  private readonly _grantBatch: number;
  private readonly _queue: HeadQueue<WindowEntry> = new HeadQueue<WindowEntry>();
  private _available: number;
  private _queuedBytes: number = 0;
  private _owed: number = 0;

  constructor(capacity: number, write: (frame: OutboundFrame) => Error | null) {
    this.capacity = capacity;
    this._available = capacity;
    this._grantBatch = Math.max(1, Math.floor(capacity / 4));
    this._write = write;
  }

  /** Window bytes currently available to spend. */
  get available(): number {
    return this._available;
  }

  /** Bytes waiting for window; the owner counts these in admission. */
  get queuedBytes(): number {
    return this._queuedBytes;
  }

  /**
   * Accepts one windowed frame: writes it now when the window allows,
   * queues it otherwise. Returns the write's refusal when it was
   * written, null when it was queued.
   */
  send(frame: OutboundFrame): Error | null {
    const cost: number = FRAME_HEADER_SIZE + (frame.body === null ? 0 : frame.body.length);
    if (this._queue.length === 0 && this.affordable(cost)) {
      this._available -= cost;
      return this._write(frame);
    }

    this._queue.push({ frame, cost });
    this._queuedBytes += cost;
    return null;
  }

  /**
   * Applies a peer grant, clamped to the capacity so the peer's
   * arithmetic can never inflate the window, then drains every queued
   * frame the replenished window affords, in arrival order.
   */
  grant(count: number): void {
    this._available = Math.min(this._available + count, this.capacity);
    for (;;) {
      const head: WindowEntry | undefined = this._queue.peek();
      if (head === undefined || !this.affordable(head.cost)) {
        return;
      }

      this._queue.shift();
      this._queuedBytes -= head.cost;
      this._available -= head.cost;
      this._write(head.frame);
    }
  }

  /**
   * Records the wire cost of one received windowed frame. Returns the
   * grant to flush to the peer now, or zero while the batch is still
   * accumulating; the owner bounds the accumulation with a deadline
   * through {@link CreditWindow.takeOwed}, since repayment held below
   * the batch forever could park the peer's queue with no way out.
   */
  accrue(cost: number): number {
    this._owed += cost;
    if (this._owed < this._grantBatch) {
      return 0;
    }

    const due: number = this._owed;
    this._owed = 0;
    return due;
  }

  /** Takes the whole owed balance for an off-batch flush. */
  takeOwed(): number {
    const due: number = this._owed;
    this._owed = 0;
    return due;
  }

  /** Drops every waiting frame; teardown owns settling their askers. */
  clear(): void {
    this._queue.clear();
    this._queuedBytes = 0;
  }

  /**
   * The send rule: affordable outright, or the window is fully
   * replenished, which admits one frame larger than the whole window.
   */
  private affordable(cost: number): boolean {
    return cost <= this._available || this._available >= this.capacity;
  }
}
