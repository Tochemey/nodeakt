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

/**
 * A priority queue over concurrent fragment moves, so a rebalance never
 * saturates the link.
 *
 * Recovery submits every move, a drain push, a refill push, or a crash reconcile
 * pull, through one scheduler. At most {@link MAX_CONCURRENT_MOVES} run at once;
 * the rest queue and start as slots free, highest priority first. Priority is
 * what keeps the one urgent case ahead of the rest: restoring a partition that is
 * down to a single copy, the only move whose delay risks data loss, runs before a
 * graceful drain, which runs before a join rebalance that is pure balance and can
 * wait.
 *
 * A bytes-per-second cap is a second lever the design names but this class does
 * not yet pull; the concurrency cap alone bounds the link, and a byte budget
 * needs an accounting the transport does not expose. It is left for a benchmark
 * to demand.
 *
 * @internal
 */

import { MAX_CONCURRENT_MOVES } from "./constants";

/**
 * Move priorities, most urgent first. A lower number runs before a higher one
 * when both are queued.
 *
 * @internal
 */
export const MovePriority = {
  /** Refill a partition down to a single copy: the only move whose delay loses data. */
  restoreReplication: 0,
  /** Hand a graceful leaver's fragment to its replacement before it departs. */
  drain: 1,
  /** Rebalance after a join: pure balance, never urgent at a replica count above one. */
  rebalance: 2,
} as const;

/** One of the {@link MovePriority} levels. @internal */
export type MovePriorityValue = (typeof MovePriority)[keyof typeof MovePriority];

/** A move waiting for a slot: its priority, its body, and its settlement callbacks. */
interface QueuedMove {
  /** Priority level; a lower number is dequeued first. */
  readonly priority: number;
  /** The move body, run once a slot is free. */
  readonly run: () => Promise<unknown>;
  /** Resolves the caller's promise with the body's result. */
  readonly resolve: (value: unknown) => void;
  /** Rejects the caller's promise with the body's failure. */
  readonly reject: (reason: unknown) => void;
}

/**
 * Runs at most a fixed number of fragment moves concurrently, in priority order.
 *
 * @internal
 */
export class MoveScheduler {
  /** Maximum moves running at once; the rest wait in {@link #queue}. */
  readonly #maxConcurrent: number;

  /** Moves waiting for a slot, scanned for the highest priority on each dequeue. */
  readonly #queue: QueuedMove[] = [];

  /** Moves currently running, never above {@link #maxConcurrent}. */
  #running: number = 0;

  /**
   * @param maxConcurrent Positive cap on concurrent moves; defaults to
   * {@link MAX_CONCURRENT_MOVES}.
   * @throws {RangeError} If `maxConcurrent` is not a positive safe integer.
   */
  constructor(maxConcurrent: number = MAX_CONCURRENT_MOVES) {
    if (!Number.isSafeInteger(maxConcurrent) || maxConcurrent <= 0) {
      throw new RangeError("max concurrent moves must be a positive integer");
    }

    this.#maxConcurrent = maxConcurrent;
  }

  /**
   * Queues `run` at `priority` and resolves with its result once it has both
   * reached the front of its priority class and found a free slot. `run` must
   * return a promise rather than throw synchronously; a move whose promise rejects
   * rejects the returned promise and still frees its slot.
   */
  submit<T>(priority: MovePriorityValue, run: () => Promise<T>): Promise<T> {
    return new Promise<T>(
      (resolve: (value: T) => void, reject: (reason: unknown) => void): void => {
        this.#queue.push({
          priority,
          run,
          resolve: resolve as (value: unknown) => void,
          reject,
        });
        this.#pump();
      },
    );
  }

  /** Starts queued moves, highest priority first, while a slot is free. */
  #pump(): void {
    while (this.#running < this.#maxConcurrent && this.#queue.length > 0) {
      const move: QueuedMove = this.#dequeue();
      this.#running += 1;
      void move.run().then(
        (value: unknown): void => this.#settle((): void => move.resolve(value)),
        (reason: unknown): void => this.#settle((): void => move.reject(reason)),
      );
    }
  }

  /** Frees the finished move's slot, delivers its outcome, and starts the next. */
  #settle(deliver: () => void): void {
    this.#running -= 1;
    deliver();
    this.#pump();
  }

  /** Removes and returns the highest-priority queued move, ties broken by arrival. */
  #dequeue(): QueuedMove {
    let best: number = 0;
    for (let index: number = 1; index < this.#queue.length; index += 1) {
      if (
        (this.#queue[index] as QueuedMove).priority < (this.#queue[best] as QueuedMove).priority
      ) {
        best = index;
      }
    }

    return this.#queue.splice(best, 1)[0] as QueuedMove;
  }
}
