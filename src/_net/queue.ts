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
 * A FIFO queue with an explicit head index, for the transport's
 * front-removal hot paths. `Array.prototype.shift` is fast where the
 * engine special-cases it, but the three supported runtimes sit on
 * two different engines, so the queues that drain from the front
 * under load advance an index instead and reclaim the consumed
 * prefix in batches.
 *
 * Consumed slots are cleared so the queue never retains a drained
 * frame, and the backing array is compacted once the prefix is both
 * large and the majority of the array.
 *
 * @internal
 */

/** Consumed-prefix bounds that trigger compaction. */
const COMPACT_MIN_HEAD: number = 64;

export class HeadQueue<T> {
  private _items: (T | undefined)[] = [];
  private _head: number = 0;

  get length(): number {
    return this._items.length - this._head;
  }

  push(item: T): void {
    this._items.push(item);
  }

  /** The oldest item without removing it. */
  peek(): T | undefined {
    return this._items[this._head];
  }

  /** Removes and returns the oldest item. */
  shift(): T | undefined {
    if (this._head === this._items.length) {
      return undefined;
    }

    const item: T | undefined = this._items[this._head];
    this._items[this._head] = undefined;
    this._head += 1;
    if (this._head === this._items.length) {
      this._items.length = 0;
      this._head = 0;
      return item;
    }

    if (this._head >= COMPACT_MIN_HEAD && this._head * 2 >= this._items.length) {
      this._items.splice(0, this._head);
      this._head = 0;
    }

    return item;
  }

  /** Removes and returns the newest item. */
  pop(): T | undefined {
    if (this._head === this._items.length) {
      return undefined;
    }

    const item: T | undefined = this._items.pop();
    if (this._head === this._items.length) {
      this._items.length = 0;
      this._head = 0;
    }

    return item;
  }

  clear(): void {
    this._items.length = 0;
    this._head = 0;
  }
}
