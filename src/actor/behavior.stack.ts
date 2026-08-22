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
 * Behavior is an actor's message handler: the function invoked for each
 * message the actor dequeues. Handlers may be synchronous or asynchronous;
 * the runtime awaits a returned promise before processing the next message.
 */
export type Behavior = (ctx: ReceiveContext) => void | Promise<void>;

/**
 * BehaviorStack holds an actor's stack of {@link Behavior} handlers and
 * powers behavior switching (become/unbecome).
 *
 * The top of the stack is the actor's current behavior. Pushing stashes the
 * previous behavior and makes the new one current; popping discards the
 * current behavior and reverts to the previous one; resetting empties the
 * stack so the actor falls back to its default receive handler.
 *
 * @internal
 */
export class BehaviorStack {
  private readonly items: Behavior[] = [];

  /** Returns the number of behaviors on the stack. */
  len(): number {
    return this.items.length;
  }

  /** Returns the current (top) behavior without removing it, or `undefined`
   * when the stack is empty. */
  peek(): Behavior | undefined {
    return this.items[this.items.length - 1];
  }

  /** Removes and returns the current (top) behavior, or `undefined` when
   * the stack is empty. */
  pop(): Behavior | undefined {
    return this.items.pop();
  }

  /** Pushes a behavior, making it the actor's current behavior. */
  push(behavior: Behavior): void {
    this.items.push(behavior);
  }

  /** Reports whether the stack holds no behaviors. */
  isEmpty(): boolean {
    return this.items.length === 0;
  }

  /** Empties the stack. */
  reset(): void {
    this.items.length = 0;
  }
}
