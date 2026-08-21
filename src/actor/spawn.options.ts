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

import type { Mailbox } from "./mailbox";
import type { PassivationStrategy } from "./passivation";
import type { Reentrancy } from "./reentrancy";
import type { Supervisor } from "./supervisor";

/** Options customizing an actor being spawned. */
export interface SpawnOptions {
  /** The mailbox backing the actor; an unbounded FIFO one when omitted. */
  mailbox?: Mailbox;

  /** The actor's passivation strategy; long lived (never passivated)
   * when omitted. */
  passivationStrategy?: PassivationStrategy;

  /**
   * The supervisor deciding how a failure in the actor's behavior is
   * handled. When omitted, any failure stops the actor.
   */
  supervisor?: Supervisor;

  /**
   * How the actor issues non-parking requests through
   * `ReceiveContext.request` and processes other messages while replies
   * are in flight. Requests are disabled when omitted.
   */
  reentrancy?: Reentrancy;
}
