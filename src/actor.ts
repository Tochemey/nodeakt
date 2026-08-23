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

import type { Context } from "./context";
import type { ReceiveContext } from "./receive.context";

/**
 * Actor is the contract every actor in the system implements.
 *
 * An actor is a lightweight, isolated unit that encapsulates both state and
 * behavior. It communicates with the rest of the system exclusively through
 * messages delivered to its mailbox, and the runtime guarantees it processes
 * those messages one at a time, so an actor's internal state needs no
 * synchronization and must never be shared or mutated from outside.
 *
 * Actors follow a lifecycle, with one hook per phase:
 * - {@link preStart}: initialization before message processing begins.
 * - {@link receive}: core behavior and message handling.
 * - {@link postStop}: cleanup when the actor shuts down.
 *
 * Initialize state in `preStart` rather than in the constructor: an actor
 * instance may be constructed without ever starting, and a supervisor that
 * restarts a failed actor re-runs `preStart` to produce a fresh state.
 */
export interface Actor {
  /**
   * Called once before the actor starts processing messages.
   *
   * Use it to acquire dependencies such as database clients, caches, or
   * connections, and to recover persistent state; `ctx` identifies the
   * actor and gives access to its actor system.
   *
   * Throwing (or returning a rejected promise) aborts the start: the
   * actor never receives a message, and the failure surfaces as an
   * `ActorInitializationError` wherever the actor was spawned.
   */
  preStart(ctx: Context): void | Promise<void>;

  /**
   * Handles every message delivered to the actor's mailbox.
   *
   * This is the heart of the actor's behavior. `ctx` carries the message
   * and the tools to act on it: sending, asking, spawning children,
   * watching peers, switching behavior, stashing.
   *
   * Define messages as classes and narrow `ctx.message` with `instanceof`
   * to dispatch by message type; each branch receives the fully typed
   * message.
   *
   * The handler may be synchronous or asynchronous; when it returns a
   * promise, the runtime awaits it before dequeuing the next message, which
   * preserves the one-message-at-a-time guarantee. Avoid long-blocking
   * synchronous work here: it stalls every actor sharing the event loop.
   */
  receive(ctx: ReceiveContext): void | Promise<void>;

  /**
   * Called when the actor is about to shut down, after it has finished
   * processing the messages already in its mailbox and before it is fully
   * terminated.
   *
   * Use it to release resources, flush buffers, or notify other systems;
   * `ctx` identifies the actor and gives access to its actor system. An
   * error thrown here is logged but does not prevent the actor from
   * stopping; keep the logic fast and resilient so shutdowns stay prompt.
   */
  postStop(ctx: Context): void | Promise<void>;
}
