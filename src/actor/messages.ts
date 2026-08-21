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

import type { PID } from "./pid";
import type { RequestHandle } from "./reentrancy";
import type { Directive, Strategy, Supervisor } from "./supervisor";

/**
 * System messages the runtime delivers to actors alongside business
 * messages. Handle them in `receive` with the same `instanceof` matching
 * used for user-defined messages.
 */

/**
 * PostStart is delivered to an actor as the very first message after it
 * has started, before any business message. Every started actor receives
 * it, whether it was created through the actor system's `spawn` or as a
 * child via `spawnChild`. Use it to trigger work that must run inside the
 * actor's own message loop rather than in `preStart`, such as spawning
 * the actor's children through `ctx.self`.
 */
export class PostStart {}

/**
 * Terminated notifies an actor that another actor it watches has stopped.
 */
export class Terminated {
  /** @param actorPath - The canonical path string of the stopped actor. */
  constructor(readonly actorPath: string) {}
}

/**
 * RuntimeCommand is the internal base of messages the runtime consumes in
 * the receive loop without delivering them to a behavior. Sharing one
 * base keeps the hot path at a single `instanceof` check per message.
 */
export class RuntimeCommand {}

/**
 * PoisonPill instructs an actor to shut down gracefully.
 *
 * It travels through the mailbox like any other message, so everything
 * enqueued ahead of it is processed first. Consuming the pill begins a
 * graceful stop: messages the mailbox had already accepted behind the
 * pill are still drained before the actor stops, and only sends arriving
 * after that point are rejected. The runtime consumes the pill itself;
 * it is never delivered to the actor's behavior.
 */
export class PoisonPill extends RuntimeCommand {}

/**
 * RequestReply is the internal control message carrying the outcome of an
 * in-actor request back through the requester's mailbox, so the
 * continuation runs on the requester's own turn. The runtime consumes it
 * in the receive loop; it is never delivered to a behavior.
 */
export class RequestReply extends RuntimeCommand {
  constructor(
    /** The request being completed. */
    readonly handle: RequestHandle,
    /** The reply payload; undefined on failure. */
    readonly reply: unknown,
    /** The failure; null on success. */
    readonly error: Error | null,
  ) {
    super();
  }
}

/**
 * PanicSignal notifies a supervisor that one of the actors it monitors
 * failed while processing a message. The sender of the signal is the
 * failing actor.
 */
export class PanicSignal {
  /** @param reason - The error that caused the failure. */
  constructor(readonly reason: Error) {}
}

/**
 * Panicking is the internal control message a failing actor sends to its
 * parent after resolving a supervision directive. The runtime consumes it
 * in the parent's receive loop; it is never delivered to a behavior.
 */
export class Panicking extends RuntimeCommand {
  constructor(
    /** The failing actor, already suspended. */
    readonly cid: PID,
    /** The failure thrown by the behavior. */
    readonly error: unknown,
    /** The message whose processing failed. */
    readonly message: unknown,
    /** The directive resolved from the failing actor's supervisor. */
    readonly directive: Directive,
    /** The supervision strategy of the failing actor's supervisor. */
    readonly strategy: Strategy,
    /** The failing actor's supervisor, driving restart budget and backoff. */
    readonly supervisor: Supervisor,
    /** The time of the failure, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {
    super();
  }
}
