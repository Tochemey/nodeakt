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
 * Deadletter is the event published for a message the runtime could not
 * hand to its receiver: the target was not running, or its mailbox
 * rejected the delivery. Consume these through `ActorSystem.subscribe`.
 *
 * Runtime announcements (`PostStart`, `Terminated`, and internal
 * commands) never become dead letters.
 */
export class Deadletter {
  constructor(
    /** The canonical path string of the sending actor, or undefined for
     * a detached delivery that carried no sender. */
    readonly sender: string | undefined,
    /** The canonical path string of the actor that could not receive. */
    readonly receiver: string,
    /** The message that was not handled. */
    readonly message: unknown,
    /** When the failed send happened, in milliseconds since the epoch. */
    readonly sendTime: number,
    /** Why the message was not handled, the failing error's message. */
    readonly reason: string,
  ) {}
}

/**
 * ActorStarted is published on the event stream once an actor has
 * started: its `preStart` hook has run and it is registered and ready to
 * receive. Consume it through `ActorSystem.subscribe`. The runtime's own
 * actors (the guardians and the dead-letter sink) do not announce their
 * lifecycle.
 */
export class ActorStarted {
  constructor(
    /** The canonical path string of the started actor. */
    readonly actorPath: string,
    /** When the actor started, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/**
 * ActorStopped is published on the event stream when an actor has fully
 * stopped: its mailbox has drained, `postStop` has run, and it has left
 * the tree. Every stop publishes it, whatever the cause. When the stop
 * was triggered by idleness an {@link ActorPassivated} follows.
 */
export class ActorStopped {
  constructor(
    /** The canonical path string of the stopped actor. */
    readonly actorPath: string,
    /** When the actor stopped, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/**
 * ActorPassivated is published on the event stream when an actor is
 * stopped because it stayed idle past its passivation strategy. It
 * accompanies the {@link ActorStopped} of the same actor, marking the
 * stop as idle-triggered rather than explicit.
 */
export class ActorPassivated {
  constructor(
    /** The canonical path string of the passivated actor. */
    readonly actorPath: string,
    /** When the actor was passivated, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/**
 * ActorChildCreated is published on the event stream when an actor spawns
 * a child. The child's own {@link ActorStarted} is published alongside
 * it; this event additionally names the parent.
 */
export class ActorChildCreated {
  constructor(
    /** The canonical path string of the newly spawned child. */
    readonly actorPath: string,
    /** The canonical path string of the parent that spawned it. */
    readonly parent: string,
    /** When the child was created, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/**
 * ActorRestarted is published on the event stream when a faulted actor
 * has been restarted in place: it kept its path and position in the tree,
 * re-ran `preStart`, and resumed processing. Watchers get no
 * {@link Terminated}, because the actor did not stop.
 */
export class ActorRestarted {
  constructor(
    /** The canonical path string of the restarted actor. */
    readonly actorPath: string,
    /** When the actor was restarted, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/**
 * ActorSuspended is published on the event stream when an actor faults
 * and its supervisor neither resumes nor stops it: the actor holds its
 * state, mailbox, and stash but processes nothing until it is restarted
 * or reinstated.
 */
export class ActorSuspended {
  constructor(
    /** The canonical path string of the suspended actor. */
    readonly actorPath: string,
    /** Why the actor was suspended, the failing error's message. */
    readonly reason: string,
    /** When the actor was suspended, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/**
 * ActorReinstated is published on the event stream when a suspended actor
 * is revived without resetting its state, the counterpart to a restart's
 * full re-initialization. The actor resumes processing its queued
 * messages.
 */
export class ActorReinstated {
  constructor(
    /** The canonical path string of the reinstated actor. */
    readonly actorPath: string,
    /** When the actor was reinstated, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/**
 * RuntimeCommand is the base of messages the runtime consumes in the
 * receive loop without delivering them to a behavior. Sharing one base
 * keeps the hot path at a single `instanceof` check per message.
 *
 * Runtime plumbing, deliberately not tagged internal: `PoisonPill` is
 * public API and extends this class, so the declaration must survive
 * `stripInternal` for the published types to resolve.
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
 *
 * @internal
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
 *
 * @internal
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
