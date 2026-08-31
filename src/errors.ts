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
 * Sentinel errors returned by {@link Mailbox} implementations.
 *
 * These are singleton values so the overload path never allocates (creating
 * a JavaScript `Error` captures a stack trace, which is expensive). Compare
 * with identity (`err === ErrMailboxFull`); their stack traces are
 * meaningless.
 */

/**
 * Returned by `enqueue` when a bounded mailbox is at capacity. The message
 * was not stored; the dispatcher should route it to the dead-letter stream.
 */
export const ErrMailboxFull: Error = new Error("mailbox is full");

/**
 * Returned by `enqueue` after the mailbox has been disposed. The message
 * was not stored.
 */
export const ErrMailboxDisposed: Error = new Error("mailbox is disposed");

/**
 * Returned when a message is sent to an actor that is not running: it has
 * not been started yet, is shutting down, or has stopped.
 */
export const ErrDead: Error = new Error("actor is not alive");

/** Raised when Ask is given a timeout that is not a positive duration. */
export const ErrInvalidTimeout: Error = new Error("invalid timeout");

/** Raised when Ask does not receive a response before the timeout elapses. */
export const ErrRequestTimeout: Error = new Error("request timed out");

/** Completes a request issued by an actor without reentrancy configured,
 * or with its mode set to `off`. */
export const ErrReentrancyDisabled: Error = new Error("reentrancy is disabled");

/** Raised when a reentrancy mode is not one of the supported values. */
export const ErrInvalidReentrancyMode: Error = new Error("invalid reentrancy mode");

/** Completes a request issued while the actor is already at its
 * configured in-flight limit. */
export const ErrReentrancyInFlightLimit: Error = new Error("reentrancy in-flight limit reached");

/** Completes a request that was canceled before its reply arrived. */
export const ErrRequestCanceled: Error = new Error("request canceled");

/** The reason a pipe carries to dead letters when its timeout expires
 * before the piped task settles; nothing is delivered to the target. */
export const ErrPipeTimeout: Error = new Error("pipe timed out");

/** The reason a pipe carries to dead letters when it is given no task to
 * run: `pipeTo` or `pipeToName` called with a null or undefined task. */
export const ErrUndefinedTask: Error = new Error("pipe task is not defined");

/** The reason a message routed to dead letters carries when its
 * receiver marked it unhandled. */
export const ErrUnhandled: Error = new Error("unhandled message");

/** Returned by `unstash` when the stash buffer holds no message. */
export const ErrStashBufferEmpty: Error = new Error("stash buffer is empty");

/**
 * Raised when spawning an actor under a name carrying the runtime's
 * reserved prefix.
 */
export const ErrReservedName: Error = new Error("actor name is reserved");

/** Raised when using an actor system that has not been started. */
export const ErrActorSystemNotStarted: Error = new Error("actor system has not started");

/** Raised when creating an actor system without a name. */
export const ErrNameRequired: Error = new Error("actor system name is required");

/**
 * Raised when an actor system name does not start with an alphanumeric
 * character or contains characters other than alphanumerics, '-' or '_'.
 */
export const ErrInvalidActorSystemName: Error = new Error("invalid actor system name");

/**
 * Raised when spawning an actor under an invalid name. A valid name starts
 * with an alphanumeric character, contains only alphanumerics, '-', '_'
 * or '.', and is at most 255 characters long.
 */
export const ErrInvalidActorName: Error = new Error("invalid actor name");

/** Raised when spawning an actor under a name that is already taken. */
export const ErrActorAlreadyExists: Error = new Error("actor already exists");

/** Raised when a schedule's delay or interval is not a positive number
 * of milliseconds. */
export const ErrInvalidInterval: Error = new Error("invalid interval");

/** Raised when registering a schedule under a reference that is already
 * held by an active or paused schedule. */
export const ErrScheduleAlreadyExists: Error = new Error("schedule already exists");

/** Raised when cancelling, pausing, or resuming a schedule and no
 * schedule holds the given reference. */
export const ErrScheduleNotFound: Error = new Error("schedule not found");

/**
 * Raised when a PID argument is missing or is the NoSender sentinel,
 * which is never a valid target for parent-child operations.
 */
export const ErrUndefinedActor: Error = new Error("actor is not defined");

/** Raised when a router's pool size, at spawn or in an
 * `AdjustRouterPoolSize` message, is not a valid integer count. */
export const ErrInvalidPoolSize: Error = new Error("invalid router pool size");

/** Raised when spawning a router with an unknown routing strategy. */
export const ErrInvalidRoutingStrategy: Error = new Error("invalid routing strategy");

/** Raised when spawning a consistent-hash router without a routing key
 * extractor. */
export const ErrRoutingKeyRequired: Error = new Error(
  "consistent hashing requires a routing key extractor",
);

/** Rejects an ask or request sent through a fan-out router: a
 * broadcast has no single answer. */
export const ErrFanOutAsk: Error = new Error("a fan-out router cannot answer an ask");

/** Rejects a remote operation on a system that was created without a
 * `remote` configuration; enable remoting when constructing the actor
 * system. */
export const ErrRemotingDisabled: Error = new Error("remoting is not enabled");

/** Raised when constructing an actor system with a `cluster` configuration but no
 * `remote` configuration: a clustered node must be reachable for actor messages,
 * so enable remoting alongside clustering. */
export const ErrClusterRequiresRemote: Error = new Error(
  "clustering requires remoting: set the remote option alongside cluster",
);

/** Raised when a clustered system's advertised host is a wildcard or empty: a
 * clustered node must advertise a concrete, peer-routable address, so a wildcard
 * `remote.host` such as `0.0.0.0` needs a concrete `remote.advertisedHost`. */
export const ErrClusterRequiresRoutableHost: Error = new Error(
  "clustering requires a concrete advertised host: set remote.advertisedHost when remote.host is a wildcard",
);

/** Raised by a placement that chooses its owning node, such as `spawnOn`, on a
 * system that was created without a `cluster` configuration; there is no cluster
 * to place across. */
export const ErrClusteringDisabled: Error = new Error("clustering is not enabled");

/**
 * Raised when constructing an actor system with an extension whose identifier is
 * not 2 to 255 characters long, does not start with an alphanumeric character, or
 * contains anything other than alphanumerics, '-' or '_'.
 */
export const ErrInvalidExtensionId: Error = new Error("invalid extension identifier");

/** Raised when constructing an actor system with two extensions reporting the
 * same identifier; an identifier names exactly one installed service. */
export const ErrExtensionAlreadyExists: Error = new Error("extension already exists");

/**
 * Raised when a message would cross an isolate boundary but its class
 * is not in the message registry, either while encoding on the sending
 * side or while decoding on the receiving side. Register the class on
 * every isolate that sends or receives it.
 *
 * @internal
 */
export class TypeNotRegisteredError extends Error {
  constructor(type: string) {
    super(`message type "${type}" is not registered`);
    this.name = "TypeNotRegisteredError";
  }
}

/**
 * Raised when spawning `Props` whose actor class was never registered.
 * Without a registration the runtime cannot know which module another
 * isolate must import to construct the actor; call `registerActor` at
 * module scope in the file that defines the class.
 */
export class ActorNotRegisteredError extends Error {
  constructor(actorName: string) {
    super(
      `actor class "${actorName}" is not registered; call registerActor(${actorName}) in its module`,
    );
    this.name = "ActorNotRegisteredError";
  }
}

/**
 * Raised when looking up or stopping an actor that is not in the tree
 * as a running child of the caller.
 */
export class ActorNotFoundError extends Error {
  constructor(actorName: string) {
    super(`actor ${actorName} not found`);
    this.name = "ActorNotFoundError";
  }
}

/**
 * ActorInitializationError signals that an actor failed to initialize:
 * its `preStart` hook returned or threw an error, so the actor was never
 * started and will receive no message. The underlying failure is carried
 * as `cause`.
 */
export class ActorInitializationError extends Error {
  constructor(actorName: string, cause: unknown) {
    super(`actor ${actorName} failed to initialize`, { cause });
    this.name = "ActorInitializationError";
  }
}
