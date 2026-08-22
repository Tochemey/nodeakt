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

import { ActorNotFoundError, ErrDead, ErrPipeTimeout, ErrUndefinedTask } from "../errors/errors";
import { addressOf, newPathAt } from "./path";
import type { PID } from "./pid";
import type { PipeOptions } from "./pipe.options";

/**
 * The asynchronous work behind one pipe: the promise itself, or a thunk
 * invoked once that returns it. Prefer the thunk when the pipe has a
 * timeout: it receives the pipe's `AbortSignal`, which fires when the
 * timeout expires, so the task can stop the underlying work instead of
 * running to completion for a result nobody will receive. Without a
 * timeout the signal is never fired.
 *
 * A thunk is invoked on the piping actor's own turn, so it should return
 * its promise promptly and leave the slow work to the promise, not run
 * heavy synchronous work before returning.
 */
export type PipeTask = Promise<unknown> | ((signal: AbortSignal) => Promise<unknown>);

/**
 * The state of one in-flight pipe: who is piping, where the result
 * goes, and the deadline machinery. One small object per pipe; the
 * timer and abort controller exist only when a timeout was set.
 */
class Pipe {
  readonly source: PID;

  /** The target handle; null for a by-name pipe. */
  readonly to: PID | null;

  /** The target name of a by-name pipe, resolved when the task
   * settles; null when a handle was given. */
  readonly name: string | null;

  /** Fires the task's abort signal on timeout; created only for a
   * thunk task with a timeout. */
  controller: AbortController | null = null;

  timer: NodeJS.Timeout | null = null;
  settled = false;

  constructor(source: PID, to: PID | null, name: string | null) {
    this.source = source;
    this.to = to;
    this.name = name;
  }
}

/**
 * Starts one pipe: runs the task off the source actor's message
 * processing and delivers its resolution value to the target through the
 * normal send path, with the source recorded as the sender. Exactly one
 * of `to` and `name` is set. The rejection handler is attached before
 * this function returns, so a piped task can never produce an unhandled
 * rejection warning. Runtime plumbing for `PID.pipeTo` and
 * `PID.pipeToName`.
 *
 * @internal
 */
export function runPipe(
  source: PID,
  to: PID | null,
  name: string | null,
  task: PipeTask,
  options?: PipeOptions,
): void {
  const timeout: number = options?.timeout ?? 0;
  const pipe: Pipe = new Pipe(source, to, name);

  // A null or undefined task (a JavaScript caller past the types) is a
  // dead letter, not a thrown fault on the actor's turn.
  if (task === null || task === undefined) {
    abandon(pipe, ErrUndefinedTask);
    return;
  }

  let promise: Promise<unknown>;
  if (typeof task === "function") {
    // Each thunk pipe owns its controller: the signal is unique, and it
    // is collected with the pipe once the pipe settles. The controller
    // only ever aborts on timeout; without one the signal never fires.
    const controller: AbortController = new AbortController();
    pipe.controller = controller;

    // A thunk that throws instead of returning a promise settles the
    // pipe the same way a rejected task does.
    try {
      promise = task(controller.signal);
    } catch (err) {
      fail(pipe, err);
      return;
    }
  } else {
    promise = task;
  }

  if (timeout > 0) {
    const timer: NodeJS.Timeout = setTimeout(expire, timeout, pipe);
    timer.unref();
    pipe.timer = timer;
  }

  promise.then(
    (result: unknown) => deliver(pipe, result),
    (err: unknown) => fail(pipe, err),
  );
}

/** Decides the pipe's outcome exactly once: the first settlement wins
 * and disarms the deadline; every later one reports false. */
function settle(pipe: Pipe): boolean {
  if (pipe.settled) {
    return false;
  }

  pipe.settled = true;
  const timer: NodeJS.Timeout | null = pipe.timer;
  if (timer !== null) {
    pipe.timer = null;
    clearTimeout(timer);
  }

  return true;
}

/** Delivers a resolved result to the target, resolving a by-name pipe
 * to the top-level actor holding the name now. */
function deliver(pipe: Pipe, result: unknown): void {
  if (!settle(pipe)) {
    return;
  }

  let to: PID | null = pipe.to;
  if (to === null) {
    const name: string = pipe.name as string;
    const found: PID | undefined = pipe.source.actorSystem().actorOf(name);
    if (found === undefined) {
      undeliverable(pipe, result, new ActorNotFoundError(name));
      return;
    }

    to = found;
  }

  // A pipe delivers only to a live actor: one that is running, not
  // stopping, and not suspended. The task ran off the actor's turn and
  // the target may have stopped or faulted in the meantime, so gate on
  // its liveness here rather than leaving the send path to drop the
  // result silently. In a single turn nothing runs between this check
  // and the send, so a passing check still holds at delivery.
  if (!to.isRunning()) {
    undeliverable(pipe, result, ErrDead);
    return;
  }

  // The target passed the liveness gate; the send still routes a mailbox
  // rejection (a bounded mailbox at capacity, a disposed mailbox) to
  // dead letters with the result, never a dropped result.
  pipe.source.tell(to, result);
}

/** Settles the pipe with a rejected task, normalizing a non-Error
 * rejection value. */
function fail(pipe: Pipe, reason: unknown): void {
  if (!settle(pipe)) {
    return;
  }

  abandon(pipe, reason instanceof Error ? reason : new Error(String(reason)));
}

/** Logs a pipe whose result could not be delivered and routes it to
 * dead letters carrying the result, so a target that died while the
 * task ran is as visible as a rejected task, never a silent drop. */
function undeliverable(pipe: Pipe, result: unknown, err: Error): void {
  const source: PID = pipe.source;
  const sender: string = source.path().toString();
  const receiver: string = receiverOf(pipe);

  source.actorSystem().logger().error("piped result undeliverable", {
    actor: sender,
    to: receiver,
    error: err,
  });
  source.actorSystem().toDeadletter(sender, receiver, result, err);
}

/** The deadline fired before the task settled: abort the task and
 * dead-letter the pipe with the timeout reason. */
function expire(pipe: Pipe): void {
  if (!settle(pipe)) {
    return;
  }

  pipe.controller?.abort();
  abandon(pipe, ErrPipeTimeout);
}

/** Logs a failed pipe and routes it to dead letters; nothing is
 * delivered to the target. */
function abandon(pipe: Pipe, err: Error): void {
  const source: PID = pipe.source;
  const sender: string = source.path().toString();
  const receiver: string = receiverOf(pipe);

  source.actorSystem().logger().error("piped task failed", {
    actor: sender,
    to: receiver,
    error: err,
  });
  source.actorSystem().toDeadletter(sender, receiver, undefined, err);
}

/** The canonical path string of the pipe's target: the handle's path,
 * or the top-level address a by-name pipe would resolve on this
 * system. */
function receiverOf(pipe: Pipe): string {
  if (pipe.to !== null) {
    return pipe.to.path().toString();
  }

  return newPathAt(pipe.name as string, addressOf(pipe.source.path())).toString();
}
