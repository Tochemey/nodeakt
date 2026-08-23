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

import type { ActorSystem } from "./actor.system";
import { ErrDead } from "./errors";
import type { Path } from "./path";
import type { PID } from "./pid";
import { completedRequest, type RequestCall, type RequestOptions } from "./reentrancy";

/**
 * IsolateRoute is the transport seam of a ref whose actor lives on
 * another isolate: bound to the owning worker, it carries the sends
 * this isolate's tree cannot resolve. The actor layer only defines the
 * seam; the runtime's mesh provides the implementations, which keeps
 * refs transport-agnostic and the dependency one-directional.
 *
 * @internal
 */
export interface IsolateRoute {
  /** The id of the worker isolate that owns the actor. */
  readonly workerId: number;

  tell(to: Path, message: unknown, sender?: PID): Error | null;
  ask(to: Path, message: unknown, timeout: number, sender?: PID): Promise<unknown>;
  request(to: Path, message: unknown, sender: PID, options?: RequestOptions): RequestCall;

  /** Registers `watcher` for a `Terminated` when the actor at `to`
   * stops; the registration lives on the owning isolate and the
   * notification travels back as an ordinary tell. */
  watch(to: Path, watcher: PID): void;

  /** Cancels a {@link watch} registration. */
  unwatch(to: Path, watcher: PID): void;
}

/**
 * ActorRef is the one addressed handle of an actor: a reference that
 * carries a {@link Path} instead of a live {@link PID}, resolved at
 * each send. It is what location transparency is built on: a ref can
 * be minted from a path string, stored, or handed around, and delivery
 * decides where the message goes. A ref without a route resolves on
 * this isolate's tree; a ref carrying an {@link IsolateRoute} sends
 * through it to the isolate that owns the actor. Either way an
 * undeliverable send becomes a dead letter, so the two cases are
 * indistinguishable to the caller.
 *
 * A ref minted from a live actor with {@link PID.ref} is pinned to that
 * actor's incarnation: once the actor stops, the ref is dead, even when
 * a new actor reuses the name. A ref minted from a path string with
 * `ActorSystem.refOf` carries no incarnation and addresses whatever
 * lives at the path when a message is sent.
 *
 * Runtime plumbing for the transport layer; developers interact with
 * actors through PIDs and receive contexts, never through refs.
 *
 * @internal
 */
export class ActorRef {
  private readonly _system: ActorSystem;
  private readonly _path: Path;
  private readonly _route: IsolateRoute | null;

  /** Construct through {@link PID.ref}, `ActorSystem.refOf`, or the
   * worker pool, which supplies the route for a placed actor.
   *
   * @internal
   */
  constructor(system: ActorSystem, path: Path, route: IsolateRoute | null = null) {
    this._system = system;
    this._path = path;
    this._route = route;
  }

  /** Returns the id of the worker isolate that owns the actor, or null
   * for an actor that resolves on this isolate. */
  workerId(): number | null {
    return this._route === null ? null : this._route.workerId;
  }

  /** Returns the path this ref addresses. */
  path(): Path {
    return this._path;
  }

  /** Returns the canonical path string this ref addresses. */
  id(): string {
    return this._path.toString();
  }

  /**
   * Reports whether this ref and `other` designate the same actor
   * identity: their paths match and they carry the same incarnation,
   * where two address-only refs (no incarnation) also match.
   */
  equals(other: ActorRef): boolean {
    return this._path.equals(other.path()) && this._path.uid() === other.path().uid();
  }

  /** Reports whether the ref currently resolves to a running actor of
   * its incarnation. A routed ref always reports false: liveness on
   * another isolate is not synchronously knowable, and sends discover
   * the truth through delivery. */
  isRunning(): boolean {
    if (this._route !== null) {
      return false;
    }

    return this.target()?.isRunning() ?? false;
  }

  /**
   * Sends a message to the actor this ref addresses, fire and forget.
   *
   * @param message - The message to deliver.
   * @param sender - The actor recorded as the sender; the system's
   * NoSender actor when omitted.
   *
   * @returns `null` when the message was accepted, {@link ErrDead} when
   * the ref does not resolve to a running actor of its incarnation, or
   * the mailbox error when the target rejected the message.
   */
  tell(message: unknown, sender?: PID): Error | null {
    const route = this._route;
    if (route !== null) {
      return route.tell(this._path, message, sender);
    }

    const pid = this.target();
    if (pid === null) {
      this.drop(message, sender);
      return ErrDead;
    }

    return (sender ?? this._system.noSender()).tell(pid, message);
  }

  /**
   * Sends a message to the actor this ref addresses and waits for its
   * response, with the same contract as `PID.ask`.
   *
   * @param message - The message to deliver.
   * @param timeout - How long to wait, in milliseconds. Must be positive.
   * @param sender - The actor recorded as the sender; the system's
   * NoSender actor when omitted.
   */
  ask(message: unknown, timeout: number, sender?: PID): Promise<unknown> {
    const route = this._route;
    if (route !== null) {
      return route.ask(this._path, message, timeout, sender);
    }

    const pid = this.target();
    if (pid === null) {
      this.drop(message, sender);
      return Promise.reject(ErrDead);
    }

    return (sender ?? this._system.noSender()).ask(pid, message, timeout);
  }

  /**
   * Issues a non-parking request to the actor this ref addresses on
   * behalf of `sender`, with the same contract as
   * `ReceiveContext.request`: the sender must be spawned with
   * reentrancy, and the continuation runs on the sender's own turn.
   */
  request(message: unknown, sender: PID, options?: RequestOptions): RequestCall {
    const route = this._route;
    if (route !== null) {
      return route.request(this._path, message, sender, options);
    }

    const pid = this.target();
    if (pid === null) {
      this.drop(message, sender);
      return completedRequest(ErrDead);
    }

    return sender.request(pid, message, options);
  }

  /**
   * Resolves the ref to the live PID it addresses, or null when nothing
   * lives at the path, the path belongs to another node, or the living
   * actor is a different incarnation than the one this ref was pinned
   * to.
   */
  private target(): PID | null {
    const pid = this._system.resolvePath(this._path);
    if (pid === undefined) {
      return null;
    }

    const uid = this._path.uid();
    if (uid !== "" && uid !== pid.path().uid()) {
      return null;
    }

    return pid;
  }

  /** Routes an undeliverable send to dead letters. */
  private drop(message: unknown, sender: PID | undefined): void {
    this._system.toDeadletter(sender?.path().toString(), this._path.toString(), message, ErrDead);
  }
}
