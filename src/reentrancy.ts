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

import { ErrRequestCanceled } from "./errors";
import type { PID } from "./pid";
import { cancelAsk, type ReceiveContext } from "./receive.context";

/**
 * ReentrancyMode determines how an actor processes other messages while
 * a request started with `ReceiveContext.request` awaits its reply.
 *
 * - `off` disables requests for the actor.
 * - `allowAll` keeps processing every message while replies are in
 *   flight; state can change between a request and its continuation.
 * - `stashNonReentrant` stashes user messages while any stash-mode
 *   request is in flight, preserving strict ordering at the cost of
 *   latency; replies and runtime messages keep flowing.
 *
 * Prefer `allowAll` to avoid deadlocks in call cycles (A asks B asks A).
 * Reserve `stashNonReentrant` for flows that require strict ordering,
 * and pair it with request timeouts and a finite in-flight limit.
 */
export type ReentrancyMode = "off" | "allowAll" | "stashNonReentrant";

/** Reports whether the given value is a known reentrancy mode; guards
 * against untyped callers.
 *
 * @internal
 */
export function isValidReentrancyMode(mode: ReentrancyMode): boolean {
  return mode === "off" || mode === "allowAll" || mode === "stashNonReentrant";
}

/** Configures how an actor issues requests; set on its spawn options. */
export interface Reentrancy {
  /** How the actor processes other messages while replies are in
   * flight. */
  mode: ReentrancyMode;

  /**
   * Caps the actor's outstanding requests; a request past the cap
   * completes with `ErrReentrancyInFlightLimit`. Zero, negative, or
   * omitted means unlimited. Use a finite cap in production to bound
   * memory and mailbox pressure.
   */
  maxInFlight?: number;
}

/** Per-request configuration for `ReceiveContext.request`. */
export interface RequestOptions {
  /**
   * How long to wait for the reply, in milliseconds; no timeout when
   * omitted or non-positive. Like ask, the wait is a lower bound with
   * coarse expiry: an unanswered request completes with
   * `ErrRequestTimeout` between one and two timeout periods.
   */
  timeout?: number;

  /** Overrides the actor's configured mode for this request alone. */
  mode?: ReentrancyMode;
}

/**
 * RequestCall is the handle of one in-flight request. Register the
 * continuation with {@link onReply}; it runs on the requesting actor's
 * own turn, serialized with its message processing, so actor state is
 * safe to touch. The handle is deliberately not awaitable: an `await`
 * would park the behavior and resume on the microtask queue, outside
 * the actor's turn, which is exactly what a request avoids.
 */
export interface RequestCall {
  /**
   * Registers the continuation, called exactly once with the reply or
   * the failure. Runs on the actor's own turn; when the request has
   * already completed, it runs immediately on the caller's stack, so
   * register it inside the actor.
   */
  onReply(callback: (reply: unknown, error: Error | null) => void): void;

  /**
   * Cancels the request: the continuation completes with
   * `ErrRequestCanceled` on the actor's own turn, and a reply arriving
   * later is ignored. A no-op once the request has completed.
   */
  cancel(): void;
}

/**
 * The request bookkeeping of one reentrant actor.
 *
 * @internal
 */
export class ReentrancyState {
  readonly mode: ReentrancyMode;
  readonly maxInFlight: number;

  /** Requests awaiting a reply. */
  inFlight = 0;

  /** In-flight requests admitted under stashNonReentrant; the actor
   * stashes user messages while this is non-zero. */
  paused = 0;

  constructor(mode: ReentrancyMode, maxInFlight: number) {
    this.mode = mode;
    this.maxInFlight = maxInFlight > 0 ? maxInFlight : 0;
  }
}

/**
 * The concrete request handle: settles exactly once, remembers the
 * outcome for a late continuation, and severs its link to the delivery
 * context the moment the outcome is known, so a recycled context can
 * never be touched afterwards.
 *
 * @internal
 */
export class RequestHandle implements RequestCall {
  /** Whether the request was admitted under stashNonReentrant. */
  readonly stashMode: boolean;

  private readonly _owner: PID | null;
  private _delivery: ReceiveContext | null = null;
  private _callback: ((reply: unknown, error: Error | null) => void) | null = null;
  private _settled = false;
  private _delivered = false;
  private _reply: unknown;
  private _error: Error | null = null;

  constructor(owner: PID | null, stashMode: boolean) {
    this._owner = owner;
    this.stashMode = stashMode;
  }

  /** Links the delivery context so a cancel can withdraw it.
   *
   * @internal
   */
  attachDelivery(ctx: ReceiveContext): void {
    this._delivery = ctx;
  }

  /** Drops the delivery link once the outcome is decided at the source:
   * the context may be recycled afterwards and must not be touched.
   *
   * @internal
   */
  detachDelivery(): void {
    this._delivery = null;
  }

  /** Records the outcome exactly once; returns whether this call was
   * the one that settled it.
   *
   * @internal
   */
  settle(reply: unknown, error: Error | null): boolean {
    if (this._settled) {
      return false;
    }

    this._settled = true;
    this._reply = reply;
    this._error = error;
    return true;
  }

  /** Runs the continuation on the owner's turn; a continuation not yet
   * registered runs when it is.
   *
   * @internal
   */
  run(): void {
    const callback = this._callback;
    if (callback === null) {
      return;
    }

    this._callback = null;
    this._delivered = true;
    callback(this._reply, this._error);
  }

  /** Settles the request without a continuation: the owner is gone and
   * can never run a turn again.
   *
   * @internal
   */
  abandon(): void {
    this._settled = true;
    this._delivered = true;
    this._callback = null;
  }

  onReply(callback: (reply: unknown, error: Error | null) => void): void {
    if (this._settled) {
      if (this._delivered) {
        return;
      }

      this._delivered = true;
      callback(this._reply, this._error);
      return;
    }

    this._callback = callback;
  }

  cancel(): void {
    if (this._settled) {
      return;
    }

    const delivery = this._delivery;
    if (delivery !== null) {
      this._delivery = null;
      cancelAsk(delivery);
    }

    (this._owner as PID).deliverRequestReply(this, undefined, ErrRequestCanceled);
  }
}

/** Builds a handle that has already completed with the given failure;
 * the continuation runs as soon as it is registered.
 *
 * @internal
 */
export function completedRequest(error: Error): RequestCall {
  const handle = new RequestHandle(null, false);
  handle.settle(undefined, error);
  return handle;
}
