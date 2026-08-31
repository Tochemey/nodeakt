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

import type { IsolateRoute } from "./actor.ref";
import type { IsolateMetrics } from "./observability/metric.snapshot";
import type { PID } from "./pid";
import type { Props } from "./props";
import type { SpawnOptions } from "./spawn.options";

/**
 * Placement is the seam through which an `ActorSystem` takes part in a
 * pool of isolates: the control plane's single name authority and the
 * routing of placed actors, seen from one isolate. The actor layer
 * only defines the seam; the runtime provides two implementations, one
 * wrapping the pool on the main isolate and one speaking the control
 * protocol from a worker facade, which keeps the dependency
 * one-directional.
 *
 * @internal
 */
export interface Placement {
  /**
   * Claims a top-level name for an actor spawned on this isolate, so
   * the name is unique across the whole pool.
   *
   * @returns `null` when the claim succeeded, or the refusal, usually
   * the `ErrActorAlreadyExists` sentinel.
   */
  claim(name: string): Promise<Error | null>;

  /** Releases a claimed name once its actor has stopped; unknown names
   * are a no-op. */
  free(name: string): void;

  /**
   * Places `Props` under the given top-level name on whichever isolate
   * the control plane chooses, and returns its handle: the live PID
   * when placement landed on this isolate, a routed handle otherwise.
   */
  place(name: string, props: Props, options?: SpawnOptions): Promise<PID>;

  /** Resolves a top-level name this isolate's own tree does not hold,
   * or undefined when no isolate holds it. */
  find(name: string): PID | undefined;

  /** The route to the isolate owning the placed top-level name, or
   * undefined when no other isolate holds it: what {@link find}'s
   * handle would carry, without minting one, for callers that address
   * a path of their own. */
  routeOf(name: string): IsolateRoute | undefined;

  /**
   * Restarts the placed top-level actor in place on its owning
   * isolate: same PID, same incarnation, fresh state through its
   * lifecycle hooks. Only the pool-owning implementation can order
   * this; a worker facade rejects, since lifecycle control of placed
   * actors enters through the main isolate alone.
   */
  respawn(name: string): Promise<void>;

  /** Stops the placed top-level actor gracefully on its owning
   * isolate; a name no isolate holds is already stopped, so the order
   * succeeds idempotently. Pool-owning implementation only, like
   * {@link respawn}. */
  stopActor(name: string): Promise<void>;

  /**
   * Gathers each worker isolate's raw metrics for a machine-wide
   * snapshot, one entry per live worker; a worker that drops out of the
   * collection contributes `null`. The pool-owning implementation fans
   * the request out over the control plane; a worker facade, which sees
   * only its own isolate, answers with nothing.
   */
  collectMetrics(): Promise<(IsolateMetrics | null)[]>;

  /** Tears the placement down; only the implementation that owns the
   * pool does anything here. */
  stop(): Promise<void>;
}
