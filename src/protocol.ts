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

import type { MessagePort } from "node:worker_threads";
import type { WireError, WireMessage } from "./envelope";
import type { MetricsOptions } from "./observability/metric.options";
import type { IsolateMetrics } from "./observability/metric.snapshot";
import type { SerializedPassivation } from "./passivation";
import type { Reentrancy } from "./reentrancy";

/**
 * ActorRecipe is a construction instruction that is data: everything an
 * isolate needs to build an actor it has never seen, without moving an
 * instance or a closure across the boundary. The owning isolate imports
 * the module, resolves the export, and constructs the actor there, so
 * its lifecycle hooks all run where it lives.
 *
 * @internal
 */
export interface ActorRecipe {
  /** Module specifier the owning isolate imports: an absolute path or
   * a file URL string. */
  readonly module: string;

  /** Name of the actor class exported by the module. */
  readonly actor: string;

  /** Constructor arguments; must be structured-cloneable. */
  readonly args?: readonly unknown[];

  /** The reentrancy configuration of the placed actor; the one spawn
   * option that is data and can cross the boundary. */
  readonly reentrancy?: Reentrancy;

  /** The placed actor's passivation strategy as plain data, rebuilt on
   * the owning isolate; omitted when the actor never passivates. */
  readonly passivation?: SerializedPassivation;

  /** Whether the placed actor is recreated on a survivor when its node
   * departs, resolved on the calling node from the system default and
   * the per-actor override; omitted leaves the owner's default in force. */
  readonly relocatable?: boolean;
}

/**
 * The boot information a worker receives through `workerData`.
 *
 * @internal
 */
export interface WorkerBootData {
  readonly workerId: number;
  readonly systemName: string;

  /** The node's advertised host and port, adopted by the facade so
   * every isolate mints and resolves the same canonical paths: on a
   * remote-enabled system the pool boots after the endpoint bound, so
   * the address here is the one the node's peers dial. */
  readonly host: string;
  readonly port: number;

  /** Silences the worker facade's logger; benches and tests set it. */
  readonly quiet: boolean;

  /** Module specifier of the message-registration setup the isolate
   * imports at boot, or null for none. Its default export receives the
   * isolate's registry, which is how every isolate ends up with the
   * same registrations. */
  readonly setup: string | null;

  /** The metrics configuration to enable on the facade, or null when the
   * system runs without metrics, so every isolate counts its own actors
   * for the machine-wide snapshot. */
  readonly metrics: MetricsOptions | null;
}

/** The control-message kinds the main isolate sends a worker; the one
 * place each discriminant is spelled, so every construction and
 * comparison names the constant instead of the literal. */
export const CONTROL_CONNECT: "connect" = "connect";
export const CONTROL_DISCONNECT: "disconnect" = "disconnect";
export const CONTROL_SPAWN: "spawn" = "spawn";
export const CONTROL_NAME_ADDED: "name-added" = "name-added";
export const CONTROL_NAME_FREED: "name-freed" = "name-freed";
export const CONTROL_CLAIMED: "claimed" = "claimed";
export const CONTROL_PLACED: "placed" = "placed";
export const CONTROL_RESTART: "restart" = "restart";
export const CONTROL_STOP_ACTOR: "stop-actor" = "stop-actor";
export const CONTROL_METRICS: "metrics" = "metrics";
export const CONTROL_STOP: "stop" = "stop";

/** The worker-message kinds a worker sends the main isolate back. */
export const WORKER_READY: "ready" = "ready";
export const WORKER_SPAWNED: "spawned" = "spawned";
export const WORKER_SPAWN_FAILED: "spawn-failed" = "spawn-failed";
export const WORKER_CLAIM: "claim" = "claim";
export const WORKER_PLACE: "place" = "place";
export const WORKER_ACTOR_STOPPED: "actor-stopped" = "actor-stopped";
export const WORKER_CONTROLLED: "controlled" = "controlled";
export const WORKER_DEADLETTER: "deadletter" = "deadletter";
export const WORKER_METRICS: "metrics-reply" = "metrics-reply";
export const WORKER_STOPPED: "stopped" = "stopped";

/**
 * Control-plane traffic from the main isolate to a worker, on the
 * worker's parent port. Application messages never travel here; they
 * ride the mesh. `name-added` and `name-freed` replicate the control
 * plane's name table into every worker, which is what lets a facade's
 * `actorOf` answer synchronously; `claimed` and `placed` answer a
 * facade's own claim and place requests. `restart` and `stop-actor`
 * drive a placed actor's lifecycle on the control plane's order,
 * answered by `controlled`.
 *
 * @internal
 */
export type ControlMessage =
  | { readonly kind: typeof CONTROL_CONNECT; readonly workerId: number; readonly port: MessagePort }
  | { readonly kind: typeof CONTROL_DISCONNECT; readonly workerId: number }
  | {
      readonly kind: typeof CONTROL_SPAWN;
      readonly seq: number;
      readonly name: string;
      readonly recipe: ActorRecipe;
    }
  | { readonly kind: typeof CONTROL_NAME_ADDED; readonly name: string; readonly workerId: number }
  | { readonly kind: typeof CONTROL_NAME_FREED; readonly name: string }
  | {
      readonly kind: typeof CONTROL_CLAIMED;
      readonly seq: number;
      readonly error: WireError | null;
    }
  | {
      readonly kind: typeof CONTROL_PLACED;
      readonly seq: number;
      readonly error: WireError | null;
      readonly workerId: number;
      readonly path: string;
      readonly uid: string;
    }
  | { readonly kind: typeof CONTROL_RESTART; readonly seq: number; readonly name: string }
  | { readonly kind: typeof CONTROL_STOP_ACTOR; readonly seq: number; readonly name: string }
  | { readonly kind: typeof CONTROL_METRICS; readonly seq: number }
  | { readonly kind: typeof CONTROL_STOP };

/**
 * Control-plane traffic from a worker back to the main isolate.
 * `actor-stopped` announces that a placed top-level actor left the
 * worker's tree, so its name frees; `deadletter` forwards one of the
 * worker facade's dead-letter events, so the one logical system's
 * event stream lives on the main isolate.
 *
 * @internal
 */
export type WorkerMessage =
  | { readonly kind: typeof WORKER_READY }
  | {
      readonly kind: typeof WORKER_SPAWNED;
      readonly seq: number;
      readonly path: string;
      readonly uid: string;
    }
  | { readonly kind: typeof WORKER_SPAWN_FAILED; readonly seq: number; readonly error: WireError }
  | { readonly kind: typeof WORKER_CLAIM; readonly seq: number; readonly name: string }
  | {
      readonly kind: typeof WORKER_PLACE;
      readonly seq: number;
      readonly name: string;
      readonly recipe: ActorRecipe;
    }
  | { readonly kind: typeof WORKER_ACTOR_STOPPED; readonly name: string }
  | {
      readonly kind: typeof WORKER_CONTROLLED;
      readonly seq: number;
      readonly error: WireError | null;
    }
  | {
      readonly kind: typeof WORKER_DEADLETTER;
      readonly sender: string | undefined;
      readonly receiver: string;
      readonly message: WireMessage;
      readonly reason: string;
    }
  | {
      readonly kind: typeof WORKER_METRICS;
      readonly seq: number;
      readonly metrics: IsolateMetrics | null;
    }
  | { readonly kind: typeof WORKER_STOPPED };
