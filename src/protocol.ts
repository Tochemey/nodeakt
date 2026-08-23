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
}

/**
 * The boot information a worker receives through `workerData`.
 *
 * @internal
 */
export interface WorkerBootData {
  readonly workerId: number;
  readonly systemName: string;

  /** Silences the worker facade's logger; benches and tests set it. */
  readonly quiet: boolean;

  /** Module specifier of the message-registration setup the isolate
   * imports at boot, or null for none. Its default export receives the
   * isolate's registry, which is how every isolate ends up with the
   * same registrations. */
  readonly setup: string | null;
}

/**
 * Control-plane traffic from the main isolate to a worker, on the
 * worker's parent port. Application messages never travel here; they
 * ride the mesh. `name-added` and `name-freed` replicate the control
 * plane's name table into every worker, which is what lets a facade's
 * `actorOf` answer synchronously; `claimed` and `placed` answer a
 * facade's own claim and place requests.
 *
 * @internal
 */
export type ControlMessage =
  | { readonly kind: "connect"; readonly workerId: number; readonly port: MessagePort }
  | { readonly kind: "disconnect"; readonly workerId: number }
  | {
      readonly kind: "spawn";
      readonly seq: number;
      readonly name: string;
      readonly recipe: ActorRecipe;
    }
  | { readonly kind: "name-added"; readonly name: string; readonly workerId: number }
  | { readonly kind: "name-freed"; readonly name: string }
  | { readonly kind: "claimed"; readonly seq: number; readonly error: WireError | null }
  | {
      readonly kind: "placed";
      readonly seq: number;
      readonly error: WireError | null;
      readonly workerId: number;
      readonly path: string;
      readonly uid: string;
    }
  | { readonly kind: "stop" };

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
  | { readonly kind: "ready" }
  | { readonly kind: "spawned"; readonly seq: number; readonly path: string; readonly uid: string }
  | { readonly kind: "spawn-failed"; readonly seq: number; readonly error: WireError }
  | { readonly kind: "claim"; readonly seq: number; readonly name: string }
  | {
      readonly kind: "place";
      readonly seq: number;
      readonly name: string;
      readonly recipe: ActorRecipe;
    }
  | { readonly kind: "actor-stopped"; readonly name: string }
  | {
      readonly kind: "deadletter";
      readonly sender: string | undefined;
      readonly receiver: string;
      readonly message: WireMessage;
      readonly reason: string;
    }
  | { readonly kind: "stopped" };
