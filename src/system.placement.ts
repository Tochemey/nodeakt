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
import type { ActorSystem } from "./actor.system";
import { mainWorkerId } from "./control.plane";
import { addressOf, newPathAt } from "./path";
import type { PID } from "./pid";
import type { Placement } from "./placement";
import { defaultMessageRegistry, placedRecipe } from "./registration";
import { routedPid } from "./routed.pid";
import { workerEntry } from "./worker.entry.locator";
import { WorkerPool } from "./worker.pool";

/**
 * What the system hands over when it boots its placement: the capacity
 * it detected at start, the recipe fence, and whether worker facades
 * should stay quiet.
 *
 * @internal
 */
export interface SystemPlacementOptions {
  readonly capacity: number;
  readonly quiet: boolean;
}

/**
 * Boots the main isolate's placement: the worker pool sized so the
 * system's isolates match its capacity, the main isolate plus one
 * worker per remaining core, and empty at a capacity of 1 so a
 * one-core machine never pays for workers. The pool is wrapped in the
 * seam the `ActorSystem` consults, and boots on the system's first
 * `Props` spawn, so a program that never places an actor never starts
 * a worker.
 *
 * Top-level actors spawned before the boot join the single name
 * authority here: their names are claimed with the control plane and
 * released when they stop.
 *
 * @internal
 */
export async function systemPlacement(
  system: ActorSystem,
  options: SystemPlacementOptions,
): Promise<Placement> {
  const size: number = Math.max(0, options.capacity - 1);

  const pool = new WorkerPool(system, defaultMessageRegistry, {
    size,
    entry: workerEntry(),
    quiet: options.quiet,
  });

  await pool.start();

  for (const pid of system.topLevelActors()) {
    const name = pid.name();
    pool.claim(name);
    pid.onStopped(() => {
      pool.release(name);
    });
  }

  return {
    claim: (name) => Promise.resolve(pool.claim(name)),

    free: (name) => {
      pool.release(name);
    },

    async place(name, props, spawnOptions) {
      const recipe = placedRecipe(props, spawnOptions);
      const ref = await pool.place(name, recipe);
      const workerId = ref.workerId();
      if (workerId === null) {
        // The placement just spawned the actor locally and pinned its
        // incarnation, so the path resolves; nothing can interleave on
        // one thread between the spawn and this lookup.
        return system.resolvePath(ref.path()) as PID;
      }

      // route() is null only for the main isolate's own id, which the
      // branch above already handled.
      const route = pool.mesh().route(workerId) as IsolateRoute;
      return routedPid(system, ref.path(), route);
    },

    find(name) {
      const workerId = pool.ownerOf(name);
      if (workerId === undefined || workerId === mainWorkerId) {
        return undefined;
      }

      const route = pool.mesh().route(workerId) as IsolateRoute;
      const path = newPathAt(name, addressOf(system.noSender().path()), undefined, "");
      return routedPid(system, path, route);
    },

    respawn: (name) => pool.restartPlaced(name),

    stopActor: (name) => pool.stopPlaced(name),

    stop: () => pool.stop(),
  };
}
