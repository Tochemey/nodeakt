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

import { ActorNotRegisteredError } from "./errors";
import { deserializePassivation } from "./passivation";
import { type ActorClass, Props } from "./props";
import type { ActorRecipe } from "./protocol";
import { defaultActorRegistry } from "./registration";
import type { SpawnOptions } from "./spawn.options";

/** The recipe fields a recreate rebuilds an actor from: the class crosses by
 * registered name, so a companion's `module` is not needed here. @internal */
export type RecreateRecipe = Pick<ActorRecipe, "actor" | "args" | "reentrancy" | "passivation">;

/**
 * Rebuilds the `Props` and spawn options a relocated actor is recreated from,
 * resolving its registered class by name and its data options from the recipe. The
 * recreated actor is always relocatable, since it is being relocated, and carries
 * the singleton marker so its own companion records it the same way. Used on the
 * node that hosts the recreated actor, whether that is the coordinator recreating
 * its own slice or a survivor handling a shipped recreate.
 *
 * @throws The {@link ActorNotRegisteredError} when the class is not registered on
 * this node under the recipe's name.
 *
 * @internal
 */
export function recipeToSpawn(
  recipe: RecreateRecipe,
  singleton: boolean,
): { props: Props; options: SpawnOptions } {
  const type: ActorClass | undefined = defaultActorRegistry.classOf(recipe.actor);
  if (type === undefined) {
    throw new ActorNotRegisteredError(recipe.actor);
  }

  const options: SpawnOptions = {
    ...(recipe.reentrancy !== undefined ? { reentrancy: recipe.reentrancy } : {}),
    ...(recipe.passivation !== undefined
      ? { passivationStrategy: deserializePassivation(recipe.passivation) }
      : {}),
    relocatable: true,
    singleton,
  };
  return { props: Props.restore(type, recipe.args ?? []), options };
}
