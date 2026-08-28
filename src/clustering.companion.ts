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

import { ByteReader, ByteWriter, decodeValue, encodeValue } from "./net/values";
import type { ActorRecipe } from "./protocol";

/**
 * The record stored beside a relocatable actor's placement, holding what a
 * survivor needs to recreate it when its node departs: the same {@link ActorRecipe}
 * a remote spawn ships, plus whether the actor is a cluster singleton, since a
 * singleton is re-pinned to the coordinator rather than placed by the balanced
 * fill. Its mere presence is the relocation flag: a non-relocatable actor stores
 * no companion, so recovery frees its name instead of rebuilding it.
 *
 * @internal
 */
export interface Companion {
  /** The construction recipe a survivor rebuilds the actor's `Props` from. */
  readonly recipe: ActorRecipe;

  /** Whether the actor is a cluster singleton, recovered onto the coordinator. */
  readonly singleton: boolean;
}

/** Serializes a companion to the opaque bytes its registry record holds. @internal */
export function encodeCompanion(companion: Companion): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, { recipe: companion.recipe, singleton: companion.singleton });
  // bytes() is a view over a reused buffer, so copy before it escapes the writer.
  return writer.bytes().slice();
}

/**
 * Rebuilds a companion from its registry record's bytes, validating the decoded
 * shape so a corrupt or foreign record is a typed rejection rather than a partly
 * built recipe. Recovery decodes each companion in isolation, so one bad record
 * fails only its own name.
 *
 * @throws A `TypeError` when the bytes do not decode to a well-formed companion.
 */
export function decodeCompanion(bytes: Uint8Array): Companion {
  const decoded: unknown = decodeValue(new ByteReader(bytes));
  if (typeof decoded !== "object" || decoded === null) {
    throw new TypeError("companion record is not an object");
  }

  const shape = decoded as { recipe?: unknown; singleton?: unknown };
  if (typeof shape.singleton !== "boolean") {
    throw new TypeError("companion record carries no singleton flag");
  }

  return { recipe: validRecipe(shape.recipe), singleton: shape.singleton };
}

/** Narrows a decoded value to an {@link ActorRecipe}, checking the fields a rebuild
 * dereferences; the codec already guarantees the value types, so only the recipe's
 * required identity fields are asserted here. */
function validRecipe(value: unknown): ActorRecipe {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("companion record carries no recipe");
  }

  const recipe = value as { module?: unknown; actor?: unknown; args?: unknown };
  if (typeof recipe.module !== "string" || typeof recipe.actor !== "string") {
    throw new TypeError("companion recipe is missing its module or actor");
  }

  if (recipe.args !== undefined && !Array.isArray(recipe.args)) {
    throw new TypeError("companion recipe has malformed arguments");
  }

  return recipe as ActorRecipe;
}
