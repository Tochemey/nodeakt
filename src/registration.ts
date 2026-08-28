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

import { isAbsolute } from "node:path";
import { pathToFileURL } from "node:url";
import type { Actor } from "./actor";
import { ActorRegistry } from "./actor.registry";
import { captureCallSites } from "./call.sites";
import { ActorNotRegisteredError } from "./errors";
import { type MessageClass, MessageRegistry } from "./message.registry";
import { type PassivationStrategy, serializePassivation } from "./passivation";
import type { ActorClass, Props } from "./props";
import type { ActorRecipe } from "./protocol";
import type { Reentrancy } from "./reentrancy";
import type { SpawnOptions } from "./spawn.options";

/**
 * The isolate's message registry: every `registerMessage` call in this
 * isolate lands here, and the transport layer reads from here.
 *
 * @internal
 */
export const defaultMessageRegistry = new MessageRegistry();

/**
 * The isolate's actor registry: every `registerActor` call in this
 * isolate lands here, and spawning `Props` resolves through it.
 *
 * @internal
 */
export const defaultActorRegistry = new ActorRegistry();

/**
 * Registers a message class so its instances can cross isolates with
 * `instanceof` intact. Call it once, at module scope, in the file that
 * defines the class; every isolate that loads the module runs the same
 * line, which is how all isolates end up agreeing on the type.
 *
 * ```ts
 * export class Hello {
 *   constructor(readonly name: string) {}
 * }
 * registerMessage(Hello);
 * ```
 *
 * Plain objects, arrays, and primitives never need registration; only
 * class instances do. Sending an unregistered class instance across an
 * isolate boundary fails on the sending side with a named error.
 *
 * @param type - The message class.
 * @param id - The stable wire id; the class name when omitted. Set it
 * explicitly when two message classes share a name.
 *
 * @throws A `TypeError` when the id is empty or already bound to a
 * different class; registering the same class under the same id again
 * is a no-op.
 */
export function registerMessage(
  type: abstract new (...args: never[]) => object,
  id?: string,
): void {
  // Every class value carries name and prototype at runtime; the cast
  // keeps the internal registry type out of the public signature.
  defaultMessageRegistry.register(type as MessageClass, id);
}

/**
 * Registers an actor class so the runtime can construct it on any
 * isolate. Call it once, at module scope, in the file that defines the
 * class:
 *
 * ```ts
 * export class Greeter implements Actor { ... }
 * registerActor(Greeter);
 * ```
 *
 * The class is recorded against the module that made the call, which
 * is inferred from the call site: the one fact the runtime needs and
 * cannot discover on its own is where the class's code lives, and the
 * registration line sits exactly there. When the call happens through
 * a wrapper of your own, inference would name the wrapper's file; pass
 * `import.meta.url` explicitly instead.
 *
 * Registration is self-propagating: an isolate that imports the module
 * to build the actor runs this same line and registers the class
 * locally, so loading the code is the synchronization.
 *
 * @param type - The actor class; it must be exported from its module.
 * @param moduleUrl - The defining module's URL, overriding inference.
 *
 * @throws A `TypeError` for an anonymous class, when the class is
 * already registered from a different module, or when the module
 * cannot be inferred and was not given.
 */
export function registerActor(type: new (...args: never[]) => Actor, moduleUrl?: string): void {
  defaultActorRegistry.register(type, moduleUrl ?? callerModule(type));
}

/** Picks the registering module's frame out of the call sites: frame 0
 * is the frame that captured the sites, frame 1 is `registerActor`,
 * frame 2 is the module that called it.
 *
 * @internal
 */
export function callerScript(sites: ReadonlyArray<{ scriptName: string }>): string {
  const site = sites[2];
  return site === undefined ? "" : site.scriptName;
}

/** Normalizes a call-site script name to an importable module URL:
 * file URLs pass through, absolute paths convert, and anything else
 * (eval frames, REPL input, internals) is not a module.
 *
 * @internal
 */
export function scriptUrlOf(script: string): string | undefined {
  if (script.startsWith("file:")) {
    return script;
  }

  if (script !== "" && isAbsolute(script)) {
    return pathToFileURL(script).href;
  }

  return undefined;
}

/** Resolves the registration call site to the caller's module URL. */
function callerModule(type: ActorClass): string {
  const url = scriptUrlOf(callerScript(captureCallSites(3)));
  if (url === undefined) {
    throw new TypeError(
      `cannot infer the module of actor class "${type.name}"; ` +
        `pass it explicitly: registerActor(${type.name}, import.meta.url)`,
    );
  }

  return url;
}

/**
 * Resolves `Props` and its spawn options to a placeable recipe. The
 * options that are data ride the recipe: reentrancy, the passivation
 * strategy reduced to its plain form, and the relocation flag. The
 * options that are live objects (mailbox, supervisor) are refused for
 * every `Props` spawn, wherever it would land, so behavior never depends
 * on placement.
 *
 * @throws A `TypeError` for options that cannot cross an isolate, and
 * everything {@link recipeOf} throws.
 *
 * @internal
 */
export function placedRecipe(props: Props, options?: SpawnOptions): ActorRecipe {
  if (options?.mailbox !== undefined || options?.supervisor !== undefined) {
    throw new TypeError(
      "Props spawns accept only data spawn options: a mailbox and a supervisor are live " +
        "objects that cannot cross an isolate; configure them in the actor's own module",
    );
  }

  const recipe: ActorRecipe = recipeOf(props);
  const reentrancy: Reentrancy | undefined = options?.reentrancy;
  const passivationStrategy: PassivationStrategy | undefined = options?.passivationStrategy;
  const relocatable: boolean | undefined = options?.relocatable;
  return {
    ...recipe,
    ...(reentrancy !== undefined ? { reentrancy } : {}),
    ...(passivationStrategy !== undefined
      ? { passivation: serializePassivation(passivationStrategy) }
      : {}),
    ...(relocatable !== undefined ? { relocatable } : {}),
  };
}

/**
 * Resolves `Props` to the recipe another isolate can construct from:
 * the registered module URL and export name plus the constructor
 * arguments, validated to be structured-cloneable so a placement never
 * behaves differently from a local construction.
 *
 * @throws An {@link ActorNotRegisteredError} when the class was never
 * registered on this isolate.
 * @throws A `TypeError` when a constructor argument cannot cross an
 * isolate boundary.
 *
 * @internal
 */
export function recipeOf(props: Props): ActorRecipe {
  const type = props.type();
  const location = defaultActorRegistry.locationOf(type);
  if (location === undefined) {
    throw new ActorNotRegisteredError(type.name);
  }

  const args = props.args();
  if (args.length === 0) {
    return { module: location.module, actor: location.actor };
  }

  try {
    structuredClone(args);
  } catch (err) {
    throw new TypeError(
      `Props arguments for "${type.name}" must be structured-cloneable data: ${(err as Error).message}`,
    );
  }

  return { module: location.module, actor: location.actor, args };
}
