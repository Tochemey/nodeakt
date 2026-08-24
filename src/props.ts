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

import type { Actor } from "./actor";

/**
 * The shape of a spawnable actor class: a concrete class whose
 * instances implement {@link Actor}.
 *
 * @internal
 */
export type ActorClass = new (...args: never[]) => Actor;

/**
 * Props describes how to construct an actor, as data: the actor class
 * and its constructor arguments. Passing `Props` to `ActorSystem.spawn`
 * instead of a constructed instance is what allows the runtime to build
 * the actor on whichever isolate it chooses; a live instance can only
 * ever run on the isolate that built it.
 *
 * Create it inline in the spawn call with {@link Props.create}, whose
 * arguments are checked by the compiler against the class constructor:
 *
 * ```ts
 * await system.spawn("greeter", Props.create(Greeter, "fr"));
 * ```
 *
 * Props carries construction only; how the actor runs (mailbox,
 * supervision, passivation, reentrancy) stays in `SpawnOptions`. The
 * class must be registered with `registerActor` in its own module, so
 * every isolate can resolve it back to importable code; spawning
 * unregistered Props rejects with `ActorNotRegisteredError`.
 */
export class Props {
  private readonly _type: ActorClass;
  private readonly _args: readonly unknown[];

  private constructor(type: ActorClass, args: readonly unknown[]) {
    this._type = type;
    this._args = args;
  }

  /**
   * Captures an actor class and its constructor arguments. The
   * argument list is typed by the class itself: omitting, adding, or
   * mistyping an argument is a compile error.
   */
  static create<C extends new (...args: never[]) => Actor>(
    type: C,
    ...args: ConstructorParameters<C>
  ): Props {
    return new Props(type, args);
  }

  /**
   * Rebuilds Props from a class and an argument list that already
   * crossed a boundary: the compile-time argument check happened on the
   * sending side, so none applies here. Runtime plumbing for remote
   * construction.
   *
   * @internal
   */
  static restore(type: ActorClass, args: readonly unknown[]): Props {
    return new Props(type, args);
  }

  /** Returns the actor class to construct.
   *
   * @internal
   */
  type(): ActorClass {
    return this._type;
  }

  /** Returns the constructor arguments, in order.
   *
   * @internal
   */
  args(): readonly unknown[] {
    return this._args;
  }
}
