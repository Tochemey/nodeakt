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
import type { Extension } from "./extension/extension";
import type { Logger } from "./logger";

/**
 * Context is the environment handed to an actor's lifecycle hooks,
 * `preStart` and `postStop`.
 *
 * Unlike a `ReceiveContext`, which accompanies one message, a Context
 * describes the actor itself: which actor system manages it and the name
 * it is registered under. Use it in `preStart` to reach system-wide
 * services while acquiring the actor's resources, and in `postStop` to
 * identify the actor while releasing them.
 *
 * A Context is immutable and only valid for the lifecycle phase it was
 * handed to.
 */
export class Context {
  private readonly _actorSystem: ActorSystem;
  private readonly _actorName: string;

  /**
   * Runtime plumbing: the actor system builds a Context around an actor
   * and hands it to `preStart` and `postStop`. Developers receive it;
   * they never construct one.
   *
   * @internal
   */
  constructor(actorSystem: ActorSystem, actorName: string) {
    this._actorName = actorName;
    this._actorSystem = actorSystem;
  }

  /** Returns the actor system that manages the actor. */
  actorSystem(): ActorSystem {
    return this._actorSystem;
  }

  /** Returns the name the actor is registered under. */
  actorName(): string {
    return this._actorName;
  }

  /** Returns the actor system's logger. */
  logger(): Logger {
    return this._actorSystem.logger();
  }

  /**
   * Returns the extension installed on the actor system under the given
   * identifier, or `undefined` when none holds it.
   *
   * This is how an actor reaches a shared service while it acquires its
   * resources in `preStart`, or releases them in `postStop`, without the
   * service being threaded through its constructor.
   *
   * ```ts
   * async preStart(ctx: Context): Promise<void> {
   *   this.store = ctx.extension<EventStore>("eventStore");
   * }
   * ```
   *
   * @param id - The identifier the extension was installed under.
   */
  extension<T extends Extension>(id: string): T | undefined {
    return this._actorSystem.extension<T>(id);
  }

  /** Returns every extension installed on the actor system. */
  extensions(): Extension[] {
    return this._actorSystem.extensions();
  }
}
