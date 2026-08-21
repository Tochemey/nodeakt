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

import type { Logger } from "../logger/logger";
import type { ActorSystem } from "./actor.system";

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
}
