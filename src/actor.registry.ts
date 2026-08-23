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

import type { ActorClass } from "./props";

/**
 * Where any isolate can rebuild a registered actor class: the module
 * to import and the export name to resolve in it.
 *
 * @internal
 */
export interface ActorLocation {
  readonly module: string;
  readonly actor: string;
}

/**
 * ActorRegistry maps actor classes to the module that defines them.
 *
 * A class object cannot cross an isolate boundary and does not know
 * which module it came from, so placing an actor on another isolate
 * requires this mapping: the owning isolate imports the module and
 * constructs the class there. Every isolate holds its own registry,
 * filled by the module-scope `registerActor` calls that run wherever
 * the module is loaded.
 *
 * @internal
 */
export class ActorRegistry {
  private readonly _locations = new Map<ActorClass, ActorLocation>();

  /**
   * Registers an actor class as defined in the given module.
   * Registering the same class with the same module again is a no-op,
   * so modules can register unconditionally on import.
   *
   * @throws A `TypeError` for an anonymous class, which no other
   * isolate could resolve by name, or when the class is already
   * registered from a different module.
   */
  register(type: ActorClass, moduleUrl: string): void {
    if (type.name === "") {
      throw new TypeError("an anonymous class cannot be registered; export it as a named class");
    }

    const existing = this._locations.get(type);
    if (existing !== undefined) {
      if (existing.module === moduleUrl) {
        return;
      }

      throw new TypeError(
        `actor class "${type.name}" is already registered from "${existing.module}"`,
      );
    }

    this._locations.set(type, { module: moduleUrl, actor: type.name });
  }

  /** Returns where the class can be rebuilt, or undefined when it was
   * never registered. */
  locationOf(type: ActorClass): ActorLocation | undefined {
    return this._locations.get(type);
  }
}
