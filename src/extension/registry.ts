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

import { ErrExtensionAlreadyExists, ErrInvalidExtensionId } from "../errors";
import type { Extension } from "./extension";

/**
 * Extension identifiers start with an alphanumeric character and may
 * contain alphanumerics, '-' or '_'.
 */
const ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9-_]*$/;

/** The shortest identifier an extension may be registered under. */
const MIN_ID_LENGTH = 2;

/** The longest identifier an extension may be registered under. */
const MAX_ID_LENGTH = 255;

/**
 * Whether the value an extension reports as its identifier is one the
 * registry accepts. The parameter is `unknown` rather than `string` so
 * the check also covers an implementation that reports something else
 * entirely, which raises the typed error instead of crashing the boot.
 */
function isValidId(id: unknown): boolean {
  if (typeof id !== "string" || id.length < MIN_ID_LENGTH || id.length > MAX_ID_LENGTH) {
    return false;
  }

  return ID_PATTERN.test(id);
}

/**
 * ExtensionRegistry is the flat identifier-to-instance table behind an
 * actor system's extensions: the services the system was created with,
 * keyed by the identifier each reports.
 *
 * It is filled once, while the system is being created, and never
 * changes afterwards, so a lookup is a plain map read on whatever path an
 * actor takes to it. Building it is where a misconfiguration surfaces: an
 * identifier that violates the syntax rules, and a second extension
 * claiming an identifier already taken, both fail the system's
 * construction rather than leaving a lookup to answer `undefined` at
 * runtime.
 *
 * @internal
 */
export class ExtensionRegistry {
  private readonly _byId = new Map<string, Extension>();

  /**
   * Registers the given extensions, in order.
   *
   * @param extensions - The extensions to install; absent or empty
   * leaves the registry with nothing to hand out.
   *
   * @throws The {@link ErrInvalidExtensionId} sentinel when an extension
   * reports an identifier that is not 2 to 255 characters starting with
   * an alphanumeric and made up of alphanumerics, `-` or `_`.
   * @throws The {@link ErrExtensionAlreadyExists} sentinel when two
   * extensions report the same identifier.
   */
  constructor(extensions?: readonly Extension[]) {
    for (const extension of extensions ?? []) {
      const id: string = extension.id();
      if (!isValidId(id)) {
        throw ErrInvalidExtensionId;
      }

      if (this._byId.has(id)) {
        throw ErrExtensionAlreadyExists;
      }

      this._byId.set(id, extension);
    }
  }

  /**
   * Returns the extension registered under the given identifier, or
   * `undefined` when nothing holds it.
   *
   * The type parameter is a caller-side convenience: the registry stores
   * plain {@link Extension} values and asserts the concrete type on the
   * way out, so ask for the type the identifier was registered with.
   */
  get<T extends Extension>(id: string): T | undefined {
    return this._byId.get(id) as T | undefined;
  }

  /** Returns every registered extension, in registration order. */
  all(): Extension[] {
    return [...this._byId.values()];
  }
}
