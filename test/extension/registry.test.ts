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

import { describe, expect, it } from "vitest";
import { ErrExtensionAlreadyExists, ErrInvalidExtensionId } from "../../src/errors";
import type { Extension } from "../../src/extension/extension";
import { ExtensionRegistry } from "../../src/extension/registry";

/** An extension that reports whatever identifier it was built with. */
class Service implements Extension {
  private readonly _id: string;

  constructor(id: string) {
    this._id = id;
  }

  id(): string {
    return this._id;
  }
}

/** A second type, so a lookup proves it returns the instance it was given. */
class Recorder implements Extension {
  count = 0;

  id(): string {
    return "metrics";
  }
}

describe("ExtensionRegistry", () => {
  it("hands back the instance registered under an identifier", () => {
    const store: Service = new Service("eventStore");
    const metrics: Recorder = new Recorder();
    const registry: ExtensionRegistry = new ExtensionRegistry([store, metrics]);

    expect(registry.get<Service>("eventStore")).toBe(store);
    expect(registry.get<Recorder>("metrics")).toBe(metrics);
    // A lookup is the only way in: nothing is constructed on the caller's behalf.
    expect(registry.get("tracing")).toBeUndefined();
  });

  it("reports every extension in registration order", () => {
    const first: Service = new Service("eventStore");
    const second: Service = new Service("featureFlags");

    expect(new ExtensionRegistry([first, second]).all()).toEqual([first, second]);
  });

  it("carries nothing when given no extensions", () => {
    expect(new ExtensionRegistry().all()).toEqual([]);
    expect(new ExtensionRegistry([]).get("eventStore")).toBeUndefined();
  });

  it("accepts identifiers the syntax rules allow", () => {
    for (const id of ["ab", "9x", "event-store", "event_store", "a".repeat(255)]) {
      expect(new ExtensionRegistry([new Service(id)]).get(id), id).toBeDefined();
    }
  });

  it("rejects an identifier that violates the syntax rules", () => {
    for (const id of ["", "a", "-bad", "_bad", "has space", "has.dot", "a".repeat(256)]) {
      expect(() => new ExtensionRegistry([new Service(id)]), id).toThrow(ErrInvalidExtensionId);
    }
  });

  it("rejects an identifier reported as something other than a string", () => {
    // Only reachable from JavaScript, where the interface cannot be enforced;
    // it fails the same way rather than crashing on a missing string method.
    const malformed: Extension = { id: () => 42 } as unknown as Extension;
    expect(() => new ExtensionRegistry([malformed])).toThrow(ErrInvalidExtensionId);
  });

  it("rejects two extensions claiming the same identifier", () => {
    const extensions: readonly Extension[] = [new Service("eventStore"), new Service("eventStore")];
    expect(() => new ExtensionRegistry(extensions)).toThrow(ErrExtensionAlreadyExists);
  });
});
