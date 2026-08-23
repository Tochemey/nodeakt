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
import type { IsolateRoute } from "../src/actor.ref";
import { ActorSystem } from "../src/actor.system";
import { discardLogger } from "../src/discard.logger";
import { ErrDead } from "../src/errors";
import { newPathAt } from "../src/path";
import { completedRequest } from "../src/reentrancy";
import { routedPid } from "../src/routed.pid";
import { moduleExtension, setWorkerEntry, workerEntry } from "../src/worker.entry.locator";

describe("routedPid", () => {
  const system = new ActorSystem("routed", { logger: discardLogger });
  const path = newPathAt("far", { system: "routed", host: "127.0.0.1", port: 0 }, undefined, "1");

  function stubRoute(calls: string[]): IsolateRoute {
    return {
      workerId: 7,
      tell: (to, message) => {
        calls.push(`tell:${to.name()}:${String(message)}`);
        return null;
      },
      ask: (to, message) => Promise.resolve(`asked:${to.name()}:${String(message)}`),
      request: () => completedRequest(ErrDead),
      watch: (to, watcher) => {
        calls.push(`watch:${to.name()}:${watcher.name()}`);
      },
      unwatch: (to, watcher) => {
        calls.push(`unwatch:${to.name()}:${watcher.name()}`);
      },
    };
  }

  it("routes every send, watch, and request through its route", async () => {
    const calls: string[] = [];
    const handle = routedPid(system, path, stubRoute(calls));
    const watcher = routedPid(
      system,
      newPathAt("me", { system: "routed", host: "127.0.0.1", port: 0 }, undefined, ""),
      stubRoute([]),
    );

    expect(handle.tell(handle, "hi")).toBeNull();
    await expect(handle.ask(handle, "there", 1000)).resolves.toBe("asked:far:there");

    let refusal: Error | null = null;
    handle.request(handle, "now").onReply((_reply, error) => {
      refusal = error;
    });
    expect(refusal).toBe(ErrDead);

    watcher.watch(handle);
    watcher.unWatch(handle);
    expect(calls).toEqual(["tell:far:hi", "watch:far:me", "unwatch:far:me"]);
  });

  it("reports identity without local liveness", () => {
    const handle = routedPid(system, path, stubRoute([]));

    expect(handle.name()).toBe("far");
    expect(handle.id()).toBe(path.toString());
    expect(handle.isRunning()).toBe(false);
    expect(handle.kind()).toBe("Object");
    expect(handle.ref().workerId()).toBe(7);
  });

  it("refuses to stop an actor it does not own, loudly", async () => {
    const handle = routedPid(system, path, stubRoute([]));

    await expect(handle.shutdown()).rejects.toThrow(
      "an actor owned by another isolate cannot be stopped through its handle",
    );
  });

  it("carries an inert stand-in actor that does nothing", () => {
    const handle = routedPid(system, path, stubRoute([]));
    const stub = handle.actor();

    expect(stub.preStart(undefined as never)).toBeUndefined();
    expect(stub.receive(undefined as never)).toBeUndefined();
    expect(stub.postStop(undefined as never)).toBeUndefined();
  });
});

describe("workerEntry locator", () => {
  it("defaults to the entry beside the runtime, matching this module's extension", () => {
    setWorkerEntry(null);
    const entry = String(workerEntry());

    // The entry sits next to the locator and shares its extension, so the
    // same code resolves `.ts` from source and the built extension in a
    // package. Under the test runner this module is TypeScript source.
    expect(entry.endsWith("/worker.entry.ts")).toBe(true);
  });

  it("derives a module's extension, ignoring query and hash", () => {
    expect(moduleExtension("file:///a/b/worker.entry.locator.ts")).toBe(".ts");
    expect(moduleExtension("file:///a/b/worker.entry.locator.mjs?v=1#x")).toBe(".mjs");
  });

  it("falls back to the built extension when the final segment has none", () => {
    expect(moduleExtension("file:///a/b/entry")).toBe(".mjs");
    expect(moduleExtension("file:///a.b/entry")).toBe(".mjs");
  });
});
