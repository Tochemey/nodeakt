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

import { mkdirSync } from "node:fs";
import { availableParallelism } from "node:os";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { discardLogger } from "../src/discard.logger";
import { Props } from "../src/props";
import { registerActor, registerMessage } from "../src/registration";
import { setWorkerEntry } from "../src/worker.entry.locator";

// Stand-ins registered on the main isolate under the same names the built
// fixture uses, so type ids ("Ping", "Pong") line up across the boundary.
class Ping {
  constructor(readonly n: number) {}
}

class Pong {
  constructor(readonly n: number) {}
}

class PingReplier implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

const outDir = resolve("node_modules/.cache/nodeakt-typed-crossisolate-test");
const entry = resolve(outDir, "worker.entry.mjs");
const typedActorUrl = pathToFileURL(resolve(outDir, "typed.actor.mjs")).href;

beforeAll(async () => {
  mkdirSync(outDir, { recursive: true });
  const { build } = await import("tsdown");

  // Both entries build together so the framework's registration module,
  // and the `defaultMessageRegistry` singleton in it, is one shared chunk
  // both the worker entry and the fixture import, exactly as an installed
  // package shares one nodeakt instance. That shared singleton is what a
  // module-scope registerMessage in the fixture must reach.
  await build({
    entry: {
      "worker.entry": "src/worker.entry.ts",
      "typed.actor": "test/fixtures/typed.actor.ts",
    },
    outDir,
    format: "esm",
    dts: false,
    clean: true,
    logLevel: "silent",
    config: false,
  });
  setWorkerEntry(entry);

  registerMessage(Ping);
  registerMessage(Pong);
  registerActor(PingReplier, typedActorUrl);
}, 120_000);

afterAll(() => {
  setWorkerEntry(null);
});

describe("typed messages across a real isolate", () => {
  it("keeps instanceof intact both ways through registerMessage", async () => {
    if (availableParallelism() < 2) {
      return;
    }

    const system = new ActorSystem("typed", { logger: discardLogger });
    await system.start();

    const replier = await system.spawn("ping-replier", Props.create(PingReplier));
    // A full pool never places on the main isolate: this actor lives on a
    // worker, so the round trip really crosses the boundary.
    expect(replier.isRunning()).toBe(false);

    const reply = await system.noSender().ask(replier, new Ping(41), 15_000);
    expect(reply).toBeInstanceOf(Pong);
    expect((reply as Pong).n).toBe(42);

    await system.stop();
  }, 60_000);
});
