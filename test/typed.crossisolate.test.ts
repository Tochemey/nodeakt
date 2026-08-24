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
import { PostStart } from "../src/messages";
import type { PID } from "../src/pid";
import { Props } from "../src/props";
import type { ReceiveContext } from "../src/receive.context";
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

class Poke {
  constructor(readonly n: number) {}
}

class PingReplier implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** Records every business message; the worker's answer to a piped
 * {@link Poke} lands here. */
class PongRecorder implements Actor {
  readonly pongs: unknown[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.pongs.push(ctx.message);
  }

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
  registerMessage(Poke);
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

  it("delivers a piped result to a worker-placed actor", async () => {
    if (availableParallelism() < 2) {
      return;
    }

    const system: ActorSystem = new ActorSystem("piped", { logger: discardLogger });
    await system.start();

    const replier: PID = await system.spawn("poke-replier", Props.create(PingReplier));
    // The routed handle reports no local liveness, exactly the shape the
    // pipe's gate must not mistake for a dead target.
    expect(replier.isRunning()).toBe(false);

    const recorder: PongRecorder = new PongRecorder();
    const piper: PID = await system.spawn("piper", recorder);

    // The piped result crosses to the worker as a plain tell with the
    // piper recorded as sender; the replier answers that sender, so a
    // recorded Pong proves the result arrived on the worker.
    piper.pipeTo(replier, Promise.resolve(new Poke(41)));

    await expect.poll(() => recorder.pongs.length, { timeout: 30_000 }).toBe(1);
    expect(recorder.pongs[0]).toBeInstanceOf(Pong);
    expect((recorder.pongs[0] as Pong).n).toBe(42);

    // The by-name pipe resolves at settlement time through actorOf,
    // whose placement fallback returns the same routed handle; the
    // route-gated delivery must hold for that path too.
    piper.pipeToName("poke-replier", Promise.resolve(new Poke(10)));

    await expect.poll(() => recorder.pongs.length, { timeout: 30_000 }).toBe(2);
    expect((recorder.pongs[1] as Pong).n).toBe(11);

    await system.stop();
  }, 60_000);
});
