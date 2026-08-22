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
import { resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { ActorRef } from "../src/actor/actor.ref";
import { ActorSystem } from "../src/actor/actor.system";
import { addressOf, newPathAt } from "../src/actor/path";
import { discardLogger } from "../src/logger/discard.logger";
import { MessageRegistry } from "../src/runtime/message.registry";
import { WorkerPool } from "../src/runtime/worker.pool";

const outDir = resolve("node_modules/.cache/nodeakt-worker-relay-test");
const entry = resolve(outDir, "relay.entry.mjs");
const echoModule = new URL("./fixtures/echo.actor.mjs", import.meta.url).href;
const muteModule = new URL("./fixtures/mute.actor.mjs", import.meta.url).href;
const quittingModule = new URL("./fixtures/quitting.actor.mjs", import.meta.url).href;

/** The shape a relay answers an "ask|..." command with. */
interface RelayOutcome {
  ok: boolean;
  reply?: unknown;
  isDead?: boolean;
}

/**
 * Worker-to-worker traffic on real threads, with the main isolate off
 * the data path: every worker runs the relay entry, whose relay actor
 * performs mesh asks from inside its own isolate on command. The tests
 * run in order and share one pool; placement is round-robin, so the
 * worker each actor lands on is deterministic and asserted anyway.
 */
describe("worker-to-worker mesh on real threads", () => {
  let system: ActorSystem;
  let pool: WorkerPool;
  let muteRef: ActorRef;
  let padRef: ActorRef;
  let muteWorker: number;
  let padWorker: number;

  function pathOf(name: string): string {
    return newPathAt(name, addressOf(system.noSender().path()), undefined, "").toString();
  }

  function relayAsk(workerId: number, command: string): Promise<unknown> {
    const relay = newPathAt(
      `relay-${workerId}`,
      addressOf(system.noSender().path()),
      undefined,
      "",
    );
    return pool.mesh().ask(workerId, relay, command, 15_000);
  }

  beforeAll(async () => {
    mkdirSync(outDir, { recursive: true });
    const { build } = await import("tsdown");
    await build({
      entry: { "relay.entry": "test/fixtures/relay.entry.ts" },
      outDir,
      format: "esm",
      dts: false,
      clean: true,
      logLevel: "silent",
      config: false,
    });

    system = new ActorSystem("mesh", { logger: discardLogger });
    await system.start();
    pool = new WorkerPool(system, new MessageRegistry(), { size: 2, entry, quiet: true });
    await pool.start();

    muteRef = await pool.place("mute", { module: muteModule, actor: "Mute" });
    padRef = await pool.place("pad", { module: echoModule, actor: "Echo", args: ["pad"] });
    muteWorker = muteRef.workerId() as number;
    padWorker = padRef.workerId() as number;
    expect([muteWorker, padWorker].sort()).toEqual([1, 2]);
  }, 120_000);

  afterAll(async () => {
    await pool.stop();
    await system.stop();
  });

  it("carries an ask from one worker to another with main off the data path", async () => {
    const outcome = (await relayAsk(
      muteWorker,
      `ask|${padWorker}|${padRef.id()}|ping`,
    )) as RelayOutcome;

    expect(outcome.ok).toBe(true);
    expect(outcome.reply).toBe("pad:ping");
  }, 30_000);

  it("shows every worker meshed to main and to each other", async () => {
    expect(await relayAsk(muteWorker, "isolates")).toEqual([0, padWorker]);
    expect(await relayAsk(padWorker, "isolates")).toEqual([0, muteWorker]);
  }, 30_000);

  it("settles a survivor's pending ask with ErrDead when the target worker dies", async () => {
    // Round-robin places the quitter on the mute actor's worker, so
    // telling it to exit kills exactly the isolate the pending ask
    // awaits.
    const quitter = await pool.place("quitter", { module: quittingModule, actor: "Quitter" });
    expect(quitter.workerId()).toBe(muteWorker);

    const pending = relayAsk(padWorker, `ask|${muteWorker}|${pathOf("mute")}|never`);
    await new Promise((settle) => setTimeout(settle, 300));

    expect(quitter.tell("exit")).toBeNull();

    const outcome = (await pending) as RelayOutcome;
    expect(outcome.ok).toBe(false);
    expect(outcome.isDead).toBe(true);
  }, 30_000);

  it("drops the dead worker from the survivor's mesh", async () => {
    await expect.poll(() => relayAsk(padWorker, "isolates"), { timeout: 10_000 }).toEqual([0]);
  }, 30_000);
});
