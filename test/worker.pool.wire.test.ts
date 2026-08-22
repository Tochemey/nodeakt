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
import { threadId } from "node:worker_threads";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ActorSystem } from "../src/actor/actor.system";
import { Deadletter } from "../src/actor/messages";
import { addressOf, newPathAt } from "../src/actor/path";
import { Props } from "../src/actor/props";
import { ErrActorAlreadyExists } from "../src/errors/errors";
import { discardLogger } from "../src/logger/discard.logger";
import type { Logger } from "../src/logger/logger";
import { MessageRegistry } from "../src/runtime/message.registry";
import type { ControlMessage, WorkerMessage } from "../src/runtime/protocol";
import { recipeOf, registerActor } from "../src/runtime/registration";
import { WorkerPool } from "../src/runtime/worker.pool";
import { Registered } from "./fixtures/registered.actor.mjs";
import { Job, Receipt } from "./fixtures/wire.messages.mjs";

const outDir = resolve("node_modules/.cache/nodeakt-worker-wire-test");
const registeredModule = new URL("./fixtures/registered.actor.mjs", import.meta.url).href;
const entry = resolve(outDir, "worker.entry.mjs");
const echoModule = new URL("./fixtures/echo.actor.mjs", import.meta.url).href;
const hogModule = new URL("./fixtures/hog.actor.mjs", import.meta.url).href;
const typedModule = new URL("./fixtures/typed.actor.mjs", import.meta.url).href;
const stoppingModule = new URL("./fixtures/stopping.actor.mjs", import.meta.url).href;
const familyModule = new URL("./fixtures/family.actor.mjs", import.meta.url).href;
const slowModule = new URL("./fixtures/slow.actor.mjs", import.meta.url).href;
const setupModule = new URL("./fixtures/wire.setup.mjs", import.meta.url).href;
const forgingEntry = new URL("./fixtures/forging.entry.mjs", import.meta.url);

/** The pool's private control-plane handlers, called directly to prove
 * they shrug off frames no live worker would send. */
interface PoolInternals {
  onMessage(workerId: number, message: WorkerMessage): void;
  send(workerId: number, message: ControlMessage): void;
  stopWorker(workerId: number): Promise<void>;
}

/** A logger that records error entries and discards the rest. */
function capturingLogger(errors: string[]): Logger {
  return {
    debug(): void {},
    info(): void {},
    warn(): void {},
    error(message: string): void {
      errors.push(message);
    },
    level(): "error" {
      return "error";
    },
    enabled(level): boolean {
      return level === "error";
    },
    with(): Logger {
      return this;
    },
  };
}

beforeAll(async () => {
  mkdirSync(outDir, { recursive: true });
  const { build } = await import("tsdown");
  await build({
    entry: { "worker.entry": "src/runtime/worker.entry.ts" },
    outDir,
    format: "esm",
    dts: false,
    clean: true,
    logLevel: "silent",
    config: false,
  });
}, 120_000);

describe("WorkerPool wire contract", () => {
  let system: ActorSystem;
  let registry: MessageRegistry;
  let pool: WorkerPool;
  let letters: Deadletter[];

  beforeAll(async () => {
    system = new ActorSystem("wire", { logger: discardLogger });
    await system.start();
    letters = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        letters.push(event);
      }
    });

    registry = new MessageRegistry();
    pool = new WorkerPool(system, registry, {
      size: 2,
      entry,
      quiet: true,
      setup: setupModule,
    });
    await pool.start();
  }, 60_000);

  afterAll(async () => {
    await pool.stop();
    await system.stop();
  });

  it("round-trips registered classes across real threads with prototypes restored", async () => {
    const ref = await pool.place("typist", { module: typedModule, actor: "Typist" });

    const receipt = await ref.ask(new Job(7, "hello"), 5000);

    expect(receipt).toBeInstanceOf(Receipt);
    expect((receipt as Receipt).sawJobInstance).toBe(true);
    expect((receipt as Receipt).stamped()).toBe("receipt-7");
  });

  it("fails an ask the receiving isolate cannot decode, and the worker survives", async () => {
    class MainOnly {
      readonly value = 1;
    }

    registry.register(MainOnly, "test.MainOnly");
    const ref = await pool.place("survivor", { module: echoModule, actor: "Echo" });

    await expect(ref.ask(new MainOnly(), 5000)).rejects.toHaveProperty(
      "name",
      "TypeNotRegisteredError",
    );

    await expect(ref.ask("still-there", 5000)).resolves.toBe("echo:still-there");
    await expect
      .poll(() => letters.some((letter) => letter.reason.includes("test.MainOnly")))
      .toBe(true);
  });

  it("forwards a worker's dead letters to the main system's event stream", async () => {
    const workerId = pool.workers()[0] as number;
    const ghost = newPathAt("ghost", addressOf(system.noSender().path()), undefined, "");

    expect(pool.mesh().tell(workerId, ghost, "lost-hop")).toBeNull();

    await expect
      .poll(() =>
        letters.some(
          (letter) => letter.receiver.includes("ghost") && letter.message === "lost-hop",
        ),
      )
      .toBe(true);
  });

  it("frees a placed name once the actor stops, wherever it was placed", async () => {
    const ref = await pool.place("phoenix", { module: stoppingModule, actor: "Stopper" });
    await expect(ref.ask("ping", 5000)).resolves.toBe("alive");

    expect(ref.tell("die")).toBeNull();
    await expect.poll(() => pool.lookup("phoenix")).toBeUndefined();

    const again = await pool.place("phoenix", { module: stoppingModule, actor: "Stopper" });
    await expect(again.ask("ping", 5000)).resolves.toBe("alive");
  });

  it("keeps children on their parent's isolate", async () => {
    const ref = await pool.place("clan", { module: familyModule, actor: "Parent" });

    const family = (await ref.ask("family", 5000)) as { parent: number; child: number };

    expect(family.child).toBe(family.parent);
    expect(family.parent).not.toBe(threadId);
  });

  it("places a Props-built recipe on a real thread, aliased scan and all", async () => {
    registerActor(Registered, registeredModule);

    const ref = await pool.place("propped", recipeOf(Props.create(Registered, "yo")));

    expect(ref.workerId()).not.toBeNull();
    await expect(ref.ask("there", 5000)).resolves.toBe("yo:there");
  });

  it("refuses to place a name already living as a top-level actor on the main system", async () => {
    await system.spawn("occupied", {
      preStart(): void {},
      receive(): void {},
      postStop(): void {},
    });

    await expect(pool.place("occupied", { module: echoModule, actor: "Echo" })).rejects.toBe(
      ErrActorAlreadyExists,
    );
  });
});

describe("WorkerPool main-isolate fallback", () => {
  it("frees a fallback-placed name on the main isolate once the actor stops", async () => {
    const system = new ActorSystem("local", { logger: discardLogger });
    await system.start();
    const pool = new WorkerPool(system, new MessageRegistry(), { size: 0, entry });
    await pool.start();

    const ref = await pool.place("local-phoenix", { module: stoppingModule, actor: "Stopper" });
    expect(ref.workerId()).toBeNull();
    await expect(ref.ask("ping", 5000)).resolves.toBe("alive");

    expect(ref.tell("die")).toBeNull();
    await expect.poll(() => pool.lookup("local-phoenix")).toBeUndefined();

    const again = await pool.place("local-phoenix", { module: stoppingModule, actor: "Stopper" });
    await expect(again.ask("ping", 5000)).resolves.toBe("alive");

    await pool.stop();
    await system.stop();
  });
});

describe("WorkerPool control-plane hardening", () => {
  it("survives late, duplicate, and malformed control frames from a worker", async () => {
    const errors: string[] = [];
    const system = new ActorSystem("forged", { logger: capturingLogger(errors) });
    await system.start();
    const pool = new WorkerPool(system, new MessageRegistry(), {
      size: 1,
      entry: forgingEntry,
      stopTimeout: 200,
    });
    await pool.start();

    await expect.poll(() => errors.includes("control reply failed")).toBe(true);
    await expect.poll(() => pool.workers()).toEqual([]);

    const ref = await pool.place("fallback", { module: echoModule, actor: "Echo" });
    expect(ref.workerId()).toBeNull();
    await expect(ref.ask("hi", 5000)).resolves.toBe("echo:hi");

    await pool.stop();
    await system.stop();
  });

  it("ignores a control reply naming another worker's spawn", async () => {
    const system = new ActorSystem("seqs", { logger: discardLogger });
    await system.start();
    const pool = new WorkerPool(system, new MessageRegistry(), { size: 1, entry, quiet: true });
    await pool.start();

    const placing = pool.place("slow-one", { module: slowModule, actor: "Slow" });
    (pool as unknown as PoolInternals).onMessage(99, {
      kind: "spawned",
      seq: 1,
      path: "nodeakt://seqs@127.0.0.1:0/hijack",
      uid: "",
    });

    const ref = await placing;
    expect(ref.id()).toContain("slow-one");

    await pool.stop();
    await system.stop();
  }, 30_000);

  it("treats control traffic for unknown workers as a no-op", async () => {
    const system = new ActorSystem("noop", { logger: discardLogger });
    await system.start();
    const pool = new WorkerPool(system, new MessageRegistry(), { size: 0, entry });
    await pool.start();
    const internals = pool as unknown as PoolInternals;

    expect(() => internals.send(99, { kind: "stop" })).not.toThrow();
    await internals.stopWorker(99);
    internals.onMessage(99, { kind: "ready" });
    internals.onMessage(99, { kind: "spawned", seq: 7, path: "x", uid: "" });

    await pool.stop();
    await system.stop();
  });

  it("republishes a forwarded dead letter it cannot decode as raw wire data", async () => {
    const system = new ActorSystem("raw", { logger: discardLogger });
    await system.start();
    const letters: Deadletter[] = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        letters.push(event);
      }
    });
    const pool = new WorkerPool(system, new MessageRegistry(), { size: 0, entry });
    await pool.start();

    (pool as unknown as PoolInternals).onMessage(1, {
      kind: "deadletter",
      sender: undefined,
      receiver: "nodeakt://raw@127.0.0.1:0/ghost",
      message: { type: "test.Unknown", data: { v: 1 } },
      reason: "gone",
    });

    await expect.poll(() => letters.length).toBe(1);
    expect(letters[0]?.message).toEqual({ v: 1 });
    expect(letters[0]?.reason).toBe("gone");

    await pool.stop();
    await system.stop();
  });

  it("contains a worker that exhausts its heap limit to that isolate", async () => {
    const system = new ActorSystem("oom", { logger: discardLogger });
    await system.start();
    const pool = new WorkerPool(system, new MessageRegistry(), {
      size: 1,
      entry,
      quiet: true,
      resourceLimits: { maxOldGenerationSizeMb: 64 },
    });
    await pool.start();

    const ref = await pool.place("hog", { module: hogModule, actor: "Hog" });
    expect(ref.tell("hog")).toBeNull();

    // The isolate dies alone: the pool evicts it, the process and the
    // main system keep running, and placement falls back to main.
    await expect.poll(() => pool.workers(), { timeout: 20_000 }).toEqual([]);
    expect(system.isRunning()).toBe(true);

    const fallback = await pool.place("after-oom", { module: echoModule, actor: "Echo" });
    expect(fallback.workerId()).toBeNull();
    await expect(fallback.ask("hi", 5000)).resolves.toBe("echo:hi");

    await pool.stop();
    await system.stop();
  }, 40_000);

  it("answers facade placement requests, success and failure alike", async () => {
    const system = new ActorSystem("byproxy", { logger: discardLogger });
    await system.start();
    const pool = new WorkerPool(system, new MessageRegistry(), { size: 0, entry });
    await pool.start();
    const internals = pool as unknown as PoolInternals;

    // Success: the recipe places (locally, with an empty pool) and the
    // reply to the requesting worker is a no-op, since it is gone.
    internals.onMessage(1, {
      kind: "place",
      seq: 1,
      name: "via-facade",
      recipe: { module: echoModule, actor: "Echo", args: ["f"] },
    });
    await expect.poll(() => pool.ownerOf("via-facade")).toBe(0);

    // Failure: the name is taken, and the error reply is equally moot.
    internals.onMessage(1, {
      kind: "place",
      seq: 2,
      name: "via-facade",
      recipe: { module: echoModule, actor: "Echo", args: ["f"] },
    });
    await new Promise((settle) => setTimeout(settle, 50));
    expect(pool.ownerOf("via-facade")).toBe(0);

    // Claims answer both ways as well.
    internals.onMessage(1, { kind: "claim", seq: 3, name: "fresh-claim" });
    expect(pool.ownerOf("fresh-claim")).toBe(1);
    internals.onMessage(2, { kind: "claim", seq: 4, name: "fresh-claim" });
    expect(pool.ownerOf("fresh-claim")).toBe(1);

    await pool.stop();
    await system.stop();
  });

  it("rejects start when the setup module is not a registration module", async () => {
    const system = new ActorSystem("badsetup", { logger: discardLogger });
    await system.start();
    const pool = new WorkerPool(system, new MessageRegistry(), {
      size: 1,
      entry,
      quiet: true,
      setup: echoModule,
    });

    await expect(pool.start()).rejects.toThrow("does not default-export a registration function");

    await pool.stop();
    await system.stop();
  });
});
