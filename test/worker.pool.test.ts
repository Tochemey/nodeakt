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
import { threadId } from "node:worker_threads";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ActorSystem } from "../src/actor.system";
import { discardLogger } from "../src/discard.logger";
import { ErrActorAlreadyExists, ErrDead } from "../src/errors";
import { MessageRegistry } from "../src/message.registry";
import { WorkerPool } from "../src/worker.pool";

const outDir = resolve("node_modules/.cache/nodeakt-worker-test");
const entry = resolve(outDir, "worker.entry.mjs");
const echoModule = new URL("./fixtures/echo.actor.mjs", import.meta.url).href;
const crashModule = new URL("./fixtures/crash.actor.mjs", import.meta.url).href;
const blockStopModule = new URL("./fixtures/block.stop.actor.mjs", import.meta.url).href;
const exitingEntry = new URL("./fixtures/exiting.entry.mjs", import.meta.url);
const threadInfoModule = new URL("./fixtures/thread.info.actor.mjs", import.meta.url).href;
const burnerModule = new URL("./fixtures/burner.actor.mjs", import.meta.url).href;
const slowModule = new URL("./fixtures/slow.actor.mjs", import.meta.url).href;

beforeAll(async () => {
  mkdirSync(outDir, { recursive: true });
  const { build } = await import("tsdown");
  await build({
    entry: { "worker.entry": "src/worker.entry.ts" },
    outDir,
    format: "esm",
    dts: false,
    clean: true,
    logLevel: "silent",
    config: false,
  });
}, 120_000);

describe("WorkerPool", () => {
  describe("with a running pool", () => {
    let system: ActorSystem;
    let pool: WorkerPool;

    beforeAll(async () => {
      system = new ActorSystem("sys", { logger: discardLogger });
      await system.start();
      pool = new WorkerPool(system, new MessageRegistry(), {
        size: 2,
        entry,
        quiet: true,
      });
      await pool.start();
    }, 60_000);

    afterAll(async () => {
      await pool.stop();
      await system.stop();
    });

    it("starts its workers and wires the mesh", () => {
      expect(pool.workers().sort()).toEqual([1, 2]);
      expect(pool.mesh().isolates().sort()).toEqual([1, 2]);
    });

    it("refuses to start twice", async () => {
      await expect(pool.start()).rejects.toThrow("worker pool already started");
    });

    it("places recipes round-robin and serves them across threads", async () => {
      const first = await pool.place("greeter-one", {
        module: echoModule,
        actor: "Echo",
        args: ["one"],
      });
      const second = await pool.place("greeter-two", {
        module: echoModule,
        actor: "Echo",
        args: ["two"],
      });

      expect([first.workerId(), second.workerId()].sort()).toEqual([1, 2]);
      expect(first.id()).toContain("greeter-one");
      expect(first.path().uid()).not.toBe("");

      await expect(first.ask("hello", 5000)).resolves.toBe("one:hello");
      await expect(second.ask("hello", 5000)).resolves.toBe("two:hello");
      expect(first.tell("fire-and-forget")).toBeNull();
    });

    it("rejects a name that is already placed anywhere in the pool", async () => {
      await pool.place("unique", { module: echoModule, actor: "Echo" });

      await expect(pool.place("unique", { module: echoModule, actor: "Echo" })).rejects.toBe(
        ErrActorAlreadyExists,
      );
    });

    it("frees the name when the spawn itself fails", async () => {
      await expect(
        pool.place("retryable", { module: echoModule, actor: "notAnActor" }),
      ).rejects.toHaveProperty("name", "TypeError");

      const ref = await pool.place("retryable", { module: echoModule, actor: "Echo" });
      await expect(ref.ask("ok", 5000)).resolves.toBe("echo:ok");
    });

    it("resolves placed names to address-only refs", async () => {
      await pool.place("known", { module: echoModule, actor: "Echo", args: ["found"] });

      const ref = pool.lookup("known");
      expect(ref).toBeDefined();
      expect(ref?.path().uid()).toBe("");
      await expect(ref?.ask("you", 5000)).resolves.toBe("found:you");

      expect(pool.lookup("unknown")).toBeUndefined();
    });

    it("carries requests from a reentrant sender across threads", async () => {
      const reentrant = await system.spawn(
        "reentrant-requester",
        {
          preStart(): void {},
          receive(): void {},
          postStop(): void {},
        },
        { reentrancy: { mode: "allowAll" } },
      );
      const target = await pool.place("request-target", {
        module: echoModule,
        actor: "Echo",
        args: ["req"],
      });
      const outcomes: Array<{ reply: unknown; error: Error | null }> = [];

      target.request("ping", reentrant).onReply((reply, error) => {
        outcomes.push({ reply, error });
      });

      await expect.poll(() => outcomes.length, { timeout: 5000 }).toBe(1);
      expect(outcomes[0]).toEqual({ reply: "req:ping", error: null });
    });

    it("runs the death sequence when a worker dies", async () => {
      // One witness on each worker, so exactly one of them dies with
      // its isolate no matter where the rotation stands.
      const witnessA = await pool.place("witness-a", { module: echoModule, actor: "Echo" });
      const witnessB = await pool.place("witness-b", { module: echoModule, actor: "Echo" });
      expect([witnessA.workerId(), witnessB.workerId()].sort()).toEqual([1, 2]);

      // A slow spawn stays in flight on the surviving worker while its
      // neighbor dies, and completes untouched by the death sequence.
      const slowPending = pool.place("slow-neighbor", {
        module: slowModule,
        actor: "Slow",
      });
      await expect(pool.place("killer", { module: crashModule, actor: "Crash" })).rejects.toBe(
        ErrDead,
      );

      await expect.poll(() => pool.workers().length, { timeout: 5000 }).toBe(1);
      const survivor = pool.workers()[0] as number;
      const dead = witnessA.workerId() === survivor ? witnessB : witnessA;
      const alive = dead === witnessA ? witnessB : witnessA;

      // The dead worker's names are free again, placement lands on the
      // survivor, refs into the dead worker fail with ErrDead, and the
      // survivor keeps serving.
      const replacement = await pool.place("killer", { module: echoModule, actor: "Echo" });
      expect(replacement.workerId()).toBe(survivor);
      expect(pool.mesh().isolates()).toEqual([survivor]);
      await expect(dead.ask("anyone", 1000)).rejects.toBe(ErrDead);
      expect(pool.lookup(dead.path().name())).toBeUndefined();
      await expect(alive.ask("still-there", 5000)).resolves.toBe("echo:still-there");

      const slowRef = await slowPending;
      expect(slowRef.workerId()).toBe(survivor);
      await expect(slowRef.ask("hello", 5000)).resolves.toBe("slow:hello");
    });
  });

  describe("scaling", () => {
    it("defaults the pool size to the machine's available parallelism", async () => {
      const system = new ActorSystem("sys", { logger: discardLogger });
      await system.start();

      const pool = new WorkerPool(system, new MessageRegistry(), { entry });

      const cores = availableParallelism();
      expect(pool.workers()).toEqual(Array.from({ length: cores }, (_, i) => i + 1));
      await system.stop();
    });

    it("runs placed actors on distinct V8 isolates in parallel", async () => {
      const system = new ActorSystem("sys", { logger: discardLogger });
      await system.start();
      const size = Math.min(4, availableParallelism());
      const pool = new WorkerPool(system, new MessageRegistry(), { size, entry, quiet: true });
      await pool.start();

      // Every placed actor reports a thread id: all distinct, none the
      // main thread's.
      const probes = await Promise.all(
        Array.from({ length: size }, (_, i) =>
          pool.place(`probe-${i}`, { module: threadInfoModule, actor: "ThreadInfo" }),
        ),
      );
      const threads = (await Promise.all(probes.map((ref) => ref.ask("tid", 5000)))) as number[];
      expect(new Set(threads).size).toBe(size);
      expect(threads).not.toContain(threadId);

      // CPU work on every isolate at once finishes in a fraction of
      // the serial time, proving the isolates genuinely run on
      // different cores.
      const burners = await Promise.all(
        Array.from({ length: size }, (_, i) =>
          pool.place(`burner-${i}`, { module: burnerModule, actor: "Burner" }),
        ),
      );
      const burnMs = 150;
      const started = Date.now();
      await Promise.all(burners.map((ref) => ref.ask(`burn:${burnMs}`, 30_000)));
      const elapsed = Date.now() - started;

      expect(elapsed).toBeLessThan((size * burnMs) / 2);

      await pool.stop();
      await system.stop();
    }, 60_000);
  });

  describe("placement fallback", () => {
    it("places locally when the pool has no workers", async () => {
      const system = new ActorSystem("sys", { logger: discardLogger });
      await system.start();
      const pool = new WorkerPool(system, new MessageRegistry(), { size: 0, entry });
      await pool.start();

      const ref = await pool.place("local-actor", {
        module: echoModule,
        actor: "Echo",
        args: ["near"],
      });

      expect(ref.workerId()).toBeNull();
      expect(ref.tell("fire")).toBeNull();
      await expect(ref.ask("by", 1000)).resolves.toBe("near:by");

      const reentrant = await system.spawn(
        "local-requester",
        { preStart(): void {}, receive(): void {}, postStop(): void {} },
        { reentrancy: { mode: "allowAll" } },
      );
      const outcomes: Array<Error | null> = [];
      ref.request("req", reentrant).onReply((_reply, error) => {
        outcomes.push(error);
      });
      await expect.poll(() => outcomes.length).toBe(1);
      expect(outcomes[0]).toBeNull();

      expect(pool.lookup("local-actor")?.workerId()).toBeNull();

      await pool.stop();
      await system.stop();
    });
  });

  describe("lifecycle", () => {
    it("refuses placements before start and after stop", async () => {
      const system = new ActorSystem("sys", { logger: discardLogger });
      await system.start();
      const pool = new WorkerPool(system, new MessageRegistry(), { size: 0, entry });

      await expect(pool.place("early", { module: echoModule, actor: "Echo" })).rejects.toBe(
        ErrDead,
      );

      await pool.start();
      await pool.stop();
      await pool.stop();

      await expect(pool.place("late", { module: echoModule, actor: "Echo" })).rejects.toBe(ErrDead);
      await system.stop();
    });

    it("rejects start when a worker entry cannot load", async () => {
      const system = new ActorSystem("sys", { logger: discardLogger });
      await system.start();
      const pool = new WorkerPool(system, new MessageRegistry(), {
        size: 1,
        entry: "/nowhere/worker.entry.mjs",
      });

      await expect(pool.start()).rejects.toHaveProperty("code", "MODULE_NOT_FOUND");

      await pool.stop();
      await system.stop();
    });

    it("rejects start when a worker exits before announcing ready", async () => {
      const system = new ActorSystem("sys", { logger: discardLogger });
      await system.start();
      const pool = new WorkerPool(system, new MessageRegistry(), {
        size: 1,
        entry: exitingEntry,
      });

      await expect(pool.start()).rejects.toBe(ErrDead);

      await pool.stop();
      await system.stop();
    }, 30_000);

    it("terminates a worker that cannot stop gracefully", async () => {
      const system = new ActorSystem("sys", { logger: discardLogger });
      await system.start();
      const pool = new WorkerPool(system, new MessageRegistry(), {
        size: 1,
        entry,
        quiet: true,
        stopTimeout: 200,
      });
      await pool.start();
      await pool.place("blocker", { module: blockStopModule, actor: "BlockStop" });

      await pool.stop();

      expect(pool.workers()).toEqual([]);
      await system.stop();
    }, 30_000);
  });
});
