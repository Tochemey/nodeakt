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
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { discardLogger } from "../src/discard.logger";
import { ErrActorAlreadyExists } from "../src/errors";
import { PostStart, Terminated } from "../src/messages";
import type { PID } from "../src/pid";
import { Props } from "../src/props";
import type { ReceiveContext } from "../src/receive.context";
import { registerActor } from "../src/registration";
import { setWorkerEntry } from "../src/worker.entry.locator";

const outDir = resolve("node_modules/.cache/nodeakt-system-pool-test");
const entry = resolve(outDir, "worker.entry.mjs");
const registeredModule = new URL("./fixtures/registered.actor.mjs", import.meta.url).href;
const stoppingModule = new URL("./fixtures/stopping.actor.mjs", import.meta.url).href;
const quittingModule = new URL("./fixtures/quitting.actor.mjs", import.meta.url).href;
const replierModule = new URL("./fixtures/replier.actor.mjs", import.meta.url).href;
const facadeModule = new URL("./fixtures/facade.actor.mjs", import.meta.url).href;
const exitingEntry = new URL("./fixtures/exiting.entry.mjs", import.meta.url);

// The classes only need to exist as registration targets; the workers
// construct their own from the fixture modules.
class Registered implements Actor {
  constructor(readonly prefix: string) {}

  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

class Stopper implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

class Quitter implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

class Replier implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

class Facade implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** Records everything it receives. */
class Probe implements Actor {
  readonly seen: unknown[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    if (typeof ctx.message === "object" && ctx.message !== null && "watch" in ctx.message) {
      ctx.watch((ctx.message as { watch: PID }).watch);
      return;
    }

    if (typeof ctx.message === "object" && ctx.message !== null && "unwatch" in ctx.message) {
      ctx.unWatch((ctx.message as { unwatch: PID }).unwatch);
      return;
    }

    if (typeof ctx.message === "object" && ctx.message !== null && "greet" in ctx.message) {
      (ctx.self as PID).tell((ctx.message as { greet: PID }).greet, "greet");
      return;
    }

    this.seen.push(ctx.message);
  }

  postStop(): void {}
}

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
  setWorkerEntry(entry);

  registerActor(Registered, registeredModule);
  registerActor(Stopper, stoppingModule);
  registerActor(Quitter, quittingModule);
  registerActor(Replier, replierModule);
  registerActor(Facade, facadeModule);
}, 120_000);

afterAll(() => {
  setWorkerEntry(null);
});

// Capacity is detected from the environment at system start; every
// stubbed override must die with its test.
afterEach(() => {
  vi.unstubAllEnvs();
});

describe("ActorSystem multi-core", () => {
  it("boots the pool on the first Props spawn and serves the full PID contract", async () => {
    const system = new ActorSystem("auto", { logger: discardLogger });
    await system.start();
    const me = () => system.noSender();

    const a = await system.spawn("a", Props.create(Registered, "one"));
    const b = await system.spawn("b", Props.create(Registered, "two"));

    // With a full pool, placement never lands on the main isolate.
    expect(a.isRunning()).toBe(false);
    await expect(me().ask(a, "hi", 10_000)).resolves.toBe("one:hi");
    await expect(me().ask(b, "hi", 10_000)).resolves.toBe("two:hi");
    expect(me().tell(a, "fire-and-forget")).toBeNull();

    // actorOf resolves across cores and the handle serves identically.
    const found = system.actorOf("a") as PID;
    expect(found).toBeDefined();
    await expect(me().ask(found, "again", 10_000)).resolves.toBe("one:again");

    // One name authority, in every combination.
    await expect(system.spawn("a", Props.create(Registered, "x"))).rejects.toBe(
      ErrActorAlreadyExists,
    );
    await expect(system.spawn("a", new Probe())).rejects.toBe(ErrActorAlreadyExists);
    await system.spawn("central", new Probe());
    await expect(system.spawn("central", Props.create(Registered, "y"))).rejects.toBe(
      ErrActorAlreadyExists,
    );

    await system.stop();
  }, 60_000);

  it("keeps everything local at capacity 1, with no workers at all", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system = new ActorSystem("solo", { logger: discardLogger });
    await system.start();

    const pid = await system.spawn("only", Props.create(Registered, "solo"));

    // Placement fell back to this isolate: the handle is the live PID.
    expect(pid.isRunning()).toBe(true);
    await expect(system.noSender().ask(pid, "here", 10_000)).resolves.toBe("solo:here");

    // The single name authority still holds locally.
    await expect(system.spawn("only", new Probe())).rejects.toBe(ErrActorAlreadyExists);

    await system.stop();
  }, 60_000);

  it("clamps a capacity override below 1 up to 1", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "0");
    const system = new ActorSystem("floored", { logger: discardLogger });
    await system.start();

    const pid = await system.spawn("grounded", Props.create(Registered, "g"));

    expect(pid.isRunning()).toBe(true);
    await expect(system.noSender().ask(pid, "hi", 10_000)).resolves.toBe("g:hi");

    await system.stop();
  }, 60_000);

  it("ignores a capacity override that is not an integer", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "not-a-number");
    const system = new ActorSystem("garbled", { logger: discardLogger });
    await system.start();

    // Detection fell back to the machine: the pool booted and the
    // placement landed off the main isolate.
    const pid = await system.spawn("afloat", Props.create(Registered, "a"));

    expect(pid.isRunning()).toBe(false);
    await expect(system.noSender().ask(pid, "hi", 10_000)).resolves.toBe("a:hi");

    await system.stop();
  }, 60_000);

  it("replies to ctx.sender across cores", async () => {
    const system = new ActorSystem("senders", { logger: discardLogger });
    await system.start();

    const replier = await system.spawn("replier", Props.create(Replier));
    const probe = new Probe();
    const local = await system.spawn("probe", probe);

    // The local actor greets the placed one; the placed one replies to
    // ctx.sender, which routes back across the boundary.
    system.noSender().tell(local, { greet: replier });

    await expect.poll(() => probe.seen, { timeout: 10_000 }).toEqual(["greeting-back"]);
    await system.stop();
  }, 60_000);

  it("delivers Terminated across cores when a watched actor stops", async () => {
    const system = new ActorSystem("watchers", { logger: discardLogger });
    await system.start();
    const me = () => system.noSender();

    const target = await system.spawn("doomed", Props.create(Stopper));
    const watching = new Probe();
    const bystanding = new Probe();
    const watcher = await system.spawn("watcher", watching);
    const bystander = await system.spawn("bystander", bystanding);

    me().tell(watcher, { watch: target });
    me().tell(bystander, { watch: target });
    me().tell(bystander, { unwatch: target });
    await expect(me().ask(target, "ping", 10_000)).resolves.toBe("alive");

    me().tell(target, "die");

    await expect.poll(() => watching.seen.length, { timeout: 10_000 }).toBe(1);
    const notice = watching.seen[0] as Terminated;
    expect(notice).toBeInstanceOf(Terminated);
    expect(notice.actorPath).toBe(target.path().toString());

    // The unwatched bystander heard nothing.
    await new Promise((settle) => setTimeout(settle, 100));
    expect(bystanding.seen).toEqual([]);

    await system.stop();
  }, 60_000);

  it("delivers Terminated when the watched actor's whole isolate dies", async () => {
    const system = new ActorSystem("graveyard", { logger: discardLogger });
    await system.start();
    const me = () => system.noSender();

    const quitter = await system.spawn("quitter", Props.create(Quitter));
    const watching = new Probe();
    const watcher = await system.spawn("watcher", watching);

    me().tell(watcher, { watch: quitter });
    await new Promise((settle) => setTimeout(settle, 200));

    me().tell(quitter, "exit");

    await expect.poll(() => watching.seen.length, { timeout: 10_000 }).toBe(1);
    expect((watching.seen[0] as Terminated).actorPath).toBe(quitter.path().toString());

    await system.stop();
  }, 60_000);

  it("gives worker facades the same authority: spawn claims, actorOf finds", async () => {
    const system = new ActorSystem("facades", { logger: discardLogger });
    await system.start();
    const me = () => system.noSender();

    const facade = await system.spawn("facade", Props.create(Facade));
    const mate = await system.spawn("cellmate", Props.create(Registered, "mate"));
    expect(mate.isRunning()).toBe(false);

    // A worker looks a name up through its facade's actorOf and talks
    // to it directly, wherever it lives; the main isolate is off the
    // data path.
    const found = (await me().ask(facade, "find|cellmate|ping", 15_000)) as {
      ok: boolean;
      reply: unknown;
    };
    expect(found.ok).toBe(true);
    expect(found.reply).toBe("mate:ping");

    // A facade spawn claims its name pool-wide.
    await expect(me().ask(facade, "spawn|claimed", 15_000)).resolves.toBe("spawned");
    await expect(system.spawn("claimed", new Probe())).rejects.toBe(ErrActorAlreadyExists);
    await expect(me().ask(facade, "spawn|claimed", 15_000)).resolves.toContain("refused:");

    // And a facade finds an actor living on the main isolate. The worker learns
    // the just-claimed name through its warm view, which the synchronous
    // actorOf reads, so retry until it resolves "central" rather than replying
    // "missing"; the ask to a silent Probe then times out, ok:false.
    await system.spawn("central", new Probe());
    await expect
      .poll(
        async () =>
          ((await me().ask(facade, "find|central|ignored", 15_000)) as { ok?: boolean }).ok,
        { timeout: 30_000 },
      )
      .toBe(false);

    await system.stop();
  }, 60_000);

  it("refuses spawn options that cannot cross an isolate, allows reentrancy", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system = new ActorSystem("options", { logger: discardLogger });
    await system.start();

    const { BoundedMailbox } = await import("../src/bounded.mailbox");
    await expect(
      system.spawn("boxed", Props.create(Registered, "x"), {
        mailbox: new BoundedMailbox(10),
      }),
    ).rejects.toThrow("cannot cross an isolate");

    const pid = await system.spawn("reentrant", Props.create(Registered, "re"), {
      reentrancy: { mode: "allowAll" },
    });
    await expect(system.noSender().ask(pid, "on", 10_000)).resolves.toBe("re:on");

    await system.stop();
  }, 60_000);

  it("recovers from a failed pool boot on the next Props spawn", async () => {
    setWorkerEntry(exitingEntry);
    const system = new ActorSystem("recovery", { logger: discardLogger });
    await system.start();

    await expect(system.spawn("first", Props.create(Registered, "a"))).rejects.toBeDefined();

    setWorkerEntry(entry);
    const pid = await system.spawn("first", Props.create(Registered, "a"));
    await expect(system.noSender().ask(pid, "back", 10_000)).resolves.toBe("a:back");

    await system.stop();
  }, 60_000);

  it("seeds pre-existing top-level names into the authority when the pool boots", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system = new ActorSystem("seeded", { logger: discardLogger });
    await system.start();
    const early = await system.spawn("early", new Probe());

    // The first Props spawn boots the placement, which claims every
    // name spawned before it existed.
    await system.spawn("boot", Props.create(Registered, "b"));
    await expect(system.spawn("early", Props.create(Registered, "e"))).rejects.toBe(
      ErrActorAlreadyExists,
    );

    // Stopping the early actor releases its seeded claim.
    await early.shutdown();
    const again = await system.spawn("early", Props.create(Registered, "e2"));
    await expect(system.noSender().ask(again, "hi", 10_000)).resolves.toBe("e2:hi");

    await system.stop();
  }, 60_000);

  it("reports no top-level actors before the system starts", () => {
    const cold = new ActorSystem("cold", { logger: discardLogger });

    expect(cold.topLevelActors()).toEqual([]);
  });

  it("frees an instance-claimed name when its preStart fails", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "1");
    const system = new ActorSystem("failing", { logger: discardLogger });
    await system.start();

    // Boot the placement so instance spawns claim names.
    await system.spawn("seed", Props.create(Registered, "s"));

    class Exploding implements Actor {
      preStart(): void {
        throw new Error("boom");
      }

      receive(): void {}

      postStop(): void {}
    }

    await expect(system.spawn("fragile", new Exploding())).rejects.toThrow();
    await expect(system.spawn("fragile", new Probe())).resolves.toBeDefined();

    await system.stop();
  }, 60_000);

  it("merges metrics across worker isolates", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const system = new ActorSystem("metered", {
      logger: discardLogger,
      metrics: { enabled: true },
    });
    await system.start();

    // The Props actor is placed on the worker isolate; asking it proves it
    // is live there and has processed a message its own registry counted.
    const placed = await system.spawn("counter", Props.create(Registered, "w"));
    await expect(system.noSender().ask(placed, "ping", 10_000)).resolves.toBe("w:ping");

    const snapshot = await system.collectMetrics();
    // Main plus the one worker contribute; the worker's actor shows up in
    // the machine-wide counts even though it lives on another isolate.
    expect(snapshot.isolates).toBe(2);
    expect(snapshot.actors.active).toBeGreaterThanOrEqual(1);
    expect(snapshot.actors.startedTotal).toBeGreaterThanOrEqual(1);
    expect(snapshot.messages.processedTotal).toBeGreaterThanOrEqual(1);

    await system.stop();
  }, 60_000);
});
