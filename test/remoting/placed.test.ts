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
import type { Actor } from "../../src/actor";
import { ActorSystem } from "../../src/actor.system";
import { discardLogger } from "../../src/discard.logger";
import { ErrRequestTimeout } from "../../src/errors";
import { PostStart, Terminated } from "../../src/messages";
import type { PID } from "../../src/pid";
import { Props } from "../../src/props";
import type { ReceiveContext } from "../../src/receive.context";
import { registerActor } from "../../src/registration";
import { setWorkerEntry } from "../../src/worker.entry.locator";
import { until } from "./helpers";

/**
 * Remote reach into worker-placed actors: the network and the isolate
 * transports composed. Node A speaks to node B over the wire; B has
 * placed the target on one of its worker isolates, so every delivery
 * crosses two hops (wire in, MessagePort across) and every reply,
 * death notification, and lifecycle order crosses back. The pool is
 * pinned to one worker with a full pool, so placement never lands on
 * B's main isolate and the composition is always exercised.
 */

const outDir = resolve("node_modules/.cache/nodeakt-remoting-placed-test");
const entry = resolve(outDir, "worker.entry.mjs");
const registeredModule = new URL("../fixtures/registered.actor.mjs", import.meta.url).href;
const stoppingModule = new URL("../fixtures/stopping.actor.mjs", import.meta.url).href;
const quittingModule = new URL("../fixtures/quitting.actor.mjs", import.meta.url).href;
const countingModule = new URL("../fixtures/counting.actor.mjs", import.meta.url).href;

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

class Counting implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** Records every Terminated it is notified with. */
class Watcher implements Actor {
  readonly terminated: string[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    if (ctx.message instanceof Terminated) {
      this.terminated.push(ctx.message.actorPath);
    }
  }

  postStop(): void {}
}

/** A remote-enabled system on an ephemeral loopback port. */
function remoteSystem(name: string): ActorSystem {
  return new ActorSystem(name, {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0 },
  });
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
  registerActor(Counting, countingModule);
}, 120_000);

afterAll(() => {
  setWorkerEntry(null);
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("remote reach into worker-placed actors", () => {
  it("delivers tells and asks to a placed actor, in order, replies bridged back", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      const placed: PID = await b.spawn("tally", Props.create(Counting));
      // A full one-worker pool: the placement never lands on main.
      expect(placed.isRunning()).toBe(false);

      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "tally")) as PID;
      expect(pid).toBeDefined();

      // A burst of tells followed by an ask, all in one turn: the
      // coalescer, the wire, and the port compose without reordering a
      // single message, so the fence answers the exact tally.
      for (let i: number = 0; i < 50; i++) {
        expect(a.noSender().tell(pid, `bump-${i}`)).toBeNull();
      }

      const count: unknown = await a.noSender().ask(pid, "count", 10_000);
      expect(count).toBe(50);
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 60_000);

  it("answers a placed ask through a registered class with its own state", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await b.spawn("echo", Props.create(Registered, "placed"));
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      const answer: unknown = await a.noSender().ask(pid, "hello", 10_000);
      expect(answer).toBe("placed:hello");
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 60_000);

  it("reaches a placed actor spawned remotely, incarnation pinned", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      // The remote spawn lands on B's worker and answers the real
      // incarnation, so every send through the returned handle carries
      // a uid pin that must survive the second hop.
      const pid: PID = await a.remoteSpawn(
        b.host(),
        b.port(),
        "pinned",
        Props.create(Registered, "p"),
      );
      expect(pid.path().uid()).not.toBe("");

      await expect(a.noSender().ask(pid, "hello", 10_000)).resolves.toBe("p:hello");
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 60_000);

  it("bridges a placed ask that never answers back as the timeout", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      // The Quitter ignores everything but "exit": the composed ask
      // expires on the deadline and the failure settles the wire
      // correlation, sentinel identity preserved.
      await b.spawn("mute", Props.create(Quitter));
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "mute")) as PID;

      await expect(a.noSender().ask(pid, "ping", 400)).rejects.toBe(ErrRequestTimeout);
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 60_000);

  it("cancels a watch on a placed actor across both hops", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await b.spawn("quiet", Props.create(Stopper));
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "quiet")) as PID;

      const watching: Watcher = new Watcher();
      const watcher: PID = await a.spawn("watcher", watching);
      watcher.watch(pid);
      await new Promise<void>((settle): void => {
        setTimeout(settle, 200);
      });

      watcher.unWatch(pid);
      await new Promise<void>((settle): void => {
        setTimeout(settle, 200);
      });

      // The registration is gone on the owning worker: the stop
      // notifies nobody.
      a.noSender().tell(pid, "die");
      await until("the name to free", (): boolean => b.actorOf("quiet") === undefined);
      await new Promise<void>((settle): void => {
        setTimeout(settle, 200);
      });
      expect(watching.terminated.length).toBe(0);
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 60_000);

  it("delivers Terminated across both hops when the placed actor stops itself", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await b.spawn("doomed", Props.create(Stopper));
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "doomed")) as PID;

      const watching: Watcher = new Watcher();
      const watcher: PID = await a.spawn("watcher", watching);
      watcher.watch(pid);
      await new Promise<void>((settle): void => {
        setTimeout(settle, 200);
      });

      // The Stopper stops itself: its Terminated travels worker to
      // main isolate first, then over the wire through the watcher's
      // handle, and the name frees across the pool.
      a.noSender().tell(pid, "die");
      await until("the Terminated", (): boolean => watching.terminated.length >= 1);
      expect(watching.terminated[0]).toBe(pid.path().toString());

      await until("the name to free", (): boolean => b.actorOf("doomed") === undefined);
      expect(await a.remoteLookup(b.host(), b.port(), "doomed")).toBeUndefined();
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 60_000);

  it("delivers Terminated across both hops when the whole worker isolate dies", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await b.spawn("quitter", Props.create(Quitter));
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "quitter")) as PID;

      const watching: Watcher = new Watcher();
      const watcher: PID = await a.spawn("watcher", watching);
      watcher.watch(pid);
      await new Promise<void>((settle): void => {
        setTimeout(settle, 200);
      });

      // The isolate dies out from under the actor: the mesh transport's
      // close settles the registration on B's main isolate, and the
      // notification still crosses the wire.
      a.noSender().tell(pid, "exit");
      await until("the Terminated", (): boolean => watching.terminated.length >= 1);
      expect(watching.terminated[0]).toBe(pid.path().toString());
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 60_000);

  it("stops a placed actor remotely, idempotently, through the pool's control plane", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await b.spawn("target", Props.create(Registered, "x"));
      expect((b.actorOf("target") as PID).isRouted()).toBe(true);

      await a.remoteStop(b.host(), b.port(), "target");
      await until("the name to free", (): boolean => b.actorOf("target") === undefined);

      // Already stopped: the second order succeeds without an actor.
      await expect(a.remoteStop(b.host(), b.port(), "target")).resolves.toBeUndefined();
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 60_000);

  it("respawns a placed actor remotely: same name, fresh state, still serving", async () => {
    vi.stubEnv("NODEAKT_PARALLELISM", "2");
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await b.spawn("tally", Props.create(Counting));
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "tally")) as PID;

      for (let i: number = 0; i < 5; i++) {
        a.noSender().tell(pid, `bump-${i}`);
      }

      await expect(a.noSender().ask(pid, "count", 10_000)).resolves.toBe(5);

      // The restart runs in place on the owning worker: same name,
      // lifecycle hooks rerun, so the tally begins a fresh life and
      // the actor keeps serving.
      const respawned: PID = await a.remoteReSpawn(b.host(), b.port(), "tally");
      expect(respawned.path().name()).toBe("tally");
      await expect(a.noSender().ask(pid, "count", 10_000)).resolves.toBe(0);

      a.noSender().tell(pid, "bump");
      await until("the fresh tally", (): boolean => true);
      await expect(a.noSender().ask(pid, "count", 10_000)).resolves.toBe(1);

      // A name nothing holds anywhere answers the not-found failure,
      // reconstructed by name on this side of the wire.
      await expect(a.remoteReSpawn(b.host(), b.port(), "nobody")).rejects.toSatisfy(
        (err: unknown): boolean => err instanceof Error && err.name === "ActorNotFoundError",
      );
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 60_000);
});
