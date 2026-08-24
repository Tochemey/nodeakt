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
import type { Actor } from "../../src/actor";
import { ActorSystem } from "../../src/actor.system";
import { discardLogger } from "../../src/discard.logger";
import { ErrDead } from "../../src/errors";
import { Deadletter, Terminated } from "../../src/messages";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";

/** Does nothing; exists to be watched and stopped. */
class Idle implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** Collects every Terminated it is notified with. */
class Watcher implements Actor {
  readonly terminated: string[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Terminated) {
      this.terminated.push(ctx.message.actorPath);
    }
  }

  postStop(): void {}
}

function remoteSystem(name: string): ActorSystem {
  return new ActorSystem(name, {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0 },
  });
}

async function until(label: string, read: () => boolean): Promise<void> {
  for (let i: number = 0; i < 800; i++) {
    if (read()) {
      return;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 5);
    });
  }

  throw new Error(`timed out waiting for ${label}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve): void => {
    setTimeout(resolve, ms);
  });
}

async function withSystems(fn: (a: ActorSystem, b: ActorSystem) => Promise<void>): Promise<void> {
  const a: ActorSystem = remoteSystem("alpha");
  const b: ActorSystem = remoteSystem("beta");
  await a.start();
  await b.start();

  try {
    await fn(a, b);
  } finally {
    await a.stop();
    await b.stop();
  }
}

describe("remote death watch", () => {
  it("delivers Terminated exactly once when the watched actor stops gracefully", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const watcher: Watcher = new Watcher();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      const local: PID = await b.spawn("subject", new Idle());

      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "subject")) as PID;
      watcherPid.watch(remote);
      await sleep(50);

      await local.shutdown();
      await until("the Terminated", (): boolean => watcher.terminated.length >= 1);
      expect(watcher.terminated[0]).toBe(remote.path().toString());

      // The graceful notification settled the watch; the node's later
      // death must not deliver a second one.
      await b.stop();
      await sleep(150);
      expect(watcher.terminated.length).toBe(1);
    });
  });

  it("delivers Terminated when the watched node dies", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const watcher: Watcher = new Watcher();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      await b.spawn("subject", new Idle());

      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "subject")) as PID;
      watcherPid.watch(remote);
      await sleep(50);

      await b.stop();
      await until("the Terminated", (): boolean => watcher.terminated.length >= 1);
      expect(watcher.terminated[0]).toBe(remote.path().toString());
    });
  });

  it("settles only the watches of the node that died", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const c: ActorSystem = remoteSystem("gamma");
      await c.start();

      try {
        const watcher: Watcher = new Watcher();
        const watcherPid: PID = await a.spawn("watcher", watcher);
        await b.spawn("subject", new Idle());
        const survivor: PID = await c.spawn("survivor", new Idle());

        const onB: PID = (await a.remoteLookup(b.host(), b.port(), "subject")) as PID;
        const onC: PID = (await a.remoteLookup(c.host(), c.port(), "survivor")) as PID;
        watcherPid.watch(onB);
        watcherPid.watch(onC);
        await sleep(50);

        await b.stop();
        await until("the Terminated for the dead node", (): boolean => {
          return watcher.terminated.length >= 1;
        });
        expect(watcher.terminated).toEqual([onB.path().toString()]);

        // The survivor's watch is untouched: its own stop still fires.
        await survivor.shutdown();
        await until("the survivor's Terminated", (): boolean => {
          return watcher.terminated.length >= 2;
        });
        expect(watcher.terminated[1]).toBe(onC.path().toString());
      } finally {
        await c.stop();
      }
    });
  });

  it("cancels a watch with unWatch on both sides", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const watcher: Watcher = new Watcher();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      const local: PID = await b.spawn("subject", new Idle());

      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "subject")) as PID;
      watcherPid.watch(remote);
      await sleep(50);
      watcherPid.unWatch(remote);
      await sleep(50);

      await local.shutdown();
      await sleep(200);
      expect(watcher.terminated.length).toBe(0);
    });
  });

  it("answers a watch on a gone actor with an immediate Terminated", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const watcher: Watcher = new Watcher();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      const local: PID = await b.spawn("subject", new Idle());

      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "subject")) as PID;
      await local.shutdown();

      watcherPid.watch(remote);
      await until("the Terminated", (): boolean => watcher.terminated.length >= 1);
      expect(watcher.terminated[0]).toBe(remote.path().toString());
    });
  });

  it("settles a watch that cannot reach its node as a death", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const watcher: Watcher = new Watcher();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      await b.spawn("subject", new Idle());

      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "subject")) as PID;
      await b.stop();
      await sleep(50);

      watcherPid.watch(remote);
      await until("the Terminated", (): boolean => watcher.terminated.length >= 1);
      expect(watcher.terminated[0]).toBe(remote.path().toString());
    });
  });

  it("refuses remote sends from teardown hooks once its node stopped remoting", async () => {
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await a.spawn("sink", new Idle());

      // Remoting closes before actors stop, so a postStop reaching for
      // a remote handle sees a terminal layer: refused, never a fresh
      // connection from a dying node.
      class Farewell implements Actor {
        self: PID | null = null;
        remote: PID | null = null;
        tellOutcome: Error | null = null;
        askOutcome: Promise<unknown> | null = null;
        requestOutcome: Error | null = null;

        preStart(): void {}

        receive(): void {}

        postStop(): void {
          const self: PID = this.self as PID;
          const remote: PID = this.remote as PID;
          this.tellOutcome = self.tell(remote, { bye: true });
          this.askOutcome = self.ask(remote, { bye: true }, 100);
          this.askOutcome.catch((): void => {});
          self
            .request(remote, { bye: true })
            .onReply((_reply: unknown, error: Error | null): void => {
              this.requestOutcome = error;
            });
          self.watch(remote);
          self.unWatch(remote);
        }
      }

      const farewell: Farewell = new Farewell();
      farewell.self = await b.spawn("farewell", farewell);
      farewell.remote = (await b.remoteLookup(a.host(), a.port(), "sink")) as PID;

      await b.stop();
      expect(farewell.tellOutcome).toBe(ErrDead);
      await expect(farewell.askOutcome).rejects.toBe(ErrDead);
      expect(farewell.requestOutcome).toBe(ErrDead);
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("drops a watch canceled before its failure surfaced", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const watcher: Watcher = new Watcher();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      await b.spawn("subject", new Idle());
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "subject")) as PID;
      await b.stop();
      await sleep(50);

      // The unwatch clears the registration synchronously, before the
      // watch's dial failure can surface; the failure then finds
      // nothing to settle.
      watcherPid.watch(remote);
      watcherPid.unWatch(remote);
      await sleep(200);
      expect(watcher.terminated.length).toBe(0);
    });
  });

  it("drops an unwatch whose node is gone, without a dead letter", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const deadletters: Deadletter[] = [];
      a.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      const watcherPid: PID = await a.spawn("watcher", new Watcher());
      await b.spawn("subject", new Idle());
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "subject")) as PID;
      await b.stop();
      await sleep(50);

      watcherPid.unWatch(remote);
      await sleep(200);
      expect(deadletters.length).toBe(0);
    });
  });
});
