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

import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { Actor } from "../../src/actor";
import type { IsolateRoute } from "../../src/actor.ref";
import { ActorSystem } from "../../src/actor.system";
import { BoundedMailbox } from "../../src/bounded.mailbox";
import { discardLogger } from "../../src/discard.logger";
import {
  ActorNotRegisteredError,
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrDead,
  ErrRemotingDisabled,
} from "../../src/errors";
import { Terminated } from "../../src/messages";
import { newPath } from "../../src/path";
import type { PID } from "../../src/pid";
import { Props } from "../../src/props";
import type { ReceiveContext } from "../../src/receive.context";
import { completedRequest } from "../../src/reentrancy";
import { registerActor } from "../../src/registration";
import { routedPid } from "../../src/routed.pid";
import { Phoenix } from "../fixtures/phoenix.actor.mjs";
import { Registered } from "../fixtures/registered.actor.mjs";
import { Relay } from "../fixtures/relay.actor.mjs";
import { RemoteCounter } from "../fixtures/remote.counter.actor.mjs";
import { remoteSystem, until, withSystems } from "./helpers";

const registeredModule: string = new URL("../fixtures/registered.actor.mjs", import.meta.url).href;
const counterModule: string = new URL("../fixtures/remote.counter.actor.mjs", import.meta.url).href;
const phoenixModule: string = new URL("../fixtures/phoenix.actor.mjs", import.meta.url).href;
const relayModule: string = new URL("../fixtures/relay.actor.mjs", import.meta.url).href;

class Stray implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

const TwinA = class Twin implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
};

const TwinB = class Twin implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
};

beforeAll((): void => {
  // A remote spawn is an ordinary spawn on the receiving node, so it
  // goes through placement; a single-core capacity keeps it on that
  // node's main isolate, since worker threads cannot boot from source
  // under the test runner.
  vi.stubEnv("NODEAKT_PARALLELISM", "1");

  registerActor(Registered, registeredModule);
  registerActor(RemoteCounter, counterModule);
  registerActor(Phoenix, phoenixModule);
  registerActor(Relay, relayModule);
  registerActor(TwinA, "file:///twin-a.actor.ts");
  registerActor(TwinB, "file:///twin-b.actor.ts");
});

afterAll((): void => {
  vi.unstubAllEnvs();
});

describe("remoteSpawn", () => {
  it("spawns on the remote node and messages it", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const pid: PID = await a.remoteSpawn(
        b.host(),
        b.port(),
        "greeter",
        Props.create(Registered, "fr"),
      );

      expect(pid.path().port()).toBe(b.port());
      expect(pid.path().name()).toBe("greeter");
      expect(b.actorOf("greeter")).toBeDefined();

      const answer: unknown = await a.noSender().ask(pid, "monde", 2000);
      expect(answer).toBe("fr:monde");
    });
  });

  it("carries the one data spawn option", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const pid: PID = await a.remoteSpawn(
        b.host(),
        b.port(),
        "reentrant",
        Props.create(Registered, "de"),
        { reentrancy: { mode: "allowAll" } },
      );
      expect(pid.path().name()).toBe("reentrant");
    });
  });

  it("rejects a held name with the identical sentinel", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await b.spawn("greeter", new Registered("en"));
      const rejection: Promise<PID> = a.remoteSpawn(
        b.host(),
        b.port(),
        "greeter",
        Props.create(Registered, "fr"),
      );
      await expect(rejection).rejects.toBe(ErrActorAlreadyExists);
    });
  });

  it("refuses an unregistered class on the sending side", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const rejection: Promise<PID> = a.remoteSpawn(
        b.host(),
        b.port(),
        "stray",
        Props.create(Stray),
      );
      await expect(rejection).rejects.toBeInstanceOf(ActorNotRegisteredError);
    });
  });

  it("refuses a name two registered classes share", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const rejection: Promise<PID> = a.remoteSpawn(
        b.host(),
        b.port(),
        "twin",
        Props.create(TwinA),
      );
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof Error && err.name === "ActorNotRegisteredError";
      });
    });
  });

  it("refuses live spawn options", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const rejection: Promise<PID> = a.remoteSpawn(
        b.host(),
        b.port(),
        "boxed",
        Props.create(Registered, "fr"),
        { mailbox: new BoundedMailbox(1) },
      );
      await expect(rejection).rejects.toBeInstanceOf(TypeError);
    });
  });
});

describe("remoteReSpawn", () => {
  it("restarts the actor in place, keeping its incarnation", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const pid: PID = await a.remoteSpawn(
        b.host(),
        b.port(),
        "counter",
        Props.create(RemoteCounter),
      );

      a.noSender().tell(pid, "bump");
      a.noSender().tell(pid, "bump");
      a.noSender().tell(pid, "bump");
      expect(await a.noSender().ask(pid, "count", 2000)).toBe(3);

      const respawned: PID = await a.remoteReSpawn(b.host(), b.port(), "counter");
      expect(respawned.path().uid()).toBe(pid.path().uid());

      // The old handle still addresses the same incarnation, and the
      // restart reset the actor's state through preStart.
      expect(await a.noSender().ask(pid, "count", 2000)).toBe(0);
    });
  });

  it("rejects an unknown name", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const rejection: Promise<PID> = a.remoteReSpawn(b.host(), b.port(), "nobody");
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof Error && err.name === "ActorNotFoundError";
      });
    });
  });

  it("settles with the restart failure", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await a.remoteSpawn(b.host(), b.port(), "phoenix", Props.create(Phoenix));
      const rejection: Promise<PID> = a.remoteReSpawn(b.host(), b.port(), "phoenix");
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        if (!(err instanceof Error) || err.name !== "ActorInitializationError") {
          return false;
        }

        return err.message.includes("failed to initialize");
      });
    });
  });
});

describe("remoteStop", () => {
  it("stops the actor, notifies watchers, and is idempotent", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const terminated: string[] = [];
      class Watcher implements Actor {
        preStart(): void {}

        receive(ctx: ReceiveContext): void {
          if (ctx.message instanceof Terminated) {
            terminated.push(ctx.message.actorPath);
          }
        }

        postStop(): void {}
      }

      const watcherPid: PID = await a.spawn("watcher", new Watcher());
      const pid: PID = await a.remoteSpawn(
        b.host(),
        b.port(),
        "doomed",
        Props.create(RemoteCounter),
      );
      watcherPid.watch(pid);
      await new Promise<void>((resolve): void => {
        setTimeout(resolve, 50);
      });

      await a.remoteStop(b.host(), b.port(), "doomed");
      expect(b.actorOf("doomed")).toBeUndefined();
      await until("the Terminated", (): boolean => terminated.length >= 1);
      expect(terminated[0]).toBe(pid.path().toString());

      await expect(a.remoteStop(b.host(), b.port(), "doomed")).resolves.toBeUndefined();
    });
  });

  it("settles a stop of an actor another isolate owns with its refusal", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      // A placement stub standing in for a booted pool: the name
      // resolves to a handle owned by another isolate, whose shutdown
      // through a handle is refused.
      const route: IsolateRoute = {
        workerId: 9,
        tell: (): Error | null => null,
        ask: (): Promise<unknown> => Promise.reject(ErrDead),
        request: () => completedRequest(ErrDead),
        watch: (): void => {},
        unwatch: (): void => {},
      };
      b.attachPlacement({
        claim: (): Promise<Error | null> => Promise.resolve(null),
        free: (): void => {},
        place: (): Promise<PID> => Promise.reject(ErrDead),
        find: (name: string): PID | undefined => {
          if (name !== "placed") {
            return undefined;
          }

          return routedPid(b, newPath("placed", "beta", b.host(), b.port(), undefined, ""), route);
        },
        stop: (): Promise<void> => Promise.resolve(),
      });

      const rejection: Promise<void> = a.remoteStop(b.host(), b.port(), "placed");
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof Error && err.message.includes("another isolate");
      });

      // A respawn meets the same honest refusal: the actor lives, but
      // lifecycle control cannot reach its isolate yet.
      const respawn: Promise<PID> = a.remoteReSpawn(b.host(), b.port(), "placed");
      await expect(respawn).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof Error && err.message.includes("another isolate");
      });
    });
  });
});

describe("remote control from a receive context", () => {
  it("mirrors lookup, respawn, and stop", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await a.remoteSpawn(b.host(), b.port(), "counter", Props.create(RemoteCounter));

      const seen: string[] = [];
      class Driver implements Actor {
        preStart(): void {}

        async receive(ctx: ReceiveContext): Promise<void> {
          if (ctx.message !== "go") {
            return;
          }

          const found: PID | undefined = await ctx.remoteLookup(b.host(), b.port(), "counter");
          seen.push(found === undefined ? "missing" : found.path().name());

          const respawned: PID = await ctx.remoteReSpawn(b.host(), b.port(), "counter");
          seen.push(respawned.path().name());

          await ctx.remoteStop(b.host(), b.port(), "counter");
          seen.push("stopped");
        }

        postStop(): void {}
      }

      const driver: PID = await a.spawn("driver", new Driver());
      a.noSender().tell(driver, "go");

      await until("all three outcomes", (): boolean => seen.length >= 3);
      expect(seen).toEqual(["counter", "counter", "stopped"]);
      expect(b.actorOf("counter")).toBeUndefined();
    });
  });
});

describe("remote control guards", () => {
  it("rejects on a system without remoting", async () => {
    const system: ActorSystem = new ActorSystem("plain", { logger: discardLogger });
    await system.start();

    try {
      await expect(system.remoteSpawn("127.0.0.1", 1, "x", Props.create(Stray))).rejects.toBe(
        ErrRemotingDisabled,
      );
      await expect(system.remoteReSpawn("127.0.0.1", 1, "x")).rejects.toBe(ErrRemotingDisabled);
      await expect(system.remoteStop("127.0.0.1", 1, "x")).rejects.toBe(ErrRemotingDisabled);
    } finally {
      await system.stop();
    }
  });

  it("rejects on a system that is not running", async () => {
    const system: ActorSystem = remoteSystem("cold");
    await expect(system.remoteSpawn("127.0.0.1", 1, "x", Props.create(Stray))).rejects.toBe(
      ErrActorSystemNotStarted,
    );
    await expect(system.remoteReSpawn("127.0.0.1", 1, "x")).rejects.toBe(ErrActorSystemNotStarted);
    await expect(system.remoteStop("127.0.0.1", 1, "x")).rejects.toBe(ErrActorSystemNotStarted);
  });
});

describe("remote spawn arguments and options", () => {
  it("refuses a constructor argument that cannot cross", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const rejection: Promise<PID> = a.remoteSpawn(
        b.host(),
        b.port(),
        "boxed-arg",
        Props.create(Registered, ((): string => "fr") as unknown as string),
      );
      await expect(rejection).rejects.toBeInstanceOf(TypeError);
    });
  });

  it("carries the reentrancy option into a working request", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const reentrant: PID = await a.remoteSpawn(b.host(), b.port(), "relay", Props.create(Relay), {
        reentrancy: { mode: "allowAll" },
      });
      expect(await a.noSender().ask(reentrant, "go", 2000)).toBe("ok:pong");

      const plain: PID = await a.remoteSpawn(
        b.host(),
        b.port(),
        "relay-plain",
        Props.create(Relay),
      );
      const refused: unknown = await a.noSender().ask(plain, "go", 2000);
      expect(String(refused).startsWith("refused:")).toBe(true);
    });
  });
});
