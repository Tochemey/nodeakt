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

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import {
  ErrActorSystemNotStarted,
  ErrDead,
  ErrFanOutAsk,
  ErrInvalidPoolSize,
  ErrInvalidRouteeDirective,
  ErrInvalidRoutingStrategy,
  ErrRoutingKeyRequired,
} from "../src/errors";
import { Deadletter, PostStart, Terminated } from "../src/messages";
import type { PID } from "../src/pid";
import { Props } from "../src/props";
import {
  createAskContext,
  createReceiveContext,
  type ReceiveContext,
  rejectAsk,
} from "../src/receive.context";
import { AdjustRouterPoolSize, GetRoutees, type Routees } from "../src/router.messages";
import type { RouterOptions, RoutingStrategy } from "../src/router.options";

const TIMEOUT = 1000;

class Job {
  constructor(readonly key: string) {}
}

class Echoed {
  constructor(
    readonly routee: string,
    readonly sender: string,
    readonly key: string,
  ) {}
}

/**
 * The routee used across the suite. A `Job` is answered on the ask
 * channel and echoed back to the sender as a message, carrying the
 * routee's own name and the sender path it observed; the key "boom"
 * fails instead, engaging the router's routee directive.
 */
class Worker implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const msg: unknown = ctx.message;

    if (msg instanceof Job) {
      if (msg.key === "boom") {
        throw new Error("boom");
      }

      const self: PID = ctx.self as PID;
      const sender: PID = ctx.sender as PID;
      const echo = new Echoed(self.name(), sender.id(), msg.key);
      ctx.response(echo);
      ctx.tell(sender, echo);
    }
  }

  postStop(): void {}
}

/** Collects every business message it receives, for observing routed
 * tells. */
class Probe implements Actor {
  readonly received: unknown[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.received.push(ctx.message);
  }

  postStop(): void {}
}

let system: ActorSystem;

beforeEach(async () => {
  system = new ActorSystem("sys");
  await system.start();
});

afterEach(async () => {
  await system.stop();
});

async function spawnPool(poolSize: number, options?: RouterOptions): Promise<PID> {
  return system.spawnRouter("workers", poolSize, Props.create(Worker), options);
}

async function livePaths(router: PID): Promise<readonly string[]> {
  const routees = (await system.noSender().ask(router, new GetRoutees(), TIMEOUT)) as Routees;
  return routees.paths;
}

describe("spawnRouter", () => {
  it("spawns a named router whose routees are its children", async () => {
    const router: PID = await spawnPool(3);

    expect(system.actorOf("workers")).toBe(router);

    const paths: readonly string[] = await livePaths(router);
    expect(paths).toHaveLength(3);

    for (const path of paths) {
      expect(path.startsWith(`${router.id()}/routee-`)).toBe(true);
    }

    expect(router.children()).toHaveLength(3);
  });

  it("rejects an invalid pool size", async () => {
    await expect(spawnPool(0)).rejects.toBe(ErrInvalidPoolSize);
    await expect(spawnPool(-2)).rejects.toBe(ErrInvalidPoolSize);
    await expect(spawnPool(1.5)).rejects.toBe(ErrInvalidPoolSize);
  });

  it("rejects an unknown routing strategy", async () => {
    await expect(spawnPool(2, { strategy: "bogus" as RoutingStrategy })).rejects.toBe(
      ErrInvalidRoutingStrategy,
    );
  });

  it("rejects consistent hashing without a routing key extractor", async () => {
    await expect(spawnPool(2, { strategy: "consistentHash" })).rejects.toBe(ErrRoutingKeyRequired);
  });

  it("rejects an unknown routee directive", async () => {
    await expect(spawnPool(2, { directive: "escalate" as unknown as "stop" })).rejects.toBe(
      ErrInvalidRouteeDirective,
    );
  });

  it("rejects routees that are not Props", async () => {
    await expect(
      system.spawnRouter("workers", 2, new Worker() as unknown as Props),
    ).rejects.toThrow(TypeError);
  });

  it("rejects when the system is not started", async () => {
    const other = new ActorSystem("other");

    await expect(other.spawnRouter("workers", 2, Props.create(Worker))).rejects.toBe(
      ErrActorSystemNotStarted,
    );
  });
});

describe("round robin routing", () => {
  it("spreads a stream evenly over the pool", async () => {
    const router: PID = await spawnPool(3);

    const replies: Echoed[] = [];
    for (let i = 0; i < 6; i++) {
      replies.push((await system.noSender().ask(router, new Job(`k${i}`), TIMEOUT)) as Echoed);
    }

    const counts = new Map<string, number>();
    for (const reply of replies) {
      counts.set(reply.routee, (counts.get(reply.routee) ?? 0) + 1);
    }

    expect(counts.size).toBe(3);
    for (const count of counts.values()) {
      expect(count).toBe(2);
    }
  });

  it("preserves the original sender on a routed tell", async () => {
    const router: PID = await spawnPool(2);
    const probe = new Probe();
    const probePid: PID = await system.spawn("probe", probe);

    probePid.tell(router, new Job("a"));

    await vi.waitFor(() => {
      expect(probe.received).toHaveLength(1);
    });

    const echo: Echoed = probe.received[0] as Echoed;
    expect(echo.sender).toBe(probePid.id());
    expect(echo.key).toBe("a");
  });
});

describe("random routing", () => {
  it("routes every message to a live routee", async () => {
    const router: PID = await spawnPool(3, { strategy: "random" });
    const members: readonly string[] = await livePaths(router);

    for (let i = 0; i < 12; i++) {
      const reply = (await system.noSender().ask(router, new Job(`k${i}`), TIMEOUT)) as Echoed;
      const path: string | undefined = members.find((member) => member.endsWith(reply.routee));
      expect(path).toBeDefined();
    }
  });
});

describe("fan-out routing", () => {
  it("delivers one tell to every routee", async () => {
    const router: PID = await spawnPool(3, { strategy: "fanOut" });
    const probe = new Probe();
    const probePid: PID = await system.spawn("probe", probe);

    probePid.tell(router, new Job("x"));

    await vi.waitFor(() => {
      expect(probe.received).toHaveLength(3);
    });

    const routees = new Set((probe.received as Echoed[]).map((echo) => echo.routee));
    expect(routees.size).toBe(3);

    for (const echo of probe.received as Echoed[]) {
      expect(echo.sender).toBe(probePid.id());
    }
  });

  it("rejects an ask with a typed error", async () => {
    const router: PID = await spawnPool(3, { strategy: "fanOut" });

    await expect(system.noSender().ask(router, new Job("x"), TIMEOUT)).rejects.toBe(ErrFanOutAsk);
  });
});

describe("consistent hash routing", () => {
  const options: RouterOptions = {
    strategy: "consistentHash",
    routingKey: (message) => (message as Job).key,
  };

  it("pins equal keys to the same routee", async () => {
    const router: PID = await spawnPool(4, options);

    for (const key of ["alpha", "beta", "gamma"]) {
      const owners = new Set<string>();

      for (let i = 0; i < 5; i++) {
        const reply = (await system.noSender().ask(router, new Job(key), TIMEOUT)) as Echoed;
        owners.add(reply.routee);
      }

      expect(owners.size).toBe(1);
    }
  });

  it("dead-letters a message whose key extraction fails", async () => {
    const router: PID = await spawnPool(2, {
      strategy: "consistentHash",
      routingKey: (message) => {
        // A non-Error throw is wrapped into one on the dead-letter path.
        if ((message as Job).key === "plain") {
          throw "no key";
        }

        throw new Error("no key");
      },
    });

    const deadletters: Deadletter[] = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        deadletters.push(event);
      }
    });

    system.noSender().tell(router, new Job("a"));
    system.noSender().tell(router, new Job("plain"));

    await vi.waitFor(() => {
      expect(deadletters).toHaveLength(2);
    });

    for (const deadletter of deadletters) {
      expect(deadletter.receiver).toBe(router.id());
      expect(deadletter.reason).toBe("no key");
    }
  });
});

describe("router management", () => {
  it("grows the pool in place", async () => {
    const router: PID = await spawnPool(2);

    const routees = (await system
      .noSender()
      .ask(router, new AdjustRouterPoolSize(5), TIMEOUT)) as Routees;
    expect(routees.paths).toHaveLength(5);

    await expect(livePaths(router)).resolves.toHaveLength(5);
  });

  it("shrinks the pool in place", async () => {
    const router: PID = await spawnPool(4);

    const routees = (await system
      .noSender()
      .ask(router, new AdjustRouterPoolSize(1), TIMEOUT)) as Routees;
    expect(routees.paths).toHaveLength(1);

    await vi.waitFor(() => {
      expect(router.children()).toHaveLength(1);
    });
  });

  it("leaves the pool unchanged when adjusted to its current size", async () => {
    const router: PID = await spawnPool(2);
    const before: readonly string[] = await livePaths(router);

    const routees = (await system
      .noSender()
      .ask(router, new AdjustRouterPoolSize(2), TIMEOUT)) as Routees;
    expect(routees.paths).toEqual(before);
  });

  it("refuses an invalid pool size", async () => {
    const router: PID = await spawnPool(2);

    await expect(system.noSender().ask(router, new AdjustRouterPoolSize(-1), TIMEOUT)).rejects.toBe(
      ErrInvalidPoolSize,
    );
    await expect(
      system.noSender().ask(router, new AdjustRouterPoolSize(1.5), TIMEOUT),
    ).rejects.toBe(ErrInvalidPoolSize);

    await expect(livePaths(router)).resolves.toHaveLength(2);
  });

  it("dead-letters sends once the pool reaches zero and grows back on demand", async () => {
    const router: PID = await spawnPool(2);
    await system.noSender().ask(router, new AdjustRouterPoolSize(0), TIMEOUT);

    const deadletters: Deadletter[] = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        deadletters.push(event);
      }
    });

    system.noSender().tell(router, new Job("a"));

    await vi.waitFor(() => {
      expect(deadletters).toHaveLength(1);
    });

    expect(deadletters[0]?.receiver).toBe(router.id());
    await expect(system.noSender().ask(router, new Job("b"), TIMEOUT)).rejects.toBe(ErrDead);

    await system.noSender().ask(router, new AdjustRouterPoolSize(2), TIMEOUT);
    const reply = (await system.noSender().ask(router, new Job("c"), TIMEOUT)) as Echoed;
    expect(reply.key).toBe("c");
  });
});

describe("routee supervision", () => {
  it("stops a failing routee by default, shrinking the rotation", async () => {
    const router: PID = await spawnPool(3);

    system.noSender().tell(router, new Job("boom"));

    await vi.waitFor(async () => {
      await expect(livePaths(router)).resolves.toHaveLength(2);
    });

    const survivors = new Set<string>();
    for (let i = 0; i < 4; i++) {
      const reply = (await system.noSender().ask(router, new Job(`k${i}`), TIMEOUT)) as Echoed;
      survivors.add(reply.routee);
    }

    expect(survivors.size).toBe(2);
  });

  it("restarts a failing routee in place with the restart directive", async () => {
    const router: PID = await spawnPool(1, { directive: "restart" });
    await expect(livePaths(router)).resolves.toHaveLength(1);

    const routee: PID = router.children()[0] as PID;

    system.noSender().tell(router, new Job("boom"));

    await vi.waitFor(() => {
      expect(routee.restartCount()).toBe(1);
    });

    await expect(livePaths(router)).resolves.toHaveLength(1);
    const reply = (await system.noSender().ask(router, new Job("ok"), TIMEOUT)) as Echoed;
    expect(reply.key).toBe("ok");
  });

  it("resumes a failing routee with the resume directive", async () => {
    const router: PID = await spawnPool(1, { directive: "resume" });
    await expect(livePaths(router)).resolves.toHaveLength(1);

    const routee: PID = router.children()[0] as PID;

    system.noSender().tell(router, new Job("boom"));

    const reply = (await system.noSender().ask(router, new Job("ok"), TIMEOUT)) as Echoed;
    expect(reply.key).toBe("ok");
    expect(routee.restartCount()).toBe(0);
  });
});

describe("empty rotation", () => {
  function collectDeadletters(): Deadletter[] {
    const seen: Deadletter[] = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        seen.push(event);
      }
    });

    return seen;
  }

  it("dead-letters a round-robin send while the last routee is still stopping", async () => {
    const router: PID = await spawnPool(1);
    await expect(livePaths(router)).resolves.toHaveLength(1);

    const routee: PID = router.children()[0] as PID;
    const seen: Deadletter[] = collectDeadletters();

    // The routee starts stopping but its Terminated has not reached the
    // router yet: the rotation still lists it, and both sends below are
    // queued ahead of the notification.
    void routee.shutdown();
    system.noSender().tell(router, new Job("late"));
    const pending: Promise<readonly string[]> = livePaths(router);

    await vi.waitFor(() => {
      expect(seen).toHaveLength(1);
    });

    expect(seen[0]?.receiver).toBe(router.id());
    expect(seen[0]?.reason).toBe(ErrDead.message);
    await expect(pending).resolves.toHaveLength(0);
  });

  it("dead-letters random sends with a stopping routee and with none left", async () => {
    const router: PID = await spawnPool(1, { strategy: "random" });
    await expect(livePaths(router)).resolves.toHaveLength(1);

    const routee: PID = router.children()[0] as PID;
    const seen: Deadletter[] = collectDeadletters();

    void routee.shutdown();
    system.noSender().tell(router, new Job("late"));

    await vi.waitFor(async () => {
      await expect(livePaths(router)).resolves.toHaveLength(0);
    });

    system.noSender().tell(router, new Job("later"));

    await vi.waitFor(() => {
      expect(seen).toHaveLength(2);
    });
  });

  it("dead-letters fan-out sends with a stopping routee and with none left", async () => {
    const router: PID = await spawnPool(1, { strategy: "fanOut" });
    await expect(livePaths(router)).resolves.toHaveLength(1);

    const routee: PID = router.children()[0] as PID;
    const seen: Deadletter[] = collectDeadletters();

    void routee.shutdown();
    system.noSender().tell(router, new Job("late"));

    await vi.waitFor(async () => {
      await expect(livePaths(router)).resolves.toHaveLength(0);
    });

    system.noSender().tell(router, new Job("later"));

    await vi.waitFor(() => {
      expect(seen).toHaveLength(2);
    });
  });

  it("dead-letters consistent-hash sends with a stopping routee and with none left", async () => {
    const router: PID = await spawnPool(1, {
      strategy: "consistentHash",
      routingKey: (message) => (message as Job).key,
    });
    await expect(livePaths(router)).resolves.toHaveLength(1);

    const routee: PID = router.children()[0] as PID;
    const seen: Deadletter[] = collectDeadletters();

    void routee.shutdown();
    system.noSender().tell(router, new Job("late"));

    await vi.waitFor(async () => {
      await expect(livePaths(router)).resolves.toHaveLength(0);
    });

    system.noSender().tell(router, new Job("later"));

    await vi.waitFor(() => {
      expect(seen).toHaveLength(2);
    });
  });
});

describe("management forgery", () => {
  it("ignores a forged PostStart instead of growing the pool again", async () => {
    const router: PID = await spawnPool(2);
    await expect(livePaths(router)).resolves.toHaveLength(2);

    system.noSender().tell(router, new PostStart());

    await expect(livePaths(router)).resolves.toHaveLength(2);
  });

  it("keeps a running routee named by a forged Terminated", async () => {
    const router: PID = await spawnPool(2);
    const paths: readonly string[] = await livePaths(router);

    system.noSender().tell(router, new Terminated(paths[0] as string));
    system.noSender().tell(router, new Terminated("nodeakt://sys@127.0.0.1:0/nobody"));

    await expect(livePaths(router)).resolves.toHaveLength(2);
  });
});

describe("reply channel rejection", () => {
  it("reports whether a delivery carried a live reply channel", async () => {
    const self: PID = system.noSender();

    expect(rejectAsk(createReceiveContext(new Job("a"), self, self), ErrDead)).toBe(false);

    const { ctx, reply } = createAskContext(new Job("a"), self, self, TIMEOUT);
    expect(rejectAsk(ctx, ErrFanOutAsk)).toBe(true);
    await expect(reply).rejects.toBe(ErrFanOutAsk);

    expect(rejectAsk(ctx, ErrDead)).toBe(false);
  });
});

describe("router lifecycle", () => {
  it("stops the whole pool when the router stops", async () => {
    const router: PID = await spawnPool(3);

    // The initial grow runs on the router's own turn; the answered ask
    // proves it is done before the children are captured.
    await expect(livePaths(router)).resolves.toHaveLength(3);

    const routees: PID[] = router.children();
    expect(routees).toHaveLength(3);

    await router.shutdown();

    expect(router.isRunning()).toBe(false);
    for (const routee of routees) {
      expect(routee.isRunning()).toBe(false);
    }

    expect(system.actorOf("workers")).toBeUndefined();
  });
});
