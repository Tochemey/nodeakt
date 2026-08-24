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
import { BoundedMailbox } from "../../src/bounded.mailbox";
import { discardLogger } from "../../src/discard.logger";
import {
  ErrActorSystemNotStarted,
  ErrDead,
  ErrInvalidTimeout,
  ErrMailboxFull,
  ErrReentrancyDisabled,
  ErrRemotingDisabled,
  ErrRequestTimeout,
  TypeNotRegisteredError,
} from "../../src/errors";
import { Deadletter, PostStart } from "../../src/messages";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { registerMessage } from "../../src/registration";

class Ping {
  constructor(readonly n: number) {}
}

class Pong {
  constructor(readonly n: number) {}
}

class Ask {
  constructor(readonly n: number) {}
}

class Answer {
  constructor(readonly n: number) {}
}

class BadReply {}

class UnregisteredMessage {
  constructor(readonly n: number) {}
}

class UnregisteredReply {}

registerMessage(Ping);
registerMessage(Pong);
registerMessage(Ask);
registerMessage(Answer);
registerMessage(BadReply);

/** Records every delivery with its sender for later assertions. */
class Collector implements Actor {
  readonly received: { message: unknown; sender: PID }[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.received.push({ message: ctx.message, sender: ctx.sender as PID });
  }

  postStop(): void {}
}

/** Answers asks and pings; a BadReply ask answers with an instance the
 * wire refuses, proving the failure settles the ask. */
class Echo implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message: unknown = ctx.message;
    if (message instanceof Ask) {
      ctx.response(new Answer(message.n + 1));
      return;
    }

    if (message instanceof Ping) {
      ctx.tell(ctx.sender as PID, new Pong(message.n));
      return;
    }

    if (message instanceof BadReply) {
      ctx.response(new UnregisteredReply());
      return;
    }

    if (typeof message === "object" && message !== null && "plain" in message) {
      ctx.response({ echoed: message });
    }
  }

  postStop(): void {}
}

/** Parks on every message until released, so later sends pile into the
 * mailbox and asks never get answered; releasing lets the mailbox
 * drain, which a graceful system stop waits for. */
class Gate implements Actor {
  private open: boolean = false;
  private readonly parked: (() => void)[] = [];

  preStart(): void {}

  receive(): Promise<void> | undefined {
    if (this.open) {
      return undefined;
    }

    return new Promise<void>((resolve): void => {
      this.parked.push(resolve);
    });
  }

  release(): void {
    this.open = true;
    for (const resolve of this.parked) {
      resolve();
    }

    this.parked.length = 0;
  }

  postStop(): void {}
}

/** Issues one request to a remote target and records the outcome. */
class Requester implements Actor {
  readonly outcomes: { reply: unknown; error: Error | null }[] = [];

  constructor(
    private readonly target: () => PID,
    private readonly message: () => unknown,
    private readonly timeout: number,
  ) {}

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message === "go") {
      ctx
        .request(
          this.target(),
          this.message(),
          this.timeout > 0 ? { timeout: this.timeout } : undefined,
        )
        .onReply((reply: unknown, error: Error | null): void => {
          this.outcomes.push({ reply, error });
        });
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

describe("remote lookup and tell", () => {
  it("resolves a remote actor and tells it, prototype intact", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      const local: PID = await b.spawn("greeter", collector);

      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "greeter")) as PID;
      expect(pid).toBeDefined();
      expect(pid.path().toString()).toBe(local.path().toString());
      expect(pid.path().uid()).toBe(local.path().uid());
      expect(pid.isRunning()).toBe(false);

      expect(a.noSender().tell(pid, new Ping(7))).toBeNull();
      await until("the tell to arrive", (): boolean => collector.received.length >= 1);

      const delivery = collector.received[0] as { message: unknown; sender: PID };
      expect(delivery.message).toBeInstanceOf(Ping);
      expect((delivery.message as Ping).n).toBe(7);

      // The sender crossed as its path: a handle addressed back to the
      // origin node's NoSender actor, exactly as isolate hops do.
      expect(delivery.sender.path().toString()).toContain("NodeAktNoSender");
      expect(delivery.sender.path().port()).toBe(a.port());
    });
  });

  it("returns undefined when no running top-level actor holds the name", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      expect(await a.remoteLookup(b.host(), b.port(), "nobody")).toBeUndefined();
    });
  });

  it("routes a reply to the remote sender back across the wire", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const caller: Collector = new Collector();
      const callerPid: PID = await a.spawn("caller", caller);
      const echo: Collector = new Collector();
      await b.spawn("echo", new Echo());
      void echo;

      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;
      expect(callerPid.tell(remote, new Ping(3))).toBeNull();

      await until("the pong to come back", (): boolean => caller.received.length >= 1);
      const delivery = caller.received[0] as { message: unknown; sender: PID };
      expect(delivery.message).toBeInstanceOf(Pong);
      expect((delivery.message as Pong).n).toBe(3);
      expect(delivery.sender.path().toString()).toBe(remote.path().toString());
    });
  });

  it("keeps the sender handle identity stable across deliveries", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      await b.spawn("greeter", collector);
      const callerPid: PID = await a.spawn("caller", new Collector());

      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "greeter")) as PID;
      callerPid.tell(pid, new Ping(1));
      callerPid.tell(pid, new Ping(2));
      await until("both tells to arrive", (): boolean => collector.received.length >= 2);

      const first = collector.received[0] as { message: unknown; sender: PID };
      const second = collector.received[1] as { message: unknown; sender: PID };
      expect(first.sender).toBe(second.sender);
    });
  });

  it("attributes a senderless ref send to the receiving node's NoSender", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      await b.spawn("greeter", collector);

      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "greeter")) as PID;
      expect(pid.ref().tell(new Ping(5))).toBeNull();

      await until("the tell to arrive", (): boolean => collector.received.length >= 1);
      const delivery = collector.received[0] as { message: unknown; sender: PID };
      expect(delivery.sender).toBe(b.noSender());
    });
  });

  it("refuses an unregistered message on the sending side and dead-letters it", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const deadletters: Deadletter[] = [];
      a.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      await b.spawn("greeter", new Collector());
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "greeter")) as PID;

      const err: Error | null = a.noSender().tell(pid, new UnregisteredMessage(1));
      expect(err).toBeInstanceOf(TypeNotRegisteredError);
      await until("the dead letter", (): boolean => deadletters.length >= 1);
      expect((deadletters[0] as Deadletter).receiver).toBe(pid.path().toString());

      await expect(a.noSender().ask(pid, new UnregisteredMessage(2), 500)).rejects.toBeInstanceOf(
        TypeNotRegisteredError,
      );
    });
  });

  it("dead-letters a tell whose node has gone away", async () => {
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      const deadletters: Deadletter[] = [];
      a.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      await b.spawn("greeter", new Collector());
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "greeter")) as PID;
      await b.stop();

      // A tell racing the node's death can be kernel-accepted and lost
      // with it; the dead-letter guarantee is for sends the transport
      // refuses. Keep telling until the death is discovered.
      await until("the dead letter", (): boolean => {
        if (deadletters.length >= 1) {
          return true;
        }

        expect(a.noSender().tell(pid, new Ping(1))).toBeNull();
        return false;
      });
      expect((deadletters[0] as Deadletter).receiver).toBe(pid.path().toString());
    } finally {
      await a.stop();
      await b.stop();
    }
  });
});

describe("remote ask", () => {
  it("round-trips a typed reply", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await b.spawn("echo", new Echo());
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      const answer: unknown = await a.noSender().ask(pid, new Ask(41), 2000);
      expect(answer).toBeInstanceOf(Answer);
      expect((answer as Answer).n).toBe(42);
    });
  });

  it("round-trips passthrough data", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await b.spawn("echo", new Echo());
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      const answer: unknown = await a.noSender().ask(pid, { plain: true, n: 9 }, 2000);
      expect(answer).toEqual({ echoed: { plain: true, n: 9 } });
    });
  });

  it("rejects with the identical sentinel when the target is gone", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const local: PID = await b.spawn("echo", new Echo());
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      await local.shutdown();
      await expect(a.noSender().ask(pid, new Ask(1), 1000)).rejects.toBe(ErrDead);
    });
  });

  it("rejects with the timeout sentinel when no reply arrives", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const gate: Gate = new Gate();
      await b.spawn("gate", gate);
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "gate")) as PID;

      await expect(a.noSender().ask(pid, new Ask(1), 200)).rejects.toBe(ErrRequestTimeout);
      gate.release();
    });
  });

  it("rejects a non-positive timeout before touching the wire", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await b.spawn("echo", new Echo());
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      await expect(a.noSender().ask(pid, new Ask(1), 0)).rejects.toBe(ErrInvalidTimeout);
    });
  });

  it("settles the ask with the failure when the reply cannot encode", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await b.spawn("echo", new Echo());
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      const rejection: Promise<unknown> = a.noSender().ask(pid, new BadReply(), 2000);
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof Error && err.name === "TypeNotRegisteredError";
      });
    });
  });

  it("settles the ask with the mailbox rejection", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const gate: Gate = new Gate();
      await b.spawn("gate", gate, { mailbox: new BoundedMailbox(1) });
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "gate")) as PID;

      // The first tell parks the actor inside its behavior, the second
      // fills the one-slot mailbox, so the ask arrives at a full queue;
      // lane FIFO guarantees the arrival order matches the send order.
      a.noSender().tell(pid, new Ping(1));
      a.noSender().tell(pid, new Ping(2));
      await expect(a.noSender().ask(pid, new Ask(1), 1000)).rejects.toBe(ErrMailboxFull);
      gate.release();
    });
  });
});

describe("remote request", () => {
  it("delivers the continuation on the requester's own turn", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await b.spawn("echo", new Echo());
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      const requester: Requester = new Requester(
        (): PID => remote,
        (): unknown => new Ask(10),
        2000,
      );
      const pid: PID = await a.spawn("requester", requester, {
        reentrancy: { mode: "allowAll" },
      });

      a.noSender().tell(pid, "go");
      await until("the request outcome", (): boolean => requester.outcomes.length >= 1);

      const outcome = requester.outcomes[0] as { reply: unknown; error: Error | null };
      expect(outcome.error).toBeNull();
      expect(outcome.reply).toBeInstanceOf(Answer);
      expect((outcome.reply as Answer).n).toBe(11);
    });
  });

  it("delivers a request that carries no timeout", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await b.spawn("echo", new Echo());
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      const requester: Requester = new Requester(
        (): PID => remote,
        (): unknown => new Ask(20),
        0,
      );
      const pid: PID = await a.spawn("requester", requester, {
        reentrancy: { mode: "allowAll" },
      });

      a.noSender().tell(pid, "go");
      await until("the request outcome", (): boolean => requester.outcomes.length >= 1);

      const outcome = requester.outcomes[0] as { reply: unknown; error: Error | null };
      expect(outcome.error).toBeNull();
      expect((outcome.reply as Answer).n).toBe(21);
    });
  });

  it("times out with the runtime sentinel", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const gate: Gate = new Gate();
      await b.spawn("gate", gate);
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "gate")) as PID;

      const requester: Requester = new Requester(
        (): PID => remote,
        (): unknown => new Ask(1),
        200,
      );
      const pid: PID = await a.spawn("requester", requester, {
        reentrancy: { mode: "allowAll" },
      });

      a.noSender().tell(pid, "go");
      await until("the request outcome", (): boolean => requester.outcomes.length >= 1);
      expect((requester.outcomes[0] as { error: Error | null }).error).toBe(ErrRequestTimeout);
      gate.release();
    });
  });

  it("completes with the admission sentinel without reentrancy", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await b.spawn("echo", new Echo());
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      let seen: Error | null = null;
      a.noSender()
        .request(remote, new Ask(1))
        .onReply((_reply: unknown, error: Error | null): void => {
          seen = error;
        });
      expect(seen).toBe(ErrReentrancyDisabled);
    });
  });

  it("completes with the encode failure for an unregistered message", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      await b.spawn("echo", new Echo());
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

      const requester: Requester = new Requester(
        (): PID => remote,
        (): unknown => new UnregisteredMessage(1),
        1000,
      );
      const pid: PID = await a.spawn("requester", requester, {
        reentrancy: { mode: "allowAll" },
      });

      a.noSender().tell(pid, "go");
      await until("the request outcome", (): boolean => requester.outcomes.length >= 1);
      expect((requester.outcomes[0] as { error: Error | null }).error).toBeInstanceOf(
        TypeNotRegisteredError,
      );
    });
  });
});

describe("remoteLookup guards", () => {
  it("rejects on a system without remoting", async () => {
    const system: ActorSystem = new ActorSystem("plain", { logger: discardLogger });
    await system.start();

    try {
      await expect(system.remoteLookup("127.0.0.1", 1, "x")).rejects.toBe(ErrRemotingDisabled);
    } finally {
      await system.stop();
    }
  });

  it("rejects on a system that is not running", async () => {
    const system: ActorSystem = remoteSystem("cold");
    await expect(system.remoteLookup("127.0.0.1", 1, "x")).rejects.toBe(ErrActorSystemNotStarted);
  });

  it("rejects with the dial failure when nothing listens", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const probe: ActorSystem = remoteSystem("gone");
      await probe.start();
      const deadPort: number = probe.port();
      await probe.stop();
      void b;

      const rejection: Promise<PID | undefined> = a.remoteLookup("127.0.0.1", deadPort, "x");
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof Error && err !== ErrRequestTimeout;
      });
    });
  });
});
