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

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { BoundedMailbox } from "../src/bounded.mailbox";
import { discardLogger } from "../src/discard.logger";
import {
  ErrActorSystemNotStarted,
  ErrDead,
  ErrMailboxFull,
  ErrStashBufferEmpty,
  ErrUnhandled,
} from "../src/errors";
import { Deadletter, PoisonPill, PostStart, Terminated } from "../src/messages";
import { newPath } from "../src/path";
import { PID } from "../src/pid";
import { createReceiveContext, type ReceiveContext } from "../src/receive.context";

class Sink implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** Parks its receive loop on the first user message so later sends
 * stay in the mailbox; releasable so the system can stop cleanly. */
class Parked implements Actor {
  started = false;

  private _released = false;
  private _release: (() => void) | null = null;

  release(): void {
    this._released = true;
    this._release?.();
  }

  preStart(): void {}

  async receive(ctx: ReceiveContext): Promise<void> {
    if (ctx.message instanceof PostStart || this._released) {
      return;
    }

    this.started = true;
    await new Promise<void>((resolve) => {
      this._release = resolve;
    });
  }

  postStop(): void {}
}

describe("dead letters", () => {
  let system: ActorSystem;
  let letters: Deadletter[];
  let parkedActors: Parked[];

  function makeParked(): Parked {
    const parked = new Parked();
    parkedActors.push(parked);
    return parked;
  }

  beforeEach(async () => {
    system = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
    letters = [];
    parkedActors = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        letters.push(event);
      }
    });
  });

  afterEach(async () => {
    for (const parked of parkedActors) {
      parked.release();
    }

    await system.stop();
  });

  it("publishes a dead letter when the target is not running", async () => {
    const pid = await system.spawn("victim", new Sink());
    const receiver = pid.path().toString();
    await pid.shutdown();

    const before = Date.now();
    expect(system.noSender().tell(pid, "lost")).toBe(ErrDead);

    await expect.poll(() => letters.length).toBe(1);
    const letter = letters[0] as Deadletter;
    expect(letter.sender).toBe(system.noSender().path().toString());
    expect(letter.receiver).toBe(receiver);
    expect(letter.message).toBe("lost");
    expect(letter.reason).toBe(ErrDead.message);
    expect(letter.sendTime).toBeGreaterThanOrEqual(before);

    await expect(system.deadlettersCount()).resolves.toBe(1);
    await expect(system.deadlettersCount(receiver)).resolves.toBe(1);
    await expect(system.deadlettersCount("nodeakt://sys@127.0.0.1:0/unknown")).resolves.toBe(0);
  });

  it("publishes a dead letter when the mailbox rejects the message", async () => {
    const parked = makeParked();
    const pid = await system.spawn("bounded", parked, { mailbox: new BoundedMailbox(1) });

    expect(system.noSender().tell(pid, "park")).toBeNull();
    await expect.poll(() => parked.started).toBe(true);
    expect(system.noSender().tell(pid, "occupy")).toBeNull();

    expect(system.noSender().tell(pid, "overflow")).toBe(ErrMailboxFull);
    await expect.poll(() => letters.length).toBe(1);
    expect((letters[0] as Deadletter).reason).toBe(ErrMailboxFull.message);
  });

  it("publishes dead letters for failed ask deliveries", async () => {
    const dead = await system.spawn("dead-ask", new Sink());
    await dead.shutdown();
    await expect(system.noSender().ask(dead, "q1", 100)).rejects.toBe(ErrDead);

    const parked = makeParked();
    const full = await system.spawn("full-ask", parked, { mailbox: new BoundedMailbox(1) });
    expect(system.noSender().tell(full, "park")).toBeNull();
    await expect.poll(() => parked.started).toBe(true);
    expect(system.noSender().tell(full, "occupy")).toBeNull();
    await expect(system.noSender().ask(full, "q2", 100)).rejects.toBe(ErrMailboxFull);

    await expect.poll(() => letters.map((letter) => letter.message)).toEqual(["q1", "q2"]);
  });

  it("publishes dead letters for failed request deliveries", async () => {
    const outcomes: Array<Error | null> = [];

    class Requesting implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message instanceof PostStart) {
          return;
        }

        ctx.request(ctx.message as PID, "payload").onReply((_reply, error) => {
          outcomes.push(error);
        });
      }

      postStop(): void {}
    }

    const requester = await system.spawn("requester", new Requesting(), {
      reentrancy: { mode: "allowAll" },
    });

    const dead = await system.spawn("dead-request", new Sink());
    await dead.shutdown();
    system.noSender().tell(requester, dead);
    await expect.poll(() => outcomes.length).toBe(1);
    expect(outcomes[0]).toBe(ErrDead);

    const parked = makeParked();
    const full = await system.spawn("full-request", parked, { mailbox: new BoundedMailbox(1) });
    expect(system.noSender().tell(full, "park")).toBeNull();
    await expect.poll(() => parked.started).toBe(true);
    expect(system.noSender().tell(full, "occupy")).toBeNull();
    system.noSender().tell(requester, full);
    await expect.poll(() => outcomes.length).toBe(2);
    expect(outcomes[1]).toBe(ErrMailboxFull);

    await expect
      .poll(() => letters.map((letter) => letter.message))
      .toEqual(["payload", "payload"]);
  });

  it("never turns runtime announcements into dead letters", async () => {
    const pid = await system.spawn("quiet", new Sink());
    await pid.shutdown();

    expect(system.noSender().tell(pid, new PostStart())).toBe(ErrDead);
    expect(system.noSender().tell(pid, new Terminated("who"))).toBe(ErrDead);
    expect(system.noSender().tell(pid, new PoisonPill())).toBe(ErrDead);
    expect(system.noSender().tell(pid, "witness")).toBe(ErrDead);

    // The witness letter proves the earlier sends were already skipped:
    // the dead-letter actor's mailbox is FIFO.
    await expect.poll(() => letters.length).toBe(1);
    expect((letters[0] as Deadletter).message).toBe("witness");
    await expect(system.deadlettersCount()).resolves.toBe(1);
  });

  it("ignores noise addressed to the dead-letter actor itself", async () => {
    const deadletter = system.deadletterPid() as PID;
    expect(system.noSender().tell(deadletter, "junk")).toBeNull();

    await expect(system.deadlettersCount()).resolves.toBe(0);
  });

  it("records no sender for a delivery that carried none", async () => {
    class Thrower implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message === "fault") {
          throw new Error("boom");
        }
      }

      postStop(): void {}
    }

    // A standalone PID: a system-spawned actor's fault would escalate
    // to the user guardian, whose default directive stops the actor and
    // disposes the stash; parentless, it stays suspended.
    const pid = new PID(new Thrower(), newPath("stasher", "sys", "127.0.0.1", 0), system);
    await pid.start();

    // Stash a senderless context, suspend the actor with a fault, then
    // unstash: the re-delivery fails against the suspended actor.
    expect(pid.stash(createReceiveContext("orphan", pid))).toBeNull();
    system.noSender().tell(pid, "fault");
    await expect.poll(() => pid.isSuspended()).toBe(true);

    expect(pid.unstash()).toBe(ErrDead);
    await expect.poll(() => letters.length).toBe(1);
    expect((letters[0] as Deadletter).sender).toBeUndefined();
    expect((letters[0] as Deadletter).message).toBe("orphan");

    expect(pid.unstash()).toBe(ErrStashBufferEmpty);
  });

  it("stops delivering after unsubscribe and subscribes a function once", async () => {
    const extra: Deadletter[] = [];
    const subscriber = (event: unknown): void => {
      if (event instanceof Deadletter) {
        extra.push(event);
      }
    };

    system.subscribe(subscriber);
    system.subscribe(subscriber);

    const pid = await system.spawn("once", new Sink());
    await pid.shutdown();

    system.noSender().tell(pid, "one");
    await expect.poll(() => letters.length).toBe(1);
    expect(extra).toHaveLength(1);

    system.unsubscribe(subscriber);
    system.noSender().tell(pid, "two");
    await expect.poll(() => letters.length).toBe(2);
    expect(extra).toHaveLength(1);
  });

  it("contains a throwing subscriber and still informs the others", async () => {
    system.subscribe(() => {
      throw new Error("subscriber boom");
    });

    const pid = await system.spawn("contained", new Sink());
    await pid.shutdown();

    expect(system.noSender().tell(pid, "lost")).toBe(ErrDead);
    await expect.poll(() => letters.length).toBe(1);
  });

  it("rejects subscriptions and counts while the system is not running", async () => {
    const stopped = new ActorSystem("stopped-sys");
    expect(() => stopped.subscribe(() => {})).toThrow(ErrActorSystemNotStarted);
    expect(() => stopped.unsubscribe(() => {})).toThrow(ErrActorSystemNotStarted);
    await expect(stopped.deadlettersCount()).rejects.toBe(ErrActorSystemNotStarted);
  });

  it("starts over with fresh counts after a stop", async () => {
    const pid = await system.spawn("reset", new Sink());
    await pid.shutdown();
    system.noSender().tell(pid, "lost");
    await expect.poll(() => letters.length).toBe(1);
    await expect(system.deadlettersCount()).resolves.toBe(1);

    await system.stop();
    await expect(system.deadlettersCount()).rejects.toBe(ErrActorSystemNotStarted);

    await system.start();
    const again = await system.spawn("reset", new Sink());
    await again.shutdown();
    system.noSender().tell(again, "lost-again");

    // The old subscription died with the stop; only counts move.
    await expect.poll(() => system.deadlettersCount(), { timeout: 2000 }).toBe(1);
    expect(letters).toHaveLength(1);
  });
});

describe("unhandled messages", () => {
  let system: ActorSystem;
  let letters: Deadletter[];

  beforeEach(async () => {
    system = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
    letters = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        letters.push(event);
      }
    });
  });

  afterEach(async () => {
    await system.stop();
  });

  it("routes a message marked unhandled to dead letters", async () => {
    class Picky implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (typeof ctx.message !== "string") {
          ctx.unhandled();
        }
      }

      postStop(): void {}
    }

    const pid = await system.spawn("picky", new Picky());
    expect(system.noSender().tell(pid, "known")).toBeNull();
    expect(system.noSender().tell(pid, 42)).toBeNull();

    await expect.poll(() => letters.length).toBe(1);
    const letter = letters[0] as Deadletter;
    expect(letter.message).toBe(42);
    expect(letter.receiver).toBe(pid.path().toString());
    expect(letter.sender).toBe(system.noSender().path().toString());
    expect(letter.reason).toBe(ErrUnhandled.message);
    await expect(system.deadlettersCount(pid.path().toString())).resolves.toBe(1);
  });

  it("keeps runtime announcements out of dead letters even when marked", async () => {
    class Blanket implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        ctx.unhandled();
      }

      postStop(): void {}
    }

    const pid = await system.spawn("blanket", new Blanket());
    expect(system.noSender().tell(pid, "witness")).toBeNull();

    // PostStart was marked unhandled too; only the witness arrives.
    await expect.poll(() => letters.length).toBe(1);
    expect((letters[0] as Deadletter).message).toBe("witness");
  });
});
