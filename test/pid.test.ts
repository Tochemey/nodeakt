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

import { afterEach, describe, expect, it, vi } from "vitest";
import type { Actor } from "../src/actor/actor";
import { ActorSystem } from "../src/actor/actor.system";
import { BoundedMailbox } from "../src/actor/bounded.mailbox";
import type { Context } from "../src/actor/context";
import { newPath } from "../src/actor/path";
import { PID } from "../src/actor/pid";
import { createReceiveContext, type ReceiveContext } from "../src/actor/receive.context";
import type { SpawnOptions } from "../src/actor/spawn.options";
import { ResumeDirective, Supervisor } from "../src/actor/supervisor";
import {
  ActorInitializationError,
  ActorNotFoundError,
  ErrDead,
  ErrMailboxFull,
  ErrStashBufferEmpty,
  ErrUndefinedActor,
} from "../src/errors/errors";

// A real but never started system: standalone PIDs receive no PostStart
// announcement because the NoSender actor does not exist.
const system = new ActorSystem("sys");

function makePid(actor: Actor, name = "test-actor", options?: SpawnOptions): PID {
  return new PID(actor, newPath(name, "sys", "127.0.0.1", 0), system, options);
}

// The sending side for messages told from outside an actor. It never
// starts: tell only checks the target's state.
const external = makePid(
  { preStart(): void {}, receive(): void {}, postStop(): void {} },
  "external",
);

class Collector implements Actor {
  readonly received: unknown[] = [];
  started = 0;

  preStart(): void {
    this.started++;
  }

  receive(ctx: ReceiveContext): void {
    this.received.push(ctx.message);
  }

  postStop(): void {}
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("PID lifecycle", () => {
  it("exposes its name, path, and system", () => {
    const pid = makePid(new Collector(), "greeter");
    expect(pid.name()).toBe("greeter");
    expect(pid.path().toString()).toBe("nodeakt://sys@127.0.0.1:0/greeter");
    expect(pid.actorSystem()).toBe(system);
  });

  it("runs preStart exactly once on start", async () => {
    const actor = new Collector();
    const pid = makePid(actor);
    expect(pid.isRunning()).toBe(false);

    await pid.start();
    await pid.start();
    expect(actor.started).toBe(1);
    expect(pid.isRunning()).toBe(true);
  });

  it("rejects messages before start", () => {
    const pid = makePid(new Collector());
    expect(external.tell(pid, "hello")).toBe(ErrDead);
  });

  it("passes a lifecycle context to preStart and postStop", async () => {
    class ContextAware implements Actor {
      preContext?: Context;
      postContext?: Context;

      preStart(ctx: Context): void {
        this.preContext = ctx;
      }

      receive(): void {}

      postStop(ctx: Context): void {
        this.postContext = ctx;
      }
    }

    const actor = new ContextAware();
    const pid = makePid(actor, "ctx-aware");
    await pid.start();
    await pid.shutdown();

    expect(actor.preContext?.actorName()).toBe("ctx-aware");
    expect(actor.preContext?.actorSystem()).toBe(system);
    expect(actor.postContext?.actorName()).toBe("ctx-aware");
  });

  it("does not start when preStart fails", async () => {
    class Failing extends Collector {
      override preStart(): void {
        throw new Error("boom");
      }
    }

    const pid = makePid(new Failing());
    const err: unknown = await pid.start().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ActorInitializationError);
    expect(((err as Error).cause as Error).message).toBe("boom");
    expect(pid.isRunning()).toBe(false);
    expect(external.tell(pid, "hello")).toBe(ErrDead);
  });
});

describe("PID message handling", () => {
  it("delivers messages to receive in order", async () => {
    const actor = new Collector();
    const pid = makePid(actor);
    await pid.start();

    for (let i = 0; i < 5; i++) {
      expect(external.tell(pid, i)).toBeNull();
    }

    await expect.poll(() => actor.received.length).toBe(5);
    expect(actor.received).toEqual([0, 1, 2, 3, 4]);
  });

  it("attaches self to the receive context", async () => {
    let self: PID | undefined;

    class SelfAware extends Collector {
      override receive(ctx: ReceiveContext): void {
        self = ctx.self;
      }
    }

    const pid = makePid(new SelfAware());
    await pid.start();
    external.tell(pid, "who am I");

    await expect.poll(() => self).toBe(pid);
  });

  it("processes one message at a time with async behaviors", async () => {
    class AsyncActor implements Actor {
      concurrent = 0;
      maxConcurrent = 0;
      done = 0;

      preStart(): void {}

      async receive(): Promise<void> {
        this.concurrent++;
        this.maxConcurrent = Math.max(this.maxConcurrent, this.concurrent);
        await new Promise((resolve) => setTimeout(resolve, 1));
        this.concurrent--;
        this.done++;
      }

      postStop(): void {}
    }

    const actor = new AsyncActor();
    const pid = makePid(actor);
    await pid.start();

    for (let i = 0; i < 5; i++) {
      external.tell(pid, i);
    }

    await expect.poll(() => actor.done).toBe(5);
    expect(actor.maxConcurrent).toBe(1);
  });

  it("keeps processing after a behavior throws when the supervisor resumes", async () => {
    class Faulty extends Collector {
      override receive(ctx: ReceiveContext): void {
        if (ctx.message === "boom") {
          throw new Error("boom");
        }

        super.receive(ctx);
      }
    }

    const actor = new Faulty();
    const pid = makePid(actor, "faulty", {
      supervisor: new Supervisor({ anyErrorDirective: ResumeDirective }),
    });
    await pid.start();

    external.tell(pid, "boom");
    external.tell(pid, "ok");

    await expect.poll(() => actor.received).toEqual(["ok"]);
    expect(pid.isRunning()).toBe(true);
  });
});

describe("PID behaviors", () => {
  it("become replaces the behavior and unBecome restores the default", async () => {
    class Switcher implements Actor {
      readonly log: string[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        this.log.push(`awake:${ctx.message}`);

        if (ctx.message === "sleep") {
          ctx.become((c) => {
            this.log.push(`asleep:${c.message}`);

            if (c.message === "wake") {
              c.unBecome();
            }
          });
        }
      }

      postStop(): void {}
    }

    const actor = new Switcher();
    const pid = makePid(actor);
    await pid.start();

    for (const msg of ["a", "sleep", "b", "wake", "c"]) {
      external.tell(pid, msg);
    }

    await expect.poll(() => actor.log.length).toBe(5);
    expect(actor.log).toEqual(["awake:a", "awake:sleep", "asleep:b", "asleep:wake", "awake:c"]);
  });

  it("becomeStacked and unBecomeStacked nest behaviors", async () => {
    class Nested implements Actor {
      readonly log: string[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        this.log.push(`base:${ctx.message}`);

        if (ctx.message === "push") {
          ctx.becomeStacked((c) => {
            this.log.push(`stacked:${c.message}`);

            if (c.message === "pop") {
              c.unBecomeStacked();
            }
          });
        }
      }

      postStop(): void {}
    }

    const actor = new Nested();
    const pid = makePid(actor);
    await pid.start();

    for (const msg of ["push", "x", "pop", "y"]) {
      external.tell(pid, msg);
    }

    await expect.poll(() => actor.log.length).toBe(4);
    expect(actor.log).toEqual(["base:push", "stacked:x", "stacked:pop", "base:y"]);
  });
});

describe("PID shutdown", () => {
  class Slow implements Actor {
    readonly received: unknown[] = [];
    stopped = 0;

    preStart(): void {}

    async receive(ctx: ReceiveContext): Promise<void> {
      await new Promise((resolve) => setTimeout(resolve, 1));
      this.received.push(ctx.message);
    }

    postStop(): void {
      this.stopped++;
    }
  }

  it("ignores transport watch registrations on a non-running actor", () => {
    const pid = makePid(new Collector(), "unwatched-target");
    const watcher = makePid(new Collector(), "transport-watcher");

    // Never started: registering and removing are both no-ops.
    pid.addWatcher(watcher);
    pid.removeWatcher(watcher);
    expect(pid.isRunning()).toBe(false);
  });

  it("invokes onStopped listeners once shutdown completes, in order", async () => {
    const pid = makePid(new Collector());
    await pid.start();

    const calls: string[] = [];
    pid.onStopped(() => calls.push("first"));
    pid.onStopped(() => calls.push("second"));

    await pid.shutdown();
    expect(calls).toEqual(["first", "second"]);

    // Shutting down again must not refire the listeners.
    await pid.shutdown();
    expect(calls).toEqual(["first", "second"]);
  });

  it("does not treat a restart as a stop", async () => {
    const pid = makePid(new Collector());
    await pid.start();

    let stops = 0;
    pid.onStopped(() => {
      stops++;
    });

    await pid.restart();
    expect(stops).toBe(0);

    await pid.shutdown();
    expect(stops).toBe(1);
  });

  it("drains the mailbox before running postStop", async () => {
    const actor = new Slow();
    const pid = makePid(actor);
    await pid.start();

    for (let i = 0; i < 5; i++) {
      external.tell(pid, i);
    }

    await pid.shutdown();
    expect(actor.received).toEqual([0, 1, 2, 3, 4]);
    expect(actor.stopped).toBe(1);
    expect(pid.isRunning()).toBe(false);
    expect(external.tell(pid, "late")).toBe(ErrDead);
  });

  it("rejects messages sent while stopping", async () => {
    const actor = new Slow();
    const pid = makePid(actor);
    await pid.start();

    external.tell(pid, "in-flight");
    const stopping = pid.shutdown();
    expect(external.tell(pid, "late")).toBe(ErrDead);

    await stopping;
    expect(actor.received).toEqual(["in-flight"]);
  });

  it("is idempotent", async () => {
    const actor = new Slow();
    const pid = makePid(actor);
    await pid.start();

    const first = pid.shutdown();
    const second = pid.shutdown();
    expect(second).toBe(first);

    await first;
    await pid.shutdown();
    expect(actor.stopped).toBe(1);
  });

  it("is a no-op on an actor that never started", async () => {
    const actor = new Slow();
    const pid = makePid(actor);

    await pid.shutdown();
    expect(actor.stopped).toBe(0);
  });

  it("completes even when postStop throws", async () => {
    const errorSpy = vi.spyOn(system.logger(), "error").mockImplementation(() => {});

    class FailingStop extends Slow {
      override postStop(): void {
        throw new Error("cleanup failed");
      }
    }

    const pid = makePid(new FailingStop());
    await pid.start();

    await pid.shutdown();
    expect(pid.isRunning()).toBe(false);
    expect(errorSpy).toHaveBeenCalledOnce();
  });
});

describe("PID sender", () => {
  class SenderAware extends Collector {
    readonly senders: Array<PID | undefined> = [];

    override receive(ctx: ReceiveContext): void {
      this.senders.push(ctx.sender);
    }
  }

  it("sees the sending actor's PID", async () => {
    const actor = new SenderAware();
    const receiver = makePid(actor, "receiver");
    const sender = makePid(new Collector(), "sender");
    await receiver.start();
    await sender.start();

    sender.tell(receiver, "hello");
    await expect.poll(() => actor.senders.length).toBe(1);
    expect(actor.senders[0]).toBe(sender);
  });

  it("has no sender on a detached receive context", () => {
    expect(createReceiveContext("x").sender).toBeUndefined();
  });
});

describe("PID message pattern matching", () => {
  class Greet {
    constructor(readonly who: string) {}
  }

  class Square {
    constructor(readonly value: number) {}
  }

  it("dispatches by message class with instanceof narrowing", async () => {
    class PatternActor implements Actor {
      readonly log: string[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        const msg = ctx.message;

        if (msg instanceof Greet) {
          this.log.push(`hello ${msg.who}`);
        } else if (msg instanceof Square) {
          this.log.push(`squared ${msg.value * msg.value}`);
        } else {
          this.log.push(`unhandled ${String(msg)}`);
        }
      }

      postStop(): void {}
    }

    const actor = new PatternActor();
    const pid = makePid(actor);
    await pid.start();

    external.tell(pid, new Greet("world"));
    external.tell(pid, new Square(4));
    external.tell(pid, "raw");

    await expect.poll(() => actor.log.length).toBe(3);
    expect(actor.log).toEqual(["hello world", "squared 16", "unhandled raw"]);
  });
});

describe("PID stashing", () => {
  it("stashes messages and re-delivers them in order on unstashAll", async () => {
    class Stasher implements Actor {
      readonly processed: unknown[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message === "ready") {
          ctx.become((c) => {
            this.processed.push(c.message);
          });
          ctx.unstashAll();
          return;
        }

        ctx.stash();
      }

      postStop(): void {}
    }

    const actor = new Stasher();
    const pid = makePid(actor);
    await pid.start();

    external.tell(pid, 1);
    external.tell(pid, 2);
    external.tell(pid, 3);
    external.tell(pid, "ready");

    await expect.poll(() => actor.processed).toEqual([1, 2, 3]);

    external.tell(pid, 4);
    await expect.poll(() => actor.processed).toEqual([1, 2, 3, 4]);
  });

  it("unstash re-delivers a single message at a time", async () => {
    class OneAtATime implements Actor {
      readonly processed: unknown[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message === "one") {
          ctx.become((c) => {
            this.processed.push(c.message);
          });
          ctx.unstash();
          return;
        }

        ctx.stash();
      }

      postStop(): void {}
    }

    const actor = new OneAtATime();
    const pid = makePid(actor);
    await pid.start();

    external.tell(pid, "a");
    external.tell(pid, "b");
    external.tell(pid, "one");

    await expect.poll(() => actor.processed).toEqual(["a"]);
  });

  it("unstashAll re-delivers behind messages already queued", async () => {
    class Flipper implements Actor {
      readonly processed: unknown[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message === "flip") {
          ctx.become((c) => {
            this.processed.push(c.message);
          });
          ctx.unstashAll();
          return;
        }

        ctx.stash();
      }

      postStop(): void {}
    }

    const actor = new Flipper();
    const pid = makePid(actor);
    await pid.start();

    // "b" already sits in the mailbox when the unstash happens, so it is
    // processed before the re-delivered "a".
    external.tell(pid, "a");
    external.tell(pid, "flip");
    external.tell(pid, "b");

    await expect.poll(() => actor.processed).toEqual(["b", "a"]);
  });

  it("counts a non-empty stash as pending work in isIdle", async () => {
    class Stasher implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message !== "flush") {
          ctx.stash();
          return;
        }

        ctx.become(() => {});
        ctx.unstashAll();
      }

      postStop(): void {}
    }

    const pid = makePid(new Stasher(), "idle-stash");
    await pid.start();

    external.tell(pid, "parked");
    await expect.poll(() => pid.processedCount()).toBe(1);

    // Mailbox drained, but the stashed message must block passivation.
    expect(pid.stashSize()).toBe(1);
    expect(pid.isIdle()).toBe(false);

    external.tell(pid, "flush");
    await expect.poll(() => pid.processedCount()).toBe(3);
    expect(pid.stashSize()).toBe(0);
    expect(pid.isIdle()).toBe(true);
  });

  it("unstashAll without a stash buffer is a no-op", async () => {
    const pid = makePid(new Collector(), "no-stash");
    await pid.start();
    expect(pid.stashSize()).toBe(0);
    expect(() => createReceiveContext("x", pid).unstashAll()).not.toThrow();
  });

  it("unstashAll surfaces a re-delivery rejection", async () => {
    class Stasher implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        ctx.stash();
      }

      postStop(): void {}
    }

    const pid = new PID(new Stasher(), newPath("stash-bounded", "sys", "127.0.0.1", 0), system, {
      mailbox: new BoundedMailbox(1),
    });
    await pid.start();

    external.tell(pid, "s1");
    await expect.poll(() => pid.processedCount()).toBe(1);
    external.tell(pid, "s2");
    await expect.poll(() => pid.processedCount()).toBe(2);
    expect(pid.stashSize()).toBe(2);

    // Both messages sit in the stash and the mailbox is empty;
    // re-delivering them overflows the single-slot mailbox on the second.
    let caught: unknown;

    try {
      createReceiveContext("x", pid).unstashAll();
    } catch (err) {
      caught = err;
    }

    expect(caught).toBe(ErrMailboxFull);
  });
});

describe("PID processing internals", () => {
  it("counts processed messages", async () => {
    const actor = new Collector();
    const pid = makePid(actor, "counting");
    await pid.start();
    expect(pid.processedCount()).toBe(0);

    external.tell(pid, "a");
    external.tell(pid, "b");
    await expect.poll(() => pid.processedCount()).toBe(2);
  });

  it("awaits asynchronous preStart and postStop hooks", async () => {
    const order: string[] = [];
    const actor: Actor = {
      async preStart(): Promise<void> {
        await new Promise((resolve) => setTimeout(resolve, 1));
        order.push("preStart");
      },
      receive(): void {},
      async postStop(): Promise<void> {
        await new Promise((resolve) => setTimeout(resolve, 1));
        order.push("postStop");
      },
    };

    const pid = makePid(actor, "async-hooks");
    await pid.start();
    expect(order).toEqual(["preStart"]);

    await pid.shutdown();
    expect(order).toEqual(["preStart", "postStop"]);
  });

  it("yields to the event loop on a large backlog", async () => {
    const actor = new Collector();
    const pid = makePid(actor, "backlog");
    await pid.start();

    // More than two full THROUGHPUT batches, forcing setImmediate yields.
    const total = 650;
    for (let i = 0; i < total; i++) {
      external.tell(pid, i);
    }

    await expect.poll(() => actor.received.length).toBe(total);
    expect(pid.processedCount()).toBe(total);
  });

  it("surfaces a mailbox rejection to the sender", async () => {
    const pid = new PID(new Collector(), newPath("bounded", "sys", "127.0.0.1", 0), system, {
      mailbox: new BoundedMailbox(1),
    });
    await pid.start();

    expect(external.tell(pid, "first")).toBeNull();
    expect(external.tell(pid, "second")).toBe(ErrMailboxFull);
  });
});

describe("ReceiveContext guards", () => {
  it("throws when a behavior method is used on a detached context", () => {
    const ctx = createReceiveContext("x");
    expect(() => ctx.become(() => {})).toThrow("not attached");
  });

  it("throws the stash error when unstashing with nothing stashed", async () => {
    const pid = makePid(new Collector(), "ctx-attached");
    await pid.start();

    const ctx = createReceiveContext("x", pid);
    let caught: unknown;

    try {
      ctx.unstash();
    } catch (err) {
      caught = err;
    }

    expect(caught).toBe(ErrStashBufferEmpty);
  });
});

describe("PID identity", () => {
  it("exposes id, actor, kind, and equals by path", () => {
    const actor = new Collector();
    const pid = makePid(actor, "greeter");
    const alias = makePid(new Collector(), "greeter");
    const other = makePid(new Collector(), "other");

    expect(pid.id()).toBe(pid.path().toString());
    expect(pid.actor()).toBe(actor);
    expect(pid.kind()).toBe("Collector");
    expect(pid.equals(pid)).toBe(true);
    expect(pid.equals(alias)).toBe(true);
    expect(pid.equals(other)).toBe(false);
  });
});

describe("PID child, parent, and stop", () => {
  it("resolves a running child and reports its parent", async () => {
    const parent = makePid(new Collector(), "parent");
    await parent.start();
    const childActor = new Collector();
    const child = await parent.spawnChild("worker", childActor);

    expect(parent.child("worker")).toBe(child);
    expect(child.parent()).toBe(parent);
    expect(parent.parent()).toBeUndefined();
    expect(parent.children()).toEqual([child]);
  });

  it("throws when the named child is missing or the parent is dead", async () => {
    const parent = makePid(new Collector(), "lookup");
    await expect(() => parent.child("worker")).toThrow(ErrDead);

    await parent.start();
    expect(() => parent.child("worker")).toThrow(ActorNotFoundError);

    await parent.shutdown();
    await expect(() => parent.child("worker")).toThrow(ErrDead);
  });

  it("stops a child and rejects stopping a non-child", async () => {
    const parent = makePid(new Collector(), "stopper");
    await parent.start();
    const child = await parent.spawnChild("worker", new Collector());
    const stranger = makePid(new Collector(), "stranger");
    await stranger.start();

    await expect(parent.stop(stranger)).rejects.toBeInstanceOf(ActorNotFoundError);

    await parent.stop(child);
    expect(child.isRunning()).toBe(false);
    expect(parent.children()).toEqual([]);
    await expect(parent.stop(child)).rejects.toBeInstanceOf(ActorNotFoundError);
  });

  it("rejects stopping a child when the parent is not running", async () => {
    const parent = makePid(new Collector(), "dead-stop");
    const child = makePid(new Collector(), "orphan");
    await expect(parent.stop(child)).rejects.toBe(ErrDead);

    await parent.start();
    await parent.shutdown();
    await expect(parent.stop(child)).rejects.toBe(ErrDead);
  });

  it("rejects stopping the NoSender sentinel", async () => {
    const parent = makePid(new Collector(), "no-sender-stop");
    await parent.start();
    const sentinel = makePid(new Collector(), "NodeAktNoSender");

    await expect(parent.stop(sentinel)).rejects.toBe(ErrUndefinedActor);
  });
});

describe("receive loop yielding", () => {
  it("drains a backlog larger than one event-loop turn", async () => {
    const actor = new Collector();
    const pid = makePid(actor, "flood");
    await pid.start();

    // Larger than the per-turn throughput quota, so the loop yields with
    // setImmediate at least once and resumes on a later turn.
    const count = 5000;
    for (let i = 0; i < count; i++) {
      external.tell(pid, i);
    }

    await expect.poll(() => actor.received.length, { timeout: 5000 }).toBe(count);
  });

  it("drains an asynchronous backlog larger than one event-loop turn", async () => {
    class AsyncCounter implements Actor {
      processed = 0;

      preStart(): void {}

      async receive(): Promise<void> {
        await Promise.resolve();
        this.processed++;
      }

      postStop(): void {}
    }

    const actor = new AsyncCounter();
    const pid = makePid(actor, "async-flood");
    await pid.start();

    const count = 2100;
    for (let i = 0; i < count; i++) {
      external.tell(pid, i);
    }

    await expect.poll(() => actor.processed, { timeout: 5000 }).toBe(count);
  });

  it("routes the rejection of an asynchronous behavior through fault handling", async () => {
    class AsyncFaulty implements Actor {
      preStart(): void {}

      async receive(): Promise<void> {
        await Promise.resolve();
        throw new Error("boom");
      }

      postStop(): void {}
    }

    const pid = makePid(new AsyncFaulty(), "async-faulty");
    await pid.start();

    expect(external.tell(pid, "hi")).toBeNull();
    await expect.poll(() => pid.isSuspended()).toBe(true);
  });
});
