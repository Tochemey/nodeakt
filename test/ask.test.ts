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
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { BoundedMailbox } from "../src/bounded.mailbox";
import { ErrDead, ErrMailboxFull, ErrRequestTimeout } from "../src/errors";
import { newPath } from "../src/path";
import { PID } from "../src/pid";
import { createReceiveContext, type ReceiveContext } from "../src/receive.context";

const system = new ActorSystem("sys");

function makePid(actor: Actor, name: string, mailbox?: BoundedMailbox): PID {
  return new PID(
    actor,
    newPath(name, "sys", "127.0.0.1", 0),
    system,
    mailbox ? { mailbox } : undefined,
  );
}

const external = makePid(
  { preStart(): void {}, receive(): void {}, postStop(): void {} },
  "external",
);

class Echo implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    ctx.response(ctx.message);
  }

  postStop(): void {}
}

describe("PID ask", () => {
  it("waits for the target's response", async () => {
    const pid = makePid(new Echo(), "echo");
    await pid.start();

    await expect(external.ask(pid, "ping", 1000)).resolves.toBe("ping");
    await expect(external.ask(pid, 42, 1000)).resolves.toBe(42);
  });

  it("records the asking actor as the sender", async () => {
    class SenderAware implements Actor {
      sender: PID | undefined;

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        this.sender = ctx.sender;
        ctx.response("ok");
      }

      postStop(): void {}
    }

    const actor = new SenderAware();
    const pid = makePid(actor, "aware");
    await pid.start();

    const sender = makePid(new Echo(), "asker");
    await sender.start();
    await sender.ask(pid, "hi", 1000);

    expect(actor.sender).toBe(sender);
  });

  it("rejects when the target is not running", async () => {
    const pid = makePid(new Echo(), "dead");
    await expect(external.ask(pid, "ping", 1000)).rejects.toBe(ErrDead);

    await pid.start();
    await pid.shutdown();
    await expect(external.ask(pid, "ping", 1000)).rejects.toBe(ErrDead);
  });

  it("falls back to the system askTimeout for a non-positive timeout", async () => {
    const pid = makePid(new Echo(), "timeout-check");
    await pid.start();

    // A non-positive timeout is no longer rejected; it falls back to
    // the system's askTimeout, so a responsive target still answers.
    await expect(external.ask(pid, "ping", 0)).resolves.toBe("ping");
    await expect(external.ask(pid, "ping", -1)).resolves.toBe("ping");
  });

  it("bounds a non-positive timeout by the configured askTimeout", async () => {
    class Silent implements Actor {
      preStart(): void {}
      receive(): void {}
      postStop(): void {}
    }

    // A small askTimeout proves the fallback is a real bound: an ask
    // with no timeout of its own still expires, never waits unbounded.
    const bounded = new ActorSystem("bounded", { askTimeout: 20 });
    const caller = new PID(
      { preStart(): void {}, receive(): void {}, postStop(): void {} },
      newPath("caller", "bounded", "127.0.0.1", 0),
      bounded,
    );
    const silent = new PID(new Silent(), newPath("silent", "bounded", "127.0.0.1", 0), bounded);
    await silent.start();

    await expect(caller.ask(silent, "ping", 0)).rejects.toBe(ErrRequestTimeout);
  });

  it("times out when the target never replies", async () => {
    class Silent implements Actor {
      preStart(): void {}
      receive(): void {}
      postStop(): void {}
    }

    const pid = makePid(new Silent(), "silent");
    await pid.start();

    await expect(external.ask(pid, "ping", 20)).rejects.toBe(ErrRequestTimeout);
  });

  it("keeps the first response and ignores a second", async () => {
    class Double implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        ctx.response("first");
        ctx.response("second");
      }

      postStop(): void {}
    }

    const pid = makePid(new Double(), "double");
    await pid.start();

    await expect(external.ask(pid, "ping", 1000)).resolves.toBe("first");
  });

  it("surfaces a mailbox rejection", async () => {
    class Slow implements Actor {
      started = false;

      preStart(): void {}

      async receive(ctx: ReceiveContext): Promise<void> {
        this.started = true;
        await new Promise((resolve) => setTimeout(resolve, 30));
        ctx.response("done");
      }

      postStop(): void {}
    }

    const actor = new Slow();
    const pid = makePid(actor, "bounded", new BoundedMailbox(1));
    await pid.start();

    const first = external.ask(pid, "one", 1000);
    await expect.poll(() => actor.started).toBe(true);
    expect(external.tell(pid, "fill")).toBeNull();
    await expect(external.ask(pid, "two", 1000)).rejects.toBe(ErrMailboxFull);
    await expect(first).resolves.toBe("done");
  });
});

describe("ReceiveContext ask and response", () => {
  it("asks another actor from receive", async () => {
    const echo = makePid(new Echo(), "echo-from-ctx");
    await echo.start();

    class Caller implements Actor {
      reply: unknown;

      preStart(): void {}

      async receive(ctx: ReceiveContext): Promise<void> {
        this.reply = await ctx.ask(echo, ctx.message, 1000);
      }

      postStop(): void {}
    }

    const actor = new Caller();
    const caller = makePid(actor, "caller");
    await caller.start();

    external.tell(caller, "hello");
    await expect.poll(() => actor.reply).toBe("hello");
  });

  it("is a no-op when response is called on a tell", async () => {
    class NoAsk implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        ctx.response("ignored");
      }

      postStop(): void {}
    }

    const pid = makePid(new NoAsk(), "no-ask");
    await pid.start();

    expect(external.tell(pid, "hi")).toBeNull();
    await expect.poll(() => pid.processedCount()).toBe(1);
    expect(() => createReceiveContext("x", pid).response("nope")).not.toThrow();
  });
});

describe("concurrent asks", () => {
  it("answers pending asks out of order", async () => {
    class Holder implements Actor {
      readonly held: ReceiveContext[] = [];

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (this.held.length < 2) {
          this.held.push(ctx);
          return;
        }

        // Reply to the middle ask first, then the oldest, then this one,
        // so pending asks leave the timeout bookkeeping in every order.
        const [first, second] = this.held as [ReceiveContext, ReceiveContext];
        second.response("second");
        first.response("first");
        ctx.response("third");
      }

      postStop(): void {}
    }

    const pid = makePid(new Holder(), "holder");
    await pid.start();

    const first = external.ask(pid, 1, 1000);
    const second = external.ask(pid, 2, 1000);
    const third = external.ask(pid, 3, 1000);

    await expect(second).resolves.toBe("second");
    await expect(first).resolves.toBe("first");
    await expect(third).resolves.toBe("third");
  });
});
