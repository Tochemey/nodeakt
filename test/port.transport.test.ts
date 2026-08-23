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

import { MessageChannel } from "node:worker_threads";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { BoundedMailbox } from "../src/bounded.mailbox";
import { discardLogger } from "../src/discard.logger";
import type { Envelope } from "../src/envelope";
import {
  ErrDead,
  ErrInvalidTimeout,
  ErrMailboxFull,
  ErrReentrancyDisabled,
  ErrReentrancyInFlightLimit,
  ErrRequestCanceled,
  ErrRequestTimeout,
  TypeNotRegisteredError,
} from "../src/errors";
import { MessageRegistry } from "../src/message.registry";
import { Deadletter, PostStart, Terminated } from "../src/messages";
import { parsePath } from "../src/path";
import type { PID } from "../src/pid";
import { PortTransport } from "../src/port.transport";
import type { ReceiveContext } from "../src/receive.context";
import type { RequestOptions } from "../src/reentrancy";

class Ping {
  constructor(readonly value: number) {}

  tag(): string {
    return `ping:${this.value}`;
  }
}

class Pong {
  constructor(readonly value: number) {}

  tag(): string {
    return `pong:${this.value}`;
  }
}

class Bare {
  constructor(readonly value: number) {}
}

/**
 * Records every message with its sender path and answers asks: a Ping
 * gets a Pong back, special string commands answer with values the
 * codec or clone must refuse, "silent" never answers, and anything
 * else echoes.
 */
class Recorder implements Actor {
  readonly seen: unknown[] = [];
  readonly senderPaths: string[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.seen.push(ctx.message);
    this.senderPaths.push((ctx.sender as PID).path().toString());

    if (ctx.message instanceof Ping) {
      ctx.response(new Pong((ctx.message as Ping).value + 1));
      return;
    }

    if (ctx.message === "unregistered-reply") {
      ctx.response(new Bare(1));
      return;
    }

    if (ctx.message === "poisoned-reply") {
      ctx.response({ fn: () => 1 });
      return;
    }

    if (ctx.message === "silent") {
      return;
    }

    ctx.response(`echo:${String(ctx.message)}`);
  }

  postStop(): void {}
}

/** Parks its behavior on a gate until the test releases it; a released
 * gate stays open so queued messages drain. */
class Gated implements Actor {
  private _release: (() => void) | null = null;
  private _open = false;
  entered = 0;

  preStart(): void {}

  async receive(ctx: ReceiveContext): Promise<void> {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.entered++;
    if (!this._open) {
      await new Promise<void>((resolve) => {
        this._release = resolve;
      });
    }

    ctx.response("late");
  }

  postStop(): void {}

  release(): void {
    this._open = true;
    this._release?.();
    this._release = null;
  }
}

describe("PortTransport", () => {
  let system: ActorSystem;
  let registry: MessageRegistry;
  let channel: MessageChannel;
  let near: PortTransport;
  let far: PortTransport;
  let letters: Deadletter[];
  let gated: Gated[];
  let extraTransports: PortTransport[];

  beforeEach(async () => {
    system = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
    registry = new MessageRegistry();
    registry.register(Ping);
    registry.register(Pong);
    channel = new MessageChannel();
    near = new PortTransport(system, registry, channel.port1, 0, 0);
    far = new PortTransport(system, registry, channel.port2, 0, 0);
    letters = [];
    gated = [];
    extraTransports = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        letters.push(event);
      }
    });
  });

  afterEach(async () => {
    for (const actor of gated) {
      actor.release();
    }

    for (const transport of extraTransports) {
      transport.close();
    }

    near.close();
    far.close();
    await system.stop();
  });

  describe("tell", () => {
    it("delivers with the prototype restored and the sender attributed", async () => {
      const actor = new Recorder();
      const target = await system.spawn("echo", actor);
      const sender = await system.spawn("caller", new Recorder());

      expect(near.tell(target.path(), new Ping(7), sender)).toBeNull();

      await expect.poll(() => actor.seen.length).toBe(1);
      const message = actor.seen[0] as Ping;
      expect(message).toBeInstanceOf(Ping);
      expect(message.tag()).toBe("ping:7");
      expect(actor.senderPaths[0]).toBe(sender.path().toString());
    });

    it("attributes to NoSender when no sender is given", async () => {
      const actor = new Recorder();
      const target = await system.spawn("echo", actor);

      expect(near.tell(target.path(), "hello")).toBeNull();

      await expect.poll(() => actor.seen).toEqual(["hello"]);
      expect(actor.senderPaths[0]).toBe(system.noSender().path().toString());
    });

    it("falls back to NoSender when the sender is gone at delivery", async () => {
      const actor = new Recorder();
      const target = await system.spawn("echo", actor);
      const sender = await system.spawn("goner", new Recorder());
      await sender.shutdown();

      expect(near.tell(target.path(), "orphan", sender)).toBeNull();

      await expect.poll(() => actor.seen).toEqual(["orphan"]);
      expect(actor.senderPaths[0]).toBe(system.noSender().path().toString());
    });

    it("preserves post order", async () => {
      const actor = new Recorder();
      const target = await system.spawn("echo", actor);

      near.tell(target.path(), "first");
      near.tell(target.path(), "second");

      await expect.poll(() => actor.seen).toEqual(["first", "second"]);
    });

    it("dead-letters an unresolvable path", async () => {
      const sender = await system.spawn("caller", new Recorder());

      const to = parsePath("nodeakt://sys@127.0.0.1:0/nobody");
      expect(near.tell(to, new Ping(1), sender)).toBeNull();

      await expect.poll(() => letters.length).toBe(1);
      expect(letters[0]?.receiver).toBe(to.toString());
      expect(letters[0]?.sender).toBe(sender.path().toString());
      expect(letters[0]?.reason).toBe(ErrDead.message);
      expect(letters[0]?.message).toBeInstanceOf(Ping);
    });

    it("dead-letters a stale incarnation instead of the name's new tenant", async () => {
      const first = await system.spawn("tenant", new Recorder());
      const stale = first.path();
      await first.shutdown();
      const second = new Recorder();
      await system.spawn("tenant", second);

      expect(near.tell(stale, "for-the-dead")).toBeNull();

      await expect.poll(() => letters.length).toBe(1);
      expect(second.seen).toEqual([]);
    });

    it("reports transport accept while a full mailbox dead-letters", async () => {
      const actor = new Gated();
      gated.push(actor);
      const target = await system.spawn("gated", actor, { mailbox: new BoundedMailbox(1) });

      system.noSender().tell(target, "hold");
      await expect.poll(() => actor.entered).toBe(1);
      system.noSender().tell(target, "filler");

      expect(near.tell(target.path(), "overflow")).toBeNull();

      await expect.poll(() => letters.length).toBe(1);
      expect(letters[0]?.reason).toBe(ErrMailboxFull.message);
    });

    it("returns the encode failure for an unregistered message", async () => {
      const target = await system.spawn("echo", new Recorder());

      const err = near.tell(target.path(), new Bare(1));

      expect(err).toBeInstanceOf(TypeNotRegisteredError);
      await expect.poll(() => letters.length).toBe(1);
      expect(letters[0]?.reason).toBe('message type "Bare" is not registered');
    });

    it("returns the clone failure for uncloneable passthrough data", async () => {
      const target = await system.spawn("echo", new Recorder());

      const err = near.tell(target.path(), { fn: () => 1 });

      expect(err).toBeInstanceOf(Error);
      expect(err?.name).toBe("DataCloneError");
      await expect.poll(() => letters.length).toBe(1);
    });
  });

  describe("ask", () => {
    it("round-trips with prototypes restored both ways", async () => {
      const target = await system.spawn("echo", new Recorder());

      const reply = await near.ask(target.path(), new Ping(7), 1000);

      expect(reply).toBeInstanceOf(Pong);
      expect((reply as Pong).tag()).toBe("pong:8");
    });

    it("round-trips passthrough replies", async () => {
      const target = await system.spawn("echo", new Recorder());

      await expect(near.ask(target.path(), "hi", 1000)).resolves.toBe("echo:hi");
    });

    it("rejects a non-positive timeout", async () => {
      const target = await system.spawn("echo", new Recorder());

      await expect(near.ask(target.path(), "hi", 0)).rejects.toBe(ErrInvalidTimeout);
    });

    it("rejects with the ErrDead sentinel for an unresolvable path", async () => {
      const to = parsePath("nodeakt://sys@127.0.0.1:0/nobody");

      await expect(near.ask(to, new Ping(1), 1000)).rejects.toBe(ErrDead);
      await expect.poll(() => letters.length).toBe(1);
      expect(letters[0]?.sender).toBeUndefined();
    });

    it("rejects with the mailbox error when the target rejects delivery", async () => {
      const actor = new Gated();
      gated.push(actor);
      const target = await system.spawn("gated", actor, { mailbox: new BoundedMailbox(1) });

      system.noSender().tell(target, "hold");
      await expect.poll(() => actor.entered).toBe(1);
      system.noSender().tell(target, "filler");

      await expect(near.ask(target.path(), new Ping(1), 1000)).rejects.toBe(ErrMailboxFull);
      await expect.poll(() => letters.length).toBe(1);
    });

    it("rejects with the coarse timeout when the target never answers", async () => {
      const actor = new Recorder();
      const target = await system.spawn("echo", actor);

      await expect(near.ask(target.path(), "silent", 30)).rejects.toBe(ErrRequestTimeout);

      // The receiving side expires its own context afterwards; the
      // trailing failed reply finds no pending entry and is dropped.
      await new Promise((resolve) => setTimeout(resolve, 80));
      expect(actor.seen).toEqual(["silent"]);
    });

    it("rejects with the encode failure for an unregistered message", async () => {
      const target = await system.spawn("echo", new Recorder());

      await expect(near.ask(target.path(), new Bare(1), 1000)).rejects.toBeInstanceOf(
        TypeNotRegisteredError,
      );
      await expect.poll(() => letters.length).toBe(1);
    });

    it("rejects with the clone failure for uncloneable passthrough data", async () => {
      const target = await system.spawn("echo", new Recorder());

      await expect(near.ask(target.path(), { fn: () => 1 }, 1000)).rejects.toHaveProperty(
        "name",
        "DataCloneError",
      );
      await expect.poll(() => letters.length).toBe(1);
    });

    it("rejects when the reply value has no registration", async () => {
      const target = await system.spawn("echo", new Recorder());

      await expect(near.ask(target.path(), "unregistered-reply", 1000)).rejects.toHaveProperty(
        "name",
        "TypeNotRegisteredError",
      );
    });

    it("rejects when the reply value cannot clone", async () => {
      const target = await system.spawn("echo", new Recorder());

      await expect(near.ask(target.path(), "poisoned-reply", 1000)).rejects.toHaveProperty(
        "name",
        "DataCloneError",
      );
    });
  });

  describe("request", () => {
    async function reentrantSender(options?: { maxInFlight?: number }): Promise<PID> {
      return system.spawn("requester", new Recorder(), {
        reentrancy: { mode: "allowAll", ...options },
      });
    }

    it("runs the continuation with the reply prototype restored", async () => {
      const sender = await reentrantSender();
      const target = await system.spawn("echo", new Recorder());
      const outcomes: Array<{ reply: unknown; error: Error | null }> = [];

      near.request(target.path(), new Ping(7), sender).onReply((reply, error) => {
        outcomes.push({ reply, error });
      });

      await expect.poll(() => outcomes.length).toBe(1);
      expect(outcomes[0]?.error).toBeNull();
      const reply = outcomes[0]?.reply as Pong;
      expect(reply).toBeInstanceOf(Pong);
      expect(reply.tag()).toBe("pong:8");
    });

    it("refuses a sender without reentrancy", async () => {
      const sender = await system.spawn("plain", new Recorder());
      const target = await system.spawn("echo", new Recorder());
      let refusal: Error | null = null;

      near.request(target.path(), new Ping(1), sender).onReply((_reply, error) => {
        refusal = error;
      });

      expect(refusal).toBe(ErrReentrancyDisabled);
    });

    it("enforces the in-flight cap across transport requests", async () => {
      const sender = await reentrantSender({ maxInFlight: 1 });
      const target = await system.spawn("echo", new Recorder());
      let refusal: Error | null = null;

      near.request(target.path(), "silent", sender);
      near.request(target.path(), new Ping(1), sender).onReply((_reply, error) => {
        refusal = error;
      });

      expect(refusal).toBe(ErrReentrancyInFlightLimit);
    });

    it("cancels into the canceled sentinel while the message still lands", async () => {
      const sender = await reentrantSender();
      const actor = new Recorder();
      const target = await system.spawn("echo", actor);
      const errors: Array<Error | null> = [];

      const call = near.request(target.path(), "silent", sender);
      call.onReply((_reply, error) => {
        errors.push(error);
      });
      call.cancel();

      await expect.poll(() => errors.length).toBe(1);
      expect(errors[0]).toBe(ErrRequestCanceled);
      await expect.poll(() => actor.seen).toEqual(["silent"]);
    });

    it("completes with the coarse timeout when the target never answers", async () => {
      const sender = await reentrantSender();
      const target = await system.spawn("echo", new Recorder());
      const errors: Array<Error | null> = [];

      near.request(target.path(), "silent", sender, { timeout: 30 }).onReply((_reply, error) => {
        errors.push(error);
      });

      await expect.poll(() => errors.length).toBe(1);
      expect(errors[0]).toBe(ErrRequestTimeout);
    });

    it("completes with ErrDead for an unresolvable path", async () => {
      const sender = await reentrantSender();
      const errors: Array<Error | null> = [];

      near
        .request(parsePath("nodeakt://sys@127.0.0.1:0/nobody"), new Ping(1), sender)
        .onReply((_reply, error) => {
          errors.push(error);
        });

      await expect.poll(() => errors.length).toBe(1);
      expect(errors[0]).toBe(ErrDead);
      expect(letters.length).toBe(1);
    });

    it("refuses invalid and off mode overrides", async () => {
      const sender = await reentrantSender();
      const target = await system.spawn("echo", new Recorder());
      const errors: Error[] = [];

      const collect = (options: RequestOptions): void => {
        near.request(target.path(), new Ping(1), sender, options).onReply((_reply, error) => {
          errors.push(error as Error);
        });
      };

      collect({ mode: "off" });
      collect({ mode: "bogus" } as unknown as RequestOptions);

      expect(errors.map((e) => e.message)).toEqual([
        "reentrancy is disabled",
        "invalid reentrancy mode",
      ]);
    });

    it("completes with the encode failure for an unregistered message", async () => {
      const sender = await reentrantSender();
      const target = await system.spawn("echo", new Recorder());
      const errors: Array<Error | null> = [];

      near.request(target.path(), new Bare(1), sender).onReply((_reply, error) => {
        errors.push(error);
      });

      expect(errors[0]).toBeInstanceOf(TypeNotRegisteredError);
      await expect.poll(() => letters.length).toBe(1);
    });

    it("completes with the clone failure for uncloneable passthrough data", async () => {
      const sender = await reentrantSender();
      const target = await system.spawn("echo", new Recorder());
      const errors: Array<Error | null> = [];

      near.request(target.path(), { fn: () => 1 }, sender).onReply((_reply, error) => {
        errors.push(error);
      });

      expect(errors[0]?.name).toBe("DataCloneError");
      await expect.poll(() => letters.length).toBe(1);
    });
  });

  describe("close", () => {
    it("settles pending asks and requests with ErrDead and refuses further sends", async () => {
      const actor = new Gated();
      gated.push(actor);
      const target = await system.spawn("gated", actor);
      const sender = await system.spawn("requester", new Recorder(), {
        reentrancy: { mode: "allowAll" },
      });
      const requestErrors: Array<Error | null> = [];

      const pendingAsk = near.ask(target.path(), "hold", 5000);
      near.request(target.path(), "hold-too", sender).onReply((_reply, error) => {
        requestErrors.push(error);
      });
      await expect.poll(() => actor.entered).toBe(1);

      near.close();

      await expect(pendingAsk).rejects.toBe(ErrDead);
      await expect.poll(() => requestErrors.length).toBe(1);
      expect(requestErrors[0]).toBe(ErrDead);

      expect(near.tell(target.path(), "later")).toBe(ErrDead);
      await expect(near.ask(target.path(), "later", 1000)).rejects.toBe(ErrDead);
      let refusal: Error | null = null;
      near.request(target.path(), "later", sender).onReply((_reply, error) => {
        refusal = error;
      });
      expect(refusal).toBe(ErrDead);
    });

    it("is idempotent", () => {
      near.close();

      expect(() => near.close()).not.toThrow();
    });

    it("dead-letters a queued message and drops a queued reply behind a close", async () => {
      const other = new Recorder();
      const otherPid = await system.spawn("other", other);

      // The first envelope's delivery closes this end; the two queued
      // behind it still fire (a queued port message survives close):
      // the tell becomes a dead letter, the reply is dropped.
      class Closer implements Actor {
        preStart(): void {}

        receive(ctx: ReceiveContext): void {
          if (ctx.message instanceof PostStart) {
            return;
          }

          near.close();
        }

        postStop(): void {}
      }

      const closer = await system.spawn("closer", new Closer());

      far.tell(closer.path(), "close-near");
      far.tell(otherPid.path(), "straggler");
      channel.port2.postMessage({
        kind: "reply",
        to: "",
        uid: "",
        sender: "",
        cid: 999,
        timeout: 0,
        message: { type: "", data: "late" },
        error: null,
      });

      await expect.poll(() => letters.length).toBe(1);
      expect(letters[0]?.message).toBe("straggler");
      expect(letters[0]?.reason).toBe(ErrDead.message);
      expect(other.seen).toEqual([]);

      // The dropped reply produces nothing: no dead letter, no crash.
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(letters.length).toBe(1);
    });

    it("refuses sends once the system is stopped", async () => {
      const other = new ActorSystem("other", { logger: discardLogger });
      await other.start();
      const channel = new MessageChannel();
      const stoppedNear = new PortTransport(other, registry, channel.port1, 0, 0);
      const stoppedFar = new PortTransport(other, registry, channel.port2, 0, 0);
      extraTransports.push(stoppedNear, stoppedFar);
      const target = await other.spawn("echo", new Recorder());
      const path = target.path();
      await other.stop();

      expect(stoppedNear.tell(path, "hi")).toBe(ErrDead);
      await expect(stoppedNear.ask(path, "hi", 1000)).rejects.toBe(ErrDead);
    });
  });

  describe("buffer transfer", () => {
    it("transfers ArrayBuffers to the far side instead of copying them", async () => {
      const actor = new Recorder();
      const target = await system.spawn("sink", actor);
      const bytes = new Uint8Array([1, 2, 3, 4]);
      const buffer = bytes.buffer;

      expect(near.tell(target.path(), { payload: buffer })).toBeNull();
      expect(buffer.byteLength).toBe(0);

      await expect.poll(() => actor.seen.length).toBe(1);
      const received = (actor.seen[0] as { payload: ArrayBuffer }).payload;
      expect(received).toBeInstanceOf(ArrayBuffer);
      expect([...new Uint8Array(received)]).toEqual([1, 2, 3, 4]);
    });

    it("transfers the buffer under a typed-array view", async () => {
      const actor = new Recorder();
      const target = await system.spawn("sink", actor);
      const view = new Float64Array([1.5, 2.5]);

      expect(near.tell(target.path(), { view })).toBeNull();
      expect(view.buffer.byteLength).toBe(0);

      await expect.poll(() => actor.seen.length).toBe(1);
      const received = (actor.seen[0] as { view: Float64Array }).view;
      expect([...received]).toEqual([1.5, 2.5]);
    });

    it("transfers a buffer referenced twice exactly once, keeping the shared identity", async () => {
      const actor = new Recorder();
      const target = await system.spawn("sink", actor);
      const buffer = new Uint8Array([9]).buffer;

      expect(near.tell(target.path(), { a: buffer, b: buffer })).toBeNull();
      expect(buffer.byteLength).toBe(0);

      await expect.poll(() => actor.seen.length).toBe(1);
      const received = actor.seen[0] as { a: ArrayBuffer; b: ArrayBuffer };
      expect(received.a).toBe(received.b);
    });

    it("walks arrays, maps, sets, and cycles, leaving shared memory attached", async () => {
      const actor = new Recorder();
      const target = await system.spawn("sink", actor);
      const inMap = new Uint8Array([1]).buffer;
      const inSet = new Uint8Array([2]).buffer;
      const inArray = new Uint8Array([3]).buffer;
      const shared = new SharedArrayBuffer(4);
      const message: Record<string, unknown> = {
        m: new Map([["k", inMap]]),
        s: new Set([inSet]),
        l: [inArray],
        shared,
      };
      message.self = message;

      expect(near.tell(target.path(), message)).toBeNull();
      expect(inMap.byteLength).toBe(0);
      expect(inSet.byteLength).toBe(0);
      expect(inArray.byteLength).toBe(0);
      expect(shared.byteLength).toBe(4);

      await expect.poll(() => actor.seen.length).toBe(1);
    });
  });

  describe("cross-isolate sender identity", () => {
    it("resolves a foreign sender to a routed handle, never to a local actor", async () => {
      const foreign = new MessageChannel();
      const a = new PortTransport(system, registry, foreign.port1, 0, 1);
      const b = new PortTransport(system, registry, foreign.port2, 1, 0);
      extraTransports.push(a, b);

      const seen: PID[] = [];
      const target = await system.spawn("probe", {
        preStart(): void {},
        receive(ctx: ReceiveContext): void {
          if (ctx.message instanceof PostStart) {
            return;
          }

          seen.push(ctx.sender as PID);
        },
        postStop(): void {},
      });
      const sender = await system.spawn("caller", new Recorder());

      expect(a.tell(target.path(), "who-sent-this", sender)).toBeNull();
      expect(a.tell(target.path(), "and-this", sender)).toBeNull();

      await expect.poll(() => seen.length).toBe(2);
      const handle = seen[0] as PID;

      // The handle carries the sender's identity but is not the local
      // actor that happens to share the path: it routes instead.
      expect(handle.path().toString()).toBe(sender.path().toString());
      expect(handle).not.toBe(sender);
      expect(handle.isRunning()).toBe(false);

      // Handle identity is stable across messages, so watch removal
      // and sender comparison by identity work.
      expect(seen[1]).toBe(handle);

      // Replying to the handle routes back through the port and, in
      // this loopback pair, lands on the real caller.
      const caller = system.actorOf("caller") as PID;
      const recorder = caller.actor() as Recorder;
      expect(handle.isRunning()).toBe(false);
      expect(target.tell(handle, "returned")).toBeNull();
      await expect.poll(() => recorder.seen).toEqual(["returned"]);
    });
  });

  describe("registry asymmetry", () => {
    function pair(
      nearRegistry: MessageRegistry,
      farRegistry: MessageRegistry,
    ): { a: PortTransport; b: PortTransport } {
      const asymmetric = new MessageChannel();
      const a = new PortTransport(system, nearRegistry, asymmetric.port1, 0, 0);
      const b = new PortTransport(system, farRegistry, asymmetric.port2, 0, 0);
      extraTransports.push(a, b);
      return { a, b };
    }

    it("dead-letters a tell the receiving side cannot decode and keeps serving", async () => {
      const nearOnly = new MessageRegistry();
      nearOnly.register(Ping);
      const { a } = pair(nearOnly, new MessageRegistry());

      const actor = new Recorder();
      const target = await system.spawn("sink", actor);
      const sender = await system.spawn("caller", new Recorder());

      expect(a.tell(target.path(), new Ping(3), sender)).toBeNull();

      await expect.poll(() => letters.length).toBe(1);
      expect(letters[0]?.reason).toContain("is not registered");
      expect(letters[0]?.message).toEqual({ value: 3 });
      expect(letters[0]?.sender).toBe(sender.path().toString());
      expect(actor.seen).toEqual([]);

      expect(a.tell(target.path(), "after")).toBeNull();
      await expect.poll(() => actor.seen.length).toBe(1);
      expect(actor.seen[0]).toBe("after");
    });

    it("fails an ask the receiving side cannot decode instead of stranding it", async () => {
      const nearOnly = new MessageRegistry();
      nearOnly.register(Ping);
      const { a } = pair(nearOnly, new MessageRegistry());
      const target = await system.spawn("sink", new Recorder());

      await expect(a.ask(target.path(), new Ping(1), 5000)).rejects.toHaveProperty(
        "name",
        "TypeNotRegisteredError",
      );
    });

    it("fails an ask whose reply this side cannot decode", async () => {
      const nearOnly = new MessageRegistry();
      nearOnly.register(Ping);
      const both = new MessageRegistry();
      both.register(Ping);
      both.register(Pong);
      const { a } = pair(nearOnly, both);
      const target = await system.spawn("sink", new Recorder());

      await expect(a.ask(target.path(), new Ping(1), 5000)).rejects.toBeInstanceOf(
        TypeNotRegisteredError,
      );
    });

    it("survives garbage frames on the port", async () => {
      const actor = new Recorder();
      const target = await system.spawn("sink", actor);

      channel.port1.postMessage(null);
      channel.port1.postMessage({ half: "an envelope" });

      expect(near.tell(target.path(), "after-garbage")).toBeNull();
      await expect.poll(() => actor.seen.length).toBe(1);
      expect(actor.seen[0]).toBe("after-garbage");
    });

    it("falls back to raw wire data for an undecodable envelope arriving after close", async () => {
      const b = new PortTransport(system, new MessageRegistry(), new MessageChannel().port2, 0, 0);
      extraTransports.push(b);
      b.close();

      (b as unknown as { deliver(envelope: Envelope): void }).deliver({
        kind: "tell",
        to: "nodeakt://sys@127.0.0.1:0/ghost",
        uid: "",
        sender: "",
        senderUid: "",
        senderWorkerId: 0,
        cid: 0,
        timeout: 0,
        message: { type: "test.Missing", data: { value: 9 } },
        error: null,
      });

      await expect.poll(() => letters.length).toBe(1);
      expect(letters[0]?.message).toEqual({ value: 9 });
      expect(letters[0]?.reason).toBe(ErrDead.message);
    });
  });

  describe("death watch across the port", () => {
    it("delivers Terminated when the watched actor stops, exactly once", async () => {
      const target = await system.spawn("doomed", new Recorder());
      const watching = new Recorder();
      const watcher = await system.spawn("observer", watching);

      near.watch(target.path(), watcher);
      await new Promise((settle) => setTimeout(settle, 20));

      await target.shutdown();

      await expect.poll(() => watching.seen.length).toBe(1);
      const notice = watching.seen[0] as Terminated;
      expect(notice).toBeInstanceOf(Terminated);
      expect(notice.actorPath).toBe(target.path().toString());

      // The arrival settled the sender-side entry, so closing delivers
      // no duplicate.
      near.close();
      await new Promise((settle) => setTimeout(settle, 20));
      expect(watching.seen.length).toBe(1);
    });

    it("stays silent after an unwatch", async () => {
      const target = await system.spawn("spared", new Recorder());
      const watching = new Recorder();
      const watcher = await system.spawn("observer", watching);

      near.watch(target.path(), watcher);
      await new Promise((settle) => setTimeout(settle, 20));
      near.unwatch(target.path(), watcher);
      await new Promise((settle) => setTimeout(settle, 20));

      await target.shutdown();
      await new Promise((settle) => setTimeout(settle, 30));

      expect(watching.seen).toEqual([]);
    });

    it("answers a watch on a gone actor with an immediate Terminated", async () => {
      const target = await system.spawn("already-gone", new Recorder());
      const stale = target.path();
      await target.shutdown();
      const watching = new Recorder();
      const watcher = await system.spawn("observer", watching);

      near.watch(stale, watcher);

      await expect.poll(() => watching.seen.length).toBe(1);
      expect((watching.seen[0] as Terminated).actorPath).toBe(stale.toString());
    });

    it("sweeps Terminated to every watcher when the transport closes", async () => {
      const target = await system.spawn("stranded", new Recorder());
      const watching = new Recorder();
      const watcher = await system.spawn("observer", watching);

      near.watch(target.path(), watcher);
      await new Promise((settle) => setTimeout(settle, 20));

      near.close();

      await expect.poll(() => watching.seen.length).toBe(1);
      expect((watching.seen[0] as Terminated).actorPath).toBe(target.path().toString());
    });

    it("ignores watches and unwatches through a closed transport", async () => {
      const target = await system.spawn("unseen", new Recorder());
      const watching = new Recorder();
      const watcher = await system.spawn("observer", watching);

      near.close();
      near.watch(target.path(), watcher);
      near.unwatch(target.path(), watcher);

      await new Promise((settle) => setTimeout(settle, 30));
      expect(watching.seen).toEqual([]);
    });

    it("drops forged watch frames without a sender and unwatches of unknown targets", async () => {
      const watching = new Recorder();
      const watcher = await system.spawn("observer", watching);

      channel.port1.postMessage({
        kind: "watch",
        to: "nodeakt://sys@127.0.0.1:0/anyone",
        uid: "",
        sender: "",
        senderUid: "",
        senderWorkerId: 0,
        cid: 0,
        timeout: 0,
        message: null,
        error: null,
      });
      channel.port1.postMessage({
        kind: "unwatch",
        to: "nodeakt://sys@127.0.0.1:0/anyone",
        uid: "",
        sender: "",
        senderUid: "",
        senderWorkerId: 0,
        cid: 0,
        timeout: 0,
        message: null,
        error: null,
      });
      near.unwatch(parsePath("nodeakt://sys@127.0.0.1:0/nobody"), watcher);

      await new Promise((settle) => setTimeout(settle, 30));
      expect(watching.seen).toEqual([]);
    });

    it("drops watch envelopes arriving after close without dead-lettering them", async () => {
      const b = new PortTransport(system, new MessageRegistry(), new MessageChannel().port2, 0, 0);
      extraTransports.push(b);
      b.close();

      (b as unknown as { deliver(envelope: Envelope): void }).deliver({
        kind: "watch",
        to: "nodeakt://sys@127.0.0.1:0/ghost",
        uid: "",
        sender: "nodeakt://sys@127.0.0.1:0/someone",
        senderUid: "",
        senderWorkerId: 0,
        cid: 0,
        timeout: 0,
        message: null,
        error: null,
      });

      await new Promise((settle) => setTimeout(settle, 20));
      expect(letters).toEqual([]);
    });

    it("skips the close sweep once its own system has stopped", async () => {
      const other = new ActorSystem("otherwatch", { logger: discardLogger });
      await other.start();
      const pair = new MessageChannel();
      const otherNear = new PortTransport(other, registry, pair.port1, 0, 0);
      const otherFar = new PortTransport(other, registry, pair.port2, 0, 0);
      extraTransports.push(otherNear, otherFar);

      const target = await other.spawn("t", new Recorder());
      const watcher = await other.spawn("w", new Recorder());
      otherNear.watch(target.path(), watcher);
      await new Promise((settle) => setTimeout(settle, 20));

      await other.stop();
      expect(() => otherNear.close()).not.toThrow();
    });
  });

  describe("the back route", () => {
    it("exposes a route whose sends, requests, and watches ride this transport", async () => {
      const actor = new Recorder();
      const target = await system.spawn("echo", actor);
      const route = far.route();
      expect(route.workerId).toBe(0);

      expect(route.tell(target.path(), "one")).toBeNull();
      await expect(route.ask(target.path(), "two", 5000)).resolves.toBe("echo:two");

      const reentrant = await system.spawn("requester", new Recorder(), {
        reentrancy: { mode: "allowAll" },
      });
      const outcome = await new Promise<{ reply: unknown; error: Error | null }>((settle) => {
        route.request(target.path(), "three", reentrant).onReply((reply, error) => {
          settle({ reply, error });
        });
      });
      expect(outcome.error).toBeNull();
      expect(outcome.reply).toBe("echo:three");

      // A watch issued and cancelled through the route stays silent.
      const watching = new Recorder();
      const watcher = await system.spawn("observer", watching);
      route.watch(target.path(), watcher);
      await new Promise((settle) => setTimeout(settle, 20));
      route.unwatch(target.path(), watcher);
      await new Promise((settle) => setTimeout(settle, 20));
      await target.shutdown();
      await new Promise((settle) => setTimeout(settle, 30));
      expect(watching.seen).toEqual([]);
    });
  });

  describe("PID.deliverAsk", () => {
    it("refuses an actor that is not running", async () => {
      const target = await system.spawn("stopper", new Recorder());
      await target.shutdown();

      const err = target.deliverAsk(
        "late",
        system.noSender(),
        1000,
        () => {},
        () => {},
      );

      expect(err).toBe(ErrDead);
      await expect.poll(() => letters.length).toBe(1);
      expect(letters[0]?.message).toBe("late");
    });
  });
});
