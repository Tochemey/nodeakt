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
import type { Actor } from "../src/actor/actor";
import { ActorRef, type IsolateRoute } from "../src/actor/actor.ref";
import { ActorSystem } from "../src/actor/actor.system";
import { Deadletter, PostStart } from "../src/actor/messages";
import type { PID } from "../src/actor/pid";
import type { ReceiveContext } from "../src/actor/receive.context";
import { completedRequest } from "../src/actor/reentrancy";
import { ErrDead, ErrRequestTimeout } from "../src/errors/errors";
import { discardLogger } from "../src/logger/discard.logger";

/** Records every message and answers asks with an echo. */
class Recorder implements Actor {
  readonly seen: unknown[] = [];
  readonly senders: Array<PID | undefined> = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.seen.push(ctx.message);
    this.senders.push(ctx.sender);
    ctx.response(`echo:${String(ctx.message)}`);
  }

  postStop(): void {}
}

describe("ActorRef", () => {
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

  it("tells through a pinned ref with NoSender attribution by default", async () => {
    const actor = new Recorder();
    const pid = await system.spawn("recorder", actor);
    const ref = pid.ref();

    expect(ref.tell("hello")).toBeNull();
    await expect.poll(() => actor.seen).toEqual(["hello"]);
    expect(actor.senders[0]).toBe(system.noSender());

    expect(ref.id()).toBe(pid.path().toString());
    expect(ref.path().uid()).toBe(pid.path().uid());
    expect(ref.isRunning()).toBe(true);
  });

  it("records an explicit sender", async () => {
    const actor = new Recorder();
    const pid = await system.spawn("recorder-sender", actor);
    const sender = await system.spawn("sender", new Recorder());

    expect(pid.ref().tell("from-actor", sender)).toBeNull();
    await expect.poll(() => actor.seen).toEqual(["from-actor"]);
    expect(actor.senders[0]).toBe(sender);
  });

  it("asks through a ref", async () => {
    const pid = await system.spawn("asked", new Recorder());

    await expect(pid.ref().ask("ping", 1000)).resolves.toBe("echo:ping");
  });

  it("requests through a ref on behalf of a reentrant sender", async () => {
    const outcomes: Array<{ reply: unknown; error: Error | null }> = [];

    class Requesting implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message instanceof PostStart) {
          return;
        }

        (ctx.message as ActorRef).request("ping", ctx.self as PID).onReply((reply, error) => {
          outcomes.push({ reply, error });
        });
      }

      postStop(): void {}
    }

    const requester = await system.spawn("ref-requester", new Requesting(), {
      reentrancy: { mode: "allowAll" },
    });
    const target = await system.spawn("ref-target", new Recorder());

    system.noSender().tell(requester, target.ref());
    await expect.poll(() => outcomes.length).toBe(1);
    expect(outcomes[0]).toEqual({ reply: "echo:ping", error: null });

    const stale = await system.spawn("ref-gone", new Recorder());
    const staleRef = stale.ref();
    await stale.shutdown();
    system.noSender().tell(requester, staleRef);
    await expect.poll(() => outcomes.length).toBe(2);
    expect(outcomes[1]?.error).toBe(ErrDead);
  });

  it("resolves an address ref minted from a path string", async () => {
    const actor = new Recorder();
    await system.spawn("addressed", actor);

    const ref = system.refOf("nodeakt://sys@127.0.0.1:0/addressed");
    expect(ref.path().uid()).toBe("");
    expect(ref.isRunning()).toBe(true);
    expect(ref.tell("via-address")).toBeNull();
    await expect.poll(() => actor.seen).toEqual(["via-address"]);
  });

  it("resolves nested children through the name chain", async () => {
    class Parent implements Actor {
      preStart(): void {}

      async receive(ctx: ReceiveContext): Promise<void> {
        if (ctx.message instanceof PostStart) {
          await ctx.spawn("child", this.childActor);
        }
      }

      postStop(): void {}

      readonly childActor = new Recorder();
    }

    const parent = new Parent();
    const pid = await system.spawn("parent", parent);
    await expect.poll(() => pid.children().length).toBe(1);

    const ref = system.refOf("nodeakt://sys@127.0.0.1:0/parent/child");
    expect(ref.tell("deep")).toBeNull();
    await expect.poll(() => parent.childActor.seen).toEqual(["deep"]);

    const childRef = (pid.children()[0] as PID).ref();
    expect(childRef.tell("pinned-deep")).toBeNull();
    await expect.poll(() => parent.childActor.seen).toEqual(["deep", "pinned-deep"]);
  });

  it("refuses a stale incarnation while an address ref reaches the successor", async () => {
    const first = await system.spawn("reborn", new Recorder());
    const staleRef = first.ref();
    await first.shutdown();

    const secondActor = new Recorder();
    const second = await system.spawn("reborn", secondActor);

    expect(staleRef.isRunning()).toBe(false);
    expect(staleRef.tell("late")).toBe(ErrDead);
    await expect.poll(() => letters.length).toBe(1);
    expect((letters[0] as Deadletter).receiver).toBe(second.path().toString());
    expect((letters[0] as Deadletter).reason).toBe(ErrDead.message);
    await expect(staleRef.ask("late-ask", 100)).rejects.toBe(ErrDead);

    const addressRef = system.refOf("nodeakt://sys@127.0.0.1:0/reborn");
    expect(addressRef.tell("fresh")).toBeNull();
    await expect.poll(() => secondActor.seen).toEqual(["fresh"]);
    expect(second.ref().tell("fresh-pinned")).toBeNull();
    await expect.poll(() => secondActor.seen).toEqual(["fresh", "fresh-pinned"]);
  });

  it("compares identity by path and incarnation", async () => {
    const pid = await system.spawn("identity", new Recorder());

    expect(pid.ref().equals(pid.ref())).toBe(true);
    expect(pid.ref().equals(system.refOf(pid.path().toString()))).toBe(false);

    const addressA = system.refOf("nodeakt://sys@127.0.0.1:0/identity");
    const addressB = system.refOf("nodeakt://sys@127.0.0.1:0/identity");
    expect(addressA.equals(addressB)).toBe(true);

    const other = await system.spawn("identity2", new Recorder());
    expect(pid.ref().equals(other.ref())).toBe(false);

    const staleRef = pid.ref();
    await pid.shutdown();
    const successor = await system.spawn("identity", new Recorder());
    expect(staleRef.equals(successor.ref())).toBe(false);
  });

  it("treats unknown and foreign paths as dead", async () => {
    const missing = system.refOf("nodeakt://sys@127.0.0.1:0/nobody");
    expect(missing.isRunning()).toBe(false);
    expect(missing.tell("void")).toBe(ErrDead);
    await expect.poll(() => letters.length).toBe(1);

    const gap = system.refOf("nodeakt://sys@127.0.0.1:0/nobody/child");
    expect(gap.isRunning()).toBe(false);

    expect(system.refOf("nodeakt://other@127.0.0.1:0/x").isRunning()).toBe(false);
    expect(system.refOf("nodeakt://sys@10.0.0.1:0/x").isRunning()).toBe(false);
    expect(system.refOf("nodeakt://sys@127.0.0.1:9/x").isRunning()).toBe(false);

    expect(() => system.refOf("not-a-path")).toThrow(TypeError);
  });

  it("skips runtime announcements when a ref delivery fails", async () => {
    const pid = await system.spawn("ref-quiet", new Recorder());
    const staleRef = pid.ref();
    await pid.shutdown();

    expect(staleRef.tell(new PostStart())).toBe(ErrDead);
    expect(staleRef.tell("witness")).toBe(ErrDead);

    await expect.poll(() => letters.length).toBe(1);
    expect((letters[0] as Deadletter).message).toBe("witness");
  });

  it("fails without throwing once the system has stopped", async () => {
    const pid = await system.spawn("outliver", new Recorder());
    const ref = pid.ref();
    await system.stop();

    expect(ref.isRunning()).toBe(false);
    expect(ref.tell("after-stop")).toBe(ErrDead);
    await expect(ref.ask("after-stop", 100)).rejects.toBe(ErrDead);
    expect(letters).toEqual([]);

    await system.start();
  });

  it("times out an ask through a ref like a direct ask", async () => {
    class Silent implements Actor {
      preStart(): void {}

      receive(): void {}

      postStop(): void {}
    }

    const pid = await system.spawn("silent-ref", new Silent());
    await expect(pid.ref().ask("ping", 20)).rejects.toBe(ErrRequestTimeout);
  });

  describe("with an isolate route", () => {
    it("sends through the route instead of the local tree", async () => {
      const sent: string[] = [];
      const route: IsolateRoute = {
        workerId: 3,
        tell: (to, message) => {
          sent.push(`tell:${to.name()}:${String(message)}`);
          return null;
        },
        ask: (to, message) => Promise.resolve(`asked:${to.name()}:${String(message)}`),
        request: () => completedRequest(ErrDead),
        watch: () => {},
        unwatch: () => {},
      };

      // The path resolves locally, which is exactly what must NOT
      // happen: a routed ref belongs to another isolate.
      const local = new Recorder();
      const pid = await system.spawn("decoy", local);
      const ref = new ActorRef(system, pid.path(), route);

      expect(ref.workerId()).toBe(3);
      expect(ref.isRunning()).toBe(false);

      expect(ref.tell("hi")).toBeNull();
      await expect(ref.ask("there", 1000)).resolves.toBe("asked:decoy:there");

      let refusal: Error | null = null;
      ref.request("now", pid).onReply((_reply, error) => {
        refusal = error;
      });
      expect(refusal).toBe(ErrDead);

      expect(sent).toEqual(["tell:decoy:hi"]);
      expect(local.seen).toEqual([]);
    });

    it("reports a local ref as owned by no worker", async () => {
      const pid = await system.spawn("here", new Recorder());
      expect(pid.ref().workerId()).toBeNull();
    });
  });
});
