import { describe, expect, it } from "vitest";
import type { Actor } from "../src/actor/actor";
import { ActorSystem } from "../src/actor/actor.system";
import { BoundedMailbox } from "../src/actor/bounded.mailbox";
import {
  ErrDead,
  ErrInvalidReentrancyMode,
  ErrMailboxFull,
  ErrReentrancyDisabled,
  ErrReentrancyInFlightLimit,
  ErrRequestCanceled,
  ErrRequestTimeout,
} from "../src/actor/errors";
import { newPath } from "../src/actor/path";
import { PID } from "../src/actor/pid";
import type { ReceiveContext } from "../src/actor/receive.context";
import type { ReentrancyMode, RequestCall, RequestOptions } from "../src/actor/reentrancy";
import type { SpawnOptions } from "../src/actor/spawn.options";

const system = new ActorSystem("sys");

function makePid(actor: Actor, name: string, options?: SpawnOptions): PID {
  return new PID(actor, newPath(name, "sys", "127.0.0.1", 0), system, options);
}

const external = makePid(
  { preStart(): void {}, receive(): void {}, postStop(): void {} },
  "external",
);

/** Asks the requester to issue one request and record its outcome. */
class DoRequest {
  constructor(
    readonly to: PID,
    readonly payload: unknown,
    readonly options?: RequestOptions,
  ) {}
}

/** Asks the requester to issue one request without a continuation. */
class DoBareRequest extends DoRequest {}

/** Asks the requester to issue two requests within one message. */
class DoDoubleRequest {
  constructor(readonly to: PID) {}
}

/** Asks the holder to answer everything it held back. */
class Release {}

/**
 * An actor that issues requests on demand and records everything it
 * observes: request outcomes through continuations, and every other
 * message it processes.
 */
class Requester implements Actor {
  readonly log: string[] = [];
  readonly outcomes: Array<{ reply: unknown; error: Error | null }> = [];
  readonly calls: RequestCall[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const msg = ctx.message;

    if (msg instanceof DoBareRequest) {
      this.calls.push(ctx.request(msg.to, msg.payload, msg.options));
      return;
    }

    if (msg instanceof DoRequest) {
      const call = ctx.request(msg.to, msg.payload, msg.options);
      call.onReply((reply, error) => {
        this.log.push(`reply:${String(reply)}`);
        this.outcomes.push({ reply, error });
      });
      this.calls.push(call);
      return;
    }

    if (msg instanceof DoDoubleRequest) {
      for (const payload of ["one", "two"]) {
        ctx.request(msg.to, payload).onReply((reply, error) => {
          this.log.push(`reply:${String(reply)}`);
          this.outcomes.push({ reply, error });
        });
      }

      return;
    }

    this.log.push(`msg:${String(msg)}`);
    ctx.response(`echo:${String(msg)}`);
  }

  postStop(): void {}
}

/** A target that holds every message until it is released. */
class Holder implements Actor {
  readonly held: ReceiveContext[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Release) {
      for (const heldCtx of this.held) {
        heldCtx.response(`released:${String(heldCtx.message)}`);
      }

      this.held.length = 0;
      return;
    }

    this.held.push(ctx);
  }

  postStop(): void {}
}

/** A target that answers every request with the request itself. */
class Echo implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    ctx.response(ctx.message);
  }

  postStop(): void {}
}

async function startRequester(
  mode: ReentrancyMode,
  maxInFlight?: number,
): Promise<{
  actor: Requester;
  pid: PID;
}> {
  const actor = new Requester();
  const pid = makePid(actor, `requester-${mode}-${counter++}`, {
    reentrancy: maxInFlight === undefined ? { mode } : { mode, maxInFlight },
  });
  await pid.start();
  return { actor, pid };
}

let counter = 0;

describe("request admission", () => {
  it("completes with ErrReentrancyDisabled without a configuration", async () => {
    const actor = new Requester();
    const pid = makePid(actor, "plain-requester");
    await pid.start();

    const echo = makePid(new Echo(), "echo-admission");
    await echo.start();

    external.tell(pid, new DoRequest(echo, "ping"));
    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]?.error).toBe(ErrReentrancyDisabled);
  });

  it("completes with ErrReentrancyDisabled when spawned with mode off", async () => {
    const actor = new Requester();
    const pid = makePid(actor, "off-requester", { reentrancy: { mode: "off" } });
    await pid.start();

    const echo = makePid(new Echo(), "echo-off");
    await echo.start();

    external.tell(pid, new DoRequest(echo, "ping"));
    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]?.error).toBe(ErrReentrancyDisabled);
  });

  it("completes with ErrReentrancyDisabled on a per-call off override", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const echo = makePid(new Echo(), "echo-override-off");
    await echo.start();

    external.tell(pid, new DoRequest(echo, "unused", { mode: "off" }));
    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]?.error).toBe(ErrReentrancyDisabled);
  });

  it("rejects an invalid per-call mode override", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const echo = makePid(new Echo(), "echo-invalid-mode");
    await echo.start();

    external.tell(pid, new DoRequest(echo, "ping", { mode: "bogus" as ReentrancyMode }));
    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]?.error).toBe(ErrInvalidReentrancyMode);
  });

  it("rejects an invalid spawn configuration", () => {
    expect(() =>
      makePid(new Requester(), "bad-mode", {
        reentrancy: { mode: "bogus" as ReentrancyMode },
      }),
    ).toThrow(ErrInvalidReentrancyMode);
  });

  it("caps in-flight requests and frees the slot on completion", async () => {
    const { actor, pid } = await startRequester("allowAll", 1);
    const holderActor = new Holder();
    const holder = makePid(holderActor, "holder-cap");
    await holder.start();

    external.tell(pid, new DoRequest(holder, "first"));
    external.tell(pid, new DoRequest(holder, "second"));

    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]?.error).toBe(ErrReentrancyInFlightLimit);
    expect(pid.isIdle()).toBe(false);

    external.tell(holder, new Release());
    await expect.poll(() => actor.outcomes.length).toBe(2);
    expect(actor.outcomes[1]?.reply).toBe("released:first");
    expect(pid.isIdle()).toBe(true);

    external.tell(pid, new DoRequest(holder, "third"));
    await expect.poll(() => holderActor.held.length).toBe(1);
    external.tell(holder, new Release());
    await expect.poll(() => actor.outcomes.length).toBe(3);
    expect(actor.outcomes[2]?.reply).toBe("released:third");
  });

  it("treats a non-positive in-flight limit as unlimited", async () => {
    const { actor, pid } = await startRequester("allowAll", -5);
    const holderActor = new Holder();
    const holder = makePid(holderActor, "holder-unlimited");
    await holder.start();

    external.tell(pid, new DoRequest(holder, "a"));
    external.tell(pid, new DoRequest(holder, "b"));
    await expect.poll(() => holderActor.held.length).toBe(2);

    external.tell(holder, new Release());
    await expect.poll(() => actor.outcomes.length).toBe(2);
  });

  it("completes with ErrDead when the target is not running", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const dead = makePid(new Echo(), "echo-dead");

    external.tell(pid, new DoRequest(dead, "ping"));
    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]?.error).toBe(ErrDead);
  });

  it("completes with the mailbox error when delivery fails", async () => {
    class Parked implements Actor {
      started = false;

      preStart(): void {}

      async receive(): Promise<void> {
        this.started = true;
        await new Promise<void>(() => {});
      }

      postStop(): void {}
    }

    const { actor, pid } = await startRequester("allowAll");
    const parked = new Parked();
    const target = makePid(parked, "bounded-target", { mailbox: new BoundedMailbox(1) });
    await target.start();

    // Park the receive loop on the first message, fill the single
    // mailbox slot, then request against the full box.
    expect(external.tell(target, "park")).toBeNull();
    await expect.poll(() => parked.started).toBe(true);
    expect(external.tell(target, "occupy")).toBeNull();

    external.tell(pid, new DoRequest(target, "overflow"));
    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]?.error).toBe(ErrMailboxFull);
  });
});

describe("request completion", () => {
  it("delivers the reply to the continuation on the actor's turn", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const echo = makePid(new Echo(), "echo-reply");
    await echo.start();

    external.tell(pid, new DoRequest(echo, "ping"));
    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]).toEqual({ reply: "ping", error: null });
  });

  it("completes a request without a timeout when the reply arrives", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const echo = makePid(new Echo(), "echo-no-timeout");
    await echo.start();

    external.tell(pid, new DoRequest(echo, "ping", {}));
    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]?.reply).toBe("ping");
  });

  it("times out an unanswered request", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const holder = makePid(new Holder(), "holder-timeout");
    await holder.start();

    external.tell(pid, new DoRequest(holder, "ping", { timeout: 20 }));
    await expect.poll(() => actor.outcomes.length, { timeout: 2000 }).toBe(1);
    expect(actor.outcomes[0]?.error).toBe(ErrRequestTimeout);
  });

  it("runs a continuation registered after completion immediately, once", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const echo = makePid(new Echo(), "echo-late");
    await echo.start();

    external.tell(pid, new DoBareRequest(echo, "ping"));
    await expect.poll(() => pid.isIdle()).toBe(true);

    const call = actor.calls[0] as RequestCall;
    const seen: unknown[] = [];
    call.onReply((reply) => seen.push(reply));
    call.onReply((reply) => seen.push(reply));

    expect(seen).toEqual(["ping"]);
  });

  it("lets an actor request itself without deadlocking", async () => {
    class SelfRequester implements Actor {
      reply: unknown;

      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message === "kick") {
          ctx.request(ctx.self as PID, "hello").onReply((reply) => {
            this.reply = reply;
          });
          return;
        }

        ctx.response(`self:${String(ctx.message)}`);
      }

      postStop(): void {}
    }

    const actor = new SelfRequester();
    const pid = makePid(actor, "self-requester", { reentrancy: { mode: "allowAll" } });
    await pid.start();

    external.tell(pid, "kick");
    await expect.poll(() => actor.reply).toBe("self:hello");
  });

  it("abandons the reply when the requester has stopped", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const holderActor = new Holder();
    const holder = makePid(holderActor, "holder-abandon");
    await holder.start();

    external.tell(pid, new DoRequest(holder, "ping"));
    await expect.poll(() => holderActor.held.length).toBe(1);

    await pid.shutdown();
    external.tell(holder, new Release());
    await expect.poll(() => holderActor.held.length).toBe(0);

    expect(actor.outcomes).toEqual([]);
    (actor.calls[0] as RequestCall).cancel();
    expect(actor.outcomes).toEqual([]);
  });
});

describe("request cancellation", () => {
  it("cancels a pending request and ignores the late reply", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const holderActor = new Holder();
    const holder = makePid(holderActor, "holder-cancel");
    await holder.start();

    external.tell(pid, new DoRequest(holder, "ping"));
    await expect.poll(() => holderActor.held.length).toBe(1);

    const call = actor.calls[0] as RequestCall;
    call.cancel();
    call.cancel();

    await expect.poll(() => actor.outcomes.length).toBe(1);
    expect(actor.outcomes[0]?.error).toBe(ErrRequestCanceled);

    external.tell(holder, new Release());
    await expect.poll(() => holderActor.held.length).toBe(0);
    expect(actor.outcomes.length).toBe(1);
  });

  it("ignores a cancel after completion", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const echo = makePid(new Echo(), "echo-cancel-late");
    await echo.start();

    external.tell(pid, new DoRequest(echo, "ping"));
    await expect.poll(() => actor.outcomes.length).toBe(1);

    (actor.calls[0] as RequestCall).cancel();
    expect(actor.outcomes.length).toBe(1);
  });
});

describe("reentrancy modes", () => {
  it("allowAll keeps processing user messages while a reply is in flight", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const holderActor = new Holder();
    const holder = makePid(holderActor, "holder-allow");
    await holder.start();

    external.tell(pid, new DoRequest(holder, "ping"));
    external.tell(pid, "a");
    external.tell(pid, "b");

    await expect.poll(() => actor.log).toEqual(["msg:a", "msg:b"]);

    external.tell(holder, new Release());
    await expect.poll(() => actor.log).toEqual(["msg:a", "msg:b", "reply:released:ping"]);
  });

  it("stashNonReentrant holds user messages until the reply arrives", async () => {
    const { actor, pid } = await startRequester("stashNonReentrant");
    const holderActor = new Holder();
    const holder = makePid(holderActor, "holder-stash");
    await holder.start();

    external.tell(pid, new DoRequest(holder, "ping"));
    external.tell(pid, "a");
    external.tell(pid, "b");

    await expect.poll(() => pid.stashSize()).toBe(2);
    expect(actor.log).toEqual([]);
    expect(pid.isIdle()).toBe(false);

    external.tell(holder, new Release());
    await expect.poll(() => actor.log.length).toBe(3);
    expect(actor.log).toEqual(["reply:released:ping", "msg:a", "msg:b"]);
  });

  it("applies a per-call stash override on an allowAll actor", async () => {
    const { actor, pid } = await startRequester("allowAll");
    const holderActor = new Holder();
    const holder = makePid(holderActor, "holder-override");
    await holder.start();

    external.tell(pid, new DoRequest(holder, "ping", { mode: "stashNonReentrant" }));
    external.tell(pid, "a");

    await expect.poll(() => pid.stashSize()).toBe(1);

    external.tell(holder, new Release());
    await expect.poll(() => actor.log).toEqual(["reply:released:ping", "msg:a"]);
  });

  it("resumes only after the last stash-mode request completes", async () => {
    const { actor, pid } = await startRequester("stashNonReentrant");
    const holderActor = new Holder();
    const holder = makePid(holderActor, "holder-two");
    await holder.start();

    external.tell(pid, new DoDoubleRequest(holder));
    external.tell(pid, "a");

    await expect.poll(() => pid.stashSize()).toBe(1);
    await expect.poll(() => holderActor.held.length).toBe(2);

    external.tell(holder, new Release());
    await expect.poll(() => actor.log.length).toBe(3);
    expect(actor.log).toEqual(["reply:released:one", "reply:released:two", "msg:a"]);
  });

  it("yields while stashing a flood and drains it all after the reply", async () => {
    const { actor, pid } = await startRequester("stashNonReentrant");
    const holderActor = new Holder();
    const holder = makePid(holderActor, "holder-flood");
    await holder.start();

    external.tell(pid, new DoRequest(holder, "ping"));
    await expect.poll(() => holderActor.held.length).toBe(1);

    const count = 2100;
    for (let i = 0; i < count; i++) {
      external.tell(pid, i);
    }

    await expect.poll(() => pid.stashSize(), { timeout: 5000 }).toBe(count);

    external.tell(holder, new Release());
    await expect.poll(() => actor.log.length, { timeout: 5000 }).toBe(count + 1);
  });
});
