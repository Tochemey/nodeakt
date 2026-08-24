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
import type { ActorSystem } from "../../src/actor.system";
import { ErrDead } from "../../src/errors";
import { Deadletter, PostStart, Terminated } from "../../src/messages";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { registerMessage } from "../../src/registration";
import { remoteSystem, sleep, until, withSystems } from "./helpers";

class Ping {
  constructor(readonly n: number) {}
}

class Pong {
  constructor(readonly n: number) {}
}

class UnregisteredResult {
  constructor(readonly n: number) {}
}

registerMessage(Ping);
registerMessage(Pong);

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

/** Records who a Ping came from and answers that sender with a tell,
 * so a forwarded message proves both the preserved origin and the
 * reply route back to it. */
class Responder implements Actor {
  readonly senders: string[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Ping) {
      this.senders.push((ctx.sender as PID).path().toString());
      ctx.tell(ctx.sender as PID, new Pong(ctx.message.n + 1));
    }
  }

  postStop(): void {}
}

/** Watches whoever pings it and records the deaths it observes. */
class Keeper implements Actor {
  readonly terminated: string[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Ping) {
      ctx.watch(ctx.sender as PID);
      return;
    }

    if (ctx.message instanceof Terminated) {
      this.terminated.push(ctx.message.actorPath);
    }
  }

  postStop(): void {}
}

/** Forwards every business message to its target, preserving the
 * original sender, the contract under test. */
class Forwarder implements Actor {
  constructor(private readonly target: () => PID) {}

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    ctx.forward(this.target());
  }

  postStop(): void {}
}

describe("remote forward", () => {
  it("forwards to a remote target preserving the original sender", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const responder: Responder = new Responder();
      await b.spawn("responder", responder);
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "responder")) as PID;

      const origin: Collector = new Collector();
      const originPid: PID = await a.spawn("origin", origin);
      const relay: PID = await a.spawn("relay", new Forwarder((): PID => remote));

      originPid.tell(relay, new Ping(1));

      // The responder saw the origin, not the relay, and its reply to
      // that sender crossed back to the origin itself.
      await until("the reply to the origin", (): boolean => origin.received.length >= 1);
      expect(responder.senders).toEqual([originPid.path().toString()]);
      const pong: unknown = (origin.received[0] as { message: unknown }).message;
      expect(pong).toBeInstanceOf(Pong);
      expect((pong as Pong).n).toBe(2);
    });
  });

  it("forwards a wire-delivered message onward to a third node, origin intact", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const c: ActorSystem = remoteSystem("gamma");
      await c.start();

      try {
        const responder: Responder = new Responder();
        await c.spawn("responder", responder);
        const onGamma: PID = (await b.remoteLookup(c.host(), c.port(), "responder")) as PID;
        await b.spawn("relay", new Forwarder((): PID => onGamma));
        const relay: PID = (await a.remoteLookup(b.host(), b.port(), "relay")) as PID;

        const origin: Collector = new Collector();
        const originPid: PID = await a.spawn("origin", origin);
        originPid.tell(relay, new Ping(5));

        // Beta forwarded a message it received over the wire, so the
        // sender it preserved was itself a reverse handle to alpha;
        // gamma still saw the origin and replied straight to it.
        await until("the reply from gamma", (): boolean => origin.received.length >= 1);
        expect(responder.senders).toEqual([originPid.path().toString()]);

        const delivery: { message: unknown; sender: PID } = origin.received[0] as {
          message: unknown;
          sender: PID;
        };
        expect(delivery.message).toBeInstanceOf(Pong);
        expect((delivery.message as Pong).n).toBe(6);
        expect(delivery.sender.path().toString()).toBe(onGamma.path().toString());
      } finally {
        await c.stop();
      }
    });
  });

  it("throws the encode refusal from forward and faults the forwarder", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const responder: Responder = new Responder();
      await b.spawn("responder", responder);
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "responder")) as PID;

      const deadletters: Deadletter[] = [];
      a.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      const origin: PID = await a.spawn("origin", new Collector());
      const relay: PID = await a.spawn("relay", new Forwarder((): PID => remote));

      // The remote route returns the encode refusal, ctx.forward throws
      // it on the relay's turn, and the default supervision stops the
      // faulted relay; the payload dead-letters on the sending node,
      // attributed to the preserved origin.
      origin.tell(relay, new UnregisteredResult(7));

      const isRefusal: (letter: Deadletter) => boolean = (letter: Deadletter): boolean =>
        letter.receiver === remote.path().toString();
      await until("the dead letter", (): boolean => deadletters.some(isRefusal));
      const letter: Deadletter = deadletters.find(isRefusal) as Deadletter;
      expect(letter.sender).toBe(origin.path().toString());
      expect(letter.reason).toContain("is not registered");

      await until("the relay to stop", (): boolean => !relay.isRunning());
      expect(responder.senders).toHaveLength(0);
    });
  });

  it("watches a forward-derived sender handle across three nodes", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const c: ActorSystem = remoteSystem("gamma");
      await c.start();

      try {
        const keeper: Keeper = new Keeper();
        await c.spawn("keeper", keeper);
        const onGamma: PID = (await b.remoteLookup(c.host(), c.port(), "keeper")) as PID;
        await b.spawn("relay", new Forwarder((): PID => onGamma));
        const relay: PID = (await a.remoteLookup(b.host(), b.port(), "relay")) as PID;

        const origin: PID = await a.spawn("origin", new Collector());
        origin.tell(relay, new Ping(1));

        // The keeper watches the reconstructed origin handle: a route
        // gamma built purely from the forwarded envelope's sender, to a
        // node it never looked anything up on. The origin's stop still
        // travels back as exactly one Terminated.
        await sleep(50);
        await origin.shutdown();

        await until("the Terminated", (): boolean => keeper.terminated.length >= 1);
        expect(keeper.terminated).toEqual([origin.path().toString()]);
      } finally {
        await c.stop();
      }
    });
  });
});

describe("remote pipeTo", () => {
  it("delivers a piped result to a remote target with the piper as sender", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      await b.spawn("sink", collector);
      const sink: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;

      const piper: PID = await a.spawn("piper", new Collector());
      piper.pipeTo(sink, Promise.resolve(new Ping(7)));

      await until("the piped result", (): boolean => collector.received.length >= 1);
      const delivery: { message: unknown; sender: PID } = collector.received[0] as {
        message: unknown;
        sender: PID;
      };
      expect(delivery.message).toBeInstanceOf(Ping);
      expect((delivery.message as Ping).n).toBe(7);
      expect(delivery.sender.path().toString()).toBe(piper.path().toString());
    });
  });

  it("dead-letters an unregistered piped result on the sending node", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      await b.spawn("sink", collector);
      const sink: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;

      const deadletters: Deadletter[] = [];
      a.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      const piper: PID = await a.spawn("piper", new Collector());
      piper.pipeTo(sink, Promise.resolve(new UnregisteredResult(1)));

      // The refusal happens on encode, before the wire: the dead letter
      // lands on the sending node and nothing reaches the target.
      await until("the dead letter", (): boolean => deadletters.length >= 1);
      const letter: Deadletter = deadletters[0] as Deadletter;
      expect(letter.receiver).toBe(sink.path().toString());
      expect(letter.sender).toBe(piper.path().toString());
      expect(letter.reason).toContain("is not registered");
      expect(letter.message).toBeInstanceOf(UnregisteredResult);

      await sleep(50);
      expect(collector.received).toHaveLength(0);
    });
  });

  it("dead-letters a piped result whose remote target stopped, on the target's node", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      const local: PID = await b.spawn("sink", collector);
      const sink: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;

      const deadletters: Deadletter[] = [];
      b.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      let release: (value: unknown) => void = (): void => {};
      const task: Promise<unknown> = new Promise<unknown>((resolve): void => {
        release = resolve;
      });

      const piper: PID = await a.spawn("piper", new Collector());
      piper.pipeTo(sink, task);

      // The target dies while the task runs; the routed handle has no
      // local liveness to gate on, so the result crosses the wire and
      // the receiving node discovers the death.
      await local.shutdown();
      release(new Ping(3));

      await until("the dead letter on the target node", (): boolean => deadletters.length >= 1);
      const letter: Deadletter = deadletters[0] as Deadletter;
      expect(letter.receiver).toBe(sink.path().toString());
      expect(letter.sender).toBe(piper.path().toString());
      expect(letter.reason).toBe(ErrDead.message);
      expect(letter.message).toBeInstanceOf(Ping);
      expect(collector.received).toHaveLength(0);
    });
  });
});
