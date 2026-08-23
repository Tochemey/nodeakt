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
import { ControlPlane } from "../src/control.plane";
import { discardLogger } from "../src/discard.logger";
import { ErrDead, ErrRequestTimeout } from "../src/errors";
import { Mesh } from "../src/mesh";
import { MessageRegistry } from "../src/message.registry";
import { Deadletter, PostStart } from "../src/messages";
import { parsePath } from "../src/path";
import type { PID } from "../src/pid";
import type { ReceiveContext } from "../src/receive.context";

class Ping {
  constructor(readonly value: number) {}
}

class Pong {
  constructor(readonly value: number) {}

  tag(): string {
    return `pong:${this.value}`;
  }
}

/** Records messages; answers a Ping with a Pong, echoes strings, and
 * never answers "silent". */
class Recorder implements Actor {
  readonly seen: unknown[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.seen.push(ctx.message);

    if (ctx.message instanceof Ping) {
      ctx.response(new Pong((ctx.message as Ping).value + 1));
      return;
    }

    if (ctx.message === "silent") {
      return;
    }

    ctx.response(`echo:${String(ctx.message)}`);
  }

  postStop(): void {}
}

/** One simulated isolate: an actor system plus its mesh. */
interface Node {
  system: ActorSystem;
  mesh: Mesh;
  letters: Deadletter[];
}

/** Wires two nodes together with a fresh channel, each side keyed by
 * the other's worker id. */
function link(a: Node, aId: number, b: Node, bId: number): void {
  const channel = new MessageChannel();
  a.mesh.connect(bId, channel.port1);
  b.mesh.connect(aId, channel.port2);
}

describe("Mesh", () => {
  let registry: MessageRegistry;
  let main: Node;
  let w1: Node;
  let w2: Node;

  async function newNode(selfId: number): Promise<Node> {
    const system = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
    const letters: Deadletter[] = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        letters.push(event);
      }
    });

    return { system, mesh: new Mesh(system, registry, selfId), letters };
  }

  beforeEach(async () => {
    registry = new MessageRegistry();
    registry.register(Ping);
    registry.register(Pong);
    main = await newNode(0);
    w1 = await newNode(1);
    w2 = await newNode(2);
    link(main, 0, w1, 1);
    link(main, 0, w2, 2);
    link(w1, 1, w2, 2);
  });

  afterEach(async () => {
    for (const node of [main, w1, w2]) {
      node.mesh.close();
      await node.system.stop();
    }
  });

  it("routes tells to the worker named in the send", async () => {
    const onW1 = new Recorder();
    const onW2 = new Recorder();
    const pidW1 = await w1.system.spawn("echo", onW1);
    const pidW2 = await w2.system.spawn("echo", onW2);

    expect(main.mesh.tell(1, pidW1.path(), "to-one")).toBeNull();
    expect(main.mesh.tell(2, pidW2.path(), "to-two")).toBeNull();

    await expect.poll(() => onW1.seen).toEqual(["to-one"]);
    await expect.poll(() => onW2.seen).toEqual(["to-two"]);
  });

  it("asks across the mesh with prototypes restored", async () => {
    const target = await w2.system.spawn("worker-actor", new Recorder());

    const reply = await main.mesh.ask(2, target.path(), new Ping(7), 1000);

    expect(reply).toBeInstanceOf(Pong);
    expect((reply as Pong).tag()).toBe("pong:8");
  });

  it("carries worker-to-worker traffic without the main isolate", async () => {
    const target = await w2.system.spawn("peer-actor", new Recorder());

    await expect(w1.mesh.ask(2, target.path(), "hi", 1000)).resolves.toBe("echo:hi");
  });

  it("requests across the mesh on behalf of a reentrant sender", async () => {
    const sender = await main.system.spawn("requester", new Recorder(), {
      reentrancy: { mode: "allowAll" },
    });
    const target = await w1.system.spawn("responder", new Recorder());
    const outcomes: Array<{ reply: unknown; error: Error | null }> = [];

    main.mesh.request(1, target.path(), new Ping(1), sender).onReply((reply, error) => {
      outcomes.push({ reply, error });
    });

    await expect.poll(() => outcomes.length).toBe(1);
    expect(outcomes[0]?.error).toBeNull();
    expect(outcomes[0]?.reply).toBeInstanceOf(Pong);
  });

  it("fails sends naming an unknown worker with ErrDead and a dead letter", async () => {
    const sender = await main.system.spawn("caller", new Recorder());
    const to = parsePath("nodeakt://sys@127.0.0.1:0/nobody");

    expect(main.mesh.tell(9, to, "lost", sender)).toBe(ErrDead);
    expect(main.mesh.tell(9, to, "lost-anon")).toBe(ErrDead);
    await expect(main.mesh.ask(9, to, "lost", 1000)).rejects.toBe(ErrDead);

    const reentrant = await main.system.spawn("requester", new Recorder(), {
      reentrancy: { mode: "allowAll" },
    });
    let refusal: Error | null = null;
    main.mesh.request(9, to, "lost", reentrant).onReply((_reply, error) => {
      refusal = error;
    });
    expect(refusal).toBe(ErrDead);

    await expect.poll(() => main.letters.length).toBe(4);
    expect(main.letters[0]?.sender).toBe(sender.path().toString());
    expect(main.letters[1]?.sender).toBeUndefined();
    expect(main.letters[2]?.sender).toBeUndefined();
    expect(main.letters.every((letter) => letter.reason === ErrDead.message)).toBe(true);
  });

  it("settles pending work immediately on disconnect and refuses the worker afterwards", async () => {
    const target = await w1.system.spawn("mute", new Recorder());

    const pending = main.mesh.ask(1, target.path(), "silent", 60_000);
    expect(main.mesh.isolates().includes(1)).toBe(true);

    main.mesh.disconnect(1);

    await expect(pending).rejects.toBe(ErrDead);
    expect(main.mesh.isolates().sort()).toEqual([2]);
    expect(main.mesh.tell(1, target.path(), "late")).toBe(ErrDead);
    await expect.poll(() => main.letters.length).toBe(1);
  });

  it("ignores disconnecting an unknown worker", () => {
    expect(() => main.mesh.disconnect(9)).not.toThrow();
    expect(main.mesh.isolates().sort()).toEqual([1, 2]);
  });

  it("rejects connecting a duplicate worker id", () => {
    const channel = new MessageChannel();

    expect(() => main.mesh.connect(1, channel.port1)).toThrow("worker 1 is already connected");
    channel.port1.close();
  });

  it("closes every isolate at once and stays closed", async () => {
    const target = await w1.system.spawn("mute", new Recorder());
    const pending = main.mesh.ask(1, target.path(), "silent", 60_000);

    main.mesh.close();

    await expect(pending).rejects.toBe(ErrDead);
    expect(main.mesh.isolates()).toEqual([]);
    expect(() => main.mesh.close()).not.toThrow();
  });

  it("cleans up a dead worker end to end with the control plane", async () => {
    const plane = new ControlPlane([1, 2]);
    plane.register("on-one", 1);
    plane.register("on-two", 2);

    const target = await w1.system.spawn("mute", new Recorder());
    const pending = main.mesh.ask(1, target.path(), "silent", 60_000);

    // Worker 1 dies: the control plane frees its names and every
    // surviving isolate disconnects it.
    const freed = plane.evict(1);
    main.mesh.disconnect(1);
    w2.mesh.disconnect(1);

    expect(freed).toEqual(["on-one"]);
    expect(plane.resolve("on-one")).toBeUndefined();
    expect(plane.resolve("on-two")).toBe(2);
    expect(plane.place()).toBe(2);
    await expect(pending).rejects.toBe(ErrDead);
    expect(main.mesh.isolates().sort()).toEqual([2]);
    expect(w2.mesh.isolates().sort()).toEqual([0]);
  });

  it("still honors ask timeouts through the mesh", async () => {
    const target = await w2.system.spawn("mute", new Recorder());

    await expect(main.mesh.ask(2, target.path(), "silent", 30)).rejects.toBe(ErrRequestTimeout);
  });

  it("attributes cross-node senders to a routed handle carrying their identity", async () => {
    const seenSenders: PID[] = [];

    class SenderProbe implements Actor {
      preStart(): void {}

      receive(ctx: ReceiveContext): void {
        if (ctx.message instanceof PostStart) {
          return;
        }

        seenSenders.push(ctx.sender as PID);
      }

      postStop(): void {}
    }

    const target = await w1.system.spawn("probe", new SenderProbe());
    const sender = await main.system.spawn("caller", new Recorder());

    main.mesh.tell(1, target.path(), "who-sent-this", sender);

    await expect.poll(() => seenSenders.length).toBe(1);
    const handle = seenSenders[0] as PID;
    expect(handle).not.toBe(w1.system.noSender());
    expect(handle.path().toString()).toBe(sender.path().toString());

    // Replying to the handle routes back to the sender's own node.
    const caller = handle;
    target.tell(caller, "right-back");
    const recorder = (main.system.actorOf("caller") as PID).actor() as Recorder;
    await expect.poll(() => recorder.seen).toEqual(["right-back"]);
  });
});
