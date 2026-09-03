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

import { MessageChannel, type MessagePort } from "node:worker_threads";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { decodeError, encodeError } from "../src/codec";
import { eventsTopic } from "../src/deadletter";
import { discardLogger } from "../src/discard.logger";
import { ErrActorAlreadyExists, ErrDead } from "../src/errors";
import type { EventStream } from "../src/eventstream";
import type { Logger } from "../src/logger";
import { Mesh } from "../src/mesh";
import { MessageRegistry } from "../src/message.registry";
import { Deadletter } from "../src/messages";
import { PASSIVATION_TIME_BASED } from "../src/passivation";
import { parsePath } from "../src/path";
import type { PID } from "../src/pid";
import { Props } from "../src/props";
import type { ControlMessage, WorkerMessage } from "../src/protocol";
import { registerActor } from "../src/registration";
import { applySetup, spawnRecipe, WorkerRuntime } from "../src/worker.runtime";

const aliasedModule = new URL("./fixtures/aliased.actor.mjs", import.meta.url).href;
const countingModule = new URL("./fixtures/counting.actor.mjs", import.meta.url).href;
const echoModule = new URL("./fixtures/echo.actor.mjs", import.meta.url).href;
const phoenixModule = new URL("./fixtures/phoenix.actor.mjs", import.meta.url).href;
const setupModule = new URL("./fixtures/wire.setup.mjs", import.meta.url).href;
const slowModule = new URL("./fixtures/slow.actor.mjs", import.meta.url).href;

/** A logger that records error entries and discards the rest. */
function capturingLogger(errors: string[]): Logger {
  return {
    debug(): void {},
    info(): void {},
    warn(): void {},
    error(message: string): void {
      errors.push(message);
    },
    level(): "error" {
      return "error";
    },
    enabled(level): boolean {
      return level === "error";
    },
    with(): Logger {
      return this;
    },
  };
}

describe("WorkerRuntime", () => {
  let mainSystem: ActorSystem;
  let mainMesh: Mesh;
  let facade: ActorSystem;
  let facadeErrors: string[];
  let runtime: WorkerRuntime;
  let control: MessageChannel;
  let received: WorkerMessage[];

  function post(message: ControlMessage, transfer?: MessagePort[]): void {
    if (transfer === undefined) {
      control.port1.postMessage(message);
      return;
    }

    control.port1.postMessage(message, transfer);
  }

  /** Waits until a worker message satisfying the predicate arrived. */
  async function receivedWhere(
    predicate: (message: WorkerMessage) => boolean,
  ): Promise<WorkerMessage> {
    await expect.poll(() => received.some(predicate)).toBe(true);

    return received.find(predicate) as WorkerMessage;
  }

  beforeEach(async () => {
    const registry = new MessageRegistry();
    mainSystem = new ActorSystem("sys", { logger: discardLogger });
    await mainSystem.start();
    mainMesh = new Mesh(mainSystem, registry, 0);

    facadeErrors = [];
    facade = new ActorSystem("sys", { logger: capturingLogger(facadeErrors) });
    await facade.start();

    control = new MessageChannel();
    received = [];
    control.port1.on("message", (message: unknown) => {
      received.push(message as WorkerMessage);
    });
    runtime = new WorkerRuntime(facade, new MessageRegistry(), control.port2, 1);
  });

  afterEach(async () => {
    runtime.mesh().close();
    mainMesh.close();
    control.port1.close();
    await facade.stop();
    await mainSystem.stop();
  });

  it("announces ready", async () => {
    runtime.announce();

    await receivedWhere((m) => m.kind === "ready");
  });

  it("spawns a recipe and serves it over the mesh", async () => {
    post({
      kind: "spawn",
      seq: 1,
      name: "echo",
      recipe: { module: echoModule, actor: "Echo", args: ["hi"] },
    });

    const spawned = await receivedWhere((m) => m.kind === "spawned");
    expect(spawned.kind).toBe("spawned");
    if (spawned.kind !== "spawned") {
      return;
    }

    const channel = new MessageChannel();
    mainMesh.connect(1, channel.port1);
    post({ kind: "connect", workerId: 0, port: channel.port2 }, [channel.port2]);

    const reply = await mainMesh.ask(1, parsePath(spawned.path, spawned.uid), "ping", 1000);
    expect(reply).toBe("hi:ping");
  });

  it("restarts an owned actor in place on the control plane's order", async () => {
    post({
      kind: "spawn",
      seq: 1,
      name: "tally",
      recipe: { module: countingModule, actor: "Counting", args: [] },
    });
    await receivedWhere((m) => m.kind === "spawned");

    const pid = facade.actorOf("tally") as PID;
    facade.noSender().tell(pid, "bump");
    facade.noSender().tell(pid, "bump");
    await expect(facade.noSender().ask(pid, "count", 1000)).resolves.toBe(2);

    // The restart reruns the lifecycle hooks in place: the tally
    // begins a fresh life under the same name.
    post({ kind: "restart", seq: 2, name: "tally" });
    const controlled = await receivedWhere((m) => m.kind === "controlled" && m.seq === 2);
    expect(controlled.kind).toBe("controlled");
    if (controlled.kind === "controlled") {
      expect(controlled.error).toBeNull();
    }

    await expect(facade.noSender().ask(pid, "count", 1000)).resolves.toBe(0);
  });

  it("answers a restart of a name this isolate does not own with not-found", async () => {
    // An unknown name, and a name the replicated table places on
    // another isolate: lifecycle orders act on owned actors alone.
    post({ kind: "restart", seq: 3, name: "ghost" });
    const unknown = await receivedWhere((m) => m.kind === "controlled" && m.seq === 3);
    if (unknown.kind === "controlled") {
      expect(decodeError(unknown.error as NonNullable<typeof unknown.error>).name).toBe(
        "ActorNotFoundError",
      );
    }

    post({ kind: "name-added", name: "elsewhere", workerId: 2 });
    post({ kind: "restart", seq: 4, name: "elsewhere" });
    const routed = await receivedWhere((m) => m.kind === "controlled" && m.seq === 4);
    if (routed.kind === "controlled") {
      expect(decodeError(routed.error as NonNullable<typeof routed.error>).name).toBe(
        "ActorNotFoundError",
      );
    }
  });

  it("answers a failing restart with its error", async () => {
    post({
      kind: "spawn",
      seq: 5,
      name: "phoenix",
      recipe: { module: phoenixModule, actor: "Phoenix", args: [] },
    });
    await receivedWhere((m) => m.kind === "spawned");

    post({ kind: "restart", seq: 6, name: "phoenix" });
    const controlled = await receivedWhere((m) => m.kind === "controlled" && m.seq === 6);
    if (controlled.kind === "controlled") {
      expect(decodeError(controlled.error as NonNullable<typeof controlled.error>).message).toBe(
        "actor phoenix failed to initialize",
      );
    }
  });

  it("stops an owned actor on order, idempotently, and frees its name", async () => {
    post({
      kind: "spawn",
      seq: 7,
      name: "leaving",
      recipe: { module: echoModule, actor: "Echo", args: ["bye"] },
    });
    await receivedWhere((m) => m.kind === "spawned");

    post({ kind: "stop-actor", seq: 8, name: "leaving" });
    const controlled = await receivedWhere((m) => m.kind === "controlled" && m.seq === 8);
    if (controlled.kind === "controlled") {
      expect(controlled.error).toBeNull();
    }

    // The stop announced itself, so the control plane frees the name.
    await receivedWhere((m) => m.kind === "actor-stopped" && m.name === "leaving");
    expect(facade.actorOf("leaving")).toBeUndefined();

    // Already stopped: the second order succeeds without an actor.
    post({ kind: "stop-actor", seq: 9, name: "leaving" });
    const again = await receivedWhere((m) => m.kind === "controlled" && m.seq === 9);
    if (again.kind === "controlled") {
      expect(again.error).toBeNull();
    }
  });

  it("answers placed routes from the facade's replicated table", async () => {
    // The seam exists on every isolate; only ownership elsewhere in
    // the pool resolves to a route, mirroring findName.
    expect(facade.placedRouteOf("nowhere")).toBeUndefined();

    post({ kind: "name-added", name: "afar", workerId: 2 });
    await expect.poll(() => facade.placedRouteOf("afar") !== undefined).toBe(true);

    post({ kind: "name-added", name: "mine", workerId: 1 });
    post({ kind: "name-added", name: "sentinel", workerId: 2 });
    await expect.poll(() => facade.placedRouteOf("sentinel") !== undefined).toBe(true);
    expect(facade.placedRouteOf("mine")).toBeUndefined();
  });

  it("refuses placed lifecycle orders through the facade's own placement", async () => {
    // Lifecycle control of placed actors enters through the main
    // isolate alone; a facade ordering it is a programming error.
    await expect(facade.respawnPlaced("anything")).rejects.toSatisfy(
      (err: unknown): boolean => err instanceof Error && err.name === "ActorNotFoundError",
    );
    await expect(facade.stopPlaced("anything")).rejects.toSatisfy(
      (err: unknown): boolean => err instanceof Error && err.name === "ActorNotFoundError",
    );

    // A system with no placement at all: nothing to respawn, nothing
    // left to stop.
    await expect(mainSystem.respawnPlaced("anything")).rejects.toSatisfy(
      (err: unknown): boolean => err instanceof Error && err.name === "ActorNotFoundError",
    );
    await expect(mainSystem.stopPlaced("anything")).resolves.toBeUndefined();
  });

  it("routes an envelope naming another node's path onward to the main isolate", async () => {
    const channel = new MessageChannel();
    mainMesh.connect(1, channel.port1);
    post({ kind: "connect", workerId: 0, port: channel.port2 }, [channel.port2]);

    const letters: Deadletter[] = [];
    mainSystem.subscribe((event) => {
      if (event instanceof Deadletter) {
        letters.push(event);
      }
    });

    // The facade holds nothing at a foreign node's path and is not the
    // node's network edge: it hands the envelope to the main isolate,
    // whose remoting-less system records the dead letter, which is the
    // proof the hop crossed back instead of dying on the facade.
    const foreign = parsePath("nodeakt://other@10.9.9.9:7/replyTo");
    expect(mainMesh.tell(1, foreign, "wandering")).toBeNull();

    // A path differing from this node's only by port is still another
    // node's, and takes the same hop.
    const samePortless = parsePath(`nodeakt://sys@${facade.host()}:9/replyToo`);
    expect(mainMesh.tell(1, samePortless, "drifting")).toBeNull();

    await expect
      .poll(() =>
        letters.some(
          (letter) => letter.receiver.includes("replyTo") && letter.message === "wandering",
        ),
      )
      .toBe(true);
    await expect
      .poll(() => letters.some((letter) => letter.receiver.includes("replyToo")))
      .toBe(true);
    expect(received.some((m) => m.kind === "deadletter")).toBe(false);

    // A path of this very node is not the resolver's to route: an
    // unknown local name dead-letters on the facade, forwarded to the
    // control port like every facade loss.
    const ghost = parsePath(`nodeakt://sys@${facade.host()}:${facade.port()}/ghostly`);
    expect(mainMesh.tell(1, ghost, "homeless")).toBeNull();
    await receivedWhere((m) => m.kind === "deadletter" && m.receiver.includes("ghostly"));
  });

  it("dead-letters a foreign path while the main route is not wired", async () => {
    // The facade is connected only to a sibling worker: with no route
    // to main yet, a foreign path has nowhere to go and dead-letters
    // here, forwarded to the control port like every facade loss.
    const sibling = new Mesh(mainSystem, new MessageRegistry(), 2);
    const channel = new MessageChannel();
    sibling.connect(1, channel.port1);
    post({ kind: "connect", workerId: 2, port: channel.port2 }, [channel.port2]);

    try {
      const foreign = parsePath("nodeakt://other@10.9.9.9:7/replyTo");
      expect(sibling.tell(1, foreign, "stranded")).toBeNull();

      await receivedWhere((m) => m.kind === "deadletter" && m.receiver.includes("replyTo"));
    } finally {
      sibling.close();
    }
  });

  it("adopts the node's advertised address before starting", async () => {
    const adopted = new ActorSystem("sys", { logger: discardLogger });
    adopted.adoptAddress("10.1.2.3", 4567);
    await adopted.start();

    try {
      const pid = await adopted.spawn("resident", {
        preStart(): void {},
        receive(): void {},
        postStop(): void {},
      } as Actor);
      expect(pid.path().host()).toBe("10.1.2.3");
      expect(pid.path().port()).toBe(4567);

      // Adoption after start would split the path space; it refuses.
      expect(() => adopted.adoptAddress("10.9.9.9", 1)).toThrow("running system");
    } finally {
      await adopted.stop();
    }
  });

  it("reports a duplicate name with the sentinel's identity intact", async () => {
    post({ kind: "spawn", seq: 1, name: "twin", recipe: { module: echoModule, actor: "Echo" } });
    await receivedWhere((m) => m.kind === "spawned");

    post({ kind: "spawn", seq: 2, name: "twin", recipe: { module: echoModule, actor: "Echo" } });
    const failed = await receivedWhere((m) => m.kind === "spawn-failed");

    expect(failed.kind).toBe("spawn-failed");
    if (failed.kind === "spawn-failed") {
      expect(decodeError(failed.error)).toBe(ErrActorAlreadyExists);
    }
  });

  it("reports an unloadable module", async () => {
    post({
      kind: "spawn",
      seq: 1,
      name: "ghost",
      recipe: { module: "file:///nowhere/nothing.mjs", actor: "Echo" },
    });

    const failed = await receivedWhere((m) => m.kind === "spawn-failed");
    if (failed.kind === "spawn-failed") {
      expect(failed.error.sentinel).toBe(-1);
    }
  });

  it("reports a recipe naming an export that is not a class", async () => {
    post({
      kind: "spawn",
      seq: 1,
      name: "wrong",
      recipe: { module: echoModule, actor: "notAnActor" },
    });

    const failed = await receivedWhere((m) => m.kind === "spawn-failed");
    if (failed.kind === "spawn-failed") {
      const error = decodeError(failed.error);
      expect(error.message).toContain('does not export an actor class named "notAnActor"');
    }
  });

  it("wires and unwires mesh connections on command", async () => {
    const channel = new MessageChannel();
    post({ kind: "connect", workerId: 0, port: channel.port2 }, [channel.port2]);
    await expect.poll(() => runtime.mesh().isolates()).toEqual([0]);

    post({ kind: "disconnect", workerId: 0 });
    await expect.poll(() => runtime.mesh().isolates()).toEqual([]);
    channel.port1.close();
  });

  it("logs a control message that fails instead of crashing", async () => {
    const first = new MessageChannel();
    const second = new MessageChannel();
    post({ kind: "connect", workerId: 0, port: first.port2 }, [first.port2]);
    post({ kind: "connect", workerId: 0, port: second.port2 }, [second.port2]);

    await expect.poll(() => facadeErrors).toEqual(["control message failed"]);
    first.port1.close();
    second.port1.close();
    second.port2.close();
  });

  it("ignores unknown control messages and keeps serving", async () => {
    post({ kind: "nope" } as unknown as ControlMessage);
    post({ kind: "spawn", seq: 1, name: "alive", recipe: { module: echoModule, actor: "Echo" } });

    await receivedWhere((m) => m.kind === "spawned");
  });

  it("stops on command: mesh closed, facade stopped, stopped posted", async () => {
    const channel = new MessageChannel();
    post({ kind: "connect", workerId: 0, port: channel.port2 }, [channel.port2]);
    await expect.poll(() => runtime.mesh().isolates()).toEqual([0]);

    post({ kind: "stop" });

    await receivedWhere((m) => m.kind === "stopped");
    expect(runtime.mesh().isolates()).toEqual([]);
    expect(facade.isRunning()).toBe(false);
    channel.port1.close();
  });

  it("stops cleanly on a control port without a close method", async () => {
    // Some worker-threads compatibility layers hand the boot port over
    // without close(); the stop sequence must still complete.
    const bare = new MessageChannel();
    const bareReceived: WorkerMessage[] = [];
    bare.port1.on("message", (message: unknown) => {
      bareReceived.push(message as WorkerMessage);
    });

    const closeless = {
      postMessage: (message: unknown): void => bare.port2.postMessage(message),
      on: (event: "message", listener: (value: unknown) => void): void => {
        bare.port2.on(event, listener);
      },
    };

    const system = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
    const local = new WorkerRuntime(
      system,
      new MessageRegistry(),
      closeless as unknown as MessagePort,
      2,
    );

    bare.port1.postMessage({ kind: "stop" });

    await expect.poll(() => bareReceived.some((m) => m.kind === "stopped")).toBe(true);
    expect(system.isRunning()).toBe(false);

    local.mesh().close();
    bare.port1.close();
    bare.port2.close();
  });

  it("answers a metrics request and gathers no siblings through its facade", async () => {
    const metered = new ActorSystem("sys", {
      logger: discardLogger,
      metrics: { enabled: true },
    });
    await metered.start();

    const channel = new MessageChannel();
    const inbox: WorkerMessage[] = [];
    channel.port1.on("message", (message: unknown) => {
      inbox.push(message as WorkerMessage);
    });
    const meteredRuntime = new WorkerRuntime(metered, new MessageRegistry(), channel.port2, 1);

    channel.port1.postMessage({ kind: "metrics", seq: 7 });
    await expect.poll(() => inbox.some((m) => m.kind === "metrics-reply")).toBe(true);
    const reply = inbox.find((m) => m.kind === "metrics-reply") as WorkerMessage;
    expect(reply.kind).toBe("metrics-reply");
    if (reply.kind === "metrics-reply") {
      expect(reply.seq).toBe(7);
      expect(reply.metrics).not.toBeNull();
    }

    // A facade sees only its own isolate: collectMetrics merges it with an
    // empty set of workers through the facade placement.
    const snapshot = await metered.collectMetrics();
    expect(snapshot.isolates).toBe(1);

    meteredRuntime.mesh().close();
    channel.port1.close();
    channel.port2.close();
    await metered.stop();
  });

  it("announces the stop of a placed actor so its name frees", async () => {
    post({ kind: "spawn", seq: 1, name: "leaver", recipe: { module: echoModule, actor: "Echo" } });
    await receivedWhere((m) => m.kind === "spawned");

    const pid = facade.actorOf("leaver");
    expect(pid).toBeDefined();
    await pid?.shutdown();

    const stopped = await receivedWhere((m) => m.kind === "actor-stopped");
    expect(stopped.kind === "actor-stopped" && stopped.name).toBe("leaver");
  });

  it("forwards facade dead letters over the control port", async () => {
    facade.toDeadletter(
      "nodeakt://sys@127.0.0.1:0/someone",
      "nodeakt://sys@127.0.0.1:0/ghost",
      "lost-hop",
      new Error("nobody home"),
    );

    const forwarded = await receivedWhere((m) => m.kind === "deadletter");
    expect(forwarded.kind).toBe("deadletter");
    if (forwarded.kind === "deadletter") {
      expect(forwarded.sender).toBe("nodeakt://sys@127.0.0.1:0/someone");
      expect(forwarded.receiver).toBe("nodeakt://sys@127.0.0.1:0/ghost");
      expect(forwarded.message).toEqual({ type: "", data: "lost-hop" });
      expect(forwarded.reason).toBe("nobody home");
    }
  });

  it("forwards a dead letter whose message cannot cross as a placeholder", async () => {
    class Secret {
      readonly hidden = true;
    }

    facade.toDeadletter(
      undefined,
      "nodeakt://sys@127.0.0.1:0/ghost",
      new Secret(),
      new Error("no route"),
    );

    const forwarded = await receivedWhere((m) => m.kind === "deadletter");
    if (forwarded.kind === "deadletter") {
      expect(forwarded.message).toEqual({ type: "", data: "unencodable message of type Secret" });
    }
  });

  it("names a rootless dead-letter message by its typeof in the placeholder", async () => {
    const rootless = Object.create(Object.create(null)) as object;

    facade.toDeadletter(
      undefined,
      "nodeakt://sys@127.0.0.1:0/ghost",
      rootless,
      new Error("no route"),
    );

    const forwarded = await receivedWhere((m) => m.kind === "deadletter");
    if (forwarded.kind === "deadletter") {
      expect(forwarded.message).toEqual({ type: "", data: "unencodable message of type object" });
    }
  });

  it("forwards only dead-letter events, ignoring other stream events", async () => {
    // Later runtime event kinds share the topic; simulate one arriving
    // ahead of a dead letter and prove only the dead letter forwards.
    (facade as unknown as { _events: EventStream })._events.publish(eventsTopic, "future-event");
    facade.toDeadletter(undefined, "nodeakt://sys@127.0.0.1:0/ghost", "after", new Error("gone"));

    await receivedWhere((m) => m.kind === "deadletter");
    expect(received.filter((m) => m.kind === "deadletter")).toHaveLength(1);
  });
});

describe("worker facade placement", () => {
  let mainSystem: ActorSystem;
  let facade: ActorSystem;
  let runtime: WorkerRuntime;
  let control: MessageChannel;
  let received: WorkerMessage[];

  class Probe implements Actor {
    preStart(): void {}

    receive(): void {}

    postStop(): void {}
  }

  function post(message: ControlMessage): void {
    control.port1.postMessage(message);
  }

  async function receivedWhere(
    predicate: (message: WorkerMessage) => boolean,
  ): Promise<WorkerMessage> {
    await expect.poll(() => received.some(predicate)).toBe(true);

    return received.find(predicate) as WorkerMessage;
  }

  /** Waits for the n-th frame of a kind, so repeated requests of the
   * same kind stay distinguishable. */
  async function frameOf(kind: WorkerMessage["kind"], index: number): Promise<WorkerMessage> {
    await expect.poll(() => received.filter((m) => m.kind === kind).length).toBeGreaterThan(index);

    return received.filter((m) => m.kind === kind)[index] as WorkerMessage;
  }

  beforeEach(async () => {
    mainSystem = new ActorSystem("sys", { logger: discardLogger });
    await mainSystem.start();
    facade = new ActorSystem("sys", { logger: discardLogger });
    await facade.start();
    control = new MessageChannel();
    received = [];
    control.port1.on("message", (message: unknown) => {
      received.push(message as WorkerMessage);
    });
    runtime = new WorkerRuntime(facade, new MessageRegistry(), control.port2, 1);
  });

  afterEach(async () => {
    runtime.mesh().close();
    control.port1.close();
    await facade.stop();
    await mainSystem.stop();
  });

  it("claims a top-level name with the control plane before an instance spawn", async () => {
    const spawning = facade.spawn("mine", new Probe());

    const claim = await receivedWhere((m) => m.kind === "claim");
    expect(claim.kind === "claim" && claim.name).toBe("mine");
    if (claim.kind !== "claim") {
      return;
    }

    post({ kind: "claimed", seq: claim.seq, error: null });
    const pid = await spawning;
    expect(facade.actorOf("mine")).toBe(pid);

    await pid.shutdown();
    await receivedWhere((m) => m.kind === "actor-stopped" && m.name === "mine");
  });

  it("refuses the spawn when the control plane refuses the claim, identity intact", async () => {
    const spawning = facade.spawn("taken", new Probe());

    const claim = await receivedWhere((m) => m.kind === "claim");
    if (claim.kind !== "claim") {
      return;
    }

    post({ kind: "claimed", seq: claim.seq, error: encodeError(ErrActorAlreadyExists) });

    await expect(spawning).rejects.toBe(ErrActorAlreadyExists);
    expect(facade.actorOf("taken")).toBeUndefined();
  });

  it("skips the claim for a name it is spawning on the plane's own order", async () => {
    post({ kind: "spawn", seq: 9, name: "shared", recipe: { module: slowModule, actor: "Slow" } });
    await new Promise((settle) => setTimeout(settle, 50));

    // The facade spawn of the same name needs no claim: the placement
    // in flight already owns the registration.
    await facade.spawn("shared", new Probe());
    expect(received.some((m) => m.kind === "claim")).toBe(false);

    // The recipe spawn settles one way or the other without ever
    // consulting the control plane for the name.
    await receivedWhere((m) => m.kind === "spawned" || m.kind === "spawn-failed");
    expect(received.some((m) => m.kind === "claim")).toBe(false);
  });

  it("replicates the name table and resolves routed handles through it", async () => {
    post({ kind: "name-added", name: "afar", workerId: 2 });
    await expect.poll(() => facade.actorOf("afar") !== undefined).toBe(true);

    const handle = facade.actorOf("afar") as PID;
    expect(handle.isRunning()).toBe(false);
    expect(handle.name()).toBe("afar");

    // A replica entry naming this very worker resolves through the
    // local tree, which already answered undefined.
    post({ kind: "name-added", name: "self-owned", workerId: 1 });
    await new Promise((settle) => setTimeout(settle, 20));
    expect(facade.actorOf("self-owned")).toBeUndefined();

    post({ kind: "name-freed", name: "afar" });
    await expect.poll(() => facade.actorOf("afar")).toBeUndefined();
  });

  it("places Props through the control plane and settles every outcome", async () => {
    class Echo implements Actor {
      constructor(readonly prefix: string) {}

      preStart(): void {}

      receive(): void {}

      postStop(): void {}
    }

    registerActor(Echo, echoModule);

    // Routed outcome: the actor landed on another worker.
    const placingA = facade.spawn("routed-p", Props.create(Echo, "a"));
    const placeA = (await frameOf("place", 0)) as Extract<WorkerMessage, { kind: "place" }>;
    expect(placeA.recipe).toEqual({ module: echoModule, actor: "Echo", args: ["a"] });
    post({
      kind: "placed",
      seq: placeA.seq,
      error: null,
      workerId: 2,
      path: "nodeakt://sys@127.0.0.1:0/routed-p",
      uid: "9",
    });
    const routed = await placingA;
    expect(routed.isRunning()).toBe(false);
    expect(routed.path().uid()).toBe("9");

    // Landed on this very isolate: the handle is the live PID.
    post({
      kind: "spawn",
      seq: 41,
      name: "here-p",
      recipe: { module: echoModule, actor: "Echo", args: ["h"] },
    });
    const spawned = (await receivedWhere((m) => m.kind === "spawned")) as Extract<
      WorkerMessage,
      { kind: "spawned" }
    >;
    const placingB = facade.spawn("here-p2", Props.create(Echo, "b"));
    const placeB = (await frameOf("place", 1)) as Extract<WorkerMessage, { kind: "place" }>;
    post({
      kind: "placed",
      seq: placeB.seq,
      error: null,
      workerId: 1,
      path: spawned.path,
      uid: spawned.uid,
    });
    const local = await placingB;
    expect(local.isRunning()).toBe(true);
    expect(local).toBe(facade.actorOf("here-p"));

    // Landed here, but nothing lives at the path: settles dead.
    const placingC = facade.spawn("here-p3", Props.create(Echo, "c"));
    const placeC = (await frameOf("place", 2)) as Extract<WorkerMessage, { kind: "place" }>;
    post({
      kind: "placed",
      seq: placeC.seq,
      error: null,
      workerId: 1,
      path: "nodeakt://sys@127.0.0.1:0/ghost",
      uid: "",
    });
    await expect(placingC).rejects.toBe(ErrDead);

    // A refusal travels back with its identity.
    const placingD = facade.spawn("denied", Props.create(Echo, "d"));
    const placeD = (await frameOf("place", 3)) as Extract<WorkerMessage, { kind: "place" }>;
    post({
      kind: "placed",
      seq: placeD.seq,
      error: encodeError(ErrActorAlreadyExists),
      workerId: 0,
      path: "",
      uid: "",
    });
    await expect(placingD).rejects.toBe(ErrActorAlreadyExists);

    // Late and forged settlements are no-ops.
    post({ kind: "placed", seq: 4242, error: null, workerId: 2, path: "x", uid: "" });
    post({ kind: "claimed", seq: 4243, error: null });
    await new Promise((settle) => setTimeout(settle, 20));
  });

  it("has nothing of its own to tear down", async () => {
    const placement = (facade as unknown as { _placement: { stop(): Promise<void> } })._placement;

    await expect(placement.stop()).resolves.toBeUndefined();
  });
});

describe("spawnRecipe", () => {
  it("resolves a class exported under a different name by its own name", async () => {
    const system = new ActorSystem("alias", { logger: discardLogger });
    await system.start();

    const pid = await spawnRecipe(system, "masked", { module: aliasedModule, actor: "Hidden" });

    await expect(system.noSender().ask(pid, "ping", 5000)).resolves.toBe("hidden:ping");
    await system.stop();
  });

  it("refuses module specifiers that are not files", async () => {
    const system = new ActorSystem("fence", { logger: discardLogger });
    await system.start();

    const specifiers = [
      "node:fs",
      "data:text/javascript,export class X {}",
      "https://evil.example/actor.mjs",
    ];
    for (const module of specifiers) {
      await expect(spawnRecipe(system, "fenced", { module, actor: "X" })).rejects.toThrow(
        "must be a file URL or a plain path",
      );
    }

    await system.stop();
  });

  it("refuses an export that does not look like an actor class", async () => {
    const system = new ActorSystem("shape", { logger: discardLogger });
    await system.start();

    await expect(
      spawnRecipe(system, "shapeless", { module: echoModule, actor: "Receiveless" }),
    ).rejects.toThrow('does not export an actor class named "Receiveless"');

    await system.stop();
  });

  it("rebuilds the reentrancy option a recipe carries", async () => {
    const system = new ActorSystem("reentrant-recipe", { logger: discardLogger });
    await system.start();

    const pid = await spawnRecipe(system, "reentrant", {
      module: aliasedModule,
      actor: "Hidden",
      reentrancy: { mode: "allowAll" },
    });

    await expect(system.noSender().ask(pid, "ping", 5000)).resolves.toBe("hidden:ping");
    await system.stop();
  });

  it("rebuilds the passivation strategy a recipe carries", async () => {
    const system = new ActorSystem("passivating-recipe", { logger: discardLogger });
    await system.start();

    const pid = await spawnRecipe(system, "passivating", {
      module: aliasedModule,
      actor: "Hidden",
      passivation: { kind: PASSIVATION_TIME_BASED, timeout: 60_000 },
    });

    await expect(system.noSender().ask(pid, "ping", 5000)).resolves.toBe("hidden:ping");
    await system.stop();
  });
});

describe("applySetup", () => {
  it("applies a setup module's registrations to the registry", async () => {
    const registry = new MessageRegistry();

    await applySetup(registry, setupModule);

    expect(registry.classOf("test.Job")?.name).toBe("Job");
    expect(registry.classOf("test.Receipt")?.name).toBe("Receipt");
  });

  it("refuses a module without a default registration function", async () => {
    await expect(applySetup(new MessageRegistry(), echoModule)).rejects.toThrow(
      "does not default-export a registration function",
    );
  });
});
