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
import type { Context } from "../../src/context";
import { ErrExtensionAlreadyExists, ErrInvalidExtensionId } from "../../src/errors";
import type { Extension } from "../../src/extension/extension";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";

/** A shared service that records what the actors appended to it. */
class EventStore implements Extension {
  readonly appended: string[] = [];

  id(): string {
    return "eventStore";
  }

  append(event: string): void {
    this.appended.push(event);
  }
}

/** A second service, so a lookup proves it resolves by identifier. */
class FeatureFlags implements Extension {
  id(): string {
    return "featureFlags";
  }

  enabled(): boolean {
    return true;
  }
}

/** An extension reporting an identifier the syntax rules reject. */
class Nameless implements Extension {
  id(): string {
    return "bad name";
  }
}

/**
 * An actor that reaches the shared store from every context it is handed:
 * the lifecycle Context in `preStart` and `postStop`, and the
 * ReceiveContext of each message.
 */
class Journal implements Actor {
  installed = 0;

  preStart(ctx: Context): void {
    this.installed = ctx.extensions().length;
    ctx.extension<EventStore>("eventStore")?.append("started");
  }

  receive(ctx: ReceiveContext): void {
    const store: EventStore | undefined = ctx.extension<EventStore>("eventStore");
    store?.append(`received:${ctx.extensions().length}`);
  }

  postStop(ctx: Context): void {
    ctx.extension<EventStore>("eventStore")?.append("stopped");
  }
}

describe("ActorSystem extensions", () => {
  it("installs the extensions it was created with", () => {
    const store: EventStore = new EventStore();
    const flags: FeatureFlags = new FeatureFlags();
    const system: ActorSystem = new ActorSystem("sys", { extensions: [store, flags] });

    expect(system.extension<EventStore>("eventStore")).toBe(store);
    expect(system.extension<FeatureFlags>("featureFlags")).toBe(flags);
    expect(system.extensions()).toEqual([store, flags]);
  });

  it("answers undefined when the system carries no such extension", () => {
    const system: ActorSystem = new ActorSystem("sys");

    expect(system.extension("eventStore")).toBeUndefined();
    expect(system.extensions()).toEqual([]);
  });

  it("fails construction on an invalid or duplicate identifier", () => {
    expect(() => new ActorSystem("sys", { extensions: [new Nameless()] })).toThrow(
      ErrInvalidExtensionId,
    );
    expect(
      () => new ActorSystem("sys", { extensions: [new EventStore(), new EventStore()] }),
    ).toThrow(ErrExtensionAlreadyExists);
  });

  it("reaches the same instance from an actor's contexts", async () => {
    const store: EventStore = new EventStore();
    const system: ActorSystem = new ActorSystem("sys", { extensions: [store] });
    await system.start();

    const actor: Journal = new Journal();
    const pid: PID = await system.spawn("journal", actor);
    // PostStart, then the message below: both handlers see the same store.
    system.noSender().tell(pid, "order");
    await expect.poll(() => store.appended.length).toBe(3);

    // The lifecycle context outlives the mailbox: postStop appends the last entry.
    await system.stop();

    expect(actor.installed).toBe(1);
    expect(store.appended).toEqual(["started", "received:1", "received:1", "stopped"]);
  });
});
