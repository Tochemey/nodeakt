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

import { describe, expect, it, vi } from "vitest";
import type { Actor } from "../src/actor";
import type { ActorSystem } from "../src/actor.system";
import { discardLogger } from "../src/discard.logger";
import { PanicSignal, PostStart, Terminated } from "../src/messages";
import { newPath } from "../src/path";
import { PID } from "../src/pid";
import { createReceiveContext } from "../src/receive.context";
import {
  isSystemName,
  reservedNamesPrefix,
  rootGuardianName,
  systemGuardianName,
  userGuardianName,
} from "../src/reserved";
import { RootGuardian } from "../src/root.guardian";
import { SystemGuardian } from "../src/system.guardian";
import { UserGuardian } from "../src/user.guardian";

const noop: Actor = {
  preStart(): void {},
  receive(): void {},
  postStop(): void {},
};

// Stands in for the NoSender actor when a running stub system announces
// PostStart during start; the announcement is swallowed.
const announcer = { tell: (): null => null } as unknown as PID;

function stubSystem(running = true): ActorSystem {
  return {
    start: vi.fn(),
    stop: vi.fn(),
    spawn: vi.fn(),
    name: () => "sys",
    isRunning: () => running,
    noSender: () => announcer,
    logger: () => discardLogger,
  } as unknown as ActorSystem;
}

function makePid(actor: Actor, name: string, system: ActorSystem): PID {
  return new PID(actor, newPath(name, "sys", "127.0.0.1", 0), system);
}

async function startGuardian(actor: Actor, name: string, system: ActorSystem): Promise<PID> {
  const pid = makePid(actor, name, system);
  await pid.start();
  return pid;
}

// The sending side for messages told from outside an actor. It never
// starts: tell only checks the target's state.
const external = makePid(noop, "external", stubSystem());

describe("isSystemName", () => {
  it("recognizes reserved runtime names", () => {
    expect(isSystemName(rootGuardianName)).toBe(true);
    expect(isSystemName(systemGuardianName)).toBe(true);
    expect(isSystemName(userGuardianName)).toBe(true);
    expect(isSystemName(`${reservedNamesPrefix}DeathWatch`)).toBe(true);
  });

  it("rejects user actor names", () => {
    expect(isSystemName("user-1")).toBe(false);
    expect(isSystemName("nodeakt")).toBe(false);
  });
});

describe.each([
  { label: "RootGuardian", name: rootGuardianName, create: () => new RootGuardian() },
  {
    label: "SystemGuardian",
    name: systemGuardianName,
    create: () => new SystemGuardian(),
  },
])("$label", ({ name, create }) => {
  it("stops the actor system when a runtime actor panics", async () => {
    const system = stubSystem();
    const guardian = await startGuardian(create(), name, system);
    const failing = makePid(noop, `${reservedNamesPrefix}DeathWatch`, system);

    failing.tell(guardian, new PanicSignal(new Error("boom")));

    await expect.poll(() => vi.mocked(system.stop).mock.calls.length).toBe(1);
  });

  it("ignores a panic from a user actor", async () => {
    const system = stubSystem();
    const guardian = await startGuardian(create(), name, system);
    const failing = makePid(noop, "user-1", system);

    failing.tell(guardian, new PanicSignal(new Error("boom")));
    external.tell(guardian, new PostStart());
    await expect.poll(() => guardian.isRunning()).toBe(true);

    expect(system.stop).not.toHaveBeenCalled();
  });

  it("does not stop a system that is not running", async () => {
    const system = stubSystem(false);
    const guardian = await startGuardian(create(), name, system);
    const failing = makePid(noop, `${reservedNamesPrefix}DeathWatch`, system);

    failing.tell(guardian, new PanicSignal(new Error("boom")));
    external.tell(guardian, new PostStart());
    await expect.poll(() => guardian.isRunning()).toBe(true);

    expect(system.stop).not.toHaveBeenCalled();
  });

  it("acknowledges lifecycle messages without effect", async () => {
    const system = stubSystem();
    const guardian = await startGuardian(create(), name, system);

    expect(external.tell(guardian, new PostStart())).toBeNull();
    expect(external.tell(guardian, new Terminated("nodeakt://sys@h:1/a"))).toBeNull();

    await expect.poll(() => guardian.isRunning()).toBe(true);
    expect(system.stop).not.toHaveBeenCalled();
  });

  it("ignores unrecognized messages", async () => {
    const system = stubSystem();
    const guardian = await startGuardian(create(), name, system);

    expect(external.tell(guardian, "noise")).toBeNull();

    await expect.poll(() => guardian.isRunning()).toBe(true);
    expect(system.stop).not.toHaveBeenCalled();
  });

  it("ignores a panic on a detached or senderless context", async () => {
    const actor = create();
    const system = stubSystem();
    const guardian = makePid(actor, name, system);
    await guardian.start();

    // Detached: without self there is no actor system to stop.
    actor.receive(createReceiveContext(new PanicSignal(new Error("boom"))));

    // Attached but senderless: the origin of the panic is unknown.
    actor.receive(createReceiveContext(new PanicSignal(new Error("boom")), guardian));

    expect(system.stop).not.toHaveBeenCalled();
  });
});

describe("UserGuardian", () => {
  it("ignores panic signals and lifecycle messages", async () => {
    const system = stubSystem();
    const guardian = await startGuardian(new UserGuardian(), userGuardianName, system);
    const failing = makePid(noop, `${reservedNamesPrefix}DeathWatch`, system);

    expect(failing.tell(guardian, new PanicSignal(new Error("boom")))).toBeNull();
    expect(external.tell(guardian, new PostStart())).toBeNull();
    expect(external.tell(guardian, new Terminated("nodeakt://sys@h:1/a"))).toBeNull();

    await expect.poll(() => guardian.isRunning()).toBe(true);
    expect(system.stop).not.toHaveBeenCalled();
  });
});
