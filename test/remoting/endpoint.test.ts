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
import { discardLogger } from "../../src/discard.logger";
import type { PID } from "../../src/pid";

/** A do-nothing actor, enough to mint a path and be looked up. */
class Idle implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** A system with remoting enabled on an ephemeral loopback port. */
function remoteSystem(name: string): ActorSystem {
  return new ActorSystem(name, {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0 },
  });
}

describe("remoting endpoint", () => {
  it("binds a listener and reports the bound address", async () => {
    const system: ActorSystem = remoteSystem("orders");
    await system.start();

    try {
      expect(system.host()).toBe("127.0.0.1");
      expect(system.port()).toBeGreaterThan(0);
    } finally {
      await system.stop();
    }
  });

  it("advertises the bound endpoint in actor paths and resolves them", async () => {
    const system: ActorSystem = remoteSystem("orders");
    await system.start();

    try {
      const pid: PID = await system.spawn("greeter", new Idle());
      const port: number = system.port();
      expect(pid.path().toString()).toBe(`nodeakt://orders@127.0.0.1:${port}/greeter`);
      expect(system.actorOf("greeter")).toBe(pid);
    } finally {
      await system.stop();
    }
  });

  it("stays single-node and unbound without remote options", async () => {
    const system: ActorSystem = new ActorSystem("orders", { logger: discardLogger });
    await system.start();

    try {
      expect(system.host()).toBe("127.0.0.1");
      expect(system.port()).toBe(0);

      const pid: PID = await system.spawn("greeter", new Idle());
      expect(pid.path().toString()).toBe("nodeakt://orders@127.0.0.1:0/greeter");
      expect(system.actorOf("greeter")).toBe(pid);
    } finally {
      await system.stop();
    }
  });

  it("releases the port on stop and rebinds on restart", async () => {
    const system: ActorSystem = remoteSystem("orders");

    await system.start();
    const first: number = system.port();
    expect(first).toBeGreaterThan(0);
    await system.stop();

    await system.start();
    const second: number = system.port();
    expect(second).toBeGreaterThan(0);

    try {
      const pid: PID = await system.spawn("greeter", new Idle());
      expect(pid.path().toString()).toBe(`nodeakt://orders@127.0.0.1:${second}/greeter`);
    } finally {
      await system.stop();
    }
  });

  it("runs two remote systems on distinct ports", async () => {
    const one: ActorSystem = remoteSystem("alpha");
    const two: ActorSystem = remoteSystem("beta");
    await one.start();
    await two.start();

    try {
      expect(one.port()).toBeGreaterThan(0);
      expect(two.port()).toBeGreaterThan(0);
      expect(one.port()).not.toBe(two.port());
    } finally {
      await one.stop();
      await two.stop();
    }
  });
});
