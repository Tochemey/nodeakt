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

import { afterEach, describe, expect, it } from "vitest";
import type { Actor } from "../../src/actor";
import { ActorSystem } from "../../src/actor.system";
import { discardLogger } from "../../src/discard.logger";
import type { DataEnvelope, Hello } from "../../src/net/envelope";
import { SERIALIZER_BINARY } from "../../src/net/envelope";
import type { NetServer } from "../../src/net/server";
import type { Session } from "../../src/net/session";
import { ByteWriter, encodeValue } from "../../src/net/values";
import type { PID } from "../../src/pid";
import { cleanupNet, dialSession, hello, startServer } from "../net/helpers";
import { remoteSystem } from "./helpers";

/** A do-nothing actor, enough to mint a path and be looked up. */
class Idle implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
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

describe("the advertised endpoint identity", () => {
  afterEach(cleanupNet);

  function nullPayload(): Uint8Array {
    const writer: ByteWriter = new ByteWriter();
    encodeValue(writer, null);
    return writer.bytes().slice();
  }

  it("advertises the bound port to dialers when configured ephemeral", async () => {
    const system: ActorSystem = remoteSystem("orders");
    await system.start();

    try {
      const session: Session = await dialSession(system.port());
      expect((session.remote as Hello).port).toBe(system.port());
      expect((session.remote as Hello).systemName).toBe("orders");
    } finally {
      await system.stop();
    }
  });

  it("advertises a nonzero configured port verbatim", async () => {
    const server: NetServer = await startServer({
      local: hello({ systemName: "fixed", port: 7777 }),
    });
    const session: Session = await dialSession(server.address.port);
    expect((session.remote as Hello).port).toBe(7777);
  });

  it("advertises the bound port in the HELLO of every dialed peer", async () => {
    const system: ActorSystem = remoteSystem("orders");
    await system.start();

    try {
      const remotes: Hello[] = [];
      const server: NetServer = await startServer(
        {},
        {
          onSession: (session: Session): void => {
            remotes.push(session.remote as Hello);
          },
          onData: (session: Session, _envelope: DataEnvelope, correlation: number): void => {
            session.reply(correlation, {
              serializerId: SERIALIZER_BINARY,
              typeRef: "",
              payload: nullPayload(),
            });
          },
        },
      );

      expect(await system.remoteLookup("127.0.0.1", server.address.port, "x")).toBeUndefined();
      expect(remotes.length).toBeGreaterThan(0);
      expect((remotes[0] as Hello).port).toBe(system.port());
    } finally {
      await system.stop();
    }
  });
});
