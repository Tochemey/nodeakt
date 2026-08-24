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

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import type { Actor } from "../../src/actor";
import { ActorSystem } from "../../src/actor.system";
import { discardLogger } from "../../src/discard.logger";
import type { EntryLevel, Fields, LazyFields, Level, Logger } from "../../src/logger";
import { Terminated } from "../../src/messages";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { registerMessage } from "../../src/registration";
import type { TlsOptions } from "../../src/remote.options";
import { sleep, until } from "./helpers";

/**
 * TLS as per-system remoting configuration: every connection of a
 * `tls`-configured system is encrypted, dialed and accepted alike, and
 * the seam above the carrier behaves byte-identically to plaintext.
 * The refusal cases pin the all-or-nothing rule at the system level: a
 * mixed pair or untrusted material surfaces as the connection failure
 * it is, never as a silent plaintext fallback.
 */

/** A fixture file's PEM contents. */
function pemContents(name: string): string {
  return readFileSync(new URL(`../net/tls/${name}`, import.meta.url), "utf8");
}

/** A fixture file's filesystem path. */
function pemPath(name: string): string {
  return fileURLToPath(new URL(`../net/tls/${name}`, import.meta.url));
}

/** The trusted material every TLS system of these tests runs on. */
function trustedTls(): TlsOptions {
  return {
    cert: pemContents("node.pem"),
    key: pemContents("node.key"),
    ca: pemContents("ca.pem"),
  };
}

/** Records warn-level messages so the endpoint's refusal signal is
 * observable; every other level is discarded. */
class WarnLogger implements Logger {
  readonly warns: string[] = [];

  debug(): void {}

  info(): void {}

  warn(message: string, _fields?: LazyFields): void {
    this.warns.push(message);
  }

  error(): void {}

  level(): Level {
    return "warn";
  }

  enabled(level: EntryLevel): boolean {
    return level === "warn" || level === "error";
  }

  with(_fields: Fields): Logger {
    return this;
  }
}

function tlsSystem(name: string, tls: TlsOptions): ActorSystem {
  return new ActorSystem(name, {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0, tls },
  });
}

class TlsPing {
  constructor(readonly n: number) {}
}

class TlsPong {
  constructor(readonly n: number) {}
}

registerMessage(TlsPing);
registerMessage(TlsPong);

/** Answers every ping with a pong one higher. */
class Echo implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof TlsPing) {
      ctx.response(new TlsPong(ctx.message.n + 1));
    }
  }

  postStop(): void {}
}

/** Collects every Terminated it is notified with. */
class Watcher implements Actor {
  readonly terminated: string[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Terminated) {
      this.terminated.push(ctx.message.actorPath);
    }
  }

  postStop(): void {}
}

describe("remoting over TLS", () => {
  it("round-trips lookup, ask, watch, and node death over encrypted connections", async () => {
    const a: ActorSystem = tlsSystem("alpha", trustedTls());
    const b: ActorSystem = tlsSystem("beta", trustedTls());
    await a.start();
    await b.start();

    try {
      const watcher: Watcher = new Watcher();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      await b.spawn("echo", new Echo());

      const remote: PID = (await a.remoteLookup("127.0.0.1", b.port(), "echo")) as PID;
      const answer: unknown = await a.noSender().ask(remote, new TlsPing(41), 5000);
      expect(answer).toBeInstanceOf(TlsPong);
      expect((answer as TlsPong).n).toBe(42);

      watcherPid.watch(remote);
      await sleep(50);
      await b.stop();
      await until("the Terminated", (): boolean => watcher.terminated.length >= 1);
      expect(watcher.terminated[0]).toBe(remote.path().toString());
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("accepts certificate material as file paths", async () => {
    const paths: TlsOptions = {
      cert: pemPath("node.pem"),
      key: pemPath("node.key"),
      ca: pemPath("ca.pem"),
    };
    const a: ActorSystem = tlsSystem("alpha", paths);
    const b: ActorSystem = tlsSystem("beta", paths);
    await a.start();
    await b.start();

    try {
      await b.spawn("echo", new Echo());
      const remote: PID | undefined = await a.remoteLookup("127.0.0.1", b.port(), "echo");
      expect(remote).toBeDefined();
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("verifies against the runtime trust store when no ca is given", async () => {
    const system: ActorSystem = tlsSystem("alpha", {
      cert: pemContents("node.pem"),
      key: pemContents("node.key"),
    });
    await system.start();

    try {
      // The runtime's default trust store does not know the
      // self-signed fixture authority, so even this node's own
      // endpoint is refused: encrypted, but unverifiable.
      await expect(
        system.remoteLookup("127.0.0.1", system.port(), "nobody"),
      ).rejects.toBeInstanceOf(Error);
    } finally {
      await system.stop();
    }
  });

  it("fails start when material is neither PEM contents nor a readable file", async () => {
    const system: ActorSystem = tlsSystem("alpha", {
      cert: "/nowhere/does-not-exist.pem",
      key: pemContents("node.key"),
    });

    await expect(system.start()).rejects.toBeInstanceOf(TypeError);
  });

  it("refuses a node whose certificate the system does not trust", async () => {
    // The far node serves a certificate from an authority this system
    // does not trust; the dial is refused before any frame crosses.
    const a: ActorSystem = tlsSystem("alpha", trustedTls());
    const b: ActorSystem = tlsSystem("beta", {
      cert: pemContents("other.pem"),
      key: pemContents("other.key"),
      ca: pemContents("ca.pem"),
    });
    await a.start();
    await b.start();

    try {
      await b.spawn("echo", new Echo());
      await expect(a.remoteLookup("127.0.0.1", b.port(), "echo")).rejects.toBeInstanceOf(Error);
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("refuses a plaintext node dialing a TLS node and logs the refusal", async () => {
    const plain: ActorSystem = new ActorSystem("alpha", {
      logger: discardLogger,
      remote: { host: "127.0.0.1", port: 0 },
    });
    // The TLS node is the only side that sees why a plaintext dialer
    // fails its carrier handshake, so it is the side that must log it.
    const warns: WarnLogger = new WarnLogger();
    const b: ActorSystem = new ActorSystem("beta", {
      logger: warns,
      remote: { host: "127.0.0.1", port: 0, tls: trustedTls() },
    });
    await plain.start();
    await b.start();

    try {
      await b.spawn("echo", new Echo());
      await expect(plain.remoteLookup("127.0.0.1", b.port(), "echo")).rejects.toBeInstanceOf(Error);
      await until("the endpoint to log the refusal", (): boolean => warns.warns.length >= 1);
      expect(warns.warns[0]).toContain("refused");
    } finally {
      await plain.stop();
      await b.stop();
    }
  });
});

describe("mutual TLS", () => {
  it("round-trips between nodes that verify each other's certificates", async () => {
    const mutual: TlsOptions = { ...trustedTls(), requestCert: true };
    const a: ActorSystem = tlsSystem("alpha", mutual);
    const b: ActorSystem = tlsSystem("beta", mutual);
    await a.start();
    await b.start();

    try {
      await b.spawn("echo", new Echo());
      const remote: PID = (await a.remoteLookup("127.0.0.1", b.port(), "echo")) as PID;
      const answer: unknown = await a.noSender().ask(remote, new TlsPing(1), 5000);
      expect(answer).toBeInstanceOf(TlsPong);
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("refuses a client certificate from an untrusted authority", async () => {
    const b: ActorSystem = tlsSystem("beta", { ...trustedTls(), requestCert: true });
    // The dialer trusts the far node, but its own certificate comes
    // from an authority the far node does not accept.
    const c: ActorSystem = tlsSystem("gamma", {
      cert: pemContents("other.pem"),
      key: pemContents("other.key"),
      ca: pemContents("ca.pem"),
    });
    await b.start();
    await c.start();

    try {
      await b.spawn("echo", new Echo());
      await expect(c.remoteLookup("127.0.0.1", b.port(), "echo")).rejects.toBeInstanceOf(Error);
    } finally {
      await b.stop();
      await c.stop();
    }
  });
});
