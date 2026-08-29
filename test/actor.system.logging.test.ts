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
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import type { Fields, LazyFields, Level, Logger } from "../src/logger";
import { MessagesCountBasedStrategy } from "../src/passivation";
import type { PID } from "../src/pid";
import type { ReceiveContext } from "../src/receive.context";
import { RestartDirective, Supervisor } from "../src/supervisor";

/** One captured entry: its level, message, and merged fields. */
interface Entry {
  readonly level: string;
  readonly message: string;
  readonly fields: Record<string, unknown>;
}

/** A {@link Logger} that records every entry, so a test can assert what the
 * runtime reported. It stays enabled at every level. */
class RecordingLogger implements Logger {
  constructor(
    readonly entries: Entry[] = [],
    private readonly bound: Record<string, unknown> = {},
  ) {}

  debug(message: string, fields?: LazyFields): void {
    this.#record("debug", message, fields);
  }

  info(message: string, fields?: LazyFields): void {
    this.#record("info", message, fields);
  }

  warn(message: string, fields?: LazyFields): void {
    this.#record("warn", message, fields);
  }

  error(message: string, fields?: LazyFields): void {
    this.#record("error", message, fields);
  }

  level(): Level {
    return "debug";
  }

  enabled(): boolean {
    return true;
  }

  with(fields: Fields): Logger {
    return new RecordingLogger(this.entries, { ...this.bound, ...fields });
  }

  messages(): string[] {
    return this.entries.map((entry: Entry): string => `${entry.level}:${entry.message}`);
  }

  find(message: string): Entry | undefined {
    return this.entries.find((entry: Entry): boolean => entry.message === message);
  }

  #record(level: string, message: string, fields?: LazyFields): void {
    const resolved: Fields = typeof fields === "function" ? fields() : (fields ?? {});
    this.entries.push({ level, message, fields: { ...this.bound, ...resolved } });
  }
}

const idle: Actor = {
  preStart(): void {},
  receive(): void {},
  postStop(): void {},
};

describe("ActorSystem lifecycle logging", () => {
  it("logs starting with a runtime banner, started, stopping, and stopped", async () => {
    const log: RecordingLogger = new RecordingLogger();
    const system: ActorSystem = new ActorSystem("orders", { logger: log });

    await system.start();
    await system.stop();

    expect(log.messages()).toEqual(
      expect.arrayContaining([
        "info:actor system starting",
        "info:actor system started",
        "info:actor system stopping",
        "info:actor system stopped",
      ]),
    );

    const starting: Entry | undefined = log.find("actor system starting");
    expect(starting?.fields.name).toBe("orders");
    expect(typeof starting?.fields.runtime).toBe("string");
    expect(starting?.fields.os).toBe(`${process.platform}/${process.arch}`);
    expect(typeof starting?.fields.version).toBe("string");
  });

  it("logs the spawn attempt at debug and the success at info", async () => {
    const log: RecordingLogger = new RecordingLogger();
    const system: ActorSystem = new ActorSystem("spawns", { logger: log });
    await system.start();

    await system.spawn("greeter", idle);

    const spawning: Entry | undefined = log.find("spawning actor");
    expect(spawning?.level).toBe("debug");
    expect(spawning?.fields.actor).toBe("greeter");

    const spawned: Entry | undefined = log.find("actor spawned");
    expect(spawned?.level).toBe("info");
    expect(spawned?.fields.actor).toBe("greeter");

    await system.stop();
  });

  it("logs a watch and an unwatch at debug", async () => {
    const log: RecordingLogger = new RecordingLogger();
    const system: ActorSystem = new ActorSystem("watches", { logger: log });
    await system.start();

    const watcher: PID = await system.spawn("watcher", idle);
    const watched: PID = await system.spawn("watched", idle);

    watcher.watch(watched);
    watcher.unWatch(watched);

    const watch: Entry | undefined = log.find("watching actor");
    expect(watch?.level).toBe("debug");
    expect(watch?.fields.watcher).toContain("watcher");
    expect(watch?.fields.watched).toContain("watched");

    expect(log.find("unwatching actor")?.level).toBe("debug");

    await system.stop();
  });

  it("logs the shutdown flow: watched actors released and watchers notified", async () => {
    const log: RecordingLogger = new RecordingLogger();
    const system: ActorSystem = new ActorSystem("shutdowns", { logger: log });
    await system.start();

    const target: PID = await system.spawn("target", idle);
    const watcher: PID = await system.spawn("observer", idle);
    watcher.watch(target);

    // Stopping the watcher releases the actor it watched; stopping the target
    // then notifies its remaining watchers.
    await watcher.shutdown();
    const released: Entry | undefined = log.find("releasing watched actors");
    expect(released?.level).toBe("debug");
    expect(released?.fields.actor).toContain("observer");
    expect(released?.fields.watched).toBe(1);

    const survivor: PID = await system.spawn("survivor", idle);
    survivor.watch(target);
    await target.shutdown();
    const notified: Entry | undefined = log.find("notifying watchers");
    expect(notified?.level).toBe("debug");
    expect(notified?.fields.watchers).toBe(1);

    await system.stop();
  });

  it("logs a failed stop at error and rethrows", async () => {
    const log: RecordingLogger = new RecordingLogger();
    const system: ActorSystem = new ActorSystem("brittle", { logger: log });
    await system.start();

    // Run the real teardown so resources are released, then fail, exercising
    // the failure path without leaking handles.
    const boom: Error = new Error("teardown failed");
    const internals = system as unknown as { teardown: () => Promise<void> };
    const realTeardown: () => Promise<void> = internals.teardown.bind(system);
    internals.teardown = async (): Promise<void> => {
      await realTeardown();
      throw boom;
    };

    await expect(system.stop()).rejects.toBe(boom);

    const failed: Entry | undefined = log.find("actor system failed to stop");
    expect(failed?.level).toBe("error");
    expect(failed?.fields.name).toBe("brittle");
    expect(failed?.fields.error).toBe(boom);
  });

  it("logs a supervised failure at warn and the restart at debug", async () => {
    const log: RecordingLogger = new RecordingLogger();
    const system: ActorSystem = new ActorSystem("supervised", { logger: log });
    await system.start();

    const boom: Error = new Error("kaboom");
    const flaky: Actor = {
      preStart(): void {},
      receive(ctx: ReceiveContext): void {
        if (ctx.message === "boom") {
          throw boom;
        }
      },
      postStop(): void {},
    };
    const pid: PID = await system.spawn("flaky", flaky, {
      supervisor: new Supervisor({ anyErrorDirective: RestartDirective }),
    });
    system.noSender().tell(pid, "boom");

    await expect.poll(() => log.find("actor failed") !== undefined, { timeout: 2000 }).toBe(true);
    const failed: Entry | undefined = log.find("actor failed");
    expect(failed?.level).toBe("warn");
    expect(failed?.fields.actor).toContain("flaky");
    expect(failed?.fields.directive).toBe("restart");
    expect(failed?.fields.error).toBe(boom);

    await expect
      .poll(() => log.find("actor restarted") !== undefined, { timeout: 2000 })
      .toBe(true);
    expect(log.find("actor restarted")?.level).toBe("debug");

    await system.stop();
  });

  it("logs an actor passivation at info", async () => {
    const log: RecordingLogger = new RecordingLogger();
    const system: ActorSystem = new ActorSystem("idlers", { logger: log });
    await system.start();

    const pid: PID = await system.spawn("worker", idle, {
      passivationStrategy: new MessagesCountBasedStrategy(1),
    });
    system.noSender().tell(pid, "wake");

    await expect
      .poll(() => log.find("actor passivated") !== undefined, { timeout: 2000 })
      .toBe(true);
    expect(log.find("actor passivated")?.fields.actor).toContain("worker");

    await system.stop();
  });
});
