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

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Actor } from "../src/actor/actor";
import { ActorSystem } from "../src/actor/actor.system";
import { Deadletter, PostStart } from "../src/actor/messages";
import type { PID } from "../src/actor/pid";
import type { PipeTask } from "../src/actor/pipe";
import type { PipeOptions } from "../src/actor/pipe.options";
import type { ReceiveContext } from "../src/actor/receive.context";
import { ErrDead, ErrPipeTimeout, ErrUndefinedTask } from "../src/errors/errors";
import { discardLogger } from "../src/logger/discard.logger";
import type { Logger } from "../src/logger/logger";

class Load {
  constructor(readonly id: number) {}
}

class Order {
  constructor(readonly id: number) {}
}

class Ping {}

interface Received {
  message: unknown;
  sender: PID | undefined;
}

/** Records every business message together with who sent it. */
class Recorder implements Actor {
  readonly received: Received[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.received.push({ message: ctx.message, sender: ctx.sender });
  }

  postStop(): void {}
}

class DoPipe {
  constructor(
    readonly to: PID,
    readonly task: PipeTask,
    readonly options?: PipeOptions,
  ) {}
}

class DoPipeName {
  constructor(
    readonly name: string,
    readonly task: PipeTask,
    readonly options?: PipeOptions,
  ) {}
}

/** Pipes on command, exercising the ReceiveContext surface. */
class Piper implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const msg: unknown = ctx.message;

    if (msg instanceof DoPipe) {
      ctx.pipeTo(msg.to, msg.task, msg.options);
    } else if (msg instanceof DoPipeName) {
      ctx.pipeToName(msg.name, msg.task, msg.options);
    }
  }

  postStop(): void {}
}

/** The issue's shape: pipes a fetch to itself and receives the result
 * as an ordinary message. */
class Loader implements Actor {
  readonly orders: Order[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const msg: unknown = ctx.message;

    if (msg instanceof Load) {
      ctx.pipeTo(ctx.self as PID, Promise.resolve(new Order(msg.id)));
    } else if (msg instanceof Order) {
      this.orders.push(msg);
    }
  }

  postStop(): void {}
}

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise: Promise<T> = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

describe("pipeTo", () => {
  let system: ActorSystem;
  let letters: Deadletter[];

  beforeEach(async () => {
    system = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
    letters = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        letters.push(event);
      }
    });
  });

  afterEach(async () => {
    await system.stop();
  });

  it("delivers the resolution value with the piping actor as sender", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());

    piper.pipeTo(target, Promise.resolve(new Order(7)));

    await expect.poll(() => recorder.received.length).toBe(1);
    const entry: Received = recorder.received[0] as Received;
    expect(entry.message).toBeInstanceOf(Order);
    expect((entry.message as Order).id).toBe(7);
    expect(entry.sender).toBe(piper);
    expect(letters).toHaveLength(0);
  });

  it("accepts a thunk and hands it an abort signal", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());

    let seen: AbortSignal | undefined;
    piper.pipeTo(target, (signal) => {
      seen = signal;
      return Promise.resolve(new Order(1));
    });

    await expect.poll(() => recorder.received.length).toBe(1);
    expect(seen).toBeInstanceOf(AbortSignal);
    expect(seen?.aborted).toBe(false);
  });

  it("returns immediately and keeps the actor processing", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());
    const task: Deferred<Order> = deferred<Order>();

    piper.pipeTo(target, task.promise);
    system.noSender().tell(target, new Ping());

    await expect.poll(() => recorder.received.length).toBe(1);
    expect(recorder.received[0]?.message).toBeInstanceOf(Ping);

    task.resolve(new Order(2));
    await expect.poll(() => recorder.received.length).toBe(2);
    expect(recorder.received[1]?.message).toBeInstanceOf(Order);
  });

  it("pipes to self from receive", async () => {
    const loader: Loader = new Loader();
    const pid: PID = await system.spawn("loader", loader);

    system.noSender().tell(pid, new Load(42));

    await expect.poll(() => loader.orders.length).toBe(1);
    expect(loader.orders[0]?.id).toBe(42);
  });

  it("routes a rejected task to dead letters and delivers nothing", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());

    piper.pipeTo(target, Promise.reject(new Error("boom")));

    await expect.poll(() => letters.length).toBe(1);
    const letter: Deadletter | undefined = letters[0];
    expect(letter?.reason).toBe("boom");
    expect(letter?.sender).toBe(piper.path().toString());
    expect(letter?.receiver).toBe(target.path().toString());
    expect(letter?.message).toBeUndefined();
    expect(recorder.received).toHaveLength(0);
  });

  it("normalizes a non-Error rejection value", async () => {
    const target: PID = await system.spawn("target", new Recorder());
    const piper: PID = await system.spawn("piper", new Piper());

    piper.pipeTo(target, Promise.reject("bad wire"));

    await expect.poll(() => letters.length).toBe(1);
    expect(letters[0]?.reason).toBe("bad wire");
  });

  it("treats a thunk that throws synchronously as a rejected task", async () => {
    const target: PID = await system.spawn("target", new Recorder());
    const piper: PID = await system.spawn("piper", new Piper());

    piper.pipeTo(target, () => {
      throw new Error("sync fault");
    });

    await expect.poll(() => letters.length).toBe(1);
    expect(letters[0]?.reason).toBe("sync fault");
  });

  it("dead-letters the result when the target stops before the task settles", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());
    const task: Deferred<Order> = deferred<Order>();

    piper.pipeTo(target, task.promise);
    await target.shutdown();
    task.resolve(new Order(3));

    await expect.poll(() => letters.length).toBe(1);
    const letter: Deadletter | undefined = letters[0];
    expect(letter?.reason).toBe(ErrDead.message);
    expect(letter?.message).toBeInstanceOf(Order);
    expect(recorder.received).toHaveLength(0);
  });

  it("logs the undeliverable result when the target is not live", async () => {
    const errorLogs: string[] = [];
    const capturing: Logger = Object.create(discardLogger);
    capturing.error = (message: string): void => {
      errorLogs.push(message);
    };

    const own: ActorSystem = new ActorSystem("gated", { logger: capturing });
    await own.start();
    try {
      const target: PID = await own.spawn("target", new Recorder());
      const piper: PID = await own.spawn("piper", new Piper());
      const task: Deferred<Order> = deferred<Order>();

      piper.pipeTo(target, task.promise);
      await target.shutdown();
      task.resolve(new Order(3));

      await expect.poll(() => errorLogs.length).toBe(1);
      expect(errorLogs[0]).toBe("piped result undeliverable");
    } finally {
      await own.stop();
    }
  });

  it("aborts the task and dead-letters when the timeout expires", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());

    let seen: AbortSignal | undefined;
    piper.pipeTo(
      target,
      (signal) => {
        seen = signal;
        return new Promise<never>(() => {});
      },
      { timeout: 20 },
    );

    await expect.poll(() => letters.length).toBe(1);
    expect(letters[0]?.reason).toBe(ErrPipeTimeout.message);
    expect(seen?.aborted).toBe(true);
    expect(recorder.received).toHaveLength(0);
  });

  it("drops a rejection arriving after the timeout", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());
    const task: Deferred<Order> = deferred<Order>();

    // The timeout settles the pipe first; the task's own later rejection
    // finds the pipe already settled and adds no second dead letter.
    piper.pipeTo(target, task.promise, { timeout: 20 });

    await expect.poll(() => letters.length).toBe(1);
    expect(letters[0]?.reason).toBe(ErrPipeTimeout.message);

    task.reject(new Error("too late"));
    await pause(30);

    expect(letters).toHaveLength(1);
    expect(recorder.received).toHaveLength(0);
  });

  it("drops a result arriving after the timeout", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());
    const task: Deferred<Order> = deferred<Order>();

    piper.pipeTo(target, task.promise, { timeout: 20 });

    await expect.poll(() => letters.length).toBe(1);
    task.resolve(new Order(4));
    await pause(30);

    expect(recorder.received).toHaveLength(0);
    expect(letters).toHaveLength(1);
  });

  it("delivers within the timeout and clears the deadline", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());

    piper.pipeTo(target, Promise.resolve(new Order(5)), { timeout: 5_000 });

    await expect.poll(() => recorder.received.length).toBe(1);
    expect(letters).toHaveLength(0);
  });

  it("treats a non-positive timeout as no deadline", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());

    piper.pipeTo(target, Promise.resolve(new Order(6)), { timeout: 0 });

    await expect.poll(() => recorder.received.length).toBe(1);
    expect(letters).toHaveLength(0);
  });

  it("dead-letters a null task instead of faulting the actor", async () => {
    const target: PID = await system.spawn("target", new Recorder());
    const piper: PID = await system.spawn("piper", new Piper());

    // A JavaScript caller past the types; the actor stays running.
    piper.pipeTo(target, null as unknown as PipeTask);

    await expect.poll(() => letters.length).toBe(1);
    expect(letters[0]?.reason).toBe(ErrUndefinedTask.message);
    expect(piper.isRunning()).toBe(true);
  });

  it("gives each thunk pipe its own abort signal", async () => {
    const target: PID = await system.spawn("target", new Recorder());
    const piper: PID = await system.spawn("piper", new Piper());

    const signals: AbortSignal[] = [];
    const capture = (signal: AbortSignal): Promise<Order> => {
      signals.push(signal);
      return Promise.resolve(new Order(0));
    };

    piper.pipeTo(target, capture);
    piper.pipeTo(target, capture);

    await expect.poll(() => signals.length).toBe(2);
    expect(signals[0]).not.toBe(signals[1]);
  });

  it("settles without crashing when the task resolves after the whole system stopped", async () => {
    const own: ActorSystem = new ActorSystem("gone", { logger: discardLogger });
    await own.start();
    const target: PID = await own.spawn("target", new Recorder());
    const piper: PID = await own.spawn("piper", new Piper());
    const byHandle: Deferred<Order> = deferred<Order>();
    const byName: Deferred<Order> = deferred<Order>();

    piper.pipeTo(target, byHandle.promise);
    piper.pipeToName("target", byName.promise);
    await own.stop();

    // Both settle against a fully torn-down system: deliver() reads the
    // source's system, tree, guardian, and dead-letter sink, all either
    // still constructed or guarded where nulled. A throw here would
    // surface as an unhandled rejection and fail the test.
    byHandle.resolve(new Order(1));
    byName.resolve(new Order(2));
    await pause(20);
  });

  it("still delivers after the piping actor stopped", async () => {
    const recorder: Recorder = new Recorder();
    const target: PID = await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());
    const task: Deferred<Order> = deferred<Order>();

    piper.pipeTo(target, task.promise);
    await piper.shutdown();
    task.resolve(new Order(8));

    await expect.poll(() => recorder.received.length).toBe(1);
    expect(recorder.received[0]?.message).toBeInstanceOf(Order);
    expect(recorder.received[0]?.sender).toBe(piper);
  });
});

describe("pipeToName", () => {
  let system: ActorSystem;
  let letters: Deadletter[];

  beforeEach(async () => {
    system = new ActorSystem("sys", { logger: discardLogger });
    await system.start();
    letters = [];
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        letters.push(event);
      }
    });
  });

  afterEach(async () => {
    await system.stop();
  });

  it("resolves the name when the task settles", async () => {
    const piper: PID = await system.spawn("piper", new Piper());
    const task: Deferred<Order> = deferred<Order>();

    // The pipe starts before any actor holds the name; the actor
    // spawned while the task runs receives the result.
    piper.pipeToName("late", task.promise);
    const recorder: Recorder = new Recorder();
    await system.spawn("late", recorder);
    task.resolve(new Order(9));

    await expect.poll(() => recorder.received.length).toBe(1);
    expect(recorder.received[0]?.message).toBeInstanceOf(Order);
    expect(recorder.received[0]?.sender).toBe(piper);
    expect(letters).toHaveLength(0);
  });

  it("works from receive", async () => {
    const recorder: Recorder = new Recorder();
    await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());

    system.noSender().tell(piper, new DoPipeName("target", Promise.resolve(new Order(10))));

    await expect.poll(() => recorder.received.length).toBe(1);
    const entry: Received = recorder.received[0] as Received;
    expect((entry.message as Order).id).toBe(10);
  });

  it("dead-letters when no running top-level actor holds the name", async () => {
    const piper: PID = await system.spawn("piper", new Piper());

    piper.pipeToName("ghost", Promise.resolve(new Order(11)));

    await expect.poll(() => letters.length).toBe(1);
    const letter: Deadletter | undefined = letters[0];
    expect(letter?.reason).toBe("actor ghost not found");
    expect(letter?.sender).toBe(piper.path().toString());
    expect(letter?.receiver.endsWith("/ghost")).toBe(true);
    expect(letter?.message).toBeInstanceOf(Order);
  });

  it("dead-letters with the timeout reason when the deadline expires", async () => {
    const recorder: Recorder = new Recorder();
    await system.spawn("target", recorder);
    const piper: PID = await system.spawn("piper", new Piper());

    piper.pipeToName("target", new Promise<never>(() => {}), { timeout: 20 });

    await expect.poll(() => letters.length).toBe(1);
    expect(letters[0]?.reason).toBe(ErrPipeTimeout.message);
    expect(recorder.received).toHaveLength(0);
  });
});
