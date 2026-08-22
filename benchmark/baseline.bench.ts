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

import { afterAll, describe, it } from "vitest";
import type { Actor } from "../src/actor/actor";
import { ActorSystem } from "../src/actor/actor.system";
import type { ReceiveContext } from "../src/actor/receive.context";
import { printBlock, printMachine } from "./harness";

/**
 * The plain baseline benchmark: one sender, one receiver, one number per
 * messaging primitive.
 *
 * Each primitive runs continuously for several seconds and the report is
 * the sustained rate: messages fully processed divided by the wall time
 * actually spent, garbage collection and buffer churn included. `tell`
 * sends fire-and-forget batches, each batch fully processed before the
 * next begins; `ask` completes sequential request/response round trips,
 * each reply awaited before the next request.
 */

/** Minimum measured wall time per primitive. */
const DURATION_MS = 5_000;

/** Wall time spent running before the clock starts, so the JIT settles
 * and the mailbox buffers reach their steady-state capacity. */
const WARMUP_MS = 1_000;

/** Messages per tell batch and asks per step; one step is the unit of
 * work between clock checks. */
const STEP = 100_000;

/** Per-ask timeout; generous so it never fires while measuring. */
const TIMEOUT = 30_000;

/** The message sent in every run; reused so the hot path measures
 * dispatch, not message construction. */
class Ping {}

const msg = new Ping();

/**
 * A receiver that resolves a promise once it has processed the number of
 * messages a run announced through {@link expect}.
 */
class CountingActor implements Actor {
  private remaining = 0;
  private resolveDone: (() => void) | null = null;

  expect(count: number): Promise<void> {
    this.remaining = count;
    return new Promise((resolve) => {
      this.resolveDone = resolve;
    });
  }

  preStart(): void {}

  receive(): void {
    this.remaining--;

    if (this.remaining === 0) {
      const resolve = this.resolveDone as () => void;
      this.resolveDone = null;
      resolve();
    }
  }

  postStop(): void {}
}

/** A receiver that answers every request with the request itself. */
class EchoActor implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    ctx.response(ctx.message);
  }

  postStop(): void {}
}

/** A sending side whose behavior is never exercised; it only lends its
 * PID as the sender. */
class SilentActor implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

const system = new ActorSystem("bench");
await system.start();

afterAll(async () => {
  await system.stop();
});

const sender = await system.spawn("baseline-sender", new SilentActor());
const counting = new CountingActor();
const receiver = await system.spawn("baseline-receiver", counting);
const echo = await system.spawn("baseline-echo", new EchoActor());

/** Sends one batch of `STEP` messages and resolves once all are processed. */
async function tellStep(): Promise<void> {
  const done = counting.expect(STEP);

  for (let i = 0; i < STEP; i++) {
    sender.tell(receiver, msg);
  }

  await done;
}

/** Completes `STEP` sequential ask round trips. */
async function askStep(): Promise<void> {
  for (let i = 0; i < STEP; i++) {
    await sender.ask(echo, msg, TIMEOUT);
  }
}

/** The measured outcome: messages processed, wall time, and the rate. */
interface Measurement {
  messages: number;
  elapsedMs: number;
  perSecond: number;
}

/**
 * Runs steps back to back until at least `DURATION_MS` of wall time has
 * been measured, after `WARMUP_MS` of unmeasured running. The rate is
 * total messages processed divided by total measured time, so it is the
 * sustained pace over the whole window, not a burst extrapolation.
 */
async function measure(step: () => Promise<void>): Promise<Measurement> {
  const warmupEnd = process.hrtime.bigint() + BigInt(WARMUP_MS) * 1_000_000n;

  while (process.hrtime.bigint() < warmupEnd) {
    await step();
  }

  let messages = 0;
  const start = process.hrtime.bigint();
  const end = start + BigInt(DURATION_MS) * 1_000_000n;

  while (process.hrtime.bigint() < end) {
    await step();
    messages += STEP;
  }

  const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;

  return { messages, elapsedMs, perSecond: messages / (elapsedMs / 1000) };
}

function row(name: string, m: Measurement): string[] {
  return [
    name,
    `${(m.elapsedMs / 1000).toFixed(2)} s`,
    m.messages.toLocaleString("en-US"),
    Math.round(m.perSecond).toLocaleString("en-US"),
  ];
}

describe("baseline", () => {
  it("reports sustained messages per second for tell and ask", { timeout: 300_000 }, async () => {
    const tell = await measure(tellStep);
    const ask = await measure(askStep);

    printMachine();
    printBlock(
      "baseline  ·  one sender, one receiver, sustained messages per second",
      ["primitive", "time", "messages processed", "msgs/sec"],
      [row("tell", tell), row("ask", ask)],
      "msgs/sec is messages processed divided by the seconds spent processing them; tell counts a message when the receiver has handled it, and ask round trips are sequential, each awaited before the next.",
    );
  });
});
