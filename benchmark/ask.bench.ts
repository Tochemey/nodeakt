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
import { printReport, runScenario, type Scenario, type ScenarioReport } from "./harness";

/**
 * Request-response throughput benchmarks for `ask`.
 *
 * Each operation completes a fixed batch of round trips: a message is
 * sent, the receiver replies through `ctx.response`, and the returned
 * promise resolves with the reply. One "message" in the report is one
 * completed round trip.
 *
 * The scenarios cover the shapes a request-response server produces on a
 * single event loop: sequential round trips isolate the full ask latency
 * (request, reply, and timeout bookkeeping with nothing else in flight),
 * the pipelined run measures throughput when a caller keeps many asks in
 * flight and the receiver drains them in batches, and the concurrent
 * askers run interleaves independent request streams into one receiver
 * the way multiple handlers sharing a backend actor would.
 */

/** Round trips per benchmark operation. */
const BATCH = 10_000;

/** Concurrent askers sharing one receiver. */
const ASKERS = 8;

/** Per-ask timeout; generous so it never fires while measuring. */
const TIMEOUT = 30_000;

/** The request sent in every benchmark; reused so the hot path measures
 * the round trip, not message construction. */
class Ping {}

const msg = new Ping();

/** A receiver that answers every request with the request itself. */
class EchoActor implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    ctx.response(ctx.message);
  }

  postStop(): void {}
}

/** An asking side whose behavior is never exercised; it only lends its
 * PID as the requester. */
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

// Single pair: one requester, one echoing receiver.
const asker = await system.spawn("asker", new SilentActor());
const echo = await system.spawn("echo", new EchoActor());

// Concurrent askers: ASKERS requesters into one echoing receiver.
const fanInAskers = await Promise.all(
  Array.from({ length: ASKERS }, (_, i) => system.spawn(`asker-${i}`, new SilentActor())),
);
const fanInEcho = await system.spawn("fan-in-echo", new EchoActor());

const scenarios: Scenario[] = [
  {
    name: "single pair, sequential",
    batch: BATCH,
    op: async () => {
      for (let i = 0; i < BATCH; i++) {
        await asker.ask(echo, msg, TIMEOUT);
      }
    },
  },
  {
    name: `single pair, pipelined: ${BATCH} in flight`,
    batch: BATCH,
    op: async () => {
      const inFlight = new Array<Promise<unknown>>(BATCH);

      for (let i = 0; i < BATCH; i++) {
        inFlight[i] = asker.ask(echo, msg, TIMEOUT);
      }

      await Promise.all(inFlight);
    },
  },
  {
    name: `fan-in: ${ASKERS} sequential askers into 1 receiver`,
    batch: BATCH,
    op: async () => {
      const perAsker = BATCH / ASKERS;
      const streams = fanInAskers.map(async (requester) => {
        for (let i = 0; i < perAsker; i++) {
          await requester.ask(fanInEcho, msg, TIMEOUT);
        }
      });

      await Promise.all(streams);
    },
  },
];

describe("ask", () => {
  it("measures round-trip throughput, allocation, and GC per scenario", {
    timeout: 300_000,
  }, async () => {
    const reports: ScenarioReport[] = [];

    for (const scenario of scenarios) {
      reports.push(await runScenario(scenario));
    }

    printReport(reports);
  });
});
