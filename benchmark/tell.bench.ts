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
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { BoundedMailbox } from "../src/bounded.mailbox";
import { UnboundedFairMailbox } from "../src/fair.mailbox";
import type { PID } from "../src/pid";
import { UnboundedSegmentedMailbox } from "../src/segmented.mailbox";
import { printReport, runScenario, type Scenario, type ScenarioReport } from "./harness";

/**
 * Fire-and-forget throughput benchmarks for `tell`.
 *
 * Each operation sends a fixed batch of messages and awaits full
 * processing: the clock stops once the receiver has handled the whole
 * batch, not when the last `tell` returns. The report shows messages per
 * second, per-operation latency, an estimated allocation per message,
 * and the garbage collection activity observed while sampling.
 *
 * The scenarios mirror the GoAkt tell suite, adapted to a single-threaded
 * runtime: the single pair isolates the per-message dispatch cost, fan-in
 * interleaves many senders into one mailbox, pairwise runs independent
 * pairs on one event loop so anything shared across actors shows up as a
 * flat curve, and the mailbox sweep compares queue implementations on the
 * same workload.
 */

/** Messages per benchmark operation. */
const BATCH = 10_000;

/** Concurrent senders and independent pairs. */
const SENDERS = 8;
const PAIRS = 8;

/** The message sent in every benchmark; reused so the hot path measures
 * dispatch, not message construction. */
class Ping {}

const msg = new Ping();

/**
 * A receiver that resolves a promise once it has processed the number of
 * messages a batch announced through {@link expect}.
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

/** A sender whose behavior is never exercised; it only lends its PID as
 * the sending side. */
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

// Single pair: one producer, one consumer.
const pairSender = await system.spawn("sender", new SilentActor());
const pairCounting = new CountingActor();
const pairReceiver = await system.spawn("receiver", pairCounting);

// Fan-in: SENDERS producers into one consumer.
const fanInSenders = await Promise.all(
  Array.from({ length: SENDERS }, (_, i) => system.spawn(`fan-in-sender-${i}`, new SilentActor())),
);
const fanInCounting = new CountingActor();
const fanInReceiver = await system.spawn("fan-in-receiver", fanInCounting);

// Pairwise: PAIRS independent producer/consumer pairs.
interface Pair {
  sender: PID;
  receiver: PID;
  counting: CountingActor;
}

const pairs: Pair[] = await Promise.all(
  Array.from({ length: PAIRS }, async (_, i) => {
    const counting = new CountingActor();
    const sender = await system.spawn(`pair-sender-${i}`, new SilentActor());
    const receiver = await system.spawn(`pair-receiver-${i}`, counting);
    return { sender, receiver, counting };
  }),
);

// Mailbox sweep: the single-pair workload over each mailbox kind.
interface Target {
  receiver: PID;
  counting: CountingActor;
}

const sweepSender = await system.spawn("sweep-sender", new SilentActor());
const sweepTargets: Array<[string, Target]> = [];

const sweepMailboxes = {
  unbounded: undefined,
  segmented: new UnboundedSegmentedMailbox(),
  bounded: new BoundedMailbox(BATCH),
  fair: new UnboundedFairMailbox((ctx) => ctx.sender?.name() ?? "anonymous"),
};

for (const [name, mailbox] of Object.entries(sweepMailboxes)) {
  const counting = new CountingActor();
  const receiver = await system.spawn(
    `sweep-${name}`,
    counting,
    mailbox === undefined ? undefined : { mailbox },
  );
  sweepTargets.push([name, { receiver, counting }]);
}

const scenarios: Scenario[] = [
  {
    name: "single pair",
    batch: BATCH,
    op: async () => {
      const done = pairCounting.expect(BATCH);

      for (let i = 0; i < BATCH; i++) {
        pairSender.tell(pairReceiver, msg);
      }

      await done;
    },
  },
  {
    name: `fan-in: ${SENDERS} senders into 1 receiver`,
    batch: BATCH,
    op: async () => {
      const done = fanInCounting.expect(BATCH);

      for (let i = 0; i < BATCH; i++) {
        (fanInSenders[i % SENDERS] as PID).tell(fanInReceiver, msg);
      }

      await done;
    },
  },
  {
    name: `pairwise: ${PAIRS} independent pairs`,
    batch: BATCH * PAIRS,
    op: async () => {
      const dones = pairs.map((pair) => pair.counting.expect(BATCH));

      // Interleave the pairs the way independent producers would,
      // instead of draining one pair at a time.
      for (let i = 0; i < BATCH; i++) {
        for (const pair of pairs) {
          pair.sender.tell(pair.receiver, msg);
        }
      }

      await Promise.all(dones);
    },
  },
  ...sweepTargets.map(
    ([name, target]): Scenario => ({
      name: `mailbox: ${name}`,
      batch: BATCH,
      op: async () => {
        const done = target.counting.expect(BATCH);

        for (let i = 0; i < BATCH; i++) {
          sweepSender.tell(target.receiver, msg);
        }

        await done;
      },
    }),
  ),
];

describe("tell", () => {
  it("measures throughput, allocation, and GC per scenario", { timeout: 300_000 }, async () => {
    const reports: ScenarioReport[] = [];

    for (const scenario of scenarios) {
      reports.push(await runScenario(scenario));
    }

    printReport(reports, "tell  ·  fire-and-forget throughput");
  });
});
