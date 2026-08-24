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
import { discardLogger } from "../src/discard.logger";
import type { PID } from "../src/pid";
import type { ReceiveContext } from "../src/receive.context";
import { registerMessage } from "../src/registration";
import { printReport, runScenario, type Scenario, type ScenarioReport } from "./harness";

/**
 * The remote send path end to end: two actor systems on loopback TCP,
 * a routed PID from a lookup, and every message crossing the full seam
 * (registry check, value encode, peer lanes, inbound resolve, decode,
 * prototype restore, mailbox delivery). The net benches next door
 * measure the transport alone; the delta between them and these
 * numbers is what the seam layer itself costs per message.
 */

/** Messages per benchmark operation. */
const TELL_BATCH: number = 10_000;
const PIPELINED_ASKS: number = 5_000;
const SEQUENTIAL_ASKS: number = 500;

class Ping {
  constructor(readonly n: number) {}
}

class Query {
  constructor(readonly n: number) {}
}

class Reply {
  constructor(readonly n: number) {}
}

registerMessage(Ping);
registerMessage(Query);
registerMessage(Reply);

let remaining: number = 0;
let resolveDone: (() => void) | null = null;

/** Counts tells toward the batch signal and answers asks. */
class Sink implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Ping) {
      remaining--;
      if (remaining <= 0 && resolveDone !== null) {
        const resolve: () => void = resolveDone;
        resolveDone = null;
        resolve();
      }

      return;
    }

    if (ctx.message instanceof Query) {
      ctx.response(new Reply(ctx.message.n + 1));
    }
  }

  postStop(): void {}
}

const receiver: ActorSystem = new ActorSystem("bench-remote-b", {
  logger: discardLogger,
  remote: { host: "127.0.0.1", port: 0 },
});
const sender: ActorSystem = new ActorSystem("bench-remote-a", {
  logger: discardLogger,
  remote: { host: "127.0.0.1", port: 0 },
});
await receiver.start();
await sender.start();
await receiver.spawn("sink", new Sink());
const sink: PID = (await sender.remoteLookup("127.0.0.1", receiver.port(), "sink")) as PID;
const outside: PID = sender.noSender();

afterAll(async () => {
  await sender.stop();
  await receiver.stop();
});

const tellScenario: Scenario = {
  name: "remote tell",
  batch: TELL_BATCH,
  op: async (): Promise<void> => {
    remaining = TELL_BATCH;
    const done: Promise<void> = new Promise<void>((resolve: () => void): void => {
      resolveDone = resolve;
    });

    for (let i: number = 0; i < TELL_BATCH; i++) {
      outside.tell(sink, new Ping(i));
    }

    await done;
  },
};

const sequentialAskScenario: Scenario = {
  name: "remote ask (sequential)",
  batch: SEQUENTIAL_ASKS,
  op: async (): Promise<void> => {
    for (let i: number = 0; i < SEQUENTIAL_ASKS; i++) {
      await outside.ask(sink, new Query(i), 10_000);
    }
  },
};

const pipelinedAskScenario: Scenario = {
  name: "remote ask (pipelined)",
  batch: PIPELINED_ASKS,
  op: async (): Promise<void> => {
    const inFlight: Promise<unknown>[] = new Array(PIPELINED_ASKS);
    for (let i: number = 0; i < PIPELINED_ASKS; i++) {
      inFlight[i] = outside.ask(sink, new Query(i), 10_000);
    }

    await Promise.all(inFlight);
  },
};

describe("remoting", () => {
  it("measures the remote send path over loopback", { timeout: 300_000 }, async () => {
    const reports: ScenarioReport[] = [];
    reports.push(await runScenario(tellScenario));
    reports.push(await runScenario(sequentialAskScenario));
    reports.push(await runScenario(pipelinedAskScenario));
    printReport(reports, "remoting  ·  loopback node to node");
  });
});
