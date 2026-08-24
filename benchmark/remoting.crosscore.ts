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

import { type ChildProcess, fork } from "node:child_process";
import { fileURLToPath } from "node:url";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { discardLogger } from "../src/discard.logger";
import type { PID } from "../src/pid";
import type { ReceiveContext } from "../src/receive.context";
import { registerMessage } from "../src/registration";
import { printMachine, printReport, runScenario, type ScenarioReport } from "./harness";

/**
 * The remote send path across two OS processes, one core each side, so
 * sender-side encoding and receiver-side decoding run in parallel the
 * way two real machines do. The single-process bench next door
 * serializes both halves on one core, which understates what a
 * deployment sees; the delta between the two files is exactly that
 * parallelism. A plain script because vitest owns the single-process
 * runs:
 *
 *   pnpm exec tsx benchmark/remoting.crosscore.ts
 *
 * Each op sends one batch of tells and then a fence ask on the same
 * lane: lane FIFO orders the fence behind every tell, and the mailbox
 * processes in arrival order, so the fence's reply proves the whole
 * batch was received, decoded, and handled.
 */

const TELL_BATCH: number = 10_000;
const PIPELINED_ASKS: number = 5_000;

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

/** Absorbs pings and answers queries; the fence rides as a Query. */
class Sink implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Query) {
      ctx.response(new Reply(ctx.message.n + 1));
    }
  }

  postStop(): void {}
}

/** The child: one receiver system on an ephemeral port, reported to
 * the parent, then parked until the parent exits. */
async function runReceiver(): Promise<void> {
  const receiver: ActorSystem = new ActorSystem("bench-cross-b", {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0 },
  });
  await receiver.start();
  await receiver.spawn("sink", new Sink());
  process.send?.({ port: receiver.port() });
  process.on("disconnect", (): void => {
    void receiver.stop().then((): void => {
      process.exit(0);
    });
  });
}

/** The parent: forks the receiver, runs the scenarios against it. */
async function runSender(): Promise<void> {
  const child: ChildProcess = fork(fileURLToPath(import.meta.url), [], {
    env: { ...process.env, CROSSCORE_ROLE: "receiver" },
  });
  const port: number = await new Promise<number>((resolve: (port: number) => void): void => {
    child.once("message", (message: unknown): void => {
      resolve((message as { port: number }).port);
    });
  });

  const sender: ActorSystem = new ActorSystem("bench-cross-a", {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0 },
  });
  await sender.start();
  const sink: PID = (await sender.remoteLookup("127.0.0.1", port, "sink")) as PID;
  const outside: PID = sender.noSender();

  const reports: ScenarioReport[] = [];
  reports.push(
    await runScenario({
      name: "remote tell (two processes)",
      batch: TELL_BATCH,
      op: async (): Promise<void> => {
        for (let i: number = 0; i < TELL_BATCH; i++) {
          outside.tell(sink, new Ping(i));
        }

        await outside.ask(sink, new Query(0), 30_000);
      },
    }),
  );
  reports.push(
    await runScenario({
      name: "remote ask (pipelined, two processes)",
      batch: PIPELINED_ASKS,
      op: async (): Promise<void> => {
        const inFlight: Promise<unknown>[] = new Array(PIPELINED_ASKS);
        for (let i: number = 0; i < PIPELINED_ASKS; i++) {
          inFlight[i] = outside.ask(sink, new Query(i), 30_000);
        }

        await Promise.all(inFlight);
      },
    }),
  );

  printMachine();
  printReport(reports, "remoting  ·  cross-process node to node");
  await sender.stop();
  child.disconnect();
}

if (process.env.CROSSCORE_ROLE === "receiver") {
  void runReceiver();
} else {
  void runSender();
}
