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

import { connect } from "node:net";
import { afterAll, describe, it } from "vitest";
import { type DataEnvelope, type Hello, KIND_ASK, SERIALIZER_BINARY } from "../src/_net/envelope";
import { LANE_CONTROL } from "../src/_net/frame";
import { NetServer } from "../src/_net/server";
import { Session } from "../src/_net/session";
import {
  printBlock,
  printReport,
  runScenario,
  type Scenario,
  type ScenarioReport,
} from "./harness";

/**
 * Transport ask round trips over loopback TCP under increasing
 * pipelining: serial asks measure the bare round-trip floor, and the
 * windowed scenarios measure how throughput and tail latency move as
 * more asks share the connection. Every ask's own latency is sampled,
 * so the report carries a real distribution per window, not just the
 * batch mean.
 */

/** Asks per benchmark operation. */
const BATCH = 2000;

/** In-flight ask windows measured. */
const WINDOWS = [1, 64, 1024];

const envelope: DataEnvelope = {
  kind: KIND_ASK,
  to: "nodeakt://bench@127.0.0.1:5100/user/orders/consumer-01",
  uid: "b3f2",
  sender: "",
  senderUid: "",
  timeout: 0,
  serializerId: SERIALIZER_BINARY,
  typeRef: "bench.GetOrder",
  payload: new Uint8Array(16).fill(7),
};

function helloOf(systemName: string): Hello {
  return {
    revision: 4,
    systemName,
    host: "127.0.0.1",
    port: 0,
    lane: LANE_CONTROL,
    compression: 0,
    maxFrameSize: 16 * 1024 * 1024,
    maxMessageSize: 16 * 1024 * 1024,
    initialCredits: 16 * 1024 * 1024,
    maxLargeTransfers: 4,
  };
}

const server: NetServer = await NetServer.listen(
  { local: helloOf("bench-server") },
  {
    onData: (session, received, correlation): void => {
      session.reply(correlation, {
        serializerId: SERIALIZER_BINARY,
        typeRef: "bench.Order",
        payload: received.payload,
      });
    },
  },
);

const client: Session = await Session.dial(
  connect(server.address.port, "127.0.0.1"),
  helloOf("bench-client"),
);

afterAll(async () => {
  client.close();
  await server.shutdown(1000);
});

/** Completes BATCH asks with at most `window` in flight, sampling each. */
function askBatch(window: number, latencies: number[]): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let issued = 0;
    let done = 0;
    const pump = (): void => {
      while (issued < BATCH && issued - done < window) {
        issued += 1;
        const start = process.hrtime.bigint();
        client.ask(envelope, 10_000).then((): void => {
          latencies.push(Number(process.hrtime.bigint() - start) / 1e6);
          done += 1;

          if (done === BATCH) {
            resolve();
            return;
          }

          pump();
        }, reject);
      }
    };
    pump();
  });
}

function percentile(sorted: number[], fraction: number): number {
  const index = Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1);
  return sorted[index] as number;
}

describe("net ask", () => {
  it("measures ask latency under pipelining", { timeout: 300_000 }, async () => {
    const reports: ScenarioReport[] = [];
    const distributions: string[][] = [];

    for (const window of WINDOWS) {
      const latencies: number[] = [];
      const scenario: Scenario = {
        name: window === 1 ? "serial asks" : `pipelined asks (window ${window})`,
        batch: BATCH,
        op: () => askBatch(window, latencies),
      };
      reports.push(await runScenario(scenario));

      // The first fifth of the samples is warmup; the distribution is
      // computed from the settled tail.
      const settled = latencies.slice(Math.floor(latencies.length / 5)).sort((a, b) => a - b);
      distributions.push([
        scenario.name,
        `${percentile(settled, 0.5).toFixed(3)} ms`,
        `${percentile(settled, 0.95).toFixed(3)} ms`,
        `${percentile(settled, 0.99).toFixed(3)} ms`,
        `${(settled[settled.length - 1] as number).toFixed(3)} ms`,
      ]);
    }

    printReport(reports, "net ask  ·  loopback round-trip throughput");
    printBlock(
      "net ask  ·  per-ask latency distribution",
      ["scenario", "p50", "p95", "p99", "max"],
      distributions,
      "computed over every sampled ask after the warmup fifth is dropped",
    );
  });
});
