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
import { type DataEnvelope, type Hello, KIND_TELL, SERIALIZER_BINARY } from "../src/_net/envelope";
import { LANE_CONTROL } from "../src/_net/frame";
import { NetServer } from "../src/_net/server";
import { Session } from "../src/_net/session";
import { printReport, runScenario, type Scenario, type ScenarioReport } from "./harness";

/**
 * Transport tell throughput over loopback TCP, with and without
 * string tables: the same envelope stream at capability revision 2,
 * where every ref travels as its inline literal, and at revision 4,
 * where the repeating path and type refs travel as one-byte interned
 * ids. The path and type ref add up to about 60 bytes per message
 * inline, which is most of a small envelope; the delta between the
 * scenarios is what interning buys on the wire and in per-message
 * encode work.
 */

/** Messages per benchmark operation. */
const BATCH = 10_000;

const TARGET_PATH = "nodeakt://bench@127.0.0.1:5100/user/orders/consumer-01";
const TYPE_REF = "bench.OrderPlaced";

const envelope: DataEnvelope = {
  kind: KIND_TELL,
  to: TARGET_PATH,
  uid: "b3f2",
  sender: "",
  senderUid: "",
  timeout: 0,
  serializerId: SERIALIZER_BINARY,
  typeRef: TYPE_REF,
  payload: new Uint8Array(16).fill(7),
};

function helloAt(revision: number, systemName: string): Hello {
  return {
    revision,
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

/** One loopback pair: a counting receiver and a dialed sender. */
interface Pair {
  server: NetServer;
  client: Session;
  expect(count: number): Promise<void>;
}

async function startPair(revision: number): Promise<Pair> {
  let remaining = 0;
  let resolveDone: (() => void) | null = null;

  const server: NetServer = await NetServer.listen(
    { local: helloAt(revision, "bench-server") },
    {
      onData: (): void => {
        remaining--;

        if (remaining === 0) {
          const resolve = resolveDone as () => void;
          resolveDone = null;
          resolve();
        }
      },
    },
  );

  const client: Session = await Session.dial(
    connect(server.address.port, "127.0.0.1"),
    helloAt(revision, "bench-client"),
  );

  return {
    server,
    client,
    expect(count: number): Promise<void> {
      remaining = count;
      return new Promise((resolve) => {
        resolveDone = resolve;
      });
    },
  };
}

const inlinePair = await startPair(2);
const internedPair = await startPair(4);

afterAll(async () => {
  inlinePair.client.close();
  internedPair.client.close();
  await inlinePair.server.shutdown(1000);
  await internedPair.server.shutdown(1000);
});

function tellScenario(name: string, pair: Pair): Scenario {
  return {
    name,
    batch: BATCH,
    op: async () => {
      const done = pair.expect(BATCH);

      for (let i = 0; i < BATCH; i++) {
        while (pair.client.tell(envelope) !== null) {
          await new Promise((resolve) => setImmediate(resolve));
        }
      }

      await done;
    },
  };
}

const scenarios: Scenario[] = [
  tellScenario("inline refs (revision 2)", inlinePair),
  tellScenario("interned refs (revision 4)", internedPair),
];

describe("net tables", () => {
  it("measures tell throughput with and without tables", { timeout: 300_000 }, async () => {
    const reports: ScenarioReport[] = [];

    for (const scenario of scenarios) {
      reports.push(await runScenario(scenario));
    }

    printReport(reports, "net tables  ·  loopback tell throughput");
  });
});
