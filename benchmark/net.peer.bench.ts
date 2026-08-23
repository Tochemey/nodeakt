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
import { type DataEnvelope, type Hello, KIND_TELL, SERIALIZER_BINARY } from "../src/net/envelope";
import { LANE_CONTROL } from "../src/net/frame";
import { Peer } from "../src/net/peer";
import { NetServer } from "../src/net/server";
import { printReport, runScenario, type Scenario, type ScenarioReport } from "./harness";

/**
 * Tell throughput through the full peer path over loopback TCP: lane
 * routing, the redelivery record and its kernel-write confirmation,
 * admission, and the session encode underneath. The session-only bench
 * next door measures the floor; the delta between the two is what the
 * peer layer itself costs per message.
 */

/** Messages per benchmark operation. */
const BATCH = 10_000;

const envelope: DataEnvelope = {
  kind: KIND_TELL,
  to: "nodeakt://bench@127.0.0.1:5100/user/orders/consumer-01",
  uid: "b3f2",
  sender: "",
  senderUid: "",
  timeout: 0,
  serializerId: SERIALIZER_BINARY,
  typeRef: "bench.OrderPlaced",
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

let remaining = 0;
let resolveDone: (() => void) | null = null;

const server: NetServer = await NetServer.listen(
  { local: helloOf("bench-server") },
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

const peer: Peer = new Peer(server.address.host, server.address.port, helloOf("bench-client"), {
  onDeadLetter: (_envelope: DataEnvelope, reason: Error): void => {
    throw new Error(`bench tell dead-lettered: ${reason.message}`);
  },
});

afterAll(async () => {
  peer.close();
  await server.shutdown(1000);
});

const scenario: Scenario = {
  name: "peer tell (steady lane)",
  batch: BATCH,
  op: async () => {
    remaining = BATCH;
    const done = new Promise<void>((resolve) => {
      resolveDone = resolve;
    });

    for (let i = 0; i < BATCH; i++) {
      peer.tell(envelope);
    }

    await done;
  },
};

describe("net peer", () => {
  it("measures tell throughput through the peer path", { timeout: 300_000 }, async () => {
    const reports: ScenarioReport[] = [];
    reports.push(await runScenario(scenario));
    printReport(reports, "net peer  ·  loopback tell throughput");
  });
});
