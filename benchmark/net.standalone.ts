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
import { type DataEnvelope, type Hello, KIND_TELL, SERIALIZER_BINARY } from "../src/_net/envelope";
import { LANE_CONTROL } from "../src/_net/frame";
import { Peer } from "../src/_net/peer";
import { NetServer } from "../src/_net/server";
import { Session } from "../src/_net/session";
import { printReport, runScenario, type Scenario, type ScenarioReport } from "./harness";

/**
 * The transport bench as a plain script, because the numbers must be
 * read per runtime and only Node runs the vitest bench config:
 *
 *   pnpm exec tsx benchmark/net.standalone.ts
 *   bun benchmark/net.standalone.ts
 *   deno run -A --unstable-sloppy-imports benchmark/net.standalone.ts
 *
 * It measures the same two hot paths as the vitest suite: bare
 * session tells and tells through the full peer path.
 */

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

interface Counter {
  expect(count: number): Promise<void>;
  onData(): void;
}

function counter(): Counter {
  let remaining = 0;
  let resolveDone: (() => void) | null = null;
  return {
    expect(count: number): Promise<void> {
      remaining = count;
      return new Promise((resolve) => {
        resolveDone = resolve;
      });
    },
    onData(): void {
      remaining--;

      if (remaining === 0) {
        const resolve = resolveDone as () => void;
        resolveDone = null;
        resolve();
      }
    },
  };
}

const sessionCount = counter();
const sessionServer: NetServer = await NetServer.listen(
  { local: helloOf("bench-server") },
  { onData: (): void => sessionCount.onData() },
);
const client: Session = await Session.dial(
  connect(sessionServer.address.port, "127.0.0.1"),
  helloOf("bench-client"),
);

const peerCount = counter();
const peerServer: NetServer = await NetServer.listen(
  { local: helloOf("bench-server") },
  { onData: (): void => peerCount.onData() },
);
const peer: Peer = new Peer(
  peerServer.address.host,
  peerServer.address.port,
  helloOf("bench-client"),
  {
    onDeadLetter: (_envelope: DataEnvelope, reason: Error): void => {
      throw new Error(`bench tell dead-lettered: ${reason.message}`);
    },
  },
);

const scenarios: Scenario[] = [
  {
    name: "session tell (interned refs)",
    batch: BATCH,
    op: async () => {
      const done = sessionCount.expect(BATCH);

      for (let i = 0; i < BATCH; i++) {
        while (client.tell(envelope) !== null) {
          await new Promise((resolve) => setImmediate(resolve));
        }
      }

      await done;
    },
  },
  {
    name: "peer tell (steady lane)",
    batch: BATCH,
    op: async () => {
      const done = peerCount.expect(BATCH);

      for (let i = 0; i < BATCH; i++) {
        peer.tell(envelope);
      }

      await done;
    },
  },
];

const reports: ScenarioReport[] = [];

for (const scenario of scenarios) {
  reports.push(await runScenario(scenario));
}

printReport(reports, "net standalone  ·  loopback tell throughput");

client.close();
peer.close();
await sessionServer.shutdown(1000);
await peerServer.shutdown(1000);
process.exit(0);
