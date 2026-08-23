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
import {
  printBlock,
  printReport,
  runScenario,
  type Scenario,
  type ScenarioReport,
} from "./harness";

/**
 * Chunked transfer throughput over loopback TCP: logical messages
 * above the 256 KiB chunk size travel as CHUNK groups, so this
 * measures the split, the credit window under multi-frame messages,
 * and reassembly end to end. Two sizes per the spec's bench list: a
 * common 1 MiB payload, and a payload at the negotiated
 * `maxMessageSize`, the largest message the connection accepts.
 */

const MIB = 1024 * 1024;

/** Message payload sizes measured, with tells per operation. */
const SHAPES: { name: string; bytes: number; batch: number }[] = [
  { name: "chunked 1 MiB tells", bytes: MIB, batch: 32 },
  // Envelope fields ride inside the logical frame, so the payload
  // sits just under the 16 MiB message cap.
  { name: "chunked tells at maxMessageSize", bytes: 16 * MIB - 1024, batch: 2 },
];

function helloOf(systemName: string): Hello {
  return {
    revision: 4,
    systemName,
    host: "127.0.0.1",
    port: 0,
    lane: LANE_CONTROL,
    compression: 0,
    maxFrameSize: 16 * MIB,
    maxMessageSize: 16 * MIB,
    initialCredits: 16 * MIB,
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

const client: Session = await Session.dial(
  connect(server.address.port, "127.0.0.1"),
  helloOf("bench-client"),
);

afterAll(async () => {
  client.close();
  await server.shutdown(1000);
});

function transferScenario(name: string, bytes: number, batch: number): Scenario {
  const envelope: DataEnvelope = {
    kind: KIND_TELL,
    to: "nodeakt://bench@127.0.0.1:5100/user/blobs/sink-01",
    uid: "b3f2",
    sender: "",
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: "bench.BlobStored",
    payload: new Uint8Array(bytes).fill(7),
  };
  return {
    name,
    batch,
    op: async () => {
      remaining = batch;
      const done = new Promise<void>((resolve) => {
        resolveDone = resolve;
      });

      for (let i = 0; i < batch; i++) {
        while (client.tell(envelope) !== null) {
          await new Promise((resolve) => setImmediate(resolve));
        }
      }

      await done;
    },
  };
}

describe("net chunked transfers", () => {
  it("measures chunked tell throughput at 1 MiB and the message cap", {
    timeout: 300_000,
  }, async () => {
    const reports: ScenarioReport[] = [];

    for (const shape of SHAPES) {
      reports.push(await runScenario(transferScenario(shape.name, shape.bytes, shape.batch)));
    }

    printReport(reports, "net chunked transfers  ·  loopback throughput");
    printBlock(
      "net chunked transfers  ·  payload bandwidth",
      ["scenario", "MiB/s"],
      reports.map((report, index) => [
        report.name,
        `${((report.messagesPerSecond * (SHAPES[index] as { bytes: number }).bytes) / MIB).toFixed(0)}`,
      ]),
      "messages per second times payload size; frame and chunk overhead excluded",
    );
  });
});
