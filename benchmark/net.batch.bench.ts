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

import { type AddressInfo, connect, createServer, type Server, type Socket } from "node:net";
import { afterAll, describe, it } from "vitest";
import { FramedConn } from "../src/net/conn";
import { FRAME_DATA, LANE_CONTROL } from "../src/net/frame";
import { printReport, runScenario, type Scenario, type ScenarioReport } from "./harness";

/**
 * The cost of the dispatch batch size: how many frames the framed
 * connection coalesces per socket write. Small batches pay a syscall
 * per few frames; big batches pay copy latency before the first byte
 * moves. The shipped default (128 frames or 64 KiB, whichever first)
 * was chosen from this sweep; rerun it per runtime before changing
 * either cap.
 */

/** Frames per benchmark operation. */
const BATCH = 10_000;

/** Frames-per-write settings measured. */
const SWEEP = [1, 8, 32, 128];

const body: Uint8Array = new Uint8Array(32).fill(7);

const listeners: Server[] = [];
const conns: FramedConn[] = [];

afterAll(() => {
  for (const conn of conns) {
    conn.destroy();
  }

  for (const listener of listeners) {
    listener.close();
  }
});

/** One loopback pair: a counting receiver and a sender at `batchFrames`. */
async function startPair(batchFrames: number): Promise<{
  sender: FramedConn;
  expect(count: number): Promise<void>;
}> {
  const listener: Server = createServer();
  listeners.push(listener);
  await new Promise<void>((resolve) => {
    listener.listen(0, "127.0.0.1", resolve);
  });
  const port = (listener.address() as AddressInfo).port;
  const acceptedSocket: Promise<Socket> = new Promise((resolve) => {
    listener.once("connection", resolve);
  });
  const clientSocket: Socket = connect(port, "127.0.0.1");
  await new Promise<void>((resolve) => {
    clientSocket.once("connect", resolve);
  });

  let remaining = 0;
  let resolveDone: (() => void) | null = null;
  const receiver: FramedConn = new FramedConn(await acceptedSocket, {
    onFrame: (): void => {
      remaining--;

      if (remaining === 0) {
        const resolve = resolveDone as () => void;
        resolveDone = null;
        resolve();
      }
    },
    onViolation: (): void => {},
    onClose: (): void => {},
  });
  conns.push(receiver);

  const sender: FramedConn = new FramedConn(
    clientSocket,
    { onFrame: (): void => {}, onViolation: (): void => {}, onClose: (): void => {} },
    { batchFrames },
  );
  conns.push(sender);

  return {
    sender,
    expect(count: number): Promise<void> {
      remaining = count;
      return new Promise((resolve) => {
        resolveDone = resolve;
      });
    },
  };
}

describe("net dispatch batch", () => {
  it("measures frame throughput across frames-per-write settings", {
    timeout: 300_000,
  }, async () => {
    const reports: ScenarioReport[] = [];

    for (const batchFrames of SWEEP) {
      const pair = await startPair(batchFrames);
      const scenario: Scenario = {
        name: `${batchFrames} frame${batchFrames === 1 ? "" : "s"} per write`,
        batch: BATCH,
        op: async () => {
          const done = pair.expect(BATCH);

          for (let i = 0; i < BATCH; i++) {
            pair.sender.send({
              type: FRAME_DATA,
              flags: 0,
              lane: LANE_CONTROL,
              correlation: 0,
              body,
            });
          }

          await done;
        },
      };
      reports.push(await runScenario(scenario));
    }

    printReport(reports, "net dispatch batch  ·  frames per socket write");
  });
});
