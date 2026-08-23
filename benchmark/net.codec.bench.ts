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

import { describe, it } from "vitest";
import { ByteReader, ByteWriter, decodeValue, encodeValue } from "../src/_net/values";
import {
  printBlock,
  printReport,
  runScenario,
  type Scenario,
  type ScenarioReport,
} from "./harness";

/**
 * The binary value codec against `JSON` on a representative message:
 * a nested order with strings, numbers, booleans, and arrays, the
 * shape a payload actually takes. Both scenarios measure one full
 * round trip, encode plus decode, on the same object; the byte sizes
 * of both encodings are reported alongside so the throughput numbers
 * carry their wire-cost context. `JSON` cannot carry the codec's full
 * domain (binary buffers, Map, Set, Date, cycles), so this compares
 * the shared subset only.
 */

/** Round trips per benchmark operation. */
const BATCH = 10_000;

const message = {
  orderId: "ord-2318-77aa",
  customerId: "cus-90211",
  status: "confirmed",
  createdAtMs: 1755907200000,
  total: 1249.99,
  currency: "USD",
  express: true,
  items: [
    { sku: "sku-1001", name: "mechanical keyboard", quantity: 1, price: 149.99 },
    { sku: "sku-1002", name: "27 inch monitor", quantity: 2, price: 329.5 },
    { sku: "sku-1003", name: "usb-c dock", quantity: 1, price: 189.0 },
    { sku: "sku-1004", name: "webcam", quantity: 1, price: 79.99 },
    { sku: "sku-1005", name: "desk mat", quantity: 3, price: 24.0 },
  ],
  shipping: {
    line1: "12 Harbor Way",
    city: "Rotterdam",
    zip: "3011",
    country: "NL",
    instructions: null,
  },
  tags: ["priority", "gift"],
};

const writer: ByteWriter = new ByteWriter(4096);

const scenarios: Scenario[] = [
  {
    name: "value codec encode+decode",
    batch: BATCH,
    op: async () => {
      for (let i = 0; i < BATCH; i++) {
        writer.reset();
        encodeValue(writer, message);
        decodeValue(new ByteReader(writer.bytes()));
      }
    },
  },
  {
    name: "JSON stringify+parse",
    batch: BATCH,
    op: async () => {
      for (let i = 0; i < BATCH; i++) {
        JSON.parse(JSON.stringify(message));
      }
    },
  },
];

describe("net value codec", () => {
  it("measures the codec against JSON on a representative message", {
    timeout: 300_000,
  }, async () => {
    const reports: ScenarioReport[] = [];

    for (const scenario of scenarios) {
      reports.push(await runScenario(scenario));
    }

    printReport(reports, "net value codec  ·  round trips against JSON");

    writer.reset();
    encodeValue(writer, message);
    printBlock(
      "net value codec  ·  encoded size",
      ["encoding", "bytes"],
      [
        ["value codec", `${writer.length}`],
        ["JSON", `${Buffer.byteLength(JSON.stringify(message))}`],
      ],
      "same message, both encodings",
    );
  });
});
