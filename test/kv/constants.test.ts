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

import { describe, expect, it } from "vitest";
import {
  BOOTSTRAP_TIMEOUT_MS,
  DEFAULT_MEMBER_QUORUM,
  DEFAULT_PARTITION_COUNT,
  DEFAULT_READ_QUORUM,
  DEFAULT_REPLICA_COUNT,
  DEFAULT_WRITE_QUORUM,
  FRAGMENT_CHUNK_BYTES,
  JANITOR_INTERVAL_MS,
  JANITOR_PARTITIONS_PER_SWEEP,
  LEAVE_DRAIN_TIMEOUT_MS,
  LOAD_FACTOR,
  MAX_KEY_BYTES,
  MAX_VALUE_BYTES,
  REPAIR_BUCKETS,
  REPAIR_INTERVAL_MS,
  REQUEST_TIMEOUT_MS,
  RING_POINTS_PER_MEMBER,
  SCAN_PAGE_SIZE,
  SCAN_YIELD_EVERY,
  TABLE_PUSH_INTERVAL_MS,
  TOMBSTONE_TTL_MS,
} from "../../src/kv/constants";

describe("kv constants", () => {
  it("matches the design defaults that make a single-node loss durable", () => {
    expect(DEFAULT_PARTITION_COUNT).toBe(512);
    expect(LOAD_FACTOR).toBe(1.25);
    expect(RING_POINTS_PER_MEMBER).toBe(20);
    expect(DEFAULT_REPLICA_COUNT).toBe(3);
    expect(DEFAULT_WRITE_QUORUM).toBe(2);
    expect(DEFAULT_READ_QUORUM).toBe(1);
    expect(DEFAULT_MEMBER_QUORUM).toBe(1);
  });

  it("matches the design operational intervals and size budgets", () => {
    expect(TOMBSTONE_TTL_MS).toBe(600_000);
    expect(REPAIR_INTERVAL_MS).toBe(10_000);
    expect(REPAIR_BUCKETS).toBe(64);
    expect(LEAVE_DRAIN_TIMEOUT_MS).toBe(30_000);
    expect(TABLE_PUSH_INTERVAL_MS).toBe(60_000);
    expect(REQUEST_TIMEOUT_MS).toBe(5_000);
    expect(BOOTSTRAP_TIMEOUT_MS).toBe(10_000);
    expect(FRAGMENT_CHUNK_BYTES).toBe(262_144);
    expect(SCAN_PAGE_SIZE).toBe(256);
    expect(SCAN_YIELD_EVERY).toBe(1_024);
    expect(JANITOR_INTERVAL_MS).toBe(30_000);
    expect(JANITOR_PARTITIONS_PER_SWEEP).toBe(64);
    expect(MAX_KEY_BYTES).toBe(1_024);
    expect(MAX_VALUE_BYTES).toBe(1_048_576);
  });
});
