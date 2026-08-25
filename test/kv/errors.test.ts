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
import { MAX_KEY_BYTES, MAX_VALUE_BYTES } from "../../src/kv/constants";
import {
  ClusterUnavailableError,
  KvLimitError,
  KvProtocolError,
  KvQuorumError,
  KvTimeoutError,
  PartitionRebalancingError,
} from "../../src/kv/errors";

describe("kv errors", () => {
  it("names a fragmented partition as retryable rebalancing", () => {
    const error: PartitionRebalancingError = new PartitionRebalancingError(17);
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("PartitionRebalancingError");
    expect(error.partitionId).toBe(17);
    expect(error.message).toBe("partition 17 is rebalancing");
  });

  it("names a downed or below-quorum node as unavailable", () => {
    const error: ClusterUnavailableError = new ClusterUnavailableError();
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("ClusterUnavailableError");
    expect(error.message).toBe("cluster is unavailable");
  });

  it("captures the elapsed RPC and bootstrap phases", () => {
    const request: KvTimeoutError = new KvTimeoutError("request", 5_000);
    expect(request.name).toBe("KvTimeoutError");
    expect(request.phase).toBe("request");
    expect(request.timeoutMs).toBe(5_000);
    expect(request.message).toBe("kv request timed out after 5000ms");

    const bootstrap: KvTimeoutError = new KvTimeoutError("bootstrap", 10_000);
    expect(bootstrap.phase).toBe("bootstrap");
    expect(bootstrap.message).toBe("kv bootstrap timed out after 10000ms");
  });

  it("captures unmet read and write quorums", () => {
    const read: KvQuorumError = new KvQuorumError("read", 0, 1);
    expect(read.name).toBe("KvQuorumError");
    expect(read.kind).toBe("read");
    expect(read.got).toBe(0);
    expect(read.need).toBe(1);
    expect(read.message).toBe("read quorum 1 not met (got 0)");

    const write: KvQuorumError = new KvQuorumError("write", 1, 2);
    expect(write.kind).toBe("write");
    expect(write.message).toBe("write quorum 2 not met (got 1)");
  });

  it("captures oversized keys and values against the published budgets", () => {
    const key: KvLimitError = new KvLimitError("key", MAX_KEY_BYTES + 1, MAX_KEY_BYTES);
    expect(key.name).toBe("KvLimitError");
    expect(key.field).toBe("key");
    expect(key.size).toBe(1_025);
    expect(key.max).toBe(MAX_KEY_BYTES);
    expect(key.message).toBe("key length 1025 exceeds 1024 bytes");

    const value: KvLimitError = new KvLimitError("value", MAX_VALUE_BYTES + 1, MAX_VALUE_BYTES);
    expect(value.field).toBe("value");
    expect(value.message).toBe("value length 1048577 exceeds 1048576 bytes");
  });

  it("preserves a protocol diagnostic without wrapping", () => {
    const error: KvProtocolError = new KvProtocolError("truncated entry");
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("KvProtocolError");
    expect(error.message).toBe("truncated entry");
  });
});
