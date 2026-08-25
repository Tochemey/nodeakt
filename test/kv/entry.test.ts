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
import { TOMBSTONE_TTL_MS } from "../../src/kv/constants";
import {
  type DigestLanes,
  entryContribution,
  isExpired,
  isLiveValue,
  isReapableTombstone,
  supersedes,
} from "../../src/kv/entry";
import type { Entry, HybridTime } from "../../src/kv/ports";

function at(wallMs: number, logical: number = 0, node: string = "n1"): HybridTime {
  return { wallMs, logical, node };
}

function value(key: string, timestamp: HybridTime, overrides: Partial<Entry> = {}): Entry {
  return {
    key,
    value: new Uint8Array([1]),
    timestamp,
    sequence: 1n,
    expiresAt: undefined,
    deleted: false,
    ...overrides,
  };
}

function tombstone(key: string, timestamp: HybridTime): Entry {
  return { key, value: undefined, timestamp, sequence: 1n, expiresAt: undefined, deleted: true };
}

describe("supersedes", () => {
  it("wins only on a strictly later timestamp", () => {
    const older: Entry = value("k", at(1));
    const newer: Entry = value("k", at(2));
    expect(supersedes(newer, older)).toBe(true);
    expect(supersedes(older, newer)).toBe(false);
    expect(supersedes(older, value("k", at(1)))).toBe(false);
  });
});

describe("isExpired", () => {
  it("is false without an expiry and tracks the boundary when set", () => {
    expect(isExpired(value("k", at(1)), 10)).toBe(false);
    expect(isExpired(value("k", at(1), { expiresAt: 10 }), 9)).toBe(false);
    expect(isExpired(value("k", at(1), { expiresAt: 10 }), 10)).toBe(true);
    expect(isExpired(value("k", at(1), { expiresAt: 10 }), 11)).toBe(true);
  });
});

describe("isLiveValue", () => {
  it("is true only for a present, unexpired, non-tombstone entry", () => {
    expect(isLiveValue(value("k", at(1)), 0)).toBe(true);
    expect(isLiveValue(tombstone("k", at(1)), 0)).toBe(false);
    expect(isLiveValue(value("k", at(1), { expiresAt: 5 }), 5)).toBe(false);
  });
});

describe("isReapableTombstone", () => {
  it("reaps a tombstone only once it has outlived the retention window", () => {
    const stamp: HybridTime = at(1_000);
    expect(isReapableTombstone(value("k", stamp), 1_000 + TOMBSTONE_TTL_MS)).toBe(false);
    expect(isReapableTombstone(tombstone("k", stamp), 1_000 + TOMBSTONE_TTL_MS - 1)).toBe(false);
    expect(isReapableTombstone(tombstone("k", stamp), 1_000 + TOMBSTONE_TTL_MS)).toBe(true);
  });
});

describe("entryContribution", () => {
  it("is deterministic for identical content", () => {
    const a: DigestLanes = entryContribution(value("payments", at(1_700_000_000_000, 3, "n7")));
    const b: DigestLanes = entryContribution(value("payments", at(1_700_000_000_000, 3, "n7")));
    expect(a).toEqual(b);
  });

  it("changes with the key, the timestamp, and the tombstone flag", () => {
    const base: DigestLanes = entryContribution(value("k", at(100, 1, "n1")));
    expect(entryContribution(value("k2", at(100, 1, "n1")))).not.toEqual(base);
    expect(entryContribution(value("k", at(101, 1, "n1")))).not.toEqual(base);
    expect(entryContribution(value("k", at(100, 2, "n1")))).not.toEqual(base);
    expect(entryContribution(value("k", at(100, 1, "n2")))).not.toEqual(base);
    expect(entryContribution(tombstone("k", at(100, 1, "n1")))).not.toEqual(base);
  });

  it("folds the high half of a wall time beyond 32 bits", () => {
    const small: DigestLanes = entryContribution(value("k", at(1, 0, "n1")));
    const large: DigestLanes = entryContribution(value("k", at(2 ** 40, 0, "n1")));
    expect(large).not.toEqual(small);
    expect(entryContribution(value("k", at(2 ** 40, 0, "n1")))).toEqual(large);
  });
});
