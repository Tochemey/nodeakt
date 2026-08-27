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
import type {
  ClusterMember,
  ClusterView,
  CompareAndSetOp,
  DeleteOp,
  Entry,
  IncrementOp,
  KvTransport,
  PutOp,
  ReplicationGroup,
  WriteOp,
  WriteResult,
} from "../../src/kv/ports";
import * as ports from "../../src/kv/ports";

function member(
  name: string,
  startedAt: number,
  ready: boolean = true,
  draining: boolean = false,
): ClusterMember {
  return { name, startedAt, ready, draining };
}

function entry(key: string, value: Uint8Array | undefined, deleted: boolean): Entry {
  return {
    key,
    value,
    timestamp: { wallMs: 1, logical: 0, node: "n1" },
    sequence: 1n,
    expiresAt: undefined,
    deleted,
  };
}

describe("kv ports", () => {
  it("erases to an empty runtime module", () => {
    expect(Object.keys(ports)).toEqual([]);
  });

  it("is implementable as a cluster view, transport, and replication group", async () => {
    const members: ClusterMember[] = [member("n1", 10), member("n2", 20, true, true)];
    const view: ClusterView = {
      self: "n1",
      members: (): readonly ClusterMember[] => members,
      onChange: (): (() => void) => (): void => undefined,
    };

    expect(view.self).toBe("n1");
    expect(view.members()).toEqual(members);
    expect(members[1]?.draining).toBe(true);
    view.onChange((): void => undefined)();

    const put: PutOp = { kind: "put", key: "a", value: new Uint8Array([1]), condition: "nx" };
    const increment: IncrementOp = { kind: "incr", key: "a", delta: 1n };
    const compareAndSet: CompareAndSetOp = {
      kind: "cas",
      key: "a",
      expected: new Uint8Array([1]),
      value: new Uint8Array([2]),
    };
    const remove: DeleteOp = { kind: "delete", key: "a" };
    const stored: Entry = entry("a", new Uint8Array([2]), false);
    const tombstone: Entry = entry("a", undefined, true);

    const transport: KvTransport = {
      request: async (): Promise<Uint8Array> => new Uint8Array([0]),
      listen: (): void => undefined,
      stop: async (): Promise<void> => undefined,
    };
    transport.listen(async (_from: string, body: Uint8Array): Promise<Uint8Array> => body);
    expect(await transport.request("n2", new Uint8Array([9]), 5_000)).toEqual(new Uint8Array([0]));
    await transport.stop();

    const group: ReplicationGroup = {
      propose: async (op: WriteOp): Promise<WriteResult> => {
        if (op.kind === "put" && op.condition === "nx") {
          return { applied: false, reason: "nx" };
        }

        return { applied: true, entry: stored };
      },
      read: async (key: string): Promise<Entry | undefined> =>
        key === stored.key ? stored : undefined,
      reconcile: async (): Promise<void> => undefined,
      memberChange: (): void => undefined,
    };

    expect(await group.propose(put)).toEqual({ applied: false, reason: "nx" });
    expect(await group.propose(increment)).toEqual({ applied: true, entry: stored });
    expect(await group.propose(compareAndSet)).toEqual({ applied: true, entry: stored });
    expect(await group.propose(remove)).toEqual({ applied: true, entry: stored });
    expect(await group.read("a")).toEqual(stored);
    expect(await group.read("missing")).toBeUndefined();
    await group.reconcile(["n2"]);
    group.memberChange(["n1", "n2"]);
    expect(tombstone.deleted).toBe(true);
    expect(tombstone.value).toBeUndefined();
  });

  it("accepts an optional put TTL without requiring it on other writes", () => {
    const leased: PutOp = {
      kind: "put",
      key: "lease",
      value: new Uint8Array([1]),
      condition: "none",
      ttlMs: 5_000,
    };
    expect(leased.ttlMs).toBe(5_000);
  });
});
