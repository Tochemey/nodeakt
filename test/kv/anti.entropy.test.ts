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
import { AntiEntropy } from "../../src/kv/anti.entropy";
import { REPAIR_BUCKETS } from "../../src/kv/constants";
import { Engine } from "../../src/kv/engine";
import { repairBucket } from "../../src/kv/entry";
import { KvProtocolError } from "../../src/kv/errors";
import { FragmentTransfer } from "../../src/kv/fragment";
import { partitionId } from "../../src/kv/hash";
import type { Entry, KvTransport } from "../../src/kv/ports";
import { decodeMessage, encodeMessage, type KvMessage } from "../../src/kv/wire";
import { SimFabric, settle } from "./sim";

const PARTITIONS: number = 8;

function bytes(...values: number[]): Uint8Array {
  return new Uint8Array(values);
}

/** A live entry for `key` stamped at `wallMs`, or a tombstone when `value` is undefined. */
function entryAt(key: string, value: Uint8Array | undefined, wallMs: number): Entry {
  return {
    key,
    value,
    timestamp: { wallMs, logical: 0, node: "peer" },
    sequence: 1n,
    expiresAt: undefined,
    deleted: value === undefined,
  };
}

/** A node under test: its engine, its anti-entropy driver, and its fragment intake. */
interface AeNode {
  readonly engine: Engine;
  readonly antiEntropy: AntiEntropy;
  readonly transfer: FragmentTransfer;
}

/**
 * The inbound handler a real node installs: anti-entropy answers its own RPCs,
 * and the pushed entries a pass ships land through the fragment handler, exactly
 * as the clustering router dispatches them.
 */
function handlerFor(target: AeNode): (from: string, body: Uint8Array) => Promise<Uint8Array> {
  return async (from: string, body: Uint8Array): Promise<Uint8Array> => {
    const message: KvMessage = decodeMessage(body);
    if (message.kind === "fragment-push") {
      target.transfer.applyChunk(message.chunk);
      return encodeMessage({ kind: "fragment-ack" });
    }

    return target.antiEntropy.receive(from, body);
  };
}

/** Builds one anti-entropy node wired to the fabric, serving inbound passes. */
function node(fabric: SimFabric, name: string): AeNode {
  const engine: Engine = new Engine(name, PARTITIONS, (): number => 1_000);
  const transport: KvTransport = fabric.transport(name);
  const target: AeNode = {
    engine,
    antiEntropy: new AntiEntropy(engine, transport),
    transfer: new FragmentTransfer(engine, transport),
  };
  transport.listen(handlerFor(target));
  return target;
}

/** Reinstalls `name`'s listener so a chosen message kind is answered by `override`. */
function intercept(
  target: AeNode,
  fabric: SimFabric,
  name: string,
  override: (message: KvMessage) => Uint8Array | undefined,
): void {
  const base: (from: string, body: Uint8Array) => Promise<Uint8Array> = handlerFor(target);
  fabric.transport(name).listen(async (from: string, body: Uint8Array): Promise<Uint8Array> => {
    const overridden: Uint8Array | undefined = override(decodeMessage(body));
    return overridden !== undefined ? overridden : base(from, body);
  });
}

/** The lowest `count` keys that all map to one shared partition. */
function keysInOnePartition(count: number): { partition: number; keys: string[] } {
  const partition: number = partitionId("key-0", PARTITIONS);
  const keys: string[] = [];
  for (let index: number = 0; keys.length < count && index < 100_000; index += 1) {
    const key: string = `key-${index}`;
    if (partitionId(key, PARTITIONS) === partition) {
      keys.push(key);
    }
  }

  return { partition, keys };
}

/** Two keys that share both a partition and a repair bucket. */
function sameBucketKeys(): { partition: number; keys: [string, string] } {
  const seen: Map<string, string> = new Map<string, string>();
  for (let index: number = 0; index < 200_000; index += 1) {
    const key: string = `key-${index}`;
    const cell: string = `${partitionId(key, PARTITIONS)}:${repairBucket(key, REPAIR_BUCKETS)}`;
    const first: string | undefined = seen.get(cell);
    if (first !== undefined) {
      return { partition: partitionId(key, PARTITIONS), keys: [first, key] };
    }

    seen.set(cell, key);
  }

  throw new Error("no two keys shared a partition and bucket");
}

describe("AntiEntropy convergence", () => {
  it("does nothing when the two replicas already agree", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const key: string = "same";
    const partition: number = partitionId(key, PARTITIONS);
    a.engine.merge(entryAt(key, bytes(1), 1_000));
    b.engine.merge(entryAt(key, bytes(1), 1_000));
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(a.engine.peek(key)?.value).toEqual(bytes(1));
    expect(b.engine.peek(key)?.value).toEqual(bytes(1));
  });

  it("pulls an entry the peer holds and this node is missing", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const key: string = "only-on-b";
    const partition: number = partitionId(key, PARTITIONS);
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(a.engine.peek(key)?.value).toEqual(bytes(9));
  });

  it("pushes an entry this node holds newer than the peer", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const key: string = "newer-on-a";
    const partition: number = partitionId(key, PARTITIONS);
    a.engine.merge(entryAt(key, bytes(2), 2_000));
    b.engine.merge(entryAt(key, bytes(1), 1_000));
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(b.engine.peek(key)?.value).toEqual(bytes(2));
  });

  it("pushes an entry the peer is missing entirely", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const key: string = "only-on-a";
    const partition: number = partitionId(key, PARTITIONS);
    a.engine.merge(entryAt(key, bytes(3), 2_000));
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(b.engine.peek(key)?.value).toEqual(bytes(3));
  });

  it("converges both sides in one pass when each holds a newer key", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const { partition, keys } = keysInOnePartition(2);
    const [left, right] = keys as [string, string];
    a.engine.merge(entryAt(left, bytes(2), 2_000));
    a.engine.merge(entryAt(right, bytes(1), 1_000));
    b.engine.merge(entryAt(left, bytes(1), 1_000));
    b.engine.merge(entryAt(right, bytes(2), 2_000));
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(a.engine.peek(left)?.value).toEqual(bytes(2));
    expect(a.engine.peek(right)?.value).toEqual(bytes(2));
    expect(b.engine.peek(left)?.value).toEqual(bytes(2));
    expect(b.engine.peek(right)?.value).toEqual(bytes(2));
  });

  it("leaves an identical key untouched inside a divergent bucket", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const { partition, keys } = sameBucketKeys();
    const [differing, identical] = keys;
    a.engine.merge(entryAt(differing, bytes(2), 2_000));
    a.engine.merge(entryAt(identical, bytes(5), 1_000));
    b.engine.merge(entryAt(differing, bytes(1), 1_000));
    b.engine.merge(entryAt(identical, bytes(5), 1_000));
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(b.engine.peek(differing)?.value).toEqual(bytes(2));
    expect(b.engine.peek(identical)?.value).toEqual(bytes(5));
  });

  it("propagates a tombstone so a missed delete is not repaired back to life", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const key: string = "deleted";
    const partition: number = partitionId(key, PARTITIONS);
    a.engine.merge(entryAt(key, undefined, 2_000));
    b.engine.merge(entryAt(key, bytes(1), 1_000));
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(b.engine.peek(key)?.deleted).toBe(true);
    expect(await b.engine.read(key)).toBeUndefined();
  });
});

describe("AntiEntropy resilience", () => {
  it("aborts when the peer is unreachable", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    node(fabric, "B");
    const key: string = "k";
    const partition: number = partitionId(key, PARTITIONS);
    a.engine.merge(entryAt(key, bytes(1), 1_000));
    fabric.partitionBoth("A", "B");
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(a.engine.peek(key)?.value).toEqual(bytes(1));
  });

  it("aborts when the digest open is answered with the wrong message", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    intercept(b, fabric, "B", (): Uint8Array => encodeMessage({ kind: "replicate-ack" }));
    const key: string = "k";
    const partition: number = partitionId(key, PARTITIONS);
    a.engine.merge(entryAt(key, bytes(1), 1_000));
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(a.engine.peek(key)?.value).toEqual(bytes(1));
  });

  it("aborts when the peer returns a bucket-digest list of the wrong length", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    intercept(
      b,
      fabric,
      "B",
      (): Uint8Array =>
        encodeMessage({ kind: "bucket-digests", partitionId: 0, digests: [{ hi: 1, lo: 1 }] }),
    );
    await settle(fabric, a.antiEntropy.sync(0, "B"));
    expect(a.engine.snapshot(0)).toEqual([]);
  });

  it("aborts when the key-versions reply is malformed", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const key: string = "only-on-b";
    const partition: number = partitionId(key, PARTITIONS);
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    intercept(b, fabric, "B", (message: KvMessage): Uint8Array | undefined =>
      message.kind === "key-versions-request" ? bytes(0xff) : undefined,
    );
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(a.engine.peek(key)).toBeUndefined();
  });

  it("aborts when the key-versions reply is the wrong message", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const key: string = "only-on-b";
    const partition: number = partitionId(key, PARTITIONS);
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    intercept(b, fabric, "B", (message: KvMessage): Uint8Array | undefined =>
      message.kind === "key-versions-request"
        ? encodeMessage({ kind: "replicate-ack" })
        : undefined,
    );
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(a.engine.peek(key)).toBeUndefined();
  });

  it("merges nothing when the pulled entries are malformed", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const key: string = "only-on-b";
    const partition: number = partitionId(key, PARTITIONS);
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    intercept(b, fabric, "B", (message: KvMessage): Uint8Array | undefined =>
      message.kind === "entries-request" ? bytes(0xff) : undefined,
    );
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(a.engine.peek(key)).toBeUndefined();
  });

  it("merges nothing when the pulled entries are the wrong message", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: AeNode = node(fabric, "A");
    const b: AeNode = node(fabric, "B");
    const key: string = "only-on-b";
    const partition: number = partitionId(key, PARTITIONS);
    b.engine.merge(entryAt(key, bytes(9), 2_000));
    intercept(b, fabric, "B", (message: KvMessage): Uint8Array | undefined =>
      message.kind === "entries-request" ? encodeMessage({ kind: "replicate-ack" }) : undefined,
    );
    await settle(fabric, a.antiEntropy.sync(partition, "B"));
    expect(a.engine.peek(key)).toBeUndefined();
  });

  it("rejects an inbound message it does not serve", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const b: AeNode = node(fabric, "B");
    await expect(
      b.antiEntropy.receive("A", encodeMessage({ kind: "replicate-ack" })),
    ).rejects.toThrow(KvProtocolError);
  });
});
