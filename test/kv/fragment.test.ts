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
import { FRAGMENT_CHUNK_BYTES } from "../../src/kv/constants";
import { Engine } from "../../src/kv/engine";
import { FragmentTransfer } from "../../src/kv/fragment";
import { partitionId } from "../../src/kv/hash";
import type { Entry, KvTransport } from "../../src/kv/ports";
import { decodeMessage, encodeMessage, type FragmentChunkWire } from "../../src/kv/wire";
import { flush, SimFabric, settle } from "./sim";

const PARTITIONS: number = 8;

/** A value large enough that two entries cannot share one chunk. */
const BIG: number = Math.floor(FRAGMENT_CHUNK_BYTES * 0.6);

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

/** An engine whose injected clock is fixed, so transfers are deterministic. */
function engineAt(name: string): Engine {
  return new Engine(name, PARTITIONS, (): number => 1_000);
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

/** A node that answers fragment pulls and pushes from its own engine. */
interface Responder {
  readonly engine: Engine;
  readonly transfer: FragmentTransfer;
}

/** Wires `name` on the fabric to serve fragment pulls and pushes over its engine. */
function responder(fabric: SimFabric, name: string): Responder {
  const engine: Engine = engineAt(name);
  const transport: KvTransport = fabric.transport(name);
  const transfer: FragmentTransfer = new FragmentTransfer(engine, transport);
  transport.listen(async (_from: string, body: Uint8Array): Promise<Uint8Array> => {
    const message = decodeMessage(body);
    if (message.kind === "fragment-request") {
      return encodeMessage({
        kind: "fragment-chunk",
        chunk: transfer.servePage(message.partitionId, message.afterKey),
      });
    }

    if (message.kind === "fragment-push") {
      transfer.applyChunk(message.chunk);
      return encodeMessage({ kind: "fragment-ack" });
    }

    return bytes(0xff);
  });
  return { engine, transfer };
}

/** An initiating transfer for `name`, needing only an outbound transport. */
function initiator(
  fabric: SimFabric,
  name: string,
): { engine: Engine; transfer: FragmentTransfer } {
  const engine: Engine = engineAt(name);
  const transfer: FragmentTransfer = new FragmentTransfer(engine, fabric.transport(name));
  return { engine, transfer };
}

/** Installs a listener that replies with a fixed queue of responses, last one repeating. */
function scriptResponses(fabric: SimFabric, name: string, replies: readonly Uint8Array[]): void {
  let index: number = 0;
  fabric.transport(name).listen(async (): Promise<Uint8Array> => {
    const reply: Uint8Array = replies[Math.min(index, replies.length - 1)] as Uint8Array;
    index += 1;
    return reply;
  });
}

describe("FragmentTransfer servePage", () => {
  it("returns one final empty page for a partition it does not hold", () => {
    const { transfer } = initiator(new SimFabric(1), "A");
    const page: FragmentChunkWire = transfer.servePage(3, undefined);
    expect(page).toEqual({ partitionId: 3, final: true, entries: [] });
  });

  it("pages a fragment by byte budget, in key order, behind a cursor", () => {
    const { engine, transfer } = initiator(new SimFabric(1), "A");
    const { partition, keys } = keysInOnePartition(3);
    engine.merge(entryAt(keys[2] as string, new Uint8Array(BIG), 1_000));
    engine.merge(entryAt(keys[0] as string, new Uint8Array(BIG), 1_000));
    engine.merge(entryAt(keys[1] as string, new Uint8Array(BIG), 1_000));

    const sorted: string[] = [...keys].sort();
    const first: FragmentChunkWire = transfer.servePage(partition, undefined);
    expect(first.final).toBe(false);
    expect(first.entries).toHaveLength(1);
    expect((first.entries[0] as Entry).key).toBe(sorted[0]);

    const second: FragmentChunkWire = transfer.servePage(partition, sorted[0]);
    expect((second.entries[0] as Entry).key).toBe(sorted[1]);

    const last: FragmentChunkWire = transfer.servePage(partition, sorted[2]);
    expect(last).toEqual({ partitionId: partition, final: true, entries: [] });
  });

  it("packs small entries, tombstones included, into one final page", () => {
    const { engine, transfer } = initiator(new SimFabric(1), "A");
    const { partition, keys } = keysInOnePartition(3);
    engine.merge(entryAt(keys[0] as string, bytes(1), 1_000));
    engine.merge(entryAt(keys[1] as string, undefined, 1_000));
    engine.merge(entryAt(keys[2] as string, bytes(3), 1_000));
    const page: FragmentChunkWire = transfer.servePage(partition, undefined);
    expect(page.final).toBe(true);
    expect(page.entries).toHaveLength(3);
  });
});

describe("FragmentTransfer push", () => {
  it("streams a multi-chunk fragment to a receiver that merges every entry", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const source = initiator(fabric, "A");
    const sink: Responder = responder(fabric, "B");
    const { partition, keys } = keysInOnePartition(3);
    for (const key of keys) {
      source.engine.merge(entryAt(key, new Uint8Array(BIG), 1_000));
    }

    expect(await settle(fabric, source.transfer.push(partition, "B"))).toBe(true);
    for (const key of keys) {
      expect(sink.engine.peek(key)?.value).toHaveLength(BIG);
    }
  });

  it("confirms a completed move for an empty fragment with one final chunk", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const source = initiator(fabric, "A");
    responder(fabric, "B");
    expect(await settle(fabric, source.transfer.push(0, "B"))).toBe(true);
  });

  it("is idempotent and never overwrites a newer entry at the receiver", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const source = initiator(fabric, "A");
    const sink: Responder = responder(fabric, "B");
    const { partition, keys } = keysInOnePartition(1);
    const key: string = keys[0] as string;
    source.engine.merge(entryAt(key, bytes(1), 1_000));
    sink.engine.merge(entryAt(key, bytes(9), 5_000));

    expect(await settle(fabric, source.transfer.push(partition, "B"))).toBe(true);
    expect(await settle(fabric, source.transfer.push(partition, "B"))).toBe(true);
    expect(sink.engine.peek(key)?.value).toEqual(bytes(9));
  });

  it("reports failure and keeps its copy when the receiver is unreachable", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const source = initiator(fabric, "A");
    responder(fabric, "B");
    const { partition, keys } = keysInOnePartition(1);
    const key: string = keys[0] as string;
    source.engine.merge(entryAt(key, bytes(7), 1_000));
    fabric.partitionBoth("A", "B");
    expect(await settle(fabric, source.transfer.push(partition, "B"))).toBe(false);
    expect(source.engine.peek(key)?.value).toEqual(bytes(7));
  });

  it("reports failure when the receiver returns a non-acknowledgment", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const source = initiator(fabric, "A");
    const { partition, keys } = keysInOnePartition(1);
    source.engine.merge(entryAt(keys[0] as string, bytes(1), 1_000));
    scriptResponses(fabric, "B", [encodeMessage({ kind: "replicate-ack" })]);
    expect(await settle(fabric, source.transfer.push(partition, "B"))).toBe(false);
  });

  it("reports failure when the receiver returns malformed bytes", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const source = initiator(fabric, "A");
    const { partition, keys } = keysInOnePartition(1);
    source.engine.merge(entryAt(keys[0] as string, bytes(1), 1_000));
    scriptResponses(fabric, "B", [bytes(0xff)]);
    expect(await settle(fabric, source.transfer.push(partition, "B"))).toBe(false);
  });
});

describe("FragmentTransfer pull", () => {
  it("pulls a multi-chunk fragment and merges every entry", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const source: Responder = responder(fabric, "B");
    const sink = initiator(fabric, "A");
    const { partition, keys } = keysInOnePartition(3);
    for (const key of keys) {
      source.engine.merge(entryAt(key, new Uint8Array(BIG), 1_000));
    }

    await settle(fabric, sink.transfer.pull(partition, "B"));
    for (const key of keys) {
      expect(sink.engine.peek(key)?.value).toHaveLength(BIG);
    }
  });

  it("merges nothing when the peer is unreachable", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const sink = initiator(fabric, "A");
    await settle(fabric, sink.transfer.pull(0, "Z"));
    expect(sink.engine.snapshot(0)).toEqual([]);
  });

  it("stops on a malformed page and on a wrong-kind page", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const sink = initiator(fabric, "A");
    scriptResponses(fabric, "Y", [bytes(0xff)]);
    scriptResponses(fabric, "Z", [encodeMessage({ kind: "replicate-ack" })]);
    await settle(fabric, sink.transfer.pull(0, "Y"));
    await settle(fabric, sink.transfer.pull(0, "Z"));
    expect(sink.engine.snapshot(0)).toEqual([]);
  });

  it("stops when a non-final page fails to advance the cursor", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const sink = initiator(fabric, "A");
    const { partition, keys } = keysInOnePartition(1);
    const stuck: Uint8Array = encodeMessage({
      kind: "fragment-chunk",
      chunk: {
        partitionId: partition,
        final: false,
        entries: [entryAt(keys[0] as string, bytes(1), 1_000)],
      },
    });
    scriptResponses(fabric, "B", [stuck]);
    await settle(fabric, sink.transfer.pull(partition, "B"));
    expect(sink.engine.peek(keys[0] as string)?.value).toEqual(bytes(1));
  });

  it("stops when a non-final page carries no entries", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const sink = initiator(fabric, "A");
    const empty: Uint8Array = encodeMessage({
      kind: "fragment-chunk",
      chunk: { partitionId: 0, final: false, entries: [] },
    });
    scriptResponses(fabric, "B", [empty]);
    await settle(fabric, sink.transfer.pull(0, "B"));
    expect(sink.engine.snapshot(0)).toEqual([]);
    await flush();
  });
});
