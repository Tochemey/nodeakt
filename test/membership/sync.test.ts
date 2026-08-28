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
import { BroadcastQueue } from "../../src/membership/broadcast";
import type { Clock, ClockTimer } from "../../src/membership/clock";
import { SeededRandom } from "../../src/membership/random";
import {
  ANTI_ENTROPY_INTERVAL_MS,
  AntiEntropy,
  cancelSupersededSuspicion,
  fanoutLeave,
  JoinError,
  type JoinResult,
  join,
  leaveTargets,
  mergeSyncTable,
  readSyncFrames,
  respondToSync,
  type SyncExchange,
  type SyncOptions,
  SyncTimeoutError,
  syncWith,
  writeSyncFrames,
} from "../../src/membership/sync";
import type {
  MembershipStream,
  MembershipTransport,
  TransportHandlers,
} from "../../src/membership/transport";
import { MembershipView } from "../../src/membership/view";
import {
  encodeSyncChunks,
  MAX_MEMBERS,
  MAX_METADATA_BYTES,
  MAX_SYNC_MESSAGE_BYTES,
  MESSAGE_SYNC_REQUEST,
  MESSAGE_SYNC_RESPONSE,
  type MembershipUpdate,
  ProtocolError,
  STATE_ALIVE,
  STATE_DEAD,
  STATE_LEFT,
  STATE_SUSPECT,
} from "../../src/membership/wire";
import { flush, SimClock, SimNetwork, settle } from "./sim";

const empty: Uint8Array = new Uint8Array(0);

function alive(
  member: string,
  incarnation: number = 0,
  metadata: Uint8Array = empty,
): MembershipUpdate {
  return {
    state: STATE_ALIVE,
    selfOriginated: true,
    incarnation,
    stateChangeTime: BigInt(incarnation),
    member,
    reporter: "",
    metadata,
  };
}

function accusation(
  member: string,
  state: typeof STATE_SUSPECT | typeof STATE_DEAD,
  incarnation: number,
  reporter: string = "reporter",
): MembershipUpdate {
  return {
    state,
    selfOriginated: false,
    incarnation,
    stateChangeTime: BigInt(incarnation),
    member,
    reporter: state === STATE_SUSPECT ? reporter : "",
    metadata: empty,
  };
}

function membership(self: string, updates: readonly MembershipUpdate[] = []): MembershipView {
  const view: MembershipView = new MembershipView(self);
  view.applyLocal(alive(self), 0);
  for (const update of updates) {
    view.apply(update, 0);
  }
  return view;
}

function options(
  network: SimNetwork,
  transport: MembershipTransport,
  view: MembershipView,
  callbacks: SyncOptions["callbacks"] = {},
): SyncOptions {
  return {
    view,
    broadcasts: new BroadcastQueue(),
    clock: network.clock,
    random: new SeededRandom(17),
    transport,
    callbacks,
  };
}

async function startSyncEndpoint(
  network: SimNetwork,
  address: string,
  view: MembershipView,
): Promise<{ readonly transport: MembershipTransport; readonly options: SyncOptions }> {
  const transport: MembershipTransport = network.endpoint(address);
  const configured: SyncOptions = options(network, transport, view);
  await transport.start({
    packet(): void {},
    stream(_from, stream): void {
      void respondToSync(configured, stream);
    },
  });
  return { transport, options: configured };
}

class ChunkStream implements MembershipStream {
  readonly writes: Uint8Array[] = [];
  private index: number = 0;

  constructor(
    readonly remoteAddress: string,
    private readonly reads: readonly Uint8Array[],
  ) {}

  read(): Promise<Uint8Array | undefined> {
    const bytes: Uint8Array | undefined = this.reads[this.index];
    this.index += 1;
    return Promise.resolve(bytes?.slice());
  }

  write(bytes: Uint8Array): Promise<void> {
    this.writes.push(bytes.slice());
    return Promise.resolve();
  }

  close(): void {}
}

function fragmented(bytes: Uint8Array): readonly Uint8Array[] {
  const parts: Uint8Array[] = [];
  for (let offset = 0; offset < bytes.length; offset += 3) {
    parts.push(bytes.slice(offset, Math.min(offset + 3, bytes.length)));
  }
  return parts;
}

describe("sync stream framing", () => {
  it("writes u32 frames and reads arbitrarily fragmented chunks", async () => {
    const writer: ChunkStream = new ChunkStream("b", []);
    await writeSyncFrames(writer, MESSAGE_SYNC_REQUEST, 7n, [alive("a"), alive("b")]);
    const wire: Uint8Array = writer.writes[0] as Uint8Array;
    expect(new DataView(wire.buffer, wire.byteOffset, wire.byteLength).getUint32(0)).toBe(
      wire.length - 4,
    );

    const read: SyncExchange = await readSyncFrames(
      new ChunkStream("a", fragmented(wire)),
      MESSAGE_SYNC_REQUEST,
      7n,
    );
    expect(read.updates.map((update): string => update.member)).toEqual(["a", "b"]);
  });

  it("rejects partial frames before exposing any table", async () => {
    const chunk: Uint8Array = encodeSyncChunks(MESSAGE_SYNC_RESPONSE, 9n, [
      alive("remote"),
    ])[0] as Uint8Array;
    const frame: Uint8Array = new Uint8Array(4 + chunk.length);
    new DataView(frame.buffer).setUint32(0, chunk.length);
    frame.set(chunk, 4);
    await expect(
      readSyncFrames(
        new ChunkStream("remote", [frame.slice(0, frame.length - 1)]),
        MESSAGE_SYNC_RESPONSE,
        9n,
      ),
    ).rejects.toThrow(/inside a frame/);
  });
});

describe("sync exchange and merge", () => {
  it("returns the responder's post-merge table and merges the response atomically", async () => {
    const network: SimNetwork = new SimNetwork(1);
    const remote: { readonly transport: MembershipTransport; readonly options: SyncOptions } =
      await startSyncEndpoint(network, "b", membership("b", [alive("c")]));
    const localTransport: MembershipTransport = network.endpoint("a");
    await localTransport.start({ packet(): void {}, stream(): void {} });
    const local: SyncOptions = options(network, localTransport, membership("a"));

    const exchange: SyncExchange = await settle(network, syncWith(local, "b"));
    expect(exchange.updates.map((update): string => update.member)).toEqual(["b", "c", "a"]);
    expect(local.view.get("c")?.state).toBe(STATE_ALIVE);
    expect(remote.options.view.get("a")?.state).toBe(STATE_ALIVE);
  });

  it("enqueues accepted truth, routes confirmations, and refutes above the observed maximum", () => {
    const network: SimNetwork = new SimNetwork(2);
    const view: MembershipView = membership("a", [accusation("b", STATE_SUSPECT, 3, "first")]);
    const queue: BroadcastQueue = new BroadcastQueue();
    const confirmations: string[] = [];
    const cancellations: string[] = [];
    const refutations: number[] = [];
    const configured: SyncOptions = {
      view,
      broadcasts: queue,
      clock: network.clock,
      random: network.random,
      transport: network.endpoint("unused"),
      callbacks: {
        applied(update): void {
          cancelSupersededSuspicion(update, (member, incarnation): void => {
            cancellations.push(`${member}:${incarnation}`);
          });
        },
        confirmSuspicion(update): void {
          confirmations.push(`${update.member}:${update.incarnation}:${update.reporter}`);
        },
        selfRefuted(update): void {
          refutations.push(update.incarnation);
        },
      },
    };

    mergeSyncTable(configured, [
      accusation("b", STATE_SUSPECT, 3, "second"),
      accusation("a", STATE_DEAD, 12),
      alive("c"),
    ]);

    expect(confirmations).toEqual(["b:3:second"]);
    expect(view.self()?.incarnation).toBe(13);
    expect(refutations).toEqual([13]);
    expect(queue.get("a")?.priority).toBe(true);
    expect(queue.get("c")).toBeDefined();
    expect(cancellations).toContain("a:13");
  });
});

describe("lifecycle gates", () => {
  it("gates outbound sync work on the engine lifecycle", async () => {
    const network: SimNetwork = new SimNetwork(11);
    await startSyncEndpoint(network, "b", membership("b"));
    const localTransport: MembershipTransport = network.endpoint("a");
    await localTransport.start({ packet(): void {}, stream(): void {} });
    let allowed: boolean = false;
    const local: SyncOptions = {
      ...options(network, localTransport, membership("a")),
      outboundAllowed: (): boolean => allowed,
    };

    await expect(syncWith(local, "b")).rejects.toThrow(/no longer permits outbound sync/);
    await expect(join(local, ["b"])).rejects.toBeInstanceOf(JoinError);

    // The gate is re-checked after the response arrives, before any merge.
    allowed = true;
    const outcome: Promise<unknown> = syncWith(local, "b").catch(
      (error: unknown): unknown => error,
    );
    allowed = false;
    await settle(network, outcome);
    expect(await outcome).toMatchObject({ name: "SyncExchangeError" });
    expect(local.view.get("b")).toBeUndefined();
  });
});

describe("join", () => {
  it("deduplicates seeds, keeps order, and treats later failures as best-effort", async () => {
    const network: SimNetwork = new SimNetwork(3);
    await startSyncEndpoint(network, "b", membership("b"));
    await startSyncEndpoint(network, "c", membership("c"));
    const localTransport: MembershipTransport = network.endpoint("a");
    await localTransport.start({ packet(): void {}, stream(): void {} });
    const local: SyncOptions = options(network, localTransport, membership("a"));

    const result: JoinResult = await settle(network, join(local, ["a", "missing", "b", "b", "c"]));
    expect(result.seed).toBe("b");
    expect(result.succeeded).toEqual(["b", "c"]);
    expect(result.failed).toEqual(["missing"]);
    expect(await join(local, [])).toEqual({ seed: undefined, succeeded: [], failed: [] });
  });

  it("rejects when no seed succeeds without applying remote partial data", async () => {
    const network: SimNetwork = new SimNetwork(4);
    const bad: MembershipTransport = network.endpoint("bad");
    await bad.start({
      packet(): void {},
      stream(_from, stream): void {
        const chunk: Uint8Array = encodeSyncChunks(MESSAGE_SYNC_RESPONSE, 1n, [
          alive("leak"),
        ])[0] as Uint8Array;
        const partial: Uint8Array = new Uint8Array(4 + chunk.length);
        new DataView(partial.buffer).setUint32(0, chunk.length);
        partial.set(chunk, 4);
        void stream.write(partial.slice(0, partial.length - 1)).then((): void => stream.close());
      },
    });
    const localTransport: MembershipTransport = network.endpoint("a");
    await localTransport.start({ packet(): void {}, stream(): void {} });
    const local: SyncOptions = options(network, localTransport, membership("a"));

    await expect(settle(network, join(local, ["bad"]))).rejects.toBeInstanceOf(JoinError);
    expect(local.view.members().map((member): string => member.member)).toEqual(["a"]);
  });
});

describe("anti-entropy and leave", () => {
  it("does not overlap anti-entropy ticks", async () => {
    const clock: SimClock = new SimClock();
    let streams: number = 0;
    const transport: MembershipTransport = {
      address: "a",
      start(): Promise<void> {
        return Promise.resolve();
      },
      stop(): Promise<void> {
        return Promise.resolve();
      },
      packet(): Promise<void> {
        return Promise.resolve();
      },
      stream(): Promise<MembershipStream> {
        streams += 1;
        return new Promise<MembershipStream>((): void => undefined);
      },
    };
    const antiEntropy: AntiEntropy = new AntiEntropy({
      view: membership("a", [alive("b")]),
      broadcasts: new BroadcastQueue(),
      clock,
      random: new SeededRandom(1),
      transport,
    });
    antiEntropy.start();
    clock.advanceTo(ANTI_ENTROPY_INTERVAL_MS * 2);
    expect(streams).toBe(1);
    antiEntropy.stop();
    await flush();
  });

  it("fans out left truth to eligible peers in deterministic name order", async () => {
    const sent: string[] = [];
    const transport: MembershipTransport = {
      address: "a",
      start(_handlers: TransportHandlers): Promise<void> {
        return Promise.resolve();
      },
      stop(): Promise<void> {
        return Promise.resolve();
      },
      packet(to): Promise<void> {
        sent.push(to);
        return Promise.resolve();
      },
      stream(): Promise<MembershipStream> {
        return Promise.reject(new Error("unused"));
      },
    };
    const view: MembershipView = membership("a", [alive("c"), alive("b")]);
    const update: MembershipUpdate = { ...alive("a"), state: STATE_LEFT, metadata: empty };

    const targets: readonly string[] = leaveTargets(view);
    expect(targets).toEqual(["b", "c"]);
    await fanoutLeave(transport, update, targets);
    expect(sent).toEqual(["b", "c"]);
  });
});

describe("sync defensive paths", () => {
  it("rejects invalid prefixes, lengths, roles, IDs, chunk order, and trailing bytes", async () => {
    await expect(readSyncFrames(new ChunkStream("b", []), MESSAGE_SYNC_REQUEST)).rejects.toThrow(
      /frame prefix/,
    );

    for (const length of [3, MAX_SYNC_MESSAGE_BYTES + 1]) {
      const prefix: Uint8Array = new Uint8Array(4);
      new DataView(prefix.buffer).setUint32(0, length);
      await expect(
        readSyncFrames(new ChunkStream("b", [prefix]), MESSAGE_SYNC_REQUEST),
      ).rejects.toBeInstanceOf(ProtocolError);
    }

    const response: ChunkStream = new ChunkStream("b", []);
    await writeSyncFrames(response, MESSAGE_SYNC_RESPONSE, 8n, [alive("b")]);
    await expect(
      readSyncFrames(new ChunkStream("b", response.writes), MESSAGE_SYNC_REQUEST),
    ).rejects.toThrow(/type/);
    await expect(
      readSyncFrames(new ChunkStream("b", response.writes), MESSAGE_SYNC_RESPONSE, 9n),
    ).rejects.toThrow(/exchange ID/);

    const reordered: readonly Uint8Array[] = encodeSyncChunks(
      MESSAGE_SYNC_RESPONSE,
      10n,
      Array.from(
        { length: MAX_MEMBERS },
        (_, index): MembershipUpdate =>
          alive(`member-${index}`, 0, new Uint8Array(MAX_METADATA_BYTES)),
      ),
    );
    expect(reordered.length).toBeGreaterThan(1);
    const framedChunks: Uint8Array[] = reordered.map((chunk): Uint8Array => {
      const bytes: Uint8Array = new Uint8Array(4 + chunk.length);
      new DataView(bytes.buffer).setUint32(0, chunk.length);
      bytes.set(chunk, 4);
      return bytes;
    });
    await expect(
      readSyncFrames(
        new ChunkStream("b", [framedChunks[1] as Uint8Array]),
        MESSAGE_SYNC_RESPONSE,
        10n,
      ),
    ).rejects.toThrow(/reordered/);
    await expect(
      readSyncFrames(
        new ChunkStream("b", [
          Uint8Array.from([...framedChunks.flatMap((bytes): number[] => Array.from(bytes)), 1]),
        ]),
        MESSAGE_SYNC_RESPONSE,
        10n,
      ),
    ).rejects.toThrow(/trailing bytes/);
  });

  it("rejects updates that cannot be canonically encoded", async () => {
    const huge: MembershipUpdate[] = Array.from(
      { length: MAX_MEMBERS },
      (_, index): MembershipUpdate =>
        alive(`member-${index.toString().padStart(4, "0")}`, 0, new Uint8Array(1_000)),
    );
    await expect(
      writeSyncFrames(new ChunkStream("b", []), MESSAGE_SYNC_REQUEST, 1n, huge),
    ).rejects.toThrow(/metadata length/);
  });

  it("times out connect and exchange, closes late streams, and preserves carrier failures", async () => {
    const network: SimNetwork = new SimNetwork(20);
    let resolveLate: ((stream: MembershipStream) => void) | undefined;
    const late: ChunkStream = new ChunkStream("late", []);
    let closes: number = 0;
    late.close = (): void => {
      closes += 1;
    };
    const transport: MembershipTransport = {
      address: "a",
      start: async (): Promise<void> => undefined,
      stop: async (): Promise<void> => undefined,
      packet: async (): Promise<void> => undefined,
      stream: (): Promise<MembershipStream> =>
        new Promise((resolve): void => {
          resolveLate = resolve;
        }),
    };
    const configured: SyncOptions = options(network, transport, membership("a"));
    const connecting: Promise<SyncExchange> = syncWith(configured, "late");
    network.clock.advanceBy(1_000);
    await expect(connecting).rejects.toMatchObject({ phase: "connect", timeoutMs: 1_000 });
    resolveLate?.(late);
    await flush();
    expect(closes).toBe(1);

    let rejectLate: ((error: Error) => void) | undefined;
    transport.stream = (): Promise<MembershipStream> =>
      new Promise((_resolve, reject): void => {
        rejectLate = reject;
      });
    const lateFailure: Promise<SyncExchange> = syncWith(configured, "bad");
    network.clock.advanceBy(1_000);
    await expect(lateFailure).rejects.toBeInstanceOf(SyncTimeoutError);
    rejectLate?.(new Error("dial failed"));
    await flush();

    const hanging: ChunkStream = new ChunkStream("b", []);
    let resolveExchange: ((bytes: Uint8Array | undefined) => void) | undefined;
    hanging.read = (): Promise<Uint8Array | undefined> =>
      new Promise<Uint8Array | undefined>((resolve): void => {
        resolveExchange = resolve;
      });
    transport.stream = async (): Promise<MembershipStream> => hanging;
    const deterministic: SyncOptions = {
      ...configured,
      random: {
        next: (): number => 0,
        integer: (): number => 0,
        shuffle: <T>(items: readonly T[]): T[] => [...items],
        pick: <T>(items: readonly T[]): T => items[0] as T,
      },
    };
    const exchanging: Promise<SyncExchange> = syncWith(deterministic, "b");
    await flush();
    network.clock.advanceBy(5_000);
    await expect(exchanging).rejects.toBeInstanceOf(SyncTimeoutError);
    const response: ChunkStream = new ChunkStream("b", []);
    await writeSyncFrames(response, MESSAGE_SYNC_RESPONSE, 1n, []);
    resolveExchange?.(response.writes[0]);
    await flush();
  });

  it("rejects oversized merges and exhausted self-refutation without partial mutation", () => {
    const network: SimNetwork = new SimNetwork(21);
    const base: MembershipView = membership("a");
    const configured: SyncOptions = options(network, network.endpoint("a"), base);
    const additions: MembershipUpdate[] = Array.from(
      { length: MAX_MEMBERS },
      (_, index): MembershipUpdate => alive(`peer-${index}`),
    );
    expect((): readonly MembershipUpdate[] => mergeSyncTable(configured, additions)).toThrow(
      /1024 retained/,
    );
    expect(base.size).toBe(1);

    base.applyLocal(alive("a", 0xffff_ffff), 0);
    expect((): readonly MembershipUpdate[] =>
      mergeSyncTable(configured, [accusation("a", STATE_DEAD, 0xffff_ffff)]),
    ).toThrow(/exhaust/);
  });

  it("covers filtered, ignored, zero-incarnation suspect, and applied callbacks", () => {
    const network: SimNetwork = new SimNetwork(22);
    const view: MembershipView = membership("a", [alive("same", 2)]);
    const cancelled: string[] = [];
    const applied: string[] = [];
    const configured: SyncOptions = {
      ...options(network, network.endpoint("a"), view),
      acceptUpdate: (item): boolean => item.member !== "filtered",
      callbacks: {
        applied: (item): void => {
          applied.push(item.member);
          cancelSupersededSuspicion(item, (member, incarnation): void => {
            cancelled.push(`${member}:${incarnation}`);
          });
        },
      },
    };
    const result: readonly MembershipUpdate[] = mergeSyncTable(configured, [
      alive("filtered"),
      alive("same", 1),
      accusation("zero", STATE_SUSPECT, 0),
      accusation("one", STATE_SUSPECT, 1),
    ]);
    expect(result.map((item): string => item.member)).toEqual(["zero", "one"]);
    expect(applied).toEqual(["zero", "one"]);
    expect(cancelled).toEqual(["one:0"]);

    // A merge with no callbacks configured still applies and disseminates.
    const bare: SyncOptions = {
      view,
      broadcasts: new BroadcastQueue(),
      clock: network.clock,
      random: network.random,
      transport: network.endpoint("bare"),
    };
    mergeSyncTable(bare, [alive("plain", 1)]);
    expect(view.get("plain")?.state).toBe(STATE_ALIVE);
  });

  it("handles responder timeout after a late request without writing a response", async () => {
    const network: SimNetwork = new SimNetwork(23);
    let resolveRead: ((bytes: Uint8Array | undefined) => void) | undefined;
    let writes: number = 0;
    let closes: number = 0;
    const stream: MembershipStream = {
      remoteAddress: "b",
      read: (): Promise<Uint8Array | undefined> =>
        new Promise((resolve): void => {
          resolveRead = resolve;
        }),
      write: async (): Promise<void> => {
        writes += 1;
      },
      close: (): void => {
        closes += 1;
      },
    };
    const configured: SyncOptions = options(network, network.endpoint("a"), membership("a"));
    const responding: Promise<void> = respondToSync(configured, stream);
    network.clock.advanceBy(5_000);
    await expect(responding).rejects.toBeInstanceOf(SyncTimeoutError);
    const request: ChunkStream = new ChunkStream("a", []);
    await writeSyncFrames(request, MESSAGE_SYNC_REQUEST, 1n, []);
    resolveRead?.(request.writes[0]);
    await flush();
    expect(writes).toBe(0);
    expect(closes).toBeGreaterThanOrEqual(1);
  });

  it("covers anti-entropy idempotence, empty peers, both settlements, and stale timers", async () => {
    class AdversarialClock implements Clock {
      readonly callbacks: Array<() => void> = [];
      now(): number {
        return 0;
      }
      epochMilliseconds(): number {
        return 0;
      }
      schedule(_delay: number, callback: () => void): ClockTimer {
        this.callbacks.push(callback);
        return { cancelled: false };
      }
      cancel(): void {}
    }
    const clock: AdversarialClock = new AdversarialClock();
    let rejectExchange: boolean = false;
    const transport: MembershipTransport = {
      address: "a",
      start: async (): Promise<void> => undefined,
      stop: async (): Promise<void> => undefined,
      packet: async (): Promise<void> => undefined,
      stream: async (): Promise<MembershipStream> => {
        if (rejectExchange) {
          throw new Error("exchange failed");
        }
        const response: ChunkStream = new ChunkStream("b", []);
        await writeSyncFrames(response, MESSAGE_SYNC_RESPONSE, 1n, []);
        return new ChunkStream("b", response.writes);
      },
    };
    const emptyScheduler: AntiEntropy = new AntiEntropy({
      view: membership("a"),
      broadcasts: new BroadcastQueue(),
      clock,
      random: new SeededRandom(1),
      transport,
    });
    expect(emptyScheduler.active).toBe(false);
    emptyScheduler.start();
    emptyScheduler.start();
    clock.callbacks[0]?.();
    emptyScheduler.stop();
    emptyScheduler.stop();
    clock.callbacks[0]?.();

    const missingTimerClock: Clock = {
      now: (): number => 0,
      epochMilliseconds: (): number => 0,
      schedule: (): ClockTimer => undefined as unknown as ClockTimer,
      cancel: (): void => undefined,
    };
    const missingTimer: AntiEntropy = new AntiEntropy({
      view: membership("a"),
      broadcasts: new BroadcastQueue(),
      clock: missingTimerClock,
      random: new SeededRandom(1),
      transport,
    });
    missingTimer.start();
    missingTimer.stop();

    const scheduler: AntiEntropy = new AntiEntropy({
      view: membership("a", [alive("b")]),
      broadcasts: new BroadcastQueue(),
      clock,
      random: {
        next: (): number => 0,
        integer: (): number => 0,
        shuffle: <T>(items: readonly T[]): T[] => [...items],
        pick: <T>(items: readonly T[]): T => items[0] as T,
      },
      transport,
    });
    scheduler.start();
    clock.callbacks.at(-1)?.();
    for (let turn = 0; turn < 100; turn += 1) {
      await Promise.resolve();
    }
    expect(scheduler.active).toBe(false);
    rejectExchange = true;
    clock.callbacks.at(-1)?.();
    await flush();
    expect(scheduler.active).toBe(false);
    scheduler.stop();
  });

  it("orders leave targets by UTF-8 bytes and swallows per-target fanout failures", async () => {
    const view: MembershipView = membership("a", [alive("é"), alive("z"), alive("x"), alive("xy")]);
    view.applyLocal({ ...alive("a"), state: STATE_LEFT, metadata: empty }, 0);
    const update: MembershipUpdate = { ...alive("a"), state: STATE_LEFT, metadata: empty };
    const sent: string[] = [];
    const transport: MembershipTransport = {
      address: "a",
      start: async (): Promise<void> => undefined,
      stop: async (): Promise<void> => undefined,
      packet: (target): Promise<void> => {
        sent.push(target);
        if (target === "z") {
          throw new Error("synchronous best effort");
        }

        if (target === "x") {
          return Promise.reject(new Error("asynchronous best effort"));
        }

        return Promise.resolve();
      },
      stream: async (): Promise<MembershipStream> => {
        throw new Error("unused");
      },
    };

    const targets: readonly string[] = leaveTargets(view);
    expect(targets).toEqual(["x", "xy", "z", "é"]);
    await expect(fanoutLeave(transport, update, targets)).resolves.toBeUndefined();
    expect(sent).toEqual(targets);
  });
});
