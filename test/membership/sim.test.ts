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

import { describe, expect, it, type Mock, vi } from "vitest";
import type { ClockTimer } from "../../src/membership/clock";
import type {
  MembershipStream,
  MembershipTransport,
  TransportHandlers,
} from "../../src/membership/transport";
import { SimClock, SimNetwork } from "./sim";

function handlers(
  packet: TransportHandlers["packet"] = (): void => {},
  stream: TransportHandlers["stream"] = (): void => {},
): TransportHandlers {
  return { packet, stream };
}

async function startedPair(seed: number = 1): Promise<{
  readonly network: SimNetwork;
  readonly first: MembershipTransport;
  readonly second: MembershipTransport;
}> {
  const network: SimNetwork = new SimNetwork(seed);
  const first: MembershipTransport = network.endpoint("first");
  const second: MembershipTransport = network.endpoint("second");
  await first.start(handlers());
  await second.start(handlers());
  return { network, first, second };
}

describe("scripted simulation clock", () => {
  it("advances monotonically and preserves insertion order at one deadline", () => {
    const clock: SimClock = new SimClock();
    const calls: string[] = [];
    clock.schedule(10, (): void => {
      calls.push("first");
    });
    clock.schedule(5, (): void => {
      calls.push("early");
    });
    clock.schedule(10, (): void => {
      calls.push("second");
    });

    clock.advanceBy(9);
    expect(calls).toEqual(["early"]);
    expect(clock.now()).toBe(9);
    clock.advanceTo(10);
    expect(calls).toEqual(["early", "first", "second"]);
    expect(clock.pending).toBe(0);
  });

  it("processes same-deadline input before protocol timers", () => {
    const clock: SimClock = new SimClock();
    const calls: string[] = [];
    clock.schedule(10, (): void => {
      calls.push("timer");
    });
    clock.scheduleInput(10, (): void => {
      calls.push("input");
    });

    clock.advanceTo(10);
    expect(calls).toEqual(["input", "timer"]);
  });

  it("supports idempotent cancellation and callbacks scheduled by callbacks", () => {
    const clock: SimClock = new SimClock();
    const calls: number[] = [];
    const cancelled: ClockTimer = clock.schedule(1, (): void => {
      calls.push(-1);
    });
    clock.cancel(cancelled);
    clock.cancel(cancelled);
    clock.schedule(2, (): void => {
      calls.push(1);
      clock.schedule(0, (): void => {
        calls.push(2);
      });
    });

    expect(clock.runNext()).toBe(true);
    expect(calls).toEqual([1, 2]);
    expect(clock.runNext()).toBe(false);
  });

  it("validates movement, scheduling, and runaway limits", () => {
    const clock: SimClock = new SimClock();
    expect((): void => clock.advanceBy(-1)).toThrow(RangeError);
    expect((): void => clock.advanceTo(-1)).toThrow(RangeError);
    expect((): void => {
      clock.schedule(Number.NaN, vi.fn());
    }).toThrow(RangeError);
    clock.schedule(0, function repeat(): void {
      clock.schedule(0, repeat);
    });
    expect((): void => clock.runAll(3)).toThrow("simulation exceeded 3 scheduled deadlines");
  });
});

describe("simulated packet links", () => {
  it("copies bytes and applies directed delay and duplication", async () => {
    const { network, first, second } = await startedPair();
    const received: Uint8Array[] = [];
    await second.stop();
    const receiver: MembershipTransport = network.endpoint("second");
    await receiver.start(
      handlers((_from: string, bytes: Uint8Array): void => {
        received.push(bytes);
      }),
    );
    network.setLink("first", "second", { delayMs: 5, duplicates: 1 });
    const bytes: Uint8Array = Uint8Array.of(1, 2, 3);

    await first.packet("second", bytes);
    bytes.fill(9);
    network.clock.advanceBy(4);
    expect(received).toEqual([]);
    network.clock.advanceBy(1);
    expect(received).toEqual([Uint8Array.of(1, 2, 3), Uint8Array.of(1, 2, 3)]);
    expect(received[0]).not.toBe(received[1]);
  });

  it("consumes deterministic scripts and reports the reproducing seed", async () => {
    const { network, first, second } = await startedPair(0x5eed);
    const received: number[] = [];
    await second.stop();
    const receiver: MembershipTransport = network.endpoint("second");
    await receiver.start(
      handlers((_from: string, bytes: Uint8Array): void => {
        received.push(bytes[0] as number);
      }),
    );
    network.scriptLink("first", "second", [{ drop: true }, { delayMs: 2 }, { duplicates: 2 }]);

    await first.packet("second", Uint8Array.of(1));
    await first.packet("second", Uint8Array.of(2));
    await first.packet("second", Uint8Array.of(3));
    network.clock.advanceBy(1);
    expect(received).toEqual([3, 3, 3]);
    network.clock.advanceBy(1);
    expect(received).toEqual([3, 3, 3, 2]);
    expect(network.seed).toBe(0x5eed);
    expect(network.seedReport).toBe("simulation seed: 24301");
  });

  it("partitions each direction independently and can heal both", async () => {
    const { network, first, second } = await startedPair();
    const firstReceived: Mock<TransportHandlers["packet"]> = vi.fn();
    const secondReceived: Mock<TransportHandlers["packet"]> = vi.fn();
    await first.stop();
    await second.stop();
    const firstRevived: MembershipTransport = network.endpoint("first");
    const secondRevived: MembershipTransport = network.endpoint("second");
    await firstRevived.start(handlers(firstReceived));
    await secondRevived.start(handlers(secondReceived));
    network.partition("first", "second");

    // A partitioned send fails only after the simulated connect backstop,
    // never with synchronous knowledge of the partition.
    const partitioned: Promise<unknown> = firstRevived
      .packet("second", Uint8Array.of(1))
      .catch((error: unknown): unknown => error);
    await secondRevived.packet("first", Uint8Array.of(2));
    network.clock.advanceBy(0);
    expect(secondReceived).not.toHaveBeenCalled();
    expect(firstReceived).toHaveBeenCalledOnce();
    network.clock.advanceBy(2_000);
    expect(await partitioned).toMatchObject({
      message: expect.stringContaining("was not delivered"),
    });

    network.partitionBoth("first", "second", false);
    await firstRevived.packet("second", Uint8Array.of(3));
    network.clock.advanceBy(0);
    expect(secondReceived).toHaveBeenCalledOnce();
  });

  it("enforces endpoint lifecycle and drops queued input after stop", async () => {
    const { network, first, second } = await startedPair();
    const received: Mock<TransportHandlers["packet"]> = vi.fn();
    await second.stop();
    const receiver: MembershipTransport = network.endpoint("second");
    await receiver.start(handlers(received));
    network.setLink("first", "second", { delayMs: 10 });
    await first.packet("second", Uint8Array.of(1));
    await receiver.stop();
    network.clock.advanceBy(10);
    expect(received).not.toHaveBeenCalled();
    await expect(first.packet("second", Uint8Array.of(2))).rejects.toThrow("stopped or missing");
    await expect(receiver.packet("first", Uint8Array.of(2))).rejects.toThrow("stopped");
    await first.stop();
    // Stop is terminal; a revived node constructs a fresh endpoint instead.
    await expect(first.start(handlers())).rejects.toThrow("cannot restart");
    const revived: MembershipTransport = network.endpoint("first");
    await expect(revived.start(handlers())).resolves.toBeUndefined();
    await expect(revived.start(handlers())).rejects.toThrow("already started");
  });
});

describe("simulated addressed streams", () => {
  it("opens addressed peers and copies ordered writes through link faults", async () => {
    const { network, first, second } = await startedPair();
    let inbound: MembershipStream | undefined;
    let inboundFrom: string = "";
    await second.stop();
    const receiver: MembershipTransport = network.endpoint("second");
    await receiver.start(
      handlers(undefined, (from: string, stream: MembershipStream): void => {
        inboundFrom = from;
        inbound = stream;
      }),
    );

    const opening: Promise<MembershipStream> = first.stream("second");
    network.clock.advanceBy(0);
    const outgoing: MembershipStream = await opening;
    expect(inboundFrom).toBe("first");
    expect(outgoing.remoteAddress).toBe("second");
    expect(inbound?.remoteAddress).toBe("first");

    network.setLink("first", "second", { delayMs: 5, duplicates: 1 });
    const bytes: Uint8Array = Uint8Array.of(4, 5);
    await outgoing.write(bytes);
    bytes.fill(9);
    network.clock.advanceBy(5);
    expect(inbound).toBeDefined();
    const target: MembershipStream = inbound as MembershipStream;
    const firstCopy: Uint8Array | undefined = await target.read();
    const secondCopy: Uint8Array | undefined = await target.read();
    expect(firstCopy).toEqual(Uint8Array.of(4, 5));
    expect(secondCopy).toEqual(Uint8Array.of(4, 5));
    expect(firstCopy).not.toBe(secondCopy);

    outgoing.close();
    outgoing.close();
    network.clock.advanceBy(0);
    await expect(target.read()).resolves.toBeUndefined();
    await expect(outgoing.write(Uint8Array.of(1))).rejects.toThrow("closed");
  });

  it("keeps stream writes ordered when later delays are shorter", async () => {
    const { network, first, second } = await startedPair();
    let inbound: MembershipStream | undefined;
    await second.stop();
    const receiver: MembershipTransport = network.endpoint("second");
    await receiver.start(
      handlers(undefined, (_from: string, stream: MembershipStream): void => {
        inbound = stream;
      }),
    );
    const opening: Promise<MembershipStream> = first.stream("second");
    network.clock.advanceBy(0);
    const outgoing: MembershipStream = await opening;
    network.scriptLink("first", "second", [{ delayMs: 10 }, { delayMs: 1 }]);

    await outgoing.write(Uint8Array.of(1));
    await outgoing.write(Uint8Array.of(2));
    network.clock.advanceBy(9);
    expect(network.clock.pending).toBe(2);
    network.clock.advanceBy(1);
    const target: MembershipStream = inbound as MembershipStream;
    await expect(target.read()).resolves.toEqual(Uint8Array.of(1));
    await expect(target.read()).resolves.toEqual(Uint8Array.of(2));
  });

  it("rejects partitioned connects and ends reads when an endpoint stops", async () => {
    const { network, first, second } = await startedPair();
    network.partition("first", "second");
    // The connect failure surfaces only after the simulated dial backstop.
    const partitioned: Promise<unknown> = first
      .stream("second")
      .catch((error: unknown): unknown => error);
    network.clock.advanceBy(2_000);
    expect(await partitioned).toMatchObject({
      message: expect.stringContaining("was not delivered"),
    });
    network.partition("first", "second", false);

    let inbound: MembershipStream | undefined;
    await second.stop();
    const receiver: MembershipTransport = network.endpoint("second");
    await receiver.start(
      handlers(undefined, (_from: string, stream: MembershipStream): void => {
        inbound = stream;
      }),
    );
    const opening: Promise<MembershipStream> = first.stream("second");
    network.clock.advanceBy(0);
    await opening;
    await receiver.stop();
    await expect((inbound as MembershipStream).read()).resolves.toBeUndefined();
  });
});
