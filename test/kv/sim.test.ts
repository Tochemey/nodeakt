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
import type { ClusterMember, KvTransport } from "../../src/kv/ports";
import {
  flush,
  member,
  SeededRandom,
  SimClock,
  SimCluster,
  SimFabric,
  type SimTimer,
  settle,
} from "./sim";

function bytes(...values: number[]): Uint8Array {
  return new Uint8Array(values);
}

/** Installs a handler that answers a request with the byte after its first. */
function echoIncrement(transport: KvTransport): void {
  transport.listen(
    async (_from: string, body: Uint8Array): Promise<Uint8Array> => bytes((body[0] as number) + 1),
  );
}

describe("SimClock", () => {
  it("fires callbacks in deadline order and tracks time", () => {
    const clock: SimClock = new SimClock();
    const fired: number[] = [];
    clock.schedule(30, (): void => {
      fired.push(30);
    });
    clock.schedule(10, (): void => {
      fired.push(10);
    });
    clock.schedule(20, (): void => {
      fired.push(20);
    });
    clock.advanceBy(30);
    expect(fired).toEqual([10, 20, 30]);
    expect(clock.now()).toBe(30);
  });

  it("orders a network delivery ahead of a protocol timer at an equal deadline", () => {
    const clock: SimClock = new SimClock();
    const fired: string[] = [];
    clock.schedule(10, (): void => {
      fired.push("protocol");
    });
    clock.scheduleInput(10, (): void => {
      fired.push("input");
    });
    clock.advanceBy(10);
    expect(fired).toEqual(["input", "protocol"]);
  });

  it("breaks a final tie by scheduling order", () => {
    const clock: SimClock = new SimClock();
    const fired: number[] = [];
    clock.schedule(5, (): void => {
      fired.push(1);
    });
    clock.schedule(5, (): void => {
      fired.push(2);
    });
    clock.advanceBy(5);
    expect(fired).toEqual([1, 2]);
  });

  it("does not fire a cancelled callback and drops it from pending", () => {
    const clock: SimClock = new SimClock();
    let fired: boolean = false;
    const timer: SimTimer = clock.schedule(10, (): void => {
      fired = true;
    });
    expect(clock.pending).toBe(1);
    clock.cancel(timer);
    expect(clock.pending).toBe(0);
    clock.cancel(timer);
    clock.advanceBy(20);
    expect(fired).toBe(false);
  });

  it("runNext advances to the earliest timer and reports emptiness", () => {
    const clock: SimClock = new SimClock();
    const fired: number[] = [];
    clock.schedule(15, (): void => {
      fired.push(15);
    });
    clock.schedule(5, (): void => {
      fired.push(5);
    });
    expect(clock.runNext()).toBe(true);
    expect(clock.now()).toBe(5);
    expect(fired).toEqual([5]);
    expect(clock.runNext()).toBe(true);
    expect(fired).toEqual([5, 15]);
    expect(clock.runNext()).toBe(false);
  });

  it("runAll drains every timer, including ones a callback schedules", () => {
    const clock: SimClock = new SimClock();
    let count: number = 0;
    const reschedule = (): void => {
      count += 1;
      if (count < 3) {
        clock.schedule(1, reschedule);
      }
    };
    clock.schedule(1, reschedule);
    clock.runAll();
    expect(count).toBe(3);
    expect(clock.pending).toBe(0);
  });

  it("runAll throws when a callback loop exceeds the limit", () => {
    const clock: SimClock = new SimClock();
    const loop = (): SimTimer => clock.schedule(1, loop);
    clock.schedule(1, loop);
    expect((): void => clock.runAll(5)).toThrow(/exceeded/);
  });

  it("rejects invalid schedules, advances, and limits", () => {
    const clock: SimClock = new SimClock();
    const noop = (): void => undefined;
    expect((): SimTimer => clock.schedule(-1, noop)).toThrow(RangeError);
    expect((): SimTimer => clock.schedule(Number.NaN, noop)).toThrow(RangeError);
    expect((): void => clock.advanceBy(-1)).toThrow(RangeError);
    expect((): void => clock.advanceTo(-1)).toThrow(RangeError);
    clock.advanceBy(10);
    expect((): void => clock.advanceTo(5)).toThrow(/monotonic/);
    expect((): void => clock.runAll(0)).toThrow(RangeError);
  });
});

describe("SeededRandom", () => {
  it("replays the same sequence for the same seed", () => {
    const first: SeededRandom = new SeededRandom(12_345);
    const second: SeededRandom = new SeededRandom(12_345);
    const sequenceA: number[] = [first.next(), first.next(), first.next()];
    const sequenceB: number[] = [second.next(), second.next(), second.next()];
    expect(sequenceA).toEqual(sequenceB);
  });

  it("produces fractions in [0,1) and integers in range", () => {
    const random: SeededRandom = new SeededRandom(9);
    for (let draw: number = 0; draw < 100; draw += 1) {
      const fraction: number = random.next();
      expect(fraction).toBeGreaterThanOrEqual(0);
      expect(fraction).toBeLessThan(1);
      const index: number = random.integer(7);
      expect(index).toBeGreaterThanOrEqual(0);
      expect(index).toBeLessThan(7);
      expect(Number.isInteger(index)).toBe(true);
    }
  });

  it("maps the zero seed to a working nonzero state", () => {
    const random: SeededRandom = new SeededRandom(0);
    expect(random.seed).toBe(0);
    expect(random.next()).toBeGreaterThan(0);
  });

  it("rejects an out-of-range seed and bound", () => {
    expect((): SeededRandom => new SeededRandom(-1)).toThrow(RangeError);
    expect((): SeededRandom => new SeededRandom(2 ** 32)).toThrow(RangeError);
    expect((): SeededRandom => new SeededRandom(1.5)).toThrow(RangeError);
    const random: SeededRandom = new SeededRandom(3);
    expect((): number => random.integer(0)).toThrow(RangeError);
    expect((): number => random.integer(2 ** 32 + 1)).toThrow(RangeError);
    expect((): number => random.integer(1.5)).toThrow(RangeError);
  });
});

describe("SimFabric transport", () => {
  it("carries a request to a handler and its response back", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    echoIncrement(b);
    const response: Uint8Array = await settle(fabric, a.request("B", bytes(41), 5_000));
    expect(response).toEqual(bytes(42));
  });

  it("delays a request and its response by each direction's latency", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    echoIncrement(b);
    fabric.setLink("A", "B", { delayMs: 100 });
    fabric.setLink("B", "A", { delayMs: 50 });
    const response: Uint8Array = await settle(fabric, a.request("B", bytes(1), 5_000));
    expect(response).toEqual(bytes(2));
    expect(fabric.clock.now()).toBe(150);
  });

  it("times out when the request is dropped", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    echoIncrement(b);
    fabric.setLink("A", "B", { drop: true });
    await expect(settle(fabric, a.request("B", bytes(1), 2_000))).rejects.toThrow(/timed out/);
    expect(fabric.clock.now()).toBe(2_000);
  });

  it("times out across a partition and recovers when it heals", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    echoIncrement(b);
    fabric.partitionBoth("A", "B");
    await expect(settle(fabric, a.request("B", bytes(1), 1_000))).rejects.toThrow(/timed out/);
    fabric.partitionBoth("A", "B", false);
    expect(await settle(fabric, a.request("B", bytes(1), 5_000))).toEqual(bytes(2));
  });

  it("delivers a duplicated request more than once but resolves the caller once", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    let calls: number = 0;
    b.listen(async (): Promise<Uint8Array> => {
      calls += 1;
      return bytes(calls);
    });
    fabric.setLink("A", "B", { duplicates: 2 });
    const response: Uint8Array = await settle(fabric, a.request("B", bytes(0), 5_000));
    expect(calls).toBe(3);
    expect(response).toEqual(bytes(1));
  });

  it("applies a scripted one-shot fault, then the persistent setting", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    echoIncrement(b);
    fabric.scriptLink("A", "B", [{ drop: true }]);
    await expect(settle(fabric, a.request("B", bytes(1), 1_000))).rejects.toThrow(/timed out/);
    expect(await settle(fabric, a.request("B", bytes(1), 5_000))).toEqual(bytes(2));
  });

  it("times out when the handler ran but its response was lost", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    let handled: boolean = false;
    b.listen(async (_from: string, body: Uint8Array): Promise<Uint8Array> => {
      handled = true;
      return body;
    });
    fabric.setLink("B", "A", { drop: true });
    await expect(settle(fabric, a.request("B", bytes(1), 1_000))).rejects.toThrow(/timed out/);
    expect(handled).toBe(true);
  });

  it("times out when the handler rejects", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    b.listen(async (): Promise<Uint8Array> => {
      throw new Error("handler failure");
    });
    await expect(settle(fabric, a.request("B", bytes(1), 1_000))).rejects.toThrow(/timed out/);
  });

  it("times out against a destination with no handler and an unknown destination", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    fabric.transport("B");
    await expect(settle(fabric, a.request("B", bytes(1), 1_000))).rejects.toThrow(/timed out/);
    await expect(settle(fabric, a.request("Z", bytes(1), 1_000))).rejects.toThrow(/timed out/);
  });

  it("rejects a request from a closed endpoint and times out one aimed at it", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    echoIncrement(b);
    await b.close();
    await expect(b.request("A", bytes(1), 1_000)).rejects.toThrow(/closed/);
    await expect(settle(fabric, a.request("B", bytes(1), 1_000))).rejects.toThrow(/timed out/);
  });

  it("revives a closed endpoint under the same name with a fresh transport", async () => {
    const fabric: SimFabric = new SimFabric(1);
    const a: KvTransport = fabric.transport("A");
    const first: KvTransport = fabric.transport("B");
    await first.close();
    const second: KvTransport = fabric.transport("B");
    expect(second).not.toBe(first);
    echoIncrement(second);
    expect(await settle(fabric, a.request("B", bytes(7), 5_000))).toEqual(bytes(8));
  });

  it("rejects an empty endpoint name and reports its seed", () => {
    const fabric: SimFabric = new SimFabric(7);
    expect((): KvTransport => fabric.transport("")).toThrow(RangeError);
    expect(fabric.seedReport).toBe("simulation seed: 7");
    expect(fabric.seed).toBe(7);
  });
});

describe("SimCluster", () => {
  it("orders members oldest first with a name tiebreak", () => {
    const view: SimCluster = new SimCluster("A", [
      member("C", 30),
      member("A", 10),
      member("B", 10),
    ]);
    expect(view.members().map((entry: ClusterMember): string => entry.name)).toEqual([
      "A",
      "B",
      "C",
    ]);
    expect(view.self).toBe("A");
  });

  it("notifies subscribers on set and stops after unsubscribe", () => {
    const view: SimCluster = new SimCluster("A", [member("A", 10)]);
    const seen: string[][] = [];
    const off: () => void = view.onChange((members: readonly ClusterMember[]): void => {
      seen.push(members.map((entry: ClusterMember): string => entry.name));
    });
    view.set([member("A", 10), member("B", 20)]);
    off();
    view.set([member("A", 10)]);
    expect(seen).toEqual([["A", "B"]]);
    expect(view.members().map((entry: ClusterMember): string => entry.name)).toEqual(["A"]);
  });

  it("member defaults to ready and not draining, and honours overrides", () => {
    expect(member("A", 1)).toEqual({ name: "A", startedAt: 1, ready: true, draining: false });
    expect(member("B", 2, { ready: false, draining: true })).toEqual({
      name: "B",
      startedAt: 2,
      ready: false,
      draining: true,
    });
  });
});

describe("simulation drivers", () => {
  it("flush drains a chain of microtasks", async () => {
    let stage: number = 0;
    void Promise.resolve()
      .then((): void => {
        stage = 1;
      })
      .then((): void => {
        stage = 2;
      });
    await flush();
    expect(stage).toBe(2);
  });

  it("settle returns the resolved value of a round-trip", async () => {
    const fabric: SimFabric = new SimFabric(2);
    const a: KvTransport = fabric.transport("A");
    const b: KvTransport = fabric.transport("B");
    echoIncrement(b);
    const value: Uint8Array = await settle(fabric, a.request("B", bytes(10), 5_000));
    expect(value).toEqual(bytes(11));
  });
});
