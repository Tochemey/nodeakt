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
import type { Backoff } from "../../src/discovery/backoff";
import {
  type BootstrapClock,
  type BootstrapResult,
  bootstrap,
  systemBootstrapClock,
} from "../../src/discovery/bootstrap";
import type { DiscoveryProvider } from "../../src/discovery/provider";

/** Virtual clock whose `now` advances by exactly the amount each `delay` is asked to wait. */
class ScriptedClock implements BootstrapClock {
  #now: number = 0;

  now(): number {
    return this.#now;
  }

  delay(milliseconds: number): Promise<void> {
    this.#now += milliseconds;
    return Promise.resolve();
  }
}

/** A fixed-delay policy, so a scripted clock advances by a predictable amount each wait. */
const fixedBackoff: Backoff = {
  nextDelay: (): number => 1_000,
};

/** A provider that returns each scripted result in turn, then repeats the last. */
function scriptedProvider(scripts: readonly (readonly string[])[]): DiscoveryProvider {
  let index: number = 0;
  return {
    resolve(): Promise<readonly string[]> {
      const slot: number = Math.min(index, scripts.length - 1);
      index += 1;
      return Promise.resolve(scripts[slot] as readonly string[]);
    },
  };
}

describe("bootstrap join outcomes", () => {
  it("joins on the first attempt with only a provider and join supplied", async () => {
    const provider: DiscoveryProvider = scriptedProvider([["a:1"]]);
    let joinCalls: number = 0;
    const join: () => Promise<boolean> = (): Promise<boolean> => {
      joinCalls += 1;
      return Promise.resolve(true);
    };

    const result: BootstrapResult = await bootstrap({ provider, join });
    expect(result).toEqual({ seeds: ["a:1"], joined: true });
    expect(joinCalls).toBe(1);
  });

  it("waits for seeds to appear before joining", async () => {
    const clock: ScriptedClock = new ScriptedClock();
    const provider: DiscoveryProvider = scriptedProvider([[], [], ["a:1"]]);
    const join: () => Promise<boolean> = (): Promise<boolean> => Promise.resolve(true);

    const result: BootstrapResult = await bootstrap({
      provider,
      join,
      clock,
      backoff: fixedBackoff,
      bootDeadlineMs: 10_000,
    });
    expect(result).toEqual({ seeds: ["a:1"], joined: true });
    expect(clock.now()).toBe(2_000);
  });

  it("anchors a fresh cluster when no seed ever appears", async () => {
    const provider: DiscoveryProvider = scriptedProvider([[]]);
    let joinCalls: number = 0;
    const join: () => Promise<boolean> = (): Promise<boolean> => {
      joinCalls += 1;
      return Promise.resolve(true);
    };

    const result: BootstrapResult = await bootstrap({
      provider,
      join,
      clock: new ScriptedClock(),
      backoff: fixedBackoff,
      bootDeadlineMs: 2_500,
    });
    expect(result).toEqual({ seeds: [], joined: false });
    expect(joinCalls).toBe(0);
  });

  it("clamps the final wait to the deadline and then anchors a fresh cluster", async () => {
    const clock: ScriptedClock = new ScriptedClock();
    const provider: DiscoveryProvider = scriptedProvider([["a:1"]]);
    const join: () => Promise<boolean> = (): Promise<boolean> => Promise.resolve(false);

    const result: BootstrapResult = await bootstrap({
      provider,
      join,
      clock,
      backoff: fixedBackoff,
      bootDeadlineMs: 2_500,
    });
    expect(result).toEqual({ seeds: ["a:1"], joined: false });
    expect(clock.now()).toBe(2_500);
  });

  it("retries the join until a peer answers", async () => {
    let joinCalls: number = 0;
    const provider: DiscoveryProvider = scriptedProvider([["a:1"]]);
    const join: () => Promise<boolean> = (): Promise<boolean> => {
      joinCalls += 1;
      return Promise.resolve(joinCalls >= 2);
    };

    const result: BootstrapResult = await bootstrap({
      provider,
      join,
      clock: new ScriptedClock(),
      backoff: fixedBackoff,
      bootDeadlineMs: 10_000,
    });
    expect(result.joined).toBe(true);
    expect(joinCalls).toBe(2);
  });
});

describe("bootstrap re-resolution between join attempts", () => {
  it("adopts newly registered seeds on a later attempt", async () => {
    let joinCalls: number = 0;
    let lastSeeds: readonly string[] = [];
    const provider: DiscoveryProvider = scriptedProvider([["a:1"], ["b:2"]]);
    const join: (seeds: readonly string[]) => Promise<boolean> = (
      seeds: readonly string[],
    ): Promise<boolean> => {
      lastSeeds = seeds;
      joinCalls += 1;
      return Promise.resolve(joinCalls >= 2);
    };

    const result: BootstrapResult = await bootstrap({
      provider,
      join,
      clock: new ScriptedClock(),
      backoff: fixedBackoff,
      bootDeadlineMs: 10_000,
    });
    expect(result.seeds).toEqual(["b:2"]);
    expect(lastSeeds).toEqual(["b:2"]);
  });

  it("keeps the prior seeds when a re-resolve comes back empty", async () => {
    let joinCalls: number = 0;
    let lastSeeds: readonly string[] = [];
    const provider: DiscoveryProvider = scriptedProvider([["a:1"], []]);
    const join: (seeds: readonly string[]) => Promise<boolean> = (
      seeds: readonly string[],
    ): Promise<boolean> => {
      lastSeeds = seeds;
      joinCalls += 1;
      return Promise.resolve(joinCalls >= 2);
    };

    const result: BootstrapResult = await bootstrap({
      provider,
      join,
      clock: new ScriptedClock(),
      backoff: fixedBackoff,
      bootDeadlineMs: 10_000,
    });
    expect(result.seeds).toEqual(["a:1"]);
    expect(lastSeeds).toEqual(["a:1"]);
  });
});

describe("bootstrap validation", () => {
  it("rejects a negative or non-integer boot deadline", async () => {
    const provider: DiscoveryProvider = scriptedProvider([["a:1"]]);
    const join: () => Promise<boolean> = (): Promise<boolean> => Promise.resolve(true);
    await expect(bootstrap({ provider, join, bootDeadlineMs: -1 })).rejects.toThrow(RangeError);
    await expect(bootstrap({ provider, join, bootDeadlineMs: 1.5 })).rejects.toThrow(RangeError);
  });
});

describe("systemBootstrapClock", () => {
  it("reads a numeric time and resolves a delay", async () => {
    expect(typeof systemBootstrapClock.now()).toBe("number");
    await systemBootstrapClock.delay(0);
  });
});
