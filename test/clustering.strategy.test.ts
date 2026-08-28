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
  STRATEGY_LEAST_LOAD,
  STRATEGY_LOCAL,
  STRATEGY_RANDOM,
  STRATEGY_ROUND_ROBIN,
  type StrategyRegistry,
  selectOwner,
} from "../src/clustering.strategy";
import type { ClusterMember } from "../src/kv/ports";

const SELF: string = "10.0.0.1:8080";

/** A present member at `name`, ready and not draining unless overridden. */
function member(name: string, ready: boolean = true, draining: boolean = false): ClusterMember {
  return { name, startedAt: 0, ready, draining };
}

/** A registry with an in-memory round-robin counter and per-host actor counts. */
class FakeRegistry implements StrategyRegistry {
  counter: bigint = 0n;
  readonly counts: Map<string, number> = new Map();

  nextRoundRobinValue(_key: string): Promise<bigint> {
    this.counter += 1n;
    return Promise.resolve(this.counter);
  }

  countsByHost(): Promise<Map<string, number>> {
    return Promise.resolve(this.counts);
  }
}

describe("selectOwner", () => {
  it("places local on this node regardless of the members", async () => {
    const owner: string = await selectOwner({
      strategy: STRATEGY_LOCAL,
      members: [member("10.0.0.2:8080")],
      self: SELF,
      registry: new FakeRegistry(),
      random: (): number => 0,
    });

    expect(owner).toBe(SELF);
  });

  it("round-robins across the ready members by the shared counter", async () => {
    const members: ClusterMember[] = [member("a:1"), member("b:1"), member("c:1")];
    const registry: FakeRegistry = new FakeRegistry();
    const pick = (): Promise<string> =>
      selectOwner({
        strategy: STRATEGY_ROUND_ROBIN,
        members,
        self: SELF,
        registry,
        random: (): number => 0,
      });

    expect(await pick()).toBe("a:1");
    expect(await pick()).toBe("b:1");
    expect(await pick()).toBe("c:1");
    expect(await pick()).toBe("a:1");
  });

  it("random picks the member the injected source lands on", async () => {
    const members: ClusterMember[] = [member("a:1"), member("b:1"), member("c:1")];
    const at = (value: number): Promise<string> =>
      selectOwner({
        strategy: STRATEGY_RANDOM,
        members,
        self: SELF,
        registry: new FakeRegistry(),
        random: (): number => value,
      });

    expect(await at(0)).toBe("a:1");
    expect(await at(0.999)).toBe("c:1");
  });

  it("leastLoad picks the emptiest member, improving on load then on id", async () => {
    const registry: FakeRegistry = new FakeRegistry();
    registry.counts.set("10.0.0.5", 3);
    registry.counts.set("10.0.0.4", 2);
    registry.counts.set("10.0.0.2", 2);
    registry.counts.set("10.0.0.8", 2);
    registry.counts.set("10.0.0.9", 7);

    // The scan improves on a lower load (.4), improves again on the id tiebreak
    // (.2), then holds against a same-load larger id (.8) and a heavier node (.9).
    const owner: string = await selectOwner({
      strategy: STRATEGY_LEAST_LOAD,
      members: [
        member("10.0.0.5:1"),
        member("10.0.0.4:1"),
        member("10.0.0.2:1"),
        member("10.0.0.8:1"),
        member("10.0.0.9:1"),
      ],
      self: SELF,
      registry,
      random: (): number => 0,
    });

    expect(owner).toBe("10.0.0.2:1");
  });

  it("leastLoad reads a member the scan never counted as empty, first or later", async () => {
    const registry: FakeRegistry = new FakeRegistry();
    registry.counts.set("10.0.0.5", 2);

    // The scan holds a count only for .5. The first candidate .3 and the later .7 both
    // read as empty, so the emptiest wins over the loaded .5 and the id tiebreak takes .3.
    const owner: string = await selectOwner({
      strategy: STRATEGY_LEAST_LOAD,
      members: [member("10.0.0.3:1"), member("10.0.0.5:1"), member("10.0.0.7:1")],
      self: SELF,
      registry,
      random: (): number => 0,
    });

    expect(owner).toBe("10.0.0.3:1");
  });

  it("falls back to this node when no member is ready", async () => {
    const owner: string = await selectOwner({
      strategy: STRATEGY_ROUND_ROBIN,
      members: [member("a:1", false), member("b:1", true, true)],
      self: SELF,
      registry: new FakeRegistry(),
      random: (): number => 0,
    });

    expect(owner).toBe(SELF);
  });
});
