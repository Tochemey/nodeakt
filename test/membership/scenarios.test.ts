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
import { SeededRandom } from "../../src/membership/random";
import { Swim } from "../../src/membership/swim";
import type { MemberRecord } from "../../src/membership/view";
import { STATE_ALIVE, STATE_DEAD, STATE_LEFT, STATE_SUSPECT } from "../../src/membership/wire";
import { flush, SimNetwork, settle } from "./sim";

async function advance(network: SimNetwork, milliseconds: number): Promise<void> {
  network.clock.advanceBy(milliseconds);
  await flush();
  network.clock.advanceBy(0);
  await flush();
}

function node(network: SimNetwork, address: string, randomSeed: number): Swim {
  return new Swim({
    address,
    metadata: Uint8Array.of(randomSeed & 0xff),
    transport: network.endpoint(address),
    clock: network.clock,
    random: new SeededRandom(randomSeed),
  });
}

async function started(network: SimNetwork, names: readonly string[]): Promise<Map<string, Swim>> {
  const nodes: Map<string, Swim> = new Map<string, Swim>();
  for (const [index, name] of names.entries()) {
    nodes.set(name, node(network, name, network.seed + index + 1));
  }
  await Promise.all(Array.from(nodes.values(), (member: Swim): Promise<void> => member.start()));
  return nodes;
}

async function joinAll(
  network: SimNetwork,
  nodes: Map<string, Swim>,
  seed: string = "a",
): Promise<void> {
  for (const [name, member] of nodes) {
    if (name !== seed) {
      await settle(network, member.join([seed]));
    }
  }
  await advance(network, 3_000);
}

async function stopAll(nodes: Iterable<Swim>): Promise<void> {
  await Promise.all(Array.from(nodes, (member: Swim): Promise<void> => member.stop()));
}

async function scenario(seed: number, run: (network: SimNetwork) => Promise<void>): Promise<void> {
  const network: SimNetwork = new SimNetwork(seed);
  try {
    await run(network);
  } catch (error) {
    throw new Error(
      `${error instanceof Error ? error.message : String(error)}\n${network.seedReport}`,
      {
        cause: error,
      },
    );
  }
}

describe("deterministic SWIM scenarios", () => {
  it("disseminates joins cluster-wide", async () => {
    await scenario(201, async (network): Promise<void> => {
      const nodes: Map<string, Swim> = await started(network, ["a", "b", "c"]);
      await joinAll(network, nodes);
      for (const member of nodes.values()) {
        expect(member.members()).toHaveLength(3);
      }
      await stopAll(nodes.values());
    });
  });

  it("declares a killed member dead within the suspicion budget", async () => {
    await scenario(202, async (network): Promise<void> => {
      const nodes: Map<string, Swim> = await started(network, ["a", "b"]);
      await joinAll(network, nodes);
      const a: Swim = nodes.get("a") as Swim;
      await (nodes.get("b") as Swim).stop();
      await advance(network, 26_000);
      expect(a.members().find((member): boolean => member.member === "b")?.state).toBe(STATE_DEAD);
      await a.stop();
    });
  });

  it("reconverges after a partition heals", async () => {
    await scenario(203, async (network): Promise<void> => {
      const nodes: Map<string, Swim> = await started(network, ["a", "b", "c"]);
      await joinAll(network, nodes);
      for (const peer of ["b", "c"]) {
        network.partitionBoth("a", peer);
      }
      await advance(network, 26_000);
      for (const peer of ["b", "c"]) {
        network.partitionBoth("a", peer, false);
      }
      await advance(network, 5_000);
      const incarnations: Map<string, number> = new Map(
        Array.from(nodes, ([name, member]): readonly [string, number] => {
          const self: MemberRecord | undefined = member
            .members()
            .find((record): boolean => record.member === name);
          return [name, self?.incarnation as number];
        }),
      );
      for (const member of nodes.values()) {
        const records: readonly MemberRecord[] = member.members();
        expect(records).toHaveLength(nodes.size);
        expect(records.every((record): boolean => record.state === STATE_ALIVE)).toBe(true);
        for (const record of records) {
          expect(record.incarnation).toBe(incarnations.get(record.member));
        }
      }
      await stopAll(nodes.values());
    });
  });

  it("lets a paused then resumed member refute suspicion and survive", async () => {
    await scenario(204, async (network): Promise<void> => {
      const nodes: Map<string, Swim> = await started(network, ["a", "b"]);
      await joinAll(network, nodes);
      network.partitionBoth("a", "b");
      await advance(network, 5_000);
      expect(
        (nodes.get("a") as Swim).members().find((member): boolean => member.member === "b")?.state,
      ).toBe(STATE_SUSPECT);
      network.partitionBoth("a", "b", false);
      await advance(network, 5_000);
      for (const member of nodes.values()) {
        expect(member.members().every((record): boolean => record.state === STATE_ALIVE)).toBe(
          true,
        );
      }
      await stopAll(nodes.values());
    });
  });

  it("restarts above an equal-incarnation obituary", async () => {
    await scenario(205, async (network): Promise<void> => {
      const nodes: Map<string, Swim> = await started(network, ["a", "b"]);
      await joinAll(network, nodes);
      const a: Swim = nodes.get("a") as Swim;
      await (nodes.get("b") as Swim).stop();
      await advance(network, 26_000);
      expect(a.members().find((member): boolean => member.member === "b")?.state).toBe(STATE_DEAD);

      const restarted: Swim = node(network, "b", 999);
      await restarted.start();
      await settle(network, restarted.join(["a"]));
      await advance(network, 2_000);
      const revived: MemberRecord | undefined = a
        .members()
        .find((member): boolean => member.member === "b");
      expect(revived).toMatchObject({ state: STATE_ALIVE, incarnation: 1 });
      await Promise.all([a.stop(), restarted.stop()]);
    });
  });

  it("keeps a lone accuser at the maximum suspicion timeout", async () => {
    await scenario(206, async (network): Promise<void> => {
      const nodes: Map<string, Swim> = await started(network, ["a", "b"]);
      await joinAll(network, nodes);
      const a: Swim = nodes.get("a") as Swim;
      await (nodes.get("b") as Swim).stop();
      await advance(network, 2_000);
      const suspected: MemberRecord | undefined = a
        .members()
        .find((member): boolean => member.member === "b");
      expect(suspected?.state).toBe(STATE_SUSPECT);
      const deadline: number = (suspected?.appliedAt as number) + 24_000;
      await advance(network, deadline - network.clock.now() - 1);
      expect(a.members().find((member): boolean => member.member === "b")?.state).toBe(
        STATE_SUSPECT,
      );
      await advance(network, 1);
      expect(a.members().find((member): boolean => member.member === "b")?.state).toBe(STATE_DEAD);
      await a.stop();
    });
  });

  it("disseminates graceful leave before stopping", async () => {
    await scenario(207, async (network): Promise<void> => {
      const nodes: Map<string, Swim> = await started(network, ["a", "b", "c"]);
      await joinAll(network, nodes);
      const leaving: Promise<void> = (nodes.get("b") as Swim).leave();
      await settle(network, leaving);
      await advance(network, 1_000);
      for (const name of ["a", "c"]) {
        expect(
          (nodes.get(name) as Swim).members().find((member): boolean => member.member === "b")
            ?.state,
        ).toBe(STATE_LEFT);
      }
      await Promise.all([(nodes.get("a") as Swim).stop(), (nodes.get("c") as Swim).stop()]);
    });
  });

  it("does not declare members dead during tolerated loss and pauses", async () => {
    await scenario(208, async (network): Promise<void> => {
      const nodes: Map<string, Swim> = await started(network, ["a", "b", "c"]);
      await joinAll(network, nodes);

      network.partitionBoth("a", "b");
      await advance(network, 5_000);

      for (const member of nodes.values()) {
        expect(member.members().every((record): boolean => record.state !== STATE_DEAD)).toBe(true);
      }

      network.partitionBoth("a", "b", false);
      await advance(network, 5_000);

      for (const member of nodes.values()) {
        expect(member.members().every((record): boolean => record.state === STATE_ALIVE)).toBe(
          true,
        );
      }

      await stopAll(nodes.values());
    });
  });

  it("holds join, failure, restart, and leave invariants across a seed campaign", async () => {
    for (let seed = 1; seed <= 32; seed += 1) {
      await scenario(seed, async (network): Promise<void> => {
        const nodes: Map<string, Swim> = await started(network, ["a", "b"]);
        await joinAll(network, nodes);
        for (const member of nodes.values()) {
          expect(member.members()).toHaveLength(2);
        }

        await (nodes.get("b") as Swim).stop();
        await advance(network, 26_000);
        const a: Swim = nodes.get("a") as Swim;
        expect(a.members().find((member): boolean => member.member === "b")?.state).toBe(
          STATE_DEAD,
        );

        const restarted: Swim = node(network, "b", (seed ^ 0xa5a5_5a5a) >>> 0);
        nodes.set("b", restarted);
        await restarted.start();
        await settle(network, restarted.join(["a"]));
        await advance(network, 3_000);
        for (const member of nodes.values()) {
          expect(member.members().find((record): boolean => record.member === "b")).toMatchObject({
            state: STATE_ALIVE,
            incarnation: 1,
          });
        }

        await settle(network, restarted.leave());
        await advance(network, 1_000);
        expect(a.members().find((member): boolean => member.member === "b")?.state).toBe(
          STATE_LEFT,
        );
        await a.stop();
      });
    }
  });
});
