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

import { parseHostPort } from "./clustering.transport";
import type { ClusterMember } from "./kv/ports";
import { reservedNamesPrefix } from "./reserved";

/** The strategies deciding which node a chosen placement lands on, the one place
 * each name is spelled so callers and comparisons name the constant. @internal */
export const STRATEGY_ROUND_ROBIN: "roundRobin" = "roundRobin";
export const STRATEGY_RANDOM: "random" = "random";
export const STRATEGY_LOCAL: "local" = "local";
export const STRATEGY_LEAST_LOAD: "leastLoad" = "leastLoad";

/**
 * How a chosen placement selects its owning node: `roundRobin` spreads placements
 * evenly by a cluster-wide counter, `random` picks a uniformly random member,
 * `local` places on the calling node, and `leastLoad` picks the member owning the
 * fewest actors. A raw address is deliberately not a strategy, so a call site
 * expresses intent, not topology.
 */
export type PlacementStrategy =
  | typeof STRATEGY_ROUND_ROBIN
  | typeof STRATEGY_RANDOM
  | typeof STRATEGY_LOCAL
  | typeof STRATEGY_LEAST_LOAD;

/** The cluster-wide counter key the round-robin strategy advances, under the
 * reserved prefix so it is never counted as a placed actor. */
const ROUND_ROBIN_KEY: string = `${reservedNamesPrefix}RoundRobin`;

/** The registry operations the strategies read, a narrow view of the cluster
 * registry so a selection unit-tests without a whole cluster. @internal */
export interface StrategyRegistry {
  /** Advances the shared round-robin counter and resolves with its new value. */
  nextRoundRobinValue(key: string): Promise<bigint>;
  /** How many actors each host owns, from one cluster scan. */
  countsByHost(): Promise<Map<string, number>>;
}

/** The inputs a placement owner is selected from. @internal */
export interface OwnerSelection {
  /** The strategy deciding the owner. */
  readonly strategy: PlacementStrategy;
  /** The present members as this node sees them, oldest first. */
  readonly members: readonly ClusterMember[];
  /** This node's cluster identity, the owner a `local` placement lands on. */
  readonly self: string;
  /** The registry the counter and load counts are read from. */
  readonly registry: StrategyRegistry;
  /** A source of uniform randomness in `[0, 1)`, injected so `random` is testable. */
  readonly random: () => number;
}

/** The members a distributed placement may land on: present, ready, and not
 * draining, as their cluster identities, oldest first. */
function candidatesOf(members: readonly ClusterMember[]): string[] {
  return members
    .filter((member: ClusterMember): boolean => member.ready && !member.draining)
    .map((member: ClusterMember): string => member.name);
}

/**
 * Selects the node a chosen placement lands on. A `local` placement is always
 * this node; every other strategy chooses among the ready, non-draining members,
 * falling back to this node when no other candidate is ready so a placement never
 * fails for want of a peer.
 *
 * @internal
 */
export async function selectOwner(selection: OwnerSelection): Promise<string> {
  if (selection.strategy === STRATEGY_LOCAL) {
    return selection.self;
  }

  const candidates: string[] = candidatesOf(selection.members);
  if (candidates.length === 0) {
    return selection.self;
  }

  if (selection.strategy === STRATEGY_RANDOM) {
    const index: number = Math.floor(selection.random() * candidates.length);
    return candidates[index] as string;
  }

  if (selection.strategy === STRATEGY_LEAST_LOAD) {
    return leastLoaded(candidates, selection.registry);
  }

  const counter: bigint = await selection.registry.nextRoundRobinValue(ROUND_ROBIN_KEY);
  const index: number = Number((counter - 1n) % BigInt(candidates.length));
  return candidates[index] as string;
}

/** The candidate owning the fewest actors, ties broken by cluster identity so
 * every node computes the same choice. Load is counted per host: one actor system
 * runs per machine, so a member's host carries its whole load and no two candidates
 * share one. */
async function leastLoaded(
  candidates: readonly string[],
  registry: StrategyRegistry,
): Promise<string> {
  const counts: Map<string, number> = await registry.countsByHost();
  let best: string = candidates[0] as string;
  let bestCount: number = counts.get(parseHostPort(best).host) ?? 0;
  for (let i: number = 1; i < candidates.length; i++) {
    const candidate: string = candidates[i] as string;
    const count: number = counts.get(parseHostPort(candidate).host) ?? 0;
    if (count < bestCount || (count === bestCount && candidate < best)) {
      best = candidate;
      bestCount = count;
    }
  }

  return best;
}
