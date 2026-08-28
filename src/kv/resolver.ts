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

/**
 * Split-brain resolution: which half of a partitioned cluster keeps serving.
 *
 * A network partition cuts the cluster in two, and each half converges to a view
 * of only itself, elects its own coordinator, and accepts writes. On heal the two
 * diverged halves merge by last write wins and one half's writes vanish, which for
 * a name registry is a duplicate claim. Recovery cannot fix this, because both
 * halves are internally consistent; they simply disagree. It cannot be solved,
 * only decided: a partitioned system either keeps both halves serving and accepts
 * the fork, or keeps one and stops the other. Resolution is the deliberate choice
 * of consistency, and this is the strategy that makes it.
 *
 * The bundled strategy is {@link KeepMajorityResolver}: a half keeps serving only
 * if it still reaches a strict majority of the last stable cluster size, the
 * member count from before the partition. Both halves compute against the same
 * denominator, so at most one can hold a strict majority and the other stops. An
 * even split has no majority, so the tiebreak keeps the half that still reaches
 * the oldest member of the last stable view, the same oldest-first identity the
 * coordinator uses; every node reaches that verdict from the same facts without
 * communicating. The decision must be made while the departed side is still in the
 * view as suspect or newly dead, so the last stable size is the denominator, not
 * the shrunken post-partition size.
 *
 * The strategy is pure policy over membership facts: it holds no state beyond its
 * configured quorum and drives nothing. The clustering layer computes this half's
 * reachable members and the last stable view, asks the strategy, and on a stop
 * verdict gates the store and steps the node down. A `minimumMemberQuorum` of one,
 * the default, disables the strategy, which is correct for a single node or a
 * development cluster where stopping the only reachable half is worse than a fork
 * a single node cannot produce.
 *
 * @internal
 */

import { DEFAULT_MEMBER_QUORUM } from "./constants";

/**
 * A split-brain strategy: given the members this half still reaches and the last
 * stable view, whether this half should keep serving.
 *
 * {@link KeepMajorityResolver} is the one strategy built in v1; a static
 * reference-node or external-arbiter strategy would satisfy the same contract.
 *
 * @internal
 */
export interface SplitBrainStrategy {
  /**
   * Whether this half keeps serving.
   *
   * @param reachable Members this half currently reaches, by canonical identity.
   * @param lastStable The membership before the partition, oldest first, whose
   * length is the majority denominator and whose first entry is the tiebreak.
   */
  survives(reachable: ReadonlySet<string>, lastStable: readonly string[]): boolean;
}

/**
 * Keep-majority with an oldest-member tiebreak, the bundled split-brain strategy.
 *
 * @internal
 */
export class KeepMajorityResolver implements SplitBrainStrategy {
  /** One disables the strategy; above one enables keep-majority over the stable size. */
  readonly #minimumMemberQuorum: number;

  /**
   * @param minimumMemberQuorum Positive integer; one, the default, disables the
   * strategy so every half survives. Above one enables keep-majority, whose
   * threshold is a strict majority of the last stable size, not this number.
   * @throws {RangeError} If it is not a positive safe integer.
   */
  constructor(minimumMemberQuorum: number = DEFAULT_MEMBER_QUORUM) {
    if (!Number.isSafeInteger(minimumMemberQuorum) || minimumMemberQuorum < 1) {
      throw new RangeError("minimum member quorum must be a positive integer");
    }

    this.#minimumMemberQuorum = minimumMemberQuorum;
  }

  /**
   * Whether this half keeps serving: always when the strategy is disabled or there
   * is no stable baseline yet, otherwise when it reaches a strict majority of the
   * last stable size, breaking an even split toward the half that still reaches
   * the oldest member.
   */
  survives(reachable: ReadonlySet<string>, lastStable: readonly string[]): boolean {
    const stableSize: number = lastStable.length;
    if (this.#minimumMemberQuorum <= 1 || stableSize === 0) {
      return true;
    }

    let held: number = 0;
    for (const name of lastStable) {
      if (reachable.has(name)) {
        held += 1;
      }
    }

    if (held * 2 > stableSize) {
      return true;
    }

    if (held * 2 < stableSize) {
      return false;
    }

    return reachable.has(lastStable[0] as string);
  }
}
