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

import { performance } from "node:perf_hooks";
import { type Backoff, ExponentialBackoff } from "./backoff";
import type { DiscoveryProvider } from "./provider";

/**
 * The boot-only bootstrap: how a node uses discovery once to enter the cluster.
 *
 * The sequence leans entirely on the membership engine's push-pull join to
 * converge, and touches discovery only here:
 *
 * 1. Ask the provider for seeds. While the list is empty, an environment that is
 *    still coming up, wait and ask again, until a seed appears or the boot
 *    deadline elapses.
 * 2. Join the seeds. A join merges two member tables, so it both attaches to an
 *    existing cluster and fuses two nodes that each believed they were alone. On
 *    a failure, a seed whose listener is not up yet, wait and retry, re-resolving
 *    each round so a newly registered seed is picked up, until a peer is reached
 *    or the deadline elapses.
 * 3. A node that reaches no peer by the deadline anchors a fresh cluster that
 *    later joiners merge into. Consistent seeds across nodes plus retry-until-
 *    reached is what makes a simultaneous cold start converge to one cluster.
 *
 * Everything after this runs through membership gossip, never through discovery.
 *
 * This runs inside the framework, not in application code: the only thing a node
 * supplies is a {@link DiscoveryProvider}. The wait policy, the clock, the boot
 * deadline, and the join wiring all default to production-grade values here, so
 * the whole configuration surface an operator sees is choosing a provider. The
 * waits between attempts follow a jittered {@link Backoff} so a synchronized cold
 * start does not become a thundering herd, and time is taken from an injectable
 * {@link BootstrapClock} so both retry loops are unit-testable without real DNS
 * or sockets; neither is something a caller needs to pass.
 */

/** Budget, in milliseconds, to reach a peer before the node anchors a fresh cluster. */
export const DISCOVERY_BOOT_DEADLINE_MS: number = 10_000;

/** Time and delay the bootstrap needs, injectable so tests drive it on a scripted clock. */
export interface BootstrapClock {
  /** Monotonic milliseconds used only for the boot-deadline comparison. */
  now(): number;
  /** Resolves after at least `milliseconds`, the wait between retries. */
  delay(milliseconds: number): Promise<void>;
}

/** How this node entered the cluster, the outcome of {@link bootstrap}. */
export interface BootstrapResult {
  /** The seeds finally used, after any resolve retries; empty when none ever appeared. */
  readonly seeds: readonly string[];
  /** True when a seed peer was reached and joined; false when the node anchored a fresh cluster. */
  readonly joined: boolean;
}

/** The dependencies and tuning of one {@link bootstrap} run. */
export interface BootstrapOptions {
  /** The provider consulted for seeds, the one piece a node must supply. */
  readonly provider: DiscoveryProvider;
  /**
   * Attempts to join the given seeds, resolving true when a peer was reached.
   * Must resolve false for an unreachable seed rather than reject, so the
   * bootstrap can retry; a rejection aborts the whole boot. Supplied by the
   * clustering layer, which wires it to the membership engine's join.
   */
  readonly join: (seeds: readonly string[]) => Promise<boolean>;
  /** The wait policy between attempts; defaults to a jittered {@link ExponentialBackoff}. */
  readonly backoff?: Backoff;
  /** The clock the retry loops and the deadline read; defaults to {@link systemBootstrapClock}. */
  readonly clock?: BootstrapClock;
  /** Override for the boot deadline; defaults to {@link DISCOVERY_BOOT_DEADLINE_MS}. */
  readonly bootDeadlineMs?: number;
}

/** The real clock used in production: monotonic `performance.now()` and a `setTimeout` delay. */
export const systemBootstrapClock: BootstrapClock = {
  now(): number {
    return Math.trunc(performance.now());
  },
  delay(milliseconds: number): Promise<void> {
    return new Promise<void>((resolve: () => void): void => {
      setTimeout(resolve, milliseconds);
    });
  },
};

/**
 * Runs the boot-only discovery sequence and reports how the node entered the cluster.
 *
 * @param options The provider and the join callback; the backoff, clock, and
 * deadline all default, so `{ provider, join }` is a complete call.
 * @returns The seeds used and whether a peer was joined; a false `joined` means
 * the node anchored a fresh cluster after the deadline.
 */
export async function bootstrap(options: BootstrapOptions): Promise<BootstrapResult> {
  const { provider, join }: BootstrapOptions = options;
  const backoff: Backoff = options.backoff ?? new ExponentialBackoff();
  const clock: BootstrapClock = options.clock ?? systemBootstrapClock;
  const bootDeadlineMs: number = options.bootDeadlineMs ?? DISCOVERY_BOOT_DEADLINE_MS;
  if (!Number.isSafeInteger(bootDeadlineMs) || bootDeadlineMs < 0) {
    throw new RangeError("boot deadline must be a non-negative integer of milliseconds");
  }

  const deadline: number = clock.now() + bootDeadlineMs;
  const waitBeforeRetry: () => Promise<void> = (): Promise<void> => {
    const remaining: number = deadline - clock.now();
    return clock.delay(Math.min(backoff.nextDelay(), remaining));
  };

  let seeds: readonly string[] = await provider.resolve();
  while (seeds.length === 0 && clock.now() < deadline) {
    await waitBeforeRetry();
    seeds = await provider.resolve();
  }

  if (seeds.length === 0) {
    return { seeds, joined: false };
  }

  let reached: boolean = await join(seeds);
  while (!reached && clock.now() < deadline) {
    await waitBeforeRetry();
    const refreshed: readonly string[] = await provider.resolve();
    if (refreshed.length > 0) {
      seeds = refreshed;
    }

    reached = await join(seeds);
  }

  return { seeds, joined: reached };
}
