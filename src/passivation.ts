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
 * Passivation stops idle actors to reclaim their resources. An actor's
 * strategy decides when it is passivated; pick one per actor at spawn
 * time. Passivation is a graceful stop: pending messages are processed,
 * `postStop` runs, and the actor is removed from the system. It is
 * strictly opt-in: an actor spawned without a strategy lives until it is
 * explicitly stopped.
 */

/** Suggested idle timeout for actors that opt into time-based
 * passivation: `new TimeBasedStrategy(DefaultPassivationTimeout)`. */
export const DefaultPassivationTimeout = 120_000;

/**
 * TimeBasedStrategy passivates an actor once it has been idle, meaning it
 * has processed no message, for the given duration.
 */
export class TimeBasedStrategy {
  /** The idle duration in milliseconds after which the actor passivates. */
  readonly timeout: number;

  /** @throws RangeError when `timeout` is not a positive number. */
  constructor(timeout: number) {
    if (!Number.isFinite(timeout) || timeout <= 0) {
      throw new RangeError(
        `passivation timeout must be a positive number of milliseconds, got ${timeout}`,
      );
    }

    this.timeout = timeout;
  }
}

/**
 * MessagesCountBasedStrategy passivates an actor after it has processed
 * the given number of messages.
 */
export class MessagesCountBasedStrategy {
  /** The number of processed messages after which the actor passivates. */
  readonly maxMessages: number;

  /** @throws RangeError when `maxMessages` is not a positive integer. */
  constructor(maxMessages: number) {
    if (!Number.isInteger(maxMessages) || maxMessages <= 0) {
      throw new RangeError(
        `passivation message count must be a positive integer, got ${maxMessages}`,
      );
    }

    this.maxMessages = maxMessages;
  }
}

/**
 * LongLivedStrategy opts an actor out of passivation entirely: it runs
 * until explicitly stopped.
 */
export class LongLivedStrategy {}

/** The strategies an actor can be passivated with. */
export type PassivationStrategy =
  | TimeBasedStrategy
  | MessagesCountBasedStrategy
  | LongLivedStrategy;

/**
 * The strategy assigned to actors spawned without an explicit one: long
 * lived, never passivated. Silently stopping an actor after an idle
 * window is surprising as a default, so passivation must be chosen
 * deliberately per actor.
 *
 * @internal
 */
export const defaultPassivationStrategy: PassivationStrategy = new LongLivedStrategy();

/**
 * Discriminants for a {@link SerializedPassivation}, the one place each is
 * spelled so every construction and comparison names the constant instead of a
 * bare string.
 *
 * @internal
 */
export const PASSIVATION_TIME_BASED: "time" = "time";
export const PASSIVATION_COUNT_BASED: "count" = "count";
export const PASSIVATION_LONG_LIVED: "longLived" = "longLived";

/**
 * A {@link PassivationStrategy} reduced to plain, structured-cloneable data: a
 * kind and its numbers. A strategy is a live class instance whose prototype does
 * not survive a hop between isolates, so a placed or relocated actor carries its
 * strategy as this record and rebuilds the instance on the node it lands on.
 *
 * @internal
 */
export type SerializedPassivation =
  | { readonly kind: typeof PASSIVATION_TIME_BASED; readonly timeout: number }
  | { readonly kind: typeof PASSIVATION_COUNT_BASED; readonly maxMessages: number }
  | { readonly kind: typeof PASSIVATION_LONG_LIVED };

/**
 * Reduces a strategy to its plain {@link SerializedPassivation} data form so it
 * can cross an isolate boundary or rest in a registry record.
 *
 * @internal
 */
export function serializePassivation(strategy: PassivationStrategy): SerializedPassivation {
  if (strategy instanceof TimeBasedStrategy) {
    return { kind: PASSIVATION_TIME_BASED, timeout: strategy.timeout };
  }

  if (strategy instanceof MessagesCountBasedStrategy) {
    return { kind: PASSIVATION_COUNT_BASED, maxMessages: strategy.maxMessages };
  }

  return { kind: PASSIVATION_LONG_LIVED };
}

/**
 * Rebuilds a strategy instance from its {@link SerializedPassivation} data form,
 * the inverse of {@link serializePassivation}. The numbers were validated when
 * the original strategy was constructed, so the rebuilt instance re-validates
 * them and matches.
 *
 * @internal
 */
export function deserializePassivation(data: SerializedPassivation): PassivationStrategy {
  switch (data.kind) {
    case PASSIVATION_TIME_BASED:
      return new TimeBasedStrategy(data.timeout);

    case PASSIVATION_COUNT_BASED:
      return new MessagesCountBasedStrategy(data.maxMessages);

    case PASSIVATION_LONG_LIVED:
      return new LongLivedStrategy();

    default: {
      // The union is closed, so this is unreachable from a value produced by
      // serializePassivation; guarding it keeps a corrupt record from silently
      // returning undefined in place of a strategy.
      const kind: string = (data as { readonly kind: string }).kind;
      throw new TypeError(`unknown passivation strategy kind "${kind}"`);
    }
  }
}
