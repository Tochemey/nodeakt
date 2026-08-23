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
