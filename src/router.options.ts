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
 * RoutingStrategy selects how a router distributes the messages it
 * forwards over its routees.
 */
export type RoutingStrategy = "roundRobin" | "random" | "fanOut" | "consistentHash";

/** Forwards each message to the next routee in rotation, so a steady
 * stream spreads evenly over the pool. */
export const RoundRobinRouting: RoutingStrategy = "roundRobin";

/** Forwards each message to a routee picked at random. */
export const RandomRouting: RoutingStrategy = "random";

/**
 * Forwards each message to every live routee: a broadcast. A fan-out
 * router cannot answer an ask, because a broadcast has no single
 * answer; asking one rejects with the `ErrFanOutAsk` sentinel.
 */
export const FanOutRouting: RoutingStrategy = "fanOut";

/**
 * Forwards each message to the routee owning its routing key on a
 * consistent-hash ring, so equal keys always land on the same routee.
 * Requires a {@link RoutingKeyFunc} in {@link RouterOptions.routingKey}.
 */
export const ConsistentHashRouting: RoutingStrategy = "consistentHash";

/**
 * RoutingKeyFunc extracts the routing key of a message for a
 * consistent-hash router. Messages with equal keys are always forwarded
 * to the same routee. A thrown error routes the message to dead letters
 * instead of failing the router.
 */
export type RoutingKeyFunc = (message: unknown) => string | number;

/** Options configuring a router being spawned. */
export interface RouterOptions {
  /** The routing strategy; {@link RoundRobinRouting} when omitted. */
  strategy?: RoutingStrategy;

  /**
   * The routing key extractor of a consistent-hash router. Required
   * when the strategy is {@link ConsistentHashRouting}; ignored
   * otherwise.
   */
  routingKey?: RoutingKeyFunc;
}
