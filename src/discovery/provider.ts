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
 * Node discovery: how a fresh node finds the cluster it should join.
 *
 * Discovery has exactly one job, and it happens once. At boot a node asks a
 * {@link DiscoveryProvider} for the seed contact points to join, hands them to
 * the membership engine, and is done. It is never consulted again. Every
 * topology change after that, a node joining, leaving, failing, or changing its
 * metadata, flows through membership gossip and reaches the clustering layer as
 * a membership event, not through discovery. Discovery finds the door;
 * membership runs the house. Solving topology is the entire reason the
 * membership engine exists, so a provider must never be used to poll for it.
 *
 * The provider is the pluggable seam an operator implements for their
 * environment. Two implementations ship in the zero-dependency core:
 * {@link StaticDiscovery} over a fixed host list, and `DnsDiscovery` over an
 * `SRV` or `A` lookup. A Kubernetes API query, a NATS or Consul registry, or a
 * cloud instance lookup is a user-implemented provider that pulls whatever
 * client it needs; the core never takes on that dependency.
 */

/**
 * Resolves the seed contact points a node attempts to join at boot.
 *
 * A seed is a `host:port` string naming another node's membership listener. The
 * provider is asked once at startup and never again; topology after boot is the
 * membership engine's concern, not the provider's.
 */
export interface DiscoveryProvider {
  /**
   * The seeds this node should try to join at startup, as `host:port` strings.
   *
   * May resolve to an empty list while the environment is still coming up, for
   * example before any pod of a headless service has registered. The boot
   * sequence treats an empty result as "not ready yet" and retries, so a
   * provider should return empty rather than reject when the source is simply
   * not populated. Reject only on a genuine, unexpected failure to query the
   * source at all.
   *
   * Consulted only during boot, never for topology once the node has joined.
   */
  resolve(): Promise<readonly string[]>;
}
