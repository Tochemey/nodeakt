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

import type { DiscoveryProvider } from "./discovery/provider";

/**
 * Enables clustering on an actor system: the node joins a cluster of peers and
 * its registry is distributed across them.
 *
 * A clustered node must be reachable for actor messages, so `remote` is required
 * alongside this option; constructing a system with `cluster` set and no `remote`
 * is a typed construction error. The node's actor remoting endpoint, its `remote`
 * host and port, is its actor identity and is advertised to the cluster
 * automatically. The gossip and data endpoints bind that same `remote` host, so
 * one host names the whole node and this option never carries a host of its own.
 * It configures only how the node finds the cluster and the two endpoints' ports,
 * and every field but {@link discovery} defaults, so a minimal clustered node
 * sets only that.
 */
export interface ClusterOptions {
  /**
   * How this node finds seed peers at boot, the one required field. Consulted
   * once at startup and never again; topology after boot flows through membership.
   */
  discovery: DiscoveryProvider;

  /**
   * Membership gossip port, the stable port seeds are dialed at; defaults to a
   * shared well-known port, so every node agrees on it without configuration.
   */
  gossipPort?: number;

  /**
   * Key/value data endpoint port; defaults to an ephemeral port, since peers
   * learn it through gossip rather than dialing a fixed one.
   */
  dataPort?: number;

  /**
   * How long a fresh node tries to reach a seed before anchoring a new cluster;
   * defaults to the discovery bootstrap deadline.
   */
  bootstrapTimeout?: number;

  /** Immutable partition count shared by every node; defaults to the cluster constant. */
  partitionCount?: number;

  /** Intended replica set size including the primary; defaults to the cluster constant. */
  replicaCount?: number;

  /** Acknowledgments a synchronous write awaits; defaults to the cluster constant. */
  writeQuorum?: number;

  /** Minimum member quorum enabling the split-brain resolver; one, the default, disables it. */
  minimumMemberQuorum?: number;
}
