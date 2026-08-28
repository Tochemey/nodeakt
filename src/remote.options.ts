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
 * Enables remoting on an actor system: the node binds a listener on the
 * given host and port, its actors' paths advertise that reachable
 * endpoint, and messages addressed to another node travel over the
 * network transport. A system created without this stays single-node
 * and pays nothing for the transport.
 */
export interface RemoteOptions {
  /** The host the node binds its listener on. A concrete address such as
   * `127.0.0.1`, or a wildcard such as `0.0.0.0` to accept on every
   * interface, not a name to resolve. It is also the address the node
   * advertises to peers unless {@link advertisedHost} overrides it, so a
   * node that binds a wildcard must set {@link advertisedHost} to a
   * reachable address, or peers cannot dial it back. */
  host: string;

  /** The host the node advertises to peers as the address to dial it back
   * on, when that differs from the bind {@link host}. Set it when the node
   * binds a wildcard, so peers reach it at a routable address rather than the
   * unroutable bind host. Defaults to {@link host}.
   *
   * On a clustered node it must also be a locally bindable interface, because
   * the cluster's gossip and data endpoints bind it: a true NAT case, where the
   * advertised address is not a local interface, is supported for plain remoting
   * but not yet for clustering. */
  advertisedHost?: string;

  /** The port the node binds. `0` lets the operating system choose a
   * free port, readable afterwards through `ActorSystem.port`. */
  port: number;

  /** Encrypts every connection with TLS. All or nothing per system: a
   * TLS node accepts and dials only TLS, so a mixed pair fails its
   * connection handshake; run every node of a cluster with the same
   * mode. Omitted, traffic is plaintext TCP for private, trusted
   * networks only. */
  tls?: TlsOptions;
}

/**
 * TLS material for a remoting endpoint. One block serves both roles
 * because every node listens and dials: the listener presents `cert`
 * and `key`, and the dialer verifies peers against `ca` and presents
 * the same certificate back when they demand one.
 *
 * Certificates are the operator's concern by design: provide PEM
 * contents or the path of a PEM file, and rotate by restarting the
 * system; nothing here generates or renews anything. The dialer
 * verifies the peer's identity against the host it dialed, so every
 * node's certificate must carry the `host` it advertises in its
 * subject alternative names (a `DNS:` entry for a hostname, an `IP:`
 * entry for an address), or the dial fails identity verification.
 */
export interface TlsOptions {
  /** The node's certificate (or chain): PEM contents, or the path of a
   * PEM file. Its subject alternative names must include the `host`
   * this node advertises, or peers dialing that host reject it. */
  cert: string;

  /** The certificate's private key: PEM contents, or a path. */
  key: string;

  /** The certificate authority peers are verified against: PEM
   * contents, or a path. Without it the runtime's default trust store
   * verifies, which refuses the self-signed material private clusters
   * typically run on. */
  ca?: string;

  /** Demands and verifies a client certificate on every accepted
   * connection: mutual TLS. Peers must present certificates the
   * verifying side trusts. */
  requestCert?: boolean;
}
