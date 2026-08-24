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
 * Carrier security for one endpoint: resolved PEM material handed to
 * `node:tls` on both sides of every connection. The protocol above is
 * carrier-agnostic, so a TLS connection changes no frame, no
 * handshake, and no negotiated parameter; the configuration below
 * only decides what carries the bytes.
 *
 * One config serves both roles because every endpoint listens and
 * dials: the listener presents `cert` and `key`, the dialer verifies
 * it against `ca` (the runtime's trust store when absent) and, under
 * mutual TLS, presents the same `cert` and `key` back.
 *
 * @internal
 */
export interface TlsConfig {
  /** The PEM certificate (or chain) this endpoint presents. A
   * listener cannot serve without one; a dialer may omit it and stay
   * verify-only, which a listener demanding client certificates then
   * refuses. The owner decides policy; this layer just carries. */
  readonly cert?: string | Buffer;

  /** The PEM private key of {@link cert}. */
  readonly key?: string | Buffer;

  /** The PEM authority bundle peers are verified against; absent, the
   * runtime's default trust store verifies instead. */
  readonly ca?: string | Buffer;

  /** Whether the listener demands and verifies a client certificate on
   * every accepted connection: mutual TLS. */
  readonly requestCert?: boolean;
}
