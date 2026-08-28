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

import type { DiscoveryProvider } from "./provider";

/**
 * Discovery over a fixed, known list of seed contact points.
 *
 * The simplest provider and the default: an operator who knows their nodes'
 * addresses lists them, and every node uses the same list. A single-node
 * deployment supplies an empty list, so the boot sequence anchors a fresh
 * cluster once its deadline elapses. This is also the provider the transport
 * and clustering tests use, since it needs no network to resolve.
 */
export class StaticDiscovery implements DiscoveryProvider {
  /** The frozen seed list resolved on every call, an independent copy of the input. */
  readonly #seeds: readonly string[];

  /**
   * @param seeds Seed contact points as `host:port` strings. The list may be
   * empty to start a node with no seeds. Each entry must be a non-blank string;
   * surrounding whitespace is trimmed.
   * @throws {TypeError} If any entry is blank once trimmed.
   */
  constructor(seeds: readonly string[]) {
    const cleaned: string[] = [];
    for (const seed of seeds) {
      const trimmed: string = seed.trim();
      if (trimmed.length === 0) {
        throw new TypeError("static discovery seed must be a non-blank host:port string");
      }

      cleaned.push(trimmed);
    }

    this.#seeds = Object.freeze(cleaned);
  }

  /** Resolves to the configured seed list; the returned array is not the internal one. */
  resolve(): Promise<readonly string[]> {
    return Promise.resolve([...this.#seeds]);
  }
}
