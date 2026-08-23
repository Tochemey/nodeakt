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

import type { PID } from "./pid";

/**
 * How many points each member occupies on the ring. More points smooth
 * the key distribution over a small pool; the ring is rebuilt only on
 * membership changes, so the cost stays off the routing path.
 */
const VIRTUAL_POINTS = 16;

/**
 * FNV-1a, 32 bits: a small, allocation-free string hash that is
 * deterministic across runtimes, so a key owns the same ring position
 * wherever it is computed.
 */
function fnv1a(value: string): number {
  let hash: number = 0x811c9dc5;

  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }

  return hash >>> 0;
}

/** One point on the ring during construction, before the sorted arrays
 * are laid out. */
interface RingEntry {
  readonly point: number;
  readonly owner: PID;
}

/**
 * HashRing pins routing keys to pool members with consistent hashing:
 * each member owns {@link VIRTUAL_POINTS} positions derived from its
 * path, and a key belongs to the first position at or after the key's
 * own hash, wrapping around. Positions depend only on member paths, so
 * resizing the pool moves as few keys as possible: keys keep their
 * owner unless that owner left or a new member's position falls
 * between them.
 *
 * The ring is immutable; the router builds a fresh one on every
 * membership change.
 *
 * @internal
 */
export class HashRing {
  /** The ring positions, sorted ascending. */
  private readonly _points: number[];

  /** The member owning the position at the same index. */
  private readonly _owners: PID[];

  constructor(members: readonly PID[]) {
    const entries: RingEntry[] = [];

    for (const member of members) {
      const id: string = member.id();

      for (let v = 0; v < VIRTUAL_POINTS; v++) {
        entries.push({ point: fnv1a(`${id}#${v}`), owner: member });
      }
    }

    // The point order decides ownership; sort is stable and the members
    // arrive in spawn order, so the ring is deterministic across
    // rebuilds even when two points collide.
    entries.sort((a, b) => a.point - b.point);

    this._points = entries.map((entry) => entry.point);
    this._owners = entries.map((entry) => entry.owner);
  }

  /**
   * Returns the live member owning `key`: the owner of the first
   * position at or after the key's hash, walking past members that are
   * not running. Returns `null` when no member is running.
   */
  lookup(key: string): PID | null {
    const points: number[] = this._points;
    const n: number = points.length;
    if (n === 0) {
      return null;
    }

    const hash: number = fnv1a(key);

    // Binary search for the first position at or after the hash; past
    // the last position the ring wraps to index 0.
    let lo: number = 0;
    let hi: number = n;

    while (lo < hi) {
      const mid: number = (lo + hi) >>> 1;

      if ((points[mid] as number) < hash) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    for (let i = 0; i < n; i++) {
      const owner: PID = this._owners[(lo + i) % n] as PID;

      if (owner.isRunning()) {
        return owner;
      }
    }

    return null;
  }
}
