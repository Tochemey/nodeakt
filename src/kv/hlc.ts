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
 * Hybrid logical clock: physical epoch time paired with a per-node logical
 * counter. It supplies the last-write-wins order for stored records.
 *
 * A plain wall clock loses writes under skew: two nodes whose clocks disagree
 * resolve a conflict by whichever machine happened to read a larger number. The
 * hybrid clock keeps the physical component, so timestamps stay close to real
 * time and expire TTLs correctly, but never lets the order regress: every local
 * event is strictly greater than the previous one, and receiving a remote
 * timestamp advances this clock past it. Wall time that stalls or steps backward
 * is absorbed by bumping the logical counter instead of moving time backward.
 *
 * The node name is the final tiebreak, so two events with identical wall and
 * logical values still order deterministically on every node.
 *
 * @internal
 */

import type { HybridTime } from "./ports";

/**
 * Total order on hybrid timestamps: wall time, then logical counter, then the
 * writing node's name.
 *
 * The node tiebreak makes the order total across nodes, so two writes stamped at
 * the same wall and logical resolve to the same winner everywhere.
 *
 * @returns Negative if `left` precedes `right`, zero if they are equal, and
 * positive if `left` follows `right`.
 * @internal
 */
export function compareHybrid(left: HybridTime, right: HybridTime): number {
  if (left.wallMs !== right.wallMs) {
    return left.wallMs < right.wallMs ? -1 : 1;
  }

  if (left.logical !== right.logical) {
    return left.logical < right.logical ? -1 : 1;
  }

  if (left.node === right.node) {
    return 0;
  }

  return left.node < right.node ? -1 : 1;
}

/**
 * Per-node hybrid logical clock.
 *
 * State is one physical reading and one logical counter. Both {@link now} and
 * {@link update} advance that state and return a detached, immutable snapshot
 * stamped with this node's name. The physical source is injected so a
 * simulation can drive time exactly, including making it stall or step back.
 *
 * @internal
 */
export class HybridClock {
  /** Canonical cluster identity stamped onto every timestamp this clock issues. */
  readonly #node: string;

  /** Injected physical clock in Unix epoch milliseconds. */
  readonly #physicalNow: () => number;

  /** Greatest wall time observed, local or remote; never decreases. */
  #wallMs: number = 0;

  /** Logical counter for events sharing {@link #wallMs}; resets when wall advances. */
  #logical: number = 0;

  /**
   * Creates a clock for one node over an injected physical source.
   *
   * @param node Non-empty canonical identity of this node.
   * @param physicalNow Returns the current Unix epoch time in milliseconds.
   * @throws {RangeError} If `node` is empty.
   */
  constructor(node: string, physicalNow: () => number) {
    if (node.length === 0) {
      throw new RangeError("node name must not be empty");
    }

    this.#node = node;
    this.#physicalNow = physicalNow;
  }

  /**
   * Timestamps a local event and advances the clock.
   *
   * The result is strictly greater than the previous timestamp this clock
   * issued: wall time moves forward when the physical clock has advanced,
   * otherwise the logical counter increments.
   *
   * @returns A detached, immutable timestamp stamped with this node.
   * @throws {RangeError} If the physical clock returns a non-finite value.
   */
  now(): HybridTime {
    const physical: number = this.#readPhysical();
    const wall: number = Math.max(this.#wallMs, physical);
    this.#logical = wall === this.#wallMs ? this.#logical + 1 : 0;
    this.#wallMs = wall;
    return this.#stamp();
  }

  /**
   * Merges a timestamp observed from another node and advances this clock past
   * it, so a local event that follows a received message is ordered after it.
   *
   * @param remote The hybrid timestamp carried by an inbound message.
   * @returns A detached, immutable timestamp stamped with this node.
   * @throws {RangeError} If the physical clock returns a non-finite value.
   */
  update(remote: HybridTime): HybridTime {
    const physical: number = this.#readPhysical();
    const wall: number = Math.max(this.#wallMs, remote.wallMs, physical);
    this.#logical = this.#mergeLogical(wall, remote);
    this.#wallMs = wall;
    return this.#stamp();
  }

  /**
   * Chooses the logical counter for a merge, reading the pre-merge state.
   *
   * Guarded rather than chained: the merged wall may equal the local wall, the
   * remote wall, both, or neither, and each case seeds the counter differently.
   */
  #mergeLogical(wall: number, remote: HybridTime): number {
    const matchesLocal: boolean = wall === this.#wallMs;
    const matchesRemote: boolean = wall === remote.wallMs;
    if (matchesLocal && matchesRemote) {
      return Math.max(this.#logical, remote.logical) + 1;
    }

    if (matchesLocal) {
      return this.#logical + 1;
    }

    if (matchesRemote) {
      return remote.logical + 1;
    }

    return 0;
  }

  /**
   * Reads the injected physical clock, rejecting a value that would poison the
   * clock's state.
   *
   * @throws {RangeError} If the reading is not a finite number.
   */
  #readPhysical(): number {
    const physical: number = this.#physicalNow();
    if (!Number.isFinite(physical)) {
      throw new RangeError("physical clock must return a finite number");
    }

    return physical;
  }

  /** Builds a detached snapshot of the current state. */
  #stamp(): HybridTime {
    return { wallMs: this.#wallMs, logical: this.#logical, node: this.#node };
  }
}
