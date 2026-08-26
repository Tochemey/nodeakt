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

import { decodeNodeMetadata, type NodeMetadata } from "./clustering.metadata";
import type { ClusterMember, ClusterView } from "./kv/ports";
import type { MemberRecord } from "./membership/view";
import { STATE_ALIVE, STATE_SUSPECT } from "./membership/wire";

/**
 * The store's {@link ClusterView} over the SWIM membership engine.
 *
 * The store wants the present members oldest first, each named by the data
 * endpoint it can dial and carrying a decoded `startedAt`, `ready`, and
 * `draining`. Membership returns detached {@link MemberRecord} snapshots in map
 * insertion order, including dead and left records, and carries only opaque
 * metadata. This adapter keeps the members that are alive or merely suspect,
 * decodes each one's metadata, names it by the data address that metadata
 * carries rather than its membership identity, and sorts by `(startedAt, name)`,
 * so `members()[0]` is the stable oldest member the coordinator and resolver
 * rely on. It never inspects membership beyond this, and never drives it.
 *
 * A node stays present through a transient suspicion and drops only when
 * membership declares it dead or left. Suspicion is silent in the event stream,
 * and false suspicions are refuted routinely, so excluding a suspect node would
 * make its presence depend on whether an unrelated event happened to fire, and
 * would flap the coordinator whenever the oldest member was briefly suspected.
 * Reacting on confirmed death instead matches how crash recovery and the
 * split-brain resolver already fire.
 *
 * Membership delivers change through a single synchronous callback fixed at
 * construction, so this adapter cannot subscribe on its own. The clustering
 * engine wires that callback to {@link publish}, which re-emits the current
 * snapshot to every {@link onChange} listener.
 */
export class SwimClusterView implements ClusterView {
  /** This node's canonical identity. */
  readonly #self: string;
  /** Reads the current membership snapshot, typically `swim.members()`. */
  readonly #snapshot: () => readonly MemberRecord[];
  /** Registered change listeners. */
  readonly #listeners: Set<(members: readonly ClusterMember[]) => void> = new Set();

  /**
   * @param self This node's own data endpoint, its name among {@link members}.
   * @param snapshot Reads the current detached membership records on demand.
   */
  constructor(self: string, snapshot: () => readonly MemberRecord[]) {
    this.#self = self;
    this.#snapshot = snapshot;
  }

  /** This node's canonical cluster identity. */
  get self(): string {
    return this.#self;
  }

  /** Present members, alive or suspect, oldest `startedAt` first, ties broken by name. */
  members(): readonly ClusterMember[] {
    return toClusterMembers(this.#snapshot());
  }

  /**
   * Subscribes to membership change. The listener receives the same snapshot
   * {@link members} would then return. Returns an unsubscribe function.
   */
  onChange(listener: (members: readonly ClusterMember[]) => void): () => void {
    this.#listeners.add(listener);
    return (): void => {
      this.#listeners.delete(listener);
    };
  }

  /**
   * Re-emits the current membership snapshot to every listener. The clustering
   * engine wires this to the membership engine's change callback, so any join,
   * leave, death, or metadata change reaches the store.
   */
  publish(): void {
    const members: readonly ClusterMember[] = this.members();
    for (const listener of [...this.#listeners]) {
      listener(members);
    }
  }
}

/**
 * Keeps the alive or suspect records that carry a data endpoint, names each by
 * that endpoint, and sorts oldest first.
 *
 * The member's name is its decoded data address, not its membership identity,
 * so the store keys and dials nodes by one identity it can reach. A record with
 * no decoded address is not a key/value participant, or is malformed, and is
 * dropped.
 */
function toClusterMembers(records: readonly MemberRecord[]): readonly ClusterMember[] {
  const members: ClusterMember[] = [];
  for (const record of records) {
    if (record.state !== STATE_ALIVE && record.state !== STATE_SUSPECT) {
      continue;
    }

    const metadata: NodeMetadata = decodeNodeMetadata(record.metadata);
    if (metadata.address.length === 0) {
      continue;
    }

    members.push({
      name: metadata.address,
      startedAt: metadata.startedAt,
      ready: metadata.ready,
      draining: metadata.draining,
    });
  }

  members.sort(compareByAgeThenName);
  return members;
}

/**
 * Orders members oldest `startedAt` first, breaking ties by ascending name.
 *
 * The name tie-break has no equal arm because a member's name is its data
 * address, unique across the cluster, so two distinct members never compare
 * equal here and the order is total.
 */
function compareByAgeThenName(left: ClusterMember, right: ClusterMember): number {
  if (left.startedAt !== right.startedAt) {
    return left.startedAt - right.startedAt;
  }

  return left.name < right.name ? -1 : 1;
}
