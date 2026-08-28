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
 * The coordinator: the single node that authors the routing table and carries
 * it to everyone.
 *
 * There is no election protocol. The coordinator is the oldest live member by
 * start time, ties broken by name, which every node reads off the same
 * {@link ClusterView} as `members()[0]`. On a membership change the coordinator
 * recomputes the table and pushes it to every other member; each member answers
 * with an ownership report of the partitions it still holds, and the next
 * recompute folds those in to prune a demoted owner once its data has drained.
 * A member keeps the higher of two table versions and accepts a table only from
 * the node it currently believes is coordinator, so a deposed coordinator cannot
 * keep publishing and versions never regress across a handover: a new
 * coordinator seeds its counter one above the highest version it has ever held.
 *
 * A node that was partitioned away and resumes as coordinator may hold a table the
 * majority has since advanced past. Its highest-ever-held version would then author
 * a table that forks from, rather than supersedes, the one in force. To prevent that,
 * a member answers a push it does not adopt with the table it holds instead of a
 * report, and the pusher adopts that table and re-authors one version above it, so
 * the cluster reconverges onto a single table rather than two at the same number.
 *
 * This module owns neither a timer nor a membership subscription. `clustering.ts`
 * calls {@link Coordinator.rebalance} on a membership change and on the periodic
 * re-push interval, and routes an inbound table push to {@link
 * Coordinator.receive}. That keeps the coordinator a deterministic function of
 * its inputs, driven entirely from outside, exactly as the store's janitor sweep
 * is a body the integration layer clocks.
 *
 * The whole table is pushed on every change and on the periodic heal. Shipping
 * only the partitions that moved is a deferred optimization: the coordinator
 * runs at control-plane rate over a table of one row per partition, and a diff
 * would add base-version tracking and a full-table fallback that no benchmark
 * has yet called for.
 *
 * A gracefully leaving member advertises `draining` in its membership metadata.
 * The coordinator builds the assignment ring from the live members that are not
 * draining, so a leaver takes no new primary or backup, but it passes the full
 * live set to {@link RoutingTable.evolve}, so the leaver stays a previous owner
 * and keeps answering reads until its fragments have drained and its report shows
 * the partition empty.
 *
 * @internal
 */

import { REQUEST_TIMEOUT_MS } from "./constants";
import { KvProtocolError } from "./errors";
import type { ClusterMember, ClusterView, KvTransport } from "./ports";
import { PartitionRing } from "./ring";
import { type OwnershipReport, RoutingTable } from "./routing.table";
import { decodeMessage, encodeMessage, type KvMessage, MessageKind } from "./wire";

/**
 * Whether two tables assign an identical owners list to every partition. The
 * caller only ever compares tables of equal partition count, which `evolve`
 * guarantees, so the counts are not re-checked here.
 */
function sameOwners(first: RoutingTable, second: RoutingTable): boolean {
  for (let id: number = 0; id < first.partitionCount; id += 1) {
    if (!sameList(first.owners(id), second.owners(id))) {
      return false;
    }
  }

  return true;
}

/** Whether two owner lists are equal in length and order. */
function sameList(first: readonly string[], second: readonly string[]): boolean {
  if (first.length !== second.length) {
    return false;
  }

  for (let index: number = 0; index < first.length; index += 1) {
    if (first[index] !== second[index]) {
      return false;
    }
  }

  return true;
}

/** Decodes a push response as an ownership report, or `undefined` if it is not one. */
function decodeReport(bytes: Uint8Array): OwnershipReport | undefined {
  let message: KvMessage;
  try {
    message = decodeMessage(bytes);
  } catch {
    return undefined;
  }

  if (message.kind !== MessageKind.ownershipReport) {
    return undefined;
  }

  return message.report;
}

/**
 * Decodes a push response as a routing table, or `undefined` if it is not one. A
 * member answers a push with its own table, instead of a report, when the pushed
 * table did not supersede the one it holds, so the coordinator can catch up to it.
 */
function decodeTable(bytes: Uint8Array): RoutingTable | undefined {
  let message: KvMessage;
  try {
    message = decodeMessage(bytes);
  } catch {
    return undefined;
  }

  if (message.kind !== MessageKind.table) {
    return undefined;
  }

  return RoutingTable.fromWire(message.table);
}

/**
 * Whether a member should answer a push with its own `held` table rather than a
 * report. It informs the pusher when the push did not supersede what it holds and
 * the two differ, either a higher version or the same version over different
 * owners, so a pusher that resumed from a stale table can catch up to it.
 */
function informs(held: RoutingTable, pushed: RoutingTable): boolean {
  if (pushed.supersedes(held)) {
    return false;
  }

  if (held.supersedes(pushed)) {
    return true;
  }

  return !sameOwners(held, pushed);
}

/**
 * Authors and distributes the routing table for one node's view of the cluster.
 *
 * @internal
 */
export class Coordinator {
  /** Membership view; `members()` is oldest first, so the coordinator is `members()[0]`. */
  readonly #view: ClusterView;

  /** Carrier for table pushes and their ownership-report responses. */
  readonly #transport: KvTransport;

  /** Immutable positive partition count every table this node authors covers. */
  readonly #partitionCount: number;

  /** This node's own report source: the partitions its local store still holds. */
  readonly #heldPartitions: () => readonly number[];

  /** The table this node currently holds, or `undefined` before the first one. */
  #table: RoutingTable | undefined;

  /** Highest table version this node has authored or accepted, for seeding. */
  #maxVersion: bigint = 0n;

  /** Reports gathered from the last push, folded into the next recompute. */
  #reports: OwnershipReport[] = [];

  /** Serializes rebalances so overlapping calls cannot race the table state. */
  #tail: Promise<void> = Promise.resolve();

  /**
   * @param view Membership view whose `members()` are ordered oldest first.
   * @param transport Carrier for table pushes and their report responses.
   * @param partitionCount Immutable positive partition count for every table.
   * @param heldPartitions This node's own ownership report source: the partition
   * ids its local store still holds entries for.
   * @throws {RangeError} If `partitionCount` is not a positive safe integer.
   */
  constructor(
    view: ClusterView,
    transport: KvTransport,
    partitionCount: number,
    heldPartitions: () => readonly number[],
  ) {
    if (!Number.isSafeInteger(partitionCount) || partitionCount <= 0) {
      throw new RangeError("partition count must be a positive integer");
    }

    this.#view = view;
    this.#transport = transport;
    this.#partitionCount = partitionCount;
    this.#heldPartitions = heldPartitions;
  }

  /** Whether this node is the current coordinator: the oldest live member. */
  isCoordinator(): boolean {
    return this.#coordinatorName() === this.#view.self;
  }

  /** The table this node currently holds, or `undefined` before it has one. */
  currentTable(): RoutingTable | undefined {
    return this.#table;
  }

  /**
   * Recomputes and pushes the table when this node is the coordinator, and is a
   * no-op otherwise. Serialized: a call awaits any rebalance already in flight.
   *
   * An unchanged membership recomputes the same owners and re-pushes the current
   * table to heal a missed push rather than minting a new version.
   */
  rebalance(): Promise<void> {
    const next: Promise<void> = this.#tail.then((): Promise<void> => this.#rebalanceOnce());
    this.#tail = next.catch((): undefined => undefined);
    return next;
  }

  /**
   * Handles an inbound table push and returns this node's ownership report
   * encoded as the response. `clustering.ts` routes a table message here.
   *
   * @throws {KvProtocolError} If `body` is not a decodable routing-table message.
   */
  receive(from: string, body: Uint8Array): Uint8Array {
    const message: KvMessage = decodeMessage(body);
    if (message.kind !== MessageKind.table) {
      throw new KvProtocolError("coordinator received a non-table message");
    }

    const pushed: RoutingTable = RoutingTable.fromWire(message.table);
    const report: OwnershipReport = this.handleTable(from, pushed);
    // When the pushed table did not supersede the one held here, the pusher is
    // behind or has forked a divergent version at the same number. Answer with the
    // held table so a coordinator that resumed from a stale table adopts it and
    // re-authors above it, rather than a report it cannot yet act on.
    const held: RoutingTable | undefined = this.#table;
    if (held !== undefined && informs(held, pushed)) {
      return encodeMessage({ kind: MessageKind.table, table: held.toWire() });
    }

    return encodeMessage({ kind: MessageKind.ownershipReport, report });
  }

  /**
   * Applies a pushed `table` on a follower and returns this node's ownership
   * report. The table is adopted only when `from` is the believed coordinator
   * and its version is newer than the held one; the report is returned either
   * way, so a push from a deposed coordinator still learns what this node holds.
   */
  handleTable(from: string, table: RoutingTable): OwnershipReport {
    const believed: string | undefined = this.#coordinatorName();
    const newer: boolean = this.#table === undefined || table.supersedes(this.#table);
    if (from === believed && newer) {
      // The held version is the highest this node has seen, so adopting a newer
      // table only advances the counter; the assignment never lowers it.
      this.#table = table;
      this.#maxVersion = table.version;
    }

    return this.#localReport();
  }

  /** Recomputes the table for the current members and pushes it, coordinator only. */
  async #rebalanceOnce(): Promise<void> {
    const members: readonly ClusterMember[] = this.#view.members();
    const oldest: ClusterMember | undefined = members[0];
    if (oldest === undefined || oldest.name !== this.#view.self) {
      return;
    }

    const table: RoutingTable = this.#recompute(members);
    const learned: RoutingTable | undefined = await this.#push(table, members);
    if (learned === undefined) {
      return;
    }

    // A member revealed a table this push did not supersede, so this node authored
    // from a stale base: it resumed as coordinator after being partitioned away
    // while the majority advanced the table. Adopt the revealed table and re-author
    // one version above it, so the cluster converges onto a single table instead of
    // forking. A member only reveals a table at least as new as the one pushed, so
    // its version never lowers the counter. A rarer residual divergence is closed by
    // the next periodic re-push.
    this.#table = learned;
    this.#maxVersion = learned.version;
    await this.#push(this.#recompute(members), members);
  }

  /**
   * Folds the new ring and last round's reports into the next table, minting a
   * new version only when the owners actually changed.
   */
  #recompute(members: readonly ClusterMember[]): RoutingTable {
    const live: Set<string> = new Set(members.map((member: ClusterMember): string => member.name));
    const assignable: string[] = members
      .filter((member: ClusterMember): boolean => !member.draining)
      .map((member: ClusterMember): string => member.name);
    // Every member draining at once leaves no node to demote onto, so the ring
    // falls back to the full set rather than emptying and throwing.
    const names: string[] = assignable.length > 0 ? assignable : [...live];
    const ring: PartitionRing = new PartitionRing(names, this.#partitionCount);
    const candidate: RoutingTable = RoutingTable.evolve(
      this.#table,
      ring,
      this.#reports,
      this.#maxVersion + 1n,
      live,
    );
    if (this.#table !== undefined && sameOwners(this.#table, candidate)) {
      return this.#table;
    }

    this.#table = candidate;
    this.#maxVersion = candidate.version;
    return candidate;
  }

  /**
   * Pushes `table` to every other member and gathers their reports for next time.
   * Resolves with the newest table a member answered with in place of a report,
   * signalling this node pushed from a stale base, or `undefined` when none did.
   */
  async #push(
    table: RoutingTable,
    members: readonly ClusterMember[],
  ): Promise<RoutingTable | undefined> {
    const bytes: Uint8Array = encodeMessage({ kind: MessageKind.table, table: table.toWire() });
    const self: string = this.#view.self;
    const targets: ClusterMember[] = members.filter(
      (member: ClusterMember): boolean => member.name !== self,
    );
    const gathered: OwnershipReport[] = [this.#localReport()];
    let learned: RoutingTable | undefined;
    const responses: PromiseSettledResult<Uint8Array>[] = await Promise.allSettled(
      targets.map(
        (member: ClusterMember): Promise<Uint8Array> =>
          this.#transport.request(member.name, bytes, REQUEST_TIMEOUT_MS),
      ),
    );
    for (const response of responses) {
      if (response.status !== "fulfilled") {
        continue;
      }

      const report: OwnershipReport | undefined = decodeReport(response.value);
      if (report !== undefined) {
        gathered.push(report);
        continue;
      }

      const peer: RoutingTable | undefined = decodeTable(response.value);
      if (peer !== undefined && (learned === undefined || peer.supersedes(learned))) {
        learned = peer;
      }
    }

    this.#reports = gathered;
    return learned;
  }

  /** This node's ownership report, a snapshot of the partitions it holds. */
  #localReport(): OwnershipReport {
    return { node: this.#view.self, partitions: [...this.#heldPartitions()] };
  }

  /** The name of the node this view currently makes coordinator, if any. */
  #coordinatorName(): string | undefined {
    return this.#view.members()[0]?.name;
  }
}
