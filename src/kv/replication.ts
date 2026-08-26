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
 * Remote routing and primary/backup replication.
 *
 * A key belongs to one partition, and its routing table names that partition's
 * primary. A write or read on any node computes the partition, looks up the
 * primary, and either serves it locally or forwards one RPC to the primary. At
 * the primary the write enters the local engine, which stamps and sequences it;
 * the primary then replicates the stamped entry to the partition's backups and,
 * in synchronous mode, waits for a write quorum of acknowledgments before
 * reporting success. A backup merges the entry under last write wins, keeping
 * the primary's timestamp and sequence.
 *
 * Two pieces divide the work along the durability boundary. {@link PrimaryBackup}
 * is the replication authority for one partition: the local apply plus the
 * backup quorum, the piece a consensus group would one day replace. {@link
 * Replicator} is the node-level router above it: it owns the routing table and
 * the ring, dispatches inbound RPCs, and picks the primary. The ring, the table,
 * the transport, and the client API all sit above the durability boundary, so
 * swapping the durability implementation would not disturb them.
 *
 * An unfragmented partition is read from its single primary, which is
 * authoritative. A fragmented partition, the transient state a move produces, is
 * read by gathering the raw version from every owner, resolving last write wins
 * across them, and repairing the stale owners off the caller's path; a previous
 * owner still serves the data a new primary has not yet received. {@link
 * PrimaryBackup.reconcile} folds a set of peers' fragments into this node under
 * the same rule, the union a promoted primary needs.
 *
 * Crash recovery names a single surviving primary, so its partition looks
 * unfragmented, but that primary may be missing writes that lived only on
 * another survivor. While the recovery layer signals such a partition through
 * `isRecovering`, this node gathers a read across itself and its backups rather
 * than trust its local fragment, and refuses conditional writes, so no
 * acknowledged write is missed and no duplicate is minted before the promoted
 * primary has reconciled.
 *
 * The rebalance gate is checked twice: at the node a request enters, as a fast
 * refusal, and again at the primary against its own view, so a stale sender
 * cannot slip a conditional write past the authoritative owner.
 *
 * @internal
 */

import { DEFAULT_REPLICA_COUNT, DEFAULT_WRITE_QUORUM, REQUEST_TIMEOUT_MS } from "./constants";
import { PutCondition, WriteKind } from "./discriminants";
import type { Engine } from "./engine";
import { isLiveValue, supersedes } from "./entry";
import { ClusterUnavailableError, KvProtocolError, PartitionRebalancingError } from "./errors";
import { FragmentTransfer } from "./fragment";
import type { Entry, KvTransport, WriteOp, WriteResult } from "./ports";
import { PrimaryBackup, type ReplicationMode } from "./primary.backup";
import type { PartitionRing } from "./ring";
import type { PartitionPlacement, RoutingTable } from "./routing.table";
import { decodeMessage, encodeMessage, type KvMessage, MessageKind } from "./wire";

/** One owner's raw version for a key during a fragmented gather. */
interface OwnerRead {
  readonly owner: string;
  readonly entry: Entry | undefined;
}

/** Whether an operation must be refused while its partition is fragmented. */
function isConditional(op: WriteOp): boolean {
  if (op.kind === WriteKind.put) {
    return op.condition !== PutCondition.none;
  }

  return op.kind === WriteKind.compareAndSet || op.kind === WriteKind.increment;
}

/** Decodes a forwarded write's response, re-raising a primary's rebalancing refusal. */
function decodeWriteResponse(bytes: Uint8Array): WriteResult {
  const message: KvMessage = decodeMessage(bytes);
  if (message.kind === MessageKind.rebalancing) {
    throw new PartitionRebalancingError(message.partitionId);
  }

  if (message.kind !== MessageKind.writeResponse) {
    throw new KvProtocolError("expected a write response");
  }

  return message.result;
}

/** Decodes a peek response as a raw entry, treating any other reply as absent. */
function decodePeek(bytes: Uint8Array): Entry | undefined {
  try {
    const message: KvMessage = decodeMessage(bytes);
    return message.kind === MessageKind.readResponse ? message.entry : undefined;
  } catch {
    return undefined;
  }
}

/** Decodes a forwarded read's response, rejecting any other message. */
function decodeReadResponse(bytes: Uint8Array): Entry | undefined {
  const message: KvMessage = decodeMessage(bytes);
  if (message.kind !== MessageKind.readResponse) {
    throw new KvProtocolError("expected a read response");
  }

  return message.entry;
}

/** Tuning for a {@link Replicator}, defaulted from the store's constants. @internal */
export interface ReplicatorOptions {
  /** Whether writes wait for the quorum or replicate in the background. */
  readonly mode?: ReplicationMode;
  /** Acknowledgments, counting the primary, a synchronous write awaits. */
  readonly writeQuorum?: number;
  /** Intended replica set size including the primary, for ring backups. */
  readonly replicaCount?: number;
  /**
   * Whether a partition this node primaries is still catching up after a crash
   * promotion, so a read must gather across the backups rather than trust the
   * local fragment and a conditional write must be refused. The recovery layer
   * supplies this; the default reports no partition recovering.
   */
  readonly isRecovering?: (partition: number) => boolean;
}

/**
 * The node-level router over the partition primaries.
 *
 * @internal
 */
export class Replicator {
  /** This node's canonical identity, compared against each partition's primary. */
  readonly #self: string;

  /** Local engine, both the primary write path and the backup merge target. */
  readonly #engine: Engine;

  /** Carrier for forwarding to a remote primary and for inbound RPCs. */
  readonly #transport: KvTransport;

  /** Paged fragment transfer serving inbound fragment pulls and pushes. */
  readonly #transfer: FragmentTransfer;

  /** Intended replica set size including the primary. */
  readonly #replicaCount: number;

  /** Synchronous or background replication, passed to each partition group. */
  readonly #mode: ReplicationMode;

  /** Write quorum passed to each partition group. */
  readonly #writeQuorum: number;

  /** Whether a partition this node primaries is still reconciling after a promotion. */
  readonly #isRecovering: (partition: number) => boolean;

  /** Replication groups for partitions this node primaries, created on first use. */
  readonly #groups: Map<number, PrimaryBackup> = new Map();

  /** Latest routing table, or `undefined` until the first {@link install}. */
  #table: RoutingTable | undefined;

  /** Ring matching {@link #table}'s member set, supplying the backups. */
  #ring: PartitionRing | undefined;

  /**
   * @param self This node's identity, the same string membership uses.
   * @param engine Local store engine; its partition mapping is authoritative.
   * @param transport Carrier for forwarding and replication.
   * @throws {RangeError} If `writeQuorum` is not a positive integer.
   */
  constructor(
    self: string,
    engine: Engine,
    transport: KvTransport,
    options: ReplicatorOptions = {},
  ) {
    const writeQuorum: number = options.writeQuorum ?? DEFAULT_WRITE_QUORUM;
    if (!Number.isSafeInteger(writeQuorum) || writeQuorum <= 0) {
      throw new RangeError("write quorum must be a positive integer");
    }

    this.#self = self;
    this.#engine = engine;
    this.#transport = transport;
    this.#transfer = new FragmentTransfer(engine, transport);
    this.#replicaCount = options.replicaCount ?? DEFAULT_REPLICA_COUNT;
    this.#mode = options.mode ?? "sync";
    this.#writeQuorum = writeQuorum;
    this.#isRecovering = options.isRecovering ?? ((): boolean => false);
  }

  /**
   * Installs a new routing table and its ring, refreshing every live group's
   * replica set. `clustering.ts` calls this whenever the coordinator's table
   * changes.
   */
  install(table: RoutingTable, ring: PartitionRing): void {
    this.#table = table;
    this.#ring = ring;
    for (const [partition, group] of this.#groups) {
      group.memberChange(this.#backupsOf(partition));
    }
  }

  /**
   * Serves a write: locally when this node is the primary, else forwarded to it.
   *
   * @throws {PartitionRebalancingError} For a conditional write whose partition
   * has more than one owner or is still reconciling after a crash promotion,
   * since a primary that is not yet authoritative could mint a duplicate.
   * @throws {ClusterUnavailableError} Before a routing table is installed.
   */
  async write(op: WriteOp): Promise<WriteResult> {
    const partition: number = this.#engine.partitionFor(op.key);
    const placement: PartitionPlacement = this.#placementOf(partition);
    if (isConditional(op) && (placement.owners.length > 1 || this.#isRecovering(partition))) {
      throw new PartitionRebalancingError(partition);
    }

    if (placement.primary === this.#self) {
      return this.#groupFor(partition).propose(op);
    }

    return this.#forwardWrite(placement.primary, op);
  }

  /**
   * Serves a read. An unfragmented partition is answered by its single primary,
   * locally or forwarded. A fragmented partition gathers across all owners and
   * returns the highest-timestamp live version, since a new primary may not yet
   * hold the data a previous owner still serves.
   *
   * @throws {ClusterUnavailableError} Before a routing table is installed.
   */
  async read(key: string): Promise<Entry | undefined> {
    const partition: number = this.#engine.partitionFor(key);
    const placement: PartitionPlacement = this.#placementOf(partition);
    if (placement.owners.length > 1) {
      return this.#gather(key, placement.owners);
    }

    if (placement.primary === this.#self) {
      return this.#servePrimaryRead(partition, key);
    }

    return this.#forwardRead(placement.primary, key);
  }

  /**
   * Handles an inbound replication RPC and returns its encoded response. A
   * forwarded write applies here as the primary; a replicate merges as a backup.
   *
   * @throws {KvProtocolError} For a message this node does not serve.
   */
  async receive(_from: string, body: Uint8Array): Promise<Uint8Array> {
    const message: KvMessage = decodeMessage(body);
    if (message.kind === MessageKind.writeRequest) {
      return this.#serveWrite(message.op);
    }

    if (message.kind === MessageKind.readRequest) {
      const partition: number = this.#engine.partitionFor(message.key);
      const entry: Entry | undefined = await this.#servePrimaryRead(partition, message.key);
      return encodeMessage({ kind: MessageKind.readResponse, entry });
    }

    if (message.kind === MessageKind.peekRequest) {
      return encodeMessage({
        kind: MessageKind.readResponse,
        entry: this.#engine.peek(message.key),
      });
    }

    if (message.kind === MessageKind.replicate) {
      this.#engine.merge(message.entry);
      return encodeMessage({ kind: MessageKind.replicateAck });
    }

    if (message.kind === MessageKind.fragmentRequest) {
      return encodeMessage({
        kind: MessageKind.fragmentChunk,
        chunk: this.#transfer.servePage(message.partitionId, message.afterKey),
      });
    }

    if (message.kind === MessageKind.fragmentPush) {
      this.#transfer.applyChunk(message.chunk);
      return encodeMessage({ kind: MessageKind.fragmentAck });
    }

    throw new KvProtocolError("replication received an unexpected message");
  }

  /**
   * Applies a forwarded write as the primary, but re-checks the rebalance gate
   * against this node's own view first: a conditional write whose partition is
   * fragmented here is refused with a rebalancing reply, so a stale sender's gate
   * cannot let a duplicate through the authoritative primary.
   */
  async #serveWrite(op: WriteOp): Promise<Uint8Array> {
    const partition: number = this.#engine.partitionFor(op.key);
    const fragmented: boolean = this.#placementOf(partition).owners.length > 1;
    if (isConditional(op) && (fragmented || this.#isRecovering(partition))) {
      return encodeMessage({
        kind: MessageKind.rebalancing,
        partitionId: partition,
      });
    }

    const result: WriteResult = await this.#groupFor(partition).propose(op);
    return encodeMessage({ kind: MessageKind.writeResponse, result });
  }

  /**
   * Serves a read where this node is the sole primary: through the partition's
   * replication group on the fast path, or, while the partition is still
   * reconciling after a crash promotion, by gathering across this node and its
   * backups and taking the newest live version, exactly as a fragmented read
   * does. Once reconcile clears the flag it collapses back to the group read.
   */
  #servePrimaryRead(partition: number, key: string): Promise<Entry | undefined> {
    if (this.#isRecovering(partition)) {
      return this.#gather(key, [this.#self, ...this.#placementOf(partition).backups]);
    }

    return this.#groupFor(partition).read(key);
  }

  /**
   * Gathers the raw version for `key` from every owner, resolves last write wins
   * across them, repairs the stale owners off the caller's path, and returns the
   * winner only when it is a live value.
   *
   * Every owner is consulted before a winner is known, since a previous owner may
   * hold the newest write, so the read cannot settle until the slowest owner
   * answers or its deadline elapses. That trades the latency an unreachable owner
   * adds for a correct cross-owner result; a quorum-bounded early return is the
   * future lever, left to a read quorum above one.
   */
  async #gather(key: string, owners: readonly string[]): Promise<Entry | undefined> {
    // Encode the peek once: the frame is identical for every owner.
    const request: Uint8Array = encodeMessage({ kind: MessageKind.peekRequest, key });
    const reads: OwnerRead[] = await Promise.all(
      owners.map(
        async (owner: string): Promise<OwnerRead> => ({
          owner,
          entry: await this.#peekFrom(owner, key, request),
        }),
      ),
    );

    let winner: Entry | undefined;
    for (const read of reads) {
      if (read.entry !== undefined && (winner === undefined || supersedes(read.entry, winner))) {
        winner = read.entry;
      }
    }

    if (winner === undefined) {
      return undefined;
    }

    this.#readRepair(winner, reads);
    return isLiveValue(winner, this.#engine.now()) ? winner : undefined;
  }

  /** The raw entry `owner` holds for `key`: locally, or by the pre-encoded peek RPC. */
  #peekFrom(owner: string, key: string, request: Uint8Array): Promise<Entry | undefined> {
    if (owner === this.#self) {
      return Promise.resolve(this.#engine.peek(key));
    }

    return this.#transport
      .request(owner, request, REQUEST_TIMEOUT_MS)
      .then(decodePeek, (): undefined => undefined);
  }

  /** Pushes `winner` to every owner that holds an older or absent version. */
  #readRepair(winner: Entry, reads: readonly OwnerRead[]): void {
    const bytes: Uint8Array = encodeMessage({
      kind: MessageKind.replicate,
      entry: winner,
    });
    for (const read of reads) {
      if (read.entry !== undefined && !supersedes(winner, read.entry)) {
        continue;
      }

      if (read.owner === this.#self) {
        this.#engine.merge(winner);
        continue;
      }

      this.#pushFrame(read.owner, bytes);
    }
  }

  /** Fire-and-forget send of one already-encoded frame; its outcome is ignored. */
  #pushFrame(to: string, bytes: Uint8Array): void {
    void this.#transport.request(to, bytes, REQUEST_TIMEOUT_MS).then(
      (): void => undefined,
      (): void => undefined,
    );
  }

  /** Placement for `partition`, requiring a table to have been installed. */
  #placementOf(partition: number): PartitionPlacement {
    if (this.#table === undefined || this.#ring === undefined) {
      throw new ClusterUnavailableError();
    }

    return this.#table.placement(partition, this.#ring, this.#replicaCount);
  }

  /** The backups for `partition` from the current placement. */
  #backupsOf(partition: number): readonly string[] {
    return this.#placementOf(partition).backups;
  }

  /** The replication group for `partition`, created and seeded on first use. */
  #groupFor(partition: number): PrimaryBackup {
    const existing: PrimaryBackup | undefined = this.#groups.get(partition);
    if (existing !== undefined) {
      return existing;
    }

    const created: PrimaryBackup = new PrimaryBackup(
      partition,
      this.#engine,
      this.#transport,
      this.#writeQuorum,
      this.#mode,
    );
    created.memberChange(this.#backupsOf(partition));
    this.#groups.set(partition, created);
    return created;
  }

  /** Forwards a write to the primary and decodes its result. */
  #forwardWrite(primary: string, op: WriteOp): Promise<WriteResult> {
    const bytes: Uint8Array = encodeMessage({
      kind: MessageKind.writeRequest,
      op,
    });
    return this.#transport.request(primary, bytes, REQUEST_TIMEOUT_MS).then(decodeWriteResponse);
  }

  /** Forwards a read to the primary and decodes its entry. */
  #forwardRead(primary: string, key: string): Promise<Entry | undefined> {
    const bytes: Uint8Array = encodeMessage({
      kind: MessageKind.readRequest,
      key,
    });
    return this.#transport.request(primary, bytes, REQUEST_TIMEOUT_MS).then(decodeReadResponse);
  }
}
