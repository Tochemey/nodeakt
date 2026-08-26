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
 * Primary/backup replication for one partition: the durability authority the
 * router routes through.
 *
 * A proposal applies locally through the engine, which stamps and sequences it,
 * then replicates the stamped entry to the partition's backups. In synchronous
 * mode the proposal resolves once a write quorum of acknowledgments arrives, the
 * primary counting as one; in background mode it resolves after the local apply
 * and replicates without waiting. A backup merges under last write wins, so a
 * repeat or an out-of-order delivery is harmless. Reconcile pulls a set of
 * peers' fragments into this node under the same rule, the union a promoted
 * primary needs to hold every acknowledged write that survived a failure.
 *
 * This is the piece a consensus implementation would replace: it satisfies the
 * {@link ReplicationGroup} contract and nothing above it, the ring, the routing
 * table, the transport, and the client API, depends on how it is built.
 *
 * @internal
 */

import { REQUEST_TIMEOUT_MS } from "./constants";
import type { Engine } from "./engine";
import { KvQuorumError } from "./errors";
import type { Entry, KvTransport, ReplicationGroup, WriteOp, WriteResult } from "./ports";
import { decodeMessage, encodeMessage, type KvMessage, MessageKind } from "./wire";

/** How far a write travels before it is acknowledged. @internal */
export type ReplicationMode = "sync" | "async";

/** Resolves with the count of `true` results once `needed` arrive or all settle. */
function firstAcks(sends: readonly Promise<boolean>[], needed: number): Promise<number> {
  return new Promise<number>((resolve: (count: number) => void): void => {
    let acks: number = 0;
    let settled: number = 0;
    const onResult = (ok: boolean): void => {
      if (ok) {
        acks += 1;
      }

      settled += 1;
      if (acks >= needed || settled === sends.length) {
        resolve(acks);
      }
    };
    for (const send of sends) {
      void send.then(onResult);
    }
  });
}

/** Whether `bytes` decode to a replication acknowledgment. */
function isReplicateAck(bytes: Uint8Array): boolean {
  try {
    return decodeMessage(bytes).kind === MessageKind.replicateAck;
  } catch {
    return false;
  }
}

/** Decodes a fragment response into its entries, treating any other reply as empty. */
function decodeFragment(bytes: Uint8Array): readonly Entry[] {
  try {
    const message: KvMessage = decodeMessage(bytes);
    return message.kind === MessageKind.fragmentChunk ? message.chunk.entries : [];
  } catch {
    return [];
  }
}

/**
 * The replication authority for one partition: the local apply followed by
 * quorum-acknowledged replication to the partition's backups.
 *
 * @internal
 */
export class PrimaryBackup implements ReplicationGroup {
  /** The partition this group is the authority for. */
  readonly #partitionId: number;

  /** Local engine that stamps and stores primary writes. */
  readonly #engine: Engine;

  /** Carrier for replicating a stamped entry to the backups and pulling reconciles. */
  readonly #transport: KvTransport;

  /** Acknowledgments, counting the primary, that a synchronous write awaits. */
  readonly #writeQuorum: number;

  /** Whether a write waits for the quorum or replicates in the background. */
  readonly #mode: ReplicationMode;

  /**
   * Backups that hold copies of this partition. The router installs the ring's
   * backups, which already exclude the primary, so this node is never present.
   */
  #replicas: readonly string[] = [];

  constructor(
    partitionId: number,
    engine: Engine,
    transport: KvTransport,
    writeQuorum: number,
    mode: ReplicationMode,
  ) {
    this.#partitionId = partitionId;
    this.#engine = engine;
    this.#transport = transport;
    this.#writeQuorum = writeQuorum;
    this.#mode = mode;
  }

  /** Installs this partition's backup set. */
  memberChange(replicas: readonly string[]): void {
    this.#replicas = replicas;
  }

  /** The live, unexpired value for `key` from the primary's own fragment. */
  read(key: string): Promise<Entry | undefined> {
    return this.#engine.read(key);
  }

  /**
   * Pulls each peer's fragment of this partition and merges it under last write
   * wins, so a promoted primary gathers every write that survived a failure. An
   * unreachable peer is skipped; the merge is idempotent, so a repeat is safe.
   *
   * Each peer answers in a single chunk. Paging a large fragment across bounded
   * chunks, and the recovery that decides when to reconcile and promote, belong
   * to the departure and recovery machinery that drives this method.
   */
  async reconcile(peers: readonly string[]): Promise<void> {
    const request: Uint8Array = encodeMessage({
      kind: MessageKind.fragmentRequest,
      partitionId: this.#partitionId,
    });
    await Promise.all(peers.map((peer: string): Promise<void> => this.#pullFrom(peer, request)));
  }

  /**
   * Applies `op` locally as the primary, then replicates the result. A rejected
   * conditional write is returned without replicating, since nothing changed.
   *
   * @throws {KvQuorumError} In synchronous mode when the write quorum is not met.
   */
  async propose(op: WriteOp): Promise<WriteResult> {
    const result: WriteResult = await this.#engine.write(op);
    if (!result.applied) {
      return result;
    }

    await this.#replicate(result.entry);
    return result;
  }

  /** Sends the entry to every backup, awaiting a quorum only in synchronous mode. */
  async #replicate(entry: Entry): Promise<void> {
    // Encode once: the frame is identical for every backup.
    const bytes: Uint8Array = encodeMessage({ kind: MessageKind.replicate, entry });
    const sends: Promise<boolean>[] = this.#replicas.map(
      (backup: string): Promise<boolean> => this.#replicateOne(backup, bytes),
    );
    // The quorum is capped by the reachable backups, so an under-replicated
    // partition acknowledges on the primary alone rather than blocking on acks
    // it can never receive.
    const needed: number =
      this.#mode === "async" ? 0 : Math.min(this.#writeQuorum - 1, sends.length);
    if (needed <= 0) {
      return;
    }

    const acks: number = await firstAcks(sends, needed);
    if (acks < needed) {
      throw new KvQuorumError("write", 1 + acks, this.#writeQuorum);
    }
  }

  /** Sends one encoded replicate frame to one backup, resolving to whether it acknowledged. */
  #replicateOne(backup: string, bytes: Uint8Array): Promise<boolean> {
    return this.#transport.request(backup, bytes, REQUEST_TIMEOUT_MS).then(
      (response: Uint8Array): boolean => isReplicateAck(response),
      (): boolean => false,
    );
  }

  /** Pulls one peer's fragment of this partition and merges every entry it returns. */
  async #pullFrom(peer: string, request: Uint8Array): Promise<void> {
    let response: Uint8Array;
    try {
      response = await this.#transport.request(peer, request, REQUEST_TIMEOUT_MS);
    } catch {
      return;
    }

    for (const entry of decodeFragment(response)) {
      this.#engine.merge(entry);
    }
  }
}
