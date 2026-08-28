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
 * Background anti-entropy: the convergence guarantee that repair rides on.
 *
 * Two replicas of a partition drift apart when a write is lost in the background,
 * a backup misses a replication while briefly unreachable, a transfer is
 * interrupted, or a split heals. Read repair only ever touches keys someone
 * reads, so it cannot close that drift on its own. Anti-entropy does, by
 * comparing digests on a schedule and repairing whatever differs.
 *
 * The comparison escalates so its cost is proportional to the divergence, not to
 * the partition:
 *
 * 1. The initiator sends its rolling partition digest, maintained incrementally
 *    on every write. If the peer's digest matches, the two agree and the pass
 *    ends in one round trip with no scan, which is the overwhelmingly common
 *    case for two healthy replicas.
 * 2. On a mismatch the peer answers with a digest per repair bucket. The
 *    initiator computes its own and keeps only the buckets that differ, so a
 *    single divergent key narrows to a single bucket.
 * 3. For the divergent buckets the two sides exchange key and last-write-wins
 *    order, no values, and the initiator learns exactly which keys differ and
 *    which side holds the newer version of each.
 * 4. The initiator pulls the entries the peer holds newer and pushes the entries
 *    it holds newer, so both replicas converge to the last write per key. Only
 *    the entries that actually differ move.
 *
 * Every merge is last write wins, so the pass is safe to repeat, safe under a
 * dropped message, and order independent. Tombstones participate like any other
 * entry, which is what stops a missed delete from being repaired back to life,
 * as long as both replicas were present within `TOMBSTONE_TTL_MS`: once a
 * tombstone is reaped, a replica that still holds the deleted key as a live
 * value would push it back, so a node absent longer than that window must
 * re-seed rather than run a pass. Enforcing that cutoff is the recovery layer's
 * job, not this one's. The schedule that drives {@link AntiEntropy.sync} lives
 * in the clustering layer; this class carries only the mechanism.
 *
 * @internal
 */

import { REQUEST_TIMEOUT_MS } from "./constants";
import type { Engine } from "./engine";
import type { DigestLanes } from "./entry";
import { KvProtocolError } from "./errors";
import { FragmentTransfer } from "./fragment";
import { compareHybrid } from "./hlc";
import type { Entry, HybridTime, KvTransport } from "./ports";
import { decodeMessage, encodeMessage, type KvMessage, MessageKind } from "./wire";

/** The entries to send and the keys to fetch that a comparison resolved to. */
interface SyncPlan {
  /** Entries this node holds a newer version of, or the peer lacks, to push. */
  readonly push: readonly Entry[];
  /** Keys the peer holds a newer version of, or this node lacks, to pull. */
  readonly pull: ReadonlySet<string>;
}

/** Decodes a response, treating a transport failure or malformed bytes as absent. */
function decodeOrUndefined(bytes: Uint8Array): KvMessage | undefined {
  try {
    return decodeMessage(bytes);
  } catch {
    return undefined;
  }
}

/** Whether two rolling digests are equal in both lanes. */
function digestsEqual(left: DigestLanes, right: DigestLanes): boolean {
  return left.hi === right.hi && left.lo === right.lo;
}

/**
 * Bucketed-digest anti-entropy between two replicas of a partition.
 *
 * @internal
 */
export class AntiEntropy {
  /** Local engine: the digests to compare and the fragment to repair. */
  readonly #engine: Engine;

  /** Carrier for the comparison round trips this node initiates. */
  readonly #transport: KvTransport;

  /** Paged transfer used to ship the entries a comparison resolved to push. */
  readonly #transfer: FragmentTransfer;

  constructor(engine: Engine, transport: KvTransport) {
    this.#engine = engine;
    this.#transport = transport;
    this.#transfer = new FragmentTransfer(engine, transport);
  }

  /**
   * Reconciles this node's fragment of `partition` with `peer` to convergence,
   * moving work proportional to how far the two diverged. A pass that finds the
   * digests equal returns after one round trip; an unreachable peer or a
   * malformed reply ends the pass without changing anything, and the next pass
   * retries idempotently.
   */
  async sync(partition: number, peer: string): Promise<void> {
    const divergent: ReadonlySet<number> | undefined = await this.#divergentBuckets(
      partition,
      peer,
    );
    if (divergent === undefined || divergent.size === 0) {
      return;
    }

    const peerVersions: Map<string, HybridTime> | undefined = await this.#peerVersions(
      partition,
      peer,
      divergent,
    );
    if (peerVersions === undefined) {
      return;
    }

    const plan: SyncPlan = this.#plan(partition, divergent, peerVersions);
    if (plan.pull.size > 0) {
      await this.#pull(partition, peer, plan.pull);
    }

    if (plan.push.length > 0) {
      await this.#transfer.pushEntries(partition, peer, plan.push);
    }
  }

  /**
   * Answers a comparison RPC from a peer initiating a pass against this node.
   *
   * @throws {KvProtocolError} For a message this responder does not serve.
   */
  async receive(_from: string, body: Uint8Array): Promise<Uint8Array> {
    const message: KvMessage = decodeMessage(body);
    if (message.kind === MessageKind.syncDigest) {
      return this.#serveDigest(message.partitionId, message.digest);
    }

    if (message.kind === MessageKind.keyVersionsRequest) {
      return this.#serveKeyVersions(message.partitionId, message.buckets);
    }

    if (message.kind === MessageKind.entriesRequest) {
      return this.#serveEntries(message.partitionId, message.keys);
    }

    throw new KvProtocolError("anti-entropy received an unexpected message");
  }

  /**
   * Answers a digest open: an empty bucket list when the partition digests match,
   * otherwise this node's per-bucket digests for the initiator to compare.
   */
  #serveDigest(partition: number, theirDigest: DigestLanes): Uint8Array {
    const digests: readonly DigestLanes[] = digestsEqual(
      this.#engine.partitionDigest(partition),
      theirDigest,
    )
      ? []
      : this.#engine.bucketDigests(partition);
    return encodeMessage({ kind: MessageKind.bucketDigests, partitionId: partition, digests });
  }

  /** Answers a key-versions request with this node's key versions for the given buckets. */
  #serveKeyVersions(partition: number, buckets: readonly number[]): Uint8Array {
    return encodeMessage({
      kind: MessageKind.keyVersions,
      partitionId: partition,
      versions: this.#engine.keyVersions(partition, new Set(buckets)),
    });
  }

  /**
   * Answers an entries request with this node's stored entries for the given
   * keys, in a single chunk. A pass resolves only the entries that diverged,
   * which for two healthy replicas is the handful of writes in flight, so the
   * reply is not paged; a bulk divergence is closed through the paged reconcile
   * of the recovery layer instead, not through a single anti-entropy pass.
   */
  #serveEntries(partition: number, keys: readonly string[]): Uint8Array {
    return encodeMessage({
      kind: MessageKind.fragmentChunk,
      chunk: {
        partitionId: partition,
        final: true,
        entries: this.#engine.entriesFor(partition, new Set(keys)),
      },
    });
  }

  /**
   * Sends this node's partition digest and returns the buckets that differ, an
   * empty set when the two agree, or `undefined` when the peer is unreachable or
   * replies with anything but bucket digests. The local bucket scan runs only on
   * a mismatch, so an agreeing pass costs no scan.
   */
  async #divergentBuckets(
    partition: number,
    peer: string,
  ): Promise<ReadonlySet<number> | undefined> {
    const request: Uint8Array = encodeMessage({
      kind: MessageKind.syncDigest,
      partitionId: partition,
      digest: this.#engine.partitionDigest(partition),
    });
    const message: KvMessage | undefined = await this.#ask(peer, request);
    if (message === undefined || message.kind !== MessageKind.bucketDigests) {
      return undefined;
    }

    const peerBuckets: readonly DigestLanes[] = message.digests;
    if (peerBuckets.length === 0) {
      return new Set();
    }

    const localBuckets: readonly DigestLanes[] = this.#engine.bucketDigests(partition);
    if (peerBuckets.length !== localBuckets.length) {
      return undefined;
    }

    const divergent: Set<number> = new Set();
    for (let bucket: number = 0; bucket < localBuckets.length; bucket += 1) {
      if (!digestsEqual(localBuckets[bucket] as DigestLanes, peerBuckets[bucket] as DigestLanes)) {
        divergent.add(bucket);
      }
    }

    return divergent;
  }

  /**
   * Fetches the peer's key versions for `buckets`, keyed by key, or `undefined`
   * when the peer is unreachable or replies with anything but key versions.
   */
  async #peerVersions(
    partition: number,
    peer: string,
    buckets: ReadonlySet<number>,
  ): Promise<Map<string, HybridTime> | undefined> {
    const request: Uint8Array = encodeMessage({
      kind: MessageKind.keyVersionsRequest,
      partitionId: partition,
      buckets: [...buckets],
    });
    const message: KvMessage | undefined = await this.#ask(peer, request);
    if (message === undefined || message.kind !== MessageKind.keyVersions) {
      return undefined;
    }

    const versions: Map<string, HybridTime> = new Map();
    for (const version of message.versions) {
      versions.set(version.key, version.timestamp);
    }

    return versions;
  }

  /**
   * Diffs this node's key versions for `buckets` against the peer's: a key this
   * node holds newer or the peer lacks is pushed; a key the peer holds newer or
   * this node lacks is pulled; an equal version is left alone. The two sets are
   * disjoint, so the pull and the push do not contend.
   */
  #plan(
    partition: number,
    buckets: ReadonlySet<number>,
    peerVersions: ReadonlyMap<string, HybridTime>,
  ): SyncPlan {
    const pushKeys: Set<string> = new Set();
    const pull: Set<string> = new Set();
    const localSeen: Set<string> = new Set();
    for (const local of this.#engine.keyVersions(partition, buckets)) {
      localSeen.add(local.key);
      const peerTimestamp: HybridTime | undefined = peerVersions.get(local.key);
      if (peerTimestamp === undefined) {
        pushKeys.add(local.key);
        continue;
      }

      const order: number = compareHybrid(local.timestamp, peerTimestamp);
      if (order > 0) {
        pushKeys.add(local.key);
        continue;
      }

      if (order < 0) {
        pull.add(local.key);
      }
    }

    for (const key of peerVersions.keys()) {
      if (!localSeen.has(key)) {
        pull.add(key);
      }
    }

    return { push: this.#engine.entriesFor(partition, pushKeys), pull };
  }

  /**
   * Fetches the peer's entries for `keys` in one request and merges them under
   * last write wins. Like {@link #serveEntries} this is unpaged because a pass
   * only ever names the diverged keys; the recovery reconcile carries a bulk
   * transfer when one is needed.
   */
  async #pull(partition: number, peer: string, keys: ReadonlySet<string>): Promise<void> {
    const request: Uint8Array = encodeMessage({
      kind: MessageKind.entriesRequest,
      partitionId: partition,
      keys: [...keys],
    });
    const message: KvMessage | undefined = await this.#ask(peer, request);
    if (message === undefined || message.kind !== MessageKind.fragmentChunk) {
      return;
    }

    this.#transfer.applyChunk(message.chunk);
  }

  /** Sends `body` to `peer` and decodes the reply, or `undefined` on failure. */
  async #ask(peer: string, body: Uint8Array): Promise<KvMessage | undefined> {
    let response: Uint8Array;
    try {
      response = await this.#transport.request(peer, body, REQUEST_TIMEOUT_MS);
    } catch {
      return undefined;
    }

    return decodeOrUndefined(response);
  }
}
