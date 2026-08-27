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
 * Paged fragment transfer: the primitive every partition move is built on.
 *
 * A partition's fragment can be larger than one message, so it moves in chunks
 * bounded by {@link FRAGMENT_CHUNK_BYTES}. Two directions share one page shape.
 * A {@link FragmentTransfer.push} streams this node's fragment to a receiver a
 * chunk at a time, awaiting an acknowledgment for each before sending the next;
 * a {@link FragmentTransfer.pull} requests a peer's fragment a page at a time
 * behind a key cursor. Both sides merge each chunk under last write wins.
 *
 * The four invariants that make a move safe under loss, duplication, and a peer
 * that dies mid-transfer hold here:
 *
 * - The sender never drops its copy inside a transfer; dropping after a
 *   confirmed move is the caller's decision, so an interrupted push loses
 *   nothing and a retry re-ships from the start.
 * - The receiver merges and never replaces, so a write that landed directly on
 *   the receiver during the transfer still wins by last write wins.
 * - Transfers are therefore idempotent and restartable: re-shipping an entry
 *   the receiver already holds is a no-op, so a retry needs no bookkeeping.
 * - The transfer holds no partition-wide lock, so reads and writes proceed
 *   against whatever copies exist while it runs.
 *
 * Pages are ordered by key so the cursor a pull carries is a stable position
 * that survives concurrent insertion and deletion: a later request asks for the
 * keys after the last it received, never a shifting numeric offset. Ordering the
 * fragment sorts a snapshot; a push sorts once and walks it, while a pull's
 * responder sorts per request because it answers each page without shared state.
 * Sorting is off the message send path and rate limited by the caller, so its
 * cost is paid where latency does not matter.
 *
 * @internal
 */

import { FRAGMENT_CHUNK_BYTES, REQUEST_TIMEOUT_MS } from "./constants";
import type { Engine } from "./engine";
import type { Entry, KvTransport } from "./ports";
import {
  decodeMessage,
  encodeMessage,
  type FragmentChunkWire,
  type KvMessage,
  MessageKind,
} from "./wire";

/**
 * Fixed serialized overhead of one entry beyond its key and value bytes: the
 * flags byte, the length prefixes, the hybrid timestamp, and the sequence. The
 * page budget is an approximation used only to bound chunk size, so a constant
 * upper estimate is enough and never has to match the codec exactly.
 */
const ENTRY_OVERHEAD_BYTES: number = 64;

/** Upper bound on a UTF-16 unit's UTF-8 width, so a key estimate never underestimates. */
const MAX_UTF8_BYTES_PER_UNIT: number = 3;

/**
 * An upper estimate of `entry`'s serialized size, used only to decide where a
 * chunk ends. It over-counts rather than under-counts, so a chunk stays within
 * the byte budget without the codec being consulted.
 */
function approximateEntrySize(entry: Entry): number {
  const valueBytes: number = entry.value === undefined ? 0 : entry.value.length;
  const keyBytes: number = entry.key.length * MAX_UTF8_BYTES_PER_UNIT;
  const nodeBytes: number = entry.timestamp.node.length * MAX_UTF8_BYTES_PER_UNIT;
  return ENTRY_OVERHEAD_BYTES + valueBytes + keyBytes + nodeBytes;
}

/**
 * Ascending order of two entries by key. Keys within a partition are unique, so
 * the sort never compares two equal keys and an equal case cannot arise.
 */
function compareByKey(left: Entry, right: Entry): number {
  return left.key < right.key ? -1 : 1;
}

/** Index of the first entry in the ascending list whose key is greater than `afterKey`. */
function firstKeyAfter(sorted: readonly Entry[], afterKey: string): number {
  let low: number = 0;
  let high: number = sorted.length;
  while (low < high) {
    const mid: number = (low + high) >>> 1;
    if ((sorted[mid] as Entry).key <= afterKey) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  return low;
}

/** Builds the byte-bounded page of `sorted` after `afterKey`, in key order. */
function pageAfter(
  sorted: readonly Entry[],
  partition: number,
  afterKey: string | undefined,
): FragmentChunkWire {
  let index: number = afterKey === undefined ? 0 : firstKeyAfter(sorted, afterKey);
  const entries: Entry[] = [];
  let bytes: number = 0;
  while (index < sorted.length) {
    const entry: Entry = sorted[index] as Entry;
    const size: number = approximateEntrySize(entry);
    if (entries.length > 0 && bytes + size > FRAGMENT_CHUNK_BYTES) {
      break;
    }

    entries.push(entry);
    bytes += size;
    index += 1;
  }

  return { partitionId: partition, final: index >= sorted.length, entries };
}

/** The last entry's key, or `undefined` when the chunk carried none. */
function lastKeyOf(chunk: FragmentChunkWire): string | undefined {
  const entries: readonly Entry[] = chunk.entries;
  return entries.length === 0 ? undefined : (entries[entries.length - 1] as Entry).key;
}

/** Whether `bytes` decode to a fragment acknowledgment. */
function isFragmentAck(bytes: Uint8Array): boolean {
  try {
    return decodeMessage(bytes).kind === MessageKind.fragmentAck;
  } catch {
    return false;
  }
}

/** Decodes a fragment chunk, treating any other reply or malformed bytes as absent. */
function decodeChunk(bytes: Uint8Array): FragmentChunkWire | undefined {
  try {
    const message: KvMessage = decodeMessage(bytes);
    return message.kind === MessageKind.fragmentChunk ? message.chunk : undefined;
  } catch {
    return undefined;
  }
}

/**
 * The paged transfer of one partition's fragment between two nodes.
 *
 * @internal
 */
export class FragmentTransfer {
  /** Local engine that supplies the fragment to send and merges the fragment received. */
  readonly #engine: Engine;

  /** Carrier for the chunk RPCs in both directions. */
  readonly #transport: KvTransport;

  constructor(engine: Engine, transport: KvTransport) {
    this.#engine = engine;
    this.#transport = transport;
  }

  /**
   * Streams this node's fragment of `partition` to `to` in byte-bounded chunks,
   * awaiting an acknowledgment for each chunk before sending the next. Resolves
   * `true` once the receiver acknowledges the final chunk, `false` when a chunk
   * goes unacknowledged. An empty fragment sends one final empty chunk, so a
   * completed move is confirmed either way. The caller drops the local copy only
   * on a `true`.
   */
  async push(partition: number, to: string): Promise<boolean> {
    return this.#pushChunks(to, partition, this.#sortedFragment(partition));
  }

  /**
   * Streams a specific set of entries of `partition` to `to` in byte-bounded
   * chunks, the same way {@link push} streams a whole fragment. Anti-entropy uses
   * this to ship only the entries that diverged, rather than the whole partition.
   * The entries need not be sorted; an empty set sends one final empty chunk.
   */
  pushEntries(partition: number, to: string, entries: readonly Entry[]): Promise<boolean> {
    return this.#pushChunks(to, partition, [...entries].sort(compareByKey));
  }

  /** Streams a pre-sorted entry list to `to`, one acknowledged chunk at a time. */
  async #pushChunks(to: string, partition: number, sorted: readonly Entry[]): Promise<boolean> {
    let afterKey: string | undefined;
    while (true) {
      const chunk: FragmentChunkWire = pageAfter(sorted, partition, afterKey);
      const acknowledged: boolean = await this.#sendChunk(to, chunk);
      if (!acknowledged) {
        return false;
      }

      if (chunk.final) {
        return true;
      }

      afterKey = lastKeyOf(chunk);
    }
  }

  /**
   * Pulls `from`'s fragment of `partition` page by page and merges each under
   * last write wins. A page that fails to arrive, decodes to another message, or
   * fails to advance the cursor ends the pull; the merges that landed stand and a
   * retry re-pulls idempotently.
   */
  async pull(partition: number, from: string): Promise<void> {
    await this.#pages(partition, from, (chunk: FragmentChunkWire): void => {
      this.applyChunk(chunk);
    });
  }

  /**
   * Reads `from`'s fragment of `partition` page by page and returns every entry
   * it held, without merging any into the local store. A cluster-wide scan reads
   * a partition it does not own this way, so it observes the remote state without
   * disturbing its own. An unreachable or malformed page ends the read early,
   * returning what arrived, the same best-effort termination {@link pull} has.
   */
  async collect(partition: number, from: string): Promise<Entry[]> {
    const entries: Entry[] = [];
    await this.#pages(partition, from, (chunk: FragmentChunkWire): void => {
      // A push rather than a spread, so a chunk with very many entries cannot blow
      // the call-argument limit the way spreading its entries would.
      for (const entry of chunk.entries) {
        entries.push(entry);
      }
    });
    return entries;
  }

  /**
   * Requests `from`'s fragment of `partition` page by page, handing each chunk to
   * `onChunk`, until the final page, an unreachable or malformed page, or a page
   * whose cursor fails to advance against a Byzantine peer.
   */
  async #pages(
    partition: number,
    from: string,
    onChunk: (chunk: FragmentChunkWire) => void,
  ): Promise<void> {
    let afterKey: string | undefined;
    while (true) {
      const chunk: FragmentChunkWire | undefined = await this.#requestPage(
        from,
        partition,
        afterKey,
      );
      if (chunk === undefined) {
        return;
      }

      onChunk(chunk);
      if (chunk.final) {
        return;
      }

      const nextKey: string | undefined = lastKeyOf(chunk);
      if (nextKey === undefined || (afterKey !== undefined && nextKey <= afterKey)) {
        return;
      }

      afterKey = nextKey;
    }
  }

  /**
   * The next byte-bounded page of `partition` after `afterKey`, in key order, for
   * a pull responder. A page carries at least one entry unless the fragment has
   * none left after the cursor, and `final` marks the page that reaches the end.
   */
  servePage(partition: number, afterKey: string | undefined): FragmentChunkWire {
    return pageAfter(this.#sortedFragment(partition), partition, afterKey);
  }

  /** Merges every entry in `chunk` into the local fragment under last write wins. */
  applyChunk(chunk: FragmentChunkWire): void {
    for (const entry of chunk.entries) {
      this.#engine.merge(entry);
    }
  }

  /** This node's fragment of `partition`, snapshotted and sorted ascending by key. */
  #sortedFragment(partition: number): Entry[] {
    const entries: Entry[] = this.#engine.snapshot(partition);
    entries.sort(compareByKey);
    return entries;
  }

  /** Sends one push chunk and resolves to whether the receiver acknowledged it. */
  #sendChunk(to: string, chunk: FragmentChunkWire): Promise<boolean> {
    const request: Uint8Array = encodeMessage({ kind: MessageKind.fragmentPush, chunk });
    return this.#transport
      .request(to, request, REQUEST_TIMEOUT_MS)
      .then(isFragmentAck, (): boolean => false);
  }

  /** Requests one page after `afterKey`, resolving to its chunk or `undefined` on failure. */
  #requestPage(
    from: string,
    partition: number,
    afterKey: string | undefined,
  ): Promise<FragmentChunkWire | undefined> {
    const request: Uint8Array = encodeMessage({
      kind: MessageKind.fragmentRequest,
      partitionId: partition,
      afterKey,
    });
    return this.#transport
      .request(from, request, REQUEST_TIMEOUT_MS)
      .then(decodeChunk, (): undefined => undefined);
  }
}
