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

import { parseHostPort } from "./clustering.transport";
import { PutCondition, WriteKind } from "./kv/discriminants";
import type { Entry, ScanEntry, WriteApplied, WriteOp, WriteResult } from "./kv/ports";
import { reservedNamesPrefix } from "./reserved";

/** Shared UTF-8 codecs for the address string a registry entry stores as its value. */
const UTF8_ENCODER: TextEncoder = new TextEncoder();
const UTF8_DECODER: TextDecoder = new TextDecoder();

/**
 * Key prefix under which a companion record lives, beside the placement it belongs
 * to.
 *
 * A placement is keyed by the bare actor name; its companion is keyed by this
 * prefix followed by the same name. The prefix carries the runtime reserved
 * prefix, which an actor name can never start with, so a companion key can never
 * collide with a placement key, and {@link ClusterRegistry.actorsByHost} skips it
 * structurally instead of inferring a record's kind from its value, which a value
 * that happens to read as an address would otherwise defeat.
 */
const COMPANION_KEY_PREFIX: string = `${reservedNamesPrefix}Companion:`;

/** The key a companion record for `name` lives under. */
function companionKey(name: string): string {
  return `${COMPANION_KEY_PREFIX}${name}`;
}

/** The host of an `host:port` address, or `undefined` when the address is not one. */
function hostOf(address: string): string | undefined {
  try {
    return parseHostPort(address).host;
  } catch {
    return undefined;
  }
}

/** The store operations a {@link ClusterRegistry} maps its calls onto. @internal */
export interface RegistryStore {
  /** Submits a mutation and resolves with its outcome. */
  write(op: WriteOp): Promise<WriteResult>;
  /** Reads the live value for `key`, or `undefined`. */
  read(key: string): Promise<Entry | undefined>;
  /** Reads every live key and value across the cluster. */
  scan(): Promise<ScanEntry[]>;
}

/**
 * The cluster registry: actor names and their locations, kept unique and looked
 * up across the cluster.
 *
 * Each call is a thin mapping onto one distributed store operation. A name is a
 * store key; the address it resolves to is the value, held as UTF-8 bytes. Name
 * uniqueness rides the store's conditional write: {@link claimActorName} is an
 * absent-only put, so two nodes racing to claim one name cannot both win, the
 * same authority the store already gives conditional writes under a partition.
 *
 * It is deliberately blind to what an address means, keeping it a store facade
 * rather than an actor concern; the runtime that consumes it decides how a
 * location is spelled. It is cluster-runtime infrastructure the actor system uses
 * for its distributed registry, not a surface an application uses directly.
 *
 * @internal
 */
export class ClusterRegistry {
  /** The distributed store the registry reads and writes through. */
  readonly #store: RegistryStore;

  /** @param store The cluster whose {@link RegistryStore.write} and read this maps onto. */
  constructor(store: RegistryStore) {
    this.#store = store;
  }

  /**
   * Claims `name` for `address` only if it is unclaimed, resolving to whether this
   * caller won it. A `false` means the name is already held, the duplicate-name
   * outcome, so a cluster-wide spawn can refuse to mint a second holder.
   */
  async claimActorName(name: string, address: string): Promise<boolean> {
    const result: WriteResult = await this.#store.write({
      kind: WriteKind.put,
      key: name,
      value: UTF8_ENCODER.encode(address),
      condition: PutCondition.ifAbsent,
    });
    return result.applied;
  }

  /** Records `address` for `name`, overwriting any current holder. */
  async putActor(name: string, address: string): Promise<void> {
    await this.#store.write({
      kind: WriteKind.put,
      key: name,
      value: UTF8_ENCODER.encode(address),
      condition: PutCondition.none,
    });
  }

  /** The address registered for `name`, or `undefined` when no live entry holds it. */
  async getActor(name: string): Promise<string | undefined> {
    const entry: Entry | undefined = await this.#store.read(name);
    if (entry?.value === undefined) {
      return undefined;
    }

    return UTF8_DECODER.decode(entry.value);
  }

  /** Removes any registration for `name`. */
  async removeActor(name: string): Promise<void> {
    await this.#store.write({ kind: WriteKind.delete, key: name });
  }

  /** Whether a live registration holds `name`. */
  async actorExists(name: string): Promise<boolean> {
    return (await this.#store.read(name)) !== undefined;
  }

  /**
   * Stores `record` as the companion of `name`, overwriting any current one. The
   * record is opaque bytes kept beside the placement under the reserved companion
   * prefix, so it never collides with an actor name and never counts as an actor
   * in {@link actorsByHost}.
   */
  async putCompanion(name: string, record: Uint8Array): Promise<void> {
    await this.#store.write({
      kind: WriteKind.put,
      key: companionKey(name),
      value: record,
      condition: PutCondition.none,
    });
  }

  /** The companion record stored for `name`, or `undefined` when none is held. */
  async getCompanion(name: string): Promise<Uint8Array | undefined> {
    const entry: Entry | undefined = await this.#store.read(companionKey(name));
    if (entry?.value === undefined) {
      return undefined;
    }

    return entry.value;
  }

  /** Removes any companion record stored for `name`. */
  async removeCompanion(name: string): Promise<void> {
    await this.#store.write({ kind: WriteKind.delete, key: companionKey(name) });
  }

  /**
   * Atomically increments the counter at `key` and resolves with its new value,
   * for a round-robin selector shared across the cluster. An absent counter starts
   * at zero, so the first call returns one.
   */
  async nextRoundRobinValue(key: string): Promise<bigint> {
    // An increment never declines, so its result always carries the updated
    // counter: the eight-byte signed value the engine wrote for the key.
    const result: WriteApplied = (await this.#store.write({
      kind: WriteKind.increment,
      key,
      delta: 1n,
    })) as WriteApplied;
    const counter: Uint8Array = result.entry.value as Uint8Array;
    return new DataView(counter.buffer, counter.byteOffset, counter.byteLength).getBigInt64(0);
  }

  /**
   * Claims `key` for `address` only if it is unclaimed, and lets the claim lapse
   * after `ttlMs`, for a singleton or a scheduled task at most one node runs. The
   * claim is an absent-only put with a lease, so exactly one caller wins until the
   * lease expires, then the next caller can claim it afresh.
   *
   * The value is an address, so like every non-placement record a lock that must
   * not be counted by {@link actorsByHost} has to use a key under the reserved
   * prefix; a bare-name claim counts as a placement on its address.
   */
  async claimOnce(key: string, address: string, ttlMs: number): Promise<boolean> {
    const result: WriteResult = await this.#store.write({
      kind: WriteKind.put,
      key,
      value: UTF8_ENCODER.encode(address),
      condition: PutCondition.ifAbsent,
      ttlMs,
    });
    return result.applied;
  }

  /**
   * The names of every actor whose registered address is on `host`. It scans the
   * whole cluster, skips every record under the runtime reserved prefix (companions
   * and any other non-placement record the runtime keys there) structurally, and
   * keeps the placement names whose address is an `host:port` on `host`, so a host's
   * actors can be found for failover or draining. A placement whose address is not
   * an `host:port` never matches. A record the runtime does not want counted as an
   * actor must therefore live under the reserved prefix, not by hoping its value
   * fails to read as an address.
   */
  async actorsByHost(host: string): Promise<string[]> {
    const names: string[] = [];
    for (const entry of await this.#store.scan()) {
      if (entry.key.startsWith(reservedNamesPrefix)) {
        continue;
      }

      if (hostOf(UTF8_DECODER.decode(entry.value)) === host) {
        names.push(entry.key);
      }
    }

    return names;
  }

  /** How many actors have a registered address on `host`. */
  async countActorsByHost(host: string): Promise<number> {
    return (await this.actorsByHost(host)).length;
  }
}
