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

import type { IsolateRoute } from "./actor.ref";
import { type Companion, encodeCompanion } from "./clustering.companion";
import { type RecreateRecipe, recipeToSpawn } from "./clustering.recreate";
import type { ClusterRegistry } from "./clustering.registry";
import type { ClusterResolver } from "./clustering.resolver";
import { parseHostPort } from "./clustering.transport";
import { ErrActorAlreadyExists } from "./errors";
import { ClusterUnavailableError, PartitionRebalancingError } from "./kv/errors";
import type { IsolateMetrics } from "./observability/metric.snapshot";
import type { PID } from "./pid";
import type { Placement } from "./placement";
import type { Props } from "./props";
import { placedRecipe } from "./registration";
import type { SpawnOptions } from "./spawn.options";

/** Retries a claim makes after its first attempt when it hits a retryable
 * refusal, before giving the error back to the caller. */
const CLAIM_RETRY_ATTEMPTS: number = 5;

/** First backoff delay; each further retry doubles it, capped below. */
const CLAIM_RETRY_BASE_DELAY_MS: number = 25;

/** Ceiling on one backoff delay, so a long rebalance never stretches a single
 * wait without bound. */
const CLAIM_RETRY_MAX_DELAY_MS: number = 250;

/**
 * The production backoff sleep: a promise that resolves after `ms` and never
 * keeps the process alive on its own.
 *
 * @internal
 */
export function timerSleep(ms: number): Promise<void> {
  return new Promise<void>((resolve): void => {
    const timer: ReturnType<typeof setTimeout> = setTimeout(resolve, ms);
    timer.unref();
  });
}

/** The backoff before the retry after `attempt` prior failures. */
function backoffDelay(attempt: number): number {
  return Math.min(CLAIM_RETRY_BASE_DELAY_MS * 2 ** attempt, CLAIM_RETRY_MAX_DELAY_MS);
}

/**
 * Whether a thrown claim error is one a retry can clear: the claim's partition
 * is rebalancing, or the local half is briefly not serving. A won or lost claim
 * is not an error, so this only ever classifies a rejection.
 */
function isRetryable(error: unknown): boolean {
  return error instanceof PartitionRebalancingError || error instanceof ClusterUnavailableError;
}

/** How a chosen placement reaches the node that owns it, for the remote arm of
 * {@link ClusterPlacement.placeOn}. @internal */
export interface ClusterPlacementRemote {
  /** Spawns `name` on the node at `host:port` and resolves with its routed handle. */
  spawn(
    host: string,
    port: number,
    name: string,
    props: Props,
    options?: SpawnOptions,
  ): Promise<PID>;

  /** Recreates a departed node's `name` on the node at `host:port` from its recipe,
   * gated on the record still naming `deadOwner`; resolves whether that node took it. */
  recreate(
    host: string,
    port: number,
    name: string,
    recipe: RecreateRecipe,
    singleton: boolean,
    deadOwner: string,
  ): Promise<boolean>;

  /** Maps an owner's cluster identity to the remoting endpoint to spawn at, or
   * undefined when it is not a present member. */
  remotingAddressOf(owner: string): string | undefined;
}

/** Construction parameters for a {@link ClusterPlacement}. @internal */
export interface ClusterPlacementOptions {
  /** The distributed registry the placement claims names and records placements in. */
  readonly registry: ClusterRegistry;

  /** This node's cluster identity, the owner address recorded for a name it hosts. */
  readonly node: string;

  /** Boots the node-local worker-pool placement this one wraps, wiring its
   * release notifications to `onRelease` so a freed name's registry record is
   * deleted. Called at most once, on the first placement. */
  readonly bootInner: (onRelease: (name: string) => void) => Promise<Placement>;

  /** Resolves a name owned by another node to a routed handle, for the
   * location-transparent lookup behind the actor system's `actorOf`. Omitted, a
   * name this node does not hold reads as absent. */
  readonly resolver?: ClusterResolver;

  /** Reaches the node a chosen placement lands on, for {@link ClusterPlacement.placeOn}.
   * Omitted, a placement targeted at another node has nowhere to ship its recipe. */
  readonly remote?: ClusterPlacementRemote;

  /** The system-wide relocation default: whether an actor whose spawn options leave
   * `relocatable` unset stores a companion recipe, so a survivor can recreate it when
   * this node departs. `true` by default across the cluster. */
  readonly relocationDefault: boolean;

  /** Backoff sleep between claim retries; the real timer when omitted, a stub in tests. */
  readonly sleep?: (ms: number) => Promise<void>;
}

/**
 * The cluster's {@link Placement}: it makes a top-level name unique across the
 * whole cluster and records where the actor lives, then delegates the node-local
 * work of building and addressing the actor to the worker-pool placement it
 * wraps.
 *
 * A name is claimed with the registry's cluster-wide conditional put, so two
 * nodes racing to spawn one name cannot both win and the loser sees the existing
 * duplicate. The wrapped worker-pool placement, booted on the first placement,
 * decides which of this node's isolates runs the actor; when the pool frees a
 * name (a placed actor stops, an explicit stop, a passivation), the release
 * notification deletes the registry record, so a freed name can be reclaimed
 * anywhere. Every claim absorbs the registry's retryable refusals, a rebalancing
 * partition or a briefly unavailable half, with bounded backoff, so a spawn
 * observes only success, a duplicate, or a failure that outlived the retries.
 *
 * @internal
 */
export class ClusterPlacement implements Placement {
  readonly #registry: ClusterRegistry;
  readonly #node: string;
  readonly #bootInner: (onRelease: (name: string) => void) => Promise<Placement>;
  readonly #resolver: ClusterResolver | undefined;
  readonly #remote: ClusterPlacementRemote | undefined;
  readonly #relocationDefault: boolean;
  readonly #sleep: (ms: number) => Promise<void>;

  /** The wrapped node-local placement once booted, null until the first placement. */
  #inner: Placement | null = null;
  /** Single-flight guard over the wrapped placement's lazy boot. */
  #innerBoot: Promise<Placement> | null = null;
  /** Names this placement is building right now, so the recipe spawn's re-entrant
   * claim of the same name is a no-op instead of colliding with its own claim. */
  readonly #placing: Set<string> = new Set();
  /** Names this node wrote a companion recipe for, so a freed name's companion is
   * deleted only when one exists, and a non-relocatable name pays no extra delete. */
  readonly #companions: Set<string> = new Set();
  /** Names this node is recreating right now: while one is here a build failure or a
   * duplicate stop must not delete its record, because the recreate owns that record
   * and returns it to the dead owner itself, so the orphan sweep can retry it. */
  readonly #recreating: Set<string> = new Set();
  /** Set on stop, so a release arriving during teardown writes nothing. */
  #stopped: boolean = false;

  constructor(options: ClusterPlacementOptions) {
    this.#registry = options.registry;
    this.#node = options.node;
    this.#bootInner = options.bootInner;
    this.#resolver = options.resolver;
    this.#remote = options.remote;
    this.#relocationDefault = options.relocationDefault;
    this.#sleep = options.sleep ?? timerSleep;
  }

  /**
   * Claims `name` cluster-wide for an instance spawn, then, once the pool is
   * booted, on the pool too. Resolves `null` when this node won the name, or the
   * {@link ErrActorAlreadyExists} sentinel when another holder already has it.
   */
  async claim(name: string): Promise<Error | null> {
    if (this.#placing.has(name)) {
      // The recipe spawn of a name this placement is itself building re-enters
      // here; the cluster claim already succeeded, so this is a quiet no-op.
      return null;
    }

    const won: boolean = await this.#claim(name, this.#node);
    if (!won) {
      return ErrActorAlreadyExists;
    }

    const inner: Placement | null = this.#inner;
    if (inner === null) {
      return null;
    }

    const refused: Error | null = await inner.claim(name);
    if (refused !== null) {
      await this.#removeRecord(name);
      return refused;
    }

    return null;
  }

  /**
   * Claims `name` cluster-wide for this node, then builds the actor on one of its
   * isolates, returning its handle. A lost claim throws {@link ErrActorAlreadyExists}.
   */
  async place(name: string, props: Props, options?: SpawnOptions): Promise<PID> {
    const won: boolean = await this.#claim(name, this.#node);
    if (!won) {
      throw ErrActorAlreadyExists;
    }

    return this.#buildLocal(name, props, options);
  }

  /**
   * Places `name` on the chosen `owner`, returning a routed handle. When `owner`
   * is this node it is a local claim and build, identical to {@link place}.
   * Otherwise it is a remote spawn the owner runs as its own local placement, so
   * the single cluster claim, and the record cleanup on stop or a failed build,
   * both happen on the owner rather than here. A lost claim surfaces as
   * {@link ErrActorAlreadyExists}, thrown here for a local owner or relayed back
   * from a remote one.
   */
  placeOn(name: string, props: Props, owner: string, options?: SpawnOptions): Promise<PID> {
    if (owner === this.#node) {
      return this.place(name, props, options);
    }

    return this.#buildRemote(name, props, owner, options);
  }

  /**
   * Recreates a departed node's actor on this node from its companion recipe: it
   * rewrites the placement record from `deadOwner` to this node, gated on the record
   * still naming the dead node, then builds the actor locally and records a fresh
   * companion. Resolves `true` when this node took the name, or `false` when the
   * record no longer named the dead node, already moved by another pass or gone.
   * The compare-and-set makes the recreate idempotent, so a successor coordinator's
   * identical plan collapses already-done work rather than double-building. A build
   * that fails after the record has moved returns the record to the dead owner rather
   * than deleting it, so the orphan sweep re-detects and retries it rather than losing
   * the actor, and rejects so the caller reports the failure.
   */
  async recreate(
    name: string,
    props: Props,
    options: SpawnOptions,
    deadOwner: string,
  ): Promise<boolean> {
    const won: boolean = await this.#retrying(
      (): Promise<boolean> => this.#registry.relocateActor(name, deadOwner, this.#node),
    );
    if (!won) {
      return false;
    }

    // The record now names this node; hold the name so a build failure or the recipe
    // spawn's own release cannot delete it, and this recreate decides its fate.
    this.#recreating.add(name);
    try {
      await this.#buildLocal(name, props, options);
      return true;
    } catch (error: unknown) {
      await this.#restoreOrphan(name, deadOwner);
      throw error;
    } finally {
      this.#recreating.delete(name);
    }
  }

  /** Returns a half-recreated orphan's record to the dead owner, best effort, so the
   * next sweep re-detects and retries it rather than losing the actor to a build that
   * failed after the record moved. A restore that cannot reach its partition leaves
   * the record naming this node, which the next successful recreate overwrites. */
  async #restoreOrphan(name: string, deadOwner: string): Promise<void> {
    try {
      await this.#retrying(
        (): Promise<boolean> => this.#registry.relocateActor(name, this.#node, deadOwner),
      );
    } catch {
      // A wedged restore must not mask the build failure that triggered it.
    }
  }

  /**
   * Places a departed node's actor on the survivor the coordinator chose, from its
   * recipe: it rebuilds and recreates here when `owner` is this node, or ships the
   * recreate to `owner` over remoting otherwise. Resolves whether that node took the
   * name, `false` when the record no longer named the dead node. The recreate is
   * always gated on the record still naming `deadOwner`, on whichever node performs it.
   */
  async relocateTo(
    owner: string,
    name: string,
    recipe: RecreateRecipe,
    singleton: boolean,
    deadOwner: string,
  ): Promise<boolean> {
    if (owner === this.#node) {
      const rebuilt: { props: Props; options: SpawnOptions } = recipeToSpawn(recipe, singleton);
      return this.recreate(name, rebuilt.props, rebuilt.options, deadOwner);
    }

    const remote: ClusterPlacementRemote | undefined = this.#remote;
    if (remote === undefined) {
      throw new Error("cluster placement was constructed without remote spawn support");
    }

    const remotingAddress: string | undefined = remote.remotingAddressOf(owner);
    if (remotingAddress === undefined) {
      throw new Error(`survivor ${owner} advertises no remoting endpoint`);
    }

    const { host, port }: { host: string; port: number } = parseHostPort(remotingAddress);
    return remote.recreate(host, port, name, recipe, singleton, deadOwner);
  }

  /**
   * Releases `name`: through the wrapped placement once it is booted, so the
   * pool's own release notification deletes the registry record, or directly
   * when no placement has booted and the name is only a cluster claim.
   */
  free(name: string): void {
    const inner: Placement | null = this.#inner;
    if (inner !== null) {
      inner.free(name);
      return;
    }

    void this.#removeRecord(name);
  }

  /** Resolves `name` to a handle: a node-local one from the wrapped placement, or
   * a routed handle to the node that owns it from the resolver, or undefined when
   * neither holds it. This is the synchronous seam behind location-transparent
   * `actorOf`; it never blocks on the network. */
  find(name: string): PID | undefined {
    return this.#inner?.find(name) ?? this.#resolver?.find(name);
  }

  /** Resolves `name` to a handle for the node that owns it, warming the resolver
   * view from the registry first, for a caller that must reach an instance it lost
   * the claim to. A local instance answers at once; otherwise the view is warmed
   * and the routed handle returned, or undefined when no live instance holds it. */
  async resolveActor(name: string): Promise<PID | undefined> {
    const local: PID | undefined = this.#inner?.find(name);
    if (local !== undefined) {
      return local;
    }

    await this.#resolver?.resolve(name);
    return this.find(name);
  }

  /** The wrapped placement's route to `name`, or undefined as {@link find}. */
  routeOf(name: string): IsolateRoute | undefined {
    return this.#inner?.routeOf(name);
  }

  /** Restarts the placed actor in place through the wrapped placement; a name no
   * booted placement holds is already stopped, so the order is a quiet no-op. */
  respawn(name: string): Promise<void> {
    const inner: Placement | null = this.#inner;
    return inner === null ? Promise.resolve() : inner.respawn(name);
  }

  /** Stops the placed actor through the wrapped placement; a no-op before boot. */
  stopActor(name: string): Promise<void> {
    const inner: Placement | null = this.#inner;
    return inner === null ? Promise.resolve() : inner.stopActor(name);
  }

  /** Gathers each worker's metrics through the wrapped placement; none
   * before boot, when no worker isolate exists yet. */
  collectMetrics(): Promise<(IsolateMetrics | null)[]> {
    const inner: Placement | null = this.#inner;
    return inner === null ? Promise.resolve([]) : inner.collectMetrics();
  }

  /** Tears the wrapped placement down and blocks further record deletions. */
  async stop(): Promise<void> {
    this.#stopped = true;
    const pending: Promise<Placement> | null = this.#innerBoot;
    if (pending !== null) {
      // A placement is mid-boot: let its pool finish so this stop tears it down,
      // rather than a pool booting after teardown and leaking its isolates.
      await pending.catch((): null => null);
    }

    const inner: Placement | null = this.#inner;
    this.#inner = null;
    this.#innerBoot = null;
    if (inner !== null) {
      await inner.stop();
    }
  }

  /** Boots the wrapped placement once, wiring its releases to record deletion. A
   * boot that fails is not cached, so the next placement retries from scratch
   * rather than every later spawn inheriting one transient failure. */
  #ensureInner(): Promise<Placement> {
    const inner: Placement | null = this.#inner;
    if (inner !== null) {
      return Promise.resolve(inner);
    }

    this.#innerBoot ??= this.#bootInner((name: string): void => this.#onReleased(name))
      .then((booted: Placement): Placement => {
        this.#inner = booted;
        return booted;
      })
      .catch((error: unknown): never => {
        this.#innerBoot = null;
        throw error;
      });
    return this.#innerBoot;
  }

  /** The wrapped pool's release notification: delete the freed name's record. */
  #onReleased(name: string): void {
    void this.#removeRecord(name);
  }

  /** Deletes `name`'s placement and companion records, best effort: a delete that
   * cannot reach its partition leaves the name held until the record is overwritten
   * or its owner departs, and a teardown release writes nothing. A release for a name
   * being recreated writes nothing either, so a build failure mid-recreate cannot
   * delete the record the recreate is about to hand back to the dead owner. The
   * companion is removed alongside the placement so a freed relocatable name leaves no
   * recipe behind; a name that never had one deletes an absent key, a harmless no-op. */
  async #removeRecord(name: string): Promise<void> {
    if (this.#stopped || this.#recreating.has(name)) {
      return;
    }

    const hadCompanion: boolean = this.#companions.delete(name);
    try {
      await this.#registry.removeActor(name);
      if (hadCompanion) {
        await this.#registry.removeCompanion(name);
      }
    } catch {
      // A wedged delete must not surface as an unhandled rejection from a
      // synchronous free or a pool release; the name is reclaimed by a later
      // overwrite or by its owner departing, and its companion by the same sweep.
    }
  }

  /** Builds `name` on one of this node's isolates through the wrapped placement,
   * the shared local tail of {@link place} and {@link placeOn}. The boot is the
   * one failure the pool cannot clean up after, because it never received the
   * name, so its record is deleted here for that case alone; a build failure once
   * the pool owns the name is freed by the pool, whose release deletes the record,
   * and a second delete here would race a reused name's fresh claim. */
  async #buildLocal(name: string, props: Props, options?: SpawnOptions): Promise<PID> {
    let inner: Placement;
    try {
      inner = await this.#ensureInner();
    } catch (error: unknown) {
      await this.#removeRecord(name);
      throw error;
    }

    // A relocatable actor records its recipe beside its placement before it builds,
    // so a survivor can recreate it if this node departs. A companion write that
    // outlives its retries rolls the claim back; a later build failure frees both
    // through the pool's release.
    try {
      await this.#writeCompanion(name, props, options);
    } catch (error: unknown) {
      await this.#removeRecord(name);
      throw error;
    }

    // From here the recipe spawn may re-enter claim() for this name; the guard
    // holds until the actor is built, then releases whether it succeeded or not.
    this.#placing.add(name);
    try {
      return await inner.place(name, props, options);
    } finally {
      this.#placing.delete(name);
    }
  }

  /** Records the companion recipe of a relocatable `name` beside its placement, so a
   * survivor can rebuild it after this node departs, and does nothing for a
   * non-relocatable one. Whether the actor relocates is its spawn option over the
   * system default; a singleton carries a marker so recovery re-pins it to the
   * coordinator. The write absorbs the registry's retryable refusals like a claim. */
  async #writeCompanion(name: string, props: Props, options?: SpawnOptions): Promise<void> {
    const relocatable: boolean = options?.relocatable ?? this.#relocationDefault;
    if (!relocatable) {
      return;
    }

    const companion: Companion = {
      recipe: placedRecipe(props, options),
      singleton: options?.singleton ?? false,
    };
    const record: Uint8Array = encodeCompanion(companion);
    await this.#retrying((): Promise<void> => this.#registry.putCompanion(name, record));
    this.#companions.add(name);
  }

  /** Ships `name` to the node that owns it as a remote spawn, returning a routed
   * handle. The owner claims and builds the name as its own local placement, so a
   * failure here writes and cleans up nothing: this node holds no record for a name
   * it does not own, and a rejected remote spawn frees the name on the owner. */
  async #buildRemote(
    name: string,
    props: Props,
    owner: string,
    options?: SpawnOptions,
  ): Promise<PID> {
    const remote: ClusterPlacementRemote | undefined = this.#remote;
    if (remote === undefined) {
      throw new Error("cluster placement was constructed without remote spawn support");
    }

    const remotingAddress: string | undefined = remote.remotingAddressOf(owner);
    if (remotingAddress === undefined) {
      throw new Error(`selected owner ${owner} advertises no remoting endpoint`);
    }

    const { host, port }: { host: string; port: number } = parseHostPort(remotingAddress);
    return remote.spawn(host, port, name, props, options);
  }

  /** Claims `name` for `owner`, retrying a retryable refusal with bounded backoff
   * so the caller sees only a win, a loss, or a lasting failure. */
  #claim(name: string, owner: string): Promise<boolean> {
    return this.#retrying((): Promise<boolean> => this.#registry.claimActorName(name, owner));
  }

  /** Runs a registry write under the claim's bounded retry: a rebalancing partition
   * or a briefly unavailable half is retried with capped backoff, and any other
   * failure, or one that outlives the retries, is raised to the caller. */
  async #retrying<T>(op: () => Promise<T>): Promise<T> {
    for (let attempt: number = 0; ; attempt++) {
      try {
        return await op();
      } catch (error: unknown) {
        if (attempt >= CLAIM_RETRY_ATTEMPTS || !isRetryable(error)) {
          throw error;
        }

        await this.#sleep(backoffDelay(attempt));
      }
    }
  }
}
