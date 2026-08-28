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

import type { ClusterRegistry } from "./clustering.registry";
import { parseHostPort } from "./clustering.transport";
import { newPathAt, type Path } from "./path";
import type { PID } from "./pid";

/** Construction parameters for a {@link ClusterResolver}. @internal */
export interface ClusterResolverOptions {
  /** The distributed directory a name's owner is read from to warm the view. */
  readonly registry: ClusterRegistry;

  /** Mints the wire-backed handle of an actor at a remote path, or undefined for
   * a path of this node; the remoting layer's own foreign-handle seam. */
  readonly handleFor: (path: Path) => PID | undefined;

  /** Maps an owner's cluster identity to the remoting endpoint its actors are
   * reached at, or undefined when the owner is not a present member. */
  readonly remotingAddressOf: (owner: string) => string | undefined;

  /** This node's cluster identity, so a name owned here is not routed remotely. */
  readonly self: string;

  /** The actor system's name, the authority segment of every remote path. */
  readonly systemName: string;
}

/**
 * Resolves a top-level name owned by another node to a handle that delivers to
 * it, from a warm local view of who owns what.
 *
 * The view is a cache of name to owning node, warmed asynchronously from the
 * registry: a lookup never blocks on the network, so a name this node has not yet
 * learned reads as undefined while a background read fills the view, and the next
 * lookup resolves it. A resolved name maps to its owner's remoting endpoint, and
 * the remoting layer's own foreign-handle cache turns that endpoint into a routed
 * handle whose tells, asks, watches, and replies cross the network exactly as the
 * local forms do.
 *
 * It resolves names owned elsewhere only; a name owned by this node is served by
 * the local tree and the worker pool before the resolver is consulted.
 *
 * @internal
 */
export class ClusterResolver {
  readonly #registry: ClusterRegistry;
  readonly #handleFor: (path: Path) => PID | undefined;
  readonly #remotingAddressOf: (owner: string) => string | undefined;
  readonly #self: string;
  readonly #systemName: string;

  /** Warm view of a top-level name to the cluster identity that owns it. */
  readonly #view: Map<string, string> = new Map();
  /** The in-flight registry read per name, so a burst of lookups fans out one read
   * and a caller that awaits {@link resolve} waits for that read to finish. */
  readonly #resolving: Map<string, Promise<void>> = new Map();

  constructor(options: ClusterResolverOptions) {
    this.#registry = options.registry;
    this.#handleFor = options.handleFor;
    this.#remotingAddressOf = options.remotingAddressOf;
    this.#self = options.self;
    this.#systemName = options.systemName;
  }

  /**
   * Resolves `name` to a routed handle for the node that owns it, or undefined
   * when the view has not yet learned the name, the name is owned by this node,
   * or its owner is no longer a present member. A miss starts a background read
   * that warms the view, so a later lookup of the same name resolves it without
   * this one ever blocking on the network.
   */
  find(name: string): PID | undefined {
    const owner: string | undefined = this.#view.get(name);
    if (owner === undefined) {
      void this.resolve(name);
      return undefined;
    }

    if (owner === this.#self) {
      // Owned here per the view, but the local tree and pool already came up
      // empty: the entry is stale (the actor stopped, or re-homed to another
      // node). Drop it and re-resolve, so a re-home is picked up rather than read
      // as absent forever.
      this.#view.delete(name);
      void this.resolve(name);
      return undefined;
    }

    const remotingAddress: string | undefined = this.#remotingAddressOf(owner);
    if (remotingAddress === undefined) {
      // The cached owner is no longer a present member: it departed, and a
      // relocatable actor it held has been recreated on a survivor. Drop the stale
      // entry and re-resolve, so the next lookup reaches the actor's new owner
      // rather than reading it as absent forever.
      this.#view.delete(name);
      void this.resolve(name);
      return undefined;
    }

    return this.#handleOf(name, remotingAddress);
  }

  /**
   * Reads `name`'s current owner from the registry into the view, or drops the entry
   * when no owner holds it, and resolves once that read has settled. One read per
   * name is in flight at a time, so a burst of lookups for a cold name collapses to a
   * single registry read that they all await, which is what lets a caller warm the
   * view and then find the routed handle.
   */
  resolve(name: string): Promise<void> {
    const inflight: Promise<void> | undefined = this.#resolving.get(name);
    if (inflight !== undefined) {
      return inflight;
    }

    const pending: Promise<void> = this.#read(name).finally((): void => {
      this.#resolving.delete(name);
    });
    this.#resolving.set(name, pending);
    return pending;
  }

  /** Reads `name`'s owner into the view, or drops the entry when none holds it; a read
   * that cannot reach its partition leaves the view untouched for the next lookup. */
  async #read(name: string): Promise<void> {
    try {
      const owner: string | undefined = await this.#registry.getActor(name);
      if (owner === undefined) {
        this.#view.delete(name);
        return;
      }

      this.#view.set(name, owner);
    } catch {
      // Swallow: the next lookup starts a fresh read against the partition.
    }
  }

  /**
   * The routed handle for `name` at `remotingAddress`, or undefined when the
   * address cannot be parsed into a reachable path. The remoting endpoint is a
   * field decoded from a peer's gossiped metadata, validated only for length, so
   * a peer advertising a malformed endpoint must collapse to "not reachable"
   * rather than throw out of the synchronous lookup.
   */
  #handleOf(name: string, remotingAddress: string): PID | undefined {
    let path: Path;
    try {
      const { host, port }: { host: string; port: number } = parseHostPort(remotingAddress);
      path = newPathAt(name, { system: this.#systemName, host, port }, undefined, "");
    } catch {
      return undefined;
    }

    return this.#handleFor(path);
  }
}
