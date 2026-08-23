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

import type { PID } from "./pid";

/**
 * PIDTree is the hierarchy of one actor system: parent/child links and
 * death-watch registrations.
 *
 * Lookups that run at spawn, watch, and stop take a PID. Children and
 * watch sets live here, allocated only for actors that actually have
 * them. An idle leaf is a membership flag on the PID; it is not wrapped
 * in a second node object and is not stored in a global string map.
 *
 * The tree is touched only when actors are spawned, watched, or stopped;
 * message processing never involves it. Deleting an actor removes its
 * whole subtree iteratively (no recursion) and drops the tree's
 * references to those actors. Replacing a registration detaches that
 * actor without taking its children.
 *
 * @internal
 */
export class PidTree {
  /** Unparented actors in this tree: the true root plus any actor whose
   * spawn parent was not registered. */
  private readonly forest = new Set<PID>();
  /** Direct children keyed by name; present only for actors that have any. */
  private readonly childrenOf = new Map<PID, Map<string, PID>>();
  /** Watchers of an actor; present only after the first watch. */
  private readonly watchersOf = new Map<PID, Set<PID>>();
  /** Actors an actor watches; present only after the first watch. */
  private readonly watcheesOf = new Map<PID, Set<PID>>();
  /** The first unparented actor registered; the guardian hierarchy's root. */
  private _root: PID | null = null;
  private _count = 0;

  /**
   * Registers an actor, attaching it under `parent` when one is given and
   * the parent is registered. Registering under a name that is already
   * taken replaces the previous actor at that name.
   */
  addNode(parent: PID | null, pid: PID): void {
    const host = parent?.isInTree() ? parent : null;

    if (host !== null) {
      const existing = this.namedChild(host, pid.name());
      if (existing !== undefined) {
        this.detach(existing);
      }

      this.adopt(host, pid);
    } else {
      for (const root of this.forest) {
        if (root.path().equals(pid.path())) {
          this.detach(root);
          break;
        }
      }

      this.forest.add(pid);
      this._root ??= pid;
    }

    pid.setInTree(true);
    this._count++;
  }

  /**
   * Removes an actor and its entire subtree, cleaning every watch
   * registration the removed actors appear in.
   */
  deleteNode(pid: PID): void {
    if (!pid.isInTree()) {
      return;
    }

    const stack: PID[] = [pid];
    const post: PID[] = [];

    while (stack.length > 0) {
      const n = stack.pop() as PID;
      post.push(n);

      for (const child of this.kids(n)) {
        stack.push(child);
      }
    }

    for (let i = post.length - 1; i >= 0; i--) {
      this.unlink(post[i] as PID);
    }
  }

  /** Returns the actor registered at the given canonical path string. */
  get(id: string): PID | undefined {
    for (const pid of this.nodes()) {
      if (pid.path().toString() === id) {
        return pid;
      }
    }

    return undefined;
  }

  /** Returns the actor registered under the given name. When several
   * actors share a name, the last one visited in a depth-first walk of
   * the forest wins. */
  getByName(name: string): PID | undefined {
    let found: PID | undefined;

    for (const pid of this.nodes()) {
      if (pid.name() === name) {
        found = pid;
      }
    }

    return found;
  }

  /** Returns `parent`'s child registered under `name`. */
  child(parent: PID, name: string): PID | undefined {
    if (!parent.isInTree()) {
      return undefined;
    }

    return this.namedChild(parent, name);
  }

  /** Returns the direct children of the given actor. */
  children(pid: PID): PID[] {
    if (!pid.isInTree()) {
      return [];
    }

    return this.kids(pid);
  }

  /** Returns the parent of the given actor in this tree. Spawn parent
   * alone is not enough: the child must have been adopted. */
  parent(pid: PID): PID | undefined {
    if (!pid.isInTree()) {
      return undefined;
    }

    const parent = pid.parent();
    if (parent === undefined || !parent.isInTree() || this.namedChild(parent, pid.name()) !== pid) {
      return undefined;
    }

    return parent;
  }

  /** Returns the root actor of the hierarchy. */
  root(): PID | undefined {
    return this._root ?? undefined;
  }

  /** Returns every registered actor, in no particular order. */
  nodes(): PID[] {
    const out: PID[] = [];
    const seen = new Set<PID>();
    const stack: PID[] = [];

    if (this._root !== null) {
      stack.push(this._root);
    }

    for (const pid of this.forest) {
      stack.push(pid);
    }

    while (stack.length > 0) {
      const pid = stack.pop() as PID;
      if (seen.has(pid) || !pid.isInTree()) {
        continue;
      }

      seen.add(pid);
      out.push(pid);

      for (const child of this.kids(pid)) {
        stack.push(child);
      }
    }

    return out;
  }

  /** Returns the siblings of the given actor: its parent's other
   * children. */
  siblings(pid: PID): PID[] {
    const parent = this.parent(pid);
    if (parent === undefined) {
      return [];
    }

    const out: PID[] = [];

    for (const sibling of this.kids(parent)) {
      if (sibling !== pid) {
        out.push(sibling);
      }
    }

    return out;
  }

  /** Returns every descendant of the given actor, depth first with no
   * order guarantee. */
  descendants(pid: PID): PID[] {
    if (!pid.isInTree()) {
      return [];
    }

    const stack = this.kids(pid);
    const out: PID[] = [];

    while (stack.length > 0) {
      const n = stack.pop() as PID;
      out.push(n);

      for (const child of this.kids(n)) {
        stack.push(child);
      }
    }

    return out;
  }

  /**
   * Registers `watcher` to be notified when `watched` stops. A no-op when
   * `watched` is not in the tree.
   */
  addWatcher(watched: PID, watcher: PID): void {
    if (!watched.isInTree()) {
      return;
    }

    this.addToSet(this.watchersOf, watched, watcher);

    if (watcher.isInTree()) {
      this.addToSet(this.watcheesOf, watcher, watched);
    }
  }

  /** Cancels a {@link addWatcher} registration. */
  removeWatcher(watched: PID, watcher: PID): void {
    this.dropFromSet(this.watchersOf, watched, watcher);
    this.dropFromSet(this.watcheesOf, watcher, watched);
  }

  /** Returns the actors watching the given actor. */
  watchers(pid: PID): PID[] {
    if (!pid.isInTree()) {
      return [];
    }

    return this.members(this.watchersOf, pid);
  }

  /** Returns the actors the given actor watches. */
  watchees(pid: PID): PID[] {
    if (!pid.isInTree()) {
      return [];
    }

    return this.members(this.watcheesOf, pid);
  }

  /** Returns the number of registered actors. */
  count(): number {
    return this._count;
  }

  /** Empties the tree. */
  reset(): void {
    for (const pid of this.nodes()) {
      pid.setInTree(false);
    }

    this.forest.clear();
    this.childrenOf.clear();
    this.watchersOf.clear();
    this.watcheesOf.clear();
    this._root = null;
    this._count = 0;
  }

  private namedChild(parent: PID, name: string): PID | undefined {
    return this.childrenOf.get(parent)?.get(name);
  }

  private kids(pid: PID): PID[] {
    const children = this.childrenOf.get(pid);
    return children === undefined ? [] : [...children.values()];
  }

  private adopt(host: PID, pid: PID): void {
    let children = this.childrenOf.get(host);
    if (children === undefined) {
      children = new Map();
      this.childrenOf.set(host, children);
    }

    children.set(pid.name(), pid);
  }

  private members(index: Map<PID, Set<PID>>, pid: PID): PID[] {
    const set = index.get(pid);
    return set === undefined ? [] : [...set];
  }

  private addToSet(index: Map<PID, Set<PID>>, key: PID, value: PID): void {
    let set = index.get(key);
    if (set === undefined) {
      set = new Set();
      index.set(key, set);
    }

    set.add(value);
  }

  private dropFromSet(index: Map<PID, Set<PID>>, key: PID, value: PID): void {
    const set = index.get(key);
    if (set === undefined) {
      return;
    }

    set.delete(value);
    if (set.size === 0) {
      index.delete(key);
    }
  }

  /** Unlinks a node being replaced: it leaves the tree without taking
   * its children with it. */
  private detach(pid: PID): void {
    this.dropFromParent(pid);
    pid.setInTree(false);
    this._count--;

    if (this._root === pid) {
      this._root = null;
    }
  }

  /** Removes one in-tree actor: watch cleanup, unlink from parent. */
  private unlink(pid: PID): void {
    for (const watcher of this.members(this.watchersOf, pid)) {
      this.dropFromSet(this.watcheesOf, watcher, pid);
    }

    for (const watchee of this.members(this.watcheesOf, pid)) {
      this.dropFromSet(this.watchersOf, watchee, pid);
    }

    this.watchersOf.delete(pid);
    this.watcheesOf.delete(pid);
    this.childrenOf.delete(pid);
    this.dropFromParent(pid);
    pid.setInTree(false);
    this._count--;

    if (this._root === pid) {
      this._root = null;
    }
  }

  private dropFromParent(pid: PID): void {
    const parent = pid.parent();
    const children = parent === undefined ? undefined : this.childrenOf.get(parent);
    if (parent !== undefined && children?.get(pid.name()) === pid) {
      children.delete(pid.name());
      if (children.size === 0) {
        this.childrenOf.delete(parent);
      }
    } else {
      this.forest.delete(pid);
    }
  }
}
