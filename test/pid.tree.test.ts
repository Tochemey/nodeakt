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

import { describe, expect, it } from "vitest";
import type { Actor } from "../src/actor";
import type { ActorSystem } from "../src/actor.system";
import { newPath } from "../src/path";
import { PID } from "../src/pid";
import { PidTree } from "../src/pid.tree";

const system = { metricRegistry: () => null } as unknown as ActorSystem;

const noop: Actor = {
  preStart(): void {},
  receive(): void {},
  postStop(): void {},
};

function pidAt(name: string, parent?: PID): PID {
  return new PID(
    noop,
    newPath(name, "sys", "127.0.0.1", 0, parent?.path()),
    system,
    undefined,
    parent,
  );
}

describe("PIDTree", () => {
  it("indexes actors by path and by name", () => {
    const tree = new PidTree();
    const root = pidAt("root");
    tree.addNode(null, root);

    expect(tree.count()).toBe(1);
    expect(tree.get(root.path().toString())).toBe(root);
    expect(tree.getByName("root")).toBe(root);
    expect(tree.parent(root)).toBeUndefined();
  });

  it("links children to their parent", () => {
    const tree = new PidTree();
    const root = pidAt("root");
    const child = pidAt("child", root);
    tree.addNode(null, root);
    tree.addNode(root, child);

    expect(tree.child(root, "child")).toBe(child);
    expect(tree.children(root)).toEqual([child]);
    expect(tree.parent(child)).toBe(root);
    expect(tree.count()).toBe(2);
  });

  it("replaces a registration at the same path", () => {
    const tree = new PidTree();
    const first = pidAt("root");
    const second = pidAt("root");
    tree.addNode(null, first);
    tree.addNode(null, second);

    expect(tree.count()).toBe(1);
    expect(tree.get(second.path().toString())).toBe(second);
  });

  it("deletes a whole subtree", () => {
    const tree = new PidTree();
    const root = pidAt("root");
    const child = pidAt("child", root);
    const grandChild = pidAt("grandchild", child);
    tree.addNode(null, root);
    tree.addNode(root, child);
    tree.addNode(child, grandChild);

    tree.deleteNode(child);

    expect(tree.count()).toBe(1);
    expect(tree.get(child.path().toString())).toBeUndefined();
    expect(tree.get(grandChild.path().toString())).toBeUndefined();
    expect(tree.children(root)).toEqual([]);
    expect(tree.getByName("grandchild")).toBeUndefined();
  });

  it("tracks watchers and watchees symmetrically", () => {
    const tree = new PidTree();
    const watched = pidAt("watched");
    const watcher = pidAt("watcher");
    tree.addNode(null, watched);
    tree.addNode(null, watcher);

    tree.addWatcher(watched, watcher);
    expect(tree.watchers(watched)).toEqual([watcher]);
    expect(tree.watchees(watcher)).toEqual([watched]);

    tree.removeWatcher(watched, watcher);
    expect(tree.watchers(watched)).toEqual([]);
    expect(tree.watchees(watcher)).toEqual([]);
  });

  it("reports watch registration and removal exactly", () => {
    const tree = new PidTree();
    const watched = pidAt("watched");
    const watcher = pidAt("watcher");
    tree.addNode(null, watched);
    tree.addNode(null, watcher);

    // Only the first registration is new; a duplicate reports false so
    // per-registration accounting can never double-count.
    expect(tree.addWatcher(watched, watcher)).toBe(true);
    expect(tree.addWatcher(watched, watcher)).toBe(false);
    expect(tree.watchers(watched)).toEqual([watcher]);

    expect(tree.removeWatcher(watched, watcher)).toBe(true);
    expect(tree.removeWatcher(watched, watcher)).toBe(false);
  });

  it("keeps remaining watch registrations when one is cancelled", () => {
    const tree = new PidTree();
    const left = pidAt("left");
    const right = pidAt("right");
    const a = pidAt("a");
    const b = pidAt("b");
    tree.addNode(null, left);
    tree.addNode(null, right);
    tree.addNode(null, a);
    tree.addNode(null, b);

    tree.addWatcher(left, a);
    tree.addWatcher(left, b);
    tree.addWatcher(right, a);

    expect(tree.watchers(left)).toHaveLength(2);
    expect(tree.watchees(a)).toHaveLength(2);

    tree.removeWatcher(left, a);

    expect(tree.watchers(left)).toEqual([b]);
    expect(tree.watchees(a)).toEqual([right]);
  });

  it("cleans watch registrations when either side is deleted", () => {
    const tree = new PidTree();
    const watched = pidAt("watched");
    const watcher = pidAt("watcher");
    tree.addNode(null, watched);
    tree.addNode(null, watcher);
    tree.addWatcher(watched, watcher);

    // Deleting the watcher must clear it from the watched side.
    tree.deleteNode(watcher);
    expect(tree.watchers(watched)).toEqual([]);

    tree.addNode(null, watcher);
    tree.addWatcher(watched, watcher);

    // Deleting the watched must clear it from the watcher side.
    tree.deleteNode(watched);
    expect(tree.watchees(watcher)).toEqual([]);
  });

  it("exposes root, nodes, siblings, and descendants", () => {
    const tree = new PidTree();
    const root = pidAt("root");
    const a = pidAt("a", root);
    const b = pidAt("b", root);
    const grandChild = pidAt("grandchild", a);
    tree.addNode(null, root);
    tree.addNode(root, a);
    tree.addNode(root, b);
    tree.addNode(a, grandChild);

    expect(tree.root()).toBe(root);
    expect(tree.nodes()).toHaveLength(4);
    expect(tree.siblings(a)).toEqual([b]);
    expect(tree.siblings(root)).toEqual([]);

    const descendants = tree.descendants(root);
    expect(descendants).toHaveLength(3);
    expect(descendants).toContain(a);
    expect(descendants).toContain(b);
    expect(descendants).toContain(grandChild);
    expect(tree.descendants(a)).toEqual([grandChild]);
    expect(tree.descendants(grandChild)).toEqual([]);

    tree.deleteNode(root);
    expect(tree.root()).toBeUndefined();
    expect(tree.nodes()).toEqual([]);
  });

  it("resets to empty", () => {
    const tree = new PidTree();
    const root = pidAt("root");
    tree.addNode(null, root);

    tree.reset();
    expect(tree.count()).toBe(0);
    expect(tree.get(root.path().toString())).toBeUndefined();
  });
});

describe("PIDTree guards", () => {
  it("ignores operations on actors that are not registered", () => {
    const tree = new PidTree();
    const registered = pidAt("registered");
    const stranger = pidAt("stranger");
    tree.addNode(null, registered);

    tree.deleteNode(stranger);
    expect(tree.addWatcher(stranger, registered)).toBe(false);
    expect(tree.removeWatcher(registered, stranger)).toBe(false);

    expect(tree.parent(stranger)).toBeUndefined();
    expect(tree.child(stranger, "x")).toBeUndefined();
    expect(tree.children(stranger)).toEqual([]);
    expect(tree.descendants(stranger)).toEqual([]);
    expect(tree.watchers(stranger)).toEqual([]);
    expect(tree.watchees(stranger)).toEqual([]);
    expect(tree.get("nodeakt://missing@127.0.0.1:0/nobody")).toBeUndefined();
    expect(tree.getByName("nobody")).toBeUndefined();
    expect(tree.count()).toBe(1);
  });

  it("records a watcher that is not itself in the tree", () => {
    const tree = new PidTree();
    const watched = pidAt("watched");
    const stranger = pidAt("stranger");
    tree.addNode(null, watched);

    tree.addWatcher(watched, stranger);

    expect(tree.watchers(watched)).toEqual([stranger]);
    expect(tree.watchees(stranger)).toEqual([]);
  });

  it("does not treat a spawn parent as the tree parent when the child was not adopted", () => {
    const tree = new PidTree();
    const parent = pidAt("parent");
    const child = new PID(
      noop,
      newPath("child", "sys", "127.0.0.1", 0, parent.path()),
      system,
      undefined,
      parent,
    );
    tree.addNode(null, parent);
    tree.addNode(null, child);

    expect(tree.parent(child)).toBeUndefined();
    expect(tree.nodes()).toHaveLength(2);
  });

  it("skips the parent link when the parent is not registered", () => {
    const tree = new PidTree();
    const parent = pidAt("parent");
    const child = pidAt("child", parent);

    tree.addNode(parent, child);

    expect(tree.parent(child)).toBeUndefined();
    expect(tree.get(child.path().toString())).toBe(child);
  });

  it("keeps a superseded name entry when deleting the older holder", () => {
    const tree = new PidTree();
    const root = pidAt("root");
    const left = pidAt("left", root);
    const right = pidAt("right", root);
    const workerLeft = pidAt("worker", left);
    const workerRight = pidAt("worker", right);
    tree.addNode(null, root);
    tree.addNode(root, left);
    tree.addNode(root, right);
    tree.addNode(left, workerLeft);
    tree.addNode(right, workerRight);

    // The name index points at the latest registration; deleting the
    // older holder must leave that entry alone.
    tree.deleteNode(workerLeft);

    expect(tree.getByName("worker")).toBe(workerRight);
    expect(tree.get(workerLeft.path().toString())).toBeUndefined();
  });

  it("replacing a child leaves the rest of the tree in place", () => {
    const tree = new PidTree();
    const root = pidAt("root");
    const left = pidAt("left", root);
    const right = pidAt("right", root);
    const workerLeft = pidAt("worker", left);
    const workerRight = pidAt("worker", right);
    const replacement = pidAt("worker", left);
    tree.addNode(null, root);
    tree.addNode(root, left);
    tree.addNode(root, right);
    tree.addNode(left, workerLeft);
    tree.addNode(right, workerRight);

    tree.addNode(left, replacement);

    expect(tree.root()).toBe(root);
    expect(tree.child(left, "worker")).toBe(replacement);
    expect(tree.child(right, "worker")).toBe(workerRight);
    expect(tree.get(workerLeft.path().toString())).toBe(replacement);
  });
});
