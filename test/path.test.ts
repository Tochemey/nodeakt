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
import { addressOf, newPath, parsePath } from "../src/actor/path";

describe("newPath", () => {
  it("exposes its constituents", () => {
    const path = newPath("user-1", "sys", "127.0.0.1", 9000);
    expect(path.name()).toBe("user-1");
    expect(path.system()).toBe("sys");
    expect(path.host()).toBe("127.0.0.1");
    expect(path.port()).toBe(9000);
    expect(path.hostPort()).toBe("127.0.0.1:9000");
    expect(path.parent()).toBeUndefined();
  });

  it("renders the canonical string form", () => {
    const path = newPath("user-1", "sys", "127.0.0.1", 9000);
    expect(path.toString()).toBe("nodeakt://sys@127.0.0.1:9000/user-1");
  });

  it("includes the parent name in the string form", () => {
    const parent = newPath("users", "sys", "127.0.0.1", 9000);
    const child = newPath("user-1", "sys", "127.0.0.1", 9000, parent);
    expect(child.toString()).toBe("nodeakt://sys@127.0.0.1:9000/users/user-1");
    expect(child.parent()).toBe(parent);
  });

  it("encodes the full ancestor chain in the string form", () => {
    const alice = newPath("alice", "sys", "127.0.0.1", 9000);
    const aliceChild = newPath("child", "sys", "127.0.0.1", 9000, alice);
    const aliceGrand = newPath("g", "sys", "127.0.0.1", 9000, aliceChild);
    expect(aliceGrand.toString()).toBe("nodeakt://sys@127.0.0.1:9000/alice/child/g");

    // Same local names under a different grandparent produce a distinct
    // canonical path, so deep hierarchies never collide.
    const bob = newPath("bob", "sys", "127.0.0.1", 9000);
    const bobChild = newPath("child", "sys", "127.0.0.1", 9000, bob);
    const bobGrand = newPath("g", "sys", "127.0.0.1", 9000, bobChild);
    expect(bobGrand.toString()).toBe("nodeakt://sys@127.0.0.1:9000/bob/child/g");
    expect(aliceGrand.equals(bobGrand)).toBe(false);
  });

  it("mints a fresh uid per path", () => {
    const a = newPath("a", "sys", "127.0.0.1", 9000);
    const b = newPath("a", "sys", "127.0.0.1", 9000);
    expect(a.uid()).not.toBe("");
    expect(a.uid()).not.toBe(b.uid());
  });

  it("rejects invalid names and systems", () => {
    expect(() => newPath("", "sys", "h", 1)).toThrow(TypeError);
    expect(() => newPath("-bad", "sys", "h", 1)).toThrow(TypeError);
    expect(() => newPath("has space", "sys", "h", 1)).toThrow(TypeError);
    expect(() => newPath("a".repeat(256), "sys", "h", 1)).toThrow(RangeError);
    expect(() => newPath("ok", "", "h", 1)).toThrow(TypeError);
    expect(() => newPath("ok", "bad sys", "h", 1)).toThrow(TypeError);
  });

  it("rejects an invalid host or port", () => {
    expect(() => newPath("ok", "sys", "", 1)).toThrow(TypeError);
    expect(() => newPath("ok", "sys", "h", -1)).toThrow(RangeError);
    expect(() => newPath("ok", "sys", "h", 65536)).toThrow(RangeError);
    expect(() => newPath("ok", "sys", "h", 1.5)).toThrow(RangeError);
  });

  it("rejects a parent from another system, node, or with the same name", () => {
    const parent = newPath("users", "sys", "127.0.0.1", 9000);
    expect(() => newPath("user-1", "other", "127.0.0.1", 9000, parent)).toThrow(TypeError);
    expect(() => newPath("user-1", "sys", "10.0.0.1", 9000, parent)).toThrow(TypeError);
    expect(() => newPath("user-1", "sys", "127.0.0.1", 9001, parent)).toThrow(TypeError);
    expect(() => newPath("users", "sys", "127.0.0.1", 9000, parent)).toThrow(TypeError);
  });

  it("matches the parent's system case-insensitively", () => {
    const parent = newPath("users", "SYS", "127.0.0.1", 9000);
    const child = newPath("user-1", "sys", "127.0.0.1", 9000, parent);
    expect(child.parent()).toBe(parent);
    expect(child.system()).toBe("SYS");
    expect(addressOf(child)).toBe(addressOf(parent));
  });
});

describe("equals and sameUid", () => {
  it("equals compares name and location, ignoring uid", () => {
    const a = newPath("a", "sys", "127.0.0.1", 9000);
    const b = newPath("a", "sys", "127.0.0.1", 9000);
    expect(a.equals(b)).toBe(true);
    expect(a.sameUid(b)).toBe(false);
  });

  it("distinguishes different names, systems, and endpoints", () => {
    const base = newPath("a", "sys", "127.0.0.1", 9000);
    expect(base.equals(newPath("b", "sys", "127.0.0.1", 9000))).toBe(false);
    expect(base.equals(newPath("a", "other", "127.0.0.1", 9000))).toBe(false);
    expect(base.equals(newPath("a", "sys", "127.0.0.1", 9001))).toBe(false);
  });

  it("sameUid requires a matching non-empty uid", () => {
    const created = newPath("a", "sys", "127.0.0.1", 9000);
    const restored = parsePath(created.toString(), created.uid());
    const reference = parsePath(created.toString());

    expect(created.sameUid(restored)).toBe(true);
    expect(created.sameUid(reference)).toBe(false);
    expect(reference.sameUid(reference)).toBe(false);
  });

  it("equals is reflexive without building the canonical string", () => {
    const path = newPath("a", "sys", "127.0.0.1", 9000);
    expect(path.equals(path)).toBe(true);
  });

  it("rejects a restored uid that is not a positive integer", () => {
    expect(() => parsePath("nodeakt://sys@127.0.0.1:9000/a", "not-an-id")).toThrow(TypeError);
    expect(() => parsePath("nodeakt://sys@127.0.0.1:9000/a", "0")).toThrow(TypeError);
    expect(() => parsePath("nodeakt://sys@127.0.0.1:9000/a", "01")).toThrow(TypeError);
  });
});

describe("parsePath", () => {
  it("round-trips a root path", () => {
    const original = newPath("user-1", "sys", "127.0.0.1", 9000);
    const parsed = parsePath(original.toString());

    expect(parsed.equals(original)).toBe(true);
    expect(parsed.name()).toBe("user-1");
    expect(parsed.system()).toBe("sys");
    expect(parsed.host()).toBe("127.0.0.1");
    expect(parsed.port()).toBe(9000);
    expect(parsed.uid()).toBe("");
  });

  it("round-trips a path with a parent segment", () => {
    const parent = newPath("users", "sys", "127.0.0.1", 9000);
    const child = newPath("user-1", "sys", "127.0.0.1", 9000, parent);
    const parsed = parsePath(child.toString());

    expect(parsed.equals(child)).toBe(true);
    expect(parsed.parent()?.name()).toBe("users");
    expect(parsed.parent()?.system()).toBe("sys");
    expect(parsed.parent()?.hostPort()).toBe("127.0.0.1:9000");
  });

  it("round-trips a full ancestor chain", () => {
    const parsed = parsePath("nodeakt://sys@127.0.0.1:9000/alice/child/g");

    expect(parsed.toString()).toBe("nodeakt://sys@127.0.0.1:9000/alice/child/g");
    expect(parsed.name()).toBe("g");
    expect(parsed.parent()?.name()).toBe("child");
    expect(parsed.parent()?.parent()?.name()).toBe("alice");
    expect(parsed.parent()?.parent()?.parent()).toBeUndefined();
    expect(parsed.uid()).toBe("");
  });

  it("rejects malformed strings", () => {
    for (const bad of [
      "",
      "user-1",
      "goakt://sys@h:1/name",
      "nodeakt://sys@h/name",
      "nodeakt://sys@h:x/name",
      "nodeakt://@h:1/name",
      "nodeakt://sys@h:1/",
      "nodeakt://sys@h:1//name",
      "nodeakt://sys@h:1/a//c",
    ]) {
      expect(() => parsePath(bad), bad).toThrow(TypeError);
    }
  });

  it("validates constituents like newPath", () => {
    expect(() => parsePath("nodeakt://sys@h:99999/name")).toThrow(RangeError);
    expect(() => parsePath("nodeakt://sys@h:1/-bad")).toThrow(TypeError);
  });
});
