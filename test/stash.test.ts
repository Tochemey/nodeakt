import { describe, expect, it } from "vitest";
import { ErrMailboxDisposed } from "../src/actor/errors";
import { createReceiveContext, type ReceiveContext } from "../src/actor/receive.context";
import { Stash } from "../src/actor/stash";

function ctx(message: unknown): ReceiveContext {
  return createReceiveContext(message);
}

describe("Stash", () => {
  it("starts empty", () => {
    const stash = new Stash();
    expect(stash.isEmpty()).toBe(true);
    expect(stash.len()).toBe(0);
    expect(stash.unstash()).toBeUndefined();
    expect(stash.unstashAll()).toEqual([]);
  });

  it("unstashes the oldest message first", () => {
    const stash = new Stash();
    expect(stash.stash(ctx(1))).toBeNull();
    expect(stash.stash(ctx(2))).toBeNull();
    expect(stash.stash(ctx(3))).toBeNull();
    expect(stash.len()).toBe(3);

    expect(stash.unstash()?.message).toBe(1);
    expect(stash.unstash()?.message).toBe(2);
    expect(stash.len()).toBe(1);
  });

  it("unstashAll returns everything in arrival order and empties the buffer", () => {
    const stash = new Stash();
    for (let i = 0; i < 50; i++) {
      stash.stash(ctx(i));
    }

    const all = stash.unstashAll().map((c) => c.message);
    expect(all).toEqual([...Array(50).keys()]);
    expect(stash.isEmpty()).toBe(true);
    expect(stash.unstashAll()).toEqual([]);
  });

  it("is reusable after being drained", () => {
    const stash = new Stash();
    stash.stash(ctx("a"));
    stash.unstashAll();

    expect(stash.stash(ctx("b"))).toBeNull();
    expect(stash.unstash()?.message).toBe("b");
  });

  it("rejects stash and drops messages after dispose", () => {
    const stash = new Stash();
    stash.stash(ctx(1));
    stash.dispose();

    expect(stash.stash(ctx(2))).toBe(ErrMailboxDisposed);
    expect(stash.unstash()).toBeUndefined();
    expect(stash.isEmpty()).toBe(true);
  });
});
