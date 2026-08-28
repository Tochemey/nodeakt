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
import type { IsolateRoute } from "../src/actor.ref";
import {
  ClusterPlacement,
  type ClusterPlacementRemote,
  timerSleep,
} from "../src/clustering.placement";
import type { RecreateRecipe } from "../src/clustering.recreate";
import { ClusterRegistry, type RegistryStore } from "../src/clustering.registry";
import type { ClusterResolver } from "../src/clustering.resolver";
import { ErrActorAlreadyExists } from "../src/errors";
import { PutCondition, RejectionReason, WriteKind } from "../src/kv/discriminants";
import { ClusterUnavailableError, PartitionRebalancingError } from "../src/kv/errors";
import type { Entry, ScanEntry, WriteApplied, WriteOp, WriteResult } from "../src/kv/ports";
import type { PID } from "../src/pid";
import type { Placement } from "../src/placement";
import type { Props } from "../src/props";
import type { SpawnOptions } from "../src/spawn.options";

const NODE: string = "10.0.0.7:8080";
const FAKE_PID: PID = {} as PID;
const FAKE_ROUTE: IsolateRoute = {} as IsolateRoute;
const FAKE_PROPS: Props = {} as Props;

/** Whether two byte arrays are equal, for the fake store's compare-and-set. */
function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte: number, i: number): boolean => byte === b[i]);
}

/** A stored entry the fake store hands back; only its value is read by callers. */
function entryOf(key: string, value: Uint8Array): Entry {
  return {
    key,
    value,
    timestamp: { wallMs: 0, logical: 0, node: "test" },
    sequence: 0n,
    expiresAt: undefined,
    deleted: false,
  };
}

/** An applied write result over a minimal entry. */
function appliedResult(key: string, value: Uint8Array): WriteApplied {
  return { applied: true, entry: entryOf(key, value) };
}

/**
 * An in-memory registry store: absent-only puts model the name claim, deletes
 * model record removal, and a queue of failures drives the retryable and fatal
 * refusals a claim must handle.
 */
class FakeStore implements RegistryStore {
  readonly values: Map<string, Uint8Array> = new Map();
  readonly failures: Error[] = [];
  /** Keys deleted, in order, so a test can prove a record is deleted exactly once. */
  readonly deletes: string[] = [];
  writes: number = 0;

  write(op: WriteOp): Promise<WriteResult> {
    this.writes++;
    const failure: Error | undefined = this.failures.shift();
    if (failure !== undefined) {
      return Promise.reject(failure);
    }

    if (op.kind === WriteKind.put) {
      if (op.condition === PutCondition.ifAbsent && this.values.has(op.key)) {
        return Promise.resolve({ applied: false, reason: RejectionReason.ifAbsent });
      }

      this.values.set(op.key, op.value);
      return Promise.resolve(appliedResult(op.key, op.value));
    }

    if (op.kind === WriteKind.compareAndSet) {
      const current: Uint8Array | undefined = this.values.get(op.key);
      if (current === undefined || !sameBytes(current, op.expected)) {
        return Promise.resolve({ applied: false, reason: RejectionReason.compareAndSet });
      }

      this.values.set(op.key, op.value);
      return Promise.resolve(appliedResult(op.key, op.value));
    }

    // The only other write the placement drives is a record delete.
    this.deletes.push(op.key);
    this.values.delete(op.key);
    return Promise.resolve(appliedResult(op.key, new Uint8Array()));
  }

  read(key: string): Promise<Entry | undefined> {
    const value: Uint8Array | undefined = this.values.get(key);
    return Promise.resolve(value === undefined ? undefined : entryOf(key, value));
  }

  scan(): Promise<ScanEntry[]> {
    return Promise.resolve(
      [...this.values].map(([key, value]: [string, Uint8Array]): ScanEntry => ({ key, value })),
    );
  }
}

/** The node-local placement the cluster placement wraps, with scriptable outcomes. */
class FakeInner implements Placement {
  onRelease: ((name: string) => void) | undefined = undefined;
  onPlace: ((name: string) => Promise<void>) | undefined = undefined;
  claimResult: Error | null = null;
  placeError: Error | null = null;
  findResult: PID | undefined = undefined;
  routeResult: IsolateRoute | undefined = undefined;
  readonly claimed: string[] = [];
  readonly placed: string[] = [];
  readonly freed: string[] = [];
  readonly respawned: string[] = [];
  readonly stoppedActors: string[] = [];
  stops: number = 0;

  claim(name: string): Promise<Error | null> {
    this.claimed.push(name);
    return Promise.resolve(this.claimResult);
  }

  async place(name: string, _props: Props, _options?: SpawnOptions): Promise<PID> {
    if (this.placeError !== null) {
      // Mirror the pool releasing a name whose build failed, before rejecting.
      this.onRelease?.(name);
      throw this.placeError;
    }

    // Mirror the recipe spawn re-entering the cluster placement while building.
    await this.onPlace?.(name);
    this.placed.push(name);
    return FAKE_PID;
  }

  free(name: string): void {
    this.freed.push(name);
    // Mirror the pool: a release notifies the cluster placement, which deletes
    // the freed name's registry record.
    this.onRelease?.(name);
  }

  find(name: string): PID | undefined {
    this.claimed.push(`find:${name}`);
    return this.findResult;
  }

  routeOf(name: string): IsolateRoute | undefined {
    this.claimed.push(`route:${name}`);
    return this.routeResult;
  }

  respawn(name: string): Promise<void> {
    this.respawned.push(name);
    return Promise.resolve();
  }

  stopActor(name: string): Promise<void> {
    this.stoppedActors.push(name);
    return Promise.resolve();
  }

  stop(): Promise<void> {
    this.stops++;
    return Promise.resolve();
  }
}

/** A cluster placement over the two fakes, plus the sleep delays it awaited. */
interface Harness {
  readonly placement: ClusterPlacement;
  readonly store: FakeStore;
  readonly registry: ClusterRegistry;
  readonly inner: FakeInner;
  readonly delays: number[];
}

/** Builds a placement whose backoff sleep is instant and recorded. */
function harness(): Harness {
  const store: FakeStore = new FakeStore();
  const registry: ClusterRegistry = new ClusterRegistry(store);
  const inner: FakeInner = new FakeInner();
  const delays: number[] = [];
  const placement: ClusterPlacement = new ClusterPlacement({
    registry,
    node: NODE,
    relocationDefault: false,
    bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
      inner.onRelease = onRelease;
      return Promise.resolve(inner);
    },
    sleep: (ms: number): Promise<void> => {
      delays.push(ms);
      return Promise.resolve();
    },
  });
  return { placement, store, registry, inner, delays };
}

/** The node a chosen placement ships its recipe to, with scriptable outcomes. */
class FakeRemote implements ClusterPlacementRemote {
  spawnError: Error | null = null;
  recreatePlaced: boolean = true;
  readonly addresses: Map<string, string> = new Map();
  readonly spawned: { host: string; port: number; name: string }[] = [];
  readonly recreated: { host: string; port: number; name: string; deadOwner: string }[] = [];

  spawn(
    host: string,
    port: number,
    name: string,
    _props: Props,
    _options?: SpawnOptions,
  ): Promise<PID> {
    if (this.spawnError !== null) {
      return Promise.reject(this.spawnError);
    }

    this.spawned.push({ host, port, name });
    return Promise.resolve(FAKE_PID);
  }

  recreate(
    host: string,
    port: number,
    name: string,
    _recipe: RecreateRecipe,
    _singleton: boolean,
    deadOwner: string,
  ): Promise<boolean> {
    this.recreated.push({ host, port, name, deadOwner });
    return Promise.resolve(this.recreatePlaced);
  }

  remotingAddressOf(owner: string): string | undefined {
    return this.addresses.get(owner);
  }
}

/** A placement configured with a fake remote, for the chosen-node placeOn path. */
interface RemoteHarness {
  readonly placement: ClusterPlacement;
  readonly store: FakeStore;
  readonly registry: ClusterRegistry;
  readonly inner: FakeInner;
  readonly remote: FakeRemote;
}

function placeOnHarness(): RemoteHarness {
  const store: FakeStore = new FakeStore();
  const registry: ClusterRegistry = new ClusterRegistry(store);
  const inner: FakeInner = new FakeInner();
  const remote: FakeRemote = new FakeRemote();
  const placement: ClusterPlacement = new ClusterPlacement({
    registry,
    node: NODE,
    relocationDefault: false,
    remote,
    bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
      inner.onRelease = onRelease;
      return Promise.resolve(inner);
    },
    sleep: (): Promise<void> => Promise.resolve(),
  });
  return { placement, store, registry, inner, remote };
}

describe("ClusterPlacement.place", () => {
  it("claims the name, records this node, and builds the actor locally", async () => {
    const h: Harness = harness();

    const pid: PID = await h.placement.place("worker", FAKE_PROPS);

    expect(pid).toBe(FAKE_PID);
    expect(h.inner.placed).toEqual(["worker"]);
    expect(await h.registry.getActor("worker")).toBe(NODE);
  });

  it("reuses the booted placement for a later placement", async () => {
    const h: Harness = harness();

    await h.placement.place("a", FAKE_PROPS);
    await h.placement.place("b", FAKE_PROPS);

    expect(h.inner.placed).toEqual(["a", "b"]);
  });

  it("retries the boot on the next placement after a boot failure", async () => {
    const store: FakeStore = new FakeStore();
    const registry: ClusterRegistry = new ClusterRegistry(store);
    const inner: FakeInner = new FakeInner();
    let boots: number = 0;
    const placement: ClusterPlacement = new ClusterPlacement({
      registry,
      node: NODE,
      relocationDefault: false,
      bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
        boots++;
        if (boots === 1) {
          return Promise.reject(new Error("pool boot failed"));
        }

        inner.onRelease = onRelease;
        return Promise.resolve(inner);
      },
      sleep: (): Promise<void> => Promise.resolve(),
    });

    await expect(placement.place("worker", FAKE_PROPS)).rejects.toThrow("pool boot failed");
    // The failed claim's record was rolled back.
    expect(await registry.getActor("worker")).toBeUndefined();

    // The next placement boots afresh and succeeds instead of inheriting the failure.
    expect(await placement.place("worker", FAKE_PROPS)).toBe(FAKE_PID);
    expect(boots).toBe(2);
  });

  it("boots the wrapped placement once across concurrent placements", async () => {
    const h: Harness = harness();
    let boots: number = 0;
    const placement: ClusterPlacement = new ClusterPlacement({
      registry: h.registry,
      node: NODE,
      relocationDefault: false,
      bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
        boots++;
        h.inner.onRelease = onRelease;
        return Promise.resolve(h.inner);
      },
      sleep: (): Promise<void> => Promise.resolve(),
    });

    await Promise.all([placement.place("a", FAKE_PROPS), placement.place("b", FAKE_PROPS)]);

    expect(boots).toBe(1);
    expect(h.inner.placed.sort()).toEqual(["a", "b"]);
  });

  it("refuses a lost claim with ErrActorAlreadyExists and never builds the actor", async () => {
    const h: Harness = harness();
    h.store.values.set("taken", new TextEncoder().encode("other-node:1"));

    await expect(h.placement.place("taken", FAKE_PROPS)).rejects.toBe(ErrActorAlreadyExists);
    expect(h.inner.placed).toEqual([]);
    // The prior holder's record is untouched.
    expect(await h.registry.getActor("taken")).toBe("other-node:1");
  });

  it("deletes the claim's record exactly once when building the actor fails", async () => {
    const h: Harness = harness();
    h.inner.placeError = new Error("spawn blew up");

    await expect(h.placement.place("worker", FAKE_PROPS)).rejects.toThrow("spawn blew up");
    expect(await h.registry.getActor("worker")).toBeUndefined();
    // The pool's release deletes the record; place() must not delete it again, or
    // a second owner-blind delete could erase a reused name's fresh claim.
    expect(h.store.deletes).toEqual(["worker"]);
  });

  it("rolls the claim back when a relocatable actor's companion cannot be built", async () => {
    const store: FakeStore = new FakeStore();
    const registry: ClusterRegistry = new ClusterRegistry(store);
    const inner: FakeInner = new FakeInner();
    const placement: ClusterPlacement = new ClusterPlacement({
      registry,
      node: NODE,
      relocationDefault: true,
      bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
        inner.onRelease = onRelease;
        return Promise.resolve(inner);
      },
      sleep: (): Promise<void> => Promise.resolve(),
    });

    // A relocatable placement records its recipe before building; the fake Props
    // carries no recipe, so that write throws and frees the name it had claimed,
    // before the actor is ever built.
    await expect(placement.place("worker", FAKE_PROPS)).rejects.toBeInstanceOf(Error);
    expect(inner.placed).toEqual([]);
    expect(await registry.getActor("worker")).toBeUndefined();
  });

  it("treats the recipe spawn's re-entrant claim of the placing name as a no-op", async () => {
    const h: Harness = harness();
    let reentrant: Error | null | undefined;
    h.inner.onPlace = async (name: string): Promise<void> => {
      reentrant = await h.placement.claim(name);
    };

    await h.placement.place("worker", FAKE_PROPS);

    expect(reentrant).toBeNull();
    // The guard releases after the build, so a later claim is a real claim.
    expect(await h.placement.claim("worker-2")).toBeNull();
  });
});

const NODE_B: string = "10.0.0.2:8080";
const NODE_B_ENDPOINT: string = "10.0.0.2:2552";

/** Two cluster placements over one shared registry, modelling two nodes on the
 * same distributed store: node A ships remote placements to node B, whose remote
 * spawn runs as B's own local placement, exactly as the owner's control-spawn
 * handler does. */
interface CrossNodeHarness {
  readonly a: ClusterPlacement;
  readonly b: ClusterPlacement;
  readonly store: FakeStore;
  readonly registry: ClusterRegistry;
  readonly innerA: FakeInner;
  readonly innerB: FakeInner;
}

function crossNodeHarness(): CrossNodeHarness {
  const store: FakeStore = new FakeStore();
  const registry: ClusterRegistry = new ClusterRegistry(store);
  const innerA: FakeInner = new FakeInner();
  const innerB: FakeInner = new FakeInner();
  const b: ClusterPlacement = new ClusterPlacement({
    registry,
    node: NODE_B,
    relocationDefault: false,
    bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
      innerB.onRelease = onRelease;
      return Promise.resolve(innerB);
    },
    sleep: (): Promise<void> => Promise.resolve(),
  });
  const remote: ClusterPlacementRemote = {
    spawn: (
      _host: string,
      _port: number,
      name: string,
      props: Props,
      options?: SpawnOptions,
    ): Promise<PID> => b.place(name, props, options),
    recreate: (): Promise<boolean> => Promise.resolve(true),
    remotingAddressOf: (owner: string): string | undefined =>
      owner === NODE_B ? NODE_B_ENDPOINT : undefined,
  };
  const a: ClusterPlacement = new ClusterPlacement({
    registry,
    node: NODE,
    relocationDefault: false,
    remote,
    bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
      innerA.onRelease = onRelease;
      return Promise.resolve(innerA);
    },
    sleep: (): Promise<void> => Promise.resolve(),
  });
  return { a, b, store, registry, innerA, innerB };
}

describe("ClusterPlacement.placeOn", () => {
  it("builds locally when the chosen owner is this node", async () => {
    const h: RemoteHarness = placeOnHarness();

    const pid: PID = await h.placement.placeOn("worker", FAKE_PROPS, NODE);

    expect(pid).toBe(FAKE_PID);
    expect(h.inner.placed).toEqual(["worker"]);
    expect(h.remote.spawned).toEqual([]);
    expect(await h.registry.getActor("worker")).toBe(NODE);
  });

  it("ships to the resolved endpoint and records nothing on the initiator", async () => {
    const h: RemoteHarness = placeOnHarness();
    h.remote.addresses.set("other:1", "10.0.0.2:2552");

    const pid: PID = await h.placement.placeOn("worker", FAKE_PROPS, "other:1");

    expect(pid).toBe(FAKE_PID);
    expect(h.remote.spawned).toEqual([{ host: "10.0.0.2", port: 2552, name: "worker" }]);
    expect(h.inner.placed).toEqual([]);
    // The initiator owns no record for a name another node holds; the owner claims it.
    expect(await h.registry.getActor("worker")).toBeUndefined();
  });

  it("throws when the owner advertises no remoting endpoint, recording nothing", async () => {
    const h: RemoteHarness = placeOnHarness();

    await expect(h.placement.placeOn("worker", FAKE_PROPS, "gone:1")).rejects.toThrow(
      "advertises no remoting endpoint",
    );
    expect(await h.registry.getActor("worker")).toBeUndefined();
  });

  it("throws when the owner endpoint is malformed, recording nothing", async () => {
    const h: RemoteHarness = placeOnHarness();
    h.remote.addresses.set("bad:1", "garbage");

    await expect(h.placement.placeOn("worker", FAKE_PROPS, "bad:1")).rejects.toBeInstanceOf(Error);
    expect(h.remote.spawned).toEqual([]);
    expect(await h.registry.getActor("worker")).toBeUndefined();
  });

  it("propagates a failed remote spawn, recording nothing", async () => {
    const h: RemoteHarness = placeOnHarness();
    h.remote.addresses.set("other:1", "10.0.0.2:2552");
    h.remote.spawnError = new Error("remote spawn blew up");

    await expect(h.placement.placeOn("worker", FAKE_PROPS, "other:1")).rejects.toThrow(
      "remote spawn blew up",
    );
    expect(await h.registry.getActor("worker")).toBeUndefined();
  });

  it("refuses a remote placement when remote support is not configured", async () => {
    const h: Harness = harness();

    await expect(h.placement.placeOn("worker", FAKE_PROPS, "other:1")).rejects.toThrow(
      "without remote spawn support",
    );
    expect(await h.registry.getActor("worker")).toBeUndefined();
  });
});

describe("ClusterPlacement.placeOn across nodes", () => {
  it("places on the owner node through a single cluster claim", async () => {
    const h: CrossNodeHarness = crossNodeHarness();

    const pid: PID = await h.a.placeOn("worker", FAKE_PROPS, NODE_B);

    expect(pid).toBe(FAKE_PID);
    // The actor is built on the owner, never on the initiator.
    expect(h.innerB.placed).toEqual(["worker"]);
    expect(h.innerA.placed).toEqual([]);
    // One record, naming the owner, written by the owner's own claim: the initiator
    // never claims a name it does not own, so the owner's build cannot lose to it.
    expect(await h.registry.getActor("worker")).toBe(NODE_B);
    expect(h.store.writes).toBe(1);
  });

  it("relays ErrActorAlreadyExists from the owner when the name is already held", async () => {
    const h: CrossNodeHarness = crossNodeHarness();
    h.store.values.set("worker", new TextEncoder().encode("someone:1"));

    await expect(h.a.placeOn("worker", FAKE_PROPS, NODE_B)).rejects.toBe(ErrActorAlreadyExists);
    // Neither node builds the actor, and the prior holder's record is untouched.
    expect(h.innerA.placed).toEqual([]);
    expect(h.innerB.placed).toEqual([]);
    expect(await h.registry.getActor("worker")).toBe("someone:1");
  });
});

describe("ClusterPlacement.claim", () => {
  it("wins a free name and records this node", async () => {
    const h: Harness = harness();

    expect(await h.placement.claim("service")).toBeNull();
    expect(await h.registry.getActor("service")).toBe(NODE);
  });

  it("returns ErrActorAlreadyExists for a held name", async () => {
    const h: Harness = harness();
    h.store.values.set("service", new TextEncoder().encode("other-node:1"));

    expect(await h.placement.claim("service")).toBe(ErrActorAlreadyExists);
  });

  it("defaults the backoff sleep to the real timer when none is given", async () => {
    const store: FakeStore = new FakeStore();
    const registry: ClusterRegistry = new ClusterRegistry(store);
    // No sleep override: the constructor falls back to the real timer.
    const placement: ClusterPlacement = new ClusterPlacement({
      registry,
      node: NODE,
      relocationDefault: false,
      bootInner: (): Promise<Placement> => Promise.resolve(new FakeInner()),
    });

    expect(await placement.claim("service")).toBeNull();
  });

  it("also claims on the wrapped placement once it has booted", async () => {
    const h: Harness = harness();
    await h.placement.place("first", FAKE_PROPS);

    expect(await h.placement.claim("second")).toBeNull();
    expect(h.inner.claimed).toContain("second");
  });

  it("releases the cluster claim when the wrapped placement refuses it", async () => {
    const h: Harness = harness();
    await h.placement.place("first", FAKE_PROPS);
    h.inner.claimResult = ErrActorAlreadyExists;

    expect(await h.placement.claim("clash")).toBe(ErrActorAlreadyExists);
    expect(await h.registry.getActor("clash")).toBeUndefined();
  });
});

describe("ClusterPlacement claim retries", () => {
  it("retries a rebalancing partition, then wins", async () => {
    const h: Harness = harness();
    h.store.failures.push(new PartitionRebalancingError(3));

    expect(await h.placement.claim("service")).toBeNull();
    expect(h.delays).toEqual([25]);
  });

  it("retries an unavailable half, then wins", async () => {
    const h: Harness = harness();
    h.store.failures.push(new ClusterUnavailableError());

    expect(await h.placement.claim("service")).toBeNull();
    expect(h.delays).toEqual([25]);
  });

  it("gives up after the bounded retries, capping the backoff", async () => {
    const h: Harness = harness();
    for (let i: number = 0; i < 6; i++) {
      h.store.failures.push(new PartitionRebalancingError(1));
    }

    await expect(h.placement.claim("service")).rejects.toBeInstanceOf(PartitionRebalancingError);
    expect(h.delays).toEqual([25, 50, 100, 200, 250]);
  });

  it("does not retry a non-retryable error", async () => {
    const h: Harness = harness();
    h.store.failures.push(new Error("fatal"));

    await expect(h.placement.claim("service")).rejects.toThrow("fatal");
    expect(h.delays).toEqual([]);
  });
});

describe("ClusterPlacement.free", () => {
  it("deletes the record directly before any placement has booted", async () => {
    const h: Harness = harness();
    await h.placement.claim("service");
    expect(await h.registry.getActor("service")).toBe(NODE);

    h.placement.free("service");
    await Promise.resolve();

    expect(await h.registry.getActor("service")).toBeUndefined();
    expect(h.inner.freed).toEqual([]);
  });

  it("routes through the wrapped placement once booted, whose release deletes the record", async () => {
    const h: Harness = harness();
    await h.placement.place("worker", FAKE_PROPS);
    expect(await h.registry.getActor("worker")).toBe(NODE);

    h.placement.free("worker");
    await Promise.resolve();

    expect(h.inner.freed).toEqual(["worker"]);
    expect(await h.registry.getActor("worker")).toBeUndefined();
  });

  it("swallows a delete that rejects, leaving no unhandled rejection", async () => {
    const h: Harness = harness();
    await h.placement.claim("service");
    h.store.failures.push(new Error("delete wedged"));

    expect(() => h.placement.free("service")).not.toThrow();
    await Promise.resolve();
    // The record survives a failed delete; a later claim of the same name loses.
    expect(await h.registry.getActor("service")).toBe(NODE);
  });
});

describe("ClusterPlacement delegation", () => {
  it("resolves nothing before boot and delegates afterward", async () => {
    const h: Harness = harness();

    expect(h.placement.find("x")).toBeUndefined();
    expect(h.placement.routeOf("x")).toBeUndefined();
    await expect(h.placement.respawn("x")).resolves.toBeUndefined();
    await expect(h.placement.stopActor("x")).resolves.toBeUndefined();

    await h.placement.place("worker", FAKE_PROPS);
    h.inner.findResult = FAKE_PID;
    h.inner.routeResult = FAKE_ROUTE;

    expect(h.placement.find("worker")).toBe(FAKE_PID);
    expect(h.placement.routeOf("worker")).toBe(FAKE_ROUTE);
    await h.placement.respawn("worker");
    await h.placement.stopActor("worker");
    expect(h.inner.respawned).toEqual(["worker"]);
    expect(h.inner.stoppedActors).toEqual(["worker"]);
  });

  it("stops the wrapped placement and then writes no more records", async () => {
    const h: Harness = harness();
    await h.placement.place("worker", FAKE_PROPS);

    await h.placement.stop();
    expect(h.inner.stops).toBe(1);

    // A release arriving after stop, and a free of an unbooted name, both no-op.
    const writesBefore: number = h.store.writes;
    h.placement.free("worker");
    await Promise.resolve();
    expect(h.store.writes).toBe(writesBefore);
  });

  it("stops cleanly when no placement ever booted", async () => {
    const h: Harness = harness();

    await expect(h.placement.stop()).resolves.toBeUndefined();
    expect(h.inner.stops).toBe(0);
  });

  it("resolves a name the wrapped placement lacks through the resolver", async () => {
    const store: FakeStore = new FakeStore();
    const registry: ClusterRegistry = new ClusterRegistry(store);
    const inner: FakeInner = new FakeInner();
    const remote: PID = {} as PID;
    const resolver: ClusterResolver = {
      find: (name: string): PID | undefined => (name === "remote" ? remote : undefined),
    } as unknown as ClusterResolver;
    const placement: ClusterPlacement = new ClusterPlacement({
      registry,
      node: NODE,
      relocationDefault: false,
      resolver,
      bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
        inner.onRelease = onRelease;
        return Promise.resolve(inner);
      },
      sleep: (): Promise<void> => Promise.resolve(),
    });

    // Before boot the wrapped placement is absent, so lookups fall to the resolver.
    expect(placement.find("remote")).toBe(remote);
    expect(placement.find("unknown")).toBeUndefined();

    // A name the wrapped placement holds wins over the resolver.
    await placement.place("local", FAKE_PROPS);
    inner.findResult = FAKE_PID;
    expect(placement.find("local")).toBe(FAKE_PID);
  });

  it("stops the pool of a placement caught mid-boot instead of leaking it", async () => {
    const store: FakeStore = new FakeStore();
    const registry: ClusterRegistry = new ClusterRegistry(store);
    const inner: FakeInner = new FakeInner();
    let resolveBoot: (placement: Placement) => void = (): void => {};
    const gate: Promise<Placement> = new Promise<Placement>((resolve): void => {
      resolveBoot = resolve;
    });
    const placement: ClusterPlacement = new ClusterPlacement({
      registry,
      node: NODE,
      relocationDefault: false,
      bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
        inner.onRelease = onRelease;
        return gate;
      },
      sleep: (): Promise<void> => Promise.resolve(),
    });

    // A placement parks on the boot; stop begins before the boot completes.
    const placing: Promise<PID> = placement.place("worker", FAKE_PROPS);
    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 0);
    });
    const stopping: Promise<void> = placement.stop();
    resolveBoot(inner);
    await stopping;
    await placing;

    expect(inner.stops).toBe(1);
  });

  it("stops cleanly when a placement's in-flight boot then fails", async () => {
    const store: FakeStore = new FakeStore();
    const registry: ClusterRegistry = new ClusterRegistry(store);
    let rejectBoot: (error: Error) => void = (): void => {};
    const gate: Promise<Placement> = new Promise<Placement>((_resolve, reject): void => {
      rejectBoot = reject;
    });
    const placement: ClusterPlacement = new ClusterPlacement({
      registry,
      node: NODE,
      relocationDefault: false,
      bootInner: (): Promise<Placement> => gate,
      sleep: (): Promise<void> => Promise.resolve(),
    });

    const placing: Promise<PID> = placement.place("worker", FAKE_PROPS);
    const settled: Promise<string> = placing.then(
      (): string => "resolved",
      (): string => "rejected",
    );
    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 0);
    });
    const stopping: Promise<void> = placement.stop();
    rejectBoot(new Error("boot failed during stop"));

    // A boot that fails during teardown does not make stop() reject; the failed
    // placement rejects on its own.
    await expect(stopping).resolves.toBeUndefined();
    expect(await settled).toBe("rejected");
  });
});

describe("ClusterPlacement.resolveActor", () => {
  it("returns a local instance without warming the resolver", async () => {
    const store: FakeStore = new FakeStore();
    const registry: ClusterRegistry = new ClusterRegistry(store);
    const inner: FakeInner = new FakeInner();
    const resolved: string[] = [];
    const resolver: ClusterResolver = {
      resolve: (name: string): Promise<void> => {
        resolved.push(name);
        return Promise.resolve();
      },
      find: (): PID | undefined => undefined,
    } as unknown as ClusterResolver;
    const placement: ClusterPlacement = new ClusterPlacement({
      registry,
      node: NODE,
      relocationDefault: false,
      resolver,
      bootInner: (onRelease: (name: string) => void): Promise<Placement> => {
        inner.onRelease = onRelease;
        return Promise.resolve(inner);
      },
      sleep: (): Promise<void> => Promise.resolve(),
    });
    await placement.place("worker", FAKE_PROPS);
    inner.findResult = FAKE_PID;

    expect(await placement.resolveActor("worker")).toBe(FAKE_PID);
    // A local hit short-circuits before the resolver read.
    expect(resolved).toEqual([]);
  });

  it("warms the resolver and returns a routed handle when not local", async () => {
    const store: FakeStore = new FakeStore();
    const registry: ClusterRegistry = new ClusterRegistry(store);
    const routed: PID = {} as PID;
    const resolved: string[] = [];
    const resolver: ClusterResolver = {
      resolve: (name: string): Promise<void> => {
        resolved.push(name);
        return Promise.resolve();
      },
      find: (name: string): PID | undefined => (name === "remote" ? routed : undefined),
    } as unknown as ClusterResolver;
    const placement: ClusterPlacement = new ClusterPlacement({
      registry,
      node: NODE,
      relocationDefault: false,
      resolver,
      bootInner: (): Promise<Placement> => Promise.resolve(new FakeInner()),
      sleep: (): Promise<void> => Promise.resolve(),
    });

    // No placement booted, so the name is not local; the resolver warms and answers.
    expect(await placement.resolveActor("remote")).toBe(routed);
    expect(resolved).toEqual(["remote"]);
  });

  it("resolves nothing without a resolver or a local instance", async () => {
    const h: Harness = harness();

    expect(await h.placement.resolveActor("nobody")).toBeUndefined();
  });
});

describe("ClusterPlacement.recreate", () => {
  it("takes a departed actor here when the record still names the dead node", async () => {
    const h: Harness = harness();
    h.store.values.set("worker", new TextEncoder().encode("dead:1"));

    const placed: boolean = await h.placement.recreate("worker", FAKE_PROPS, {}, "dead:1");

    expect(placed).toBe(true);
    expect(h.inner.placed).toEqual(["worker"]);
    // The compare-and-set moved the record from the dead node to this one.
    expect(await h.registry.getActor("worker")).toBe(NODE);
  });

  it("skips a recreate when the record no longer names the dead node", async () => {
    const h: Harness = harness();
    h.store.values.set("worker", new TextEncoder().encode("moved:2"));

    const placed: boolean = await h.placement.recreate("worker", FAKE_PROPS, {}, "dead:1");

    expect(placed).toBe(false);
    expect(h.inner.placed).toEqual([]);
    expect(await h.registry.getActor("worker")).toBe("moved:2");
  });

  it("returns the record to the dead node when the rebuild fails, so the sweep retries", async () => {
    const h: Harness = harness();
    h.store.values.set("worker", new TextEncoder().encode("dead:1"));
    h.inner.placeError = new Error("preStart threw");

    await expect(h.placement.recreate("worker", FAKE_PROPS, {}, "dead:1")).rejects.toThrow(
      "preStart threw",
    );

    // The compare-and-set moved the record here to build, but the failed build handed it
    // back to the dead node rather than deleting it, so a later sweep re-detects and
    // retries the orphan instead of losing it.
    expect(await h.registry.getActor("worker")).toBe("dead:1");
  });
});

describe("ClusterPlacement.relocateTo", () => {
  const RECIPE: RecreateRecipe = { actor: "Worker", args: ["w"] };

  it("ships the recreate to another survivor over remoting", async () => {
    const h: RemoteHarness = placeOnHarness();
    h.remote.addresses.set("s:1", "10.0.0.2:2552");

    const placed: boolean = await h.placement.relocateTo("s:1", "worker", RECIPE, false, "dead:1");

    expect(placed).toBe(true);
    expect(h.remote.recreated).toEqual([
      { host: "10.0.0.2", port: 2552, name: "worker", deadOwner: "dead:1" },
    ]);
  });

  it("refuses a remote recreate when remote support is not configured", async () => {
    const h: Harness = harness();

    await expect(h.placement.relocateTo("s:1", "worker", RECIPE, false, "dead:1")).rejects.toThrow(
      "without remote spawn support",
    );
  });

  it("throws when the survivor advertises no remoting endpoint", async () => {
    const h: RemoteHarness = placeOnHarness();

    await expect(
      h.placement.relocateTo("gone:1", "worker", RECIPE, false, "dead:1"),
    ).rejects.toThrow("advertises no remoting endpoint");
  });
});

describe("timerSleep", () => {
  it("resolves after the delay", async () => {
    await expect(timerSleep(1)).resolves.toBeUndefined();
  });
});
