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
import {
  appendRemotingAddress,
  encodeNodeMetadata,
  type NodeMetadata,
} from "../src/clustering.metadata";
import { SwimClusterView } from "../src/clustering.view";
import type { ClusterMember } from "../src/kv/ports";
import type { MemberRecord } from "../src/membership/view";
import {
  type MemberState,
  STATE_ALIVE,
  STATE_DEAD,
  STATE_LEFT,
  STATE_SUSPECT,
} from "../src/membership/wire";

/**
 * Builds a membership record whose metadata carries `metadata`. The record's own
 * `member` (its membership identity) is deliberately distinct from the data
 * address, since the adapter names members by the address, not by `member`.
 */
function record(
  metadata: NodeMetadata,
  state: MemberState = STATE_ALIVE,
  remotingAddress: string = "",
): MemberRecord {
  return {
    state,
    selfOriginated: false,
    incarnation: 0,
    stateChangeTime: 0n,
    member: `gossip-${metadata.address}`,
    reporter: "",
    metadata: appendRemotingAddress(encodeNodeMetadata(metadata), remotingAddress),
    appliedAt: 0,
  };
}

/** A live record whose metadata is too short to decode, so it has no data address. */
function malformedRecord(): MemberRecord {
  return {
    state: STATE_ALIVE,
    selfOriginated: false,
    incarnation: 0,
    stateChangeTime: 0n,
    member: "gossip-malformed",
    reporter: "",
    metadata: new Uint8Array(3),
    appliedAt: 0,
  };
}

describe("SwimClusterView", () => {
  it("exposes the configured self identity", () => {
    const view: SwimClusterView = new SwimClusterView(
      "10.0.0.1:6000",
      (): readonly MemberRecord[] => [],
    );
    expect(view.self).toBe("10.0.0.1:6000");
  });

  it("names members by their data address, oldest first, dropping dead, left, and unaddressable", () => {
    const records: readonly MemberRecord[] = [
      record({ startedAt: 100, ready: true, draining: false, address: "c:1" }),
      record({ startedAt: 5, ready: true, draining: false, address: "gone:1" }, STATE_DEAD),
      record({ startedAt: 100, ready: true, draining: false, address: "a:1" }),
      record({ startedAt: 100, ready: true, draining: false, address: "b:1" }),
      record({ startedAt: 150, ready: true, draining: false, address: "susp:2" }, STATE_SUSPECT),
      record({ startedAt: 500, ready: true, draining: false, address: "late:1" }),
      record({ startedAt: 1, ready: true, draining: false, address: "bye:1" }, STATE_LEFT),
      malformedRecord(),
    ];
    const view: SwimClusterView = new SwimClusterView(
      "a:1",
      (): readonly MemberRecord[] => records,
    );
    expect(view.members().map((member: ClusterMember): string => member.name)).toEqual([
      "a:1",
      "b:1",
      "c:1",
      "susp:2",
      "late:1",
    ]);
  });

  it("decodes the ready and draining flags of each member", () => {
    const records: readonly MemberRecord[] = [
      record({ startedAt: 100, ready: true, draining: false, address: "a:1" }),
      record({ startedAt: 200, ready: false, draining: true, address: "b:2" }),
    ];
    const view: SwimClusterView = new SwimClusterView(
      "a:1",
      (): readonly MemberRecord[] => records,
    );
    expect(view.members()).toEqual([
      { name: "a:1", startedAt: 100, ready: true, draining: false },
      { name: "b:2", startedAt: 200, ready: false, draining: true },
    ]);
  });

  it("notifies a listener on publish and stops after it unsubscribes", () => {
    const records: readonly MemberRecord[] = [
      record({ startedAt: 100, ready: true, draining: false, address: "a:1" }),
    ];
    const view: SwimClusterView = new SwimClusterView(
      "a:1",
      (): readonly MemberRecord[] => records,
    );
    const seen: (readonly ClusterMember[])[] = [];
    const unsubscribe: () => void = view.onChange((members: readonly ClusterMember[]): void => {
      seen.push(members);
    });

    view.publish();
    expect(seen).toHaveLength(1);
    expect(seen[0]).toEqual([{ name: "a:1", startedAt: 100, ready: true, draining: false }]);

    unsubscribe();
    view.publish();
    expect(seen).toHaveLength(1);
  });
});

describe("SwimClusterView remoting-address lookup", () => {
  it("returns the remoting endpoint a present member advertises", () => {
    const records: readonly MemberRecord[] = [
      record({ startedAt: 1, ready: true, draining: false, address: "a:1" }, STATE_ALIVE, "a:9000"),
    ];
    const view: SwimClusterView = new SwimClusterView(
      "a:1",
      (): readonly MemberRecord[] => records,
    );
    expect(view.remotingAddressOf("a:1")).toBe("a:9000");
  });

  it("returns undefined for a present member that advertises no remoting endpoint", () => {
    const records: readonly MemberRecord[] = [
      record({ startedAt: 1, ready: true, draining: false, address: "a:1" }),
    ];
    const view: SwimClusterView = new SwimClusterView(
      "a:1",
      (): readonly MemberRecord[] => records,
    );
    expect(view.remotingAddressOf("a:1")).toBeUndefined();
  });

  it("returns undefined for a data address no present member carries", () => {
    const records: readonly MemberRecord[] = [
      record({ startedAt: 1, ready: true, draining: false, address: "a:1" }, STATE_ALIVE, "a:9000"),
    ];
    const view: SwimClusterView = new SwimClusterView(
      "a:1",
      (): readonly MemberRecord[] => records,
    );
    expect(view.remotingAddressOf("z:1")).toBeUndefined();
  });

  it("ignores a dead member that still carries the data address", () => {
    const records: readonly MemberRecord[] = [
      record({ startedAt: 1, ready: true, draining: false, address: "a:1" }, STATE_DEAD, "a:9000"),
    ];
    const view: SwimClusterView = new SwimClusterView(
      "a:1",
      (): readonly MemberRecord[] => records,
    );
    expect(view.remotingAddressOf("a:1")).toBeUndefined();
  });

  it("returns undefined for an empty data address without scanning members", () => {
    const records: readonly MemberRecord[] = [
      record({ startedAt: 1, ready: true, draining: false, address: "a:1" }, STATE_ALIVE, "a:9000"),
    ];
    const view: SwimClusterView = new SwimClusterView(
      "a:1",
      (): readonly MemberRecord[] => records,
    );
    expect(view.remotingAddressOf("")).toBeUndefined();
  });
});
