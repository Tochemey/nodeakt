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
import { RelocationCompleted, RelocationFailed, RelocationStarted } from "../src/cluster.events";
import { encodeCompanion } from "../src/clustering.companion";
import type { PlacementRecord } from "../src/clustering.registry";
import {
  NodeDeparted,
  type RelocationMember,
  RelocationSweep,
  Relocator,
  type RelocatorDeps,
} from "../src/clustering.relocator";
import type { PID } from "../src/pid";
import { ReceiveContext } from "../src/receive.context";

/** One recreate the fake seam was asked to perform. */
interface RecreateCall {
  readonly owner: string;
  readonly name: string;
  readonly singleton: boolean;
  readonly deadOwner: string;
}

/** What a driven relocation recorded, for assertions. */
interface Recorded {
  readonly recreated: RecreateCall[];
  readonly freed: string[];
  readonly events: unknown[];
}

/** The overridable pieces of a relocation's environment. */
interface Env {
  placements?: PlacementRecord[];
  self?: string;
  coordinator?: string;
  members?: RelocationMember[];
  recreate?: (
    owner: string,
    name: string,
    singleton: boolean,
    deadOwner: string,
  ) => Promise<boolean>;
  scanPlacements?: () => Promise<PlacementRecord[]>;
}

const COORD: string = "coord:1";

/** A relocatable orphan's placement, its companion carrying the recipe and marker. */
function orphan(name: string, owner: string, singleton: boolean = false): PlacementRecord {
  return {
    name,
    owner,
    companion: encodeCompanion({
      recipe: { module: "m", actor: "Worker", args: [name] },
      singleton,
    }),
  };
}

/** A non-relocatable placement, storing no companion. */
function bound(name: string, owner: string): PlacementRecord {
  return { name, owner, companion: undefined };
}

/** A live member, ready and not draining unless overridden. */
function member(name: string, ready: boolean = true, draining: boolean = false): RelocationMember {
  return { name, ready, draining };
}

/** A relocator over fake deps, plus the calls its pass recorded. */
function build(env: Env): { relocator: Relocator; recorded: Recorded } {
  const recorded: Recorded = { recreated: [], freed: [], events: [] };
  const deps: RelocatorDeps = {
    scanPlacements:
      env.scanPlacements ??
      ((): Promise<PlacementRecord[]> => Promise.resolve(env.placements ?? [])),
    free: (name: string): Promise<void> => {
      recorded.freed.push(name);
      return Promise.resolve();
    },
    recreate: (
      owner: string,
      name: string,
      _recipe,
      singleton: boolean,
      deadOwner: string,
    ): Promise<boolean> => {
      recorded.recreated.push({ owner, name, singleton, deadOwner });
      return env.recreate?.(owner, name, singleton, deadOwner) ?? Promise.resolve(true);
    },
    self: env.self ?? COORD,
    coordinator: (): string => env.coordinator ?? COORD,
    members: (): readonly RelocationMember[] => env.members ?? [member(COORD)],
    publish: (event: unknown): void => {
      recorded.events.push(event);
    },
    now: (): number => 0,
    sweepIntervalMs: 30_000,
  };
  return { relocator: new Relocator(deps), recorded };
}

/** Drives one message through the relocator with no self, so arming skips scheduling. */
async function drive(relocator: Relocator, message: unknown): Promise<void> {
  await relocator.receive(ReceiveContext.create(message));
}

/** The names an event of the given class carries, across the recorded events. */
function relocatedIn(events: unknown[]): string[] {
  const completed: RelocationCompleted | undefined = events.find(
    (event: unknown): event is RelocationCompleted => event instanceof RelocationCompleted,
  );
  return [...(completed?.relocated ?? [])];
}

function failedIn(events: unknown[]): string[] {
  const failure: RelocationFailed | undefined = events.find(
    (event: unknown): event is RelocationFailed => event instanceof RelocationFailed,
  );
  return [...(failure?.names ?? [])];
}

describe("Relocator recovery", () => {
  it("recreates a departed node's ordinary actors across the survivors", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("w1", "dead:1"), orphan("w2", "dead:1"), orphan("kept", COORD)],
      members: [member(COORD), member("s:1")],
    });

    await drive(relocator, new NodeDeparted("dead:1"));

    // Two orphans spread across the two survivors, one each, none left on the dead node.
    expect(recorded.recreated.map((call: RecreateCall): string => call.name).sort()).toEqual([
      "w1",
      "w2",
    ]);
    expect(new Set(recorded.recreated.map((call: RecreateCall): string => call.owner)).size).toBe(
      2,
    );
    expect(recorded.events[0]).toBeInstanceOf(RelocationStarted);
    expect(relocatedIn(recorded.events).sort()).toEqual(["w1", "w2"]);
  });

  it("pins a singleton to the coordinator", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("seq", "dead:1", true)],
      members: [member(COORD), member("s:1")],
    });

    await drive(relocator, new NodeDeparted("dead:1"));

    expect(recorded.recreated).toEqual([
      { owner: COORD, name: "seq", singleton: true, deadOwner: "dead:1" },
    ]);
  });

  it("frees a non-relocatable orphan rather than recreating it", async () => {
    const { relocator, recorded } = build({
      placements: [bound("bound", "dead:1")],
      members: [member(COORD)],
    });

    await drive(relocator, new NodeDeparted("dead:1"));

    expect(recorded.freed).toEqual(["bound"]);
    expect(recorded.recreated).toEqual([]);
  });

  it("frees an orphan whose companion cannot be decoded", async () => {
    const { relocator, recorded } = build({
      placements: [{ name: "corrupt", owner: "dead:1", companion: Uint8Array.of(9, 9, 9) }],
      members: [member(COORD)],
    });

    await drive(relocator, new NodeDeparted("dead:1"));

    expect(recorded.freed).toEqual(["corrupt"]);
    expect(recorded.recreated).toEqual([]);
  });

  it("does nothing when this node is not the coordinator", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("w1", "dead:1")],
      coordinator: "other:1",
      members: [member(COORD), member("other:1")],
    });

    await drive(relocator, new NodeDeparted("dead:1"));

    expect(recorded.recreated).toEqual([]);
    expect(recorded.events).toEqual([]);
  });

  it("reports the orphans it cannot place when there is no survivor", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("w1", "dead:1"), orphan("w2", "dead:1")],
      // The only member is the dead node, filtered out, so there is nowhere to place.
      members: [member("dead:1")],
      self: "dead:1",
      coordinator: "dead:1",
    });

    await drive(relocator, new NodeDeparted("dead:1"));

    expect(recorded.recreated).toEqual([]);
    expect(failedIn(recorded.events).sort()).toEqual(["w1", "w2"]);
  });

  it("reports a recreate whose build fails", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("w1", "dead:1")],
      members: [member(COORD), member("s:1")],
      recreate: (): Promise<boolean> => Promise.reject(new Error("build blew up")),
    });

    await drive(relocator, new NodeDeparted("dead:1"));

    expect(failedIn(recorded.events)).toEqual(["w1"]);
    expect(relocatedIn(recorded.events)).toEqual([]);
  });

  it("counts a recreate that finds the record already moved as neither placed nor failed", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("w1", "dead:1")],
      members: [member(COORD), member("s:1")],
      recreate: (): Promise<boolean> => Promise.resolve(false),
    });

    await drive(relocator, new NodeDeparted("dead:1"));

    expect(relocatedIn(recorded.events)).toEqual([]);
    expect(failedIn(recorded.events)).toEqual([]);
  });

  it("skips a departure that left no orphans", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("kept", COORD)],
      members: [member(COORD)],
    });

    await drive(relocator, new NodeDeparted("dead:1"));

    expect(recorded.events).toEqual([]);
  });
});

describe("Relocator sweep", () => {
  it("re-fills a record still naming a departed member", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("stray", "gone:1")],
      members: [member(COORD), member("s:1")],
    });

    await drive(relocator, new RelocationSweep());

    expect(recorded.recreated.map((call: RecreateCall): string => call.name)).toEqual(["stray"]);
    expect(recorded.recreated[0]?.deadOwner).toBe("gone:1");
  });

  it("short-circuits when every placement names a live member", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("kept", COORD)],
      members: [member(COORD)],
    });

    await drive(relocator, new RelocationSweep());

    expect(recorded.recreated).toEqual([]);
    expect(recorded.events).toEqual([]);
  });

  it("does not sweep when this node is not the coordinator", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("stray", "gone:1")],
      coordinator: "other:1",
      members: [member(COORD)],
    });

    await drive(relocator, new RelocationSweep());

    expect(recorded.recreated).toEqual([]);
  });

  it("ignores a message it does not handle", async () => {
    const { relocator, recorded } = build({
      placements: [orphan("stray", "gone:1")],
      members: [member(COORD)],
    });

    await drive(relocator, { unrecognized: true });

    expect(recorded.recreated).toEqual([]);
    expect(recorded.events).toEqual([]);
  });
});

describe("Relocator resilience", () => {
  it("survives a failing pass and completes the recovery on a later one", async () => {
    // The scan rejects on the first pass, the way a read against a cluster still
    // converging on a departure does, then serves the orphan. The failed pass must
    // not fault the actor: the follow-up sweep is the retry that finishes recovery.
    let scans: number = 0;
    const { relocator, recorded } = build({
      members: [member(COORD)],
      scanPlacements: (): Promise<PlacementRecord[]> => {
        scans += 1;
        if (scans === 1) {
          return Promise.reject(new Error("scan against a converging cluster"));
        }

        return Promise.resolve([orphan("stray", "gone:1")]);
      },
    });

    await drive(relocator, new NodeDeparted("gone:1"));
    expect(recorded.recreated).toEqual([]);

    await drive(relocator, new RelocationSweep());
    expect(recorded.recreated).toEqual([
      { owner: COORD, name: "stray", singleton: false, deadOwner: "gone:1" },
    ]);
  });
});

describe("Relocator arming", () => {
  it("schedules the recurring sweep on its first message", async () => {
    const { relocator } = build({ members: [member(COORD)] });
    let scheduled: { message: unknown; interval: number } | undefined;
    const self: PID = {
      actorSystem: (): unknown => ({
        scheduleFrom: (_from: PID, message: unknown, _to: PID, interval: number): Promise<void> => {
          scheduled = { message, interval };
          return Promise.resolve();
        },
      }),
    } as unknown as PID;

    await relocator.receive(ReceiveContext.create(new RelocationSweep(), self));

    expect(scheduled?.message).toBeInstanceOf(RelocationSweep);
    expect(scheduled?.interval).toBe(30_000);
  });
});
