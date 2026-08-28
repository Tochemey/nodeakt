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

import type { Actor } from "./actor";
import { RelocationCompleted, RelocationFailed, RelocationStarted } from "./cluster.events";
import { type Companion, decodeCompanion } from "./clustering.companion";
import type { PlacementRecord } from "./clustering.registry";
import { type FillAssignment, type FillTarget, planFill } from "./clustering.relocation";
import type { PID } from "./pid";
import type { ActorRecipe } from "./protocol";
import type { ReceiveContext } from "./receive.context";

/** A fill candidate the relocation reads from the membership view: its cluster
 * address and readiness, the fields the balanced fill needs. @internal */
export interface RelocationMember {
  readonly name: string;
  readonly ready: boolean;
  readonly draining: boolean;
}

/** A node has departed the cluster; the coordinator relocates its actors. @internal */
export class NodeDeparted {
  constructor(readonly address: string) {}
}

/** The periodic self-tick that re-fills any record still naming a departed node,
 * the backstop behind the `NodeLeft`-driven pass. @internal */
export class RelocationSweep {}

/** A relocation the coordinator drives on one departed node. @internal */
interface Reassignment {
  readonly name: string;
  readonly owner: string;
  readonly recipe: ActorRecipe;
  readonly singleton: boolean;
}

/** What the {@link Relocator} needs to drive recovery, injected so it unit-tests
 * against fakes with no live cluster. @internal */
export interface RelocatorDeps {
  /** Every placement across the cluster with its companion, from one scan. */
  readonly scanPlacements: () => Promise<PlacementRecord[]>;

  /** Frees a name and its companion, for a non-relocatable or unreadable orphan,
   * only while the record still names `deadOwner`, so a name another node has already
   * reclaimed is left untouched. */
  readonly free: (name: string, deadOwner: string) => Promise<void>;

  /** Recreates `name` on `owner` from its recipe, gated on the record still naming
   * `deadOwner`; resolves whether that node took the name. Rejects on a build
   * failure. This node's own slice builds locally, another survivor's over remoting. */
  readonly recreate: (
    owner: string,
    name: string,
    recipe: ActorRecipe,
    singleton: boolean,
    deadOwner: string,
  ) => Promise<boolean>;

  /** This node's cluster address, the coordinator every singleton is pinned to. */
  readonly self: string;

  /** The current coordinator; recovery runs only when it is this node. */
  readonly coordinator: () => string;

  /** The live members with their ready and draining state, the fill candidates. */
  readonly members: () => readonly RelocationMember[];

  /** Publishes a relocation event on the system's stream. */
  readonly publish: (event: unknown) => void;

  /** The wall clock the events are stamped with. */
  readonly now: () => number;

  /** How often the orphan sweep re-runs, its first tick one interval after start. */
  readonly sweepIntervalMs: number;
}

/**
 * The relocation coordinator, a system actor. It learns of departures as messages,
 * and when it is the cluster coordinator it recreates the departed node's
 * relocatable actors on the survivors: one scan collects the orphans, their
 * companion recipes, and every survivor's current load, then a balanced fill spreads
 * the ordinary actors while singletons pin to the coordinator and non-relocatable
 * actors are freed. Each recreate is a compare-and-set gated on the record still
 * naming the dead node, so the pass is idempotent and a successor coordinator
 * resuming it collapses already-done work rather than double-building. A periodic
 * self-tick sweeps any record still naming a departed node, so a recreate that could
 * not be placed is retried until it lands. A pass that fails wholesale, a scan or a
 * free against a cluster still converging on the departure, is absorbed for the same
 * reason: this actor is the recovery mechanism, so it must outlive every failed pass
 * for the sweep to retry it.
 *
 * @internal
 */
export class Relocator implements Actor {
  readonly #deps: RelocatorDeps;
  /** Whether the recurring sweep has been armed, so it is scheduled exactly once. */
  #armed: boolean = false;

  constructor(deps: RelocatorDeps) {
    this.#deps = deps;
  }

  /** No initialization state; the sweep is armed on the first message, once the
   * system is running and this actor can schedule against it. */
  preStart(): void {}

  async receive(ctx: ReceiveContext): Promise<void> {
    await this.#armSweep(ctx);

    const message: unknown = ctx.message;
    if (message instanceof NodeDeparted) {
      await this.#guarded((): Promise<void> => this.#recover([message.address]));
      return;
    }

    if (message instanceof RelocationSweep) {
      await this.#guarded((): Promise<void> => this.#sweep());
      return;
    }
  }

  /** Runs one recovery pass, absorbing its rejection. A scan or a free can fail
   * while the cluster is still converging on a departure, a member already dead but
   * not yet declared so, and a failure escaping here would fault the actor and end
   * relocation for the life of this node; dropped instead, because the armed sweep
   * re-runs the recovery until every orphan is re-owned. */
  async #guarded(pass: () => Promise<void>): Promise<void> {
    try {
      await pass();
    } catch {
      // Absorbed: the next sweep tick retries the whole pass.
    }
  }

  postStop(): void {}

  /** Arms the recurring orphan sweep on the first message this actor handles: a
   * system actor started before its system is running never receives `PostStart`, so
   * the first delivered message, an initial tick or a departure, is where the sweep
   * is scheduled. The schedule is owned by this actor and cancelled when it stops. */
  async #armSweep(ctx: ReceiveContext): Promise<void> {
    if (this.#armed) {
      return;
    }

    const self: PID | undefined = ctx.self;
    if (self === undefined) {
      return;
    }

    try {
      await ctx.schedule(new RelocationSweep(), self, this.#deps.sweepIntervalMs);
      this.#armed = true;
    } catch {
      // A schedule that could not be placed leaves the sweep unarmed so the next
      // message retries it, and does not fail this message's own recovery; the
      // departure that arrived alongside it still drives the coordinator's pass.
    }
  }

  /** Re-fills any record still naming a departed node, the backstop pass. Only the
   * coordinator acts, and it short-circuits when no record names a departed member. */
  async #sweep(): Promise<void> {
    if (this.#deps.coordinator() !== this.#deps.self) {
      return;
    }

    const placements: PlacementRecord[] = await this.#deps.scanPlacements();
    const live: Set<string> = new Set(
      this.#deps.members().map((member: { name: string }): string => member.name),
    );
    const departed: string[] = [
      ...new Set(
        placements
          .filter((placement: PlacementRecord): boolean => !live.has(placement.owner))
          .map((placement: PlacementRecord): string => placement.owner),
      ),
    ];
    if (departed.length === 0) {
      return;
    }

    await this.#relocate(departed, placements);
  }

  /** Recovers the actors of the departed nodes, only when this node is coordinator. */
  async #recover(departed: string[]): Promise<void> {
    if (this.#deps.coordinator() !== this.#deps.self) {
      return;
    }

    const placements: PlacementRecord[] = await this.#deps.scanPlacements();
    await this.#relocate(departed, placements);
  }

  /** Relocates every departed node's orphans from one shared scan, so each survivor's
   * running load is counted once and each departure's fill accounts for the actors the
   * earlier ones already placed. */
  async #relocate(departed: string[], placements: PlacementRecord[]): Promise<void> {
    const dead: Set<string> = new Set(departed);
    const counts: Map<string, number> = new Map();
    for (const placement of placements) {
      if (!dead.has(placement.owner)) {
        counts.set(placement.owner, (counts.get(placement.owner) ?? 0) + 1);
      }
    }

    for (const deadOwner of departed) {
      const orphans: PlacementRecord[] = placements.filter(
        (placement: PlacementRecord): boolean => placement.owner === deadOwner,
      );
      if (orphans.length > 0) {
        await this.#relocateOne(deadOwner, orphans, counts);
      }
    }
  }

  /** Relocates one departed node's orphans: classify, plan by balanced fill, recreate,
   * and report the pass on the event stream. */
  async #relocateOne(
    deadOwner: string,
    orphans: PlacementRecord[],
    counts: Map<string, number>,
  ): Promise<void> {
    this.#deps.publish(new RelocationStarted(deadOwner, this.#deps.now()));

    const singletons: { name: string; recipe: ActorRecipe }[] = [];
    const ordinary: { name: string; recipe: ActorRecipe }[] = [];
    const nonRelocatable: string[] = [];
    for (const orphan of orphans) {
      const companion: Companion | undefined = readCompanion(orphan);
      if (companion === undefined) {
        nonRelocatable.push(orphan.name);
        continue;
      }

      const bucket: { name: string; recipe: ActorRecipe }[] = companion.singleton
        ? singletons
        : ordinary;
      bucket.push({ name: orphan.name, recipe: companion.recipe });
    }

    for (const name of nonRelocatable) {
      await this.#deps.free(name, deadOwner);
    }

    const failed: string[] = [];
    const reassignments: Reassignment[] = this.#plan(
      deadOwner,
      ordinary,
      singletons,
      counts,
      failed,
    );

    const relocated: string[] = await this.#recreateAll(reassignments, deadOwner, counts, failed);

    if (failed.length > 0) {
      this.#deps.publish(new RelocationFailed(deadOwner, failed, this.#deps.now()));
    }

    this.#deps.publish(new RelocationCompleted(deadOwner, relocated, this.#deps.now()));
  }

  /** The plan for one departed node: ordinary actors by balanced fill across the
   * survivors, singletons pinned to the coordinator; an ordinary actor with no
   * survivor to take it is a failure this pass reports and a later sweep retries. */
  #plan(
    deadOwner: string,
    ordinary: { name: string; recipe: ActorRecipe }[],
    singletons: { name: string; recipe: ActorRecipe }[],
    counts: Map<string, number>,
    failed: string[],
  ): Reassignment[] {
    const survivors: FillTarget[] = this.#deps
      .members()
      .filter(
        (member: { name: string; ready: boolean; draining: boolean }): boolean =>
          member.ready && !member.draining && member.name !== deadOwner,
      )
      .map(
        (member: { name: string }): FillTarget => ({
          id: member.name,
          count: counts.get(member.name) ?? 0,
        }),
      );

    const owners: Map<string, string> = new Map(
      planFill(
        ordinary.map((actor: { name: string }): string => actor.name),
        survivors,
      ).map((assignment: FillAssignment): [string, string] => [assignment.name, assignment.owner]),
    );

    const reassignments: Reassignment[] = [];
    for (const actor of ordinary) {
      const owner: string | undefined = owners.get(actor.name);
      if (owner === undefined) {
        failed.push(actor.name);
        continue;
      }

      reassignments.push({ name: actor.name, owner, recipe: actor.recipe, singleton: false });
    }

    for (const actor of singletons) {
      reassignments.push({
        name: actor.name,
        owner: this.#deps.self,
        recipe: actor.recipe,
        singleton: true,
      });
    }

    return reassignments;
  }

  /** Recreates every reassignment concurrently, returning the names given a new
   * owner and appending the ones whose build failed to `failed`. A recreate that
   * finds the record already moved is neither: another pass placed it. */
  async #recreateAll(
    reassignments: Reassignment[],
    deadOwner: string,
    counts: Map<string, number>,
    failed: string[],
  ): Promise<string[]> {
    const outcomes: { name: string; owner: string; placed: boolean; failed: boolean }[] =
      await Promise.all(
        reassignments.map(
          async (
            reassignment: Reassignment,
          ): Promise<{ name: string; owner: string; placed: boolean; failed: boolean }> => {
            try {
              const placed: boolean = await this.#deps.recreate(
                reassignment.owner,
                reassignment.name,
                reassignment.recipe,
                reassignment.singleton,
                deadOwner,
              );
              return { name: reassignment.name, owner: reassignment.owner, placed, failed: false };
            } catch {
              return {
                name: reassignment.name,
                owner: reassignment.owner,
                placed: false,
                failed: true,
              };
            }
          },
        ),
      );

    const relocated: string[] = [];
    for (const outcome of outcomes) {
      if (outcome.failed) {
        failed.push(outcome.name);
        continue;
      }

      if (outcome.placed) {
        relocated.push(outcome.name);
        counts.set(outcome.owner, (counts.get(outcome.owner) ?? 0) + 1);
      }
    }

    return relocated;
  }
}

/** An orphan's companion, or undefined when it stores none, or one whose bytes will
 * not decode, in which case its name is freed like a non-relocatable actor's. */
function readCompanion(orphan: PlacementRecord): Companion | undefined {
  if (orphan.companion === undefined) {
    return undefined;
  }

  try {
    return decodeCompanion(orphan.companion);
  } catch {
    return undefined;
  }
}
