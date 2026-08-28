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

/** A survivor a relocation may fill, its cluster address and how many actors it
 * already owns before the fill. @internal */
export interface FillTarget {
  /** The survivor's cluster address, the owner recorded for a name placed on it. */
  readonly id: string;

  /** How many actors it owns now, the level the fill raises from. */
  readonly count: number;
}

/** One name's assignment to the survivor a relocation places it on. @internal */
export interface FillAssignment {
  /** The relocated actor's name. */
  readonly name: string;

  /** The survivor's cluster address the name is placed on. */
  readonly owner: string;
}

/**
 * Distributes `orphans` across `survivors` by balanced fill: each name lands on the
 * survivor that is currently emptiest, ties broken by cluster address, so the counts
 * end up equal across the cluster to within one and any indivisible remainder lands
 * on the emptiest members. The orphans are placed in name order and the choice is a
 * deterministic function of the inputs, so every coordinator computes the identical
 * plan and a successor's conditional writes collapse already-done work rather than
 * reshuffle. An empty survivor set yields no assignments; the caller keeps the
 * orphans it cannot place.
 *
 * @internal
 */
export function planFill(
  orphans: readonly string[],
  survivors: readonly FillTarget[],
): FillAssignment[] {
  if (survivors.length === 0) {
    return [];
  }

  const load: { id: string; count: number }[] = survivors.map(
    (survivor: FillTarget): { id: string; count: number } => ({
      id: survivor.id,
      count: survivor.count,
    }),
  );

  const ordered: string[] = [...orphans].sort();
  const assignments: FillAssignment[] = [];
  for (const name of ordered) {
    const target: { id: string; count: number } = emptiest(load);
    assignments.push({ name, owner: target.id });
    target.count += 1;
  }

  return assignments;
}

/** The emptiest entry in `load`, ties broken by the lower cluster address, so the
 * choice is stable across nodes. */
function emptiest(load: { id: string; count: number }[]): {
  id: string;
  count: number;
} {
  let best: { id: string; count: number } = load[0] as {
    id: string;
    count: number;
  };

  for (const target of load) {
    if (target.count < best.count || (target.count === best.count && target.id < best.id)) {
      best = target;
    }
  }

  return best;
}
