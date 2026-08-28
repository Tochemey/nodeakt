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
import { type FillAssignment, type FillTarget, planFill } from "../src/clustering.relocation";

/** The owner each name is assigned, keyed by name, for order-independent assertions. */
function ownersByName(assignments: FillAssignment[]): Record<string, string> {
  return Object.fromEntries(
    assignments.map((assignment: FillAssignment): [string, string] => [
      assignment.name,
      assignment.owner,
    ]),
  );
}

/** How many names each owner was given, for balance assertions. */
function loadByOwner(assignments: FillAssignment[]): Map<string, number> {
  const load: Map<string, number> = new Map();
  for (const assignment of assignments) {
    load.set(assignment.owner, (load.get(assignment.owner) ?? 0) + 1);
  }

  return load;
}

describe("planFill", () => {
  it("places nothing when there are no survivors", () => {
    expect(planFill(["a", "b"], [])).toEqual([]);
  });

  it("places every orphan on the one survivor there is", () => {
    const survivors: FillTarget[] = [{ id: "s:1", count: 5 }];

    expect(ownersByName(planFill(["a", "b", "c"], survivors))).toEqual({
      a: "s:1",
      b: "s:1",
      c: "s:1",
    });
  });

  it("spreads orphans evenly across empty survivors", () => {
    const survivors: FillTarget[] = [
      { id: "a:1", count: 0 },
      { id: "b:1", count: 0 },
    ];

    const load: Map<string, number> = loadByOwner(planFill(["w", "x", "y", "z"], survivors));

    expect(load.get("a:1")).toBe(2);
    expect(load.get("b:1")).toBe(2);
  });

  it("breaks ties by the lower cluster address, deterministically", () => {
    const survivors: FillTarget[] = [
      { id: "b:1", count: 0 },
      { id: "a:1", count: 0 },
    ];

    // Both start empty, so the first orphan by name lands on the lower address, then
    // the second on the other. Every coordinator computes this same plan.
    expect(ownersByName(planFill(["one", "two"], survivors))).toEqual({
      one: "a:1",
      two: "b:1",
    });
  });

  it("raises the emptiest members first, leaving a loaded one alone", () => {
    const survivors: FillTarget[] = [
      { id: "busy:1", count: 3 },
      { id: "idle:1", count: 0 },
    ];

    // Both orphans go to the idle member, which is still below the busy one after them.
    expect(ownersByName(planFill(["a", "b"], survivors))).toEqual({
      a: "idle:1",
      b: "idle:1",
    });
  });

  it("lands an indivisible remainder on the emptiest members, ties by id", () => {
    const survivors: FillTarget[] = [
      { id: "a:1", count: 0 },
      { id: "b:1", count: 0 },
    ];

    const load: Map<string, number> = loadByOwner(planFill(["o1", "o2", "o3"], survivors));

    // Three across two ends one apart, the remainder on the lower address.
    expect(load.get("a:1")).toBe(2);
    expect(load.get("b:1")).toBe(1);
  });
});
