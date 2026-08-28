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
import { StaticDiscovery } from "../../src/discovery/static";

describe("StaticDiscovery", () => {
  it("resolves the configured seeds", async () => {
    const discovery: StaticDiscovery = new StaticDiscovery(["a:1", "b:2"]);
    const seeds: readonly string[] = await discovery.resolve();
    expect(seeds).toEqual(["a:1", "b:2"]);
  });

  it("resolves an empty list for a single-node deployment", async () => {
    const discovery: StaticDiscovery = new StaticDiscovery([]);
    const seeds: readonly string[] = await discovery.resolve();
    expect(seeds).toEqual([]);
  });

  it("trims surrounding whitespace from each seed", async () => {
    const discovery: StaticDiscovery = new StaticDiscovery(["  a:1 ", "\tb:2\n"]);
    const seeds: readonly string[] = await discovery.resolve();
    expect(seeds).toEqual(["a:1", "b:2"]);
  });

  it("rejects a blank seed", () => {
    expect((): StaticDiscovery => new StaticDiscovery(["a:1", "   "])).toThrow(TypeError);
    expect((): StaticDiscovery => new StaticDiscovery([""])).toThrow(TypeError);
  });

  it("returns an independent copy that a caller cannot mutate into the provider", async () => {
    const discovery: StaticDiscovery = new StaticDiscovery(["a:1"]);
    const first: string[] = [...(await discovery.resolve())];
    first.push("tampered:9");
    const second: readonly string[] = await discovery.resolve();
    expect(second).toEqual(["a:1"]);
  });
});
