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

import { describe, expect, it, vi } from "vitest";
import { type CallSiteScript, captureCallSites } from "../src/call.sites";

// The runtimes this fallback exists for lack util.getCallSites while
// still speaking the V8 stack-trace API; removing the function from the
// mocked module reproduces them on the test runner itself.
vi.mock("node:util", async (importOriginal: () => Promise<object>) => {
  const actual: object = await importOriginal();
  return { ...actual, getCallSites: undefined };
});

describe("captureCallSites without util.getCallSites", () => {
  it("answers frame 0 as the caller's script through the V8 fallback", () => {
    const sites: ReadonlyArray<CallSiteScript> = captureCallSites(1);

    expect(sites.length).toBe(1);
    expect(sites[0]?.scriptName).toContain("call.sites.fallback.test");
  });

  it("caps the answer at the requested frame count", () => {
    const sites: ReadonlyArray<CallSiteScript> = captureCallSites(2);

    expect(sites.length).toBeLessThanOrEqual(2);
  });
});
