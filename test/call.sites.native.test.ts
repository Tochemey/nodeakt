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

// Some runtimes report a null script or line for a native or internal frame
// (Deno does), even though the type says otherwise. The native capture path
// must normalize those to the string/number shape its callers rely on, or a
// consumer like the logger's caller resolver reads a property off null.
vi.mock("node:util", async (importActual) => {
  const actual = await importActual<typeof import("node:util")>();
  return {
    ...actual,
    getCallSites: (): unknown[] => [
      { scriptName: "/anchor.ts", lineNumber: 1 },
      { scriptName: null, lineNumber: null },
      { scriptName: "/app.ts", lineNumber: 7 },
    ],
  };
});

import { type CallSiteScript, captureCallSites } from "../src/call.sites";

describe("captureCallSites native normalization", () => {
  it("normalizes a null script and line to an empty string and zero", () => {
    const sites: ReadonlyArray<CallSiteScript> = captureCallSites(2);

    expect(sites).toEqual([
      { scriptName: "", lineNumber: 0 },
      { scriptName: "/app.ts", lineNumber: 7 },
    ]);
  });
});
