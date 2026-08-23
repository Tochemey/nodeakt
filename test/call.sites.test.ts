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
import { type CallSiteScript, captureCallSites, v8CallSites } from "../src/call.sites";

describe("captureCallSites", () => {
  it("answers frame 0 as the caller's script", () => {
    const sites: ReadonlyArray<CallSiteScript> = captureCallSites(1);

    expect(sites.length).toBe(1);
    expect(sites[0]?.scriptName).toContain("call.sites.test");
  });

  it("caps the answer at the requested frame count", () => {
    const sites: ReadonlyArray<CallSiteScript> = captureCallSites(2);

    expect(sites.length).toBeLessThanOrEqual(2);
  });
});

describe("v8CallSites", () => {
  it("omits every frame up to and including the anchor", () => {
    function probe(): ReadonlyArray<CallSiteScript> {
      const sites: ReadonlyArray<CallSiteScript> = v8CallSites(probe);
      return sites;
    }

    const sites: ReadonlyArray<CallSiteScript> = probe();

    expect(sites.length).toBeGreaterThan(0);
    expect(sites[0]?.scriptName).toContain("call.sites.test");
  });

  it("restores a previously installed prepareStackTrace hook", () => {
    const previous: unknown = Error.prepareStackTrace;
    const marker = (_err: Error, _sites: ReadonlyArray<unknown>): string => "marker";
    Error.prepareStackTrace = marker as typeof Error.prepareStackTrace;

    try {
      function probe(): ReadonlyArray<CallSiteScript> {
        const sites: ReadonlyArray<CallSiteScript> = v8CallSites(probe);
        return sites;
      }

      probe();
      expect(Error.prepareStackTrace).toBe(marker);
    } finally {
      Error.prepareStackTrace = previous as typeof Error.prepareStackTrace;
    }
  });

  it("answers empty on a runtime without captureStackTrace", () => {
    const saved: typeof Error.captureStackTrace = Error.captureStackTrace;
    delete (Error as { captureStackTrace?: unknown }).captureStackTrace;

    try {
      const sites: ReadonlyArray<CallSiteScript> = v8CallSites(() => undefined);
      expect(sites).toEqual([]);
    } finally {
      Error.captureStackTrace = saved;
    }
  });

  it("answers empty when the runtime ignores the prepareStackTrace hook", () => {
    const saved: typeof Error.captureStackTrace = Error.captureStackTrace;
    Error.captureStackTrace = (target: object): void => {
      (target as { stack?: unknown }).stack = "a plain formatted stack string";
    };

    try {
      const sites: ReadonlyArray<CallSiteScript> = v8CallSites(() => undefined);
      expect(sites).toEqual([]);
    } finally {
      Error.captureStackTrace = saved;
    }
  });

  it("falls back through the script accessors a frame may lack", () => {
    const saved: typeof Error.captureStackTrace = Error.captureStackTrace;
    Error.captureStackTrace = (target: object): void => {
      (target as { stack?: unknown }).stack = [
        { getFileName: (): string => "/from/get-file-name.ts" },
        {
          getFileName: (): null => null,
          getScriptNameOrSourceURL: (): string => "file:///from/source-url.ts",
        },
        { getFileName: (): null => null },
      ];
    };

    try {
      const sites: ReadonlyArray<CallSiteScript> = v8CallSites(() => undefined);
      expect(sites.map((site: CallSiteScript): string => site.scriptName)).toEqual([
        "/from/get-file-name.ts",
        "file:///from/source-url.ts",
        "",
      ]);
    } finally {
      Error.captureStackTrace = saved;
    }
  });
});
