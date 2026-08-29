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
import { describeRuntime, readVersion, startupBanner } from "../src/runtime.info";

describe("describeRuntime", () => {
  it("names bun and deno ahead of the node version they also report", () => {
    expect(describeRuntime({ node: "22.0.0" })).toBe("node 22.0.0");
    expect(describeRuntime({ node: "22.0.0", bun: "1.3.0" })).toBe("bun 1.3.0");
    expect(describeRuntime({ node: "22.0.0", deno: "2.0.0" })).toBe("deno 2.0.0");
  });
});

describe("readVersion", () => {
  it("answers the string version from the package file", () => {
    expect(readVersion((): string => JSON.stringify({ version: "9.9.9" }))).toBe("9.9.9");
  });

  it("answers unknown when the file is unreadable", () => {
    expect(
      readVersion((): string => {
        throw new Error("missing");
      }),
    ).toBe("unknown");
  });

  it("answers unknown when the version is absent or not a string", () => {
    expect(readVersion((): string => JSON.stringify({}))).toBe("unknown");
    expect(readVersion((): string => JSON.stringify({ version: 3 }))).toBe("unknown");
  });
});

describe("startupBanner", () => {
  it("carries the name, runtime, os, and the package version", () => {
    const banner: Record<string, unknown> = startupBanner("orders");

    expect(banner.name).toBe("orders");
    expect(typeof banner.runtime).toBe("string");
    expect(banner.os).toBe(`${process.platform}/${process.arch}`);
    expect(banner.version).toMatch(/^\d+\.\d+\.\d+$/);
  });
});
