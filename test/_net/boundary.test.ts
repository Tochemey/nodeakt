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

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

/**
 * The transport boundary, enforced: `src/_net/` modules import only
 * platform modules and each other, and the only module outside the
 * folder allowed to import from it is the `remoting.ts` seam. A
 * violation here is a circular-dependency risk by definition.
 */

const srcDir: string = fileURLToPath(new URL("../../src", import.meta.url));
const netDir: string = join(srcDir, "_net");

/** Collects every static import specifier in a source file. */
function importsOf(filePath: string): string[] {
  const source: string = readFileSync(filePath, "utf8");
  const specifiers: string[] = [];
  const pattern: RegExp = /(?:from|import)\s+"([^"]+)"/g;
  for (const match of source.matchAll(pattern)) {
    specifiers.push(match[1] as string);
  }

  return specifiers;
}

function sourceFiles(directory: string): string[] {
  return readdirSync(directory)
    .filter((name: string): boolean => name.endsWith(".ts"))
    .map((name: string): string => join(directory, name));
}

describe("the _net boundary", () => {
  it("keeps _net modules free of runtime imports", () => {
    for (const filePath of sourceFiles(netDir)) {
      for (const specifier of importsOf(filePath)) {
        const allowed: boolean = specifier.startsWith("node:") || /^\.\/[^.]/.test(specifier);
        expect(allowed, `${filePath} imports "${specifier}"`).toBe(true);
      }
    }
  });

  it("lets only the remoting seam import from _net", () => {
    for (const filePath of sourceFiles(srcDir)) {
      if (filePath.endsWith("/remoting.ts")) {
        continue;
      }

      for (const specifier of importsOf(filePath)) {
        expect(specifier.includes("_net"), `${filePath} imports "${specifier}"`).toBe(false);
      }
    }
  });
});
