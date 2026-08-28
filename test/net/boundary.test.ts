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
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

/**
 * The transport, membership, key/value, and discovery boundaries, enforced:
 * modules inside each package import only platform modules and their own
 * package. For the internal subsystems only a flat seam module may import them
 * from outside; discovery is a public module with no such restriction, but it
 * still depends on nothing beyond the platform, so it stays free of the actor
 * runtime. A violation here is a circular-dependency risk by definition.
 */

const srcDir: string = fileURLToPath(new URL("../../src", import.meta.url));
const netDir: string = join(srcDir, "net");
const membershipDir: string = join(srcDir, "membership");
const kvDir: string = join(srcDir, "kv");
const discoveryDir: string = join(srcDir, "discovery");

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

/** Whether a file is a package's flat seam module: `<seam>.ts` or `<seam>.*.ts`. */
function isSeamModule(base: string, seam: string): boolean {
  return base === `${seam}.ts` || base.startsWith(`${seam}.`);
}

function sourceFiles(directory: string): string[] {
  // A missing or empty package directory must fail the boundary loudly: a
  // silent empty list would let a rename disable enforcement with a green run.
  const files: string[] = readdirSync(directory)
    .filter((name: string): boolean => name.endsWith(".ts"))
    .map((name: string): string => join(directory, name));
  expect(files.length, `no sources found under ${directory}`).toBeGreaterThan(0);
  return files;
}

describe("the net boundary", () => {
  it("keeps net modules free of runtime imports", () => {
    for (const filePath of sourceFiles(netDir)) {
      for (const specifier of importsOf(filePath)) {
        const allowed: boolean = specifier.startsWith("node:") || /^\.\/[^.]/.test(specifier);
        expect(allowed, `${filePath} imports "${specifier}"`).toBe(true);
      }
    }
  });

  it("lets only the remoting and clustering seams import from net", () => {
    for (const filePath of sourceFiles(srcDir)) {
      const base: string = basename(filePath);
      if (isSeamModule(base, "remoting") || isSeamModule(base, "clustering")) {
        continue;
      }

      for (const specifier of importsOf(filePath)) {
        expect(/(^|\/)net\//.test(specifier), `${filePath} imports "${specifier}"`).toBe(false);
      }
    }
  });
});

describe("the membership boundary", () => {
  it("keeps membership modules free of runtime and net imports", () => {
    for (const filePath of sourceFiles(membershipDir)) {
      for (const specifier of importsOf(filePath)) {
        const allowed: boolean = specifier.startsWith("node:") || /^\.\/[^.]/.test(specifier);
        expect(allowed, `${filePath} imports "${specifier}"`).toBe(true);
      }
    }
  });

  it("lets only the clustering seam import from membership", () => {
    for (const filePath of sourceFiles(srcDir)) {
      const base: string = basename(filePath);
      if (isSeamModule(base, "clustering")) {
        continue;
      }

      for (const specifier of importsOf(filePath)) {
        expect(/(^|\/)membership\//.test(specifier), `${filePath} imports "${specifier}"`).toBe(
          false,
        );
      }
    }
  });
});

describe("the kv boundary", () => {
  it("keeps kv modules free of runtime, net, and membership imports", () => {
    for (const filePath of sourceFiles(kvDir)) {
      for (const specifier of importsOf(filePath)) {
        const allowed: boolean = specifier.startsWith("node:") || /^\.\/[^.]/.test(specifier);
        expect(allowed, `${filePath} imports "${specifier}"`).toBe(true);
      }
    }
  });

  it("lets only the clustering seam import from kv", () => {
    for (const filePath of sourceFiles(srcDir)) {
      const base: string = basename(filePath);
      if (isSeamModule(base, "clustering")) {
        continue;
      }

      for (const specifier of importsOf(filePath)) {
        expect(/(^|\/)kv\//.test(specifier), `${filePath} imports "${specifier}"`).toBe(false);
      }
    }
  });
});

describe("the discovery boundary", () => {
  it("keeps discovery modules free of runtime, net, membership, and kv imports", () => {
    for (const filePath of sourceFiles(discoveryDir)) {
      for (const specifier of importsOf(filePath)) {
        const allowed: boolean = specifier.startsWith("node:") || /^\.\/[^.]/.test(specifier);
        expect(allowed, `${filePath} imports "${specifier}"`).toBe(true);
      }
    }
  });
});
