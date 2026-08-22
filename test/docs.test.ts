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
import { describe, expect, it } from "vitest";

const root = join(import.meta.dirname, "..");

/**
 * Collects every identifier exported from the package entry point, the
 * surface the documentation promises to cover.
 */
function exportedNames(): string[] {
  const source = readFileSync(join(root, "src/index.ts"), "utf8");
  const names: string[] = [];
  for (const block of source.matchAll(/export (?:type )?\{([^}]*)\}/g)) {
    const body = block[1];
    if (body === undefined) {
      continue;
    }

    for (const entry of body.split(",")) {
      const name = entry.replace(/\btype\b/, "").trim();
      if (name.length > 0) {
        names.push(name);
      }
    }
  }

  return names;
}

/** Concatenates every markdown page under docs/. */
function docsCorpus(): string {
  const pages: string[] = [];
  const walk = (dir: string): void => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === ".vitepress") {
        continue;
      }

      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(path);
      } else if (entry.name.endsWith(".md")) {
        pages.push(readFileSync(path, "utf8"));
      }
    }
  };

  walk(join(root, "docs"));
  return pages.join("\n");
}

describe("documentation coverage", () => {
  it("mentions every name exported from the package entry point", () => {
    const corpus = docsCorpus();
    const missing = exportedNames().filter((name) => !corpus.includes(name));
    expect(missing).toEqual([]);
  });
});
