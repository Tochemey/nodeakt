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

const membershipDir = fileURLToPath(new URL("../../src/membership", import.meta.url));

function membershipSources(): readonly string[] {
  return readdirSync(membershipDir)
    .filter((name: string): boolean => name.endsWith(".ts"))
    .map((name: string): string => join(membershipDir, name));
}

describe("membership dependency injection boundaries", () => {
  it("confines wall clocks and timers to clock.ts", () => {
    const ambientTime = /\b(?:Date\.now|setTimeout|clearTimeout|setInterval|clearInterval)\b/;
    for (const path of membershipSources()) {
      if (basename(path) === "clock.ts") {
        continue;
      }
      expect(readFileSync(path, "utf8"), path).not.toMatch(ambientTime);
    }
  });

  it("confines entropy to random.ts", () => {
    const ambientEntropy = /\b(?:Math\.random|randomBytes|randomInt|getRandomValues|randomUUID)\b/;
    for (const path of membershipSources()) {
      if (basename(path) === "random.ts") {
        continue;
      }
      expect(readFileSync(path, "utf8"), path).not.toMatch(ambientEntropy);
    }
  });
});
