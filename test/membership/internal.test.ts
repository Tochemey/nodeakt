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

const membershipDirectory = fileURLToPath(new URL("../../src/membership", import.meta.url));

function documentationBefore(lines: readonly string[], exportLine: number): string {
  let end = exportLine - 1;
  while (end >= 0 && (lines[end] as string).trim().length === 0) {
    end -= 1;
  }

  if (end < 0 || !(lines[end] as string).includes("*/")) {
    return "";
  }

  let start = end;
  while (start >= 0 && !(lines[start] as string).includes("/**")) {
    start -= 1;
  }

  return start < 0 ? "" : lines.slice(start, end + 1).join("\n");
}

describe("membership declaration visibility", () => {
  it("marks every exported declaration as internal", () => {
    const failures: string[] = [];
    const files = readdirSync(membershipDirectory).filter((name: string): boolean =>
      name.endsWith(".ts"),
    );

    for (const file of files) {
      const lines = readFileSync(join(membershipDirectory, file), "utf8").split(/\r?\n/);
      for (let index = 0; index < lines.length; index += 1) {
        if (!(lines[index] as string).startsWith("export ")) {
          continue;
        }

        const documentation = documentationBefore(lines, index);
        if (!documentation.includes("@internal")) {
          failures.push(`${file}:${index + 1}`);
        }
      }
    }

    expect(failures).toEqual([]);
  });
});
