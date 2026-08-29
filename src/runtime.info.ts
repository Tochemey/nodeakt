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

import { readFileSync } from "node:fs";

/** The version fields a runtime exposes; `bun` and `deno` mark those runtimes. */
interface RuntimeVersions {
  readonly node: string;
  readonly bun?: string;
  readonly deno?: string;
}

/** Names the runtime and its version, `bun` and `deno` taking precedence over
 * the `node` version they both also report under compatibility.
 *
 * @internal
 */
export function describeRuntime(versions: RuntimeVersions): string {
  if (versions.bun !== undefined) {
    return `bun ${versions.bun}`;
  }

  if (versions.deno !== undefined) {
    return `deno ${versions.deno}`;
  }

  return `node ${versions.node}`;
}

/** Reads the package version through `read`, answering `unknown` when the file
 * is unreadable or carries no string version.
 *
 * @internal
 */
export function readVersion(read: (url: URL) => string): string {
  try {
    const parsed: unknown = JSON.parse(read(new URL("../package.json", import.meta.url)));
    const version: unknown = (parsed as { version?: unknown }).version;
    return typeof version === "string" ? version : "unknown";
  } catch {
    return "unknown";
  }
}

/** The startup banner fields for a named system: which runtime, on which OS and
 * CPU, at what package version.
 *
 * @internal
 */
export function startupBanner(name: string): Record<string, unknown> {
  return {
    name,
    runtime: describeRuntime(process.versions as unknown as RuntimeVersions),
    os: `${process.platform}/${process.arch}`,
    version: readVersion((url: URL): string => readFileSync(url, "utf8")),
  };
}
