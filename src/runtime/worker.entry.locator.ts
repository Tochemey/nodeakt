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

let override: string | URL | null = null;

/**
 * Overrides where the system-owned pool finds the built worker entry.
 * Tests point it at their freshly built entry; production never calls
 * it. Runtime plumbing.
 *
 * @internal
 */
export function setWorkerEntry(entry: string | URL | null): void {
  override = entry;
}

/**
 * Returns the worker entry script the system-owned pool boots its
 * isolates from. The published package ships the built entry beside
 * its own modules, so the default resolves relative to this file;
 * {@link setWorkerEntry} overrides it for tests running from source.
 *
 * @internal
 */
export function workerEntry(): string | URL {
  return override ?? new URL("./worker.entry.mjs", import.meta.url);
}
