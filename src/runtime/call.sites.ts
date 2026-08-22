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

import * as util from "node:util";

/** The one call-site field registration reads: the script that owns the
 * frame, as a path or URL.
 *
 * @internal
 */
export interface CallSiteScript {
  readonly scriptName: string;
}

/** The V8 stack-trace API surface the fallback relies on. Both methods
 * answer the frame's script; `getScriptNameOrSourceURL` is preferred
 * because it survives source maps, and not every runtime exposes it.
 *
 * @internal
 */
interface V8CallSite {
  getFileName(): string | null | undefined;
  getScriptNameOrSourceURL?(): string | null | undefined;
}

/** `Error.captureStackTrace` and `Error.prepareStackTrace` as optional
 * members, so their absence is a checkable state instead of a crash.
 *
 * @internal
 */
interface StackTraceCapableError {
  captureStackTrace?(target: object, aboveFn?: (...args: never[]) => unknown): void;
  prepareStackTrace?: unknown;
}

/**
 * Captures up to `frameCount` call sites, frame 0 being this function's
 * caller. Runtimes with `util.getCallSites` answer through it; the rest
 * answer through the V8 stack-trace API when they expose one. A runtime
 * with neither gets an empty list, which callers surface as "pass the
 * module URL explicitly".
 *
 * @internal
 */
export function captureCallSites(frameCount: number): ReadonlyArray<CallSiteScript> {
  if (typeof util.getCallSites === "function") {
    // Frame 0 of the native answer is this function itself; one extra
    // frame is requested so dropping it still fills the count.
    return util.getCallSites(frameCount + 1).slice(1);
  }

  // The count is applied here, after the fallback returns, so that call
  // is never in tail position no matter how the code is bundled: an
  // engine that eliminates tail calls would otherwise drop this frame,
  // and with it the anchor the fallback cuts the stack at.
  return v8CallSites(captureCallSites).slice(0, frameCount);
}

/** The V8 fallback behind {@link captureCallSites}: every frame up to
 * and including `anchor` is omitted, so frame 0 is `anchor`'s caller.
 * The anchor must be on the stack when this runs; a call whose result
 * feeds further work is guaranteed to keep it there. Restores
 * `Error.prepareStackTrace` before returning, whatever value it held.
 *
 * @internal
 */
export function v8CallSites(anchor: (...args: never[]) => unknown): ReadonlyArray<CallSiteScript> {
  const errorType = Error as StackTraceCapableError;
  if (typeof errorType.captureStackTrace !== "function") {
    return [];
  }

  const previous: unknown = errorType.prepareStackTrace;
  try {
    errorType.prepareStackTrace = (_err: Error, sites: ReadonlyArray<V8CallSite>) => sites;
    const holder: { stack?: unknown } = {};
    errorType.captureStackTrace(holder, anchor);
    const sites: unknown = holder.stack;
    if (!Array.isArray(sites)) {
      return [];
    }

    return sites.map((site: V8CallSite): CallSiteScript => {
      const script: string | null | undefined =
        site.getScriptNameOrSourceURL?.() ?? site.getFileName();
      return { scriptName: script ?? "" };
    });
  } finally {
    errorType.prepareStackTrace = previous;
  }
}
