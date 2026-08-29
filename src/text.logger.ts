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

import { isMainThread, threadId } from "node:worker_threads";
import { type CallSiteScript, captureCallSites } from "./call.sites";
import {
  type EntryLevel,
  type Fields,
  type LazyFields,
  type Level,
  type Logger,
  levelWeight,
} from "./logger";

/** Configuration for a {@link TextLogger}. */
export interface TextLoggerOptions {
  /** The minimum level to emit; `info` by default. */
  level?: Level;

  /** The sink entries are written to; `process.stderr` by default. */
  stream?: NodeJS.WritableStream;

  /** Fields every entry carries; none by default. */
  fields?: Fields;
}

/** Field key lifted into the `[%logger]` column instead of the fields. */
const LOGGER_KEY = "logger";

/** Field key lifted into the `[%marker]` column instead of the fields. */
const MARKER_KEY = "marker";

/** The rendering of a value that cannot be serialized to JSON. */
const UNSERIALIZABLE = "[unserializable]";

/** How many frames to walk when locating the log call site: the two
 * internal frames (the level method and {@link TextLogger.write}) plus a
 * margin for the caller and any wrapper above it. */
const CALLER_FRAME_LIMIT = 6;

/** The three-letter column text for each emittable level. */
const LEVEL_LABEL: Record<EntryLevel, string> = {
  debug: "DBG",
  info: "INF",
  warn: "WRN",
  error: "ERR",
};

/** The `[%thread]` column for a thread: `main` on the main thread and
 * `worker-<id>` inside a worker thread.
 *
 * @internal
 */
export function threadName(main: boolean, id: number): string {
  return main ? "main" : `worker-${id}`;
}

/** The `[%thread]` value for the current thread, fixed for its lifetime. */
const THREAD_LABEL: string = threadName(isMainThread, threadId);

/** The script owning frame 0 of a capture, or `""` when nothing was
 * captured. Used to learn this module's own script at load time.
 *
 * @internal
 */
export function firstScriptName(frames: ReadonlyArray<CallSiteScript>): string {
  return frames[0]?.scriptName ?? "";
}

/** This module's own script, captured through the same mechanism the
 * per-entry capture uses, source maps included, so the two strings
 * compare exactly. Frames from this script are the logger's own machinery
 * and never the call site. */
const OWN_SCRIPT: string = firstScriptName(captureCallSites(1, true));

/** Trims a script path or URL to its final directory and file, the form
 * that identifies a source without the noise of an absolute path. */
function shortenScript(scriptName: string): string {
  const lastSlash: number = scriptName.lastIndexOf("/");
  if (lastSlash === -1) {
    return scriptName;
  }

  const base: string = scriptName.slice(lastSlash + 1);
  const parentStart: number = scriptName.lastIndexOf("/", lastSlash - 1);
  const parent: string = scriptName.slice(parentStart + 1, lastSlash);
  return parent === "" ? base : `${parent}/${base}`;
}

/** The `[%caller]` column: the first captured frame outside this module,
 * rendered as `dir/file:line`, or `""` when none can be determined.
 *
 * @internal
 */
export function resolveCaller(frames: ReadonlyArray<CallSiteScript>, ownScript: string): string {
  if (ownScript === "") {
    return "";
  }

  for (const frame of frames) {
    const script: string = frame.scriptName;
    if (script !== "" && script !== ownScript) {
      return `${shortenScript(script)}:${frame.lineNumber}`;
    }
  }

  return "";
}

/** Renders one field value. Errors return `name: message`
 * and push their stack onto `stacks` for the reader to append below the
 * line; objects serialize as JSON with an `[unserializable]` fallback;
 * everything else stringifies. */
function renderValue(value: unknown, stacks: string[]): string {
  if (value instanceof Error) {
    if (value.stack !== undefined && value.stack !== "") {
      stacks.push(value.stack);
    }

    return `${value.name}: ${value.message}`;
  }

  const type: string = typeof value;
  if (type === "string") {
    return value as string;
  }

  if (type === "object" && value !== null) {
    try {
      return JSON.stringify(value) ?? String(value);
    } catch {
      return UNSERIALIZABLE;
    }
  }

  return String(value);
}

/** Renders a lifted reserved field for its bracketed column: `""` when
 * absent, otherwise the value as text. */
function reservedLabel(value: unknown): string {
  return value === undefined ? "" : String(value);
}

/** Renders an optional column: a non-empty value as ` [value]`, an empty
 * one as nothing, so a missing part leaves no `[]` gap in the line. */
function column(value: string): string {
  return value === "" ? "" : ` [${value}]`;
}

/**
 * TextLogger writes each entry as one human-readable line on standard
 * error, laid out as
 * `[date] [level] [logger] [marker] [thread] [caller] - msg {field=value}`.
 * The `logger` and `marker` fields are lifted into their own columns; the
 * caller is the source location of the log call as `dir/file:line`; the
 * remaining bound and per-entry fields render as comma-separated
 * `key=value` pairs. `Error` values render as `name: message` with their
 * stack on the following lines, and a payload that cannot be serialized
 * still produces a line instead of throwing into the caller.
 *
 * The layout is fixed. Disabled calls return without building anything,
 * and lazy fields resolve only when the entry is emitted.
 */
export class TextLogger implements Logger {
  private readonly _level: Level;
  private readonly _weight: number;
  private readonly _stream: NodeJS.WritableStream;
  private readonly _bound: Fields | null;

  constructor(options?: TextLoggerOptions) {
    this._level = options?.level ?? "info";
    this._weight = levelWeight[this._level];
    this._stream = options?.stream ?? process.stderr;
    this._bound = options?.fields ?? null;
  }

  debug(message: string, fields?: LazyFields): void {
    if (this._weight <= levelWeight.debug) {
      this.write("debug", message, fields);
    }
  }

  info(message: string, fields?: LazyFields): void {
    if (this._weight <= levelWeight.info) {
      this.write("info", message, fields);
    }
  }

  warn(message: string, fields?: LazyFields): void {
    if (this._weight <= levelWeight.warn) {
      this.write("warn", message, fields);
    }
  }

  error(message: string, fields?: LazyFields): void {
    if (this._weight <= levelWeight.error) {
      this.write("error", message, fields);
    }
  }

  level(): Level {
    return this._level;
  }

  enabled(level: EntryLevel): boolean {
    return this._weight <= levelWeight[level];
  }

  with(fields: Fields): Logger {
    return new TextLogger({
      level: this._level,
      stream: this._stream,
      fields: this._bound === null ? { ...fields } : { ...this._bound, ...fields },
    });
  }

  private write(level: EntryLevel, message: string, fields?: LazyFields): void {
    const resolved: Fields | undefined = typeof fields === "function" ? fields() : fields;
    const merged: Fields | null = mergeFields(this._bound, resolved);

    let loggerLabel = "";
    let markerLabel = "";
    let pairs = "";
    let first = true;
    const stacks: string[] = [];

    if (merged !== null) {
      for (const key of Object.keys(merged)) {
        const value: unknown = merged[key];
        if (key === LOGGER_KEY) {
          loggerLabel = reservedLabel(value);
          continue;
        }

        if (key === MARKER_KEY) {
          markerLabel = reservedLabel(value);
          continue;
        }

        const text: string = renderValue(value, stacks);
        pairs += first ? `${key}=${text}` : `, ${key}=${text}`;
        first = false;
      }
    }

    const date: string = new Date().toISOString();
    const caller: string = resolveCaller(captureCallSites(CALLER_FRAME_LIMIT, true), OWN_SCRIPT);

    // Empty parts are dropped rather than rendered as `[]` or `{}`, so a line
    // carries only the fields it actually has.
    let line = `[${date}] [${LEVEL_LABEL[level]}]${column(loggerLabel)}${column(markerLabel)} [${THREAD_LABEL}]${column(caller)} - ${message}`;
    if (pairs !== "") {
      line += ` {${pairs}}`;
    }

    line += "\n";
    for (const stack of stacks) {
      line += `${stack}\n`;
    }

    this._stream.write(line);
  }
}

/** Merges bound and per-entry fields for one line, per-entry winning,
 * without allocating when only one side is present. */
function mergeFields(bound: Fields | null, resolved: Fields | undefined): Fields | null {
  if (bound === null) {
    return resolved ?? null;
  }

  if (resolved === undefined) {
    return bound;
  }

  return { ...bound, ...resolved };
}

/** The runtime's default logger: info level and above, one readable line
 * per entry on standard error. */
export const defaultLogger: Logger = new TextLogger();
