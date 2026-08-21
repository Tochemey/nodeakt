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

/**
 * Level orders log entries by severity. A logger emits entries at its
 * configured level and above; `off` silences it entirely.
 */
export type Level = "debug" | "info" | "warn" | "error" | "off";

/** An emittable level: every level a log entry can carry. */
export type EntryLevel = Exclude<Level, "off">;

/**
 * Fields is the structured payload attached to a log entry: plain keys
 * to values, serialized alongside the message. `Error` values are
 * serialized as their name, message, and stack.
 */
export type Fields = Record<string, unknown>;

/**
 * Fields for one entry, either ready-made or computed on demand: pass a
 * function when building the payload is expensive, and it only runs if
 * the entry's level is enabled.
 */
export type LazyFields = Fields | (() => Fields);

/** The numeric weight of each level; entries at or above a logger's
 * configured weight are emitted. */
export const levelWeight: Record<Level, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
  off: 100,
};

/**
 * Logger is the structured logging interface of the runtime. Entries
 * carry a severity level, a human message, and optional structured
 * fields; implementations decide the sink and the encoding.
 *
 * Loggers are cheap to consult when disabled: a call below the
 * configured level returns without building anything, and lazy fields
 * are never computed for it.
 */
export interface Logger {
  /** Logs a message at debug level. */
  debug(message: string, fields?: LazyFields): void;

  /** Logs a message at info level. */
  info(message: string, fields?: LazyFields): void;

  /** Logs a message at warn level. */
  warn(message: string, fields?: LazyFields): void;

  /** Logs a message at error level. */
  error(message: string, fields?: LazyFields): void;

  /** Returns the minimum level this logger emits. */
  level(): Level;

  /** Reports whether entries at the given level are emitted. */
  enabled(level: EntryLevel): boolean;

  /**
   * Returns a logger whose every entry also carries the given fields.
   * Use it to bind stable context once, an actor path for example,
   * instead of repeating it per call.
   */
  with(fields: Fields): Logger;
}
