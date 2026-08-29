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

import {
  type EntryLevel,
  type Fields,
  type LazyFields,
  type Level,
  type Logger,
  levelWeight,
} from "./logger";

/** Configuration for a {@link JsonLogger}. */
export interface JsonLoggerOptions {
  /** The minimum level to emit; `info` by default. */
  level?: Level;

  /** The sink entries are written to; `process.stderr` by default. */
  stream?: NodeJS.WritableStream;

  /** Fields every entry carries; none by default. */
  fields?: Fields;
}

/** Serializes errors as data and leaves everything else untouched. */
function replacer(_key: string, value: unknown): unknown {
  if (value instanceof Error) {
    return { name: value.name, message: value.message, stack: value.stack };
  }

  return value;
}

/**
 * JsonLogger writes each entry as one JSON line: `time` (ISO 8601),
 * `level`, and `msg` first, followed by the bound and per-entry fields.
 * A payload that cannot be serialized, a circular object for example,
 * still produces a line carrying the message and an `unserializable`
 * marker instead of throwing into the caller.
 */
export class JsonLogger implements Logger {
  private readonly _level: Level;
  private readonly _weight: number;
  private readonly _stream: NodeJS.WritableStream;
  private readonly _bound: Fields | null;

  constructor(options?: JsonLoggerOptions) {
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
    return new JsonLogger({
      level: this._level,
      stream: this._stream,
      fields: this._bound === null ? { ...fields } : { ...this._bound, ...fields },
    });
  }

  private write(level: EntryLevel, message: string, fields?: LazyFields): void {
    const resolved = typeof fields === "function" ? fields() : fields;
    const entry: Fields = { time: new Date().toISOString(), level, msg: message };

    if (this._bound !== null) {
      Object.assign(entry, this._bound);
    }

    if (resolved !== undefined) {
      Object.assign(entry, resolved);
    }

    let line: string;
    try {
      line = JSON.stringify(entry, replacer);
    } catch {
      line = JSON.stringify({
        time: entry.time,
        level,
        msg: message,
        unserializable: true,
      });
    }

    this._stream.write(`${line}\n`);
  }
}
