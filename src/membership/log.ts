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

/** Fields attached to a membership log entry, ready-made or built on
 * demand so an expensive payload is skipped when the level is disabled.
 *
 * @internal
 */
export type LogFields = Record<string, unknown> | (() => Record<string, unknown>);

/**
 * The subset of the runtime logger the membership protocol emits through.
 * It is declared here, rather than imported, so the protocol stays free of
 * framework dependencies; the runtime logger satisfies it structurally and
 * is handed in already tagged with its component.
 *
 * @internal
 */
export interface Log {
  /** Logs a message at debug level. */
  debug(message: string, fields?: LogFields): void;

  /** Logs a message at info level. */
  info(message: string, fields?: LogFields): void;

  /** Logs a message at warn level. */
  warn(message: string, fields?: LogFields): void;

  /** Logs a message at error level. */
  error(message: string, fields?: LogFields): void;
}

/** A log that drops every entry, the default when none is injected.
 *
 * @internal
 */
export const nopLog: Log = {
  debug(): void {},
  info(): void {},
  warn(): void {},
  error(): void {},
};
