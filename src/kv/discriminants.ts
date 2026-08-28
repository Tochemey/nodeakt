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
 * Named values for the write-operation discriminants, so the engine's dispatch
 * and the codec's tables key on one shared name rather than a bare string
 * literal each. The strings themselves are the in-memory discriminants of the
 * {@link WriteOp} union, not the wire encoding; the codec maps them to bytes.
 *
 * These live apart from `ports.ts`, which stays a types-only contract that
 * erases to nothing at runtime.
 *
 * @internal
 */

import type { ConditionFailure, PutOp, WriteOp } from "./ports";

/**
 * The four {@link WriteOp} discriminants as named values. `satisfies` validates
 * each against the union while `as const` keeps the literal type for narrowing.
 *
 * @internal
 */
export const WriteKind = {
  put: "put",
  delete: "delete",
  increment: "incr",
  compareAndSet: "cas",
} as const satisfies Record<string, WriteOp["kind"]>;

/**
 * Put presence conditions, following the Redis `NX` and `XX` convention:
 * `ifAbsent` writes only when no live key exists, `ifPresent` only when one does.
 *
 * @internal
 */
export const PutCondition = {
  none: "none",
  ifAbsent: "nx",
  ifPresent: "xx",
} as const satisfies Record<string, PutOp["condition"]>;

/**
 * Reasons a conditional write declined to mutate, named for the check that
 * failed rather than for its two-letter code.
 *
 * @internal
 */
export const RejectionReason = {
  ifAbsent: "nx",
  ifPresent: "xx",
  compareAndSet: "cas",
} as const satisfies Record<string, ConditionFailure>;
