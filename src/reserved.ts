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
 * Reserved names of the actors the runtime spawns for itself. User actors
 * must not use names carrying the reserved prefix; the actor system
 * enforces this at spawn time.
 */

/** Prefix that marks an actor name as belonging to the runtime.
 *
 * @internal
 */
export const reservedNamesPrefix = "NodeAkt";

/** Name of the root guardian, the top of the actor tree.
 *
 * @internal
 */
export const rootGuardianName = "NodeAktRootGuardian";

/** Name of the system guardian, the ancestor of runtime-created actors.
 *
 * @internal
 */
export const systemGuardianName = "NodeAktSystemGuardian";

/** Name of the user guardian, the ancestor of user-created actors.
 *
 * @internal
 */
export const userGuardianName = "NodeAktUserGuardian";

/** Name of the NoSender sentinel PID.
 *
 * @internal
 */
export const noSenderName = "NodeAktNoSender";

/** Name of the dead-letter actor, the sink of unhandled messages.
 *
 * @internal
 */
export const deadletterName = "NodeAktDeadletter";

/** Name of the relocation actor, the coordinator's driver for recreating a departed
 * node's actors on the survivors.
 *
 * @internal
 */
export const relocatorName = "NodeAktRelocator";

/**
 * Reports whether the given actor name belongs to the runtime, that is,
 * whether it carries the reserved prefix.
 *
 * @internal
 */
export function isSystemName(name: string): boolean {
  return name.startsWith(reservedNamesPrefix);
}
