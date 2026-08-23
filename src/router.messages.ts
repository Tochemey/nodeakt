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

import { registerMessage } from "./registration";

/**
 * Management messages a router consumes itself instead of forwarding.
 * The router is addressed like any other actor, so these are ordinary
 * message classes: send them with `tell` or `ask` on the router's PID.
 */

/**
 * GetRoutees asks a router for its pool. The router answers with a
 * {@link Routees} message listing the live routees:
 *
 * ```ts
 * const routees = await system.noSender().ask(router, new GetRoutees(), timeout);
 * ```
 */
export class GetRoutees {}

/**
 * Routees is a router's answer to {@link GetRoutees} and to an asked
 * {@link AdjustRouterPoolSize}: the canonical path strings of the live
 * routees at the time the router processed the request.
 */
export class Routees {
  /** @param paths - The canonical path strings of the live routees. */
  constructor(readonly paths: readonly string[]) {}
}

/**
 * AdjustRouterPoolSize grows or shrinks a router's pool in place to the
 * given number of live routees: a grow spawns fresh routees, a shrink
 * stops the newest ones gracefully. Sent with `tell` it is fire and
 * forget; sent with `ask` the router answers with the resulting
 * {@link Routees} once the adjustment is done.
 *
 * A size that is not a non-negative integer is refused: the message is
 * routed to dead letters with the `ErrInvalidPoolSize` sentinel as the
 * reason, and an ask rejects with the same sentinel.
 */
export class AdjustRouterPoolSize {
  /** @param poolSize - The number of live routees to end up with. */
  constructor(readonly poolSize: number) {}
}

// Registered under reserved ids so the management surface works
// unchanged when a router is addressed from another isolate, and user
// classes reusing these names keep their own registrations.
registerMessage(GetRoutees, "nodeakt.GetRoutees");
registerMessage(Routees, "nodeakt.Routees");
registerMessage(AdjustRouterPoolSize, "nodeakt.AdjustRouterPoolSize");
