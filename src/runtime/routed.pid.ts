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

import type { Actor } from "../actor/actor";
import type { IsolateRoute } from "../actor/actor.ref";
import type { ActorSystem } from "../actor/actor.system";
import type { Path } from "../actor/path";
import { PID } from "../actor/pid";
import { UnboundedMailbox } from "../actor/unbounded.mailbox";

/** The stand-in actor of every routed handle; it never starts and never
 * receives, because every send routes before touching the local
 * machinery. Not exported: module-private plumbing.
 *
 * @internal
 */
const routedStub: Actor = {
  preStart(): void {},

  receive(): void {},

  postStop(): void {},
};

/** One inert mailbox shared by every routed handle: the route branch
 * fires before any enqueue, so the mailbox exists only to satisfy the
 * PID shape without allocating a ring per handle. Not exported:
 * module-private plumbing.
 *
 * @internal
 */
const routedMailbox = new UnboundedMailbox();

/**
 * Builds the PID-shaped handle of an actor owned by another isolate:
 * a real `PID` carrying the target's path and a route instead of local
 * machinery. Tells, asks, requests, watches, and replies through it
 * are indistinguishable from the local forms at the call site; the
 * route carries them to the isolate that owns the actor. The handle
 * never starts, reports `isRunning()` as false (liveness across
 * isolates is not synchronously knowable; use `watch`), and refuses
 * `shutdown()` loudly.
 *
 * @internal
 */
export function routedPid(system: ActorSystem, path: Path, route: IsolateRoute): PID {
  const pid = new PID(routedStub, path, system, { mailbox: routedMailbox });
  pid.setRoute(route);
  return pid;
}
