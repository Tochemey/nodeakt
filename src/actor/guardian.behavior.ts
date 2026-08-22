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

import { PanicSignal } from "./messages";
import type { ReceiveContext } from "./receive.context";
import { isSystemName } from "./reserved";

/**
 * The receive behavior shared by the root and system guardians.
 *
 * A panic signaled by a runtime actor stops the whole actor system while
 * it is running: a dead runtime actor means the system can no longer
 * honor its guarantees. Every other message (PostStart, Terminated, and
 * strays) is acknowledged without action until logging and death-watch
 * handling arrive.
 *
 * @internal
 */
export function guardianReceive(ctx: ReceiveContext): void {
  const msg = ctx.message;

  if (!(msg instanceof PanicSignal)) {
    return;
  }

  // Without self there is no actor system to stop; without a sender the
  // origin of the panic is unknown.
  const system = ctx.self?.actorSystem();
  if (system === undefined || ctx.sender === undefined) {
    return;
  }

  if (system.isRunning() && isSystemName(ctx.sender.name())) {
    system.stop();
  }
}
