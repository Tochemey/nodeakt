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

import type { Actor } from "./actor";
import { guardianReceive } from "./guardian.behavior";
import type { ReceiveContext } from "./receive.context";

/**
 * RootGuardian is the actor at the top of the actor tree: every other
 * actor in the system descends from it, with runtime actors under the
 * system guardian and user actors under the user guardian.
 *
 * Its job is to monitor the guardians beneath it. When a runtime actor
 * signals a panic while the system is running, the whole actor system is
 * shut down: a dead runtime actor means the system can no longer honor
 * its guarantees.
 *
 * @internal
 */
export class RootGuardian implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    guardianReceive(ctx);
  }

  postStop(): void {}
}
