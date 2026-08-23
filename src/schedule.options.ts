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

import type { PID } from "./pid";

/**
 * Configuration of one schedule registered with `schedule` or
 * `scheduleOnce`, on the actor system or on a receive context.
 */
export interface ScheduleOptions {
  /**
   * The identifier used to cancel, pause, and resume the schedule.
   * References live in one flat, system-wide namespace: a schedule
   * created inside an actor is still addressable by its reference from
   * anywhere. A schedule registered without a reference cannot be
   * addressed individually; it is still cancelled when its owning actor
   * or the system stops.
   */
  reference?: string;

  /**
   * The actor recorded as the sender of every delivered message, which
   * the receiving behavior sees as `ctx.sender`. Defaults to the
   * scheduling actor for a schedule created through a receive context,
   * and to the system's NoSender actor otherwise.
   */
  sender?: PID;
}
