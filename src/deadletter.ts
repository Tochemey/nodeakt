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
import type { EventStream } from "./eventstream";
import type { Deadletter } from "./messages";
import type { ReceiveContext } from "./receive.context";

/** The topic the actor system publishes its runtime events to; dead
 * letters are the first event kind that flows through it.
 *
 * @internal
 */
export const eventsTopic = "events";

/**
 * Carries one dead letter to the dead-letter actor. Internal to the
 * runtime; the send paths construct it, nothing else sees it.
 *
 * @internal
 */
export class SendDeadletter {
  constructor(readonly deadletter: Deadletter) {}
}

/**
 * Asks the dead-letter actor for a count: one receiver's when a
 * canonical path is given, the system-wide total otherwise.
 *
 * @internal
 */
export class DeadlettersCountRequest {
  constructor(readonly receiver?: string) {}
}

/**
 * The dead-letter actor: the runtime-spawned sink of every unhandled
 * message. Dead letters reach it as ordinary messages through its own
 * mailbox, so counting needs no synchronization and observing the sink
 * is the same ask machinery every actor uses. It tallies per-receiver
 * and total counts and publishes each {@link Deadletter} event to the
 * system's event stream.
 *
 * @internal
 */
export class DeadletterActor implements Actor {
  private readonly _events: EventStream;
  private readonly _counts = new Map<string, number>();
  private _total = 0;

  constructor(events: EventStream) {
    this._events = events;
  }

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const msg = ctx.message;

    if (msg instanceof SendDeadletter) {
      this.record(msg.deadletter);
      return;
    }

    if (msg instanceof DeadlettersCountRequest) {
      ctx.response(this.count(msg.receiver));
      return;
    }

    // Anything else is noise addressed to a runtime actor; ignore it.
  }

  postStop(): void {}

  private record(deadletter: Deadletter): void {
    this._total++;
    this._counts.set(deadletter.receiver, (this._counts.get(deadletter.receiver) ?? 0) + 1);
    this._events.publish(eventsTopic, deadletter);
  }

  private count(receiver?: string): number {
    if (receiver === undefined) {
      return this._total;
    }

    return this._counts.get(receiver) ?? 0;
  }
}
