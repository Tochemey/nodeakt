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
 * The delivery kinds a transport routes: `tell` is fire and forget,
 * `ask` expects a correlated `reply`, and `reply` settles the pending
 * entry registered under the envelope's correlation id. In-actor
 * requests ride the `ask` kind; the difference between an ask and a
 * request is entirely a sending-side concern. `watch` registers the
 * envelope's sender for a `Terminated` when the target stops, and
 * `unwatch` cancels it; both carry no payload, and the notification
 * itself travels back as an ordinary tell.
 *
 * @internal
 */
export type EnvelopeKind = "tell" | "ask" | "reply" | "watch" | "unwatch";

/**
 * One message payload in wire form: the registered type id and the
 * message data. An empty type id marks a passthrough value that needs
 * no prototype restoration.
 *
 * @internal
 */
export interface WireMessage {
  readonly type: string;
  readonly data: unknown;
}

/**
 * One failure in wire form. A non-negative `sentinel` is the index of
 * one of the runtime's identity-compared sentinel errors, so the
 * receiving side can restore the very same instance; `-1` carries a
 * reconstructed error's name and message instead.
 *
 * @internal
 */
export interface WireError {
  readonly sentinel: number;
  readonly name: string;
  readonly message: string;
}

/**
 * Envelope is the unit a transport moves between isolates: a plain,
 * structured-cloneable object with no class prototypes, no live PIDs,
 * and no functions.
 *
 * `to` and `uid` address the target by canonical path string and
 * incarnation, where an empty uid addresses whatever lives at the path.
 * `sender`, `senderUid`, and `senderWorkerId` identify the sender the
 * same way plus the worker isolate it lives on, which is the full
 * cross-isolate identity of an actor: path, incarnation, worker id.
 * An empty sender path means the send carried none; the worker id then
 * still names the isolate the envelope left. `cid` correlates an ask
 * with its reply and is zero for tells; `timeout` bounds the receiving
 * side's wait in milliseconds, zero for none. Tells, asks, and
 * successful replies carry the message; failed replies carry the
 * error; watches and unwatches carry neither.
 *
 * @internal
 */
export interface Envelope {
  readonly kind: EnvelopeKind;
  readonly to: string;
  readonly uid: string;
  readonly sender: string;
  readonly senderUid: string;
  readonly senderWorkerId: number;
  readonly cid: number;
  readonly timeout: number;
  readonly message: WireMessage | null;
  readonly error: WireError | null;
}
