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
 * An Extension is a service the whole actor system shares: an event store,
 * a metrics recorder, a tracing client, a feature-flag provider. It is
 * installed once, when the system is created, and every actor reaches it
 * from its context by identifier, with no per-actor wiring.
 *
 * The interface is deliberately minimal: one identifier, no base class,
 * no lifecycle hook. The system stores the instance it is handed and
 * returns that same instance to every lookup, so an extension that opens
 * a connection or flushes a buffer does so through its own API, on the
 * schedule its owner chooses.
 *
 * ```ts
 * class EventStore implements Extension {
 *   id(): string {
 *     return "eventStore";
 *   }
 *
 *   async append(streamId: string, event: unknown): Promise<void> {
 *     // ...
 *   }
 * }
 *
 * const system = new ActorSystem("orders", { extensions: [new EventStore()] });
 * const store = system.extension<EventStore>("eventStore");
 * ```
 *
 * Extensions are per-process object instances: they are never serialized,
 * so an actor running on a worker isolate does not see an instance
 * installed on the isolate that created the system. Reach for one to
 * share a service the local process owns; a dependency that belongs to a
 * single actor, and must survive its restart or its relocation, travels
 * as a `Props` argument instead.
 */
export interface Extension {
  /**
   * The identifier the extension is registered and looked up under, such
   * as `"eventStore"`. It starts with an alphanumeric character, contains
   * only alphanumerics, `-` or `_`, and is 2 to 255 characters long. No
   * two extensions on one system may share an identifier.
   *
   * The system reads it once, while it is being created, so report a
   * constant rather than a value derived from mutable state.
   */
  id(): string;
}
