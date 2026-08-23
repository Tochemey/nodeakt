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

import { Terminated } from "./messages";

/**
 * The shape of a registrable message class: any concrete or abstract
 * class whose instances carry the message data. The registry never
 * constructs through it; decoding restores instances from the class
 * prototype.
 *
 * @internal
 */
export type MessageClass = (abstract new (
  ...args: never[]
) => object) & {
  readonly name: string;
  readonly prototype: object;
};

/**
 * MessageRegistry maps message classes to stable type ids and back.
 *
 * Crossing an isolate boundary strips class prototypes: a message
 * arrives as a plain object and `instanceof` narrowing dies with it.
 * The registry is what keeps it alive: the sending side stamps each
 * message with the id registered for its class, and the receiving side
 * resolves the id back to the class and restores the prototype. Every
 * isolate that sends or receives a message class must register it,
 * under the same id.
 *
 * Runtime plumbing for the transport layer.
 *
 * @internal
 */
export class MessageRegistry {
  private readonly _byId = new Map<string, MessageClass>();
  private readonly _byClass = new Map<MessageClass, string>();

  constructor() {
    // Runtime messages that cross isolates are pre-registered under the
    // reserved "nodeakt." prefix, so every registry can carry them
    // without any application setup.
    this.register(Terminated, "nodeakt.Terminated");
  }

  /**
   * Registers a message class under the given type id, or under the
   * class name when the id is omitted. Registering the same class under
   * the same id again is a no-op, so modules can register their
   * messages unconditionally on import.
   *
   * @throws A `TypeError` when the id is empty, already bound to a
   * different class, or the class is already registered under a
   * different id.
   */
  register(type: MessageClass, id?: string): void {
    const key = id ?? type.name;
    if (key === "") {
      throw new TypeError("message type id must not be empty");
    }

    const boundClass = this._byId.get(key);
    const boundId = this._byClass.get(type);
    if (boundClass === type && boundId === key) {
      return;
    }

    if (boundClass !== undefined) {
      throw new TypeError(`message type id "${key}" is already registered`);
    }

    if (boundId !== undefined) {
      throw new TypeError(`message type "${type.name}" is already registered as "${boundId}"`);
    }

    this._byId.set(key, type);
    this._byClass.set(type, key);
  }

  /** Returns the type id registered for the class, or undefined. */
  idOf(type: MessageClass): string | undefined {
    return this._byClass.get(type);
  }

  /** Returns the class registered under the type id, or undefined. */
  classOf(id: string): MessageClass | undefined {
    return this._byId.get(id);
  }
}
