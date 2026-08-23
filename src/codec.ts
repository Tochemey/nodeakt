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

import type { WireError, WireMessage } from "./envelope";
import {
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrDead,
  ErrFanOutAsk,
  ErrInvalidActorName,
  ErrInvalidActorSystemName,
  ErrInvalidPoolSize,
  ErrInvalidReentrancyMode,
  ErrInvalidTimeout,
  ErrMailboxDisposed,
  ErrMailboxFull,
  ErrNameRequired,
  ErrReentrancyDisabled,
  ErrReentrancyInFlightLimit,
  ErrRequestCanceled,
  ErrRequestTimeout,
  ErrReservedName,
  ErrStashBufferEmpty,
  ErrUndefinedActor,
  ErrUnhandled,
  TypeNotRegisteredError,
} from "./errors";
import type { MessageClass, MessageRegistry } from "./message.registry";

/**
 * The runtime's identity-compared sentinel errors in wire order. The
 * position of each sentinel is its wire index, so the list is
 * append-only: reordering or removing entries changes the meaning of
 * envelopes already in flight.
 *
 * @internal
 */
const sentinelErrors: readonly Error[] = [
  ErrMailboxFull,
  ErrMailboxDisposed,
  ErrDead,
  ErrInvalidTimeout,
  ErrRequestTimeout,
  ErrReentrancyDisabled,
  ErrInvalidReentrancyMode,
  ErrReentrancyInFlightLimit,
  ErrRequestCanceled,
  ErrUnhandled,
  ErrStashBufferEmpty,
  ErrReservedName,
  ErrActorSystemNotStarted,
  ErrNameRequired,
  ErrInvalidActorSystemName,
  ErrInvalidActorName,
  ErrActorAlreadyExists,
  ErrUndefinedActor,
  ErrFanOutAsk,
  ErrInvalidPoolSize,
];

/**
 * Encodes a failure into wire form: a sentinel travels as its index so
 * the receiving side restores the identical instance, any other error
 * travels as its name and message.
 *
 * @internal
 */
export function encodeError(error: Error): WireError {
  const index = sentinelErrors.indexOf(error);
  if (index >= 0) {
    return { sentinel: index, name: "", message: "" };
  }

  return { sentinel: -1, name: error.name, message: error.message };
}

/**
 * Restores a failure from wire form. A known sentinel index yields the
 * identity-compared sentinel itself; anything else yields a
 * reconstructed error carrying the wire name and message, without the
 * original stack or prototype.
 *
 * @internal
 */
export function decodeError(wire: WireError): Error {
  const sentinel = wire.sentinel >= 0 ? sentinelErrors[wire.sentinel] : undefined;
  if (sentinel !== undefined) {
    return sentinel;
  }

  const error = new Error(wire.message);
  error.name = wire.name;
  return error;
}

/** Names the class of a value for the failure message of an encode
 * that found no registration. */
function classNameOf(value: object): string {
  const name = (value as { constructor?: { name?: string } }).constructor?.name;
  return name === undefined || name === "" ? "unknown" : name;
}

/**
 * Codec is the v0 message serializer of the transport layer: it stamps
 * outgoing messages with their registered type id and restores incoming
 * ones to their class prototype, on top of structured clone doing the
 * actual data copy.
 *
 * The v0 contract: a message is either passthrough data (a primitive,
 * or an object whose prototype is `Object.prototype`, `null`, or
 * `Array.prototype`) or an instance of a registered class. Decoding
 * restores the prototype of the top-level message only, without running
 * its constructor; own enumerable properties survive, private fields
 * and nested class instances do not. Anything else refuses to encode,
 * so a message that cannot survive the boundary fails on the sending
 * side instead of arriving broken.
 *
 * @internal
 */
export class Codec {
  private readonly _registry: MessageRegistry;

  constructor(registry: MessageRegistry) {
    this._registry = registry;
  }

  /**
   * Encodes one message into wire form.
   *
   * @throws A `TypeError` for a function or symbol, which no boundary
   * can carry.
   * @throws A {@link TypeNotRegisteredError} for an instance of an
   * unregistered class.
   */
  encodeMessage(value: unknown): WireMessage {
    if (value === null || typeof value !== "object") {
      if (typeof value === "function" || typeof value === "symbol") {
        throw new TypeError(`a ${typeof value} cannot cross an isolate boundary`);
      }

      return { type: "", data: value };
    }

    const proto = Object.getPrototypeOf(value);
    if (proto === Object.prototype || proto === null || proto === Array.prototype) {
      return { type: "", data: value };
    }

    const ctor = (value as { constructor?: MessageClass }).constructor;
    const id = ctor === undefined ? undefined : this._registry.idOf(ctor);
    if (id === undefined) {
      throw new TypeNotRegisteredError(classNameOf(value));
    }

    return { type: id, data: value };
  }

  /**
   * Restores one message from wire form: passthrough data comes back as
   * is, a typed message comes back as an instance of its registered
   * class with the wire data as its own properties.
   *
   * Restoration copies the wire data as own data properties, never
   * through assignment: prototype setters do not run, and a `__proto__`
   * or `constructor` key in the wire data is dropped instead of
   * swapping the instance's prototype or shadowing its class.
   *
   * @throws A {@link TypeNotRegisteredError} when the type id has no
   * registration on this side.
   */
  decodeMessage(wire: WireMessage): unknown {
    if (wire.type === "") {
      return wire.data;
    }

    const type = this._registry.classOf(wire.type);
    if (type === undefined) {
      throw new TypeNotRegisteredError(wire.type);
    }

    const instance = Object.create(type.prototype) as object;
    const data = wire.data;
    if (data === null || typeof data !== "object") {
      return instance;
    }

    for (const key of Object.keys(data)) {
      if (key === "__proto__" || key === "constructor") {
        continue;
      }

      Object.defineProperty(instance, key, {
        value: (data as Record<string, unknown>)[key],
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }

    return instance;
  }
}
