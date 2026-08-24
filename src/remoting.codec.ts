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

import { type Codec, decodeError, encodeError } from "./codec";
import type { WireError, WireMessage } from "./envelope";
import { ERROR_APPLICATION, type ErrorBody } from "./net/envelope";
import { ByteReader, type ByteWriter, decodeValue, encodeValue } from "./net/values";

/**
 * The bridge between runtime messages and wire payloads: part of the
 * remoting seam, so it shares the seam's exemption from the transport
 * boundary. Encoding keeps the exact contract of the in-process codec
 * (registration checked on the sending side, prototypes restored on
 * arrival), with the binary value codec carrying the data instead of
 * structured clone.
 *
 * @internal
 */

/**
 * A runtime message in wire form: the registered type id (empty for
 * passthrough data) and the value-codec encoding of its data.
 *
 * @internal
 */
export interface WirePayload {
  readonly typeRef: string;
  readonly payload: Uint8Array;
}

/**
 * Encodes one message for the wire, with the exact contract of the
 * in-process codec: the registry check refuses an unregistered class
 * instance on the sending side, and the payload is the binary value
 * encoding of the message's data. The writer is a retained scratch
 * buffer; the returned payload is a copy the caller owns.
 *
 * @throws A `TypeError` for a value no boundary can carry.
 * @throws A `TypeNotRegisteredError` for an unregistered class instance.
 *
 * @internal
 */
export function encodePayload(codec: Codec, writer: ByteWriter, value: unknown): WirePayload {
  const wire: WireMessage = codec.encodeMessage(value);
  writer.reset();
  encodeValue(writer, wire.data);
  return { typeRef: wire.type, payload: Uint8Array.from(writer.bytes()) };
}

/**
 * Restores one message from its wire form: the payload decodes to a
 * plain value tree, and a nonempty type ref restores the registered
 * class prototype on top, without running its constructor and dropping
 * `__proto__` and `constructor` keys.
 *
 * @throws A `ValueDecodeError` for a payload that does not decode.
 * @throws A `TypeNotRegisteredError` for a type id with no registration
 * on this side.
 *
 * @internal
 */
export function decodePayload(codec: Codec, typeRef: string, payload: Uint8Array): unknown {
  const data: unknown = decodeValue(new ByteReader(payload));
  return codec.decodeMessage({ type: typeRef, data });
}

/**
 * Encodes a failure settling an ask into the wire's error body: a
 * sentinel error travels as one plus its index so the receiving side
 * restores the identical instance, any other error travels as its name
 * and message.
 *
 * @internal
 */
export function encodeFailure(error: Error): ErrorBody {
  const wire: WireError = encodeError(error);
  return {
    code: ERROR_APPLICATION,
    sentinel: wire.sentinel + 1,
    name: wire.name,
    message: wire.message,
  };
}

/**
 * Restores a failure from the wire's sentinel convention: a nonzero
 * sentinel is one plus the index of an identity-compared sentinel
 * error, zero carries a reconstructed error under its wire name and
 * message.
 *
 * @internal
 */
export function decodeFailure(sentinel: number, name: string, message: string): Error {
  return decodeError({ sentinel: sentinel - 1, name, message });
}
