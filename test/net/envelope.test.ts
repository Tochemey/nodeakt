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

import { describe, expect, it } from "vitest";
import { decodeError, encodeError } from "../../src/codec";
import type { Envelope, WireError } from "../../src/envelope";
import { ErrDead } from "../../src/errors";
import {
  type DataEnvelope,
  decodeDataEnvelope,
  decodeErrorBody,
  decodeHello,
  decodeReplyEnvelope,
  ERROR_APPLICATION,
  type ErrorBody,
  encodeDataEnvelope,
  encodeErrorBody,
  encodeHello,
  encodeReplyEnvelope,
  type Hello,
  KIND_ASK,
  KIND_TELL,
  KIND_UNWATCH,
  KIND_WATCH,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
  SERIALIZER_CUSTOM,
} from "../../src/net/envelope";
import {
  decodeFrameHeader,
  encodeFrameHeader,
  FLAG_EXPECTS_REPLY,
  FRAME_DATA,
  type FrameHeader,
  LANE_CONTROL,
  ProtocolError,
} from "../../src/net/frame";
import {
  ByteReader,
  ByteWriter,
  decodeValue,
  encodeValue,
  ValueDecodeError,
} from "../../src/net/values";

const EMPTY: Uint8Array = new Uint8Array(0);

function bytesOf(encode: (writer: ByteWriter) => void): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encode(writer);
  return Uint8Array.from(writer.bytes());
}

function dataRoundTrip(envelope: DataEnvelope): DataEnvelope {
  return decodeDataEnvelope(
    new ByteReader(bytesOf((writer: ByteWriter): void => encodeDataEnvelope(writer, envelope))),
  );
}

describe("DATA envelope codec", () => {
  it("encodes the documented byte layout", () => {
    const envelope: DataEnvelope = {
      kind: KIND_TELL,
      to: "a",
      uid: "",
      sender: "",
      senderUid: "",
      timeout: 0,
      serializerId: SERIALIZER_BINARY,
      typeRef: "",
      payload: Uint8Array.from([0x2a]),
    };
    const bytes: Uint8Array = bytesOf((writer: ByteWriter): void =>
      encodeDataEnvelope(writer, envelope),
    );
    expect(Array.from(bytes)).toEqual([0, 0, 1, 0x61, 0, 0, 0, 0, 0, 0, 0, 0, 0x2a]);
  });

  it("round-trips a tell and an ask", () => {
    const tell: DataEnvelope = {
      kind: KIND_TELL,
      to: "nodeakt://orders@10.0.0.5:5100/user/charger",
      uid: "b3f2",
      sender: "nodeakt://orders@10.0.0.9:5100/user/gateway",
      senderUid: "77aa",
      timeout: 0,
      serializerId: SERIALIZER_BINARY,
      typeRef: "app.Charge",
      payload: Uint8Array.from([1, 2, 3, 4, 5]),
    };
    expect(dataRoundTrip(tell)).toEqual(tell);

    const ask: DataEnvelope = { ...tell, kind: KIND_ASK, timeout: 5000 };
    expect(dataRoundTrip(ask)).toEqual(ask);
  });

  it("round-trips a watch and an unwatch, which carry no message", () => {
    for (const kind of [KIND_WATCH, KIND_UNWATCH]) {
      const watch: DataEnvelope = {
        kind,
        to: "nodeakt://orders@10.0.0.5:5100/user/charger",
        uid: "b3f2",
        sender: "nodeakt://orders@10.0.0.9:5100/user/watcher",
        senderUid: "9c",
        timeout: 0,
        serializerId: SERIALIZER_BINARY,
        typeRef: "",
        payload: EMPTY,
      };
      expect(dataRoundTrip(watch)).toEqual(watch);
    }
  });

  it("refuses layout violations on encode as programming errors", () => {
    const valid: DataEnvelope = {
      kind: KIND_TELL,
      to: "a",
      uid: "",
      sender: "",
      senderUid: "",
      timeout: 0,
      serializerId: SERIALIZER_BINARY,
      typeRef: "",
      payload: EMPTY,
    };
    const writer: ByteWriter = new ByteWriter();
    expect(() => encodeDataEnvelope(writer, { ...valid, kind: 4 })).toThrow(TypeError);
    expect(() => encodeDataEnvelope(writer, { ...valid, serializerId: 7 })).toThrow(TypeError);
    expect(() =>
      encodeDataEnvelope(writer, { ...valid, kind: KIND_WATCH, payload: Uint8Array.from([1]) }),
    ).toThrow(TypeError);
  });

  it("rejects malformed bodies as request-scoped decode errors", () => {
    expect(() => decodeDataEnvelope(new ByteReader(Uint8Array.from([9])))).toThrow(
      ValueDecodeError,
    );

    const badSerializer: Uint8Array = Uint8Array.from([0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0]);
    expect(() => decodeDataEnvelope(new ByteReader(badSerializer))).toThrow(ValueDecodeError);

    const watchWithPayload: Uint8Array = Uint8Array.from([2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x2a]);
    expect(() => decodeDataEnvelope(new ByteReader(watchWithPayload))).toThrow(ValueDecodeError);
  });

  it("rejects a table ref as connection-scoped while tables are not negotiated", () => {
    const tableRef: Uint8Array = Uint8Array.from([0, 5]);
    expect(() => decodeDataEnvelope(new ByteReader(tableRef))).toThrow(ProtocolError);
  });
});

describe("REPLY envelope codec", () => {
  it("round-trips typed and passthrough replies", () => {
    const typed: ReplyEnvelope = {
      serializerId: SERIALIZER_BINARY,
      typeRef: "app.Receipt",
      payload: Uint8Array.from([9, 9, 9]),
    };
    const decodedTyped: ReplyEnvelope = decodeReplyEnvelope(
      new ByteReader(bytesOf((writer: ByteWriter): void => encodeReplyEnvelope(writer, typed))),
    );
    expect(decodedTyped).toEqual(typed);

    const passthrough: ReplyEnvelope = {
      serializerId: SERIALIZER_CUSTOM,
      typeRef: "",
      payload: EMPTY,
    };
    const decodedPassthrough: ReplyEnvelope = decodeReplyEnvelope(
      new ByteReader(
        bytesOf((writer: ByteWriter): void => encodeReplyEnvelope(writer, passthrough)),
      ),
    );
    expect(decodedPassthrough).toEqual(passthrough);
  });

  it("validates the serializer on both paths", () => {
    const writer: ByteWriter = new ByteWriter();
    expect(() =>
      encodeReplyEnvelope(writer, { serializerId: 3, typeRef: "", payload: EMPTY }),
    ).toThrow(TypeError);
    expect(() => decodeReplyEnvelope(new ByteReader(Uint8Array.from([3, 0, 0])))).toThrow(
      ValueDecodeError,
    );
  });
});

describe("ERROR body codec", () => {
  it("round-trips reconstructed and sentinel forms", () => {
    const reconstructed: ErrorBody = {
      code: ERROR_APPLICATION,
      sentinel: 0,
      name: "PaymentDeclined",
      message: "card expired",
    };
    const decodedReconstructed: ErrorBody = decodeErrorBody(
      new ByteReader(bytesOf((writer: ByteWriter): void => encodeErrorBody(writer, reconstructed))),
    );
    expect(decodedReconstructed).toEqual(reconstructed);

    const sentinel: ErrorBody = { code: ERROR_APPLICATION, sentinel: 3, name: "", message: "" };
    const decodedSentinel: ErrorBody = decodeErrorBody(
      new ByteReader(bytesOf((writer: ByteWriter): void => encodeErrorBody(writer, sentinel))),
    );
    expect(decodedSentinel).toEqual(sentinel);
  });

  it("validates the code on both paths", () => {
    const writer: ByteWriter = new ByteWriter();
    expect(() => encodeErrorBody(writer, { code: 0, sentinel: 0, name: "", message: "" })).toThrow(
      TypeError,
    );
    expect(() => encodeErrorBody(writer, { code: 6, sentinel: 0, name: "", message: "" })).toThrow(
      TypeError,
    );
    expect(() => decodeErrorBody(new ByteReader(Uint8Array.from([0, 0, 0, 0])))).toThrow(
      ValueDecodeError,
    );
  });
});

describe("HELLO codec", () => {
  const hello: Hello = {
    revision: 4,
    systemName: "orders",
    host: "10.0.0.5",
    port: 5100,
    lane: LANE_CONTROL,
    compression: 0,
    maxFrameSize: 16 * 1024 * 1024,
    maxMessageSize: 16 * 1024 * 1024,
    initialCredits: 16 * 1024 * 1024,
    maxLargeTransfers: 4,
  };

  it("round-trips the full parameter set", () => {
    const decoded: Hello = decodeHello(
      new ByteReader(bytesOf((writer: ByteWriter): void => encodeHello(writer, hello))),
    );
    expect(decoded).toEqual(hello);
  });

  it("ignores trailing bytes so a newer peer can append fields", () => {
    const writer: ByteWriter = new ByteWriter();
    encodeHello(writer, hello);
    writer.writeUvarint(999);
    writer.writeString("a future field");
    expect(decodeHello(new ByteReader(writer.bytes()))).toEqual(hello);
  });

  it("fails cleanly on truncation", () => {
    const bytes: Uint8Array = bytesOf((writer: ByteWriter): void => encodeHello(writer, hello));
    expect(() => decodeHello(new ByteReader(bytes.subarray(0, 5)))).toThrow(ValueDecodeError);
  });
});

describe("the seam shape", () => {
  it("maps an in-process ask envelope onto the wire and back", () => {
    const inProcess: Envelope = {
      kind: "ask",
      to: "nodeakt://orders@10.0.0.5:5100/user/charger",
      uid: "b3f2",
      sender: "nodeakt://orders@10.0.0.9:5100/user/gateway",
      senderUid: "77aa",
      senderWorkerId: 3,
      cid: 42,
      timeout: 5000,
      message: { type: "app.Charge", data: { orderId: "o-1", amount: 25.5 } },
      error: null,
    };

    const writer: ByteWriter = new ByteWriter();
    encodeValue(writer, inProcess.message?.data);
    const wire: DataEnvelope = {
      kind: KIND_ASK,
      to: inProcess.to,
      uid: inProcess.uid,
      sender: inProcess.sender,
      senderUid: inProcess.senderUid,
      timeout: inProcess.timeout,
      serializerId: SERIALIZER_BINARY,
      typeRef: inProcess.message?.type ?? "",
      payload: Uint8Array.from(writer.bytes()),
    };
    const header: FrameHeader = {
      type: FRAME_DATA,
      flags: FLAG_EXPECTS_REPLY,
      lane: LANE_CONTROL,
      length: 0,
      correlation: inProcess.cid,
    };

    const headerBytes: ByteWriter = new ByteWriter();
    encodeFrameHeader(headerBytes, header);
    expect(decodeFrameHeader(headerBytes.bytes(), 0).correlation).toBe(inProcess.cid);

    const decoded: DataEnvelope = dataRoundTrip(wire);
    expect(decoded.to).toBe(inProcess.to);
    expect(decoded.uid).toBe(inProcess.uid);
    expect(decoded.sender).toBe(inProcess.sender);
    expect(decoded.senderUid).toBe(inProcess.senderUid);
    expect(decoded.timeout).toBe(inProcess.timeout);
    expect(decoded.typeRef).toBe(inProcess.message?.type);
    expect(decodeValue(new ByteReader(decoded.payload))).toEqual(inProcess.message?.data);
  });

  it("carries a sentinel error across the wire as the identical instance", () => {
    const wireError: WireError = encodeError(ErrDead);
    expect(wireError.sentinel).toBeGreaterThanOrEqual(0);

    const body: ErrorBody = {
      code: ERROR_APPLICATION,
      sentinel: wireError.sentinel + 1,
      name: wireError.name,
      message: wireError.message,
    };
    const decodedBody: ErrorBody = decodeErrorBody(
      new ByteReader(bytesOf((writer: ByteWriter): void => encodeErrorBody(writer, body))),
    );
    const restored: Error = decodeError({
      sentinel: decodedBody.sentinel - 1,
      name: decodedBody.name,
      message: decodedBody.message,
    });
    expect(restored).toBe(ErrDead);
  });

  it("carries an ordinary error by name and message", () => {
    const original: Error = new Error("card expired");
    original.name = "PaymentDeclined";
    const wireError: WireError = encodeError(original);
    expect(wireError.sentinel).toBe(-1);

    const body: ErrorBody = {
      code: ERROR_APPLICATION,
      sentinel: wireError.sentinel + 1,
      name: wireError.name,
      message: wireError.message,
    };
    const decodedBody: ErrorBody = decodeErrorBody(
      new ByteReader(bytesOf((writer: ByteWriter): void => encodeErrorBody(writer, body))),
    );
    const restored: Error = decodeError({
      sentinel: decodedBody.sentinel - 1,
      name: decodedBody.name,
      message: decodedBody.message,
    });
    expect(restored).not.toBe(original);
    expect(restored.name).toBe("PaymentDeclined");
    expect(restored.message).toBe("card expired");
  });
});
