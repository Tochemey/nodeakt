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

import type { Socket } from "node:net";
import {
  decodeFrameHeader,
  encodeFrameHeader,
  FRAME_HEADER_SIZE,
  type FrameHeader,
  ProtocolError,
  validateFrameHeader,
} from "./frame";
import { HeadQueue } from "./queue";
import { defaultTimers, type TimerHandle, type Timers } from "./timers";
import { ByteWriter } from "./values";

/**
 * FramedConn turns one socket into frames in and frames out, and is
 * the only transport module that touches socket events.
 *
 * The read side is a resumable parser: header bytes accumulate to 16,
 * the header is validated before anything else looks at the frame,
 * then the body accumulates to its declared length and the frame is
 * handed to the owner. A body that lies within one socket chunk is a
 * view into it, the common case; a body spanning chunks is copied
 * exactly once into an exact-size buffer.
 *
 * The write side coalesces: accepted frames queue, and a flush
 * scheduled at microtask boundary drains them in batches of one
 * socket write each. Small bodies are copied into the batch; a body
 * above the copy threshold is written as its own buffer so the
 * runtime's vectored write path takes it without copying. When the
 * socket reports backpressure the flush parks until `drain`, and a
 * write that makes no progress within the write timeout destroys the
 * connection, so a peer that stops reading cannot wedge it.
 *
 * @internal
 */

/** Close reason when the write timeout expires without progress. */
export const ErrWriteTimeout: Error = new Error("tcp write timed out");

/** Returned by {@link FramedConn.send} once the connection is closed. */
export const ErrConnClosed: Error = new Error("tcp connection is closed");

/** Bodies at or below this size are copied into the write batch. */
const COPY_THRESHOLD: number = 4096;

/** Per-batch caps: frames per socket write, and coalesced bytes. */
const BATCH_FRAMES: number = 128;
const BATCH_BYTES: number = 64 * 1024;

const EMPTY: Uint8Array = new Uint8Array(0);

/** One frame accepted for sending; the length is derived from the body. */
export interface OutboundFrame {
  readonly type: number;
  readonly flags: number;
  readonly lane: number;
  readonly correlation: number;
  readonly body: Uint8Array | null;
  /**
   * Opaque confirmation token. When present, the owner's `onWrote`
   * handler receives it once the kernel has accepted the frame's
   * bytes, the transport's delivery-confirmation point for
   * redelivery: a frame confirmed there is never resent. A token
   * beats a callback here because confirmations are per message on
   * the hot path, and one shared handler avoids allocating a closure
   * for each. Never confirmed for a frame dropped by close.
   */
  readonly wrote?: unknown;
}

/**
 * One accepted frame in the send queue: the frame's header fields
 * flattened beside its body and confirmation callback, so accepting a
 * frame allocates exactly one object. Structurally a
 * {@link FrameHeader}, which is what lets validation and encoding
 * take the entry directly.
 */
interface QueuedFrame {
  readonly type: number;
  readonly flags: number;
  readonly lane: number;
  readonly length: number;
  readonly correlation: number;
  readonly body: Uint8Array | null;
  /** The frame's confirmation token; null when it carries none. */
  readonly wrote: unknown;
}

/**
 * The owner's callbacks. `onFrame` bodies may alias a receive buffer;
 * an owner that retains one past the callback should copy it.
 * `onViolation` reports a protocol violation on the inbound stream:
 * parsing has stopped, and the owner is expected to answer with a
 * connection-scoped ERROR and close. `onClose` fires exactly once,
 * with null for a clean end.
 */
export interface FramedConnHandlers {
  onFrame(header: FrameHeader, body: Uint8Array): void;
  onViolation(error: ProtocolError): void;
  onClose(error: Error | null): void;
  /**
   * A frame carrying a confirmation token had its bytes accepted by
   * the kernel; see {@link OutboundFrame.wrote}. Must not throw.
   */
  onWrote?(token: unknown): void;
}

export interface FramedConnOptions {
  /** Inbound length bound; zero applies the protocol floor. */
  readonly maxFrameSize?: number;
  /** Milliseconds without write progress before teardown; zero disables. */
  readonly writeTimeoutMs?: number;
  /**
   * Frames coalesced per socket write, default {@link BATCH_FRAMES}.
   * The default is bench-chosen; the knob exists so the dispatch
   * batch cost stays measurable per runtime.
   */
  readonly batchFrames?: number;
  readonly timers?: Timers;
}

export class FramedConn {
  private readonly _socket: Socket;
  private readonly _handlers: FramedConnHandlers;
  private readonly _writeTimeoutMs: number;
  private readonly _batchFrames: number;
  private readonly _timers: Timers;
  private _maxFrameSize: number;

  private readonly _headerBytes: Uint8Array = new Uint8Array(FRAME_HEADER_SIZE);
  private _headerFill: number = 0;
  private _header: FrameHeader | null = null;
  private _bodyBytes: Uint8Array | null = null;
  private _bodyFill: number = 0;
  private _broken: boolean = false;

  private readonly _queue: HeadQueue<QueuedFrame> = new HeadQueue<QueuedFrame>();
  /**
   * The one hot batch writer, retained for the connection's life; the
   * exact-size copy handed to the socket per batch measured faster
   * than rotating retained buffers through the write callbacks.
   */
  private readonly _batch: ByteWriter = new ByteWriter(BATCH_BYTES);
  private _queuedBytes: number = 0;
  private _inFlightBytes: number = 0;
  private _flushScheduled: boolean = false;
  private _blocked: boolean = false;
  private _ending: boolean = false;
  private _writeTimer: TimerHandle | null = null;
  /** Transport-clock stamp of the last kernel-accepted write. */
  private _lastWriteProgress: number = 0;

  private _closed: boolean = false;
  private _closeError: Error | null = null;
  private _closeReported: boolean = false;

  constructor(socket: Socket, handlers: FramedConnHandlers, options: FramedConnOptions = {}) {
    this._socket = socket;
    this._handlers = handlers;
    this._maxFrameSize = options.maxFrameSize ?? 0;
    this._writeTimeoutMs = options.writeTimeoutMs ?? 0;
    this._batchFrames = options.batchFrames ?? BATCH_FRAMES;
    this._timers = options.timers ?? defaultTimers;

    if (typeof socket.setNoDelay === "function") {
      socket.setNoDelay(true);
    }

    socket.on("data", (chunk: Buffer): void => this.onData(chunk));

    socket.on("error", (error: Error): void => {
      this._closeError = this._closeError ?? error;
    });

    socket.on("close", (): void => {
      this._closed = true;
      this.clearWriteTimer();
      this.reportClose();
    });

    socket.on("drain", (): void => {
      this._blocked = false;
      this.scheduleFlush();
    });
  }

  /** Bytes accepted for sending and not yet flushed to the kernel. */
  get outstandingBytes(): number {
    return this._queuedBytes + this._inFlightBytes;
  }

  get closed(): boolean {
    return this._closed;
  }

  /** Rebinds the inbound length bound after negotiation. */
  setMaxFrameSize(size: number): void {
    this._maxFrameSize = size;
  }

  /**
   * Accepts one frame for sending. The connection owns `body` from
   * here on; a caller reusing a scratch buffer must hand over a copy.
   * Header violations throw as programming errors; a closed
   * connection is an operational outcome and is returned instead.
   */
  send(frame: OutboundFrame): Error | null {
    if (this._closed || this._ending) {
      return ErrConnClosed;
    }

    const entry: QueuedFrame = {
      type: frame.type,
      flags: frame.flags,
      lane: frame.lane,
      length: frame.body === null ? 0 : frame.body.length,
      correlation: frame.correlation,
      body: frame.body,
      wrote: frame.wrote ?? null,
    };
    validateFrameHeader(entry);

    this._queue.push(entry);
    this._queuedBytes += FRAME_HEADER_SIZE + entry.length;
    this.scheduleFlush();
    return null;
  }

  /**
   * Flushes everything accepted so far, then ends the write side with
   * a FIN so the peer can read to the end. Frames sent after this are
   * refused.
   */
  end(): void {
    if (this._closed || this._ending) {
      return;
    }

    this._ending = true;
    this.scheduleFlush();
    this.maybeFinishEnd();
  }

  /** Destroys the connection now; queued frames are dropped. */
  destroy(error: Error | null = null): void {
    if (this._closed) {
      return;
    }

    this._closed = true;
    this._closeError = this._closeError ?? error;
    this._queue.clear();
    this._queuedBytes = 0;
    this.clearWriteTimer();
    this._socket.destroy();
    this.reportClose();
  }

  private reportClose(): void {
    if (this._closeReported) {
      return;
    }

    this._closeReported = true;
    this._handlers.onClose(this._closeError);
  }

  // The write side.

  private scheduleFlush(): void {
    if (this._flushScheduled || this._blocked || this._closed) {
      return;
    }

    this._flushScheduled = true;
    queueMicrotask((): void => {
      this._flushScheduled = false;
      this.flush();
    });
  }

  private flush(): void {
    while (!this._blocked && !this._closed && this._queue.length > 0) {
      this._batch.reset();
      let frames: number = 0;
      let batched: number = 0;
      let largeBody: Uint8Array | null = null;
      let largeWrote: unknown = null;
      let wrote: unknown[] | null = null;
      while (frames < this._batchFrames && this._batch.length < BATCH_BYTES) {
        const next: QueuedFrame | undefined = this._queue.peek();
        if (next === undefined) {
          break;
        }

        const isLarge: boolean = next.body !== null && next.body.length > COPY_THRESHOLD;
        if (isLarge && this._batch.length > 0) {
          break;
        }

        this._queue.shift();
        encodeFrameHeader(this._batch, next);
        batched += FRAME_HEADER_SIZE + next.length;
        frames += 1;
        if (isLarge) {
          largeBody = next.body;
          largeWrote = next.wrote;
          break;
        }

        if (next.wrote !== null) {
          wrote = wrote ?? [];
          wrote.push(next.wrote);
        }

        if (next.body !== null) {
          this._batch.writeBytes(next.body);
        }
      }

      this._queuedBytes -= batched;
      if (largeBody === null) {
        this.writeOut(this._batch.bytes().slice(), batched, wrote);
        continue;
      }

      // The large frame confirms on its body write, its second half.
      this.writeOut(this._batch.bytes().slice(), batched - largeBody.length, wrote);
      this.writeOut(largeBody, largeBody.length, largeWrote === null ? null : [largeWrote]);
    }

    this.maybeFinishEnd();
  }

  private writeOut(bytes: Uint8Array, accounted: number, wrote: unknown[] | null): void {
    this._inFlightBytes += accounted;
    this._lastWriteProgress = this._timers.now();
    this.armWriteTimer();
    const flushed: boolean = this._socket.write(bytes, (error?: Error | null): void => {
      this._inFlightBytes -= accounted;
      if (wrote !== null && (error === undefined || error === null)) {
        for (const token of wrote) {
          this._handlers.onWrote?.(token);
        }
      }

      if (this._closed) {
        return;
      }

      this._lastWriteProgress = this._timers.now();
      if (this._inFlightBytes === 0) {
        this.clearWriteTimer();
        this.maybeFinishEnd();
      }
    });
    if (!flushed) {
      this._blocked = true;
    }
  }

  private maybeFinishEnd(): void {
    if (!this._ending || this._closed) {
      return;
    }

    if (this._queue.length === 0 && this._inFlightBytes === 0) {
      this._socket.end();
    }
  }

  /**
   * The write timeout is a deadline check, not a per-write timer:
   * one timer stays armed while bytes are in flight, and on firing it
   * either tears the connection down or re-arms for the time the last
   * progress bought, so the steady state re-schedules nothing.
   */
  private armWriteTimer(): void {
    if (this._writeTimeoutMs <= 0 || this._writeTimer !== null) {
      return;
    }

    this._writeTimer = this._timers.schedule(this._writeTimeoutMs, (): void => {
      this.onWriteDeadline();
    });
  }

  private onWriteDeadline(): void {
    this._writeTimer = null;
    const elapsed: number = this._timers.now() - this._lastWriteProgress;
    if (elapsed >= this._writeTimeoutMs) {
      this.destroy(ErrWriteTimeout);
      return;
    }

    // Bytes moved since this deadline was armed; measure from there.
    this._writeTimer = this._timers.schedule(this._writeTimeoutMs - elapsed, (): void => {
      this.onWriteDeadline();
    });
  }

  private clearWriteTimer(): void {
    if (this._writeTimer === null) {
      return;
    }

    this._writeTimer.cancel();
    this._writeTimer = null;
  }

  // The read side.

  private onData(chunk: Buffer): void {
    if (this._broken || this._closed) {
      return;
    }

    try {
      this.parse(chunk);
    } catch (thrown) {
      if (thrown instanceof ProtocolError) {
        this._broken = true;
        this._handlers.onViolation(thrown);
        return;
      }

      // The owner's frame handler failed; that is fatal for the
      // connection but must never escape into the event loop.
      this.destroy(thrown instanceof Error ? thrown : new Error(String(thrown)));
    }
  }

  private parse(chunk: Buffer): void {
    let offset: number = 0;
    while (offset < chunk.length) {
      if (this._header === null) {
        offset = this.parseHeader(chunk, offset);
        if (this._header === null) {
          return;
        }
      }

      const header: FrameHeader = this._header;
      if (header.length === 0) {
        this.emit(header, EMPTY);
        continue;
      }

      if (this._bodyBytes === null && chunk.length - offset >= header.length) {
        const body: Uint8Array = chunk.subarray(offset, offset + header.length);
        offset += header.length;
        this.emit(header, body);
        continue;
      }

      if (this._bodyBytes === null) {
        this._bodyBytes = new Uint8Array(header.length);
        this._bodyFill = 0;
      }

      const wanted: number = header.length - this._bodyFill;
      const available: number = chunk.length - offset;
      const take: number = wanted < available ? wanted : available;
      this._bodyBytes.set(chunk.subarray(offset, offset + take), this._bodyFill);
      this._bodyFill += take;
      offset += take;
      if (this._bodyFill < header.length) {
        return;
      }

      const body: Uint8Array = this._bodyBytes;
      this.emit(header, body);
    }
  }

  /**
   * Consumes header bytes from the chunk; decodes straight out of the
   * chunk when all 16 bytes are there, accumulating into the retained
   * header buffer only when they span chunks.
   */
  private parseHeader(chunk: Buffer, offset: number): number {
    if (this._headerFill === 0 && chunk.length - offset >= FRAME_HEADER_SIZE) {
      this._header = decodeFrameHeader(chunk, this._maxFrameSize, offset);
      return offset + FRAME_HEADER_SIZE;
    }

    const wanted: number = FRAME_HEADER_SIZE - this._headerFill;
    const available: number = chunk.length - offset;
    const take: number = wanted < available ? wanted : available;
    this._headerBytes.set(chunk.subarray(offset, offset + take), this._headerFill);
    this._headerFill += take;
    if (this._headerFill < FRAME_HEADER_SIZE) {
      return offset + take;
    }

    this._headerFill = 0;
    this._header = decodeFrameHeader(this._headerBytes, this._maxFrameSize);
    return offset + take;
  }

  private emit(header: FrameHeader, body: Uint8Array): void {
    this._header = null;
    this._bodyBytes = null;
    this._bodyFill = 0;
    this._handlers.onFrame(header, body);
  }
}
