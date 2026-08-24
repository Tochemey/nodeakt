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

import type { MessagePort } from "node:worker_threads";
import type { IsolateRoute } from "./actor.ref";
import type { ActorSystem } from "./actor.system";
import { Codec, decodeError, encodeError } from "./codec";
import type { Envelope, WireMessage } from "./envelope";
import { ErrDead } from "./errors";
import type { MessageRegistry } from "./message.registry";
import { Terminated } from "./messages";
import { type Path, parsePath } from "./path";
import type { PID } from "./pid";
import { cancelAsk, createRequestContext, type ReceiveContext } from "./receive.context";
import { completedRequest, type RequestCall, type RequestOptions } from "./reentrancy";
import { routedPid } from "./routed.pid";

/**
 * One unanswered ask or request on the sending side, keyed by
 * correlation id. The context is a real pending-ask context, so the
 * shared timeout wheel expires it exactly like a local ask; `fail`
 * settles it with a failure that arrived as a reply envelope.
 */
interface Pending {
  readonly ctx: ReceiveContext;
  fail(error: Error): void;
}

/** The sender path of an envelope in dead-letter form: undefined when
 * the send carried no sender. */
function senderPathOf(envelope: Envelope): string | undefined {
  return envelope.sender === "" ? undefined : envelope.sender;
}

/** Collects every transferable ArrayBuffer reachable from the value:
 * buffers themselves, the buffers under typed arrays and DataViews, and
 * anything nested in arrays, maps, sets, and object properties. Shared
 * buffers are not transferable and are left alone; `seen` breaks cycles
 * and deduplicates a buffer referenced twice. */
function collectBuffers(value: unknown, seen: Set<object>, out: ArrayBuffer[]): void {
  if (value === null || typeof value !== "object" || seen.has(value)) {
    return;
  }

  seen.add(value);
  if (value instanceof ArrayBuffer) {
    out.push(value);
    return;
  }

  if (ArrayBuffer.isView(value)) {
    collectBuffers(value.buffer, seen, out);
    return;
  }

  if (value instanceof Map) {
    for (const [key, entry] of value) {
      collectBuffers(key, seen, out);
      collectBuffers(entry, seen, out);
    }

    return;
  }

  if (value instanceof Set) {
    for (const entry of value) {
      collectBuffers(entry, seen, out);
    }

    return;
  }

  for (const key of Object.keys(value)) {
    collectBuffers((value as Record<string, unknown>)[key], seen, out);
  }
}

/** Returns the transfer list of one wire payload, or undefined when it
 * holds no transferable buffer; primitive payloads never allocate. */
function transferablesOf(wire: WireMessage | null): ArrayBuffer[] | undefined {
  if (wire === null || wire.data === null || typeof wire.data !== "object") {
    return undefined;
  }

  const out: ArrayBuffer[] = [];
  collectBuffers(wire.data, new Set(), out);
  return out.length === 0 ? undefined : out;
}

/**
 * PortTransport carries envelopes over one `MessagePort` to the
 * isolate at the other end: sends go out on the port, envelopes
 * arriving on the port are delivered into this end's actor system, and
 * replies travel back the way their ask came. One instance serves one
 * end of one channel; a worker holds one per connected isolate, and
 * that map is the port mesh.
 *
 * The port does the moving and the cloning; this class supplies the
 * semantics, which are the cross-isolate semantics:
 *
 * - `tell` reports transport accept, not mailbox accept: a full
 *   mailbox on the target surfaces as a dead letter, not as a
 *   synchronous error. This is the documented location-transparency
 *   trade.
 * - `ask` and `request` correlate replies by id. The sending side
 *   keeps the pending entry on the shared timeout wheel, so a far
 *   isolate that never answers cannot strand it; the receiving side enforces
 *   the same coarse timeout on its own pending context.
 * - An undeliverable envelope becomes a dead letter on the receiving
 *   system, carried by the {@link ErrDead} sentinel.
 * - Every ask and request carries a timeout, defaulted to the system's
 *   `askTimeout` when the caller omits one, so a correlation entry is
 *   always bounded by that deadline; a cancel settles the handle early
 *   and the entry clears when the deadline or the reply arrives.
 *
 * Delivery is FIFO per port pair, so per-sender-per-target ordering
 * holds across the boundary exactly as it holds locally.
 *
 * The transport owns its port: {@link close} settles every pending
 * entry with {@link ErrDead} and closes the port, which tears the
 * channel down for both ends. An envelope already queued on this end
 * still fires afterwards and is dead-lettered (or dropped, for a
 * reply); an envelope the closing port discards before dispatch is
 * gone, and isolate death is the control plane's concern, not this
 * class's.
 *
 * Under test, a `MessageChannel` pair connects two transports inside
 * one isolate; in production the ports come from worker wiring. The
 * class cannot tell the difference.
 *
 * @internal
 */
export class PortTransport {
  private readonly _system: ActorSystem;
  private readonly _codec: Codec;
  private readonly _port: MessagePort;
  private readonly _selfId: number;
  private readonly _pending = new Map<number, Pending>();

  /** Handles minted for foreign senders, keyed by path and incarnation,
   * so `ctx.sender` identity is stable across messages and a watch
   * registration can be removed by the very instance that made it. */
  private readonly _senders = new Map<string, PID>();

  /** Watches this side registered on the far isolate, keyed by watcher
   * and target path. Close settles them all with `Terminated`: a dead
   * isolate means every actor it owned is dead. */
  private readonly _watches = new Map<string, { watcher: PID; target: string }>();

  /** The route back to the isolate at the other end of the port, given
   * to every foreign-sender handle this side mints. */
  private readonly _backRoute: IsolateRoute;

  private _nextCid = 1;
  private _closed = false;

  constructor(
    system: ActorSystem,
    registry: MessageRegistry,
    port: MessagePort,
    selfId: number,
    peerId: number,
  ) {
    this._system = system;
    this._codec = new Codec(registry);
    this._port = port;
    this._selfId = selfId;
    this._backRoute = {
      workerId: peerId,
      tell: (to, message, sender) => this.tell(to, message, sender),
      ask: (to, message, timeout, sender) => this.ask(to, message, timeout, sender),
      request: (to, message, sender, options) => this.request(to, message, sender, options),
      watch: (to, watcher) => this.watch(to, watcher),
      unwatch: (to, watcher) => this.unwatch(to, watcher),
    };
    port.on("message", (envelope: unknown) => {
      // Nothing arriving on the port may take the isolate down: a
      // malformed or forged envelope is logged and dropped.
      try {
        this.deliver(envelope as Envelope);
      } catch (err) {
        system.logger().error("envelope delivery failed", { error: err as Error });
      }
    });
  }

  /** Returns the route to the isolate at the other end of the port.
   * Handles built on it carry sends, watches, and replies back there. */
  route(): IsolateRoute {
    return this._backRoute;
  }

  /**
   * Sends a message to the actor addressed by the path, fire and
   * forget.
   *
   * @returns `null` when the transport accepted the envelope, which is
   * not delivery: an unresolvable path or a rejecting mailbox surfaces
   * as a dead letter on the receiving side. An encode or clone failure
   * returns its error, and the {@link ErrDead} sentinel reports a
   * closed transport or a stopped system, the refused message recorded
   * as a dead letter.
   */
  tell(to: Path, message: unknown, sender?: PID): Error | null {
    if (this.refused()) {
      // A caller that checks the return sees the refusal, but a caller
      // that fires and forgets (a pipe delivering a task result) must
      // never lose a message silently, so the refusal is also a dead
      // letter, exactly as the network route records it.
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, ErrDead);
      return ErrDead;
    }

    let wire: WireMessage;
    try {
      wire = this._codec.encodeMessage(message);
    } catch (err) {
      const error = err as Error;
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, error);
      return error;
    }

    const err = this.post({
      kind: "tell",
      to: to.toString(),
      uid: to.uid(),
      sender: sender?.path().toString() ?? "",
      senderUid: sender?.path().uid() ?? "",
      senderWorkerId: this._selfId,
      cid: 0,
      timeout: 0,
      message: wire,
      error: null,
    });
    if (err !== null) {
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, err);
      return err;
    }

    return null;
  }

  /**
   * Sends a message to the actor addressed by the path and waits for
   * its response, with the contract of `PID.ask`: the promise settles
   * with the value passed to `ReceiveContext.response`, falls back to
   * the system's `askTimeout` for a non-positive timeout so the ask is
   * never unbounded, rejects with {@link ErrRequestTimeout} on coarse
   * expiry, {@link ErrDead} when the path does not resolve to a running
   * actor of its incarnation or the transport is closed, and the
   * delivery error otherwise.
   */
  ask(to: Path, message: unknown, timeout: number, sender?: PID): Promise<unknown> {
    if (this.refused()) {
      return Promise.reject(ErrDead);
    }

    const deadline = timeout > 0 ? timeout : this._system.askTimeout();
    const from = sender ?? this._system.noSender();

    let wire: WireMessage;
    try {
      wire = this._codec.encodeMessage(message);
    } catch (err) {
      const error = err as Error;
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, error);
      return Promise.reject(error);
    }

    const cid = this._nextCid++;
    let resolve!: (value: unknown) => void;
    let reject!: (reason: Error) => void;
    const reply = new Promise<unknown>((res, rej) => {
      resolve = res;
      reject = rej;
    });

    // The context never enters a mailbox; it exists so the pending
    // entry rides the shared timeout wheel, and its self and sender
    // slots are inert bookkeeping.
    const done = (value: unknown): void => {
      this._pending.delete(cid);
      resolve(value);
    };

    const failed = (reason: Error): void => {
      this._pending.delete(cid);
      reject(reason);
    };

    const ctx = createRequestContext(message, from, from, deadline, done, failed);
    this._pending.set(cid, {
      ctx,
      fail: (error) => {
        cancelAsk(ctx);
        this._pending.delete(cid);
        reject(error);
      },
    });

    const err = this.post({
      kind: "ask",
      to: to.toString(),
      uid: to.uid(),
      sender: sender?.path().toString() ?? "",
      senderUid: sender?.path().uid() ?? "",
      senderWorkerId: this._selfId,
      cid,
      timeout: deadline,
      message: wire,
      error: null,
    });
    if (err !== null) {
      cancelAsk(ctx);
      this._pending.delete(cid);
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, err);
      return Promise.reject(err);
    }

    return reply;
  }

  /**
   * Issues a non-parking request to the actor addressed by the path on
   * behalf of `sender`, with the contract of `ReceiveContext.request`:
   * admission runs against the sender's reentrancy configuration and
   * the continuation runs on the sender's own turn. A closed transport
   * or stopped system completes the handle with {@link ErrDead}.
   */
  request(to: Path, message: unknown, sender: PID, options?: RequestOptions): RequestCall {
    if (this.refused()) {
      return completedRequest(ErrDead);
    }

    const opened = sender.openRequest(options);
    if (opened instanceof Error) {
      return completedRequest(opened);
    }

    let wire: WireMessage;
    try {
      wire = this._codec.encodeMessage(message);
    } catch (err) {
      const error = err as Error;
      this._system.toDeadletter(sender.path().toString(), to.toString(), message, error);
      return completedRequest(error);
    }

    const handle = opened;
    const cid = this._nextCid++;
    const rawTimeout = options?.timeout ?? 0;
    const timeout = rawTimeout > 0 ? rawTimeout : this._system.askTimeout();

    const delivered = (reply: unknown): void => {
      this._pending.delete(cid);
      handle.detachDelivery();
      sender.deliverRequestReply(handle, reply, null);
    };

    const failed = (error: Error): void => {
      this._pending.delete(cid);
      handle.detachDelivery();
      sender.deliverRequestReply(handle, undefined, error);
    };

    const ctx = createRequestContext(message, sender, sender, timeout, delivered, failed);
    handle.attachDelivery(ctx);
    this._pending.set(cid, {
      ctx,
      fail: (error) => {
        cancelAsk(ctx);
        failed(error);
      },
    });

    const err = this.post({
      kind: "ask",
      to: to.toString(),
      uid: to.uid(),
      sender: sender.path().toString(),
      senderUid: sender.path().uid(),
      senderWorkerId: this._selfId,
      cid,
      timeout,
      message: wire,
      error: null,
    });
    if (err !== null) {
      cancelAsk(ctx);
      this._pending.delete(cid);
      this._system.toDeadletter(sender.path().toString(), to.toString(), message, err);
      return completedRequest(err);
    }

    sender.admitRequest(handle);
    return handle;
  }

  /**
   * Registers `watcher` for a {@link Terminated} when the actor at
   * `to` stops on the far isolate. The registration is recorded on
   * both sides: the far isolate delivers the notification when the
   * actor stops gracefully, and this side delivers it on
   * {@link close}, because a dead isolate means every actor it owned
   * is dead. Watching through a closed transport is a no-op, exactly
   * as watching a stopped local actor is.
   */
  watch(to: Path, watcher: PID): void {
    if (this.refused()) {
      return;
    }

    this._watches.set(`${watcher.id()}#${to.toString()}`, {
      watcher,
      target: to.toString(),
    });
    this.post({
      kind: "watch",
      to: to.toString(),
      uid: to.uid(),
      sender: watcher.path().toString(),
      senderUid: watcher.path().uid(),
      senderWorkerId: this._selfId,
      cid: 0,
      timeout: 0,
      message: null,
      error: null,
    });
  }

  /** Cancels a {@link watch} registration on both sides; unknown
   * registrations and a closed transport are a no-op. */
  unwatch(to: Path, watcher: PID): void {
    if (this.refused()) {
      return;
    }

    this._watches.delete(`${watcher.id()}#${to.toString()}`);
    this.post({
      kind: "unwatch",
      to: to.toString(),
      uid: to.uid(),
      sender: watcher.path().toString(),
      senderUid: watcher.path().uid(),
      senderWorkerId: this._selfId,
      cid: 0,
      timeout: 0,
      message: null,
      error: null,
    });
  }

  /**
   * Closes this end: every pending ask and request settles with the
   * {@link ErrDead} sentinel, every watch this side registered on the
   * far isolate delivers {@link Terminated} to its watcher, further
   * sends are refused with {@link ErrDead}, and the port is closed,
   * tearing the channel down for both ends. An envelope already queued
   * on this end still fires afterwards: a message becomes a dead
   * letter, a reply is dropped. Closing again is a no-op.
   */
  close(): void {
    if (this._closed) {
      return;
    }

    this._closed = true;
    const entries = [...this._pending.values()];
    this._pending.clear();
    for (const entry of entries) {
      entry.fail(ErrDead);
    }

    const watches = [...this._watches.values()];
    this._watches.clear();
    if (this._system.isRunning()) {
      for (const { watcher, target } of watches) {
        this._system.noSender().tell(watcher, new Terminated(target));
      }
    }

    this._port.close();
  }

  /** Reports whether sends must be refused: the transport is closed or
   * the system it delivers into is not running. */
  private refused(): boolean {
    return this._closed || !this._system.isRunning();
  }

  /**
   * Posts one envelope on the port; the port clones it, exactly like a
   * hop between isolates, except that `ArrayBuffer`s in the payload are
   * transferred instead of copied. Transfer detaches the sender's
   * buffer, so a message must not carry a buffer its sender still
   * needs; posting the same buffer twice fails as a clone error.
   *
   * @returns `null`, or the clone error when the envelope holds data no
   * boundary can carry.
   */
  private post(envelope: Envelope): Error | null {
    try {
      const transfers = transferablesOf(envelope.message);
      if (transfers === undefined) {
        this._port.postMessage(envelope);
      } else {
        this._port.postMessage(envelope, transfers);
      }
    } catch (err) {
      return err as Error;
    }

    return null;
  }

  /** Decodes an arrived payload for dead-letter reporting, falling
   * back to the raw wire data when this side cannot restore it. */
  private decodeLoose(wire: WireMessage): unknown {
    try {
      return this._codec.decodeMessage(wire);
    } catch {
      return wire.data;
    }
  }

  /** Routes one arrived envelope by kind; on a closed transport a
   * message becomes a dead letter, while replies, watches, and
   * unwatches are dropped, since close already settled every pending
   * entry and every watch. Unknown kinds are ignored so the wire
   * contract can grow without breaking older isolates. */
  private deliver(envelope: Envelope): void {
    if (this._closed) {
      if (envelope.kind === "tell" || envelope.kind === "ask") {
        this._system.toDeadletter(
          senderPathOf(envelope),
          envelope.to,
          this.decodeLoose(envelope.message as WireMessage),
          ErrDead,
        );
      }

      return;
    }

    if (envelope.kind === "tell") {
      this.deliverTell(envelope);
      return;
    }

    if (envelope.kind === "ask") {
      this.deliverAsk(envelope);
      return;
    }

    if (envelope.kind === "reply") {
      this.deliverReply(envelope);
      return;
    }

    if (envelope.kind === "watch") {
      this.deliverWatch(envelope);
      return;
    }

    if (envelope.kind === "unwatch") {
      this.deliverUnwatch(envelope);
    }
  }

  /** Registers the far watcher on the local actor's tree; the actor's
   * eventual stop tells the watcher handle, which routes the
   * {@link Terminated} back through this transport. Watching an actor
   * that is already gone answers with an immediate Terminated, the
   * Erlang and Akka semantics: once a watch crossed the boundary, the
   * watcher is always eventually notified of a death that is or
   * becomes true. A watch without a sender is a forged frame and is
   * dropped. */
  private deliverWatch(envelope: Envelope): void {
    if (envelope.sender === "") {
      return;
    }

    const target = this.target(envelope);
    if (target === null || !target.isRunning()) {
      this.post({
        kind: "tell",
        to: envelope.sender,
        uid: envelope.senderUid,
        sender: "",
        senderUid: "",
        senderWorkerId: this._selfId,
        cid: 0,
        timeout: 0,
        message: this._codec.encodeMessage(new Terminated(envelope.to)),
        error: null,
      });
      return;
    }

    target.addWatcher(this.senderHandle(envelope.sender, envelope.senderUid));
  }

  /** Removes the far watcher from the local actor's tree; the sender
   * cache guarantees the same handle instance that was registered. */
  private deliverUnwatch(envelope: Envelope): void {
    if (envelope.sender === "") {
      return;
    }

    const target = this.target(envelope);
    if (target === null || !target.isRunning()) {
      return;
    }

    target.removeWatcher(this.senderHandle(envelope.sender, envelope.senderUid));
  }

  /** Hands a tell to its target's mailbox; an unresolvable target, a
   * rejecting mailbox, or a payload this side cannot decode becomes a
   * dead letter. */
  private deliverTell(envelope: Envelope): void {
    const wire = envelope.message as WireMessage;
    let message: unknown;
    try {
      message = this._codec.decodeMessage(wire);
    } catch (err) {
      this._system.toDeadletter(senderPathOf(envelope), envelope.to, wire.data, err as Error);
      return;
    }

    // A death notification settles this side's watch entry, so the
    // close sweep never delivers a second Terminated for it.
    if (message instanceof Terminated) {
      this._watches.delete(`${envelope.to}#${message.actorPath}`);
    }

    const target = this.target(envelope);
    if (target === null) {
      this._system.toDeadletter(senderPathOf(envelope), envelope.to, message, ErrDead);
      return;
    }

    this.senderOf(envelope).tell(target, message);
  }

  /** Hands an ask to its target's mailbox with the response routed
   * back as a reply envelope; every failure, a payload this side
   * cannot decode included, travels back as a failed reply so the
   * asker settles early instead of waiting out its timeout. */
  private deliverAsk(envelope: Envelope): void {
    const cid = envelope.cid;
    const wire = envelope.message as WireMessage;
    let message: unknown;
    try {
      message = this._codec.decodeMessage(wire);
    } catch (err) {
      this._system.toDeadletter(senderPathOf(envelope), envelope.to, wire.data, err as Error);
      this.replyFailure(cid, err as Error);
      return;
    }

    const target = this.target(envelope);
    if (target === null) {
      this._system.toDeadletter(senderPathOf(envelope), envelope.to, message, ErrDead);
      this.replyFailure(cid, ErrDead);
      return;
    }

    const err = target.deliverAsk(
      message,
      this.senderOf(envelope),
      envelope.timeout,
      (value) => this.replySuccess(cid, value),
      (reason) => this.replyFailure(cid, reason),
    );
    if (err !== null) {
      this.replyFailure(cid, err);
    }
  }

  /** Settles the pending entry a reply correlates with; a reply whose
   * ask already settled, timed out, or was canceled is dropped. */
  private deliverReply(envelope: Envelope): void {
    const entry = this._pending.get(envelope.cid);
    if (entry === undefined) {
      return;
    }

    this._pending.delete(envelope.cid);
    const error = envelope.error;
    if (error !== null) {
      entry.fail(decodeError(error));
      return;
    }

    let value: unknown;
    try {
      value = this._codec.decodeMessage(envelope.message as WireMessage);
    } catch (err) {
      entry.fail(err as Error);
      return;
    }

    entry.ctx.response(value);
  }

  /** Posts a successful reply; a response value that cannot encode or
   * clone travels back as a failed reply instead. */
  private replySuccess(cid: number, value: unknown): void {
    let wire: WireMessage;
    try {
      wire = this._codec.encodeMessage(value);
    } catch (err) {
      this.replyFailure(cid, err as Error);
      return;
    }

    const err = this.post({
      kind: "reply",
      to: "",
      uid: "",
      sender: "",
      senderUid: "",
      senderWorkerId: this._selfId,
      cid,
      timeout: 0,
      message: wire,
      error: null,
    });
    if (err !== null) {
      this.replyFailure(cid, err);
    }
  }

  /** Posts a failed reply; the error wire form always clones. */
  private replyFailure(cid: number, error: Error): void {
    this.post({
      kind: "reply",
      to: "",
      uid: "",
      sender: "",
      senderUid: "",
      senderWorkerId: this._selfId,
      cid,
      timeout: 0,
      message: null,
      error: encodeError(error),
    });
  }

  /** Resolves the envelope's target to the live PID it addresses, or
   * null when nothing lives at the path or the living actor is a
   * different incarnation than the envelope was pinned to. */
  private target(envelope: Envelope): PID | null {
    const pid = this._system.resolvePath(parsePath(envelope.to, envelope.uid));
    if (pid === undefined) {
      return null;
    }

    if (envelope.uid !== "" && envelope.uid !== pid.path().uid()) {
      return null;
    }

    return pid;
  }

  /** Resolves the envelope's sender. A sender living on this very
   * isolate resolves to its live PID; a foreign sender resolves to a
   * routed handle carrying its path and incarnation, so replying to
   * `ctx.sender` routes back to wherever it lives. Only an absent
   * sender falls back to the system's NoSender actor. */
  private senderOf(envelope: Envelope): PID {
    if (envelope.sender === "") {
      return this._system.noSender();
    }

    if (envelope.senderWorkerId === this._selfId) {
      return this._system.resolvePath(parsePath(envelope.sender)) ?? this._system.noSender();
    }

    return this.senderHandle(envelope.sender, envelope.senderUid);
  }

  /** Returns the routed handle of a foreign sender, minting it on
   * first sight; the cache keeps handle identity stable per path and
   * incarnation, which watch removal relies on. */
  private senderHandle(path: string, uid: string): PID {
    const key = `${path}#${uid}`;
    let handle = this._senders.get(key);
    if (handle === undefined) {
      handle = routedPid(this._system, parsePath(path, uid), this._backRoute);
      this._senders.set(key, handle);
    }

    return handle;
  }
}
