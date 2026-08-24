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

import type { IsolateRoute } from "./actor.ref";
import type { ActorSystem } from "./actor.system";
import { Codec } from "./codec";
import {
  ActorNotFoundError,
  ActorNotRegisteredError,
  ErrDead,
  ErrInvalidTimeout,
  ErrRequestTimeout,
} from "./errors";
import { Terminated } from "./messages";
import {
  type DataEnvelope,
  ERROR_APPLICATION,
  ERROR_BAD_REQUEST,
  type ErrorBody,
  type Hello,
  KIND_ASK,
  KIND_TELL,
  KIND_UNWATCH,
  KIND_WATCH,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "./net/envelope";
import { LANE_CONTROL } from "./net/frame";
import { Peer } from "./net/peer";
import { NetServer } from "./net/server";
import { ErrAskTimeout, PeerError, type Session } from "./net/session";
import { ByteReader, ByteWriter, decodeValue, encodeValue } from "./net/values";
import { type Path, parsePath } from "./path";
import type { PID } from "./pid";
import { Props } from "./props";
import type { ActorRecipe } from "./protocol";
import {
  completedRequest,
  type Reentrancy,
  type RequestCall,
  type RequestOptions,
} from "./reentrancy";
import { defaultActorRegistry, defaultMessageRegistry, placedRecipe } from "./registration";
import type { RemoteOptions } from "./remote.options";
import {
  decodeFailure,
  decodePayload,
  encodeFailure,
  encodePayload,
  type WirePayload,
} from "./remoting.codec";
import { routedPid } from "./routed.pid";
import type { SpawnOptions } from "./spawn.options";

/**
 * Remoting is the seam that joins the actor runtime to the network
 * transport in `src/net/`. Only the seam modules (`remoting.ts` and its
 * `remoting.*` companions) may import from that folder, so the
 * transport stays actor-blind and the dependency flows one way: the
 * runtime reaches the wire only through here, and a system that never
 * enables remoting never loads any of it.
 *
 * The seam owns the node's listening endpoint, the peer set, and the
 * routing that makes a remote actor an ordinary `PID`: a handle minted
 * here carries a route whose sends travel the wire, and inbound
 * envelopes resolve to local actors and deliver through the same send
 * paths a local message uses. The bridge between runtime messages and
 * wire payloads lives in `remoting.codec.ts`.
 *
 * @internal
 */

/** The capability revision the endpoint advertises: the first shipped
 * transport implements the whole ladder. */
const CAPABILITY_REVISION: number = 4;

/** The size caps and receive window the endpoint advertises, the
 * transport's own defaults (16 MiB) and its default concurrent-transfer
 * limit. Every advertisement is negotiated down to the pairwise minimum,
 * so these are ceilings, not commitments. */
const MAX_FRAME_SIZE: number = 16 * 1024 * 1024;
const MAX_MESSAGE_SIZE: number = 16 * 1024 * 1024;
const INITIAL_CREDITS: number = 16 * 1024 * 1024;
const MAX_LARGE_TRANSFERS: number = 4;

/** The route's worker id for a remote node: never a pool isolate's id,
 * so remote handles can never be mistaken for placed ones. */
const REMOTE_WORKER_ID: number = -1;

/** How long a control request (lookup, spawn, and friends) may wait
 * before it fails, in milliseconds. */
const CONTROL_TIMEOUT_MS: number = 10_000;

/** The control request resolving a top-level name on the receiving
 * node. Control requests address the node itself: their envelopes carry
 * an empty target path and settle without touching any actor. */
const CONTROL_LOOKUP: string = "nodeakt.remote.lookup";

/** The control request spawning a named actor on the receiving node
 * from a class registered there. */
const CONTROL_SPAWN: string = "nodeakt.remote.spawn";

/** The control request restarting a named actor on the receiving node
 * in place: same PID, same incarnation, fresh state. */
const CONTROL_RESPAWN: string = "nodeakt.remote.respawn";

/** The control request stopping a named actor on the receiving node
 * gracefully. */
const CONTROL_STOP: string = "nodeakt.remote.stop";

/** What a control lookup answers with: where the actor lives and which
 * incarnation holds the name, or null when no running top-level actor
 * does. */
interface ControlActorRef {
  readonly path: string;
  readonly uid: string;
}

/** What a control spawn carries: the actor name to hold, the class
 * name to construct (registered on the receiving node), the
 * constructor arguments, and the one spawn option that is data. */
interface ControlSpawn {
  readonly name: string;
  readonly actor: string;
  readonly args?: readonly unknown[];
  readonly reentrancy?: Reentrancy;
}

/** The empty body watch and unwatch envelopes carry. */
const EMPTY_PAYLOAD: Uint8Array = new Uint8Array(0);

/** One watch this node registered on a remote actor: who watches, the
 * watched path, and the node holding it, so a died connection settles
 * exactly the watches it carried. */
interface RemoteWatch {
  readonly watcher: PID;
  readonly target: string;
  readonly node: string;
}

/** Builds the HELLO the endpoint advertises to every peer it accepts or
 * dials. The lane is the control lane; the acceptor echoes each dialer's
 * chosen lane during negotiation. */
function buildHello(systemName: string, options: RemoteOptions): Hello {
  return {
    revision: CAPABILITY_REVISION,
    systemName,
    host: options.host,
    port: options.port,
    lane: LANE_CONTROL,
    compression: 0,
    maxFrameSize: MAX_FRAME_SIZE,
    maxMessageSize: MAX_MESSAGE_SIZE,
    initialCredits: INITIAL_CREDITS,
    maxLargeTransfers: MAX_LARGE_TRANSFERS,
  };
}

/**
 * The runtime's remoting layer for one actor system: the listening
 * endpoint, the peer set, and the wire-backed route behind every remote
 * PID.
 *
 * @internal
 */
export class Remoting {
  private readonly _system: ActorSystem;
  private readonly _server: NetServer;
  private readonly _local: Hello;
  private readonly _codec: Codec = new Codec(defaultMessageRegistry);

  /** The retained scratch buffer every outbound payload encodes into. */
  private readonly _writer: ByteWriter = new ByteWriter();

  /** The outbound side, one peer per remote node, dialed lazily. */
  private readonly _peers = new Map<string, Peer>();

  /** Handles minted for foreign senders, per path and incarnation, so
   * the same sender always resolves to the same handle instance. */
  private readonly _senders = new Map<string, PID>();

  /** The watches this node registered on remote actors, keyed by
   * watcher and target path, settled by an inbound Terminated or by the
   * death of the connection that carried them. */
  private readonly _watches = new Map<string, RemoteWatch>();

  /** Whether the layer has been stopped: closing is terminal, and a
   * stopping system's actors must not dial fresh connections from their
   * teardown hooks. */
  private _closed: boolean = false;

  private constructor(system: ActorSystem, server: NetServer, local: Hello) {
    this._system = system;
    this._server = server;
    this._local = local;
  }

  /**
   * Binds the endpoint on the configured host and port and returns the
   * live remoting layer. A bind failure rejects, so a system whose
   * endpoint cannot open fails to start rather than starting deaf.
   */
  static async start(system: ActorSystem, options: RemoteOptions): Promise<Remoting> {
    const local: Hello = buildHello(system.name(), options);
    let seam: Remoting | null = null;
    const server: NetServer = await NetServer.listen(
      { host: options.host, port: options.port, local },
      {
        onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
          (seam as Remoting).onData(session, envelope, correlation);
        },
      },
    );
    seam = new Remoting(system, server, local);
    return seam;
  }

  /** The port the endpoint is bound to, resolved from an ephemeral `0`
   * to the port the operating system chose. */
  get port(): number {
    return this._server.address.port;
  }

  /**
   * Closes the remoting layer: every peer closes first, dead-lettering
   * what it still holds, then the endpoint tears down every connection
   * at once so a stopping system leaves no listener and no live socket
   * behind.
   */
  async stop(): Promise<void> {
    this._closed = true;
    for (const peer of this._peers.values()) {
      peer.close();
    }

    this._peers.clear();
    await this._server.shutdown(-1);
  }

  /**
   * Resolves a top-level name on the remote node at `host:port` to a
   * PID-shaped handle, or undefined when no running top-level actor
   * holds the name there. Rejections carry the dial or transport
   * failure; a control answer that does not decode rejects with its
   * decode error.
   */
  async remoteLookup(host: string, port: number, name: string): Promise<PID | undefined> {
    const reply: ReplyEnvelope = await this.controlAsk(host, port, CONTROL_LOOKUP, { name });
    const answer = decodeValue(new ByteReader(reply.payload)) as ControlActorRef | null;
    if (answer === null) {
      return undefined;
    }

    return routedPid(this._system, parsePath(answer.path, answer.uid), this.routeTo(host, port));
  }

  /**
   * Spawns a top-level actor on the remote node at `host:port` and
   * returns its PID. Construction crosses by name: the class must be
   * registered on both nodes, here to validate and resolve the Props,
   * there to construct, under a name no other registered class shares.
   * A spawn failure on the far node settles the returned promise with
   * the failure, sentinel identity preserved.
   */
  async remoteSpawn(
    host: string,
    port: number,
    name: string,
    props: Props,
    options?: SpawnOptions,
  ): Promise<PID> {
    const recipe: ActorRecipe = placedRecipe(props, options);
    const reply: ReplyEnvelope = await this.controlAsk(host, port, CONTROL_SPAWN, {
      name,
      actor: recipe.actor,
      args: recipe.args,
      reentrancy: recipe.reentrancy,
    });
    return this.mintFrom(reply, host, port);
  }

  /**
   * Restarts the named top-level actor on the remote node at
   * `host:port` in place and returns its PID: same path, same
   * incarnation, fresh state through its lifecycle hooks.
   */
  async remoteReSpawn(host: string, port: number, name: string): Promise<PID> {
    const reply: ReplyEnvelope = await this.controlAsk(host, port, CONTROL_RESPAWN, { name });
    return this.mintFrom(reply, host, port);
  }

  /**
   * Stops the named top-level actor on the remote node at `host:port`
   * gracefully. A name no running actor holds there is already
   * stopped, so the request succeeds idempotently.
   */
  async remoteStop(host: string, port: number, name: string): Promise<void> {
    await this.controlAsk(host, port, CONTROL_STOP, { name });
  }

  /** Sends one control request to the node at `host:port` and returns
   * its raw answer, transport rejections lifted to the runtime's
   * contract. */
  private async controlAsk(
    host: string,
    port: number,
    typeRef: string,
    value: unknown,
  ): Promise<ReplyEnvelope> {
    const envelope: DataEnvelope = {
      kind: KIND_ASK,
      to: "",
      uid: "",
      sender: "",
      senderUid: "",
      timeout: 0,
      serializerId: SERIALIZER_BINARY,
      typeRef,
      payload: this.encodeControl(value),
    };

    try {
      return await this.peerFor(host, port).ask(envelope, CONTROL_TIMEOUT_MS);
    } catch (err) {
      throw this.liftError(err as Error);
    }
  }

  /** Mints the remote handle a control answer names. */
  private mintFrom(reply: ReplyEnvelope, host: string, port: number): PID {
    const answer = decodeValue(new ByteReader(reply.payload)) as ControlActorRef;
    return routedPid(this._system, parsePath(answer.path, answer.uid), this.routeTo(host, port));
  }

  /** Returns the wire-backed route to the node at `host:port`; a PID
   * minted with it sends, asks, and watches over the transport. */
  routeTo(host: string, port: number): IsolateRoute {
    return {
      workerId: REMOTE_WORKER_ID,
      tell: (to: Path, message: unknown, sender?: PID): Error | null =>
        this.tell(host, port, to, message, sender),
      ask: (to: Path, message: unknown, timeout: number, sender?: PID): Promise<unknown> =>
        this.ask(host, port, to, message, timeout, sender),
      request: (to: Path, message: unknown, sender: PID, options?: RequestOptions): RequestCall =>
        this.request(host, port, to, message, sender, options),
      watch: (to: Path, watcher: PID): void => this.watch(host, port, to, watcher),
      unwatch: (to: Path, watcher: PID): void => this.unwatch(host, port, to, watcher),
    };
  }

  /**
   * Registers `watcher` for a {@link Terminated} when the actor at `to`
   * on the node at `host:port` stops. The registration is recorded on
   * both sides: the far node delivers the notification when the actor
   * stops gracefully, and this side delivers it when the connection to
   * the node dies, because remote death and connection loss are
   * indistinguishable by design.
   */
  private watch(host: string, port: number, to: Path, watcher: PID): void {
    if (this._closed) {
      return;
    }

    const target: string = to.toString();
    this._watches.set(`${watcher.path().toString()}#${target}`, {
      watcher,
      target,
      node: `${host}:${port}`,
    });
    this.peerFor(host, port).tell(this.watchEnvelope(KIND_WATCH, to, watcher));
  }

  /** Cancels a {@link watch} registration on both sides; unknown
   * registrations are a no-op. */
  private unwatch(host: string, port: number, to: Path, watcher: PID): void {
    if (this._closed) {
      return;
    }

    this._watches.delete(`${watcher.path().toString()}#${to.toString()}`);
    this.peerFor(host, port).tell(this.watchEnvelope(KIND_UNWATCH, to, watcher));
  }

  /** Builds one watch or unwatch envelope: an empty body, the watcher
   * riding as the sender. */
  private watchEnvelope(kind: number, to: Path, watcher: PID): DataEnvelope {
    return {
      kind,
      to: to.toString(),
      uid: to.uid(),
      sender: watcher.path().toString(),
      senderUid: watcher.path().uid(),
      timeout: 0,
      serializerId: SERIALIZER_BINARY,
      typeRef: "",
      payload: EMPTY_PAYLOAD,
    };
  }

  /**
   * A control-lane connection died. Watches ride the control lane, and
   * remote death is indistinguishable from connection loss, so every
   * watch over that node settles now: each watcher receives one
   * {@link Terminated} for its target. A stopping system skips the
   * sweep, since its watchers are shutting down with it.
   */
  private onLaneClose(node: string, lane: number): void {
    if (lane !== LANE_CONTROL || !this._system.isRunning()) {
      return;
    }

    for (const [key, entry] of this._watches) {
      if (entry.node !== node) {
        continue;
      }

      this._watches.delete(key);
      this._system.noSender().tell(entry.watcher, new Terminated(entry.target));
    }
  }

  /** Sends one fire-and-forget message over the wire. Returns transport
   * accept, not delivery: an undeliverable envelope surfaces as a dead
   * letter on whichever side discovered it, and only an encode failure
   * returns its error, since the message could never leave. */
  private tell(host: string, port: number, to: Path, message: unknown, sender?: PID): Error | null {
    if (this._closed) {
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, ErrDead);
      return ErrDead;
    }

    let wire: WirePayload;
    try {
      wire = encodePayload(this._codec, this._writer, message);
    } catch (err) {
      const error = err as Error;
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, error);
      return error;
    }

    this.peerFor(host, port).tell(this.dataEnvelope(KIND_TELL, to, wire, 0, sender));
    return null;
  }

  /** Sends one ask over the wire; the transport owns the pending entry
   * and its timer, so settlement arrives as a promise. The receiving
   * node sees the remaining budget and re-derives its own deadline. */
  private ask(
    host: string,
    port: number,
    to: Path,
    message: unknown,
    timeout: number,
    sender?: PID,
  ): Promise<unknown> {
    if (this._closed) {
      return Promise.reject(ErrDead);
    }

    if (timeout <= 0) {
      return Promise.reject(ErrInvalidTimeout);
    }

    let wire: WirePayload;
    try {
      wire = encodePayload(this._codec, this._writer, message);
    } catch (err) {
      const error = err as Error;
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, error);
      return Promise.reject(error);
    }

    const envelope: DataEnvelope = this.dataEnvelope(KIND_ASK, to, wire, 0, sender);
    return this.peerFor(host, port)
      .ask(envelope, timeout)
      .then(
        (reply: ReplyEnvelope): unknown => decodePayload(this._codec, reply.typeRef, reply.payload),
        (err: Error): never => {
          throw this.liftError(err);
        },
      );
  }

  /** Issues a non-parking request over the wire on behalf of `sender`:
   * admission runs against the sender's reentrancy configuration and
   * the continuation runs on the sender's own turn. The handle settles
   * exactly once, so a reply racing a cancel is dropped by the
   * bookkeeping, never delivered twice. */
  private request(
    host: string,
    port: number,
    to: Path,
    message: unknown,
    sender: PID,
    options?: RequestOptions,
  ): RequestCall {
    if (this._closed) {
      return completedRequest(ErrDead);
    }

    const opened = sender.openRequest(options);
    if (opened instanceof Error) {
      return completedRequest(opened);
    }

    let wire: WirePayload;
    try {
      wire = encodePayload(this._codec, this._writer, message);
    } catch (err) {
      const error = err as Error;
      this._system.toDeadletter(sender.path().toString(), to.toString(), message, error);
      return completedRequest(error);
    }

    const handle = opened;
    const timeout: number = options?.timeout ?? 0;
    const envelope: DataEnvelope = this.dataEnvelope(KIND_ASK, to, wire, 0, sender);

    this.peerFor(host, port)
      .ask(envelope, timeout)
      .then(
        (reply: ReplyEnvelope): void => {
          let value: unknown;
          try {
            value = decodePayload(this._codec, reply.typeRef, reply.payload);
          } catch (err) {
            sender.deliverRequestReply(handle, undefined, err as Error);
            return;
          }

          sender.deliverRequestReply(handle, value, null);
        },
        (err: Error): void => {
          sender.deliverRequestReply(handle, undefined, this.liftError(err));
        },
      );

    sender.admitRequest(handle);
    return handle;
  }

  /** Dispatches one arrived envelope: control requests settle against
   * the node itself, everything else resolves to a local actor and
   * delivers through the ordinary send paths. */
  private onData(session: Session, envelope: DataEnvelope, correlation: number): void {
    if (envelope.to === "") {
      this.handleControl(session, envelope, correlation);
      return;
    }

    if (envelope.kind === KIND_WATCH) {
      this.handleWatch(envelope);
      return;
    }

    if (envelope.kind === KIND_UNWATCH) {
      this.handleUnwatch(envelope);
      return;
    }

    let message: unknown;
    try {
      message = decodePayload(this._codec, envelope.typeRef, envelope.payload);
    } catch (err) {
      this.undeliverable(session, envelope, correlation, envelope.payload, err as Error);
      return;
    }

    // A death notification settles this side's watch entry, so the
    // connection's eventual close never delivers a second Terminated
    // for it.
    if (message instanceof Terminated) {
      this._watches.delete(`${envelope.to}#${message.actorPath}`);
    }

    const target = this.targetOf(envelope);
    if (target === undefined) {
      this.undeliverable(session, envelope, correlation, message, ErrDead);
      return;
    }

    const sender = this.senderOf(envelope);
    if (correlation === 0) {
      sender.tell(target, message);
      return;
    }

    const err = target.deliverAsk(
      message,
      sender,
      envelope.timeout,
      (value: unknown): void => {
        this.reply(session, correlation, value);
      },
      (reason: Error): void => {
        session.replyError(correlation, encodeFailure(reason));
      },
    );
    if (err !== null) {
      session.replyError(correlation, encodeFailure(err));
    }
  }

  /** Records an envelope nothing could receive: a tell becomes a dead
   * letter here, an ask travels back as a request-scoped failure so the
   * asker settles early instead of waiting out its timeout. */
  private undeliverable(
    session: Session,
    envelope: DataEnvelope,
    correlation: number,
    message: unknown,
    reason: Error,
  ): void {
    if (correlation === 0) {
      this._system.toDeadletter(this.senderPathOf(envelope), envelope.to, message, reason);
      return;
    }

    session.replyError(correlation, encodeFailure(reason));
  }

  /**
   * Registers the far watcher on the local actor: the actor's eventual
   * stop tells the watcher handle, which routes the {@link Terminated}
   * back over the wire. Watching an actor that is already gone answers
   * with an immediate Terminated: once a watch crossed the boundary,
   * the watcher is always eventually notified of a death that is or
   * becomes true. A watch without a resolvable sender is a forged frame
   * and is dropped.
   */
  private handleWatch(envelope: DataEnvelope): void {
    const watcher = this.senderOf(envelope);
    if (watcher === this._system.noSender()) {
      return;
    }

    const target = this.targetOf(envelope);
    if (target === undefined || !target.isRunning()) {
      this._system.noSender().tell(watcher, new Terminated(envelope.to));
      return;
    }

    target.addWatcher(watcher);
  }

  /** Removes the far watcher from the local actor; the sender cache
   * guarantees the same handle instance that was registered. */
  private handleUnwatch(envelope: DataEnvelope): void {
    const watcher = this.senderOf(envelope);
    if (watcher === this._system.noSender()) {
      return;
    }

    const target = this.targetOf(envelope);
    if (target === undefined || !target.isRunning()) {
      return;
    }

    target.removeWatcher(watcher);
  }

  /** Settles one control request against this node. Control envelopes
   * carry an empty target path; a control tell is meaningless and is
   * dropped, and an unknown request answers a request-scoped failure so
   * a newer peer settles instead of timing out. */
  private handleControl(session: Session, envelope: DataEnvelope, correlation: number): void {
    if (correlation === 0) {
      return;
    }

    let data: unknown;
    try {
      data = decodeValue(new ByteReader(envelope.payload));
    } catch (err) {
      session.replyError(correlation, this.badRequest((err as Error).message));
      return;
    }

    if (envelope.typeRef === CONTROL_LOOKUP) {
      this.handleLookup(session, correlation, data);
      return;
    }

    if (envelope.typeRef === CONTROL_SPAWN) {
      this.handleSpawn(session, correlation, data);
      return;
    }

    if (envelope.typeRef === CONTROL_RESPAWN) {
      this.handleRespawn(session, correlation, data);
      return;
    }

    if (envelope.typeRef === CONTROL_STOP) {
      this.handleStop(session, correlation, data);
      return;
    }

    this.badControl(session, correlation, envelope.typeRef);
  }

  /** Answers a lookup with where the named top-level actor lives, or
   * null when no running actor holds the name. */
  private handleLookup(session: Session, correlation: number, data: unknown): void {
    const pid = this._system.actorOf((data as { name: string }).name);
    const answer: ControlActorRef | null =
      pid === undefined ? null : { path: pid.path().toString(), uid: pid.path().uid() };
    this.replyControl(session, correlation, answer);
  }

  /** Spawns the requested actor from its registered class: remote
   * construction crosses by name, so the class must be registered on
   * this node under a name no other registered class shares. Spawn
   * failures travel back settling the ask, sentinel identity
   * preserved. */
  private handleSpawn(session: Session, correlation: number, data: unknown): void {
    const request = data as ControlSpawn;
    const type = defaultActorRegistry.classOf(request.actor);
    if (type === undefined) {
      session.replyError(correlation, encodeFailure(new ActorNotRegisteredError(request.actor)));
      return;
    }

    const options: SpawnOptions | undefined =
      request.reentrancy === undefined ? undefined : { reentrancy: request.reentrancy };
    this._system.spawn(request.name, Props.restore(type, request.args ?? []), options).then(
      (pid: PID): void => {
        this.replyControl(session, correlation, {
          path: pid.path().toString(),
          uid: pid.path().uid(),
        });
      },
      (err: Error): void => {
        session.replyError(correlation, encodeFailure(err));
      },
    );
  }

  /** Restarts the named actor in place: same path, same incarnation,
   * fresh state through its lifecycle hooks. */
  private handleRespawn(session: Session, correlation: number, data: unknown): void {
    const pid = this._system.actorOf((data as { name: string }).name);
    if (pid === undefined) {
      session.replyError(
        correlation,
        encodeFailure(new ActorNotFoundError((data as { name: string }).name)),
      );
      return;
    }

    pid.restart().then(
      (): void => {
        this.replyControl(session, correlation, {
          path: pid.path().toString(),
          uid: pid.path().uid(),
        });
      },
      (err: Error): void => {
        session.replyError(correlation, encodeFailure(err));
      },
    );
  }

  /** Stops the named actor gracefully; a name nobody holds is already
   * stopped, so the request succeeds idempotently. */
  private handleStop(session: Session, correlation: number, data: unknown): void {
    const pid = this._system.actorOf((data as { name: string }).name);
    if (pid === undefined) {
      this.replyControl(session, correlation, null);
      return;
    }

    pid.shutdown().then(
      (): void => {
        this.replyControl(session, correlation, null);
      },
      (err: Error): void => {
        session.replyError(correlation, encodeFailure(err));
      },
    );
  }

  /** Answers a control request the node does not know; split out so the
   * dispatch above stays a flat list of guards. */
  private badControl(session: Session, correlation: number, typeRef: string): void {
    session.replyError(correlation, this.badRequest(`unknown control request "${typeRef}"`));
  }

  /** Builds the request-scoped failure body of a malformed or unknown
   * control request. */
  private badRequest(message: string): ErrorBody {
    return { code: ERROR_BAD_REQUEST, sentinel: 0, name: "Error", message };
  }

  /** Answers one control request with a plain value. */
  private replyControl(session: Session, correlation: number, value: unknown): void {
    session.reply(correlation, {
      serializerId: SERIALIZER_BINARY,
      typeRef: "",
      payload: this.encodeControl(value),
    });
  }

  /** Answers one ask with the value the actor responded, or with the
   * encode failure when the response cannot cross the wire. */
  private reply(session: Session, correlation: number, value: unknown): void {
    let wire: WirePayload;
    try {
      wire = encodePayload(this._codec, this._writer, value);
    } catch (err) {
      session.replyError(correlation, encodeFailure(err as Error));
      return;
    }

    session.reply(correlation, {
      serializerId: SERIALIZER_BINARY,
      typeRef: wire.typeRef,
      payload: wire.payload,
    });
  }

  /** Resolves the envelope's target to the live local PID it addresses,
   * or undefined when the path is malformed, nothing lives at it, or
   * the living actor is a different incarnation than the envelope was
   * pinned to. */
  private targetOf(envelope: DataEnvelope): PID | undefined {
    let path: Path;
    try {
      path = parsePath(envelope.to, envelope.uid);
    } catch {
      return undefined;
    }

    const pid = this._system.resolvePath(path);
    if (pid === undefined) {
      return undefined;
    }

    if (envelope.uid !== "" && envelope.uid !== pid.path().uid()) {
      return undefined;
    }

    return pid;
  }

  /** Resolves the envelope's sender. A sender on this very node
   * resolves to its live PID; a foreign sender resolves to a stable
   * handle carrying its path and incarnation, so replying to
   * `ctx.sender` routes back to the node it lives on. An absent or
   * malformed sender falls back to the system's NoSender actor. */
  private senderOf(envelope: DataEnvelope): PID {
    if (envelope.sender === "") {
      return this._system.noSender();
    }

    let path: Path;
    try {
      path = parsePath(envelope.sender, envelope.senderUid);
    } catch {
      return this._system.noSender();
    }

    if (this.isLocalNode(path)) {
      return this._system.resolvePath(path) ?? this._system.noSender();
    }

    return this.senderHandle(path, envelope.sender, envelope.senderUid);
  }

  /** Returns the stable handle of a foreign sender, minting it on first
   * sight; identity stays per path and incarnation, which watch removal
   * relies on. */
  private senderHandle(path: Path, sender: string, senderUid: string): PID {
    const key: string = `${sender}#${senderUid}`;
    let handle = this._senders.get(key);
    if (handle === undefined) {
      handle = routedPid(this._system, path, this.routeTo(path.host(), path.port()));
      this._senders.set(key, handle);
    }

    return handle;
  }

  /** Reports whether the path addresses an actor of this very node. */
  private isLocalNode(path: Path): boolean {
    return (
      path.system() === this._system.name() &&
      path.host() === this._system.host() &&
      path.port() === this._system.port()
    );
  }

  /** The dead-letter path string of the envelope's sender, or undefined
   * when the send carried none. */
  private senderPathOf(envelope: DataEnvelope): string | undefined {
    return envelope.sender === "" ? undefined : envelope.sender;
  }

  /** Builds one outbound data envelope around an encoded payload. */
  private dataEnvelope(
    kind: number,
    to: Path,
    wire: WirePayload,
    timeout: number,
    sender?: PID,
  ): DataEnvelope {
    return {
      kind,
      to: to.toString(),
      uid: to.uid(),
      sender: sender?.path().toString() ?? "",
      senderUid: sender?.path().uid() ?? "",
      timeout,
      serializerId: SERIALIZER_BINARY,
      typeRef: wire.typeRef,
      payload: wire.payload,
    };
  }

  /** Returns the peer of the node at `host:port`, creating it on first
   * use; dialing stays lazy inside the peer itself. */
  private peerFor(host: string, port: number): Peer {
    const key: string = `${host}:${port}`;
    let peer = this._peers.get(key);
    if (peer === undefined) {
      peer = new Peer(host, port, this._local, {
        onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
          this.onData(session, envelope, correlation);
        },
        onDeadLetter: (envelope: DataEnvelope, reason: Error): void => {
          this.deadLetterOut(envelope, reason);
        },
        onLaneClose: (lane: number): void => {
          this.onLaneClose(key, lane);
        },
      });
      this._peers.set(key, peer);
    }

    return peer;
  }

  /** Records one outbound envelope the peer could not deliver. A watch
   * that could not reach its node settles as the death it can no longer
   * observe, an unwatch has nothing left to cancel, and everything else
   * becomes a dead letter. The payload was encoded on this side, so
   * restoring it for the record can only fail against a registry that
   * changed mid-flight. */
  private deadLetterOut(envelope: DataEnvelope, reason: Error): void {
    if (envelope.kind === KIND_WATCH) {
      const key: string = `${envelope.sender}#${envelope.to}`;
      const entry = this._watches.get(key);
      if (entry !== undefined && this._system.isRunning()) {
        this._watches.delete(key);
        this._system.noSender().tell(entry.watcher, new Terminated(entry.target));
      }

      return;
    }

    if (envelope.kind === KIND_UNWATCH) {
      return;
    }

    let message: unknown;
    /* v8 ignore start -- this side encoded the payload, so the decode
       can only fail if the registry lost the type between the send and
       the dead letter, which no test can arrange. */
    try {
      message = decodePayload(this._codec, envelope.typeRef, envelope.payload);
    } catch {
      message = envelope.payload;
    }
    /* v8 ignore stop */

    this._system.toDeadletter(this.senderPathOf(envelope), envelope.to, message, reason);
  }

  /** Maps a transport rejection to the runtime's contract: the
   * transport's ask expiry becomes the runtime's timeout sentinel, a
   * peer's application failure decodes back to the error the far side
   * encoded (sentinel identity preserved), and every other failure
   * passes through as the transport reported it. */
  private liftError(err: Error): Error {
    if (err === ErrAskTimeout) {
      return ErrRequestTimeout;
    }

    if (err instanceof PeerError && err.code === ERROR_APPLICATION) {
      return decodeFailure(err.sentinel, err.errorName, err.message);
    }

    return err;
  }

  /** Encodes one control value into a payload the caller owns. */
  private encodeControl(value: unknown): Uint8Array {
    this._writer.reset();
    encodeValue(this._writer, value);
    return Uint8Array.from(this._writer.bytes());
  }
}
