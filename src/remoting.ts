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

import { readFile } from "node:fs/promises";
import type { IsolateRoute } from "./actor.ref";
import type { ActorSystem } from "./actor.system";
import { Codec } from "./codec";
import { ActorNotFoundError, ActorNotRegisteredError, ErrDead, ErrRequestTimeout } from "./errors";
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
import { ErrAskTimeout, PeerError, REVISION_CURRENT, type Session } from "./net/session";
import type { TlsConfig } from "./net/tls";
import { ByteReader, ByteWriter, decodeValue, encodeValue } from "./net/values";
import { type Path, parsePath } from "./path";
import type { PID } from "./pid";
import { type ActorClass, Props } from "./props";
import type { ActorRecipe } from "./protocol";
import {
  completedRequest,
  type Reentrancy,
  type RequestCall,
  type RequestHandle,
  type RequestOptions,
} from "./reentrancy";
import { defaultActorRegistry, defaultMessageRegistry, placedRecipe } from "./registration";
import type { RemoteOptions, TlsOptions } from "./remote.options";
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
 * The trust model is a private network whose nodes trust each other:
 * an envelope's sender path is self-declared, so a hostile peer can
 * steer reply dials with forged paths. The seam's own state is
 * bounded (the sender cache is a capped LRU and idle peers are
 * reclaimed), and the `tls` option encrypts the carrier, mutual TLS
 * included. A verified peer certificate is an identity, not an
 * authorization: nothing checks what a peer may do, so nothing here
 * should face an untrusted network.
 *
 * @internal
 */

/** The capability revision the endpoint advertises: whatever the
 * transport implements, so the seam can never lag or outrun the wire. */
const CAPABILITY_REVISION: number = REVISION_CURRENT;

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

/** The target path of a control envelope: control requests address the
 * node itself, so the field is deliberately empty. An absent sender
 * shares the spelling, so dispatch names this constant. */
const CONTROL_TARGET: string = "";

/** The shutdown grace handed to the endpoint on stop: negative means
 * destroy every connection now. */
const SHUTDOWN_NOW: number = -1;

/** The sender-cache cap: generous, so a conforming topology never
 * feels it, while a hostile peer forging sender paths meets a bound.
 * An internal sizing fact, not configuration; exported only so the
 * bounding tests can push past it.
 *
 * @internal
 */
export const SENDER_CACHE_SIZE: number = 4096;

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

/** One watch a far node registered here over the wire: the local
 * target, the watcher handle registered on it, and the sender-cache
 * key the watcher was pinned under, so the sweep can release the pin
 * it acquired. */
interface InboundWatch {
  readonly target: PID;
  readonly watcher: PID;
  readonly watcherKey: string;
}

/** One cached foreign-sender handle. The pin set holds the path of
 * every local actor the handle is registered on as an inbound watcher:
 * a pinned entry survives eviction, because evicting it would mint a
 * different instance for the same sender and break the
 * unwatch-by-identity the inbound registrations rely on. Pins are
 * keyed per watched target so no path can release one twice. */
interface SenderEntry {
  readonly handle: PID;
  readonly pins: Set<string>;
}

/** The peer-map key of one remote node. */
function nodeKey(host: string, port: number): string {
  return `${host}:${port}`;
}

/** The registration key of one outbound watch: who watches what. */
function watchKey(watcher: string, target: string): string {
  return `${watcher}#${target}`;
}

/** The cache key of one foreign sender: its path and incarnation. */
function senderKey(sender: string, uid: string): string {
  return `${sender}#${uid}`;
}

/** Resolves one TLS option to the material `node:tls` consumes: PEM
 * contents pass through, anything else is read as a file path, so
 * certificates stay the operator's concern in whichever form their
 * tooling produces. */
async function resolvePem(field: string, value: string): Promise<string | Buffer> {
  if (value.includes("-----BEGIN ")) {
    return value;
  }

  try {
    return await readFile(value);
  } catch (err) {
    throw new TypeError(
      `remote tls ${field} is neither PEM contents nor a readable file: ${(err as Error).message}`,
    );
  }
}

/** Resolves the configured TLS block into the transport's carrier
 * config, reading whichever fields arrived as file paths. */
async function resolveTls(options: TlsOptions): Promise<TlsConfig> {
  const resolved: TlsConfig = {
    cert: await resolvePem("cert", options.cert),
    key: await resolvePem("key", options.key),
    requestCert: options.requestCert === true,
  };
  if (options.ca === undefined) {
    return resolved;
  }

  return { ...resolved, ca: await resolvePem("ca", options.ca) };
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

  /** The resolved carrier security every peer dials with, or undefined
   * on a plaintext system. */
  private readonly _tls: TlsConfig | undefined;

  /** The retained scratch buffer every outbound payload encodes into. */
  private readonly _writer: ByteWriter = new ByteWriter();

  /** The outbound side, one peer per remote node, dialed lazily and
   * reclaimed lazily: a peer left with no connection, nothing pending,
   * and no watch referencing its node is dropped on the sweep a lane
   * close already runs, and a later send recreates it. */
  private readonly _peers = new Map<string, Peer>();

  /** Handles minted for foreign senders, per path and incarnation, so
   * the same sender always resolves to the same handle instance. A
   * capped LRU in Map iteration order: a hit reinserts its entry, and
   * an insert past the cap evicts the least recently heard-from
   * unpinned entry, so actor churn on peer nodes and forged sender
   * paths meet a bound instead of growing the cache forever. */
  private readonly _senders = new Map<string, SenderEntry>();

  /** Whether the all-pinned overflow has been logged; once is enough,
   * since the condition persists until watches unwind. */
  private _senderOverflowLogged: boolean = false;

  /** Watch registrations that arrived over the wire, per delivering
   * session. The watching node treats that session's loss as the death
   * of everything it watched here, so the registrations are dead
   * weight the moment the session closes and are swept then. */
  private readonly _inboundWatches = new Map<Session, InboundWatch[]>();

  /** The watches this node registered on remote actors, keyed by
   * watcher and target path, settled by an inbound Terminated or by the
   * death of the connection that carried them. */
  private readonly _watches = new Map<string, RemoteWatch>();

  /** Whether the layer has been stopped: closing is terminal, and a
   * stopping system's actors must not dial fresh connections from their
   * teardown hooks. */
  private _closed: boolean = false;

  private constructor(
    system: ActorSystem,
    server: NetServer,
    local: Hello,
    tls: TlsConfig | undefined,
  ) {
    this._system = system;
    this._server = server;
    this._local = local;
    this._tls = tls;
  }

  /**
   * Binds the endpoint on the configured host and port and returns the
   * live remoting layer. A bind failure rejects, so a system whose
   * endpoint cannot open fails to start rather than starting deaf; so
   * does TLS material that cannot be resolved, since a node meant to
   * be encrypted must never come up plaintext.
   */
  static async start(system: ActorSystem, options: RemoteOptions): Promise<Remoting> {
    const tls: TlsConfig | undefined =
      options.tls === undefined ? undefined : await resolveTls(options.tls);
    const local: Hello = buildHello(system.name(), options);
    let seam: Remoting | null = null;
    // The casts below never observe the null: a handshake needs socket
    // round trips, which cannot complete before listen resolves and
    // the assignment underneath runs on that very resolution turn.
    const bind: { host: string; port: number; local: Hello } = {
      host: options.host,
      port: options.port,
      local,
    };
    const server: NetServer = await NetServer.listen(tls === undefined ? bind : { ...bind, tls }, {
      onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
        (seam as Remoting).onData(session, envelope, correlation);
      },
      onSessionClose: (session: Session): void => {
        (seam as Remoting).sweepInbound(session);
      },
      // A rejected connection is refused for the right reason (a peer
      // whose TLS handshake fails never becomes a session), but the
      // acceptor is the only side that sees why. Logging it turns a
      // silent refusal into an operator signal, which is exactly what
      // a mixed plaintext-versus-TLS misconfiguration needs.
      onError: (error: Error): void => {
        system.logger().warn("remote endpoint refused a connection", { error });
      },
    });

    // An ephemeral port resolves to the bound one, so every HELLO this
    // node dials out advertises an endpoint a peer can reach; the
    // server patches its own copy the same way for accepted sessions.
    seam = new Remoting(system, server, { ...local, port: server.address.port }, tls);
    return seam;
  }

  /** The port the endpoint is bound to, resolved from an ephemeral `0`
   * to the port the operating system chose. */
  get port(): number {
    return this._server.address.port;
  }

  /** The number of cached foreign-sender handles, pinned ones
   * included; introspection for the bounding tests. */
  get cachedSenders(): number {
    return this._senders.size;
  }

  /** The number of peer entries currently held; introspection for the
   * reclaim tests. */
  get peerCount(): number {
    return this._peers.size;
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
    await this._server.shutdown(SHUTDOWN_NOW);
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
    const answer: ControlActorRef | null = decodeValue(
      new ByteReader(reply.payload),
    ) as ControlActorRef | null;
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
      to: CONTROL_TARGET,
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
    const answer: ControlActorRef = decodeValue(new ByteReader(reply.payload)) as ControlActorRef;
    return routedPid(this._system, parsePath(answer.path, answer.uid), this.routeTo(host, port));
  }

  /** Returns the wire-backed route to the node at `host:port`; a PID
   * minted with it sends, asks, and watches over the transport. */
  private routeTo(host: string, port: number): IsolateRoute {
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
    this._watches.set(watchKey(watcher.path().toString(), target), {
      watcher,
      target,
      node: nodeKey(host, port),
    });
    this.peerFor(host, port).tell(this.watchEnvelope(KIND_WATCH, to, watcher));
  }

  /** Cancels a {@link watch} registration on both sides; unknown
   * registrations are a no-op. */
  private unwatch(host: string, port: number, to: Path, watcher: PID): void {
    if (this._closed) {
      return;
    }

    this._watches.delete(watchKey(watcher.path().toString(), to.toString()));
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
   * A lane's connection died. Watches ride the control lane, and
   * remote death is indistinguishable from connection loss, so a
   * control-lane loss settles every watch over that node: each watcher
   * receives one {@link Terminated} for its target. A stopping system
   * skips the settlement, since its watchers are shutting down with
   * it. Either way the close is the moment to reclaim the node's peer,
   * which the settlement may have just unreferenced.
   */
  private onLaneClose(node: string, lane: number): void {
    // Watches a far node registered here rode sessions of its own; any
    // lane closure is a chance to release the ones whose delivering
    // session has since closed, wherever the close callback could not
    // reach (sessions this node dialed report no per-session close).
    for (const tracked of [...this._inboundWatches.keys()]) {
      if (tracked.closed) {
        this.sweepInbound(tracked);
      }
    }

    if (lane === LANE_CONTROL && this._system.isRunning()) {
      for (const [key, entry] of this._watches) {
        if (entry.node !== node) {
          continue;
        }

        this._watches.delete(key);
        this._system.noSender().tell(entry.watcher, new Terminated(entry.target));
      }
    }

    this.reclaimPeer(node);
  }

  /**
   * Drops the node's peer entry once it holds nothing: no connection
   * on any lane, nothing pending on one, and no outbound watch left
   * referencing the node, so its loss could not even settle anything.
   * Reclaim keeps a long-lived node from accumulating one peer per
   * endpoint it ever contacted; a later send just recreates the peer,
   * so nothing on the wire changes.
   */
  private reclaimPeer(node: string): void {
    const peer: Peer | undefined = this._peers.get(node);
    if (peer === undefined || !peer.reclaimable) {
      return;
    }

    for (const entry of this._watches.values()) {
      if (entry.node === node) {
        return;
      }
    }

    this._peers.delete(node);
    peer.close();
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

    // A death notification leaving through a watcher handle is the
    // seam's only sight of a watched local actor stopping on its own:
    // the tree already dropped the registration, so the pin it held
    // releases here. Any other Terminated finds no matching pair and
    // releases nothing.
    if (message instanceof Terminated) {
      this.releaseSender(senderKey(to.toString(), to.uid()), message.actorPath);
    }

    let wire: WirePayload;
    try {
      wire = encodePayload(this._codec, this._writer, message);
    } catch (err) {
      const error: Error = err as Error;
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, error);
      return error;
    }

    this.peerFor(host, port).tell(this.dataEnvelope(KIND_TELL, to, wire, sender));
    return null;
  }

  /** Sends one ask over the wire; the transport owns the pending entry
   * and its timer, so settlement arrives as a promise. The receiving
   * node sees the remaining budget and re-derives its own deadline. A
   * non-positive or omitted timeout falls back to the system's
   * `askTimeout`, so a remote ask is never unbounded. The timer arms
   * once the lane is acquired, so a first ask on a cold lane can
   * additionally wait out the dial before its budget starts. */
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

    const deadline: number = timeout > 0 ? timeout : this._system.askTimeout();

    let wire: WirePayload;
    try {
      wire = encodePayload(this._codec, this._writer, message);
    } catch (err) {
      const error: Error = err as Error;
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, error);
      return Promise.reject(error);
    }

    const envelope: DataEnvelope = this.dataEnvelope(KIND_ASK, to, wire, sender);
    return this.peerFor(host, port)
      .ask(envelope, deadline)
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
   * bookkeeping, never delivered twice. Every request carries a
   * positive timeout by contract, so its transport entry is always
   * bounded by that deadline; a cancel settles the handle early, and
   * the entry then clears when the deadline or the reply arrives. */
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

    const opened: RequestHandle | Error = sender.openRequest(options);
    if (opened instanceof Error) {
      return completedRequest(opened);
    }

    let wire: WirePayload;
    try {
      wire = encodePayload(this._codec, this._writer, message);
    } catch (err) {
      const error: Error = err as Error;
      this._system.toDeadletter(sender.path().toString(), to.toString(), message, error);
      return completedRequest(error);
    }

    const handle: RequestHandle = opened;
    const rawTimeout: number = options?.timeout ?? 0;
    const timeout: number = rawTimeout > 0 ? rawTimeout : this._system.askTimeout();
    const envelope: DataEnvelope = this.dataEnvelope(KIND_ASK, to, wire, sender);

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
    if (envelope.to === CONTROL_TARGET) {
      this.handleControl(session, envelope, correlation);
      return;
    }

    if (envelope.kind === KIND_WATCH) {
      this.handleWatch(session, envelope);
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
      // The payload may alias the receive buffer, and the dead letter
      // retains it, so hand over a copy.
      this.undeliverable(session, envelope, correlation, envelope.payload.slice(), err as Error);
      return;
    }

    // A death notification is delivered only when it settles a watch
    // this node holds: the far node's notification travels on its own
    // dialed connection, so without the gate a lost unwatch or a
    // redelivered watch frame would notify an actor that unwatched,
    // and a connection sweep followed by the real stop would notify
    // the same watcher twice. A gated tell drops silently; a gated ask
    // is degenerate (no conforming node asks a death notification) and
    // is answered, never stranded until its timeout.
    if (message instanceof Terminated) {
      if (!this._watches.delete(watchKey(envelope.to, message.actorPath))) {
        if (correlation !== 0) {
          session.replyError(correlation, this.badRequest("a death notification settles no ask"));
        }

        return;
      }
    }

    const target: PID | undefined = this.targetOf(envelope);
    if (target === undefined) {
      this.undeliverable(session, envelope, correlation, message, ErrDead);
      return;
    }

    const sender: PID = this.senderOf(envelope);
    if (correlation === 0) {
      sender.tell(target, message);
      return;
    }

    const err: Error | null = target.deliverAsk(
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
  private handleWatch(session: Session, envelope: DataEnvelope): void {
    const watcher: PID = this.senderOf(envelope);
    if (watcher === this._system.noSender()) {
      return;
    }

    const target: PID | undefined = this.targetOf(envelope);
    if (target === undefined || !target.isRunning()) {
      this._system.noSender().tell(watcher, new Terminated(envelope.to));
      return;
    }

    // Only a registration the tree did not already hold pins: a
    // redelivered watch frame must not acquire a second pin its single
    // eventual removal could never release.
    if (target.addWatcher(watcher)) {
      this.pinSender(envelope, target.path().toString());
    }

    this.trackInbound(session, target, watcher, envelope);
  }

  /** Records one wire-registered watch under its delivering session. An
   * unwatch does not prune the record: the sweep's removal of an
   * already-removed watcher is a no-op, so the list may hold settled
   * entries rather than pay a scan per unwatch. */
  private trackInbound(session: Session, target: PID, watcher: PID, envelope: DataEnvelope): void {
    let list: InboundWatch[] | undefined = this._inboundWatches.get(session);
    if (list === undefined) {
      list = [];
      this._inboundWatches.set(session, list);
    }

    list.push({
      target,
      watcher,
      watcherKey: senderKey(envelope.sender, envelope.senderUid),
    });
  }

  /** Releases every watcher a closed session registered: the watching
   * node treats the same connection loss as the death of everything it
   * watched here, so the registrations are settled the moment the
   * session is gone. The removal report gates the pin release, so the
   * settled entries the list deliberately keeps release nothing. */
  private sweepInbound(session: Session): void {
    const list: InboundWatch[] | undefined = this._inboundWatches.get(session);
    if (list === undefined) {
      return;
    }

    this._inboundWatches.delete(session);
    for (const { target, watcher, watcherKey } of list) {
      if (target.removeWatcher(watcher)) {
        this.releaseSender(watcherKey, target.path().toString());
      }
    }
  }

  /** Removes the far watcher from the local actor; the sender cache
   * guarantees the same handle instance that was registered. */
  private handleUnwatch(envelope: DataEnvelope): void {
    const watcher: PID = this.senderOf(envelope);
    if (watcher === this._system.noSender()) {
      return;
    }

    const target: PID | undefined = this.targetOf(envelope);
    if (target === undefined || !target.isRunning()) {
      return;
    }

    if (target.removeWatcher(watcher)) {
      this.releaseSender(senderKey(envelope.sender, envelope.senderUid), target.path().toString());
    }
  }

  /** Settles one control request against this node. Control envelopes
   * carry an empty target path; a control tell is meaningless and is
   * dropped, and an unknown or malformed request answers a
   * request-scoped failure so a peer settles instead of timing out. */
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

    session.replyError(
      correlation,
      this.badRequest(`unknown control request "${envelope.typeRef}"`),
    );
  }

  /** The actor name a control request carries, or undefined for a
   * payload of the wrong shape, which the value codec can legitimately
   * decode from a malformed or hostile request. */
  private controlName(data: unknown): string | undefined {
    if (typeof data !== "object" || data === null) {
      return undefined;
    }

    const name: unknown = (data as { name?: unknown }).name;
    return typeof name === "string" ? name : undefined;
  }

  /** The spawn request a control payload carries, or undefined for a
   * payload of the wrong shape. */
  private spawnRequestOf(data: unknown): ControlSpawn | undefined {
    if (typeof data !== "object" || data === null) {
      return undefined;
    }

    const request = data as { name?: unknown; actor?: unknown; args?: unknown };
    if (typeof request.name !== "string" || typeof request.actor !== "string") {
      return undefined;
    }

    if (request.args !== undefined && !Array.isArray(request.args)) {
      return undefined;
    }

    return request as ControlSpawn;
  }

  /** Answers a lookup with where the named top-level actor lives, or
   * null when no running actor holds the name. */
  private handleLookup(session: Session, correlation: number, data: unknown): void {
    const name: string | undefined = this.controlName(data);
    if (name === undefined) {
      session.replyError(correlation, this.badRequest("malformed lookup request"));
      return;
    }

    const pid: PID | undefined = this._system.actorOf(name);
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
    const request: ControlSpawn | undefined = this.spawnRequestOf(data);
    if (request === undefined) {
      session.replyError(correlation, this.badRequest("malformed spawn request"));
      return;
    }

    const type: ActorClass | undefined = defaultActorRegistry.classOf(request.actor);
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
    const name: string | undefined = this.controlName(data);
    if (name === undefined) {
      session.replyError(correlation, this.badRequest("malformed respawn request"));
      return;
    }

    const pid: PID | undefined = this._system.actorOf(name);
    if (pid === undefined) {
      session.replyError(correlation, encodeFailure(new ActorNotFoundError(name)));
      return;
    }

    if (pid.isRouted()) {
      session.replyError(correlation, encodeFailure(this.placedRefusal(name, "respawned")));
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
    const name: string | undefined = this.controlName(data);
    if (name === undefined) {
      session.replyError(correlation, this.badRequest("malformed stop request"));
      return;
    }

    const pid: PID | undefined = this._system.actorOf(name);
    if (pid === undefined) {
      this.replyControl(session, correlation, null);
      return;
    }

    if (pid.isRouted()) {
      session.replyError(correlation, encodeFailure(this.placedRefusal(name, "stopped")));
      return;
    }

    pid.shutdown().then(
      (): void => {
        this.replyControl(session, correlation, null);
      },
      /* v8 ignore start -- a graceful stop of a running local actor
         does not reject; the arm exists so a future refusal still
         settles the ask instead of stranding it. */
      (err: Error): void => {
        session.replyError(correlation, encodeFailure(err));
      },
      /* v8 ignore stop */
    );
  }

  /** The honest refusal for lifecycle control of an actor placed on
   * another isolate of this node: reaching it through the pool is not
   * wired yet, and the placement is a same-machine detail the caller
   * could not know about. */
  private placedRefusal(name: string, verb: string): Error {
    return new Error(
      `actor "${name}" is placed on another isolate of its node and cannot be ${verb} remotely yet`,
    );
  }

  /** Builds the request-scoped failure body of a malformed or unknown
   * control request. */
  private badRequest(message: string): ErrorBody {
    return { code: ERROR_BAD_REQUEST, sentinel: 0, name: "", message };
  }

  /** Answers one control request with a plain value. */
  private replyControl(session: Session, correlation: number, value: unknown): void {
    this.sendReply(session, correlation, {
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

    this.sendReply(session, correlation, {
      serializerId: SERIALIZER_BINARY,
      typeRef: wire.typeRef,
      payload: wire.payload,
    });
  }

  /** Hands one reply to the session, falling back to a request-scoped
   * failure when the session refuses it (an oversize reply, a full
   * admission budget): error frames are admission-exempt, so the asker
   * settles with the real reason instead of waiting out its timeout. */
  private sendReply(session: Session, correlation: number, reply: ReplyEnvelope): void {
    const refused: Error | null = session.reply(correlation, reply);
    if (refused !== null) {
      session.replyError(correlation, encodeFailure(refused));
    }
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

    const pid: PID | undefined = this._system.resolvePath(path);
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

    // Cache first: a hit skips the path parse entirely, and a cached
    // handle is foreign by construction, since a sender of this very
    // node is never cached. The reinsert keeps the map in
    // least-recently-heard-from order, which is what eviction walks;
    // order is meaningless below the cap, so the steady state of a
    // conforming topology skips the touch and pays nothing for it.
    const key: string = senderKey(envelope.sender, envelope.senderUid);
    const cached: SenderEntry | undefined = this._senders.get(key);
    if (cached !== undefined) {
      if (this._senders.size >= SENDER_CACHE_SIZE) {
        this._senders.delete(key);
        this._senders.set(key, cached);
      }

      return cached.handle;
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

    const handle: PID = routedPid(this._system, path, this.routeTo(path.host(), path.port()));
    this._senders.set(key, { handle, pins: new Set<string>() });
    this.evictSenders(key);
    return handle;
  }

  /**
   * Holds the sender cache to its cap after an insert. The walk is in
   * least-recently-heard-from order and skips pinned entries and the
   * entry just inserted: evicting either would hand a later envelope a
   * different instance for a sender whose identity still matters, the
   * pinned one for its live watch, the fresh one for whatever the
   * envelope that minted it starts. A pass that finds nothing evictable
   * lets the cache exceed the cap, since every extra entry is behind a
   * live inbound watch, and says so once.
   */
  private evictSenders(inserted: string): void {
    if (this._senders.size <= SENDER_CACHE_SIZE) {
      return;
    }

    for (const [key, entry] of this._senders) {
      if (key === inserted || entry.pins.size > 0) {
        continue;
      }

      this._senders.delete(key);
      if (this._senders.size <= SENDER_CACHE_SIZE) {
        return;
      }
    }

    if (this._senderOverflowLogged) {
      return;
    }

    this._senderOverflowLogged = true;
    this._system.logger().warn("remote sender cache exceeded its cap: every entry is pinned", {
      cap: SENDER_CACHE_SIZE,
      size: this._senders.size,
    });
  }

  /** Pins the cached entry of the watcher a registered inbound watch
   * delivered, keyed by the watched target's path. A watcher of this
   * very node was never cached, so there is nothing to pin. */
  private pinSender(envelope: DataEnvelope, target: string): void {
    const entry: SenderEntry | undefined = this._senders.get(
      senderKey(envelope.sender, envelope.senderUid),
    );
    entry?.pins.add(target);
  }

  /** Releases the pin a watch registration held; exact by construction,
   * since every caller gates on the removal or delivery that settled
   * the registration, and the pair key cannot release twice. */
  private releaseSender(watcherKey: string, target: string): void {
    this._senders.get(watcherKey)?.pins.delete(target);
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

  /** Builds one outbound data envelope around an encoded payload. The
   * timeout field rides as zero: the session stamps an ask's remaining
   * budget at send time, and a tell carries none. */
  private dataEnvelope(kind: number, to: Path, wire: WirePayload, sender?: PID): DataEnvelope {
    return {
      kind,
      to: to.toString(),
      uid: to.uid(),
      sender: sender?.path().toString() ?? "",
      senderUid: sender?.path().uid() ?? "",
      timeout: 0,
      serializerId: SERIALIZER_BINARY,
      typeRef: wire.typeRef,
      payload: wire.payload,
    };
  }

  /** Returns the peer of the node at `host:port`, creating it on first
   * use; dialing stays lazy inside the peer itself, and every dial
   * carries the system's carrier security. */
  private peerFor(host: string, port: number): Peer {
    const key: string = nodeKey(host, port);
    let peer: Peer | undefined = this._peers.get(key);
    if (peer === undefined) {
      const tls: TlsConfig | undefined = this._tls;
      peer = new Peer(
        host,
        port,
        this._local,
        {
          onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
            this.onData(session, envelope, correlation);
          },
          onDeadLetter: (envelope: DataEnvelope, reason: Error): void => {
            this.deadLetterOut(envelope, reason);
          },
          onLaneClose: (lane: number): void => {
            this.onLaneClose(key, lane);
          },
        },
        tls === undefined ? {} : { tls },
      );
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
      const key: string = watchKey(envelope.sender, envelope.to);
      const entry: RemoteWatch | undefined = this._watches.get(key);
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
    return this._writer.bytes().slice();
  }
}
