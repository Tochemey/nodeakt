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

import { MessageChannel, type MessagePort, type ResourceLimits, Worker } from "node:worker_threads";
import { ActorRef } from "./actor.ref";
import type { ActorSystem } from "./actor.system";
import { Codec, decodeError, encodeError } from "./codec";
import { ControlPlane, mainWorkerId } from "./control.plane";
import { ActorNotFoundError, ErrActorAlreadyExists, ErrDead } from "./errors";
import { Mesh } from "./mesh";
import type { MessageRegistry } from "./message.registry";
import type { IsolateMetrics } from "./observability/metric.snapshot";
import { addressOf, newPathAt, parsePath } from "./path";
import {
  type ActorRecipe,
  CONTROL_CLAIMED,
  CONTROL_CONNECT,
  CONTROL_DISCONNECT,
  CONTROL_METRICS,
  CONTROL_NAME_ADDED,
  CONTROL_NAME_FREED,
  CONTROL_PLACED,
  CONTROL_RESTART,
  CONTROL_SPAWN,
  CONTROL_STOP,
  CONTROL_STOP_ACTOR,
  type ControlMessage,
  WORKER_ACTOR_STOPPED,
  WORKER_CLAIM,
  WORKER_CONTROLLED,
  WORKER_DEADLETTER,
  WORKER_METRICS,
  WORKER_PLACE,
  WORKER_READY,
  WORKER_SPAWN_FAILED,
  WORKER_SPAWNED,
  type WorkerBootData,
  type WorkerMessage,
} from "./protocol";
import { applySetup, spawnRecipe } from "./worker.runtime";

/**
 * Configuration of a {@link WorkerPool}.
 *
 * @internal
 */
export interface WorkerPoolOptions {
  /** Number of worker isolates; zero means every placement lands on
   * the main isolate. The system's placement is the one topology
   * authority: it sizes the pool to the system's capacity minus the
   * main isolate, so the isolates together match the machine. */
  readonly size: number;

  /** The worker entry script: the built JavaScript module that boots
   * the facade system and its runtime. */
  readonly entry: string | URL;

  /** Silences the worker facades' loggers. */
  readonly quiet?: boolean;

  /** Module specifier of the message-registration setup: its default
   * export receives a `MessageRegistry` and registers every message
   * class that crosses isolates. The pool applies it to its own
   * registry at start and every worker applies it at boot, so all
   * isolates share the same registrations; omitting it means only
   * passthrough data crosses the boundary. */
  readonly setup?: string;

  /** Per-worker V8 heap and stack limits, passed to each worker
   * thread. An isolate that exhausts its limits dies alone and the
   * pool runs its death sequence, instead of one runaway actor taking
   * the whole process down with it. */
  readonly resourceLimits?: ResourceLimits;

  /** How long a stop waits for a worker's graceful exit before
   * terminating it, in milliseconds; defaults to 5000. */
  readonly stopTimeout?: number;

  /** Called with a top-level name each time it is freed pool-wide, so a
   * layer above the pool can mirror the release: the cluster placement
   * deletes the freed actor's registry record through it. Fires for an
   * explicit release, a placed actor stopping, and a failed placement. */
  readonly onRelease?: (name: string) => void;
}

/** A spawn reply's payload: where the placed actor lives. */
interface SpawnedAt {
  readonly path: string;
  readonly uid: string;
}

/** One spawn command awaiting its reply. */
interface PendingSpawn {
  readonly workerId: number;
  resolve(spawned: SpawnedAt): void;
  reject(error: Error): void;
}

/** One worker awaiting its ready announcement. */
interface ReadyWaiter {
  resolve(): void;
  reject(error: Error): void;
}

/** Resolves true when the promise settles within the window, false on
 * timeout; the timer never keeps the process alive. */
function settledWithin(exited: Promise<unknown>, ms: number): Promise<boolean> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), ms);
    timer.unref();
    void exited.then(() => {
      clearTimeout(timer);
      resolve(true);
    });
  });
}

/**
 * WorkerPool is the main isolate's side of the worker dispatchers: it
 * starts N `worker_threads`, wires the full port mesh (main included,
 * as worker id zero), places recipes through the {@link ControlPlane},
 * and handles worker death.
 *
 * Placement: {@link place} asks the control plane for the owning
 * worker (the least-occupied isolate), claims the top-level name, and
 * spawns the recipe there; on the main isolate (an empty or dead pool)
 * the recipe spawns locally through the same resolution, so behavior
 * does not depend on where a recipe lands. The returned {@link ActorRef} carries path and incarnation,
 * with the owning worker's mesh route when the actor lives on another
 * isolate. A placed actor's stop, however it comes about, frees its
 * top-level name again.
 *
 * Worker facades forward their dead-letter events here, and the pool
 * republishes them on the main system: subscribing to the system the
 * user started observes the one logical system, hops that failed on a
 * worker included.
 *
 * Worker death runs the full cleanup sequence: the control plane
 * frees the dead worker's names and drops it from placement, the main
 * mesh disconnects it (settling pending asks and requests with
 * {@link ErrDead} immediately), spawn commands awaiting it reject, and
 * every surviving worker is told to disconnect it too.
 *
 * Application messages never route through the pool or the control
 * port; they ride the mesh.
 *
 * @internal
 */
export class WorkerPool {
  private readonly _system: ActorSystem;
  private readonly _options: WorkerPoolOptions;
  private readonly _registry: MessageRegistry;
  private readonly _codec: Codec;
  private readonly _mesh: Mesh;
  private readonly _plane: ControlPlane;
  private readonly _workers = new Map<number, Worker>();
  private readonly _exits = new Map<number, Promise<number>>();
  private readonly _faults = new Map<number, Error>();
  private readonly _ready = new Map<number, ReadyWaiter>();
  private readonly _pending = new Map<number, PendingSpawn>();

  /** Lifecycle orders (restart, stop-actor) awaiting a worker's
   * `controlled` reply, by sequence. */
  private readonly _controls = new Map<
    number,
    { workerId: number; resolve: () => void; reject: (error: Error) => void }
  >();

  /** Metrics requests awaiting a worker's `metrics-reply`, by sequence; a
   * worker that dies mid-collection resolves to null and drops out. */
  private readonly _metricsPending = new Map<
    number,
    { workerId: number; resolve: (metrics: IsolateMetrics | null) => void }
  >();

  /** Names whose placement is in flight on this isolate, so the claim
   * an instance spawn makes for the same placement is a no-op instead
   * of a duplicate registration. */
  private readonly _placing = new Set<string>();

  private _nextSeq = 1;
  private _started = false;
  private _stopping = false;

  constructor(system: ActorSystem, registry: MessageRegistry, options: WorkerPoolOptions) {
    this._system = system;
    this._options = options;
    this._registry = registry;
    this._codec = new Codec(registry);
    this._mesh = new Mesh(system, registry, mainWorkerId);
    this._plane = new ControlPlane(Array.from({ length: options.size }, (_, i) => i + 1));
  }

  /** Returns the main isolate's mesh. */
  mesh(): Mesh {
    return this._mesh;
  }

  /** Returns the live pool worker ids. */
  workers(): number[] {
    return this._plane.workers();
  }

  /** Returns the worker id owning a placed or claimed top-level name,
   * or undefined when no isolate holds it. */
  ownerOf(name: string): number | undefined {
    return this._plane.resolve(name);
  }

  /**
   * Claims a top-level name for an actor spawned directly on the given
   * isolate (the main isolate by default), making it unique across the
   * pool; the claim replicates to every worker. A name whose placement
   * this pool is itself performing is a no-op claim, because the
   * placement already owns the registration.
   *
   * @returns `null`, or the `ErrActorAlreadyExists` sentinel.
   */
  claim(name: string, workerId: number = mainWorkerId): Error | null {
    if (this._placing.has(name)) {
      return null;
    }

    const refused = this._plane.register(name, workerId);
    if (refused === null) {
      this.broadcast({ kind: CONTROL_NAME_ADDED, name, workerId });
    }

    return refused;
  }

  /** Releases a claimed or placed top-level name everywhere; unknown
   * names are a no-op. Notifies {@link WorkerPoolOptions.onRelease} last,
   * so a layer above the pool sees every release the pool performs. */
  release(name: string): void {
    this._plane.free(name);
    this.broadcast({ kind: CONTROL_NAME_FREED, name });
    this._options.onRelease?.(name);
  }

  /** Posts one control message to every live worker. */
  private broadcast(message: ControlMessage): void {
    for (const id of this._workers.keys()) {
      this.send(id, message);
    }
  }

  /**
   * Starts the pool: boots every worker, waits for each to announce
   * ready, and wires the full mesh, main isolate included.
   *
   * @throws A `TypeError` when the pool was already started. Rejects
   * with the boot failure when any worker dies before announcing
   * ready; call {@link stop} to clean up the survivors.
   */
  async start(): Promise<void> {
    if (this._started) {
      throw new TypeError("worker pool already started");
    }

    this._started = true;
    const setup = this._options.setup;
    if (setup !== undefined) {
      await applySetup(this._registry, setup);
    }

    const ids = this._plane.workers();
    await Promise.all(ids.map((id) => this.launch(id)));
    this.wire(ids);
  }

  /**
   * Places a recipe: claims the top-level name, spawns the actor on
   * the worker chosen by the control plane (or locally when placement
   * falls back to the main isolate), and returns its handle.
   *
   * Rejects with `ErrActorAlreadyExists` when the name is taken
   * anywhere in the pool or already lives as a top-level actor on the
   * main system (a name the pool did not place is still not free),
   * with {@link ErrDead} when the pool is not running or the owning
   * worker dies mid-spawn, and with the spawn failure otherwise; a
   * failed placement frees the name.
   */
  async place(name: string, recipe: ActorRecipe): Promise<ActorRef> {
    if (!this._started || this._stopping) {
      return Promise.reject(ErrDead);
    }

    if (this._system.actorOf(name) !== undefined) {
      return Promise.reject(ErrActorAlreadyExists);
    }

    const workerId = this._plane.place();
    const claimed = this._plane.register(name, workerId);
    if (claimed !== null) {
      return Promise.reject(claimed);
    }

    this._placing.add(name);
    try {
      if (workerId === mainWorkerId) {
        const pid = await spawnRecipe(this._system, name, recipe);
        pid.onStopped(() => {
          this.release(name);
        });
        this.broadcast({ kind: CONTROL_NAME_ADDED, name, workerId: mainWorkerId });
        return new ActorRef(this._system, pid.path(), this._mesh.route(mainWorkerId));
      }

      const spawned = await this.spawnOn(workerId, name, recipe);
      this.broadcast({ kind: CONTROL_NAME_ADDED, name, workerId });
      return new ActorRef(
        this._system,
        parsePath(spawned.path, spawned.uid),
        this._mesh.route(workerId),
      );
    } catch (err) {
      this.release(name);
      throw err;
    } finally {
      this._placing.delete(name);
    }
  }

  /**
   * Resolves a placed top-level name to an address-only handle: the
   * ref carries no incarnation and addresses whatever lives under the
   * name when a message is sent. Returns undefined for a name the
   * control plane does not know.
   */
  lookup(name: string): ActorRef | undefined {
    const workerId = this._plane.resolve(name);
    if (workerId === undefined) {
      return undefined;
    }

    const address = addressOf(this._system.noSender().path());
    return new ActorRef(
      this._system,
      newPathAt(name, address, undefined, ""),
      this._mesh.route(workerId),
    );
  }

  /**
   * Stops the pool: every worker is asked to stop gracefully and
   * terminated after the configured window if it has not exited; the
   * main mesh closes last. Stopping again is a no-op.
   */
  async stop(): Promise<void> {
    if (this._stopping) {
      return;
    }

    this._stopping = true;
    const ids = [...this._workers.keys()];
    await Promise.all(ids.map((id) => this.stopWorker(id)));
    this._mesh.close();
  }

  /** Boots one worker and resolves when it announces ready; rejects
   * when it dies first. */
  private launch(workerId: number): Promise<void> {
    const boot: WorkerBootData = {
      workerId,
      systemName: this._system.name(),
      host: this._system.host(),
      port: this._system.port(),
      quiet: this._options.quiet ?? false,
      setup: this._options.setup ?? null,
      metrics: this._system.metricsConfig(),
    };
    const worker = new Worker(this._options.entry, {
      workerData: boot,
      resourceLimits: this._options.resourceLimits,
    });
    this._workers.set(workerId, worker);
    this._exits.set(workerId, new Promise<number>((resolve) => worker.once("exit", resolve)));
    worker.on("message", (message: unknown) => {
      // No control reply may take the control plane down: a malformed
      // or forged frame is logged and dropped.
      try {
        this.onMessage(workerId, message as WorkerMessage);
      } catch (err) {
        this._system.logger().error("control reply failed", { error: err as Error });
      }
    });
    worker.on("error", (err: Error) => {
      this._faults.set(workerId, err);
    });
    worker.on("exit", () => {
      this.onExit(workerId);
    });

    return new Promise<void>((resolve, reject) => {
      this._ready.set(workerId, { resolve, reject });
    });
  }

  /** Wires the full mesh: one channel between the main isolate and
   * each worker, one between every worker pair. */
  private wire(ids: readonly number[]): void {
    for (const id of ids) {
      const channel = new MessageChannel();
      this._mesh.connect(id, channel.port1);
      this.send(id, { kind: CONTROL_CONNECT, workerId: mainWorkerId, port: channel.port2 }, [
        channel.port2,
      ]);
    }

    for (let i = 0; i < ids.length; i++) {
      for (let j = i + 1; j < ids.length; j++) {
        const channel = new MessageChannel();
        this.send(
          ids[i] as number,
          { kind: CONTROL_CONNECT, workerId: ids[j] as number, port: channel.port1 },
          [channel.port1],
        );
        this.send(
          ids[j] as number,
          { kind: CONTROL_CONNECT, workerId: ids[i] as number, port: channel.port2 },
          [channel.port2],
        );
      }
    }
  }

  /** Sends one spawn command and returns its correlated reply. */
  private spawnOn(workerId: number, name: string, recipe: ActorRecipe): Promise<SpawnedAt> {
    const seq = this._nextSeq++;
    const reply = new Promise<SpawnedAt>((resolve, reject) => {
      this._pending.set(seq, { workerId, resolve, reject });
    });
    this.send(workerId, { kind: CONTROL_SPAWN, seq, name, recipe });
    return reply;
  }

  /** Sends one lifecycle order and settles on the worker's reply; the
   * owning worker's death rejects it with {@link ErrDead}. */
  private controlOn(
    workerId: number,
    order:
      | { kind: typeof CONTROL_RESTART; seq: number; name: string }
      | { kind: typeof CONTROL_STOP_ACTOR; seq: number; name: string },
  ): Promise<void> {
    const reply: Promise<void> = new Promise<void>((resolve, reject) => {
      this._controls.set(order.seq, { workerId, resolve, reject });
    });
    this.send(workerId, order);
    return reply;
  }

  /**
   * Gathers every live worker's raw metrics for a machine-wide snapshot.
   * Each worker answers with its own isolate's numbers; a worker that dies
   * before answering contributes null and is dropped from the merge, so a
   * collection never hangs on a departed isolate.
   */
  collectMetrics(): Promise<(IsolateMetrics | null)[]> {
    return Promise.all([...this._workers.keys()].map((id) => this.metricsFrom(id)));
  }

  /** Requests one worker's metrics and settles on its reply. */
  private metricsFrom(workerId: number): Promise<IsolateMetrics | null> {
    const seq = this._nextSeq++;
    const reply = new Promise<IsolateMetrics | null>((resolve) => {
      this._metricsPending.set(seq, { workerId, resolve });
    });
    this.send(workerId, { kind: CONTROL_METRICS, seq });
    return reply;
  }

  /**
   * Restarts the placed top-level actor in place on its owning worker:
   * same PID, same incarnation, fresh state through its lifecycle
   * hooks. Rejects with {@link ActorNotFoundError} for a name no
   * worker owns (an actor of the main isolate resolves on the tree and
   * never reaches here), and with the restart failure otherwise.
   */
  restartPlaced(name: string): Promise<void> {
    const workerId: number | undefined = this.ownerOf(name);
    if (workerId === undefined || workerId === mainWorkerId || !this._workers.has(workerId)) {
      return Promise.reject(new ActorNotFoundError(name));
    }

    return this.controlOn(workerId, { kind: CONTROL_RESTART, seq: this._nextSeq++, name });
  }

  /**
   * Stops the placed top-level actor gracefully on its owning worker.
   * A name no worker owns is already stopped, so the order succeeds
   * idempotently; the stop frees the name through the worker's own
   * announcement.
   */
  stopPlaced(name: string): Promise<void> {
    const workerId: number | undefined = this.ownerOf(name);
    if (workerId === undefined || workerId === mainWorkerId || !this._workers.has(workerId)) {
      return Promise.resolve();
    }

    return this.controlOn(workerId, { kind: CONTROL_STOP_ACTOR, seq: this._nextSeq++, name });
  }

  /** Routes one control reply from a worker. Late and forged frames
   * are no-ops: a reply for a spawn that exit handling already
   * rejected, a reply naming another worker's sequence, or a second
   * ready announcement must not disturb the control plane. "stopped"
   * needs no action because stop awaits the exit itself. */
  private onMessage(workerId: number, message: WorkerMessage): void {
    if (message.kind === WORKER_READY) {
      const waiter = this._ready.get(workerId);
      if (waiter === undefined) {
        return;
      }

      this._ready.delete(workerId);
      waiter.resolve();
      return;
    }

    if (message.kind === WORKER_ACTOR_STOPPED) {
      this.release(message.name);
      return;
    }

    if (message.kind === WORKER_CLAIM) {
      const refused = this.claim(message.name, workerId);
      this.send(workerId, {
        kind: CONTROL_CLAIMED,
        seq: message.seq,
        error: refused === null ? null : encodeError(refused),
      });
      return;
    }

    if (message.kind === WORKER_PLACE) {
      void this.placeFor(workerId, message.seq, message.name, message.recipe);
      return;
    }

    if (message.kind === WORKER_DEADLETTER) {
      this.republish(message);
      return;
    }

    if (message.kind === WORKER_METRICS) {
      const pending = this._metricsPending.get(message.seq);
      if (pending === undefined || pending.workerId !== workerId) {
        return;
      }

      this._metricsPending.delete(message.seq);
      pending.resolve(message.metrics);
      return;
    }

    if (message.kind === WORKER_CONTROLLED) {
      const pending:
        | { workerId: number; resolve: () => void; reject: (error: Error) => void }
        | undefined = this._controls.get(message.seq);
      if (pending === undefined || pending.workerId !== workerId) {
        return;
      }

      this._controls.delete(message.seq);
      if (message.error === null) {
        pending.resolve();
        return;
      }

      pending.reject(decodeError(message.error));
      return;
    }

    if (message.kind === WORKER_SPAWNED || message.kind === WORKER_SPAWN_FAILED) {
      const pending = this._pending.get(message.seq);
      if (pending === undefined || pending.workerId !== workerId) {
        return;
      }

      if (message.kind === WORKER_SPAWNED) {
        this._pending.delete(message.seq);
        pending.resolve({ path: message.path, uid: message.uid });
        return;
      }

      // Decode before the entry leaves the table: a malformed error
      // frame then leaves the entry to exit handling instead of
      // stranding the spawn.
      const error = decodeError(message.error);
      this._pending.delete(message.seq);
      pending.reject(error);
    }
  }

  /** Publishes a worker facade's forwarded dead letter on the main
   * system, where the one logical event stream lives; a payload this
   * side cannot restore is published as its raw wire data. */
  private republish(
    forwarded: Extract<WorkerMessage, { readonly kind: typeof WORKER_DEADLETTER }>,
  ): void {
    let message: unknown;
    try {
      message = this._codec.decodeMessage(forwarded.message);
    } catch {
      message = forwarded.message.data;
    }

    this._system.toDeadletter(
      forwarded.sender,
      forwarded.receiver,
      message,
      new Error(forwarded.reason),
    );
  }

  /** Answers a worker facade's placement request with where the actor
   * landed, or with the failure in wire form. */
  private async placeFor(
    workerId: number,
    seq: number,
    name: string,
    recipe: ActorRecipe,
  ): Promise<void> {
    try {
      const ref = await this.place(name, recipe);
      this.send(workerId, {
        kind: CONTROL_PLACED,
        seq,
        error: null,
        workerId: ref.workerId() ?? mainWorkerId,
        path: ref.path().toString(),
        uid: ref.path().uid(),
      });
    } catch (err) {
      this.send(workerId, {
        kind: CONTROL_PLACED,
        seq,
        error: encodeError(err as Error),
        workerId: mainWorkerId,
        path: "",
        uid: "",
      });
    }
  }

  /** Runs the death sequence of one worker: names freed, mesh
   * disconnected everywhere, waiters and pending spawns rejected. */
  private onExit(workerId: number): void {
    this._workers.delete(workerId);
    const freed = this._plane.evict(workerId);
    this._mesh.disconnect(workerId);

    const waiter = this._ready.get(workerId);
    if (waiter !== undefined) {
      this._ready.delete(workerId);
      waiter.reject(this._faults.get(workerId) ?? ErrDead);
    }

    for (const [seq, pending] of this._pending) {
      if (pending.workerId === workerId) {
        this._pending.delete(seq);
        pending.reject(ErrDead);
      }
    }

    for (const [seq, pending] of this._controls) {
      if (pending.workerId === workerId) {
        this._controls.delete(seq);
        pending.reject(ErrDead);
      }
    }

    /* v8 ignore start -- a worker answers a metrics request synchronously,
       so a death in the window between the request and its reply cannot be
       forced by a test; the arm drops the worker from the in-flight
       collection instead of stranding it. */
    for (const [seq, pending] of this._metricsPending) {
      if (pending.workerId === workerId) {
        this._metricsPending.delete(seq);
        pending.resolve(null);
      }
    }
    /* v8 ignore stop */

    if (!this._stopping) {
      for (const id of this._workers.keys()) {
        this.send(id, { kind: CONTROL_DISCONNECT, workerId });
      }

      for (const name of freed) {
        this.broadcast({ kind: CONTROL_NAME_FREED, name });
        // A crashed isolate frees its names as an explicit release does, so the
        // layer above the pool learns of it too and clears the freed name's
        // registry record instead of leaving it pointing at a gone actor.
        this._options.onRelease?.(name);
      }
    }
  }

  /** Asks one worker to stop and terminates it when the graceful
   * window elapses. */
  private async stopWorker(workerId: number): Promise<void> {
    const worker = this._workers.get(workerId);
    const exited = this._exits.get(workerId);
    if (worker === undefined || exited === undefined) {
      return;
    }

    this.send(workerId, { kind: CONTROL_STOP });
    const graceful = await settledWithin(exited, this._options.stopTimeout ?? 5000);
    if (!graceful) {
      await worker.terminate();
    }
  }

  /** Posts one control message to a worker; a worker that already
   * exited is a no-op, matching the port's own behavior for a worker
   * that exits between the lookup and the post. */
  private send(workerId: number, message: ControlMessage, transfer?: MessagePort[]): void {
    const worker = this._workers.get(workerId);
    if (worker === undefined) {
      return;
    }

    if (transfer === undefined) {
      worker.postMessage(message);
      return;
    }

    worker.postMessage(message, transfer);
  }
}
