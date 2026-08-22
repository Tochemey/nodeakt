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
import type { IsolateRoute } from "../actor/actor.ref";
import type { ActorSystem } from "../actor/actor.system";
import type { Path } from "../actor/path";
import type { PID } from "../actor/pid";
import { completedRequest, type RequestCall, type RequestOptions } from "../actor/reentrancy";
import { ErrDead } from "../errors/errors";
import type { MessageRegistry } from "./message.registry";
import { PortTransport } from "./port.transport";

/**
 * Mesh is one isolate's view of the other isolates: a worker-id-keyed
 * map of {@link PortTransport}s, one per connected isolate. Sends name
 * the worker that owns the target actor; the mesh routes on that
 * isolate's transport and everything else (codec, correlation, dead
 * letters) happens inside it.
 *
 * A send naming a worker that is not connected, because it never was
 * or because it died and was {@link disconnect}ed, fails with
 * {@link ErrDead} and becomes a dead letter on this isolate's system:
 * envelopes addressed to a dead worker never leave the sender.
 *
 * Disconnecting an isolate closes its transport, which settles every
 * pending ask and request awaiting that worker with {@link ErrDead}
 * immediately instead of letting them ride out their timeouts. That is
 * the per-isolate half of worker-exit handling; freeing the dead
 * worker's names is the control plane's half.
 *
 * @internal
 */
export class Mesh {
  private readonly _system: ActorSystem;
  private readonly _registry: MessageRegistry;
  private readonly _selfId: number;
  private readonly _isolates = new Map<number, PortTransport>();

  constructor(system: ActorSystem, registry: MessageRegistry, selfId: number) {
    this._system = system;
    this._registry = registry;
    this._selfId = selfId;
  }

  /**
   * Connects an isolate: envelopes to and from `workerId` travel on
   * the given port from now on.
   *
   * @throws A `TypeError` when the worker id is already connected;
   * re-wiring a live isolate is a bug, disconnect it first.
   */
  connect(workerId: number, port: MessagePort): void {
    if (this._isolates.has(workerId)) {
      throw new TypeError(`worker ${workerId} is already connected`);
    }

    this._isolates.set(
      workerId,
      new PortTransport(this._system, this._registry, port, this._selfId, workerId),
    );
  }

  /**
   * Disconnects an isolate, closing its transport: pending asks and
   * requests awaiting that worker settle with {@link ErrDead}
   * immediately, and further sends naming it become dead letters.
   * Unknown ids are a no-op, so exit handling need not know whether
   * the wiring ever completed.
   */
  disconnect(workerId: number): void {
    const transport = this._isolates.get(workerId);
    if (transport === undefined) {
      return;
    }

    this._isolates.delete(workerId);
    transport.close();
  }

  /** Returns the ids of the currently connected isolates. */
  isolates(): number[] {
    return [...this._isolates.keys()];
  }

  /**
   * Returns the route an `ActorRef` uses to reach an actor owned by
   * the given worker, or null when the worker is this mesh's own
   * isolate: locality is the same-worker-id check, so a ref built on
   * any isolate short-circuits to its local tree exactly when the
   * actor lives there. The route is bound to the worker id, not to the
   * transport, so it survives a disconnect and reports {@link ErrDead}
   * through the ordinary send contract afterwards.
   */
  route(workerId: number): IsolateRoute | null {
    if (workerId === this._selfId) {
      return null;
    }

    return {
      workerId,
      tell: (to, message, sender) => this.tell(workerId, to, message, sender),
      ask: (to, message, timeout, sender) => this.ask(workerId, to, message, timeout, sender),
      request: (to, message, sender, options) =>
        this.request(workerId, to, message, sender, options),
      watch: (to, watcher) => this.watch(workerId, to, watcher),
      unwatch: (to, watcher) => this.unwatch(workerId, to, watcher),
    };
  }

  /**
   * Registers `watcher` for a {@link Terminated} when the actor at
   * `to` on the given worker stops, with the contract of
   * `PortTransport.watch`. A worker that is not connected is a no-op,
   * exactly as watching a stopped local actor is.
   */
  watch(workerId: number, to: Path, watcher: PID): void {
    this._isolates.get(workerId)?.watch(to, watcher);
  }

  /** Cancels a {@link watch} registration; unknown workers and
   * registrations are a no-op. */
  unwatch(workerId: number, to: Path, watcher: PID): void {
    this._isolates.get(workerId)?.unwatch(to, watcher);
  }

  /**
   * Sends a message to an actor owned by the given worker, fire and
   * forget, with the contract of `PortTransport.tell`. A worker that
   * is not connected fails with {@link ErrDead} and a dead letter.
   */
  tell(workerId: number, to: Path, message: unknown, sender?: PID): Error | null {
    const transport = this._isolates.get(workerId);
    if (transport === undefined) {
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, ErrDead);
      return ErrDead;
    }

    return transport.tell(to, message, sender);
  }

  /**
   * Sends a message to an actor owned by the given worker and waits
   * for its response, with the contract of `PortTransport.ask`. A
   * worker that is not connected rejects with {@link ErrDead} and a
   * dead letter.
   */
  ask(
    workerId: number,
    to: Path,
    message: unknown,
    timeout: number,
    sender?: PID,
  ): Promise<unknown> {
    const transport = this._isolates.get(workerId);
    if (transport === undefined) {
      this._system.toDeadletter(sender?.path().toString(), to.toString(), message, ErrDead);
      return Promise.reject(ErrDead);
    }

    return transport.ask(to, message, timeout, sender);
  }

  /**
   * Issues a non-parking request to an actor owned by the given worker
   * on behalf of `sender`, with the contract of
   * `PortTransport.request`. A worker that is not connected completes
   * the handle with {@link ErrDead} and a dead letter.
   */
  request(
    workerId: number,
    to: Path,
    message: unknown,
    sender: PID,
    options?: RequestOptions,
  ): RequestCall {
    const transport = this._isolates.get(workerId);
    if (transport === undefined) {
      this._system.toDeadletter(sender.path().toString(), to.toString(), message, ErrDead);
      return completedRequest(ErrDead);
    }

    return transport.request(to, message, sender, options);
  }

  /** Disconnects every isolate; the mesh is empty afterwards and
   * every pending entry has settled with {@link ErrDead}. */
  close(): void {
    const transports = [...this._isolates.values()];
    this._isolates.clear();
    for (const transport of transports) {
      transport.close();
    }
  }
}
