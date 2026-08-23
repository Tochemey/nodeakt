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
import type { Actor } from "./actor";
import type { IsolateRoute } from "./actor.ref";
import type { ActorSystem } from "./actor.system";
import { Codec, decodeError, encodeError } from "./codec";
import type { WireMessage } from "./envelope";
import { ErrDead } from "./errors";
import { Mesh } from "./mesh";
import type { MessageRegistry } from "./message.registry";
import { Deadletter } from "./messages";
import { addressOf, newPathAt, parsePath } from "./path";
import type { PID } from "./pid";
import type { Props } from "./props";
import type { ActorRecipe, ControlMessage, WorkerMessage } from "./protocol";
import { placedRecipe } from "./registration";
import { routedPid } from "./routed.pid";
import type { SpawnOptions } from "./spawn.options";

/**
 * Imports a message-registration setup module and applies it to the
 * given registry: the module's default export receives the registry and
 * registers every message class the isolate sends or receives. Running
 * the same setup on every isolate is what keeps type ids and classes
 * aligned across the pool.
 *
 * @throws A `TypeError` when the module's default export is not a
 * function; import and registration failures propagate as their own
 * errors.
 *
 * @internal
 */
export async function applySetup(registry: MessageRegistry, specifier: string): Promise<void> {
  const mod = (await import(specifier)) as { default?: unknown };
  if (typeof mod.default !== "function") {
    throw new TypeError(
      `setup module "${specifier}" does not default-export a registration function`,
    );
  }

  (mod.default as (registry: MessageRegistry) => void)(registry);
}

/** Matches a URL scheme of two or more characters, so a Windows drive
 * letter is never mistaken for one. */
const schemePattern = /^([a-z][a-z0-9+.-]+):/i;

/**
 * Resolves an {@link ActorRecipe} to a running actor: imports the
 * module, resolves the exported class, constructs the instance with
 * the recipe's arguments, and spawns it as a top-level actor of the
 * given system. Shared by worker isolates and the main-isolate
 * placement fallback, so a recipe behaves identically wherever it
 * lands.
 *
 * A recipe is code loading, so what it may load is fenced twice, both
 * automatically: the module must import through a `file:` URL or a plain
 * path, never a `node:`, `data:`, or network scheme, and the resolved
 * export must actually look like an actor class before it is
 * constructed. The application never configures this. The module a
 * recipe names is always one a `registerActor` call recorded, since a
 * recipe is only ever built by resolving a registered class, so
 * placement imports exactly the modules the application registered
 * actors from and nothing else.
 *
 * @throws A `TypeError` when the module specifier carries a scheme
 * other than `file:`, or when the module does not export an actor
 * class under the recipe's actor name; module loading and spawning
 * failures propagate as their own errors.
 *
 * @internal
 */
export async function spawnRecipe(
  system: ActorSystem,
  name: string,
  recipe: ActorRecipe,
): Promise<PID> {
  const scheme = schemePattern.exec(recipe.module);
  if (scheme !== null && (scheme[1] as string).toLowerCase() !== "file") {
    throw new TypeError(
      `recipe module "${recipe.module}" must be a file URL or a plain path, not a "${scheme[1]}:" specifier`,
    );
  }

  const mod = (await import(recipe.module)) as Record<string, unknown>;
  // Exact export key first; an aliased export still resolves, because
  // aliasing changes the key but never the class's own name.
  let type = mod[recipe.actor];
  if (typeof type !== "function") {
    type = Object.values(mod).find(
      (value) => typeof value === "function" && value.name === recipe.actor,
    );
  }

  const receive = (type as { prototype?: { receive?: unknown } } | undefined)?.prototype?.receive;
  if (typeof type !== "function" || typeof receive !== "function") {
    throw new TypeError(
      `module "${recipe.module}" does not export an actor class named "${recipe.actor}"`,
    );
  }

  const instance = new (type as new (...args: unknown[]) => Actor)(...(recipe.args ?? []));
  const reentrancy = recipe.reentrancy;
  return system.spawn(name, instance, reentrancy === undefined ? undefined : { reentrancy });
}

/**
 * WorkerRuntime is the worker-side half of the control plane: it owns
 * the isolate's mesh and executes the control messages arriving on the
 * parent port. Connect and disconnect wire the mesh, spawn resolves a
 * recipe and answers with the actor's path and incarnation (or the
 * failure in wire form), and stop tears the isolate down: mesh closed,
 * facade system stopped, `stopped` posted, control port closed, after
 * which the worker exits on its own.
 *
 * The class is port-agnostic on purpose: production hands it the
 * worker's parent port, tests hand it one end of a `MessageChannel`
 * and drive the whole protocol in-process.
 *
 * @internal
 */
export class WorkerRuntime {
  private readonly _system: ActorSystem;
  private readonly _mesh: Mesh;
  private readonly _codec: Codec;
  private readonly _control: MessagePort;
  private readonly _workerId: number;

  /** The control plane's name table, replicated here through
   * `name-added` and `name-freed`; what lets the facade's `actorOf`
   * answer synchronously. */
  private readonly _names = new Map<string, number>();

  /** Claim requests awaiting the control plane's answer, by sequence. */
  private readonly _claims = new Map<number, (error: Error | null) => void>();

  /** Placement requests awaiting the control plane's answer. */
  private readonly _placements = new Map<
    number,
    { resolve: (pid: PID) => void; reject: (error: Error) => void }
  >();

  /** Names this runtime is spawning on the control plane's own order,
   * so the facade's claim for them is a no-op instead of a duplicate
   * registration. */
  private readonly _placing = new Set<string>();

  private _nextSeq = 1;

  constructor(
    system: ActorSystem,
    registry: MessageRegistry,
    control: MessagePort,
    workerId: number,
  ) {
    this._system = system;
    this._mesh = new Mesh(system, registry, workerId);
    this._codec = new Codec(registry);
    this._control = control;
    this._workerId = workerId;

    // The facade takes part in the pool through the control plane:
    // instance spawns claim their top-level names, Props spawns place
    // through it, and lookups consult the replicated name table.
    system.attachPlacement({
      claim: (name) => this.claimName(name),
      free: (name) => {
        this.post({ kind: "actor-stopped", name });
      },
      place: (name, props, options) => this.placeProps(name, props, options),
      find: (name) => this.findName(name),
      stop: () => Promise.resolve(),
    });

    control.on("message", (message: unknown) => {
      this.handle(message as ControlMessage).catch((err: unknown) => {
        system.logger().error("control message failed", {
          error: err as Error,
        });
      });
    });

    // The one logical system's event stream lives on the main isolate:
    // every dead letter this facade publishes is forwarded there, so a
    // subscriber on the system the user started sees hops that failed
    // on this side of the boundary.
    system.subscribe((event) => {
      if (event instanceof Deadletter) {
        this.post({
          kind: "deadletter",
          sender: event.sender,
          receiver: event.receiver,
          message: this.wireOf(event.message),
          reason: event.reason,
        });
      }
    });
  }

  /** Announces the isolate as booted and ready for wiring. */
  announce(): void {
    this.post({ kind: "ready" });
  }

  /** Returns the isolate's mesh. */
  mesh(): Mesh {
    return this._mesh;
  }

  /** Executes one control message; unknown kinds are ignored so the
   * protocol can grow without breaking older workers. */
  private async handle(message: ControlMessage): Promise<void> {
    if (message.kind === "connect") {
      this._mesh.connect(message.workerId, message.port);
      return;
    }

    if (message.kind === "disconnect") {
      this._mesh.disconnect(message.workerId);
      return;
    }

    if (message.kind === "spawn") {
      await this.spawn(message.seq, message.name, message.recipe);
      return;
    }

    if (message.kind === "name-added") {
      this._names.set(message.name, message.workerId);
      return;
    }

    if (message.kind === "name-freed") {
      this._names.delete(message.name);
      return;
    }

    if (message.kind === "claimed") {
      const settle = this._claims.get(message.seq);
      if (settle !== undefined) {
        this._claims.delete(message.seq);
        settle(message.error === null ? null : decodeError(message.error));
      }

      return;
    }

    if (message.kind === "placed") {
      this.settlePlacement(message);
      return;
    }

    if (message.kind === "stop") {
      await this.stop();
    }
  }

  /** Claims a top-level name with the control plane; a name this
   * runtime is spawning on the plane's own order is already owned by
   * the placement and needs no claim. */
  private claimName(name: string): Promise<Error | null> {
    if (this._placing.has(name)) {
      return Promise.resolve(null);
    }

    const seq = this._nextSeq++;
    return new Promise((resolve) => {
      this._claims.set(seq, resolve);
      this.post({ kind: "claim", seq, name });
    });
  }

  /** Places Props through the control plane and resolves to the placed
   * actor's handle: the live PID when placement landed on this very
   * isolate, a routed handle otherwise. */
  private placeProps(name: string, props: Props, options?: SpawnOptions): Promise<PID> {
    const recipe = placedRecipe(props, options);
    const seq = this._nextSeq++;
    return new Promise<PID>((resolve, reject) => {
      this._placements.set(seq, { resolve, reject });
      this.post({ kind: "place", seq, name, recipe });
    });
  }

  /** Settles one placement request from the control plane's answer. */
  private settlePlacement(message: Extract<ControlMessage, { kind: "placed" }>): void {
    const pending = this._placements.get(message.seq);
    if (pending === undefined) {
      return;
    }

    this._placements.delete(message.seq);
    if (message.error !== null) {
      pending.reject(decodeError(message.error));
      return;
    }

    const path = parsePath(message.path, message.uid);
    if (message.workerId === this._workerId) {
      const pid = this._system.resolvePath(path);
      if (pid === undefined) {
        pending.reject(ErrDead);
      } else {
        pending.resolve(pid);
      }

      return;
    }

    // route() is null only for this runtime's own worker id, which the
    // branch above already handled.
    const route = this._mesh.route(message.workerId) as IsolateRoute;
    pending.resolve(routedPid(this._system, path, route));
  }

  /** Resolves a top-level name through the replicated name table to a
   * routed handle; names this isolate owns resolve through the local
   * tree before the facade ever asks here. */
  private findName(name: string): PID | undefined {
    const owner = this._names.get(name);
    if (owner === undefined || owner === this._workerId) {
      return undefined;
    }

    const route = this._mesh.route(owner) as IsolateRoute;
    const path = newPathAt(name, addressOf(this._system.noSender().path()), undefined, "");
    return routedPid(this._system, path, route);
  }

  /** Resolves a spawn command and answers with the outcome; failures
   * travel back in wire form so sentinels keep their identity. The
   * placed actor's eventual stop, however it comes about, is announced
   * back so the control plane frees the name. */
  private async spawn(seq: number, name: string, recipe: ActorRecipe): Promise<void> {
    this._placing.add(name);
    try {
      const pid = await spawnRecipe(this._system, name, recipe);
      pid.onStopped(() => {
        this.post({ kind: "actor-stopped", name });
      });
      this.post({ kind: "spawned", seq, path: pid.path().toString(), uid: pid.path().uid() });
    } catch (err) {
      this.post({ kind: "spawn-failed", seq, error: encodeError(err as Error) });
    } finally {
      this._placing.delete(name);
    }
  }

  /** Tears the isolate down on the control plane's order. */
  private async stop(): Promise<void> {
    this._mesh.close();
    await this._system.stop();
    this.post({ kind: "stopped" });

    // Deno's worker-threads compatibility layer omits close() on the
    // boot port; the pool terminates the isolate after "stopped", so
    // skipping the close there leaks nothing.
    if (typeof this._control.close === "function") {
      this._control.close();
    }
  }

  /** Encodes a forwarded dead letter's message, falling back to a
   * descriptive placeholder when the payload cannot cross the control
   * port; the dead letter must arrive even when its message cannot. */
  private wireOf(message: unknown): WireMessage {
    try {
      return this._codec.encodeMessage(message);
    } catch {
      const name = (message as { constructor?: { name?: string } })?.constructor?.name;
      return { type: "", data: `unencodable message of type ${name ?? typeof message}` };
    }
  }

  private post(message: WorkerMessage): void {
    this._control.postMessage(message);
  }
}
