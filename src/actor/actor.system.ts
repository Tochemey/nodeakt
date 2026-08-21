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

import { defaultLogger } from "../logger/json.logger";
import type { Logger } from "../logger/logger";
import type { Actor } from "./actor";
import type { ActorSystemOptions } from "./actor.system.options";
import {
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrInvalidActorName,
  ErrInvalidActorSystemName,
  ErrNameRequired,
  ErrReservedName,
} from "./errors";
import { NoSender } from "./no.sender";
import { PassivationManager } from "./passivation.manager";
import { isValidActorName, newPathAt, type Path, type PathAddress } from "./path";
import { PID } from "./pid";
import { PidTree } from "./pid.tree";
import {
  isSystemName,
  noSenderName,
  rootGuardianName,
  systemGuardianName,
  userGuardianName,
} from "./reserved";
import { RootGuardian } from "./root.guardian";
import type { SpawnOptions } from "./spawn.options";
import { SystemGuardian } from "./system.guardian";
import { UserGuardian } from "./user.guardian";

/** The node endpoint of a single-node actor system. */
const HOST = "127.0.0.1";
const PORT = 0;

/**
 * Actor system names must start with an alphanumeric character and may
 * contain alphanumerics, '-' or '_'. Stricter than actor names: no dots.
 */
const SYSTEM_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9-_]*$/;

/**
 * ActorSystem is the runtime that hosts and manages actors: one logical
 * system per machine, owning the actor tree, the guardian hierarchy, and
 * the passivation scheduler.
 *
 * Create one, `start` it, `spawn` actors, and `stop` it on the way out.
 * Every actor lives under one of the guardians: runtime actors under the
 * system guardian, actors created with {@link spawn} under the user
 * guardian, both children of the root guardian.
 */
export class ActorSystem {
  private readonly _name: string;
  private readonly _address: PathAddress;
  private readonly _tree = new PidTree();
  private readonly _passivation = new PassivationManager();
  private readonly _logger: Logger;

  private _rootGuardian: PID | null = null;
  private _systemGuardian: PID | null = null;
  private _userGuardian: PID | null = null;
  private _noSender: PID | null = null;
  private _started = false;
  private _stopping = false;

  /**
   * Creates an actor system with the given name.
   *
   * @param name - The system name; it must start with an alphanumeric
   * character and contain only alphanumerics, `-` or `_`.
   * @param options - The system configuration; every setting has a
   * default.
   *
   * @throws The {@link ErrNameRequired} sentinel when the name is empty.
   * @throws The {@link ErrInvalidActorSystemName} sentinel when the name
   * violates the syntax rules.
   */
  constructor(name: string, options?: ActorSystemOptions) {
    if (name.length === 0) {
      throw ErrNameRequired;
    }

    if (!SYSTEM_NAME_PATTERN.test(name)) {
      throw ErrInvalidActorSystemName;
    }

    this._name = name;
    this._address = { system: name, host: HOST, port: PORT };
    this._logger = options?.logger ?? defaultLogger;
  }

  /** Returns the actor system name. */
  name(): string {
    return this._name;
  }

  /** Returns the logger the runtime reports through. */
  logger(): Logger {
    return this._logger;
  }

  /** Reports whether the system has been started and is not stopping. */
  isRunning(): boolean {
    return this._started && !this._stopping;
  }

  /**
   * Starts the actor system: the guardian hierarchy is created and the
   * system begins accepting spawns. Starting a running system is a no-op.
   *
   * @throws An `ActorInitializationError` when a guardian fails to
   * initialize, in which case the system did not start.
   */
  async start(): Promise<void> {
    if (this._started) {
      return;
    }

    // Runtime actors rely on the long-lived default strategy and never
    // passivate. The tree records that the system and user guardians are
    // children of the root guardian and that the NoSender actor is a
    // child of the system guardian; guardians are supervision parents
    // only, so they never appear in the path of an actor beneath them.
    this._rootGuardian = await this.configPID(rootGuardianName, new RootGuardian(), null);

    this._systemGuardian = await this.configPID(
      systemGuardianName,
      new SystemGuardian(),
      this._rootGuardian,
    );

    this._noSender = await this.configPID(noSenderName, new NoSender(), this._systemGuardian);

    this._userGuardian = await this.configPID(
      userGuardianName,
      new UserGuardian(),
      this._rootGuardian,
    );

    this._started = true;
  }

  /**
   * Stops the actor system gracefully: every actor is shut down through
   * the guardian hierarchy (mailboxes drain, `postStop` hooks run) and
   * the passivation scheduler is released. Stopping a stopped system is a
   * no-op; a stopped system can be started again.
   */
  async stop(): Promise<void> {
    if (!this._started || this._stopping) {
      return;
    }

    this._stopping = true;
    this._passivation.stop();
    await this._rootGuardian?.shutdown();

    this._tree.reset();
    this._rootGuardian = null;
    this._systemGuardian = null;
    this._userGuardian = null;
    this._noSender = null;
    this._started = false;
    this._stopping = false;
  }

  /**
   * Returns the PID of the NoSender actor, the runtime actor representing
   * an anonymous or absent sender. Use it to send messages from outside an
   * actor: `system.noSender().tell(target, message)`; the receiving
   * behavior then sees this PID as `ctx.sender`.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when the system
   * is not started.
   */
  noSender(): PID {
    if (this._noSender === null) {
      throw ErrActorSystemNotStarted;
    }

    return this._noSender;
  }

  /**
   * Hooks an actor into the system's passivation scheduler according to
   * the actor's own strategy. Runtime plumbing for the spawn paths;
   * strategies other than time based involve no scheduling.
   *
   * @internal
   */
  schedulePassivation(pid: PID): void {
    this._passivation.register(pid, pid.passivationStrategy());
  }

  /**
   * Creates and starts an actor under the given unique name.
   *
   * The actor becomes a child of the user guardian and receives a
   * {@link PostStart} message before anything else.
   *
   * @param name - The actor's name, unique within the system.
   * @param actor - The actor implementation to run.
   * @param options - The mailbox and passivation configuration; both have
   * defaults.
   *
   * @returns The PID of the started actor.
   *
   * @throws The {@link ErrActorSystemNotStarted} sentinel when the system
   * is not running.
   * @throws The {@link ErrReservedName} sentinel when the name carries
   * the runtime's reserved prefix.
   * @throws The {@link ErrInvalidActorName} sentinel when the name is
   * empty, longer than 255 characters, or violates the name syntax.
   * @throws The {@link ErrActorAlreadyExists} sentinel when the name is
   * still held by an actor that has not fully stopped, including a
   * suspended or currently stopping one.
   * @throws An `ActorInitializationError` when the actor's `preStart`
   * fails; the underlying failure is available as its `cause` and the
   * actor is not registered.
   */
  async spawn(name: string, actor: Actor, options?: SpawnOptions): Promise<PID> {
    if (!this.isRunning() || this._userGuardian === null) {
      throw ErrActorSystemNotStarted;
    }

    if (isSystemName(name)) {
      throw ErrReservedName;
    }

    if (!isValidActorName(name)) {
      throw ErrInvalidActorName;
    }

    // Occupancy is registration in the tree: a suspended or stopping
    // actor still holds its name until it has fully stopped.
    const existing = this._tree.child(this._userGuardian, name);
    if (existing !== undefined) {
      throw ErrActorAlreadyExists;
    }

    const pid = await this.configPID(name, actor, this._userGuardian, options);
    this.schedulePassivation(pid);
    return pid;
  }

  /**
   * Resolves a top-level actor by name: one created with {@link spawn}.
   *
   * Actors deeper in the hierarchy are reached through their parent, not
   * by bare name, and the runtime's own actors are not resolvable.
   *
   * @param name - The actor name to look up.
   *
   * @returns The running actor's PID, or `undefined` when no running
   * top-level actor holds the name.
   */
  actorOf(name: string): PID | undefined {
    if (this._userGuardian === null) {
      return undefined;
    }

    const pid = this._tree.child(this._userGuardian, name);
    if (pid === undefined || !pid.isRunning()) {
      return undefined;
    }

    return pid;
  }

  /**
   * Returns the address of a top-level actor: a path with no parent
   * segment. An actor's path chain begins at its top-level ancestor;
   * the guardian layer above is supervision structure recorded in the
   * tree, never part of an address.
   */
  private actorAddress(name: string): Path {
    return newPathAt(name, this._address);
  }

  /** Constructs, starts, and registers one actor in the tree under the
   * given parent. Bypasses the user-facing spawn checks, so guardians
   * can carry reserved names. */
  private async configPID(
    name: string,
    actor: Actor,
    parent: PID | null,
    options?: SpawnOptions,
  ): Promise<PID> {
    const pid = new PID(
      actor,
      this.actorAddress(name),
      this,
      options,
      parent ?? undefined,
      this._tree,
    );

    await pid.start();
    return pid;
  }
}
