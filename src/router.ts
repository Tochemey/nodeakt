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

import type { Actor } from "./actor";
import {
  ErrDead,
  ErrFanOutAsk,
  ErrInvalidPoolSize,
  ErrInvalidRoutingStrategy,
  ErrRoutingKeyRequired,
} from "./errors";
import { HashRing } from "./hash.ring";
import { PanicSignal, PostStart, Terminated } from "./messages";
import { LongLivedStrategy } from "./passivation";
import type { PID } from "./pid";
import { type ActorClass, Props } from "./props";
import { type ReceiveContext, rejectAsk } from "./receive.context";
import { AdjustRouterPoolSize, GetRoutees, Routees } from "./router.messages";
import {
  ConsistentHashRouting,
  FanOutRouting,
  RandomRouting,
  RoundRobinRouting,
  type RouterOptions,
  type RoutingKeyFunc,
  type RoutingStrategy,
} from "./router.options";
import { EscalateDirective, Supervisor } from "./supervisor";

/** Prefix of the names a router mints for its routees; the counter
 * suffix is never reused within one router. */
const ROUTEE_PREFIX = "routee-";

/**
 * Router is the actor behind `ActorSystem.spawnRouter`: it owns a pool
 * of identical routees spawned as its children and forwards every user
 * message to them according to its routing strategy, never processing
 * one itself. Deliveries are handed over live, so a routee sees the
 * original sender as `ctx.sender` and an ask answered by a routee
 * settles the asker's promise directly, bypassing the router.
 *
 * The forwarding path is bound to the strategy once at construction,
 * so routing a message costs no per-message strategy dispatch.
 *
 * The router consumes its management messages itself: `GetRoutees`,
 * `AdjustRouterPoolSize`, and the `Terminated` notifications of the
 * routees it watches. A dead routee leaves the rotation; at zero
 * routees, forwarding matches the send-path rules and every user
 * message becomes a dead letter.
 *
 * Runtime plumbing; developers create routers with
 * `ActorSystem.spawnRouter`.
 *
 * @internal
 */
export class Router implements Actor {
  private readonly _poolSize: number;
  private readonly _routees: Props;
  private readonly _strategy: RoutingStrategy;
  private readonly _routingKey: RoutingKeyFunc | null;

  /** Forwards one user message; bound to the strategy at construction
   * so the hot path runs no branching chain. */
  private readonly _route: (ctx: ReceiveContext) => void;

  /** The supervisor every routee is spawned with: any failure escalates
   * to the router, which then drops the routee from the pool. */
  private readonly _routeeSupervisor: Supervisor;

  /** The routees in spawn order; a dead routee leaves the list when its
   * `Terminated` arrives. */
  private _members: PID[] = [];

  /** The round-robin cursor into {@link _members}. */
  private _next = 0;

  /** How many routee names this router has minted. Never reused, so a
   * routee spawned by a grow cannot collide with the name of one that
   * has not fully stopped. */
  private _minted = 0;

  /** The consistent-hash ring, rebuilt on every membership change; null
   * for every other strategy. */
  private _ring: HashRing | null;

  constructor(
    poolSize: number,
    routees: Props,
    strategy: RoutingStrategy,
    routingKey: RoutingKeyFunc | null,
  ) {
    this._poolSize = poolSize;
    this._routees = routees;
    this._strategy = strategy;
    this._routingKey = routingKey;
    this._routeeSupervisor = new Supervisor({ anyErrorDirective: EscalateDirective });
    this._ring = strategy === ConsistentHashRouting ? new HashRing([]) : null;

    switch (strategy) {
      case RandomRouting:
        this._route = this.routeRandom;
        break;

      case FanOutRouting:
        this._route = this.broadcast;
        break;

      case ConsistentHashRouting:
        this._route = this.routeHashed;
        break;

      default:
        this._route = this.routeRoundRobin;
    }
  }

  preStart(): void {}

  receive(ctx: ReceiveContext): void | Promise<void> {
    const msg: unknown = ctx.message;

    if (msg instanceof PostStart) {
      return this.seed(ctx);
    }

    if (msg instanceof Terminated) {
      this.forget(msg.actorPath);
      return;
    }

    if (msg instanceof GetRoutees) {
      ctx.response(new Routees(this.livePaths()));
      return;
    }

    if (msg instanceof AdjustRouterPoolSize) {
      return this.resize(ctx, msg.poolSize);
    }

    if (msg instanceof PanicSignal) {
      return this.dropFailed(ctx);
    }

    this._route(ctx);
  }

  postStop(): void {}

  /** Builds the initial pool. The runtime delivers `PostStart` exactly
   * once; a forged one arriving later must not grow the pool again. */
  private seed(ctx: ReceiveContext): Promise<void> | undefined {
    if (this._minted !== 0) {
      return undefined;
    }

    return this.grow(ctx, this._poolSize);
  }

  /** Forwards one user message to the next routee in rotation. */
  private routeRoundRobin(ctx: ReceiveContext): void {
    this.deliver(ctx, this.nextMember());
  }

  /** Forwards one user message to a random live routee. */
  private routeRandom(ctx: ReceiveContext): void {
    this.deliver(ctx, this.randomMember());
  }

  /** Forwards one user message to the routee owning its routing key. A
   * key extractor that throws does not fault the router: the message is
   * routed to dead letters carrying the thrown reason. */
  private routeHashed(ctx: ReceiveContext): void {
    let key: string | number;

    try {
      key = (this._routingKey as RoutingKeyFunc)(ctx.message);
    } catch (err) {
      this.undeliverable(ctx, err instanceof Error ? err : new Error(String(err)));
      return;
    }

    this.deliver(ctx, (this._ring as HashRing).lookup(String(key)));
  }

  /** Hands one delivery to the chosen routee, or to dead letters when
   * no routee is live. */
  private deliver(ctx: ReceiveContext, routee: PID | null): void {
    if (routee === null) {
      this.undeliverable(ctx, ErrDead);
      return;
    }

    // The live context moves to the routee's mailbox, so the original
    // sender and any reply channel survive the hop. The delivery cannot
    // be refused: the routee was vetted as running on this same turn
    // and routee mailboxes are unbounded.
    routee.redeliver(ctx);
  }

  /**
   * Delivers one message to every live routee, with the original
   * sender preserved. A reply channel is rejected up front:
   * request/response to a broadcast has no single answer.
   */
  private broadcast(ctx: ReceiveContext): void {
    if (rejectAsk(ctx, ErrFanOutAsk)) {
      return;
    }

    const members: PID[] = this._members;
    const sender: PID = ctx.sender as PID;
    let delivered = false;

    for (let i = 0; i < members.length; i++) {
      const member: PID = members[i] as PID;

      if (member.isRunning()) {
        sender.tell(member, ctx.message);
        delivered = true;
      }
    }

    if (!delivered) {
      this.undeliverable(ctx, ErrDead);
    }
  }

  /** Returns the next live routee in rotation, or null when none is
   * running. */
  private nextMember(): PID | null {
    const members: PID[] = this._members;
    const n: number = members.length;
    let cursor: number = this._next;

    for (let i = 0; i < n; i++) {
      if (cursor >= n) {
        cursor = 0;
      }

      const member: PID = members[cursor] as PID;
      cursor++;

      if (member.isRunning()) {
        this._next = cursor;
        return member;
      }
    }

    return null;
  }

  /** Returns a random live routee, or null when none is running. */
  private randomMember(): PID | null {
    const members: PID[] = this._members;
    const n: number = members.length;
    if (n === 0) {
      return null;
    }

    const start: number = Math.floor(Math.random() * n);

    for (let i = 0; i < n; i++) {
      const member: PID = members[(start + i) % n] as PID;

      if (member.isRunning()) {
        return member;
      }
    }

    return null;
  }

  /**
   * Routes an unforwardable message to dead letters with the router as
   * the failing receiver, rejecting its reply channel first so an ask
   * fails with the same reason instead of timing out: a pool at zero
   * routees behaves like a send to a stopped actor.
   */
  private undeliverable(ctx: ReceiveContext, err: Error): void {
    rejectAsk(ctx, err);

    const self: PID = ctx.self as PID;
    const sender: PID = ctx.sender as PID;
    self.actorSystem().toDeadletter(sender.id(), self.id(), ctx.message, err);
  }

  /** Spawns `count` fresh routees as children of the router, watching
   * each one so its death leaves the rotation. */
  private async grow(ctx: ReceiveContext, count: number): Promise<void> {
    for (let i = 0; i < count; i++) {
      const name: string = `${ROUTEE_PREFIX}${this._minted++}`;
      const routee: PID = await ctx.spawn(name, this.buildRoutee(), {
        supervisor: this._routeeSupervisor,
        // Routees are managed pool members, not idle-passivatable actors:
        // one dropping out on an idle window would silently shrink the pool.
        passivationStrategy: new LongLivedStrategy(),
      });

      ctx.watch(routee);
      this._members.push(routee);
    }

    this.rebuildRing();
  }

  /** Constructs one routee instance from the pool's Props. */
  private buildRoutee(): Actor {
    const type: ActorClass = this._routees.type();
    return new type(...(this._routees.args() as never[]));
  }

  /**
   * Grows or shrinks the pool to `size` live routees. A shrink stops
   * the newest routees gracefully; an asked adjustment is answered with
   * the resulting {@link Routees} once the change is done.
   */
  private async resize(ctx: ReceiveContext, size: number): Promise<void> {
    if (!Number.isInteger(size) || size < 0) {
      this.undeliverable(ctx, ErrInvalidPoolSize);
      return;
    }

    const live: PID[] = this._members.filter((member) => member.isRunning());

    if (size > live.length) {
      await this.grow(ctx, size - live.length);
    }

    if (size < live.length) {
      await this.shrink(ctx, live.slice(size));
    }

    ctx.response(new Routees(this.livePaths()));
  }

  /** Stops the given routees and drops them from the rotation. Members
   * that are not running, a suspended routee for example, stay tracked
   * until the router observes their end. */
  private async shrink(ctx: ReceiveContext, excess: PID[]): Promise<void> {
    const dropped: Set<PID> = new Set(excess);
    this._members = this._members.filter((member) => !dropped.has(member));
    this._next = 0;

    await Promise.all(excess.map((member) => ctx.stop(member)));
    this.rebuildRing();
  }

  /**
   * Drops a routee that escalated a failure. Stopping it removes it from
   * the pool through the same `Terminated` path a dead routee takes, so
   * the pool shrinks; `AdjustRouterPoolSize` grows it back. A signal whose
   * sender is not a current routee is ignored, covering a forged one.
   */
  private dropFailed(ctx: ReceiveContext): Promise<void> | undefined {
    const failed: PID = ctx.sender as PID;
    const member: PID | undefined = this._members.find((routee) => routee.equals(failed));
    if (member === undefined) {
      return undefined;
    }

    return ctx.stop(member);
  }

  /**
   * Removes a stopped routee from the rotation. Unknown paths are a
   * no-op, covering routees an explicit shrink already dropped, and a
   * member that is still running is kept: a genuine `Terminated` only
   * arrives after its routee fully stopped, so anything else is forged.
   */
  private forget(path: string): void {
    const members: PID[] = this._members;

    for (let i = 0; i < members.length; i++) {
      const member: PID = members[i] as PID;

      if (member.id() !== path) {
        continue;
      }

      if (member.isRunning()) {
        return;
      }

      members.splice(i, 1);
      this.rebuildRing();
      return;
    }
  }

  /** Rebuilds the consistent-hash ring after a membership change; a
   * no-op for every other strategy. */
  private rebuildRing(): void {
    if (this._strategy === ConsistentHashRouting) {
      this._ring = new HashRing(this._members);
    }
  }

  /** Returns the canonical path strings of the live routees. */
  private livePaths(): string[] {
    const paths: string[] = [];

    for (const member of this._members) {
      if (member.isRunning()) {
        paths.push(member.id());
      }
    }

    return paths;
  }
}

/**
 * Validates a router configuration and constructs the router actor.
 * Runtime plumbing for `ActorSystem.spawnRouter`.
 *
 * @throws The {@link ErrInvalidPoolSize} sentinel when `poolSize` is
 * not a positive integer.
 * @throws A `TypeError` when `routees` is not a `Props`.
 * @throws The {@link ErrInvalidRoutingStrategy} sentinel for an unknown
 * strategy.
 * @throws The {@link ErrRoutingKeyRequired} sentinel when the strategy
 * is consistent hashing and no routing key extractor is given.
 *
 * @internal
 */
export function createRouter(poolSize: number, routees: Props, options?: RouterOptions): Router {
  if (!Number.isInteger(poolSize) || poolSize < 1) {
    throw ErrInvalidPoolSize;
  }

  if (!(routees instanceof Props)) {
    throw new TypeError("routees must be created with Props.create");
  }

  const strategy: RoutingStrategy = options?.strategy ?? RoundRobinRouting;
  if (
    strategy !== RoundRobinRouting &&
    strategy !== RandomRouting &&
    strategy !== FanOutRouting &&
    strategy !== ConsistentHashRouting
  ) {
    throw ErrInvalidRoutingStrategy;
  }

  const routingKey: RoutingKeyFunc | null = options?.routingKey ?? null;
  if (strategy === ConsistentHashRouting && typeof routingKey !== "function") {
    throw ErrRoutingKeyRequired;
  }

  return new Router(poolSize, routees, strategy, routingKey);
}
