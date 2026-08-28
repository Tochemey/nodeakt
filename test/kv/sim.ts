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

/**
 * The deterministic test harness for the key/value store: a virtual clock, a
 * reproducible random source, an in-memory transport fabric that models loss,
 * delay, duplication, and partition, and a scripted cluster view.
 *
 * Everything a scenario needs to observe is a function of the seed and the
 * scripted faults, so a failure reproduces exactly from the seed it prints. The
 * harness is self-contained: it fakes the {@link ClusterView} and
 * {@link KvTransport} contracts the store depends on and imports no protocol
 * internals, so it stays valid as the store's implementation grows.
 */

import type { ClusterMember, ClusterView, KvTransport } from "../../src/kv/ports";

/** Rejects a duration that could not be scheduled deterministically. */
function validateDelay(value: number, field: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`${field} must be a finite non-negative number`);
  }
}

/**
 * A handle to a scheduled callback, returned by the clock so a caller can
 * cancel a deadline before it fires.
 */
export interface SimTimer {
  readonly deadline: number;
  readonly order: number;
  readonly priority: number;
  readonly callback: () => void;
}

/**
 * A manually advanced monotonic clock shared by every endpoint in a simulation.
 *
 * Time moves only when a test advances it, so ordering is total and
 * reproducible. Network deliveries schedule at priority zero and protocol
 * deadlines at priority one, so a response that lands exactly on its deadline is
 * delivered before the deadline fires.
 */
export class SimClock {
  #current: number = 0;
  #sequence: number = 0;
  readonly #timers: SimTimer[] = [];

  /** Current virtual time in milliseconds since the simulation began. */
  now(): number {
    return this.#current;
  }

  /** Number of callbacks still waiting to fire. */
  get pending(): number {
    return this.#timers.length;
  }

  /** Schedules a protocol callback `delayMs` in the future. */
  schedule(delayMs: number, callback: () => void): SimTimer {
    return this.#scheduleAt(delayMs, callback, 1);
  }

  /** Schedules a network delivery, ordered ahead of protocol timers at an equal deadline. */
  scheduleInput(delayMs: number, callback: () => void): SimTimer {
    return this.#scheduleAt(delayMs, callback, 0);
  }

  /** Removes a scheduled callback so it never fires. Idempotent once fired or cancelled. */
  cancel(timer: SimTimer): void {
    const index: number = this.#timers.indexOf(timer);
    if (index >= 0) {
      this.#timers.splice(index, 1);
    }
  }

  /** Advances time by `durationMs`, firing every callback that comes due. */
  advanceBy(durationMs: number): void {
    validateDelay(durationMs, "duration");
    this.advanceTo(this.#current + durationMs);
  }

  /** Advances time to `targetTime`, firing due callbacks in deadline order. */
  advanceTo(targetTime: number): void {
    if (!Number.isFinite(targetTime) || targetTime < this.#current) {
      throw new RangeError("target time must be finite and monotonic");
    }

    while (true) {
      const next: SimTimer | undefined = this.#nextTimer(targetTime);
      if (next === undefined) {
        break;
      }

      this.#timers.splice(this.#timers.indexOf(next), 1);
      this.#current = next.deadline;
      next.callback();
    }

    this.#current = targetTime;
  }

  /** Runs the single earliest pending callback, returning whether one existed. */
  runNext(): boolean {
    const next: SimTimer | undefined = this.#nextTimer(Number.POSITIVE_INFINITY);
    if (next === undefined) {
      return false;
    }

    this.advanceTo(next.deadline);
    return true;
  }

  /** Drains every pending callback, guarding against a callback loop with `limit`. */
  runAll(limit: number = 10_000): void {
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new RangeError("run limit must be a positive integer");
    }

    let count: number = 0;
    while (this.pending > 0) {
      if (count >= limit) {
        throw new Error(`simulation exceeded ${limit} scheduled deadlines`);
      }

      this.runNext();
      count += 1;
    }
  }

  #scheduleAt(delayMs: number, callback: () => void, priority: number): SimTimer {
    validateDelay(delayMs, "delay");
    const timer: SimTimer = {
      deadline: this.#current + delayMs,
      order: this.#sequence,
      priority,
      callback,
    };
    this.#sequence += 1;
    this.#timers.push(timer);
    return timer;
  }

  #nextTimer(atOrBefore: number): SimTimer | undefined {
    let selected: SimTimer | undefined;
    for (const timer of this.#timers) {
      if (timer.deadline > atOrBefore) {
        continue;
      }

      if (this.#earlier(timer, selected)) {
        selected = timer;
      }
    }

    return selected;
  }

  #earlier(timer: SimTimer, selected: SimTimer | undefined): boolean {
    if (selected === undefined) {
      return true;
    }

    if (timer.deadline !== selected.deadline) {
      return timer.deadline < selected.deadline;
    }

    if (timer.priority !== selected.priority) {
      return timer.priority < selected.priority;
    }

    return timer.order < selected.order;
  }
}

/** Number of distinct unsigned 32-bit values and exclusive seed upper bound. */
const UINT32_RANGE: number = 0x1_0000_0000;
/** Nonzero xorshift32 state substituted for the absorbing all-zero state. */
const ZERO_SEED_STATE: number = 0x6d2b_79f5;

/**
 * A reproducible xorshift32 source so a scenario's random choices, which node to
 * kill, which key to write, are fixed by its seed and replay exactly.
 *
 * It is fast and adequate for test choices; it is not cryptographically secure.
 * Seed zero is preserved publicly but mapped to a fixed nonzero internal state,
 * because zero is absorbing for xorshift32.
 */
export class SeededRandom {
  /** Original unsigned 32-bit seed, including zero before state substitution. */
  readonly seed: number;

  /** Current nonzero unsigned xorshift32 state, advanced by each draw. */
  #state: number;

  constructor(seed: number) {
    if (!Number.isInteger(seed) || seed < 0 || seed >= UINT32_RANGE) {
      throw new RangeError("seed must be an unsigned 32-bit integer");
    }

    this.seed = seed;
    this.#state = seed === 0 ? ZERO_SEED_STATE : seed;
  }

  /** Draws a reproducible fraction in the half-open interval `[0, 1)`. */
  next(): number {
    return this.#uint32() / UINT32_RANGE;
  }

  /** Draws a uniform integer in `[0, maximumExclusive)` without modulo bias. */
  integer(maximumExclusive: number): number {
    if (
      !Number.isInteger(maximumExclusive) ||
      maximumExclusive <= 0 ||
      maximumExclusive > UINT32_RANGE
    ) {
      throw new RangeError("maximum must be an integer from 1 through 2^32");
    }

    const rejectedBelow: number = UINT32_RANGE % maximumExclusive;
    let value: number = this.#uint32();
    while (value < rejectedBelow) {
      value = this.#uint32();
    }

    return value % maximumExclusive;
  }

  #uint32(): number {
    let value: number = this.#state;
    value ^= value << 13;
    value ^= value >>> 17;
    value ^= value << 5;
    this.#state = value >>> 0;
    return this.#state;
  }
}

/**
 * A fault applied to one directional link. Every field is optional; an omitted
 * field keeps the link's current setting.
 */
export interface LinkFault {
  /** Drops the next message, which surfaces to the sender as a deadline timeout. */
  readonly drop?: boolean;
  /** Delivers after this many virtual milliseconds. */
  readonly delayMs?: number;
  /** Extra deliveries after the original, so the receiver handles the message again. */
  readonly duplicates?: number;
  /** Cuts the link until healed; every message is lost while set. */
  readonly partitioned?: boolean;
}

interface NormalizedFault {
  readonly drop: boolean;
  readonly delayMs: number;
  readonly duplicates: number;
  readonly partitioned: boolean;
}

interface LinkState {
  fault: NormalizedFault;
  script: LinkFault[];
}

function normalizeFault(fault: LinkFault, base?: NormalizedFault): NormalizedFault {
  const delayMs: number = fault.delayMs ?? base?.delayMs ?? 0;
  const duplicates: number = fault.duplicates ?? base?.duplicates ?? 0;
  validateDelay(delayMs, "link delay");
  if (!Number.isInteger(duplicates) || duplicates < 0) {
    throw new RangeError("link duplicates must be a non-negative integer");
  }

  return {
    drop: fault.drop ?? base?.drop ?? false,
    delayMs,
    duplicates,
    partitioned: fault.partitioned ?? base?.partitioned ?? false,
  };
}

const HEALTHY_LINK: NormalizedFault = {
  drop: false,
  delayMs: 0,
  duplicates: 0,
  partitioned: false,
};

/** The inbound handler a transport installs through {@link KvTransport.listen}. */
type RequestHandler = (from: string, body: Uint8Array) => Promise<Uint8Array>;

/**
 * One node's endpoint on the fabric. Requests and responses both travel through
 * the fabric, so both are subject to link faults.
 */
class SimTransport implements KvTransport {
  #handler: RequestHandler | undefined;
  #closed: boolean = false;

  constructor(
    readonly name: string,
    private readonly fabric: SimFabric,
  ) {}

  /** Whether {@link close} has released this endpoint. */
  get closed(): boolean {
    return this.#closed;
  }

  request(to: string, body: Uint8Array, deadlineMs: number): Promise<Uint8Array> {
    if (this.#closed) {
      return Promise.reject(new Error(`endpoint ${this.name} is closed`));
    }

    return this.fabric.dispatch(this.name, to, body, deadlineMs);
  }

  listen(handler: RequestHandler): void {
    this.#handler = handler;
  }

  stop(): Promise<void> {
    this.#closed = true;
    this.#handler = undefined;
    return Promise.resolve();
  }

  /** Runs the installed handler, or returns `undefined` when none can answer. */
  accept(from: string, body: Uint8Array): Promise<Uint8Array> | undefined {
    if (this.#closed || this.#handler === undefined) {
      return undefined;
    }

    return this.#handler(from, body);
  }
}

/**
 * A deterministic request/response network shared by a simulation.
 *
 * Links are directional channels keyed by (from, to) and carry both requests
 * and their responses. Each link has a persistent fault set through
 * {@link setLink} or {@link partition}, plus a queue of one-shot faults set
 * through {@link scriptLink} that each apply to a single message before the
 * persistent fault takes over again. A dropped or partitioned message is never
 * delivered, so the sender learns of the failure only when its deadline fires.
 */
export class SimFabric {
  /** The shared virtual clock; scenarios advance it to move the simulation. */
  readonly clock: SimClock = new SimClock();

  /** The reproducible random source, seeded from {@link seed}. */
  readonly random: SeededRandom;

  /** The seed that fixes this run; print it to reproduce a failure. */
  readonly seed: number;

  readonly #endpoints: Map<string, SimTransport> = new Map<string, SimTransport>();
  readonly #links: Map<string, Map<string, LinkState>> = new Map<string, Map<string, LinkState>>();

  constructor(seed: number) {
    this.random = new SeededRandom(seed);
    this.seed = seed;
  }

  /** A one-line seed banner for a scenario to print when it fails. */
  get seedReport(): string {
    return `simulation seed: ${this.seed}`;
  }

  /**
   * The transport for `name`, created on first use. A closed endpoint is
   * replaced by a fresh one, modelling a node that restarts under the same name.
   */
  transport(name: string): KvTransport {
    if (name.length === 0) {
      throw new RangeError("endpoint name must not be empty");
    }

    const existing: SimTransport | undefined = this.#endpoints.get(name);
    if (existing !== undefined && !existing.closed) {
      return existing;
    }

    const endpoint: SimTransport = new SimTransport(name, this);
    this.#endpoints.set(name, endpoint);
    return endpoint;
  }

  /** Sets the persistent fault on the `from` to `to` link. */
  setLink(from: string, to: string, fault: LinkFault): void {
    this.#link(from, to).fault = normalizeFault(fault);
  }

  /** Queues one-shot faults, each applied to a single `from` to `to` message in order. */
  scriptLink(from: string, to: string, faults: readonly LinkFault[]): void {
    const link: LinkState = this.#link(from, to);
    link.script = faults.map((fault: LinkFault): LinkFault => ({ ...fault }));
    for (const fault of link.script) {
      normalizeFault(fault, link.fault);
    }
  }

  /** Cuts or heals the `from` to `to` link. */
  partition(from: string, to: string, partitioned: boolean = true): void {
    const link: LinkState = this.#link(from, to);
    link.fault = { ...link.fault, partitioned };
  }

  /** Cuts or heals both directions between two nodes. */
  partitionBoth(first: string, second: string, partitioned: boolean = true): void {
    this.partition(first, second, partitioned);
    this.partition(second, first, partitioned);
  }

  /**
   * Carries one request to `to` and its response back to `from`, honouring the
   * fault on each direction, and rejects if no response arrives by the deadline.
   */
  dispatch(from: string, to: string, body: Uint8Array, deadlineMs: number): Promise<Uint8Array> {
    const request: Uint8Array = body.slice();
    return new Promise<Uint8Array>((resolve, reject): void => {
      let settled: boolean = false;
      const timeout: SimTimer = this.clock.schedule(deadlineMs, (): void => {
        if (settled) {
          return;
        }

        settled = true;
        reject(new Error(`kv request from ${from} to ${to} timed out after ${deadlineMs}ms`));
      });

      const deliver = (response: Uint8Array): void => {
        if (settled) {
          return;
        }

        settled = true;
        this.clock.cancel(timeout);
        resolve(response);
      };

      const invoke = (): void => {
        const destination: SimTransport | undefined = this.#endpoints.get(to);
        const answered: Promise<Uint8Array> | undefined = destination?.accept(
          from,
          request.slice(),
        );
        if (answered === undefined) {
          return;
        }

        answered.then(
          (response: Uint8Array): void => this.#relayResponse(to, from, response, deliver),
          (): void => undefined,
        );
      };

      const forward: NormalizedFault = this.#nextFault(from, to);
      if (forward.partitioned || forward.drop) {
        return;
      }

      for (let copy: number = 0; copy <= forward.duplicates; copy += 1) {
        this.clock.scheduleInput(forward.delayMs, invoke);
      }
    });
  }

  /** Schedules a handler's response back to the requester, honouring the return link. */
  #relayResponse(
    from: string,
    to: string,
    response: Uint8Array,
    deliver: (bytes: Uint8Array) => void,
  ): void {
    const reverse: NormalizedFault = this.#nextFault(from, to);
    if (reverse.partitioned || reverse.drop) {
      return;
    }

    const payload: Uint8Array = response.slice();
    this.clock.scheduleInput(reverse.delayMs, (): void => deliver(payload.slice()));
  }

  #nextFault(from: string, to: string): NormalizedFault {
    const link: LinkState = this.#link(from, to);
    const scripted: LinkFault | undefined = link.script.shift();
    return scripted === undefined ? link.fault : normalizeFault(scripted, link.fault);
  }

  #link(from: string, to: string): LinkState {
    let destinations: Map<string, LinkState> | undefined = this.#links.get(from);
    if (destinations === undefined) {
      destinations = new Map<string, LinkState>();
      this.#links.set(from, destinations);
    }

    let state: LinkState | undefined = destinations.get(to);
    if (state === undefined) {
      state = { fault: HEALTHY_LINK, script: [] };
      destinations.set(to, state);
    }

    return state;
  }
}

/** Optional flags when building a member; both default to a ready, non-draining node. */
export interface MemberOptions {
  readonly ready?: boolean;
  readonly draining?: boolean;
}

/** Builds a {@link ClusterMember}, defaulting a node to ready and not draining. */
export function member(
  name: string,
  startedAt: number,
  options: MemberOptions = {},
): ClusterMember {
  return {
    name,
    startedAt,
    ready: options.ready ?? true,
    draining: options.draining ?? false,
  };
}

/** Total order over member names, so ties on start time break deterministically. */
function compareName(first: string, second: string): number {
  if (first < second) {
    return -1;
  }

  if (first > second) {
    return 1;
  }

  return 0;
}

/** Returns a copy sorted oldest first, ties broken by name, as the real view guarantees. */
function sortMembers(members: readonly ClusterMember[]): readonly ClusterMember[] {
  return [...members].sort(
    (first: ClusterMember, second: ClusterMember): number =>
      first.startedAt - second.startedAt || compareName(first.name, second.name),
  );
}

/**
 * A scripted {@link ClusterView} whose membership a test drives by hand.
 *
 * {@link members} always returns the canonical order the real view promises,
 * oldest `startedAt` first with ties broken by name, so the coordinator is
 * always the first element. {@link set} installs a new membership and notifies
 * every subscriber; it does not fire on subscription.
 */
export class SimCluster implements ClusterView {
  readonly self: string;
  #members: readonly ClusterMember[];
  readonly #listeners: Set<(members: readonly ClusterMember[]) => void> = new Set();

  constructor(self: string, members: readonly ClusterMember[] = []) {
    this.self = self;
    this.#members = sortMembers(members);
  }

  members(): readonly ClusterMember[] {
    return this.#members;
  }

  onChange(listener: (members: readonly ClusterMember[]) => void): () => void {
    this.#listeners.add(listener);
    return (): void => {
      this.#listeners.delete(listener);
    };
  }

  /** Replaces the membership and notifies every current subscriber. */
  set(members: readonly ClusterMember[]): void {
    this.#members = sortMembers(members);
    for (const listener of this.#listeners) {
      listener(this.#members);
    }
  }
}

/**
 * Drains chained microtasks so promise reactions scheduled by store code observe
 * their results before a test asserts.
 */
export async function flush(turns: number = 10): Promise<void> {
  for (let turn: number = 0; turn < turns; turn += 1) {
    await Promise.resolve();
  }
}

/**
 * Drives the simulation until `operation` settles or the turn budget runs out,
 * alternating microtask drains with the next due virtual timer.
 */
export async function settle<T>(fabric: SimFabric, operation: Promise<T>): Promise<T> {
  let done: boolean = false;
  void operation.then(
    (): void => {
      done = true;
    },
    (): void => {
      done = true;
    },
  );
  for (let turns: number = 0; !done && turns < 200; turns += 1) {
    await flush();
    if (!done) {
      fabric.clock.runNext();
    }
  }

  return operation;
}
