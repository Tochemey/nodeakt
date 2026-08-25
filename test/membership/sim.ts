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

import type { Clock, ClockTimer } from "../../src/membership/clock";
import { SeededRandom } from "../../src/membership/random";
import type {
  MembershipStream,
  MembershipTransport,
  TransportHandlers,
} from "../../src/membership/transport";

interface SimTimer extends ClockTimer {
  cancelled: boolean;
  readonly deadline: number;
  readonly order: number;
  readonly priority: number;
  readonly callback: () => void;
}

function validateDuration(value: number, field: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`${field} must be a finite non-negative number`);
  }
}

/** A manually advanced monotonic clock shared by every endpoint in a simulation. */
export class SimClock implements Clock {
  private current: number = 0;
  private sequence: number = 0;
  private readonly timers: SimTimer[] = [];

  now(): number {
    return this.current;
  }

  /** Virtual time doubles as the epoch domain; stamps never affect ordering. */
  epochMilliseconds(): number {
    return this.current;
  }

  schedule(delayMs: number, callback: () => void): ClockTimer {
    return this.scheduleAt(delayMs, callback, 1);
  }

  /** Schedules network input ahead of protocol timers at an equal deadline. */
  scheduleInput(delayMs: number, callback: () => void): ClockTimer {
    return this.scheduleAt(delayMs, callback, 0);
  }

  private scheduleAt(delayMs: number, callback: () => void, priority: number): ClockTimer {
    validateDuration(delayMs, "delay");
    const timer: SimTimer = {
      cancelled: false,
      deadline: this.current + delayMs,
      order: this.sequence,
      priority,
      callback,
    };
    this.sequence += 1;
    this.timers.push(timer);
    return timer;
  }

  cancel(timer: ClockTimer): void {
    (timer as SimTimer).cancelled = true;
  }

  get pending(): number {
    return this.timers.filter((timer: SimTimer): boolean => !timer.cancelled).length;
  }

  advanceBy(durationMs: number): void {
    validateDuration(durationMs, "duration");
    this.advanceTo(this.current + durationMs);
  }

  advanceTo(targetTime: number): void {
    if (!Number.isFinite(targetTime) || targetTime < this.current) {
      throw new RangeError("target time must be finite and monotonic");
    }
    while (true) {
      const next: SimTimer | undefined = this.nextTimer(targetTime);
      if (next === undefined) {
        break;
      }
      const index: number = this.timers.indexOf(next);
      this.timers.splice(index, 1);
      if (next.cancelled) {
        continue;
      }
      this.current = next.deadline;
      next.callback();
    }
    this.current = targetTime;
  }

  runNext(): boolean {
    const next: SimTimer | undefined = this.nextTimer(Number.POSITIVE_INFINITY);
    if (next === undefined) {
      return false;
    }
    this.advanceTo(next.deadline);
    return true;
  }

  runAll(limit: number = 10_000): void {
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new RangeError("run limit must be a positive integer");
    }

    let count: number = 0;
    while (this.pending > 0) {
      if (count >= limit) {
        throw new Error(`simulation exceeded ${limit} scheduled deadlines`);
      }

      const next: SimTimer | undefined = this.nextTimer(Number.POSITIVE_INFINITY);
      if (next === undefined) {
        break;
      }

      const index: number = this.timers.indexOf(next);
      this.timers.splice(index, 1);
      this.current = next.deadline;
      next.callback();
      count += 1;
    }
  }

  private nextTimer(atOrBefore: number): SimTimer | undefined {
    let selected: SimTimer | undefined;
    for (const timer of this.timers) {
      if (timer.cancelled || timer.deadline > atOrBefore) {
        continue;
      }
      if (
        selected === undefined ||
        timer.deadline < selected.deadline ||
        (timer.deadline === selected.deadline &&
          (timer.priority < selected.priority ||
            (timer.priority === selected.priority && timer.order < selected.order)))
      ) {
        selected = timer;
      }
    }
    return selected;
  }
}

export interface LinkFault {
  readonly drop?: boolean;
  readonly delayMs?: number;
  /** Number of additional deliveries after the original. */
  readonly duplicates?: number;
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
  validateDuration(delayMs, "link delay");
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

/**
 * Simulated dial-failure delay for an unreachable (partitioned) destination.
 *
 * The real carrier only discovers unreachability when its connect backstop
 * expires, strictly after the protocol's own 1s connect deadline, so the sim
 * must not hand senders faster knowledge than production would.
 */
const UNREACHABLE_REJECTION_DELAY_MS: number = 2_000;

interface PendingRead {
  readonly resolve: (bytes: Uint8Array | undefined) => void;
}

class SimStream implements MembershipStream {
  private readonly queued: Uint8Array[] = [];
  private readonly readers: PendingRead[] = [];
  private ended: boolean = false;
  private closed: boolean = false;
  private peer: SimStream | undefined;
  deliveryFloor: number = 0;

  constructor(
    readonly localAddress: string,
    readonly remoteAddress: string,
    private readonly network: SimNetwork,
  ) {}

  connect(peer: SimStream): void {
    this.peer = peer;
  }

  read(): Promise<Uint8Array | undefined> {
    // A locally closed stream reads as ended, exactly like the TCP stream's
    // contract, even when undelivered bytes remain queued.
    if (this.closed) {
      return Promise.resolve(undefined);
    }

    const bytes: Uint8Array | undefined = this.queued.shift();
    if (bytes !== undefined) {
      return Promise.resolve(bytes);
    }

    if (this.ended) {
      return Promise.resolve(undefined);
    }

    return new Promise<Uint8Array | undefined>((resolve): void => {
      this.readers.push({ resolve });
    });
  }

  write(bytes: Uint8Array): Promise<void> {
    if (this.closed) {
      return Promise.reject(new Error("stream is closed"));
    }
    const peer: SimStream | undefined = this.peer;
    if (peer === undefined) {
      return Promise.reject(new Error("stream is not connected"));
    }
    return this.network.writeStream(this, peer, bytes);
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    // Local waiters settle immediately: a close during a pending read must not
    // leave that read hanging past the exchange that abandoned it.
    for (const reader of this.readers.splice(0)) {
      reader.resolve(undefined);
    }
    const peer: SimStream | undefined = this.peer;
    if (peer !== undefined) {
      this.network.endStream(this, peer);
    }
  }

  receive(bytes: Uint8Array): void {
    if (this.ended) {
      return;
    }
    const reader: PendingRead | undefined = this.readers.shift();
    if (reader === undefined) {
      this.queued.push(bytes);
    } else {
      reader.resolve(bytes);
    }
  }

  receiveEnd(): void {
    if (this.ended) {
      return;
    }
    this.ended = true;
    for (const reader of this.readers.splice(0)) {
      reader.resolve(undefined);
    }
  }

  abort(): void {
    this.closed = true;
    this.queued.splice(0);
    this.receiveEnd();
  }
}

class SimEndpoint implements MembershipTransport {
  private handlers: TransportHandlers | undefined;
  private stopped: boolean = false;

  constructor(
    readonly address: string,
    private readonly network: SimNetwork,
  ) {}

  get started(): boolean {
    return this.handlers !== undefined;
  }

  /** Whether stop has made this endpoint permanently unusable, like the TCP transport. */
  get terminal(): boolean {
    return this.stopped;
  }

  async start(handlers: TransportHandlers): Promise<void> {
    if (this.stopped) {
      throw new Error(`cannot restart stopped endpoint ${this.address}`);
    }
    if (this.handlers !== undefined) {
      throw new Error(`endpoint ${this.address} is already started`);
    }
    this.handlers = handlers;
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.handlers === undefined) {
      return;
    }
    this.handlers = undefined;
    this.network.endpointStopped(this.address);
  }

  async packet(to: string, bytes: Uint8Array): Promise<void> {
    if (!this.started) {
      throw new Error(`endpoint ${this.address} is stopped`);
    }
    await this.network.sendPacket(this.address, to, bytes);
  }

  async stream(to: string): Promise<MembershipStream> {
    if (!this.started) {
      throw new Error(`endpoint ${this.address} is stopped`);
    }
    return this.network.openStream(this.address, to);
  }

  deliverPacket(from: string, bytes: Uint8Array): void {
    // Dispatch stays synchronous so handlers run at their virtual delivery
    // time; a rejected asynchronous handler loses only its own packet, the
    // way a real handler failure severs only its own connection.
    const outcome: void | Promise<void> | undefined = this.handlers?.packet(from, bytes);
    if (outcome !== undefined) {
      void outcome.catch((): void => undefined);
    }
  }

  deliverStream(from: string, stream: MembershipStream): void {
    const outcome: void | Promise<void> | undefined = this.handlers?.stream(from, stream);
    if (outcome !== undefined) {
      void outcome.catch((): void => undefined);
    }
  }
}

/**
 * Drains chained microtasks so promise reactions scheduled by protocol code
 * observe their results before a test asserts.
 */
export async function flush(turns: number = 10): Promise<void> {
  for (let turn = 0; turn < turns; turn += 1) {
    await Promise.resolve();
  }
}

/**
 * Drives the simulation until `operation` settles or the turn budget runs out,
 * alternating microtask drains with the next due virtual timer.
 */
export async function settle<T>(network: SimNetwork, operation: Promise<T>): Promise<T> {
  let done: boolean = false;
  void operation.then(
    (): void => {
      done = true;
    },
    (): void => {
      done = true;
    },
  );
  for (let turns = 0; !done && turns < 200; turns += 1) {
    await flush();
    if (!done) {
      network.clock.runNext();
    }
  }

  return operation;
}

/** A deterministic packet-and-stream network shared by a simulation. */
export class SimNetwork {
  readonly clock: SimClock = new SimClock();
  readonly random: SeededRandom;
  readonly seed: number;
  private readonly endpoints: Map<string, SimEndpoint> = new Map<string, SimEndpoint>();
  private readonly links: Map<string, Map<string, LinkState>> = new Map<
    string,
    Map<string, LinkState>
  >();
  private readonly streams: Set<SimStream> = new Set<SimStream>();

  constructor(seed: number) {
    this.random = new SeededRandom(seed);
    this.seed = seed;
  }

  get seedReport(): string {
    return `simulation seed: ${this.seed}`;
  }

  endpoint(address: string): MembershipTransport {
    if (address.length === 0) {
      throw new RangeError("endpoint address must not be empty");
    }
    const existing: SimEndpoint | undefined = this.endpoints.get(address);
    if (existing !== undefined) {
      if (existing.started) {
        throw new Error(`endpoint ${address} already exists`);
      }
      // Stop is terminal, exactly as on the TCP transport: a revived node
      // constructs a fresh transport instead of restarting the old one.
      if (!existing.terminal) {
        return existing;
      }
    }
    const endpoint: SimEndpoint = new SimEndpoint(address, this);
    this.endpoints.set(address, endpoint);
    return endpoint;
  }

  setLink(from: string, to: string, fault: LinkFault): void {
    this.link(from, to).fault = normalizeFault(fault);
  }

  scriptLink(from: string, to: string, faults: readonly LinkFault[]): void {
    const link: LinkState = this.link(from, to);
    link.script = faults.map((fault: LinkFault): LinkFault => ({ ...fault }));
    for (const fault of link.script) {
      normalizeFault(fault, link.fault);
    }
  }

  partition(from: string, to: string, partitioned: boolean = true): void {
    const link: LinkState = this.link(from, to);
    link.fault = { ...link.fault, partitioned };
  }

  partitionBoth(first: string, second: string, partitioned: boolean = true): void {
    this.partition(first, second, partitioned);
    this.partition(second, first, partitioned);
  }

  sendPacket(from: string, to: string, bytes: Uint8Array): Promise<void> {
    const fault: NormalizedFault = this.nextFault(from, to);
    // A partitioned destination is unreachable, not known-dead: the failure
    // surfaces only after the carrier's connect backstop, never synchronously.
    if (fault.partitioned) {
      return this.rejectAfter(
        UNREACHABLE_REJECTION_DELAY_MS,
        `packet from ${from} to ${to} was not delivered`,
      );
    }

    const destination: SimEndpoint | undefined = this.endpoints.get(to);
    if (destination === undefined || !destination.started) {
      // A stopped peer refuses the dial promptly; the rejection is
      // asynchronous, never same-tick sender knowledge.
      return Promise.reject(new Error(`endpoint ${to} is stopped or missing`));
    }

    // Loss on a reachable path models a dropped datagram: the local write
    // still resolves, exactly as the carrier reports only local completion.
    if (fault.drop) {
      return Promise.resolve();
    }
    const accepted: Uint8Array = bytes.slice();
    for (let copy = 0; copy <= fault.duplicates; copy += 1) {
      this.clock.scheduleInput(fault.delayMs, (): void => {
        if (destination.started) {
          destination.deliverPacket(from, accepted.slice());
        }
      });
    }
    return Promise.resolve();
  }

  openStream(from: string, to: string): Promise<MembershipStream> {
    const fault: NormalizedFault = this.nextFault(from, to);
    if (fault.partitioned || fault.drop) {
      return this.rejectAfter(
        UNREACHABLE_REJECTION_DELAY_MS,
        `stream from ${from} to ${to} was not delivered`,
      );
    }

    const destination: SimEndpoint | undefined = this.endpoints.get(to);
    if (destination === undefined || !destination.started) {
      return Promise.reject(new Error(`endpoint ${to} is stopped or missing`));
    }

    return new Promise<MembershipStream>((resolve, reject): void => {
      this.clock.scheduleInput(fault.delayMs, (): void => {
        if (!destination.started) {
          reject(new Error(`endpoint ${to} is stopped`));
          return;
        }
        const outgoing: SimStream = new SimStream(from, to, this);
        const incoming: SimStream = new SimStream(to, from, this);
        outgoing.connect(incoming);
        incoming.connect(outgoing);
        this.streams.add(outgoing);
        this.streams.add(incoming);
        destination.deliverStream(from, incoming);
        resolve(outgoing);
      });
    });
  }

  /** Rejects after a virtual delay so senders never learn outcomes synchronously. */
  private rejectAfter<T>(delayMs: number, message: string): Promise<T> {
    return new Promise<T>((_resolve, reject): void => {
      this.clock.scheduleInput(delayMs, (): void => {
        reject(new Error(message));
      });
    });
  }

  writeStream(from: SimStream, to: SimStream, bytes: Uint8Array): Promise<void> {
    const fault: NormalizedFault = this.nextFault(from.localAddress, from.remoteAddress);
    if (fault.partitioned || fault.drop) {
      return Promise.resolve();
    }
    const accepted: Uint8Array = bytes.slice();
    const requestedDeadline: number = this.clock.now() + fault.delayMs;
    const deadline: number = Math.max(requestedDeadline, from.deliveryFloor);
    from.deliveryFloor = deadline;
    for (let copy = 0; copy <= fault.duplicates; copy += 1) {
      this.clock.scheduleInput(deadline - this.clock.now(), (): void => {
        to.receive(accepted.slice());
      });
    }
    return Promise.resolve();
  }

  endStream(from: SimStream, to: SimStream): void {
    const deadline: number = Math.max(this.clock.now(), from.deliveryFloor);
    this.clock.scheduleInput(deadline - this.clock.now(), (): void => {
      to.receiveEnd();
    });
  }

  endpointStopped(address: string): void {
    for (const stream of this.streams) {
      if (stream.localAddress === address || stream.remoteAddress === address) {
        stream.abort();
        this.streams.delete(stream);
      }
    }
  }

  private link(from: string, to: string): LinkState {
    let destinations: Map<string, LinkState> | undefined = this.links.get(from);
    if (destinations === undefined) {
      destinations = new Map<string, LinkState>();
      this.links.set(from, destinations);
    }
    let link: LinkState | undefined = destinations.get(to);
    if (link === undefined) {
      link = { fault: HEALTHY_LINK, script: [] };
      destinations.set(to, link);
    }
    return link;
  }

  private nextFault(from: string, to: string): NormalizedFault {
    const link: LinkState = this.link(from, to);
    const scripted: LinkFault | undefined = link.script.shift();
    return scripted === undefined ? link.fault : normalizeFault(scripted, link.fault);
  }
}
