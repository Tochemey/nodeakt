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
  private current = 0;
  private sequence = 0;
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
      const next = this.nextTimer(targetTime);
      if (next === undefined) {
        break;
      }
      const index = this.timers.indexOf(next);
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
    const next = this.nextTimer(Number.POSITIVE_INFINITY);
    if (next === undefined) {
      return false;
    }
    this.advanceTo(next.deadline);
    return true;
  }

  runAll(limit = 10_000): void {
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new RangeError("run limit must be a positive integer");
    }

    let count = 0;
    while (this.pending > 0) {
      if (count >= limit) {
        throw new Error(`simulation exceeded ${limit} scheduled deadlines`);
      }

      const next = this.nextTimer(Number.POSITIVE_INFINITY);
      if (next === undefined) {
        break;
      }

      const index = this.timers.indexOf(next);
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
  const delayMs = fault.delayMs ?? base?.delayMs ?? 0;
  const duplicates = fault.duplicates ?? base?.duplicates ?? 0;
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

interface PendingRead {
  readonly resolve: (bytes: Uint8Array | undefined) => void;
}

class SimStream implements MembershipStream {
  private readonly queued: Uint8Array[] = [];
  private readonly readers: PendingRead[] = [];
  private ended = false;
  private closed = false;
  private peer: SimStream | undefined;
  deliveryFloor = 0;

  constructor(
    readonly localAddress: string,
    readonly remoteAddress: string,
    private readonly network: SimNetwork,
  ) {}

  connect(peer: SimStream): void {
    this.peer = peer;
  }

  read(): Promise<Uint8Array | undefined> {
    const bytes = this.queued.shift();
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
    const peer = this.peer;
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
    const peer = this.peer;
    if (peer !== undefined) {
      this.network.endStream(this, peer);
    }
  }

  receive(bytes: Uint8Array): void {
    if (this.ended) {
      return;
    }
    const reader = this.readers.shift();
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

  constructor(
    readonly address: string,
    private readonly network: SimNetwork,
  ) {}

  get started(): boolean {
    return this.handlers !== undefined;
  }

  async start(handlers: TransportHandlers): Promise<void> {
    if (this.handlers !== undefined) {
      throw new Error(`endpoint ${this.address} is already started`);
    }
    this.handlers = handlers;
  }

  async stop(): Promise<void> {
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
    void this.handlers?.packet(from, bytes);
  }

  deliverStream(from: string, stream: MembershipStream): void {
    void this.handlers?.stream(from, stream);
  }
}

/** A deterministic packet-and-stream network shared by a simulation. */
export class SimNetwork {
  readonly clock = new SimClock();
  readonly random: SeededRandom;
  readonly seed: number;
  private readonly endpoints = new Map<string, SimEndpoint>();
  private readonly links = new Map<string, Map<string, LinkState>>();
  private readonly streams = new Set<SimStream>();

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
    const existing = this.endpoints.get(address);
    if (existing !== undefined) {
      if (existing.started) {
        throw new Error(`endpoint ${address} already exists`);
      }
      return existing;
    }
    const endpoint = new SimEndpoint(address, this);
    this.endpoints.set(address, endpoint);
    return endpoint;
  }

  setLink(from: string, to: string, fault: LinkFault): void {
    this.link(from, to).fault = normalizeFault(fault);
  }

  scriptLink(from: string, to: string, faults: readonly LinkFault[]): void {
    const link = this.link(from, to);
    link.script = faults.map((fault: LinkFault): LinkFault => ({ ...fault }));
    for (const fault of link.script) {
      normalizeFault(fault, link.fault);
    }
  }

  partition(from: string, to: string, partitioned = true): void {
    const link = this.link(from, to);
    link.fault = { ...link.fault, partitioned };
  }

  partitionBoth(first: string, second: string, partitioned = true): void {
    this.partition(first, second, partitioned);
    this.partition(second, first, partitioned);
  }

  sendPacket(from: string, to: string, bytes: Uint8Array): Promise<void> {
    const destination = this.startedEndpoint(to);
    const fault = this.nextFault(from, to);
    if (fault.partitioned || fault.drop) {
      return Promise.resolve();
    }
    const accepted = bytes.slice();
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
    const destination = this.startedEndpoint(to);
    const fault = this.nextFault(from, to);
    if (fault.partitioned || fault.drop) {
      return Promise.reject(new Error(`stream from ${from} to ${to} was not delivered`));
    }
    return new Promise<MembershipStream>((resolve, reject): void => {
      this.clock.scheduleInput(fault.delayMs, (): void => {
        if (!destination.started) {
          reject(new Error(`endpoint ${to} is stopped`));
          return;
        }
        const outgoing = new SimStream(from, to, this);
        const incoming = new SimStream(to, from, this);
        outgoing.connect(incoming);
        incoming.connect(outgoing);
        this.streams.add(outgoing);
        this.streams.add(incoming);
        destination.deliverStream(from, incoming);
        resolve(outgoing);
      });
    });
  }

  writeStream(from: SimStream, to: SimStream, bytes: Uint8Array): Promise<void> {
    const fault = this.nextFault(from.localAddress, from.remoteAddress);
    if (fault.partitioned || fault.drop) {
      return Promise.resolve();
    }
    const accepted = bytes.slice();
    const requestedDeadline = this.clock.now() + fault.delayMs;
    const deadline = Math.max(requestedDeadline, from.deliveryFloor);
    from.deliveryFloor = deadline;
    for (let copy = 0; copy <= fault.duplicates; copy += 1) {
      this.clock.scheduleInput(deadline - this.clock.now(), (): void => {
        to.receive(accepted.slice());
      });
    }
    return Promise.resolve();
  }

  endStream(from: SimStream, to: SimStream): void {
    const deadline = Math.max(this.clock.now(), from.deliveryFloor);
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

  private startedEndpoint(address: string): SimEndpoint {
    const endpoint = this.endpoints.get(address);
    if (endpoint === undefined || !endpoint.started) {
      throw new Error(`endpoint ${address} is stopped or missing`);
    }
    return endpoint;
  }

  private link(from: string, to: string): LinkState {
    let destinations = this.links.get(from);
    if (destinations === undefined) {
      destinations = new Map<string, LinkState>();
      this.links.set(from, destinations);
    }
    let link = destinations.get(to);
    if (link === undefined) {
      link = { fault: HEALTHY_LINK, script: [] };
      destinations.set(to, link);
    }
    return link;
  }

  private nextFault(from: string, to: string): NormalizedFault {
    const link = this.link(from, to);
    const scripted = link.script.shift();
    return scripted === undefined ? link.fault : normalizeFault(scripted, link.fault);
  }
}
