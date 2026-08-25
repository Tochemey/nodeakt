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

import { afterEach, describe, expect, it, vi } from "vitest";
import type { Clock, ClockTimer } from "../../src/membership/clock";
import { SeededRandom } from "../../src/membership/random";
import { Swim, SwimLifecycleError } from "../../src/membership/swim";
import { writeSyncFrames } from "../../src/membership/sync";
import type {
  MembershipStream,
  MembershipTransport,
  TransportHandlers,
} from "../../src/membership/transport";
import {
  type ApplyResult,
  MAX_INCARNATION,
  type MemberRecord,
  type ReapOperation,
} from "../../src/membership/view";
import {
  encodeMessage,
  MAX_MEMBERS,
  MAX_METADATA_BYTES,
  MESSAGE_GOSSIP,
  MESSAGE_SYNC_REQUEST,
  type MembershipUpdate,
  STATE_ALIVE,
  STATE_DEAD,
  STATE_LEFT,
  STATE_SUSPECT,
} from "../../src/membership/wire";
import { flush, SimNetwork, settle } from "./sim";

class CountingTransport implements MembershipTransport {
  starts: number = 0;
  stops: number = 0;

  constructor(readonly inner: MembershipTransport) {}

  get address(): string {
    return this.inner.address;
  }

  start(handlers: TransportHandlers): Promise<void> {
    this.starts += 1;
    return this.inner.start(handlers);
  }

  stop(): Promise<void> {
    this.stops += 1;
    return this.inner.stop();
  }

  packet(to: string, bytes: Uint8Array): Promise<void> {
    return this.inner.packet(to, bytes);
  }

  stream(to: string): Promise<MembershipStream> {
    return this.inner.stream(to);
  }
}

describe("Swim lifecycle and composition", () => {
  it("owns the transport once and exposes defensive snapshots", async () => {
    const network: SimNetwork = new SimNetwork(101);
    const transport: CountingTransport = new CountingTransport(network.endpoint("a"));
    const events: string[] = [];
    const swim: Swim = new Swim({
      address: "a",
      metadata: Uint8Array.of(1, 2),
      transport,
      clock: network.clock,
      random: new SeededRandom(1),
      onEvent: (event): void => {
        events.push(event.type);
      },
    });

    await Promise.all([swim.start(), swim.start()]);
    expect(transport.starts).toBe(1);
    expect(swim.self()).toMatchObject({ member: "a", state: STATE_ALIVE, incarnation: 0 });
    expect(events).toEqual(["joined"]);
    const snapshot: MemberRecord | undefined = swim.self();
    if (snapshot === undefined) {
      throw new Error("self snapshot missing");
    }
    snapshot.metadata[0] = 99;
    expect(swim.self()?.metadata).toEqual(Uint8Array.of(1, 2));

    await swim.stop();
    await swim.stop();
    expect(transport.stops).toBe(1);
    await expect(swim.start()).rejects.toBeInstanceOf(SwimLifecycleError);
  });

  it("enforces join lifecycle and drains graceful leave while inbound stays bound", async () => {
    const network: SimNetwork = new SimNetwork(102);
    const a: Swim = new Swim({
      address: "a",
      metadata: new Uint8Array(0),
      transport: network.endpoint("a"),
      clock: network.clock,
      random: new SeededRandom(2),
    });
    const bTransport: CountingTransport = new CountingTransport(network.endpoint("b"));
    const b: Swim = new Swim({
      address: "b",
      metadata: new Uint8Array(0),
      transport: bTransport,
      clock: network.clock,
      random: new SeededRandom(3),
    });

    await expect(a.join(["b"])).rejects.toBeInstanceOf(SwimLifecycleError);
    await Promise.all([a.start(), b.start()]);
    await settle(network, b.join(["a"]));
    expect(a.members().some((member): boolean => member.member === "b")).toBe(true);

    const leaving: Promise<void> = b.leave();
    expect(b.lifecycle).toBe("leaving");
    expect(bTransport.stops).toBe(0);
    expect(b.self()?.state).toBe(STATE_LEFT);
    await flush();
    network.clock.advanceBy(999);
    await flush();
    expect(bTransport.stops).toBe(0);
    network.clock.advanceBy(1);
    await settle(network, leaving);
    expect(b.lifecycle).toBe("stopped");
    expect(bTransport.stops).toBe(1);
    await b.leave();
    await a.stop();
  });
});

function membershipUpdate(
  member: string,
  state: typeof STATE_ALIVE | typeof STATE_SUSPECT | typeof STATE_DEAD | typeof STATE_LEFT,
  incarnation: number = 0,
  reporter: string = "",
): MembershipUpdate {
  return {
    state,
    selfOriginated: state === STATE_ALIVE || state === STATE_LEFT,
    incarnation,
    stateChangeTime: BigInt(incarnation),
    member,
    reporter,
    metadata: state === STATE_ALIVE ? Uint8Array.of(incarnation) : new Uint8Array(0),
  };
}

class ControlledTransport implements MembershipTransport {
  handlers: TransportHandlers | undefined;
  starts: number = 0;
  stops: number = 0;
  startResult: Promise<void> = Promise.resolve();
  packetResult: Promise<void> = Promise.resolve();
  stopResult: Promise<void> = Promise.resolve();
  readonly packets: string[] = [];

  constructor(readonly address: string) {}

  start(handlers: TransportHandlers): Promise<void> {
    this.starts += 1;
    this.handlers = handlers;
    return this.startResult;
  }

  stop(): Promise<void> {
    this.stops += 1;
    return this.stopResult;
  }

  packet(to: string): Promise<void> {
    this.packets.push(to);
    return this.packetResult;
  }

  stream(): Promise<MembershipStream> {
    return Promise.reject(new Error("no stream"));
  }
}

interface RecordedTimer extends ClockTimer {
  readonly callback: () => void;
  readonly inner: ClockTimer;
}

class InspectableClock implements Clock {
  readonly timers: RecordedTimer[] = [];

  constructor(readonly inner: Clock) {}

  now(): number {
    return this.inner.now();
  }

  epochMilliseconds(): number {
    return this.inner.epochMilliseconds();
  }

  schedule(delayMs: number, callback: () => void): ClockTimer {
    const timer: RecordedTimer = {
      callback,
      inner: this.inner.schedule(delayMs, callback),
      get cancelled(): boolean {
        return this.inner.cancelled;
      },
    };
    this.timers.push(timer);
    return timer;
  }

  cancel(timer: ClockTimer): void {
    this.inner.cancel((timer as RecordedTimer).inner);
  }
}

class MemoryStream implements MembershipStream {
  readonly writes: Uint8Array[] = [];
  private index: number = 0;
  closes: number = 0;

  constructor(
    readonly remoteAddress: string,
    readonly reads: readonly Uint8Array[] = [],
  ) {}

  read(): Promise<Uint8Array | undefined> {
    const value: Uint8Array | undefined = this.reads[this.index];
    this.index += 1;
    return Promise.resolve(value);
  }

  write(bytes: Uint8Array): Promise<void> {
    this.writes.push(bytes.slice());
    return Promise.resolve();
  }

  close(): void {
    this.closes += 1;
  }
}

function controlledSwim(network: SimNetwork, transport: ControlledTransport): Swim {
  return new Swim({
    address: transport.address,
    metadata: new Uint8Array(0),
    transport,
    clock: network.clock,
    random: new SeededRandom(7),
  });
}

describe("Swim defensive lifecycle and protocol paths", () => {
  afterEach(() => {
    vi.doUnmock("../../src/membership/probe");
    vi.doUnmock("../../src/membership/suspicion");
    vi.doUnmock("../../src/membership/view");
    vi.resetModules();
  });

  it("validates transport identity and metadata size", () => {
    const network: SimNetwork = new SimNetwork(200);
    expect(
      (): Swim =>
        new Swim({
          address: "a",
          metadata: new Uint8Array(0),
          transport: new ControlledTransport("b"),
          clock: network.clock,
          random: network.random,
        }),
    ).toThrow(/transport address/);
    expect(
      (): Swim =>
        new Swim({
          address: "a",
          metadata: new Uint8Array(MAX_METADATA_BYTES + 1),
          transport: new ControlledTransport("a"),
          clock: network.clock,
          random: network.random,
        }),
    ).toThrow(/metadata/);
  });

  it("recovers from listener startup failure", async () => {
    const network: SimNetwork = new SimNetwork(201);
    const transport: ControlledTransport = new ControlledTransport("a");
    transport.startResult = Promise.reject(new Error("bind failed"));
    const swim: Swim = controlledSwim(network, transport);
    await expect(swim.start()).rejects.toThrow("bind failed");
    expect(swim.lifecycle).toBe("new");

    // The retry finds the local record already installed and enqueued; only
    // the listener work runs again.
    transport.startResult = Promise.resolve();
    await swim.start();
    expect(swim.lifecycle).toBe("started");
    expect(swim.self()).toMatchObject({ member: "a", incarnation: 0 });
    await swim.stop();
    expect(swim.lifecycle).toBe("stopped");
  });

  it("rejects a start that cannot install local truth and returns to new", async () => {
    const network: SimNetwork = new SimNetwork(219);
    const transport: ControlledTransport = new ControlledTransport("bad\u0000address");
    const swim: Swim = new Swim({
      address: "bad\u0000address",
      metadata: new Uint8Array(0),
      transport,
      clock: network.clock,
      random: new SeededRandom(13),
    });

    // The NUL byte survives view installation but fails wire validation when
    // the record is enqueued, before any listener work begins.
    await expect(swim.start()).rejects.toBeInstanceOf(Error);
    expect(swim.lifecycle).toBe("new");
    expect(transport.starts).toBe(0);
  });

  it("stops safely while listener startup is pending", async () => {
    const network: SimNetwork = new SimNetwork(202);
    const transport: ControlledTransport = new ControlledTransport("a");
    let release: (() => void) | undefined;
    transport.startResult = new Promise<void>((resolve): void => {
      release = resolve;
    });
    const swim: Swim = controlledSwim(network, transport);
    const starting: Promise<void> = swim.start();
    const stopping: Promise<void> = swim.stop();
    await expect(swim.leave()).rejects.toMatchObject({ operation: "leave", state: "stopping" });
    release?.();
    await Promise.all([starting, stopping]);
    expect(swim.lifecycle).toBe("stopped");
    expect(transport.stops).toBe(1);
  });

  it("absorbs a listener rejection after stop has already begun", async () => {
    const network: SimNetwork = new SimNetwork(207);
    const transport: ControlledTransport = new ControlledTransport("a");
    let rejectStart: ((error: Error) => void) | undefined;
    transport.startResult = new Promise<void>((_resolve, reject): void => {
      rejectStart = reject;
    });
    const swim: Swim = controlledSwim(network, transport);
    const starting: Promise<void> = swim.start();
    const stopping: Promise<void> = swim.stop();

    rejectStart?.(new Error("late bind failure"));
    await expect(starting).rejects.toThrow("late bind failure");
    await stopping;

    expect(swim.lifecycle).toBe("stopped");
  });

  it("closes malformed inbound sync streams after responder failure", async () => {
    const network: SimNetwork = new SimNetwork(203);
    const transport: ControlledTransport = new ControlledTransport("a");
    const swim: Swim = controlledSwim(network, transport);
    await swim.start();
    let closes: number = 0;
    const stream: MembershipStream = {
      remoteAddress: "b",
      read: (): Promise<Uint8Array | undefined> =>
        Promise.resolve(
          encodeMessage({
            type: MESSAGE_SYNC_REQUEST,
            exchangeId: 1n,
            chunkIndex: 0,
            chunkCount: 1,
            updates: [],
          }),
        ),
      write: (): Promise<void> => Promise.resolve(),
      close: (): void => {
        closes += 1;
      },
    };
    transport.handlers?.stream("b", stream);
    for (let turn = 0; turn < 20; turn += 1) {
      await Promise.resolve();
    }
    expect(closes).toBeGreaterThan(0);
    await swim.stop();
  });

  it("applies inbound truth, confirmations, ignored records, and self-refutation", async () => {
    const network: SimNetwork = new SimNetwork(204);
    const transport: ControlledTransport = new ControlledTransport("a");
    const events: string[] = [];
    const swim: Swim = new Swim({
      address: "a",
      metadata: new Uint8Array(0),
      transport,
      clock: network.clock,
      random: network.random,
      onEvent: (event): void => {
        events.push(event.type);
      },
    });
    await swim.start();
    await swim.start();
    const deliver = (update: MembershipUpdate): void => {
      transport.handlers?.packet(
        "peer",
        encodeMessage({ type: MESSAGE_GOSSIP, updates: [update] }),
      );
    };
    deliver(membershipUpdate("b", STATE_ALIVE, 2));
    deliver(membershipUpdate("b", STATE_ALIVE, 1));
    deliver(membershipUpdate("b", STATE_SUSPECT, 2, "first"));
    deliver(membershipUpdate("b", STATE_SUSPECT, 2, "second"));
    deliver(membershipUpdate("a", STATE_SUSPECT, 0, "enemy"));
    expect(swim.self()?.incarnation).toBe(1);
    expect(events).toContain("joined");
    expect(swim.members().find((member): boolean => member.member === "b")?.state).toBe(
      STATE_SUSPECT,
    );

    const request: MemoryStream = new MemoryStream("peer");
    await writeSyncFrames(request, MESSAGE_SYNC_REQUEST, 9n, [
      membershipUpdate("b", STATE_SUSPECT, 2, "third"),
      membershipUpdate("a", STATE_DEAD, 5),
    ]);
    const inbound: MemoryStream = new MemoryStream("peer", request.writes);
    transport.handlers?.stream("peer", inbound);
    for (let turn = 0; turn < 20; turn += 1) {
      await Promise.resolve();
    }
    expect(swim.self()?.incarnation).toBe(6);

    const leaving: Promise<void> = swim.leave();
    deliver(membershipUpdate("a", STATE_ALIVE, 10));
    await flush();
    network.clock.advanceBy(1_000);
    await leaving;
    expect(swim.self()?.state).toBe(STATE_LEFT);
  });

  it("shares concurrent leave and stop promises and interrupts the drain", async () => {
    const network: SimNetwork = new SimNetwork(205);
    const transport: ControlledTransport = new ControlledTransport("a");
    const swim: Swim = controlledSwim(network, transport);
    await swim.start();
    const firstLeave: Promise<void> = swim.leave();
    const secondLeave: Promise<void> = swim.leave();
    expect(secondLeave).toBe(firstLeave);
    await flush();
    const firstStop: Promise<void> = swim.stop();
    const secondStop: Promise<void> = swim.stop();
    expect(secondStop).toBe(firstStop);
    await firstStop;
    await firstLeave;
    expect(swim.lifecycle).toBe("stopped");
  });

  it("handles an observer stopping the engine during leave publication", async () => {
    const network: SimNetwork = new SimNetwork(212);
    const transport: ControlledTransport = new ControlledTransport("a");
    let stopping: Promise<void> | undefined;
    let swim: Swim;
    swim = new Swim({
      address: "a",
      metadata: new Uint8Array(0),
      transport,
      clock: network.clock,
      random: network.random,
      onEvent: (event): void => {
        if (event.type === "left") {
          stopping = swim.stop();
        }
      },
    });
    await swim.start();

    await swim.leave();
    await stopping;

    expect(swim.lifecycle).toBe("stopped");
  });

  it("lets stop absorb a failed leave shutdown", async () => {
    const network: SimNetwork = new SimNetwork(208);
    const transport: ControlledTransport = new ControlledTransport("a");
    const swim: Swim = controlledSwim(network, transport);
    await swim.start();
    transport.handlers?.packet(
      "b",
      encodeMessage({ type: MESSAGE_GOSSIP, updates: [membershipUpdate("b", STATE_ALIVE)] }),
    );
    transport.stopResult = Promise.reject(new Error("shutdown failed"));

    const leaving: Promise<void> = swim.leave();
    await flush();
    network.clock.advanceBy(1_000);
    const stopping: Promise<void> = swim.stop();
    await expect(leaving).rejects.toThrow("shutdown failed");
    await stopping;

    expect(swim.lifecycle).toBe("stopped");
  });

  it("rejects self truth received by sync during leave", async () => {
    const network: SimNetwork = new SimNetwork(209);
    const transport: ControlledTransport = new ControlledTransport("a");
    const swim: Swim = controlledSwim(network, transport);
    await swim.start();

    const leaving: Promise<void> = swim.leave();
    const request: MemoryStream = new MemoryStream("peer");
    await writeSyncFrames(request, MESSAGE_SYNC_REQUEST, 10n, [
      membershipUpdate("a", STATE_ALIVE, 10),
    ]);
    const inbound: MemoryStream = new MemoryStream("peer", request.writes);
    transport.handlers?.stream("peer", inbound);
    await flush();

    expect(swim.self()).toMatchObject({ state: STATE_LEFT, incarnation: 0 });
    network.clock.advanceBy(1_000);
    await leaving;
  });

  it("cancels superseded reaps and ignores stale timer callbacks", async () => {
    const network: SimNetwork = new SimNetwork(210);
    const clock: InspectableClock = new InspectableClock(network.clock);
    const transport: ControlledTransport = new ControlledTransport("a");
    const swim: Swim = new Swim({
      address: "a",
      metadata: new Uint8Array(0),
      transport,
      clock,
      random: network.random,
    });
    await swim.start();
    const deliver = (update: MembershipUpdate): void => {
      transport.handlers?.packet(
        "peer",
        encodeMessage({ type: MESSAGE_GOSSIP, updates: [update] }),
      );
    };

    deliver(membershipUpdate("b", STATE_ALIVE, 0));
    deliver(membershipUpdate("b", STATE_DEAD, 0));
    const firstReap: RecordedTimer | undefined = clock.timers.at(-1);
    deliver(membershipUpdate("b", STATE_DEAD, 1));
    firstReap?.callback();
    expect(swim.members()).toContainEqual(expect.objectContaining({ member: "b", incarnation: 1 }));

    deliver(membershipUpdate("c", STATE_ALIVE, 0));
    deliver(membershipUpdate("c", STATE_DEAD, 0));
    deliver(membershipUpdate("c", STATE_ALIVE, 1));
    expect(swim.members()).toContainEqual(
      expect.objectContaining({ member: "c", state: STATE_ALIVE }),
    );
    await swim.stop();
  });

  it("arms a local suspicion timer for suspect truth learned from gossip", async () => {
    let probe:
      | { readonly callbacks: { readonly updates: (updates: readonly MembershipUpdate[]) => void } }
      | undefined;
    interface ProbeSeamOptions {
      readonly callbacks: NonNullable<typeof probe>["callbacks"];
    }
    vi.doMock("../../src/membership/probe", () => ({
      BASE_PROTOCOL_PERIOD_MS: 1_000,
      Probe: class {
        constructor(options: ProbeSeamOptions) {
          probe = options;
        }

        get scale(): number {
          return 1;
        }

        start(): Promise<void> {
          return Promise.resolve();
        }

        stop(): Promise<void> {
          return Promise.resolve();
        }

        pause(): void {}

        receivePacket(): void {}

        selfRefute(): number {
          return 0;
        }
      },
    }));
    const { Swim: SeamSwim } = await import("../../src/membership/swim");
    const network: SimNetwork = new SimNetwork(212);
    const transport: ControlledTransport = new ControlledTransport("a");
    const swim: Swim = new SeamSwim({
      address: "a",
      metadata: new Uint8Array(0),
      transport,
      clock: network.clock,
      random: network.random,
    });
    await swim.start();
    const stateOf = (member: string): number | undefined =>
      swim.members().find((record): boolean => record.member === member)?.state;

    probe?.callbacks.updates([membershipUpdate("b", STATE_ALIVE, 0)]);
    probe?.callbacks.updates([membershipUpdate("b", STATE_SUSPECT, 0, "peer")]);
    expect(stateOf("b")).toBe(STATE_SUSPECT);

    // The accuser never speaks again and probing is disabled entirely: the
    // timer armed when the gossiped suspicion was applied must expire it into
    // dead truth on its own, at the unconfirmed 6x maximum of the window.
    network.clock.advanceBy(23_999);
    expect(stateOf("b")).toBe(STATE_SUSPECT);
    network.clock.advanceBy(1);
    expect(stateOf("b")).toBe(STATE_DEAD);
    await swim.stop();
  });

  it("drops unapplicable wire-legal updates instead of failing the packet path", async () => {
    const network: SimNetwork = new SimNetwork(213);
    const transport: ControlledTransport = new ControlledTransport("a");
    const swim: Swim = controlledSwim(network, transport);
    await swim.start();
    const deliver = (updates: readonly MembershipUpdate[]): void => {
      transport.handlers?.packet(
        "peer",
        encodeMessage({ type: MESSAGE_GOSSIP, updates: [...updates] }),
      );
    };

    // A suspect claim about self at the wire's maximum incarnation cannot be
    // answered with a higher incarnation. It must be dropped without severing
    // the packet path or losing the rest of its piggybacked updates.
    expect((): void => {
      deliver([
        membershipUpdate("a", STATE_SUSPECT, MAX_INCARNATION, "peer"),
        membershipUpdate("b", STATE_ALIVE, 1),
      ]);
    }).not.toThrow();
    expect(swim.self()).toMatchObject({ state: STATE_ALIVE, incarnation: 0 });
    expect(swim.members().some((record): boolean => record.member === "b")).toBe(true);

    let index: number = 0;
    while (swim.members().length < MAX_MEMBERS) {
      const batch: MembershipUpdate[] = [];
      const room: number = MAX_MEMBERS - swim.members().length;
      for (let count = 0; count < Math.min(40, room); count += 1) {
        batch.push(membershipUpdate(`m${index}`, STATE_ALIVE, 0));
        index += 1;
      }

      deliver(batch);
    }

    expect(swim.members()).toHaveLength(MAX_MEMBERS);
    expect((): void => {
      deliver([membershipUpdate("overflow", STATE_ALIVE, 0)]);
    }).not.toThrow();
    expect(swim.members()).toHaveLength(MAX_MEMBERS);
    await swim.stop();
  });

  it("reaches the terminal state even when transport stop rejects", async () => {
    const network: SimNetwork = new SimNetwork(214);
    const transport: ControlledTransport = new ControlledTransport("a");
    const failure: Error = new Error("sticky listener failure");
    transport.stopResult = Promise.reject(failure);
    void transport.stopResult.catch((): void => undefined);
    const swim: Swim = controlledSwim(network, transport);
    await swim.start();

    await expect(swim.stop()).rejects.toBe(failure);
    expect(swim.lifecycle).toBe("stopped");
    await expect(swim.stop()).resolves.toBeUndefined();
  });

  it("hands reentrant lifecycle calls the shared promise from synchronous events", async () => {
    const network: SimNetwork = new SimNetwork(215);
    const transport: ControlledTransport = new ControlledTransport("a");
    let reentrantStart: Promise<void> | undefined;
    let reentrantLeave: Promise<void> | undefined;
    const swim: Swim = new Swim({
      address: "a",
      metadata: new Uint8Array(0),
      transport,
      clock: network.clock,
      random: new SeededRandom(9),
      onEvent: (event): void => {
        if (event.member.member !== "a") {
          return;
        }

        if (event.type === "joined" && reentrantStart === undefined) {
          reentrantStart = swim.start();
        }

        if (event.type === "left" && reentrantLeave === undefined) {
          reentrantLeave = swim.leave();
        }
      },
    });

    const starting: Promise<void> = swim.start();
    expect(reentrantStart).toBeInstanceOf(Promise);
    await starting;
    await reentrantStart;

    const leaving: Promise<void> = swim.leave();
    expect(reentrantLeave).toBeInstanceOf(Promise);
    network.clock.advanceBy(1_000);
    await settle(network, leaving);
    await reentrantLeave;
    expect(swim.lifecycle).toBe("stopped");
  });

  it("suspects failed probes, expires suspicion, and reaps terminal members", async () => {
    const network: SimNetwork = new SimNetwork(206);
    const transport: ControlledTransport = new ControlledTransport("a");
    const swim: Swim = controlledSwim(network, transport);
    await swim.start();
    transport.handlers?.packet(
      "b",
      encodeMessage({ type: MESSAGE_GOSSIP, updates: [membershipUpdate("b", STATE_ALIVE)] }),
    );
    network.clock.advanceBy(2_000);
    expect(swim.members().find((member): boolean => member.member === "b")?.state).toBe(
      STATE_SUSPECT,
    );
    network.clock.advanceBy(60_000);
    expect(swim.members().find((member): boolean => member.member === "b")?.state).not.toBe(
      STATE_SUSPECT,
    );

    transport.handlers?.packet(
      "c",
      encodeMessage({ type: MESSAGE_GOSSIP, updates: [membershipUpdate("c", STATE_DEAD)] }),
    );
    network.clock.advanceBy(30_000);
    expect(swim.members().some((member): boolean => member.member === "c")).toBe(false);
    await swim.stop();
  });

  it("defensively rejects stale callbacks from composed components", async () => {
    const actualView: typeof import("../../src/membership/view") = await vi.importActual<
      typeof import("../../src/membership/view")
    >("../../src/membership/view");
    let probe:
      | {
          readonly callbacks: {
            readonly suspect: (failure: {
              target: string;
              incarnation: number;
              sequence: number;
              effectivePeriod: number;
              periodStart: number;
            }) => boolean;
            readonly updates: (updates: readonly MembershipUpdate[]) => void;
          };
        }
      | undefined;
    let expire: ((expiry: { member: string; incarnation: number }) => void) | undefined;
    let ignoreNextDead: boolean = false;
    let throwNextApply: boolean = false;
    let hideSelf: boolean = false;
    let hideReapOperation: boolean = false;
    let onProbeStart: (() => void) | undefined;
    interface ProbeSeamOptions {
      readonly callbacks: NonNullable<typeof probe>["callbacks"];
    }

    vi.doMock("../../src/membership/probe", () => ({
      BASE_PROTOCOL_PERIOD_MS: 1_000,
      Probe: class {
        constructor(options: ProbeSeamOptions) {
          probe = options;
        }

        get scale(): number {
          return 1;
        }

        start(): Promise<void> {
          onProbeStart?.();
          return Promise.resolve();
        }

        stop(): Promise<void> {
          return Promise.resolve();
        }

        pause(): void {}

        receivePacket(): void {}

        selfRefute(): number {
          return 0;
        }
      },
    }));
    vi.doMock("../../src/membership/suspicion", () => ({
      SuspicionManager: class {
        constructor(_clock: Clock, callback: typeof expire) {
          expire = callback;
        }

        start(): boolean {
          return true;
        }

        cancelAll(): void {}

        cancelThrough(): void {}

        confirm(): boolean {
          return false;
        }
      },
    }));
    vi.doMock("../../src/membership/view", () => ({
      ...actualView,
      MembershipView: class extends actualView.MembershipView {
        override applyLocal(update: MembershipUpdate, now: number): ApplyResult {
          if (ignoreNextDead && update.state === STATE_DEAD) {
            ignoreNextDead = false;
            return { kind: "ignored" as const };
          }

          return super.applyLocal(update, now);
        }

        override apply(
          update: MembershipUpdate,
          now: number,
          selfStateChangeTime?: bigint,
        ): ApplyResult {
          if (throwNextApply) {
            throwNextApply = false;
            throw new Error("unexpected apply failure");
          }

          return selfStateChangeTime === undefined
            ? super.apply(update, now)
            : super.apply(update, now, selfStateChangeTime);
        }

        override self(): MemberRecord | undefined {
          return hideSelf ? undefined : super.self();
        }

        override reapOperation(member: string): ReapOperation | undefined {
          return hideReapOperation ? undefined : super.reapOperation(member);
        }
      },
    }));
    const { Swim: SeamSwim } = await import("../../src/membership/swim");
    const network: SimNetwork = new SimNetwork(211);
    const transport: ControlledTransport = new ControlledTransport("a");
    const swim: Swim = new SeamSwim({
      address: "a",
      metadata: new Uint8Array(0),
      transport,
      clock: network.clock,
      random: network.random,
    });
    await swim.start();
    const failure = (
      target: string,
      incarnation: number,
    ): {
      target: string;
      incarnation: number;
      sequence: number;
      effectivePeriod: number;
      periodStart: number;
    } => ({
      target,
      incarnation,
      sequence: 1,
      effectivePeriod: 1_000,
      periodStart: 0,
    });

    expect(probe?.callbacks.suspect(failure("missing", 0))).toBe(false);
    probe?.callbacks.updates([membershipUpdate("b", STATE_ALIVE, 1)]);
    expect(probe?.callbacks.suspect(failure("b", 0))).toBe(false);
    probe?.callbacks.updates([membershipUpdate("c", STATE_LEFT, 0)]);
    expect(probe?.callbacks.suspect(failure("c", 0))).toBe(false);
    probe?.callbacks.updates([membershipUpdate("b", STATE_SUSPECT, 1, "peer")]);
    // A failed probe of an already-suspected target must still arm the local
    // suspicion timer, so the callback reports that timing may start.
    expect(probe?.callbacks.suspect(failure("b", 1))).toBe(true);

    // A failure that is not a typed unapplicability error must not be
    // swallowed by the packet path's guard.
    throwNextApply = true;
    expect((): void => {
      probe?.callbacks.updates([membershipUpdate("f", STATE_ALIVE, 0)]);
    }).toThrow("unexpected apply failure");

    // Terminal truth whose reap operation evaporates mid-application is a
    // defensive impossibility that must fail loudly, not silently.
    probe?.callbacks.updates([membershipUpdate("g", STATE_ALIVE, 0)]);
    hideReapOperation = true;
    expect((): void => {
      probe?.callbacks.updates([membershipUpdate("g", STATE_DEAD, 0)]);
    }).toThrow(/cannot schedule a reap/);
    hideReapOperation = false;

    expire?.({ member: "missing", incarnation: 0 });
    probe?.callbacks.updates([membershipUpdate("d", STATE_ALIVE, 0)]);
    expire?.({ member: "d", incarnation: 0 });
    probe?.callbacks.updates([membershipUpdate("e", STATE_SUSPECT, 2, "peer")]);
    expire?.({ member: "e", incarnation: 1 });
    ignoreNextDead = true;
    expire?.({ member: "e", incarnation: 2 });

    // Leave without an installed self record rejects through the shared
    // promise instead of throwing past a dangling undefined.
    hideSelf = true;
    await expect(swim.leave()).rejects.toThrow(/local member record was never installed/);
    hideSelf = false;

    await swim.stop();
    expect(probe?.callbacks.suspect(failure("b", 1))).toBe(false);
    expire?.({ member: "e", incarnation: 2 });

    const secondTransport: ControlledTransport = new ControlledTransport("z");
    const second: Swim = new SeamSwim({
      address: "z",
      metadata: new Uint8Array(0),
      transport: secondTransport,
      clock: network.clock,
      random: network.random,
    });
    let secondStopping: Promise<void> | undefined;
    onProbeStart = (): void => {
      secondStopping = second.stop();
    };

    await second.start();
    await secondStopping;
    expect(second.lifecycle).toBe("stopped");
  });
});
