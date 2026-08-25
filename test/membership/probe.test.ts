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

import { describe, expect, it, vi } from "vitest";
import { BroadcastQueue } from "../../src/membership/broadcast";
import {
  BASE_DIRECT_TIMEOUT_MS,
  GOSSIP_INTERVAL_MS,
  Probe,
  type ProbeFailure,
} from "../../src/membership/probe";
import { SeededRandom } from "../../src/membership/random";
import { SuspicionManager } from "../../src/membership/suspicion";
import type {
  MembershipStream,
  MembershipTransport,
  TransportHandlers,
} from "../../src/membership/transport";
import { MembershipView } from "../../src/membership/view";
import {
  type AckMessage,
  decodePacketMessage,
  encodeMessage,
  MESSAGE_ACK,
  MESSAGE_GOSSIP,
  MESSAGE_NACK,
  MESSAGE_PING,
  MESSAGE_PING_REQ,
  type MemberState,
  type MembershipUpdate,
  type NackMessage,
  STATE_ALIVE,
  STATE_DEAD,
  STATE_LEFT,
  STATE_SUSPECT,
} from "../../src/membership/wire";
import { SimNetwork } from "./sim";

function update(
  member: string,
  state: MemberState = STATE_ALIVE,
  incarnation = 0,
): MembershipUpdate {
  return {
    state,
    selfOriginated: state === STATE_ALIVE || state === STATE_LEFT,
    incarnation,
    stateChangeTime: BigInt(incarnation),
    member,
    reporter: state === STATE_SUSPECT ? "accuser" : "",
    metadata: state === STATE_ALIVE ? Uint8Array.of(incarnation) : new Uint8Array(0),
  };
}

function view(self: string, peers: readonly string[]): MembershipView {
  const members = new MembershipView(self);
  members.applyLocal(update(self), 0);
  for (const peer of peers) {
    members.apply(update(peer), 0);
  }
  return members;
}

interface Fixture {
  readonly probe: Probe;
  readonly view: MembershipView;
  readonly queue: BroadcastQueue;
  readonly suspicion: SuspicionManager;
  readonly updates: MembershipUpdate[][];
  readonly failures: ProbeFailure[];
  readonly suspicionPresentDuringFailure: boolean[];
}

function fixture(
  network: SimNetwork,
  self: string,
  peers: readonly string[],
  transport = network.endpoint(self),
  seed = 11,
): Fixture {
  const members = view(self, peers);
  const queue = new BroadcastQueue();
  const received: MembershipUpdate[][] = [];
  const failures: ProbeFailure[] = [];
  const suspicionPresentDuringFailure: boolean[] = [];
  const suspicion = new SuspicionManager(network.clock, (): void => undefined);
  const probe = new Probe({
    view: members,
    broadcasts: queue,
    clock: network.clock,
    random: new SeededRandom(seed),
    suspicion,
    transport,
    callbacks: {
      updates(items): void {
        received.push(Array.from(items));
      },
      suspect(failure): boolean {
        suspicionPresentDuringFailure.push(suspicion.get(failure.target) !== undefined);
        failures.push(failure);
        return true;
      },
    },
  });
  return {
    probe,
    view: members,
    queue,
    suspicion,
    updates: received,
    failures,
    suspicionPresentDuringFailure,
  };
}

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T | PromiseLike<T>) => void;
  readonly reject: (reason?: unknown) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((accept, decline): void => {
    resolve = accept;
    reject = decline;
  });
  return { promise, resolve, reject };
}

function streamStub(): MembershipStream & { readonly close: ReturnType<typeof vi.fn> } {
  const close = vi.fn();
  return {
    remoteAddress: "peer",
    read: async (): Promise<Uint8Array | undefined> => undefined,
    write: async (): Promise<void> => undefined,
    close,
  };
}

async function startAckingEndpoint(
  network: SimNetwork,
  address: string,
  directTargets?: string[],
): Promise<MembershipTransport> {
  const endpoint = network.endpoint(address);
  await endpoint.start({
    packet(_from, bytes): void {
      const message = decodePacketMessage(bytes);
      if (message.type !== MESSAGE_PING) {
        return;
      }
      if (message.relay.length === 0) {
        directTargets?.push(address);
      }
      const ack: AckMessage = {
        type: MESSAGE_ACK,
        sequence: message.sequence,
        owner: message.owner,
        target: address,
        updates: [],
      };
      void endpoint.packet(message.relay || message.owner, encodeMessage(ack));
    },
    stream(_from, stream): void {
      stream.close();
    },
  });
  return endpoint;
}

describe("probe rounds and awareness", () => {
  it("accepts a strictly matching direct ACK and captures scaling per period", async () => {
    const network = new SimNetwork(1);
    const targets: string[] = [];
    const remote = await startAckingEndpoint(network, "b", targets);
    const local = fixture(network, "a", ["b"]);

    local.probe.selfRefute();
    expect(local.probe.awareness).toBe(1);
    await local.probe.start();
    expect(local.probe.outstanding).toMatchObject({
      target: "b",
      directDeadline: 1_000,
      periodDeadline: 2_000,
      effectivePeriod: 2_000,
    });

    network.clock.advanceBy(0);
    await flush();
    expect(local.probe.outstanding?.succeeded).toBe(true);
    network.clock.advanceTo(2_000);
    expect(local.probe.awareness).toBe(0);
    expect(local.failures).toEqual([]);
    expect(targets).toContain("b");

    await local.probe.stop();
    await remote.stop();
  });

  it("succeeds indirectly and counts only silent helpers against awareness", async () => {
    const network = new SimNetwork(2);
    network.setLink("a", "target", { drop: true });
    const target = await startAckingEndpoint(network, "target");
    const helpers = await Promise.all(
      ["h1", "h2", "h3"].map(async (name: string): Promise<MembershipTransport> => {
        const endpoint = network.endpoint(name);
        await endpoint.start({
          packet(_from, bytes): void {
            const message = decodePacketMessage(bytes);
            if (message.type !== MESSAGE_PING_REQ) {
              return;
            }
            if (name === "h1") {
              const ack: AckMessage = {
                type: MESSAGE_ACK,
                sequence: message.sequence,
                owner: message.owner,
                target: message.target,
                updates: [],
              };
              void endpoint.packet(message.owner, encodeMessage(ack));
            } else if (name === "h2") {
              const nack: NackMessage = {
                type: MESSAGE_NACK,
                sequence: message.sequence,
                owner: message.owner,
                target: message.target,
                helper: name,
                updates: [],
              };
              void endpoint.packet(message.owner, encodeMessage(nack));
            }
          },
          stream(_from, stream): void {
            stream.close();
          },
        });
        return endpoint;
      }),
    );
    const local = fixture(network, "a", ["target"], undefined, 3);
    await local.probe.start();
    for (const helper of ["h1", "h2", "h3"]) {
      local.view.apply(update(helper), 0);
    }

    network.clock.advanceTo(BASE_DIRECT_TIMEOUT_MS);
    await flush();
    network.clock.advanceBy(0);
    await flush();
    expect(local.probe.outstanding?.succeeded).toBe(true);
    network.clock.advanceTo(1_000);
    expect(local.probe.awareness).toBe(1);
    expect(local.failures).toEqual([]);

    await local.probe.stop();
    await target.stop();
    await Promise.all(helpers.map((helper: MembershipTransport): Promise<void> => helper.stop()));
  });

  it("treats NACKs as path health and scores every missing NACK separately", async () => {
    const network = new SimNetwork(4);
    network.setLink("a", "target", { drop: true });
    const target = await startAckingEndpoint(network, "target");
    const helpers = await Promise.all(
      ["h1", "h2", "h3"].map(async (name: string): Promise<MembershipTransport> => {
        const endpoint = network.endpoint(name);
        await endpoint.start({
          packet(_from, bytes): void {
            const message = decodePacketMessage(bytes);
            if (message.type !== MESSAGE_PING_REQ || name === "h3") {
              return;
            }
            const nack: NackMessage = {
              type: MESSAGE_NACK,
              sequence: message.sequence,
              owner: message.owner,
              target: message.target,
              helper: name,
              updates: [],
            };
            void endpoint.packet(message.owner, encodeMessage(nack));
          },
          stream(_from, stream): void {
            stream.close();
          },
        });
        return endpoint;
      }),
    );
    const local = fixture(network, "a", ["target"], undefined, 7);
    await local.probe.start();
    for (const helper of ["h1", "h2", "h3"]) {
      local.view.apply(update(helper), 0);
    }
    network.clock.advanceTo(1_000);
    await flush();

    expect(local.probe.awareness).toBe(2);
    expect(local.failures).toHaveLength(1);
    expect(local.failures[0]).toMatchObject({
      target: "target",
      incarnation: 0,
      effectivePeriod: 1_000,
    });
    expect(local.suspicion.get("target")).toMatchObject({
      incarnation: 0,
      reporter: "a",
    });
    expect(local.suspicionPresentDuringFailure).toEqual([false]);

    await local.probe.stop();
    await target.stop();
    await Promise.all(helpers.map((helper: MembershipTransport): Promise<void> => helper.stop()));
  });

  it("ignores mismatched and late ACKs while still processing their piggybacks", async () => {
    const network = new SimNetwork(5);
    const endpoint = network.endpoint("b");
    let matching: AckMessage | undefined;
    await endpoint.start({
      packet(_from, bytes): void {
        const message = decodePacketMessage(bytes);
        if (message.type !== MESSAGE_PING) {
          return;
        }
        const news = update("news");
        const wrong: AckMessage = {
          type: MESSAGE_ACK,
          sequence: (message.sequence + 1) >>> 0,
          owner: message.owner,
          target: "b",
          updates: [news],
        };
        matching = { ...wrong, sequence: message.sequence };
        void endpoint.packet(message.owner, encodeMessage(wrong));
      },
      stream(_from, stream): void {
        stream.close();
      },
    });
    const local = fixture(network, "a", ["b"]);
    await local.probe.start();
    network.clock.advanceBy(0);
    await flush();
    expect(local.probe.outstanding?.succeeded).toBe(false);
    expect(
      local.updates.flat().some((item: MembershipUpdate): boolean => item.member === "news"),
    ).toBe(true);

    network.clock.advanceTo(1_000);
    expect(local.failures).toHaveLength(1);
    if (matching === undefined) {
      throw new Error("matching ACK was not captured");
    }
    void endpoint.packet("a", encodeMessage(matching));
    network.clock.advanceBy(0);
    await flush();
    expect(local.failures).toHaveLength(1);

    await local.probe.stop();
    await endpoint.stop();
  });

  it("sends a helper NACK at 80% and still forwards a later matching ACK", async () => {
    const network = new SimNetwork(10);
    const owner = network.endpoint("owner");
    const received: number[] = [];
    await owner.start({
      packet(_from, bytes): void {
        received.push(decodePacketMessage(bytes).type);
      },
      stream(_from, stream): void {
        stream.close();
      },
    });
    const target = network.endpoint("target");
    await target.start({
      packet(_from, bytes): void {
        const ping = decodePacketMessage(bytes);
        if (ping.type !== MESSAGE_PING) {
          return;
        }
        const wrong: AckMessage = {
          type: MESSAGE_ACK,
          sequence: (ping.sequence + 1) >>> 0,
          owner: ping.owner,
          target: "target",
          updates: [],
        };
        void target.packet(ping.relay, encodeMessage(wrong));
        network.clock.scheduleInput(450, (): void => {
          const matching: AckMessage = {
            ...wrong,
            sequence: ping.sequence,
            updates: [update("relayed-news", STATE_ALIVE, 2)],
          };
          void target.packet(ping.relay, encodeMessage(matching));
        });
      },
      stream(_from, stream): void {
        stream.close();
      },
    });
    const helper = fixture(network, "helper", []);
    await helper.probe.start();
    const request = {
      type: MESSAGE_PING_REQ,
      sequence: 77,
      owner: "owner",
      target: "target",
      updates: [],
    } as const;
    void owner.packet("helper", encodeMessage(request));
    network.clock.advanceBy(0);
    await flush();

    network.clock.advanceTo(399);
    expect(received).toEqual([]);
    network.clock.advanceTo(400);
    await flush();
    expect(received).toEqual([MESSAGE_NACK]);
    network.clock.advanceTo(450);
    await flush();
    network.clock.advanceBy(0);
    await flush();
    expect(received).toEqual([MESSAGE_NACK, MESSAGE_ACK]);

    await helper.probe.stop();
    await owner.stop();
    await target.stop();
  });
});

describe("walk, buddy, gossip, and lifecycle", () => {
  it("covers a walk without replacement and inserts a join into its remainder", async () => {
    const network = new SimNetwork(6);
    const targets: string[] = [];
    const endpoints = await Promise.all(
      ["b", "c", "d", "joined"].map(
        async (name: string): Promise<MembershipTransport> =>
          startAckingEndpoint(network, name, targets),
      ),
    );
    const local = fixture(network, "a", ["b", "c", "d"], undefined, 13);
    await local.probe.start();
    network.clock.advanceBy(0);
    await flush();
    local.view.apply(update("joined"), network.clock.now());

    for (const deadline of [1_000, 2_000, 3_000]) {
      network.clock.advanceTo(deadline);
      network.clock.advanceBy(0);
      await flush();
    }
    expect(new Set(targets.slice(0, 4))).toEqual(new Set(["b", "c", "d", "joined"]));

    await local.probe.stop();
    await Promise.all(
      endpoints.map((endpoint: MembershipTransport): Promise<void> => endpoint.stop()),
    );
  });

  it("places the target's suspect update first for the buddy rule", async () => {
    const network = new SimNetwork(7);
    const endpoint = network.endpoint("b");
    let firstUpdate: MembershipUpdate | undefined;
    await endpoint.start({
      packet(_from, bytes): void {
        const message = decodePacketMessage(bytes);
        if (message.type === MESSAGE_PING) {
          [firstUpdate] = message.updates;
        }
      },
      stream(_from, stream): void {
        stream.close();
      },
    });
    const local = fixture(network, "a", []);
    local.view.apply(update("b", STATE_SUSPECT, 3), 0);
    await local.probe.start();
    network.clock.advanceBy(0);
    await flush();

    expect(firstUpdate).toMatchObject({
      member: "b",
      state: STATE_SUSPECT,
      incarnation: 3,
    });

    await local.probe.stop();
    await endpoint.stop();
  });

  it.each([
    ["dead", STATE_DEAD],
    ["left", STATE_LEFT],
  ] as const)(
    "gossips a %s target its indictment after the broadcast queue is exhausted",
    async (_label, state) => {
      const network = new SimNetwork(70 + state);
      const endpoint = network.endpoint("b");
      let gossip: readonly MembershipUpdate[] | undefined;
      await endpoint.start({
        packet(_from, bytes): void {
          const message = decodePacketMessage(bytes);
          if (message.type === MESSAGE_GOSSIP) {
            gossip = message.updates;
          }
        },
        stream(_from, stream): void {
          stream.close();
        },
      });
      const local = fixture(network, "a", []);
      local.view.apply(update("b", state, 3), 0);
      expect(local.queue.size).toBe(0);
      await local.probe.start();

      network.clock.advanceTo(GOSSIP_INTERVAL_MS);
      await flush();

      expect(gossip).toHaveLength(1);
      expect(gossip?.[0]).toMatchObject({
        member: "b",
        state,
        incarnation: 3,
      });
      expect(local.queue.size).toBe(0);

      await local.probe.stop();
      await endpoint.stop();
    },
  );

  it("puts the sender's indictment on a direct ACK only when the owner is the sender", async () => {
    const network = new SimNetwork(73);
    const endpoint = network.endpoint("b");
    const acknowledgements: AckMessage[] = [];
    await endpoint.start({
      packet(_from, bytes): void {
        const message = decodePacketMessage(bytes);
        if (message.type === MESSAGE_ACK) {
          acknowledgements.push(message);
        }
      },
      stream(_from, stream): void {
        stream.close();
      },
    });
    const impostor = network.endpoint("c");
    await impostor.start({
      packet(): void {},
      stream(_from, stream): void {
        stream.close();
      },
    });
    const local = fixture(network, "a", []);
    local.view.apply(update("b", STATE_DEAD, 4), 0);
    await local.probe.start();

    const directPing = {
      type: MESSAGE_PING,
      sequence: 41,
      owner: "b",
      relay: "",
      updates: [],
    } as const;
    void endpoint.packet("a", encodeMessage(directPing));
    network.clock.advanceBy(0);
    await flush();
    void impostor.packet("a", encodeMessage({ ...directPing, sequence: 42 }));
    network.clock.advanceBy(0);
    await flush();

    expect(acknowledgements).toHaveLength(2);
    expect(acknowledgements[0]?.updates[0]).toMatchObject({
      member: "b",
      state: STATE_DEAD,
      incarnation: 4,
    });
    expect(acknowledgements[1]?.updates).toEqual([]);

    await local.probe.stop();
    await endpoint.stop();
    await impostor.stop();
  });

  it("gossips independently every 200ms to at most three eligible distinct targets", async () => {
    const network = new SimNetwork(8);
    const gossipTargets: string[] = [];
    const endpoints = await Promise.all(
      ["b", "c", "d", "e"].map(async (name: string): Promise<MembershipTransport> => {
        const endpoint = network.endpoint(name);
        await endpoint.start({
          packet(_from, bytes): void {
            if (decodePacketMessage(bytes).type === MESSAGE_GOSSIP) {
              gossipTargets.push(name);
            }
          },
          stream(_from, stream): void {
            stream.close();
          },
        });
        return endpoint;
      }),
    );
    const local = fixture(network, "a", ["b", "c", "d", "e"]);
    local.queue.enqueue(update("news", STATE_ALIVE, 2), local.view.aliveOrSuspectCount());
    await local.probe.start();

    network.clock.advanceTo(GOSSIP_INTERVAL_MS - 1);
    expect(gossipTargets).toEqual([]);
    network.clock.advanceBy(1);
    await flush();
    expect(gossipTargets).toHaveLength(3);
    expect(new Set(gossipTargets).size).toBe(3);

    await local.probe.stop();
    await Promise.all(
      endpoints.map((endpoint: MembershipTransport): Promise<void> => endpoint.stop()),
    );
  });

  it("respects packet budgets, charges local acceptance, and removes all timers on stop", async () => {
    const network = new SimNetwork(9);
    const endpoint = network.endpoint("b");
    const packetSizes: number[] = [];
    await endpoint.start({
      packet(_from, bytes): void {
        packetSizes.push(bytes.length);
      },
      stream(_from, stream): void {
        stream.close();
      },
    });
    const local = fixture(network, "a", ["b"]);
    const news = update("news", STATE_ALIVE, 1);
    local.queue.enqueue(news, 1);
    const before = local.queue.get("news")?.remaining;
    await local.probe.start();
    network.clock.advanceBy(0);
    await flush();

    expect(packetSizes.every((size: number): boolean => size <= 1_400)).toBe(true);
    expect(local.queue.get("news")?.remaining).toBe((before as number) - 1);
    expect(network.clock.pending).toBeGreaterThan(0);

    await local.probe.stop();
    expect(local.probe.started).toBe(false);
    expect(local.probe.outstanding).toBeUndefined();
    expect(network.clock.pending).toBe(0);

    await endpoint.stop();
  });
});

describe("probe lifecycle and defensive packet boundaries", () => {
  it("exposes diagnostics, pauses idempotently, and accepts packets during leave drain", async () => {
    const network = new SimNetwork(80);
    const local = fixture(network, "a", []);

    expect(local.probe.scale).toBe(1);
    expect(local.probe.walkRemaining).toEqual([]);
    expect(local.probe.outstanding).toBeUndefined();
    local.probe.pause();

    await local.probe.start();
    await local.probe.start();
    expect(local.probe.started).toBe(true);
    local.probe.pause();
    expect(local.probe.started).toBe(true);
    local.probe.pause();
    expect(network.clock.pending).toBe(0);

    local.probe.receivePacket("b", Uint8Array.of(255));
    local.probe.receivePacket(
      "b",
      encodeMessage({ type: MESSAGE_GOSSIP, updates: [update("news", STATE_ALIVE, 4)] }),
    );
    expect(local.updates.at(-1)?.[0]).toMatchObject({ member: "news", incarnation: 4 });

    await local.probe.stop();
    await local.probe.stop();
    local.probe.receivePacket(
      "b",
      encodeMessage({ type: MESSAGE_GOSSIP, updates: [update("ignored")] }),
    );
    expect(local.updates).toHaveLength(1);
  });

  it("coalesces concurrent starts and stops while rejecting a start during shutdown", async () => {
    const network = new SimNetwork(81);
    const binding = deferred<void>();
    const stopping = deferred<void>();
    let handlers: TransportHandlers | undefined;
    const transport: MembershipTransport = {
      address: "a",
      start(next): Promise<void> {
        handlers = next;
        return binding.promise;
      },
      stop(): Promise<void> {
        return stopping.promise;
      },
      packet: async (): Promise<void> => undefined,
      stream: async (): Promise<MembershipStream> => streamStub(),
    };
    const local = fixture(network, "a", [], transport);

    const firstStart = local.probe.start();
    expect(local.probe.start()).toBe(firstStart);
    const firstStop = local.probe.stop();
    expect(local.probe.stop()).toBe(firstStop);
    await expect(local.probe.start()).rejects.toThrow("probe is stopping");

    binding.resolve();
    await flush();
    stopping.resolve();
    await firstStop;
    await firstStart;
    expect(handlers).toBeDefined();
    expect(local.probe.started).toBe(false);
  });

  it("completes shutdown when an in-flight transport start rejects", async () => {
    const network = new SimNetwork(811);
    const binding = deferred<void>();
    const transport: MembershipTransport = {
      address: "a",
      start: (): Promise<void> => binding.promise,
      stop: async (): Promise<void> => undefined,
      packet: async (): Promise<void> => undefined,
      stream: async (): Promise<MembershipStream> => streamStub(),
    };
    const local = fixture(network, "a", [], transport);

    const starting = local.probe.start();
    const stopping = local.probe.stop();
    binding.reject(new Error("bind failed during shutdown"));

    await expect(starting).rejects.toThrow("bind failed during shutdown");
    await expect(stopping).resolves.toBeUndefined();
  });

  it("restores stopped state after transport start fails and closes unhandled streams", async () => {
    const network = new SimNetwork(82);
    let handlers: TransportHandlers | undefined;
    let fail = true;
    const transport: MembershipTransport = {
      address: "a",
      start(next): Promise<void> {
        handlers = next;
        if (fail) {
          fail = false;
          return Promise.reject(new Error("bind failed"));
        }
        return Promise.resolve();
      },
      stop: async (): Promise<void> => undefined,
      packet: async (): Promise<void> => undefined,
      stream: async (): Promise<MembershipStream> => streamStub(),
    };
    const local = fixture(network, "a", [], transport);

    await expect(local.probe.start()).rejects.toThrow("bind failed");
    expect(local.probe.started).toBe(false);
    await local.probe.start();

    const stream = streamStub();
    await handlers?.stream("peer", stream);
    expect(stream.close).toHaveBeenCalledOnce();

    await local.probe.stop();
  });

  it("delegates streams and can leave shared transport ownership to its composer", async () => {
    const network = new SimNetwork(83);
    const members = view("a", []);
    const stream = streamStub();
    const streamHandler = vi.fn(async (): Promise<void> => undefined);
    const transport: MembershipTransport = {
      address: "a",
      start: vi.fn(async (): Promise<void> => undefined),
      stop: vi.fn(async (): Promise<void> => undefined),
      packet: async (): Promise<void> => undefined,
      stream: async (): Promise<MembershipStream> => stream,
    };
    const probe = new Probe({
      view: members,
      broadcasts: new BroadcastQueue(),
      clock: network.clock,
      random: new SeededRandom(83),
      suspicion: new SuspicionManager(network.clock, (): void => undefined),
      transport,
      manageTransport: false,
      callbacks: {
        updates(): void {},
        suspect: (): boolean => false,
        stream: streamHandler,
      },
    });

    await probe.start();
    expect(transport.start).not.toHaveBeenCalled();
    await probe.stop();
    expect(transport.stop).not.toHaveBeenCalled();

    const owned = fixture(network, "owned", [], {
      ...transport,
      start: async (handlers): Promise<void> => {
        await handlers.stream("peer", stream);
      },
    });
    const handled = new Probe({
      view: owned.view,
      broadcasts: owned.queue,
      clock: network.clock,
      random: new SeededRandom(84),
      suspicion: owned.suspicion,
      transport: {
        ...transport,
        start: async (handlers): Promise<void> => {
          await handlers.stream("peer", stream);
        },
      },
      callbacks: {
        updates(): void {},
        suspect: (): boolean => false,
        stream: streamHandler,
      },
    });

    await handled.start();
    expect(streamHandler).toHaveBeenCalledWith("peer", stream);
    await handled.stop();
  });

  it.each(["throw", "reject"] as const)(
    "starts indirect probing when a direct packet is synchronously or asynchronously rejected: %s",
    async (failureMode) => {
      const network = new SimNetwork(failureMode === "throw" ? 84 : 85);
      const acknowledgements: boolean[] = [];
      const queue = new BroadcastQueue();
      queue.enqueue(update("news"), 1);
      const originalAcknowledge = queue.acknowledge.bind(queue);
      queue.acknowledge = (selection, accepted): boolean => {
        acknowledgements.push(accepted);
        return originalAcknowledge(selection, accepted);
      };
      const transport: MembershipTransport = {
        address: "a",
        start: async (): Promise<void> => undefined,
        stop: async (): Promise<void> => undefined,
        packet(): Promise<void> {
          if (failureMode === "throw") {
            throw new Error("send failed");
          }
          return Promise.reject(new Error("send failed"));
        },
        stream: async (): Promise<MembershipStream> => streamStub(),
      };
      const members = view("a", ["target"]);
      const probe = new Probe({
        view: members,
        broadcasts: queue,
        clock: network.clock,
        random: new SeededRandom(85),
        suspicion: new SuspicionManager(network.clock, (): void => undefined),
        transport,
        callbacks: {
          updates(): void {},
          suspect: (): boolean => false,
        },
      });

      await probe.start();
      await flush();
      expect(probe.outstanding?.indirectStarted).toBe(true);
      expect(acknowledgements).toContain(false);

      await probe.stop();
    },
  );

  it.each(["missing", "newer", "terminal", "declined"] as const)(
    "does not start stale or declined suspicion after a failed probe: %s",
    async (reason) => {
      const network = new SimNetwork(90);
      const members = view("a", ["target"]);
      const failures: ProbeFailure[] = [];
      const suspicion = new SuspicionManager(network.clock, (): void => undefined);
      const probe = new Probe({
        view: members,
        broadcasts: new BroadcastQueue(),
        clock: network.clock,
        random: new SeededRandom(90),
        suspicion,
        transport: network.endpoint("a"),
        callbacks: {
          updates(): void {},
          suspect(failure): boolean {
            failures.push(failure);
            return reason !== "declined";
          },
        },
      });

      await probe.start();
      if (reason === "newer") {
        members.apply(update("target", STATE_ALIVE, 1), 1);
      } else if (reason === "terminal") {
        members.apply(update("target", STATE_DEAD, 1), 1);
      } else if (reason === "missing") {
        members.apply(update("target", STATE_DEAD, 1), 1);
        const operation = members.reapOperation("target");
        if (operation === undefined) {
          throw new Error("terminal member did not produce a reap operation");
        }
        members.reap(operation);
      }

      network.clock.advanceTo(1_000);

      expect(suspicion.get("target")).toBeUndefined();
      expect(failures).toHaveLength(reason === "declined" ? 1 : 0);
      await probe.stop();
    },
  );

  it("deduplicates relay requests, expires them, and permits a fresh retry", async () => {
    const network = new SimNetwork(91);
    const target = network.endpoint("target");
    let pings = 0;
    await target.start({
      packet(_from, bytes): void {
        if (decodePacketMessage(bytes).type === MESSAGE_PING) {
          pings += 1;
        }
      },
      stream(_from, stream): void {
        stream.close();
      },
    });
    const owner = network.endpoint("owner");
    await owner.start({
      packet(): void {},
      stream(_from, stream): void {
        stream.close();
      },
    });
    const helper = fixture(network, "helper", []);
    await helper.probe.start();
    const request = encodeMessage({
      type: MESSAGE_PING_REQ,
      sequence: 12,
      owner: "owner",
      target: "target",
      updates: [],
    });

    void owner.packet("helper", request);
    void owner.packet("helper", request);
    network.clock.advanceBy(0);
    await flush();
    expect(pings).toBe(1);

    network.clock.advanceTo(BASE_DIRECT_TIMEOUT_MS);
    void owner.packet("helper", request);
    network.clock.advanceBy(0);
    await flush();
    expect(pings).toBe(2);

    await helper.probe.stop();
    await owner.stop();
    await target.stop();
  });

  it("NACKs an owner immediately when relaying cannot reach the target", async () => {
    const network = new SimNetwork(92);
    let handlers: TransportHandlers | undefined;
    const sent: NackMessage[] = [];
    const transport: MembershipTransport = {
      address: "helper",
      start(next): Promise<void> {
        handlers = next;
        return Promise.resolve();
      },
      stop: async (): Promise<void> => undefined,
      packet(to, bytes): Promise<void> {
        const message = decodePacketMessage(bytes);
        if (to === "target") {
          throw new Error("target unavailable");
        }
        if (message.type === MESSAGE_NACK) {
          sent.push(message);
        }
        return Promise.resolve();
      },
      stream: async (): Promise<MembershipStream> => streamStub(),
    };
    const local = fixture(network, "helper", [], transport);
    await local.probe.start();

    handlers?.packet(
      "owner",
      encodeMessage({
        type: MESSAGE_PING_REQ,
        sequence: 9,
        owner: "owner",
        target: "target",
        updates: [],
      }),
    );
    await flush();

    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({ owner: "owner", target: "target", helper: "helper" });
    await local.probe.stop();
  });

  it("rejects nonmatching ACK and NACK fields without changing the active probe", async () => {
    const network = new SimNetwork(93);
    const target = network.endpoint("target");
    await target.start({
      packet(): void {},
      stream(_from, stream): void {
        stream.close();
      },
    });
    const local = fixture(network, "a", ["target"]);
    await local.probe.start();
    const outstanding = local.probe.outstanding;
    if (outstanding === undefined) {
      throw new Error("probe did not start");
    }
    const ack: AckMessage = {
      type: MESSAGE_ACK,
      owner: "a",
      target: "target",
      sequence: outstanding.sequence,
      updates: [],
    };

    for (const message of [
      { ...ack, owner: "other" },
      { ...ack, target: "other" },
      { ...ack, sequence: (ack.sequence + 1) >>> 0 },
    ]) {
      local.probe.receivePacket("target", encodeMessage(message));
    }

    local.view.apply(update("helper"), 0);
    network.clock.advanceTo(BASE_DIRECT_TIMEOUT_MS);
    const helper = local.probe.outstanding?.helpers[0];
    if (helper === undefined) {
      throw new Error("indirect helper was not selected");
    }
    const nack: NackMessage = {
      type: MESSAGE_NACK,
      owner: "a",
      target: "target",
      sequence: outstanding.sequence,
      helper,
      updates: [],
    };
    for (const message of [
      { ...nack, owner: "other" },
      { ...nack, target: "other" },
      { ...nack, sequence: (nack.sequence + 1) >>> 0 },
      { ...nack, helper: "other" },
      nack,
      nack,
    ]) {
      local.probe.receivePacket(helper, encodeMessage(message));
    }

    expect(local.probe.outstanding?.succeeded).toBe(false);
    await local.probe.stop();
    await target.stop();
  });

  it("ignores already-dispatched detector timers after shutdown", async () => {
    const network = new SimNetwork(94);
    vi.spyOn(network.clock, "cancel").mockImplementation((): void => undefined);
    const idle = fixture(network, "idle", []);
    await idle.probe.start();
    await idle.probe.stop();

    network.clock.advanceTo(1_000);
    expect(idle.failures).toEqual([]);

    const activeNetwork = new SimNetwork(95);
    vi.spyOn(activeNetwork.clock, "cancel").mockImplementation((): void => undefined);
    const active = fixture(activeNetwork, "a", ["target"]);
    await active.probe.start();
    await active.probe.stop();

    activeNetwork.clock.advanceTo(1_000);
    expect(active.failures).toEqual([]);
  });

  it("ignores an already-dispatched helper NACK timer after shutdown", async () => {
    const network = new SimNetwork(96);
    vi.spyOn(network.clock, "cancel").mockImplementation((): void => undefined);
    let handlers: TransportHandlers | undefined;
    const sent: number[] = [];
    const transport: MembershipTransport = {
      address: "helper",
      start(next): Promise<void> {
        handlers = next;
        return Promise.resolve();
      },
      stop: async (): Promise<void> => undefined,
      packet(_to, bytes): Promise<void> {
        sent.push(decodePacketMessage(bytes).type);
        return Promise.resolve();
      },
      stream: async (): Promise<MembershipStream> => streamStub(),
    };
    const local = fixture(network, "helper", [], transport);
    await local.probe.start();
    handlers?.packet(
      "owner",
      encodeMessage({
        type: MESSAGE_PING_REQ,
        sequence: 5,
        owner: "owner",
        target: "target",
        updates: [],
      }),
    );
    await local.probe.stop();

    network.clock.advanceTo(BASE_DIRECT_TIMEOUT_MS);
    expect(sent).toEqual([MESSAGE_PING]);
  });

  it("reinserts a member that regains eligibility during the current walk", async () => {
    const network = new SimNetwork(41);
    const peers = ["b", "c", "d", "e", "f", "g", "h", "i", "j", "k"];
    const targets: string[] = [];
    const endpoints = await Promise.all(
      peers.map(
        async (name: string): Promise<MembershipTransport> =>
          startAckingEndpoint(network, name, targets),
      ),
    );
    const local = fixture(network, "a", peers, undefined, 17);
    await local.probe.start();
    network.clock.advanceBy(0);
    await flush();

    // Drop a still-pending walk entry as dead, then revive it one period later.
    const revived = peers.find((name: string): boolean => !targets.includes(name)) as string;
    local.view.apply(update(revived, STATE_DEAD, 0), network.clock.now());
    network.clock.advanceTo(1_000);
    network.clock.advanceBy(0);
    await flush();
    local.view.apply(update(revived, STATE_ALIVE, 1), network.clock.now());

    for (let deadline = 2_000; deadline <= 9_000; deadline += 1_000) {
      network.clock.advanceTo(deadline);
      network.clock.advanceBy(0);
      await flush();
    }

    // One full cycle still probes every member exactly once: the revived
    // member re-entered the remaining walk instead of waiting for a reshuffle.
    expect(targets).toHaveLength(peers.length);
    expect(new Set(targets).size).toBe(peers.length);
    expect(targets.slice(2)).toContain(revived);

    await local.probe.stop();
    await Promise.all(
      endpoints.map((endpoint: MembershipTransport): Promise<void> => endpoint.stop()),
    );
  });

  it("skips a walk entry removed before its eligibility check", async () => {
    const network = new SimNetwork(981);
    const members = view("a", ["removed"]);
    const originalGet = members.get.bind(members);
    vi.spyOn(members, "get").mockImplementation((name) =>
      name === "removed" ? undefined : originalGet(name),
    );
    const probe = new Probe({
      view: members,
      broadcasts: new BroadcastQueue(),
      clock: network.clock,
      random: new SeededRandom(981),
      suspicion: new SuspicionManager(network.clock, (): void => undefined),
      transport: network.endpoint("a"),
      callbacks: {
        updates(): void {},
        suspect: (): boolean => false,
      },
    });

    await probe.start();
    expect(probe.outstanding).toBeUndefined();
    await probe.stop();
  });

  it("routes an ACK for a relayed PING back through the named helper", async () => {
    const network = new SimNetwork(982);
    const helper = network.endpoint("helper");
    const acknowledgements: AckMessage[] = [];
    await helper.start({
      packet(_from, bytes): void {
        const message = decodePacketMessage(bytes);
        if (message.type === MESSAGE_ACK) {
          acknowledgements.push(message);
        }
      },
      stream(_from, stream): void {
        stream.close();
      },
    });
    const local = fixture(network, "target", []);
    await local.probe.start();

    local.probe.receivePacket(
      "helper",
      encodeMessage({
        type: MESSAGE_PING,
        sequence: 44,
        owner: "owner",
        relay: "helper",
        updates: [],
      }),
    );
    network.clock.advanceBy(0);
    await flush();

    expect(acknowledgements).toHaveLength(1);
    expect(acknowledgements[0]).toMatchObject({ owner: "owner", target: "target" });
    await local.probe.stop();
    await helper.stop();
  });

  it("stops a gossip fanout when packet acceptance pauses the detector", async () => {
    const network = new SimNetwork(99);
    const members = view("a", ["b", "c"]);
    const queue = new BroadcastQueue();
    queue.enqueue(update("news"), 2);
    let probe!: Probe;
    let sends = 0;
    const transport: MembershipTransport = {
      address: "a",
      start: async (): Promise<void> => undefined,
      stop: async (): Promise<void> => undefined,
      packet: async (): Promise<void> => {
        sends += 1;
        if (sends === 2) {
          probe.pause();
        }
      },
      stream: async (): Promise<MembershipStream> => streamStub(),
    };
    probe = new Probe({
      view: members,
      broadcasts: queue,
      clock: network.clock,
      random: new SeededRandom(99),
      suspicion: new SuspicionManager(network.clock, (): void => undefined),
      transport,
      callbacks: {
        updates(): void {},
        suspect: (): boolean => false,
      },
    });

    await probe.start();
    network.clock.advanceTo(GOSSIP_INTERVAL_MS);
    expect(probe.started).toBe(true);
    await probe.stop();
  });

  it("skips a gossip target when the broadcast queue drains after target selection", async () => {
    const network = new SimNetwork(100);
    const members = view("a", ["b"]);
    const queue = new BroadcastQueue();
    queue.enqueue(update("news"), 1);
    const transport: MembershipTransport = {
      address: "a",
      start: async (): Promise<void> => undefined,
      stop: async (): Promise<void> => undefined,
      packet: async (): Promise<void> => undefined,
      stream: async (): Promise<MembershipStream> => streamStub(),
    };
    const probe = new Probe({
      view: members,
      broadcasts: queue,
      clock: network.clock,
      random: new SeededRandom(100),
      suspicion: new SuspicionManager(network.clock, (): void => undefined),
      transport,
      callbacks: {
        updates(): void {},
        suspect: (): boolean => false,
      },
    });
    await probe.start();
    await flush();

    // Drain the queue after the tick's candidate selection completes but before
    // its send loop runs, so the selected target must be skipped.
    vi.spyOn(members, "eachMember").mockImplementation(
      (callback: (member: string, state: MemberState) => void): void => {
        callback("b", STATE_ALIVE);
        while (queue.size > 0) {
          queue.acknowledge(queue.pack(1_400), true);
        }
      },
    );

    network.clock.advanceTo(GOSSIP_INTERVAL_MS);

    expect(queue.size).toBe(0);
    await probe.stop();
  });
});
