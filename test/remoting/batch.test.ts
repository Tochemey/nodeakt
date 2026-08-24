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

import { afterEach, describe, expect, it } from "vitest";
import type { Actor } from "../../src/actor";
import type { ActorSystem } from "../../src/actor.system";
import { ErrDead } from "../../src/errors";
import { Deadletter, PostStart } from "../../src/messages";
import { type DataEnvelope, KIND_ASK, KIND_TELL, SERIALIZER_BINARY } from "../../src/net/envelope";
import { PeerError, type Session } from "../../src/net/session";
import { ByteWriter, encodeValue } from "../../src/net/values";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { registerMessage } from "../../src/registration";
import { BATCH_TYPE_REF } from "../../src/remoting";
import { cleanupNet, dialSession } from "../net/helpers";
import { remoteSystem, sleep, until, withSystems } from "./helpers";

/**
 * The tell coalescer. Tells fired within one turn for the same
 * sender-receiver pair cross as one batch envelope; the receiver
 * unpacks entries and delivers them one at a time, in send order, so
 * an actor still receives exactly one message per turn and FIFO holds
 * no matter how the messages traveled. These tests pin the ordering
 * contract (bursts, the ask fence, oversize messages), the failure
 * taxonomy (per-entry versus structural, dead-letter fan-out, stop
 * drain), and the receiver's robustness against hostile batches.
 */

class BatchProbe {
  constructor(readonly n: number) {}
}

class BatchAlt {
  constructor(readonly tag: string) {}
}

class BatchQuery {
  constructor(readonly n: number) {}
}

class BatchAnswer {
  constructor(readonly n: number) {}
}

class BatchBig {
  constructor(
    readonly n: number,
    readonly blob: Uint8Array,
  ) {}
}

registerMessage(BatchProbe);
registerMessage(BatchAlt);
registerMessage(BatchQuery);
registerMessage(BatchAnswer);
registerMessage(BatchBig);

/** Records every delivery in arrival order and answers queries. */
class Collector implements Actor {
  readonly received: unknown[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.received.push(ctx.message);
    if (ctx.message instanceof BatchQuery) {
      ctx.response(new BatchAnswer(ctx.message.n));
    }
  }

  postStop(): void {}
}

function payloadOf(value: unknown): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, value);
  return Uint8Array.from(writer.bytes());
}

/** Builds one batch payload in the seam's own layout: the type table,
 * the entry count, then (type index, length, bytes) per entry. */
function batchPayload(types: string[], entries: { index: number; value: unknown }[]): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  writer.writeUvarint(types.length);
  for (const type of types) {
    writer.writeString(type);
  }

  writer.writeUvarint(entries.length);
  for (const entry of entries) {
    const bytes: Uint8Array = payloadOf(entry.value);
    writer.writeUvarint(entry.index);
    writer.writeUvarint(bytes.length);
    writer.writeBytes(bytes);
  }

  return Uint8Array.from(writer.bytes());
}

/** One inbound batch envelope from a forged foreign sender. */
function batchEnvelope(to: string, payload: Uint8Array, sender: string): DataEnvelope {
  return {
    kind: KIND_TELL,
    to,
    uid: "",
    sender,
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: BATCH_TYPE_REF,
    payload,
  };
}

/** The dead letters a system publishes, captured for assertions. */
function captureDeadletters(system: ActorSystem): Deadletter[] {
  const seen: Deadletter[] = [];
  system.subscribe((event: unknown): void => {
    if (event instanceof Deadletter) {
      seen.push(event);
    }
  });
  return seen;
}

afterEach(cleanupNet);

describe("tell coalescing order", () => {
  it("delivers a burst past every cap in send order, prototypes intact", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      await b.spawn("sink", collector);
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;

      // Three times the entry cap in one turn: the coalescer emits at
      // its caps mid-burst and flushes the tail at the microtask, and
      // none of that may reorder a single message.
      const total: number = 600;
      for (let i: number = 0; i < total; i++) {
        expect(a.noSender().tell(pid, new BatchProbe(i))).toBeNull();
      }

      await until("the burst to arrive", (): boolean => collector.received.length >= total);
      collector.received.forEach((message: unknown, i: number): void => {
        expect(message).toBeInstanceOf(BatchProbe);
        expect((message as BatchProbe).n).toBe(i);
      });
    });
  });

  it("carries mixed classes and passthrough data in one turn, in order", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      await b.spawn("sink", collector);
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;

      expect(a.noSender().tell(pid, new BatchProbe(1))).toBeNull();
      expect(a.noSender().tell(pid, new BatchAlt("two"))).toBeNull();
      expect(a.noSender().tell(pid, { plain: 3 })).toBeNull();
      expect(a.noSender().tell(pid, new BatchProbe(4))).toBeNull();

      await until("the mixed burst", (): boolean => collector.received.length >= 4);
      expect(collector.received[0]).toBeInstanceOf(BatchProbe);
      expect(collector.received[1]).toBeInstanceOf(BatchAlt);
      expect((collector.received[1] as BatchAlt).tag).toBe("two");
      expect(collector.received[2]).toEqual({ plain: 3 });
      expect(collector.received[3]).toBeInstanceOf(BatchProbe);
      expect((collector.received[3] as BatchProbe).n).toBe(4);
    });
  });

  it("never lets an ask overtake the tells buffered before it", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      await b.spawn("sink", collector);
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;

      // Three tells and an ask in the same synchronous turn: the ask
      // flushes the coalescer first, so the receiver's arrival order
      // is exactly the send order.
      expect(a.noSender().tell(pid, new BatchProbe(0))).toBeNull();
      expect(a.noSender().tell(pid, new BatchProbe(1))).toBeNull();
      expect(a.noSender().tell(pid, new BatchProbe(2))).toBeNull();
      const answer: unknown = await a.noSender().ask(pid, new BatchQuery(3), 2000);

      expect(answer).toBeInstanceOf(BatchAnswer);
      expect(collector.received.length).toBe(4);
      expect(
        collector.received.map((message: unknown): number =>
          message instanceof BatchProbe ? message.n : (message as BatchQuery).n,
        ),
      ).toEqual([0, 1, 2, 3]);
      expect(collector.received[3]).toBeInstanceOf(BatchQuery);
    });
  });

  it("flushes only the asked node, and the other node's tells still arrive", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const third: ActorSystem = remoteSystem("gamma");
      await third.start();

      try {
        const collector: Collector = new Collector();
        await third.spawn("sink", collector);
        const echo: Collector = new Collector();
        await b.spawn("echo", echo);

        const far: PID = (await a.remoteLookup(third.host(), third.port(), "sink")) as PID;
        const near: PID = (await a.remoteLookup(b.host(), b.port(), "echo")) as PID;

        // Tells to gamma buffer; the ask to beta flushes only beta's
        // pending, stepping over gamma's batch, which the microtask
        // then delivers untouched.
        expect(a.noSender().tell(far, new BatchProbe(1))).toBeNull();
        expect(a.noSender().tell(far, new BatchProbe(2))).toBeNull();
        const answer: unknown = await a.noSender().ask(near, new BatchQuery(9), 2000);
        expect(answer).toBeInstanceOf(BatchAnswer);

        await until("gamma's tells", (): boolean => collector.received.length >= 2);
        expect((collector.received[0] as BatchProbe).n).toBe(1);
        expect((collector.received[1] as BatchProbe).n).toBe(2);
      } finally {
        await third.stop();
      }
    });
  });

  it("sends an oversize message alone, after the pair's buffered tells", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      await b.spawn("sink", collector);
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;

      // Two small tells buffer, then a message past the batch byte cap
      // flushes them first and travels solo, so arrival order is send
      // order even across the two shapes.
      expect(a.noSender().tell(pid, new BatchProbe(1))).toBeNull();
      expect(a.noSender().tell(pid, new BatchProbe(2))).toBeNull();
      expect(a.noSender().tell(pid, new BatchBig(3, new Uint8Array(80 * 1024)))).toBeNull();

      await until("all three to arrive", (): boolean => collector.received.length >= 3);
      expect((collector.received[0] as BatchProbe).n).toBe(1);
      expect((collector.received[1] as BatchProbe).n).toBe(2);
      expect(collector.received[2]).toBeInstanceOf(BatchBig);
      expect((collector.received[2] as BatchBig).blob.length).toBe(80 * 1024);
    });
  });

  it("sends a senderless oversize message through the ref path", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const collector: Collector = new Collector();
      await b.spawn("sink", collector);
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;

      expect(pid.ref().tell(new BatchBig(1, new Uint8Array(80 * 1024)))).toBeNull();
      await until("the big tell", (): boolean => collector.received.length >= 1);
      expect(collector.received[0]).toBeInstanceOf(BatchBig);
    });
  });
});

describe("tell coalescing failures", () => {
  it("dead-letters every buffered tell when the system stops mid-turn", async () => {
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await b.spawn("sink", new Collector());
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;
      const deadletters: Deadletter[] = captureDeadletters(a);

      // Accepted tells from an attributed sender and a senderless ref,
      // then the stop lands in the same turn: the coalescer's holdings
      // can no longer travel, so each becomes its own dead letter with
      // its message restored, attributed or not exactly as sent.
      expect(a.noSender().tell(pid, new BatchProbe(1))).toBeNull();
      expect(a.noSender().tell(pid, new BatchProbe(2))).toBeNull();
      expect(a.noSender().tell(pid, new BatchProbe(3))).toBeNull();
      expect(pid.ref().tell(new BatchProbe(4))).toBeNull();
      expect(pid.ref().tell(new BatchProbe(5))).toBeNull();
      await a.stop();

      expect(deadletters.length).toBe(5);
      deadletters.forEach((letter: Deadletter, i: number): void => {
        expect(letter.reason).toBe(ErrDead.message);
        expect(letter.message).toBeInstanceOf(BatchProbe);
        expect((letter.message as BatchProbe).n).toBe(i + 1);
      });

      // The ref tells carried no sender, and their dead letters say so.
      expect((deadletters[3] as Deadletter).sender).toBeUndefined();
      expect((deadletters[0] as Deadletter).sender).toBeDefined();
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("fans a batch the peer could not deliver out into one dead letter per tell", async () => {
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await b.spawn("sink", new Collector());
      const pid: PID = (await a.remoteLookup(b.host(), b.port(), "sink")) as PID;
      await b.stop();
      await sleep(50);

      const deadletters: Deadletter[] = captureDeadletters(a);

      // One turn's burst to the dead node rides one batch envelope;
      // the failed dial must fan it back out into the five original
      // messages, not one opaque batch.
      for (let i: number = 0; i < 5; i++) {
        expect(a.noSender().tell(pid, new BatchProbe(i))).toBeNull();
      }

      await until("the fan-out", (): boolean => deadletters.length >= 5);
      deadletters.slice(0, 5).forEach((letter: Deadletter, i: number): void => {
        expect(letter.message).toBeInstanceOf(BatchProbe);
        expect((letter.message as BatchProbe).n).toBe(i);
        expect(letter.receiver).toBe(pid.path().toString());
      });
    } finally {
      await a.stop();
      await b.stop();
    }
  });
});

describe("hostile inbound batches", () => {
  it("answers a batch that claims to be an ask with a request-scoped refusal", async () => {
    const system: ActorSystem = remoteSystem("batchy");
    await system.start();

    try {
      const sink: PID = await system.spawn("sink", new Collector());
      const session: Session = await dialSession(system.port());

      const payload: Uint8Array = batchPayload(["BatchProbe"], [{ index: 0, value: { n: 1 } }]);
      const rejection: Promise<unknown> = session.ask(
        { ...batchEnvelope(sink.path().toString(), payload, ""), kind: KIND_ASK },
        2000,
      );
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof PeerError && err.message.includes("settles no ask");
      });
    } finally {
      await system.stop();
    }
  });

  it("survives a batch whose payload is garbage and keeps serving", async () => {
    const system: ActorSystem = remoteSystem("batchy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const session: Session = await dialSession(system.port());
      const deadletters: Deadletter[] = captureDeadletters(system);

      const garbage: Uint8Array = Uint8Array.of(0xff, 0xfe, 0xfd, 0xfc, 0xfb);
      expect(session.tell(batchEnvelope(sink.path().toString(), garbage, ""))).toBeNull();
      await until("the malformed batch to dead-letter", (): boolean => deadletters.length >= 1);

      // The connection lives and clean traffic still flows.
      const clean: Uint8Array = batchPayload(
        ["BatchProbe"],
        [
          { index: 0, value: { n: 7 } },
          { index: 0, value: { n: 8 } },
        ],
      );
      expect(session.tell(batchEnvelope(sink.path().toString(), clean, ""))).toBeNull();
      await until("the clean batch", (): boolean => collector.received.length >= 2);
      expect((collector.received[0] as BatchProbe).n).toBe(7);
      expect((collector.received[1] as BatchProbe).n).toBe(8);
    } finally {
      await system.stop();
    }
  });

  it("refuses declared counts over the coalescer caps before any delivery", async () => {
    const system: ActorSystem = remoteSystem("batchy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const session: Session = await dialSession(system.port());
      const deadletters: Deadletter[] = captureDeadletters(system);

      // A conforming sender flushes at the entry cap, so a batch
      // declaring more is a violation: one frame must not amplify into
      // an unbounded synchronous delivery loop. Same for the type
      // table's declared size.
      const overEntries: ByteWriter = new ByteWriter();
      overEntries.writeUvarint(1);
      overEntries.writeString("BatchProbe");
      overEntries.writeUvarint(1_000_000);
      expect(
        session.tell(
          batchEnvelope(sink.path().toString(), Uint8Array.from(overEntries.bytes()), ""),
        ),
      ).toBeNull();

      const overTypes: ByteWriter = new ByteWriter();
      overTypes.writeUvarint(1_000_000);
      expect(
        session.tell(batchEnvelope(sink.path().toString(), Uint8Array.from(overTypes.bytes()), "")),
      ).toBeNull();

      await until("both refusals to dead-letter", (): boolean => deadletters.length >= 2);
      expect((deadletters[0] as Deadletter).reason).toContain("over the");
      expect((deadletters[1] as Deadletter).reason).toContain("over the");
      expect(collector.received.length).toBe(0);
    } finally {
      await system.stop();
    }
  });

  it("dead-letters the remainder on a structural violation mid-stream", async () => {
    const system: ActorSystem = remoteSystem("batchy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const session: Session = await dialSession(system.port());
      const deadletters: Deadletter[] = captureDeadletters(system);

      // A valid head, one good entry, then an entry naming a type the
      // table does not hold: the stream cannot be resynchronized past
      // a violation, so the good prefix delivers and the rest fails.
      const writer: ByteWriter = new ByteWriter();
      writer.writeUvarint(1);
      writer.writeString("BatchProbe");
      writer.writeUvarint(2);
      const good: Uint8Array = payloadOf({ n: 1 });
      writer.writeUvarint(0);
      writer.writeUvarint(good.length);
      writer.writeBytes(good);
      writer.writeUvarint(9);
      writer.writeUvarint(good.length);
      writer.writeBytes(good);

      expect(
        session.tell(batchEnvelope(sink.path().toString(), Uint8Array.from(writer.bytes()), "")),
      ).toBeNull();
      await until("the violation to dead-letter", (): boolean => deadletters.length >= 1);
      await until("the good prefix", (): boolean => collector.received.length >= 1);
      expect((collector.received[0] as BatchProbe).n).toBe(1);
      expect((deadletters[0] as Deadletter).reason).toContain("names type 9");
    } finally {
      await system.stop();
    }
  });

  it("dead-letters an undecodable entry alone and delivers the rest", async () => {
    const system: ActorSystem = remoteSystem("batchy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const session: Session = await dialSession(system.port());
      const deadletters: Deadletter[] = captureDeadletters(system);

      const payload: Uint8Array = batchPayload(
        ["BatchProbe", "NeverRegisteredBatchType"],
        [
          { index: 0, value: { n: 1 } },
          { index: 1, value: { n: 2 } },
          { index: 0, value: { n: 3 } },
        ],
      );
      expect(session.tell(batchEnvelope(sink.path().toString(), payload, ""))).toBeNull();

      await until("the two good entries", (): boolean => collector.received.length >= 2);
      expect((collector.received[0] as BatchProbe).n).toBe(1);
      expect((collector.received[1] as BatchProbe).n).toBe(3);
      await until("the bad entry to dead-letter", (): boolean => deadletters.length >= 1);
    } finally {
      await system.stop();
    }
  });

  it("gates a forged death notification inside a batch", async () => {
    const system: ActorSystem = remoteSystem("batchy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const session: Session = await dialSession(system.port());

      // The forged Terminated settles no watch this node holds, so the
      // gate drops it silently; its neighbors deliver untouched.
      const payload: Uint8Array = batchPayload(
        ["BatchProbe", "nodeakt.Terminated"],
        [
          { index: 0, value: { n: 1 } },
          { index: 1, value: { actorPath: sink.path().toString() } },
          { index: 0, value: { n: 2 } },
        ],
      );
      expect(session.tell(batchEnvelope(sink.path().toString(), payload, ""))).toBeNull();

      await until("the neighbors", (): boolean => collector.received.length >= 2);
      expect((collector.received[0] as BatchProbe).n).toBe(1);
      expect((collector.received[1] as BatchProbe).n).toBe(2);
      expect(collector.received.length).toBe(2);
    } finally {
      await system.stop();
    }
  });

  it("settles a real watch through a death notification that arrived batched", async () => {
    await withSystems(async (a: ActorSystem, b: ActorSystem): Promise<void> => {
      const watcher: Collector = new Collector();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      await b.spawn("subject", new Collector());
      const remote: PID = (await a.remoteLookup(b.host(), b.port(), "subject")) as PID;

      watcherPid.watch(remote);
      await sleep(50);

      // The notification arrives inside a batch, as a far node whose
      // watched actor stopped amid a burst would send it: the gate
      // settles the real registration and the watcher is notified,
      // alongside the batch's ordinary neighbor.
      const session: Session = await dialSession(a.port());
      const payload: Uint8Array = batchPayload(
        ["BatchProbe", "nodeakt.Terminated"],
        [
          { index: 0, value: { n: 1 } },
          { index: 1, value: { actorPath: remote.path().toString() } },
        ],
      );
      expect(session.tell(batchEnvelope(watcherPid.path().toString(), payload, ""))).toBeNull();

      await until("both deliveries", (): boolean => watcher.received.length >= 2);
      expect((watcher.received[0] as BatchProbe).n).toBe(1);
      expect(watcher.received[1]).toHaveProperty("actorPath", remote.path().toString());
    });
  });

  it("dead-letters every entry of a batch aimed at nothing", async () => {
    const system: ActorSystem = remoteSystem("batchy");
    await system.start();

    try {
      const session: Session = await dialSession(system.port());
      const deadletters: Deadletter[] = captureDeadletters(system);

      const payload: Uint8Array = batchPayload(
        ["BatchProbe"],
        [
          { index: 0, value: { n: 1 } },
          { index: 0, value: { n: 2 } },
        ],
      );
      const to: string = `nodeakt://batchy@127.0.0.1:${system.port()}/nobody`;
      expect(session.tell(batchEnvelope(to, payload, ""))).toBeNull();

      await until("both entries to dead-letter", (): boolean => deadletters.length >= 2);
      expect((deadletters[0] as Deadletter).reason).toBe(ErrDead.message);
      expect((deadletters[1] as Deadletter).reason).toBe(ErrDead.message);
      expect((deadletters[0] as Deadletter).message).toBeInstanceOf(BatchProbe);
    } finally {
      await system.stop();
    }
  });
});
