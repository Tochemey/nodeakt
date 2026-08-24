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
import { ActorSystem } from "../../src/actor.system";
import type { EntryLevel, Fields, LazyFields, Level, Logger } from "../../src/logger";
import { Deadletter, PostStart } from "../../src/messages";
import {
  type DataEnvelope,
  KIND_TELL,
  KIND_UNWATCH,
  KIND_WATCH,
  SERIALIZER_BINARY,
} from "../../src/net/envelope";
import type { Session } from "../../src/net/session";
import { ByteWriter, encodeValue } from "../../src/net/values";
import { type Path, parsePath } from "../../src/path";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { registerMessage } from "../../src/registration";
import { type Remoting, SENDER_CACHE_SIZE } from "../../src/remoting";
import { cleanupNet, dialSession } from "../net/helpers";
import { remoteSystem, sleep, until } from "./helpers";

/**
 * The sender-cache bound. The cache trades memory for the parse and
 * mint a repeated sender would otherwise pay per envelope, so these
 * tests pin the three properties the bound must keep: identity (the
 * same sender resolves to the same handle instance while cached),
 * recency (a heard-from sender outlives quieter ones), and pinning (a
 * sender behind a live inbound watch is never evicted, because its
 * unwatch removes the watcher by identity).
 */

class CacheProbe {
  constructor(readonly n: number) {}
}

registerMessage(CacheProbe);

/** Records every delivery with its sender for identity assertions. */
class Collector implements Actor {
  readonly senders: PID[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.senders.push(ctx.sender as PID);
  }

  postStop(): void {}
}

/** Does nothing; exists to be watched. */
class Idle implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** Counts warn-level entries, so the overflow log-once rule is
 * observable; everything else is discarded. */
class WarnCounter implements Logger {
  readonly warns: string[] = [];

  debug(): void {}

  info(): void {}

  warn(message: string, _fields?: LazyFields): void {
    this.warns.push(message);
  }

  error(): void {}

  level(): Level {
    return "warn";
  }

  enabled(level: EntryLevel): boolean {
    return level === "warn" || level === "error";
  }

  with(_fields: Fields): Logger {
    return this;
  }
}

/** The live seam of a started system; the cache getters live on it. */
function seamOf(system: ActorSystem): Remoting {
  return (system as unknown as { _remoting: Remoting })._remoting;
}

/** A parseable forged sender path a reply dial can only fail fast on. */
function ghost(name: string): string {
  return `nodeakt://cachy@127.0.0.1:1/${name}`;
}

function payloadOf(value: unknown): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, value);
  return Uint8Array.from(writer.bytes());
}

/** One inbound envelope with every field spelled out and overridable. */
function envelope(overrides: Partial<DataEnvelope>): DataEnvelope {
  return {
    kind: KIND_TELL,
    to: "",
    uid: "",
    sender: "",
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: "",
    payload: new Uint8Array(0),
    ...overrides,
  };
}

/** One probe tell to `to` from the forged sender path. */
function probeFrom(sender: string, to: string, n: number): DataEnvelope {
  return {
    kind: KIND_TELL,
    to,
    uid: "",
    sender,
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: "CacheProbe",
    payload: payloadOf({ n }),
  };
}

/** One watch or unwatch envelope from the forged sender path. */
function watchFrom(kind: number, sender: string, to: string): DataEnvelope {
  return envelope({ kind, to, sender });
}

/** Floods the session with one tell per distinct forged sender. */
function flood(session: Session, to: string, count: number, tag: string): void {
  for (let i: number = 0; i < count; i++) {
    expect(session.tell(probeFrom(ghost(`${tag}-${i}`), to, i))).toBeNull();
  }
}

afterEach(cleanupNet);

describe("the sender cache bound", () => {
  it("keeps identity for repeated senders, refreshes on hit, and evicts past the cap", async () => {
    const system: ActorSystem = remoteSystem("cachy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const to: string = sink.path().toString();
      const session: Session = await dialSession(system.port());

      // One sender, then churn filling the cache to its cap exactly:
      // the sender is now the oldest entry.
      expect(session.tell(probeFrom(ghost("a"), to, 0))).toBeNull();
      flood(session, to, SENDER_CACHE_SIZE - 1, "churn");

      // A repeat at the cap resolves to the very same instance and
      // refreshes its recency, so the next insert evicts the quietest
      // churn entry instead; the sender survives, the evicted churn
      // entry resolves to a fresh instance when heard from again.
      expect(session.tell(probeFrom(ghost("a"), to, 1))).toBeNull();
      expect(session.tell(probeFrom(ghost("c"), to, 2))).toBeNull();
      expect(session.tell(probeFrom(ghost("a"), to, 3))).toBeNull();
      expect(session.tell(probeFrom(ghost("churn-0"), to, 4))).toBeNull();

      const total: number = SENDER_CACHE_SIZE + 4;
      await until("every probe to arrive", (): boolean => collector.senders.length >= total);

      const a1: PID = collector.senders[0] as PID;
      const churn1: PID = collector.senders[1] as PID;
      expect(collector.senders[total - 4]).toBe(a1);
      expect(collector.senders[total - 2]).toBe(a1);
      expect(collector.senders[total - 1]).not.toBe(churn1);
      expect(seamOf(system).cachedSenders).toBe(SENDER_CACHE_SIZE);
    } finally {
      await system.stop();
    }
  });

  it("holds identity for an active watcher under churn past the cap, until its unwatch", async () => {
    const system: ActorSystem = remoteSystem("cachy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const subject: PID = await system.spawn("subject", new Idle());
      const to: string = sink.path().toString();
      const session: Session = await dialSession(system.port());

      const deadletters: Deadletter[] = [];
      system.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      // The watcher is heard from once, then watches: the watch pins
      // its cached handle.
      expect(session.tell(probeFrom(ghost("w"), to, 0))).toBeNull();
      expect(session.tell(watchFrom(KIND_WATCH, ghost("w"), subject.path().toString()))).toBeNull();

      // Churn far past the cap: every unpinned entry cycles out, the
      // pinned watcher must not.
      flood(session, to, SENDER_CACHE_SIZE + 8, "churn");
      expect(session.tell(probeFrom(ghost("w"), to, 1))).toBeNull();
      await until(
        "every probe to arrive",
        (): boolean => collector.senders.length >= SENDER_CACHE_SIZE + 10,
      );
      const w1: PID = collector.senders[0] as PID;
      expect(collector.senders[collector.senders.length - 1]).toBe(w1);

      // The unwatch removes the watcher by identity and releases the
      // pin; a full round of fresh churn now ages the handle out like
      // any other.
      expect(
        session.tell(watchFrom(KIND_UNWATCH, ghost("w"), subject.path().toString())),
      ).toBeNull();
      flood(session, to, SENDER_CACHE_SIZE + 1, "after");
      expect(session.tell(probeFrom(ghost("w"), to, 2))).toBeNull();
      await until(
        "the post-unwatch probes",
        (): boolean => collector.senders.length >= 2 * SENDER_CACHE_SIZE + 12,
      );
      expect(collector.senders[collector.senders.length - 1]).not.toBe(w1);

      // And the registration is truly gone: the subject's stop
      // notifies nobody, so nothing dials the forged watcher.
      await subject.shutdown();
      await sleep(150);
      expect(deadletters.length).toBe(0);
    } finally {
      await system.stop();
    }
  });

  it("acquires one pin for a redelivered watch, released by a single unwatch", async () => {
    const system: ActorSystem = remoteSystem("cachy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const subject: PID = await system.spawn("subject", new Idle());
      const to: string = sink.path().toString();
      const session: Session = await dialSession(system.port());

      // The same watch twice, as a redelivery would look: the second
      // registers nothing, so it must pin nothing.
      expect(session.tell(probeFrom(ghost("w"), to, 0))).toBeNull();
      expect(session.tell(watchFrom(KIND_WATCH, ghost("w"), subject.path().toString()))).toBeNull();
      expect(session.tell(watchFrom(KIND_WATCH, ghost("w"), subject.path().toString()))).toBeNull();
      expect(
        session.tell(watchFrom(KIND_UNWATCH, ghost("w"), subject.path().toString())),
      ).toBeNull();

      // One unwatch released the only pin: churn ages the handle out,
      // which a leaked second pin would forbid.
      flood(session, to, SENDER_CACHE_SIZE + 1, "churn");
      expect(session.tell(probeFrom(ghost("w"), to, 1))).toBeNull();
      await until(
        "the post-unwatch probe",
        (): boolean => collector.senders.length >= SENDER_CACHE_SIZE + 3,
      );
      const w1: PID = collector.senders[0] as PID;
      expect(collector.senders[collector.senders.length - 1]).not.toBe(w1);
    } finally {
      await system.stop();
    }
  });

  it("lets pinned watchers exceed the cap, says so once, and shrinks as pins release", async () => {
    const warns: WarnCounter = new WarnCounter();
    const system: ActorSystem = new ActorSystem("cachy", {
      logger: warns,
      remote: { host: "127.0.0.1", port: 0 },
    });
    await system.start();

    try {
      const subject: PID = await system.spawn("subject", new Idle());
      const to: string = subject.path().toString();
      const session: Session = await dialSession(system.port());

      // One watcher per entry past the cap: every entry is pinned, so
      // eviction has nothing it may take and the cache exceeds the cap
      // instead of breaking a watcher's identity.
      const pinned: number = SENDER_CACHE_SIZE + 1;
      for (let i: number = 0; i < pinned; i++) {
        expect(session.tell(watchFrom(KIND_WATCH, ghost(`w-${i}`), to))).toBeNull();
      }

      await until("every watch to register", (): boolean => seamOf(system).cachedSenders >= pinned);
      expect(warns.warns.length).toBe(1);

      // Two more transient senders: the first parks unevictable beside
      // the pins without a second log, the next evicts it.
      expect(session.tell(probeFrom(ghost("f-1"), to, 0))).toBeNull();
      expect(session.tell(probeFrom(ghost("f-2"), to, 1))).toBeNull();
      await until(
        "the transient senders",
        (): boolean => seamOf(system).cachedSenders === pinned + 1,
      );
      expect(warns.warns.length).toBe(1);

      // Unwatching a few watchers releases their pins; the next insert
      // drains the cache back to its cap.
      for (let i: number = 0; i < 8; i++) {
        expect(session.tell(watchFrom(KIND_UNWATCH, ghost(`w-${i}`), to))).toBeNull();
      }

      expect(session.tell(probeFrom(ghost("f-3"), to, 2))).toBeNull();
      await until(
        "the cache to shrink to its cap",
        (): boolean => seamOf(system).cachedSenders === SENDER_CACHE_SIZE,
      );
      expect(warns.warns.length).toBe(1);
    } finally {
      await system.stop();
    }
  });

  it("releases the pin when the watched actor stops on its own", async () => {
    const system: ActorSystem = remoteSystem("cachy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const subject: PID = await system.spawn("subject", new Idle());
      const to: string = sink.path().toString();
      const session: Session = await dialSession(system.port());

      expect(session.tell(probeFrom(ghost("w"), to, 0))).toBeNull();
      expect(session.tell(watchFrom(KIND_WATCH, ghost("w"), subject.path().toString()))).toBeNull();
      await until("the probe to arrive", (): boolean => collector.senders.length >= 1);
      const w1: PID = collector.senders[0] as PID;

      // The graceful stop routes the Terminated through the watcher
      // handle; that outbound notification is the seam's cue that the
      // registration, and with it the pin, is gone.
      await subject.shutdown();
      flood(session, to, SENDER_CACHE_SIZE + 2, "churn");
      expect(session.tell(probeFrom(ghost("w"), to, 1))).toBeNull();
      await until(
        "the post-stop probes",
        (): boolean => collector.senders.length >= SENDER_CACHE_SIZE + 4,
      );
      expect(collector.senders[collector.senders.length - 1]).not.toBe(w1);
      expect(seamOf(system).cachedSenders).toBe(SENDER_CACHE_SIZE);
    } finally {
      await system.stop();
    }
  });

  it("drops a watch whose sender path is not canonical, pinning nothing", async () => {
    const system: ActorSystem = remoteSystem("cachy");
    await system.start();

    try {
      const subject: PID = await system.spawn("subject", new Idle());
      const to: string = subject.path().toString();
      const session: Session = await dialSession(system.port());

      // A non-canonical port ("007") does not stringify back to itself,
      // so its handle could be pinned under one key and released under
      // another, a leak. Sender resolution rejects it outright: the
      // watch registers nothing and caches nothing, so its target's
      // later stop has no dangling pin to leak.
      expect(
        session.tell(watchFrom(KIND_WATCH, "nodeakt://cachy@127.0.0.1:007/ghost", to)),
      ).toBeNull();
      await sleep(50);
      expect(seamOf(system).cachedSenders).toBe(0);

      await subject.shutdown();
      await sleep(100);
      expect(seamOf(system).cachedSenders).toBe(0);
    } finally {
      await system.stop();
    }
  });

  it("registers a wire watch from a watcher of this very node without pinning", async () => {
    const system: ActorSystem = remoteSystem("cachy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const subject: PID = await system.spawn("subject", new Idle());
      const session: Session = await dialSession(system.port());

      // The watcher is a live local actor: its handle is resolved, not
      // cached, so there is no entry to pin and nothing to bound.
      expect(
        session.tell(
          envelope({
            kind: KIND_WATCH,
            to: subject.path().toString(),
            sender: sink.path().toString(),
            senderUid: sink.path().uid(),
          }),
        ),
      ).toBeNull();
      await sleep(50);
      expect(seamOf(system).cachedSenders).toBe(0);

      // The registration itself is real: the stop notifies the local
      // watcher through the ordinary delivery path.
      await subject.shutdown();
      await until("the Terminated", (): boolean => collector.senders.length >= 1);
    } finally {
      await system.stop();
    }
  });

  it("hands the isolate transport stable foreign handles from the same cache", async () => {
    const system: ActorSystem = remoteSystem("cachy");
    await system.start();

    try {
      const seam: Remoting = seamOf(system);
      const local: Path = parsePath(`nodeakt://cachy@127.0.0.1:${system.port()}/resident`, "1");
      const far: Path = parsePath("nodeakt://elsewhere@127.0.0.1:1/caller", "7");

      // A path of this very node is not the seam's to answer; a
      // foreign path resolves to one stable handle, cache-first, so
      // the identity the port transport hands out matches what the
      // inbound side registered.
      expect(seam.handleFor(local)).toBeUndefined();
      const first: PID = seam.handleFor(far) as PID;
      expect(first).toBeDefined();
      expect(seam.handleFor(far)).toBe(first);

      // At the cap the hit refreshes recency, exactly as an inbound
      // envelope's hit does, so the active handle outlives the next
      // insert's eviction pass instead of aging out as the oldest.
      for (let i: number = 0; i < SENDER_CACHE_SIZE - 1; i++) {
        seam.handleFor(parsePath(`nodeakt://elsewhere@127.0.0.1:1/churn-${i}`, ""));
      }

      expect(seam.handleFor(far)).toBe(first);
      seam.handleFor(parsePath("nodeakt://elsewhere@127.0.0.1:1/overflow", ""));
      expect(seam.handleFor(far)).toBe(first);
    } finally {
      await system.stop();
    }
  });

  it("survives an unwatch whose watcher was force-evicted mid-watch", async () => {
    const system: ActorSystem = remoteSystem("cachy");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const sink: PID = await system.spawn("sink", collector);
      const subject: PID = await system.spawn("subject", new Idle());
      const to: string = sink.path().toString();
      const session: Session = await dialSession(system.port());

      expect(session.tell(watchFrom(KIND_WATCH, ghost("w"), subject.path().toString()))).toBeNull();
      await until("the watch to register", (): boolean => seamOf(system).cachedSenders >= 1);

      // Force the failure the pin exists to prevent: strip the pin so
      // churn evicts a handle that is still registered as a watcher.
      const senders: Map<string, { pins: Set<string> }> = (
        seamOf(system) as unknown as { _senders: Map<string, { pins: Set<string> }> }
      )._senders;
      for (const entry of senders.values()) {
        entry.pins.clear();
      }

      flood(session, to, SENDER_CACHE_SIZE + 2, "churn");

      // The unwatch now resolves a different instance, removes nothing,
      // and the stale registration stays behind: a bounded leak, not a
      // user-visible failure.
      expect(
        session.tell(watchFrom(KIND_UNWATCH, ghost("w"), subject.path().toString())),
      ).toBeNull();
      await subject.shutdown();
      await sleep(100);

      // The node still serves, and the cache still holds its bound.
      expect(session.tell(probeFrom(ghost("final"), to, 0))).toBeNull();
      await until(
        "the final probe",
        (): boolean =>
          collector.senders.length >= SENDER_CACHE_SIZE + 3 &&
          seamOf(system).cachedSenders <= SENDER_CACHE_SIZE,
      );
    } finally {
      await system.stop();
    }
  });
});
