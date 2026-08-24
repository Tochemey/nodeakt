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
import { ErrRequestTimeout } from "../../src/errors";
import { Deadletter, PostStart, Terminated } from "../../src/messages";
import {
  type DataEnvelope,
  KIND_TELL,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "../../src/net/envelope";
import { LANE_CONTROL } from "../../src/net/frame";
import type { Session } from "../../src/net/session";
import { ByteReader, ByteWriter, decodeValue, encodeValue } from "../../src/net/values";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { registerMessage } from "../../src/registration";
import { BATCH_TYPE_REF, type Remoting } from "../../src/remoting";
import { cleanupNet, dialSession, hello } from "../net/helpers";
import { remoteSystem, sleep, until } from "./helpers";

/**
 * Duplex reuse of inbound sessions. A node that dials us but whose own
 * advertised endpoint is not dialable back (NAT, one-way container
 * networking) is still answered: its reply, its Terminated, and an ask
 * or watch aimed at it all ride the connection it opened, because the
 * seam elects that accepted session as the node's carrier when no peer
 * reaches it. These tests drive that from a raw client that advertises
 * an unreachable endpoint, so a dial back could only fail fast.
 */

class DuplexPing {
  constructor(readonly n: number) {}
}

class DuplexPong {
  constructor(readonly n: number) {}
}

class DuplexAnswer {
  constructor(readonly n: number) {}
}

registerMessage(DuplexPing);
registerMessage(DuplexPong);
registerMessage(DuplexAnswer);

/** The unreachable node every raw client here impersonates: a real
 * loopback host with a port nothing listens on, so a reply dial to it
 * can only fail. The client dials the system on its true port but
 * advertises this endpoint, exactly as a NAT'd node would. */
const FAR_HOST: string = "127.0.0.1";
const FAR_PORT: number = 1;

/** The forged sender path of a caller on the unreachable node. */
function farSender(name: string): string {
  return `nodeakt://farside@${FAR_HOST}:${FAR_PORT}/${name}`;
}

/** The client HELLO that advertises the unreachable endpoint, so its
 * accepted session is keyed under the same node the forged sender paths
 * name. */
function farHello(overrides: Record<string, number> = {}): ReturnType<typeof hello> {
  return hello({ systemName: "farside", host: FAR_HOST, port: FAR_PORT, ...overrides });
}

function payloadOf(value: unknown): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, value);
  return Uint8Array.from(writer.bytes());
}

/** One inbound tell from a caller on the unreachable node. */
function tellFrom(sender: string, to: string, typeRef: string, value: unknown): DataEnvelope {
  return {
    kind: KIND_TELL,
    to,
    uid: "",
    sender,
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef,
    payload: payloadOf(value),
  };
}

/** The live seam of a started system. */
function seamOf(system: ActorSystem): Remoting {
  return (system as unknown as { _remoting: Remoting })._remoting;
}

/** The seam's carrier registry, for asserting the one-carrier rule. */
function carriersOf(system: ActorSystem): Map<string, { peer: unknown; session: Session | null }> {
  return (
    seamOf(system) as unknown as {
      _carriers: Map<string, { peer: unknown; session: Session | null }>;
    }
  )._carriers;
}

/** The seam's accepted-session registry, keyed by advertised endpoint. */
function inboundSessionsOf(system: ActorSystem): Map<string, Set<Session>> {
  return (seamOf(system) as unknown as { _inboundSessions: Map<string, Set<Session>> })
    ._inboundSessions;
}

/** Answers a ping by telling its sender back, so the reply must find a
 * route home to a node it cannot dial. */
class Echo implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof DuplexPing) {
      ctx.tell(ctx.sender as PID, new DuplexPong(ctx.message.n));
    }
  }

  postStop(): void {}
}

/** Asks its sender back and records the outcome, proving an ask aimed
 * at the unreachable node rides the reused session both ways. */
class Backcaller implements Actor {
  readonly answers: { reply: unknown; error: Error | null }[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof DuplexPing) {
      const sender: PID = ctx.sender as PID;
      ctx.ask(sender, new DuplexPing(ctx.message.n), 1000).then(
        (reply: unknown): void => {
          this.answers.push({ reply, error: null });
        },
        (error: Error): void => {
          this.answers.push({ reply: undefined, error });
        },
      );
    }
  }

  postStop(): void {}
}

/** Answers a ping with a payload past the tiny negotiated cap, so the
 * back-channel tell is refused and dead-letters. */
class BigReplier implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof DuplexPing) {
      ctx.tell(ctx.sender as PID, { blob: new Uint8Array(64 * 1024) });
    }
  }

  postStop(): void {}
}

/** Watches its sender and records the Terminated, proving a watch on
 * the unreachable node settles when its reused carrier closes. */
class SenderWatcher implements Actor {
  readonly terminated: string[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    if (ctx.message instanceof Terminated) {
      this.terminated.push(ctx.message.actorPath);
      return;
    }

    if (ctx.message instanceof DuplexPing) {
      ctx.watch(ctx.sender as PID);
    }
  }

  postStop(): void {}
}

afterEach(cleanupNet);

describe("duplex reuse of inbound sessions", () => {
  it("answers a foreign sender over the session it dialed in on, not its unreachable endpoint", async () => {
    const system: ActorSystem = remoteSystem("nearside");
    await system.start();

    try {
      const echo: PID = await system.spawn("echo", new Echo());
      const back: { typeRef: string; n: number }[] = [];
      const envelopes: DataEnvelope[] = [];
      const client: Session = await dialSession(system.port(), farHello(), {
        onData: (_session: Session, envelope: DataEnvelope): void => {
          envelopes.push(envelope);
          back.push(...unpack(envelope));
        },
      });

      // A burst of tells from the unreachable caller: each reply must
      // come back over this very session, in order. Replies fired
      // within one turn coalesce, so what arrives is fewer envelopes
      // than logical messages.
      for (let i: number = 0; i < 5; i++) {
        expect(
          client.tell(
            tellFrom(farSender("caller"), echo.path().toString(), "DuplexPing", { n: i }),
          ),
        ).toBeNull();
      }

      await until("every pong to ride the session back", (): boolean => back.length >= 5);
      expect(back.map((entry: { typeRef: string }): string => entry.typeRef)).toEqual(
        Array.from({ length: 5 }, (): string => "DuplexPong"),
      );
      expect(back.map((entry: { n: number }): number => entry.n)).toEqual([0, 1, 2, 3, 4]);
      expect(envelopes.length).toBeLessThan(5);

      // Exactly one carrier for the node, and it is the one accepted
      // session, not a peer: the seam answered over the connection the
      // caller opened and never dialed the dead endpoint. The carrier
      // holds this side's accepted session, not the client's own.
      const inbound: Set<Session> | undefined = inboundSessionsOf(system).get(
        `${FAR_HOST}:${FAR_PORT}`,
      );
      expect(inbound?.size).toBe(1);
      const accepted: Session = [...(inbound as Set<Session>)][0] as Session;
      const carrier: { peer: unknown; session: Session | null } | undefined = carriersOf(
        system,
      ).get(`${FAR_HOST}:${FAR_PORT}`);
      expect(carrier).toBeDefined();
      expect((carrier as { peer: unknown }).peer).toBeNull();
      expect((carrier as { session: Session | null }).session).toBe(accepted);
    } finally {
      await system.stop();
    }
  });

  it("asks a foreign sender back over the reused session", async () => {
    const system: ActorSystem = remoteSystem("nearside");
    await system.start();

    try {
      const backcaller: Backcaller = new Backcaller();
      const pid: PID = await system.spawn("backcaller", backcaller);

      // The client answers whatever ask the actor sends it, over the
      // same session, so the round trip never touches the dead endpoint.
      const client: Session = await dialSession(system.port(), farHello(), {
        onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
          if (correlation !== 0 && envelope.typeRef === "DuplexPing") {
            const reply: ReplyEnvelope = {
              serializerId: SERIALIZER_BINARY,
              typeRef: "DuplexAnswer",
              payload: payloadOf({ n: 99 }),
            };
            session.reply(correlation, reply);
          }
        },
      });

      client.tell(tellFrom(farSender("caller"), pid.path().toString(), "DuplexPing", { n: 7 }));
      await until("the ask to be answered", (): boolean => backcaller.answers.length >= 1);

      const outcome: { reply: unknown; error: Error | null } = backcaller.answers[0] as {
        reply: unknown;
        error: Error | null;
      };
      expect(outcome.error).toBeNull();
      expect(outcome.reply).toBeInstanceOf(DuplexAnswer);
      expect((outcome.reply as DuplexAnswer).n).toBe(99);
    } finally {
      await system.stop();
    }
  });

  it("times out an ask to a foreign sender that never answers over the reused session", async () => {
    const system: ActorSystem = remoteSystem("nearside");
    await system.start();

    try {
      const backcaller: Backcaller = new Backcaller();
      const pid: PID = await system.spawn("backcaller", backcaller);

      // The client stays silent: the ask still rode the session, and its
      // deadline is what settles it, not a dead endpoint's dial failure.
      const client: Session = await dialSession(system.port(), farHello());
      client.tell(tellFrom(farSender("caller"), pid.path().toString(), "DuplexPing", { n: 1 }));

      await until("the ask to time out", (): boolean => backcaller.answers.length >= 1);
      expect((backcaller.answers[0] as { error: Error | null }).error).toBe(ErrRequestTimeout);
    } finally {
      await system.stop();
    }
  });

  it("dead-letters a back-channel tell the reused session refuses", async () => {
    const system: ActorSystem = remoteSystem("nearside");
    await system.start();

    try {
      const deadletters: Deadletter[] = [];
      system.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      const pid: PID = await system.spawn("big", new BigReplier());

      // A tiny negotiated message cap: the oversize back-channel reply
      // is refused on this side and dead-letters, the same outcome a
      // peer's undeliverable tell has.
      const client: Session = await dialSession(
        system.port(),
        farHello({ maxFrameSize: 16 * 1024, maxMessageSize: 16 * 1024 }),
      );
      client.tell(tellFrom(farSender("caller"), pid.path().toString(), "DuplexPing", { n: 1 }));

      await until("the refused reply to dead-letter", (): boolean => deadletters.length >= 1);
      expect((deadletters[0] as Deadletter).receiver).toBe(farSender("caller"));
    } finally {
      await system.stop();
    }
  });

  it("settles an outbound watch on the foreign sender when its reused session closes", async () => {
    const system: ActorSystem = remoteSystem("nearside");
    await system.start();

    try {
      const watcher: SenderWatcher = new SenderWatcher();
      const pid: PID = await system.spawn("watcher", watcher);

      const client: Session = await dialSession(system.port(), farHello());
      client.tell(tellFrom(farSender("caller"), pid.path().toString(), "DuplexPing", { n: 1 }));

      // The watch is registered against a node reachable only through
      // this session; its close is the death that node's watch settles
      // on, exactly as a control-lane peer close would be.
      await until(
        "the watch to register on the node",
        (): boolean => carriersOf(system).get(`${FAR_HOST}:${FAR_PORT}`) !== undefined,
      );
      client.destroy();

      await until("the Terminated", (): boolean => watcher.terminated.length >= 1);
      expect(watcher.terminated[0]).toBe(farSender("caller"));
    } finally {
      await system.stop();
    }
  });

  it("leaves session-carried watches alone when a stray peer control lane closes", async () => {
    const system: ActorSystem = remoteSystem("nearside");
    await system.start();

    try {
      const watcher: SenderWatcher = new SenderWatcher();
      const pid: PID = await system.spawn("watcher", watcher);

      const client: Session = await dialSession(system.port(), farHello());
      client.tell(tellFrom(farSender("caller"), pid.path().toString(), "DuplexPing", { n: 1 }));
      await until(
        "the watch to register on the node",
        (): boolean => carriersOf(system).get(`${FAR_HOST}:${FAR_PORT}`) !== undefined,
      );

      // Settlement belongs to the elected carrier. A control-lane
      // close reported for the same node by a non-carrier peer (no
      // wire sequence produces one today, but the gate must not lean
      // on that) settles nothing: the session still carries the node,
      // so the watch stays live until that session actually dies.
      const seam: Remoting = seamOf(system);
      (seam as unknown as { onLaneClose(node: string, lane: number): void }).onLaneClose(
        `${FAR_HOST}:${FAR_PORT}`,
        LANE_CONTROL,
      );
      await sleep(100);
      expect(watcher.terminated.length).toBe(0);

      client.destroy();
      await until("the Terminated", (): boolean => watcher.terminated.length >= 1);
      expect(watcher.terminated[0]).toBe(farSender("caller"));
    } finally {
      await system.stop();
    }
  });
});

/** Flattens one arrived envelope into its logical messages: a batch
 * envelope yields every entry it coalesced, anything else yields
 * itself, so assertions see the message stream however it traveled.
 * The batch layout is the seam's own: a type table, an entry count,
 * then one (type index, length, payload) triple per entry. */
function unpack(envelope: DataEnvelope): { typeRef: string; n: number }[] {
  const read: (bytes: Uint8Array) => number = (bytes: Uint8Array): number =>
    (decodeValue(new ByteReader(bytes)) as { n: number }).n;
  if (envelope.typeRef !== BATCH_TYPE_REF) {
    return [{ typeRef: envelope.typeRef, n: read(envelope.payload) }];
  }

  const reader: ByteReader = new ByteReader(envelope.payload);
  const types: string[] = [];
  for (let count: number = reader.readUvarint(); count > 0; count--) {
    types.push(reader.readString());
  }

  const entries: { typeRef: string; n: number }[] = [];
  for (let count: number = reader.readUvarint(); count > 0; count--) {
    const typeRef: string = types[reader.readUvarint()] as string;
    entries.push({ typeRef, n: read(reader.readBytes(reader.readUvarint())) });
  }

  return entries;
}
