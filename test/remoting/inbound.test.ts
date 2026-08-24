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
import { Deadletter, PostStart, Terminated } from "../../src/messages";
import {
  type DataEnvelope,
  ERROR_BAD_REQUEST,
  KIND_ASK,
  KIND_TELL,
  KIND_UNWATCH,
  KIND_WATCH,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "../../src/net/envelope";
import type { NetServer } from "../../src/net/server";
import { PeerError, type Session } from "../../src/net/session";
import { ByteReader, ByteWriter, decodeValue, encodeValue } from "../../src/net/values";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { registerMessage } from "../../src/registration";
import { decodeFailure } from "../../src/remoting.codec";
import { cleanupNet, dialSession, hello, startServer } from "../net/helpers";
import { remoteSystem, until } from "./helpers";

class Probe {
  constructor(readonly n: number) {}
}

registerMessage(Probe);

/** Records every delivery with its sender for later assertions. */
class Collector implements Actor {
  readonly received: { message: unknown; sender: PID }[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    this.received.push({ message: ctx.message, sender: ctx.sender as PID });
  }

  postStop(): void {}
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

afterEach(cleanupNet);

describe("the control endpoint", () => {
  it("answers a raw lookup and ignores a control tell", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const local: PID = await system.spawn("greeter", new Collector());
      const session: Session = await dialSession(system.port());

      // A control tell is meaningless: dropped without an answer, and
      // the endpoint keeps serving.
      expect(
        session.tell(
          envelope({ typeRef: "nodeakt.remote.lookup", payload: payloadOf({ name: "greeter" }) }),
        ),
      ).toBeNull();

      const reply: ReplyEnvelope = await session.ask(
        envelope({
          kind: KIND_ASK,
          typeRef: "nodeakt.remote.lookup",
          payload: payloadOf({ name: "greeter" }),
        }),
        2000,
      );
      const answer = decodeValue(new ByteReader(reply.payload)) as { path: string; uid: string };
      expect(answer.path).toBe(local.path().toString());
      expect(answer.uid).toBe(local.path().uid());
    } finally {
      await system.stop();
    }
  });

  it("answers an unknown control request with a bad-request failure", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const session: Session = await dialSession(system.port());
      const rejection: Promise<ReplyEnvelope> = session.ask(
        envelope({ kind: KIND_ASK, typeRef: "nodeakt.remote.bogus", payload: payloadOf({}) }),
        2000,
      );
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof PeerError && err.code === ERROR_BAD_REQUEST;
      });
    } finally {
      await system.stop();
    }
  });

  it("answers a malformed control payload with a bad-request failure", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const session: Session = await dialSession(system.port());
      const rejection: Promise<ReplyEnvelope> = session.ask(
        envelope({
          kind: KIND_ASK,
          typeRef: "nodeakt.remote.lookup",
          payload: Uint8Array.of(0xff),
        }),
        2000,
      );
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof PeerError && err.code === ERROR_BAD_REQUEST;
      });
    } finally {
      await system.stop();
    }
  });
});

describe("inbound delivery edges", () => {
  it("settles an ask to an unknown or malformed target with the dead sentinel", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const session: Session = await dialSession(system.port());
      const missing: string = `nodeakt://beta@127.0.0.1:${system.port()}/nobody`;

      for (const to of [missing, "not-a-path"]) {
        const rejection: Promise<ReplyEnvelope> = session.ask(
          envelope({
            kind: KIND_ASK,
            to,
            typeRef: "Probe",
            payload: payloadOf({ n: 1 }),
          }),
          2000,
        );
        await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
          if (!(err instanceof PeerError)) {
            return false;
          }

          return decodeFailure(err.sentinel, err.errorName, err.message) === ErrDead;
        });
      }
    } finally {
      await system.stop();
    }
  });

  it("settles an ask pinned to a stale incarnation with the dead sentinel", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const local: PID = await system.spawn("greeter", new Collector());
      const session: Session = await dialSession(system.port());

      const rejection: Promise<ReplyEnvelope> = session.ask(
        envelope({
          kind: KIND_ASK,
          to: local.path().toString(),
          uid: "999999999",
          typeRef: "Probe",
          payload: payloadOf({ n: 1 }),
        }),
        2000,
      );
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        if (!(err instanceof PeerError)) {
          return false;
        }

        return decodeFailure(err.sentinel, err.errorName, err.message) === ErrDead;
      });
    } finally {
      await system.stop();
    }
  });

  it("dead-letters a tell to an unknown target", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const deadletters: Deadletter[] = [];
      system.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      const session: Session = await dialSession(system.port());
      const missing: string = `nodeakt://beta@127.0.0.1:${system.port()}/nobody`;
      const from: string = "nodeakt://ghost@10.9.9.9:9/g";
      session.tell(
        envelope({ to: missing, sender: from, typeRef: "Probe", payload: payloadOf({ n: 2 }) }),
      );

      await until("the dead letter", (): boolean => deadletters.length >= 1);
      const letter: Deadletter = deadletters[0] as Deadletter;
      expect(letter.receiver).toBe(missing);
      expect(letter.sender).toBe(from);
      expect(letter.reason).toBe(ErrDead.message);
      expect(letter.message).toBeInstanceOf(Probe);
    } finally {
      await system.stop();
    }
  });

  it("dead-letters a tell whose payload does not decode", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const deadletters: Deadletter[] = [];
      system.subscribe((event: unknown): void => {
        if (event instanceof Deadletter) {
          deadletters.push(event);
        }
      });

      const local: PID = await system.spawn("greeter", new Collector());
      const session: Session = await dialSession(system.port());
      session.tell(
        envelope({
          to: local.path().toString(),
          typeRef: "Probe",
          payload: Uint8Array.of(0xff),
        }),
      );

      await until("the dead letter", (): boolean => deadletters.length >= 1);
      expect((deadletters[0] as Deadletter).receiver).toBe(local.path().toString());
    } finally {
      await system.stop();
    }
  });

  it("settles an ask whose payload does not decode with the decode failure", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const local: PID = await system.spawn("greeter", new Collector());
      const session: Session = await dialSession(system.port());

      const rejection: Promise<ReplyEnvelope> = session.ask(
        envelope({
          kind: KIND_ASK,
          to: local.path().toString(),
          typeRef: "Probe",
          payload: Uint8Array.of(0xff),
        }),
        2000,
      );
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        if (!(err instanceof PeerError)) {
          return false;
        }

        return decodeFailure(err.sentinel, err.errorName, err.message).name === "ValueDecodeError";
      });
    } finally {
      await system.stop();
    }
  });

  it("resolves a sender of this very node to its live PID", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const target: PID = await system.spawn("target", collector);
      const impersonated: PID = await system.spawn("local-sender", new Collector());

      const session: Session = await dialSession(system.port());
      session.tell(
        envelope({
          to: target.path().toString(),
          sender: impersonated.path().toString(),
          senderUid: impersonated.path().uid(),
          typeRef: "Probe",
          payload: payloadOf({ n: 3 }),
        }),
      );

      await until("the delivery", (): boolean => collector.received.length >= 1);
      expect((collector.received[0] as { sender: PID }).sender).toBe(impersonated);
    } finally {
      await system.stop();
    }
  });

  it("falls back to NoSender for an absent or malformed sender", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const target: PID = await system.spawn("target", collector);
      const session: Session = await dialSession(system.port());

      for (const sender of ["", "garbage-path"]) {
        session.tell(
          envelope({
            to: target.path().toString(),
            sender,
            typeRef: "Probe",
            payload: payloadOf({ n: 4 }),
          }),
        );
      }

      await until("both deliveries", (): boolean => collector.received.length >= 2);
      expect((collector.received[0] as { sender: PID }).sender).toBe(system.noSender());
      expect((collector.received[1] as { sender: PID }).sender).toBe(system.noSender());
    } finally {
      await system.stop();
    }
  });

  it("falls back to NoSender for a local-node sender that does not resolve", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const target: PID = await system.spawn("target", collector);
      const session: Session = await dialSession(system.port());

      session.tell(
        envelope({
          to: target.path().toString(),
          sender: `nodeakt://beta@127.0.0.1:${system.port()}/ghost-sender`,
          typeRef: "Probe",
          payload: payloadOf({ n: 5 }),
        }),
      );

      await until("the delivery", (): boolean => collector.received.length >= 1);
      expect((collector.received[0] as { sender: PID }).sender).toBe(system.noSender());
    } finally {
      await system.stop();
    }
  });

  it("drops a watch or unwatch without a resolvable sender", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const collector: Collector = new Collector();
      const target: PID = await system.spawn("target", collector);
      const session: Session = await dialSession(system.port());

      // Forged frames: no sender, and an unwatch for a target that does
      // not exist under a resolvable impersonated sender. Both must be
      // ignored without harming the endpoint.
      session.tell(envelope({ kind: KIND_WATCH, to: target.path().toString() }));
      session.tell(envelope({ kind: KIND_UNWATCH, to: target.path().toString() }));
      session.tell(
        envelope({
          kind: KIND_UNWATCH,
          to: `nodeakt://beta@127.0.0.1:${system.port()}/nobody`,
          sender: target.path().toString(),
          senderUid: target.path().uid(),
        }),
      );

      session.tell(
        envelope({
          to: target.path().toString(),
          typeRef: "Probe",
          payload: payloadOf({ n: 6 }),
        }),
      );
      await until("the follow-up tell", (): boolean => collector.received.length >= 1);
      expect((collector.received[0] as { message: unknown }).message).toBeInstanceOf(Probe);
    } finally {
      await system.stop();
    }
  });
});

describe("a misbehaving remote node", () => {
  /** A fake node whose control lookup hands out an actor ref and whose
   * data asks answer whatever the script says. */
  async function ghostNode(script: {
    lookup: (server: NetServer) => unknown;
    answer: (session: Session, correlation: number) => void;
  }): Promise<NetServer> {
    let bound: NetServer | null = null;
    const server: NetServer = await startServer(
      {},
      {
        onData: (session: Session, arrived: DataEnvelope, correlation: number): void => {
          if (arrived.to === "") {
            session.reply(correlation, {
              serializerId: SERIALIZER_BINARY,
              typeRef: "",
              payload: payloadOf(script.lookup(bound as NetServer)),
            });
            return;
          }

          script.answer(session, correlation);
        },
      },
    );
    bound = server;
    return server;
  }

  function ghostRef(server: NetServer): { path: string; uid: string } {
    return { path: `nodeakt://ghost@127.0.0.1:${server.address.port}/g`, uid: "" };
  }

  it("rejects the ask with the decode failure when the reply is garbage", async () => {
    const system: ActorSystem = remoteSystem("alpha");
    await system.start();

    try {
      const server: NetServer = await ghostNode({
        lookup: (bound: NetServer): unknown => ghostRef(bound),
        answer: (session: Session, correlation: number): void => {
          session.reply(correlation, {
            serializerId: SERIALIZER_BINARY,
            typeRef: "",
            payload: Uint8Array.of(0xff),
          });
        },
      });

      const pid: PID = (await system.remoteLookup("127.0.0.1", server.address.port, "g")) as PID;
      const rejection: Promise<unknown> = system.noSender().ask(pid, new Probe(1), 2000);
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof Error && err.name === "ValueDecodeError";
      });
    } finally {
      await system.stop();
    }
  });

  it("delivers the decode failure to a request continuation", async () => {
    const system: ActorSystem = remoteSystem("alpha");
    await system.start();

    try {
      const server: NetServer = await ghostNode({
        lookup: (bound: NetServer): unknown => ghostRef(bound),
        answer: (session: Session, correlation: number): void => {
          session.reply(correlation, {
            serializerId: SERIALIZER_BINARY,
            typeRef: "",
            payload: Uint8Array.of(0xff),
          });
        },
      });

      const remote: PID = (await system.remoteLookup("127.0.0.1", server.address.port, "g")) as PID;

      const outcomes: { error: Error | null }[] = [];
      class Once implements Actor {
        preStart(): void {}

        receive(ctx: ReceiveContext): void {
          if (ctx.message !== "go") {
            return;
          }

          ctx
            .request(remote, new Probe(2), { timeout: 2000 })
            .onReply((_reply: unknown, error: Error | null): void => {
              outcomes.push({ error });
            });
        }

        postStop(): void {}
      }

      const pid: PID = await system.spawn("once", new Once(), {
        reentrancy: { mode: "allowAll" },
      });
      system.noSender().tell(pid, "go");

      await until("the request outcome", (): boolean => outcomes.length >= 1);
      const error: Error | null = (outcomes[0] as { error: Error | null }).error;
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).name).toBe("ValueDecodeError");
    } finally {
      await system.stop();
    }
  });

  it("passes a non-application peer failure through unchanged", async () => {
    const system: ActorSystem = remoteSystem("alpha");
    await system.start();

    try {
      const server: NetServer = await startServer(
        {},
        {
          onData: (session: Session, _arrived: DataEnvelope, correlation: number): void => {
            session.replyError(correlation, {
              code: ERROR_BAD_REQUEST,
              sentinel: 0,
              name: "Error",
              message: "not today",
            });
          },
        },
      );

      const rejection: Promise<PID | undefined> = system.remoteLookup(
        "127.0.0.1",
        server.address.port,
        "g",
      );
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof PeerError && err.code === ERROR_BAD_REQUEST;
      });
    } finally {
      await system.stop();
    }
  });

  it("delivers a push the far node initiates over the dialed connection", async () => {
    const system: ActorSystem = remoteSystem("alpha");
    await system.start();

    try {
      const sink: Collector = new Collector();
      const sinkPid: PID = await system.spawn("sink", sink);
      const sinkPath: string = sinkPid.path().toString();

      // The far node answers the lookup and, over the same accepted
      // connection, pushes a tell back toward the dialer: full duplex,
      // no second connection required.
      const pushing: NetServer = await startServer(
        {},
        {
          onData: (session: Session, arrived: DataEnvelope, correlation: number): void => {
            if (arrived.to === "") {
              session.reply(correlation, {
                serializerId: SERIALIZER_BINARY,
                typeRef: "",
                payload: payloadOf(null),
              });
              session.tell(
                envelope({
                  to: sinkPath,
                  typeRef: "Probe",
                  payload: payloadOf({ n: 42 }),
                }),
              );
            }
          },
        },
      );

      expect(
        await system.remoteLookup("127.0.0.1", pushing.address.port, "anything"),
      ).toBeUndefined();
      await until("the pushed tell", (): boolean => sink.received.length >= 1);
      expect((sink.received[0] as { message: unknown }).message).toBeInstanceOf(Probe);
      expect(((sink.received[0] as { message: unknown }).message as Probe).n).toBe(42);
    } finally {
      await system.stop();
    }
  });

  it("rejects the lookup with the decode failure when the answer is garbage", async () => {
    const system: ActorSystem = remoteSystem("alpha");
    await system.start();

    try {
      const server: NetServer = await startServer(
        {},
        {
          onData: (session: Session, _arrived: DataEnvelope, correlation: number): void => {
            session.reply(correlation, {
              serializerId: SERIALIZER_BINARY,
              typeRef: "",
              payload: Uint8Array.of(0xff),
            });
          },
        },
      );

      const rejection: Promise<PID | undefined> = system.remoteLookup(
        "127.0.0.1",
        server.address.port,
        "g",
      );
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof Error && err.name === "ValueDecodeError";
      });
    } finally {
      await system.stop();
    }
  });
});

describe("malformed control shapes", () => {
  it("answers a decodable payload of the wrong shape with a bad request", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const session: Session = await dialSession(system.port());
      const requests: { typeRef: string; body: unknown }[] = [
        { typeRef: "nodeakt.remote.lookup", body: null },
        { typeRef: "nodeakt.remote.lookup", body: { wrong: 1 } },
        { typeRef: "nodeakt.remote.spawn", body: null },
        { typeRef: "nodeakt.remote.spawn", body: { name: "x", actor: 5 } },
        { typeRef: "nodeakt.remote.spawn", body: { name: "x", actor: "A", args: "not-a-list" } },
        { typeRef: "nodeakt.remote.respawn", body: 42 },
        { typeRef: "nodeakt.remote.stop", body: null },
      ];

      for (const request of requests) {
        const rejection: Promise<ReplyEnvelope> = session.ask(
          envelope({
            kind: KIND_ASK,
            typeRef: request.typeRef,
            payload: payloadOf(request.body),
          }),
          2000,
        );
        await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
          if (!(err instanceof PeerError) || err.code !== ERROR_BAD_REQUEST) {
            return false;
          }

          return err.message.includes("malformed");
        });
      }
    } finally {
      await system.stop();
    }
  });
});

describe("refused replies", () => {
  /** Answers any probe with a payload far above the small negotiated
   * message cap, so the session must refuse the reply. */
  class BigMouth implements Actor {
    preStart(): void {}

    receive(ctx: ReceiveContext): void {
      if (ctx.message instanceof PostStart) {
        return;
      }

      ctx.response({ blob: new Uint8Array(64 * 1024) });
    }

    postStop(): void {}
  }

  it("settles the ask with the refusal when the reply exceeds the negotiated cap", async () => {
    const system: ActorSystem = remoteSystem("beta");
    await system.start();

    try {
      const local: PID = await system.spawn("big", new BigMouth());
      const session: Session = await dialSession(
        system.port(),
        hello({ systemName: "client", maxFrameSize: 16 * 1024, maxMessageSize: 16 * 1024 }),
      );

      const rejection: Promise<ReplyEnvelope> = session.ask(
        envelope({
          kind: KIND_ASK,
          to: local.path().toString(),
          typeRef: "Probe",
          payload: payloadOf({ n: 1 }),
        }),
        2000,
      );
      await expect(rejection).rejects.toSatisfy((err: unknown): boolean => {
        return err instanceof PeerError && err.message.includes("exceeds");
      });
    } finally {
      await system.stop();
    }
  });
});

describe("unsolicited death notifications", () => {
  /** Collects every Terminated it is notified with. */
  class Bystander implements Actor {
    readonly terminated: string[] = [];

    preStart(): void {}

    receive(ctx: ReceiveContext): void {
      if (ctx.message instanceof Terminated) {
        this.terminated.push(ctx.message.actorPath);
      }
    }

    postStop(): void {}
  }

  it("drops a Terminated that settles no watch this node holds", async () => {
    const system: ActorSystem = remoteSystem("alpha");
    await system.start();

    try {
      const bystander: Bystander = new Bystander();
      const collector: Collector = new Collector();
      const bystanderPid: PID = await system.spawn("bystander", bystander);
      const probePid: PID = await system.spawn("probe-sink", collector);
      const session: Session = await dialSession(system.port());

      session.tell(
        envelope({
          to: bystanderPid.path().toString(),
          typeRef: "nodeakt.Terminated",
          payload: payloadOf({ actorPath: "nodeakt://x@10.0.0.4:1/foo" }),
        }),
      );
      session.tell(
        envelope({
          to: probePid.path().toString(),
          typeRef: "Probe",
          payload: payloadOf({ n: 8 }),
        }),
      );

      // Lane FIFO orders the probe behind the forged notification, so
      // its arrival proves the notification was already dropped.
      await until("the probe", (): boolean => collector.received.length >= 1);
      expect(bystander.terminated.length).toBe(0);
    } finally {
      await system.stop();
    }
  });
});

describe("inbound watches over dialed sessions", () => {
  it("releases a watch pushed over a dialed connection once its node dies", async () => {
    const system: ActorSystem = remoteSystem("alpha");
    await system.start();

    try {
      const sink: Collector = new Collector();
      const sinkPid: PID = await system.spawn("sink", sink);
      const sinkPath: string = sinkPid.path().toString();

      // The far node answers the lookup and pushes a watch back over
      // the same accepted connection, registering itself as a watcher
      // through a session this node dialed.
      const server: NetServer = await startServer(
        {},
        {
          onData: (session: Session, arrived: DataEnvelope, correlation: number): void => {
            if (arrived.to !== "") {
              return;
            }

            session.reply(correlation, {
              serializerId: SERIALIZER_BINARY,
              typeRef: "",
              payload: payloadOf(null),
            });
            session.tell(
              envelope({
                kind: KIND_WATCH,
                to: sinkPath,
                sender: `nodeakt://ghost@127.0.0.1:${server.address.port}/g`,
              }),
            );
          },
        },
      );

      expect(await system.remoteLookup("127.0.0.1", server.address.port, "x")).toBeUndefined();
      await until("the pushed watch to land", (): boolean => system.isRunning());
      await new Promise<void>((resolve): void => {
        setTimeout(resolve, 100);
      });

      // The node dies; the lane closure sweeps the registration the
      // close callback cannot reach, so the sink's eventual stop tells
      // no ghost and the system still stops cleanly.
      await server.shutdown(-1);
      await new Promise<void>((resolve): void => {
        setTimeout(resolve, 150);
      });
      await sinkPid.shutdown();
      expect(sink.received.length).toBe(0);
    } finally {
      await system.stop();
    }
  });
});
