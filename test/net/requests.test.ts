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
import { decodeError, encodeError } from "../../src/codec";
import type { WireError } from "../../src/envelope";
import { ErrDead } from "../../src/errors";
import {
  type DataEnvelope,
  ERROR_APPLICATION,
  ERROR_INTERNAL,
  ERROR_UNAVAILABLE,
  KIND_ASK,
  KIND_TELL,
  KIND_WATCH,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "../../src/net/envelope";
import { FLAG_EXPECTS_REPLY, FRAME_DATA } from "../../src/net/frame";
import { NetServer } from "../../src/net/server";
import { ErrAskTimeout, ErrBackpressure, PeerError, type Session } from "../../src/net/session";
import { ByteReader, ByteWriter, decodeValue, encodeValue } from "../../src/net/values";
import { cleanupNet, dialSession, EMPTY, hello, trackServer } from "./helpers";

afterEach(cleanupNet);

function encodePayload(value: unknown): Uint8Array {
  const writer: ByteWriter = new ByteWriter();
  encodeValue(writer, value);
  return Uint8Array.from(writer.bytes());
}

function decodePayload(payload: Uint8Array): unknown {
  return decodeValue(new ByteReader(payload));
}

function data(kind: number, typeRef: string, value: unknown): DataEnvelope {
  return {
    kind,
    to: "nodeakt://orders@10.0.0.5:5100/user/charger",
    uid: "b3f2",
    sender: "nodeakt://orders@10.0.0.9:5100/user/gateway",
    senderUid: "77aa",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef,
    payload: kind === KIND_WATCH ? EMPTY : encodePayload(value),
  };
}

interface ReceivedData {
  readonly envelope: DataEnvelope;
  readonly correlation: number;
}

/**
 * A miniature seam standing in for the actor system: echoes asks,
 * fails some with a sentinel error, answers some late, never answers
 * others, and records everything it saw.
 */
async function startSeamServer(): Promise<{ server: NetServer; received: ReceivedData[] }> {
  const received: ReceivedData[] = [];
  const server: NetServer = await NetServer.listen(
    { local: hello({ systemName: "server" }) },
    {
      onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
        received.push({
          envelope: { ...envelope, payload: Uint8Array.from(envelope.payload) },
          correlation,
        });
        if (correlation === 0) {
          return;
        }

        if (envelope.typeRef === "test.Echo") {
          const reply: ReplyEnvelope = {
            serializerId: SERIALIZER_BINARY,
            typeRef: "test.EchoReply",
            payload: encodePayload({ echoed: decodePayload(envelope.payload) }),
          };
          session.reply(correlation, reply);
          return;
        }

        if (envelope.typeRef === "test.Fail") {
          const wire: WireError = encodeError(ErrDead);
          session.replyError(correlation, {
            code: ERROR_APPLICATION,
            sentinel: wire.sentinel + 1,
            name: wire.name,
            message: wire.message,
          });
          return;
        }

        if (envelope.typeRef === "test.Late") {
          setTimeout((): void => {
            session.reply(correlation, {
              serializerId: SERIALIZER_BINARY,
              typeRef: "test.EchoReply",
              payload: encodePayload("late"),
            });
          }, 250);
          return;
        }

        // "test.Slow": never answer.
      },
    },
  );
  trackServer(server);
  return { server, received };
}

function dialTo(
  server: NetServer,
  local: Parameters<typeof dialSession>[1] = hello({ systemName: "client" }),
  handlers: Parameters<typeof dialSession>[2] = {},
): Promise<Session> {
  return dialSession(server.address.port, local, handlers);
}

describe("tells and asks end to end", () => {
  it("delivers a tell with its envelope and payload intact", async () => {
    const { server, received } = await startSeamServer();
    const client: Session = await dialTo(server);

    const envelope: DataEnvelope = data(KIND_TELL, "test.Note", { order: "o-1", amount: 25.5 });
    expect(client.tell(envelope)).toBeNull();

    await vi.waitFor((): void => {
      expect(received.length).toBe(1);
    });
    const seen: ReceivedData = received[0] as ReceivedData;
    expect(seen.correlation).toBe(0);
    expect(seen.envelope.kind).toBe(KIND_TELL);
    expect(seen.envelope.to).toBe(envelope.to);
    expect(seen.envelope.uid).toBe(envelope.uid);
    expect(seen.envelope.sender).toBe(envelope.sender);
    expect(seen.envelope.typeRef).toBe("test.Note");
    expect(decodePayload(seen.envelope.payload)).toEqual({ order: "o-1", amount: 25.5 });
  });

  it("settles pipelined asks by correlation, not order", async () => {
    const { server } = await startSeamServer();
    const client: Session = await dialTo(server);

    const answers: Promise<ReplyEnvelope>[] = [];
    for (let i = 0; i < 10; i++) {
      answers.push(client.ask(data(KIND_ASK, "test.Echo", { n: i }), 2000));
    }

    const replies: ReplyEnvelope[] = await Promise.all(answers);
    for (let i = 0; i < 10; i++) {
      expect(replies[i]?.typeRef).toBe("test.EchoReply");
      expect(decodePayload(replies[i]?.payload ?? EMPTY)).toEqual({ echoed: { n: i } });
    }
  });

  it("rejects a failed ask with the sentinel surviving identity", async () => {
    const { server } = await startSeamServer();
    const client: Session = await dialTo(server);

    const failure: PeerError = await client
      .ask(data(KIND_ASK, "test.Fail", null), 2000)
      .then(() => {
        throw new Error("the ask must not resolve");
      })
      .catch((error: unknown): PeerError => error as PeerError);

    expect(failure).toBeInstanceOf(PeerError);
    expect(failure.code).toBe(ERROR_APPLICATION);
    const restored: Error = decodeError({
      sentinel: failure.sentinel - 1,
      name: failure.errorName,
      message: failure.message,
    });
    expect(restored).toBe(ErrDead);
  });

  it("times out an ask locally and drops the late reply", async () => {
    const { server } = await startSeamServer();
    const client: Session = await dialTo(server);

    await expect(client.ask(data(KIND_ASK, "test.Late", null), 100)).rejects.toBe(ErrAskTimeout);

    // The late reply lands after the timeout and must vanish without
    // damage: the session still answers new asks.
    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 300);
    });
    const reply: ReplyEnvelope = await client.ask(data(KIND_ASK, "test.Echo", "still fine"), 2000);
    expect(decodePayload(reply.payload)).toEqual({ echoed: "still fine" });
  });

  it("answers an undecodable ask with a request-scoped error and lives on", async () => {
    const { server, received } = await startSeamServer();
    const client: Session = await dialTo(server);

    client.send({
      type: FRAME_DATA,
      flags: FLAG_EXPECTS_REPLY,
      lane: client.lane,
      correlation: client.nextCorrelation(),
      body: Uint8Array.from([9, 9, 9]),
    });

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 100);
    });
    expect(received.length).toBe(0);
    expect(server.activeConnections).toBe(1);

    const reply: ReplyEnvelope = await client.ask(data(KIND_ASK, "test.Echo", 42), 2000);
    expect(decodePayload(reply.payload)).toEqual({ echoed: 42 });
  });

  it("refuses tells over the admission budget with backpressure", async () => {
    const { server } = await startSeamServer();
    const client: Session = await dialTo(
      server,
      hello({ systemName: "client", initialCredits: 32 * 1024 }),
    );
    expect(client.effective?.initialCredits).toBe(32 * 1024);

    const payload: Uint8Array = new Uint8Array(16 * 1024);
    const results: (Error | null)[] = [];
    for (let i = 0; i < 6; i++) {
      results.push(client.tell(data(KIND_TELL, "test.Note", payload)));
    }

    expect(results[0]).toBeNull();
    expect(results).toContain(ErrBackpressure);

    // Once the burst drains, the budget frees up again.
    await vi.waitFor((): void => {
      expect(client.tell(data(KIND_TELL, "test.Note", "small"))).toBeNull();
    });
  });
});

describe("failure of one side", () => {
  it("fails every pending ask when the peer is killed", async () => {
    const { server } = await startSeamServer();
    const client: Session = await dialTo(server);

    const outcomes: Promise<Error>[] = [];
    for (let i = 0; i < 3; i++) {
      outcomes.push(
        client.ask(data(KIND_ASK, "test.Slow", i), 0).then(
          () => {
            throw new Error("the ask must not resolve");
          },
          (error: unknown): Error => error as Error,
        ),
      );
    }

    await server.shutdown(-1);
    const failures: Error[] = await Promise.all(outcomes);
    for (const failure of failures) {
      expect(failure).toBeInstanceOf(Error);
    }

    expect(client.closed).toBe(true);
  });

  it("fails pending asks with the peer's reason on graceful shutdown", async () => {
    const { server } = await startSeamServer();
    const client: Session = await dialTo(server);

    const outcome: Promise<Error> = client.ask(data(KIND_ASK, "test.Slow", null), 0).then(
      () => {
        throw new Error("the ask must not resolve");
      },
      (error: unknown): Error => error as Error,
    );

    await server.shutdown(2000);
    const failure: Error = await outcome;
    expect(failure).toBeInstanceOf(PeerError);
    expect((failure as PeerError).code).toBe(ERROR_UNAVAILABLE);
  });

  it("lets a seam turn connection loss into Terminated for watched actors", async () => {
    const { server, received } = await startSeamServer();

    // The client-side seam: remembers what it watched, and on close
    // synthesizes a Terminated notification per watched path.
    const watched: string[] = [];
    const terminated: string[] = [];
    const client: Session = await dialTo(server, hello({ systemName: "client" }), {
      onClose: (): void => {
        terminated.push(...watched);
      },
    });

    const watch: DataEnvelope = data(KIND_WATCH, "", null);
    expect(client.tell(watch)).toBeNull();
    watched.push(watch.to);

    await vi.waitFor((): void => {
      expect(received.length).toBe(1);
    });
    expect(received[0]?.envelope.kind).toBe(KIND_WATCH);
    expect(received[0]?.envelope.sender).toBe(watch.sender);

    await server.shutdown(-1);
    await vi.waitFor((): void => {
      expect(terminated).toEqual([watch.to]);
    });
  });
});

describe("full duplex over one connection", () => {
  it("lets the acceptor ask and tell over the connection the dialer opened", async () => {
    let accepted: Session | null = null;
    const serverSeen: number[] = [];
    const server: NetServer = trackServer(
      await NetServer.listen(
        { local: hello({ systemName: "server" }) },
        {
          onSession: (session: Session): void => {
            accepted = session;
          },
          onData: (session: Session, _envelope: DataEnvelope, correlation: number): void => {
            serverSeen.push(correlation);
            if (correlation === 0) {
              return;
            }

            session.reply(correlation, {
              serializerId: SERIALIZER_BINARY,
              typeRef: "test.ServerEcho",
              payload: encodePayload("answered by the acceptor"),
            });
          },
        },
      ),
    );

    const clientSeen: number[] = [];
    const clientTells: string[] = [];
    const client: Session = await dialTo(server, hello({ systemName: "client" }), {
      onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
        clientSeen.push(correlation);
        if (correlation !== 0) {
          session.reply(correlation, {
            serializerId: SERIALIZER_BINARY,
            typeRef: "test.ClientEcho",
            payload: encodePayload("answered by the dialer"),
          });
          return;
        }

        clientTells.push(envelope.typeRef);
      },
    });
    await vi.waitFor((): void => {
      expect(accepted).not.toBeNull();
    });
    const acceptor: Session = accepted as unknown as Session;

    // Both sides ask at once over the same byte stream.
    const [fromServer, fromClient] = await Promise.all([
      acceptor.ask(data(KIND_ASK, "test.AskClient", "same pipe"), 2000),
      client.ask(data(KIND_ASK, "test.Echo", "hello"), 2000),
    ]);
    expect(fromServer.typeRef).toBe("test.ClientEcho");
    expect(fromClient.typeRef).toBe("test.ServerEcho");

    // The acceptor initiates tells too.
    expect(acceptor.tell(data(KIND_TELL, "test.ServerNote", 7))).toBeNull();
    await vi.waitFor((): void => {
      expect(clientTells).toContain("test.ServerNote");
    });

    // The parity split holds: asks arriving at the dialer carry even
    // correlations, asks arriving at the acceptor odd ones.
    const toClient: number[] = clientSeen.filter((correlation): boolean => correlation !== 0);
    const toServer: number[] = serverSeen.filter((correlation): boolean => correlation !== 0);
    expect(toClient.length).toBeGreaterThan(0);
    expect(toServer.length).toBeGreaterThan(0);
    expect(toClient.every((correlation): boolean => correlation % 2 === 0)).toBe(true);
    expect(toServer.every((correlation): boolean => correlation % 2 === 1)).toBe(true);
  });
});

describe("handler failure scoping", () => {
  it("answers an ask whose handler throws with a request-scoped internal error", async () => {
    const server: NetServer = trackServer(
      await NetServer.listen(
        { local: hello({ systemName: "server" }) },
        {
          onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
            if (envelope.typeRef === "test.Boom") {
              throw new Error("kaboom");
            }

            if (correlation !== 0) {
              session.reply(correlation, {
                serializerId: SERIALIZER_BINARY,
                typeRef: "test.EchoReply",
                payload: encodePayload("ok"),
              });
            }
          },
        },
      ),
    );
    const client: Session = await dialTo(server);

    const failure: unknown = await client.ask(data(KIND_ASK, "test.Boom", null), 2000).then(
      (): null => null,
      (thrown: Error): Error => thrown,
    );
    expect(failure).toBeInstanceOf(PeerError);
    expect((failure as PeerError).code).toBe(ERROR_INTERNAL);
    expect((failure as PeerError).message).toBe("kaboom");

    // One message died; the connection did not.
    const reply: ReplyEnvelope = await client.ask(data(KIND_ASK, "test.Echo", "still"), 2000);
    expect(reply.typeRef).toBe("test.EchoReply");
  });

  it("drops a tell whose handler throws a non-error and keeps the connection", async () => {
    const server: NetServer = trackServer(
      await NetServer.listen(
        { local: hello({ systemName: "server" }) },
        {
          onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
            if (envelope.typeRef === "test.Boom") {
              // A foreign throw on purpose: the wrap must stringify
              // it rather than assume an Error.
              throw "kaboom-literal";
            }

            if (correlation !== 0) {
              session.reply(correlation, {
                serializerId: SERIALIZER_BINARY,
                typeRef: "test.EchoReply",
                payload: encodePayload("ok"),
              });
            }
          },
        },
      ),
    );
    const client: Session = await dialTo(server);

    expect(client.tell(data(KIND_TELL, "test.Boom", null))).toBeNull();
    const reply: ReplyEnvelope = await client.ask(data(KIND_ASK, "test.Echo", "alive"), 2000);
    expect(reply.typeRef).toBe("test.EchoReply");

    // The same foreign throw on an ask comes back stringified.
    const failure: unknown = await client.ask(data(KIND_ASK, "test.Boom", null), 2000).then(
      (): null => null,
      (thrown: Error): Error => thrown,
    );
    expect(failure).toBeInstanceOf(PeerError);
    expect((failure as PeerError).message).toBe("kaboom-literal");
  });
});
