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

import {
  type DataEnvelope,
  type Hello,
  KIND_ASK,
  KIND_TELL,
  type ReplyEnvelope,
  SERIALIZER_BINARY,
} from "../../src/net/envelope";
import { LANE_CONTROL } from "../../src/net/frame";
import { Peer } from "../../src/net/peer";
import { NetServer } from "../../src/net/server";
import type { Session } from "../../src/net/session";

/**
 * The transport smoke, run per runtime by test/smoke/net.sh: one
 * server, one peer, and the protocol's load-bearing behaviors end to
 * end on real sockets. Tells with kernel-confirmed delivery, an ask
 * round trip, a chunked megabyte through the large lane, an
 * acceptor-initiated ask over the dialed connection, and a clean
 * shutdown. Exits nonzero on the first broken expectation or after
 * the watchdog deadline.
 */

const TELLS: number = 200;
const BIG_BYTES: number = 1024 * 1024;

function fail(label: string): never {
  console.error(`FAIL: ${label}`);
  process.exit(1);
}

function check(condition: boolean, label: string): void {
  if (!condition) {
    fail(label);
  }
}

async function waitUntil(label: string, read: () => boolean): Promise<void> {
  for (let i = 0; i < 1000; i++) {
    if (read()) {
      return;
    }

    await new Promise<void>((resolve: () => void): void => {
      setTimeout(resolve, 5);
    });
  }

  fail(`timed out waiting for ${label}`);
}

function helloOf(systemName: string): Hello {
  return {
    revision: 4,
    systemName,
    host: "127.0.0.1",
    port: 0,
    lane: LANE_CONTROL,
    compression: 0,
    maxFrameSize: 4 * 1024 * 1024,
    maxMessageSize: 4 * 1024 * 1024,
    initialCredits: 4 * 1024 * 1024,
    maxLargeTransfers: 4,
  };
}

function envelopeOf(kind: number, payload: Uint8Array): DataEnvelope {
  return {
    kind,
    to: "nodeakt://smoke@127.0.0.1:0/user/probe",
    uid: "s1",
    sender: "",
    senderUid: "",
    timeout: 0,
    serializerId: SERIALIZER_BINARY,
    typeRef: "smoke.Probe",
    payload,
  };
}

const watchdog: NodeJS.Timeout = setTimeout((): void => {
  fail("smoke timed out after 20s");
}, 20_000);

const runtime: string =
  "bun" in process.versions
    ? `bun ${process.versions.bun}`
    : "deno" in process.versions
      ? `deno ${(process.versions as { deno?: string }).deno}`
      : `node ${process.versions.node}`;

let tellsReceived: number = 0;
let bigReceived: number = 0;
let accepted: Session | null = null;

const server: NetServer = await NetServer.listen(
  { local: helloOf("smoke-server") },
  {
    onSession: (session: Session): void => {
      accepted = accepted ?? session;
    },
    onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
      if (correlation !== 0) {
        session.reply(correlation, {
          serializerId: SERIALIZER_BINARY,
          typeRef: "smoke.Echo",
          payload: Uint8Array.from(envelope.payload),
        });
        return;
      }

      if (envelope.payload.length >= BIG_BYTES) {
        bigReceived += 1;
        return;
      }

      tellsReceived += 1;
    },
  },
);

const peer: Peer = new Peer("127.0.0.1", server.address.port, helloOf("smoke-client"), {
  onData: (session: Session, envelope: DataEnvelope, correlation: number): void => {
    if (correlation !== 0) {
      session.reply(correlation, {
        serializerId: SERIALIZER_BINARY,
        typeRef: "smoke.ClientEcho",
        payload: Uint8Array.from(envelope.payload),
      });
    }
  },
  onDeadLetter: (_envelope: DataEnvelope, reason: Error): void => {
    fail(`tell dead-lettered: ${reason.message}`);
  },
});

// Fire-and-forget tells, delivered and counted.
for (let i = 0; i < TELLS; i++) {
  peer.tell(envelopeOf(KIND_TELL, Uint8Array.from([i & 0xff])));
}

await waitUntil("tells", (): boolean => tellsReceived === TELLS);

// An ask round trip with the payload echoed back intact.
const payload: Uint8Array = new Uint8Array(64).fill(0x2a);
const reply: ReplyEnvelope = await peer.ask(envelopeOf(KIND_ASK, payload), 5000);
check(reply.typeRef === "smoke.Echo", "ask reply type");
check(reply.payload.length === payload.length && reply.payload[3] === 0x2a, "ask reply payload");

// A megabyte rides the large lane as a chunked transfer.
peer.tell(envelopeOf(KIND_TELL, new Uint8Array(BIG_BYTES).fill(0x11)));
await waitUntil("chunked transfer", (): boolean => bigReceived === 1);

// Full duplex: the acceptor asks back over a connection it accepted.
await waitUntil("accepted session", (): boolean => accepted !== null);
const back: ReplyEnvelope = await (accepted as unknown as Session).ask(
  envelopeOf(KIND_ASK, Uint8Array.from([7])),
  5000,
);
check(back.typeRef === "smoke.ClientEcho", "acceptor-initiated ask");

peer.close();
await server.shutdown(1000);
check(server.activeConnections === 0, "connections drained");

clearTimeout(watchdog);
console.log(`PASS: net smoke on ${runtime}`);
process.exit(0);
