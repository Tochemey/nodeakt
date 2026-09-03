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
import { discardLogger } from "../../src/discard.logger";
import type { MetricsSnapshot, RemotingMetrics } from "../../src/observability/metric.snapshot";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { registerMessage } from "../../src/registration";
import { until } from "./helpers";

/**
 * The remoting section of the metrics snapshot: the peers a node holds,
 * the frames and bytes its transport has moved, and what its live
 * connections still hold for sending. The totals are per node and
 * monotonic over the node's life, so a connection that closes folds its
 * final counts in instead of taking them with it.
 */

class Ping {
  constructor(readonly n: number) {}
}

registerMessage(Ping);

/** Counts the pings it receives. */
class Counter implements Actor {
  count: number = 0;

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Ping) {
      this.count++;
    }
  }

  postStop(): void {}
}

/** A metered system with remoting on an ephemeral loopback port. */
function meteredSystem(name: string): ActorSystem {
  return new ActorSystem(name, {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0 },
    metrics: { enabled: true },
  });
}

/** The remoting section of a fresh snapshot, which a remoting-enabled system always carries. */
async function remotingOf(system: ActorSystem): Promise<RemotingMetrics> {
  const snapshot: MetricsSnapshot = await system.collectMetrics();
  if (snapshot.remoting === undefined) {
    throw new Error("expected the remoting section on a remoting-enabled system");
  }

  return snapshot.remoting;
}

/** Polls the remoting section until `check` holds, failing loudly after four seconds. */
async function untilRemoting(
  label: string,
  system: ActorSystem,
  check: (remoting: RemotingMetrics) => boolean,
): Promise<RemotingMetrics> {
  let latest: RemotingMetrics = await remotingOf(system);
  for (let i: number = 0; i < 800; i++) {
    if (check(latest)) {
      return latest;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 5);
    });
    latest = await remotingOf(system);
  }

  throw new Error(`timed out waiting for ${label}`);
}

/** Starts alpha and beta, spawns a counter on beta, and resolves alpha's handle to it. */
async function connectedPair(
  running: ActorSystem[],
): Promise<{ alpha: ActorSystem; beta: ActorSystem; counter: Counter; target: PID }> {
  const alpha: ActorSystem = meteredSystem("alpha");
  const beta: ActorSystem = meteredSystem("beta");
  running.push(alpha, beta);
  await alpha.start();
  await beta.start();
  const counter: Counter = new Counter();
  await beta.spawn("counter", counter);
  const target: PID = (await alpha.remoteLookup("127.0.0.1", beta.port(), "counter")) as PID;
  return { alpha, beta, counter, target };
}

describe("remoting metrics", () => {
  const running: ActorSystem[] = [];

  afterEach(async (): Promise<void> => {
    for (const system of running.splice(0)) {
      await system.stop();
    }
  });

  it("reports an idle transport with no peers and nothing moved", async () => {
    const alpha: ActorSystem = meteredSystem("alpha");
    running.push(alpha);
    await alpha.start();

    expect(await remotingOf(alpha)).toEqual({
      peers: 0,
      messagesSent: 0,
      messagesReceived: 0,
      bytesSent: 0,
      bytesReceived: 0,
      sendQueueBytes: 0,
    });
  });

  it("counts the peer each side holds and the frames and bytes they exchange", async () => {
    const { alpha, beta, counter, target } = await connectedPair(running);
    const outside: PID = alpha.noSender();
    for (let i: number = 0; i < 100; i++) {
      outside.tell(target, new Ping(i));
    }

    await until("every ping delivered", (): boolean => counter.count === 100);

    // The dialer holds a peer; the acceptor holds the session it accepted.
    const sender: RemotingMetrics = await remotingOf(alpha);
    expect(sender.peers).toBe(1);
    expect(sender.messagesSent).toBeGreaterThan(0);
    expect(sender.messagesReceived).toBeGreaterThan(0);
    expect(sender.bytesSent).toBeGreaterThan(0);
    expect(sender.bytesReceived).toBeGreaterThan(0);
    expect(sender.sendQueueBytes).toBe(0);

    // What one side sent the other has, by the time the wire is quiet.
    const receiver: RemotingMetrics = await untilRemoting(
      "the receiver to catch up with the sender",
      beta,
      (remoting: RemotingMetrics): boolean =>
        remoting.bytesReceived === sender.bytesSent &&
        remoting.messagesReceived === sender.messagesSent,
    );
    expect(receiver.peers).toBe(1);
    expect(receiver.bytesSent).toBe(sender.bytesReceived);
    expect(receiver.messagesSent).toBe(sender.messagesReceived);
    expect(receiver.sendQueueBytes).toBe(0);
  });

  it("keeps a dialed peer's totals after its node goes away", async () => {
    const { alpha, beta, counter, target } = await connectedPair(running);
    alpha.noSender().tell(target, new Ping(0));
    await until("the ping delivered", (): boolean => counter.count === 1);
    const before: RemotingMetrics = await remotingOf(alpha);

    // The acceptor stops: the dialer's lanes close, its peer is reclaimed,
    // and the totals fold into the node instead of leaving with the peer.
    await beta.stop();
    const after: RemotingMetrics = await untilRemoting(
      "the dialer to drop its peer",
      alpha,
      (remoting: RemotingMetrics): boolean => remoting.peers === 0,
    );
    expect(after.messagesSent).toBeGreaterThanOrEqual(before.messagesSent);
    expect(after.messagesReceived).toBeGreaterThanOrEqual(before.messagesReceived);
    expect(after.bytesSent).toBeGreaterThanOrEqual(before.bytesSent);
    expect(after.bytesReceived).toBeGreaterThanOrEqual(before.bytesReceived);
    expect(after.sendQueueBytes).toBe(0);
  });

  it("keeps an accepted session's totals after its dialer goes away", async () => {
    const { alpha, beta, counter, target } = await connectedPair(running);
    alpha.noSender().tell(target, new Ping(0));
    await until("the ping delivered", (): boolean => counter.count === 1);
    const before: RemotingMetrics = await remotingOf(beta);
    expect(before.peers).toBe(1);

    // The dialer stops: the acceptor's session closes and its totals fold in.
    await alpha.stop();
    const after: RemotingMetrics = await untilRemoting(
      "the acceptor to drop the closed session",
      beta,
      (remoting: RemotingMetrics): boolean => remoting.peers === 0,
    );
    expect(after.messagesSent).toBeGreaterThanOrEqual(before.messagesSent);
    expect(after.messagesReceived).toBeGreaterThanOrEqual(before.messagesReceived);
    expect(after.bytesSent).toBeGreaterThanOrEqual(before.bytesSent);
    expect(after.bytesReceived).toBeGreaterThanOrEqual(before.bytesReceived);
    expect(after.sendQueueBytes).toBe(0);
  });
});
