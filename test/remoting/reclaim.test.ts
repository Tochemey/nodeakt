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

import { describe, expect, it } from "vitest";
import type { Actor } from "../../src/actor";
import { ActorSystem } from "../../src/actor.system";
import { discardLogger } from "../../src/discard.logger";
import { Terminated } from "../../src/messages";
import { LANE_CONTROL } from "../../src/net/frame";
import type { Session } from "../../src/net/session";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import type { Remoting } from "../../src/remoting";
import { remoteSystem, sleep, until } from "./helpers";

/**
 * Peer reclaim: a peer whose connections have all closed, and whose
 * node no outbound watch references any more, is dropped from the peer
 * map on the lane-close sweep. Reclaim is bookkeeping only; the next
 * send to the node recreates the peer whole, so these tests pin both
 * the drop and the full watch cycle over a recreated peer.
 */

/** Does nothing; exists to be watched and stopped. */
class Idle implements Actor {
  preStart(): void {}

  receive(): void {}

  postStop(): void {}
}

/** Collects every Terminated it is notified with. */
class Watcher implements Actor {
  readonly terminated: string[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Terminated) {
      this.terminated.push(ctx.message.actorPath);
    }
  }

  postStop(): void {}
}

/** The live seam of a started system. */
function seamOf(system: ActorSystem): Remoting {
  return (system as unknown as { _remoting: Remoting })._remoting;
}

/** The accepted server-side sessions of a started system's endpoint. */
function acceptedSessions(system: ActorSystem): Set<Session> {
  return (seamOf(system) as unknown as { _server: { _sessions: Set<Session> } })._server._sessions;
}

/** One fabricated outbound-watch record, shaped like the seam's own. */
interface FakeWatch {
  readonly watcher: PID;
  readonly target: string;
  readonly node: string;
}

/** The seam's outbound watch registry, for interleavings no public
 * sequence of calls can force. */
function watchesOf(system: ActorSystem): Map<string, FakeWatch> {
  return (seamOf(system) as unknown as { _watches: Map<string, FakeWatch> })._watches;
}

describe("peer reclaim", () => {
  it("reclaims the peer of a dead node and serves a fresh watch cycle on its recreated peer", async () => {
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();
    const port: number = b.port();

    try {
      const watcher: Watcher = new Watcher();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      await b.spawn("subject", new Idle());

      const remote: PID = (await a.remoteLookup("127.0.0.1", port, "subject")) as PID;
      watcherPid.watch(remote);
      await sleep(50);
      expect(seamOf(a).peerCount).toBe(1);

      // The node dies: the watch settles and, with nothing referencing
      // the node, its peer entry goes with it.
      await b.stop();
      await until("the Terminated", (): boolean => watcher.terminated.length >= 1);
      await until("the peer to be reclaimed", (): boolean => seamOf(a).peerCount === 0);

      // The same endpoint comes back: dialing recreates the peer, and
      // the whole watch cycle works over it.
      const revived: ActorSystem = new ActorSystem("beta", {
        logger: discardLogger,
        remote: { host: "127.0.0.1", port },
      });
      await revived.start();

      try {
        await revived.spawn("subject", new Idle());
        const again: PID = (await a.remoteLookup("127.0.0.1", port, "subject")) as PID;
        watcherPid.watch(again);
        await sleep(50);
        expect(seamOf(a).peerCount).toBe(1);
      } finally {
        await revived.stop();
      }

      await until("the second Terminated", (): boolean => watcher.terminated.length >= 2);
      await until("the second reclaim", (): boolean => seamOf(a).peerCount === 0);
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("keeps the peer while another lane still serves the watch", async () => {
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      const watcher: Watcher = new Watcher();
      const watcherPid: PID = await a.spawn("watcher", watcher);
      const subject: PID = await b.spawn("subject", new Idle());

      // The lookup rides an ordinary lane and the watch the control
      // lane, so the peer holds two connections.
      const remote: PID = (await a.remoteLookup("127.0.0.1", b.port(), "subject")) as PID;
      watcherPid.watch(remote);
      await sleep(50);

      // Kill only the ordinary connection: its lane-close sweep must
      // not reclaim a peer whose control lane still carries the watch.
      for (const session of acceptedSessions(b)) {
        if (session.lane !== LANE_CONTROL) {
          session.destroy();
        }
      }

      await sleep(150);
      expect(seamOf(a).peerCount).toBe(1);

      // The watch is untouched: the subject's stop still notifies.
      await subject.shutdown();
      await until("the Terminated", (): boolean => watcher.terminated.length >= 1);
      expect(watcher.terminated[0]).toBe(remote.path().toString());
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("leaves a peer alone while any watch record still references its node", async () => {
    const a: ActorSystem = remoteSystem("alpha");
    const b: ActorSystem = remoteSystem("beta");
    await a.start();
    await b.start();

    try {
      await b.spawn("subject", new Idle());
      await a.remoteLookup("127.0.0.1", b.port(), "subject");
      expect(seamOf(a).peerCount).toBe(1);

      // A watch record can outlive every connection in interleavings
      // the wire cannot line up on demand (a watch racing a teardown);
      // fabricate one, plus a record of another node the scan must
      // step over, and prove the sweep keeps the peer.
      const noSender: PID = a.noSender();
      watchesOf(a).set("fake-other", {
        watcher: noSender,
        target: "t",
        node: "127.0.0.1:1",
      });
      watchesOf(a).set("fake-here", {
        watcher: noSender,
        target: "t",
        node: `127.0.0.1:${b.port()}`,
      });

      // The lookup rode an ordinary lane, so no watch settlement runs
      // on its close and the fabricated records survive to the sweep.
      for (const session of acceptedSessions(b)) {
        session.destroy();
      }

      await sleep(150);
      expect(seamOf(a).peerCount).toBe(1);

      watchesOf(a).delete("fake-other");
      watchesOf(a).delete("fake-here");
    } finally {
      await a.stop();
      await b.stop();
    }
  });
});
