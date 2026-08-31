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
import type { Actor } from "../src/actor";
import type { ActorSystem } from "../src/actor.system";
import { UnboundedFairMailbox } from "../src/fair.mailbox";
import { newPath } from "../src/path";
import { PID } from "../src/pid";
import { createReceiveContext, type ReceiveContext } from "../src/receive.context";
import { ctx, describeMailboxContract, drain } from "./mailbox.contract";

const fairKey = (msg: ReceiveContext): string => (msg.message as { sender: string }).sender;

const noop: Actor = {
  preStart(): void {},
  receive(): void {},
  postStop(): void {},
};

function senderPid(name: string): PID {
  return new PID(noop, newPath(name, "sys", "127.0.0.1", 0), {
    metricRegistry: () => null,
  } as unknown as ActorSystem);
}

describe("UnboundedFairMailbox", () => {
  describeMailboxContract(() => new UnboundedFairMailbox(() => "sender"));

  it("round-robins across senders while keeping per-sender FIFO", () => {
    const mb = new UnboundedFairMailbox(fairKey);
    // A chatty sender floods before quieter ones get a word in.
    for (let i = 0; i < 3; i++) {
      mb.enqueue(ctx({ sender: "chatty", n: i }));
    }

    mb.enqueue(ctx({ sender: "quiet", n: 0 }));
    mb.enqueue(ctx({ sender: "calm", n: 0 }));

    const order = drain(mb).map((m) => {
      const { sender, n } = m as { sender: string; n: number };
      return `${sender}:${n}`;
    });
    expect(order).toEqual(["chatty:0", "quiet:0", "calm:0", "chatty:1", "chatty:2"]);
  });

  it("keys by sender path by default, pooling senderless messages", () => {
    const mb = new UnboundedFairMailbox();
    const chatty = senderPid("chatty");
    const quiet = senderPid("quiet");

    for (let i = 0; i < 3; i++) {
      mb.enqueue(createReceiveContext(`chatty:${i}`, undefined, chatty));
    }

    mb.enqueue(createReceiveContext("quiet:0", undefined, quiet));
    // Messages without a sender share one sub-queue of their own.
    mb.enqueue(createReceiveContext("anon:0"));

    expect(drain(mb)).toEqual(["chatty:0", "quiet:0", "anon:0", "chatty:1", "chatty:2"]);
  });

  it("keeps serving a sender that stays active across turns", () => {
    const mb = new UnboundedFairMailbox(fairKey);
    mb.enqueue(ctx({ sender: "a", n: 0 }));
    expect(mb.dequeue()?.message).toEqual({ sender: "a", n: 0 });

    // The sender's sub-queue drained; a later message must still arrive.
    mb.enqueue(ctx({ sender: "a", n: 1 }));
    expect(mb.dequeue()?.message).toEqual({ sender: "a", n: 1 });
    expect(mb.isEmpty()).toBe(true);
  });
});
