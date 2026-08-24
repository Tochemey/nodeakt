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

/**
 * The payments node: one actor that takes the money.
 *
 * It binds a remoting endpoint and spawns a `payments` actor. Nothing
 * here knows the checkout node exists: charges arrive as ordinary
 * messages from whatever node looked this actor up, and the reply to
 * `ctx.sender` routes back over the wire on its own.
 *
 * Run: docker compose -f examples/remoting/docker-compose.yml up --build
 * (or see examples/remoting/README.md for the two-terminal local run).
 */

import type { Actor, PID, ReceiveContext } from "../../src/index";
import { ActorSystem } from "../../src/index";
import { advertisedHost } from "./host";
import { ChargeCard, Declined, Receipt } from "./messages";

/** Charges above this limit are refused, so the demo shows declines. */
const CHARGE_LIMIT_CENTS: number = 50_000;

/** The one card the issuer always refuses, so the demo shows both
 * outcomes deterministically. */
const HOTLISTED_LAST4: string = "0042";

/**
 * The payment service as an actor: private state (the transaction
 * counter), one charge at a time, and a typed answer for every ask.
 * A decline is a business outcome and travels as a `Declined` message,
 * not as an error.
 */
class PaymentsActor implements Actor {
  private transactions: number = 0;

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message: unknown = ctx.message;
    if (!(message instanceof ChargeCard)) {
      return;
    }

    if (message.last4 === HOTLISTED_LAST4) {
      console.log(`[payments] DECLINED ${message.orderId}: card ending ${message.last4}`);
      ctx.response(new Declined(message.orderId, `card ending ${message.last4} refused`));
      return;
    }

    if (message.amountCents > CHARGE_LIMIT_CENTS) {
      console.log(`[payments] DECLINED ${message.orderId}: over the charge limit`);
      ctx.response(new Declined(message.orderId, "amount over the charge limit"));
      return;
    }

    this.transactions++;
    const transactionId: string = `txn-${String(this.transactions).padStart(5, "0")}`;
    console.log(
      `[payments] charged ${message.orderId}: $${(message.amountCents / 100).toFixed(2)} (${transactionId})`,
    );
    ctx.response(new Receipt(message.orderId, transactionId, message.amountCents));
  }

  postStop(): void {}
}

const host: string = await advertisedHost();
const port: number = Number(process.env.NODEAKT_PORT ?? "5100");

const system: ActorSystem = new ActorSystem("payments", {
  remote: { host, port },
});
await system.start();
const payments: PID = await system.spawn("payments", new PaymentsActor());

console.log(`[payments] up: ${payments.path().toString()}`);

async function shutdown(signal: string): Promise<void> {
  console.log(`[payments] ${signal} received, stopping`);
  await system.stop();
  process.exit(0);
}

process.on("SIGINT", (): void => {
  void shutdown("SIGINT");
});

process.on("SIGTERM", (): void => {
  void shutdown("SIGTERM");
});
