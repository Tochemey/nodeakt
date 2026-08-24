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
 * The checkout node: takes orders and gets them paid by the payments
 * node, and survives that node's death.
 *
 * One desk actor does everything through ordinary messages: a schedule
 * mints an order every tick, a remote lookup resolves the payments
 * actor into a PID, each charge is an ask piped back to the desk's own
 * mailbox, and a death watch turns the payments node's disappearance
 * into a `Terminated` message. While payments is gone the desk queues
 * orders; when the node returns, the backlog flushes.
 *
 * Run: docker compose -f examples/remoting/docker-compose.yml up --build
 * (or see examples/remoting/README.md for the two-terminal local run).
 */

import type { Actor, PID, ReceiveContext } from "../../src/index";
import { ActorSystem, PostStart, Terminated } from "../../src/index";
import { advertisedHost } from "./host";
import { ChargeCard, Declined, Receipt } from "./messages";

/** How often the shop takes a new order. */
const ORDER_INTERVAL_MS: number = 1500;

/** How long a charge may wait for the payments node's answer. */
const CHARGE_TIMEOUT_MS: number = 2000;

/** How long to wait between lookup attempts while payments is down. */
const RETRY_MS: number = 1000;

// The desk's local protocol. None of it crosses the wire, so none of
// it is registered: only `messages.ts` travels.

/** The repeating tick that mints the next order. */
class NextOrder {}

/** The located payments actor, delivered by the lookup pipe. */
class PaymentsUp {
  constructor(readonly pid: PID) {}
}

/** A charge that failed in transit (timeout, connection loss) instead
 * of answering; the order is kept for the next incarnation. */
class ChargeFailed {
  constructor(
    readonly charge: ChargeCard,
    readonly reason: Error,
  ) {}
}

/** What the shop sells; the last two entries exercise both decline
 * paths of the payments actor deterministically. */
interface CartTemplate {
  readonly item: string;
  readonly amountPence: number;
  readonly last4: string;
}

const CARTS: readonly CartTemplate[] = [
  { item: "espresso beans, 1kg", amountPence: 2350, last4: "4242" },
  { item: "pour-over kettle", amountPence: 6900, last4: "4242" },
  { item: "hand grinder", amountPence: 12800, last4: "4242" },
  { item: "barista course", amountPence: 89900, last4: "4242" },
  { item: "ceramic mugs, set of 4", amountPence: 4200, last4: "0042" },
];

function pause(ms: number): Promise<void> {
  return new Promise<void>((resolve: () => void): void => {
    setTimeout(resolve, ms);
  });
}

/**
 * The whole shop as one actor. Every outcome, an order tick, the
 * located service, a receipt, a decline, a transport failure, a death
 * notification, arrives as a message on the desk's own turn, so the
 * backlog and the payments handle need no lock and no reconnection
 * callback soup.
 */
class CheckoutDesk implements Actor {
  private payments: PID | null = null;
  private readonly backlog: ChargeCard[] = [];
  private minted: number = 0;

  constructor(
    private readonly system: ActorSystem,
    private readonly paymentsHost: string,
    private readonly paymentsPort: number,
  ) {}

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message: unknown = ctx.message;

    if (message instanceof PostStart) {
      console.log("[checkout] desk open; resolving the payments service");
      ctx.pipeTo(ctx.self as PID, this.locatePayments());
      // The shop takes orders whether or not payments is up. The
      // schedule is owned by this actor and dies with it; nothing here
      // can make the registration reject, so the promise is dropped.
      void ctx.schedule(new NextOrder(), ctx.self as PID, ORDER_INTERVAL_MS);
      return;
    }

    if (message instanceof NextOrder) {
      this.placeOrder(ctx);
      return;
    }

    if (message instanceof PaymentsUp) {
      this.payments = message.pid;
      // Death watch: the payments actor stopping, or its whole node
      // dying, comes back as one Terminated message.
      ctx.watch(message.pid);
      console.log(`[checkout] payments connected: ${message.pid.path().toString()}`);
      this.flushBacklog(ctx);
      return;
    }

    if (message instanceof Terminated) {
      this.payments = null;
      console.log("[checkout] payments is GONE; queueing orders and re-resolving");
      ctx.pipeTo(ctx.self as PID, this.locatePayments());
      return;
    }

    if (message instanceof Receipt) {
      console.log(`[checkout] ${message.orderId} paid (${message.transactionId})`);
      return;
    }

    if (message instanceof Declined) {
      console.log(`[checkout] ${message.orderId} declined: ${message.reason}`);
      return;
    }

    if (message instanceof ChargeFailed) {
      this.backlog.push(message.charge);
      console.log(
        `[checkout] ${message.charge.orderId} unsettled (${message.reason.message}); queued for retry`,
      );
    }
  }

  postStop(): void {}

  /** Mints the next order and charges it, or queues it while the
   * payments node is away. */
  private placeOrder(ctx: ReceiveContext): void {
    const template: CartTemplate = CARTS[this.minted % CARTS.length] as CartTemplate;
    this.minted++;
    const charge: ChargeCard = new ChargeCard(
      `ord-${String(this.minted).padStart(4, "0")}`,
      template.amountPence,
      template.last4,
    );
    console.log(
      `[checkout] order ${charge.orderId}: ${template.item} (£${(template.amountPence / 100).toFixed(2)})`,
    );

    if (this.payments === null) {
      this.backlog.push(charge);
      console.log(`[checkout] ${charge.orderId} queued; ${this.backlog.length} order(s) waiting`);
      return;
    }

    this.charge(ctx, charge);
  }

  /** Asks the payments node to take the money. The ask crosses the
   * wire, and its settlement comes back as a message on this actor's
   * own turn, so the desk never parks while a charge is in flight. */
  private charge(ctx: ReceiveContext, charge: ChargeCard): void {
    const payments: PID = this.payments as PID;
    ctx.pipeTo(
      ctx.self as PID,
      ctx
        .ask(payments, charge, CHARGE_TIMEOUT_MS)
        .catch((err: Error): ChargeFailed => new ChargeFailed(charge, err)),
    );
  }

  private flushBacklog(ctx: ReceiveContext): void {
    if (this.backlog.length === 0) {
      return;
    }

    console.log(`[checkout] flushing ${this.backlog.length} queued order(s)`);
    for (const charge of this.backlog) {
      this.charge(ctx, charge);
    }

    this.backlog.length = 0;
  }

  /** Resolves the payments actor, retrying until its node answers. The
   * task runs off the desk's turn and touches only the immutable fields
   * captured at construction; the result arrives as a message. */
  private async locatePayments(): Promise<PaymentsUp> {
    for (;;) {
      try {
        const pid: PID | undefined = await this.system.remoteLookup(
          this.paymentsHost,
          this.paymentsPort,
          "payments",
        );
        if (pid !== undefined) {
          return new PaymentsUp(pid);
        }
      } catch {
        // The node is not up yet, or went away mid-dial; keep trying.
      }

      await pause(RETRY_MS);
    }
  }
}

const host: string = await advertisedHost();
const port: number = Number(process.env.NODEAKT_PORT ?? "5200");
const paymentsHost: string = process.env.PAYMENTS_HOST ?? "127.0.0.1";
const paymentsPort: number = Number(process.env.PAYMENTS_PORT ?? "5100");

const system: ActorSystem = new ActorSystem("checkout", {
  remote: { host, port },
});
await system.start();
await system.spawn("desk", new CheckoutDesk(system, paymentsHost, paymentsPort));

console.log(
  `[checkout] up at ${host}:${system.port()}, paying through ${paymentsHost}:${paymentsPort}`,
);

async function shutdown(signal: string): Promise<void> {
  console.log(`[checkout] ${signal} received, stopping`);
  await system.stop();
  process.exit(0);
}

process.on("SIGINT", (): void => {
  void shutdown("SIGINT");
});
process.on("SIGTERM", (): void => {
  void shutdown("SIGTERM");
});
