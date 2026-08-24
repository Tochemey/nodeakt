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
 * The messages that cross the wire between the checkout node and the
 * payments node. Both nodes import this module, and the module-scope
 * `registerMessage` calls run on each, so `instanceof` narrowing
 * survives the hop in both directions.
 */

import { registerMessage } from "../../src/index";

/** A charge request: checkout asks payments to take the money. */
export class ChargeCard {
  constructor(
    readonly orderId: string,
    readonly amountCents: number,
    readonly last4: string,
  ) {}
}

/** A successful charge, answered by the payments actor. */
export class Receipt {
  constructor(
    readonly orderId: string,
    readonly transactionId: string,
    readonly amountCents: number,
  ) {}
}

/** A refused charge: a business outcome, not a failure. */
export class Declined {
  constructor(
    readonly orderId: string,
    readonly reason: string,
  ) {}
}

registerMessage(ChargeCard);
registerMessage(Receipt);
registerMessage(Declined);
