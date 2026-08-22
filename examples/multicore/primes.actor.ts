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

import { type Actor, type ReceiveContext, registerActor, registerMessage } from "../../src/index";

/** Ask a counter to count the primes below `upTo`. Registered so
 * `instanceof` survives a hop to another core. */
export class CountPrimes {
  constructor(readonly upTo: number) {}
}

/** The reply, crossing back the other way. */
export class PrimeCount {
  constructor(
    readonly upTo: number,
    readonly count: number,
  ) {}
}

registerMessage(CountPrimes);
registerMessage(PrimeCount);

/** Deliberately naive trial division: the point is to burn CPU so the
 * parallel speedup is visible. */
function countPrimesBelow(upTo: number): number {
  let count = 0;

  for (let n = 2; n < upTo; n++) {
    let isPrime = true;
    for (let d = 2; d * d <= n; d++) {
      if (n % d === 0) {
        isPrime = false;
        break;
      }
    }

    if (isPrime) {
      count++;
    }
  }

  return count;
}

/** A CPU-bound actor. One per core, it counts primes on its own isolate. */
export class PrimeCounter implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof CountPrimes) {
      ctx.response(new PrimeCount(ctx.message.upTo, countPrimesBelow(ctx.message.upTo)));
    }
  }

  postStop(): void {}
}

registerActor(PrimeCounter);
