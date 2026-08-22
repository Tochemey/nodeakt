// A consumer-side actor module for the packaged smoke test: it imports
// the built package by name, exactly as an npm install would resolve it.
import { registerActor, registerMessage } from "@tochemey/nodeakt";

export class CountPrimes {
  constructor(upTo) {
    this.upTo = upTo;
  }
}

export class PrimeCount {
  constructor(upTo, count) {
    this.upTo = upTo;
    this.count = count;
  }
}

registerMessage(CountPrimes);
registerMessage(PrimeCount);

function countPrimesBelow(upTo) {
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

export class PrimeCounter {
  preStart() {}

  receive(ctx) {
    if (ctx.message instanceof CountPrimes) {
      ctx.response(new PrimeCount(ctx.message.upTo, countPrimesBelow(ctx.message.upTo)));
    }
  }

  postStop() {}
}

registerActor(PrimeCounter);
