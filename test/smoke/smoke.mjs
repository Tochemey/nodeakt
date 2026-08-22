// The packaged smoke test: spawns actors through the multi-core runtime
// of the built, packed, and installed package and verifies a cross-core
// ask round-trips a correct answer. Run through run.sh, which packs the
// library and stages this file next to a real install.
import { ActorSystem, Props } from "@tochemey/nodeakt";
import { CountPrimes, PrimeCounter } from "./counter.actor.mjs";

const system = new ActorSystem("smoke");
await system.start();

const counters = await Promise.all(
  Array.from({ length: 2 }, (_, i) => system.spawn(`counter-${i}`, Props.create(PrimeCounter))),
);

const me = system.noSender();
const answers = await Promise.all(
  counters.map((counter) => me.ask(counter, new CountPrimes(100_000), 30_000)),
);

await system.stop();

for (const answer of answers) {
  if (answer.count !== 9592) {
    console.error(`FAIL: expected 9592 primes below 100000, got ${answer.count}`);
    process.exit(1);
  }
}

console.log("smoke OK: 2 cross-core asks answered correctly");
