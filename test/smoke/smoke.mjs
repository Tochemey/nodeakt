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

// A scheduled one-shot must fire on every runtime: the scheduler's
// shared timer is the same machinery repeating schedules use.
let resolveTick;
const ticked = new Promise((resolve) => {
  resolveTick = resolve;
});
const probe = await system.spawn("probe", {
  preStart() {},
  receive(ctx) {
    // The first delivery is the PostStart announcement; the scheduled
    // tick is the string sent below.
    if (ctx.message === "tick") {
      resolveTick(ctx.message);
    }
  },
  postStop() {},
});
await system.scheduleOnce("tick", probe, 50);
const deadline = setTimeout(() => resolveTick("timeout"), 10_000);
const tick = await ticked;
clearTimeout(deadline);

await system.stop();

if (tick !== "tick") {
  console.error(`FAIL: expected the scheduled tick, got ${tick}`);
  process.exit(1);
}

for (const answer of answers) {
  if (answer.count !== 9592) {
    console.error(`FAIL: expected 9592 primes below 100000, got ${answer.count}`);
    process.exit(1);
  }
}

console.log("smoke OK: 2 cross-core asks answered correctly and a scheduled tick fired");
