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
 * Showcase: every core, invisibly.
 *
 * Spawn one CPU-bound actor per core with `Props` and `ask` them all.
 * There is no worker, pool, or isolate wiring in this file. Cross-core
 * messages are registered classes, so `instanceof` still works.
 *
 * Run: make multicore
 */

import { availableParallelism } from "node:os";
import { ActorSystem, Props, TextLogger } from "../../src/index";
import { CountPrimes, type PrimeCount, PrimeCounter } from "./primes.actor";

const cores = availableParallelism();
const upTo = 1_000_000;

const logger = new TextLogger({ level: "debug" });
const system = new ActorSystem("multicore", { logger });
await system.start();

// Create one counter per core. Because we spawn with Props (data, not a
// live instance), the runtime is free to build each PrimeCounter on its own
// isolate; a live instance would pin them all to this core.
const counters = await Promise.all(
  Array.from({ length: cores }, (_, i) => system.spawn(`counter-${i}`, Props.create(PrimeCounter))),
);
logger.info(`spawned ${counters.length} counters, each on its own core`);

const me = system.noSender();

// Ask every counter at once. `ask` looks the same whether the actor is
// local or on another core; firing them together runs the cores in
// parallel. Each reply is a registered PrimeCount, typed on arrival.
const parallelStart = Date.now();
const answers = await Promise.all(
  counters.map((counter) => me.ask(counter, new CountPrimes(upTo), 60_000)),
);
const parallelMs = Date.now() - parallelStart;

for (const [i, answer] of answers.entries()) {
  const result = answer as PrimeCount;
  logger.info(`  counter-${i}: ${result.count} primes below ${result.upTo}`);
}

// The same work, but one ask at a time so nothing overlaps: the baseline
// the parallel run is faster than.
const serialStart = Date.now();
for (const counter of counters) {
  await me.ask(counter, new CountPrimes(upTo), 60_000);
}
const serialMs = Date.now() - serialStart;

logger.info(`parallel (all ${cores} cores at once): ${parallelMs} ms`);
logger.info(`serial   (one core at a time):        ${serialMs} ms`);
logger.info(`speedup: ${(serialMs / parallelMs).toFixed(1)}x`);

await system.stop();
