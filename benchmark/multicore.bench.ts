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

import { availableParallelism } from "node:os";
import { resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { ActorRef } from "../src/actor.ref";
import { ActorSystem } from "../src/actor.system";
import { discardLogger } from "../src/discard.logger";
import { MessageRegistry } from "../src/message.registry";
import { WorkerPool } from "../src/worker.pool";
import { printBlock, printMachine } from "./harness";

/**
 * Multi-core behavior of the worker pool, three angles:
 *
 * - CPU-bound scaling: a fixed amount of CPU work spread over 1..N
 *   isolates; wall time shows how much of the machine the runtime
 *   uses. One isolate is the serial baseline.
 * - Cross-isolate messaging: pipelined ask round trips from the main
 *   isolate through the port and codec; the price of a message that
 *   leaves its isolate, and why cross-worker traffic should be the
 *   exception.
 * - Machine capacity by method: for tell and ask, the cumulative
 *   messages per second one machine processes, locally inside one
 *   isolate, locally on every isolate at once, and across the port
 *   boundary.
 *
 * The pool sizes itself: one V8 isolate per core, from
 * `os.availableParallelism()`.
 */

const cores = availableParallelism();
const outDir = resolve("node_modules/.cache/nodeakt-worker-bench");
const entry = resolve(outDir, "worker.entry.mjs");
const burnerModule = new URL("../test/fixtures/burner.actor.mjs", import.meta.url).href;
const counterModule = new URL("../test/fixtures/counter.actor.mjs", import.meta.url).href;
const echoModule = new URL("../test/fixtures/echo.actor.mjs", import.meta.url).href;
const flooderModule = new URL("../test/fixtures/flooder.actor.mjs", import.meta.url).href;
const meminfoModule = new URL("../test/fixtures/meminfo.actor.mjs", import.meta.url).href;

/** The isolate counts to measure: powers of two up to the core count,
 * and the core count itself. */
function fanouts(): number[] {
  const counts: number[] = [];
  for (let k = 1; k < cores; k *= 2) {
    counts.push(k);
  }

  counts.push(cores);
  return counts;
}

/** Formats a rate as millions per second. */
function rate(messages: number, wallMs: number): string {
  return `${(messages / wallMs / 1000).toFixed(2)}M/s`;
}

describe("multi-core", () => {
  let system: ActorSystem;
  let pool: WorkerPool;
  const burners: ActorRef[] = [];
  const echoes: ActorRef[] = [];
  const flooders: ActorRef[] = [];

  beforeAll(async () => {
    const { build } = await import("tsdown");
    await build({
      entry: { "worker.entry": "src/worker.entry.ts" },
      outDir,
      format: "esm",
      dts: false,
      clean: true,
      logLevel: "silent",
      config: false,
    });

    printMachine();
    system = new ActorSystem("bench", { logger: discardLogger });
    await system.start();
    pool = new WorkerPool(system, new MessageRegistry(), { size: cores, entry, quiet: true });
    await pool.start();
    expect(pool.workers().length).toBe(cores);

    // Placement fills workers of level occupancy in pool order, so
    // placing one of each kind per core pins one burner, one echo, and
    // one flooder on every isolate.
    for (let i = 0; i < cores; i++) {
      burners.push(await pool.place(`burner-${i}`, { module: burnerModule, actor: "Burner" }));
    }

    for (let i = 0; i < cores; i++) {
      echoes.push(await pool.place(`echo-${i}`, { module: echoModule, actor: "Echo" }));
    }

    for (let i = 0; i < cores; i++) {
      flooders.push(await pool.place(`flooder-${i}`, { module: flooderModule, actor: "Flooder" }));
    }
  }, 120_000);

  afterAll(async () => {
    await pool.stop();
    await system.stop();
  });

  it("scales CPU-bound processing across auto-sized isolates", async () => {
    // Calibrate fixed work against this machine: enough iterations to
    // keep one isolate busy for roughly 1.2 seconds. Fixed work, not
    // fixed time, so heterogeneous cores show up honestly in the
    // speedup column.
    const probe = 5_000_000;
    const calibrationStart = Date.now();
    let acc = 1;
    let n = probe;
    while (n-- > 0) {
      acc = (acc * 48271 + n) % 2147483647;
    }

    const perMs = probe / Math.max(1, Date.now() - calibrationStart);
    const totalIterations = Math.round(perMs * 1200) + (acc % 2);

    const rows: string[][] = [];
    let serialWall = 0;

    for (const k of fanouts()) {
      const perWorker = Math.round(totalIterations / k);
      const started = Date.now();
      await Promise.all(burners.slice(0, k).map((ref) => ref.ask(`work:${perWorker}`, 300_000)));
      const wall = Date.now() - started;

      if (k === 1) {
        serialWall = wall;
      }

      const speedup = serialWall / wall;
      rows.push([
        String(k),
        `${k}x${(perWorker / 1_000_000).toFixed(0)}M iter`,
        `${wall} ms`,
        `${speedup.toFixed(1)}x`,
        `${((speedup / k) * 100).toFixed(0)}%`,
      ]);
    }

    printBlock(
      `CPU-bound scaling: pool sized itself to ${cores} isolates (os.availableParallelism)`,
      ["isolates", "cpu work", "wall", "speedup", "efficiency"],
      rows,
      "fixed total work per row; sub-linear rows expose heterogeneous cores, not runtime overhead",
    );

    // The machine's cores must actually show up: with every isolate
    // working at once, the run must beat the serial baseline by at
    // least half the ideal factor.
    if (cores >= 2) {
      const wallAtFull = Number.parseInt((rows[rows.length - 1] as string[])[2] as string, 10);
      expect(serialWall / wallAtFull).toBeGreaterThan(cores / 2);
    }
  });

  it("measures cross-isolate messaging through the ports", async () => {
    const total = 60_000;
    const lanes = 256;
    const rows: string[][] = [];
    let best = 0;

    for (const k of fanouts()) {
      const perWorker = Math.ceil(total / k);
      const perLane = Math.ceil(perWorker / lanes);
      const sent = k * lanes * perLane;

      const started = Date.now();
      await Promise.all(
        echoes.slice(0, k).flatMap((ref) =>
          Array.from({ length: lanes }, async () => {
            for (let i = 0; i < perLane; i++) {
              await ref.ask("ping", 300_000);
            }
          }),
        ),
      );
      const wall = Date.now() - started;
      const perSecond = Math.round((sent / wall) * 1000);
      best = Math.max(best, perSecond);

      rows.push([String(k), String(sent), String(lanes * k), `${wall} ms`, `${perSecond}/s`]);
    }

    printBlock(
      "cross-isolate messaging: ask round trips from the main isolate, port + codec per message",
      ["isolates", "round trips", "in flight", "wall", "rt/sec"],
      rows,
      "every message pays the port hop; this is why cross-worker traffic should be the exception",
    );

    expect(best).toBeGreaterThan(10_000);
  });

  it("reports cumulative machine capacity by method", async () => {
    // Local rates come from the flooders: each isolate measures its
    // own processing and the machine number is total messages over
    // the concurrent wall.
    const local = async (command: string, perIsolate: number, k: number): Promise<number> => {
      const started = Date.now();
      await Promise.all(
        flooders.slice(0, k).map((ref) => ref.ask(`${command}:${perIsolate}`, 300_000)),
      );

      return Math.round(((perIsolate * k) / Math.max(1, Date.now() - started)) * 1000);
    };

    // Cross-isolate tell: fire the batch at each worker's echo actor
    // and close with one ask; FIFO per port and per mailbox means the
    // barrier answers only after every tell was processed.
    const crossTell = async (k: number, total: number): Promise<number> => {
      const perWorker = Math.ceil(total / k);
      const started = Date.now();
      await Promise.all(
        echoes.slice(0, k).map(async (ref) => {
          for (let i = 0; i < perWorker; i++) {
            ref.tell(i);
          }

          await ref.ask("done", 300_000);
        }),
      );

      return Math.round(((perWorker * k) / Math.max(1, Date.now() - started)) * 1000);
    };

    // Cross-isolate ask: pipelined round trips, as in the messaging
    // scenario, sized down to a sample.
    const crossAsk = async (k: number, total: number): Promise<number> => {
      const lanes = 256;
      const perLane = Math.ceil(total / k / lanes);
      const started = Date.now();
      await Promise.all(
        echoes.slice(0, k).flatMap((ref) =>
          Array.from({ length: lanes }, async () => {
            for (let i = 0; i < perLane; i++) {
              await ref.ask("ping", 300_000);
            }
          }),
        ),
      );

      return Math.round(((k * lanes * perLane) / Math.max(1, Date.now() - started)) * 1000);
    };

    const capacity: Array<[string, number, number]> = [
      ["tell", await local("flood", 2_000_000, 1), await local("flood", 2_000_000, cores)],
      ["ask", await local("askflood", 700_000, 1), await local("askflood", 700_000, cores)],
    ];

    printBlock(
      `machine capacity by method: cumulative messages per second (${cores} isolates)`,
      ["method", "1 isolate", `${cores} isolates`, "scaling"],
      capacity.map(([method, one, all]) => [
        method,
        rate(one, 1000),
        rate(all, 1000),
        `${(all / one).toFixed(1)}x`,
      ]),
      "every isolate processes its own local traffic at once; a local request is an ask plus one turn",
    );

    const ceiling: Array<[string, number, number]> = [
      ["tell", await crossTell(1, 200_000), await crossTell(cores, 200_000)],
      ["ask", await crossAsk(1, 40_000), await crossAsk(cores, 40_000)],
    ];

    printBlock(
      "cross-isolate ceiling of one sending isolate (here: main), by method",
      ["method", "1 target", `${cores} targets`],
      ceiling.map(([method, one, all]) => [method, rate(one, 1000), rate(all, 1000)]),
      [
        "not machine capacity: encoding and cloning serialize on the sending isolate, so this",
        "is a per-sender ceiling (about 1.6us per tell, 3us per ask round trip) that adding",
        "target workers cannot lift; every additional sending isolate brings its own ceiling.",
        "request rides the ask path. Keep chatty actors on one isolate; cross the boundary to",
        "hand off work, not to converse.",
      ].join("\n"),
    );

    // The threshold guards against a scaling collapse (a serial
    // bottleneck would measure ~1x), with margin for the thermal
    // throttling a long benchmark session induces on laptop-class
    // machines; cold runs measure 2.5x and above.
    const tellLocal = capacity[0] as [string, number, number];
    const askLocal = capacity[1] as [string, number, number];
    if (cores >= 4) {
      expect(tellLocal[2] / tellLocal[1]).toBeGreaterThan(1.5);
      expect(askLocal[2] / askLocal[1]).toBeGreaterThan(1.5);
    }
  });

  it("measures per-worker baseline memory and port-queue lag", async () => {
    // Baseline heap: every isolate reports its own V8 heap, holding
    // only the facade runtime, the bench actors, and one probe.
    const probes: ActorRef[] = [];
    for (let i = 0; i < cores; i++) {
      probes.push(await pool.place(`mem-${i}`, { module: meminfoModule, actor: "MemInfo" }));
    }

    const heaps = (await Promise.all(probes.map((ref) => ref.ask("heap", 60_000)))) as number[];
    const mb = heaps.map((bytes) => bytes / 1024 / 1024).sort((a, b) => a - b);
    const avgMb = mb.reduce((sum, value) => sum + value, 0) / mb.length;

    // Port-queue lag: flood one isolate with stamped tells and let the
    // receiver sample how long each spent between the sender's clock
    // and its own. Depth has no direct API on a MessagePort; lag times
    // observed throughput is the honest estimate of the backlog.
    const counter = await pool.place("depth-counter", {
      module: counterModule,
      actor: "Counter",
    });
    const total = 200_000;
    const started = Date.now();
    for (let i = 0; i < total; i++) {
      counter.tell({ n: i, t: Date.now() });
    }

    const report = (await counter.ask("report", 60_000)) as { count: number; lags: number[] };
    const wall = Math.max(1, Date.now() - started);
    const perSecond = Math.round((report.count / wall) * 1000);
    const lags = [...report.lags].sort((a, b) => a - b);
    const at = (q: number) =>
      lags[Math.min(lags.length - 1, Math.floor(lags.length * q))] as number;
    const depthAtP99 = Math.round((at(0.99) / 1000) * perSecond);

    printBlock(
      "step-5 measurements: what the pool costs and what the port hides",
      ["measure", "value"],
      [
        [
          "per-isolate baseline heap",
          `${avgMb.toFixed(1)} MB avg (${(mb[0] as number).toFixed(1)}..${(mb[mb.length - 1] as number).toFixed(1)} MB, ${cores} isolates)`,
        ],
        [
          "port-queue lag under flood",
          `p50 ${at(0.5)} ms, p99 ${at(0.99)} ms, max ${lags[lags.length - 1]} ms`,
        ],
        ["port-queue depth at p99", `~${depthAtP99} envelopes (at ${rate(perSecond, 1000)})`],
      ],
      "MessagePort exposes no queue-depth API: lag times observed throughput estimates the backlog\nbetween mailbox limits; if this grows in practice, the spec's answer is credit-based flow control",
    );

    expect(heaps.length).toBe(cores);
    expect(report.count).toBe(total);
    expect(lags.length).toBeGreaterThan(0);
  });
});
