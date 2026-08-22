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

import { execSync } from "node:child_process";
import { arch, availableParallelism, cpus, platform, release, totalmem } from "node:os";
import { PerformanceObserver } from "node:perf_hooks";

/** Reads one sysctl value, or undefined where sysctl does not exist. */
function sysctl(name: string): string | undefined {
  try {
    return execSync(`sysctl -n ${name}`, { stdio: ["ignore", "pipe", "ignore"] })
      .toString()
      .trim();
  } catch {
    return undefined;
  }
}

let machinePrinted = false;

/** Left margin on every printed line, so each report reads as one framed
 * block rather than text flush against the terminal edge. */
const INDENT = "  ";

/** The gutter between table columns. */
const GUTTER = "  ";

/**
 * Prints the machine the numbers were measured on: CPU brand and model,
 * core topology (performance and efficiency cores where the OS exposes
 * them), memory, and the runtime versions, as two dense lines: hardware
 * then software. Throughput numbers mean nothing without this context,
 * so every report leads with it; printing is once per process.
 */
export function printMachine(): void {
  if (machinePrinted) {
    return;
  }

  machinePrinted = true;
  const processors = cpus();
  const brand = processors[0]?.model ?? "unknown cpu";

  let topology = `${processors.length} cores`;
  const model = platform() === "darwin" ? sysctl("hw.model") : undefined;
  const performance = platform() === "darwin" ? sysctl("hw.perflevel0.logicalcpu") : undefined;
  const efficiency = platform() === "darwin" ? sysctl("hw.perflevel1.logicalcpu") : undefined;
  if (performance !== undefined && efficiency !== undefined) {
    topology += ` (${performance}P + ${efficiency}E)`;
  }

  const hardware = [
    brand,
    `${model ?? platform()} (${arch()})`,
    topology,
    `${Math.round(totalmem() / 1024 ** 3)} GB`,
  ];
  const software = [
    `node ${process.version}`,
    `v8 ${process.versions.v8}`,
    `${platform()} ${release()}`,
    `availableParallelism ${availableParallelism()}`,
  ];

  console.log("");
  console.log(INDENT + hardware.join("  ·  "));
  console.log(INDENT + software.join("  ·  "));
}

/**
 * Prints one titled table block: a heading over a full-width rule, the
 * table with a per-column underline and right-aligned value columns, and
 * an optional caption. Every benchmark report is one or more of these, so
 * the whole suite reads with a single consistent frame.
 */
export function printBlock(
  title: string,
  headers: string[],
  rows: string[][],
  caption?: string | string[],
): void {
  const widths = headers.map((header, column) =>
    Math.max(header.length, ...rows.map((row) => (row[column] ?? "").length)),
  );
  const width = widths.reduce((sum, w) => sum + w, 0) + GUTTER.length * (widths.length - 1);
  const cell = (cells: string[]): string =>
    INDENT +
    cells
      .map((value, column) =>
        column === 0
          ? (value ?? "").padEnd(widths[column] as number)
          : (value ?? "").padStart(widths[column] as number),
      )
      .join(GUTTER);

  const out: string[] = [""];
  out.push(INDENT + title);
  out.push(INDENT + "─".repeat(Math.max(width, title.length)));
  out.push(cell(headers));
  out.push(INDENT + widths.map((w) => "─".repeat(w)).join(GUTTER));
  for (const row of rows) {
    out.push(cell(row));
  }

  if (caption !== undefined) {
    out.push("");
    const lines = (Array.isArray(caption) ? caption : [caption]).flatMap((line) =>
      line.split("\n"),
    );
    for (const line of lines) {
      out.push(INDENT + INDENT + line);
    }
  }

  console.log(out.join("\n"));
}

/**
 * One measurable workload: an operation that sends and fully processes
 * `batch` messages, resolving only once the last message was handled.
 */
export interface Scenario {
  name: string;
  /** Messages sent and processed by one operation. */
  batch: number;
  op: () => Promise<void>;
}

/** The measured outcome of one scenario. */
export interface ScenarioReport {
  name: string;
  batch: number;
  messagesPerSecond: number;
  meanMs: number;
  p99Ms: number;
  /** Relative standard deviation of the sampled operation times, as a
   * percentage of the mean; the run-to-run noise band. */
  spreadPercent: number;
  /** Estimated heap bytes allocated per message; null without --expose-gc. */
  bytesPerMessage: number | null;
  /** Garbage collections observed while sampling, and their total pause time. */
  gcCount: number;
  gcTimeMs: number;
}

/** Operations run before any measurement so the JIT settles. */
const WARMUP_OPS = 50;

/** Operations timed per scenario. */
const SAMPLE_OPS = 200;

/** Operations used for the allocation estimate. */
const ALLOC_OPS = 5;

const exposedGc = (globalThis as { gc?: () => void }).gc;

/**
 * Runs one scenario: warmup, an allocation estimate under forced garbage
 * collection, then timed samples with a GC observer attached.
 */
export async function runScenario(scenario: Scenario): Promise<ScenarioReport> {
  for (let i = 0; i < WARMUP_OPS; i++) {
    await scenario.op();
  }

  const bytesPerMessage = await estimateAllocation(scenario);

  const gcDurations: number[] = [];
  const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      gcDurations.push(entry.duration);
    }
  });
  observer.observe({ entryTypes: ["gc"] });

  const samples = new Array<number>(SAMPLE_OPS);

  for (let i = 0; i < SAMPLE_OPS; i++) {
    const start = process.hrtime.bigint();
    await scenario.op();
    samples[i] = Number(process.hrtime.bigint() - start) / 1e6;
  }

  // GC observations are delivered asynchronously; let them land.
  await new Promise((resolve) => setImmediate(resolve));
  observer.disconnect();

  samples.sort((a, b) => a - b);
  const meanMs = samples.reduce((sum, ms) => sum + ms, 0) / samples.length;
  const p99Index = Math.min(samples.length - 1, Math.ceil(samples.length * 0.99) - 1);
  const p99Ms = samples[p99Index] as number;

  const variance =
    samples.reduce((sum, ms) => sum + (ms - meanMs) * (ms - meanMs), 0) / samples.length;
  const spreadPercent = (Math.sqrt(variance) / meanMs) * 100;

  let gcTimeMs = 0;

  for (const duration of gcDurations) {
    gcTimeMs += duration;
  }

  return {
    name: scenario.name,
    batch: scenario.batch,
    messagesPerSecond: scenario.batch / (meanMs / 1000),
    meanMs,
    p99Ms,
    spreadPercent,
    bytesPerMessage,
    gcCount: gcDurations.length,
    gcTimeMs,
  };
}

/**
 * Estimates heap bytes allocated per message: collect fully, run one
 * operation, and read the heap growth before the next collection. A
 * single batch stays well under the nursery size, so the delta closely
 * tracks what the operation allocated. The median of a few rounds guards
 * against a stray mid-operation scavenge.
 */
async function estimateAllocation(scenario: Scenario): Promise<number | null> {
  if (exposedGc === undefined) {
    return null;
  }

  const estimates: number[] = [];

  for (let i = 0; i < ALLOC_OPS; i++) {
    exposedGc();
    const before = process.memoryUsage().heapUsed;
    await scenario.op();
    const after = process.memoryUsage().heapUsed;
    estimates.push((after - before) / scenario.batch);
  }

  estimates.sort((a, b) => a - b);
  return Math.max(0, estimates[Math.floor(estimates.length / 2)] as number);
}

function formatRate(perSecond: number): string {
  return `${(perSecond / 1_000_000).toFixed(2)}M/s`;
}

function formatBytes(bytes: number | null): string {
  return bytes === null ? "n/a" : `${Math.round(bytes)} B`;
}

/**
 * Prints one benchmark suite: the machine header (once per process), the
 * titled table of scenarios, and the methodology notes under it.
 */
export function printReport(reports: ScenarioReport[], heading: string): void {
  printMachine();

  const headers = ["scenario", "msgs/sec", "mean", "p99", "spread", "alloc", "gc (count / time)"];
  const rows = reports.map((report) => [
    report.name,
    formatRate(report.messagesPerSecond),
    `${report.meanMs.toFixed(2)} ms`,
    `${report.p99Ms.toFixed(2)} ms`,
    `±${report.spreadPercent.toFixed(1)}%`,
    formatBytes(report.bytesPerMessage),
    `${report.gcCount} / ${report.gcTimeMs.toFixed(1)} ms`,
  ]);

  const notes = [
    `each op sends and fully processes one batch (${SAMPLE_OPS} sampled, ${WARMUP_OPS} warmup)`,
    "spread is the relative standard deviation of op times; deltas within it are noise, not regressions",
    exposedGc === undefined
      ? "alloc needs --expose-gc; run through `pnpm bench`"
      : `alloc is the median heap growth of ${ALLOC_OPS} freshly collected ops, divided by batch size`,
  ].map((note) => `·  ${note}`);

  printBlock(heading, headers, rows, notes);
}
