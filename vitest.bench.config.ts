import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["benchmark/**/*.bench.ts"],
    pool: "forks",
    // Benchmark files must not compete for the machine: run them one at
    // a time so the numbers measure the runtime, not sibling contention.
    fileParallelism: false,
    // The harness estimates per-message allocation with forced
    // collections, which needs the gc() global.
    execArgv: ["--expose-gc"],
    testTimeout: 300_000,
    // The report is the product; let it reach the terminal untouched.
    disableConsoleIntercept: true,
  },
});
