import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Drops the runtime's default log lines so a suite that builds an actor
    // system without configuring a logger does not flood the test output.
    setupFiles: ["./test/setup.ts"],
    // A leaked handle surfaces as a named report instead of a silent
    // hang in CI.
    reporters: ["default", "hanging-process"],
    coverage: {
      provider: "v8",
      // text for the terminal, lcov for coverage services such as Codecov.
      reporter: ["text", "lcov"],
      include: ["src/**"],
      // The worker entry executes inside worker isolates only, beyond
      // the main isolate's instrumentation; it is pure wiring and the
      // logic it wires lives in covered modules.
      exclude: ["src/worker.entry.ts"],
      thresholds: {
        statements: 100,
        branches: 100,
        functions: 100,
        lines: 100,
      },
    },
  },
});
