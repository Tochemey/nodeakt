import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      include: ["src/**"],
      // The worker entry executes inside worker isolates only, beyond
      // the main isolate's instrumentation; it is pure wiring and the
      // logic it wires lives in covered modules.
      exclude: ["src/runtime/worker.entry.ts"],
      thresholds: {
        statements: 100,
        branches: 100,
        functions: 100,
        lines: 100,
      },
    },
  },
});
