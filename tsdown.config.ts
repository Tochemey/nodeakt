import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    "worker.entry": "src/runtime/worker.entry.ts",
  },
  format: ["esm"],
  platform: "node",
  dts: true,
  clean: true,
});
