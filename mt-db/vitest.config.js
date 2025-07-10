import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    sequence: {
      concurrent: false,
    },
    fileParallelism: false
  },
});
