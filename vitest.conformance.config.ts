import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["conformance.vitest.ts"],
    exclude: ["node_modules/**", "test/**"],
    testTimeout: 30_000,
    hookTimeout: 60_000,
  },
});
