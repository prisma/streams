import { describe, expect, test } from "bun:test";
import { ensureComputeArgv } from "../../src/compute/entry";

describe("compute entrypoint", () => {
  test("adds r2 object-store args when missing", () => {
    expect(ensureComputeArgv(["bun", "src/compute/entry.ts", "--stats"])).toEqual([
      "bun",
      "src/compute/entry.ts",
      "--stats",
      "--object-store",
      "r2",
    ]);
  });

  test("preserves an explicit object-store choice", () => {
    expect(ensureComputeArgv(["bun", "src/compute/entry.ts", "--object-store", "local"])).toEqual([
      "bun",
      "src/compute/entry.ts",
      "--object-store",
      "local",
    ]);
  });

  test("adds auto-tune when DS_MEMORY_LIMIT_MB is set", () => {
    expect(ensureComputeArgv(["bun", "src/compute/entry.ts"], { DS_MEMORY_LIMIT_MB: "1024" })).toEqual([
      "bun",
      "src/compute/entry.ts",
      "--object-store",
      "r2",
      "--auto-tune",
    ]);
  });

  test("preserves an explicit auto-tune choice", () => {
    expect(ensureComputeArgv(["bun", "src/compute/entry.ts", "--auto-tune=2048"], { DS_MEMORY_LIMIT_MB: "1024" })).toEqual([
      "bun",
      "src/compute/entry.ts",
      "--auto-tune=2048",
      "--object-store",
      "r2",
    ]);
  });
});
