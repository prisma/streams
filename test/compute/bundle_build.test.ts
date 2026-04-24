import { describe, expect, test } from "bun:test";
import { fileURLToPath } from "node:url";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { buildComputeBundle } from "../../scripts/compute/build-bundle.mjs";

describe("compute bundle build", () => {
  test("emits the server entrypoint and worker entrypoints together", async () => {
    const tmpRoot = await mkdtemp(path.join(os.tmpdir(), "streams-compute-bundle-"));
    const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
    try {
      const outDir = path.join(tmpRoot, "bundle");
      const { bundleDir, entrypoint } = await buildComputeBundle({ cwd: repoRoot, outDir });
      expect(entrypoint).toBe("compute/entry.js");

      const files = await readdir(bundleDir);
      const computeFiles = await readdir(path.join(bundleDir, "compute"));
      const segmentFiles = await readdir(path.join(bundleDir, "segment"));
      const touchFiles = await readdir(path.join(bundleDir, "touch"));
      expect(files).toContain("compute");
      expect(computeFiles).toContain("entry.js");
      expect(segmentFiles).toContain("segmenter_worker.js");
      expect(touchFiles).toContain("processor_worker.js");

      const runtimeEntrypoint = await readFile(path.join(bundleDir, "compute", "entry.js"), "utf8");
      expect(runtimeEntrypoint).toContain(
        'resolveWorkerModuleUrl(import.meta.url, "./segmenter_worker.ts", "./segment/segmenter_worker.js")'
      );
      expect(runtimeEntrypoint).toContain(
        'resolveWorkerModuleUrl(import.meta.url, "./processor_worker.ts", "./touch/processor_worker.js")'
      );
    } finally {
      await rm(tmpRoot, { recursive: true, force: true });
    }
  });
});
