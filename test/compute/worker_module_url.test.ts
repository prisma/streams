import { describe, expect, test } from "bun:test";
import { resolveWorkerModuleUrl } from "../../src/compute/worker_module_url";

describe("resolveWorkerModuleUrl", () => {
  test("keeps source-worker paths on ts modules", () => {
    expect(resolveWorkerModuleUrl("file:///repo/src/segment/segmenter_workers.ts", "./segmenter_worker.ts").href).toBe(
      "file:///repo/src/segment/segmenter_worker.ts"
    );
  });

  test("switches to built worker paths on js modules", () => {
    expect(
      resolveWorkerModuleUrl("file:///repo/bundle/compute/entry.js", "./segmenter_worker.ts", "../segment/segmenter_worker.js")
        .href
    ).toBe("file:///repo/bundle/segment/segmenter_worker.js");
  });

  test("keeps the default js rewrite when the built path matches the source layout", () => {
    expect(resolveWorkerModuleUrl("file:///repo/src/touch/worker_pool.js", "./processor_worker.ts").href).toBe(
      "file:///repo/src/touch/processor_worker.js"
    );
  });
});
