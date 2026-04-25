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

  test("resolves bundled segment workers from the compute entrypoint", () => {
    expect(
      resolveWorkerModuleUrl("file:///repo/bundle/compute/entry.js", "./segmenter_worker.ts", "../segment/segmenter_worker.js").href
    ).toBe(
      "file:///repo/bundle/segment/segmenter_worker.js"
    );
  });

  test("resolves bundled touch workers beside the local bundle", () => {
    expect(
      resolveWorkerModuleUrl("file:///repo/dist/local/index-hash.js", "./processor_worker.ts", "../touch/processor_worker.js").href
    ).toBe(
      "file:///repo/dist/touch/processor_worker.js"
    );
  });
});
