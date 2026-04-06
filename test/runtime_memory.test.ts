import { describe, expect, test } from "bun:test";
import {
  buildProcessMemoryBreakdown,
  parseLinuxStatusRssBreakdown,
} from "../src/runtime_memory";

describe("runtime memory helpers", () => {
  test("parses linux rss breakdown from /proc/self/status content", () => {
    const parsed = parseLinuxStatusRssBreakdown(
      [
        "Name:\tbun",
        "RssAnon:\t   681020 kB",
        "RssFile:\t    45751 kB",
        "RssShmem:\t        0 kB",
      ].join("\n")
    );
    expect(parsed).toEqual({
      rss_anon_bytes: 681020 * 1024,
      rss_file_bytes: 45751 * 1024,
      rss_shmem_bytes: 0,
    });
  });

  test("computes unattributed rss conservatively from process and sqlite state", () => {
    const breakdown = buildProcessMemoryBreakdown({
      process: {
        rss_bytes: 700,
        heap_total_bytes: 250,
        heap_used_bytes: 200,
        external_bytes: 100,
        array_buffers_bytes: 40,
      },
      mappedFileBytes: 150,
      sqliteRuntimeBytes: 50,
    });
    expect(breakdown.js_managed_bytes).toBe(300);
    expect(breakdown.js_external_non_array_buffers_bytes).toBe(60);
    expect(breakdown.unattributed_rss_bytes).toBe(200);
  });
});
