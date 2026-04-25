import { describe, expect, test } from "bun:test";
import {
  applyColocatedComputeDemoArgv,
  createExternalStreamsTarget,
  resolveExternalStreamsServerUrl,
} from "../../src/compute/demo_entry";

describe("compute demo entrypoint", () => {
  test("applies colocated Compute auto-tune before config loading", () => {
    const env: NodeJS.ProcessEnv = { DS_MEMORY_LIMIT_MB: "1024" };
    const logs: string[] = [];

    const argv = applyColocatedComputeDemoArgv(["bun", "src/compute/demo_entry.ts"], env, {
      log: (message) => logs.push(message),
    });
    expect(argv).toEqual([
      "bun",
      "src/compute/demo_entry.ts",
      "--object-store",
      "r2",
      "--auto-tune",
    ]);
    expect(env.DS_AUTO_TUNE_PRESET_MB).toBe("1024");
    expect(env.DS_SEGMENT_MAX_BYTES).toBe(String(8 * 1024 * 1024));
    expect(env.DS_SEGMENT_TARGET_ROWS).toBe("50000");
    expect(env.DS_SEGMENT_CACHE_MAX_BYTES).toBe("0");
    expect(env.DS_SEGMENTER_WORKERS).toBe("0");
    expect(env.DS_UPLOAD_CONCURRENCY).toBe("1");
    expect(logs.some((line) => line.includes("Auto-tuning for memory preset"))).toBe(true);
  });

  test("resolves and normalizes external Streams server URLs", () => {
    expect(
      resolveExternalStreamsServerUrl({
        COMPUTE_DEMO_STREAMS_SERVER_URL: "cmoa45nql0u6bzycn7dwdpxe0.cdg.prisma.build/",
      }),
    ).toBe("https://cmoa45nql0u6bzycn7dwdpxe0.cdg.prisma.build");
  });

  test("external target rewrites incoming requests to the configured Streams server", async () => {
    const originalFetch = globalThis.fetch;
    const calls: Array<{ body: string; method: string; url: string }> = [];

    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({
        body:
          init?.body instanceof ArrayBuffer
            ? new TextDecoder().decode(init.body)
            : "",
        method: init?.method ?? "GET",
        url: String(input),
      });
      return Response.json({ ok: true });
    }) as typeof fetch;

    try {
      const target = createExternalStreamsTarget(
        "https://cmoa45nql0u6bzycn7dwdpxe0.cdg.prisma.build/",
      );
      const response = await target.fetch(
        new Request("http://demo.local/v1/stream/demo?format=json", {
          body: JSON.stringify([{ ok: true }]),
          headers: {
            "content-type": "application/json",
          },
          method: "POST",
        }),
      );

      expect(response.status).toBe(200);
      expect(calls).toEqual([
        {
          body: JSON.stringify([{ ok: true }]),
          method: "POST",
          url: "https://cmoa45nql0u6bzycn7dwdpxe0.cdg.prisma.build/v1/stream/demo?format=json",
        },
      ]);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
