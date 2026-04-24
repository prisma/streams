import { describe, expect, test } from "bun:test";
import {
  createExternalStreamsTarget,
  resolveExternalStreamsServerUrl,
} from "../../src/compute/demo_entry";

describe("compute demo entrypoint", () => {
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
