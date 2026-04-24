import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createApp } from "../../src/app";
import { createComputeDemoSite, type PrebuiltStudioAssets } from "../../src/compute/demo_site";
import { loadConfig } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";

function createDemoTestApp(rootDir: string) {
  const base = loadConfig();
  const app = createApp(
    {
      ...base,
      dbPath: `${rootDir}/wal.sqlite`,
      port: 0,
      rootDir,
      searchWalOverlayQuietPeriodMs: 0,
      segmentCheckIntervalMs: 60_000,
      uploadIntervalMs: 60_000,
    },
    new MockR2Store(),
  );

  return app;
}

const fakeStudioAssets: PrebuiltStudioAssets = {
  appScript: "window.__studioLoaded = true;",
  appStyles: "body{background:#000;color:#fff;}",
  builtAssets: new Map([
    [
      "/asset.svg",
      {
        bytes: new TextEncoder().encode("<svg></svg>"),
        contentType: "image/svg+xml; charset=utf-8",
      },
    ],
  ]),
};

const roots: string[] = [];

afterEach(() => {
  while (roots.length > 0) {
    const root = roots.pop();
    if (root) rmSync(root, { force: true, recursive: true });
  }
});

async function readJson(response: Response): Promise<any> {
  const text = await response.text();
  return text === "" ? null : JSON.parse(text);
}

async function waitForJob(site: ReturnType<typeof createComputeDemoSite>, id: string): Promise<any> {
  const deadline = Date.now() + 10_000;

  while (Date.now() < deadline) {
    const response = await site.fetch(
      new Request(`http://local/api/generate/jobs/${encodeURIComponent(id)}`, {
        method: "GET",
      }),
    );
    expect(response.status).toBe(200);
    const payload = await readJson(response);
    if (payload.job.status === "succeeded" || payload.job.status === "failed") {
      return payload.job;
    }
    await Bun.sleep(10);
  }

  throw new Error("timed out waiting for generate job");
}

describe("compute demo site", () => {
  test("serves studio shell and proxies streams requests", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-compute-demo-studio-"));
    roots.push(root);
    const streamsApp = createDemoTestApp(root);
    const site = createComputeDemoSite({
      studioAssets: fakeStudioAssets,
      streamsApp,
    });

    try {
      const studioResponse = await site.fetch(new Request("http://local/studio"));
      expect(studioResponse.status).toBe(200);
      expect(await studioResponse.text()).toContain("/studio/app.js");

      const generateResponse = await site.fetch(new Request("http://local/generate"));
      expect(generateResponse.status).toBe(200);
      const generateHtml = await generateResponse.text();
      expect(generateHtml).toContain('value="demo-app"');
      expect(generateHtml).toContain("Insert 100k");

      const configResponse = await site.fetch(
        new Request("http://local/api/config"),
      );
      expect(configResponse.status).toBe(200);
      expect(await readJson(configResponse)).toEqual({
        ai: { enabled: false },
        bootId: expect.any(String),
        database: { enabled: false },
        streams: { url: "/studio/api/streams" },
      });

      const createResponse = await site.fetch(
        new Request("http://local/studio/api/streams/v1/stream/proxy-demo", {
          headers: {
            "content-type": "application/json",
          },
          method: "PUT",
        }),
      );
      expect([201, 204]).toContain(createResponse.status);

      const listResponse = await site.fetch(
        new Request("http://local/studio/api/streams/v1/streams"),
      );
      expect(listResponse.status).toBe(200);
      const body = await readJson(listResponse);
      expect(body.some((stream: { name: string }) => stream.name === "proxy-demo")).toBe(true);

      const assetResponse = await site.fetch(
        new Request("http://local/studio/asset.svg"),
      );
      expect(assetResponse.status).toBe(200);
      expect(assetResponse.headers.get("content-type")).toContain("image/svg+xml");
    } finally {
      site.close();
      streamsApp.close();
    }
  });

  test("uses the requested evlog stream and appends events through the generate API", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-compute-demo-generate-"));
    roots.push(root);
    const streamsApp = createDemoTestApp(root);
    const site = createComputeDemoSite({
      studioAssets: fakeStudioAssets,
      streamsApp,
    });

    try {
      const startResponse = await site.fetch(
        new Request("http://local/api/generate/jobs", {
          body: JSON.stringify({ count: 1_000, stream: "demo-app" }),
          headers: {
            "content-type": "application/json",
          },
          method: "POST",
        }),
      );
      expect(startResponse.status).toBe(202);
      const startPayload = await readJson(startResponse);
      expect(startPayload.job.total).toBe(1_000);
      expect(startPayload.job.stream).toBe("demo-app");

      const job = await waitForJob(site, startPayload.job.id);
      expect(job.status).toBe("succeeded");
      expect(job.inserted).toBe(1_000);
      expect(job.batchSize).toBeGreaterThan(0);

      const secondStartResponse = await site.fetch(
        new Request("http://local/api/generate/jobs", {
          body: JSON.stringify({ count: 1_000, stream: "demo-app" }),
          headers: {
            "content-type": "application/json",
          },
          method: "POST",
        }),
      );
      expect(secondStartResponse.status).toBe(202);
      const secondStartPayload = await readJson(secondStartResponse);
      expect(secondStartPayload.job.stream).toBe("demo-app");

      const secondJob = await waitForJob(site, secondStartPayload.job.id);
      expect(secondJob.status).toBe("succeeded");
      expect(secondJob.inserted).toBe(1_000);

      const detailsResponse = await streamsApp.fetch(
        new Request(
          `http://streams.internal/v1/stream/${encodeURIComponent(job.stream)}/_details`,
          { method: "GET" },
        ),
      );
      expect(detailsResponse.status).toBe(200);
      const detailsPayload = await readJson(detailsResponse);
      expect(detailsPayload.stream.name).toBe(job.stream);
      expect(detailsPayload.stream.profile).toBe("evlog");
      expect(detailsPayload.stream.next_offset).toBe("2000");
      expect(detailsPayload.profile.profile.kind).toBe("evlog");

      const readResponse = await streamsApp.fetch(
        new Request(
          `http://streams.internal/v1/stream/${encodeURIComponent(job.stream)}?format=json`,
          { method: "GET" },
        ),
      );
      expect(readResponse.status).toBe(200);
      const readPayload = await readJson(readResponse);
      expect(readPayload.length).toBeGreaterThan(0);
      const timestamps = readPayload
        .map((event: { timestamp?: unknown }) =>
          typeof event.timestamp === "string"
            ? Date.parse(event.timestamp)
            : Number.NaN,
        )
        .filter((timestamp: number) => Number.isFinite(timestamp));
      expect(timestamps.length).toBeGreaterThan(0);
      expect(Math.max(...timestamps)).toBeLessThanOrEqual(Date.now() + 1_000);
      expect(readPayload[0]).toEqual(
        expect.objectContaining({
          context: expect.objectContaining({
            fingerprint: expect.any(String),
          }),
          environment: expect.any(String),
          message: expect.any(String),
          path: expect.any(String),
          requestId: expect.any(String),
          service: expect.any(String),
          traceId: expect.any(String),
        }),
      );
    } finally {
      site.close();
      streamsApp.close();
    }
  });
});
