import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { hashSecondaryIndexField } from "../src/index/secondary_schema";
import { MockR2Store } from "../src/objectstore/mock_r2";

const STREAM = "exact-backfill";

function makeConfig(rootDir: string, overrides: Partial<Config>): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    ...overrides,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const SCHEMA_V1 = {
  schema: {
    type: "object",
    additionalProperties: true,
  },
  search: {
    primaryTimestampField: "eventTime",
    fields: {
      eventTime: {
        kind: "date",
        bindings: [{ version: 1, jsonPointer: "/eventTime" }],
        column: true,
        exists: true,
        sortable: true,
      },
      tag: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/tagA" }],
        normalizer: "lowercase_v1",
        exact: true,
        exists: true,
      },
    },
  },
};

const SEARCH_V2 = {
  primaryTimestampField: "eventTime",
  fields: {
    eventTime: {
      kind: "date",
      bindings: [{ version: 1, jsonPointer: "/eventTime" }],
      column: true,
      exists: true,
      sortable: true,
    },
    tag: {
      kind: "keyword",
      bindings: [{ version: 1, jsonPointer: "/tagB" }],
      normalizer: "lowercase_v1",
      exact: true,
      exists: true,
    },
  },
};

const EXACT_HASH_V1 = hashSecondaryIndexField({ name: "tag", config: SCHEMA_V1.search.fields.tag });
const EXACT_HASH_V2 = hashSecondaryIndexField({ name: "tag", config: SEARCH_V2.fields.tag });

async function waitForExactIndex(
  app: ReturnType<typeof createApp>,
  expectedHash: string,
  opts: { manualKick?: boolean } = {},
  timeoutMs = 10_000
): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const row = app.deps.db.getStream(STREAM);
    const uploadedSegments = app.deps.db.countUploadedSegments(STREAM);
    const state = app.deps.db.getSecondaryIndexState(STREAM, "tag");
    const fullyUploaded = !!row && row.uploaded_through >= row.sealed_through && uploadedSegments > 0;
    if (fullyUploaded && state?.config_hash === expectedHash && state.indexed_through >= uploadedSegments) {
      return uploadedSegments;
    }
    if (opts.manualKick !== false) app.deps.indexer?.enqueue(STREAM);
    await sleep(50);
  }
  throw new Error(`timeout waiting for exact index hash ${expectedHash}`);
}

describe("exact secondary index backfill", () => {
  test(
    "falls back immediately and rebuilds exact indexes when search bindings change on an existing stream",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-exact-backfill-"));
      const store = new MockR2Store();
      const buildCfg = makeConfig(root, {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });

      let app = createApp(buildCfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201, 204]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(SCHEMA_V1),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 6; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-30T10:0${i}:00.000Z`,
                tagA: i % 2 === 0 ? "legacy-a" : "legacy-b",
                tagB: i % 2 === 0 ? "current-a" : "current-b",
                pad: "x".repeat(256),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForExactIndex(app, EXACT_HASH_V1);
      } finally {
        app.close();
      }

      const pausedCfg = makeConfig(root, {
        ...buildCfg,
        indexCheckIntervalMs: 60_000,
      });
      app = createApp(pausedCfg, store);
      try {
        const updateRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ search: SEARCH_V2 }),
          })
        );
        expect(updateRes.status).toBe(200);

        const detailsRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_details`));
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.index_status.exact_indexes).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              name: "tag",
              fully_indexed_uploaded_segments: false,
              stale_configuration: true,
            }),
          ])
        );

        const filterRes = await app.fetch(
          new Request(
            `http://local/v1/stream/${encodeURIComponent(STREAM)}?offset=-1&format=json&filter=${encodeURIComponent('tag:"current-b"')}`
          )
        );
        expect(filterRes.status).toBe(200);
        const filterBody = await filterRes.json();
        expect(filterBody).toHaveLength(3);
        expect(filterBody.every((entry: any) => entry.tagB === "current-b")).toBe(true);
      } finally {
        app.close();
      }

      app = createApp(buildCfg, store);
      try {
        await waitForExactIndex(app, EXACT_HASH_V2, { manualKick: false });
        const detailsRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_details`));
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.index_status.exact_indexes).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              name: "tag",
              fully_indexed_uploaded_segments: true,
              stale_configuration: false,
            }),
          ])
        );

        const filterRes = await app.fetch(
          new Request(
            `http://local/v1/stream/${encodeURIComponent(STREAM)}?offset=-1&format=json&filter=${encodeURIComponent('tag:"current-b"')}`
          )
        );
        expect(filterRes.status).toBe(200);
        const filterBody = await filterRes.json();
        expect(filterBody).toHaveLength(3);
        expect(filterBody.every((entry: any) => entry.tagB === "current-b")).toBe(true);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );
});
