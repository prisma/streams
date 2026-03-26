import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { createApp } from "../src/app";
import { bootstrapFromR2 } from "../src/bootstrap";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

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

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

describe("bootstrap from R2", () => {
  test(
    "rebuilds sqlite state from manifest, segments, schema, and index runs",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-bootstrap-src-"));
      const root2 = mkdtempSync(join(tmpdir(), "ds-bootstrap-dst-"));
      const stream = "boot";
      const payload = new TextEncoder().encode(JSON.stringify({ x: 1 }));

      const cfg = makeConfig(root, {
        segmentMaxBytes: payload.byteLength * 2,
        segmentCheckIntervalMs: 25,
        uploadIntervalMs: 25,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 25,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });
      const store = new MockR2Store();
      const app = createApp(cfg, store);
      try {
        const createRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([201, 204]).toContain(createRes.status);

        const schemaRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              schema: {
                type: "object",
                properties: {
                  eventTime: { type: "string" },
                  x: { type: "number" },
                  service: { type: "string" },
                },
                required: ["x", "service"],
              },
              search: {
                primaryTimestampField: "eventTime",
                fields: {
                  eventTime: {
                    kind: "date",
                    bindings: [{ version: 1, jsonPointer: "/eventTime" }],
                    exact: true,
                    column: true,
                    exists: true,
                    sortable: true,
                  },
                  service: {
                    kind: "keyword",
                    bindings: [{ version: 1, jsonPointer: "/service" }],
                    normalizer: "lowercase_v1",
                    exact: true,
                    prefix: true,
                    exists: true,
                  },
                  duration: {
                    kind: "float",
                    bindings: [{ version: 1, jsonPointer: "/duration" }],
                    exact: true,
                    column: true,
                    exists: true,
                    sortable: true,
                    aggregatable: true,
                  },
                },
                rollups: {
                  requests: {
                    dimensions: ["service"],
                    intervals: ["1m"],
                    measures: {
                      requests: { kind: "count" },
                      latency: { kind: "summary", field: "duration", histogram: "log2_v1" },
                    },
                  },
                },
              },
            }),
          })
        );
        expect(schemaRes.status).toBe(200);

        const profileRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_profile`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              apiVersion: "durable.streams/profile/v1",
              profile: {
                kind: "state-protocol",
                touch: { enabled: true },
              },
            }),
          })
        );
        expect(profileRes.status).toBe(200);

        for (let i = 0; i < 4; i++) {
          const r = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:0${i}:00.000Z`,
                x: i,
                service: i % 2 === 0 ? "api" : "worker",
                duration: 100 + i * 10,
              }),
            })
          );
          expect(r.status).toBe(204);
        }

        const deadline = Date.now() + 10_000;
        while (Date.now() < deadline) {
          const segs = app.deps.db.listSegmentsForStream(stream);
          const pending = app.deps.db.countPendingSegments();
          const srow = app.deps.db.getStream(stream);
          const uploadedOk = srow ? srow.uploaded_through >= srow.sealed_through : false;
          const secondaryStates = app.deps.db.listSecondaryIndexStates(stream);
          const secondaryRuns = secondaryStates.flatMap((state) => app.deps.db.listSecondaryIndexRuns(stream, state.index_name));
          const searchFamilies = app.deps.db.listSearchFamilyStates(stream);
          const colSegments = app.deps.db.listSearchFamilySegments(stream, "col");
          const ftsSegments = app.deps.db.listSearchFamilySegments(stream, "fts");
          const aggSegments = app.deps.db.listSearchFamilySegments(stream, "agg");
          if (
            segs.length >= 2 &&
            pending === 0 &&
            uploadedOk &&
            secondaryStates.length > 0 &&
            secondaryRuns.length > 0 &&
            searchFamilies.length >= 3 &&
            colSegments.length > 0 &&
            ftsSegments.length > 0 &&
            aggSegments.length > 0
          ) {
            break;
          }
          await sleep(50);
        }
      } finally {
        app.close();
      }

      const cfg2 = makeConfig(root2, {
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });
      await bootstrapFromR2(cfg2, store, { clearLocal: true });
      const app2 = createApp(cfg2, store);
      try {
        const row = app2.deps.db.getStream(stream);
        expect(row).not.toBeNull();
        expect(row!.content_type).toBe("application/json");
        expect(row!.profile).toBe("state-protocol");

        const profileRow = app2.deps.db.getStreamProfile(stream);
        expect(profileRow).not.toBeNull();
        const profileJson = JSON.parse(profileRow!.profile_json);
        expect(profileJson.kind).toBe("state-protocol");
        expect(profileJson.touch?.enabled).toBe(true);

        const touchStateRow = app2.deps.db.getStreamTouchState(stream);
        expect(touchStateRow).not.toBeNull();

        const schemaRow = app2.deps.db.getSchemaRegistry(stream);
        expect(schemaRow).not.toBeNull();
        expect(JSON.parse(schemaRow!.registry_json).search).toEqual({
          primaryTimestampField: "eventTime",
          fields: {
            eventTime: {
              kind: "date",
              bindings: [{ version: 1, jsonPointer: "/eventTime" }],
              exact: true,
              column: true,
              exists: true,
              sortable: true,
            },
            service: {
              kind: "keyword",
              bindings: [{ version: 1, jsonPointer: "/service" }],
              normalizer: "lowercase_v1",
              exact: true,
              prefix: true,
              exists: true,
            },
            duration: {
              kind: "float",
              bindings: [{ version: 1, jsonPointer: "/duration" }],
              exact: true,
              column: true,
              exists: true,
              sortable: true,
              aggregatable: true,
            },
          },
          rollups: {
            requests: {
              timestampField: "eventTime",
              dimensions: ["service"],
              intervals: ["1m"],
              measures: {
                requests: { kind: "count" },
                latency: { kind: "summary", field: "duration", histogram: "log2_v1" },
              },
            },
          },
        });

        const segs = app2.deps.db.listSegmentsForStream(stream);
        expect(segs.length).toBeGreaterThan(0);
        expect(segs[0].r2_etag).not.toBeNull();
        const meta = app2.deps.db.getSegmentMeta(stream);
        expect(meta).not.toBeNull();
        expect(meta!.segment_count).toBe(segs.length);

        const srow = app2.deps.db.getStream(stream);
        expect(srow).not.toBeNull();
        expect(srow!.uploaded_segment_count).toBe(segs.length);

        const secondaryStates = app2.deps.db.listSecondaryIndexStates(stream);
        expect(secondaryStates.length).toBe(3);
        expect(secondaryStates.map((state) => state.index_name).sort()).toEqual(["duration", "eventTime", "service"]);
        const secondaryRuns = app2.deps.db.listSecondaryIndexRuns(stream, "service");
        expect(secondaryRuns.length).toBeGreaterThan(0);

        const searchFamilies = app2.deps.db.listSearchFamilyStates(stream);
        expect(searchFamilies.map((family) => family.family).sort()).toEqual(["agg", "col", "fts"]);
        expect(app2.deps.db.listSearchFamilySegments(stream, "agg").length).toBeGreaterThan(0);
        expect(app2.deps.db.listSearchFamilySegments(stream, "col").length).toBeGreaterThan(0);
        expect(app2.deps.db.listSearchFamilySegments(stream, "fts").length).toBeGreaterThan(0);

        const readRes = await app2.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}?offset=-1`, { method: "GET" })
        );
        expect(readRes.status).toBe(200);

        const touchMetaRes = await app2.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" })
        );
        expect(touchMetaRes.status).toBe(200);

        const searchRes = await app2.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ q: "service:api" }),
          })
        );
        expect(searchRes.status).toBe(200);
        const searchBody = await searchRes.json();
        expect(searchBody.total.value).toBe(2);

        const aggregateRes = await app2.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_aggregate`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              rollup: "requests",
              from: "2026-03-25T10:00:00.000Z",
              to: "2026-03-25T10:04:00.000Z",
              interval: "1m",
              q: "service:api",
              group_by: ["service"],
            }),
          })
        );
        expect(aggregateRes.status).toBe(200);
        const aggregateBody = await aggregateRes.json();
        expect(aggregateBody.coverage.index_families_used).toEqual(["agg"]);
        expect(aggregateBody.buckets).toHaveLength(2);
      } finally {
        app2.close();
        rmSync(root, { recursive: true, force: true });
        rmSync(root2, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "does not resurrect a deleted stream after bootstrap-from-R2",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-bootstrap-delete-src-"));
      const root2 = mkdtempSync(join(tmpdir(), "ds-bootstrap-delete-dst-"));
      const stream = "deleted-before-restart";

      const cfg = makeConfig(root, {
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });
      const store = new MockR2Store();
      const app = createApp(cfg, store);
      try {
        const createRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([201, 204]).toContain(createRes.status);

        // Ensure a stream manifest exists in object storage before deletion.
        await app.deps.uploader.publishManifest(stream);

        const delRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "DELETE",
          })
        );
        expect(delRes.status).toBe(204);
      } finally {
        app.close();
      }

      const cfg2 = makeConfig(root2, {
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });
      await bootstrapFromR2(cfg2, store, { clearLocal: true });
      const app2 = createApp(cfg2, store);
      try {
        const headRes = await app2.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "HEAD",
          })
        );
        expect(headRes.status).toBe(404);

        const listRes = await app2.fetch(new Request("http://local/v1/streams", { method: "GET" }));
        expect(listRes.status).toBe(200);
        const list = (await listRes.json()) as Array<{ name: string }>;
        expect(list.find((x) => x.name === stream)).toBeUndefined();
      } finally {
        app2.close();
        rmSync(root, { recursive: true, force: true });
        rmSync(root2, { recursive: true, force: true });
      }
    },
    30_000
  );
});
