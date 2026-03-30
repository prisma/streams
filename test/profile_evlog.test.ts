import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bootstrapFromR2 } from "../src/bootstrap";
import { createProfileTestApp, fetchJsonApp, makeProfileTestConfig } from "./profile_test_utils";

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForEvlogIndexing(
  app: ReturnType<typeof createProfileTestApp>["app"],
  stream: string,
  timeoutMs: number
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const row = app.deps.db.getStream(stream);
    const uploadedSegments = app.deps.db.countUploadedSegments(stream);
    const fullySealed =
      !!row &&
      row.next_offset > 0n &&
      row.sealed_through === row.next_offset - 1n &&
      row.pending_bytes === 0n &&
      row.pending_rows === 0n;
    const exactStates = app.deps.db.listSecondaryIndexStates(stream);
    const exactReady = exactStates.length > 0 && exactStates.every((state) => state.indexed_through >= uploadedSegments);
    const companionPlan = app.deps.db.getSearchCompanionPlan(stream);
    const companionSegments = app.deps.db.listSearchSegmentCompanions(stream);
    const searchReady =
      !!companionPlan &&
      companionSegments.length >= uploadedSegments;
    if (fullySealed && uploadedSegments > 0 && exactReady && searchReady) return;
    app.deps.indexer?.enqueue(stream);
    await sleep(50);
  }
  throw new Error("timeout waiting for evlog segments and indexes");
}

describe("evlog profile", () => {
  test("installs on json streams and is visible in profile resource and listing", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-evlog-install-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/evlog-install", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const res = await fetchJsonApp(app, "http://local/v1/stream/evlog-install/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "evlog",
            redactKeys: ["sessionToken"],
          },
        }),
      });
      expect(res.status).toBe(200);
      expect(res.body).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: {
          kind: "evlog",
          redactKeys: ["sessiontoken"],
        },
      });

      const getRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-install/_profile", { method: "GET" });
      expect(getRes.status).toBe(200);
      expect(getRes.body?.profile?.kind).toBe("evlog");
      expect(getRes.body?.profile?.redactKeys).toEqual(["sessiontoken"]);

      const listRes = await fetchJsonApp(app, "http://local/v1/streams", { method: "GET" });
      expect(listRes.status).toBe(200);
      expect(listRes.body.find((row: any) => row.name === "evlog-install")?.profile).toBe("evlog");

      expect(app.deps.db.getStream("evlog-install")?.profile).toBe("evlog");
      expect(app.deps.db.getStreamProfile("evlog-install")).not.toBeNull();

      const schemaRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-install/_schema", { method: "GET" });
      expect(schemaRes.status).toBe(200);
      expect(schemaRes.body?.currentVersion).toBe(1);
      expect(schemaRes.body?.boundaries).toEqual([{ offset: 0, version: 1 }]);
      expect(schemaRes.body?.search?.profile).toBe("evlog");
      expect(schemaRes.body?.search?.primaryTimestampField).toBe("timestamp");
      expect(schemaRes.body?.search?.fields?.service?.kind).toBe("keyword");
      expect(schemaRes.body?.search?.fields?.status?.kind).toBe("integer");
      expect(schemaRes.body?.search?.fields?.message?.kind).toBe("text");
      expect(schemaRes.body?.schemas?.["1"]).toBeDefined();
      expect(app.deps.db.getSchemaRegistry("evlog-install")).not.toBeNull();
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("rejects non-json streams, invalid config, and late install after data exists", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-evlog-validate-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/evlog-non-json", {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        })
      );

      const nonJsonRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-non-json/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "evlog" },
        }),
      });
      expect(nonJsonRes.status).toBe(400);
      expect(nonJsonRes.body?.error?.message).toContain("application/json");

      await app.fetch(
        new Request("http://local/v1/stream/evlog-invalid", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const invalidConfigRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-invalid/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "evlog",
            extra: true,
          },
        }),
      });
      expect(invalidConfigRes.status).toBe(400);
      expect(invalidConfigRes.body?.error?.message).toContain("profile.extra");

      await app.fetch(
        new Request("http://local/v1/stream/evlog-late", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      await app.fetch(
        new Request("http://local/v1/stream/evlog-late", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ ok: true }),
        })
      );

      const lateInstallRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-late/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "evlog" },
        }),
      });
      expect(lateInstallRes.status).toBe(400);
      expect(lateInstallRes.body?.error?.message).toContain("before appending data");
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("normalizes events, redacts sensitive context, and supports requestId lookup", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-evlog-write-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/evlog-write", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      await fetchJsonApp(app, "http://local/v1/stream/evlog-write/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "evlog",
            redactKeys: ["sessionToken"],
          },
        }),
      });

      const appendRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-write", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            timestamp: "2026-03-25T10:00:00.000Z",
            status: 402,
            requestId: "req_123",
            traceContext: { traceId: "trace_123", spanId: "span_123" },
            method: "POST",
            path: "/api/checkout",
            service: "checkout",
            environment: "prod",
            message: "Payment failed",
            why: "Card declined by issuer",
            fix: "Retry with another card",
            password: "hunter2",
            sessionToken: "tok_secret",
            user: { id: 123, plan: "pro" },
            sampling: { kept: true },
          }),
        })
      );
      expect([200, 204]).toContain(appendRes.status);

      const readRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-write?format=json", { method: "GET" });
      expect(readRes.status).toBe(200);
      expect(readRes.body).toHaveLength(1);
      expect(readRes.body[0]).toEqual({
        timestamp: "2026-03-25T10:00:00.000Z",
        level: "error",
        service: "checkout",
        environment: "prod",
        version: null,
        region: null,
        requestId: "req_123",
        traceId: "trace_123",
        spanId: "span_123",
        method: "POST",
        path: "/api/checkout",
        status: 402,
        duration: null,
        message: "Payment failed",
        why: "Card declined by issuer",
        fix: "Retry with another card",
        link: null,
        sampling: { kept: true },
        redaction: { keys: ["password", "sessionToken"] },
        context: {
          traceContext: { traceId: "trace_123", spanId: "span_123" },
          password: "[REDACTED]",
          sessionToken: "[REDACTED]",
          user: { id: 123, plan: "pro" },
        },
      });

      const byRequestIdRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-write?format=json&key=req_123", { method: "GET" });
      expect(byRequestIdRes.status).toBe(200);
      expect(byRequestIdRes.body).toHaveLength(1);
      expect(byRequestIdRes.body[0]?.requestId).toBe("req_123");

      const filteredRes = await fetchJsonApp(
        app,
        "http://local/v1/stream/evlog-write?format=json&filter=service:checkout%20status:>=400%20requestId:req_123",
        { method: "GET" }
      );
      expect(filteredRes.status).toBe(200);
      expect(filteredRes.body).toHaveLength(1);
      expect(filteredRes.body[0]?.requestId).toBe("req_123");

      const searchRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-write/_search", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          q: 'service:checkout status:>=400 req:req_123 why:"card declined"',
          sort: ["timestamp:desc", "offset:desc"],
        }),
      });
      expect(searchRes.status).toBe(200);
      expect(searchRes.body?.total).toEqual({ value: 1, relation: "eq" });
      expect(searchRes.body?.coverage?.index_families_used).toEqual([]);
      expect(searchRes.body?.hits).toHaveLength(1);
      expect(searchRes.body?.hits?.[0]?.fields).toMatchObject({
        service: "checkout",
        requestId: "req_123",
        status: 402,
        message: "Payment failed",
        why: "Card declined by issuer",
      });

      const invalidRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-write", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(["not-an-object"]),
        })
      );
      expect(invalidRes.status).toBe(400);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("falls back to traceId routing when requestId is absent", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-evlog-trace-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/evlog-trace", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      await fetchJsonApp(app, "http://local/v1/stream/evlog-trace/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "evlog" },
        }),
      });

      const appendRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-trace", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            traceContext: { traceId: "trace_only_1", spanId: "span_only_1" },
            path: "/api/background",
            status: 200,
          }),
        })
      );
      expect([200, 204]).toContain(appendRes.status);

      const byTraceIdRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-trace?format=json&key=trace_only_1", { method: "GET" });
      expect(byTraceIdRes.status).toBe(200);
      expect(byTraceIdRes.body).toHaveLength(1);
      expect(byTraceIdRes.body[0]?.requestId).toBeNull();
      expect(byTraceIdRes.body[0]?.traceId).toBe("trace_only_1");
      expect(byTraceIdRes.body[0]?.spanId).toBe("span_only_1");
      expect(byTraceIdRes.body[0]?.level).toBe("info");
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test(
    "details and index status expose evlog schema, profile, and async index progress",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-profile-evlog-details-"));
      const { app } = createProfileTestApp(root, {
        segmentMaxBytes: 256,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        indexCheckIntervalMs: 10,
        indexL0SpanSegments: 1,
      });
      try {
        await app.fetch(
          new Request("http://local/v1/stream/evlog-details", {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );

        const profileRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-details/_profile", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            apiVersion: "durable.streams/profile/v1",
            profile: { kind: "evlog" },
          }),
        });
        expect(profileRes.status).toBe(200);

        const appendRes = await app.fetch(
          new Request("http://local/v1/stream/evlog-details", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              timestamp: "2026-03-26T10:00:00.000Z",
              requestId: "req_details",
              traceId: "trace_details",
              spanId: "span_details",
              service: "checkout",
              environment: "prod",
              method: "POST",
              path: "/api/checkout",
              status: 402,
              duration: 123.5,
              message: "Payment failed during authorization",
              why: "Card declined by issuer",
              fix: "Retry with another card",
              context: {
                error: { message: "issuer-declined" },
                pad: "x".repeat(1024),
              },
            }),
          })
        );
        expect([200, 204]).toContain(appendRes.status);

        await waitForEvlogIndexing(app, "evlog-details", 10_000);

        const indexStatusRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-details/_index_status", { method: "GET" });
        expect(indexStatusRes.status).toBe(200);
        expect(indexStatusRes.body).toMatchObject({
          stream: "evlog-details",
          profile: "evlog",
          routing_key_index: {
            configured: false,
          },
        });
        expect(indexStatusRes.body?.segments?.total_count).toBeGreaterThan(0);
        expect(indexStatusRes.body?.segments?.uploaded_count).toBeGreaterThan(0);
        expect(indexStatusRes.body?.exact_indexes.map((entry: any) => entry.name)).toEqual(
          expect.arrayContaining(["timestamp", "service", "status", "duration", "requestId"])
        );
        expect(indexStatusRes.body?.exact_indexes.every((entry: any) => entry.fully_indexed_uploaded_segments === true)).toBe(true);
        expect(indexStatusRes.body?.search_families).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              family: "col",
              fields: expect.arrayContaining(["timestamp", "status", "duration"]),
              fully_indexed_uploaded_segments: true,
            }),
            expect.objectContaining({
              family: "fts",
              fields: expect.arrayContaining(["service", "message", "why", "fix", "error.message"]),
              fully_indexed_uploaded_segments: true,
            }),
          ])
        );

        const detailsRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-details/_details", { method: "GET" });
        expect(detailsRes.status).toBe(200);
        expect(detailsRes.body?.stream).toMatchObject({
          name: "evlog-details",
          content_type: "application/json",
          profile: "evlog",
        });
        expect(detailsRes.body?.profile?.profile?.kind).toBe("evlog");
        expect(detailsRes.body?.schema?.search?.profile).toBe("evlog");
        expect(detailsRes.body?.schema?.search?.primaryTimestampField).toBe("timestamp");
        expect(detailsRes.body?.index_status).toMatchObject({
          stream: "evlog-details",
          profile: "evlog",
        });
        expect(detailsRes.body?.index_status?.segments?.uploaded_count).toBe(indexStatusRes.body?.segments?.uploaded_count);
        expect(detailsRes.body?.index_status?.exact_indexes).toEqual(indexStatusRes.body?.exact_indexes);
        expect(detailsRes.body?.index_status?.search_families).toEqual(indexStatusRes.body?.search_families);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test("evlog survives bootstrap with config", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-evlog-bootstrap-src-"));
    const root2 = mkdtempSync(join(tmpdir(), "ds-profile-evlog-bootstrap-dst-"));
    const { app, store } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/evlog-bootstrap", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const profileRes = await fetchJsonApp(app, "http://local/v1/stream/evlog-bootstrap/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "evlog",
            redactKeys: ["sessionToken"],
          },
        }),
      });
      expect(profileRes.status).toBe(200);
      const installedProfile = profileRes.body?.profile;

      await app.deps.uploader.publishManifest("evlog-bootstrap");
      const cfg2 = makeProfileTestConfig(root2, { segmentCacheMaxBytes: 0, segmentFooterCacheEntries: 0 });
      await bootstrapFromR2(cfg2, store, { clearLocal: true });
      const { app: app2 } = createProfileTestApp(root2);
      try {
        expect(app2.deps.db.getStream("evlog-bootstrap")?.profile).toBe("evlog");

        const profileRow = app2.deps.db.getStreamProfile("evlog-bootstrap");
        expect(profileRow).not.toBeNull();
        expect(JSON.parse(profileRow!.profile_json)).toEqual(installedProfile);

        const getRes = await fetchJsonApp(app2, "http://local/v1/stream/evlog-bootstrap/_profile", { method: "GET" });
        expect(getRes.status).toBe(200);
        expect(getRes.body).toEqual({
          apiVersion: "durable.streams/profile/v1",
          profile: installedProfile,
        });

        const schemaRes = await fetchJsonApp(app2, "http://local/v1/stream/evlog-bootstrap/_schema", { method: "GET" });
        expect(schemaRes.status).toBe(200);
        expect(schemaRes.body?.currentVersion).toBe(1);
        expect(schemaRes.body?.search?.profile).toBe("evlog");
        expect(schemaRes.body?.search?.primaryTimestampField).toBe("timestamp");

        const listRes = await fetchJsonApp(app2, "http://local/v1/streams", { method: "GET" });
        expect(listRes.status).toBe(200);
        expect(listRes.body.find((row: any) => row.name === "evlog-bootstrap")?.profile).toBe("evlog");
      } finally {
        app2.close();
      }
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
      rmSync(root2, { recursive: true, force: true });
    }
  });
});
