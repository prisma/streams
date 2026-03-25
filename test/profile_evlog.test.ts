import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bootstrapFromR2 } from "../src/bootstrap";
import { createProfileTestApp, fetchJsonApp, makeProfileTestConfig } from "./profile_test_utils";

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
