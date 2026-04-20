import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bootstrapFromR2 } from "../src/bootstrap";
import { tableKeyFor } from "../src/touch/live_keys";
import { createProfileTestApp, fetchJsonApp, makeProfileTestConfig } from "./profile_test_utils";

describe("state-protocol profile", () => {
  test("installs on json streams and is visible in profile resource and listing", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-state-install-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/state-install", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const res = await fetchJsonApp(app, "http://local/v1/stream/state-install/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            touch: {
              enabled: true,
              onMissingBefore: "coarse",
              coarseIntervalMs: 75,
            },
          },
        }),
      });
      expect(res.status).toBe(200);
      expect(res.body?.profile?.kind).toBe("state-protocol");
      expect(res.body?.profile?.touch?.enabled).toBe(true);
      expect(res.body?.profile?.touch?.onMissingBefore).toBe("coarse");
      expect(res.body?.profile?.touch?.coarseIntervalMs).toBe(75);

      const getRes = await fetchJsonApp(app, "http://local/v1/stream/state-install/_profile", { method: "GET" });
      expect(getRes.status).toBe(200);
      expect(getRes.body?.profile?.kind).toBe("state-protocol");
      expect(getRes.body?.profile?.touch?.enabled).toBe(true);

      const listRes = await fetchJsonApp(app, "http://local/v1/streams", { method: "GET" });
      expect(listRes.status).toBe(200);
      expect(listRes.body.find((row: any) => row.name === "state-install")?.profile).toBe("state-protocol");

      expect(app.deps.db.getStream("state-install")?.profile).toBe("state-protocol");
      expect(app.deps.db.getStreamProfile("state-install")).not.toBeNull();
      expect(app.deps.db.getStreamTouchState("state-install")).not.toBeNull();
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("rejects non-json streams", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-state-non-json-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/state-non-json", {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        })
      );

      const res = await fetchJsonApp(app, "http://local/v1/stream/state-non-json/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            touch: { enabled: true },
          },
        }),
      });
      expect(res.status).toBe(400);
      expect(res.body?.error?.message).toContain("application/json");

      const getRes = await fetchJsonApp(app, "http://local/v1/stream/state-non-json/_profile", { method: "GET" });
      expect(getRes.status).toBe(200);
      expect(getRes.body).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "generic" },
      });
      expect(app.deps.db.getStream("state-non-json")?.profile).toBe("generic");
      expect(app.deps.db.getStreamProfile("state-non-json")).toBeNull();
      expect(app.deps.db.getStreamTouchState("state-non-json")).toBeNull();
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("rejects invalid state-protocol config", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-state-validate-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/state-validate", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const invalidProfileField = await fetchJsonApp(app, "http://local/v1/stream/state-validate/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            extra: true,
          },
        }),
      });
      expect(invalidProfileField.status).toBe(400);
      expect(invalidProfileField.body?.error?.message).toContain("profile.extra");

      const invalidTouchField = await fetchJsonApp(app, "http://local/v1/stream/state-validate/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            touch: {
              enabled: true,
              storage: "sqlite",
            },
          },
        }),
      });
      expect(invalidTouchField.status).toBe(400);
      expect(invalidTouchField.body?.error?.message).toContain("profile.touch.storage");
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("state-protocol without enabled touch keeps the profile but not the touch routes", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-state-disabled-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/state-disabled", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const res = await fetchJsonApp(app, "http://local/v1/stream/state-disabled/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            touch: { enabled: false },
          },
        }),
      });
      expect(res.status).toBe(200);
      expect(res.body).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: {
          kind: "state-protocol",
          touch: { enabled: false },
        },
      });

      expect(app.deps.db.getStream("state-disabled")?.profile).toBe("state-protocol");
      expect(app.deps.db.getStreamProfile("state-disabled")).not.toBeNull();
      expect(app.deps.db.getStreamTouchState("state-disabled")).toBeNull();

      const touchMetaRes = await app.fetch(new Request("http://local/v1/stream/state-disabled/touch/meta", { method: "GET" }));
      expect(touchMetaRes.status).toBe(404);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("state-protocol survives bootstrap with config and touch state", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-state-bootstrap-src-"));
    const root2 = mkdtempSync(join(tmpdir(), "ds-profile-state-bootstrap-dst-"));
    const { app, store } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/state-bootstrap", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const profileRes = await fetchJsonApp(app, "http://local/v1/stream/state-bootstrap/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            touch: {
              enabled: true,
              onMissingBefore: "coarse",
            },
          },
        }),
      });
      expect(profileRes.status).toBe(200);
      expect(profileRes.body?.profile?.kind).toBe("state-protocol");
      const installedProfile = profileRes.body?.profile;
      expect(installedProfile).toBeDefined();

      await app.deps.uploader.publishManifest("state-bootstrap");
      const cfg2 = makeProfileTestConfig(root2, { segmentCacheMaxBytes: 0, segmentFooterCacheEntries: 0 });
      await bootstrapFromR2(cfg2, store, { clearLocal: true });
      const { app: app2 } = createProfileTestApp(root2);
      try {
        await app2.deps.touch.tick();

        expect(app2.deps.db.getStream("state-bootstrap")?.profile).toBe("state-protocol");

        const profileRow = app2.deps.db.getStreamProfile("state-bootstrap");
        expect(profileRow).not.toBeNull();
        expect(JSON.parse(profileRow!.profile_json)).toEqual(installedProfile);

        expect(app2.deps.db.getStreamTouchState("state-bootstrap")).not.toBeNull();

        const getRes = await fetchJsonApp(app2, "http://local/v1/stream/state-bootstrap/_profile", { method: "GET" });
        expect(getRes.status).toBe(200);
        expect(getRes.body).toEqual({
          apiVersion: "durable.streams/profile/v1",
          profile: installedProfile,
        });

        const listRes = await fetchJsonApp(app2, "http://local/v1/streams", { method: "GET" });
        expect(listRes.status).toBe(200);
        expect(listRes.body.find((row: any) => row.name === "state-bootstrap")?.profile).toBe("state-protocol");

        const touchMetaRes = await app2.fetch(new Request("http://local/v1/stream/state-bootstrap/touch/meta", { method: "GET" }));
        expect(touchMetaRes.status).toBe(200);
      } finally {
        app2.close();
      }
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
      rmSync(root2, { recursive: true, force: true });
    }
  });

  test("accepts valid control messages and ignores them for touch derivation", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-state-control-"));
    const { app } = createProfileTestApp(root);
    try {
      const stream = "state-control";
      await app.fetch(
        new Request(`http://local/v1/stream/${stream}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const profileRes = await fetchJsonApp(app, `http://local/v1/stream/${stream}/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            touch: {
              enabled: true,
            },
          },
        }),
      });
      expect(profileRes.status).toBe(200);

      const metaRes = await fetchJsonApp(app, `http://local/v1/stream/${stream}/touch/meta`, { method: "GET" });
      expect(metaRes.status).toBe(200);

      const appendRes = await app.fetch(
        new Request(`http://local/v1/stream/${stream}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            headers: {
              control: "reset",
              offset: "-1",
            },
          }),
        })
      );
      expect(appendRes.status).toBe(204);

      app.deps.touch.notify(stream);
      await app.deps.touch.tick();

      const waitRes = await fetchJsonApp(app, `http://local/v1/stream/${stream}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: metaRes.body.cursor,
          keys: [tableKeyFor("public.posts")],
          timeoutMs: 0,
        }),
      });
      expect(waitRes.status).toBe(200);
      expect(waitRes.body?.touched).toBe(false);

      const readRes = await fetchJsonApp(app, `http://local/v1/stream/${stream}?format=json`, { method: "GET" });
      expect(readRes.status).toBe(200);
      expect(readRes.body).toEqual([
        {
          headers: {
            control: "reset",
            offset: "-1",
          },
        },
      ]);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("rejects malformed state-protocol records on append", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-state-append-validate-"));
    const { app } = createProfileTestApp(root);
    try {
      const stream = "state-append-validate";
      await app.fetch(
        new Request(`http://local/v1/stream/${stream}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const profileRes = await fetchJsonApp(app, `http://local/v1/stream/${stream}/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            touch: {
              enabled: true,
            },
          },
        }),
      });
      expect(profileRes.status).toBe(200);

      const cases: Array<{ name: string; body: unknown; message: string }> = [
        {
          name: "update missing value",
          body: {
            type: "public.posts",
            key: "42",
            headers: { operation: "update" },
          },
          message: "must include value",
        },
        {
          name: "invalid timestamp",
          body: {
            type: "public.posts",
            key: "42",
            value: { id: 42 },
            headers: { operation: "insert", timestamp: "not-a-timestamp" },
          },
          message: "valid RFC 3339 timestamp",
        },
        {
          name: "empty txid",
          body: {
            type: "public.posts",
            key: "42",
            value: { id: 42 },
            headers: { operation: "insert", txid: "" },
          },
          message: "txid must be a non-empty string",
        },
        {
          name: "invalid control offset",
          body: {
            headers: { control: "reset", offset: "not-an-offset" },
          },
          message: "valid stream offset string",
        },
        {
          name: "mixed control and operation",
          body: {
            headers: { control: "reset", operation: "delete" },
          },
          message: "cannot mix control and operation",
        },
        {
          name: "control message with extra field",
          body: {
            type: "public.posts",
            headers: { control: "snapshot-start" },
          },
          message: "state-protocol record.type is not supported",
        },
        {
          name: "non-object payload",
          body: "hello",
          message: "must be JSON objects",
        },
      ];

      for (const tc of cases) {
        const appendRes = await fetchJsonApp(app, `http://local/v1/stream/${stream}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(tc.body),
        });
        expect(appendRes.status, tc.name).toBe(400);
        expect(String(appendRes.body?.error?.message ?? ""), tc.name).toContain(tc.message);
      }
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
