import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bootstrapFromR2 } from "../src/bootstrap";
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
});
