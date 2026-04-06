import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bootstrapFromR2 } from "../src/bootstrap";
import { createProfileTestApp, fetchJsonApp, makeProfileTestConfig } from "./profile_test_utils";

describe("generic profile", () => {
  test("new streams default to generic in profile resource and listing", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-generic-default-"));
    const { app } = createProfileTestApp(root);
    try {
      const createRes = await app.fetch(
        new Request("http://local/v1/stream/generic-default", {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        })
      );
      expect([200, 201]).toContain(createRes.status);

      const profileRes = await fetchJsonApp(app, "http://local/v1/stream/generic-default/_profile", { method: "GET" });
      expect(profileRes.status).toBe(200);
      expect(profileRes.body).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "generic" },
      });

      const listRes = await fetchJsonApp(app, "http://local/v1/streams", { method: "GET" });
      expect(listRes.status).toBe(200);
      expect(listRes.body.find((row: any) => row.name === "generic-default")?.profile).toBe("generic");

      const row = app.deps.db.getStream("generic-default");
      expect(row?.profile).toBe("generic");
      expect(app.deps.db.getStreamProfile("generic-default")).toBeNull();
      expect(app.deps.db.getStreamTouchState("generic-default")).toBeNull();
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("generic rejects extra config", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-generic-validate-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/generic-validate", {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        })
      );

      const res = await fetchJsonApp(app, "http://local/v1/stream/generic-validate/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "generic",
            touch: { enabled: true },
          },
        }),
      });
      expect(res.status).toBe(400);
      expect(res.body?.error?.message).toContain("profile.touch");
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("switching back to generic removes state-protocol-owned state", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-generic-switch-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/generic-switch", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      let res = await fetchJsonApp(app, "http://local/v1/stream/generic-switch/_profile", {
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
      expect(res.status).toBe(200);
      expect(app.deps.db.getStreamProfile("generic-switch")).not.toBeNull();
      expect(app.deps.db.getStreamTouchState("generic-switch")).not.toBeNull();

      const touchMetaBefore = await app.fetch(new Request("http://local/v1/stream/generic-switch/touch/meta", { method: "GET" }));
      expect(touchMetaBefore.status).toBe(200);

      res = await fetchJsonApp(app, "http://local/v1/stream/generic-switch/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "generic" },
        }),
      });
      expect(res.status).toBe(200);
      expect(res.body).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "generic" },
      });

      const touchMetaAfter = await app.fetch(new Request("http://local/v1/stream/generic-switch/touch/meta", { method: "GET" }));
      expect(touchMetaAfter.status).toBe(404);
      expect(app.deps.db.getStream("generic-switch")?.profile).toBe("generic");
      expect(app.deps.db.getStreamProfile("generic-switch")).toBeNull();
      expect(app.deps.db.getStreamTouchState("generic-switch")).toBeNull();
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("generic survives bootstrap without profile_json state", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-generic-bootstrap-src-"));
    const root2 = mkdtempSync(join(tmpdir(), "ds-profile-generic-bootstrap-dst-"));
    const { app, store } = createProfileTestApp(root);
    try {
      await app.fetch(
        new Request("http://local/v1/stream/generic-bootstrap", {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        })
      );
      await app.deps.uploader.publishManifest("generic-bootstrap");
    } finally {
      app.close();
    }

    const cfg2 = makeProfileTestConfig(root2, { segmentCacheMaxBytes: 0, segmentFooterCacheEntries: 0 });
    await bootstrapFromR2(cfg2, store, { clearLocal: true });
    const { app: app2 } = createProfileTestApp(root2);
    try {
      const row = app2.deps.db.getStream("generic-bootstrap");
      expect(row?.profile).toBe("generic");
      expect(app2.deps.db.getStreamProfile("generic-bootstrap")).toBeNull();

      const profileRes = await fetchJsonApp(app2, "http://local/v1/stream/generic-bootstrap/_profile", { method: "GET" });
      expect(profileRes.status).toBe(200);
      expect(profileRes.body).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "generic" },
      });

      const listRes = await fetchJsonApp(app2, "http://local/v1/streams", { method: "GET" });
      expect(listRes.status).toBe(200);
      expect(listRes.body.find((entry: any) => entry.name === "generic-bootstrap")?.profile).toBe("generic");
    } finally {
      app2.close();
      rmSync(root, { recursive: true, force: true });
      rmSync(root2, { recursive: true, force: true });
    }
  });
});
