// The issuer's operator routes (public-beta issuer triage B2.9). Run
// with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { FakeCell } from "../cell-admin/fake-cell";
import { cleanup, keyFile, publicKeyFile, readBundle, run } from "../cell-admin/test-rig";
import { boot } from "./boot";
import { StateCache } from "./cache";
import { readConfig } from "./config";
import {
  admin,
  CELL,
  cellState,
  customerSecret,
  envWith,
  fakeS3,
  type IssuerCell,
  issuerCell,
  managed,
  wrongSecret,
} from "./rig";
import { Issuer } from "./server";

afterEach(cleanup);

const T0 = 1_800_000_000_000;

async function issuerFor(cell: IssuerCell) {
  const clock = { t: T0 };
  const now = () => clock.t;
  const cfg = readConfig(cell.env);
  const b = await boot(cfg);
  const cache = new StateCache(b.store, b, { now, log: () => {} });
  return { cfg, cache, clock, issuer: new Issuer(cfg, { store: b.store, cache, now, log: () => {} }) };
}

const get = (issuer: Issuer, path: string, headers: Record<string, string> = {}, method = "GET") =>
  issuer.fetch(new Request(`https://issuer.example.com${path}`, { method, headers }));

const exchange = (issuer: Issuer, secret: string) =>
  issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
    method: "POST",
    headers: { authorization: `StreamsCredential ${secret}` },
  }));

async function answer(res: Response): Promise<[number, any]> {
  return [res.status, await res.json()];
}

describe("GET /healthz", () => {
  test("is 200 while the issuer can mint, 503 with the reason while it cannot", async () => {
    const cell = await issuerCell();
    const { issuer, cache, clock } = await issuerFor(cell);
    expect(await answer(await get(issuer, "/healthz"))).toEqual([200, { status: "ok" }]);
    fakeS3().failReads = true;
    clock.t += 301_000;
    await cache.refresh();
    expect(await answer(await get(issuer, "/healthz"))).toEqual([503, { status: "unavailable", reason: "state_stale" }]);
    fakeS3().failReads = false;
    const next = publicKeyFile(keyFile(cell.root, "ed25519", 0o600, "next.pem"), "next.pub");
    expect((await run("keys", "add", "--state", cell.state, "--public-key", next, "--retire-others")).code).toBe(0);
    await cache.refresh();
    expect(await answer(await get(issuer, "/healthz"))).toEqual([
      503, { status: "unavailable", reason: "key_not_in_state" },
    ]);
    const post = await get(issuer, "/healthz", {}, "POST");
    expect([post.status, post.headers.get("allow")]).toEqual([405, "GET"]);
  });
});

describe("GET /v1/admin/metrics", () => {
  test("answers only the metrics bearer", async () => {
    const cell = await issuerCell();
    const { issuer } = await issuerFor(cell);
    for (const auth of [undefined, "Bearer wrong", `Bearer ${cell.env.STREAMS_ISSUER_METRICS_TOKEN}x`, cell.env.STREAMS_ISSUER_METRICS_TOKEN]) {
      const res = await get(issuer, "/v1/admin/metrics", auth === undefined ? {} : { authorization: auth });
      expect([res.status, await res.json(), res.headers.get("www-authenticate")]).toEqual([
        401, { error: { code: "unauthorized", message: "send Authorization: Bearer <the metrics token>" } },
        'Bearer realm="streams-issuer"',
      ]);
    }
  });

  test("reports the counters, the copy's age, the bundle's version and the admission gate", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const { issuer, cache, clock, cfg } = await issuerFor(cell);
    const bearer = { authorization: `Bearer ${cell.env.STREAMS_ISSUER_METRICS_TOKEN}` };
    for (const s of [a.secret, a.secret, wrongSecret(a.id)]) await exchange(issuer, s);
    clock.t += 42_000;
    expect(await answer(await get(issuer, "/v1/admin/metrics", bearer))).toEqual([200, {
      cell_id: CELL,
      kid: cfg.signingKey.record.kid,
      uptime_secs: 42,
      healthy: true,
      cache_age_secs: 42,
      cache_fresh: true,
      state_feed_version: 3,
      published_feed_version: null,
      gate_open: true,
      gate_n_max: 50,
      exchanges: {
        issued: 2, invalid_request: 0, invalid_credential: 1, credential_not_active: 0, project_not_active: 0,
        rate_limited: 0, issuer_unavailable: 0,
      },
      rate_limit_refusals: {
        exchange_global: 0, exchange_credential: 0, exchange_malformed: 0, exchange_credential_failed: 0,
        management_session: 0, oauth_start: 0,
        project_create_workspace: 0, credential_change_project: 0, exchange_ip: 0, exchange_failed_ip: 0,
        oauth_start_ip: 0,
      },
      revalidation_failures: 0,
      republishes: 0,
      offboarding_projects: 0,
      offboarding_max_attempts: 0,
      rehash_pending: 0,
      live_credentials_by_pepper: { v1: 1 },
    }]);
    expect((await run("gate", "--state", cell.state, "--close")).code).toBe(0);
    await cache.refresh();
    const [, after] = await answer(await get(issuer, "/v1/admin/metrics", bearer));
    expect([after.gate_open, after.cache_age_secs, after.published_feed_version, after.state_feed_version]).toEqual([
      false, 0, cellState(cell).feed_version, cellState(cell).feed_version,
    ]);
  });

  test("counts the projects being offboarded and the most attempts any of their drains has failed", async () => {
    const cell = await issuerCell();
    for (const p of ["sp_a", "sp_b", "sp_c"]) await customerSecret(cell, p);
    const fake = new FakeCell();
    try {
      const m = await managed(cell, envWith(cell, { STREAMS_CELL_ORIGIN: fake.url }));
      const bearer = { authorization: `Bearer ${cell.env.STREAMS_ISSUER_METRICS_TOKEN}` };
      const offboarding = async () => {
        const [, got] = await answer(await m.call("GET", "/v1/admin/metrics", undefined, { headers: bearer }));
        return [got.offboarding_projects, got.offboarding_max_attempts];
      };
      expect(await offboarding()).toEqual([0, 0]);
      // The cell serves sp_a's revocation, never sp_c's or sp_b's.
      await admin(cell, "offboard", "--project", "sp_a");
      fake.publish(readBundle(cell.state));
      await admin(cell, "offboard", "--project", "sp_c");
      await m.cache.refresh();
      expect(await offboarding()).toEqual([2, 0]);
      await m.issuer.offboarding.runDue();
      expect(cellState(cell).projects.sp_a.phase).toBe("offboarded");
      expect(await offboarding()).toEqual([1, 1]);
      await admin(cell, "offboard", "--project", "sp_b");
      m.clock.t += 60_000;
      await m.cache.refresh();
      await m.issuer.offboarding.runDue();
      expect([cellState(cell).projects.sp_b.offboard_attempts, cellState(cell).projects.sp_c.offboard_attempts]).toEqual([1, 2]);
      expect(await offboarding()).toEqual([2, 2]);
    } finally {
      fake.stop();
    }
  });
});
