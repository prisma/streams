// The issuer's rate limits (public-beta issuer triage B2.7). Run with
// `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { cleanup } from "../cell-admin/test-rig";
import { boot } from "./boot";
import { StateCache } from "./cache";
import { readConfig } from "./config";
import { LIMITS, type LimitName, RateLimits, TRUSTED_CLIENT_IP_HEADER } from "./limits";
import { customerSecret, type IssuerCell, issuerCell, wrongSecret } from "./rig";
import { Issuer } from "./server";

afterEach(cleanup);

describe("the buckets", () => {
  test("each allows its burst, then refuses with the wait for one token, and takes one after that wait", () => {
    const clock = { t: 1_000_000 };
    const limits = new RateLimits(() => clock.t);
    const expected: Record<LimitName, [number, number]> = {
      exchange_global: [200, 1],
      exchange_credential: [60, 1],
      exchange_malformed: [100, 1],
      exchange_credential_failed: [20, 3],
      management_session: [60, 1],
      oauth_start: [30, 2],
      project_create_workspace: [5, 17_280],
      credential_change_project: [20, 4_320],
      exchange_ip: [50, 1],
      exchange_failed_ip: [20, 3],
      oauth_start_ip: [30, 2],
    };
    expect(Object.keys(expected).sort()).toEqual(Object.keys(LIMITS).sort());
    for (const [name, [burst, wait]] of Object.entries(expected) as [LimitName, [number, number]][]) {
      for (let i = 0; i < burst; i++) expect(limits.take(name, "k")).toBe(0);
      expect([name, limits.take(name, "k")]).toEqual([name, wait]);
      expect(limits.take(name, "other")).toBe(0);
      clock.t += wait * 1000;
      expect([name, limits.take(name, "k")]).toEqual([name, 0]);
      expect(limits.refused[name]).toBe(1);
    }
  });

  test("a spent failure budget locks the key out for 10 minutes", () => {
    const clock = { t: 1_000_000 };
    const limits = new RateLimits(() => clock.t);
    for (let i = 0; i < 19; i++) limits.charge("exchange_failed_ip", "a");
    expect(limits.locked("exchange_failed_ip", "a")).toBe(0);
    limits.charge("exchange_failed_ip", "a");
    expect([limits.locked("exchange_failed_ip", "a"), limits.locked("exchange_failed_ip", "b")]).toEqual([600, 0]);
    clock.t += 599_000;
    expect(limits.locked("exchange_failed_ip", "a")).toBe(1);
    clock.t += 1_000;
    expect(limits.locked("exchange_failed_ip", "a")).toBe(0);
  });

  test("forgets refilled keys once it holds 10,000, so sprayed ids cannot grow it without bound", () => {
    const clock = { t: 1_000_000 };
    const limits = new RateLimits(() => clock.t);
    for (let i = 0; i < 10_000; i++) limits.take("exchange_credential", `k${i}`);
    expect(limits.keys("exchange_credential")).toBe(10_000);
    clock.t += 1_000; // every bucket refills from 59 to 60
    limits.take("exchange_credential", "new");
    expect(limits.keys("exchange_credential")).toBe(1);
  });
});

async function issuerFor(cell: IssuerCell, ipHeader?: string | null) {
  const clock = { t: Date.now() };
  const now = () => clock.t;
  const cfg = readConfig(cell.env);
  const b = await boot(cfg);
  const cache = new StateCache(b.store, b, { now, log: () => {} });
  return { clock, cache, issuer: new Issuer(cfg, { store: b.store, cache, now, ...(ipHeader !== undefined ? { ipHeader } : {}) }) };
}

const exchange = (issuer: Issuer, secret: string | undefined, headers: Record<string, string> = {}) =>
  issuer.fetch(
    new Request("https://issuer.example.com/v1/token/streams", {
      method: "POST",
      headers: { ...headers, ...(secret === undefined ? {} : { authorization: `StreamsCredential ${secret}` }) },
    }),
  );

async function statuses(n: number, f: () => Promise<Response>): Promise<Record<number, number>> {
  const seen: Record<number, number> = {};
  for (let i = 0; i < n; i++) {
    const s = (await f()).status;
    seen[s] = (seen[s] ?? 0) + 1;
  }
  return seen;
}

describe("the exchange's limits", () => {
  test("a credential gets 60 exchanges at once, then 429 rate_limited with Retry-After; wrong secrets for its id never spend them", async () => {
    const cell = await issuerCell();
    const { id, secret } = await customerSecret(cell, "sp_a");
    const { secret: other } = await customerSecret(cell, "sp_b");
    const { issuer, clock } = await issuerFor(cell);
    // A stranger who knows the id (every token names it) sends wrong
    // secrets: 401, then 429 once the failures' own budget is spent.
    expect(await statuses(25, () => exchange(issuer, wrongSecret(id)))).toEqual({ 401: 20, 429: 5 });
    const failed = await exchange(issuer, wrongSecret(id));
    expect([failed.status, (await failed.json()).error.message, failed.headers.get("retry-after")]).toEqual([
      429, `too many failed exchanges for credential ${id}; retry after 3 s`, "3",
    ]);
    // The credential's owner still gets its whole budget.
    expect(await statuses(60, () => exchange(issuer, secret))).toEqual({ 200: 60 });
    const res = await exchange(issuer, secret);
    expect([res.status, await res.json(), res.headers.get("retry-after")]).toEqual([
      429, { error: { code: "rate_limited", message: `too many exchanges for credential ${id}; retry after 1 s` } }, "1",
    ]);
    expect((await exchange(issuer, other)).status).toBe(200);
    clock.t += 1_000;
    expect(await statuses(3, () => exchange(issuer, secret))).toEqual({ 200: 2, 429: 1 });
    expect([issuer.limits.refused.exchange_credential, issuer.limits.refused.exchange_credential_failed]).toEqual([2, 6]);
  });

  test("malformed exchanges spend their own instance budget, never the one valid credentials mint from", async () => {
    const cell = await issuerCell();
    const { secret } = await customerSecret(cell, "sp_a");
    const { issuer } = await issuerFor(cell);
    expect(await statuses(200, () => exchange(issuer, undefined))).toEqual({ 401: 100, 429: 100 });
    const res = await exchange(issuer, undefined);
    expect([res.status, (await res.json()).error.message]).toEqual([429, "too many malformed exchanges; retry after 1 s"]);
    expect((await exchange(issuer, secret)).status).toBe(200);
  });

  test("the instance mints 200 tokens at once, then refuses every credential", async () => {
    const cell = await issuerCell();
    const secrets = [];
    for (let i = 0; i < 4; i++) secrets.push((await customerSecret(cell, "sp_a")).secret);
    const { issuer } = await issuerFor(cell);
    for (const secret of secrets) expect(await statuses(50, () => exchange(issuer, secret))).toEqual({ 200: 50 });
    const res = await exchange(issuer, secrets[0]);
    expect([res.status, (await res.json()).error.message]).toEqual([429, "the issuer is at its exchange rate; retry after 1 s"]);
  });

  test("per-address limits apply only once the edge's client-IP header is known (K4)", async () => {
    expect(TRUSTED_CLIENT_IP_HEADER).toBeNull();
    const cell = await issuerCell();
    const { id, secret } = await customerSecret(cell, "sp_a");
    const ip = { "x-client-ip": "203.0.113.7" };
    const unwired = await issuerFor(cell);
    expect(await statuses(55, () => exchange(unwired.issuer, secret, ip))).toEqual({ 200: 55 });
    const wired = await issuerFor(cell, "x-client-ip");
    expect(await statuses(51, () => exchange(wired.issuer, secret, ip))).toEqual({ 200: 50, 429: 1 });
    expect((await exchange(wired.issuer, secret, { "x-client-ip": "203.0.113.8" })).status).toBe(200);
    const failing = await issuerFor(cell, "x-client-ip");
    expect(await statuses(20, () => exchange(failing.issuer, wrongSecret(id), ip))).toEqual({ 401: 20 });
    const locked = await exchange(failing.issuer, secret, ip);
    expect([locked.status, locked.headers.get("retry-after")]).toEqual([429, "600"]);
    failing.clock.t += 600_000;
    await failing.cache.refresh();
    expect((await exchange(failing.issuer, secret, ip)).status).toBe(200);
  });
});
