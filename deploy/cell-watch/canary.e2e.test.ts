// The cell watch's canary (public-beta alerting triage B7) against the
// real server and the real issuer: a shared cell initialised by cell-admin
// (which places the canary project) on the shared-cell profile over
// s3lite, the watch's hidden credential made by `credential --hidden`, and
// the issuer's own exchange served on a local port, as cell-admin's and
// the issuer's tests boot them. Opt-in, because it needs built binaries:
//
//   cargo build --locked --bin streams-slate --bin s3lite
//   CELL_WATCH_E2E_BIN_DIR=target/debug bun test ./deploy/cell-watch/canary.e2e.test.ts
//
// It proves what canary.test.ts's stand-ins assume: the exchange answers
// the cell's origin as `endpoint`, the tail read of a missing stream
// answers 404 and the create then 201, `cursor=now` answers the tail, an
// acknowledged record is read back durable from it, a second watch finds
// the stream without creating it, and the issuer's and the cell's
// refusals carry the codes and the origin header the rows classify.
import { afterAll, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { DEFAULT_PROFILES } from "../cell-admin/commands";
import { readProfiles } from "../cell-admin/profile";
import { canaryProjectId } from "../cell-admin/rules";
import { cleanup, readBundle, scratchDir } from "../cell-admin/test-rig";
import { boot } from "../issuer/boot";
import { StateCache } from "../issuer/cache";
import { readConfig } from "../issuer/config";
import { admin, envWith, issuerCell, wrongSecret } from "../issuer/rig";
import { Issuer } from "../issuer/server";
import { Canary, CANARY_SCOPES, canaryStreamKey, RECORDS_PATH } from "./canary";
import { FakeIssuer } from "./fake-cell";
import { TIMEOUT_MS } from "./poll";
import { ERROR_CODES } from "./rules";

const BIN_DIR = process.env.CELL_WATCH_E2E_BIN_DIR;
const maybe = BIN_DIR ? test : test.skip;

const children: ReturnType<typeof Bun.spawn>[] = [];
const stops: (() => void)[] = [];

afterAll(() => {
  for (const s of stops) s();
  for (const c of children) c.kill();
  cleanup();
});

function freePort(): number {
  const s = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const port = Number(s.port);
  s.stop(true);
  return port;
}

async function until<T>(what: string, secs: number, probe: () => Promise<T | undefined>): Promise<T> {
  for (let i = 0; i < secs * 4; i++) {
    const v = await probe().catch(() => undefined);
    if (v !== undefined) return v;
    await Bun.sleep(250);
  }
  throw new Error(`timed out waiting for ${what}`);
}

/// What deploy/app-server does with the bundle: three files, each replaced atomically.
function publish(state: string, feeds: string): void {
  const b = readBundle(state);
  for (const [name, doc] of [["keys", b.keys], ["policies", b.policies], ["grants", b.grants]] as const) {
    writeFileSync(join(feeds, `${name}.json.tmp`), JSON.stringify(doc));
    renameSync(join(feeds, `${name}.json.tmp`), join(feeds, `${name}.json`));
  }
}

maybe("the canary exchanges at the real issuer and appends and reads back on the real cell; their refusals classify", async () => {
  const bin = resolve(BIN_DIR ?? "");
  const cell = await issuerCell();
  const secretFile = join(cell.root, "canary-watch.secret");
  await admin(cell, "credential", "--project", canaryProjectId("cell_sc1"), "--hidden", "--name", "watch",
    "--scopes", CANARY_SCOPES, "--pepper-file", cell.pepper, "--out", secretFile);
  const credential = readFileSync(secretFile, "utf8").trim();
  const feeds = join(scratchDir(), "feeds");
  mkdirSync(feeds);
  publish(cell.state, feeds);

  const [s3Port, port] = [freePort(), freePort()];
  children.push(Bun.spawn([join(bin, "s3lite"), "--listen", `127.0.0.1:${s3Port}`, "--latency-ms", "2"],
    { stdout: "ignore", stderr: "ignore" }));
  const env: Record<string, string> = {
    PATH: process.env.PATH ?? "",
    HOME: process.env.HOME ?? "",
    ...Object.fromEntries(readProfiles(DEFAULT_PROFILES)),
    STREAMS_AUTH_KEYS_FILE: join(feeds, "keys.json"),
    STREAMS_AUTH_POLICY_FILE: join(feeds, "policies.json"),
    STREAMS_AUTH_GRANTS_FILE: join(feeds, "grants.json"),
    CELL_ID: "cell_sc1",
    PROJECT_ID: "proj_deploy",
    ACCOUNT_ID: "acct_sink",
    USAGE_STREAM_KEY: randomBytes(32).toString("base64url"),
    AUTH_TOKEN: randomBytes(24).toString("hex"),
  };
  const log = join(cell.root, "server.log");
  children.push(Bun.spawn([join(bin, "streams-slate"), "--listen", `127.0.0.1:${port}`,
    "--s3-endpoint", `http://127.0.0.1:${s3Port}`, "--bucket", `cell-watch-canary-e2e-${Date.now()}`],
    { env, stdout: Bun.file(log), stderr: Bun.file(log) }));
  const cellOrigin = `http://127.0.0.1:${port}`;
  await until("the cell to be ready", 90, async () => ((await fetch(`${cellOrigin}/health`)).ok ? true : undefined));

  // The issuer, as main.ts runs it, for this cell's origin.
  const cfg = readConfig(envWith(cell, { STREAMS_CELL_ORIGIN: cellOrigin }));
  const booted = await boot(cfg);
  const cache = new StateCache(booted.store, booted, { log: () => {} });
  const issuer = new Issuer(cfg, { store: booted.store, cache, log: () => {} });
  stops.push(cache.start());
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: (req) => issuer.fetch(req) });
  stops.push(() => server.stop(true));
  const issuerOrigin = `http://127.0.0.1:${server.port}`;

  let n = 0;
  const canary = (more: { issuerOrigin?: string; credential?: string } = {}) =>
    new Canary({ cellOrigin, issuerOrigin, credential, cellId: "cell_sc1", timeoutMs: TIMEOUT_MS, ...more }, Date.now,
      () => `e2e-${++n}`);
  // The canary project takes 5 requests a second with one second's burst
  // (429 project_rate_limit beyond): the watch makes at most five a round,
  // a round a minute; here a pause of 1.5 s stands for the minute.
  const pause = () => Bun.sleep(1_500);
  // At boot the billing sweep's probe holds the cell's one shard off for a
  // few seconds (edge record #17), so the first rounds may be refused
  // retryably: a round that passes first.
  await until("a canary round that passes", 60, async () => {
    const r = await canary().run();
    await pause();
    return r.append?.ok && r.read?.ok ? true : undefined;
  });

  // A second watch: one exchange, whose endpoint is the cell's origin; the
  // stream found under the cell's key, not created again (a create inside
  // the first's store debt would be refused 429, edge record #143); the
  // record read back.
  const ok = { ok: true, status: 200, errorCode: null, origin: true };
  const watch = canary();
  const first = await watch.run();
  expect(first).toEqual({ exchange: { ok: true, status: 200, errorCode: null, origin: false, endpoint: cellOrigin }, append: ok, read: ok });
  await pause();
  expect(await watch.run()).toEqual({ exchange: null, append: ok, read: ok });
  await pause();
  // The stream holds the two records last appended, as written.
  const token = await (await fetch(`${issuerOrigin}/v1/token/streams`, {
    method: "POST", headers: { authorization: `StreamsCredential ${credential}` },
  })).json() as { accessToken: string };
  const all = await fetch(`${cellOrigin}${RECORDS_PATH}?cursor=beginning`, {
    headers: { authorization: `Bearer ${token.accessToken}`, "streams-encryption-key": canaryStreamKey("cell_sc1") },
  });
  const records = await all.json() as { canary: string; at: number }[];
  expect([all.status, records.slice(-2).map((r) => r.canary)]).toEqual([200, [`e2e-${n - 1}`, `e2e-${n}`]]);

  // The issuer refuses a wrong secret: its step, and nothing reaches the cell.
  expect(await canary({ credential: wrongSecret(credential.slice(15, 55)) }).run()).toEqual({
    exchange: { ok: false, status: 401, errorCode: "invalid_credential", origin: false, endpoint: null },
    append: null,
    read: null,
  });
  // The cell refuses a token it cannot verify: the tail read (the read
  // step, before any append), with the server's origin header and an auth
  // code rules.ts names.
  const forger = new FakeIssuer();
  stops.push(() => forger.stop());
  forger.exchange = { status: 200, body: JSON.stringify({ accessToken: "not-a-token", expiresIn: 3600, endpoint: cellOrigin }) };
  const refused = await canary({ issuerOrigin: forger.origin }).run();
  expect([refused.append, refused.read]).toEqual([null, { ok: false, status: 401, errorCode: "malformed", origin: true }]);
  expect(ERROR_CODES).toContain("malformed");
}, 240_000);
