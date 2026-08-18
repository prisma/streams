#!/usr/bin/env node
// SR-6c: the REAL multi-project noisy-neighbor campaign (contract
// Stage 8: 32-64 noisy projects, hundreds idle, locked thresholds).
//
// Unlike the in-suite mechanism test (Review 8), this drives a REAL
// streams-slate process in ENFORCE mode through the operator feed
// files: generated RSA JWKS + per-project policies/grants, per-project
// RS256 JWTs, product-surface traffic. It measures a victim project's
// latency alone vs under the noisy herd and FAILS on the locked
// thresholds below.
//
//   node scripts/mt-noisy-campaign.mjs
//
// Env knobs: NOISY (default 48), IDLE (200), WINDOW_SECS (30),
// NOISY_RPS (20 per noisy project), BIN (target/release/streams-slate),
// KEEP=1 to leave the server running for inspection.
//
// LOCKED THRESHOLDS (the campaign's contract — change only with a
// reviewed commit):
//   T1 victim p99 append under load <= max(2.0x solo p99, 25 ms)
//   T2 victim p99 read   under load <= max(2.0x solo p99, 25 ms)
//   T3 victim hard-error count (non-2xx excluding typed 429/503) == 0
//   T4 noisy 5xx (untyped) rate < 0.5% — refusals must be TYPED
//      quota/backpressure answers, never internal errors.
import { spawn, execSync } from "node:child_process";
import { generateKeyPairSync, createSign, randomBytes } from "node:crypto";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const NOISY = +(process.env.NOISY ?? 48);
const IDLE = +(process.env.IDLE ?? 200);
const WINDOW_SECS = +(process.env.WINDOW_SECS ?? 30);
const NOISY_RPS = +(process.env.NOISY_RPS ?? 20);
const BIN = process.env.BIN ?? "target/release/streams-slate";
const S3LITE = process.env.S3LITE ?? "target/release/s3lite";
const PORT = +(process.env.PORT ?? 8471);
const S3PORT = PORT + 1;
const CELL = "noisy-cell";
const ISS = "https://auth.prisma.io";
const KEY_B64 = Buffer.from(Array(32).fill(7)).toString("base64");

const dir = mkdtempSync(join(tmpdir(), "mt-noisy-"));
const { publicKey, privateKey } = generateKeyPairSync("rsa", {
  modulusLength: 2048,
  publicKeyEncoding: { type: "spki", format: "pem" },
  privateKeyEncoding: { type: "pkcs8", format: "pem" },
});

const scopes =
  "streams.create streams.records.append streams.records.read streams.metadata.read";
const projName = (i) => (i === 0 ? "proj-victim" : `proj-noisy-${String(i).padStart(3, "0")}`);
const credName = (i) => `cred-${projName(i)}`;
const total = 1 + NOISY + IDLE;
const projects = [];
const credentials = [];
for (let i = 0; i < total; i++) {
  const pid = i <= NOISY ? projName(i) : `proj-idle-${String(i - NOISY).padStart(3, "0")}`;
  projects.push({
    project_id: pid,
    workspace_id: `ws-${i % 16}`,
    cell_id: CELL,
    project_policy_version: 1,
    ownership_version: 1,
    status: "active",
    quotas: {},
  });
  credentials.push({
    credential_id: `cred-${pid}`,
    project_id: pid,
    grant_version: 1,
    status: "active",
    scopes,
  });
}
writeFileSync(join(dir, "keys.json"), JSON.stringify({ feed_version: 1, keys: [{ kid: "camp-1", alg: "RS256", pem: publicKey }] }));
writeFileSync(join(dir, "policies.json"), JSON.stringify({ feed_version: 1, projects }));
writeFileSync(join(dir, "grants.json"), JSON.stringify({ feed_version: 1, credentials }));

const b64u = (b) => Buffer.from(b).toString("base64url");
function jwt(project, ws) {
  const now = Math.floor(Date.now() / 1000);
  const header = b64u(JSON.stringify({ alg: "RS256", typ: "JWT", kid: "camp-1" }));
  const claims = b64u(
    JSON.stringify({
      iss: ISS, aud: "prisma-streams-data", sub: "camp",
      credential_id: `cred-${project}`, project_id: project,
      workspace_id: ws, cell_id: CELL,
      ownership_version: 1, grant_version: 1, scope: scopes,
      jti: randomBytes(8).toString("hex"), iat: now - 60, exp: now + 7200,
    }),
  );
  const s = createSign("RSA-SHA256");
  s.update(`${header}.${claims}`);
  return `${header}.${claims}.${s.sign(privateKey, "base64url")}`;
}

function spawnServer() {
  const s3 = spawn(S3LITE, ["--listen", `127.0.0.1:${S3PORT}`, "--latency-ms", "2"], { stdio: "ignore" });
  const env = {
    ...process.env,
    STREAMS_AUTH_MODE: "enforce",
    STREAMS_AUTH_ISSUER: ISS,
    STREAMS_AUTH_KEYS_FILE: join(dir, "keys.json"),
    STREAMS_AUTH_POLICY_FILE: join(dir, "policies.json"),
    STREAMS_AUTH_GRANTS_FILE: join(dir, "grants.json"),
    CELL_ID: CELL,
    PROJECT_ID: "proj-deploy-camp",
    AUTH_TOKEN: "camp-account-token",
    USAGE_STREAM_KEY: KEY_B64,
  };
  const srv = spawn(
    BIN,
    ["--listen", `127.0.0.1:${PORT}`, "--s3-endpoint", `http://127.0.0.1:${S3PORT}`,
     "--bucket", `noisy-${Date.now()}`, "--max-unflushed-bytes", "67108864",
     "--flush-interval-ms", "1", "--wal-flush-gap-ms", "2"],
    { env, stdio: ["ignore", "inherit", "inherit"] },
  );
  return { s3, srv };
}

const base = `http://127.0.0.1:${PORT}`;
async function req(method, path, token, body, extra = {}) {
  const t0 = performance.now();
  const r = await fetch(base + path, {
    method,
    headers: {
      authorization: `Bearer ${token}`,
      "prisma-encryption-key": KEY_B64,
      "content-type": "application/json",
      ...extra,
    },
    body: body ?? undefined,
  }).catch(() => ({ status: 0 }));
  if (r.body) await r.arrayBuffer?.().catch(() => {});
  return { status: r.status, ms: performance.now() - t0 };
}

function pct(sorted, p) {
  if (!sorted.length) return NaN;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

async function victimWindow(tok, label, secs) {
  const app = [], rd = [];
  let hardErrors = 0;
  const stop = Date.now() + secs * 1000;
  let i = 0;
  while (Date.now() < stop) {
    const a = await req("POST", "/v1/streams/victim/records", tok, JSON.stringify({ i: i++ }));
    if (a.status === 200) app.push(a.ms);
    else if (a.status !== 429 && a.status !== 503) hardErrors++;
    const g = await req("GET", "/v1/streams/victim/records", tok);
    if (g.status === 200) rd.push(g.ms);
    else if (g.status !== 429 && g.status !== 503) hardErrors++;
    await new Promise((r) => setTimeout(r, 20));
  }
  app.sort((x, y) => x - y); rd.sort((x, y) => x - y);
  const out = {
    label,
    appends: app.length, ap50: pct(app, 50), ap99: pct(app, 99),
    reads: rd.length, rp50: pct(rd, 50), rp99: pct(rd, 99),
    hardErrors,
  };
  console.log(
    `[victim ${label}] appends=${out.appends} p50=${out.ap50?.toFixed(1)}ms p99=${out.ap99?.toFixed(1)}ms | ` +
    `reads=${out.reads} p50=${out.rp50?.toFixed(1)}ms p99=${out.rp99?.toFixed(1)}ms | hard-errors=${out.hardErrors}`,
  );
  return out;
}

async function main() {
  execSync(`cargo build --release --bin streams-slate --bin s3lite`, { stdio: "inherit" });
  const { s3, srv } = spawnServer();
  const kill = () => { try { srv.kill(); } catch {} try { s3.kill(); } catch {} };
  process.on("exit", () => { if (!process.env.KEEP) kill(); });
  await new Promise((r) => setTimeout(r, 2500));

  const vt = jwt("proj-victim", "ws-0");
  // Victim + noisy streams exist under the SAME name in every project
  // — the adversarial configuration.
  let r = await req("PUT", "/v1/streams/victim", vt, JSON.stringify({ format: { kind: "json" } }));
  if (r.status !== 201) throw new Error(`victim create: ${r.status}`);

  console.log(`\n== Phase 0: victim SOLO (${WINDOW_SECS}s) ==`);
  const solo = await victimWindow(vt, "solo", WINDOW_SECS);

  console.log(`\n== Phase 1: ${NOISY} noisy projects @ ~${NOISY_RPS} rps each + ${IDLE} idle (${WINDOW_SECS}s) ==`);
  const noisyToks = [];
  for (let i = 1; i <= NOISY; i++) {
    const p = projName(i);
    const t = jwt(p, `ws-${i % 16}`);
    const c = await req("PUT", "/v1/streams/victim", t, JSON.stringify({ format: { kind: "json" } }));
    if (c.status !== 201) throw new Error(`noisy ${p} create: ${c.status}`);
    noisyToks.push(t);
  }
  // Idle projects exist only in the feed (policies+grants) — the
  // hundreds-idle posture is about tracker/feed load, not traffic.
  let noisyTotal = 0, noisy5xx = 0, noisyTyped = 0;
  let stopNoise = false;
  const noise = noisyToks.map(async (t) => {
    let i = 0;
    while (!stopNoise) {
      const a = await req("POST", "/v1/streams/victim/records", t, JSON.stringify({ n: i++ }));
      noisyTotal++;
      if (a.status === 429 || a.status === 503) noisyTyped++;
      else if (a.status >= 500) noisy5xx++;
      await new Promise((r) => setTimeout(r, Math.max(1, 1000 / NOISY_RPS)));
    }
  });
  const loaded = await victimWindow(vt, "under-noise", WINDOW_SECS);
  stopNoise = true;
  await Promise.allSettled(noise);
  console.log(`[noisy] total=${noisyTotal} typed-refusals=${noisyTyped} untyped-5xx=${noisy5xx}`);

  // ---- locked thresholds ----
  const lim = (v) => Math.max(2.0 * v, 25);
  const fails = [];
  if (!(loaded.ap99 <= lim(solo.ap99))) fails.push(`T1 append p99 ${loaded.ap99?.toFixed(1)}ms > limit ${lim(solo.ap99).toFixed(1)}ms`);
  if (!(loaded.rp99 <= lim(solo.rp99))) fails.push(`T2 read p99 ${loaded.rp99?.toFixed(1)}ms > limit ${lim(solo.rp99).toFixed(1)}ms`);
  if (solo.hardErrors + loaded.hardErrors > 0) fails.push(`T3 victim hard errors: solo=${solo.hardErrors} loaded=${loaded.hardErrors}`);
  if (!(noisy5xx / Math.max(1, noisyTotal) < 0.005)) fails.push(`T4 noisy untyped 5xx ${noisy5xx}/${noisyTotal}`);

  if (!process.env.KEEP) kill();
  if (fails.length) {
    console.error(`\nNOISY_CAMPAIGN_FAIL:\n  ${fails.join("\n  ")}`);
    process.exit(1);
  }
  console.log(`\nNOISY_CAMPAIGN_OK (victim p99 append ${solo.ap99?.toFixed(1)}->${loaded.ap99?.toFixed(1)}ms, read ${solo.rp99?.toFixed(1)}->${loaded.rp99?.toFixed(1)}ms, ${NOISY} noisy, ${IDLE} idle)`);
}

main().catch((e) => { console.error(e); process.exit(1); });
