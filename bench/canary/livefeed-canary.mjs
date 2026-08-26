#!/usr/bin/env node
// Round-11.6: the LiveFeed FIELD CANARY battery (livefeed-canary-rc1).
//
// Everything runs the RELEASE SHAPE: STREAMS_RELEASE_POSTURE=1, the
// pinned 1-GiB memory profile (deploy/profiles/compute-1g.env, applied
// verbatim so the battery certifies the profile that ships), workload
// fleet auth, enforce-mode customer auth from the platform emulator,
// and the livefeed engine — on a real three-instance fleet over one
// shared store.
//
// Canary geometries (Stage 8):
//   A: 1000 streams x 1 subscriber  (breadth)
//   B:  500 streams x 2 subscribers (shared-feed dedup at breadth)
//   C:   10 streams x 100 subscribers (fanout)
// Failure campaigns: owner movement at fanout, a blackholed (SIGSTOP)
// remote owner under active sessions, the WIDENED seal-publication
// window (STREAMS_CERT_SEALED_PUBLISH_DELAY_MS under
// STREAMS_CERTIFICATION_MODE=1) at fanout, cross-project retention
// pressure, and the largest LEGAL record including worst-case text
// framing (plus the one-over refusal).
//
// Produces target/livefeed-canary-manifest.json:
//   { commit, server_sha256, verdict, legs, reconciliation }.
import { spawn, execSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const EMU = 9870;
const S3 = EMU + 3;
const PORTS = { "streams-1": 9872, "streams-2": 9874, "streams-3": 9876 };
const BUCKET = `lfcanary-${Date.now()}`;
const KEY_B64 = Buffer.from(Array(32).fill(9)).toString("base64");
const DEBUG_TOKEN = "lfcanary-debug-token-0123456789";
// Exact-artifact mode (round 11.8 RC mint): point both binaries at
// verified release artifacts and pin the manifest commit when git is
// not resolvable in the sandbox (defaults = the local build/repo).
const SERVER_BIN = process.env.CANARY_SERVER_BIN ?? "./target/release/streams-slate";
const S3LITE_BIN = process.env.CANARY_S3LITE_BIN ?? "./target/release/s3lite";
const legs = {};
const reconciliation = {};
let failed = 0;
let holdActive = false;
const leg = (name, ok, extra = "") => {
  legs[name] = ok ? "PASS" : "FAIL";
  console.log(`${new Date().toISOString()} ${ok ? "ok  " : "FAIL"} ${name} ${ok ? "" : extra}`);
  if (!ok) failed++;
  if (!ok && process.env.CERT_HOLD === "1") {
    holdActive = true;
    console.log(`CERT_HOLD: fleet stays up for probing (ports 9872/9874/9876, emu 9870, s3 9873; tmp ${root})`);
    throw new Error("CERT_HOLD");
  }
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const root = mkdtempSync(join(tmpdir(), "lfcanary-"));
const dirA = join(root, "a");
const emu = spawn(process.execPath, [
  "platform-demo/src/emulator.mjs",
  "--port", String(EMU),
  "--cells", `cell-a=${dirA}`,
  "--fixture", "proj-lc:ws-lc:cell-a",
  "--fixture", "proj-noisy:ws-noisy:cell-a",
  "--enable-fault-api",
], { stdio: ["ignore", "inherit", "inherit"] });
const s3 = spawn(S3LITE_BIN, ["--listen", `127.0.0.1:${S3}`, "--latency-ms", "2"], { stdio: "ignore" });
await sleep(800);

// The PINNED 1-GiB profile, verbatim — the battery certifies the file
// that release campaigns source, not a copy of its values.
const profile = Object.fromEntries(
  readFileSync("deploy/profiles/compute-1g.env", "utf8")
    .split("\n")
    .filter((l) => l.trim() && !l.trim().startsWith("#"))
    .map((l) => l.split("=", 2)),
);

const instEnv = (name) => ({
  ...process.env,
  ...profile,
  STREAMS_RELEASE_POSTURE: "1",
  // The certification instrument (round-11.6): the widened seal
  // window is armed for the WHOLE battery; only the seal-herd leg
  // drives a seal, so nothing else observes it.
  STREAMS_CERTIFICATION_MODE: "1",
  STREAMS_CERT_SEALED_PUBLISH_DELAY_MS: "1500",
  STREAMS_AUTH_MODE: "enforce",
  STREAMS_AUTH_ISSUER: "https://auth.prisma.io",
  STREAMS_AUTH_KEYS_FILE: join(dirA, "keys.json"),
  STREAMS_AUTH_POLICY_FILE: join(dirA, "policies.json"),
  STREAMS_AUTH_GRANTS_FILE: join(dirA, "grants.json"),
  STREAMS_AUTH_REFRESH_SECS: "1",
  FLEET_AUTH_MODE: "workload",
  WORKLOAD_TOKEN_FILE: join(dirA, "workload.jwt"),
  CELL_ID: "cell-a",
  PROJECT_ID: "proj-lc",
  USAGE_STREAM_KEY: KEY_B64,
  AUTH_TOKEN: DEBUG_TOKEN,
  // Round-11.7: NO engine selection — the release binary defaults to
  // livefeed, and the teardown leg's zero-legacy-dispatches assertion
  // certifies the default engaged.
  FLEET_ALLOW_HTTP_PEERS: "1",
  SSE_HEARTBEAT_MS: "1000",
  INSTANCE_NAME: name,
  SELF_URL: `http://127.0.0.1:${PORTS[name]}`,
  FLEET_PREFIX: "fleet",
  FLEET_MAX: "3",
  FLEET_MIN: "3",
  REBALANCE_LAG_SECS: "1000000",
  REBALANCE_RETURN_SECS: "1000000",
  INITIAL_SHARDS: "8",
  SCALE_EVAL_SECS: "5",
  SCALE_RATE_WINDOW_SECS: "10",
  SCALE_HOT_PCT: "1",
  SCALE_HOT_EVALS: "1",
  SCALE_COOLDOWN_SECS: "5",
});
const args = (name) => [
  "--listen", `127.0.0.1:${PORTS[name]}`,
  "--s3-endpoint", `http://127.0.0.1:${S3}`,
  "--bucket", BUCKET,
  "--flush-interval-ms", "1", "--wal-flush-gap-ms", "2",
];
const procs = {};
mkdirSync("target/canary-logs", { recursive: true });
for (const n of Object.keys(PORTS)) {
  const log = openSync(`target/canary-logs/${n}.log`, "w");
  procs[n] = spawn(SERVER_BIN, args(n), { env: instEnv(n), stdio: ["ignore", log, log] });
}
const kill = () => {
  if (holdActive) return;
  for (const p of [...Object.values(procs), s3, emu]) try { p.kill("SIGKILL"); } catch {}
};
process.on("exit", kill);
await sleep(3000);

const base = (n) => `http://127.0.0.1:${PORTS[n]}`;
const sfetch = async (...a) => {
  try { return await fetch(...a); } catch { return { status: 0, ok: false, text: async () => "", json: async () => ({}), headers: new Map(), body: null }; }
};
const putStore = (path, body) =>
  sfetch(`http://127.0.0.1:${S3}/${BUCKET}/${path}`, { method: "PUT", body });
const debug = async (n) =>
  (await (await sfetch(`${base(n)}/v1/debug/load`, { headers: { authorization: `Bearer ${DEBUG_TOKEN}` } })).json());

// Customer tokens for both projects.
const mkCred = async (proj) => {
  const c = await (await sfetch(`http://127.0.0.1:${EMU}/v1/projects/${proj}/streams/credentials`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({
      displayName: `canary-${proj}`,
      scopes: ["streams.records.append", "streams.records.read", "streams.create",
        "streams.metadata.read", "streams.lifecycle.manage"],
    }),
  })).json();
  return c;
};
const tokenFor = async (cred) =>
  (await (await sfetch(`http://127.0.0.1:${EMU}/v1/token/streams`, {
    method: "POST", headers: { authorization: `StreamsCredential ${cred.secret}` },
  })).json()).accessToken;
const credLc = await mkCred("proj-lc");
const credNoisy = await mkCred("proj-noisy");
await sleep(1600); // feeds land
let TOK = await tokenFor(credLc);
let TOK_NOISY = await tokenFor(credNoisy);
// Customer tokens live 600 s; the battery outlives that. Refresh in
// the background exactly the way SDKs do.
const refresher = setInterval(async () => {
  try { TOK = await tokenFor(credLc); TOK_NOISY = await tokenFor(credNoisy); } catch {}
}, 240_000);
refresher.unref();
const H = (extra = {}) => ({
  authorization: `Bearer ${TOK}`,
  "prisma-encryption-key": KEY_B64,
  "content-type": "application/json",
  ...extra,
});
// Workload identity for the raw/observability surfaces. The emulator
// auto-rotates the signing key every 60 s, so mint FRESH per use.
const freshWl = async (ops) => (await (await sfetch(`http://127.0.0.1:${EMU}/admin/mint-workload`, {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ cell: "cell-a", operations: ops }),
})).json()).jwt;

const rfetch = async (url, opts) => {
  for (let i = 0; ; i++) {
    const r = await sfetch(url, opts);
    if ((r.status !== 503 && r.status !== 429) || i >= 15) return r;
    await sleep(Math.min(500 * (i + 1), 2000));
  }
};
const ownerFetch = async (path, opts) => {
  let last = null;
  for (let round = 0; round < 3; round++) {
    for (const n of Object.keys(PORTS)) {
      const r = await rfetch(`${base(n)}${path}`, opts);
      last = r;
      if (r.status !== 409 && r.status !== 421 && r.status !== 0) return r;
    }
    await sleep(1000);
  }
  return last;
};

for (let i = 0; i < 120; i++) {
  const ok = (await Promise.all(Object.keys(PORTS).map(async (n) =>
    (await sfetch(`${base(n)}/v1/debug/load`, { headers: { authorization: `Bearer ${DEBUG_TOKEN}` } })).status
  ))).every((s) => s === 200);
  if (ok) break;
  await sleep(500);
}
const boots = await Promise.all(Object.keys(PORTS).map(async (n) =>
  (await sfetch(`${base(n)}/v1/debug/load`, { headers: { authorization: `Bearer ${DEBUG_TOKEN}` } })).status));
leg("release-posture fleet boots with the pinned 1-GiB profile", boots.every((s) => s === 200),
  Object.keys(PORTS).map((n, i) => `${n}:${boots[i]}`).join(" "));
if (boots.some((s) => s !== 200)) finish();

// SSE collector (always drops its connection on return).
async function sseCollect(url, { headers = {}, ms = 15000, until = () => false } = {}) {
  const ctl = new AbortController();
  const out = { text: "", eof: false, status: 0, replayTo: null };
  const timer = setTimeout(() => ctl.abort(), ms);
  try {
    const r = await fetch(url, { headers: { ...H(), accept: "text/event-stream", ...headers }, signal: ctl.signal });
    out.status = r.status;
    out.replayTo = r.headers.get("streams-replay-to");
    if (!r.body) { out.eof = true; return out; }
    const reader = r.body.getReader();
    const dec = new TextDecoder();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) { out.eof = true; break; }
      out.text += dec.decode(value, { stream: true });
      if (until(out.text)) break;
    }
  } catch { /* abort/timeout: not EOF */ }
  clearTimeout(timer);
  ctl.abort();
  return out;
}
const count = (t, n) => t.split(n).length - 1;
const sseRetry = async (n, path, opts, deadlineMs = 60000) => {
  const t0 = Date.now();
  for (;;) {
    const r = await sseCollect(`${base(n)}${path}`, opts);
    const routing = r.status === 409 || r.status === 421 || r.status === 503;
    if (!routing || Date.now() - t0 > deadlineMs) return r;
    await sleep(1500);
  }
};
// Bounded-concurrency map.
async function pmap(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) {
      const k = i++;
      if (k >= items.length) return;
      out[k] = await fn(items[k], k);
    }
  });
  await Promise.all(workers);
  return out;
}
// Which instance serves a keyed subscription for `name` (the "gateway"
// step production performs); null when nobody does.
async function ownerOfStream(name, key = "k") {
  for (const n of Object.keys(PORTS)) {
    const r = await sseCollect(`${base(n)}/v1/streams/${name}/records:sse?routingKey=${key}&cursor=now`, { ms: 1200 });
    if (r.status === 200) return n;
  }
  return null;
}

// ---- Canary A: 1000 streams x 1 subscriber --------------------------
{
  console.log("== canary A: 1000 x 1 ==");
  const N = 1000;
  const names = Array.from({ length: N }, (_, i) => `ca${i}`);
  const created = await pmap(names, 32, async (nm) => {
    const c = await ownerFetch(`/v1/streams/${nm}`, {
      method: "PUT", headers: H(), body: JSON.stringify({ format: { kind: "json" } }),
    });
    if (![200, 201].includes(c.status)) return `create:${c.status}`;
    const a = await ownerFetch(`/v1/streams/${nm}/records`, {
      method: "POST", headers: H({ "prisma-routing-key": "k" }), body: JSON.stringify({ s: nm }),
    });
    return [200, 204].includes(a.status) ? null : `append:${a.status}`;
  });
  const createFails = created.filter(Boolean);
  leg("canary A setup: 1000 streams created + seeded", createFails.length === 0,
    `fails=${createFails.length} first=${createFails[0] ?? ""}`);
  // Every stream replays to upToDate with its seed, exactly once, via
  // whichever instance owns it (the router step).
  const owners = {};
  for (const n of Object.keys(PORTS)) owners[n] = 0;
  const replays = await pmap(names, 64, async (nm) => {
    for (const n of Object.keys(PORTS)) {
      const r = await sseCollect(`${base(n)}/v1/streams/${nm}/records:sse?routingKey=k`, {
        ms: 20000, until: (t) => t.includes('"upToDate":true'),
      });
      if (r.status === 200) {
        owners[n]++;
        return r.text.includes('"upToDate":true') && count(r.text, `"s":"${nm}"`) === 1 ? null : `bad:${nm}`;
      }
    }
    return `noowner:${nm}`;
  });
  const replayFails = replays.filter(Boolean);
  leg("canary A: 1000/1000 replays complete, honest upToDate, exactly-once", replayFails.length === 0,
    `fails=${replayFails.length} first=${replayFails[0] ?? ""}`);
  leg("canary A: streams spread across the whole fleet",
    Object.values(owners).every((c) => c > 100), JSON.stringify(owners));
}

// ---- Canary B: 500 streams x 2 subscribers (shared feeds) -----------
{
  console.log("== canary B: 500 x 2 ==");
  const N = 500;
  const names = Array.from({ length: N }, (_, i) => `cb${i}`);
  await pmap(names, 32, async (nm) => {
    await ownerFetch(`/v1/streams/${nm}`, {
      method: "PUT", headers: H(), body: JSON.stringify({ format: { kind: "json" } }),
    });
    await ownerFetch(`/v1/streams/${nm}/records`, {
      method: "POST", headers: H({ "prisma-routing-key": "k" }), body: JSON.stringify({ b: nm }),
    });
  });
  // Resolve each stream's owner once, then TWO subscribers each.
  const owner = {};
  await pmap(names, 32, async (nm) => { owner[nm] = await ownerOfStream(nm); });
  const unresolved = names.filter((nm) => !owner[nm]);
  leg("canary B setup: every stream has a serving owner", unresolved.length === 0,
    `unresolved=${unresolved.length}`);
  const pairs = await pmap(names, 50, async (nm) => {
    const one = () => sseCollect(`${base(owner[nm])}/v1/streams/${nm}/records:sse?routingKey=k`, {
      ms: 20000, until: (t) => t.includes('"upToDate":true'),
    });
    const [r1, r2] = await Promise.all([one(), one()]);
    const ok = (r) => r.status === 200 && count(r.text, `"b":"${nm}"`) === 1 && r.text.includes('"upToDate":true');
    return ok(r1) && ok(r2) ? null : `bad:${nm}:${r1.status}/${r2.status}`;
  });
  const pairFails = pairs.filter(Boolean);
  leg("canary B: 500 x 2 subscriber pairs complete independently", pairFails.length === 0,
    `fails=${pairFails.length} first=${pairFails[0] ?? ""}`);
}

// ---- Canary C: 10 streams x 100 subscribers (fanout) ----------------
{
  console.log("== canary C: 10 x 100 ==");
  const names = Array.from({ length: 10 }, (_, i) => `cc${i}`);
  await pmap(names, 10, async (nm) => {
    await ownerFetch(`/v1/streams/${nm}`, {
      method: "PUT", headers: H(), body: JSON.stringify({ format: { kind: "json" } }),
    });
    await ownerFetch(`/v1/streams/${nm}/records`, {
      method: "POST", headers: H({ "prisma-routing-key": "k" }), body: JSON.stringify({ c: nm }),
    });
  });
  const owner = {};
  for (const nm of names) owner[nm] = await ownerOfStream(nm);
  // 100 subscribers per stream hold open until the LIVE marker lands.
  const subs = names.flatMap((nm) => Array.from({ length: 100 }, () => nm));
  const results = pmap(subs, 250, async (nm) =>
    sseCollect(`${base(owner[nm])}/v1/streams/${nm}/records:sse?routingKey=k`, {
      ms: 45000, until: (t) => t.includes('"live":1'),
    }));
  await sleep(4000); // all 1000 attach and reach the tail
  const feedsAtFanout = (await Promise.all(Object.keys(PORTS).map(async (n) =>
    (await debug(n))?.sse_livefeed?.live_feeds ?? 0))).reduce((a, b) => a + b, 0);
  for (const nm of names) {
    await rfetch(`${base(owner[nm])}/v1/streams/${nm}/records`, {
      method: "POST", headers: H({ "prisma-routing-key": "k" }), body: JSON.stringify({ live: 1 }),
    });
  }
  const got = await results;
  const bad = got.filter((r) => !(r.status === 200 && r.text.includes('"live":1') && count(r.text, '"c":"') === 1));
  leg("canary C: 1000 fanout subscribers each get history + the live record", bad.length === 0,
    `bad=${bad.length} first=${bad[0]?.status} ${bad[0]?.text?.slice(-120) ?? ""}`);
  leg("canary C: one shared feed per stream at fanout (dedup)",
    feedsAtFanout <= 12, `feeds=${feedsAtFanout}`);
}

// ---- Failure campaign: owner movement at fanout ---------------------
const allPrefixes = ["000", "001", "010", "011", "100", "101", "110", "111"];
async function moveEverythingTo(target) {
  const entries = Object.fromEntries(allPrefixes.map((p) => [p, { to: target, ms: Date.now() }]));
  await putStore("fleet/fleet/overrides.json", JSON.stringify({ entries }));
  const t0 = Date.now();
  for (;;) {
    const views = await Promise.all(Object.keys(PORTS).map(async (n) => (await debug(n))?.ring ?? {}));
    if (views.every((v) => allPrefixes.every((p) => v?.overrides?.[p] === target))) break;
    if (Date.now() - t0 > 30000) { console.log("  [diag] override convergence TIMEOUT"); break; }
    await sleep(700);
  }
  await sleep(2500);
}
{
  console.log("== failure: owner movement at fanout ==");
  const nm = "cmove";
  await ownerFetch(`/v1/streams/${nm}`, {
    method: "PUT", headers: H(), body: JSON.stringify({ format: { kind: "json" } }),
  });
  await ownerFetch(`/v1/streams/${nm}/records`, {
    method: "POST", headers: H({ "prisma-routing-key": "k" }), body: JSON.stringify({ m: 0 }),
  });
  const before = await ownerOfStream(nm);
  const target = Object.keys(PORTS).find((n) => n !== before);
  // 100 established subscribers parked at the live tail of the owner.
  const subs = pmap(Array.from({ length: 100 }), 250, async () =>
    sseCollect(`${base(before)}/v1/streams/${nm}/records:sse?routingKey=k&cursor=now`, {
      ms: 45000, until: (t) => t.includes("NEVER"),
    }));
  await sleep(3000);
  const t0 = Date.now();
  await moveEverythingTo(target);
  const ended = await subs;
  const cutMs = Date.now() - t0;
  const badEnd = ended.filter((r) => !(r.status === 200 && r.eof && !r.text.includes('"sealed":true')));
  leg("movement at fanout: all 100 parked sessions end resumable, zero terminals, no traffic needed",
    badEnd.length === 0 && cutMs < 40000, `bad=${badEnd.length} cutMs=${cutMs}`);
  const back = await sseRetry(target, `/v1/streams/${nm}/records:sse?routingKey=k`, {
    ms: 25000, until: (t) => t.includes('"upToDate":true'),
  });
  leg("movement at fanout: reconnect at the new owner completes",
    back.status === 200 && count(back.text, '"m":0') === 1, `status=${back.status}`);
}

// ---- Failure campaign: blackholed remote owner under active herd ----
{
  console.log("== failure: blackholed remote owner ==");
  // A hot0-heavy split stream (the 11.4 recipe): sealed spans large
  // enough that unread sessions wedge INSIDE the lineage.
  const nm = "cblack";
  const c = await ownerFetch(`/v1/streams/${nm}`, {
    method: "PUT", headers: H(), body: JSON.stringify({ format: { kind: "json" } }),
  });
  leg("blackhole setup: stream created", [200, 201].includes(c.status), `status=${c.status}`);
  for (let i = 0; i < 3; i++) {
    await ownerFetch(`/v1/streams/${nm}/records`, {
      method: "POST", headers: H({ "prisma-routing-key": "hot0" }), body: JSON.stringify({ seed: i }),
    });
  }
  const hot = Array.from({ length: 32 }, (_, i) => `hot${i}`);
  // hot0 at ~11% of round bytes (the scaler still finds effective
  // split points) but sized so the SEALED hot0 lane reaches ~5 MB —
  // far past any client/socket buffering, so 25 unread sessions all
  // wedge INSIDE the sealed lineage.
  const PAD = "x".repeat(16384);
  const PADS = "y".repeat(4096);
  const deadline = Date.now() + 240_000;
  let seg = null;
  let rounds = 0;
  while (Date.now() < deadline) {
    await Promise.all(hot.map((k) =>
      ownerFetch(`/v1/streams/${nm}/records`, {
        method: "POST", headers: H({ "prisma-routing-key": k }), body: JSON.stringify({ k, r: rounds, pad: k === "hot0" ? PAD : PADS }),
      })));
    rounds++;
    if (rounds % 4 === 0) {
      const sr = await ownerFetch(`/v1/segments/${nm}`, { headers: { authorization: `Bearer ${await freshWl(["segment-read"])}` } });
      if (rounds % 40 === 0) console.log(`  [diag] split poll status=${sr.status} rounds=${rounds}`);
      if (sr.status === 200) {
        const m = await sr.json();
        const live = (m.segments ?? []).filter((x) => x.live !== false).length;
        const sealed = (m.segments ?? []).map((x) => x.sealed_next_offset ?? 0).reduce((a, b) => a + b, 0);
        if (live > 1 && !m.pending && sealed >= 10000) { seg = m; break; }
      }
    }
  }
  leg("blackhole setup: real scaler split with a wedgeable sealed lane", !!seg, `rounds=${rounds}`);
  const own = await ownerOfStream(nm, "hot0");
  const target = Object.keys(PORTS).find((n) => n !== own);
  // 25 wedged sessions (unread bodies) mid-lineage at the owner.
  const mkWedge = async () => {
    const r = await fetch(`${base(own)}/v1/streams/${nm}/records:sse?routingKey=hot0`, {
      headers: { ...H(), accept: "text/event-stream" } });
    const w = { status: r.status, text: "", eof: false, pumping: false };
    const reader = r.body?.getReader();
    const dec = new TextDecoder();
    w.pump = () => {
      if (w.pumping || !reader) return;
      w.pumping = true;
      (async () => {
        try {
          for (;;) {
            const { done, value } = await reader.read();
            if (done) { w.eof = true; return; }
            w.text += dec.decode(value, { stream: true });
          }
        } catch { w.eof = true; }
      })();
    };
    return w;
  };
  const wedges = await pmap(Array.from({ length: 25 }), 25, mkWedge);
  leg("blackhole: 25 sessions established at the owner", wedges.every((w) => w.status === 200),
    wedges.map((w) => w.status).join(","));
  await moveEverythingTo(target);
  procs[target].kill("SIGSTOP");
  for (const w of wedges) w.pump();
  await sleep(3000);
  const marks = wedges.map((w) => ({ ka: count(w.text, ": keep-alive"), recs: count(w.text, '"k":') }));
  await sleep(5000);
  // Violations: a terminal during the stall, or an OPEN session whose
  // keep-alives stopped (the heartbeat-suppression defect this leg
  // exists for). A session that already drained to its resumable EOF
  // (its lineage fit the buffers) is not a violation.
  const violations = wedges.filter((w, i) =>
    w.text.includes('"sealed":true')
    || (!w.eof && count(w.text, ": keep-alive") <= marks[i].ka));
  const stillOpen = wedges.filter((w) => !w.eof).length;
  leg("blackhole: keep-alives continue on every open session, zero terminals",
    violations.length === 0 && stillOpen >= 20,
    `violations=${violations.length} open=${stillOpen}/25`);
  procs[target].kill("SIGCONT");
  const t0 = Date.now();
  while (Date.now() - t0 < 45000 && !wedges.every((w) => w.eof)) await sleep(500);
  const badDrain = wedges.filter((w) => !w.eof || w.text.includes('"sealed":true'));
  leg("blackhole: after SIGCONT every session drains to a resumable EOF",
    badDrain.length === 0, `bad=${badDrain.length}`);
  const back = await sseRetry(target, `/v1/streams/${nm}/records:sse?routingKey=hot0`, {
    ms: 30000, until: (t) => t.includes('"upToDate":true'),
  });
  leg("blackhole: full replay at the restored owner completes",
    back.status === 200 && count(back.text, '"seed":0') === 1, `status=${back.status}`);
}

// ---- Failure campaign: widened seal-publication window at fanout ----
{
  console.log("== failure: seal-publication delay herd ==");
  const nm = "cseal";
  await ownerFetch(`/v1/streams/${nm}`, {
    method: "PUT", headers: H(), body: JSON.stringify({ format: { kind: "json" } }),
  });
  await ownerFetch(`/v1/streams/${nm}/records`, {
    method: "POST", headers: H({ "prisma-routing-key": "k" }), body: JSON.stringify({ z: 0 }),
  });
  const own = await ownerOfStream(nm);
  const subs = pmap(Array.from({ length: 100 }), 250, async () =>
    sseCollect(`${base(own)}/v1/streams/${nm}/records:sse?routingKey=k`, {
      ms: 60000, until: (t) => t.includes('"sealed":true'),
    }));
  await sleep(3000);
  const t0 = Date.now();
  let sealSt = 0;
  for (let i = 0; i < 20; i++) {
    const r = await rfetch(`${base(own)}/v1/streams/${nm}:seal`, { method: "POST", headers: H(), body: "{}" });
    sealSt = r.status;
    if (r.status === 200 || r.status === 204) break;
    await sleep(1000);
  }
  const sealMs = Date.now() - t0;
  const ended = await subs;
  const bad = ended.filter((r) => !(r.status === 200 && r.text.includes('"sealed":true')
    && count(r.text, '"sealed":true') === 1 && count(r.text, '"z":0') === 1));
  if (bad.length) console.log(`  [diag] seal-herd sample status=${bad[0].status} eof=${bad[0].eof} text=${JSON.stringify(bad[0].text.slice(-500))}`);
  leg("seal herd: the widened window holds and all 100 subscribers converge to ONE terminal",
    sealSt < 300 && sealMs >= 1500 && bad.length === 0,
    `seal=${sealSt} sealMs=${sealMs} bad=${bad.length}`);
}

// ---- Failure campaign: cross-project retention pressure -------------
{
  console.log("== failure: cross-project retention pressure ==");
  const HN = (extra = {}) => ({
    authorization: `Bearer ${TOK_NOISY}`, "prisma-encryption-key": KEY_B64,
    "content-type": "application/json", ...extra,
  });
  await ownerFetch(`/v1/streams/victim`, {
    method: "PUT", headers: H(), body: JSON.stringify({ format: { kind: "json" } }),
  });
  const nc = await ownerFetch(`/v1/streams/noisy`, {
    method: "PUT", headers: HN(), body: JSON.stringify({ format: { kind: "json" } }),
  });
  leg("cross-project setup: noisy project stream created", [200, 201].includes(nc.status), `status=${nc.status}`);
  await ownerFetch(`/v1/streams/victim/records`, {
    method: "POST", headers: H({ "prisma-routing-key": "k" }), body: JSON.stringify({ v: 0 }),
  });
  const vown = await ownerOfStream("victim");
  const victim = sseCollect(`${base(vown)}/v1/streams/victim/records:sse?routingKey=k`, {
    ms: 40000, until: (t) => t.includes('"v":9'),
  });
  // The noisy project blasts 64 KiB records THROUGH ITS OWN FEED (a
  // subscriber pins the feed so retention is exercised) while the
  // victim's records trickle.
  const noisyPad = "n".repeat(65536);
  const noisySub = sseCollect(`${base(await ownerOfStream("noisy") ?? vown)}/v1/streams/noisy/records:sse`, {
    headers: { authorization: `Bearer ${TOK_NOISY}` }, ms: 30000, until: () => false,
  });
  const blast = pmap(Array.from({ length: 120 }), 8, async (_, i) =>
    ownerFetch(`/v1/streams/noisy/records`, {
      method: "POST", headers: HN({ "prisma-routing-key": `n${i % 4}` }), body: JSON.stringify({ i, pad: noisyPad }),
    }));
  for (let i = 1; i < 10; i++) {
    await ownerFetch(`/v1/streams/victim/records`, {
      method: "POST", headers: H({ "prisma-routing-key": "k" }), body: JSON.stringify({ v: i }),
    });
    await sleep(200);
  }
  await blast;
  const vr = await victim;
  leg("cross-project pressure: the victim project's delivery is uninterrupted",
    vr.status === 200 && vr.text.includes('"v":0') && vr.text.includes('"v":9') && !vr.eof,
    `status=${vr.status} eof=${vr.eof} tail=${vr.text.slice(-150)}`);
  noisySub.then(() => {});
  const rows = (await Promise.all(Object.keys(PORTS).map(async (n) =>
    (await debug(n))?.sse_livefeed?.project_retention ?? []))).flat();
  leg("cross-project pressure: per-project retention rows stay bounded",
    rows.length <= 6, `rows=${rows.length}`);
}

// ---- Failure campaign: largest LEGAL record (worst text framing) ----
{
  console.log("== failure: largest legal record ==");
  const nm = "cbig";
  // The PRODUCT surface canonicalizes JSON — raw whitespace never
  // reaches storage — so worst-case text framing (a payload of 0x0A
  // bytes, 6 output bytes per input byte) is a RAW-surface property:
  // create + append through the raw adapter under workload identity,
  // then replay through the livefeed product SSE (default lane).
  const rawH = async (extra = {}) => ({
    authorization: `Bearer ${await freshWl(["raw-lifecycle", "raw-append", "raw-read"])}`,
    "stream-encryption-key": KEY_B64,
    ...extra,
  });
  const cr = await ownerFetch(`/v1/stream/${nm}`, {
    method: "PUT", headers: await rawH({ "content-type": "text/plain" }), body: "",
  });
  leg("raw text stream created for the framing probe", [200, 201].includes(cr.status),
    `status=${cr.status} body=${((await cr.text?.().catch(() => "")) ?? "").slice(0, 120)}`);
  const okBig = await ownerFetch(`/v1/stream/${nm}`, {
    method: "POST", headers: await rawH({ "content-type": "text/plain" }), body: Buffer.alloc(131072, 0x0a),
  });
  leg("largest legal record (128 KiB of raw newlines) is accepted",
    [200, 204].includes(okBig.status),
    `status=${okBig.status} body=${((await okBig.text?.().catch(() => "")) ?? "").slice(0, 120)}`);
  const over = await ownerFetch(`/v1/stream/${nm}`, {
    method: "POST", headers: await rawH({ "content-type": "text/plain" }), body: Buffer.alloc(131073, 0x0a),
  });
  leg("one byte over the ceiling is refused (413)",
    over.status === 413, `status=${over.status}`);
  // The worst frame must ride the livefeed ring end to end with NO
  // lag disconnect (release geometry: 6x ceiling + framing <= ring).
  const own = await ownerOfStream(nm, "");
  const r = await sseRetry(own ?? "streams-1", `/v1/streams/${nm}/records:sse`, {
    ms: 30000, until: (t) => t.includes('"upToDate":true'),
  });
  leg("the worst-case frame serves through the livefeed ring to upToDate",
    r.status === 200 && r.text.includes('"upToDate":true') && r.text.length > 131072,
    `status=${r.status} len=${r.text.length}`);
}

// ---- Teardown accounting -------------------------------------------
{
  for (let i = 0; i < 120; i++) {
    const feeds = (await Promise.all(Object.keys(PORTS).map(async (n) =>
      (await debug(n))?.sse_livefeed?.live_feeds ?? 1))).reduce((a, b) => a + b, 0);
    if (feeds === 0) break;
    await sleep(500);
  }
  let ok = true;
  let detail = "";
  for (const n of Object.keys(PORTS)) {
    const d = await debug(n);
    const lf = d?.sse_livefeed ?? {};
    reconciliation[n] = {
      live_feeds: lf.live_feeds ?? null,
      reserved_bytes: lf.reserved_bytes ?? null,
      legacy_engine_fields_absent: d?.legacy_sse_dispatches === undefined,
      cutoff_wrong_owner: lf.cutoff_wrong_owner ?? 0,
      cutoff_fleet_auth: lf.cutoff_fleet_auth ?? 0,
      cutoff_incarnation: lf.cutoff_incarnation ?? 0,
      cutoff_incompatible: lf.cutoff_incompatible ?? 0,
      cutoff_target_mismatch: lf.cutoff_target_mismatch ?? 0,
      cutoff_redirect_loop: lf.cutoff_redirect_loop ?? 0,
      scaler: d?.scaler ?? null,
    };
    // Round-11.8: the legacy engine is DELETED — the counter's very
    // field must be gone from the debug surface.
    const legacy = d?.legacy_sse_dispatches === undefined ? 0 : 1;
    const feeds = lf.live_feeds ?? -1;
    const reserved = lf.reserved_bytes ?? -1;
    if (legacy !== 0) { ok = false; detail += `${n}:legacy=${legacy} `; }
    if (feeds !== 0) { ok = false; detail += `${n}:feeds=${feeds} `; }
    if (reserved !== 0) { ok = false; detail += `${n}:reserved=${reserved} `; }
    const cut = ["cutoff_incarnation", "cutoff_incompatible", "cutoff_target_mismatch", "cutoff_redirect_loop"]
      .map((k) => lf[k] ?? 0).reduce((a, b) => a + b, 0);
    if (cut !== 0) { ok = false; detail += `${n}:unexpected_cutoffs=${cut} `; }
    // The blackhole herd's 25 in-flight remote pages may each age out
    // their workload token across the freeze: typed FleetAuth cutoffs,
    // resumable, recovered (the restored-owner replay leg proves it).
    if ((lf.cutoff_fleet_auth ?? 0) > 25) { ok = false; detail += `${n}:fleet_auth=${lf.cutoff_fleet_auth} `; }
  }
  leg("teardown: zero residual feeds/bytes, zero legacy dispatches, zero unclassified cutoffs", ok, detail);
}

function finish() {
  kill();
  const sha = createHash("sha256").update(readFileSync(SERVER_BIN)).digest("hex");
  const manifest = {
    commit: process.env.CANARY_COMMIT ?? execSync("git rev-parse HEAD").toString().trim(),
    server_sha256: sha,
    verdict: failed === 0 ? "PASS" : "FAIL",
    legs,
    reconciliation,
  };
  writeFileSync("target/livefeed-canary-manifest.json", JSON.stringify(manifest, null, 2));
  console.log(`LIVEFEED_CANARY_${failed === 0 ? "OK" : "FAIL"} server=sha256:${sha}`);
  process.exit(failed === 0 ? 0 : 1);
}
finish();
