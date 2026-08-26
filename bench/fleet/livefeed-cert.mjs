#!/usr/bin/env node
// Round-11.4: the REAL two-process (three-instance) LiveFeed fleet
// certification battery. Everything is real: release binaries, TCP
// listeners, one shared s3lite store, fleet heartbeats + a shared
// fleet/overrides.json steering ownership, enforce-mode auth feeds
// from the platform emulator, per-instance rotating workload JWTs,
// and a REAL scaler split forced by load (the field-gate recipe).
//
// Legs marked COVERED_INPROC are protocol cases that need staged
// state divergence a shared overrides file cannot express (the
// redirect LOOP) — the in-process three-instance suite certifies
// them deterministically; this battery certifies the real process
// boundary for everything else.
import { spawn, execSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const EMU = 9860;
const S3 = EMU + 3;
const PORTS = { "streams-1": 9862, "streams-2": 9864, "streams-3": 9866 };
const BUCKET = `lfcert-${Date.now()}`;
const KEY_B64 = Buffer.from(Array(32).fill(9)).toString("base64");
const DEBUG_TOKEN = "lfcert-debug-token-0123456789";
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
    console.log(`CERT_HOLD: fleet stays up for probing (ports 9862/9864/9866, emu 9860, s3 9863; tmp ${root})`);
    throw new Error("CERT_HOLD");
  }
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const root = mkdtempSync(join(tmpdir(), "lfcert-"));
const dirA = join(root, "a");
const emu = spawn(process.execPath, [
  "platform-demo/src/emulator.mjs",
  "--port", String(EMU),
  "--cells", `cell-a=${dirA}`,
  "--fixture", "proj-lc:ws-lc:cell-a",
  "--enable-fault-api",
], { stdio: ["ignore", "inherit", "inherit"] });
const s3 = spawn("./target/release/s3lite", ["--listen", `127.0.0.1:${S3}`, "--latency-ms", "2"], { stdio: "ignore" });
await sleep(800);

// ONE logical cell across three fleet instances: every instance reads
// the same auth feed files and the same rotating workload JWT.
const instEnv = (name) => ({
  ...process.env,
  STREAMS_AUTH_MODE: "enforce",
  STREAMS_AUTH_ISSUER: "https://auth.prisma.io",
  STREAMS_AUTH_KEYS_FILE: join(dirA, "keys.json"),
  STREAMS_AUTH_POLICY_FILE: join(dirA, "policies.json"),
  STREAMS_AUTH_GRANTS_FILE: join(dirA, "grants.json"),
  STREAMS_AUTH_REFRESH_SECS: "1",
  FLEET_AUTH_MODE: "workload",
  WORKLOAD_TOKEN_FILE: join(dirA, "workload.jwt"),
  CELL_ID: "cell-a",
  // Single-project cell: the deployment tenant IS the certification
  // project, so the raw observability surface (/v1/segments) resolves
  // product streams directly.
  PROJECT_ID: "proj-lc",
  USAGE_STREAM_KEY: KEY_B64,
  AUTH_TOKEN: DEBUG_TOKEN,
  STREAMS_SSE_ENGINE: "livefeed",
  // Loopback peers are rejected by default (production wants https);
  // the certification fleet runs on 127.0.0.1.
  FLEET_ALLOW_HTTP_PEERS: "1",
  MAX_RECORD_PAYLOAD_BYTES: "131072",
  SSE_HEARTBEAT_MS: "500",
  // Small feed ring => small driver pages (read_cap = 2/3 ring): the
  // wedge legs need server-side buffering (4-chunk channel x page) to
  // stay well under the hot0 lane's sealed volume.
  SSE_FEED_RING_BYTES: "65536",
  INSTANCE_NAME: name,
  SELF_URL: `http://127.0.0.1:${PORTS[name]}`,
  FLEET_PREFIX: "fleet",
  FLEET_MAX: "3",
  // The ring's ACTIVE set follows fleet/desired.json, and the instance
  // autoscaler keeps desired=1 at certification load — pin the floor so
  // all three ordinals are ring members and overrides can target them.
  FLEET_MIN: "3",
  // The battery steers ownership through overrides.json; the
  // auto-rebalancer must not fight it (local absorb lag under blast
  // load triggers its own moves), and nothing may age-return the
  // steered entries mid-battery.
  REBALANCE_LAG_SECS: "1000000",
  REBALANCE_RETURN_SECS: "1000000",
  INITIAL_SHARDS: "8",
  // The field-gate recipe: a hot key burst forces a REAL scaler split
  // in seconds.
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
  "--max-unflushed-bytes", "67108864",
  "--flush-interval-ms", "1", "--wal-flush-gap-ms", "2",
];
const procs = {};
mkdirSync("target/cert-logs", { recursive: true });
for (const n of Object.keys(PORTS)) {
  const log = openSync(`target/cert-logs/${n}.log`, "w");
  procs[n] = spawn("./target/release/streams-slate", args(n), { env: instEnv(n), stdio: ["ignore", log, log] });
}
const kill = () => {
  if (holdActive) return;
  for (const p of [...Object.values(procs), s3, emu]) try { p.kill("SIGKILL"); } catch {}
};
process.on("exit", kill);
await sleep(3000);
for (const n of Object.keys(PORTS)) {
  leg(`boot ${n} (livefeed + enforce + workload)`, procs[n].exitCode === null);
}

const base = (n) => `http://127.0.0.1:${PORTS[n]}`;
const sfetch = async (...a) => {
  try { return await fetch(...a); } catch { return { status: 0, ok: false, text: async () => "", json: async () => ({}), headers: new Map(), body: null }; }
};
// Store I/O for fleet control documents (s3lite speaks plain S3).
const putStore = (path, body) =>
  sfetch(`http://127.0.0.1:${S3}/${BUCKET}/${path}`, { method: "PUT", body });
const debug = async (n) =>
  (await (await sfetch(`${base(n)}/v1/debug/load`, { headers: { authorization: `Bearer ${DEBUG_TOKEN}` } })).json());

// Mint a customer token.
const cred = await (await sfetch(`http://127.0.0.1:${EMU}/v1/projects/proj-lc/streams/credentials`, {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ displayName: "cert" }),
})).json();
const tokenOf = async () =>
  (await (await sfetch(`http://127.0.0.1:${EMU}/v1/token/streams`, {
    method: "POST", headers: { authorization: `StreamsCredential ${cred.secret}` },
  })).json()).accessToken;
await sleep(1500); // feeds land
let TOK = await tokenOf();
// Observability probes (/v1/segments) live on the internal surface:
// workload identity with the exact operation, never the customer token.
const wlSeg = await (await sfetch(`http://127.0.0.1:${EMU}/admin/mint-workload`, {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ cell: "cell-a", operations: ["segment-read"] }),
})).json();
console.log(`  [diag] mint-workload keys=${Object.keys(wlSeg)} jwt=${String(wlSeg.jwt).slice(0, 24)}...`);
const SEG_AUTH = { authorization: `Bearer ${wlSeg.jwt}` };
// Product streams surface on the raw observability vocabulary as
// {project}/{name}.
const segPath = (name) => `/v1/segments/${name}`;
const H = (extra = {}) => ({
  authorization: `Bearer ${TOK}`,
  "prisma-encryption-key": KEY_B64,
  "content-type": "application/json",
  ...extra,
});
const rfetch = async (url, opts) => {
  for (let i = 0; ; i++) {
    const r = await sfetch(url, opts);
    if ((r.status !== 503 && r.status !== 429) || i >= 15) return r;
    await sleep(Math.min(500 * (i + 1), 2000));
  }
};
// Owner-following request: the fleet routes by shard, so a request
// landing on a non-owner answers 409/421 — try each instance and
// return the owner's answer (production has a gateway for this).
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

// Wait for fleet convergence: every instance serves basic requests.
for (let i = 0; i < 60; i++) {
  const ok = (await Promise.all(Object.keys(PORTS).map(async (n) =>
    (await sfetch(`${base(n)}/v1/debug/load`, { headers: { authorization: `Bearer ${DEBUG_TOKEN}` } })).status
  ))).every((s) => s === 200);
  if (ok) break;
  await sleep(500);
}

// SSE collector over fetch streaming.
async function sseCollect(url, { headers = {}, ms = 10000, until = () => false } = {}) {
  const ctl = new AbortController();
  const out = { text: "", eof: false, status: 0 };
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
  // Always drop the connection: a collector that returns on `until`
  // without aborting keeps its subscriber slot alive and the feed
  // never tears down (the teardown leg's phantom residual).
  ctl.abort();
  return out;
}
const count = (t, n) => t.split(n).length - 1;

// ---- Build the split topology with a REAL scaler split --------------
// Create through streams-1; blast one hot key until the scaler splits.
async function makeSplitStream(name) {
  const c = await ownerFetch(`/v1/streams/${name}`, {
    method: "PUT", headers: H(), body: JSON.stringify({ format: { kind: "json" } }),
  });
  if (![200, 201].includes(c.status)) {
    console.log(`  [diag] create status=${c.status} body=${await c.text().catch(() => "")}`);
    return null;
  }
  // The certification LANE: distinct seed markers on hot0.
  for (let i = 0; i < 3; i++) {
    const r = await ownerFetch(`/v1/streams/${name}/records`, {
      method: "POST", headers: H({ "prisma-routing-key": "hot0" }), body: JSON.stringify({ seed: i }),
    });
    if (![200, 204].includes(r.status)) {
      console.log(`  [diag] seed ${i} status=${r.status} body=${await r.text().catch(() => "")}`);
      return null;
    }
  }
  // A split partitions the KEYSPACE: the load must ride many distinct
  // keys (the field-gate recipe — one hot key is an ineffective split
  // the scaler rightly avoids).
  const hot = Array.from({ length: 32 }, (_, i) => `hot${i}`);
  // The wedge legs hold a session INSIDE its sealed spans by not
  // reading the response: the hot0 LANE's sealed volume must dwarf
  // socket+fetch buffers (~1 MB). hot0 records carry a 24 KiB pad;
  // the other 31 keys stay small (they only drive the splits).
  // hot0 at ~11% of round bytes: heavy enough to out-size the wedge
  // buffers once ~6000 records seal, light enough that the scaler
  // still finds EFFECTIVE split points (a 60%-dominant key made every
  // candidate split ineffective and none fired).
  const PAD = "x".repeat(8192);
  const PADS = "y".repeat(2048);
  const deadline = Date.now() + 240_000;
  let rounds = 0;
  while (Date.now() < deadline) {
    await Promise.all(hot.map((k) =>
      ownerFetch(`/v1/streams/${name}/records`, {
        method: "POST", headers: H({ "prisma-routing-key": k }), body: JSON.stringify({ k, r: rounds, pad: k === "hot0" ? PAD : PADS }),
      })));
    rounds++;
    if (rounds % 4 === 0) {
      for (const n of Object.keys(PORTS)) {
        try {
          const d = await debug(n);
          console.log(`  [diag ${n}] scaler=${JSON.stringify(d.scaler)}`);
        } catch { /* booting */ }
      }
      const sr = await ownerFetch(segPath(name), { headers: SEG_AUTH });
      console.log(`  [diag] segments status=${sr.status} body=${(await sr.clone?.().text?.().catch(() => "") ?? "")}`);
      if (sr.status === 200) {
        const m = await sr.json();
        const liveCount = (m.segments ?? []).filter((x) => x.live !== false).length;
        const sealedRecords = (m.segments ?? [])
          .map((x) => x.sealed_next_offset ?? 0).reduce((a, b) => a + b, 0);
        console.log(`  [diag] split poll live=${liveCount} sealed=${sealedRecords} pending=${!!m.pending}`);
        if (liveCount > 1 && !m.pending && sealedRecords >= 6000) return m;
      }
    }
  }
  return null;
}

// Which instance OWNS a segment: probe each with a direct raw read of
// the internal segment surface — only the owner answers 200.
async function ownerOf(name, segId) {
  for (const n of Object.keys(PORTS)) {
    const d = await debug(n);
    if (!d) continue;
  }
  // Behavioral probe: the records:sse dispatch on a non-owner of the
  // LIVE tail answers 409; the owner serves. For sealed parents the
  // internal probe is indirect — use the append path for the live
  // segment and the reroute answer for the rest.
  return null;
}

console.log("== forcing a real scaler split ==");
const segs = await makeSplitStream("certsplit");
leg("real scaler split lands (multi-segment, nothing pending)", !!segs, JSON.stringify(segs ?? {}));
if (!segs) { finish(); }

// Identify the live child (contains the hot key) and the sealed parent.
const live = segs.segments.filter((s) => s.live);
const sealedParent = segs.segments.find((s) => !s.live) ?? segs.segments[0];
leg("split produced a sealed parent + live successor(s)", live.length >= 1 && !!sealedParent);

// ---- Helpers for the movement/blackhole legs ------------------------
// Production read contract: not_stream_owner is retryable through the
// router. Bounded retry at one instance until the verdict is terminal.
const sseRetry = async (n, path, opts, deadlineMs = 60000) => {
  const t0 = Date.now();
  for (;;) {
    const r = await sseCollect(`${base(n)}${path}`, opts);
    const routing = r.status === 409 || r.status === 421 || r.status === 503;
    if (routing) console.log(`  [diag retry ${n}${path.slice(0, 40)}] status=${r.status} replayTo=${r.replayTo} body=${r.text.slice(0, 120)}`);
    if (!routing || Date.now() - t0 > deadlineMs) return r;
    await sleep(1500);
  }
};
// Open an SSE subscription and deliberately do NOT read the body: the
// socket and fetch buffers fill, and the session wedges mid-lineage —
// the only way to hold a REAL fleet session inside its sealed spans
// while ownership moves underneath it. pump() releases it.
const mkWedge = async (url) => {
  let r;
  const t0 = Date.now();
  for (;;) {
    r = await fetch(url, { headers: { ...H(), accept: "text/event-stream" } });
    if (r.status === 200 || Date.now() - t0 > 60000) break;
    await r.body?.cancel?.().catch(() => {});
    await sleep(1500);
  }
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
const waitFor = async (pred, ms) => {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) {
    if (pred()) return true;
    await sleep(250);
  }
  return pred();
};

// ---- Leg group A: replay via EVERY instance (initial ownership) -----
// Exactly one instance owns the live tail and serves the full lineage;
// the others answer the TYPED routing refusal carrying the owner name
// (round-11.4 field finding: without Streams-Replay-To the product
// translator renders the bounce as non-retryable cursor_beyond_tail).
let initialOwner = null;
{
  const results = {};
  for (const n of Object.keys(PORTS)) {
    // Boot-window churn can answer a transient 503; the verdict under
    // test is the TERMINAL one (200 serve or 409 reroute).
    for (let i = 0; i < 20; i++) {
      results[n] = await sseCollect(`${base(n)}/v1/streams/certsplit/records:sse?routingKey=hot0`, {
        ms: 20000, until: (t) => t.includes('"upToDate":true'),
      });
      if (results[n].status !== 503) break;
      await sleep(1000);
    }
  }
  const owners = Object.keys(PORTS).filter((n) => results[n].status === 200);
  initialOwner = owners[0] ?? null;
  leg("exactly one instance serves the keyed replay", owners.length === 1,
    Object.entries(results).map(([n, r]) => `${n}:${r.status}`).join(" "));
  if (initialOwner) {
    const r = results[initialOwner];
    const utd = r.text.includes('"upToDate":true');
    const seeds = count(r.text, '"seed":0');
    leg(`replay via the owner (${initialOwner}): full lineage, honest upToDate, exactly-once seed`,
      utd && seeds === 1, `utd=${utd} seed0x${seeds}`);
    for (const n of Object.keys(PORTS)) {
      if (n === initialOwner) continue;
      const nr = results[n];
      leg(`replay via ${n}: typed routing refusal names the owner`,
        nr.status === 409 && nr.replayTo === initialOwner && !nr.text.includes("cursor_beyond_tail"),
        `status=${nr.status} replayTo=${nr.replayTo} body=${nr.text.slice(0, 160)}`);
    }
  }
}
if (!initialOwner) finish();

// now-cursor at the owner: no history, and the verdict is a real 200.
{
  const probe = await sseCollect(`${base(initialOwner)}/v1/streams/certsplit/records:sse?routingKey=hot0&cursor=now`, {
    ms: 4000, until: (t) => t.includes('"upToDate":true'),
  });
  leg("cursor=now at the owner: 200, no history",
    probe.status === 200 && !probe.text.includes('"seed":'), `status=${probe.status}`);
}

// ---- Leg group B: ownership movement --------------------------------
// The shared overrides file is the production steering surface; both
// moves must converge without data loss for a fresh replay.
const allPrefixes = ["000", "001", "010", "011", "100", "101", "110", "111"];
async function moveEverythingTo(owner) {
  const entries = Object.fromEntries(
    allPrefixes.map((p) => [p, { to: owner, ms: Date.now() }]),
  );
  // The fleet store is a PrefixStore rooted at FLEET_PREFIX — the
  // real object is {prefix}/fleet/overrides.json in the bucket.
  const put = await putStore("fleet/fleet/overrides.json", JSON.stringify({ entries }));
  if (![200, 201].includes(put.status)) console.log(`  [diag] overrides PUT status=${put.status}`);
  // Wait for REAL convergence: every instance's mirror maps every
  // prefix to the target (each also fences/opens eagerly on its tick).
  const t0 = Date.now();
  for (;;) {
    const views = await Promise.all(Object.keys(PORTS).map(async (n) => (await debug(n))?.ring ?? {}));
    const ok = views.every((v) => allPrefixes.every((p) => v?.overrides?.[p] === owner));
    if (ok) break;
    if (Date.now() - t0 > 30000) {
      console.log(`  [diag] override convergence TIMEOUT to=${owner} views=${JSON.stringify(views)}`);
      const raw = await (await sfetch(`http://127.0.0.1:${S3}/${BUCKET}/fleet/fleet/overrides.json`)).text();
      console.log(`  [diag] store overrides=${raw.slice(0, 400)}`);
      break;
    }
    await sleep(700);
  }
  await sleep(2500); // eager-handoff fences settle
}
let fullReplayRecords = 0;
{
  await moveEverythingTo("streams-2");
  const r = await sseRetry("streams-2", `/v1/streams/certsplit/records:sse?routingKey=hot0`, {
    ms: 30000, until: (t) => t.includes('"upToDate":true'),
  });
  fullReplayRecords = count(r.text, '"k":') + count(r.text, '"seed":');
  leg("after moving ownership to streams-2, a fresh keyed replay is complete there",
    r.status === 200 && r.text.includes('"upToDate":true') && count(r.text, '"seed":0') === 1,
    `status=${r.status} replayTo=${r.replayTo} records=${fullReplayRecords} tail=${r.text.slice(-200)}`);
}
{
  // Established subscriber AT the owner; move everything away; a
  // straggler request at the old owner forces the possession-yield
  // (production: routers converge, stragglers still arrive). The
  // established session must end with a RESUMABLE EOF — no terminal.
  const sub = sseCollect(`${base("streams-2")}/v1/streams/certsplit/records:sse?routingKey=hot0&cursor=now`, {
    ms: 40000, until: (t) => t.includes("NEVER"),
  });
  await sleep(2000);
  const t0 = Date.now();
  await moveEverythingTo("streams-3");
  await ownerFetch(`/v1/streams/certsplit/records`, {
    method: "POST", headers: H({ "prisma-routing-key": "hot0" }), body: JSON.stringify({ moved: 1 }),
  });
  const ended = await sub;
  const cutMs = Date.now() - t0;
  leg("moved live tail: the established session ends WITHOUT a terminal (tick-yield wake, no traffic at the loser)",
    ended.status === 200 && ended.eof && cutMs < 30000 && !ended.text.includes('"sealed":true') && !ended.text.includes('"moved":1'),
    `status=${ended.status} eof=${ended.eof} cutMs=${cutMs} tail=${ended.text.slice(-200)}`);
  const rec = await sseRetry("streams-3", `/v1/streams/certsplit/records:sse?routingKey=hot0`, {
    ms: 25000, until: (t) => t.includes('"moved":1'),
  });
  leg("reconnect at the new owner serves the full lineage + the new record",
    rec.status === 200 && count(rec.text, '"seed":0') === 1 && rec.text.includes('"moved":1'),
    `status=${rec.status} replayTo=${rec.replayTo}`);
}

// ---- Leg group C: blackholed REMOTE owner under an active session ---
// A session established at the owner wedges mid-lineage (unread body);
// ownership moves away; the sealed spans continue REMOTELY from the
// new owner. SIGSTOP freezes that owner — TCP accepted, nothing ever
// answers — and the body-owned keep-alives must keep flowing with no
// terminal. SIGCONT: the remote pages resume, the session drains its
// sealed spans and ends at the moved live tail with a resumable EOF.
{
  const w = await mkWedge(`${base("streams-3")}/v1/streams/certsplit/records:sse?routingKey=hot0`);
  leg("wedge session established at the owner", w.status === 200, `status=${w.status}`);
  await moveEverythingTo("streams-1");
  procs["streams-1"].kill("SIGSTOP");
  w.pump();
  // Let the buffered prefix drain, then observe the stall window.
  await waitFor(() => false, 3000);
  const mark = { len: w.text.length, ka: count(w.text, ": keep-alive"), recs: count(w.text, '"k":') };
  await waitFor(() => false, 5000);
  const ka2 = count(w.text, ": keep-alive");
  const recs2 = count(w.text, '"k":');
  leg("blackholed remote owner: body-owned keep-alives continue, no data, no terminal",
    !w.eof && ka2 > mark.ka && recs2 === mark.recs && !w.text.includes('"sealed":true'),
    `eof=${w.eof} ka=${mark.ka}->${ka2} recs=${mark.recs}->${recs2}`);
  procs["streams-1"].kill("SIGCONT");
  const drained = await waitFor(() => w.eof, 30000);
  const total = count(w.text, '"k":') + count(w.text, '"seed":');
  leg("recovery: the remote pages complete and the session ends resumable",
    drained && !w.text.includes('"sealed":true') && total > mark.recs,
    `eof=${w.eof} records=${total} tail=${w.text.slice(-200)}`);
  const rec = await sseRetry("streams-1", `/v1/streams/certsplit/records:sse?routingKey=hot0`, {
    ms: 30000, until: (t) => t.includes('"upToDate":true'),
  });
  leg("full replay at the restored owner is complete",
    rec.status === 200 && count(rec.text, '"seed":0') === 1 && rec.text.includes('"upToDate":true'),
    `status=${rec.status} replayTo=${rec.replayTo}`);
}

// ---- Leg group D: workload rotation under REMOTE pages --------------
// The wedged session's sealed spans go remote after the move; the
// workload JWT rotates BEFORE the drain — every remote page is served
// under post-rotation fleet identity.
{
  const w = await mkWedge(`${base("streams-1")}/v1/streams/certsplit/records:sse?routingKey=hot0`);
  leg("rotation wedge established at the owner", w.status === 200, `status=${w.status}`);
  await moveEverythingTo("streams-2");
  const before = readFileSync(join(dirA, "workload.jwt"), "utf8");
  await sfetch(`http://127.0.0.1:${EMU}/admin/rotate-workload`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" });
  await sleep(1000);
  const after = readFileSync(join(dirA, "workload.jwt"), "utf8");
  w.pump();
  const drained = await waitFor(() => w.eof, 30000);
  const total = count(w.text, '"k":') + count(w.text, '"seed":');
  // A remote page issued in the sub-second window where the receiver
  // has not yet loaded the rotated key set can take the typed FleetAuth
  // cutoff — nonterminal, resumable (the locked failure semantics).
  // What rotation must NEVER do is terminalize or wedge the session;
  // the post-rotation replay below proves full recovery.
  leg("workload JWT rotation never terminalizes an active remote-paging session",
    before !== after && drained && !w.text.includes('"sealed":true'),
    `rotated=${before !== after} eof=${w.eof} records=${total}/${fullReplayRecords}`);
  const r = await sseRetry("streams-2", `/v1/streams/certsplit/records:sse?routingKey=hot0`, {
    ms: 25000, until: (t) => t.includes('"upToDate":true'),
  });
  leg("post-rotation full replay is complete",
    r.status === 200 && count(r.text, '"seed":0') === 1 && r.text.includes('"upToDate":true'),
    `status=${r.status} replayTo=${r.replayTo}`);
}

// ---- Authorization: revoke mid-subscription -------------------------
// Established AT the current owner (streams-2 after group D). The
// body-owned heartbeat re-proves authorization: revocation must end
// the session promptly with zero frames after the cutoff — the marker
// exactness legs live in-process; here the REAL auth feed drives it.
{
  // The churn-guard holdoff from the preceding moves can defer this
  // owner's engines ~30s; wait until it actually serves.
  await sseRetry("streams-2", `/v1/streams/certsplit/records:sse?routingKey=hot0&cursor=now`, { ms: 1500 });
  const sub = sseCollect(`${base("streams-2")}/v1/streams/certsplit/records:sse?routingKey=hot0&cursor=now`, {
    ms: 20000, until: (t) => t.includes("NEVER"),
  });
  await sleep(1500);
  const t0 = Date.now();
  await sfetch(`http://127.0.0.1:${EMU}/v1/projects/proj-lc/streams/credentials/${cred.credential.id}/revoke`, { method: "POST" });
  const ended = await sub;
  const cutMs = Date.now() - t0;
  leg("revocation mid-subscription: established 200, prompt resumable EOF",
    ended.status === 200 && ended.eof && cutMs < 15000 && !ended.text.includes('"sealed":true'),
    `status=${ended.status} eof=${ended.eof} cutMs=${cutMs}`);
  // Fresh credential for the rest of the battery.
  const c2 = await (await sfetch(`http://127.0.0.1:${EMU}/v1/projects/proj-lc/streams/credentials`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ displayName: "cert2" }),
  })).json();
  cred.secret = c2.secret;
  await sleep(1500);
  TOK = await tokenOf();
  const back = await sseRetry("streams-2", `/v1/streams/certsplit/records:sse?routingKey=hot0`, {
    ms: 25000, until: (t) => t.includes('"upToDate":true'),
  });
  leg("a fresh credential serves again after the revocation",
    back.status === 200 && count(back.text, '"seed":0') === 1,
    `status=${back.status}`);
}

// ---- Billing + residuals + legacy dispatches ------------------------
{
  // Let aborted subscriber connections tear their feeds down.
  // A feed mid-remote-page against a fenced peer drains on its retry
  // bound; give teardown a real window before calling residuals.
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
      legacy_sse_dispatches: d?.legacy_sse_dispatches ?? null,
      cutoff_wrong_owner: lf.cutoff_wrong_owner ?? 0,
      cutoff_fleet_auth: lf.cutoff_fleet_auth ?? 0,
      cutoff_incarnation: lf.cutoff_incarnation ?? 0,
      cutoff_incompatible: lf.cutoff_incompatible ?? 0,
      cutoff_target_mismatch: lf.cutoff_target_mismatch ?? 0,
      cutoff_redirect_loop: lf.cutoff_redirect_loop ?? 0,
      scaler: d?.scaler ?? null,
    };
    const legacy = d?.legacy_sse_dispatches ?? -1;
    const feeds = lf.live_feeds ?? -1;
    const reserved = lf.reserved_bytes ?? -1;
    if (legacy !== 0) { ok = false; detail += `${n}:legacy=${legacy} `; }
    if (feeds !== 0) { ok = false; detail += `${n}:feeds=${feeds} `; }
    if (reserved !== 0) { ok = false; detail += `${n}:reserved=${reserved} `; }
    const cut = ["cutoff_incarnation", "cutoff_incompatible", "cutoff_target_mismatch", "cutoff_redirect_loop"]
      .map((k) => lf[k] ?? 0).reduce((a, b) => a + b, 0);
    if (cut !== 0) { ok = false; detail += `${n}:unexpected_cutoffs=${cut} `; }
    // The blackhole leg's in-flight remote page may age out its
    // workload token across the freeze: ONE FleetAuth cutoff is the
    // typed, resumable answer (the reconnect leg proved recovery).
    if ((lf.cutoff_fleet_auth ?? 0) > 1) { ok = false; detail += `${n}:fleet_auth=${lf.cutoff_fleet_auth} `; }
  }
  leg("teardown: zero residual feeds, zero retained bytes, zero legacy dispatches, zero unclassified cutoffs on every instance", ok, detail);
}
legs["redirect loop refused (second redirect)"] = "COVERED_INPROC";
legs["target mismatch / recreated epoch / cross-project"] = "COVERED_INPROC";

function finish() {
  kill();
  const sha = createHash("sha256").update(readFileSync("./target/release/streams-slate")).digest("hex");
  const manifest = {
    commit: execSync("git rev-parse HEAD").toString().trim(),
    server_sha256: sha,
    verdict: failed === 0 ? "PASS" : "FAIL",
    legs,
    reconciliation,
  };
  writeFileSync("target/livefeed-cert-manifest.json", JSON.stringify(manifest, null, 2));
  console.log(`LIVEFEED_FLEET_CERT_${failed === 0 ? "OK" : "FAIL"} server=sha256:${sha}`);
  process.exit(failed === 0 ? 0 : 1);
}
finish();
