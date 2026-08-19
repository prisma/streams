#!/usr/bin/env node
// Platform end-to-end battery (docs/CONTROL-PLANE-INTEGRATION.md §14.4):
// real processes, real wire contract — the emulator publishes feed
// FILES and mints RS256 JWTs; TWO Rust cells consume them unmodified
// behind the reference gateway. Nothing here calls
// AuthService::publish_* — that is the point.
//
//   node scripts/platform-e2e.mjs
//
// Phase A (through the gateway): credential lifecycle (secret once,
// wrong secret, exchange, rotate invalidates after feed publication,
// revoke refuses exchange), suspension cuts off a live token, cells
// BOOT AND SERVE under the full release posture (enforce + workload +
// no static fleet token, STREAMS_RELEASE_POSTURE=1).
// Phase C/D + faults: placement isolation, direct-to-wrong-cell 421,
// forged path project refused before routing, unknown-kid nudge,
// retired-kid cutoff, ownership transfer + deletion sagas, torn/
// regressed/drifted/resurrected feed publications refused, operation-
// scoped workload JWTs, per-cell workload rotation.
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const KEY_B64 = Buffer.from(Array(32).fill(7)).toString("base64");
const EMU_PORT = 9700;
const A_PORT = 9702, B_PORT = 9704, C_PORT = 9706, GW_PORT = 9710;
let failures = 0;
const check = (name, cond, extra = "") => {
  console.log(`${cond ? "ok  " : "FAIL"} ${name} ${cond ? "" : extra}`);
  if (!cond) failures++;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// A refused connection fails the relevant check instead of crashing
// the battery: status 0, empty body.
const sfetch = async (...args) => {
  try {
    return await fetch(...args);
  } catch {
    return { status: 0, ok: false, headers: new Map(), json: async () => ({}), text: async () => "" };
  }
};
const j = async (r) => ({ status: r.status, body: await r.json().catch(() => ({})) });

const root = mkdtempSync(join(tmpdir(), "platform-e2e-"));
const dirA = join(root, "a"), dirB = join(root, "b"), dirC = join(root, "c");
execSync("cargo build --release --bin streams-slate --bin s3lite", { stdio: "inherit" });

const emu = spawn(process.execPath, [
  "platform-demo/src/emulator.mjs",
  "--port", String(EMU_PORT),
  "--cells", `cell-a=${dirA},cell-b=${dirB},cell-c=${dirC}`,
  "--fixture", "proj-e2e:ws-e2e:cell-a",
  "--fixture", "proj-b:ws-b:cell-b",
  "--fixture", "proj-del:ws-del:cell-b",
  "--fixture", "proj-c:ws-c:cell-c",
  "--enable-fault-api",
], { stdio: ["ignore", "inherit", "inherit"] });
const s3 = spawn("./target/release/s3lite", ["--listen", `127.0.0.1:${EMU_PORT + 3}`, "--latency-ms", "2"], { stdio: "ignore" });
await sleep(800);

// proj-c's credential must exist BEFORE cell C boots: C refreshes at
// 100s, so a grant published after its boot feed load stays invisible
// until the first cadence tick — and the nudge leg needs the grant in
// the boot snapshot so only the KID is unknown.
const credC = await (await fetch(`http://127.0.0.1:${EMU_PORT}/v1/projects/proj-c/streams/credentials`, {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ displayName: "c ingest" }),
})).json().then((body) => ({ status: 201, body }));

// The RELEASE POSTURE, end to end: no static fleet token exists.
const cellEnv = (cellId, dir, refreshSecs) => {
  const env = {
    ...process.env,
    STREAMS_AUTH_MODE: "enforce",
    STREAMS_AUTH_ISSUER: "https://auth.prisma.io",
    STREAMS_AUTH_KEYS_FILE: join(dir, "keys.json"),
    STREAMS_AUTH_POLICY_FILE: join(dir, "policies.json"),
    STREAMS_AUTH_GRANTS_FILE: join(dir, "grants.json"),
    STREAMS_AUTH_REFRESH_SECS: String(refreshSecs),
    FLEET_AUTH_MODE: "workload",
    WORKLOAD_TOKEN_FILE: join(dir, "workload.jwt"),
    STREAMS_RELEASE_POSTURE: "1",
    CELL_ID: cellId,
    PROJECT_ID: "proj-deploy-e2e",
    USAGE_STREAM_KEY: KEY_B64,
  };
  delete env.FLEET_INTERNAL_TOKEN;
  return env;
};
const cellArgs = (port, bucket) => [
  "--listen", `127.0.0.1:${port}`,
  "--s3-endpoint", `http://127.0.0.1:${EMU_PORT + 3}`,
  "--bucket", bucket,
  "--max-unflushed-bytes", "67108864",
  "--flush-interval-ms", "1", "--wal-flush-gap-ms", "2",
];
const ts = Date.now();
const cellA = spawn("./target/release/streams-slate", cellArgs(A_PORT, `pe2e-a-${ts}`), { env: cellEnv("cell-a", dirA, 1), stdio: ["ignore", "inherit", "inherit"] });
const cellB = spawn("./target/release/streams-slate", cellArgs(B_PORT, `pe2e-b-${ts}`), { env: cellEnv("cell-b", dirB, 1), stdio: ["ignore", "inherit", "inherit"] });
// Cell C refreshes at 100s — the cell's own ceiling (refresh must be
// <= a third of the 300s staleness window). The nudge leg asserts
// success within 10s, so the cadence cannot explain it.
const cellC = spawn("./target/release/streams-slate", cellArgs(C_PORT, `pe2e-c-${ts}`), { env: cellEnv("cell-c", dirC, 100), stdio: ["ignore", "inherit", "inherit"] });
const gw = spawn(process.execPath, [
  "platform-demo/src/gateway.mjs",
  "--port", String(GW_PORT), "--emulator", `http://127.0.0.1:${EMU_PORT}`,
  "--cell", `cell-a=http://127.0.0.1:${A_PORT}`,
  "--cell", `cell-b=http://127.0.0.1:${B_PORT}`,
  "--cell", `cell-c=http://127.0.0.1:${C_PORT}`,
], { stdio: ["ignore", "inherit", "inherit"] });
const kill = () => { for (const p of [gw, cellA, cellB, cellC, s3, emu]) try { p.kill(); } catch {} };
process.on("exit", kill);
await sleep(2500);
check("cell A boots under the release posture (workload, no static token)", cellA.exitCode === null);
check("cell B boots under the release posture", cellB.exitCode === null);
check("cell C boots under the release posture", cellC.exitCode === null);

const emuBase = `http://127.0.0.1:${EMU_PORT}`;
const gwBase = `http://127.0.0.1:${GW_PORT}`;
const aBase = `http://127.0.0.1:${A_PORT}`;
const bBase = `http://127.0.0.1:${B_PORT}`;
const cBase = `http://127.0.0.1:${C_PORT}`;
const mkCred = async (pid, name) =>
  j(await sfetch(`${emuBase}/v1/projects/${pid}/streams/credentials`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ displayName: name }),
  }));
const exchange = (secret) =>
  sfetch(`${emuBase}/v1/token/streams`, { method: "POST", headers: { authorization: `StreamsCredential ${secret}` } });
const readRecords = (base, name, token) =>
  sfetch(`${base}/v1/streams/${name}/records`, { headers: { authorization: `Bearer ${token}`, "prisma-encryption-key": KEY_B64 } });

// ---- Phase A: credential lifecycle through the GATEWAY -------------------
const created = await mkCred("proj-e2e", "e2e ingest");
check("credential created", created.status === 201 && !!created.body.secret);
const CRED_ID = created.body.credential.id;
const SECRET = created.body.secret;
// The client's tokenProvider must survive the rotation leg: it closes
// over a MUTABLE secret, updated when the credential rotates.
let CURRENT_SECRET = SECRET;
const listed = await j(await sfetch(`${emuBase}/v1/projects/proj-e2e/streams/credentials`));
check("secret appears exactly once (list omits it)",
  JSON.stringify(listed.body).includes(CRED_ID) && !JSON.stringify(listed.body).includes(SECRET.split(".")[1]));
check("wrong secret refused", (await exchange(`${SECRET}x`)).status === 401);
const tok1 = await j(await exchange(SECRET));
check("exchange succeeds for the active credential", tok1.status === 200 && !!tok1.body.accessToken);

await sleep(1500); // feeds refresh (1s cadence on A/B)
const { StreamsClient } = await import("../sdk/dist/index.js");
const client = new StreamsClient({
  url: gwBase, project: "proj-e2e",
  tokenProvider: async () => (await (await exchange(CURRENT_SECRET)).json()).accessToken,
});
let stream;
try {
  stream = await client.createStream("e2e/orders", { encryptionKey: KEY_B64, format: { kind: "json" } });
  check("SDK create through gateway + exchanged token", true);
} catch (e) {
  check("SDK create through gateway + exchanged token", false, String(e));
}
try {
  await stream.append({ src: "cell-a" });
  const page = await stream.read();
  check("SDK append + read round-trip through gateway", page.records.length >= 1 && page.records[0]?.src === "cell-a");
} catch (e) {
  check("SDK append + read round-trip through gateway", false, String(e));
}

const oldToken = tok1.body.accessToken;
const rotated = await j(await sfetch(`${emuBase}/v1/projects/proj-e2e/streams/credentials/${CRED_ID}/rotate`, { method: "POST" }));
check("rotation returns a new secret once", rotated.status === 200 && !!rotated.body.secret);
const NEW_SECRET = rotated.body.secret;
CURRENT_SECRET = NEW_SECRET;
await sleep(2000);
const oldRead = await readRecords(gwBase, "e2e/orders", oldToken);
check("old-grant token refused after rotation reaches the cell", oldRead.status === 401 || oldRead.status === 403, `status ${oldRead.status}`);
const tok2 = await j(await exchange(NEW_SECRET));
check("new-grant token serves through gateway", (await readRecords(gwBase, "e2e/orders", tok2.body.accessToken)).status === 200);

// ---- Phase C: placement, isolation, wrong cell, forged project -----------
const credB = await mkCred("proj-b", "b ingest");
const SECRET_B = credB.body.secret;
const tokB = await j(await exchange(SECRET_B));
check("cell-b project exchanges", tokB.status === 200);
await sleep(1500);
const tb = tokB.body.accessToken;
const bCreate = await sfetch(`${gwBase}/v1/streams/e2e/orders`, {
  method: "PUT",
  headers: { authorization: `Bearer ${tb}`, "content-type": "application/json", "prisma-encryption-key": KEY_B64 },
  body: JSON.stringify({ format: { kind: "json" } }),
});
check("proj-b creates its own e2e/orders through the gateway", bCreate.status === 200 || bCreate.status === 201,
  `status ${bCreate.status} body ${await bCreate.text().catch(() => "")}`);
const bAppend = await sfetch(`${gwBase}/v1/streams/e2e/orders/records`, {
  method: "POST",
  headers: { authorization: `Bearer ${tb}`, "content-type": "application/json", "prisma-encryption-key": KEY_B64 },
  body: JSON.stringify({ src: "cell-b" }),
});
check("proj-b appends through the gateway", bAppend.status === 200 || bAppend.status === 201,
  `status ${bAppend.status} body ${await bAppend.text().catch(() => "")}`);
const bRead = await readRecords(gwBase, "e2e/orders", tb);
const bRecords = bRead.status === 200 ? await bRead.json().catch(() => []) : [];
const pageA = await stream.read();
check("same stream name on two projects/cells stays isolated",
  bRecords.length >= 1 && bRecords[0]?.src === "cell-b" &&
  pageA.records.length >= 1 && pageA.records[0]?.src === "cell-a",
  `b=${JSON.stringify(bRecords[0] ?? null)} a=${JSON.stringify(pageA.records[0] ?? null)}`);
const wrongCell = await readRecords(aBase, "e2e/orders", tokB.body.accessToken);
check("direct call to the wrong cell answers 421 wrong_cell", wrongCell.status === 421, `status ${wrongCell.status}`);
check("same token through the gateway routes to its cell", (await readRecords(gwBase, "e2e/orders", tokB.body.accessToken)).status === 200);
const forged = await j(await sfetch(`${gwBase}/v1/projects/proj-e2e/usage`, { headers: { authorization: `Bearer ${tokB.body.accessToken}` } }));
check("forged path project refused BEFORE routing", forged.status === 403 && forged.body.error === "forged_project", `status ${forged.status}`);
check("unverified request never selects a cell", (await sfetch(`${gwBase}/v1/streams/e2e/orders/records`, { headers: { authorization: "Bearer not.a.jwt" } })).status === 401);

// ---- JWKS lifecycle: unknown-kid nudge + permanent retirement -------------
const tokC1 = await j(await exchange(credC.body.secret)); // signed with kid #1
await sleep(1500);
const mk = await streamCreateRaw(cBase, "c/probe", tokC1.body.accessToken);
check("cell C serves before rotation (kid #1)", mk === 200 || mk === 201, `status ${mk}`);
await sfetch(`${emuBase}/admin/rotate-jwks`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ phase: "overlap" }) });
const tokC2 = await j(await exchange(credC.body.secret)); // signed with kid #2 — unknown to cell C (1h refresh)
const t0 = Date.now();
let nudged = null;
for (let i = 0; i < 20 && nudged !== 200; i++) {
  nudged = (await readRecords(cBase, "c/probe", tokC2.body.accessToken)).status;
  if (nudged !== 200) await sleep(500);
}
check("unknown kid triggers an immediate feed refresh (nudge, not the 1h cadence)",
  nudged === 200 && Date.now() - t0 < 10_000, `status ${nudged} after ${Date.now() - t0}ms`);
const tokBOld = tokB.body.accessToken; // kid #1, grant still active
await sfetch(`${emuBase}/admin/rotate-jwks`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ phase: "retire" }) });
await sleep(2000);
const retiredRead = await readRecords(bBase, "e2e/orders", tokBOld);
check("token signed with a retired kid is refused", retiredRead.status === 401, `status ${retiredRead.status}`);
const tokB2 = await j(await exchange(SECRET_B)); // kid #2
check("current-kid token still serves after retirement", (await readRecords(bBase, "e2e/orders", tokB2.body.accessToken)).status === 200);

// ---- Phase D: ownership transfer saga -------------------------------------
const preTransfer = await j(await exchange(NEW_SECRET));
check("pre-transfer token minted", preTransfer.status === 200);
await sfetch(`${emuBase}/admin/projects/proj-e2e/transfer`, {
  method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ toWorkspace: "ws-new" }),
});
check("no new token during transfer_pending", (await exchange(NEW_SECRET)).status === 403);
await sfetch(`${emuBase}/admin/projects/proj-e2e/transfer/complete`, { method: "POST" });
await sleep(2000);
const oldOwnership = await readRecords(gwBase, "e2e/orders", preTransfer.body.accessToken);
check("old-ownership token fails after feed publication", oldOwnership.status === 401 || oldOwnership.status === 403, `status ${oldOwnership.status}`);
check("revoked-by-transfer credential cannot exchange", (await exchange(NEW_SECRET)).status === 403);
const credNewWs = await mkCred("proj-e2e", "post-transfer ingest");
const tokNewWs = await j(await exchange(credNewWs.body.secret));
await sleep(1500);
const survivor = await readRecords(gwBase, "e2e/orders", tokNewWs.body.accessToken);
let survivorRecords = [];
if (survivor.status === 200) try { survivorRecords = await survivor.json(); } catch {}
check("storage identity unchanged: new owner reads the existing stream",
  survivor.status === 200 && Array.isArray(survivorRecords) && survivorRecords.length >= 1, `status ${survivor.status}`);

// ---- Phase D: deletion saga ------------------------------------------------
const credDel = await mkCred("proj-del", "del ingest");
const tokDel = await j(await exchange(credDel.body.secret));
await sleep(1500);
const mkDel = await streamCreateRaw(gwBase, "del/data", tokDel.body.accessToken);
check("doomed project serves before deletion", mkDel === 200 || mkDel === 201, `status ${mkDel}`);
await sfetch(`${emuBase}/admin/projects/proj-del/delete`, { method: "POST" });
check("deleted project cannot exchange", (await exchange(credDel.body.secret)).status === 403);
await sleep(2000);
const delRead = await readRecords(bBase, "del/data", tokDel.body.accessToken);
check("deletion cuts off a live token (authorization before storage cleanup)",
  [401, 403, 421].includes(delRead.status), `status ${delRead.status}`);

// ---- Feed faults against cell B (deliberate contract violations) ----------
const credF = await mkCred("proj-b", "fault probe");
const tokF = await j(await exchange(credF.body.secret));
await sleep(1500);
check("fault-probe token serves", (await readRecords(bBase, "e2e/orders", tokF.body.accessToken)).status === 200);
const fault = (body) =>
  sfetch(`${emuBase}/admin/faults`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ cell: "cell-b", ...body }) });
await fault({ kind: "partial-write", feed: "grants" });
await sfetch(`${emuBase}/v1/projects/proj-b/streams/credentials/${credF.body.credential.id}/revoke`, { method: "POST" });
await sleep(2500);
check("torn feed file never becomes visible: revocation does NOT land, old snapshot serves",
  (await readRecords(bBase, "e2e/orders", tokF.body.accessToken)).status === 200);
await fault({ kind: "clear" });
await sleep(2500);
const afterClear = await readRecords(bBase, "e2e/orders", tokF.body.accessToken);
check("clean republication lands the pending revocation", afterClear.status === 401 || afterClear.status === 403, `status ${afterClear.status}`);
await fault({ kind: "generation-regression", feed: "grants" });
await sleep(2500);
check("generation regression refused: revoked grant does not resurrect",
  (await readRecords(bBase, "e2e/orders", tokF.body.accessToken)).status !== 200);
await fault({ kind: "same-gen-drift", feed: "grants" });
await sleep(2500);
check("same-generation content drift refused: revoked grant still dead",
  (await readRecords(bBase, "e2e/orders", tokF.body.accessToken)).status !== 200);
await fault({ kind: "resurrect-kid" });
await sleep(2500);
check("retired-kid resurrection refused: old-kid token stays dead", (await readRecords(bBase, "e2e/orders", tokBOld)).status === 401);
check("refused snapshot does not clobber good state: current-kid token still serves",
  (await readRecords(bBase, "e2e/orders", tokB2.body.accessToken)).status === 200);

// ---- Operation-scoped workload identity -----------------------------------
const wlNone = await j(await sfetch(`${emuBase}/admin/mint-workload`, {
  method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ cell: "cell-a", operations: [] }),
}));
const wlRead = await j(await sfetch(`${emuBase}/admin/mint-workload`, {
  method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ cell: "cell-a", operations: ["segment-read"] }),
}));
const seg = (tok) => sfetch(`${aBase}/v1/segments/e2e/orders`, { headers: { authorization: `Bearer ${tok}` } });
check("workload JWT with empty operations grants nothing", (await seg(wlNone.body.jwt)).status === 401);
check("workload JWT with the exact operation passes auth", (await seg(wlRead.body.jwt)).status !== 401);
check("customer token cannot enter the internal surface", (await seg(tokB2.body.accessToken)).status === 401);

// ---- Workload rotation per cell -------------------------------------------
const wA1 = readFileSync(join(dirA, "workload.jwt"), "utf8");
const wB1 = readFileSync(join(dirB, "workload.jwt"), "utf8");
await sfetch(`${emuBase}/admin/rotate-workload`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" });
const wA2 = readFileSync(join(dirA, "workload.jwt"), "utf8");
const wB2 = readFileSync(join(dirB, "workload.jwt"), "utf8");
check("workload JWTs rotate atomically on every cell", wA1 !== wA2 && wB1 !== wB2 && wA2.split(".").length === 3 && wB2.split(".").length === 3);

async function streamCreateRaw(base, name, token) {
  const r = await sfetch(`${base}/v1/streams/${name}`, {
    method: "PUT",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json", "prisma-encryption-key": KEY_B64 },
    body: JSON.stringify({ format: { kind: "json" } }),
  });
  return r.status;
}

kill();
if (failures) { console.error(`PLATFORM_E2E_FAIL (${failures})`); process.exit(1); }
console.log("PLATFORM_E2E_OK");
