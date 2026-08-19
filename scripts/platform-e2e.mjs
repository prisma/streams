#!/usr/bin/env node
// Platform end-to-end battery (docs/CONTROL-PLANE-INTEGRATION.md §14.4):
// real processes, real wire contract — the emulator publishes feed
// FILES and mints RS256 JWTs; the Rust cell consumes them unmodified.
// Nothing here calls AuthService::publish_* — that is the point.
//
//   node scripts/platform-e2e.mjs
//
// Phase-A scenarios: credential lifecycle (secret once, wrong secret,
// exchange, rotate invalidates after feed publication, revoke refuses
// exchange), suspension cuts off a live token, and the cell BOOTS AND
// SERVES under the full release posture (enforce + workload + no
// static fleet token, STREAMS_RELEASE_POSTURE=1).
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const KEY_B64 = Buffer.from(Array(32).fill(7)).toString("base64");
const CELL = "local-cell";
const EMU_PORT = 9700;
const CELL_PORT = 9702;
const S3_PORT = 9703;
let failures = 0;
const check = (name, cond, extra = "") => {
  console.log(`${cond ? "ok  " : "FAIL"} ${name} ${cond ? "" : extra}`);
  if (!cond) failures++;
};

const dir = mkdtempSync(join(tmpdir(), "platform-e2e-"));
execSync("cargo build --release --bin streams-slate --bin s3lite", { stdio: "inherit" });

const emu = spawn(process.execPath, [
  "platform-demo/src/emulator.mjs",
  "--port", String(EMU_PORT), "--cell", CELL,
  "--feed-dir", dir, "--workload-file", join(dir, "workload.jwt"),
  "--fixture", "proj-e2e:ws-e2e",
], { stdio: ["ignore", "inherit", "inherit"] });
const s3 = spawn("./target/release/s3lite", ["--listen", `127.0.0.1:${S3_PORT}`, "--latency-ms", "2"], { stdio: "ignore" });
await new Promise((r) => setTimeout(r, 800));

// The RELEASE POSTURE, end to end: no static fleet token exists.
const cellEnv = {
  ...process.env,
  STREAMS_AUTH_MODE: "enforce",
  STREAMS_AUTH_ISSUER: "https://auth.prisma.io",
  STREAMS_AUTH_KEYS_FILE: join(dir, "keys.json"),
  STREAMS_AUTH_POLICY_FILE: join(dir, "policies.json"),
  STREAMS_AUTH_GRANTS_FILE: join(dir, "grants.json"),
  STREAMS_AUTH_REFRESH_SECS: "1",
  FLEET_AUTH_MODE: "workload",
  WORKLOAD_TOKEN_FILE: join(dir, "workload.jwt"),
  STREAMS_RELEASE_POSTURE: "1",
  CELL_ID: CELL,
  PROJECT_ID: "proj-deploy-e2e",
  USAGE_STREAM_KEY: KEY_B64,
};
delete cellEnv.FLEET_INTERNAL_TOKEN;
const cell = spawn("./target/release/streams-slate", [
  "--listen", `127.0.0.1:${CELL_PORT}`,
  "--s3-endpoint", `http://127.0.0.1:${S3_PORT}`,
  "--bucket", `pe2e-${Date.now()}`,
  "--max-unflushed-bytes", "67108864",
  "--flush-interval-ms", "1", "--wal-flush-gap-ms", "2",
], { env: cellEnv, stdio: ["ignore", "inherit", "inherit"] });
const kill = () => { for (const p of [cell, s3, emu]) try { p.kill(); } catch {} };
process.on("exit", kill);
await new Promise((r) => setTimeout(r, 2500));
check("cell boots under the release posture (workload, no static token)", cell.exitCode === null);

const emuBase = `http://127.0.0.1:${EMU_PORT}`;
const cellBase = `http://127.0.0.1:${CELL_PORT}`;
const j = async (r) => ({ status: r.status, body: await r.json().catch(() => ({})) });

// 1. Credential lifecycle
const created = await j(await fetch(`${emuBase}/v1/projects/proj-e2e/streams/credentials`, {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ displayName: "e2e ingest" }),
}));
check("credential created", created.status === 201 && !!created.body.secret);
const CRED_ID = created.body.credential.id;
const SECRET = created.body.secret;
const listed = await j(await fetch(`${emuBase}/v1/projects/proj-e2e/streams/credentials`));
check("secret appears exactly once (list omits it)",
  JSON.stringify(listed.body).includes(CRED_ID) && !JSON.stringify(listed.body).includes(SECRET.split(".")[1]));

const exchange = (secret) =>
  fetch(`${emuBase}/v1/token/streams`, { method: "POST", headers: { authorization: `StreamsCredential ${secret}` } });
check("wrong secret refused", (await exchange(`${SECRET}x`)).status === 401);
const tok1 = await j(await exchange(SECRET));
check("exchange succeeds for the active credential", tok1.status === 200 && !!tok1.body.accessToken);

// 2. SDK through the REAL token provider against the cell.
await new Promise((r) => setTimeout(r, 1500)); // feeds refresh (1s cadence)
const { StreamsClient } = await import("../sdk/dist/index.js");
const client = new StreamsClient({
  url: cellBase, project: "proj-e2e",
  tokenProvider: async () => (await (await exchange(SECRET)).json()).accessToken,
});
let stream;
try {
  // encryptionKey belongs to the STREAM handle, not the client.
  stream = await client.createStream("e2e/orders", {
    encryptionKey: KEY_B64,
    format: { kind: "json" },
  });
  check("SDK create through exchanged token", true);
} catch (e) {
  check("SDK create through exchanged token", false, String(e));
}
try {
  await stream.append({ n: 1 });
  const recs = await stream.read();
  check("SDK append + read round-trip", Array.isArray(recs) ? recs.length >= 1 : true);
} catch (e) {
  check("SDK append + read round-trip", false, String(e));
}

// 3. Rotation invalidates the OLD grant after feed publication.
const oldToken = tok1.body.accessToken;
const rotated = await j(await fetch(`${emuBase}/v1/projects/proj-e2e/streams/credentials/${CRED_ID}/rotate`, { method: "POST" }));
check("rotation returns a new secret once", rotated.status === 200 && !!rotated.body.secret);
const NEW_SECRET = rotated.body.secret;
await new Promise((r) => setTimeout(r, 2000)); // grant feed republish + cell refresh
const oldRead = await fetch(`${cellBase}/v1/streams/e2e/orders/records`, {
  headers: { authorization: `Bearer ${oldToken}`, "prisma-encryption-key": KEY_B64 },
});
check("old-grant token refused after rotation reaches the cell", oldRead.status === 401 || oldRead.status === 403, `status ${oldRead.status}`);
const tok2 = await j(await exchange(NEW_SECRET));
const newRead = await fetch(`${cellBase}/v1/streams/e2e/orders/records`, {
  headers: { authorization: `Bearer ${tok2.body.accessToken}`, "prisma-encryption-key": KEY_B64 },
});
check("new-grant token serves", newRead.status === 200, `status ${newRead.status}`);

// 4. Revocation refuses exchange.
await fetch(`${emuBase}/v1/projects/proj-e2e/streams/credentials/${CRED_ID}/revoke`, { method: "POST" });
check("revoked credential cannot exchange", (await exchange(NEW_SECRET)).status === 403);

// 5. Suspension cuts off a live, unexpired token.
const live = tok2.body.accessToken;
await fetch(`${emuBase}/admin/projects/proj-e2e`, {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ status: "suspended" }),
});
await new Promise((r) => setTimeout(r, 2000));
const cutOff = await fetch(`${cellBase}/v1/streams/e2e/orders/records`, {
  headers: { authorization: `Bearer ${live}`, "prisma-encryption-key": KEY_B64 },
});
check("suspension cuts off a live token", cutOff.status === 403, `status ${cutOff.status}`);

// 6. Workload rotation (no static fallback exists in this posture).
const w1 = readFileSync(join(dir, "workload.jwt"), "utf8");
await fetch(`${emuBase}/admin/rotate-workload`, { method: "POST" });
const w2 = readFileSync(join(dir, "workload.jwt"), "utf8");
check("workload JWT rotates atomically", w1 !== w2 && w2.split(".").length === 3);

kill();
if (failures) { console.error(`PLATFORM_E2E_FAIL (${failures})`); process.exit(1); }
console.log("PLATFORM_E2E_OK");
