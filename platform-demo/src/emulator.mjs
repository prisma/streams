#!/usr/bin/env node
// Prisma Streams platform EMULATOR (docs/CONTROL-PLANE-INTEGRATION.md §14).
//
// An independent, dependency-light reference implementation of the
// platform side of the Streams contract: Management API, one-time
// secrets, Prisma-Auth token exchange, JWKS/policy/grant full-snapshot
// feeds with atomic file projection, workload-JWT rotation, project
// placement across cells, transfer/deletion sagas, and a minimal
// console. It executes the REAL wire contract — the Rust cell consumes
// its files and tokens unmodified — and shares only the JSON schemas
// in contracts/streams-platform/v1, never Rust serialization code, so
// producer and consumer cannot agree on the same bug.
//
//   node platform-demo/src/emulator.mjs \
//     --port 9700 \
//     --cells cell-a=/tmp/feeds/a,cell-b=/tmp/feeds/b \
//     --fixture proj-a:ws-a:cell-a --fixture proj-b:ws-b:cell-b
//
// Single-cell legacy form still works:
//     --cell local-cell --feed-dir /tmp/feeds [--workload-file F]
//
// --enable-fault-api additionally exposes POST /admin/faults and
// POST /admin/mint-workload — DELIBERATE contract violations (torn
// writes, generation regressions, retired-kid resurrection, arbitrary
// workload identities) used by the e2e battery to prove the cell's
// defenses fire. Never enable it outside tests.
//
// NOTE: plain ESM JavaScript by design for a zero-build first version;
// the TypeScript split in the integration doc remains the target shape.
import http from "node:http";
import { generateKeyPairSync, createSign, randomBytes, scryptSync, timingSafeEqual } from "node:crypto";
import { writeFileSync, renameSync, mkdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { validateDocument } from "./validate.mjs";

// §14.3: the emulator validates every snapshot against the SHARED
// schemas before publication — an emulator bug must crash a request,
// never reach a feed file. (The fault API writes files directly and
// deliberately bypasses this: that is what it is for.)
const SCHEMA_DIR = new URL("../../contracts/streams-platform/v1/", import.meta.url);
const SCHEMAS = Object.fromEntries(
  ["keys", "project-policies", "credential-grants"].map((n) => [
    n, JSON.parse(readFileSync(new URL(`${n}.schema.json`, SCHEMA_DIR))),
  ]),
);
const FEED_SCHEMA = { keys: "keys", policies: "project-policies", grants: "credential-grants" };

const argOne = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : dflt;
};
const argAll = (name) => process.argv.flatMap((a, i) => (a === `--${name}` ? [process.argv[i + 1]] : []));
const PORT = +argOne("port", "9700");
const ISS = argOne("issuer", "https://auth.prisma.io");
const WORKLOAD_OPS = argOne("workload-ops", "telemetry-append,segment-read").split(",");
const FAULT_API = process.argv.includes("--enable-fault-api");

// ---- cells --------------------------------------------------------------
/** cell_id -> {dir, workloadFile} */
const cells = new Map();
const cellsSpec = argOne("cells", null);
if (cellsSpec) {
  for (const part of cellsSpec.split(",")) {
    const [id, dir] = part.split("=");
    cells.set(id, { dir, workloadFile: join(dir, "workload.jwt") });
  }
} else {
  const id = argOne("cell", "local-cell");
  const dir = argOne("feed-dir", "/tmp/streams-feeds");
  cells.set(id, { dir, workloadFile: argOne("workload-file", join(dir, "workload.jwt")) });
}
for (const c of cells.values()) {
  mkdirSync(c.dir, { recursive: true });
  mkdirSync(dirname(c.workloadFile), { recursive: true });
}
const firstCell = [...cells.keys()][0];

// ---- signing keys (kid lifecycle per MULTITENANCY §7) --------------------
function newRsaKid() {
  const { publicKey, privateKey } = generateKeyPairSync("rsa", {
    modulusLength: 2048,
    publicKeyEncoding: { type: "spki", format: "pem" },
    privateKeyEncoding: { type: "pkcs8", format: "pem" },
  });
  return { kid: `streams-rs256-${randomBytes(4).toString("hex")}`, publicKey, privateKey, retired: false };
}
/** ordered oldest→newest; sign with the newest non-retired */
const jwksKeys = [newRsaKid()];
const signingKey = () => [...jwksKeys].reverse().find((k) => !k.retired);

// ---- model --------------------------------------------------------------
/** project_id -> {workspace_id, status, ppv, ov, quotas, cell_id,
 *  pending_workspace?} — status ∈ active|suspended|transfer_pending|
 *  deleting|deleted. */
const projects = new Map();
/** credential_id -> {project_id, display, scopes, prefixes, grant_version,
 *  status, expires_at, hash (scrypt), salt, hint, created_at, last_used} */
const credentials = new Map();
let gen = { keys: 0, policies: 0, grants: 0 };
let credSeq = 0;

for (const fx of argAll("fixture")) {
  const [pid, ws, cell] = fx.split(":");
  const cell_id = cell ?? firstCell;
  if (!cells.has(cell_id)) throw new Error(`fixture ${fx}: unknown cell ${cell_id}`);
  projects.set(pid, { workspace_id: ws, status: "active", ppv: 1, ov: 1, quotas: {}, cell_id });
}

// ---- feed projection (atomic full snapshots, per cell) -------------------
// faults.modes: cell_id -> { partialWrite?: feedName, freeze?: true }
// published: cell_id -> feed -> [{gen, body} newest-first, depth 2] — the
// clean-publication history the regression fault replays.
const faultModes = new Map();
const published = new Map();
function atomicWrite(path, body) {
  const tmp = `${path}.tmp-${process.pid}-${Date.now()}`;
  writeFileSync(tmp, body);
  renameSync(tmp, path); // §7.2: never truncate the live file
}
const FEED_FILES = { keys: "keys.json", policies: "policies.json", grants: "grants.json" };
function snapshotBodies(cellId) {
  const own = ([, p]) => p.cell_id === cellId && p.status !== "deleted"; // deletion = omission tombstone
  return {
    keys: ({
      feed_version: gen.keys,
      keys: jwksKeys.filter((k) => !k.retired).map((k) => ({ kid: k.kid, alg: "RS256", pem: k.publicKey })),
    }),
    policies: ({
      feed_version: gen.policies,
      projects: [...projects.entries()].filter(own).map(([project_id, p]) => ({
        project_id,
        workspace_id: p.workspace_id,
        cell_id: p.cell_id,
        project_policy_version: p.ppv,
        ownership_version: p.ov,
        status: p.status === "active" ? "active" : "suspended", // cell vocabulary; saga states read as not-active
        quotas: p.quotas,
      })),
    }),
    grants: ({
      feed_version: gen.grants,
      credentials: [...credentials.entries()]
        .filter(([, c]) => own([c.project_id, projects.get(c.project_id) ?? { cell_id: null, status: "deleted" }]))
        .map(([credential_id, c]) => ({
          credential_id,
          project_id: c.project_id,
          grant_version: c.grant_version,
          status: c.status,
          scopes: c.scopes,
          ...(c.prefixes ? { stream_prefixes: c.prefixes } : {}),
          expires_at: c.expires_at ?? null,
        })),
    }),
  };
}
function project() {
  gen.keys += 1;
  gen.policies += 1;
  gen.grants += 1;
  for (const [cellId, cell] of cells) {
    const mode = faultModes.get(cellId) ?? {};
    if (mode.freeze) continue;
    const docs = snapshotBodies(cellId);
    if (!published.has(cellId)) published.set(cellId, { keys: [], policies: [], grants: [] });
    const hist = published.get(cellId);
    for (const [feed, doc] of Object.entries(docs)) {
      const schemaErrs = validateDocument(doc, SCHEMAS[FEED_SCHEMA[feed]]);
      if (schemaErrs.length)
        throw new Error(`refusing to publish schema-invalid ${feed} snapshot: ${schemaErrs[0]}`);
      const body = JSON.stringify(doc);
      const path = join(cell.dir, FEED_FILES[feed]);
      if (mode.partialWrite === feed) {
        // FAULT: a torn, non-atomic write of half the document straight
        // to the live path — exactly what §7.2 forbids. The cell must
        // fail to parse it and keep its previous accepted snapshot.
        writeFileSync(path, body.slice(0, Math.floor(body.length / 2)));
        continue;
      }
      atomicWrite(path, body);
      hist[feed] = [{ gen: gen[feed], body }, ...(hist[feed] ?? [])].slice(0, 2);
    }
  }
}

// ---- tokens -------------------------------------------------------------
const b64u = (b) => Buffer.from(b).toString("base64url");
function signJwt(claims, key = signingKey()) {
  const h = b64u(JSON.stringify({ alg: "RS256", typ: "JWT", kid: key.kid }));
  const c = b64u(JSON.stringify(claims));
  const s = createSign("RSA-SHA256");
  s.update(`${h}.${c}`);
  return `${h}.${c}.${s.sign(key.privateKey, "base64url")}`;
}
function mintCustomer(credId, cred) {
  const now = Math.floor(Date.now() / 1000);
  const p = projects.get(cred.project_id);
  return signJwt({
    iss: ISS, aud: "prisma-streams-data", sub: `cred:${credId}`,
    credential_id: credId, project_id: cred.project_id,
    workspace_id: p.workspace_id, cell_id: p.cell_id,
    ownership_version: p.ov, grant_version: cred.grant_version,
    scope: cred.scopes,
    ...(cred.prefixes ? { stream_prefixes: cred.prefixes } : {}),
    jti: randomBytes(8).toString("hex"), iat: now, nbf: now, exp: now + 600,
  });
}
function mintWorkload(cellId, operations) {
  const now = Math.floor(Date.now() / 1000);
  return signJwt({
    iss: ISS, aud: "prisma-streams-internal", sub: "emulator-slot-1",
    cell_id: cellId, operations, nbf: now, exp: now + 300,
  });
}
function rotateWorkload(onlyCell) {
  for (const [cellId, cell] of cells) {
    if (onlyCell && cellId !== onlyCell) continue;
    atomicWrite(cell.workloadFile, mintWorkload(cellId, WORKLOAD_OPS));
  }
}

// ---- secrets ------------------------------------------------------------
function newSecret(id) {
  const raw = randomBytes(24).toString("base64url");
  const salt = randomBytes(16);
  return {
    secret: `prisma_streams_${id}.${raw}`,
    salt,
    hash: scryptSync(raw, salt, 32),
    hint: `…${raw.slice(-6)}`,
  };
}
function verifySecret(cred, presentedRaw) {
  const h = scryptSync(presentedRaw, cred.salt, 32);
  return h.length === cred.hash.length && timingSafeEqual(h, cred.hash);
}
function revokeAllCredentials(pid) {
  for (const c of credentials.values())
    if (c.project_id === pid && c.status === "active") {
      c.status = "revoked";
      c.grant_version += 1;
    }
}

// ---- HTTP ---------------------------------------------------------------
const json = (res, code, body, headers = {}) => {
  res.writeHead(code, { "content-type": "application/json", ...headers });
  res.end(JSON.stringify(body));
};
const readBody = (req) =>
  new Promise((resolve) => {
    let b = "";
    req.on("data", (c) => (b += c));
    req.on("end", () => resolve(b));
  });

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://x`);
  const parts = url.pathname.split("/").filter(Boolean);
  try {
    // Console
    if (req.method === "GET" && url.pathname === "/") {
      res.writeHead(200, { "content-type": "text/html" });
      res.end(readFileSync(new URL("../public/index.html", import.meta.url)));
      return;
    }
    // Management: credentials
    if (parts[0] === "v1" && parts[1] === "projects" && parts[3] === "streams" && parts[4] === "credentials") {
      const pid = parts[2];
      const proj = projects.get(pid);
      if (!proj || proj.status === "deleted") return json(res, 404, { error: "unknown project" });
      if (req.method === "POST" && parts.length === 5) {
        const body = JSON.parse((await readBody(req)) || "{}");
        const id = `strcred_${String(++credSeq).padStart(3, "0")}`;
        const sec = newSecret(id);
        const cred = {
          project_id: pid,
          display: body.displayName ?? id,
          scopes: (body.scopes ?? ["streams.records.append", "streams.records.read", "streams.create", "streams.metadata.read"]).join(" "),
          prefixes: body.streamPrefixes ?? null,
          grant_version: 1, status: "active",
          expires_at: body.expiresAt ?? null,
          salt: sec.salt, hash: sec.hash, hint: sec.hint,
          created_at: new Date().toISOString(), last_used: null,
        };
        credentials.set(id, cred);
        project();
        // The secret member appears EXACTLY once, here.
        return json(res, 201, { credential: publicCred(id, cred), secret: sec.secret });
      }
      if (req.method === "GET" && parts.length === 5) {
        return json(res, 200, {
          credentials: [...credentials.entries()]
            .filter(([, c]) => c.project_id === pid)
            .map(([id, c]) => publicCred(id, c)),
        });
      }
      const cid = parts[5];
      const cred = credentials.get(cid);
      if (!cred || cred.project_id !== pid) return json(res, 404, { error: "unknown credential" });
      if (req.method === "POST" && parts[6] === "rotate") {
        const sec = newSecret(cid);
        Object.assign(cred, { salt: sec.salt, hash: sec.hash, hint: sec.hint, grant_version: cred.grant_version + 1 });
        project();
        return json(res, 200, { credential: publicCred(cid, cred), secret: sec.secret });
      }
      if (req.method === "POST" && parts[6] === "revoke") {
        cred.status = "revoked";
        cred.grant_version += 1;
        project();
        return json(res, 200, { credential: publicCred(cid, cred) });
      }
    }
    // Prisma Auth: token exchange
    if (req.method === "POST" && url.pathname === "/v1/token/streams") {
      const auth = req.headers.authorization ?? "";
      const m = auth.match(/^StreamsCredential prisma_streams_([A-Za-z0-9_]+)\.(.+)$/);
      if (!m) return json(res, 401, { error: "missing or malformed StreamsCredential" });
      const cred = credentials.get(m[1]);
      if (!cred || !verifySecret(cred, m[2])) return json(res, 401, { error: "unknown credential or bad secret" });
      if (cred.status !== "active") return json(res, 403, { error: `credential ${cred.status}` });
      const p = projects.get(cred.project_id);
      // §10.1: token exchange stops the moment a saga begins — any
      // non-active status (suspended/transfer_pending/deleting) refuses.
      if (!p || p.status !== "active") return json(res, 403, { error: `project ${p?.status ?? "unknown"}` });
      cred.last_used = new Date().toISOString();
      return json(res, 200, {
        accessToken: mintCustomer(m[1], cred),
        tokenType: "Bearer", expiresIn: 600,
        projectId: cred.project_id, endpoint: null,
      });
    }
    // Feeds (HTTP form; the file projection is the primary transport)
    if (req.method === "GET" && parts[0] === "internal" && parts[1] === "streams" && parts[2] === "cells") {
      const cellId = cells.has(parts[3]) ? parts[3] : firstCell;
      const feed = { jwks: "keys", "project-policies": "policies", "credential-grants": "grants" }[parts[4]];
      if (!feed) return json(res, 404, { error: "unknown feed" });
      res.writeHead(200, {
        "content-type": "application/json",
        "cache-control": "no-store",
        "prisma-streams-feed-generation": String(gen[feed]),
      });
      return res.end(JSON.stringify(snapshotBodies(cellId)[feed]));
    }
    // Gateway support: authoritative placement map.
    if (req.method === "GET" && url.pathname === "/admin/placement") {
      return json(res, 200, {
        cells: [...cells.keys()],
        projects: Object.fromEntries(
          [...projects.entries()].map(([pid, p]) => [pid, { cell_id: p.cell_id, status: p.status, workspace_id: p.workspace_id }]),
        ),
      });
    }
    // Admin: sagas + status
    if (req.method === "POST" && parts[0] === "admin" && parts[1] === "projects" && parts[2]) {
      const pid = parts[2];
      const p = projects.get(pid);
      if (!p) return json(res, 404, { error: "unknown project" });
      const body = JSON.parse((await readBody(req)) || "{}");
      // §10 ownership-transfer saga, split so tests can observe the
      // intermediate state: step 1 stops exchange; complete performs
      // ov++/workspace swap/revocation/publication atomically.
      if (parts[3] === "transfer" && parts.length === 4) {
        if (p.status !== "active") return json(res, 409, { error: `project ${p.status}` });
        if (!body.toWorkspace) return json(res, 400, { error: "toWorkspace required" });
        p.status = "transfer_pending";
        p.pending_workspace = body.toWorkspace;
        return json(res, 200, { project_id: pid, status: p.status });
      }
      if (parts[3] === "transfer" && parts[4] === "complete") {
        if (p.status !== "transfer_pending") return json(res, 409, { error: `project ${p.status}` });
        p.ov += 1;
        p.workspace_id = p.pending_workspace;
        delete p.pending_workspace;
        revokeAllCredentials(pid); // §10 step 4: revoke-all by default
        p.ppv += 1;
        p.status = "active";
        project();
        return json(res, 200, { project_id: pid, status: p.status, ownership_version: p.ov, workspace_id: p.workspace_id });
      }
      // §11 deletion saga: authorization cutoff FIRST (revoke + publish
      // while `deleting`), then the project leaves the feed entirely —
      // the omission tombstone. Storage cleanup is the cell's own saga.
      if (parts[3] === "delete") {
        if (p.status === "deleted") return json(res, 200, { project_id: pid, status: p.status });
        p.status = "deleting";
        revokeAllCredentials(pid);
        p.ppv += 1;
        project(); // cutoff publication
        p.status = "deleted";
        project(); // omission publication
        return json(res, 200, { project_id: pid, status: p.status });
      }
      if (body.status || body.quotas) {
        if (body.status) p.status = body.status;
        if (body.quotas) p.quotas = { ...p.quotas, ...body.quotas };
        p.ppv += 1;
        project();
      }
      return json(res, 200, { project_id: pid, status: p.status, ppv: p.ppv, quotas: p.quotas });
    }
    if (req.method === "POST" && url.pathname === "/admin/rotate-workload") {
      const body = JSON.parse((await readBody(req)) || "{}");
      rotateWorkload(body.cell);
      return json(res, 200, { rotated: true });
    }
    // JWKS lifecycle (§7): overlap adds a new kid and starts signing
    // with it; retire drops every older kid from the snapshot — at the
    // cell that retirement is PERMANENT.
    if (req.method === "POST" && url.pathname === "/admin/rotate-jwks") {
      const body = JSON.parse((await readBody(req)) || "{}");
      if (body.phase === "overlap") {
        jwksKeys.push(newRsaKid());
        project();
        return json(res, 200, { kids: jwksKeys.map((k) => ({ kid: k.kid, retired: k.retired })) });
      }
      if (body.phase === "retire") {
        for (const k of jwksKeys.slice(0, -1)) k.retired = true;
        project();
        return json(res, 200, { kids: jwksKeys.map((k) => ({ kid: k.kid, retired: k.retired })) });
      }
      return json(res, 400, { error: "phase must be overlap|retire" });
    }
    // Test-only surfaces below: deliberate contract violations.
    if (req.method === "POST" && url.pathname === "/admin/mint-workload") {
      if (!FAULT_API) return json(res, 403, { error: "fault API not enabled" });
      const body = JSON.parse((await readBody(req)) || "{}");
      return json(res, 200, { jwt: mintWorkload(body.cell ?? firstCell, body.operations ?? []) });
    }
    if (req.method === "POST" && url.pathname === "/admin/faults") {
      if (!FAULT_API) return json(res, 403, { error: "fault API not enabled" });
      const body = JSON.parse((await readBody(req)) || "{}");
      const cellId = body.cell ?? firstCell;
      const cell = cells.get(cellId);
      if (!cell) return json(res, 404, { error: "unknown cell" });
      const hist = published.get(cellId) ?? {};
      switch (body.kind) {
        case "partial-write": // torn file on the NEXT projection(s)
          faultModes.set(cellId, { ...(faultModes.get(cellId) ?? {}), partialWrite: body.feed ?? "grants" });
          return json(res, 200, { fault: "partial-write", cell: cellId, feed: body.feed ?? "grants" });
        case "freeze": // stale feeds: no further projections
          faultModes.set(cellId, { ...(faultModes.get(cellId) ?? {}), freeze: true });
          return json(res, 200, { fault: "freeze", cell: cellId });
        case "generation-regression": {
          // Replay the PREVIOUS clean publication (older feed_version).
          const prev = hist[body.feed ?? "grants"]?.[1];
          if (!prev) return json(res, 409, { error: "no previous publication to replay" });
          atomicWrite(join(cell.dir, FEED_FILES[body.feed ?? "grants"]), prev.body);
          return json(res, 200, { fault: "generation-regression", cell: cellId, replayed_gen: prev.gen });
        }
        case "same-gen-drift": {
          // Same feed_version, different content: flip every revoked
          // grant back to active without touching the version.
          const cur = hist[body.feed ?? "grants"]?.[0];
          if (!cur) return json(res, 409, { error: "no current publication" });
          const doc = JSON.parse(cur.body);
          for (const c of doc.credentials ?? []) if (c.status === "revoked") c.status = "active";
          for (const p of doc.projects ?? []) p.status = "active";
          atomicWrite(join(cell.dir, FEED_FILES[body.feed ?? "grants"]), JSON.stringify(doc));
          return json(res, 200, { fault: "same-gen-drift", cell: cellId, gen: cur.gen });
        }
        case "resurrect-kid": {
          // Re-add every retired kid at a NEW generation: the cell must
          // refuse the whole snapshot (retirement is permanent).
          const doc = {
            feed_version: gen.keys + 1,
            keys: jwksKeys.map((k) => ({ kid: k.kid, alg: "RS256", pem: k.publicKey })),
          };
          atomicWrite(join(cell.dir, "keys.json"), JSON.stringify(doc));
          return json(res, 200, { fault: "resurrect-kid", cell: cellId, gen: doc.feed_version });
        }
        case "clear": {
          faultModes.delete(cellId);
          project(); // clean republication
          return json(res, 200, { cleared: true });
        }
        default:
          return json(res, 400, { error: "unknown fault kind" });
      }
    }
    json(res, 404, { error: "no such route" });
  } catch (e) {
    json(res, 500, { error: String(e) });
  }
});

function publicCred(id, c) {
  return {
    id, projectId: c.project_id, displayName: c.display,
    scopes: c.scopes.split(" "), streamPrefixes: c.prefixes,
    grantVersion: c.grant_version, status: c.status,
    valueHint: c.hint, createdAt: c.created_at, lastUsedAt: c.last_used,
    expiresAt: c.expires_at,
  };
}

project();
rotateWorkload();
setInterval(rotateWorkload, 60_000).unref();
server.listen(PORT, "127.0.0.1", () =>
  console.log(`platform emulator on 127.0.0.1:${PORT} cells=[${[...cells.keys()].join(",")}]${FAULT_API ? " FAULT-API" : ""}`),
);
