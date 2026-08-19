#!/usr/bin/env node
// Prisma Streams platform EMULATOR (docs/CONTROL-PLANE-INTEGRATION.md §14).
//
// An independent, dependency-light reference implementation of the
// platform side of the Streams contract: Management API, one-time
// secrets, Prisma-Auth token exchange, JWKS/policy/grant full-snapshot
// feeds with atomic file projection, workload-JWT rotation, and a
// minimal console. It executes the REAL wire contract — the Rust cell
// consumes its files and tokens unmodified — and shares only the JSON
// schemas in contracts/streams-platform/v1, never Rust serialization
// code, so producer and consumer cannot agree on the same bug.
//
//   node platform-demo/src/emulator.mjs \
//     --port 9700 --cell local-cell --feed-dir /tmp/feeds \
//     --workload-file /tmp/feeds/workload.jwt \
//     --fixture proj-e2e:ws-e2e
//
// NOTE: plain ESM JavaScript by design for a zero-build first version;
// the TypeScript split in the integration doc remains the target shape.
import http from "node:http";
import { generateKeyPairSync, createSign, randomBytes, scryptSync, timingSafeEqual } from "node:crypto";
import { writeFileSync, renameSync, mkdirSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";

const arg = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : dflt;
};
const PORT = +arg("port", "9700");
const CELL = arg("cell", "local-cell");
const FEED_DIR = arg("feed-dir", "/tmp/streams-feeds");
const WORKLOAD_FILE = arg("workload-file", join(FEED_DIR, "workload.jwt"));
const WORKLOAD_OPS = arg("workload-ops", "telemetry-append,segment-read").split(",");
const ISS = arg("issuer", "https://auth.prisma.io");
mkdirSync(FEED_DIR, { recursive: true });
mkdirSync(dirname(WORKLOAD_FILE), { recursive: true });

// ---- signing keys -------------------------------------------------------
const { publicKey, privateKey } = generateKeyPairSync("rsa", {
  modulusLength: 2048,
  publicKeyEncoding: { type: "spki", format: "pem" },
  privateKeyEncoding: { type: "pkcs8", format: "pem" },
});
const KID = `streams-rs256-${randomBytes(4).toString("hex")}`;

// ---- model --------------------------------------------------------------
/** project_id -> {workspace_id, status, ppv, ov, quotas} */
const projects = new Map();
/** credential_id -> {project_id, display, scopes, prefixes, grant_version,
 *  status, expires_at, hash (scrypt), salt, hint, created_at, last_used} */
const credentials = new Map();
let gen = { keys: 0, policies: 0, grants: 0 };
let credSeq = 0;

for (const fx of process.argv.flatMap((a, i) => (a === "--fixture" ? [process.argv[i + 1]] : []))) {
  const [pid, ws] = fx.split(":");
  projects.set(pid, { workspace_id: ws, status: "active", ppv: 1, ov: 1, quotas: {} });
}

// ---- feed projection (atomic full snapshots) ----------------------------
function atomicWrite(path, body) {
  const tmp = `${path}.tmp-${process.pid}-${Date.now()}`;
  writeFileSync(tmp, body);
  renameSync(tmp, path); // §7.2: never truncate the live file
}
function project() {
  gen.keys += 1;
  gen.policies += 1;
  gen.grants += 1;
  atomicWrite(
    join(FEED_DIR, "keys.json"),
    JSON.stringify({ feed_version: gen.keys, keys: [{ kid: KID, alg: "RS256", pem: publicKey }] }),
  );
  atomicWrite(
    join(FEED_DIR, "policies.json"),
    JSON.stringify({
      feed_version: gen.policies,
      projects: [...projects.entries()].map(([project_id, p]) => ({
        project_id,
        workspace_id: p.workspace_id,
        cell_id: CELL,
        project_policy_version: p.ppv,
        ownership_version: p.ov,
        status: p.status,
        quotas: p.quotas,
      })),
    }),
  );
  atomicWrite(
    join(FEED_DIR, "grants.json"),
    JSON.stringify({
      feed_version: gen.grants,
      credentials: [...credentials.entries()].map(([credential_id, c]) => ({
        credential_id,
        project_id: c.project_id,
        grant_version: c.grant_version,
        status: c.status,
        scopes: c.scopes,
        ...(c.prefixes ? { stream_prefixes: c.prefixes } : {}),
        expires_at: c.expires_at ?? null,
      })),
    }),
  );
}

// ---- tokens -------------------------------------------------------------
const b64u = (b) => Buffer.from(b).toString("base64url");
function signJwt(claims) {
  const h = b64u(JSON.stringify({ alg: "RS256", typ: "JWT", kid: KID }));
  const c = b64u(JSON.stringify(claims));
  const s = createSign("RSA-SHA256");
  s.update(`${h}.${c}`);
  return `${h}.${c}.${s.sign(privateKey, "base64url")}`;
}
function mintCustomer(credId, cred) {
  const now = Math.floor(Date.now() / 1000);
  const p = projects.get(cred.project_id);
  return signJwt({
    iss: ISS, aud: "prisma-streams-data", sub: `cred:${credId}`,
    credential_id: credId, project_id: cred.project_id,
    workspace_id: p.workspace_id, cell_id: CELL,
    ownership_version: p.ov, grant_version: cred.grant_version,
    scope: cred.scopes,
    ...(cred.prefixes ? { stream_prefixes: cred.prefixes } : {}),
    jti: randomBytes(8).toString("hex"), iat: now, nbf: now, exp: now + 600,
  });
}
function rotateWorkload() {
  const now = Math.floor(Date.now() / 1000);
  const jwt = signJwt({
    iss: ISS, aud: "prisma-streams-internal", sub: `emulator-slot-1`,
    cell_id: CELL, operations: WORKLOAD_OPS, nbf: now, exp: now + 300,
  });
  atomicWrite(WORKLOAD_FILE, jwt);
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

// ---- HTTP ---------------------------------------------------------------
const json = (res, code, body, headers = {}) => {
  const b = JSON.stringify(body);
  res.writeHead(code, { "content-type": "application/json", ...headers });
  res.end(b);
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
      if (!projects.has(pid)) return json(res, 404, { error: "unknown project" });
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
      if (!p || p.status !== "active") return json(res, 403, { error: "project not active" });
      cred.last_used = new Date().toISOString();
      return json(res, 200, {
        accessToken: mintCustomer(m[1], cred),
        tokenType: "Bearer", expiresIn: 600,
        projectId: cred.project_id, endpoint: null,
      });
    }
    // Feeds (HTTP form; the file projection is the primary transport)
    if (req.method === "GET" && parts[0] === "internal" && parts[1] === "streams" && parts[2] === "cells") {
      const which = { jwks: "keys.json", "project-policies": "policies.json", "credential-grants": "grants.json" }[parts[4]];
      if (!which) return json(res, 404, { error: "unknown feed" });
      const body = readFileSync(join(FEED_DIR, which));
      res.writeHead(200, {
        "content-type": "application/json",
        "cache-control": "no-store",
        "prisma-streams-feed-generation": String(gen[parts[4] === "jwks" ? "keys" : parts[4] === "project-policies" ? "policies" : "grants"]),
      });
      return res.end(body);
    }
    // Admin / test-fault surface
    if (req.method === "POST" && parts[0] === "admin" && parts[1] === "projects") {
      const p = projects.get(parts[2]);
      if (!p) return json(res, 404, { error: "unknown project" });
      const body = JSON.parse((await readBody(req)) || "{}");
      if (body.status) {
        p.status = body.status;
        p.ppv += 1;
        project();
      }
      return json(res, 200, { project_id: parts[2], status: p.status, ppv: p.ppv });
    }
    if (req.method === "POST" && url.pathname === "/admin/rotate-workload") {
      rotateWorkload();
      return json(res, 200, { rotated: true });
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
  console.log(`platform emulator on 127.0.0.1:${PORT} cell=${CELL} feeds=${FEED_DIR}`),
);
