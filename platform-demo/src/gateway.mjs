#!/usr/bin/env node
// Reference GATEWAY (docs/CONTROL-PLANE-INTEGRATION.md §14.2 item 10):
// a verifying reverse proxy in front of the Streams cells.
//
//   node platform-demo/src/gateway.mjs --port 9710 \
//     --emulator http://127.0.0.1:9700 \
//     --cell cell-a=http://127.0.0.1:9702 --cell cell-b=http://127.0.0.1:9704
//
// Contract properties it demonstrates (§14.5 "Placement and gateway"):
//   * the JWT is VERIFIED (signature, iss, aud, exp/nbf) BEFORE any
//     routing decision — an unverified request never selects a cell;
//   * the routing project comes from the VERIFIED claims, never from
//     the URL — a forged path project is refused, so a caller cannot
//     aim someone else's cell;
//   * placement is resolved from the control plane's authoritative
//     map, not from the token's cell_id claim;
//   * a 421 wrong_cell from a cell triggers exactly one placement
//     re-resolution + retry (feed lag after a move); an unresolved 421
//     is passed through unchanged, never converted to 401 (§8).
//
// Deliberately small: no TLS, no connection pooling, no quotas — it is
// the executable shape of the routing contract, not a production LB.
import http from "node:http";
import { createVerify } from "node:crypto";

const argOne = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : dflt;
};
const PORT = +argOne("port", "9710");
const EMU = argOne("emulator", "http://127.0.0.1:9700");
const ISS = argOne("issuer", "https://auth.prisma.io");
const cellBases = new Map(
  process.argv.flatMap((a, i) => (a === "--cell" ? [process.argv[i + 1].split("=")] : [])),
);
if (cellBases.size === 0) throw new Error("at least one --cell id=base required");

// ---- JWKS + placement caches --------------------------------------------
let jwks = new Map(); // kid -> pem
let placement = { projects: {}, fetched: 0 };
async function refreshJwks() {
  const r = await fetch(`${EMU}/internal/streams/cells/gw/jwks`);
  const doc = await r.json();
  jwks = new Map(doc.keys.filter((k) => k.alg === "RS256").map((k) => [k.kid, k.pem]));
}
async function resolvePlacement(force = false) {
  if (!force && Date.now() - placement.fetched < 1000) return placement;
  const r = await fetch(`${EMU}/admin/placement`);
  placement = { ...(await r.json()), fetched: Date.now() };
  return placement;
}

// ---- verification BEFORE routing ----------------------------------------
const SKEW = 30;
function b64uJson(part) {
  try {
    return JSON.parse(Buffer.from(part, "base64url").toString());
  } catch {
    return null;
  }
}
async function verifyBearer(header) {
  const m = /^Bearer (.+)$/.exec(header ?? "");
  if (!m) return null;
  const [h, c, sig] = m[1].split(".");
  if (!sig) return null;
  const hdr = b64uJson(h);
  const claims = b64uJson(c);
  if (!hdr || !claims || hdr.alg !== "RS256") return null;
  let pem = jwks.get(hdr.kid);
  if (!pem) {
    await refreshJwks().catch(() => {});
    pem = jwks.get(hdr.kid);
    if (!pem) return null;
  }
  const v = createVerify("RSA-SHA256");
  v.update(`${h}.${c}`);
  if (!v.verify(pem, sig, "base64url")) return null;
  const now = Math.floor(Date.now() / 1000);
  if (claims.iss !== ISS || claims.aud !== "prisma-streams-data") return null;
  if (typeof claims.exp !== "number" || claims.exp + SKEW < now) return null;
  if (typeof claims.nbf === "number" && claims.nbf - SKEW > now) return null;
  if (typeof claims.project_id !== "string") return null;
  return claims;
}

// ---- proxy ---------------------------------------------------------------
function forward(req, res, base, body) {
  return new Promise((resolve) => {
    const target = new URL(req.url, base);
    const up = http.request(
      target,
      { method: req.method, headers: { ...req.headers, host: target.host } },
      (upRes) => resolve(upRes),
    );
    up.on("error", () => {
      res.writeHead(502, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "upstream unreachable" }));
      resolve(null);
    });
    up.end(body);
  });
}
const readBody = (req) =>
  new Promise((resolve) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks)));
  });
const refuse = (res, code, error) => {
  res.writeHead(code, { "content-type": "application/json" });
  res.end(JSON.stringify({ error }));
};

const server = http.createServer(async (req, res) => {
  try {
    // Verify BEFORE routing: no cell is selected for an unverified call.
    const claims = await verifyBearer(req.headers.authorization);
    if (!claims) return refuse(res, 401, "invalid or missing token");
    // The URL never overrides the verified identity (§14.5: forged
    // project_id cannot select a cell).
    const pathProject = /^\/v1\/projects\/([^/]+)\//.exec(req.url)?.[1];
    if (pathProject && pathProject !== claims.project_id) return refuse(res, 403, "forged_project");
    const pl = await resolvePlacement();
    const entry = pl.projects[claims.project_id];
    const base = entry && cellBases.get(entry.cell_id);
    if (!base) return refuse(res, 403, "no active placement");

    // Body must be buffered once so the single 421 retry can resend it.
    const body = await readBody(req);
    let upRes = await forward(req, res, base, body);
    if (!upRes) return;
    if (upRes.statusCode === 421) {
      upRes.resume(); // discard; placement may have moved under us
      const fresh = await resolvePlacement(true);
      const moved = fresh.projects[claims.project_id];
      const newBase = moved && cellBases.get(moved.cell_id);
      if (newBase && newBase !== base) {
        upRes = await forward(req, res, newBase, body);
        if (!upRes) return;
      }
      // else: preserve the 421 — never convert to 401 (§8).
    }
    res.writeHead(upRes.statusCode, upRes.headers);
    upRes.pipe(res);
  } catch (e) {
    refuse(res, 500, String(e));
  }
});

await refreshJwks();
server.listen(PORT, "127.0.0.1", () =>
  console.log(`reference gateway on 127.0.0.1:${PORT} cells=[${[...cellBases.keys()].join(",")}]`),
);
