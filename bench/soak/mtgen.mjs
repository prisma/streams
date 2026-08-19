#!/usr/bin/env node
// MT-tenants campaign feed/token generator: N projects, one credential
// each, one RS256 kid — written as ONE feeds bundle {keys, policies,
// grants} (the server app splits it into the three feed FILES) plus a
// token map [{project, token}] for awsbench's BENCH_MT mode.
//
//   node bench/soak/mtgen.mjs --projects 1000 --cell mt-cell-1 \
//     --out $SOAK_HOME/mt-<run>              [--issuer https://auth.prisma.io]
//
// The bundle is validated against contracts/streams-platform/v1 before
// it is written — the same shared-schema gate the platform emulator
// enforces (§14.3); a campaign must not ship a drifted wire shape.
import { generateKeyPairSync, createSign, randomBytes } from "node:crypto";
import { writeFileSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { validateDocument } from "../../platform-demo/src/validate.mjs";

const arg = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : dflt;
};
const N = Number(arg("projects", "1000"));
const CELL = arg("cell", "mt-cell-1");
const OUT = arg("out", null) ?? (() => { throw new Error("--out required"); })();
const ISS = arg("issuer", "https://auth.prisma.io");
const TOKEN_TTL_S = Number(arg("ttl", String(12 * 3600))); // cell ceiling: 24h (MAX_TOKEN_LIFETIME_SECS)
mkdirSync(OUT, { recursive: true });

const { publicKey, privateKey } = generateKeyPairSync("rsa", {
  modulusLength: 2048,
  publicKeyEncoding: { type: "spki", format: "pem" },
  privateKeyEncoding: { type: "pkcs8", format: "pem" },
});
const KID = `streams-rs256-mt-${randomBytes(4).toString("hex")}`;
const b64u = (b) => Buffer.from(b).toString("base64url");
const signJwt = (claims) => {
  const h = b64u(JSON.stringify({ alg: "RS256", typ: "JWT", kid: KID }));
  const c = b64u(JSON.stringify(claims));
  const s = createSign("RSA-SHA256");
  s.update(`${h}.${c}`);
  return `${h}.${c}.${s.sign(privateKey, "base64url")}`;
};

const SCOPES = "streams.create streams.records.append streams.records.read streams.metadata.read";
const pid = (i) => `proj-mt-${String(i).padStart(4, "0")}`;
const WS = "ws-mt";
const now = Math.floor(Date.now() / 1000);

const bundle = {
  keys: { feed_version: 1, keys: [{ kid: KID, alg: "RS256", pem: publicKey }] },
  policies: {
    feed_version: 1,
    projects: Array.from({ length: N }, (_, i) => ({
      project_id: pid(i + 1),
      workspace_id: WS,
      cell_id: CELL,
      project_policy_version: 1,
      ownership_version: 1,
      status: "active",
      quotas: {},
    })),
  },
  grants: {
    feed_version: 1,
    credentials: Array.from({ length: N }, (_, i) => ({
      credential_id: `strcred_mt_${String(i + 1).padStart(4, "0")}`,
      project_id: pid(i + 1),
      grant_version: 1,
      status: "active",
      scopes: SCOPES,
      expires_at: null,
    })),
  },
};

const SCHEMA_DIR = new URL("../../contracts/streams-platform/v1/", import.meta.url);
for (const [part, schema] of [
  ["keys", "keys.schema.json"],
  ["policies", "project-policies.schema.json"],
  ["grants", "credential-grants.schema.json"],
]) {
  const errs = validateDocument(bundle[part], JSON.parse(readFileSync(new URL(schema, SCHEMA_DIR))));
  if (errs.length) throw new Error(`${part} snapshot violates ${schema}: ${errs[0]}`);
}

const tokens = Array.from({ length: N }, (_, i) => ({
  project: pid(i + 1),
  token: signJwt({
    iss: ISS, aud: "prisma-streams-data", sub: `campaign:mt-tenants`,
    credential_id: `strcred_mt_${String(i + 1).padStart(4, "0")}`,
    project_id: pid(i + 1), workspace_id: WS, cell_id: CELL,
    ownership_version: 1, grant_version: 1,
    scope: SCOPES,
    jti: randomBytes(8).toString("hex"),
    iat: now, nbf: now, exp: now + TOKEN_TTL_S,
  }),
}));

writeFileSync(join(OUT, "feeds-bundle.json"), JSON.stringify(bundle));
writeFileSync(join(OUT, "tokens.json"), JSON.stringify(tokens));
writeFileSync(join(OUT, "key.pem"), privateKey); // stays in SOAK_HOME, never deployed
console.log(`mtgen: ${N} projects cell=${CELL} kid=${KID} ttl=${TOKEN_TTL_S}s -> ${OUT}`);
