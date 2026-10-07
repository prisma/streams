#!/usr/bin/env node
// Auth feeds and customer tokens for one K2 field cell, built the way
// bench/soak/mtgen.mjs builds them for mt-tenants.sh: one RS256 key, one
// customer project with one credential, written as ONE feeds bundle
// {keys, policies, grants} that deploy/app-server materializes from
// FEEDS_S3_KEY into the three STREAMS_AUTH_*_FILE files, and customer
// JWTs (aud prisma-streams-data) for the generator. Differences from
// mtgen: one project instead of N, every scope the product defines (the
// generator also pulls and settles consumer groups, deletes streams and
// reads project usage), and the private key stays in the run's secrets
// directory so later generators can be given fresh tokens.
//
//   node feeds.mjs init  --dir SECRETS --cell-id C --project P --workspace W --credential K
//   node feeds.mjs token --dir SECRETS --cell-id C --project P --workspace W --credential K
//                        --ttl SECS --sub NAME --out FILE
//
// The bundle is validated against contracts/streams-platform/v1 before
// it is written. Tokens live at most 24 h (MAX_TOKEN_LIFETIME_SECS,
// src/auth.rs), so a generator that runs longer must be redeployed.
import { generateKeyPairSync, createPrivateKey, createSign, randomBytes } from "node:crypto";
import { existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { validateDocument } from "../../../platform-demo/src/validate.mjs";

const ISS = "https://auth.prisma.io";
const SCOPES = [
  "streams.metadata.read", "streams.records.read", "streams.records.append", "streams.create",
  "streams.lifecycle.manage", "streams.consumers.pull", "streams.consumers.settle",
  "streams.consumers.configure", "streams.forks.create", "streams.dlq.configure",
  "streams.watches.manage", "streams.catalog.read", "streams.usage.read",
].join(" ");
const MAX_TTL = 24 * 3600;

const cmd = process.argv[2];
const arg = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  if (i > 0) return process.argv[i + 1];
  if (dflt === undefined) throw new Error(`--${name} is required`);
  return dflt;
};
const secretWrite = (path, text) => {
  writeFileSync(`${path}.tmp`, text, { mode: 0o600 });
  renameSync(`${path}.tmp`, path);
};

const dir = arg("dir");
const cell = arg("cell-id");
const project = arg("project");
const workspace = arg("workspace");
const credential = arg("credential");
const keyPath = join(dir, "feeds-key.pem");
const kidPath = join(dir, "feeds-kid.txt");

if (cmd === "init") {
  if (existsSync(keyPath)) throw new Error(`${keyPath} exists: a cell's feed key is minted once`);
  const { publicKey, privateKey } = generateKeyPairSync("rsa", {
    modulusLength: 2048,
    publicKeyEncoding: { type: "spki", format: "pem" },
    privateKeyEncoding: { type: "pkcs8", format: "pem" },
  });
  const kid = `streams-rs256-k2c-${randomBytes(4).toString("hex")}`;
  const bundle = {
    keys: { feed_version: 1, keys: [{ kid, alg: "RS256", aud: "prisma-streams-data", pem: publicKey }] },
    policies: {
      feed_version: 1,
      projects: [{
        project_id: project, workspace_id: workspace, cell_id: cell,
        project_policy_version: 1, ownership_version: 1, status: "active", quotas: {},
      }],
    },
    grants: {
      feed_version: 1,
      credentials: [{
        credential_id: credential, project_id: project, grant_version: 1,
        status: "active", scopes: SCOPES, expires_at: null,
      }],
    },
  };
  const SCHEMA_DIR = new URL("../../../contracts/streams-platform/v1/", import.meta.url);
  for (const [part, schema] of [
    ["keys", "keys.schema.json"],
    ["policies", "project-policies.schema.json"],
    ["grants", "credential-grants.schema.json"],
  ]) {
    const errs = validateDocument(bundle[part], JSON.parse(readFileSync(new URL(schema, SCHEMA_DIR))));
    if (errs.length) throw new Error(`${part} snapshot violates ${schema}: ${errs[0]}`);
  }
  secretWrite(keyPath, privateKey);
  secretWrite(kidPath, kid);
  secretWrite(join(dir, "feeds-bundle.json"), JSON.stringify(bundle));
  console.log(`feeds: project=${project} cell=${cell} kid=${kid}`);
} else if (cmd === "token") {
  const ttl = Number(arg("ttl"));
  if (!(ttl > 0 && ttl <= MAX_TTL - 120)) throw new Error(`--ttl must be in 1..${MAX_TTL - 120}`);
  const kid = readFileSync(kidPath, "utf8").trim();
  const key = createPrivateKey(readFileSync(keyPath));
  const b64u = (b) => Buffer.from(b).toString("base64url");
  // iat 60 s in the past: the cell refuses an iat in its future beyond
  // the clock skew, and this laptop's clock is not the cell's.
  const now = Math.floor(Date.now() / 1000) - 60;
  const claims = {
    iss: ISS, aud: "prisma-streams-data", sub: arg("sub", "k2c-generator"),
    credential_id: credential, project_id: project, workspace_id: workspace, cell_id: cell,
    ownership_version: 1, grant_version: 1, scope: SCOPES,
    jti: randomBytes(8).toString("hex"), iat: now, nbf: now, exp: now + 60 + ttl,
  };
  const h = b64u(JSON.stringify({ alg: "RS256", typ: "JWT", kid }));
  const c = b64u(JSON.stringify(claims));
  const s = createSign("RSA-SHA256");
  s.update(`${h}.${c}`);
  secretWrite(arg("out"), `${h}.${c}.${s.sign(key, "base64url")}`);
  console.log(`token: project=${project} exp=${new Date(claims.exp * 1000).toISOString()}`);
} else {
  console.error("usage: feeds.mjs init|token --dir D --cell-id C --project P --workspace W --credential K ...");
  process.exit(2);
}
