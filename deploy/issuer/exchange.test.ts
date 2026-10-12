// POST /v1/token/streams (public-beta issuer triage B2.6). Run with
// `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { validateDocument } from "../../platform-demo/src/validate.mjs";
import { FakeCell } from "../cell-admin/fake-cell";
import { schema } from "../cell-admin/feeds";
import { CUSTOMER_SCOPES } from "../cell-admin/rules";
import { hashSecret, parsePeppers, verifySecret } from "../cell-admin/secrets";
import {
  cleanup,
  decodeJwt,
  keyFile,
  pepperFile,
  publicKeyFile,
  readBundle,
  run,
  verifiedClaims,
  writeStateText,
} from "../cell-admin/test-rig";
import { boot } from "./boot";
import { StateCache } from "./cache";
import { type Env, readConfig } from "./config";
import { CELL, CELL_ORIGIN, cellState, customerSecret, envWith, fakeS3, type IssuerCell, issuerCell, wrongSecret } from "./rig";
import { Issuer } from "./server";

afterEach(cleanup);

const T0 = 1_800_000_000_000;

async function issuerFor(cell: IssuerCell, env: Env = cell.env) {
  const clock = { t: T0 };
  const now = () => clock.t;
  const cfg = readConfig(env);
  const b = await boot(cfg);
  const cache = new StateCache(b.store, b, { now, log: () => {} });
  return { cfg, cache, clock, issuer: new Issuer(cfg, { store: b.store, cache, now }) };
}

function post(issuer: Issuer, auth?: string, body?: unknown, method = "POST"): Promise<Response> {
  return issuer.fetch(
    new Request("https://issuer.example.com/v1/token/streams", {
      method,
      headers: auth === undefined ? {} : { authorization: `StreamsCredential ${auth}` },
      body: body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body),
    }),
  );
}

async function answer(res: Response): Promise<[number, any]> {
  return [res.status, await res.json()];
}

const refusal = (code: string, message: string) => ({ error: { code, message } });

describe("the exchange", () => {
  test("mints a token of the credential's project that the cell serving the published bundle accepts", async () => {
    const cell = await issuerCell();
    const { id, secret } = await customerSecret(cell, "sp_a");
    const { issuer, cfg } = await issuerFor(cell);
    const res = await post(issuer, secret);
    expect(res.status).toBe(200);
    expect(res.headers.get("cache-control")).toBe("no-store");
    expect([...res.headers.keys()].filter((h) => h.startsWith("access-control-"))).toEqual([]);
    const body = await res.json();
    expect(Object.keys(body)).toEqual(["accessToken", "tokenType", "expiresIn", "projectId", "cellId", "endpoint"]);
    expect([body.tokenType, body.expiresIn, body.projectId, body.cellId, body.endpoint]).toEqual([
      "Bearer", 3540, "sp_a", CELL, CELL_ORIGIN,
    ]);
    const { header, claims } = decodeJwt(body.accessToken);
    expect(header).toEqual({ alg: "EdDSA", typ: "JWT", kid: cfg.signingKey.record.kid });
    expect(validateDocument(claims, schema("customer-token-claims.schema.json"))).toEqual([]);
    const state = cellState(cell);
    expect(claims).toMatchObject({
      iss: "https://auth.prisma.io", aud: "prisma-streams-data", sub: `cred:${id}`, credential_id: id,
      project_id: "sp_a", workspace_id: "ws_sp_a", cell_id: CELL, ownership_version: 1,
      grant_version: state.credentials[id].grant_version, scope: CUSTOMER_SCOPES.join(" "),
      iat: T0 / 1000 - 60, nbf: T0 / 1000 - 60, exp: T0 / 1000 + 3540,
    });
    expect(claims.jti).toMatch(/^[0-9a-f]{32}$/);
    expect(verifiedClaims(readBundle(cell.state), body.accessToken)).toEqual(claims);
    // A cell serving the bundle cell-admin published takes it (the clock
    // the fake cell reads is the real one: mint at the real time).
    const real = await issuerFor(cell);
    real.clock.t = Date.now();
    const fresh = await (await post(real.issuer, secret)).json();
    const fake = new FakeCell();
    try {
      fake.publish(readBundle(cell.state));
      const r = await fetch(`${fake.url}/v1/streams?limit=1`, { headers: { authorization: `Bearer ${fresh.accessToken}` } });
      expect(r.status).toBe(200);
    } finally {
      fake.stop();
    }
    expect(JSON.stringify(body)).not.toContain(secret.split(".")[1]);
  });

  test("narrows the scopes to those asked for, and the lifetime to one asked for down to 300 s", async () => {
    const cell = await issuerCell();
    const { secret } = await customerSecret(cell, "sp_a", ["--scopes", "streams.records.read streams.records.append"]);
    const { issuer } = await issuerFor(cell);
    const res = await post(issuer, secret, { requestedScopes: ["streams.records.read", "streams.create"], ttlSecs: 300 });
    const body = await res.json();
    const { claims } = decodeJwt(body.accessToken);
    expect([res.status, body.expiresIn, claims.scope, claims.exp - claims.iat]).toEqual([200, 240, "streams.records.read", 300]);
    expect(decodeJwt((await (await post(issuer, secret, {})).json()).accessToken).claims.scope).toBe(
      "streams.records.read streams.records.append",
    );
    expect(await answer(await post(issuer, secret, { requestedScopes: ["streams.create"] }))).toEqual([
      400, refusal("invalid_request", "the credential grants none of requestedScopes"),
    ]);
  });

  test("refuses a body it cannot take with 400 invalid_request, naming what is wrong", async () => {
    const cell = await issuerCell();
    const { secret } = await customerSecret(cell, "sp_a");
    const { issuer } = await issuerFor(cell);
    const cases: [unknown, string][] = [
      ["{", "the body is not JSON"],
      [[], "the body is not a JSON object"],
      [{ ttl: 600 }, 'unknown member "ttl": the body takes requestedScopes and ttlSecs'],
      [{ requestedScopes: [] }, "requestedScopes is a non-empty array of scope names"],
      [{ requestedScopes: "streams.records.read" }, "requestedScopes is a non-empty array of scope names"],
      [{ ttlSecs: 299 }, "ttlSecs is a whole number of seconds in 300..3600"],
      [{ ttlSecs: 3601 }, "ttlSecs is a whole number of seconds in 300..3600"],
      [{ ttlSecs: "600" }, "ttlSecs is a whole number of seconds in 300..3600"],
      [{ ttlSecs: 600.5 }, "ttlSecs is a whole number of seconds in 300..3600"],
      [JSON.stringify({ requestedScopes: Array(500).fill("streams.records.read") }), "the body is larger than 4096 bytes"],
    ];
    for (const [body, message] of cases) {
      expect(await answer(await post(issuer, secret, body))).toEqual([400, refusal("invalid_request", message)]);
    }
  });

  test("answers 401 invalid_credential alike for a malformed, an unknown, a secretless and a wrongly-guessed credential", async () => {
    const cell = await issuerCell();
    const { id, secret } = await customerSecret(cell, "sp_a");
    const { issuer } = await issuerFor(cell);
    const canary = Object.entries(cellState(cell).credentials).find(([, c]) => c.project_id === "sp_canary_cell_sc1")![0];
    const form = refusal("invalid_credential", "send Authorization: StreamsCredential prisma_streams_<credential id>.<secret>");
    const unknown = refusal("invalid_credential", "unknown credential or wrong secret");
    const cases: [string | undefined, unknown][] = [
      [undefined, form],
      [secret.replace("prisma_streams_", ""), form],
      [`${secret}x`, form],
      [`prisma_streams_strcred_op_${"0".repeat(32)}.${secret.split(".")[1]}`, form],
      [wrongSecret(`strcred_${"0".repeat(32)}`), unknown],
      [wrongSecret(id), unknown],
      [wrongSecret(canary), unknown],
    ];
    for (const [auth, body] of cases) {
      const res = await post(issuer, auth);
      expect([res.status, await res.json(), res.headers.get("www-authenticate")]).toEqual([
        401, body, 'StreamsCredential realm="streams"',
      ]);
    }
    const bearer = await issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
      method: "POST", headers: { authorization: `Bearer ${secret}` },
    }));
    expect(await answer(bearer)).toEqual([401, form]);
  });

  test("checks the secret before the credential's status, the credential's before the project's", async () => {
    const cell = await issuerCell();
    const { id, secret } = await customerSecret(cell, "sp_a");
    const { secret: other } = await customerSecret(cell, "sp_b");
    const { issuer, cache } = await issuerFor(cell);
    expect((await run("suspend", "--state", cell.state, "--project", "sp_b")).code).toBe(0);
    expect((await run("offboard", "--state", cell.state, "--project", "sp_a")).code).toBe(0);
    await cache.refresh();
    expect(await answer(await post(issuer, wrongSecret(id)))).toEqual([
      401, refusal("invalid_credential", "unknown credential or wrong secret"),
    ]);
    expect(await answer(await post(issuer, secret))).toEqual([
      403, refusal("credential_not_active", `credential ${id} is revoked`),
    ]);
    expect(await answer(await post(issuer, other))).toEqual([
      403, refusal("project_not_active", "project sp_b is suspended"),
    ]);
    expect((await run("resume", "--state", cell.state, "--project", "sp_b")).code).toBe(0);
    await cache.refresh();
    expect((await post(issuer, other)).status).toBe(200);
  });

  test("checks a body that arrives slowly against the state as it is once the body is in, not as it was", async () => {
    const cell = await issuerCell();
    const { id, secret } = await customerSecret(cell, "sp_a");
    const { issuer, cache } = await issuerFor(cell);
    // The client sends its headers, then trickles the body.
    let send!: ReadableStreamDefaultController<Uint8Array>;
    const body = new ReadableStream<Uint8Array>({ start: (c) => void (send = c) });
    const pending = issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
      method: "POST", headers: { authorization: `StreamsCredential ${secret}` }, body, duplex: "half",
    } as RequestInit));
    await new Promise((r) => setTimeout(r, 20));
    // Meanwhile the owner revokes the credential, and the copy revalidates.
    await run("offboard", "--state", cell.state, "--project", "sp_a");
    await cache.refresh();
    send.enqueue(new TextEncoder().encode("{}"));
    send.close();
    expect(await answer(await pending)).toEqual([403, refusal("credential_not_active", `credential ${id} is revoked`)]);
  });

  test("refuses a credential past its expiry, and a project being deleted", async () => {
    const cell = await issuerCell();
    const { id, secret } = await customerSecret(cell, "sp_a");
    const { issuer, cache } = await issuerFor(cell);
    const state = cellState(cell);
    state.credentials[id].expires_at = T0 / 1000;
    writeStateText(cell.state, JSON.stringify(state));
    await cache.refresh();
    expect(await answer(await post(issuer, secret))).toEqual([
      403, refusal("credential_not_active", `credential ${id} has expired`),
    ]);
    state.credentials[id].expires_at = T0 / 1000 + 1;
    state.projects.sp_a.phase = "offboarding";
    writeStateText(cell.state, JSON.stringify(state));
    await cache.refresh();
    expect(await answer(await post(issuer, secret))).toEqual([
      403, refusal("project_not_active", "project sp_a is being deleted"),
    ]);
  });

  test("answers 503 issuer_unavailable with Retry-After: 5 once its copy of the state is over 300 s old", async () => {
    const cell = await issuerCell();
    const { secret } = await customerSecret(cell, "sp_a");
    const { issuer, cache, clock } = await issuerFor(cell);
    fakeS3().failReads = true;
    clock.t += 300_000;
    await cache.refresh();
    expect((await post(issuer, secret)).status).toBe(200);
    clock.t += 1_000;
    const stale = await post(issuer, secret);
    expect([stale.status, await stale.json(), stale.headers.get("retry-after")]).toEqual([
      503, refusal("issuer_unavailable", "the issuer cannot see the cell's current state; retry shortly"), "5",
    ]);
    fakeS3().failReads = false;
    await cache.refresh();
    expect((await post(issuer, secret)).status).toBe(200);
  });

  test("answers 503 once the cell's keys feed no longer holds its signing key", async () => {
    const cell = await issuerCell();
    const { secret } = await customerSecret(cell, "sp_a");
    const { issuer, cache } = await issuerFor(cell);
    const next = publicKeyFile(keyFile(cell.root, "ed25519", 0o600, "next.pem"), "next.pub");
    expect((await run("keys", "add", "--state", cell.state, "--public-key", next, "--retire-others")).code).toBe(0);
    await cache.refresh();
    expect(await answer(await post(issuer, secret))).toEqual([
      503, refusal("issuer_unavailable", "the issuer's signing key is not the cell's; retry shortly"),
    ]);
  });

  test("exchanges the canary's watch credential like a customer's", async () => {
    const cell = await issuerCell();
    const out = `${cell.root}/watch-secret`;
    const made = await run("credential", "--state", cell.state, "--project", "sp_canary_cell_sc1", "--hidden",
      "--pepper-file", cell.pepper, "--out", out, "--scopes", "streams.records.append streams.records.read");
    expect(made.code).toBe(0);
    const { issuer } = await issuerFor(cell);
    const body = await (await post(issuer, readFileSync(out, "utf8").trim())).json();
    expect([body.projectId, decodeJwt(body.accessToken).claims.scope]).toEqual([
      "sp_canary_cell_sc1", "streams.records.append streams.records.read",
    ]);
  });

  test("verifies a credential hashed under an older pepper, and writes nothing on the exchange path", async () => {
    const cell = await issuerCell();
    const { secret } = await customerSecret(cell, "sp_a");
    const newer = pepperFile(cell.root, 0o600, 2);
    const both = `v2:${newer.key.toString("base64")},${readFileSync(cell.pepper, "utf8").trim()}`;
    const { issuer } = await issuerFor(cell, envWith(cell, { STREAMS_ISSUER_PEPPER: both }));
    const puts = fakeS3().requests.filter((q) => q.method === "PUT").length;
    expect((await post(issuer, secret)).status).toBe(200);
    expect(fakeS3().requests.filter((q) => q.method === "PUT").length).toBe(puts);
    const { issuer: newOnly } = await issuerFor(cell, envWith(cell, { STREAMS_ISSUER_PEPPER: `v2:${newer.key.toString("base64")}` }));
    expect((await post(newOnly, secret)).status).toBe(401);
  });

  test("answers other methods 405 and other paths 404, with no CORS header", async () => {
    const cell = await issuerCell();
    const { issuer } = await issuerFor(cell);
    for (const method of ["GET", "OPTIONS", "PUT"]) {
      const res = await post(issuer, undefined, undefined, method);
      expect([res.status, res.headers.get("allow"), res.headers.get("access-control-allow-origin")]).toEqual([
        405, "POST", null,
      ]);
    }
    const res = await issuer.fetch(new Request("https://issuer.example.com/v1/token"));
    expect(await answer(res)).toEqual([404, refusal("not_found", "no route /v1/token")]);
  });
});

describe("verifySecret", () => {
  test("matches only a customer credential's secret under the pepper of its hv", () => {
    const peppers = parsePeppers(`v2:${randomBytes(32).toString("base64")},v1:${randomBytes(32).toString("base64")}`, "test");
    const secret = randomBytes(32).toString("base64url");
    const cred = (hv: number, role: "customer" | "operator" = "customer") => ({
      project_id: "p", role, grant_version: 1, status: "active" as const, scopes: "", expires_at: null, omitted: false,
      secret_hash: hashSecret(peppers.find((p) => p.version === hv)!, secret), hv,
    });
    expect(verifySecret(peppers, cred(1), secret)?.version).toBe(1);
    expect(verifySecret(peppers, cred(2), secret)?.version).toBe(2);
    const other = `${secret.slice(0, -1)}${secret.endsWith("A") ? "B" : "A"}`;
    expect(verifySecret(peppers, cred(2), other)).toBeNull();
    expect(verifySecret(peppers, cred(2, "operator"), secret)).toBeNull();
    expect(verifySecret(peppers, { ...cred(2), hv: 3 }, secret)).toBeNull();
    expect(verifySecret(peppers, undefined, secret)).toBeNull();
    const { secret_hash: _, ...noHash } = cred(2);
    expect(verifySecret(peppers, noHash, secret)).toBeNull();
  });
});
