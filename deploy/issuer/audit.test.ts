// The issuer's audit trail (public-beta issuer triage B2.8). Run with
// `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { auditTexts, cleanup, decodeJwt, pepperFile } from "../cell-admin/test-rig";
import { Audit, JTIS_PER_HOUR, UNKNOWN_CREDENTIAL } from "./audit";
import { boot } from "./boot";
import { StateCache } from "./cache";
import { type Env, readConfig } from "./config";
import { CELL, customerSecret, envWith, type IssuerCell, issuerCell, wrongSecret } from "./rig";
import { Issuer } from "./server";

afterEach(cleanup);

const T0 = 1_800_000_000_000; // a whole hour

async function issuerFor(cell: IssuerCell, env: Env = cell.env) {
  const clock = { t: T0 };
  const now = () => clock.t;
  const log: string[] = [];
  const cfg = readConfig(env);
  const b = await boot(cfg);
  const cache = new StateCache(b.store, b, { now, log: () => {} });
  return { clock, log, store: b.store, issuer: new Issuer(cfg, { store: b.store, cache, now, log: (l) => log.push(l) }) };
}

const exchange = (issuer: Issuer, auth: string) =>
  issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
    method: "POST",
    headers: { authorization: `StreamsCredential ${auth}` },
  }));

const lines = (log: string[]) => log.filter((l) => l.startsWith("{")).map((l) => JSON.parse(l));

describe("the exchange audit", () => {
  test("counts exchanges per credential and outcome, and logs the minute's counts when it ends", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const { issuer, clock, log } = await issuerFor(cell);
    for (const auth of [a.secret, a.secret, wrongSecret(a.id), wrongSecret(`strcred_${"0".repeat(32)}`), "nonsense"]) {
      await exchange(issuer, auth);
    }
    issuer.audit.tick();
    expect(lines(log)).toEqual([]);
    clock.t += 60_000;
    issuer.audit.tick();
    expect(lines(log)).toEqual([
      { kind: "exchanges", minute: new Date(T0).toISOString(), cell_id: CELL, credential_id: a.id, project_id: "sp_a",
        outcomes: { issued: 2, invalid_credential: 1 } },
      { kind: "exchanges", minute: new Date(T0).toISOString(), cell_id: CELL, credential_id: UNKNOWN_CREDENTIAL,
        outcomes: { invalid_credential: 2 } },
    ]);
  });

  test("writes the hour's counts and the jti of every minted token to one audit object, holding no secret or token", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const { issuer, clock, log } = await issuerFor(cell);
    const tokens: string[] = [];
    for (let i = 0; i < 3; i++) tokens.push((await (await exchange(issuer, a.secret)).json()).accessToken);
    await exchange(issuer, wrongSecret(a.id));
    clock.t += 3_600_000;
    issuer.audit.tick();
    await issuer.audit.flush();
    const hours = auditTexts(cell.state).map((t) => JSON.parse(t)).filter((d) => d.kind === "exchanges_hour");
    expect(hours).toEqual([{
      kind: "exchanges_hour", hour: new Date(T0).toISOString(), cell_id: CELL,
      credentials: {
        [a.id]: {
          project_id: "sp_a", outcomes: { issued: 3, invalid_credential: 1 },
          jtis: tokens.map((t) => decodeJwt(t).claims.jti), jtis_dropped: 0,
        },
      },
    }]);
    const everything = [...auditTexts(cell.state), ...log].join("\n");
    expect(everything).not.toContain(a.secret.split(".")[1]);
    for (const t of tokens) expect(everything).not.toContain(t.split(".")[2]);
  });

  test("keeps at most 10,000 jtis a credential an hour, and counts the rest", async () => {
    const cell = await issuerCell();
    const { store } = await issuerFor(cell);
    const clock = { t: T0 };
    const audit = new Audit(store, CELL, { now: () => clock.t, log: () => {} });
    for (let i = 0; i <= JTIS_PER_HOUR; i++) audit.exchange("issued", "strcred_x", "sp_x", `jti${i}`);
    await audit.flush();
    const [hour] = auditTexts(cell.state).map((t) => JSON.parse(t)).filter((d) => d.kind === "exchanges_hour");
    expect([hour.credentials.strcred_x.jtis.length, hour.credentials.strcred_x.jtis_dropped]).toEqual([10_000, 1]);
  });

  test("flush closes the open minute and hour at shutdown", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const { issuer, log } = await issuerFor(cell);
    await exchange(issuer, a.secret);
    await issuer.audit.flush();
    expect(lines(log).map((l) => [l.kind, l.outcomes])).toEqual([["exchanges", { issued: 1 }]]);
    expect(auditTexts(cell.state).map((t) => JSON.parse(t).kind)).toContain("exchanges_hour");
  });
});

describe("the management audit", () => {
  test("writes one object per change, with who, what, the ids and the versions", async () => {
    const cell = await issuerCell();
    const { store, clock } = await issuerFor(cell);
    const audit = new Audit(store, CELL, { now: () => clock.t });
    await audit.record({
      actor: { user_id: "usr_1", email: "a@example.com", workspace_id: "ws_1" },
      action: "credential.revoke", outcome: "done", project_id: "sp_a", credential_ids: ["strcred_x"],
      before: { grant_version: 1, feed_version: 4 }, after: { grant_version: 2, feed_version: 5 }, request_id: "req_1",
    });
    const docs = auditTexts(cell.state).map((t) => JSON.parse(t));
    expect(docs.at(-1)).toEqual({
      kind: "management", at: new Date(T0).toISOString(), cell_id: CELL,
      actor: { user_id: "usr_1", email: "a@example.com", workspace_id: "ws_1" },
      action: "credential.revoke", outcome: "done", project_id: "sp_a", credential_ids: ["strcred_x"],
      before: { grant_version: 1, feed_version: 4 }, after: { grant_version: 2, feed_version: 5 }, request_id: "req_1",
    });
  });

  test("records the re-hash sweep's write with the credentials it re-hashed", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const v2 = pepperFile(cell.root, 0o600, 2);
    const peppers = `v2:${v2.key.toString("base64")},${readFileSync(cell.pepper, "utf8").trim()}`;
    const { issuer } = await issuerFor(cell, envWith(cell, { STREAMS_ISSUER_PEPPER: peppers }));
    expect((await exchange(issuer, a.secret)).status).toBe(200);
    expect(await issuer.rehasher.sweep()).toBe(1);
    const record = auditTexts(cell.state).map((t) => JSON.parse(t)).find((d) => d.action === "credential.rehash");
    expect(record).toMatchObject({
      kind: "management", actor: "issuer:rehash", outcome: "done", reason: "re-hashed under pepper v2",
      credential_ids: [a.id],
    });
  });
});
