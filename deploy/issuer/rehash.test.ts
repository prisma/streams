// Re-hashing under the newest pepper (public-beta issuer triage B2.5;
// skeptic S5g). Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { hashSecret } from "../cell-admin/secrets";
import { cleanup, pepperFile, readBundleText, writeStateText } from "../cell-admin/test-rig";
import { boot } from "./boot";
import { StateCache } from "./cache";
import { readConfig } from "./config";
import { REHASH_DELAY_MS } from "./rehash";
import { CELL, cellState, customerSecret, envWith, fakeS3, type IssuerCell, issuerCell } from "./rig";
import { Issuer } from "./server";

afterEach(cleanup);

const statePuts = () =>
  fakeS3().requests.filter((q) => q.method === "PUT" && q.key.endsWith(`/cells/${CELL}/cell-state.json`)).length;

/// An issuer whose newest pepper is a new v2, the credentials' v1 after it.
async function rotated(cell: IssuerCell, now: () => number = Date.now) {
  const v2 = pepperFile(cell.root, 0o600, 2);
  const peppers = `v2:${v2.key.toString("base64")},${readFileSync(cell.pepper, "utf8").trim()}`;
  const cfg = readConfig(envWith(cell, { STREAMS_ISSUER_PEPPER: peppers }));
  const b = await boot(cfg);
  const cache = new StateCache(b.store, b, { log: () => {}, now });
  const log: string[] = [];
  return { v2, cache, issuer: new Issuer(cfg, { store: b.store, cache, now, log: (l) => log.push(l) }), log };
}

const exchange = (issuer: Issuer, secret: string) =>
  issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
    method: "POST",
    headers: { authorization: `StreamsCredential ${secret}` },
  }));

describe("the re-hash sweep", () => {
  test("re-hashes what exchanges under an older pepper noted, in one write that moves no version", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const b = await customerSecret(cell, "sp_b");
    const c = await customerSecret(cell, "sp_c");
    const { v2, issuer } = await rotated(cell);
    for (const x of [a, b, c, a]) expect((await exchange(issuer, x.secret)).status).toBe(200);
    expect(issuer.rehasher.pending).toBe(3);
    const before = cellState(cell);
    const [puts, bundle] = [statePuts(), readBundleText(cell.state)];
    expect(await issuer.rehasher.sweep()).toBe(3);
    expect([statePuts() - puts, issuer.rehasher.pending]).toEqual([1, 0]);
    const after = cellState(cell);
    for (const x of [a, b, c]) {
      const raw = x.secret.split(".")[1];
      expect([after.credentials[x.id].hv, after.credentials[x.id].secret_hash]).toEqual([2, hashSecret({ version: 2, key: v2.key }, raw)]);
      expect(after.credentials[x.id].grant_version).toBe(before.credentials[x.id].grant_version);
      expect(after.credentials[x.id].hint).toBe(raw.slice(-6));
    }
    expect([after.feed_version, readBundleText(cell.state)]).toEqual([before.feed_version, bundle]);
    expect(after.journal.at(-1)?.command).toBe("rehash");
    // Only the newest pepper is needed now.
    const v2only = readConfig(envWith(cell, { STREAMS_ISSUER_PEPPER: `v2:${v2.key.toString("base64")}` }));
    const booted = await boot(v2only);
    const fresh = new Issuer(v2only, { store: booted.store, cache: new StateCache(booted.store, booted, { log: () => {} }) });
    expect((await exchange(fresh, a.secret)).status).toBe(200);
  });

  test("notes nothing for a credential already under the newest pepper", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const { issuer } = await issuerFor(cell);
    expect((await exchange(issuer, a.secret)).status).toBe(200);
    expect(issuer.rehasher.pending).toBe(0);
  });

  test("leaves a credential whose hash changed since it was noted, and writes nothing for it", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const { issuer, cache } = await rotated(cell);
    expect((await exchange(issuer, a.secret)).status).toBe(200);
    const state = cellState(cell);
    state.credentials[a.id].secret_hash = "rotated-meanwhile";
    writeStateText(cell.state, JSON.stringify(state));
    await cache.refresh();
    const puts = statePuts();
    expect(await issuer.rehasher.sweep()).toBe(0);
    expect([statePuts(), cellState(cell).credentials[a.id].secret_hash]).toEqual([puts, "rotated-meanwhile"]);
  });

  test("keeps what it noted when the write fails, and writes it at the next sweep", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const { issuer } = await rotated(cell);
    expect((await exchange(issuer, a.secret)).status).toBe(200);
    fakeS3().denyWrites = true;
    await expect(issuer.rehasher.sweep()).rejects.toThrow("answered 403 AccessDenied");
    expect(issuer.rehasher.pending).toBe(1);
    fakeS3().denyWrites = false;
    expect(await issuer.rehasher.sweep()).toBe(1);
    expect(cellState(cell).credentials[a.id].hv).toBe(2);
  });

  test("writes what it noted 10 minutes after the first note, not a day after boot", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const b = await customerSecret(cell, "sp_b");
    const clock = { t: Date.now() };
    const { issuer } = await rotated(cell, () => clock.t);
    expect((await exchange(issuer, a.secret)).status).toBe(200);
    clock.t += 5 * 60_000;
    expect((await exchange(issuer, b.secret)).status).toBe(200);
    const puts = statePuts();
    clock.t += REHASH_DELAY_MS - 5 * 60_000 - 1;
    await issuer.rehasher.tick();
    expect([statePuts(), issuer.rehasher.pending]).toEqual([puts, 2]);
    clock.t += 1;
    await issuer.rehasher.tick();
    expect([statePuts(), issuer.rehasher.pending]).toEqual([puts + 1, 0]);
    expect([cellState(cell).credentials[a.id].hv, cellState(cell).credentials[b.id].hv]).toEqual([2, 2]);
    expect(REHASH_DELAY_MS).toBe(10 * 60_000);
  });

  test("sweeps on its interval once started", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const { issuer, log } = await rotated(cell);
    expect((await exchange(issuer, a.secret)).status).toBe(200);
    const stop = issuer.rehasher.start(20, 0);
    try {
      const deadline = Date.now() + 2_000;
      while (log.length === 0 && Date.now() < deadline) await Bun.sleep(10);
    } finally {
      stop();
    }
    expect(cellState(cell).credentials[a.id].hv).toBe(2);
    expect(log).toContain("re-hashed 1 credential(s) under pepper v2");
  });
});

async function issuerFor(cell: IssuerCell) {
  const cfg = readConfig(cell.env);
  const b = await boot(cfg);
  const cache = new StateCache(b.store, b, { log: () => {} });
  return { issuer: new Issuer(cfg, { store: b.store, cache }) };
}
