// The signing key's rotation and compromise drills (public-beta issuer
// triage B5.3; design §7.3, §7.4), step by step in the order
// deploy/cell-admin/README.md gives them, against the fake cell
// (deploy/cell-admin/fake-cell.ts). Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { FakeCell } from "../cell-admin/fake-cell";
import { cleanup, decodeJwt, keyFile, publicKeyFile, readBundle, run } from "../cell-admin/test-rig";
import { cellState, customerSecret, envWith, type IssuerCell, issuerCell, managed } from "./rig";

const fakes: FakeCell[] = [];
afterEach(() => {
  for (const f of fakes.splice(0)) f.stop();
  cleanup();
});

type Managed = Awaited<ReturnType<typeof managed>>;

/// A cell with one customer credential, the fake cell serving its bundle,
/// and the issuer deployed with the cell's first key.
async function deployed() {
  const cell = await issuerCell();
  const a = await customerSecret(cell, "sp_a");
  const fake = new FakeCell();
  fakes.push(fake);
  fake.publish(readBundle(cell.state));
  const env = envWith(cell, { STREAMS_CELL_ORIGIN: fake.url });
  const old = await managed(cell, env);
  const [oldKid] = cellState(cell).keys.map((k) => k.kid);
  return { cell, a, fake, env, old, oldKid };
}

/// The new key made offline (step 1), and its public half.
const newKey = (cell: IssuerCell) => {
  const pem = keyFile(cell.root, "ed25519", 0o600, "new.pem");
  return { pem, pub: publicKeyFile(pem, "new.pub") };
};

/// A token from `m`'s exchange for `secret`, or the refusal's status.
async function mint(m: Managed, secret: string): Promise<string | number> {
  const res = await m.issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
    method: "POST",
    headers: { authorization: `StreamsCredential ${secret}` },
  }));
  return res.status === 200 ? (await res.json()).accessToken : res.status;
}

/// What the cell answers a catalog read under `token`.
const read = async (fake: FakeCell, token: string | number) =>
  (await fetch(`${fake.url}/v1/streams?limit=1`, { headers: { authorization: `Bearer ${token}` } })).status;

const health = async (m: Managed) => {
  const res = await m.get("/healthz");
  return [res.status, await res.json()];
};

/// cell-admin on the cell's state: its exit code and its error output.
const admin = async (cell: IssuerCell, ...argv: string[]) => {
  const r = await run(...argv, "--state", cell.state);
  return [r.code, r.err];
};

describe("the key drills", () => {
  test("rotation: overlap, canary, redeploy, retire; no token fails until the old kid is retired", async () => {
    const { cell, a, fake, env, old, oldKid } = await deployed();
    const before = await mint(old, a.secret);
    expect(await read(fake, before)).toBe(200);
    // 1. The new key; redeploying with it now is refused at boot.
    const next = newKey(cell);
    const nextEnv = { ...env, STREAMS_ISSUER_SIGNING_KEY: readFileSync(next.pem, "utf8") };
    await expect(managed(cell, nextEnv)).rejects.toThrow(/is not a key of cell cell_sc1: publish its public half with cell-admin keys add/);
    // 2. The overlap generation: the running issuer keeps minting.
    expect(await admin(cell, "keys", "add", "--public-key", next.pub)).toEqual([0, ""]);
    fake.publish(readBundle(cell.state));
    await old.cache.refresh();
    const newKid = cellState(cell).keys.map((k) => k.kid).find((k) => k !== oldKid)!;
    expect(cellState(cell).keys.map((k) => k.kid)).toEqual([oldKid, newKid]);
    expect(await health(old)).toEqual([200, { status: "ok" }]);
    expect(await read(fake, await mint(old, a.secret))).toBe(200);
    // 3. The canary under the new key.
    expect(await admin(cell, "canary", "--key-file", next.pem, "--cell-url", fake.url)).toEqual([0, ""]);
    // 4. The redeployed issuer mints under the new kid; old tokens still work.
    const redeployed = await managed(cell, nextEnv);
    const after = await mint(redeployed, a.secret);
    expect(decodeJwt(after as string).header.kid).toBe(newKid);
    expect([await read(fake, after), await read(fake, before)]).toEqual([200, 200]);
    // 5. The old kid retired: its tokens answer 401, the new ones 200.
    expect(await admin(cell, "keys", "retire", "--kid", oldKid)).toEqual([0, ""]);
    fake.publish(readBundle(cell.state));
    expect([await read(fake, before), await read(fake, after)]).toEqual([401, 200]);
    await redeployed.cache.refresh();
    expect(await health(redeployed)).toEqual([200, { status: "ok" }]);
    expect(cellState(cell).retired_kids).toEqual([oldKid]);
    // An instance left on the old key would mint nothing.
    await old.cache.refresh();
    expect([await health(old), await mint(old, a.secret)]).toEqual([
      [503, { status: "unavailable", reason: "key_not_in_state" }], 503,
    ]);
  });

  test("compromise: the new kid alone; the running issuer stops minting and every outstanding token answers 401", async () => {
    const { cell, a, fake, env, old, oldKid } = await deployed();
    const outstanding = await mint(old, a.secret);
    expect(await read(fake, outstanding)).toBe(200);
    // 1-2. The new kid alone, published.
    const next = newKey(cell);
    expect(await admin(cell, "keys", "add", "--public-key", next.pub, "--retire-others")).toEqual([0, ""]);
    fake.publish(readBundle(cell.state));
    expect(await read(fake, outstanding)).toBe(401);
    expect(cellState(cell).retired_kids).toEqual([oldKid]);
    await old.cache.refresh();
    expect([await health(old), await mint(old, a.secret)]).toEqual([
      [503, { status: "unavailable", reason: "key_not_in_state" }], 503,
    ]);
    // 3. The redeployed issuer: a client's refresh gets a working token.
    const redeployed = await managed(cell, { ...env, STREAMS_ISSUER_SIGNING_KEY: readFileSync(next.pem, "utf8") });
    const fresh = await mint(redeployed, a.secret);
    expect(decodeJwt(fresh as string).header.kid).not.toBe(oldKid);
    expect(await read(fake, fresh)).toBe(200);
  });
});
