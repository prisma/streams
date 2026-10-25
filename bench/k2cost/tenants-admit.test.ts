// tenants-admit.ts on a real cell-admin state (the shipped profiles, a key
// made for the test): a manifest's projects admitted through cell-admin's
// CLI with their quota sets, a rerun that admits nothing, the token map
// (one verifiable 12 h token per project, mode 0600), its 8-hourly
// re-mint and upload, and the bundle's publication, against a fake S3.
// Run with `bun test ./bench/k2cost`.
import { afterAll, expect, test } from "bun:test";
import { chmodSync, existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { canaryProjectId } from "../../deploy/cell-admin/rules.ts";
import { cleanup, keyFile, readBundle, readState, scratchDir, verifiedClaims } from "../../deploy/cell-admin/test-rig.ts";
import { Opts } from "./common.ts";
import { expand, type Manifest } from "./tenant-plan.ts";
import { REFRESH_SECS, TOKEN_TTL_SECS, main, tokens, writeFeeds } from "./tenants-admit.ts";

/** A fake S3 endpoint: PUT stores, a ranged GET reads back. */
const objects = new Map<string, Uint8Array>();
const s3 = Bun.serve({
  port: 0, hostname: "127.0.0.1",
  async fetch(req) {
    const path = new URL(req.url).pathname;
    if (req.method === "PUT") {
      objects.set(path, new Uint8Array(await req.arrayBuffer()));
      return new Response(null, { status: 200, headers: { etag: '"x"' } });
    }
    const body = objects.get(path);
    if (!body) return new Response("<Error><Code>NoSuchKey</Code></Error>", { status: 404 });
    const range = /bytes=(\d+)-(\d+)/.exec(req.headers.get("range") ?? "");
    if (!range) return new Response(body);
    const [a, b] = [Number(range[1]), Number(range[2])];
    return new Response(body.slice(a, b + 1), {
      status: 206, headers: { "content-range": `bytes ${a}-${Math.min(b, body.length - 1)}/${body.length}` },
    });
  },
});
afterAll(() => {
  s3.stop(true);
  cleanup();
});

const manifest: Manifest = {
  name: "adm", seed: 1, duration: 10, quota_sets: { "q-hostile": { requests_per_sec: 20, max_streams: 50 } },
  classes: [
    { class: "app", count: 3, role: "compliant", phases: [] },
    { class: "flood", count: 1, role: "hostile", quotas: "q-hostile", phases: [] },
    { class: "cold", count: 2, role: "idle", phases: [] },
  ],
};

function world() {
  const root = scratchDir();
  const file = join(root, "m.json");
  writeFileSync(file, JSON.stringify(manifest));
  const creds = join(root, "s3.json");
  writeFileSync(creds, JSON.stringify({ accessKeyId: "id", secretAccessKey: "secret" }), { mode: 0o600 });
  return { root, file, key: keyFile(root), state: join(root, "state"), creds, endpoint: `http://127.0.0.1:${s3.port}` };
}

async function cmd(...argv: string[]): Promise<{ code: number; out: string[] }> {
  const out: string[] = [];
  const code = await main(argv, (l) => out.push(l));
  return { code, out };
}

test("admit places every project with its workspace and quota set through cell-admin, once", async () => {
  const w = world();
  const ids = ["--cell-id", "cell_t", "--deployment-project", "proj_dep", "--account", "acct_sink"];
  const first = await cmd("admit", "--manifest", w.file, "--state", w.state, "--key-file", w.key, ...ids);
  expect([first.code, first.out.at(-1)]).toEqual([0, "admit: 6 admitted, 0 already held, 0 failed of 6"]);
  const st = readState(w.state);
  const plan = expand(manifest, "");
  // The manifest's projects, beside the canary cell-admin's init places.
  expect(Object.keys(st.projects).sort()).toEqual([...plan.projects.map((p) => p.id), canaryProjectId("cell_t")].sort());
  for (const p of plan.projects) expect(st.projects[p.id]!.workspace_id).toBe(p.workspace);
  const flood = st.projects["proj_adm_flood_0000"]!.quotas;
  const app = st.projects["proj_adm_app_0000"]!.quotas;
  expect([flood.requests_per_sec, flood.max_streams, flood.read_bytes_per_sec]).toEqual([20, 50, st.cell.ceilings.read_bytes_per_sec]);
  expect([app.requests_per_sec, app.max_streams]).toEqual([st.cell.ceilings.requests_per_sec, st.cell.ceilings.max_streams]);
  const again = await cmd("admit", "--manifest", w.file, "--state", w.state, "--key-file", w.key);
  expect([again.code, again.out.at(-1)]).toEqual([0, "admit: 0 admitted, 6 already held, 0 failed of 6"]);
  await expect(cmd("admit", "--manifest", w.file, "--state", join(w.root, "other"), "--key-file", w.key))
    .rejects.toThrow("holds no cell: give --cell-id, --deployment-project and --account to init it");
}, 30_000);

test("tokens writes one verifiable 12 h token per project, re-mints them on its period and uploads each map", async () => {
  const w = world();
  await cmd("admit", "--manifest", w.file, "--state", w.state, "--key-file", w.key,
    "--cell-id", "cell_t", "--deployment-project", "proj_dep", "--account", "acct_sink");
  const map = join(w.root, "tokens.json");
  const r = await cmd("tokens", "--manifest", w.file, "--state", w.state, "--key-file", w.key, "--out", map);
  expect(r.code).toBe(0);
  expect(statSync(map).mode & 0o777).toBe(0o600);
  const list = JSON.parse(readFileSync(map, "utf8")) as Array<{ project: string; token: string }>;
  const plan = expand(manifest, "");
  expect(list.map((e) => e.project)).toEqual(plan.projects.map((p) => p.id));
  const bundle = readBundle(w.state);
  for (const e of list) {
    const c = verifiedClaims(bundle, e.token);
    expect([c?.project_id, c.exp - c.iat, c.sub]).toEqual([e.project, TOKEN_TTL_SECS, "tenants-harness"]);
  }
  expect(r.out.join("\n")).not.toContain(list[0]!.token);
  expect([TOKEN_TTL_SECS, REFRESH_SECS]).toEqual([43_200, 28_800]);

  // Re-minted every second (a 120 s life here), each map uploaded.
  const o = new Opts(new Map([
    ["state", w.state], ["key-file", w.key], ["out", map], ["ttl", "120"], ["every", "1"],
    ["upload", "s3://maps/run/tokens.json"], ["s3-endpoint", w.endpoint], ["s3-creds", w.creds],
  ]));
  const ctl = new AbortController();
  setTimeout(() => ctl.abort(), 1500);
  const said: string[] = [];
  expect(await tokens(plan, o, (l) => said.push(l), ctl.signal)).toBe(2);
  const second = readFileSync(map, "utf8");
  expect(second).not.toBe(JSON.stringify(list));
  expect(new TextDecoder().decode(objects.get("/maps/run/tokens.json"))).toBe(second);
  expect(said[1]).toBe(`token map ${map}: 6 tokens, 120 s, uploaded to run/tokens.json (round 2)`);

  chmodSync(w.creds, 0o644);
  await expect(tokens(plan, o, () => {}, ctl.signal)).rejects.toThrow("group or others can read it");
  chmodSync(w.creds, 0o600);
  await expect(tokens(plan, new Opts(new Map([...o.flags, ["every", "100"]])), () => {}, ctl.signal))
    .rejects.toThrow("leaves under a third of the 120 s token life");
  const pub = await cmd("publish", "--state", w.state, "--upload", "s3://feeds/cells/cell_t/feeds-bundle.json",
    "--s3-endpoint", w.endpoint, "--s3-creds", w.creds);
  expect(pub).toEqual({ code: 0, out: ["published feed_version 7 to cells/cell_t/feeds-bundle.json"] });
  expect(new TextDecoder().decode(objects.get("/feeds/cells/cell_t/feeds-bundle.json"))).toBe(readFileSync(join(w.state, "feeds-bundle.json"), "utf8"));
}, 30_000);

test("feeds writes the bundle's three feeds as a native cell reads them, and refuses a bundle that lacks one", async () => {
  // run-local.sh's tenants mode boots the binary with no wrapper: its
  // STREAMS_AUTH_{KEYS,POLICY,GRANTS}_FILE are these files, as
  // deploy/app-server's poller would have written them from the bundle.
  const w = world();
  await cmd("admit", "--manifest", w.file, "--state", w.state, "--key-file", w.key,
    "--cell-id", "cell_t", "--deployment-project", "proj_dep", "--account", "acct_sink");
  const feeds = join(w.root, "feeds");
  const r = await cmd("feeds", "--state", w.state, "--out", feeds);
  const bundle = readBundle(w.state);
  expect(r).toEqual({ code: 0, out: [`feeds ${feeds}: feed_version ${bundle.keys.feed_version}`] });
  for (const name of ["keys", "policies", "grants"] as const) {
    expect([name, JSON.parse(readFileSync(join(feeds, `${name}.json`), "utf8"))]).toEqual([name, bundle[name]]);
    expect(existsSync(join(feeds, `${name}.json.tmp`))).toBe(false);
  }
  // Every admitted project is in the policy feed the cell will read, the
  // canary beside them.
  const plan = expand(manifest, "");
  const ids = (JSON.parse(readFileSync(join(feeds, "policies.json"), "utf8")).projects as Array<{ project_id: string }>).map((p) => p.project_id);
  expect(ids.sort()).toEqual([...plan.projects.map((p) => p.id), canaryProjectId("cell_t")].sort());

  const torn = join(w.root, "torn");
  const bad = join(w.root, "bad-state");
  await cmd("admit", "--manifest", w.file, "--state", bad, "--key-file", w.key,
    "--cell-id", "cell_u", "--deployment-project", "proj_dep", "--account", "acct_sink");
  const doc = JSON.parse(readFileSync(join(bad, "feeds-bundle.json"), "utf8"));
  delete doc.grants;
  writeFileSync(join(bad, "feeds-bundle.json"), JSON.stringify(doc));
  expect(() => writeFeeds(bad, torn)).toThrow("feeds-bundle.json has no grants feed");
  expect(existsSync(torn)).toBe(false);
}, 30_000);
