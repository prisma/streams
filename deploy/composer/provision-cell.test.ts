// provision-cell.py (public-beta deploy-q5 triage B5): a stage's project or
// Branch, its three buckets and four keys, with a ledger entry before every
// POST, resolution by name after a lost answer, no POST on a re-run with
// everything in place, the key pairs in the files set-secrets reads, the
// cells file set by key, and a teardown that never takes production.
// Offline, against a fake Management API (fake-platform.ts). Run with
// `bun test ./deploy/composer`.
import { afterAll, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakePlatform } from "./fake-platform";

const TOOL = join(import.meta.dir, "provision-cell.py");
const scratch: string[] = [];
const fakes: FakePlatform[] = [];
afterAll(() => {
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
  for (const f of fakes.splice(0)) f.stop();
});

function rig() {
  const root = mkdtempSync(join(tmpdir(), "provision-cell-"));
  scratch.push(root);
  const api = new FakePlatform();
  fakes.push(api);
  const token = join(root, "token");
  writeFileSync(token, `${api.token}\n`, { mode: 0o600 });
  const cells = join(root, "cells");
  mkdirSync(cells);
  return { root, api, token, cells, secrets: join(root, "secrets") };
}
type Rig = ReturnType<typeof rig>;

async function provision(r: Rig, stage: string, ...extra: string[]) {
  const proc = Bun.spawn(
    ["python3", TOOL, stage, "--app", "streams-cell", "--cells", r.cells, "--management-api", r.api.url, ...extra],
    { env: { PATH: process.env.PATH ?? "/usr/bin:/bin", PRISMA_TOKEN_FILE: r.token }, stdout: "pipe", stderr: "pipe" },
  );
  const [out, err, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  return { code, out, err };
}

const full = (r: Rig) => ["--region", "eu-central-1", "--secrets", r.secrets];
const read = (path: string) => readFileSync(path, "utf8");
const posts = (r: Rig) => r.api.requests.filter((q) => q.method === "POST").map((q) => q.path);
const ledger = (r: Rig, stage: string) => JSON.parse(read(join(r.cells, `${stage}.provision.json`)));

describe("provision-cell.py", () => {
  test("provisions production: the project, three buckets, four keys, the key files, the cells file by key", async () => {
    const r = rig();
    writeFileSync(join(r.cells, "production.env"), "# the cell\nCELL_ID=cell_p1\nREGION=us-east-1\nPATH_PREFIX=cells/cell_p1\n");
    const run = await provision(r, "production", ...full(r));
    expect([run.code, run.err]).toEqual([0, ""]);
    const [p] = r.api.projects;
    expect([p.name, p.defaultRegion, r.api.branches]).toEqual(["streams-cell", "eu-central-1", []]);
    expect(r.api.buckets.map((b) => b.name)).toEqual(["streams-cell-production-data", "streams-cell-production-feeds", "streams-cell-production-control"]);
    const bucketOf = (id: string) => r.api.buckets.find((b) => b.id === id)?.name.split("-").at(-1);
    expect(r.api.keys.map((k) => [k.name, bucketOf(k.bucketId), k.role])).toEqual([
      ["streams-cell-production-data-cell", "data", "read_write"],
      ["streams-cell-production-feeds-cell", "feeds", "read"],
      ["streams-cell-production-feeds-writer", "feeds", "read_write"],
      ["streams-cell-production-control-writer", "control", "read_write"],
    ]);
    const files = [
      "STREAMS_DATA_S3_ACCESS_KEY_ID", "STREAMS_DATA_S3_SECRET_ACCESS_KEY",
      "STREAMS_FEEDS_S3_ACCESS_KEY_ID", "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY",
      "issuer/STREAMS_FEEDS_S3_ACCESS_KEY_ID", "issuer/STREAMS_FEEDS_S3_SECRET_ACCESS_KEY",
      "issuer/STREAMS_CONTROL_S3_ACCESS_KEY_ID", "issuer/STREAMS_CONTROL_S3_SECRET_ACCESS_KEY",
      "cell-admin-s3.json",
    ];
    expect(files.map((f) => statSync(join(r.secrets, f)).mode & 0o777)).toEqual(files.map(() => 0o600));
    const [dataKey, feedsKey] = r.api.keys;
    expect([read(join(r.secrets, "STREAMS_DATA_S3_ACCESS_KEY_ID")), read(join(r.secrets, "STREAMS_FEEDS_S3_ACCESS_KEY_ID"))])
      .toEqual([`AK${dataKey.id}\n`, `AK${feedsKey.id}\n`]);
    expect(read(join(r.cells, "production.env"))).toBe(
      "# the cell\nCELL_ID=cell_p1\nREGION=eu-central-1\nPATH_PREFIX=cells/cell_p1\nCELL_STAGE=production\n" +
        "SLATE_S3_ENDPOINT=https://t3.storage.example\nSLATE_S3_BUCKET=phys-streams-cell-production-data\nSLATE_S3_REGION=auto\n" +
        "FEEDS_S3_ENDPOINT=https://t3.storage.example\nFEEDS_S3_BUCKET=phys-streams-cell-production-feeds\nFEEDS_S3_REGION=auto\n" +
        "FEEDS_S3_KEY=cells/cell_p1/feeds-bundle.json\n",
    );
    const admin = JSON.parse(read(join(r.secrets, "cell-admin-s3.json")));
    expect([admin.bucket, admin.feeds.bucket, admin.region]).toEqual(["phys-streams-cell-production-control", "phys-streams-cell-production-feeds", "auto"]);
    expect(read(join(r.cells, "production.issuer.env"))).toContain("STREAMS_CONTROL_S3_BUCKET=phys-streams-cell-production-control\n");
    const secrets = files.slice(0, 8).map((f) => read(join(r.secrets, f)).trim());
    expect(secrets.filter((s) => run.out.includes(s) || read(join(r.cells, "production.provision.json")).includes(s))).toEqual([]);
  }, 30_000);

  test("a re-run with everything in place makes no POST and changes no file", async () => {
    const r = rig();
    expect((await provision(r, "production", ...full(r), "--cell-id", "cell_p1")).code).toBe(0);
    const before = [read(join(r.cells, "production.env")), read(join(r.secrets, "STREAMS_DATA_S3_SECRET_ACCESS_KEY"))];
    const posted = posts(r).length;
    const again = await provision(r, "production", ...full(r));
    expect([again.code, again.err, posts(r).length, r.api.keys.length]).toEqual([0, "", posted, 4]);
    expect([read(join(r.cells, "production.env")), read(join(r.secrets, "STREAMS_DATA_S3_SECRET_ACCESS_KEY"))]).toEqual(before);
  }, 30_000);

  test("a lost answer leaves a pending ledger entry that the next run resolves by name, never a second bucket", async () => {
    const r = rig();
    r.api.loseNextAnswer = "/v1/buckets";
    const lost = await provision(r, "production", ...full(r), "--cell-id", "cell_p1");
    expect([lost.code, lost.err.trim()]).toEqual([
      1,
      "provision-cell: POST /buckets answered 502 (no error code): its outcome is unknown; the ledger keeps a pending " +
        "entry, run again to resolve it by name",
    ]);
    expect(ledger(r, "production").buckets.data).toMatchObject({ name: "streams-cell-production-data", status: "pending" });
    const again = await provision(r, "production", ...full(r));
    expect([again.code, again.err]).toEqual([0, ""]);
    expect(r.api.buckets.map((b) => b.name)).toEqual(["streams-cell-production-data", "streams-cell-production-feeds", "streams-cell-production-control"]);
    expect(ledger(r, "production").buckets.data.status).toBe("resolved_by_name");
  }, 30_000);

  test("a key whose files are lost is deleted and minted again", async () => {
    const r = rig();
    expect((await provision(r, "production", ...full(r), "--cell-id", "cell_p1")).code).toBe(0);
    const old = r.api.keys.find((k) => k.name.endsWith("feeds-writer"))?.id;
    rmSync(join(r.secrets, "issuer", "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY"));
    const run = await provision(r, "production", ...full(r));
    expect([run.code, run.err]).toEqual([0, ""]);
    const now = r.api.keys.find((k) => k.name.endsWith("feeds-writer"))?.id;
    expect([r.api.keys.length, r.api.keys.some((k) => k.id === old), now === old]).toEqual([4, false, false]);
    expect(read(join(r.secrets, "issuer", "STREAMS_FEEDS_S3_ACCESS_KEY_ID"))).toBe(`AK${now}\n`);
  }, 30_000);

  test("a named stage is a Branch of the app's project, after its production Branch, with buckets of its own", async () => {
    const r = rig();
    const none = await provision(r, "cert", ...full(r), "--cell-id", "cell_c1");
    expect([none.code, none.err.trim()]).toEqual([
      1, "provision-cell: no project is named streams-cell: provision the production stage first (a named stage is its Branch)",
    ]);
    const p = r.api.addProject("streams-cell", "eu-central-1");
    const bare = await provision(r, "cert", ...full(r), "--cell-id", "cell_c1");
    expect([bare.code, bare.err.trim(), r.api.branches]).toEqual([
      1,
      `provision-cell: project ${p.id} has no production Branch yet: the first Branch of a project becomes its production ` +
        "Branch, so cert's can only follow it",
      [],
    ]);
    r.api.addBranch(p.id, "main");
    const run = await provision(r, "cert", ...full(r), "--cell-id", "cell_c1");
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(r.api.branches.map((b) => [b.gitName, b.role])).toEqual([["main", "production"], ["cert", "preview"]]);
    expect(r.api.buckets.map((b) => b.name)).toEqual(["streams-cell-cert-data", "streams-cell-cert-feeds", "streams-cell-cert-control"]);
    expect(run.out).toContain(`set-secrets cert --project ${p.id} --branch ${r.api.branches[1].id} --from ${r.secrets}`);
  }, 30_000);

  test("refuses a project of the app's name in another region, and a stage without a cell id", async () => {
    const r = rig();
    const p = r.api.addProject("streams-cell", "us-east-1");
    const other = await provision(r, "production", ...full(r), "--cell-id", "cell_p1");
    expect([other.code, other.err.trim(), posts(r)]).toEqual([
      1, `provision-cell: project ${p.id} is streams-cell in us-east-1, not streams-cell in eu-central-1`, [],
    ]);
    expect(existsSync(join(r.cells, "production.provision.json"))).toBe(false);
    const r2 = rig();
    const anon = await provision(r2, "production", ...full(r2));
    expect([anon.code, anon.err.trim(), r2.api.requests]).toEqual([
      2, `provision-cell: no cell id: give --cell-id, or run \`cell-admin init --cell-env ${join(r2.cells, "production.env")}\` first`, [],
    ]);
  }, 30_000);

  test("--teardown deletes a named stage's buckets and keys, and refuses production", async () => {
    const r = rig();
    expect((await provision(r, "production", ...full(r), "--cell-id", "cell_p1")).code).toBe(0);
    r.api.addBranch(r.api.projects[0].id, "main");
    expect((await provision(r, "scratch", "--region", "eu-central-1", "--secrets", join(r.root, "s2"), "--cell-id", "cell_s1")).code).toBe(0);
    const prod = await provision(r, "production", "--teardown");
    expect([prod.code, prod.err.trim()]).toEqual([1, "provision-cell: --teardown refuses production: its buckets hold every tenant's data"]);
    const run = await provision(r, "scratch", "--teardown");
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(r.api.buckets.map((b) => b.name)).toEqual(["streams-cell-production-data", "streams-cell-production-feeds", "streams-cell-production-control"]);
    expect([r.api.keys.length, ledger(r, "scratch").buckets.data.status]).toEqual([4, "deleted"]);
    expect(existsSync(join(r.cells, "production.provision.json"))).toBe(true);
  }, 30_000);
});
