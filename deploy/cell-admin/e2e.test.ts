// cell-admin against the real server, booted on the shared-cell profile
// exactly as layered for Compute (compute-1g.env, then shared-cell.env),
// over s3lite. Opt-in, because it needs built binaries:
//
//   cargo build --locked --bin streams-slate --bin s3lite
//   CELL_ADMIN_E2E_BIN_DIR=target/debug bun test ./deploy/cell-admin/e2e.test.ts
//
// It proves what the fake cell of offboard.test.ts assumes: the bundle
// boots an enforce cell with BILLING_MODE=required, minted tokens verify,
// a published revocation answers 403 credential_not_active, offboarding
// deletes every stream of the project and nothing of its neighbour (which
// uses the same stream name), and an omitted project's tokens answer 421.
import { afterAll, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { DEFAULT_PROFILES } from "./commands";
import { readProfiles } from "./profile";
import { cleanup, customerCredential, initCell, readBundle, run, scratchDir } from "./test-rig";

const BIN_DIR = process.env.CELL_ADMIN_E2E_BIN_DIR;
const maybe = BIN_DIR ? test : test.skip;
const children: ReturnType<typeof Bun.spawn>[] = [];

afterAll(() => {
  for (const c of children) c.kill();
  cleanup();
});

function freePort(): number {
  const s = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const port = s.port;
  s.stop(true);
  return port;
}

/// What deploy/app-server does with a bundle: three files, each replaced
/// atomically.
function publish(stateDir: string, feeds: string): number {
  const b = readBundle(stateDir);
  for (const [name, doc] of [["keys", b.keys], ["policies", b.policies], ["grants", b.grants]] as const) {
    writeFileSync(join(feeds, `${name}.json.tmp`), JSON.stringify(doc));
    renameSync(join(feeds, `${name}.json.tmp`), join(feeds, `${name}.json`));
  }
  return b.keys.feed_version;
}

async function until<T>(what: string, secs: number, probe: () => Promise<T | undefined>): Promise<T> {
  for (let i = 0; i < secs * 4; i++) {
    const v = await probe().catch(() => undefined);
    if (v !== undefined) return v;
    await Bun.sleep(250);
  }
  throw new Error(`timed out waiting for ${what}`);
}

const KEY = randomBytes(32).toString("base64");

maybe("a shared cell admits, serves and offboards projects on the real binary", async () => {
  const c = await initCell();
  const feeds = join(scratchDir(), "feeds");
  mkdirSync(feeds);
  for (const [p, w] of [["proj_a", "wksp_a"], ["proj_b", "wksp_b"]]) {
    expect((await run("admit", "--state", c.state, "--project", p, "--workspace", w)).code).toBe(0);
  }
  publish(c.state, feeds);
  const [s3Port, port] = [freePort(), freePort()];
  const bin = resolve(BIN_DIR ?? "");
  children.push(Bun.spawn([join(bin, "s3lite"), "--listen", `127.0.0.1:${s3Port}`, "--latency-ms", "2"],
    { stdout: "ignore", stderr: "ignore" }));
  const operator = randomBytes(24).toString("hex");
  const env: Record<string, string> = {
    PATH: process.env.PATH ?? "",
    HOME: process.env.HOME ?? "",
    ...Object.fromEntries(readProfiles(DEFAULT_PROFILES)),
    STREAMS_AUTH_KEYS_FILE: join(feeds, "keys.json"),
    STREAMS_AUTH_POLICY_FILE: join(feeds, "policies.json"),
    STREAMS_AUTH_GRANTS_FILE: join(feeds, "grants.json"),
    STREAMS_AUTH_REFRESH_SECS: "1",
    CELL_ID: "cell_sc1",
    PROJECT_ID: "proj_deploy",
    ACCOUNT_ID: "acct_sink",
    USAGE_STREAM_KEY: randomBytes(32).toString("base64url"),
    AUTH_TOKEN: operator,
  };
  const log = join(c.root, "server.log");
  children.push(Bun.spawn([join(bin, "streams-slate"), "--listen", `127.0.0.1:${port}`,
    "--s3-endpoint", `http://127.0.0.1:${s3Port}`, "--bucket", `cell-admin-e2e-${Date.now()}`],
    { env, stdout: Bun.file(log), stderr: Bun.file(log) }));
  const cell = `http://127.0.0.1:${port}`;
  await until("the cell to be ready", 90, async () => ((await fetch(`${cell}/health`)).ok ? true : undefined));

  const tokens: Record<string, string> = {};
  for (const p of ["proj_a", "proj_b"]) {
    const out = join(c.root, `${p}.jwt`);
    expect((await run("token", "--state", c.state, "--credential", customerCredential(c.state, p),
      "--key-file", c.key, "--out", out)).code).toBe(0);
    tokens[p] = readFileSync(out, "utf8");
  }
  const as = (p: string, method: string, path: string, body?: string) =>
    fetch(`${cell}${path}`, {
      method,
      headers: { authorization: `Bearer ${tokens[p]}`, "content-type": "application/json", "prisma-encryption-key": KEY },
      body,
    });
  // The status, and the body too when it is not the one wanted, so a
  // failure names the refusal (its code and message).
  const answered = async (res: Response, want: number) =>
    res.status === want ? `${want}` : `${res.status} ${await res.text()}`;
  const created = async (p: string, name: string) =>
    answered(await as(p, "PUT", `/v1/streams/${name}`, JSON.stringify({ format: { kind: "json" } })), 201);
  for (const name of ["orders", "dir/x", "events"]) expect(await created("proj_a", name)).toBe("201");
  expect(await created("proj_b", "orders")).toBe("201");
  // At boot the billing sweep opens the cell's one shard to probe it for
  // debt and, finding none on a new cell, closes it and holds it off for
  // 3 s (edge record #17). Until a customer request adopts the shard, a
  // request to it can be refused retryably: 503 temporarily_unavailable
  // inside the holdoff, or 429 rate_limited when it joined the probe's
  // open and the sweep retired the engine before the request adopted it.
  // The appends below need the shard serving the projects, so wait for
  // exactly that: the first open has completed with none in flight (the
  // open gate's counters, behind the deployment bearer), and a read sent
  // after that is served, which adopts the shard so no sweep closes it.
  const shardOpens = async () =>
    (await (await fetch(`${cell}/v1/debug/store`, { headers: { authorization: `Bearer ${operator}` } })).json())
      .shard_opens as { completed: number; in_flight: number };
  await until("the shard to serve after the boot sweep's probe", 30, async () => {
    const opens = await shardOpens();
    const read = await as("proj_a", "GET", "/v1/streams/orders/records?cursor=beginning");
    await read.arrayBuffer();
    return opens.completed >= 1 && opens.in_flight === 0 && read.status === 200 ? true : undefined;
  });
  const appended = async (p: string, body: string) =>
    answered(await as(p, "POST", "/v1/streams/orders/records", body), 200);
  expect(await appended("proj_a", '{"m":"a-marker"}')).toBe("200");
  expect(await appended("proj_b", '{"m":"b-marker"}')).toBe("200");

  // Offboarding, first run: revoke and publish; second run: prove, walk, omit.
  expect((await run("offboard", "--state", c.state, "--project", "proj_a")).code).toBe(0);
  publish(c.state, feeds);
  const done = await until("offboarding to finish", 60, async () => {
    const r = await run("offboard", "--state", c.state, "--project", "proj_a", "--cell-url", cell,
      "--key-file", c.key, "--settle-secs", "2");
    if (r.code === 3) return undefined;
    return r;
  });
  expect([done.code, done.err]).toEqual([0, ""]);
  console.log(done.out);
  const walks = done.out.split("\n").filter((l) => l.startsWith("walk "));
  expect(walks[0]).toBe("walk 1: listed 3, deleted 3");
  expect(walks[walks.length - 1]).toMatch(/^walk \d+: listed 0, deleted 0$/);
  const omitted = publish(c.state, feeds);
  expect(omitted).toBe(5);

  // The omitted project's tokens are placement refusals; the neighbour
  // keeps its stream of the same name and its record.
  await until("the omission to reach the cell", 30, async () =>
    (await as("proj_a", "GET", "/v1/streams")).status === 421 ? true : undefined);
  const list = await (await as("proj_b", "GET", "/v1/streams")).json();
  expect(list.streams.map((s: { name: string }) => s.name)).toEqual(["orders"]);
  const read = await as("proj_b", "GET", "/v1/streams/orders/records?cursor=beginning");
  expect(await read.json()).toEqual([{ m: "b-marker" }]);
  expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_z")).err)
    .toBe("cell-admin admit: project id proj_a was used before: an id is never placed twice");
}, 240_000);
