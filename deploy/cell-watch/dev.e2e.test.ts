// The cell watch's app in `prisma dev` against a local shared cell (public-
// beta alerting triage B4's dev e2e): the real binary over s3lite on the
// shared-cell profile with cell-admin's feeds, as cell-admin's e2e test
// boots it, and the watch deployed by Composer's local target from app.ts,
// with a local webhook sink and heartbeat monitor. Opt-in, because it
// needs built binaries, this directory's packages and Composer's local
// emulator (`prisma dev` builds nothing: the test bundles entry.js first):
//
//   cargo build --locked --bin streams-slate --bin s3lite
//   (cd deploy/cell-watch && bun install --frozen-lockfile)
//   CELL_WATCH_E2E_BIN_DIR=target/debug bun test ./deploy/cell-watch/dev.e2e.test.ts
//
// `prisma dev` runs with an emulator directory of its own (`--fresh`), whose
// compute daemon the test stops at the end. Keep-awake cannot be tested
// here: off Compute the guard is a no-op (triage N4), and the boot line
// says `keep_awake unavailable`.
import { afterAll, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { DEFAULT_PROFILES } from "../cell-admin/commands";
import { readProfiles } from "../cell-admin/profile";
import { cleanup, initCell, readBundle, scratchDir } from "../cell-admin/test-rig";
import { FakeServer } from "./fake-cell";

const BIN_DIR = process.env.CELL_WATCH_E2E_BIN_DIR;
const maybe = BIN_DIR ? test : test.skip;
const HERE = import.meta.dir;
const BEAT = "/beat/heartbeat-secret-dev-e2e";
const SINK_BEARER = `webhook-bearer-${randomBytes(8).toString("hex")}`;
const OPERATOR = randomBytes(24).toString("hex");

const children: ReturnType<typeof Bun.spawn>[] = [];
const servers: FakeServer[] = [];
let emulators: string | undefined;

afterAll(async () => {
  for (const c of children.reverse()) {
    c.kill("SIGINT");
    await Promise.race([c.exited, Bun.sleep(10_000)]);
    c.kill("SIGKILL");
  }
  // The compute daemon `prisma dev` started for this emulator directory.
  const daemon = emulators && join(emulators, "compute.json");
  if (daemon && existsSync(daemon)) {
    try {
      process.kill(JSON.parse(readFileSync(daemon, "utf8")).pid, "SIGTERM");
    } catch {}
  }
  for (const s of servers) s.stop();
  cleanup();
});

function freePort(): number {
  const s = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const port = Number(s.port);
  s.stop(true);
  return port;
}

async function until<T>(what: string, secs: number, probe: () => Promise<T | undefined> | T | undefined): Promise<T> {
  for (let i = 0; i < secs * 4; i++) {
    const v = await Promise.resolve(probe()).catch(() => undefined);
    if (v !== undefined) return v;
    await Bun.sleep(250);
  }
  throw new Error(`timed out waiting for ${what}`);
}

/// What deploy/app-server does with the bundle: three files, each replaced atomically.
function publish(stateDir: string, feeds: string): void {
  const b = readBundle(stateDir);
  for (const [name, doc] of [["keys", b.keys], ["policies", b.policies], ["grants", b.grants]] as const) {
    writeFileSync(join(feeds, `${name}.json.tmp`), JSON.stringify(doc));
    renameSync(join(feeds, `${name}.json.tmp`), join(feeds, `${name}.json`));
  }
}

/// The shared cell on `port`: started, and ready once /health answers 200.
async function startCell(bin: string, port: number, s3: string, bucket: string, feeds: string, log: string) {
  const env: Record<string, string> = {
    PATH: process.env.PATH ?? "",
    HOME: process.env.HOME ?? "",
    ...Object.fromEntries(readProfiles(DEFAULT_PROFILES)),
    STREAMS_AUTH_KEYS_FILE: join(feeds, "keys.json"),
    STREAMS_AUTH_POLICY_FILE: join(feeds, "policies.json"),
    STREAMS_AUTH_GRANTS_FILE: join(feeds, "grants.json"),
    CELL_ID: "cell_sc1",
    PROJECT_ID: "proj_deploy",
    ACCOUNT_ID: "acct_sink",
    USAGE_STREAM_KEY: randomBytes(32).toString("base64url"),
    AUTH_TOKEN: OPERATOR,
  };
  const child = Bun.spawn([join(bin, "streams-slate"), "--listen", `127.0.0.1:${port}`, "--s3-endpoint", s3, "--bucket", bucket],
    { env, stdout: Bun.file(log), stderr: Bun.file(log) });
  children.push(child);
  const health = await until("the cell to be ready", 90, async () => {
    const res = await fetch(`http://127.0.0.1:${port}/health`);
    return res.ok ? res : undefined;
  });
  return { child, git: health.headers.get("x-streams-git") ?? "", boot: health.headers.get("x-streams-boot-id") ?? "" };
}

/// `prisma dev app.ts` with the watch's inputs in its shell; the watch's URL.
async function startWatch(inputs: Record<string, string>): Promise<{ url: string; out: () => string }> {
  const built = await Bun.build({ entrypoints: [join(HERE, "entry.ts")], target: "bun", format: "esm", outdir: HERE, naming: "entry.js" });
  if (!built.success) throw new Error(`bun build failed: ${built.logs.join("\n")}`);
  emulators = scratchDir();
  const dev = Bun.spawn([join(HERE, "node_modules", ".bin", "prisma"), "dev", "app.ts", "--fresh", "--no-interactive", "--no-color"], {
    cwd: HERE,
    env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "", PRISMA_COMPOSER_EMULATORS_DIR: emulators, ...inputs },
    stdout: "pipe",
    stderr: "pipe",
  });
  children.push(dev);
  const out: string[] = [];
  const pump = async (s: ReadableStream<Uint8Array>) => {
    for await (const chunk of s) out.push(new TextDecoder().decode(chunk));
  };
  void pump(dev.stdout as ReadableStream<Uint8Array>);
  void pump(dev.stderr as ReadableStream<Uint8Array>);
  const url = await until(`prisma dev's endpoint line (output so far: ${out.join("").slice(-2000)})`, 120, () =>
    /"kind":"endpoint","name":"cellwatch\.watch","url":"([^"]+)"/.exec(out.join(""))?.[1]);
  return { url, out: () => out.join("") };
}

/// Row 17 is open on an idle cell at this tip: the cell's own activity
/// grows ops.counters.unowned_meter_events_total by about nine each
/// telemetry tick with no customer traffic and only the canary in the
/// feed (measured on target/debug; reported with B4 for B2 and B6).
const IDLE_CELL_OPEN = "cell_sc1/r17_unowned_usage/notify";

/// `[kind, alert key]` of each event the webhook got, but row 17's.
const sent = (sink: FakeServer) => sink.seen.map((s) => {
  const b = JSON.parse(s.body);
  return [b.kind, b.dedupKey];
}).filter(([, key]) => key !== IDLE_CELL_OPEN);

maybe("in prisma dev the watch polls a local shared cell, beats each cycle, answers ok, and notifies a restart, then resolves it", async () => {
  const bin = resolve(BIN_DIR ?? "");
  const c = await initCell();
  const feeds = join(scratchDir(), "feeds");
  mkdirSync(feeds);
  publish(c.state, feeds);
  const s3Port = freePort();
  children.push(Bun.spawn([join(bin, "s3lite"), "--listen", `127.0.0.1:${s3Port}`, "--latency-ms", "2"], { stdout: "ignore", stderr: "ignore" }));
  const port = freePort();
  const bucket = `cell-watch-e2e-${Date.now()}`;
  const s3 = `http://127.0.0.1:${s3Port}`;
  const first = await startCell(bin, port, s3, bucket, feeds, join(c.root, "server-1.log"));

  const sink = new FakeServer({ "/hook": () => ({ status: 202, body: "" }) });
  const monitor = new FakeServer({ [BEAT]: () => ({ status: 200, body: "OK" }) });
  servers.push(sink, monitor);
  const inputs = {
    STREAMS_OPERATOR_TOKEN: OPERATOR,
    ALERT_SINK_URL: `${sink.origin}/hook`,
    ALERT_SINK_KEY: SINK_BEARER,
    HEARTBEAT_URL: `${monitor.origin}${BEAT}`,
    STREAMS_ISSUER_METRICS_TOKEN: "none",
    CANARY_CREDENTIAL: "none",
    STREAMS_CELL_ORIGIN: `http://127.0.0.1:${port}`,
    STREAMS_CELL_ID: "cell_sc1",
    ALERT_SINK_KIND: "webhook",
    EXPECTED_GIT: first.git,
    NOTIFY_ONLY: "0",
    ISSUER_ORIGIN: "none",
  };
  const watch = await startWatch(inputs);
  const ok = await fetch(`${watch.url}/`);
  expect([ok.status, await ok.text(), (await fetch(`${watch.url}/x`)).status]).toEqual([200, "ok", 404]);
  // Two cycles of a healthy cell: two heartbeats, and no event but row 17's.
  await until("two heartbeats", 45, () => (monitor.seen.length >= 2 ? true : undefined));
  expect(sent(sink)).toEqual([]);

  // A restart is a new boot id: NOTIFY, then the resolve after four clean cycles.
  first.child.kill("SIGTERM");
  await first.child.exited;
  const second = await startCell(bin, port, s3, bucket, feeds, join(c.root, "server-2.log"));
  expect(second.boot).not.toBe(first.boot);
  await until("the restart's trigger", 60, () => (sent(sink).length >= 1 ? true : undefined));
  await until("its resolve", 120, () => (sent(sink).length >= 2 ? true : undefined));
  expect(sent(sink)).toEqual([["trigger", "cell_sc1/r04_restarts/notify"], ["resolve", "cell_sc1/r04_restarts/notify"]]);
  const trigger = JSON.parse(sink.seen.find((s) => s.body.includes('"r04_restarts"'))!.body);
  expect([trigger.class, trigger.values]).toEqual(["NOTIFY", { boot_ids_30m: 2 }]);
  expect(new Set(sink.seen.map((s) => s.authorization))).toEqual(new Set([`Bearer ${SINK_BEARER}`]));
  // Row 17 opened once and stayed open: the only other event.
  expect(sink.seen.map((s) => JSON.parse(s.body)).filter((b) => b.dedupKey === IDLE_CELL_OPEN).map((b) => b.kind))
    .toEqual(["trigger"]);
  expect(monitor.seen.every((s) => s.method === "GET")).toBe(true);

  // The service's log (the emulator's): the boot line first; no secret anywhere.
  const log = readFileSync(join(emulators ?? "", "compute", "logs", "streams-cell-watch", "cellwatch-watch.log"), "utf8");
  expect(log.split("\n")[0]).toBe(
    "cell-watch: cell cell_sc1, sink webhook, notify_only false, issuer false, expected_git true, " +
      `keep_awake unavailable, listening on :${new URL(watch.url).port}`,
  );
  for (const secret of [OPERATOR, SINK_BEARER, BEAT]) expect([log, watch.out()].filter((t) => t.includes(secret))).toEqual([]);
}, 330_000);
