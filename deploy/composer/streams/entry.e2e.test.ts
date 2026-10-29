// The shared cell's Composer entry, run the way Compute runs it (public-beta
// deploy-q5 triage B2; D0's local findings 2 and 4): entry.ts bundled into
// one file as cut-release builds it, inside a release directory away from
// the repository (no node_modules above it), with the stage and the secret
// pointers in the input document Composer's runtime reads (COMPOSER_INPUT)
// and the seven platform names in its environment. Opt-in, because it
// needs built binaries and this directory's packages:
//
//   (cd deploy/composer && bun install --frozen-lockfile)
//   cargo build --locked --release --bin streams-slate --bin s3lite
//   CELL_ADMIN_E2E_BIN_DIR=target/release bun test ./deploy/composer
//
// The refusals run on any host: their binaries are ELF headers that never
// run. So does the Layer B rig's test: bench/k2cost/run-shared.sh
// --dry-run stages a release that release.ts staged as cut-release does,
// and the bundled entry, run from that staging, must pass every check up
// to the rig's feeds bucket, which only a rig's host serves. The good boot
// needs what a release ships, an x86_64 Linux ELF, so it runs on an
// x86_64 Linux host only and a Mac skips it, unless
// COMPOSER_E2E_REQUIRE_BOOT=1 is set: then a good boot that cannot run
// fails, naming why, so no runner change can pass it unbooted. CI's
// general bun line sets no CELL_ADMIN_E2E_BIN_DIR and installs no packages
// here, so it skips the whole file but its last two tests; the sdk-package
// job's steps after cell-admin's end to end install the packages and run
// this file on the release binaries with COMPOSER_E2E_REQUIRE_BOOT=1 (the
// owner's round-3 call 14 of 2026-10-13).
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createHash, randomBytes } from "node:crypto";
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { join, resolve } from "node:path";
import { cleanup, keyFile, run, scratchDir } from "../../cell-admin/test-rig";
import { stageRelease as cutRelease } from "../release";
import { cellsText, cleanupScratch, fakeTree, stageInputs } from "../release-rig";
import { buildChildEnv } from "./env";

const BIN_DIR = process.env.CELL_ADMIN_E2E_BIN_DIR;
const maybe = BIN_DIR ? test : test.skip;
const DEPLOY = join(import.meta.dir, "..", "..");
const SERVER = BIN_DIR ? join(resolve(BIN_DIR), "streams-slate") : "";
const native = (path: string) => {
  const b = existsSync(path) ? readFileSync(path) : Buffer.alloc(0);
  return b.length > 20 && b.subarray(0, 4).equals(Buffer.from("\x7fELF", "latin1")) && b.readUInt16LE(18) === 0x3e;
};
const onLinuxX64 = process.platform === "linux" && process.arch === "x64";
/// Why the good boot cannot run here, or undefined when it can.
const bootBlocker = !BIN_DIR
  ? "CELL_ADMIN_E2E_BIN_DIR is unset"
  : !onLinuxX64
    ? `this host is ${process.platform}-${process.arch}, not linux-x64`
    : !native(SERVER)
      ? `${SERVER} is not an x86_64 ELF`
      : undefined;
/// COMPOSER_E2E_REQUIRE_BOOT=1 (for CI, by the owner's round-3 call 14)
/// fails a good boot that cannot run, which otherwise skips.
const maybeBoot = bootBlocker === undefined || process.env.COMPOSER_E2E_REQUIRE_BOOT === "1" ? test : test.skip;

/// What the wrapper's port answers while no binary runs (supervise.ts).
const GENERIC = JSON.stringify({
  error: "binary_exited",
  message: "the service is not running; its exit is recorded in the platform log",
});

const children: ReturnType<typeof Bun.spawn>[] = [];
afterAll(() => {
  for (const c of children) c.kill("SIGKILL");
  cleanup();
  cleanupScratch();
});

function freePort(): number {
  const s = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const port = s.port;
  s.stop(true);
  return port;
}

async function until<T>(what: string, secs: number, probe: () => Promise<T | undefined>): Promise<T> {
  for (let i = 0; i < secs * 4; i++) {
    const v = await probe().catch(() => undefined);
    if (v !== undefined) return v;
    await Bun.sleep(250);
  }
  throw new Error(`timed out waiting for ${what}`);
}

const sha256 = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
/// An ELF header for `machine` that never runs.
const elf = (machine: number, tag: string) =>
  Buffer.concat([Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1, 1, ...new Array(9).fill(0), 2, 0, machine, 0]), Buffer.from(tag)]);

/// The input document Composer writes for app.ts's binding: a $secret
/// pointer per envSecret, the stage and the deploy's nonce as literals.
function inputDocument(stage: string, deployNonce = "e2e.r1"): string {
  const app = readFileSync(join(DEPLOY, "composer", "app.ts"), "utf8");
  const doc: Record<string, unknown> = {};
  for (const m of app.matchAll(/^\s*(\w+): envSecret\("([A-Z0-9_]+)"\),$/gm)) doc[m[1]] = { $secret: m[2] };
  doc.cellStage = stage;
  doc.deployNonce = deployNonce;
  return JSON.stringify(doc);
}

let entryJs = "";
beforeAll(async () => {
  if (!BIN_DIR) return;
  if (!existsSync(join(DEPLOY, "composer", "node_modules"))) {
    throw new Error("deploy/composer has no node_modules: run `bun install --frozen-lockfile` there first");
  }
  const out = scratchDir();
  const built = await Bun.build({
    entrypoints: [join(import.meta.dir, "entry.ts")],
    target: "bun",
    format: "esm",
    outdir: out,
    naming: "entry.js",
  });
  if (!built.success) throw new Error(`bun build failed: ${built.logs.join("\n")}`);
  entryJs = join(out, "entry.js");
});

type Release = { root: string; feeds: string; profiles: { path: string; text: string }[]; cellEnv: { path: string; text: string } };

/// A release directory as cut-release stages its deploy/ tree: the bundled
/// entry and the binary in composer/streams/, release.json, the profiles
/// (their feed files moved into the release's own scratch directory, so
/// runs never share /tmp/feeds), and cells/<stage>.env.
function stageRelease(binary: Uint8Array, releaseSha: string, stage: string, cellLines: string[]): Release {
  const root = scratchDir();
  const deploy = join(root, "deploy");
  const streams = join(deploy, "composer", "streams");
  const feeds = join(root, "feeds");
  for (const d of [streams, join(deploy, "profiles"), join(deploy, "cells"), feeds, join(root, "tmp")]) mkdirSync(d, { recursive: true });
  copyFileSync(entryJs, join(streams, "entry.js"));
  writeFileSync(join(streams, "streams-slate"), binary, { mode: 0o755 });
  writeFileSync(join(deploy, "release.json"), JSON.stringify({ tag: "e2e", server_sha256: releaseSha }));
  const profiles = ["compute-1g.env", "shared-cell.env"].map((name) => {
    const text = readFileSync(join(DEPLOY, "profiles", name), "utf8").replaceAll("=/tmp/feeds/", `=${feeds}/`);
    writeFileSync(join(deploy, "profiles", name), text);
    return { path: `profiles/${name}`, text };
  });
  const cellText = `${cellLines.join("\n")}\n`;
  writeFileSync(join(deploy, "cells", `${stage}.env`), cellText);
  return { root, feeds, profiles, cellEnv: { path: `cells/${stage}.env`, text: cellText } };
}

function secrets(): Record<string, string> {
  return {
    STREAMS_OPERATOR_TOKEN: randomBytes(24).toString("hex"),
    STREAMS_USAGE_STREAM_KEY: randomBytes(32).toString("base64url"),
    STREAMS_CURSOR_KEY: randomBytes(32).toString("base64"),
    STREAMS_DATA_S3_ACCESS_KEY_ID: randomBytes(8).toString("hex"),
    STREAMS_DATA_S3_SECRET_ACCESS_KEY: randomBytes(16).toString("hex"),
    STREAMS_FEEDS_S3_ACCESS_KEY_ID: randomBytes(8).toString("hex"),
    STREAMS_FEEDS_S3_SECRET_ACCESS_KEY: randomBytes(16).toString("hex"),
  };
}

function cellLines(stage: string, s3: string, extra: string[] = []): string[] {
  return [
    `CELL_STAGE=${stage}`,
    "CELL_ID=cell_e2e",
    "PROJECT_ID=proj_deploy",
    "ACCOUNT_ID=acct_sink",
    "STREAMS_AUTH_ISSUER=https://auth.prisma.io",
    "PATH_PREFIX=e2e",
    "REGION=local",
    `SLATE_S3_ENDPOINT=${s3}`,
    `SLATE_S3_BUCKET=data-${Date.now()}`,
    "SLATE_S3_REGION=us-east-1",
    `FEEDS_S3_ENDPOINT=${s3}`,
    "FEEDS_S3_BUCKET=feeds",
    "FEEDS_S3_REGION=us-east-1",
    "FEEDS_S3_KEY=cells/cell_e2e/feeds-bundle.json",
    ...extra,
  ];
}

type Launched = { child: ReturnType<typeof Bun.spawn>; port: number; out: () => string };

/// The bundled entry with exactly `env` (plus PORT and a TMPDIR of the
/// release's own), as Compute's runner imports it: cwd the release.
function launch(r: Pick<Release, "root">, env: Record<string, string>): Launched {
  const port = freePort();
  const child = Bun.spawn([process.execPath, join(r.root, "deploy", "composer", "streams", "entry.js")], {
    env: { ...env, PORT: String(port), TMPDIR: join(r.root, "tmp") },
    cwd: r.root,
    stdout: "pipe",
    stderr: "pipe",
  });
  children.push(child);
  const chunks: string[] = [];
  for (const stream of [child.stdout, child.stderr] as ReadableStream<Uint8Array>[]) {
    void (async () => {
      const dec = new TextDecoder();
      for await (const c of stream) chunks.push(dec.decode(c, { stream: true }));
    })();
  }
  return { child, port, out: () => chunks.join("") };
}

/// A refused entry answers every request with the generic 500; the reason
/// is in its log only.
async function refusedWith(l: Launched, reason: string, secretValues: string[]): Promise<void> {
  const res = await until("the refused entry's port", 30, async () => fetch(`http://127.0.0.1:${l.port}/v1/streams`));
  const body = await res.text();
  await until("the refusal in the log", 10, async () => (l.out().includes("streams-cell: refused:") ? true : undefined));
  const log = l.out();
  expect([res.status, res.headers.get("content-type"), res.headers.get("cache-control"), body]).toEqual([
    500,
    "application/json",
    "no-store",
    GENERIC,
  ]);
  expect(log).toContain(`streams-cell: refused: ${reason}`);
  expect([log.includes("starting streams-slate"), secretValues.filter((v) => log.includes(v) || body.includes(v))]).toEqual([false, []]);
}

const PLATFORM = { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/" };

describe("the bundled entry", () => {
  maybe("is one file whose imports are the runtime's own", () => {
    const text = readFileSync(entryJs, "utf8");
    const specifiers = [...text.matchAll(/(?:from\s*|import\s*\(\s*)"([^"]+)"/g)].map((m) => m[1]);
    const runtime = (s: string) => /^(node:|bun(:|$))/.test(s) || builtinModules.includes(s);
    expect([specifiers.length > 0, [...new Set(specifiers.filter((s) => !runtime(s)))]]).toEqual([true, []]);
  });

  maybe("serves the wrapper's generic 500 for a binary that is not release.json's", async () => {
    const binary = elf(0x3e, "a build release.json does not name");
    const other = sha256(Buffer.from("another build"));
    const r = stageRelease(binary, other, "scratch", cellLines("scratch", "http://127.0.0.1:9"));
    const s = secrets();
    const l = launch(r, { ...PLATFORM, ...s, COMPOSER_INPUT: inputDocument("scratch") });
    await refusedWith(l, `composer/streams/streams-slate has sha256 ${sha256(binary)}, release.json names ${other}`, Object.values(s));
  }, 60_000);

  maybe("serves the generic 500 for a binary built for another machine", async () => {
    const binary = elf(0xb7, "an aarch64 build");
    const r = stageRelease(binary, sha256(binary), "scratch", cellLines("scratch", "http://127.0.0.1:9"));
    const s = secrets();
    const l = launch(r, { ...PLATFORM, ...s, COMPOSER_INPUT: inputDocument("scratch") });
    await refusedWith(l, "composer/streams/streams-slate is an ELF for machine 0xb7, not x86_64 (0x3e)", Object.values(s));
  }, 60_000);

  maybe("serves the generic 500 when a secret is missing, and names it in the log only", async () => {
    const binary = elf(0x3e, "the release's build");
    const r = stageRelease(binary, sha256(binary), "scratch", cellLines("scratch", "http://127.0.0.1:9"));
    const { STREAMS_CURSOR_KEY: _, ...rest } = secrets();
    const l = launch(r, { ...PLATFORM, ...rest, COMPOSER_INPUT: inputDocument("scratch") });
    await refusedWith(l, "the service's input: secret input is not provisioned", Object.values(rest));
    expect(l.out()).toContain('the platform var "STREAMS_CURSOR_KEY" is unset or empty');
  }, 60_000);

  maybe("serves the generic 500 when a secret is blank (set-secrets --blank on scratch), as when it is missing", async () => {
    const binary = elf(0x3e, "the release's build");
    const r = stageRelease(binary, sha256(binary), "scratch", cellLines("scratch", "http://127.0.0.1:9"));
    const s = { ...secrets(), STREAMS_CURSOR_KEY: "" };
    const l = launch(r, { ...PLATFORM, ...s, COMPOSER_INPUT: inputDocument("scratch") });
    await refusedWith(l, "the service's input: secret input is not provisioned", Object.values(s).filter((v) => v !== ""));
    expect(l.out()).toContain('the platform var "STREAMS_CURSOR_KEY" is unset or empty');
  }, 60_000);

  maybe("serves the generic 500 for a cells file the child environment refuses", async () => {
    const binary = elf(0x3e, "the release's build");
    const lines = cellLines("production", "http://127.0.0.1:9", ["KEEP_AWAKE_UNTIL_MS=1767225600000"]);
    const r = stageRelease(binary, sha256(binary), "production", lines);
    const s = secrets();
    const l = launch(r, { ...PLATFORM, ...s, COMPOSER_INPUT: inputDocument("production") });
    await refusedWith(
      l,
      "keep_awake: cells/production.env sets KEEP_AWAKE_UNTIL_MS: only a scratch cell's keep-awake may end; a production cell is kept awake for good",
      Object.values(s),
    );
    // The deploy's nonce is logged before anything can refuse, and is the
    // input's only use of it: it never reaches the child environment.
    expect(l.out()).toContain("deploy: stage production, nonce e2e.r1\n");
  }, 60_000);
});

/// The Layer B rig.
const RIG = join(DEPLOY, "..", "bench", "k2cost", "run-shared.sh");

/// The rig's staging of the release at `dir` (run-shared.sh --dry-run, plus
/// `extra`): the staged copy, which holds the release's build dir as
/// deploy/, the service's input the rig hands the container, and the
/// platform names its env file holds.
function rigStaging(dir: string, extra: string[]): { root: string; input: string; names: string[] } {
  const home = scratchDir();
  const s3lite = join(home, "s3lite");
  writeFileSync(s3lite, "#!/bin/sh\n", { mode: 0o755 });
  const argv = ["/bin/bash", RIG, "--release", dir, "--s3lite", s3lite, "--bun-baseline-sha256", "0f".repeat(32), "--dry-run", ...extra];
  const r = Bun.spawnSync(argv, { env: { ...process.env, K2_HOME: home }, stdout: "pipe", stderr: "pipe" });
  const out = r.stdout.toString();
  expect([r.exitCode, r.exitCode === 0 ? "" : r.stderr.toString()]).toEqual([0, ""]);
  const root = /^DRY_RUN staged: (\S+) /m.exec(out)?.[1];
  const input = / -e COMPOSER_INPUT=(\S+) /.exec(out)?.[1];
  const names = /^DRY_RUN container env names: (.*)$/m.exec(out)?.[1].trim().split(" ");
  if (!root || !input || !names) throw new Error(`run-shared.sh --dry-run printed no staging:\n${out}`);
  mkdirSync(join(root, "tmp"));
  return { root, input, names };
}

describe("the Layer B rig's staging", () => {
  maybe("runs a cut release's bundled entry, and a --binary rehearsal's, through every check before the rig's feeds bucket", async () => {
    const { tree, commit } = fakeTree();
    const cut = stageInputs({ tree, commit, tag: "rc-rig", cells: { production: cellsText("production") } });
    cutRelease({ ...cut, entryJs });
    const rehearsal = join(scratchDir(), "streams-slate");
    writeFileSync(rehearsal, elf(0x3e, "a build of the candidate commit"));
    for (const extra of [[], ["--binary", rehearsal]]) {
      const staged = rigStaging(cut.out, extra);
      const s = secrets();
      expect([...staged.names].sort()).toEqual(Object.keys(s).sort());
      const l = launch(staged, { ...PLATFORM, ...s, COMPOSER_INPUT: staged.input });
      // The stage, the deploy's nonce (the release's tag), the cells file,
      // the profiles and the binary all passed: what refuses is the feeds
      // download from the rig's s3lite, which nothing here serves.
      await refusedWith(l, "feeds bundle cells/cell_rig/feeds-bundle.json: ", Object.values(s));
      expect(l.out()).toContain("deploy: stage scratch, nonce rc-rig\n");
      l.child.kill("SIGKILL");
    }
  }, 120_000);
});

/// The effective configuration the binary logs at boot, by the env name
/// each key is read from: the child environment must decide every one.
const SUMMARY: [string, string][] = [
  ["storage.bulk_inflight_max_bytes", "STORE_BULK_INFLIGHT_MAX_BYTES"],
  ["engine.compactor_max_concurrent", "COMPACTOR_MAX_CONCURRENT"],
  ["engine.compact_max_subcompactions", "COMPACT_MAX_SUBCOMPACTIONS"],
  ["engine.compact_max_fetch_tasks", "COMPACT_MAX_FETCH_TASKS"],
  ["engine.compact_bytes_to_fetch", "COMPACT_BYTES_TO_FETCH"],
  ["engine.compact_max_sst_size", "COMPACT_MAX_SST_SIZE_BYTES"],
  ["engine.slatedb_rt_threads", "SLATEDB_RT_THREADS"],
  ["sse.feed_ring_bytes", "SSE_FEED_RING_BYTES"],
  ["sse.feed_total_bytes", "SSE_FEED_TOTAL_BYTES"],
  ["sse.feed_project_bytes", "SSE_FEED_PROJECT_BYTES"],
  ["billing.telemetry_cache_bytes", "TELEMETRY_CACHE_BYTES"],
  ["http.binary_sha256", "APP_BINARY_SHA256"],
  ["runtime.memprofile_cert", "MEMPROFILE_CERT"],
  // Set by neither the profiles nor the cell: the binary's defaults.
  ["engine.compactor_poll_ms", "COMPACTOR_POLL_MS"],
  ["billing.telemetry_drain_secs", "TELEMETRY_DRAIN_SECS"],
];

describe("a good boot", () => {
  maybeBoot("serves the release's binary on the computed environment and stops on SIGTERM", async () => {
    if (bootBlocker !== undefined) throw new Error(`COMPOSER_E2E_REQUIRE_BOOT=1, but the good boot cannot run: ${bootBlocker}`);
    const bin = resolve(BIN_DIR ?? "");
    const s3Port = freePort();
    children.push(Bun.spawn([join(bin, "s3lite"), "--listen", `127.0.0.1:${s3Port}`, "--latency-ms", "2"], { stdout: "ignore", stderr: "ignore" }));
    const s3 = `http://127.0.0.1:${s3Port}`;
    await until("s3lite to listen", 30, async () => ((await fetch(`${s3}/ctl/`)).status < 500 ? true : undefined));
    // cell-admin publishes the cell's bundle to the feeds bucket on s3lite.
    const admin = scratchDir();
    const target = (bucket: string) => ({ endpoint: s3, bucket, region: "auto", accessKeyId: "e2e", secretAccessKey: "e2e" });
    const creds = join(admin, "s3.json");
    writeFileSync(creds, JSON.stringify({ ...target("ctl"), feeds: target("feeds") }), { mode: 0o600 });
    const init = await run("init", "--state", "s3://ctl/cells/cell_e2e", "--s3-credentials", creds, "--cell-id", "cell_e2e",
      "--deployment-project", "proj_deploy", "--account", "acct_sink", "--key-file", keyFile(admin));
    expect([init.code, init.err]).toEqual([0, ""]);

    const binary = readFileSync(SERVER);
    const sha = sha256(binary);
    const r = stageRelease(binary, sha, "scratch", cellLines("scratch", s3, ["CPU_LOG_SECS=1"]));
    const s = secrets();
    // A process environment laced with a campaign wrapper's names and with
    // other values for every name the summary reports.
    const laced: Record<string, string> = { MODE: "gen", BILLING_METER: "off", BINARY_URL: "https://bin.example/x", SERVER_BINARY_S3_KEY: "bin/x" };
    for (const [, name] of SUMMARY) laced[name] = name === "APP_BINARY_SHA256" ? "f".repeat(64) : name === "MEMPROFILE_CERT" ? "laced" : "4242";
    const l = launch(r, { ...laced, ...PLATFORM, ...s, COMPOSER_INPUT: inputDocument("scratch") });
    const origin = `http://127.0.0.1:${l.port}`;
    const health = await until("the cell to answer /health", 90, async () => {
      const res = await fetch(`${origin}/health`);
      return res.status === 200 ? res : undefined;
    });
    const git = health.headers.get("x-streams-git");
    const load = await (await fetch(`${origin}/v1/debug/load`, { headers: { authorization: `Bearer ${s.STREAMS_OPERATOR_TOKEN}` } })).json();
    expect([typeof git, (git ?? "").length > 0, load.binary_sha256, load.git_commit]).toEqual(["string", true, sha, git]);
    expect(l.out()).toContain(`binary sha256 ${sha}`);

    // The binary's effective configuration is the computed child
    // environment's, whatever the process environment carried.
    const computed = buildChildEnv({
      profiles: r.profiles,
      cellEnv: r.cellEnv,
      stage: "scratch",
      platformEnv: { ...laced, ...PLATFORM, ...s },
      binarySha256: sha,
      port: String(l.port),
    });
    if ("refused" in computed) throw new Error(computed.message);
    // tracing colours its fields even into a pipe.
    const plain = l.out().replace(/\x1b\[[0-9;]*m/g, "");
    const line = /effective configuration \(redacted\) config=(\{.*\})\s*$/m.exec(plain);
    expect(line).not.toBeNull();
    const summary = JSON.parse(line![1]);
    // Each key is reported, and holds the child environment's value, or,
    // for a name the cell does not set, anything but the laced one.
    const decided = SUMMARY.map(([path, name]) => {
      const got = path.split(".").reduce((o, k) => o?.[k], summary);
      const want = computed.env[name];
      return [name, got !== undefined && (want !== undefined ? String(got) === want : String(got) !== laced[name])];
    });
    expect(decided).toEqual(SUMMARY.map(([, name]) => [name, true]));
    expect(["COMPACTOR_POLL_MS", "TELEMETRY_DRAIN_SECS"].map((n) => n in computed.env)).toEqual([false, false]);
    // CPU_LOG_SECS=1: the wrapper's CPU line, from /proc.
    await until("a cpu sample", 10, async () => (/^cpu sample: \{"boot_ms":/m.test(l.out()) ? true : undefined));
    expect(readFileSync(join(r.feeds, "keys.json"), "utf8")).toContain("prisma-streams-data");

    l.child.kill("SIGTERM");
    expect(await l.child.exited).toBe(0);
    expect(l.out()).toContain("wrapper received SIGTERM; forwarding it to the binary");
  }, 180_000);
});

test("the generic body is the wrapper's own", () => {
  expect(readFileSync(join(DEPLOY, "app-server", "supervise.ts"), "utf8")).toContain(
    'message: "the service is not running; its exit is recorded in the platform log",',
  );
});

// This file again, with COMPOSER_E2E_REQUIRE_BOOT=1 and no
// CELL_ADMIN_E2E_BIN_DIR: a good boot that cannot run fails the run. Not in
// that child itself.
(process.env.COMPOSER_E2E_REQUIRE_BOOT === "1" ? test.skip : test)(
  "COMPOSER_E2E_REQUIRE_BOOT=1 fails a good boot that cannot run, naming why",
  async () => {
    // Uncoloured, so each result line reads (pass), (skip) or (fail).
    const { CELL_ADMIN_E2E_BIN_DIR: _, FORCE_COLOR: __, ...env } = process.env;
    const child = Bun.spawn([process.execPath, "test", join(import.meta.dir, "entry.e2e.test.ts")], {
      cwd: join(DEPLOY, ".."),
      env: { ...env, NO_COLOR: "1", COMPOSER_E2E_REQUIRE_BOOT: "1" },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [code, out, err] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
    const log = out + err;
    expect([
      code,
      log.match(/^ \d+ (pass|skip|fail)$/gm),
      log.match(/^\(fail\) .*$/gm)?.map((l) => l.replace(/ \[[\d.]+m?s\]$/, "")),
      log.includes("COMPOSER_E2E_REQUIRE_BOOT=1, but the good boot cannot run: CELL_ADMIN_E2E_BIN_DIR is unset"),
    ]).toEqual([
      1,
      [" 1 pass", " 8 skip", " 1 fail"],
      ["(fail) a good boot > serves the release's binary on the computed environment and stops on SIGTERM"],
      true,
    ]);
  },
  60_000,
);
