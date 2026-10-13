// The shared cell's Compute entry (public-beta deploy-q5 triage B2): what
// the platform's runner starts, bundled into one file at cut time
// (service.ts). Its build's dir is the release's deploy/ tree, which holds
// release.json, cells/<stage>.env for every stage, profiles/ and the
// x86_64-musl streams-slate beside this file.
//
// In order:
//   1. release.json's server_sha256; the shipped binary must be an ELF for
//      x86_64 (e_machine 0x3e) with exactly that sha256 (an integrity
//      check: the identity root is the operator's release.json, which
//      cell-check compares with /v1/debug/load);
//   2. the binary copied to $TMPDIR/streams-slate (/tmp on Compute), mode
//      0755, on every boot: a warm instance keeps /tmp across versions;
//   3. the `instance shape:` line: the campaign wrapper's (memory, CPUs,
//      libc) plus Bun's version, the CA bundle's paths and whether
//      Compute's scale-to-zero control exists;
//   4. the stage and the deploy's nonce from the service's input
//      (CELL_STAGE and CELL_DEPLOY_NONCE, envParams: not in the process
//      environment; D0 finding 4), which also fails when a secret's
//      platform name is unset or empty; the `deploy:` line names both (the
//      nonce only tells one version of a release from another, and goes
//      nowhere else);
//   5. the child environment (env.ts) from the profiles, the stage's cells
//      file, the platform names, the sha256 and the runtime pass-through;
//   6. Compute's keep-awake guard, for good, or until KEEP_AWAKE_UNTIL_MS
//      on a scratch cell, exactly as deploy/app-server/index.ts holds it;
//   7. CPU_LOG_SECS's `cpu sample:` lines and RESOLV_OVERRIDE, as that
//      wrapper does;
//   8. the auth-feed bundle from the feeds bucket (FEEDS_S3_*) into the
//      directory the profiles' STREAMS_AUTH_*_FILE name, then kept current
//      by the feed poller;
//   9. streams-slate on 0.0.0.0:$PORT under the wrapper's supervisor, with
//      the child environment and nothing else.
// Any refusal before step 9 is logged with its reason, and $PORT then
// answers every request with the wrapper's generic 500 (supervise.ts),
// never the reason: $PORT is customer-reachable. A refused cell holds no
// keep-awake guard.
//
// RESOLV_OVERRIDE: a cells file is space-free, so it names the nameservers
// as a comma-separated list of addresses (RESOLV_OVERRIDE=108.61.10.10,8.8.8.8)
// and the entry writes one `nameserver` line for each; the campaign
// wrapper's escaped resolv.conf text cannot be written there.
import { createHash } from "node:crypto";
import { chmodSync, copyFileSync, existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { cpus, tmpdir, totalmem } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { KeepAwakeGuard } from "@prisma/compute";
import { downloadFile } from "../../app-server/downloader.ts";
import { FeedPoller, materialize, s3BundleFetcher } from "../../app-server/feeds.ts";
import { policyFor, superviseBinary } from "../../app-server/supervise.ts";
import { buildChildEnv, type ChildEnv, PROFILES } from "./env.ts";
import service from "./service.ts";

/// The build's dir, two levels up from this file.
const ROOT = fileURLToPath(new URL("../../", import.meta.url));
/// The shipped binary, relative to ROOT.
const BINARY = "composer/streams/streams-slate";
/// The port the platform routes to; supervise.ts binds and probes the same.
const PORT = process.env.PORT ?? "8080";
const ELF_X86_64 = 0x3e;
const CA_BUNDLE = "/etc/ssl/certs/ca-certificates.crt";
const SCALE_TO_ZERO_CONTROL = "/uk/libukp/scale_to_zero_disable";
/// supervise.ts's body for a port no binary serves.
const GENERIC = JSON.stringify({
  error: "binary_exited",
  message: "the service is not running; its exit is recorded in the platform log",
});

class Refuse extends Error {}

/// Log why, then answer every request on $PORT with the generic 500.
function refuse(reason: string): Promise<never> {
  console.error(`streams-cell: refused: ${reason}`);
  Bun.serve({
    port: Number(PORT),
    fetch: () =>
      new Response(GENERIC, { status: 500, headers: { "content-type": "application/json", "cache-control": "no-store" } }),
  });
  return new Promise<never>(() => {});
}

function readText(path: string): string {
  try {
    return readFileSync(join(ROOT, path), "utf8");
  } catch {
    throw new Refuse(`${path} cannot be read`);
  }
}

/// Steps 1-2: the verified binary's sha256 and the fresh copy's path.
function shippedBinary(): { sha: string; bin: string } {
  let release: { server_sha256?: unknown };
  try {
    release = JSON.parse(readText("release.json"));
  } catch (e) {
    throw e instanceof Refuse ? e : new Refuse("release.json is not JSON");
  }
  const want = release.server_sha256;
  if (typeof want !== "string" || !/^[0-9a-f]{64}$/.test(want)) {
    throw new Refuse("release.json's server_sha256 is not 64 lowercase hex digits");
  }
  let bytes: Buffer;
  try {
    bytes = readFileSync(join(ROOT, BINARY));
  } catch {
    throw new Refuse(`${BINARY} cannot be read`);
  }
  if (bytes.length < 20 || !bytes.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) {
    throw new Refuse(`${BINARY} is not an ELF executable`);
  }
  const machine = bytes.readUInt16LE(18);
  if (machine !== ELF_X86_64) throw new Refuse(`${BINARY} is an ELF for machine 0x${machine.toString(16)}, not x86_64 (0x3e)`);
  const sha = createHash("sha256").update(bytes).digest("hex");
  if (sha !== want) throw new Refuse(`${BINARY} has sha256 ${sha}, release.json names ${want}`);
  const bin = join(tmpdir(), "streams-slate");
  copyFileSync(join(ROOT, BINARY), bin);
  chmodSync(bin, 0o755);
  return { sha, bin };
}

/// Step 3.
function logInstanceShape(): void {
  const read = (p: string) => {
    try {
      return readFileSync(p, "utf8").trim();
    } catch {
      return null;
    }
  };
  const shape = {
    mem_total_bytes: totalmem(),
    cgroup_memory_max: read("/sys/fs/cgroup/memory.max"),
    cpus: cpus().length,
    cgroup_cpu_max: read("/sys/fs/cgroup/cpu.max"),
    libc: existsSync("/lib/ld-musl-x86_64.so.1") ? "musl" : existsSync("/lib64/ld-linux-x86-64.so.2") ? "glibc" : "unknown",
    bun: process.versions.bun ?? null,
    ca_bundle: existsSync(CA_BUNDLE),
    ssl_cert_file: process.env.SSL_CERT_FILE ?? null,
    ssl_cert_dir: process.env.SSL_CERT_DIR ?? null,
    keep_awake_control: existsSync(SCALE_TO_ZERO_CONTROL),
  };
  console.log(`instance shape: ${JSON.stringify(shape)}`);
}

/// Step 4: the stage the deploy named, its nonce logged.
function stage(): string {
  let input: { cellStage: string; deployNonce: string };
  try {
    input = service.input();
  } catch (e) {
    throw new Refuse(`the service's input: ${(e as Error).message}`);
  }
  console.log(`deploy: stage ${input.cellStage}, nonce ${input.deployNonce}`);
  return input.cellStage;
}

/// The resolv.conf a cells file's RESOLV_OVERRIDE names.
function resolvConf(value: string): string {
  const servers = value.split(",");
  for (const s of servers) {
    if (!/^([0-9]{1,3}\.){3}[0-9]{1,3}$/.test(s) && !/^[0-9a-fA-F:]*:[0-9a-fA-F:]*$/.test(s)) {
      throw new Refuse(`RESOLV_OVERRIDE ${value} is not a comma-separated list of nameserver addresses`);
    }
  }
  return servers.map((s) => `nameserver ${s}\n`).join("");
}

type Prepared = ChildEnv & { sha: string; bin: string; feedsDir: string; resolv: string | null };

/// Steps 1-5: everything that can refuse before anything starts.
function prepare(): Prepared {
  const { sha, bin } = shippedBinary();
  logInstanceShape();
  const cellStage = stage();
  const built = buildChildEnv({
    profiles: PROFILES.map((path) => ({ path, text: readText(path) })),
    cellEnv: { path: `cells/${cellStage}.env`, text: readText(`cells/${cellStage}.env`) },
    stage: cellStage,
    platformEnv: process.env,
    binarySha256: sha,
    port: PORT,
  });
  if ("refused" in built) throw new Refuse(`${built.refused}: ${built.message}`);
  const feedsDir = dirname(built.env.STREAMS_AUTH_KEYS_FILE ?? "");
  const files = ["KEYS", "POLICY", "GRANTS"].map((f) => built.env[`STREAMS_AUTH_${f}_FILE`]);
  if (files.join() !== ["keys", "policies", "grants"].map((f) => join(feedsDir, `${f}.json`)).join()) {
    throw new Refuse("the profiles' STREAMS_AUTH_{KEYS,POLICY,GRANTS}_FILE are not keys.json, policies.json and grants.json of one directory");
  }
  const resolv = built.env.RESOLV_OVERRIDE === undefined ? null : resolvConf(built.env.RESOLV_OVERRIDE);
  return { ...built, sha, bin, feedsDir, resolv };
}

/// Step 6, as deploy/app-server/index.ts:7-17.
function holdKeepAwake({ hold, untilMs }: ChildEnv["keepAwake"]): KeepAwakeGuard | null {
  if (!hold) return null;
  if (untilMs === null) return new KeepAwakeGuard();
  const left = untilMs - Date.now();
  console.log(`keep-awake: until ${new Date(untilMs).toISOString()} (${Math.round(left / 1000)} s left)`);
  if (left <= 0) return null;
  const signal = AbortSignal.timeout(left);
  signal.addEventListener("abort", () => console.log("keep-awake: released at KEEP_AWAKE_UNTIL_MS"));
  return new KeepAwakeGuard({ signal });
}

/// CPU_LOG_SECS, as deploy/app-server/index.ts: every N seconds the
/// cumulative CPU seconds of every process in the instance's pid namespace
/// (user+sys, reaped children included) and the kernel's busy time.
function startCpuLog(secs: number): void {
  const bootMs = Date.now();
  const sum = (v: string[]) => v.reduce((a, x) => a + Number(x || 0), 0);
  setInterval(() => {
    let pids: string[];
    try {
      pids = readdirSync("/proc").filter((p) => /^\d+$/.test(p));
    } catch {
      return; // no /proc: not Linux
    }
    let proc = 0;
    for (const pid of pids) {
      try {
        const st = readFileSync(`/proc/${pid}/stat`, "utf8");
        proc += sum(st.slice(st.lastIndexOf(")") + 2).split(" ").slice(11, 15)); // utime stime cutime cstime
      } catch {
        // exited
      }
    }
    let busy: number | null = null;
    try {
      const c = readFileSync("/proc/stat", "utf8").split("\n")[0].trim().split(/\s+/).slice(1);
      busy = sum([c[0], c[1], c[2], c[5], c[6]]); // user nice system irq softirq
    } catch {
      // none
    }
    console.log(`cpu sample: ${JSON.stringify({ boot_ms: bootMs, t_ms: Date.now(), proc_cpu_s: proc / 100, vm_busy_s: busy === null ? null : busy / 100 })}`);
  }, secs * 1000);
}

/// RESOLV_OVERRIDE, written before anything resolves a name (the feeds
/// download below, and the musl binary, which re-reads resolv.conf per
/// lookup); a failure is logged, as the campaign wrapper does.
function applyResolv(conf: string): void {
  try {
    const before = readFileSync("/etc/resolv.conf", "utf8").trim();
    writeFileSync("/etc/resolv.conf", conf);
    console.log(`resolv.conf override: was ${JSON.stringify(before)} now ${JSON.stringify(conf.trim())}`);
  } catch (e) {
    console.error(`resolv.conf override FAILED: ${e}`);
  }
}

/// Step 8: the bundle materialized once, then polled.
async function startFeeds(env: Record<string, string>, dir: string): Promise<void> {
  const key = env.FEEDS_S3_KEY;
  const bundlePath = join(tmpdir(), "feeds-bundle.json");
  try {
    await downloadFile(key, bundlePath, console.log, env);
    const version = materialize(dir, readFileSync(bundlePath, "utf8"));
    console.log(`feeds materialized: ${dir}/{keys,policies,grants}.json gen=${version}`);
  } catch (e) {
    throw new Refuse(`feeds bundle ${key}: ${(e as Error).message}`);
  }
  new FeedPoller(s3BundleFetcher(key, env), dir, console.log).start();
}

let prepared: Prepared;
try {
  prepared = prepare();
} catch (e) {
  await refuse(e instanceof Refuse ? e.message : `boot failed: ${(e as Error).message}`);
}
const { env, keepAwake, sha, bin, feedsDir, resolv } = prepared;
/// Held for the process's life: nothing releases it but a refusal below
/// or KEEP_AWAKE_UNTIL_MS.
const keepAwakeGuard = holdKeepAwake(keepAwake);
const cpuLogSecs = Number(env.CPU_LOG_SECS ?? 0);
if (cpuLogSecs > 0) startCpuLog(cpuLogSecs);
if (resolv !== null) applyResolv(resolv);
try {
  await startFeeds(env, feedsDir);
} catch (e) {
  keepAwakeGuard?.release();
  await refuse(e instanceof Refuse ? e.message : `feeds failed: ${(e as Error).message}`);
}
console.log(`binary sha256 ${sha}`);
console.log(`starting streams-slate on :${PORT}`);
await superviseBinary(bin, ["--listen", `0.0.0.0:${PORT}`], env, policyFor("app-server", env));
