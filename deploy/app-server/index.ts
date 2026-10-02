// Downloads the binary (SERVER_BINARY_S3_KEY) and runs it.
import { chmod } from "node:fs/promises";
import { KeepAwakeGuard } from "@prisma/compute";
// KEEP_AWAKE_UNTIL_MS (epoch ms, set by bench/k2cost/field) bounds the guard
// with AbortSignal.timeout, so an instance nobody tears down stops billing on
// its own once idle; without it the guard holds for the process's life.
const keepAwake = (() => {
  if (process.env.KEEP_AWAKE !== "1") return null;
  const until = Number(process.env.KEEP_AWAKE_UNTIL_MS ?? 0);
  if (!until) return new KeepAwakeGuard();
  const left = until - Date.now();
  console.log(`keep-awake: until ${new Date(until).toISOString()} (${Math.round(left / 1000)} s left)`);
  if (left <= 0) return null;
  const signal = AbortSignal.timeout(left);
  signal.addEventListener("abort", () => console.log("keep-awake: released at KEEP_AWAKE_UNTIL_MS"));
  return new KeepAwakeGuard({ signal });
})();

// Instance shape, logged once at boot: the K2 cost field runs price memory by
// instance size, which no platform API reports (bench/k2cost/field/README.md).
const instanceShape = await (async () => {
  const { existsSync, readFileSync } = await import("node:fs");
  const { cpus, totalmem } = await import("node:os");
  const read = (p: string) => { try { return readFileSync(p, "utf8").trim(); } catch { return null; } };
  return {
    mem_total_bytes: totalmem(), cgroup_memory_max: read("/sys/fs/cgroup/memory.max"),
    cpus: cpus().length, cgroup_cpu_max: read("/sys/fs/cgroup/cpu.max"),
    libc: existsSync("/lib/ld-musl-x86_64.so.1") ? "musl" : existsSync("/lib64/ld-linux-x86-64.so.2") ? "glibc" : "unknown",
  };
})();
console.log(`instance shape: ${JSON.stringify(instanceShape)}`);

// CPU_LOG_SECS (set by bench/k2cost/field): every N awake seconds, log the
// cumulative CPU seconds since boot of every process in this instance's pid
// namespace (user+sys, reaped children included) and the kernel's busy time,
// so active CPU is priced for every role, routers included.
if (Number(process.env.CPU_LOG_SECS ?? 0) > 0) {
  const { readdirSync, readFileSync } = await import("node:fs");
  const bootMs = Date.now();
  const sum = (v: string[]) => v.reduce((a, x) => a + Number(x || 0), 0);
  setInterval(() => {
    let proc = 0;
    for (const pid of readdirSync("/proc")) {
      if (!/^\d+$/.test(pid)) continue;
      try {
        const st = readFileSync(`/proc/${pid}/stat`, "utf8");
        proc += sum(st.slice(st.lastIndexOf(")") + 2).split(" ").slice(11, 15)); // utime stime cutime cstime
      } catch { /* exited */ }
    }
    let busy: number | null = null;
    try {
      const c = readFileSync("/proc/stat", "utf8").split("\n")[0]!.trim().split(/\s+/).slice(1);
      busy = sum([c[0]!, c[1]!, c[2]!, c[5]!, c[6]!]); // user nice system irq softirq
    } catch { /* none */ }
    console.log(`cpu sample: ${JSON.stringify({ boot_ms: bootMs, t_ms: Date.now(), proc_cpu_s: proc / 100, vm_busy_s: busy === null ? null : busy / 100 })}`);
  }, Number(process.env.CPU_LOG_SECS) * 1000);
}

// DNS override (soak3 finding, docs/SOAK-REGIONS.md): the platform's
// per-node DNS forwarder episodically hands wrong-geo answers for
// Tigris's geo-DNS endpoint, which turns into cross-region object-store
// serving under load. When RESOLV_OVERRIDE is set, write it before
// anything resolves a name — the download below and the musl binary
// (which re-reads resolv.conf per lookup) both pick it up. "\\n" arrives
// literally through the deploy CLI's --env; unescape it.
if (process.env.RESOLV_OVERRIDE) {
  const conf = process.env.RESOLV_OVERRIDE.replace(/\\n/g, "\n") + "\n";
  try {
    const { readFileSync, writeFileSync } = await import("node:fs");
    const before = readFileSync("/etc/resolv.conf", "utf8").trim();
    writeFileSync("/etc/resolv.conf", conf);
    console.log(`resolv.conf override: was ${JSON.stringify(before)} now ${JSON.stringify(conf.trim())}`);
  } catch (e) {
    console.error(`resolv.conf override FAILED: ${e}`);
  }
}

const bin = "/tmp/streams-slate";
import("./downloader").catch(() => null); // static hint for bundler
const { downloadBinary } = await import("./downloader");
// R25-G: a failed download must be DIAGNOSABLE from outside. Exiting
// here leaves a platform 404 indistinguishable from an edge-routing
// failure — which cost the 2026-08-11 campaign its longest debugging
// detour. Serve the failure instead.
const serveDownloadFailure = (err: unknown) => {
  const body = JSON.stringify({
    stage: "binary_download",
    key: process.env.SERVER_BINARY_S3_KEY ?? process.env.AWSBENCH_S3_KEY ?? "",
    error: String(err),
  }, null, 2);
  console.error(`binary download failed; serving diagnostic: ${body}`);
  Bun.serve({
    port: Number(process.env.PORT ?? 8080),
    fetch: () => new Response(body, {
      status: 500,
      headers: { "content-type": "application/json" },
    }),
  });
  return new Promise<never>(() => {});
};
// ALWAYS download: reused warm instances keep /tmp across versions,
// so a cached binary silently pins the previous release (2026-07-19).
{
  try {
  await downloadBinary(process.env.SERVER_BINARY_S3_KEY ?? "", bin, console.log);
} catch (e) {
  await serveDownloadFailure(e);
}
}
await chmod(bin, 0o755);
// MT campaign: materialize the auth feed FILES before the binary
// starts — enforce mode refuses to serve without them. The bundle is
// one JSON {keys, policies, grants}; files are written atomically
// (tmp + rename) exactly like a platform projector would.
if (process.env.FEEDS_S3_KEY) {
  const { downloadFile } = await import("./downloader");
  const bundlePath = "/tmp/feeds-bundle.json";
  try {
    await downloadFile(process.env.FEEDS_S3_KEY, bundlePath, console.log);
  } catch (e) {
    await serveDownloadFailure(e);
  }
  const { mkdirSync, writeFileSync, renameSync } = await import("node:fs");
  mkdirSync("/tmp/feeds", { recursive: true });
  const bundle = JSON.parse(await Bun.file(bundlePath).text());
  for (const [name, doc] of [["keys", bundle.keys], ["policies", bundle.policies], ["grants", bundle.grants]]) {
    const path = `/tmp/feeds/${name}.json`;
    writeFileSync(`${path}.tmp`, JSON.stringify(doc));
    renameSync(`${path}.tmp`, path);
  }
  console.log(`feeds materialized: /tmp/feeds/{keys,policies,grants}.json gen=${bundle.keys?.feed_version}`);
}
// R26-9 build identity: hash the binary we actually downloaded and pass
// it into the child's env; the server echoes it on /v1/debug/load and
// verify-running compares it against the campaign's upload manifest.
// An "R25 marker present" check alone admits ANY post-R25 binary.
const hasher = new Bun.CryptoHasher("sha256");
hasher.update(await Bun.file(bin).arrayBuffer());
process.env.APP_BINARY_SHA256 = hasher.digest("hex");
console.log(`binary sha256 ${process.env.APP_BINARY_SHA256}`);
const port = process.env.PORT ?? "8080";
console.log(`starting streams-slate on :${port}`);
// superviseBinary: a binary that dies at boot (before it accepted on $PORT,
// or within its first minute) is held, and the wrapper binds $PORT and
// serves its exit code + stderr tail, so a dead service is diagnosable over
// HTTP instead of looking like a platform 404; one that dies after it was
// serving (an OOM kill, item 38's critical exit) ends this wrapper with its
// code, so Compute replaces the instance (item 39, deploy/README.md;
// policyFor, pinned by deploy/supervise.test.ts).
const { superviseBinary, policyFor } = await import("./supervise");
await superviseBinary(bin, ["--listen", `0.0.0.0:${port}`], process.env, policyFor("app-server", process.env));
