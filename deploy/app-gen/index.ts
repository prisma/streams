// Downloads awsbench (AWSBENCH_S3_KEY) and runs it as a load generator.
// awsbench binds $PORT itself and serves its collected JSONL there, so the
// results are scrapeable over HTTP for the whole run.
//
// Deployed IN the region under test, co-located with the server: that makes
// the measurement Streams' own roundtrip rather than the operator's distance
// to the region (docs/SOAK-REGIONS.md).
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

const bin = "/tmp/awsbench";
import("./downloader").catch(() => null); // static hint for the bundler
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
// ALWAYS download: warm instances keep /tmp across versions (2026-07-19).
try {
  await downloadBinary(process.env.AWSBENCH_S3_KEY ?? "", bin, console.log);
} catch (e) {
  await serveDownloadFailure(e);
}
await chmod(bin, 0o755);
// MT campaign: the per-project customer-token map for BENCH_MT=1.
if (process.env.TOKENS_S3_KEY) {
  const { downloadFile } = await import("./downloader");
  try {
    await downloadFile(process.env.TOKENS_S3_KEY, "/tmp/tokens.json", console.log);
  } catch (e) {
    await serveDownloadFailure(e);
  }
  process.env.BENCH_TOKENS_FILE = "/tmp/tokens.json";
}
// R26-9 build identity: hash the downloaded binary; awsbench echoes it
// in every stats line ("binSha256") for verify-running.
const hasher = new Bun.CryptoHasher("sha256");
hasher.update(await Bun.file(bin).arrayBuffer());
process.env.APP_BINARY_SHA256 = hasher.digest("hex");
console.log(`binary sha256 ${process.env.APP_BINARY_SHA256}`);
console.log(
  `starting awsbench system=${process.env.BENCH_SYSTEM} shape=${process.env.BENCH_SHAPE}`,
);
// See app-server/index.ts: a dead binary serves its own diagnostic rather
// than leaving the domain to 404 like a cold start. A load generator holds
// every death, a ready one's included (item 39; policyFor, pinned by
// deploy/supervise.test.ts).
const { superviseBinary, policyFor } = await import("./supervise");
await superviseBinary(bin, [], process.env, policyFor("app-gen", process.env));
