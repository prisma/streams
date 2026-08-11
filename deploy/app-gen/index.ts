// Downloads awsbench (AWSBENCH_S3_KEY) and runs it as a load generator.
// awsbench binds $PORT itself and serves its collected JSONL there, so the
// results are scrapeable over HTTP for the whole run.
//
// Deployed IN the region under test, co-located with the server: that makes
// the measurement Streams' own roundtrip rather than the operator's distance
// to the region (docs/SOAK-REGIONS.md).
import { chmod } from "node:fs/promises";
import { KeepAwakeGuard } from "@prisma/compute";
if (process.env.KEEP_AWAKE === "1") new KeepAwakeGuard();

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
// than leaving the domain to 404 like a cold start.
const { superviseBinary } = await import("./supervise");
await superviseBinary(bin);
