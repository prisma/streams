// Downloads the binary (SERVER_BINARY_S3_KEY) and runs it.
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
// superviseBinary never returns: if the binary exits it binds $PORT and
// serves the exit code + stderr tail, so a dead service is diagnosable
// over HTTP instead of looking like a platform 404 (deploy/README.md).
const { superviseBinary } = await import("./supervise");
await superviseBinary(bin, ["--listen", `0.0.0.0:${port}`]);
