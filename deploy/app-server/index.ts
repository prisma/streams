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
// ALWAYS download: reused warm instances keep /tmp across versions,
// so a cached binary silently pins the previous release (2026-07-19).
{
  await downloadBinary(process.env.SERVER_BINARY_S3_KEY ?? "", bin, console.log);
}
await chmod(bin, 0o755);
const port = process.env.PORT ?? "8080";
console.log(`starting streams-slate on :${port}`);
// superviseBinary never returns: if the binary exits it binds $PORT and
// serves the exit code + stderr tail, so a dead service is diagnosable
// over HTTP instead of looking like a platform 404 (deploy/README.md).
const { superviseBinary } = await import("./supervise");
await superviseBinary(bin, ["--listen", `0.0.0.0:${port}`]);
