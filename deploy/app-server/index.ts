// Downloads the binary (SERVER_BINARY_S3_KEY) and runs it.
import { chmod } from "node:fs/promises";
import { KeepAwakeGuard } from "@prisma/compute";
if (process.env.KEEP_AWAKE === "1") new KeepAwakeGuard();
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
