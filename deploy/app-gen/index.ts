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
const bin = "/tmp/awsbench";
import("./downloader").catch(() => null); // static hint for the bundler
const { downloadBinary } = await import("./downloader");
// ALWAYS download: warm instances keep /tmp across versions (2026-07-19).
await downloadBinary(process.env.AWSBENCH_S3_KEY ?? "", bin, console.log);
await chmod(bin, 0o755);
console.log(
  `starting awsbench system=${process.env.BENCH_SYSTEM} shape=${process.env.BENCH_SHAPE}`,
);
// See app-server/index.ts: a dead binary serves its own diagnostic rather
// than leaving the domain to 404 like a cold start.
const { superviseBinary } = await import("./supervise");
await superviseBinary(bin);
