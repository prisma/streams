// Downloads the pilot binary (LB_BINARY_S3_KEY) and runs it as the
// rendezvous-hash router. Clients only ever talk to this tier: Compute
// sleeps idle instances and answers 404 while one wakes, so routing must
// never be the wake mechanism (RUNBOOK section 6).
import { chmod } from "node:fs/promises";
import { KeepAwakeGuard } from "@prisma/compute";
if (process.env.KEEP_AWAKE === "1") new KeepAwakeGuard();
const bin = "/tmp/pilot";
import("./downloader").catch(() => null); // static hint for the bundler
const { downloadBinary } = await import("./downloader");
// ALWAYS download: warm instances keep /tmp across versions, so a cached
// binary silently pins the previous release (2026-07-19).
await downloadBinary(process.env.LB_BINARY_S3_KEY ?? "", bin, console.log);
await chmod(bin, 0o755);
console.log(`starting pilot MODE=lb on :${process.env.PORT ?? "8080"}`);
// See app-server/index.ts: a dead binary serves its own diagnostic.
const { superviseBinary } = await import("./supervise");
await superviseBinary(bin, [], { ...process.env, MODE: "lb" });
