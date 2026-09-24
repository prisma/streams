#!/usr/bin/env node
// Negative control for the platform-e2e battery's feed-fault legs: with
// two injections refused by the emulator, the battery must fail on exactly
// the checks that prove a fault was applied (external review: acceptance
// record). A refused injection that still passed a leg is the defect
// 034d809d fixed; this keeps that fix from regressing silently.
import { spawnSync } from "node:child_process";

const expected = [
  "FAIL fault generation-regression injected status 400",
  "FAIL fault same-gen-drift injected status 400",
  "FAIL regression replayed a generation below the one drift reuses",
  "PLATFORM_E2E_FAIL (3)",
];
const run = spawnSync(process.execPath, ["scripts/platform-e2e.mjs"], {
  env: { ...process.env, PLATFORM_E2E_REFUSE_FAULTS: "generation-regression,same-gen-drift" },
  encoding: "utf8",
  maxBuffer: 64 * 1024 * 1024,
});
const output = `${run.stdout}\n${run.stderr}`;
const missing = expected.filter((line) => !output.includes(line));
if (run.status === 0 || missing.length) {
  process.stdout.write(output);
  console.error(`PLATFORM_E2E_NEGATIVE_FAIL: exit ${run.status}; missing ${JSON.stringify(missing)}`);
  process.exit(1);
}
console.log("PLATFORM_E2E_NEGATIVE_OK: two refused injections failed exactly their three checks");
