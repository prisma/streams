// Gate the conformance run against an EXACT expected outcome.
//
// A pass/fail count alone is not a gate: a suite that silently ran
// nothing also reports zero failures, and a test that turns into a skip
// looks identical to one that was never there. So the expected totals
// are recorded and compared, and every skip must belong to the one
// family we knowingly do not implement.
import { readFileSync } from "node:fs";

const want = JSON.parse(readFileSync(new URL("expected.json", import.meta.url)));
const got = JSON.parse(readFileSync(new URL("result.json", import.meta.url)));

let bad = 0;
for (const k of ["numTotalTests", "numPassedTests", "numFailedTests", "numPendingTests"]) {
  if (got[k] !== want[k]) {
    console.error(`${k}: expected ${want[k]}, got ${got[k]}`);
    bad++;
  }
}
for (const suite of got.testResults ?? []) {
  for (const t of suite.assertionResults ?? []) {
    if (t.status === "failed") {
      console.error(`FAILED  ${(t.ancestorTitles ?? []).join(" > ")} > ${t.title}`);
      bad++;
    } else if (t.status !== "passed") {
      const family = (t.ancestorTitles ?? [])[0];
      if (family !== want.skippedAreAllIn) {
        console.error(`unexpected skip outside "${want.skippedAreAllIn}": ${family} > ${t.title}`);
        bad++;
      }
    }
  }
}
if (bad) {
  console.error(`\nconformance gate FAILED (${bad} problem(s)) against ${want.suite}`);
  process.exit(1);
}
console.log(
  `conformance gate OK: ${got.numPassedTests} passed, ${got.numFailedTests} failed, ` +
    `${got.numPendingTests} skipped (${got.numTotalTests}) against ${want.suite}`,
);
