import { readFileSync, writeFileSync } from "node:fs";
import { Result } from "better-result";
import { decodeIndexBuildWire, encodeIndexBuildWire } from "../index/index_build_wire";
import type { CompanionBuildInput, CompanionBuildOutput } from "./companion_build";
import { buildEncodedBundledCompanionPayloadResult } from "./companion_build";
import { dsError } from "../util/ds_error";

type CompanionBuildSubprocessOutput =
  | { ok: true; result: CompanionBuildOutput }
  | { ok: false; message: string };

function main(): void {
  const inputPath = process.argv[2];
  const outputPath = process.argv[3];
  if (!inputPath || !outputPath) throw dsError("companion build subprocess runner requires input and output paths");
  const input = decodeIndexBuildWire<CompanionBuildInput>(readFileSync(inputPath, "utf8"));
  const buildRes = buildEncodedBundledCompanionPayloadResult(input);
  const output: CompanionBuildSubprocessOutput = Result.isOk(buildRes)
    ? { ok: true, result: buildRes.value }
    : { ok: false, message: buildRes.error.message };
  writeFileSync(outputPath, encodeIndexBuildWire(output));
}

try {
  main();
  process.exit(0);
} catch (error: unknown) {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exit(1);
}
