import { readFileSync, writeFileSync } from "node:fs";
import { Result } from "better-result";
import { runIndexBuildJobResult } from "./index_build_executor";
import type { IndexBuildJobInput, IndexBuildJobOutput } from "./index_build_job";
import { dsError } from "../util/ds_error";
import { decodeIndexBuildWire, encodeIndexBuildWire } from "./index_build_wire";

type IndexBuildSubprocessResult =
  | { ok: true; result: IndexBuildJobOutput }
  | { ok: false; message: string };

function main(): void {
  const inputPath = process.argv[2];
  const outputPath = process.argv[3];
  if (!inputPath || !outputPath) {
    throw dsError("index build subprocess requires input and output paths");
  }
  const job = decodeIndexBuildWire<IndexBuildJobInput>(readFileSync(inputPath, "utf8"));
  const buildRes = runIndexBuildJobResult(job);
  const output: IndexBuildSubprocessResult = Result.isOk(buildRes)
    ? { ok: true, result: buildRes.value }
    : { ok: false, message: buildRes.error.message };
  writeFileSync(outputPath, encodeIndexBuildWire(output));
}

try {
  main();
  process.exit(0);
} catch (error) {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exit(1);
}
