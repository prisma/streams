import { fileURLToPath } from "node:url";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { Result } from "better-result";
import type { CompanionBuildInput, CompanionBuildOutput } from "./companion_build";
import { decodeIndexBuildWire, encodeIndexBuildWire } from "../index/index_build_wire";

type CompanionBuildSubprocessOutput =
  | { ok: true; result: CompanionBuildOutput }
  | { ok: false; message: string };

function readPipeText(pipe: Uint8Array | null | undefined): string {
  if (!pipe) return "";
  return new TextDecoder().decode(pipe);
}

export function runCompanionBuildSubprocessResult(
  input: CompanionBuildInput
): Result<CompanionBuildOutput, { message: string }> {
  const tempDir = mkdtempSync(join(tmpdir(), "ds-companion-build-subprocess-"));
  const inputPath = join(tempDir, "input.json");
  const outputPath = join(tempDir, "output.json");
  const runnerPath = fileURLToPath(new URL("./companion_build_subprocess_runner.ts", import.meta.url));
  try {
    writeFileSync(inputPath, encodeIndexBuildWire(input));
    const proc = Bun.spawnSync({
      cmd: [process.execPath, "run", runnerPath, inputPath, outputPath],
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
      env: process.env,
    });
    if (proc.exitCode !== 0) {
      const details = [readPipeText(proc.stderr), readPipeText(proc.stdout)].map((value) => value.trim()).filter((value) => value.length > 0).join("\n");
      return Result.err({
        message: details.length > 0 ? details : `companion build subprocess exited with code ${proc.exitCode}`,
      });
    }
    const output = decodeIndexBuildWire<CompanionBuildSubprocessOutput>(readFileSync(outputPath, "utf8"));
    return output.ok ? Result.ok(output.result) : Result.err({ message: output.message });
  } catch (error: unknown) {
    return Result.err({ message: String((error as Error)?.message ?? error) });
  } finally {
    try {
      rmSync(tempDir, { recursive: true, force: true });
    } catch {
      // ignore cleanup failures
    }
  }
}
