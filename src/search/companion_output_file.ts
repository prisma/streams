import { closeSync, mkdirSync, openSync, renameSync, unlinkSync, writeFileSync, writeSync } from "node:fs";
import { join } from "node:path";
import { Result } from "better-result";

export type CompanionOutputFile = {
  localPath: string;
  sizeBytes: number;
};

function buildCompanionOutputFilePath(outputDir: string, filePrefix: string): string {
  const nonce = `${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2)}`;
  return join(outputDir, `${filePrefix}-${nonce}.cix`);
}

export function writeCompanionOutputFileResult(
  outputDir: string,
  filePrefix: string,
  payload: Uint8Array
): Result<CompanionOutputFile, { message: string }> {
  mkdirSync(outputDir, { recursive: true });
  const finalPath = buildCompanionOutputFilePath(outputDir, filePrefix);
  const tmpPath = `${finalPath}.tmp`;
  try {
    writeFileSync(tmpPath, payload);
    renameSync(tmpPath, finalPath);
    return Result.ok({ localPath: finalPath, sizeBytes: payload.byteLength });
  } catch (error: unknown) {
    try {
      unlinkSync(tmpPath);
    } catch {
      // ignore temp cleanup failures
    }
    return Result.err({ message: String((error as any)?.message ?? error) });
  }
}

export function writeCompanionOutputChunksResult(
  outputDir: string,
  filePrefix: string,
  chunks: Uint8Array[],
  sizeBytes: number
): Result<CompanionOutputFile, { message: string }> {
  mkdirSync(outputDir, { recursive: true });
  const finalPath = buildCompanionOutputFilePath(outputDir, filePrefix);
  const tmpPath = `${finalPath}.tmp`;
  let fd: number | null = null;
  try {
    fd = openSync(tmpPath, "w");
    for (const chunk of chunks) {
      let written = 0;
      while (written < chunk.byteLength) {
        written += writeSync(fd, chunk, written, chunk.byteLength - written);
      }
    }
    closeSync(fd);
    fd = null;
    renameSync(tmpPath, finalPath);
    return Result.ok({ localPath: finalPath, sizeBytes });
  } catch (error: unknown) {
    if (fd != null) {
      try {
        closeSync(fd);
      } catch {
        // ignore close failures
      }
    }
    try {
      unlinkSync(tmpPath);
    } catch {
      // ignore temp cleanup failures
    }
    return Result.err({ message: String((error as any)?.message ?? error) });
  }
}
