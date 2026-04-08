import { closeSync, mkdirSync, openSync, renameSync, unlinkSync, writeFileSync, writeSync } from "node:fs";
import { join } from "node:path";
import { Result } from "better-result";

export type IndexRunOutputFile = {
  localPath: string;
  sizeBytes: number;
};

export function writeIndexRunOutputFileResult(
  outputDir: string,
  filePrefix: string,
  payload: Uint8Array
): Result<IndexRunOutputFile, { message: string }> {
  mkdirSync(outputDir, { recursive: true });
  const nonce = `${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2)}`;
  const finalPath = join(outputDir, `${filePrefix}-${nonce}.irn`);
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

export function writeIndexRunOutputChunksResult(
  outputDir: string,
  filePrefix: string,
  chunks: Uint8Array[],
  sizeBytes: number
): Result<IndexRunOutputFile, { message: string }> {
  mkdirSync(outputDir, { recursive: true });
  const nonce = `${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2)}`;
  const finalPath = join(outputDir, `${filePrefix}-${nonce}.irn`);
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
