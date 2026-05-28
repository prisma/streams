#!/usr/bin/env bun

import { Result } from "better-result";
import { migrateVfsRepoToGitRepoResult } from "../src/vfs/migration";
import { dsError } from "../src/util/ds_error.ts";

const args = process.argv.slice(2);

function argValue(flag: string): string | null {
  const idx = args.indexOf(flag);
  if (idx < 0) return null;
  return args[idx + 1] ?? null;
}

function required(flag: string): string {
  const value = argValue(flag);
  if (!value) throw dsError(`${flag} is required`);
  return value;
}

function optionalNumber(flag: string): number | undefined {
  const raw = argValue(flag);
  if (raw == null) return undefined;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) throw dsError(`invalid ${flag}: ${raw}`);
  return value;
}

const streamsUrl = argValue("--streams-url") ?? "http://127.0.0.1:8080";
const sourceStream = required("--source-stream");
const targetStream = required("--target-stream");
const ref = argValue("--ref") ?? "main";
const authToken = argValue("--auth-token") ?? undefined;

const result = await migrateVfsRepoToGitRepoResult({
  streamsUrl,
  sourceStream,
  targetStream,
  ref,
  limit: optionalNumber("--limit"),
  authToken,
});

if (Result.isError(result)) {
  console.error(result.error.message);
  process.exit(1);
}

console.log(JSON.stringify(result.value, null, 2));
