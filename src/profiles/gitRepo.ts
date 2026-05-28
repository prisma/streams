import { Result } from "better-result";
import type {
  StreamProfileDefinition,
  StreamProfilePersistResult,
  StreamProfileReadResult,
} from "./profile";
import {
  cloneStreamProfileSpec,
  expectPlainObjectResult,
  normalizeProfileContentType,
  parseStoredProfileJsonResult,
  rejectUnknownKeysResult,
  type StreamProfileSpec,
} from "./profile";
import { handleGitRepoRoute } from "../git_repo/server";
import {
  GIT_REPO_PROFILE_KIND,
  GIT_REPO_PROFILE_VERSION,
  defaultGitRepoProfileConfig,
  type GitObjectFormat,
  type GitRepoProfileConfig,
} from "../git_repo/types";

function defaultGitRepoProfile(): GitRepoProfileConfig {
  return defaultGitRepoProfileConfig();
}

function readObjectFormat(value: unknown): Result<GitObjectFormat, { message: string }> {
  if (value === undefined) return Result.ok("sha1");
  if (value === "sha1" || value === "sha256") return Result.ok(value);
  return Result.err({ message: "profile.objectFormat must be sha1 or sha256" });
}

function normalizeDefaultBranch(value: unknown): Result<string, { message: string }> {
  if (value === undefined) return Result.ok("refs/heads/main");
  if (typeof value !== "string" || value.trim() === "") return Result.err({ message: "profile.defaultBranch must be a non-empty string" });
  const branch = value.trim();
  return Result.ok(branch.startsWith("refs/") ? branch : `refs/heads/${branch}`);
}

function validateGitRepoProfileResult(raw: unknown, path: string): Result<GitRepoProfileConfig, { message: string }> {
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  if (objRes.value.kind !== GIT_REPO_PROFILE_KIND) return Result.err({ message: `${path}.kind must be ${GIT_REPO_PROFILE_KIND}` });
  const keyCheck = rejectUnknownKeysResult(
    objRes.value,
    ["kind", "version", "objectFormat", "defaultBranch", "http", "fetch", "push", "materialization", "importExport"],
    path
  );
  if (Result.isError(keyCheck)) return keyCheck;
  if (objRes.value.version !== undefined && objRes.value.version !== GIT_REPO_PROFILE_VERSION) {
    return Result.err({ message: `${path}.version must be ${GIT_REPO_PROFILE_VERSION}` });
  }
  const objectFormatRes = readObjectFormat(objRes.value.objectFormat);
  if (Result.isError(objectFormatRes)) return objectFormatRes;
  const defaultBranchRes = normalizeDefaultBranch(objRes.value.defaultBranch);
  if (Result.isError(defaultBranchRes)) return defaultBranchRes;
  const defaults = defaultGitRepoProfile();
  return Result.ok({
    ...defaults,
    objectFormat: objectFormatRes.value,
    defaultBranch: defaultBranchRes.value,
    http: { ...defaults.http!, ...((objRes.value.http ?? {}) as Record<string, unknown>) },
    fetch: { ...defaults.fetch!, ...((objRes.value.fetch ?? {}) as Record<string, unknown>) },
    push: { ...defaults.push!, ...((objRes.value.push ?? {}) as Record<string, unknown>) },
    materialization: { ...defaults.materialization!, ...((objRes.value.materialization ?? {}) as Record<string, unknown>) },
    importExport: { ...defaults.importExport!, ...((objRes.value.importExport ?? {}) as Record<string, unknown>) },
  } as GitRepoProfileConfig);
}

export const GIT_REPO_STREAM_PROFILE_DEFINITION: StreamProfileDefinition = {
  kind: GIT_REPO_PROFILE_KIND,
  usesStoredProfileRow: true,

  defaultProfile(): GitRepoProfileConfig {
    return defaultGitRepoProfile();
  },

  validateResult(raw, path) {
    return validateGitRepoProfileResult(raw, path);
  },

  readProfileResult({ row, cached }): Result<StreamProfileReadResult, { message: string }> {
    if (!row) return Result.ok({ profile: defaultGitRepoProfile(), cache: null });
    if (cached && cached.updatedAtMs === row.updated_at_ms) {
      return Result.ok({ profile: cloneStreamProfileSpec(cached.profile), cache: cached });
    }
    const parsedRes = parseStoredProfileJsonResult(row.profile_json);
    if (Result.isError(parsedRes)) return parsedRes;
    const profileRes = validateGitRepoProfileResult(parsedRes.value, "profile");
    if (Result.isError(profileRes)) return profileRes;
    return Result.ok({
      profile: cloneStreamProfileSpec(profileRes.value),
      cache: { profile: cloneStreamProfileSpec(profileRes.value), updatedAtMs: row.updated_at_ms },
    });
  },

  persistProfileResult({ db, stream, streamRow, profile }): Result<StreamProfilePersistResult, { kind: "bad_request"; message: string }> {
    const profileRes = validateGitRepoProfileResult(profile, "profile");
    if (Result.isError(profileRes)) return Result.err({ kind: "bad_request", message: profileRes.error.message });
    const contentType = normalizeProfileContentType(streamRow.content_type);
    if (contentType !== "application/json") {
      return Result.err({
        kind: "bad_request",
        message: "git-repo profile requires application/json stream content-type",
      });
    }
    if (streamRow.profile !== GIT_REPO_PROFILE_KIND && streamRow.next_offset > 0n) {
      return Result.err({
        kind: "bad_request",
        message: "git-repo profile must be installed before appending data",
      });
    }
    const persisted = profileRes.value;
    db.updateStreamProfile(stream, GIT_REPO_PROFILE_KIND);
    db.upsertStreamProfile(stream, JSON.stringify(persisted));
    db.deleteStreamTouchState(stream);
    const row = db.getStreamProfile(stream);
    return Result.ok({
      profile: cloneStreamProfileSpec(persisted) as StreamProfileSpec,
      cache: { profile: cloneStreamProfileSpec(persisted), updatedAtMs: row?.updated_at_ms ?? db.nowMs() },
      schemaRegistry: null,
    });
  },

  routes: {
    handleRoute(args) {
      return handleGitRepoRoute(args);
    },
  },
};
