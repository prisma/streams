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
import { handleVfsRepoRoute } from "../vfs/server";
import { VFS_PROFILE_VERSION } from "../vfs/types";

export const WORKSPACE_FS_PROFILE_KIND = "workspace-fs" as const;

export type WorkspaceFsStreamProfile = {
  kind: typeof WORKSPACE_FS_PROFILE_KIND;
  version: typeof VFS_PROFILE_VERSION;
  gitRepo?: {
    stream: string;
  };
};

function cloneWorkspaceFsProfile(): WorkspaceFsStreamProfile {
  return { kind: WORKSPACE_FS_PROFILE_KIND, version: VFS_PROFILE_VERSION };
}

function validateWorkspaceFsProfileResult(raw: unknown, path: string): Result<WorkspaceFsStreamProfile, { message: string }> {
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  if (objRes.value.kind !== WORKSPACE_FS_PROFILE_KIND) return Result.err({ message: `${path}.kind must be ${WORKSPACE_FS_PROFILE_KIND}` });
  const keyCheck = rejectUnknownKeysResult(objRes.value, ["kind", "version", "gitRepo"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  if (objRes.value.version !== undefined && objRes.value.version !== VFS_PROFILE_VERSION) {
    return Result.err({ message: `${path}.version must be ${VFS_PROFILE_VERSION}` });
  }
  const profile = cloneWorkspaceFsProfile();
  if (objRes.value.gitRepo !== undefined) {
    const gitRepoRes = expectPlainObjectResult(objRes.value.gitRepo, `${path}.gitRepo`);
    if (Result.isError(gitRepoRes)) return gitRepoRes;
    const gitKeyCheck = rejectUnknownKeysResult(gitRepoRes.value, ["stream"], `${path}.gitRepo`);
    if (Result.isError(gitKeyCheck)) return gitKeyCheck;
    if (typeof gitRepoRes.value.stream !== "string" || gitRepoRes.value.stream.trim() === "") {
      return Result.err({ message: `${path}.gitRepo.stream must be a non-empty string` });
    }
    profile.gitRepo = { stream: gitRepoRes.value.stream.trim() };
  }
  return Result.ok(profile);
}

export const WORKSPACE_FS_STREAM_PROFILE_DEFINITION: StreamProfileDefinition = {
  kind: WORKSPACE_FS_PROFILE_KIND,
  usesStoredProfileRow: true,

  defaultProfile(): WorkspaceFsStreamProfile {
    return cloneWorkspaceFsProfile();
  },

  validateResult(raw, path) {
    return validateWorkspaceFsProfileResult(raw, path);
  },

  readProfileResult({ row, cached }): Result<StreamProfileReadResult, { message: string }> {
    if (!row) return Result.ok({ profile: cloneWorkspaceFsProfile(), cache: null });
    if (cached && cached.updatedAtMs === row.updated_at_ms) {
      return Result.ok({ profile: cloneStreamProfileSpec(cached.profile), cache: cached });
    }
    const parsedRes = parseStoredProfileJsonResult(row.profile_json);
    if (Result.isError(parsedRes)) return parsedRes;
    const profileRes = validateWorkspaceFsProfileResult(parsedRes.value, "profile");
    if (Result.isError(profileRes)) return profileRes;
    return Result.ok({
      profile: cloneStreamProfileSpec(profileRes.value),
      cache: { profile: cloneStreamProfileSpec(profileRes.value), updatedAtMs: row.updated_at_ms },
    });
  },

  persistProfileResult({ db, stream, streamRow, profile }): Result<StreamProfilePersistResult, { kind: "bad_request"; message: string }> {
    if (profile.kind !== WORKSPACE_FS_PROFILE_KIND) return Result.err({ kind: "bad_request", message: "invalid workspace-fs profile" });
    const contentType = normalizeProfileContentType(streamRow.content_type);
    if (contentType !== "application/json") {
      return Result.err({
        kind: "bad_request",
        message: "workspace-fs profile requires application/json stream content-type",
      });
    }
    if (streamRow.profile !== WORKSPACE_FS_PROFILE_KIND && streamRow.next_offset > 0n) {
      return Result.err({
        kind: "bad_request",
        message: "workspace-fs profile must be installed before appending data",
      });
    }
    const profileRes = validateWorkspaceFsProfileResult(profile, "profile");
    if (Result.isError(profileRes)) return Result.err({ kind: "bad_request", message: profileRes.error.message });
    db.updateStreamProfile(stream, WORKSPACE_FS_PROFILE_KIND);
    db.upsertStreamProfile(stream, JSON.stringify(profileRes.value));
    db.deleteStreamTouchState(stream);
    const row = db.getStreamProfile(stream);
    const persisted: StreamProfileSpec = cloneStreamProfileSpec(profileRes.value);
    return Result.ok({
      profile: persisted,
      cache: { profile: cloneStreamProfileSpec(profileRes.value), updatedAtMs: row?.updated_at_ms ?? db.nowMs() },
      schemaRegistry: null,
    });
  },

  vfs: {
    handleRoute(args) {
      return handleVfsRepoRoute(args);
    },
  },
};
