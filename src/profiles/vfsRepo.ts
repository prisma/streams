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
  rejectUnknownKeysResult,
  type StreamProfileSpec,
} from "./profile";
import { handleVfsRepoRoute } from "../vfs/server";
import { VFS_PROFILE_KIND, VFS_PROFILE_VERSION } from "../vfs/types";

export type VfsRepoStreamProfile = {
  kind: typeof VFS_PROFILE_KIND;
  version: typeof VFS_PROFILE_VERSION;
};

function cloneVfsRepoProfile(): VfsRepoStreamProfile {
  return { kind: VFS_PROFILE_KIND, version: VFS_PROFILE_VERSION };
}

function validateVfsRepoProfileResult(raw: unknown, path: string): Result<VfsRepoStreamProfile, { message: string }> {
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  if (objRes.value.kind !== VFS_PROFILE_KIND) return Result.err({ message: `${path}.kind must be ${VFS_PROFILE_KIND}` });
  const keyCheck = rejectUnknownKeysResult(objRes.value, ["kind", "version"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  if (objRes.value.version !== undefined && objRes.value.version !== VFS_PROFILE_VERSION) {
    return Result.err({ message: `${path}.version must be ${VFS_PROFILE_VERSION}` });
  }
  return Result.ok(cloneVfsRepoProfile());
}

export const VFS_REPO_STREAM_PROFILE_DEFINITION: StreamProfileDefinition = {
  kind: VFS_PROFILE_KIND,
  usesStoredProfileRow: false,

  defaultProfile(): VfsRepoStreamProfile {
    return cloneVfsRepoProfile();
  },

  validateResult(raw, path) {
    return validateVfsRepoProfileResult(raw, path);
  },

  readProfileResult(): Result<StreamProfileReadResult, { message: string }> {
    return Result.ok({ profile: cloneVfsRepoProfile(), cache: null });
  },

  persistProfileResult({ db, stream, streamRow, profile }): Result<StreamProfilePersistResult, { kind: "bad_request"; message: string }> {
    if (profile.kind !== VFS_PROFILE_KIND) return Result.err({ kind: "bad_request", message: "invalid vfs-repo profile" });
    const contentType = normalizeProfileContentType(streamRow.content_type);
    if (contentType !== "application/json") {
      return Result.err({
        kind: "bad_request",
        message: "vfs-repo profile requires application/json stream content-type",
      });
    }
    if (streamRow.profile !== VFS_PROFILE_KIND && streamRow.next_offset > 0n) {
      return Result.err({
        kind: "bad_request",
        message: "vfs-repo profile must be installed before appending data",
      });
    }
    db.updateStreamProfile(stream, VFS_PROFILE_KIND);
    db.deleteStreamProfile(stream);
    db.deleteStreamTouchState(stream);
    const persisted: StreamProfileSpec = cloneStreamProfileSpec(cloneVfsRepoProfile());
    return Result.ok({ profile: persisted, cache: null, schemaRegistry: null });
  },

  vfs: {
    handleRoute(args) {
      return handleVfsRepoRoute(args);
    },
  },
};
