import { Result } from "better-result";
import type {
  StreamProfileDefinition,
  StreamProfilePersistResult,
  StreamProfileReadResult,
} from "./profile";
import { cloneStreamProfileSpec, expectPlainObjectResult, rejectUnknownKeysResult, type StreamProfileSpec } from "./profile";

export type GenericStreamProfile = {
  kind: "generic";
};

function cloneGenericProfile(): GenericStreamProfile {
  return { kind: "generic" };
}

export const GENERIC_STREAM_PROFILE_DEFINITION: StreamProfileDefinition = {
  kind: "generic",
  usesStoredProfileRow: false,

  defaultProfile(): GenericStreamProfile {
    return cloneGenericProfile();
  },

  validateResult(raw, path) {
    const objRes = expectPlainObjectResult(raw, path);
    if (Result.isError(objRes)) return objRes;
    if (objRes.value.kind !== "generic") {
      return Result.err({ message: `${path}.kind must be generic` });
    }
    const keyCheck = rejectUnknownKeysResult(objRes.value, ["kind"], path);
    if (Result.isError(keyCheck)) return keyCheck;
    return Result.ok(cloneGenericProfile());
  },

  readProfileResult(): Result<StreamProfileReadResult, { message: string }> {
    return Result.ok({ profile: cloneGenericProfile(), cache: null });
  },

  persistProfileResult({ db, stream }): Result<StreamProfilePersistResult, { kind: "bad_request"; message: string; code?: string }> {
    db.updateStreamProfile(stream, "generic");
    db.deleteStreamProfile(stream);
    db.deleteStreamTouchState(stream);
    const profile: StreamProfileSpec = cloneStreamProfileSpec(cloneGenericProfile());
    return Result.ok({ profile, cache: null });
  },
};
