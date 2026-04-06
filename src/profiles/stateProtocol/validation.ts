import { Result } from "better-result";
import type { CachedStreamProfile, StreamProfileSpec } from "../profile";
import {
  cloneStreamProfileSpec,
  expectPlainObjectResult,
  rejectUnknownKeysResult,
} from "../profile";
import { validateTouchConfigResult, type TouchConfig } from "../../touch/spec";
import type { StateProtocolStreamProfile } from "./types";

export function isStateProtocolProfile(
  profile: StreamProfileSpec | null | undefined
): profile is StateProtocolStreamProfile {
  return !!profile && profile.kind === "state-protocol";
}

export function getStateProtocolTouchConfig(profile: StreamProfileSpec | null | undefined): TouchConfig | null {
  return isStateProtocolProfile(profile) && profile.touch?.enabled ? profile.touch : null;
}

export function cloneStateProtocolProfile(profile: StateProtocolStreamProfile): StateProtocolStreamProfile {
  return cloneStreamProfileSpec(profile) as StateProtocolStreamProfile;
}

export function cloneStateProtocolCache(cache: CachedStreamProfile | null): CachedStreamProfile | null {
  if (!cache || cache.profile.kind !== "state-protocol") return null;
  return {
    profile: cloneStateProtocolProfile(cache.profile as StateProtocolStreamProfile),
    updatedAtMs: cache.updatedAtMs,
  };
}

export function validateStateProtocolProfileResult(
  raw: unknown,
  path: string
): Result<StateProtocolStreamProfile, { message: string }> {
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  if (objRes.value.kind !== "state-protocol") {
    return Result.err({ message: `${path}.kind must be state-protocol` });
  }
  const keyCheck = rejectUnknownKeysResult(objRes.value, ["kind", "touch"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  let touch = undefined;
  if (objRes.value.touch !== undefined) {
    const touchRes = validateTouchConfigResult(objRes.value.touch, `${path}.touch`);
    if (Result.isError(touchRes)) return Result.err({ message: touchRes.error.message });
    touch = touchRes.value;
  }
  return Result.ok(touch ? { kind: "state-protocol", touch } : { kind: "state-protocol" });
}
