import { Result } from "better-result";
import type {
  StreamProfileDefinition,
  StreamProfilePersistResult,
  StreamProfileReadResult,
} from "./profile";
import { cloneStreamProfileSpec, expectPlainObjectResult, rejectUnknownKeysResult, normalizeProfileContentType } from "./profile";
import { buildInternalMetricsRegistry, buildMetricsDefaultRegistry } from "./metrics/schema";
import { normalizeMetricsRecordResult } from "./metrics/normalize";

const INTERNAL_METRICS_STREAM = "__stream_metrics__";

export type MetricsStreamProfile = {
  kind: "metrics";
};

function cloneMetricsProfile(): MetricsStreamProfile {
  return { kind: "metrics" };
}

function validateMetricsProfileResult(raw: unknown, path: string): Result<MetricsStreamProfile, { message: string }> {
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  if (objRes.value.kind !== "metrics") return Result.err({ message: `${path}.kind must be metrics` });
  const keyCheck = rejectUnknownKeysResult(objRes.value, ["kind"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  return Result.ok(cloneMetricsProfile());
}

export const METRICS_STREAM_PROFILE_DEFINITION: StreamProfileDefinition = {
  kind: "metrics",
  usesStoredProfileRow: false,

  defaultProfile(): MetricsStreamProfile {
    return cloneMetricsProfile();
  },

  validateResult(raw, path) {
    return validateMetricsProfileResult(raw, path);
  },

  readProfileResult(): Result<StreamProfileReadResult, { message: string }> {
    return Result.ok({ profile: cloneMetricsProfile(), cache: null });
  },

  persistProfileResult({ db, registry, stream, streamRow, profile }): Result<StreamProfilePersistResult, { kind: "bad_request"; message: string }> {
    if (profile.kind !== "metrics") return Result.err({ kind: "bad_request", message: "invalid metrics profile" });
    const contentType = normalizeProfileContentType(streamRow.content_type);
    if (contentType !== "application/json") {
      return Result.err({
        kind: "bad_request",
        message: "metrics profile requires application/json stream content-type",
      });
    }
    const desiredRegistry =
      stream === INTERNAL_METRICS_STREAM ? buildInternalMetricsRegistry(stream) : buildMetricsDefaultRegistry(stream);
    const registryRes = registry.replaceRegistryResult(stream, desiredRegistry);
    if (Result.isError(registryRes)) return Result.err({ kind: "bad_request", message: registryRes.error.message });
    db.updateStreamProfile(stream, "metrics");
    db.deleteStreamProfile(stream);
    db.deleteStreamTouchState(stream);
    return Result.ok({
      profile: cloneStreamProfileSpec(cloneMetricsProfile()),
      cache: null,
      schemaRegistry: registryRes.value,
    });
  },

  jsonIngest: {
    prepareRecordResult({ value }) {
      const normalizedRes = normalizeMetricsRecordResult(value);
      if (Result.isError(normalizedRes)) return normalizedRes;
      return Result.ok({
        value: normalizedRes.value.value,
        routingKey: normalizedRes.value.routingKey,
      });
    },
  },

  metrics: {
    normalizeRecordResult({ value }) {
      return normalizeMetricsRecordResult(value);
    },
  },
};
