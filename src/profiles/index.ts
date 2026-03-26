import { Result } from "better-result";
import type { SqliteDurableStore, StreamRow } from "../db/db";
import type { SchemaRegistry, SchemaRegistryStore } from "../schema/registry";
import { LruCache } from "../util/lru";
import { dsError } from "../util/ds_error.ts";
import { GENERIC_STREAM_PROFILE_DEFINITION } from "./generic";
import { EVLOG_STREAM_PROFILE_DEFINITION } from "./evlog";
import {
  buildStreamProfileResource,
  cloneStreamProfileSpec,
  DEFAULT_STREAM_PROFILE,
  parseProfileUpdateEnvelopeResult,
  readProfileKindResult,
  type CachedStreamProfile,
  type StoredProfileRow,
  type StreamProfileJsonIngestCapability,
  type StreamProfileDefinition,
  type StreamProfileReadError,
  type StreamProfileResource,
  type StreamProfileSpec,
  type StreamProfileMutationError,
  type StreamTouchCapability,
} from "./profile";
import { STATE_PROTOCOL_STREAM_PROFILE_DEFINITION } from "./stateProtocol";

export * from "./profile";
export { EVLOG_STREAM_PROFILE_DEFINITION } from "./evlog";
export { GENERIC_STREAM_PROFILE_DEFINITION } from "./generic";
export { STATE_PROTOCOL_STREAM_PROFILE_DEFINITION } from "./stateProtocol";

const STREAM_PROFILE_DEFINITIONS: Record<string, StreamProfileDefinition> = {
  [EVLOG_STREAM_PROFILE_DEFINITION.kind]: EVLOG_STREAM_PROFILE_DEFINITION,
  [GENERIC_STREAM_PROFILE_DEFINITION.kind]: GENERIC_STREAM_PROFILE_DEFINITION,
  [STATE_PROTOCOL_STREAM_PROFILE_DEFINITION.kind]: STATE_PROTOCOL_STREAM_PROFILE_DEFINITION,
};
// New built-in profiles are wired here. Core runtime paths must resolve the
// definition and dispatch through its hooks rather than branching on profile
// kinds directly.

function supportedProfileKindsMessage(): string {
  return listSupportedStreamProfileKinds().join("|");
}

export function listSupportedStreamProfileKinds(): string[] {
  return Object.keys(STREAM_PROFILE_DEFINITIONS);
}

export function resolveStreamProfileDefinition(kind: string | null | undefined): StreamProfileDefinition | null {
  const normalized = typeof kind === "string" && kind !== "" ? kind : DEFAULT_STREAM_PROFILE;
  return STREAM_PROFILE_DEFINITIONS[normalized] ?? null;
}

function resolveStreamProfileDefinitionResult(
  kind: string | null | undefined
): Result<StreamProfileDefinition, StreamProfileReadError> {
  const normalized = typeof kind === "string" && kind !== "" ? kind : DEFAULT_STREAM_PROFILE;
  const definition = resolveStreamProfileDefinition(normalized);
  if (!definition) {
    return Result.err({ kind: "invalid_profile", message: `unknown stream profile: ${normalized}` });
  }
  return Result.ok(definition);
}

function cloneCachedProfile(cache: CachedStreamProfile | null): CachedStreamProfile | null {
  if (!cache) return null;
  return {
    profile: cloneStreamProfileSpec(cache.profile),
    updatedAtMs: cache.updatedAtMs,
  };
}

export function parseProfileUpdateResult(body: unknown): Result<StreamProfileSpec, { message: string }> {
  const envelopeRes = parseProfileUpdateEnvelopeResult(body);
  if (Result.isError(envelopeRes)) return envelopeRes;
  const kindRes = readProfileKindResult(envelopeRes.value, "profile");
  if (Result.isError(kindRes)) return kindRes;
  const definition = resolveStreamProfileDefinition(kindRes.value);
  if (!definition) {
    return Result.err({ message: `profile.kind must be ${supportedProfileKindsMessage()}` });
  }
  return definition.validateResult(envelopeRes.value, "profile");
}

export function resolveTouchCapability(profile: StreamProfileSpec | null | undefined): StreamTouchCapability | null {
  if (!profile) return null;
  return resolveStreamProfileDefinition(profile.kind)?.touch ?? null;
}

export function resolveJsonIngestCapability(profile: StreamProfileSpec | null | undefined): StreamProfileJsonIngestCapability | null {
  if (!profile) return null;
  return resolveStreamProfileDefinition(profile.kind)?.jsonIngest ?? null;
}

export function resolveEnabledTouchCapability(
  profile: StreamProfileSpec | null | undefined
): { capability: StreamTouchCapability; touchCfg: NonNullable<ReturnType<StreamTouchCapability["getTouchConfig"]>> } | null {
  const capability = resolveTouchCapability(profile);
  if (!profile || !capability) return null;
  const touchCfg = capability.getTouchConfig(profile);
  if (!touchCfg) return null;
  return { capability, touchCfg };
}

export function listTouchCapableProfileKinds(): string[] {
  return Object.values(STREAM_PROFILE_DEFINITIONS)
    .filter((definition) => !!definition.touch)
    .map((definition) => definition.kind);
}

export type StreamProfileUpdateResult = {
  resource: StreamProfileResource;
  schemaRegistry: SchemaRegistry | null;
};

export class StreamProfileStore {
  private readonly db: SqliteDurableStore;
  private readonly registry: SchemaRegistryStore;
  private readonly cache: LruCache<string, CachedStreamProfile>;

  constructor(db: SqliteDurableStore, registry: SchemaRegistryStore, opts?: { cacheEntries?: number }) {
    this.db = db;
    this.registry = registry;
    this.cache = new LruCache(opts?.cacheEntries ?? 1024);
  }

  private loadRow(stream: string): StoredProfileRow | null {
    return this.db.getStreamProfile(stream);
  }

  getProfile(stream: string, streamRow?: StreamRow | null): StreamProfileSpec {
    const res = this.getProfileResult(stream, streamRow);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  getProfileResult(stream: string, streamRow?: StreamRow | null): Result<StreamProfileSpec, StreamProfileReadError> {
    const srow = streamRow ?? this.db.getStream(stream);
    if (!srow) return Result.ok(GENERIC_STREAM_PROFILE_DEFINITION.defaultProfile());

    const definitionRes = resolveStreamProfileDefinitionResult(srow.profile);
    if (Result.isError(definitionRes)) return definitionRes;

    const row = definitionRes.value.usesStoredProfileRow ? this.loadRow(stream) : null;
    const cached = cloneCachedProfile(this.cache.get(stream) ?? null);
    const readRes = definitionRes.value.readProfileResult({
      row,
      cached: cached && cached.profile.kind === definitionRes.value.kind ? cached : null,
    });
    if (Result.isError(readRes)) {
      return Result.err({ kind: "invalid_profile", message: readRes.error.message });
    }

    if (readRes.value.cache) this.cache.set(stream, cloneCachedProfile(readRes.value.cache)!);
    else this.cache.delete(stream);
    return Result.ok(cloneStreamProfileSpec(readRes.value.profile));
  }

  getProfileResource(stream: string, streamRow?: StreamRow | null): StreamProfileResource {
    const res = this.getProfileResourceResult(stream, streamRow);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  getProfileResourceResult(stream: string, streamRow?: StreamRow | null): Result<StreamProfileResource, StreamProfileReadError> {
    const profileRes = this.getProfileResult(stream, streamRow);
    if (Result.isError(profileRes)) return profileRes;
    return Result.ok(buildStreamProfileResource(profileRes.value));
  }

  updateProfile(stream: string, streamRow: StreamRow, profile: StreamProfileSpec): StreamProfileResource {
    const res = this.updateProfileResult(stream, streamRow, profile);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value.resource;
  }

  updateProfileResult(
    stream: string,
    streamRow: StreamRow,
    profile: StreamProfileSpec
  ): Result<StreamProfileUpdateResult, StreamProfileMutationError> {
    const definition = resolveStreamProfileDefinition(profile.kind);
    if (!definition) {
      return Result.err({ kind: "bad_request", message: `profile.kind must be ${supportedProfileKindsMessage()}` });
    }

    const persistRes = definition.persistProfileResult({ db: this.db, registry: this.registry, stream, streamRow, profile });
    if (Result.isError(persistRes)) return persistRes;

    if (persistRes.value.cache) this.cache.set(stream, cloneCachedProfile(persistRes.value.cache)!);
    else this.cache.delete(stream);
    return Result.ok({
      resource: buildStreamProfileResource(cloneStreamProfileSpec(persistRes.value.profile)),
      schemaRegistry: persistRes.value.schemaRegistry ?? null,
    });
  }
}
