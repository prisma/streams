import Ajv from "ajv";
import { createHash } from "node:crypto";
import type { SqliteDurableStore, StreamRow } from "../db/db";
import { Result } from "better-result";
import { LruCache } from "../util/lru";
import { DURABLE_LENS_V1_SCHEMA } from "./lens_schema";
import { compileLensResult, lensFromJson, type CompiledLens, type Lens } from "../lens/lens";
import { validateLensAgainstSchemasResult, fillLensDefaultsResult } from "./proof";
import { parseJsonPointerResult } from "../util/json_pointer";
import { dsError } from "../util/ds_error.ts";

export const SCHEMA_REGISTRY_API_VERSION = "durable.streams/schema-registry/v1" as const;

export type RoutingKeyConfig = {
  jsonPointer: string;
  required: boolean;
};

export type SchemaRegistry = {
  apiVersion: typeof SCHEMA_REGISTRY_API_VERSION;
  schema: string;
  currentVersion: number;
  routingKey?: RoutingKeyConfig;
  boundaries: Array<{ offset: number; version: number }>;
  schemas: Record<string, any>;
  lenses: Record<string, any>;
};

export type SchemaRegistryMutationError = {
  kind: "version_mismatch" | "bad_request";
  message: string;
  code?: string;
};

export type SchemaRegistryReadError = {
  kind: "invalid_registry" | "invalid_lens_chain";
  message: string;
  code?: string;
};

type RegistryRow = { stream: string; registry_json: string; updated_at_ms: bigint };

type Validator = ReturnType<Ajv["compile"]>;

const AJV = new Ajv({
  allErrors: true,
  strict: false,
  allowUnionTypes: true,
  validateSchema: false,
});

const LENS_VALIDATOR = AJV.compile(DURABLE_LENS_V1_SCHEMA);

function sha256Hex(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

function defaultRegistry(stream: string): SchemaRegistry {
  return {
    apiVersion: SCHEMA_REGISTRY_API_VERSION,
    schema: stream,
    currentVersion: 0,
    boundaries: [],
    schemas: {},
    lenses: {},
  };
}

function ensureNoRefResult(schema: any): Result<void, { message: string }> {
  const stack: any[] = [schema];
  while (stack.length > 0) {
    const cur = stack.pop();
    if (!cur || typeof cur !== "object") continue;
    if (Object.prototype.hasOwnProperty.call(cur, "$ref")) {
      return Result.err({ message: "external $ref is not supported" });
    }
    for (const v of Object.values(cur)) {
      if (v && typeof v === "object") stack.push(v);
    }
  }
  return Result.ok(undefined);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function rejectUnknownKeysResult(
  obj: Record<string, unknown>,
  allowed: readonly string[],
  path: string
): Result<void, { message: string }> {
  const allowedSet = new Set(allowed);
  for (const key of Object.keys(obj)) {
    if (!allowedSet.has(key)) return Result.err({ message: `${path}.${key} is not supported` });
  }
  return Result.ok(undefined);
}

function parseRoutingKeyConfigResult(raw: unknown, path: string): Result<RoutingKeyConfig | null, { message: string }> {
  if (raw == null) return Result.ok(null);
  if (!isPlainObject(raw)) return Result.err({ message: `${path} must be an object or null` });
  const keyCheck = rejectUnknownKeysResult(raw, ["jsonPointer", "required"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  if (typeof raw.jsonPointer !== "string") return Result.err({ message: `${path}.jsonPointer must be a string` });
  const pointerRes = parseJsonPointerResult(raw.jsonPointer);
  if (Result.isError(pointerRes)) return Result.err({ message: pointerRes.error.message });
  if (typeof raw.required !== "boolean") return Result.err({ message: `${path}.required must be boolean` });
  return Result.ok({ jsonPointer: raw.jsonPointer, required: raw.required });
}

function validateJsonSchemaResult(schema: any): Result<void, { message: string }> {
  const noRefRes = ensureNoRefResult(schema);
  if (Result.isError(noRefRes)) return noRefRes;
  try {
    const validate = AJV.compile(schema);
    if (!validate) return Result.err({ message: "schema validation failed" });
  } catch (e: any) {
    return Result.err({ message: String(e?.message ?? e) });
  }
  return Result.ok(undefined);
}

function parseRegistryResult(stream: string, json: string): Result<SchemaRegistry, { message: string }> {
  let raw: unknown;
  try {
    raw = JSON.parse(json);
  } catch (e: any) {
    return Result.err({ message: String(e?.message ?? e) });
  }
  if (!isPlainObject(raw)) return Result.err({ message: "invalid schema registry" });
  const keyCheck = rejectUnknownKeysResult(
    raw,
    ["apiVersion", "schema", "currentVersion", "routingKey", "boundaries", "schemas", "lenses"],
    "registry"
  );
  if (Result.isError(keyCheck)) return keyCheck;
  if (raw.apiVersion !== SCHEMA_REGISTRY_API_VERSION) return Result.err({ message: "invalid registry apiVersion" });

  const routingKeyRes = parseRoutingKeyConfigResult(raw.routingKey, "routingKey");
  if (Result.isError(routingKeyRes)) return routingKeyRes;

  const boundariesRaw = Array.isArray(raw.boundaries) ? raw.boundaries : [];
  const boundaries: Array<{ offset: number; version: number }> = [];
  for (const item of boundariesRaw) {
    if (!isPlainObject(item)) return Result.err({ message: "invalid boundary entry" });
    const offset = typeof item.offset === "number" && Number.isFinite(item.offset) ? item.offset : null;
    const version = typeof item.version === "number" && Number.isFinite(item.version) ? item.version : null;
    if (offset == null || version == null) return Result.err({ message: "invalid boundary entry" });
    boundaries.push({ offset, version });
  }

  const schemas = isPlainObject(raw.schemas) ? raw.schemas : {};
  const lenses = isPlainObject(raw.lenses) ? raw.lenses : {};
  const currentVersion =
    typeof raw.currentVersion === "number" && Number.isFinite(raw.currentVersion) ? raw.currentVersion : 0;
  const schemaName = typeof raw.schema === "string" && raw.schema.trim() !== "" ? raw.schema : stream;

  return Result.ok({
    apiVersion: SCHEMA_REGISTRY_API_VERSION,
    schema: schemaName,
    currentVersion,
    routingKey: routingKeyRes.value ?? undefined,
    boundaries,
    schemas,
    lenses,
  });
}

function serializeRegistry(reg: SchemaRegistry): string {
  return JSON.stringify(reg);
}

function validateLensResult(raw: any): Result<Lens, { message: string }> {
  const ok = LENS_VALIDATOR(raw);
  if (!ok) {
    const msg = AJV.errorsText(LENS_VALIDATOR.errors || undefined);
    return Result.err({ message: `invalid lens: ${msg}` });
  }
  return Result.ok(raw as Lens);
}

export function parseSchemaUpdateResult(
  body: unknown
): Result<{ schema?: any; lens?: any; routingKey?: RoutingKeyConfig | null }, { message: string }> {
  if (!isPlainObject(body)) return Result.err({ message: "schema update must be a JSON object" });
  const keyCheck = rejectUnknownKeysResult(body, ["apiVersion", "schema", "lens", "routingKey"], "schemaUpdate");
  if (Result.isError(keyCheck)) return keyCheck;
  if (body.apiVersion !== undefined && body.apiVersion !== SCHEMA_REGISTRY_API_VERSION) {
    return Result.err({ message: "invalid schema apiVersion" });
  }

  const hasSchema = Object.prototype.hasOwnProperty.call(body, "schema");
  const hasRoutingKey = Object.prototype.hasOwnProperty.call(body, "routingKey");
  if (!hasSchema && !hasRoutingKey) {
    return Result.err({ message: "schema update must include schema or routingKey" });
  }
  if (!hasSchema && body.lens !== undefined) {
    return Result.err({ message: "schema update lens requires schema" });
  }

  const routingKeyRes = hasRoutingKey ? parseRoutingKeyConfigResult(body.routingKey, "routingKey") : Result.ok(null);
  if (Result.isError(routingKeyRes)) return routingKeyRes;
  if (hasSchema && hasRoutingKey && routingKeyRes.value == null) {
    return Result.err({ message: "schema update routingKey must be an object when schema is provided" });
  }

  const out: { schema?: any; lens?: any; routingKey?: RoutingKeyConfig | null } = {};
  if (hasSchema) out.schema = body.schema;
  if (body.lens !== undefined) out.lens = body.lens;
  if (hasRoutingKey) out.routingKey = routingKeyRes.value;
  return Result.ok(out);
}

function bigintToNumberSafeResult(v: bigint): Result<number, { message: string }> {
  const max = BigInt(Number.MAX_SAFE_INTEGER);
  if (v > max) return Result.err({ message: "offset exceeds MAX_SAFE_INTEGER" });
  return Result.ok(Number(v));
}

export class SchemaRegistryStore {
  private readonly db: SqliteDurableStore;
  private readonly registryCache: LruCache<string, { reg: SchemaRegistry; updatedAtMs: bigint }>;
  private readonly validatorCache: LruCache<string, Validator>;
  private readonly lensCache: LruCache<string, CompiledLens>;
  private readonly lensChainCache: LruCache<string, CompiledLens[]>;

  constructor(db: SqliteDurableStore, opts?: { registryCacheEntries?: number; validatorCacheEntries?: number; lensCacheEntries?: number }) {
    this.db = db;
    this.registryCache = new LruCache(opts?.registryCacheEntries ?? 1024);
    this.validatorCache = new LruCache(opts?.validatorCacheEntries ?? 256);
    this.lensCache = new LruCache(opts?.lensCacheEntries ?? 256);
    this.lensChainCache = new LruCache(opts?.lensCacheEntries ?? 256);
  }

  private loadRow(stream: string): RegistryRow | null {
    return this.db.getSchemaRegistry(stream);
  }

  getRegistry(stream: string): SchemaRegistry {
    const res = this.getRegistryResult(stream);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  getRegistryResult(stream: string): Result<SchemaRegistry, SchemaRegistryReadError> {
    const row = this.loadRow(stream);
    if (!row) return Result.ok(defaultRegistry(stream));
    const cached = this.registryCache.get(stream);
    if (cached && cached.updatedAtMs === row.updated_at_ms) return Result.ok(cached.reg);
    const parseRes = parseRegistryResult(stream, row.registry_json);
    if (Result.isError(parseRes)) {
      return Result.err({ kind: "invalid_registry", message: parseRes.error.message });
    }
    const reg = parseRes.value;
    this.registryCache.set(stream, { reg, updatedAtMs: row.updated_at_ms });
    return Result.ok(reg);
  }

  updateRegistry(
    stream: string,
    streamRow: StreamRow,
    update: { schema: any; lens?: any; routingKey?: RoutingKeyConfig }
  ): SchemaRegistry {
    const res = this.updateRegistryResult(stream, streamRow, update);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  updateRegistryResult(
    stream: string,
    streamRow: StreamRow,
    update: { schema: any; lens?: any; routingKey?: RoutingKeyConfig }
  ): Result<SchemaRegistry, SchemaRegistryMutationError> {
    if (update.routingKey) {
      const pointerRes = parseJsonPointerResult(update.routingKey.jsonPointer);
      if (Result.isError(pointerRes)) {
        return Result.err({ kind: "bad_request", message: pointerRes.error.message });
      }
      if (typeof update.routingKey.required !== "boolean") {
        return Result.err({ kind: "bad_request", message: "routingKey.required must be boolean" });
      }
    }
    if (update.schema === undefined) return Result.err({ kind: "bad_request", message: "missing schema" });
    const schemaRes = validateJsonSchemaResult(update.schema);
    if (Result.isError(schemaRes)) return Result.err({ kind: "bad_request", message: schemaRes.error.message });

    const regRes = this.getRegistryResult(stream);
    if (Result.isError(regRes)) return Result.err({ kind: "bad_request", message: regRes.error.message, code: regRes.error.code });
    const reg = regRes.value;
    const currentVersion = reg.currentVersion ?? 0;
    const streamEmpty = streamRow.next_offset === 0n;

    if (currentVersion === 0) {
      if (!streamEmpty) return Result.err({ kind: "bad_request", message: "first schema requires empty stream" });
      if (update.lens) {
        const lensRes = validateLensResult(update.lens);
        if (Result.isError(lensRes)) return Result.err({ kind: "bad_request", message: lensRes.error.message });
        if (lensRes.value.from !== 0 || lensRes.value.to !== 1) {
          return Result.err({
            kind: "version_mismatch",
            message: "lens version mismatch",
            code: "schema_lens_version_mismatch",
          });
        }
      }
      const nextReg: SchemaRegistry = {
        apiVersion: "durable.streams/schema-registry/v1",
        schema: stream,
        currentVersion: 1,
        routingKey: update.routingKey,
        boundaries: [{ offset: 0, version: 1 }],
        schemas: { ...reg.schemas, ["1"]: update.schema },
        lenses: { ...reg.lenses },
      };
      this.persist(stream, nextReg);
      return Result.ok(nextReg);
    }

    if (!update.lens) return Result.err({ kind: "bad_request", message: "lens required" });
    const lensRes = validateLensResult(update.lens);
    if (Result.isError(lensRes)) return Result.err({ kind: "bad_request", message: lensRes.error.message });
    const lens = lensRes.value;
    if (lens.from !== currentVersion || lens.to !== currentVersion + 1) {
      return Result.err({
        kind: "version_mismatch",
        message: "lens version mismatch",
        code: "schema_lens_version_mismatch",
      });
    }
    if (lens.schema && lens.schema !== reg.schema) return Result.err({ kind: "bad_request", message: "lens schema mismatch" });

    const oldSchema = reg.schemas[String(currentVersion)];
    if (!oldSchema) return Result.err({ kind: "bad_request", message: "missing current schema" });
    const proofRes = validateLensAgainstSchemasResult(oldSchema, update.schema, lens);
    if (Result.isError(proofRes)) return Result.err({ kind: "bad_request", message: proofRes.error.message });
    const defaultsRes = fillLensDefaultsResult(lens, update.schema);
    if (Result.isError(defaultsRes)) return Result.err({ kind: "bad_request", message: defaultsRes.error.message });

    const boundaryRes = bigintToNumberSafeResult(streamRow.next_offset);
    if (Result.isError(boundaryRes)) return Result.err({ kind: "bad_request", message: boundaryRes.error.message });

    const nextVersion = currentVersion + 1;
    const nextReg: SchemaRegistry = {
      apiVersion: "durable.streams/schema-registry/v1",
      schema: reg.schema ?? stream,
      currentVersion: nextVersion,
      routingKey: update.routingKey ?? reg.routingKey,
      boundaries: [...reg.boundaries, { offset: boundaryRes.value, version: nextVersion }],
      schemas: { ...reg.schemas, [String(nextVersion)]: update.schema },
      lenses: { ...reg.lenses, [String(currentVersion)]: defaultsRes.value },
    };
    this.persist(stream, nextReg);
    return Result.ok(nextReg);
  }

  updateRoutingKey(stream: string, routingKey: RoutingKeyConfig | null): SchemaRegistry {
    const res = this.updateRoutingKeyResult(stream, routingKey);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  updateRoutingKeyResult(stream: string, routingKey: RoutingKeyConfig | null): Result<SchemaRegistry, SchemaRegistryMutationError> {
    if (routingKey) {
      const pointerRes = parseJsonPointerResult(routingKey.jsonPointer);
      if (Result.isError(pointerRes)) {
        return Result.err({ kind: "bad_request", message: pointerRes.error.message });
      }
      if (typeof routingKey.required !== "boolean") {
        return Result.err({ kind: "bad_request", message: "routingKey.required must be boolean" });
      }
    }
    const regRes = this.getRegistryResult(stream);
    if (Result.isError(regRes)) return Result.err({ kind: "bad_request", message: regRes.error.message, code: regRes.error.code });
    const nextReg: SchemaRegistry = {
      ...regRes.value,
      routingKey: routingKey ?? undefined,
    };
    this.persist(stream, nextReg);
    return Result.ok(nextReg);
  }

  private persist(stream: string, reg: SchemaRegistry): void {
    const json = serializeRegistry(reg);
    this.db.upsertSchemaRegistry(stream, json);
    this.registryCache.set(stream, { reg, updatedAtMs: this.db.nowMs() });
  }

  getValidatorForVersion(reg: SchemaRegistry, version: number): Validator | null {
    const schema = reg.schemas[String(version)];
    if (!schema) return null;
    const hash = sha256Hex(JSON.stringify(schema));
    const cached = this.validatorCache.get(hash);
    if (cached) return cached;
    const validate = AJV.compile(schema);
    this.validatorCache.set(hash, validate);
    return validate;
  }

  getLensChain(reg: SchemaRegistry, fromVersion: number, toVersion: number): CompiledLens[] {
    const res = this.getLensChainResult(reg, fromVersion, toVersion);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  getLensChainResult(reg: SchemaRegistry, fromVersion: number, toVersion: number): Result<CompiledLens[], SchemaRegistryReadError> {
    const key = `${reg.schema}:${fromVersion}->${toVersion}`;
    const cached = this.lensChainCache.get(key);
    if (cached) return Result.ok(cached);
    const chain: CompiledLens[] = [];
    for (let v = fromVersion; v < toVersion; v++) {
      const lensRaw = reg.lenses[String(v)];
      if (!lensRaw) {
        return Result.err({
          kind: "invalid_lens_chain",
          message: `missing lens v${v}->v${v + 1}`,
        });
      }
      const hash = sha256Hex(JSON.stringify(lensRaw));
      let compiled = this.lensCache.get(hash);
      if (!compiled) {
        const compiledRes = compileLensResult(lensFromJson(lensRaw));
        if (Result.isError(compiledRes)) {
          return Result.err({
            kind: "invalid_lens_chain",
            message: compiledRes.error.message,
          });
        }
        compiled = compiledRes.value;
        this.lensCache.set(hash, compiled);
      }
      chain.push(compiled);
    }
    this.lensChainCache.set(key, chain);
    return Result.ok(chain);
  }
}
