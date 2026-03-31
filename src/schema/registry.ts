import Ajv from "ajv";
import { createHash } from "node:crypto";
import type { SqliteDurableStore, StreamRow } from "../db/db";
import { Result } from "better-result";
import { LruCache } from "../util/lru";
import { DURABLE_LENS_V1_SCHEMA } from "./lens_schema";
import { compileLensResult, lensFromJson, type CompiledLens, type Lens } from "../lens/lens";
import { validateLensAgainstSchemasResult, fillLensDefaultsResult } from "./proof";
import { parseJsonPointerResult } from "../util/json_pointer";
import { parseDurationMsResult } from "../util/duration";
import { dsError } from "../util/ds_error.ts";

export const SCHEMA_REGISTRY_API_VERSION = "durable.streams/schema-registry/v1" as const;

export type RoutingKeyConfig = {
  jsonPointer: string;
  required: boolean;
};

export type SearchFieldKind = "keyword" | "text" | "integer" | "float" | "date" | "bool";

export type SearchFieldBinding = {
  version: number;
  jsonPointer: string;
};

export type SearchDefaultField = {
  field: string;
  boost?: number;
};

export type SearchFieldConfig = {
  kind: SearchFieldKind;
  bindings: SearchFieldBinding[];
  normalizer?: "identity_v1" | "lowercase_v1";
  analyzer?: "unicode_word_v1";
  exact?: boolean;
  prefix?: boolean;
  column?: boolean;
  exists?: boolean;
  sortable?: boolean;
  aggregatable?: boolean;
  contains?: boolean;
  positions?: boolean;
};

export type SearchRollupMeasureConfig =
  | { kind: "count" }
  | { kind: "summary"; field: string; histogram?: "log2_v1" }
  | {
      kind: "summary_parts";
      countJsonPointer: string;
      sumJsonPointer: string;
      minJsonPointer: string;
      maxJsonPointer: string;
      histogramJsonPointer?: string;
    };

export type SearchRollupConfig = {
  timestampField?: string;
  dimensions?: string[];
  intervals: string[];
  measures: Record<string, SearchRollupMeasureConfig>;
};

export type SearchConfig = {
  profile?: string;
  primaryTimestampField: string;
  defaultFields?: SearchDefaultField[];
  containsDefaultFields?: string[];
  aliases?: Record<string, string>;
  fields: Record<string, SearchFieldConfig>;
  rollups?: Record<string, SearchRollupConfig>;
};

export type SchemaRegistry = {
  apiVersion: typeof SCHEMA_REGISTRY_API_VERSION;
  schema: string;
  currentVersion: number;
  routingKey?: RoutingKeyConfig;
  search?: SearchConfig;
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

function isDateTimeString(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) {
    return false;
  }
  return !Number.isNaN(Date.parse(value));
}

AJV.addFormat("date-time", {
  type: "string",
  validate: isDateTimeString,
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

function validateSearchFieldNameResult(name: string, path: string): Result<string, { message: string }> {
  const trimmed = name.trim();
  if (trimmed === "") return Result.err({ message: `${path} must not be empty` });
  if (trimmed.length > 64) return Result.err({ message: `${path} too long (max 64)` });
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(trimmed)) {
    return Result.err({ message: `${path} must match ^[a-zA-Z0-9][a-zA-Z0-9._-]*$` });
  }
  return Result.ok(trimmed);
}

function parseSearchFieldBindingResult(raw: unknown, path: string): Result<SearchFieldBinding, { message: string }> {
  if (!isPlainObject(raw)) return Result.err({ message: `${path} must be an object` });
  const keyCheck = rejectUnknownKeysResult(raw, ["version", "jsonPointer"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  if (typeof raw.version !== "number" || !Number.isFinite(raw.version) || raw.version <= 0 || !Number.isInteger(raw.version)) {
    return Result.err({ message: `${path}.version must be a positive integer` });
  }
  if (typeof raw.jsonPointer !== "string") return Result.err({ message: `${path}.jsonPointer must be a string` });
  const pointerRes = parseJsonPointerResult(raw.jsonPointer);
  if (Result.isError(pointerRes)) return Result.err({ message: pointerRes.error.message });
  return Result.ok({
    version: raw.version,
    jsonPointer: raw.jsonPointer,
  });
}

function parseSearchDefaultFieldResult(raw: unknown, path: string): Result<SearchDefaultField, { message: string }> {
  if (!isPlainObject(raw)) return Result.err({ message: `${path} must be an object` });
  const keyCheck = rejectUnknownKeysResult(raw, ["field", "boost"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  if (typeof raw.field !== "string") return Result.err({ message: `${path}.field must be a string` });
  const fieldRes = validateSearchFieldNameResult(raw.field, `${path}.field`);
  if (Result.isError(fieldRes)) return fieldRes;
  if (raw.boost !== undefined && (typeof raw.boost !== "number" || !Number.isFinite(raw.boost) || raw.boost <= 0)) {
    return Result.err({ message: `${path}.boost must be a positive number` });
  }
  return Result.ok({
    field: fieldRes.value,
    boost: typeof raw.boost === "number" ? raw.boost : undefined,
  });
}

function parseSearchFieldConfigResult(raw: unknown, path: string): Result<SearchFieldConfig, { message: string }> {
  if (!isPlainObject(raw)) return Result.err({ message: `${path} must be an object` });
  const keyCheck = rejectUnknownKeysResult(
    raw,
    ["kind", "bindings", "normalizer", "analyzer", "exact", "prefix", "column", "exists", "sortable", "aggregatable", "contains", "positions"],
    path
  );
  if (Result.isError(keyCheck)) return keyCheck;
  if (
    raw.kind !== "keyword" &&
    raw.kind !== "text" &&
    raw.kind !== "integer" &&
    raw.kind !== "float" &&
    raw.kind !== "date" &&
    raw.kind !== "bool"
  ) {
    return Result.err({ message: `${path}.kind must be keyword, text, integer, float, date, or bool` });
  }
  if (!Array.isArray(raw.bindings) || raw.bindings.length === 0) {
    return Result.err({ message: `${path}.bindings must be a non-empty array` });
  }
  const bindings: SearchFieldBinding[] = [];
  const seenVersions = new Set<number>();
  for (let i = 0; i < raw.bindings.length; i++) {
    const bindingRes = parseSearchFieldBindingResult(raw.bindings[i], `${path}.bindings[${i}]`);
    if (Result.isError(bindingRes)) return bindingRes;
    if (seenVersions.has(bindingRes.value.version)) {
      return Result.err({ message: `${path}.bindings[${i}].version duplicates ${bindingRes.value.version}` });
    }
    seenVersions.add(bindingRes.value.version);
    bindings.push(bindingRes.value);
  }
  if (raw.normalizer !== undefined && raw.normalizer !== "identity_v1" && raw.normalizer !== "lowercase_v1") {
    return Result.err({ message: `${path}.normalizer must be identity_v1 or lowercase_v1` });
  }
  if (raw.analyzer !== undefined && raw.analyzer !== "unicode_word_v1") {
    return Result.err({ message: `${path}.analyzer must be unicode_word_v1` });
  }
  const out: SearchFieldConfig = {
    kind: raw.kind,
    bindings,
    normalizer: raw.normalizer as SearchFieldConfig["normalizer"] | undefined,
    analyzer: raw.analyzer as SearchFieldConfig["analyzer"] | undefined,
    exact: raw.exact === true ? true : undefined,
    prefix: raw.prefix === true ? true : undefined,
    column: raw.column === true ? true : undefined,
    exists: raw.exists === true ? true : undefined,
    sortable: raw.sortable === true ? true : undefined,
    aggregatable: raw.aggregatable === true ? true : undefined,
    contains: raw.contains === true ? true : undefined,
    positions: raw.positions === true ? true : undefined,
  };
  if (out.kind === "text") {
    if (!out.analyzer) return Result.err({ message: `${path}.analyzer is required for text fields` });
    if (out.column) return Result.err({ message: `${path}.column is not supported for text fields` });
    if (out.sortable) return Result.err({ message: `${path}.sortable is not supported for text fields` });
    if (out.aggregatable) return Result.err({ message: `${path}.aggregatable is not supported for text fields` });
  } else {
    if (out.positions) return Result.err({ message: `${path}.positions is only supported for text fields` });
  }
  if (out.kind === "keyword") {
    if (out.analyzer) return Result.err({ message: `${path}.analyzer is not supported for keyword fields` });
  }
  if (out.kind === "integer" || out.kind === "float" || out.kind === "date" || out.kind === "bool") {
    if (out.prefix) return Result.err({ message: `${path}.prefix is not supported for typed fields` });
    if (out.contains) return Result.err({ message: `${path}.contains is not supported for typed fields` });
    if (out.normalizer) return Result.err({ message: `${path}.normalizer is not supported for typed fields` });
  }
  return Result.ok(out);
}

function parseSearchRollupMeasureResult(
  raw: unknown,
  path: string,
  fields: Record<string, SearchFieldConfig>
): Result<SearchRollupMeasureConfig, { message: string }> {
  if (!isPlainObject(raw)) return Result.err({ message: `${path} must be an object` });
  if (raw.kind === "count") {
    const keyCheck = rejectUnknownKeysResult(raw, ["kind"], path);
    if (Result.isError(keyCheck)) return keyCheck;
    return Result.ok({ kind: "count" });
  }
  if (raw.kind === "summary") {
    const keyCheck = rejectUnknownKeysResult(raw, ["kind", "field", "histogram"], path);
    if (Result.isError(keyCheck)) return keyCheck;
    if (typeof raw.field !== "string") return Result.err({ message: `${path}.field must be a string` });
    const fieldRes = validateSearchFieldNameResult(raw.field, `${path}.field`);
    if (Result.isError(fieldRes)) return fieldRes;
    const field = fields[fieldRes.value];
    if (!field) return Result.err({ message: `${path}.field must reference a declared search field` });
    if (!field.aggregatable) return Result.err({ message: `${path}.field must reference an aggregatable search field` });
    if (field.kind !== "integer" && field.kind !== "float") {
      return Result.err({ message: `${path}.field must reference an integer or float field` });
    }
    if (raw.histogram !== undefined && raw.histogram !== "log2_v1") {
      return Result.err({ message: `${path}.histogram must be log2_v1` });
    }
    return Result.ok({
      kind: "summary",
      field: fieldRes.value,
      histogram: raw.histogram === "log2_v1" ? "log2_v1" : undefined,
    });
  }
  if (raw.kind === "summary_parts") {
    const keyCheck = rejectUnknownKeysResult(
      raw,
      ["kind", "countJsonPointer", "sumJsonPointer", "minJsonPointer", "maxJsonPointer", "histogramJsonPointer"],
      path
    );
    if (Result.isError(keyCheck)) return keyCheck;
    for (const key of ["countJsonPointer", "sumJsonPointer", "minJsonPointer", "maxJsonPointer"] as const) {
      if (typeof raw[key] !== "string") return Result.err({ message: `${path}.${key} must be a string` });
      const pointerRes = parseJsonPointerResult(raw[key]);
      if (Result.isError(pointerRes)) return Result.err({ message: pointerRes.error.message });
    }
    if (raw.histogramJsonPointer !== undefined) {
      if (typeof raw.histogramJsonPointer !== "string") return Result.err({ message: `${path}.histogramJsonPointer must be a string` });
      const pointerRes = parseJsonPointerResult(raw.histogramJsonPointer);
      if (Result.isError(pointerRes)) return Result.err({ message: pointerRes.error.message });
    }
    return Result.ok({
      kind: "summary_parts",
      countJsonPointer: raw.countJsonPointer as string,
      sumJsonPointer: raw.sumJsonPointer as string,
      minJsonPointer: raw.minJsonPointer as string,
      maxJsonPointer: raw.maxJsonPointer as string,
      histogramJsonPointer: typeof raw.histogramJsonPointer === "string" ? raw.histogramJsonPointer : undefined,
    });
  }
  return Result.err({ message: `${path}.kind must be count, summary, or summary_parts` });
}

function parseSearchRollupConfigResult(
  raw: unknown,
  path: string,
  fields: Record<string, SearchFieldConfig>,
  primaryTimestampField: string
): Result<SearchRollupConfig, { message: string }> {
  if (!isPlainObject(raw)) return Result.err({ message: `${path} must be an object` });
  const keyCheck = rejectUnknownKeysResult(raw, ["timestampField", "dimensions", "intervals", "measures"], path);
  if (Result.isError(keyCheck)) return keyCheck;

  const timestampFieldRaw = raw.timestampField === undefined ? primaryTimestampField : raw.timestampField;
  if (typeof timestampFieldRaw !== "string") return Result.err({ message: `${path}.timestampField must be a string` });
  const timestampFieldRes = validateSearchFieldNameResult(timestampFieldRaw, `${path}.timestampField`);
  if (Result.isError(timestampFieldRes)) return timestampFieldRes;
  const timestampField = fields[timestampFieldRes.value];
  if (!timestampField) return Result.err({ message: `${path}.timestampField must reference a declared field` });
  if (timestampField.kind !== "date") return Result.err({ message: `${path}.timestampField must reference a date field` });

  let dimensions: string[] | undefined;
  if (raw.dimensions !== undefined) {
    if (!Array.isArray(raw.dimensions)) return Result.err({ message: `${path}.dimensions must be an array` });
    dimensions = [];
    const seen = new Set<string>();
    for (let i = 0; i < raw.dimensions.length; i++) {
      if (typeof raw.dimensions[i] !== "string") return Result.err({ message: `${path}.dimensions[${i}] must be a string` });
      const dimRes = validateSearchFieldNameResult(raw.dimensions[i], `${path}.dimensions[${i}]`);
      if (Result.isError(dimRes)) return dimRes;
      if (seen.has(dimRes.value)) return Result.err({ message: `${path}.dimensions[${i}] duplicates ${dimRes.value}` });
      const field = fields[dimRes.value];
      if (!field) return Result.err({ message: `${path}.dimensions[${i}] must reference a declared field` });
      if (!field.exact) return Result.err({ message: `${path}.dimensions[${i}] must reference an exact-capable field` });
      seen.add(dimRes.value);
      dimensions.push(dimRes.value);
    }
  }

  if (!Array.isArray(raw.intervals) || raw.intervals.length === 0) {
    return Result.err({ message: `${path}.intervals must be a non-empty array` });
  }
  const intervals: string[] = [];
  const seenIntervals = new Set<string>();
  for (let i = 0; i < raw.intervals.length; i++) {
    if (typeof raw.intervals[i] !== "string") return Result.err({ message: `${path}.intervals[${i}] must be a string` });
    const parsedRes = parseDurationMsResult(raw.intervals[i]);
    if (Result.isError(parsedRes) || parsedRes.value <= 0) {
      return Result.err({ message: `${path}.intervals[${i}] must be a positive duration string` });
    }
    if (seenIntervals.has(raw.intervals[i])) {
      return Result.err({ message: `${path}.intervals[${i}] duplicates ${raw.intervals[i]}` });
    }
    seenIntervals.add(raw.intervals[i]);
    intervals.push(raw.intervals[i]);
  }

  if (!isPlainObject(raw.measures) || Object.keys(raw.measures).length === 0) {
    return Result.err({ message: `${path}.measures must be a non-empty object` });
  }
  const measures: Record<string, SearchRollupMeasureConfig> = {};
  for (const [measureName, measureRaw] of Object.entries(raw.measures)) {
    const nameRes = validateSearchFieldNameResult(measureName, `${path}.measures`);
    if (Result.isError(nameRes)) return nameRes;
    const measureRes = parseSearchRollupMeasureResult(measureRaw, `${path}.measures.${measureName}`, fields);
    if (Result.isError(measureRes)) return measureRes;
    measures[nameRes.value] = measureRes.value;
  }

  return Result.ok({
    timestampField: timestampFieldRes.value,
    dimensions,
    intervals,
    measures,
  });
}

function parseSearchConfigResult(raw: unknown, path: string): Result<SearchConfig | null, { message: string }> {
  if (raw == null) return Result.ok(null);
  if (!isPlainObject(raw)) return Result.err({ message: `${path} must be an object` });
  const keyCheck = rejectUnknownKeysResult(
    raw,
    ["profile", "primaryTimestampField", "defaultFields", "containsDefaultFields", "aliases", "fields", "rollups"],
    path
  );
  if (Result.isError(keyCheck)) return keyCheck;
  if (typeof raw.primaryTimestampField !== "string") {
    return Result.err({ message: `${path}.primaryTimestampField must be a string` });
  }
  const primaryFieldRes = validateSearchFieldNameResult(raw.primaryTimestampField, `${path}.primaryTimestampField`);
  if (Result.isError(primaryFieldRes)) return primaryFieldRes;
  if (!isPlainObject(raw.fields) || Object.keys(raw.fields).length === 0) {
    return Result.err({ message: `${path}.fields must be a non-empty object` });
  }
  const fields: Record<string, SearchFieldConfig> = {};
  for (const [fieldName, fieldRaw] of Object.entries(raw.fields)) {
    const nameRes = validateSearchFieldNameResult(fieldName, `${path}.fields`);
    if (Result.isError(nameRes)) return nameRes;
    const fieldRes = parseSearchFieldConfigResult(fieldRaw, `${path}.fields.${fieldName}`);
    if (Result.isError(fieldRes)) return fieldRes;
    fields[nameRes.value] = fieldRes.value;
  }
  if (!fields[primaryFieldRes.value]) {
    return Result.err({ message: `${path}.primaryTimestampField must reference a declared field` });
  }
  if (fields[primaryFieldRes.value].kind !== "date") {
    return Result.err({ message: `${path}.primaryTimestampField must reference a date field` });
  }
  let defaultFields: SearchDefaultField[] | undefined;
  if (raw.defaultFields !== undefined) {
    if (!Array.isArray(raw.defaultFields)) return Result.err({ message: `${path}.defaultFields must be an array` });
    defaultFields = [];
    for (let i = 0; i < raw.defaultFields.length; i++) {
      const fieldRes = parseSearchDefaultFieldResult(raw.defaultFields[i], `${path}.defaultFields[${i}]`);
      if (Result.isError(fieldRes)) return fieldRes;
      if (!fields[fieldRes.value.field]) {
        return Result.err({ message: `${path}.defaultFields[${i}].field must reference a declared field` });
      }
      defaultFields.push(fieldRes.value);
    }
  }
  let containsDefaultFields: string[] | undefined;
  if (raw.containsDefaultFields !== undefined) {
    if (!Array.isArray(raw.containsDefaultFields)) {
      return Result.err({ message: `${path}.containsDefaultFields must be an array` });
    }
    containsDefaultFields = [];
    for (let i = 0; i < raw.containsDefaultFields.length; i++) {
      if (typeof raw.containsDefaultFields[i] !== "string") {
        return Result.err({ message: `${path}.containsDefaultFields[${i}] must be a string` });
      }
      const nameRes = validateSearchFieldNameResult(raw.containsDefaultFields[i], `${path}.containsDefaultFields[${i}]`);
      if (Result.isError(nameRes)) return nameRes;
      if (!fields[nameRes.value]) {
        return Result.err({ message: `${path}.containsDefaultFields[${i}] must reference a declared field` });
      }
      containsDefaultFields.push(nameRes.value);
    }
  }
  let aliases: Record<string, string> | undefined;
  if (raw.aliases !== undefined) {
    if (!isPlainObject(raw.aliases)) return Result.err({ message: `${path}.aliases must be an object` });
    aliases = {};
    for (const [aliasRaw, targetRaw] of Object.entries(raw.aliases)) {
      const aliasRes = validateSearchFieldNameResult(aliasRaw, `${path}.aliases`);
      if (Result.isError(aliasRes)) return aliasRes;
      if (typeof targetRaw !== "string") return Result.err({ message: `${path}.aliases.${aliasRaw} must be a string` });
      const targetRes = validateSearchFieldNameResult(targetRaw, `${path}.aliases.${aliasRaw}`);
      if (Result.isError(targetRes)) return targetRes;
      if (!fields[targetRes.value]) {
        return Result.err({ message: `${path}.aliases.${aliasRaw} must reference a declared field` });
      }
      aliases[aliasRes.value] = targetRes.value;
    }
  }
  let rollups: Record<string, SearchRollupConfig> | undefined;
  if (raw.rollups !== undefined) {
    if (!isPlainObject(raw.rollups)) return Result.err({ message: `${path}.rollups must be an object` });
    rollups = {};
    for (const [rollupName, rollupRaw] of Object.entries(raw.rollups)) {
      const nameRes = validateSearchFieldNameResult(rollupName, `${path}.rollups`);
      if (Result.isError(nameRes)) return nameRes;
      const rollupRes = parseSearchRollupConfigResult(
        rollupRaw,
        `${path}.rollups.${rollupName}`,
        fields,
        primaryFieldRes.value
      );
      if (Result.isError(rollupRes)) return rollupRes;
      rollups[nameRes.value] = rollupRes.value;
    }
  }
  return Result.ok({
    profile: typeof raw.profile === "string" ? raw.profile : undefined,
    primaryTimestampField: primaryFieldRes.value,
    defaultFields,
    containsDefaultFields,
    aliases,
    fields,
    rollups,
  });
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
    ["apiVersion", "schema", "currentVersion", "routingKey", "search", "boundaries", "schemas", "lenses"],
    "registry"
  );
  if (Result.isError(keyCheck)) return keyCheck;
  if (raw.apiVersion !== SCHEMA_REGISTRY_API_VERSION) return Result.err({ message: "invalid registry apiVersion" });

  const routingKeyRes = parseRoutingKeyConfigResult(raw.routingKey, "routingKey");
  if (Result.isError(routingKeyRes)) return routingKeyRes;
  const searchRes = parseSearchConfigResult(raw.search, "search");
  if (Result.isError(searchRes)) return searchRes;

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
    search: searchRes.value ?? undefined,
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
): Result<{ schema?: any; lens?: any; routingKey?: RoutingKeyConfig | null; search?: SearchConfig | null }, { message: string }> {
  if (!isPlainObject(body)) return Result.err({ message: "schema update must be a JSON object" });
  const keyCheck = rejectUnknownKeysResult(body, ["apiVersion", "schema", "lens", "routingKey", "search"], "schemaUpdate");
  if (Result.isError(keyCheck)) return keyCheck;
  if (body.apiVersion !== undefined && body.apiVersion !== SCHEMA_REGISTRY_API_VERSION) {
    return Result.err({ message: "invalid schema apiVersion" });
  }

  const hasSchema = Object.prototype.hasOwnProperty.call(body, "schema");
  const hasRoutingKey = Object.prototype.hasOwnProperty.call(body, "routingKey");
  const hasSearch = Object.prototype.hasOwnProperty.call(body, "search");
  if (!hasSchema && !hasRoutingKey && !hasSearch) {
    return Result.err({ message: "schema update must include schema, routingKey, or search" });
  }
  if (!hasSchema && body.lens !== undefined) {
    return Result.err({ message: "schema update lens requires schema" });
  }

  const routingKeyRes = hasRoutingKey ? parseRoutingKeyConfigResult(body.routingKey, "routingKey") : Result.ok(null);
  if (Result.isError(routingKeyRes)) return routingKeyRes;
  if (hasSchema && hasRoutingKey && routingKeyRes.value == null) {
    return Result.err({ message: "schema update routingKey must be an object when schema is provided" });
  }

  const searchRes = hasSearch ? parseSearchConfigResult(body.search, "search") : Result.ok(null);
  if (Result.isError(searchRes)) return searchRes;

  const out: { schema?: any; lens?: any; routingKey?: RoutingKeyConfig | null; search?: SearchConfig | null } = {};
  if (hasSchema) out.schema = body.schema;
  if (body.lens !== undefined) out.lens = body.lens;
  if (hasRoutingKey) out.routingKey = routingKeyRes.value;
  if (hasSearch) out.search = searchRes.value;
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
    update: { schema: any; lens?: any; routingKey?: RoutingKeyConfig; search?: SearchConfig | null }
  ): SchemaRegistry {
    const res = this.updateRegistryResult(stream, streamRow, update);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  updateRegistryResult(
    stream: string,
    streamRow: StreamRow,
    update: { schema: any; lens?: any; routingKey?: RoutingKeyConfig; search?: SearchConfig | null }
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
        search: update.search === undefined ? reg.search : update.search ?? undefined,
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
      search: update.search === undefined ? reg.search : update.search ?? undefined,
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

  updateSearch(stream: string, search: SearchConfig | null): SchemaRegistry {
    const res = this.updateSearchResult(stream, search);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  updateSearchResult(stream: string, search: SearchConfig | null): Result<SchemaRegistry, SchemaRegistryMutationError> {
    const searchRes = parseSearchConfigResult(search, "search");
    if (Result.isError(searchRes)) return Result.err({ kind: "bad_request", message: searchRes.error.message });
    const regRes = this.getRegistryResult(stream);
    if (Result.isError(regRes)) return Result.err({ kind: "bad_request", message: regRes.error.message, code: regRes.error.code });
    if (searchRes.value && (regRes.value.currentVersion <= 0 || regRes.value.boundaries.length === 0)) {
      return Result.err({
        kind: "bad_request",
        message: "search config requires an installed schema version",
      });
    }
    const nextReg: SchemaRegistry = {
      ...regRes.value,
      search: searchRes.value ?? undefined,
    };
    this.persist(stream, nextReg);
    return Result.ok(nextReg);
  }

  replaceRegistry(stream: string, registry: SchemaRegistry): SchemaRegistry {
    const res = this.replaceRegistryResult(stream, registry);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  replaceRegistryResult(stream: string, registry: SchemaRegistry): Result<SchemaRegistry, SchemaRegistryMutationError> {
    const parseRes = parseRegistryResult(stream, JSON.stringify(registry));
    if (Result.isError(parseRes)) return Result.err({ kind: "bad_request", message: parseRes.error.message });
    this.persist(stream, parseRes.value);
    return Result.ok(parseRes.value);
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
