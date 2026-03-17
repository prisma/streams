import { Result } from "better-result";
import type { Lens, LensOp } from "../lens/lens";
import { parseJsonPointerResult } from "../util/json_pointer";
import { dsError } from "../util/ds_error.ts";

type Schema = any;
export type LensProofError = {
  kind: "invalid_lens_proof" | "invalid_lens_defaults";
  message: string;
};

type PathInfo = {
  schema: Schema;
  required: boolean;
  exists: boolean;
  explicit: boolean;
};

function invalidLensProof<T = never>(message: string): Result<T, LensProofError> {
  return Result.err({ kind: "invalid_lens_proof", message });
}

function invalidLensDefaults<T = never>(message: string): Result<T, LensProofError> {
  return Result.err({ kind: "invalid_lens_defaults", message });
}

function parsePointerResult(
  ptr: string,
  kind: LensProofError["kind"]
): Result<string[], LensProofError> {
  const res = parseJsonPointerResult(ptr);
  if (Result.isError(res)) {
    return Result.err({ kind, message: res.error.message });
  }
  return Result.ok(res.value);
}

function isObjectSchema(schema: Schema): boolean {
  if (schema === true) return true;
  if (schema === false || schema == null) return false;
  const t = schema.type;
  if (t === undefined) return true;
  if (Array.isArray(t)) return t.includes("object");
  return t === "object";
}

function isArraySchema(schema: Schema): boolean {
  if (schema === true) return true;
  if (schema === false || schema == null) return false;
  const t = schema.type;
  if (t === undefined) return true;
  if (Array.isArray(t)) return t.includes("array");
  return t === "array";
}

function getTypeSet(schema: Schema): Set<string> | null {
  if (schema === true) return null;
  if (schema === false || schema == null) return new Set();
  if (schema.const !== undefined) return new Set([inferType(schema.const)]);
  if (schema.enum) return new Set(schema.enum.map((v: any) => inferType(v)));
  const t = schema.type;
  if (t === undefined) return null;
  if (Array.isArray(t)) return new Set(t);
  return new Set([t]);
}

function inferType(value: any): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  switch (typeof value) {
    case "string":
      return "string";
    case "number":
      return Number.isInteger(value) ? "integer" : "number";
    case "boolean":
      return "boolean";
    case "object":
      return "object";
    default:
      return "unknown";
  }
}

function typeSubset(src: string, dest: string): boolean {
  if (src === dest) return true;
  if (src === "integer" && dest === "number") return true;
  return false;
}

function isSchemaCompatible(src: Schema, dest: Schema): boolean {
  if (dest === true) return true;
  if (dest === false || dest == null) return false;

  if (src && src.const !== undefined) {
    return valueConformsToSchema(src.const, dest);
  }
  if (src && Array.isArray(src.enum)) {
    return src.enum.every((v: any) => valueConformsToSchema(v, dest));
  }

  const srcTypes = getTypeSet(src);
  const destTypes = getTypeSet(dest);
  if (srcTypes && destTypes) {
    for (const t of srcTypes) {
      let ok = false;
      for (const d of destTypes) {
        if (typeSubset(t, d)) {
          ok = true;
          break;
        }
      }
      if (!ok) return false;
    }
    return true;
  }

  // If dest is narrowly constrained but src is broad, we cannot prove.
  if (dest.const !== undefined || Array.isArray(dest.enum)) return false;
  return true;
}

function valueConformsToSchema(value: any, schema: Schema): boolean {
  if (schema === true) return true;
  if (schema === false || schema == null) return false;
  if (schema.const !== undefined) return deepEqual(value, schema.const);
  if (Array.isArray(schema.enum)) return schema.enum.some((v: any) => deepEqual(v, value));
  const t = getTypeSet(schema);
  if (t) {
    const vt = inferType(value);
    for (const allowed of t) {
      if (typeSubset(vt, allowed)) return true;
    }
    return false;
  }
  return true;
}

function deepEqual(a: any, b: any): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function getPropertySchema(schema: Schema, prop: string, requireExplicit: boolean): PathInfo {
  if (!schema || schema === false) return { schema: schema, required: false, exists: false, explicit: false };
  if (!isObjectSchema(schema)) return { schema, required: false, exists: false, explicit: false };
  const props = schema.properties ?? {};
  const required = Array.isArray(schema.required) && schema.required.includes(prop);
  if (Object.prototype.hasOwnProperty.call(props, prop)) {
    return { schema: props[prop], required, exists: true, explicit: true };
  }
  if (requireExplicit) return { schema: schema, required, exists: false, explicit: false };
  if (schema.additionalProperties === false) return { schema: schema, required, exists: false, explicit: false };
  if (schema.additionalProperties && schema.additionalProperties !== true) {
    return { schema: schema.additionalProperties, required, exists: true, explicit: false };
  }
  return { schema: true, required, exists: true, explicit: false };
}

function getItemsSchema(schema: Schema): Schema {
  if (!schema || schema === false) return schema;
  if (!isArraySchema(schema)) return schema;
  if (schema.items !== undefined) return schema.items;
  return true;
}

function getPathInfo(schema: Schema, segments: string[], requireExplicit: boolean): PathInfo {
  if (segments.length === 0) return { schema, required: true, exists: true, explicit: true };
  let cur = schema;
  let required = true;
  for (let i = 0; i < segments.length; i++) {
    const seg = segments[i];
    if (/^[0-9]+$/.test(seg)) {
      if (!isArraySchema(cur)) return { schema: cur, required: false, exists: false, explicit: false };
      cur = getItemsSchema(cur);
      continue;
    }
    const info = getPropertySchema(cur, seg, requireExplicit);
    if (!info.exists) return info;
    required = required && info.required;
    cur = info.schema;
  }
  return { schema: cur, required, exists: true, explicit: true };
}

function ensureParentRequiredResult(
  schema: Schema,
  segments: string[],
  requireExplicit: boolean,
  kind: LensProofError["kind"]
): Result<void, LensProofError> {
  if (segments.length === 0) return Result.ok(undefined);
  let cur = schema;
  for (let i = 0; i < segments.length - 1; i++) {
    const seg = segments[i];
    const info = getPropertySchema(cur, seg, requireExplicit);
    if (!info.exists || !info.required) {
      return Result.err({ kind, message: "parent path not required" });
    }
    if (!isObjectSchema(info.schema)) {
      return Result.err({ kind, message: "parent path not object" });
    }
    cur = info.schema;
  }
  return Result.ok(undefined);
}

function isPathRequired(schema: Schema, segments: string[]): boolean {
  const info = getPathInfo(schema, segments, false);
  return info.exists && info.required;
}

function deriveDefault(schema: Schema): any | undefined {
  if (schema === false || schema == null) return undefined;
  if (schema.default !== undefined) return schema.default;
  const types = getTypeSet(schema);
  if (types) {
    if (types.has("object")) return {};
    if (types.has("array")) return [];
  }
  return undefined;
}

function ensureEnumOrConstResult(kind: LensProofError["kind"], schema: Schema): Result<void, LensProofError> {
  if (schema && (schema.const !== undefined || Array.isArray(schema.enum))) return Result.ok(undefined);
  return Result.err({ kind, message: "schema must be enum/const for non-total transform" });
}

function samePointer(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

function hasWrapAfterResult(ops: LensOp[], pathSeg: string[]): Result<boolean, LensProofError> {
  for (const op of ops) {
    if (op.op !== "wrap") continue;
    const wrapPathRes = parsePointerResult(op.path, "invalid_lens_proof");
    if (Result.isError(wrapPathRes)) return wrapPathRes;
    if (samePointer(wrapPathRes.value, pathSeg)) return Result.ok(true);
  }
  return Result.ok(false);
}

function findRenameToResult(priorOps: LensOp[], pathSeg: string[]): Result<LensOp | null, LensProofError> {
  for (let i = priorOps.length - 1; i >= 0; i--) {
    const op = priorOps[i];
    if (op.op !== "rename") continue;
    const toRes = parsePointerResult(op.to, "invalid_lens_proof");
    if (Result.isError(toRes)) return toRes;
    if (samePointer(toRes.value, pathSeg)) return Result.ok(op);
  }
  return Result.ok(null);
}

function validateOpResult(
  oldSchema: Schema,
  newSchema: Schema,
  op: LensOp,
  priorOps: LensOp[],
  remainingOps: LensOp[]
): Result<void, LensProofError> {
  switch (op.op) {
    case "rename": {
      const fromSegRes = parsePointerResult(op.from, "invalid_lens_proof");
      if (Result.isError(fromSegRes)) return fromSegRes;
      const toSegRes = parsePointerResult(op.to, "invalid_lens_proof");
      if (Result.isError(toSegRes)) return toSegRes;
      const fromSeg = fromSegRes.value;
      const toSeg = toSegRes.value;
      const src = getPathInfo(oldSchema, fromSeg, false);
      if (!src.exists || !src.required) return invalidLensProof("rename source not required");
      const parentRes = ensureParentRequiredResult(oldSchema, toSeg, false, "invalid_lens_proof");
      if (Result.isError(parentRes)) return parentRes;
      const dest = getPathInfo(newSchema, toSeg, false);
      if (!dest.exists) return invalidLensProof("rename dest missing in new schema");
      if (!isSchemaCompatible(src.schema, dest.schema)) {
        if (isArraySchema(dest.schema)) {
          const hasWrapRes = hasWrapAfterResult(remainingOps, toSeg);
          if (Result.isError(hasWrapRes)) return hasWrapRes;
          if (hasWrapRes.value) {
            const items = getItemsSchema(dest.schema);
            if (!isSchemaCompatible(src.schema, items)) {
              return invalidLensProof("rename schema incompatible");
            }
            return Result.ok(undefined);
          }
        }
        return invalidLensProof("rename schema incompatible");
      }
      return Result.ok(undefined);
    }
    case "copy": {
      const fromSegRes = parsePointerResult(op.from, "invalid_lens_proof");
      if (Result.isError(fromSegRes)) return fromSegRes;
      const toSegRes = parsePointerResult(op.to, "invalid_lens_proof");
      if (Result.isError(toSegRes)) return toSegRes;
      const fromSeg = fromSegRes.value;
      const toSeg = toSegRes.value;
      const src = getPathInfo(oldSchema, fromSeg, false);
      if (!src.exists || !src.required) return invalidLensProof("copy source not required");
      const parentRes = ensureParentRequiredResult(oldSchema, toSeg, false, "invalid_lens_proof");
      if (Result.isError(parentRes)) return parentRes;
      const dest = getPathInfo(newSchema, toSeg, false);
      if (!dest.exists) return invalidLensProof("copy dest missing in new schema");
      if (!isSchemaCompatible(src.schema, dest.schema)) return invalidLensProof("copy schema incompatible");
      return Result.ok(undefined);
    }
    case "add": {
      const pathSegRes = parsePointerResult(op.path, "invalid_lens_proof");
      if (Result.isError(pathSegRes)) return pathSegRes;
      const pathSeg = pathSegRes.value;
      const parentRes = ensureParentRequiredResult(oldSchema, pathSeg, true, "invalid_lens_proof");
      if (Result.isError(parentRes)) return parentRes;
      const dest = getPathInfo(newSchema, pathSeg, true);
      if (!dest.exists) return invalidLensProof("add dest missing in new schema");
      const def = op.default ?? deriveDefault(dest.schema);
      if (def === undefined) return invalidLensProof("add missing default");
      if (!valueConformsToSchema(def, dest.schema)) return invalidLensProof("add default invalid");
      return Result.ok(undefined);
    }
    case "remove": {
      const pathSegRes = parsePointerResult(op.path, "invalid_lens_proof");
      if (Result.isError(pathSegRes)) return pathSegRes;
      const pathSeg = pathSegRes.value;
      const src = getPathInfo(oldSchema, pathSeg, true);
      if (!src.exists || !src.required) return invalidLensProof("remove path not required");
      if (isPathRequired(newSchema, pathSeg)) return invalidLensProof("remove path still required in new schema");
      return Result.ok(undefined);
    }
    case "hoist": {
      const hostSegRes = parsePointerResult(op.host, "invalid_lens_proof");
      if (Result.isError(hostSegRes)) return hostSegRes;
      const toSegRes = parsePointerResult(op.to, "invalid_lens_proof");
      if (Result.isError(toSegRes)) return toSegRes;
      const hostSeg = hostSegRes.value;
      const toSeg = toSegRes.value;
      const host = getPathInfo(oldSchema, hostSeg, true);
      if (!host.exists || !host.required || !isObjectSchema(host.schema)) return invalidLensProof("hoist host invalid");
      const child = getPropertySchema(host.schema, op.name, true);
      if (!child.exists || !child.required) return invalidLensProof("hoist name missing/optional");
      const parentRes = ensureParentRequiredResult(oldSchema, toSeg, true, "invalid_lens_proof");
      if (Result.isError(parentRes)) return parentRes;
      const dest = getPathInfo(newSchema, toSeg, true);
      if (!dest.exists) return invalidLensProof("hoist dest missing in new schema");
      if (!isSchemaCompatible(child.schema, dest.schema)) return invalidLensProof("hoist schema incompatible");
      if (op.removeFromHost !== false) {
        const hostNew = getPathInfo(newSchema, hostSeg, true);
        if (hostNew.exists && hostNew.required && isPathRequired(hostNew.schema, [op.name])) {
          return invalidLensProof("hoist removed field still required");
        }
      }
      return Result.ok(undefined);
    }
    case "plunge": {
      const fromSegRes = parsePointerResult(op.from, "invalid_lens_proof");
      if (Result.isError(fromSegRes)) return fromSegRes;
      const hostSegRes = parsePointerResult(op.host, "invalid_lens_proof");
      if (Result.isError(hostSegRes)) return hostSegRes;
      const fromSeg = fromSegRes.value;
      const hostSeg = hostSegRes.value;
      const src = getPathInfo(oldSchema, fromSeg, true);
      if (!src.exists || !src.required) return invalidLensProof("plunge source not required");
      if (op.createHost !== false) {
        const parentRes = ensureParentRequiredResult(oldSchema, hostSeg, true, "invalid_lens_proof");
        if (Result.isError(parentRes)) return parentRes;
      } else {
        const host = getPathInfo(oldSchema, hostSeg, true);
        if (!host.exists || !host.required || !isObjectSchema(host.schema)) return invalidLensProof("plunge host invalid");
      }
      const hostNew = getPathInfo(newSchema, hostSeg, true);
      if (!hostNew.exists || !isObjectSchema(hostNew.schema)) return invalidLensProof("plunge host missing in new schema");
      const child = getPropertySchema(hostNew.schema, op.name, true);
      if (!child.exists) return invalidLensProof("plunge name missing in new schema");
      if (!isSchemaCompatible(src.schema, child.schema)) return invalidLensProof("plunge schema incompatible");
      if (op.removeFromSource !== false && isPathRequired(newSchema, fromSeg)) {
        return invalidLensProof("plunge removed field still required");
      }
      return Result.ok(undefined);
    }
    case "wrap": {
      const pathSegRes = parsePointerResult(op.path, "invalid_lens_proof");
      if (Result.isError(pathSegRes)) return pathSegRes;
      const pathSeg = pathSegRes.value;
      let src = getPathInfo(oldSchema, pathSeg, true);
      if (!src.exists || !src.required) {
        const renameRes = findRenameToResult(priorOps, pathSeg);
        if (Result.isError(renameRes)) return renameRes;
        if (renameRes.value) {
          const fromRes = parsePointerResult((renameRes.value as any).from, "invalid_lens_proof");
          if (Result.isError(fromRes)) return fromRes;
          src = getPathInfo(oldSchema, fromRes.value, true);
        }
      }
      if (!src.exists || !src.required) return invalidLensProof("wrap path not required");
      const dest = getPathInfo(newSchema, pathSeg, true);
      if (!dest.exists || !isArraySchema(dest.schema)) return invalidLensProof("wrap dest not array");
      const items = getItemsSchema(dest.schema);
      if (!isSchemaCompatible(src.schema, items)) return invalidLensProof("wrap items incompatible");
      return Result.ok(undefined);
    }
    case "head": {
      const pathSegRes = parsePointerResult(op.path, "invalid_lens_proof");
      if (Result.isError(pathSegRes)) return pathSegRes;
      const pathSeg = pathSegRes.value;
      const src = getPathInfo(oldSchema, pathSeg, true);
      if (!src.exists || !src.required || !isArraySchema(src.schema)) return invalidLensProof("head path not required array");
      const hasEnum = src.schema && Array.isArray(src.schema.enum);
      if (hasEnum) {
        const ok = src.schema.enum.every((v: any) => Array.isArray(v) && v.length > 0);
        if (!ok) return invalidLensProof("head requires enum non-empty");
      }
      const minItems = src.schema?.minItems;
      if (!hasEnum && (minItems === undefined || minItems < 1)) {
        return invalidLensProof("head requires minItems");
      }
      if (minItems !== undefined && minItems < 1) return invalidLensProof("head requires non-empty array");
      const dest = getPathInfo(newSchema, pathSeg, true);
      if (!dest.exists) return invalidLensProof("head dest missing");
      const items = getItemsSchema(src.schema);
      if (!isSchemaCompatible(items, dest.schema)) return invalidLensProof("head schema incompatible");
      return Result.ok(undefined);
    }
    case "convert": {
      const pathSegRes = parsePointerResult(op.path, "invalid_lens_proof");
      if (Result.isError(pathSegRes)) return pathSegRes;
      const pathSeg = pathSegRes.value;
      const src = getPathInfo(oldSchema, pathSeg, true);
      if (!src.exists || !src.required) return invalidLensProof("convert path not required");
      if ((op.forward as any).map && !(op.forward as any).default) {
        const enumRes = ensureEnumOrConstResult("invalid_lens_proof", src.schema);
        if (Result.isError(enumRes)) return enumRes;
      }
      const builtin = (op.forward as any).builtin as string | undefined;
      if (builtin === "string_to_int" || builtin === "rfc3339_to_unix_millis") {
        const enumRes = ensureEnumOrConstResult("invalid_lens_proof", src.schema);
        if (Result.isError(enumRes)) return enumRes;
      }
      const dest = getPathInfo(newSchema, pathSeg, true);
      if (!dest.exists) return invalidLensProof("convert dest missing");
      const outType = { type: op.toType };
      if (!isSchemaCompatible(outType, dest.schema)) return invalidLensProof("convert dest incompatible");
      return Result.ok(undefined);
    }
    case "in": {
      const pathSegRes = parsePointerResult(op.path, "invalid_lens_proof");
      if (Result.isError(pathSegRes)) return pathSegRes;
      const pathSeg = pathSegRes.value;
      const src = getPathInfo(oldSchema, pathSeg, true);
      const dest = getPathInfo(newSchema, pathSeg, true);
      if (!src.exists || !src.required || !isObjectSchema(src.schema)) return invalidLensProof("in path invalid");
      if (!dest.exists || !isObjectSchema(dest.schema)) return invalidLensProof("in dest invalid");
      return validateOpsResult(src.schema, dest.schema, op.ops);
    }
    case "map": {
      const pathSegRes = parsePointerResult(op.path, "invalid_lens_proof");
      if (Result.isError(pathSegRes)) return pathSegRes;
      const pathSeg = pathSegRes.value;
      const src = getPathInfo(oldSchema, pathSeg, true);
      const dest = getPathInfo(newSchema, pathSeg, true);
      if (!src.exists || !src.required || !isArraySchema(src.schema)) return invalidLensProof("map path invalid");
      if (!dest.exists || !isArraySchema(dest.schema)) return invalidLensProof("map dest invalid");
      const srcItems = getItemsSchema(src.schema);
      const destItems = getItemsSchema(dest.schema);
      return validateOpsResult(srcItems, destItems, op.ops);
    }
    default:
      return invalidLensProof(`unknown op: ${(op as any).op}`);
  }
}

function validateOpsResult(oldSchema: Schema, newSchema: Schema, ops: LensOp[]): Result<void, LensProofError> {
  for (let i = 0; i < ops.length; i++) {
    const res = validateOpResult(oldSchema, newSchema, ops[i], ops.slice(0, i), ops.slice(i + 1));
    if (Result.isError(res)) return res;
  }
  return Result.ok(undefined);
}

export function validateLensAgainstSchemas(oldSchema: Schema, newSchema: Schema, lens: { ops: LensOp[] }): void {
  const res = validateLensAgainstSchemasResult(oldSchema, newSchema, lens);
  if (Result.isError(res)) throw dsError(res.error.message);
}

export function validateLensAgainstSchemasResult(
  oldSchema: Schema,
  newSchema: Schema,
  lens: { ops: LensOp[] }
): Result<void, LensProofError> {
  return validateOpsResult(oldSchema, newSchema, lens.ops);
}

function fillOpsDefaultsResult(ops: LensOp[], newSchema: Schema): Result<void, LensProofError> {
  for (const op of ops) {
    if (op.op === "add") {
      if (op.default === undefined) {
        const pathRes = parsePointerResult(op.path, "invalid_lens_defaults");
        if (Result.isError(pathRes)) return pathRes;
        const info = getPathInfo(newSchema, pathRes.value, true);
        if (!info.exists) return invalidLensDefaults("add dest missing in new schema");
        const def = deriveDefault(info.schema);
        if (def === undefined) return invalidLensDefaults("add missing default");
        op.default = def;
      }
      continue;
    }
    if (op.op === "in") {
      const pathRes = parsePointerResult(op.path, "invalid_lens_defaults");
      if (Result.isError(pathRes)) return pathRes;
      const info = getPathInfo(newSchema, pathRes.value, true);
      if (!info.exists) return invalidLensDefaults("in dest missing in new schema");
      const nestedRes = fillOpsDefaultsResult(op.ops, info.schema);
      if (Result.isError(nestedRes)) return nestedRes;
      continue;
    }
    if (op.op === "map") {
      const pathRes = parsePointerResult(op.path, "invalid_lens_defaults");
      if (Result.isError(pathRes)) return pathRes;
      const info = getPathInfo(newSchema, pathRes.value, true);
      if (!info.exists) return invalidLensDefaults("map dest missing in new schema");
      const items = getItemsSchema(info.schema);
      const nestedRes = fillOpsDefaultsResult(op.ops, items);
      if (Result.isError(nestedRes)) return nestedRes;
    }
  }
  return Result.ok(undefined);
}

function cloneLensForDefaultsResult(lens: Lens): Result<Lens, LensProofError> {
  try {
    return Result.ok(JSON.parse(JSON.stringify(lens)) as Lens);
  } catch (e: any) {
    return invalidLensDefaults(String(e?.message ?? e));
  }
}

export function fillLensDefaults(lens: Lens, newSchema: Schema): Lens {
  const res = fillLensDefaultsResult(lens, newSchema);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function fillLensDefaultsResult(lens: Lens, newSchema: Schema): Result<Lens, LensProofError> {
  const copyRes = cloneLensForDefaultsResult(lens);
  if (Result.isError(copyRes)) return copyRes;
  const fillRes = fillOpsDefaultsResult(copyRes.value.ops, newSchema);
  if (Result.isError(fillRes)) return fillRes;
  return Result.ok(copyRes.value);
}
