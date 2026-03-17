import { Result } from "better-result";
import { parseJsonPointerResult } from "../util/json_pointer";
import { dsError } from "../util/ds_error.ts";

export type LensTransformMap = { map: Record<string, any>; default?: any };
export type LensTransformBuiltin = { builtin: string };
export type LensTransform = LensTransformMap | LensTransformBuiltin;

export type LensOp =
  | { op: "rename"; from: string; to: string }
  | { op: "copy"; from: string; to: string }
  | { op: "add"; path: string; schema: any; default?: any }
  | { op: "remove"; path: string; schema: any; default?: any }
  | { op: "hoist"; host: string; name: string; to: string; removeFromHost?: boolean }
  | { op: "plunge"; from: string; host: string; name: string; createHost?: boolean; removeFromSource?: boolean }
  | { op: "wrap"; path: string; mode: "singleton"; reverseMode?: "first" }
  | { op: "head"; path: string; reverseMode?: "singleton" }
  | { op: "convert"; path: string; fromType: JsonTypeName; toType: JsonTypeName; forward: LensTransform; backward: LensTransform }
  | { op: "in"; path: string; ops: LensOp[] }
  | { op: "map"; path: string; ops: LensOp[] };

export type Lens = {
  apiVersion: "durable.lens/v1";
  schema: string;
  from: number;
  to: number;
  description?: string;
  ops: LensOp[];
};

export type JsonTypeName = "string" | "number" | "integer" | "boolean" | "null" | "object" | "array";

type CompiledOp =
  | { op: "rename"; from: string[]; to: string[] }
  | { op: "copy"; from: string[]; to: string[] }
  | { op: "add"; path: string[]; default?: any }
  | { op: "remove"; path: string[] }
  | { op: "hoist"; host: string[]; name: string; to: string[]; removeFromHost: boolean }
  | { op: "plunge"; from: string[]; host: string[]; name: string; createHost: boolean; removeFromSource: boolean }
  | { op: "wrap"; path: string[] }
  | { op: "head"; path: string[] }
  | { op: "convert"; path: string[]; fromType: JsonTypeName; toType: JsonTypeName; forward: LensTransform }
  | { op: "in"; path: string[]; ops: CompiledOp[] }
  | { op: "map"; path: string[]; ops: CompiledOp[] };

export type CompiledLens = {
  schema: string;
  from: number;
  to: number;
  ops: CompiledOp[];
};

export type LensCompileError = { kind: "invalid_lens"; message: string };
export type LensApplyError = { kind: "invalid_lens_ops"; message: string };

function invalidLensApply<T = never>(message: string): Result<T, LensApplyError> {
  return Result.err({ kind: "invalid_lens_ops", message });
}

function resolveSegments(doc: any, segments: string[]): { parent: any; key: string | null; value: any; exists: boolean } {
  if (segments.length === 0) return { parent: null, key: null, value: doc, exists: true };
  let cur: any = doc;
  for (let i = 0; i < segments.length - 1; i++) {
    cur = getChild(cur, segments[i]);
    if (cur === undefined) return { parent: null, key: null, value: undefined, exists: false };
  }
  const key = segments[segments.length - 1];
  const value = cur === undefined ? undefined : getChild(cur, key);
  return { parent: cur, key, value, exists: value !== undefined };
}

function getChild(container: any, seg: string): any {
  if (Array.isArray(container)) {
    const idx = Number(seg);
    if (!Number.isInteger(idx)) return undefined;
    return container[idx];
  }
  if (container && typeof container === "object") return (container as any)[seg];
  return undefined;
}

function setChildResult(container: any, seg: string, value: any): Result<void, LensApplyError> {
  if (Array.isArray(container)) {
    const idx = Number(seg);
    if (!Number.isInteger(idx) || idx < 0 || idx >= container.length) return invalidLensApply("array index out of bounds");
    container[idx] = value;
    return Result.ok(undefined);
  }
  if (container && typeof container === "object") {
    (container as any)[seg] = value;
    return Result.ok(undefined);
  }
  return invalidLensApply("invalid parent");
}

function deleteChildResult(container: any, seg: string): Result<void, LensApplyError> {
  if (Array.isArray(container)) {
    const idx = Number(seg);
    if (!Number.isInteger(idx) || idx < 0 || idx >= container.length) return invalidLensApply("array index out of bounds");
    container.splice(idx, 1);
    return Result.ok(undefined);
  }
  if (container && typeof container === "object") {
    delete (container as any)[seg];
    return Result.ok(undefined);
  }
  return invalidLensApply("invalid parent");
}

function setAtResult(doc: any, segments: string[], value: any, opts?: { createParents?: boolean }): Result<any, LensApplyError> {
  if (segments.length === 0) return Result.ok(value);
  let cur: any = doc;
  for (let i = 0; i < segments.length - 1; i++) {
    const seg = segments[i];
    let next = getChild(cur, seg);
    if (next === undefined) {
      if (!opts?.createParents) return invalidLensApply("missing parent");
      next = {};
      const setRes = setChildResult(cur, seg, next);
      if (Result.isError(setRes)) return setRes;
    }
    cur = next;
  }
  const setLeafRes = setChildResult(cur, segments[segments.length - 1], value);
  if (Result.isError(setLeafRes)) return setLeafRes;
  return Result.ok(doc);
}

function deleteAtResult(doc: any, segments: string[]): Result<any, LensApplyError> {
  if (segments.length === 0) return invalidLensApply("cannot delete document root");
  let cur: any = doc;
  for (let i = 0; i < segments.length - 1; i++) {
    cur = getChild(cur, segments[i]);
    if (cur === undefined) return invalidLensApply("missing parent");
  }
  const deleteRes = deleteChildResult(cur, segments[segments.length - 1]);
  if (Result.isError(deleteRes)) return deleteRes;
  return Result.ok(doc);
}

function coerceTypeNameResult(value: any): Result<JsonTypeName, LensApplyError> {
  if (value === null) return Result.ok("null");
  if (Array.isArray(value)) return Result.ok("array");
  switch (typeof value) {
    case "string":
      return Result.ok("string");
    case "number":
      return Result.ok(Number.isInteger(value) ? "integer" : "number");
    case "boolean":
      return Result.ok("boolean");
    case "object":
      return Result.ok("object");
    default:
      return invalidLensApply("invalid json type");
  }
}

function ensureTypeResult(value: any, expected: JsonTypeName): Result<void, LensApplyError> {
  const actualRes = coerceTypeNameResult(value);
  if (Result.isError(actualRes)) return actualRes;
  const actual = actualRes.value;
  if (expected === "number" && (actual === "number" || actual === "integer")) return Result.ok(undefined);
  if (actual !== expected) return invalidLensApply(`type mismatch: expected ${expected}, got ${actual}`);
  return Result.ok(undefined);
}

function applyTransformResult(transform: LensTransform, value: any): Result<any, LensApplyError> {
  if ((transform as any).builtin) {
    const name = (transform as LensTransformBuiltin).builtin;
    return applyBuiltinResult(name, value);
  }
  const t = transform as LensTransformMap;
  const key = String(value);
  if (Object.prototype.hasOwnProperty.call(t.map, key)) return Result.ok(t.map[key]);
  if (Object.prototype.hasOwnProperty.call(t, "default")) return Result.ok((t as any).default);
  return invalidLensApply("convert map missing key and default");
}

function applyBuiltinResult(name: string, value: any): Result<any, LensApplyError> {
  switch (name) {
    case "lowercase":
      if (typeof value !== "string") return invalidLensApply("builtin lowercase expects string");
      return Result.ok(value.toLowerCase());
    case "uppercase":
      if (typeof value !== "string") return invalidLensApply("builtin uppercase expects string");
      return Result.ok(value.toUpperCase());
    case "string_to_int": {
      if (typeof value !== "string") return invalidLensApply("builtin string_to_int expects string");
      const n = Number.parseInt(value, 10);
      if (!Number.isFinite(n)) return invalidLensApply("builtin string_to_int invalid");
      return Result.ok(n);
    }
    case "int_to_string":
      if (typeof value !== "number" || !Number.isInteger(value)) return invalidLensApply("builtin int_to_string expects integer");
      return Result.ok(String(value));
    case "rfc3339_to_unix_millis": {
      if (typeof value !== "string") return invalidLensApply("builtin rfc3339_to_unix_millis expects string");
      const ms = new Date(value).getTime();
      if (Number.isNaN(ms)) return invalidLensApply("builtin rfc3339_to_unix_millis invalid");
      return Result.ok(ms);
    }
    case "unix_millis_to_rfc3339":
      if (typeof value !== "number" || !Number.isFinite(value)) return invalidLensApply("builtin unix_millis_to_rfc3339 expects number");
      return Result.ok(new Date(value).toISOString());
    default:
      return invalidLensApply(`unknown builtin: ${name}`);
  }
}

export function compileLens(lens: Lens): CompiledLens {
  const res = compileLensResult(lens);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function compileLensResult(lens: Lens): Result<CompiledLens, LensCompileError> {
  const opsRes = compileOpsResult(lens.ops);
  if (Result.isError(opsRes)) return opsRes;
  return Result.ok({
    schema: lens.schema,
    from: lens.from,
    to: lens.to,
    ops: opsRes.value,
  });
}

function invalidLens<T = never>(message: string): Result<T, LensCompileError> {
  return Result.err({ kind: "invalid_lens", message });
}

function parsePointer(pointer: string, field: string): Result<string[], LensCompileError> {
  const res = parseJsonPointerResult(pointer);
  if (Result.isError(res)) return invalidLens(`${field} ${res.error.message}`);
  return Result.ok(res.value);
}

function compileOpsResult(ops: LensOp[]): Result<CompiledOp[], LensCompileError> {
  const compiled: CompiledOp[] = [];
  for (const op of ops) {
    switch (op.op) {
      case "rename": {
        const fromRes = parsePointer(op.from, "rename.from:");
        if (Result.isError(fromRes)) return fromRes;
        const toRes = parsePointer(op.to, "rename.to:");
        if (Result.isError(toRes)) return toRes;
        compiled.push({ op: "rename", from: fromRes.value, to: toRes.value });
        break;
      }
      case "copy": {
        const fromRes = parsePointer(op.from, "copy.from:");
        if (Result.isError(fromRes)) return fromRes;
        const toRes = parsePointer(op.to, "copy.to:");
        if (Result.isError(toRes)) return toRes;
        compiled.push({ op: "copy", from: fromRes.value, to: toRes.value });
        break;
      }
      case "add": {
        const pathRes = parsePointer(op.path, "add.path:");
        if (Result.isError(pathRes)) return pathRes;
        compiled.push({ op: "add", path: pathRes.value, default: op.default });
        break;
      }
      case "remove": {
        const pathRes = parsePointer(op.path, "remove.path:");
        if (Result.isError(pathRes)) return pathRes;
        compiled.push({ op: "remove", path: pathRes.value });
        break;
      }
      case "hoist": {
        const hostRes = parsePointer(op.host, "hoist.host:");
        if (Result.isError(hostRes)) return hostRes;
        const toRes = parsePointer(op.to, "hoist.to:");
        if (Result.isError(toRes)) return toRes;
        compiled.push({
          op: "hoist",
          host: hostRes.value,
          name: op.name,
          to: toRes.value,
          removeFromHost: op.removeFromHost !== false,
        });
        break;
      }
      case "plunge": {
        const fromRes = parsePointer(op.from, "plunge.from:");
        if (Result.isError(fromRes)) return fromRes;
        const hostRes = parsePointer(op.host, "plunge.host:");
        if (Result.isError(hostRes)) return hostRes;
        compiled.push({
          op: "plunge",
          from: fromRes.value,
          host: hostRes.value,
          name: op.name,
          createHost: op.createHost !== false,
          removeFromSource: op.removeFromSource !== false,
        });
        break;
      }
      case "wrap": {
        const pathRes = parsePointer(op.path, "wrap.path:");
        if (Result.isError(pathRes)) return pathRes;
        compiled.push({ op: "wrap", path: pathRes.value });
        break;
      }
      case "head": {
        const pathRes = parsePointer(op.path, "head.path:");
        if (Result.isError(pathRes)) return pathRes;
        compiled.push({ op: "head", path: pathRes.value });
        break;
      }
      case "convert": {
        const pathRes = parsePointer(op.path, "convert.path:");
        if (Result.isError(pathRes)) return pathRes;
        compiled.push({
          op: "convert",
          path: pathRes.value,
          fromType: op.fromType,
          toType: op.toType,
          forward: op.forward,
        });
        break;
      }
      case "in": {
        const pathRes = parsePointer(op.path, "in.path:");
        if (Result.isError(pathRes)) return pathRes;
        const nestedRes = compileOpsResult(op.ops);
        if (Result.isError(nestedRes)) return nestedRes;
        compiled.push({ op: "in", path: pathRes.value, ops: nestedRes.value });
        break;
      }
      case "map": {
        const pathRes = parsePointer(op.path, "map.path:");
        if (Result.isError(pathRes)) return pathRes;
        const nestedRes = compileOpsResult(op.ops);
        if (Result.isError(nestedRes)) return nestedRes;
        compiled.push({ op: "map", path: pathRes.value, ops: nestedRes.value });
        break;
      }
      default: {
        return invalidLens(`unknown op: ${(op as any).op}`);
      }
    }
  }
  return Result.ok(compiled);
}

export function applyCompiledLens(lens: CompiledLens, doc: any): any {
  const res = applyCompiledLensResult(lens, doc);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function applyLensChain(lenses: CompiledLens[], doc: any): any {
  const res = applyLensChainResult(lenses, doc);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

function applyCompiledLensResult(lens: CompiledLens, doc: any): Result<any, LensApplyError> {
  return applyOpsResult(doc, lens.ops);
}

export function applyLensChainResult(lenses: CompiledLens[], doc: any): Result<any, LensApplyError> {
  let cur = doc;
  for (const l of lenses) {
    const stepRes = applyCompiledLensResult(l, cur);
    if (Result.isError(stepRes)) return stepRes;
    cur = stepRes.value;
  }
  return Result.ok(cur);
}

function applyOpsResult(doc: any, ops: CompiledOp[]): Result<any, LensApplyError> {
  let root = doc;
  for (const op of ops) {
    switch (op.op) {
      case "rename": {
        const src = resolveSegments(root, op.from);
        if (!src.exists) return invalidLensApply("rename missing source");
        const setRes = setAtResult(root, op.to, src.value);
        if (Result.isError(setRes)) return setRes;
        const deleteRes = deleteAtResult(setRes.value, op.from);
        if (Result.isError(deleteRes)) return deleteRes;
        root = deleteRes.value;
        break;
      }
      case "copy": {
        const src = resolveSegments(root, op.from);
        if (!src.exists) return invalidLensApply("copy missing source");
        const setRes = setAtResult(root, op.to, src.value);
        if (Result.isError(setRes)) return setRes;
        root = setRes.value;
        break;
      }
      case "add": {
        const setRes = setAtResult(root, op.path, op.default);
        if (Result.isError(setRes)) return setRes;
        root = setRes.value;
        break;
      }
      case "remove": {
        const dst = resolveSegments(root, op.path);
        if (!dst.exists) return invalidLensApply("remove missing path");
        const deleteRes = deleteAtResult(root, op.path);
        if (Result.isError(deleteRes)) return deleteRes;
        root = deleteRes.value;
        break;
      }
      case "hoist": {
        const host = resolveSegments(root, op.host);
        if (!host.exists || !host.value || typeof host.value !== "object" || Array.isArray(host.value)) {
          return invalidLensApply("hoist host missing or not object");
        }
        if (!(op.name in host.value)) return invalidLensApply("hoist missing name");
        const value = (host.value as any)[op.name];
        const setRes = setAtResult(root, op.to, value);
        if (Result.isError(setRes)) return setRes;
        root = setRes.value;
        if (op.removeFromHost) delete (host.value as any)[op.name];
        break;
      }
      case "plunge": {
        const src = resolveSegments(root, op.from);
        if (!src.exists) return invalidLensApply("plunge missing source");
        let host = resolveSegments(root, op.host);
        if (!host.exists) {
          if (!op.createHost) return invalidLensApply("plunge host missing");
          const setHostRes = setAtResult(root, op.host, {}, { createParents: true });
          if (Result.isError(setHostRes)) return setHostRes;
          root = setHostRes.value;
          host = resolveSegments(root, op.host);
        }
        if (!host.value || typeof host.value !== "object" || Array.isArray(host.value)) {
          return invalidLensApply("plunge host not object");
        }
        (host.value as any)[op.name] = src.value;
        if (op.removeFromSource) {
          const deleteRes = deleteAtResult(root, op.from);
          if (Result.isError(deleteRes)) return deleteRes;
          root = deleteRes.value;
        }
        break;
      }
      case "wrap": {
        const dst = resolveSegments(root, op.path);
        if (!dst.exists) return invalidLensApply("wrap missing path");
        const setRes = setAtResult(root, op.path, [dst.value]);
        if (Result.isError(setRes)) return setRes;
        root = setRes.value;
        break;
      }
      case "head": {
        const dst = resolveSegments(root, op.path);
        if (!dst.exists) return invalidLensApply("head missing path");
        if (!Array.isArray(dst.value) || dst.value.length === 0) return invalidLensApply("head expects non-empty array");
        const setRes = setAtResult(root, op.path, dst.value[0]);
        if (Result.isError(setRes)) return setRes;
        root = setRes.value;
        break;
      }
      case "convert": {
        const dst = resolveSegments(root, op.path);
        if (!dst.exists) return invalidLensApply("convert missing path");
        const ensureFromRes = ensureTypeResult(dst.value, op.fromType);
        if (Result.isError(ensureFromRes)) return ensureFromRes;
        const outRes = applyTransformResult(op.forward, dst.value);
        if (Result.isError(outRes)) return outRes;
        const ensureToRes = ensureTypeResult(outRes.value, op.toType);
        if (Result.isError(ensureToRes)) return ensureToRes;
        const setRes = setAtResult(root, op.path, outRes.value);
        if (Result.isError(setRes)) return setRes;
        root = setRes.value;
        break;
      }
      case "in": {
        const dst = resolveSegments(root, op.path);
        if (!dst.exists) return invalidLensApply("in missing path");
        if (!dst.value || typeof dst.value !== "object" || Array.isArray(dst.value)) return invalidLensApply("in expects object");
        const nestedRes = applyOpsResult(dst.value, op.ops);
        if (Result.isError(nestedRes)) return nestedRes;
        break;
      }
      case "map": {
        const dst = resolveSegments(root, op.path);
        if (!dst.exists) return invalidLensApply("map missing path");
        if (!Array.isArray(dst.value)) return invalidLensApply("map expects array");
        for (const item of dst.value) {
          const nestedRes = applyOpsResult(item, op.ops);
          if (Result.isError(nestedRes)) return nestedRes;
        }
        break;
      }
      default:
        return invalidLensApply(`unknown op: ${(op as any).op}`);
    }
  }
  return Result.ok(root);
}

export function lensFromJson(raw: any): Lens {
  return raw as Lens;
}
