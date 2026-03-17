import { Result } from "better-result";
import { dsError } from "./ds_error.ts";

type JsonPointerError = { kind: "invalid_json_pointer" | "invalid_json_pointer_operation"; message: string };

function invalidPointerOperation<T = never>(message: string): Result<T, JsonPointerError> {
  return Result.err({ kind: "invalid_json_pointer_operation", message });
}

export function parseJsonPointerResult(ptr: string): Result<string[], JsonPointerError> {
  if (ptr === "") return Result.ok([]);
  if (!ptr.startsWith("/")) return Result.err({ kind: "invalid_json_pointer", message: "invalid json pointer" });
  return Result.ok(
    ptr
      .split("/")
      .slice(1)
      .map((seg) => seg.replace(/~1/g, "/").replace(/~0/g, "~"))
  );
}

export function parseJsonPointer(ptr: string): string[] {
  const res = parseJsonPointerResult(ptr);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

function isArrayIndex(seg: string): boolean {
  return seg !== "" && /^[0-9]+$/.test(seg);
}

function getChild(container: any, seg: string): any {
  if (Array.isArray(container) && isArrayIndex(seg)) {
    return container[Number(seg)];
  }
  if (container && typeof container === "object") {
    return (container as any)[seg];
  }
  return undefined;
}

function setChildResult(container: any, seg: string, value: any): Result<void, JsonPointerError> {
  if (Array.isArray(container) && isArrayIndex(seg)) {
    const idx = Number(seg);
    if (idx < 0 || idx >= container.length) return invalidPointerOperation("array index out of bounds");
    container[idx] = value;
    return Result.ok(undefined);
  }
  if (container && typeof container === "object") {
    (container as any)[seg] = value;
    return Result.ok(undefined);
  }
  return invalidPointerOperation("invalid parent for set");
}

function deleteChildResult(container: any, seg: string): Result<void, JsonPointerError> {
  if (Array.isArray(container) && isArrayIndex(seg)) {
    const idx = Number(seg);
    if (idx < 0 || idx >= container.length) return invalidPointerOperation("array index out of bounds");
    container.splice(idx, 1);
    return Result.ok(undefined);
  }
  if (container && typeof container === "object") {
    delete (container as any)[seg];
    return Result.ok(undefined);
  }
  return invalidPointerOperation("invalid parent for delete");
}

export type PointerResolution = {
  parent: any;
  key: string | null;
  value: any;
  exists: boolean;
};

export function resolvePointerResult(doc: any, ptr: string): Result<PointerResolution, JsonPointerError> {
  const segmentsRes = parseJsonPointerResult(ptr);
  if (Result.isError(segmentsRes)) return Result.err(segmentsRes.error);
  const segments = segmentsRes.value;
  if (segments.length === 0) {
    return Result.ok({ parent: null, key: null, value: doc, exists: true });
  }
  let cur: any = doc;
  for (let i = 0; i < segments.length - 1; i++) {
    cur = getChild(cur, segments[i]);
    if (cur === undefined) return Result.ok({ parent: null, key: null, value: undefined, exists: false });
  }
  const key = segments[segments.length - 1];
  const value = cur === undefined ? undefined : getChild(cur, key);
  return Result.ok({ parent: cur, key, value, exists: value !== undefined });
}

export function resolvePointer(doc: any, ptr: string): PointerResolution {
  const res = resolvePointerResult(doc, ptr);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function setPointerValueResult(doc: any, ptr: string, value: any, opts?: { createParents?: boolean }): Result<any, JsonPointerError> {
  const segmentsRes = parseJsonPointerResult(ptr);
  if (Result.isError(segmentsRes)) return Result.err(segmentsRes.error);
  const segments = segmentsRes.value;
  if (segments.length === 0) {
    return Result.ok(value);
  }
  let cur: any = doc;
  for (let i = 0; i < segments.length - 1; i++) {
    const seg = segments[i];
    let next = getChild(cur, seg);
    if (next === undefined) {
      if (!opts?.createParents) return invalidPointerOperation("missing parent");
      next = {};
      const setRes = setChildResult(cur, seg, next);
      if (Result.isError(setRes)) return setRes;
    }
    cur = next;
  }
  const leafRes = setChildResult(cur, segments[segments.length - 1], value);
  if (Result.isError(leafRes)) return leafRes;
  return Result.ok(doc);
}

export function setPointerValue(doc: any, ptr: string, value: any, opts?: { createParents?: boolean }): any {
  const res = setPointerValueResult(doc, ptr, value, opts);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function deletePointerValueResult(doc: any, ptr: string): Result<any, JsonPointerError> {
  const segmentsRes = parseJsonPointerResult(ptr);
  if (Result.isError(segmentsRes)) return Result.err(segmentsRes.error);
  const segments = segmentsRes.value;
  if (segments.length === 0) return invalidPointerOperation("cannot delete document root");
  let cur: any = doc;
  for (let i = 0; i < segments.length - 1; i++) {
    cur = getChild(cur, segments[i]);
    if (cur === undefined) return invalidPointerOperation("missing parent");
  }
  const deleteRes = deleteChildResult(cur, segments[segments.length - 1]);
  if (Result.isError(deleteRes)) return deleteRes;
  return Result.ok(doc);
}

export function deletePointerValue(doc: any, ptr: string): any {
  const res = deletePointerValueResult(doc, ptr);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}
