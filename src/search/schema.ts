import { Result } from "better-result";
import type { SchemaRegistry, SearchConfig, SearchFieldBinding, SearchFieldConfig } from "../schema/registry";
import { parseJsonPointerResult, resolvePointerResult } from "../util/json_pointer";
import { schemaVersionForOffset } from "../schema/read_json";

export type SearchExactTerm = {
  field: string;
  config: SearchFieldConfig;
  canonical: string;
  bytes: Uint8Array;
};

export type CompiledSearchFieldAccessor = {
  fieldName: string;
  config: SearchFieldConfig;
  path: string[];
};

export type CompiledSearchFieldValueVisitor = (
  accessor: CompiledSearchFieldAccessor,
  rawValue: unknown
) => void | Result<void, { message: string }>;

type FastJsonScalarExtraction = {
  exists: boolean;
  value: unknown;
};

const FAST_JSON_DECODER = new TextDecoder();

export function resolveSearchAlias(search: SearchConfig | undefined, fieldName: string): string {
  return search?.aliases?.[fieldName] ?? fieldName;
}

export function getSearchFieldConfig(search: SearchConfig | undefined, fieldName: string): SearchFieldConfig | null {
  const resolved = resolveSearchAlias(search, fieldName);
  return search?.fields?.[resolved] ?? null;
}

export function getSearchFieldBinding(config: SearchFieldConfig, version: number): SearchFieldBinding | null {
  let selected: SearchFieldBinding | null = null;
  for (const binding of config.bindings) {
    if (binding.version <= version && (!selected || binding.version > selected.version)) {
      selected = binding;
    }
  }
  return selected;
}

export function normalizeKeywordValue(value: unknown, normalizer: SearchFieldConfig["normalizer"]): string | null {
  if (typeof value !== "string") return null;
  return normalizer === "lowercase_v1" ? value.toLowerCase() : value;
}

export function compareSearchStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function canonicalizeExactValue(config: SearchFieldConfig, value: unknown): string | null {
  switch (config.kind) {
    case "keyword":
      return normalizeKeywordValue(value, config.normalizer);
    case "integer":
      if (typeof value === "bigint") return value.toString();
      if (typeof value === "number" && Number.isFinite(value) && Number.isInteger(value)) return String(value);
      if (typeof value === "string" && /^-?(0|[1-9][0-9]*)$/.test(value.trim())) return String(BigInt(value.trim()));
      return null;
    case "float":
      if (typeof value === "bigint") return value.toString();
      if (typeof value === "number" && Number.isFinite(value)) return String(value);
      if (typeof value === "string" && value.trim() !== "") {
        const n = Number(value);
        if (Number.isFinite(n)) return String(n);
      }
      return null;
    case "date":
      if (typeof value === "number" && Number.isFinite(value)) return String(Math.trunc(value));
      if (typeof value === "bigint") return value.toString();
      if (typeof value === "string" && value.trim() !== "") {
        const parsed = Date.parse(value);
        if (Number.isFinite(parsed)) return String(Math.trunc(parsed));
        if (/^-?(0|[1-9][0-9]*)$/.test(value.trim())) return String(BigInt(value.trim()));
      }
      return null;
    case "bool":
      if (typeof value === "boolean") return value ? "true" : "false";
      if (typeof value === "string") {
        const lowered = value.trim().toLowerCase();
        if (lowered === "true" || lowered === "false") return lowered;
      }
      return null;
    default:
      return null;
  }
}

export function canonicalizeColumnValue(config: SearchFieldConfig, value: unknown): bigint | number | boolean | null {
  switch (config.kind) {
    case "integer": {
      const canonical = canonicalizeExactValue(config, value);
      return canonical == null ? null : BigInt(canonical);
    }
    case "date": {
      const canonical = canonicalizeExactValue(config, value);
      return canonical == null ? null : BigInt(canonical);
    }
    case "float": {
      const canonical = canonicalizeExactValue(config, value);
      if (canonical == null) return null;
      const parsed = Number(canonical);
      return Number.isFinite(parsed) ? parsed : null;
    }
    case "bool":
      return canonicalizeExactValue(config, value) === "true"
        ? true
        : canonicalizeExactValue(config, value) === "false"
          ? false
          : null;
    default:
      return null;
  }
}

export function analyzeTextValue(value: string, analyzer: SearchFieldConfig["analyzer"]): string[] {
  if (analyzer !== "unicode_word_v1") return [];
  if (!/[^\x00-\x7f]/.test(value)) return analyzeAsciiWordValue(value);
  const matches = value.toLowerCase().match(/[\p{L}\p{N}]+/gu);
  return matches ? matches.filter((token) => token.length > 0) : [];
}

export function visitAnalyzedTextValue(
  value: string,
  analyzer: SearchFieldConfig["analyzer"],
  visitor: (token: string) => void
): void {
  if (analyzer !== "unicode_word_v1") return;
  if (!/[^\x00-\x7f]/.test(value)) {
    visitAsciiWordTokens(value, visitor);
    return;
  }
  const matches = value.toLowerCase().match(/[\p{L}\p{N}]+/gu);
  if (!matches) return;
  for (const token of matches) {
    if (token.length > 0) visitor(token);
  }
}

function analyzeAsciiWordValue(value: string): string[] {
  const tokens: string[] = [];
  visitAsciiWordTokens(value, (token) => tokens.push(token));
  return tokens;
}

function visitAsciiWordTokens(value: string, visitor: (token: string) => void): void {
  let tokenStart = -1;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    const isDigit = code >= 48 && code <= 57;
    const isUpper = code >= 65 && code <= 90;
    const isLower = code >= 97 && code <= 122;
    if (isDigit || isUpper || isLower) {
      if (tokenStart < 0) tokenStart = index;
      continue;
    }
    if (tokenStart >= 0) {
      visitor(value.slice(tokenStart, index).toLowerCase());
      tokenStart = -1;
    }
  }
  if (tokenStart >= 0) visitor(value.slice(tokenStart).toLowerCase());
}

function addRawValues(out: unknown[], value: unknown): void {
  if (Array.isArray(value)) {
    for (const item of value) addRawValues(out, item);
    return;
  }
  out.push(value);
}

function visitRawValuesResult(
  value: unknown,
  visitor: (rawValue: unknown) => void | Result<void, { message: string }>
): Result<void, { message: string }> {
  if (Array.isArray(value)) {
    for (const item of value) {
      const visitRes = visitRawValuesResult(item, visitor);
      if (Result.isError(visitRes)) return visitRes;
    }
    return Result.ok(undefined);
  }
  const visitRes = visitor(value);
  if (visitRes && typeof visitRes === "object" && "status" in visitRes && Result.isError(visitRes)) return visitRes;
  return Result.ok(undefined);
}

function isArrayIndex(seg: string): boolean {
  return seg !== "" && /^[0-9]+$/.test(seg);
}

function getChild(container: unknown, seg: string): unknown {
  if (Array.isArray(container) && isArrayIndex(seg)) {
    return container[Number(seg)];
  }
  if (container && typeof container === "object") {
    return (container as Record<string, unknown>)[seg];
  }
  return undefined;
}

function resolveCompiledPointer(value: unknown, path: string[]): { exists: boolean; value: unknown } {
  if (path.length === 0) return { exists: true, value };
  let current: unknown = value;
  for (const segment of path) {
    current = getChild(current, segment);
    if (current === undefined) return { exists: false, value: undefined };
  }
  return { exists: true, value: current };
}

export function compileSearchFieldAccessorsResult(
  reg: SchemaRegistry,
  fieldNames: Iterable<string>,
  version: number
): Result<CompiledSearchFieldAccessor[], { message: string }> {
  if (!reg.search) return Result.ok([]);
  const out: CompiledSearchFieldAccessor[] = [];
  for (const fieldName of fieldNames) {
    const config = reg.search.fields[fieldName];
    if (!config) continue;
    const binding = getSearchFieldBinding(config, version);
    if (!binding) continue;
    const pathRes = parseJsonPointerResult(binding.jsonPointer);
    if (Result.isError(pathRes)) return Result.err({ message: pathRes.error.message });
    out.push({ fieldName, config, path: pathRes.value });
  }
  return Result.ok(out);
}

export function extractRawSearchValuesWithCompiledAccessorsResult(
  value: unknown,
  accessors: ReadonlyArray<CompiledSearchFieldAccessor>
): Result<Map<string, unknown[]>, { message: string }> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return Result.err({ message: "search fields require JSON object records" });
  }
  const out = new Map<string, unknown[]>();
  for (const accessor of accessors) {
    const resolved = resolveCompiledPointer(value, accessor.path);
    if (!resolved.exists) continue;
    const values: unknown[] = [];
    addRawValues(values, resolved.value);
    if (values.length > 0) out.set(accessor.fieldName, values);
  }
  return Result.ok(out);
}

export function visitRawSearchValuesWithCompiledAccessorsResult(
  value: unknown,
  accessors: ReadonlyArray<CompiledSearchFieldAccessor>,
  visitor: CompiledSearchFieldValueVisitor
): Result<void, { message: string }> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return Result.err({ message: "search fields require JSON object records" });
  }
  for (const accessor of accessors) {
    const resolved = resolveCompiledPointer(value, accessor.path);
    if (!resolved.exists) continue;
    const visitRes = visitRawValuesResult(resolved.value, (rawValue) => visitor(accessor, rawValue));
    if (Result.isError(visitRes)) return visitRes;
  }
  return Result.ok(undefined);
}

export function visitCompiledAccessorRawValuesResult(
  value: unknown,
  accessor: CompiledSearchFieldAccessor,
  visitor: (rawValue: unknown) => void | Result<void, { message: string }>
): Result<void, { message: string }> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return Result.err({ message: "search fields require JSON object records" });
  }
  const resolved = resolveCompiledPointer(value, accessor.path);
  if (!resolved.exists) return Result.ok(undefined);
  return visitRawValuesResult(resolved.value, visitor);
}

function skipJsonWhitespace(json: string, index: number): number {
  let cursor = index;
  while (cursor < json.length) {
    const code = json.charCodeAt(cursor);
    if (code === 32 || code === 10 || code === 13 || code === 9) {
      cursor += 1;
      continue;
    }
    break;
  }
  return cursor;
}

function scanJsonStringEndResult(json: string, start: number): Result<number, { message: string }> {
  let index = start + 1;
  while (index < json.length) {
    const code = json.charCodeAt(index);
    if (code === 34) return Result.ok(index);
    if (code === 92) {
      index += 1;
      if (index >= json.length) return Result.err({ message: "unterminated JSON escape" });
      if (json.charCodeAt(index) === 117) {
        index += 4;
        if (index >= json.length) return Result.err({ message: "unterminated unicode escape" });
      }
    }
    index += 1;
  }
  return Result.err({ message: "unterminated JSON string" });
}

function decodeJsonStringTokenResult(json: string, start: number, end: number): Result<string, { message: string }> {
  const raw = json.slice(start + 1, end);
  if (!raw.includes("\\")) return Result.ok(raw);
  try {
    return Result.ok(JSON.parse(json.slice(start, end + 1)));
  } catch {
    return Result.err({ message: "invalid JSON string token" });
  }
}

function parseJsonScalarTokenResult(json: string, start: number): Result<{ value: unknown; next: number }, { message: string }> {
  const ch = json[start];
  if (ch === "\"") {
    const endRes = scanJsonStringEndResult(json, start);
    if (Result.isError(endRes)) return endRes;
    const valueRes = decodeJsonStringTokenResult(json, start, endRes.value);
    if (Result.isError(valueRes)) return valueRes;
    return Result.ok({ value: valueRes.value, next: endRes.value + 1 });
  }
  if (ch === "t" && json.startsWith("true", start)) return Result.ok({ value: true, next: start + 4 });
  if (ch === "f" && json.startsWith("false", start)) return Result.ok({ value: false, next: start + 5 });
  if (ch === "n" && json.startsWith("null", start)) return Result.ok({ value: null, next: start + 4 });
  if (ch === "-" || (ch != null && ch >= "0" && ch <= "9")) {
    let end = start + 1;
    while (end < json.length) {
      const code = json.charCodeAt(end);
      const isNumberChar =
        (code >= 48 && code <= 57) || code === 45 || code === 43 || code === 46 || code === 69 || code === 101;
      if (!isNumberChar) break;
      end += 1;
    }
    const token = json.slice(start, end);
    const value = Number(token);
    if (!Number.isFinite(value)) return Result.err({ message: "invalid JSON number token" });
    return Result.ok({ value, next: end });
  }
  return Result.err({ message: "unsupported JSON scalar token" });
}

function skipJsonValueResult(json: string, start: number): Result<number, { message: string }> {
  const ch = json[start];
  if (ch === "\"") {
    const endRes = scanJsonStringEndResult(json, start);
    if (Result.isError(endRes)) return endRes;
    return Result.ok(endRes.value + 1);
  }
  if (ch === "{" || ch === "[") {
    const stack = [ch];
    let index = start + 1;
    while (index < json.length) {
      const current = json[index];
      if (current === "\"") {
        const endRes = scanJsonStringEndResult(json, index);
        if (Result.isError(endRes)) return endRes;
        index = endRes.value + 1;
        continue;
      }
      if (current === "{" || current === "[") {
        stack.push(current);
      } else if (current === "}" || current === "]") {
        const open = stack.pop();
        if ((current === "}" && open !== "{") || (current === "]" && open !== "[")) {
          return Result.err({ message: "mismatched JSON container" });
        }
        if (stack.length === 0) return Result.ok(index + 1);
      }
      index += 1;
    }
    return Result.err({ message: "unterminated JSON container" });
  }
  const scalarRes = parseJsonScalarTokenResult(json, start);
  if (Result.isError(scalarRes)) return scalarRes;
  return Result.ok(scalarRes.value.next);
}

export function supportsFastScalarJsonExtraction(accessor: CompiledSearchFieldAccessor): boolean {
  return (
    accessor.path.length === 1 &&
    (accessor.config.kind === "keyword" ||
      accessor.config.kind === "integer" ||
      accessor.config.kind === "float" ||
      accessor.config.kind === "date" ||
      accessor.config.kind === "bool")
  );
}

export function supportsFastTopLevelScalarJsonScan(accessor: CompiledSearchFieldAccessor): boolean {
  return (
    accessor.path.length === 1 &&
    (accessor.config.kind === "keyword" ||
      accessor.config.kind === "text" ||
      accessor.config.kind === "integer" ||
      accessor.config.kind === "float" ||
      accessor.config.kind === "date" ||
      accessor.config.kind === "bool")
  );
}

export function supportsFastScalarJsonScan(accessor: CompiledSearchFieldAccessor): boolean {
  return (
    accessor.path.length > 0 &&
    accessor.path.every((segment) => !isArrayIndex(segment)) &&
    (accessor.config.kind === "keyword" ||
      accessor.config.kind === "text" ||
      accessor.config.kind === "integer" ||
      accessor.config.kind === "float" ||
      accessor.config.kind === "date" ||
      accessor.config.kind === "bool")
  );
}

export function extractFastScalarJsonValueResult(
  json: string,
  accessor: CompiledSearchFieldAccessor
): Result<FastJsonScalarExtraction, { message: string }> {
  if (!supportsFastScalarJsonExtraction(accessor)) {
    return Result.err({ message: "compiled accessor does not support fast scalar JSON extraction" });
  }
  let index = skipJsonWhitespace(json, 0);
  if (json[index] !== "{") return Result.err({ message: "search fields require JSON object records" });
  index += 1;
  const targetKey = accessor.path[0]!;
  while (index < json.length) {
    index = skipJsonWhitespace(json, index);
    const token = json[index];
    if (token === "}") return Result.ok({ exists: false, value: undefined });
    if (token !== "\"") return Result.err({ message: "invalid JSON object key" });
    const keyEndRes = scanJsonStringEndResult(json, index);
    if (Result.isError(keyEndRes)) return keyEndRes;
    const keyRes = decodeJsonStringTokenResult(json, index, keyEndRes.value);
    if (Result.isError(keyRes)) return keyRes;
    index = skipJsonWhitespace(json, keyEndRes.value + 1);
    if (json[index] !== ":") return Result.err({ message: "invalid JSON object separator" });
    index = skipJsonWhitespace(json, index + 1);
    if (keyRes.value === targetKey) {
      const valueRes = parseJsonScalarTokenResult(json, index);
      if (Result.isError(valueRes)) return valueRes;
      return Result.ok({ exists: true, value: valueRes.value.value });
    }
    const skipRes = skipJsonValueResult(json, index);
    if (Result.isError(skipRes)) return skipRes;
    index = skipJsonWhitespace(json, skipRes.value);
    if (json[index] === ",") {
      index += 1;
      continue;
    }
    if (json[index] === "}") return Result.ok({ exists: false, value: undefined });
    return Result.err({ message: "invalid JSON object delimiter" });
  }
  return Result.err({ message: "unterminated JSON object" });
}

function skipJsonWhitespaceBytes(bytes: Uint8Array, index: number): number {
  let cursor = index;
  while (cursor < bytes.length) {
    const code = bytes[cursor]!;
    if (code === 32 || code === 10 || code === 13 || code === 9) {
      cursor += 1;
      continue;
    }
    break;
  }
  return cursor;
}

function scanJsonStringEndBytesResult(
  bytes: Uint8Array,
  start: number
): Result<{ end: number; hadEscape: boolean }, { message: string }> {
  let index = start + 1;
  let hadEscape = false;
  while (index < bytes.length) {
    const code = bytes[index]!;
    if (code === 34) return Result.ok({ end: index, hadEscape });
    if (code === 92) {
      hadEscape = true;
      index += 1;
      if (index >= bytes.length) return Result.err({ message: "unterminated JSON escape" });
      if (bytes[index] === 117) {
        index += 4;
        if (index >= bytes.length) return Result.err({ message: "unterminated unicode escape" });
      }
    }
    index += 1;
  }
  return Result.err({ message: "unterminated JSON string" });
}

function decodeJsonStringBytesResult(
  bytes: Uint8Array,
  start: number,
  end: number,
  hadEscape: boolean
): Result<string, { message: string }> {
  if (!hadEscape) return Result.ok(FAST_JSON_DECODER.decode(bytes.subarray(start + 1, end)));
  try {
    return Result.ok(JSON.parse(FAST_JSON_DECODER.decode(bytes.subarray(start, end + 1))));
  } catch {
    return Result.err({ message: "invalid JSON string token" });
  }
}

function bytesEqualAsciiString(bytes: Uint8Array, start: number, end: number, expected: string): boolean {
  if (end - start !== expected.length) return false;
  for (let index = 0; index < expected.length; index += 1) {
    if (bytes[start + index] !== expected.charCodeAt(index)) return false;
  }
  return true;
}

function parseJsonScalarBytesResult(
  bytes: Uint8Array,
  start: number
): Result<{ value: unknown; next: number }, { message: string }> {
  const code = bytes[start];
  if (code === 34) {
    const endRes = scanJsonStringEndBytesResult(bytes, start);
    if (Result.isError(endRes)) return endRes;
    const valueRes = decodeJsonStringBytesResult(bytes, start, endRes.value.end, endRes.value.hadEscape);
    if (Result.isError(valueRes)) return valueRes;
    return Result.ok({ value: valueRes.value, next: endRes.value.end + 1 });
  }
  if (
    code === 116 &&
    bytes[start + 1] === 114 &&
    bytes[start + 2] === 117 &&
    bytes[start + 3] === 101
  ) {
    return Result.ok({ value: true, next: start + 4 });
  }
  if (
    code === 102 &&
    bytes[start + 1] === 97 &&
    bytes[start + 2] === 108 &&
    bytes[start + 3] === 115 &&
    bytes[start + 4] === 101
  ) {
    return Result.ok({ value: false, next: start + 5 });
  }
  if (
    code === 110 &&
    bytes[start + 1] === 117 &&
    bytes[start + 2] === 108 &&
    bytes[start + 3] === 108
  ) {
    return Result.ok({ value: null, next: start + 4 });
  }
  if (code === 45 || (code != null && code >= 48 && code <= 57)) {
    let end = start + 1;
    while (end < bytes.length) {
      const c = bytes[end]!;
      const isNumberChar = (c >= 48 && c <= 57) || c === 45 || c === 43 || c === 46 || c === 69 || c === 101;
      if (!isNumberChar) break;
      end += 1;
    }
    const value = Number(FAST_JSON_DECODER.decode(bytes.subarray(start, end)));
    if (!Number.isFinite(value)) return Result.err({ message: "invalid JSON number token" });
    return Result.ok({ value, next: end });
  }
  return Result.err({ message: "unsupported JSON scalar token" });
}

function skipJsonValueBytesResult(bytes: Uint8Array, start: number): Result<number, { message: string }> {
  const code = bytes[start];
  if (code === 34) {
    const endRes = scanJsonStringEndBytesResult(bytes, start);
    if (Result.isError(endRes)) return endRes;
    return Result.ok(endRes.value.end + 1);
  }
  if (code === 123 || code === 91) {
    const stack = [code];
    let index = start + 1;
    while (index < bytes.length) {
      const current = bytes[index]!;
      if (current === 34) {
        const endRes = scanJsonStringEndBytesResult(bytes, index);
        if (Result.isError(endRes)) return endRes;
        index = endRes.value.end + 1;
        continue;
      }
      if (current === 123 || current === 91) {
        stack.push(current);
      } else if (current === 125 || current === 93) {
        const open = stack.pop();
        if ((current === 125 && open !== 123) || (current === 93 && open !== 91)) {
          return Result.err({ message: "mismatched JSON container" });
        }
        if (stack.length === 0) return Result.ok(index + 1);
      }
      index += 1;
    }
    return Result.err({ message: "unterminated JSON container" });
  }
  const scalarRes = parseJsonScalarBytesResult(bytes, start);
  if (Result.isError(scalarRes)) return scalarRes;
  return Result.ok(scalarRes.value.next);
}

export function extractFastScalarJsonValueFromBytesResult(
  bytes: Uint8Array,
  accessor: CompiledSearchFieldAccessor
): Result<FastJsonScalarExtraction, { message: string }> {
  if (!supportsFastScalarJsonExtraction(accessor)) {
    return Result.err({ message: "compiled accessor does not support fast scalar JSON extraction" });
  }
  let index = skipJsonWhitespaceBytes(bytes, 0);
  if (bytes[index] !== 123) return Result.err({ message: "search fields require JSON object records" });
  index += 1;
  const targetKey = accessor.path[0]!;
  while (index < bytes.length) {
    index = skipJsonWhitespaceBytes(bytes, index);
    const token = bytes[index];
    if (token === 125) return Result.ok({ exists: false, value: undefined });
    if (token !== 34) return Result.err({ message: "invalid JSON object key" });
    const keyEndRes = scanJsonStringEndBytesResult(bytes, index);
    if (Result.isError(keyEndRes)) return keyEndRes;
    let matchesTarget = false;
    if (keyEndRes.value.hadEscape) {
      const keyRes = decodeJsonStringBytesResult(bytes, index, keyEndRes.value.end, true);
      if (Result.isError(keyRes)) return keyRes;
      matchesTarget = keyRes.value === targetKey;
    } else {
      matchesTarget = bytesEqualAsciiString(bytes, index + 1, keyEndRes.value.end, targetKey);
    }
    index = skipJsonWhitespaceBytes(bytes, keyEndRes.value.end + 1);
    if (bytes[index] !== 58) return Result.err({ message: "invalid JSON object separator" });
    index = skipJsonWhitespaceBytes(bytes, index + 1);
    if (matchesTarget) {
      const valueRes = parseJsonScalarBytesResult(bytes, index);
      if (Result.isError(valueRes)) return valueRes;
      return Result.ok({ exists: true, value: valueRes.value.value });
    }
    const skipRes = skipJsonValueBytesResult(bytes, index);
    if (Result.isError(skipRes)) return skipRes;
    index = skipJsonWhitespaceBytes(bytes, skipRes.value);
    if (bytes[index] === 44) {
      index += 1;
      continue;
    }
    if (bytes[index] === 125) return Result.ok({ exists: false, value: undefined });
    return Result.err({ message: "invalid JSON object delimiter" });
  }
  return Result.err({ message: "unterminated JSON object" });
}

export function visitFastTopLevelScalarJsonValuesFromBytesResult(
  bytes: Uint8Array,
  accessors: ReadonlyArray<CompiledSearchFieldAccessor>,
  visitor: CompiledSearchFieldValueVisitor
): Result<void, { message: string }> {
  if (accessors.length === 0) return Result.ok(undefined);
  const accessorsByKey = new Map<string, CompiledSearchFieldAccessor[]>();
  for (const accessor of accessors) {
    if (!supportsFastTopLevelScalarJsonScan(accessor)) {
      return Result.err({ message: `compiled accessor ${accessor.fieldName} does not support fast top-level scalar scan` });
    }
    const key = accessor.path[0]!;
    const existing = accessorsByKey.get(key) ?? [];
    existing.push(accessor);
    accessorsByKey.set(key, existing);
  }

  let index = skipJsonWhitespaceBytes(bytes, 0);
  if (bytes[index] !== 123) return Result.err({ message: "search fields require JSON object records" });
  index += 1;

  while (index < bytes.length) {
    index = skipJsonWhitespaceBytes(bytes, index);
    const token = bytes[index];
    if (token === 125) return Result.ok(undefined);
    if (token !== 34) return Result.err({ message: "invalid JSON object key" });
    const keyEndRes = scanJsonStringEndBytesResult(bytes, index);
    if (Result.isError(keyEndRes)) return keyEndRes;
    let key: string;
    if (!keyEndRes.value.hadEscape) {
      key = FAST_JSON_DECODER.decode(bytes.subarray(index + 1, keyEndRes.value.end));
    } else {
      const keyRes = decodeJsonStringBytesResult(bytes, index, keyEndRes.value.end, true);
      if (Result.isError(keyRes)) return keyRes;
      key = keyRes.value;
    }
    index = skipJsonWhitespaceBytes(bytes, keyEndRes.value.end + 1);
    if (bytes[index] !== 58) return Result.err({ message: "invalid JSON object separator" });
    index = skipJsonWhitespaceBytes(bytes, index + 1);
    const matched = accessorsByKey.get(key) ?? [];
    if (matched.length === 0) {
      const skipRes = skipJsonValueBytesResult(bytes, index);
      if (Result.isError(skipRes)) return skipRes;
      index = skipRes.value;
    } else if (bytes[index] === 123 || bytes[index] === 91) {
      const skipRes = skipJsonValueBytesResult(bytes, index);
      if (Result.isError(skipRes)) return skipRes;
      index = skipRes.value;
    } else {
      const valueRes = parseJsonScalarBytesResult(bytes, index);
      if (Result.isError(valueRes)) return valueRes;
      for (const accessor of matched) {
        const visitRes = visitor(accessor, valueRes.value.value);
        if (visitRes && typeof visitRes === "object" && "status" in visitRes && Result.isError(visitRes)) return visitRes;
      }
      index = valueRes.value.next;
    }

    index = skipJsonWhitespaceBytes(bytes, index);
    if (bytes[index] === 44) {
      index += 1;
      continue;
    }
    if (bytes[index] === 125) return Result.ok(undefined);
    return Result.err({ message: "invalid JSON object delimiter" });
  }
  return Result.err({ message: "unterminated JSON object" });
}

export type FastScalarAccessorTrie = {
  accessors: CompiledSearchFieldAccessor[];
  children: Map<string, FastScalarAccessorTrie>;
};

function createFastAccessorTrieNode(): FastScalarAccessorTrie {
  return {
    accessors: [],
    children: new Map(),
  };
}

export function buildFastScalarAccessorTrieResult(
  accessors: ReadonlyArray<CompiledSearchFieldAccessor>
): Result<FastScalarAccessorTrie, { message: string }> {
  const root = createFastAccessorTrieNode();
  for (const accessor of accessors) {
    if (!supportsFastScalarJsonScan(accessor)) {
      return Result.err({ message: `compiled accessor ${accessor.fieldName} does not support fast scalar JSON scan` });
    }
    let node = root;
    for (const segment of accessor.path) {
      let child = node.children.get(segment);
      if (!child) {
        child = createFastAccessorTrieNode();
        node.children.set(segment, child);
      }
      node = child;
    }
    node.accessors.push(accessor);
  }
  return Result.ok(root);
}

function visitFastScalarObjectNodeResult(
  bytes: Uint8Array,
  start: number,
  node: FastScalarAccessorTrie,
  visitor: CompiledSearchFieldValueVisitor
): Result<number, { message: string }> {
  let index = skipJsonWhitespaceBytes(bytes, start);
  if (bytes[index] !== 123) return Result.err({ message: "search fields require JSON object records" });
  index += 1;
  while (index < bytes.length) {
    index = skipJsonWhitespaceBytes(bytes, index);
    const token = bytes[index];
    if (token === 125) return Result.ok(index + 1);
    if (token !== 34) return Result.err({ message: "invalid JSON object key" });
    const keyEndRes = scanJsonStringEndBytesResult(bytes, index);
    if (Result.isError(keyEndRes)) return keyEndRes;
    let key: string;
    if (!keyEndRes.value.hadEscape) {
      key = FAST_JSON_DECODER.decode(bytes.subarray(index + 1, keyEndRes.value.end));
    } else {
      const keyRes = decodeJsonStringBytesResult(bytes, index, keyEndRes.value.end, true);
      if (Result.isError(keyRes)) return keyRes;
      key = keyRes.value;
    }
    index = skipJsonWhitespaceBytes(bytes, keyEndRes.value.end + 1);
    if (bytes[index] !== 58) return Result.err({ message: "invalid JSON object separator" });
    index = skipJsonWhitespaceBytes(bytes, index + 1);
    const child = node.children.get(key);
    if (!child) {
      const skipRes = skipJsonValueBytesResult(bytes, index);
      if (Result.isError(skipRes)) return skipRes;
      index = skipRes.value;
    } else if (bytes[index] === 123 && child.children.size > 0) {
      const nestedRes = visitFastScalarObjectNodeResult(bytes, index, child, visitor);
      if (Result.isError(nestedRes)) return nestedRes;
      index = nestedRes.value;
    } else if (bytes[index] === 123 || bytes[index] === 91) {
      const skipRes = skipJsonValueBytesResult(bytes, index);
      if (Result.isError(skipRes)) return skipRes;
      index = skipRes.value;
    } else {
      const valueRes = parseJsonScalarBytesResult(bytes, index);
      if (Result.isError(valueRes)) return valueRes;
      for (const accessor of child.accessors) {
        const visitRes = visitor(accessor, valueRes.value.value);
        if (visitRes && typeof visitRes === "object" && "status" in visitRes && Result.isError(visitRes)) return visitRes;
      }
      index = valueRes.value.next;
    }

    index = skipJsonWhitespaceBytes(bytes, index);
    if (bytes[index] === 44) {
      index += 1;
      continue;
    }
    if (bytes[index] === 125) return Result.ok(index + 1);
    return Result.err({ message: "invalid JSON object delimiter" });
  }
  return Result.err({ message: "unterminated JSON object" });
}

export function visitFastScalarJsonValuesFromBytesWithTrieResult(
  bytes: Uint8Array,
  trie: FastScalarAccessorTrie,
  visitor: CompiledSearchFieldValueVisitor
): Result<void, { message: string }> {
  const visitRes = visitFastScalarObjectNodeResult(bytes, 0, trie, visitor);
  if (Result.isError(visitRes)) return visitRes;
  return Result.ok(undefined);
}

export function visitFastScalarJsonValuesFromBytesResult(
  bytes: Uint8Array,
  accessors: ReadonlyArray<CompiledSearchFieldAccessor>,
  visitor: CompiledSearchFieldValueVisitor
): Result<void, { message: string }> {
  if (accessors.length === 0) return Result.ok(undefined);
  const trieRes = buildFastScalarAccessorTrieResult(accessors);
  if (Result.isError(trieRes)) return trieRes;
  return visitFastScalarJsonValuesFromBytesWithTrieResult(bytes, trieRes.value, visitor);
}

export function extractRawSearchValuesForFieldsResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown,
  fieldNames: Iterable<string>
): Result<Map<string, unknown[]>, { message: string }> {
  if (!reg.search) return Result.ok(new Map());
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return Result.err({ message: "search fields require JSON object records" });
  }
  const version = schemaVersionForOffset(reg, offset);
  const out = new Map<string, unknown[]>();
  for (const fieldName of fieldNames) {
    const config = reg.search.fields[fieldName];
    if (!config) continue;
    const binding = getSearchFieldBinding(config, version);
    if (!binding) continue;
    const resolvedRes = resolvePointerResult(value, binding.jsonPointer);
    if (Result.isError(resolvedRes)) return Result.err({ message: resolvedRes.error.message });
    if (!resolvedRes.value.exists) continue;
    const values: unknown[] = [];
    addRawValues(values, resolvedRes.value.value);
    if (values.length > 0) out.set(fieldName, values);
  }
  return Result.ok(out);
}

export function extractRawSearchValuesResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<Map<string, unknown[]>, { message: string }> {
  return extractRawSearchValuesForFieldsResult(reg, offset, value, Object.keys(reg.search?.fields ?? {}));
}

export function extractSearchExactTermsResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<SearchExactTerm[], { message: string }> {
  const rawValuesRes = extractRawSearchValuesResult(reg, offset, value);
  if (Result.isError(rawValuesRes)) return rawValuesRes;
  const out: SearchExactTerm[] = [];
  const seen = new Set<string>();
  for (const [fieldName, values] of rawValuesRes.value) {
    const config = reg.search?.fields[fieldName];
    if (!config?.exact) continue;
    for (const rawValue of values) {
      const canonical = canonicalizeExactValue(config, rawValue);
      if (canonical == null) continue;
      const dedupeKey = `${fieldName}\u0000${canonical}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      out.push({
        field: fieldName,
        config,
        canonical,
        bytes: new TextEncoder().encode(canonical),
      });
    }
  }
  return Result.ok(out);
}

export function extractSearchExactValuesResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<Map<string, string[]>, { message: string }> {
  const rawValuesRes = extractRawSearchValuesResult(reg, offset, value);
  if (Result.isError(rawValuesRes)) return rawValuesRes;
  const out = new Map<string, string[]>();
  for (const [fieldName, values] of rawValuesRes.value) {
    const config = reg.search?.fields[fieldName];
    if (!config) continue;
    const exactValues: string[] = [];
    for (const rawValue of values) {
      const canonical = canonicalizeExactValue(config, rawValue);
      if (canonical != null) exactValues.push(canonical);
    }
    if (exactValues.length > 0) out.set(fieldName, exactValues);
  }
  return Result.ok(out);
}

export function extractSearchColumnValuesResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<Map<string, Array<bigint | number | boolean>>, { message: string }> {
  const rawValuesRes = extractRawSearchValuesResult(reg, offset, value);
  if (Result.isError(rawValuesRes)) return rawValuesRes;
  const out = new Map<string, Array<bigint | number | boolean>>();
  for (const [fieldName, values] of rawValuesRes.value) {
    const config = reg.search?.fields[fieldName];
    if (!config?.column) continue;
    const colValues: Array<bigint | number | boolean> = [];
    for (const rawValue of values) {
      const normalized = canonicalizeColumnValue(config, rawValue);
      if (normalized != null) colValues.push(normalized);
    }
    if (colValues.length > 0) out.set(fieldName, colValues);
  }
  return Result.ok(out);
}

export function extractSearchTextValuesResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<Map<string, string[]>, { message: string }> {
  const rawValuesRes = extractRawSearchValuesResult(reg, offset, value);
  if (Result.isError(rawValuesRes)) return rawValuesRes;
  const out = new Map<string, string[]>();
  for (const [fieldName, values] of rawValuesRes.value) {
    const config = reg.search?.fields[fieldName];
    if (!config) continue;
    const textValues: string[] = [];
    for (const rawValue of values) {
      if (config.kind === "keyword") {
        const normalized = normalizeKeywordValue(rawValue, config.normalizer);
        if (normalized != null) textValues.push(normalized);
      } else if (config.kind === "text" && typeof rawValue === "string") {
        textValues.push(rawValue);
      }
    }
    if (textValues.length > 0) out.set(fieldName, textValues);
  }
  return Result.ok(out);
}
