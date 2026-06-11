import { gunzipSync } from "node:zlib";
import { Result } from "better-result";
import type { OtlpTraceExportError, OtlpTraceExportResult } from "../profile";
import {
  normalizeOtelDecodedSpanResult,
  type DecodedOtelEvent,
  type DecodedOtelLink,
  type DecodedOtelSpan,
  type OtelTracesStreamProfile,
} from "./normalize";

const JSON_TEXT_DECODER = new TextDecoder();
const JSON_CONTENT_TYPE = "application/json";
const PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

type ResourceSpansDecoded = {
  resourceAttributes: Record<string, unknown>;
  resourceSchemaUrl: string | null;
  scopeSpans: ScopeSpansDecoded[];
};

type ScopeSpansDecoded = {
  scope: {
    name: string | null;
    version: string | null;
    schemaUrl: string | null;
    attributes: Record<string, unknown>;
  };
  spans: Array<Omit<DecodedOtelSpan, "resourceAttributes" | "resourceSchemaUrl" | "instrumentationScope">>;
};

function baseContentType(value: string): string {
  return value.split(";")[0]?.trim().toLowerCase() ?? "";
}

function hexFromBytes(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function normalizeString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

function normalizeNanoString(value: unknown): string | null {
  if (value == null) return null;
  if (typeof value === "bigint") return value >= 0n ? value.toString() : null;
  if (typeof value === "number" && Number.isFinite(value) && Number.isInteger(value) && value >= 0) return BigInt(value).toString();
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^(0|[1-9][0-9]*)$/.test(trimmed)) return trimmed;
  }
  return null;
}

function anyValueFromJson(raw: unknown): unknown {
  if (!isPlainObject(raw)) return structuredClone(raw);
  if (Object.prototype.hasOwnProperty.call(raw, "stringValue")) return normalizeString(raw.stringValue) ?? "";
  if (Object.prototype.hasOwnProperty.call(raw, "boolValue")) return raw.boolValue === true;
  if (Object.prototype.hasOwnProperty.call(raw, "intValue")) {
    const value = raw.intValue;
    if (typeof value === "string" && /^-?(0|[1-9][0-9]*)$/.test(value.trim())) return value.trim();
    if (typeof value === "number" && Number.isFinite(value)) return Math.trunc(value);
    return null;
  }
  if (Object.prototype.hasOwnProperty.call(raw, "doubleValue")) return typeof raw.doubleValue === "number" ? raw.doubleValue : Number(raw.doubleValue);
  if (Object.prototype.hasOwnProperty.call(raw, "bytesValue")) return normalizeString(raw.bytesValue) ?? "";
  if (isPlainObject(raw.arrayValue) && Array.isArray(raw.arrayValue.values)) {
    return raw.arrayValue.values.map(anyValueFromJson);
  }
  if (isPlainObject(raw.kvlistValue) && Array.isArray(raw.kvlistValue.values)) {
    return keyValuesFromJson(raw.kvlistValue.values);
  }
  return structuredClone(raw);
}

function keyValuesFromJson(raw: unknown): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  if (!Array.isArray(raw)) return out;
  for (const item of raw) {
    if (!isPlainObject(item)) continue;
    const key = normalizeString(item.key);
    if (!key) continue;
    out[key] = anyValueFromJson(item.value);
  }
  return out;
}

function eventFromJson(raw: unknown): DecodedOtelEvent | null {
  if (!isPlainObject(raw)) return null;
  return {
    timeUnixNano: normalizeNanoString(raw.timeUnixNano),
    name: normalizeString(raw.name) ?? "",
    attributes: keyValuesFromJson(raw.attributes),
    droppedAttributesCount: typeof raw.droppedAttributesCount === "number" ? raw.droppedAttributesCount : Number(raw.droppedAttributesCount ?? 0),
  };
}

function linkFromJson(raw: unknown): DecodedOtelLink | null {
  if (!isPlainObject(raw)) return null;
  const traceId = normalizeString(raw.traceId);
  const spanId = normalizeString(raw.spanId);
  if (!traceId || !spanId) return null;
  return {
    traceId,
    spanId,
    traceState: normalizeString(raw.traceState),
    attributes: keyValuesFromJson(raw.attributes),
    droppedAttributesCount: typeof raw.droppedAttributesCount === "number" ? raw.droppedAttributesCount : Number(raw.droppedAttributesCount ?? 0),
  };
}

function spanFromJson(raw: unknown): Omit<DecodedOtelSpan, "resourceAttributes" | "resourceSchemaUrl" | "instrumentationScope"> | null {
  if (!isPlainObject(raw)) return null;
  const traceId = normalizeString(raw.traceId);
  const spanId = normalizeString(raw.spanId);
  if (!traceId || !spanId) return null;
  const status = isPlainObject(raw.status) ? raw.status : {};
  return {
    traceId,
    spanId,
    parentSpanId: normalizeString(raw.parentSpanId),
    traceState: normalizeString(raw.traceState),
    traceFlags: typeof raw.flags === "number" ? raw.flags : Number(raw.flags ?? raw.traceFlags ?? 0),
    name: normalizeString(raw.name) ?? "",
    kind: raw.kind as number | string | null | undefined,
    startUnixNano: normalizeNanoString(raw.startTimeUnixNano),
    endUnixNano: normalizeNanoString(raw.endTimeUnixNano),
    status: {
      code: status.code as number | string | null | undefined,
      message: normalizeString(status.message),
    },
    attributes: keyValuesFromJson(raw.attributes),
    events: Array.isArray(raw.events) ? raw.events.map(eventFromJson).filter((event): event is DecodedOtelEvent => !!event) : [],
    links: Array.isArray(raw.links) ? raw.links.map(linkFromJson).filter((link): link is DecodedOtelLink => !!link) : [],
    droppedAttributesCount: typeof raw.droppedAttributesCount === "number" ? raw.droppedAttributesCount : Number(raw.droppedAttributesCount ?? 0),
    droppedEventsCount: typeof raw.droppedEventsCount === "number" ? raw.droppedEventsCount : Number(raw.droppedEventsCount ?? 0),
    droppedLinksCount: typeof raw.droppedLinksCount === "number" ? raw.droppedLinksCount : Number(raw.droppedLinksCount ?? 0),
  };
}

function decodeJsonExportResult(body: Uint8Array): Result<DecodedOtelSpan[], { message: string }> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(JSON_TEXT_DECODER.decode(body));
  } catch {
    return Result.err({ message: "invalid OTLP JSON" });
  }
  if (!isPlainObject(parsed)) return Result.err({ message: "OTLP JSON request must be an object" });
  const out: DecodedOtelSpan[] = [];
  const resourceSpans = Array.isArray(parsed.resourceSpans) ? parsed.resourceSpans : [];
  for (const resourceSpanRaw of resourceSpans) {
    if (!isPlainObject(resourceSpanRaw)) continue;
    const resource = isPlainObject(resourceSpanRaw.resource) ? resourceSpanRaw.resource : {};
    const resourceAttributes = keyValuesFromJson(resource.attributes);
    const resourceSchemaUrl = normalizeString(resourceSpanRaw.schemaUrl);
    const scopeSpans = [
      ...(Array.isArray(resourceSpanRaw.scopeSpans) ? resourceSpanRaw.scopeSpans : []),
      ...(Array.isArray(resourceSpanRaw.instrumentationLibrarySpans) ? resourceSpanRaw.instrumentationLibrarySpans : []),
    ];
    for (const scopeSpanRaw of scopeSpans) {
      if (!isPlainObject(scopeSpanRaw)) continue;
      const scopeRaw = isPlainObject(scopeSpanRaw.scope) ? scopeSpanRaw.scope : isPlainObject(scopeSpanRaw.instrumentationLibrary) ? scopeSpanRaw.instrumentationLibrary : {};
      const scope = {
        name: normalizeString(scopeRaw.name),
        version: normalizeString(scopeRaw.version),
        schemaUrl: normalizeString(scopeSpanRaw.schemaUrl),
        attributes: keyValuesFromJson(scopeRaw.attributes),
      };
      const spans = Array.isArray(scopeSpanRaw.spans) ? scopeSpanRaw.spans : [];
      for (const spanRaw of spans) {
        const span = spanFromJson(spanRaw);
        if (!span) continue;
        out.push({
          ...span,
          resourceAttributes,
          resourceSchemaUrl,
          instrumentationScope: scope,
        });
      }
    }
  }
  return Result.ok(out);
}

class ProtoReader {
  private pos = 0;

  constructor(private readonly bytes: Uint8Array) {}

  eof(): boolean {
    return this.pos >= this.bytes.byteLength;
  }

  readTag(): Result<{ field: number; wire: number }, { message: string }> {
    const tagRes = this.readVarint();
    if (Result.isError(tagRes)) return tagRes;
    const tag = Number(tagRes.value);
    if (tag === 0) return Result.err({ message: "invalid protobuf tag" });
    return Result.ok({ field: tag >>> 3, wire: tag & 7 });
  }

  readVarint(): Result<bigint, { message: string }> {
    let shift = 0n;
    let out = 0n;
    while (shift <= 63n) {
      if (this.pos >= this.bytes.byteLength) return Result.err({ message: "truncated protobuf varint" });
      const byte = this.bytes[this.pos++]!;
      out |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return Result.ok(out);
      shift += 7n;
    }
    return Result.err({ message: "protobuf varint too long" });
  }

  readFixed32(): Result<number, { message: string }> {
    if (this.pos + 4 > this.bytes.byteLength) return Result.err({ message: "truncated protobuf fixed32" });
    const view = new DataView(this.bytes.buffer, this.bytes.byteOffset + this.pos, 4);
    this.pos += 4;
    return Result.ok(view.getUint32(0, true));
  }

  readFixed64(): Result<bigint, { message: string }> {
    if (this.pos + 8 > this.bytes.byteLength) return Result.err({ message: "truncated protobuf fixed64" });
    const view = new DataView(this.bytes.buffer, this.bytes.byteOffset + this.pos, 8);
    this.pos += 8;
    return Result.ok(view.getBigUint64(0, true));
  }

  readDouble(): Result<number, { message: string }> {
    if (this.pos + 8 > this.bytes.byteLength) return Result.err({ message: "truncated protobuf double" });
    const view = new DataView(this.bytes.buffer, this.bytes.byteOffset + this.pos, 8);
    this.pos += 8;
    return Result.ok(view.getFloat64(0, true));
  }

  readBytes(): Result<Uint8Array, { message: string }> {
    const lenRes = this.readVarint();
    if (Result.isError(lenRes)) return lenRes;
    const len = Number(lenRes.value);
    if (!Number.isSafeInteger(len) || len < 0 || this.pos + len > this.bytes.byteLength) {
      return Result.err({ message: "truncated protobuf bytes" });
    }
    const out = this.bytes.slice(this.pos, this.pos + len);
    this.pos += len;
    return Result.ok(out);
  }

  readString(): Result<string, { message: string }> {
    const bytesRes = this.readBytes();
    if (Result.isError(bytesRes)) return bytesRes;
    return Result.ok(JSON_TEXT_DECODER.decode(bytesRes.value));
  }

  skip(wire: number): Result<void, { message: string }> {
    if (wire === 0) {
      const res = this.readVarint();
      return Result.isError(res) ? res : Result.ok(undefined);
    }
    if (wire === 1) {
      const res = this.readFixed64();
      return Result.isError(res) ? res : Result.ok(undefined);
    }
    if (wire === 2) {
      const res = this.readBytes();
      return Result.isError(res) ? res : Result.ok(undefined);
    }
    if (wire === 5) {
      const res = this.readFixed32();
      return Result.isError(res) ? res : Result.ok(undefined);
    }
    return Result.err({ message: `unsupported protobuf wire type ${wire}` });
  }
}

function signedInt64(value: bigint): string {
  return value > 9_223_372_036_854_775_807n ? (value - 18_446_744_073_709_551_616n).toString() : value.toString();
}

function decodeAnyValue(bytes: Uint8Array): Result<unknown, { message: string }> {
  const reader = new ProtoReader(bytes);
  let value: unknown = null;
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    const { field, wire } = tagRes.value;
    if (field === 1 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      value = res.value;
    } else if (field === 2 && wire === 0) {
      const res = reader.readVarint();
      if (Result.isError(res)) return res;
      value = res.value !== 0n;
    } else if (field === 3 && wire === 0) {
      const res = reader.readVarint();
      if (Result.isError(res)) return res;
      value = signedInt64(res.value);
    } else if (field === 4 && wire === 1) {
      const res = reader.readDouble();
      if (Result.isError(res)) return res;
      value = res.value;
    } else if (field === 5 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const arrayRes = decodeArrayValue(bytesRes.value);
      if (Result.isError(arrayRes)) return arrayRes;
      value = arrayRes.value;
    } else if (field === 6 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const kvRes = decodeKeyValueList(bytesRes.value);
      if (Result.isError(kvRes)) return kvRes;
      value = kvRes.value;
    } else if (field === 7 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      value = Buffer.from(bytesRes.value).toString("base64");
    } else {
      const skipRes = reader.skip(wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(value);
}

function decodeArrayValue(bytes: Uint8Array): Result<unknown[], { message: string }> {
  const reader = new ProtoReader(bytes);
  const out: unknown[] = [];
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    if (tagRes.value.field === 1 && tagRes.value.wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const valueRes = decodeAnyValue(bytesRes.value);
      if (Result.isError(valueRes)) return valueRes;
      out.push(valueRes.value);
    } else {
      const skipRes = reader.skip(tagRes.value.wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(out);
}

function decodeKeyValue(bytes: Uint8Array): Result<{ key: string; value: unknown } | null, { message: string }> {
  const reader = new ProtoReader(bytes);
  let key = "";
  let value: unknown = null;
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    const { field, wire } = tagRes.value;
    if (field === 1 && wire === 2) {
      const keyRes = reader.readString();
      if (Result.isError(keyRes)) return keyRes;
      key = keyRes.value;
    } else if (field === 2 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const valueRes = decodeAnyValue(bytesRes.value);
      if (Result.isError(valueRes)) return valueRes;
      value = valueRes.value;
    } else {
      const skipRes = reader.skip(wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(key === "" ? null : { key, value });
}

function decodeKeyValueList(bytes: Uint8Array): Result<Record<string, unknown>, { message: string }> {
  const reader = new ProtoReader(bytes);
  const out: Record<string, unknown> = {};
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    if (tagRes.value.field === 1 && tagRes.value.wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const kvRes = decodeKeyValue(bytesRes.value);
      if (Result.isError(kvRes)) return kvRes;
      if (kvRes.value) out[kvRes.value.key] = kvRes.value.value;
    } else {
      const skipRes = reader.skip(tagRes.value.wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(out);
}

function decodeResource(bytes: Uint8Array): Result<Record<string, unknown>, { message: string }> {
  return decodeKeyValueList(bytes);
}

function decodeScope(bytes: Uint8Array): Result<ScopeSpansDecoded["scope"], { message: string }> {
  const reader = new ProtoReader(bytes);
  const scope: ScopeSpansDecoded["scope"] = { name: null, version: null, schemaUrl: null, attributes: {} };
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    const { field, wire } = tagRes.value;
    if (field === 1 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      scope.name = res.value;
    } else if (field === 2 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      scope.version = res.value;
    } else if (field === 3 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const attrsRes = decodeKeyValueList(bytesRes.value);
      if (Result.isError(attrsRes)) return attrsRes;
      scope.attributes = { ...scope.attributes, ...attrsRes.value };
    } else {
      const skipRes = reader.skip(wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(scope);
}

function decodeStatus(bytes: Uint8Array): Result<{ code?: number; message?: string | null }, { message: string }> {
  const reader = new ProtoReader(bytes);
  const status: { code?: number; message?: string | null } = {};
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    const { field, wire } = tagRes.value;
    if ((field === 1 || field === 3) && wire === 0) {
      const res = reader.readVarint();
      if (Result.isError(res)) return res;
      status.code = Number(res.value);
    } else if (field === 2 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      status.message = res.value;
    } else {
      const skipRes = reader.skip(wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(status);
}

function decodeEvent(bytes: Uint8Array): Result<DecodedOtelEvent, { message: string }> {
  const reader = new ProtoReader(bytes);
  const event: DecodedOtelEvent = { timeUnixNano: null, name: "", attributes: {}, droppedAttributesCount: 0 };
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    const { field, wire } = tagRes.value;
    if (field === 1 && (wire === 1 || wire === 0)) {
      const res = wire === 1 ? reader.readFixed64() : reader.readVarint();
      if (Result.isError(res)) return res;
      event.timeUnixNano = res.value.toString();
    } else if (field === 2 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      event.name = res.value;
    } else if (field === 3 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const kvRes = decodeKeyValue(bytesRes.value);
      if (Result.isError(kvRes)) return kvRes;
      if (kvRes.value) event.attributes[kvRes.value.key] = kvRes.value.value;
    } else if (field === 4 && wire === 0) {
      const res = reader.readVarint();
      if (Result.isError(res)) return res;
      event.droppedAttributesCount = Number(res.value);
    } else {
      const skipRes = reader.skip(wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(event);
}

function decodeLink(bytes: Uint8Array): Result<DecodedOtelLink, { message: string }> {
  const reader = new ProtoReader(bytes);
  const link: DecodedOtelLink = { traceId: "", spanId: "", traceState: null, attributes: {}, droppedAttributesCount: 0 };
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    const { field, wire } = tagRes.value;
    if (field === 1 && wire === 2) {
      const res = reader.readBytes();
      if (Result.isError(res)) return res;
      link.traceId = hexFromBytes(res.value);
    } else if (field === 2 && wire === 2) {
      const res = reader.readBytes();
      if (Result.isError(res)) return res;
      link.spanId = hexFromBytes(res.value);
    } else if (field === 3 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      link.traceState = res.value;
    } else if (field === 4 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const kvRes = decodeKeyValue(bytesRes.value);
      if (Result.isError(kvRes)) return kvRes;
      if (kvRes.value) link.attributes[kvRes.value.key] = kvRes.value.value;
    } else if (field === 5 && wire === 0) {
      const res = reader.readVarint();
      if (Result.isError(res)) return res;
      link.droppedAttributesCount = Number(res.value);
    } else {
      const skipRes = reader.skip(wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(link);
}

function decodeSpan(bytes: Uint8Array): Result<Omit<DecodedOtelSpan, "resourceAttributes" | "resourceSchemaUrl" | "instrumentationScope">, { message: string }> {
  const reader = new ProtoReader(bytes);
  const span: Omit<DecodedOtelSpan, "resourceAttributes" | "resourceSchemaUrl" | "instrumentationScope"> = {
    traceId: "",
    spanId: "",
    parentSpanId: null,
    traceState: null,
    traceFlags: null,
    name: "",
    kind: 0,
    startUnixNano: null,
    endUnixNano: null,
    status: { code: 0, message: null },
    attributes: {},
    events: [],
    links: [],
    droppedAttributesCount: 0,
    droppedEventsCount: 0,
    droppedLinksCount: 0,
  };
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    const { field, wire } = tagRes.value;
    if (field === 1 && wire === 2) {
      const res = reader.readBytes();
      if (Result.isError(res)) return res;
      span.traceId = hexFromBytes(res.value);
    } else if (field === 2 && wire === 2) {
      const res = reader.readBytes();
      if (Result.isError(res)) return res;
      span.spanId = hexFromBytes(res.value);
    } else if (field === 3 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      span.traceState = res.value;
    } else if (field === 4 && wire === 2) {
      const res = reader.readBytes();
      if (Result.isError(res)) return res;
      span.parentSpanId = res.value.byteLength === 0 ? null : hexFromBytes(res.value);
    } else if (field === 5 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      span.name = res.value;
    } else if (field === 6 && wire === 0) {
      const res = reader.readVarint();
      if (Result.isError(res)) return res;
      span.kind = Number(res.value);
    } else if ((field === 7 || field === 8) && (wire === 1 || wire === 0)) {
      const res = wire === 1 ? reader.readFixed64() : reader.readVarint();
      if (Result.isError(res)) return res;
      if (field === 7) span.startUnixNano = res.value.toString();
      else span.endUnixNano = res.value.toString();
    } else if (field === 9 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const kvRes = decodeKeyValue(bytesRes.value);
      if (Result.isError(kvRes)) return kvRes;
      if (kvRes.value) span.attributes[kvRes.value.key] = kvRes.value.value;
    } else if (field === 10 && wire === 0) {
      const res = reader.readVarint();
      if (Result.isError(res)) return res;
      span.droppedAttributesCount = Number(res.value);
    } else if (field === 11 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const eventRes = decodeEvent(bytesRes.value);
      if (Result.isError(eventRes)) return eventRes;
      span.events.push(eventRes.value);
    } else if (field === 12 && wire === 0) {
      const res = reader.readVarint();
      if (Result.isError(res)) return res;
      span.droppedEventsCount = Number(res.value);
    } else if (field === 13 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const linkRes = decodeLink(bytesRes.value);
      if (Result.isError(linkRes)) return linkRes;
      span.links.push(linkRes.value);
    } else if (field === 14 && wire === 0) {
      const res = reader.readVarint();
      if (Result.isError(res)) return res;
      span.droppedLinksCount = Number(res.value);
    } else if (field === 15 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const statusRes = decodeStatus(bytesRes.value);
      if (Result.isError(statusRes)) return statusRes;
      span.status = statusRes.value;
    } else if (field === 16 && (wire === 5 || wire === 0)) {
      if (wire === 5) {
        const res = reader.readFixed32();
        if (Result.isError(res)) return res;
        span.traceFlags = res.value;
      } else {
        const res = reader.readVarint();
        if (Result.isError(res)) return res;
        span.traceFlags = Number(res.value);
      }
    } else {
      const skipRes = reader.skip(wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(span);
}

function decodeScopeSpans(bytes: Uint8Array): Result<ScopeSpansDecoded, { message: string }> {
  const reader = new ProtoReader(bytes);
  const out: ScopeSpansDecoded = {
    scope: { name: null, version: null, schemaUrl: null, attributes: {} },
    spans: [],
  };
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    const { field, wire } = tagRes.value;
    if ((field === 1 || field === 1000) && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const scopeRes = decodeScope(bytesRes.value);
      if (Result.isError(scopeRes)) return scopeRes;
      out.scope = { ...out.scope, ...scopeRes.value };
    } else if (field === 2 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const spanRes = decodeSpan(bytesRes.value);
      if (Result.isError(spanRes)) return spanRes;
      out.spans.push(spanRes.value);
    } else if (field === 3 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      out.scope.schemaUrl = res.value;
    } else {
      const skipRes = reader.skip(wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(out);
}

function decodeResourceSpans(bytes: Uint8Array): Result<ResourceSpansDecoded, { message: string }> {
  const reader = new ProtoReader(bytes);
  const out: ResourceSpansDecoded = { resourceAttributes: {}, resourceSchemaUrl: null, scopeSpans: [] };
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    const { field, wire } = tagRes.value;
    if (field === 1 && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const resourceRes = decodeResource(bytesRes.value);
      if (Result.isError(resourceRes)) return resourceRes;
      out.resourceAttributes = resourceRes.value;
    } else if ((field === 2 || field === 1000) && wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const scopeRes = decodeScopeSpans(bytesRes.value);
      if (Result.isError(scopeRes)) return scopeRes;
      out.scopeSpans.push(scopeRes.value);
    } else if (field === 3 && wire === 2) {
      const res = reader.readString();
      if (Result.isError(res)) return res;
      out.resourceSchemaUrl = res.value;
    } else {
      const skipRes = reader.skip(wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(out);
}

function decodeProtobufExportResult(body: Uint8Array): Result<DecodedOtelSpan[], { message: string }> {
  const reader = new ProtoReader(body);
  const out: DecodedOtelSpan[] = [];
  while (!reader.eof()) {
    const tagRes = reader.readTag();
    if (Result.isError(tagRes)) return tagRes;
    if (tagRes.value.field === 1 && tagRes.value.wire === 2) {
      const bytesRes = reader.readBytes();
      if (Result.isError(bytesRes)) return bytesRes;
      const resourceSpansRes = decodeResourceSpans(bytesRes.value);
      if (Result.isError(resourceSpansRes)) return resourceSpansRes;
      for (const scopeSpans of resourceSpansRes.value.scopeSpans) {
        for (const span of scopeSpans.spans) {
          out.push({
            ...span,
            resourceAttributes: resourceSpansRes.value.resourceAttributes,
            resourceSchemaUrl: resourceSpansRes.value.resourceSchemaUrl,
            instrumentationScope: scopeSpans.scope,
          });
        }
      }
    } else {
      const skipRes = reader.skip(tagRes.value.wire);
      if (Result.isError(skipRes)) return skipRes;
    }
  }
  return Result.ok(out);
}

function decodeBody(args: {
  contentType: string;
  contentEncoding: string | null;
  body: Uint8Array;
  maxDecodedBytes: number;
}): Result<{ spans: DecodedOtelSpan[]; responseEncoding: "protobuf" | "json" }, OtlpTraceExportError> {
  let body = args.body;
  const encoding = args.contentEncoding?.trim().toLowerCase() ?? "";
  if (encoding !== "" && encoding !== "identity" && encoding !== "gzip") {
    return Result.err({ status: 415, message: "unsupported content-encoding" });
  }
  if (encoding === "gzip") {
    try {
      body = new Uint8Array(gunzipSync(body));
    } catch {
      return Result.err({ status: 400, message: "invalid gzip body" });
    }
  }
  if (body.byteLength > args.maxDecodedBytes) {
    return Result.err({ status: 400, message: `decoded OTLP body too large (max ${args.maxDecodedBytes})` });
  }

  const contentType = baseContentType(args.contentType);
  if (contentType === JSON_CONTENT_TYPE) {
    const spansRes = decodeJsonExportResult(body);
    if (Result.isError(spansRes)) return Result.err({ status: 400, message: spansRes.error.message });
    return Result.ok({ spans: spansRes.value, responseEncoding: "json" });
  }
  if (contentType === PROTOBUF_CONTENT_TYPE) {
    const spansRes = decodeProtobufExportResult(body);
    if (Result.isError(spansRes)) return Result.err({ status: 400, message: spansRes.error.message });
    return Result.ok({ spans: spansRes.value, responseEncoding: "protobuf" });
  }
  return Result.err({ status: 415, message: "OTLP traces require application/x-protobuf or application/json" });
}

export function decodeOtlpTraceExportRequestResult(args: {
  stream: string;
  profile: OtelTracesStreamProfile;
  contentType: string;
  contentEncoding: string | null;
  body: Uint8Array;
  maxDecodedBytes: number;
}): Result<OtlpTraceExportResult, OtlpTraceExportError> {
  const decodedRes = decodeBody(args);
  if (Result.isError(decodedRes)) return decodedRes;
  const records: OtlpTraceExportResult["records"] = [];
  const warnings: string[] = [];
  let rejectedSpans = 0;
  for (const span of decodedRes.value.spans) {
    const normalizedRes = normalizeOtelDecodedSpanResult(args.profile, span);
    if (Result.isError(normalizedRes)) {
      rejectedSpans += 1;
      if (warnings.length < 8) warnings.push(normalizedRes.error.message);
      continue;
    }
    records.push({
      value: normalizedRes.value,
      routingKey: normalizedRes.value.traceId,
    });
  }
  return Result.ok({
    records,
    acceptedSpans: records.length,
    rejectedSpans,
    warnings,
    responseEncoding: decodedRes.value.responseEncoding,
  });
}

function writeVarint(out: number[], value: bigint): void {
  let n = value;
  while (n >= 0x80n) {
    out.push(Number((n & 0x7fn) | 0x80n));
    n >>= 7n;
  }
  out.push(Number(n));
}

function writeTag(out: number[], field: number, wire: number): void {
  writeVarint(out, BigInt((field << 3) | wire));
}

function writeString(out: number[], field: number, value: string): void {
  const bytes = new TextEncoder().encode(value);
  writeTag(out, field, 2);
  writeVarint(out, BigInt(bytes.byteLength));
  out.push(...bytes);
}

function writeInt64(out: number[], field: number, value: bigint): void {
  writeTag(out, field, 0);
  writeVarint(out, value);
}

function writeMessage(out: number[], field: number, body: number[]): void {
  writeTag(out, field, 2);
  writeVarint(out, BigInt(body.length));
  out.push(...body);
}

export function encodeOtlpTraceExportResponse(result: Pick<OtlpTraceExportResult, "rejectedSpans" | "warnings" | "responseEncoding">): {
  contentType: string;
  body: Uint8Array | string;
} {
  const message =
    result.rejectedSpans > 0
      ? `${result.rejectedSpans} spans rejected${result.warnings.length > 0 ? `: ${result.warnings.join("; ")}` : ""}`
      : "";
  if (result.responseEncoding === "json") {
    if (result.rejectedSpans === 0) return { contentType: "application/json; charset=utf-8", body: "{}" };
    return {
      contentType: "application/json; charset=utf-8",
      body: JSON.stringify({
        partialSuccess: {
          rejectedSpans: result.rejectedSpans,
          errorMessage: message,
        },
      }),
    };
  }
  if (result.rejectedSpans === 0) return { contentType: PROTOBUF_CONTENT_TYPE, body: new Uint8Array() };
  const partial: number[] = [];
  writeInt64(partial, 1, BigInt(result.rejectedSpans));
  writeString(partial, 2, message);
  const response: number[] = [];
  writeMessage(response, 1, partial);
  return { contentType: PROTOBUF_CONTENT_TYPE, body: new Uint8Array(response) };
}
