import { createHash } from "node:crypto";
import { Result } from "better-result";
import type { PreparedJsonRecord } from "../profile";
import { expectPlainObjectResult, isPlainObject } from "../profile";

export type OTelSpanKind = "unspecified" | "internal" | "server" | "client" | "producer" | "consumer";
export type OTelStatusCode = "unset" | "ok" | "error";
export type DbStatementMode = "drop" | "raw";

export type OtelTraceAttributeLimits = {
  maxAttributeValueBytes: number;
  maxAttributesPerSpan: number;
  maxEventsPerSpan: number;
  maxLinksPerSpan: number;
  maxStatementBytes: number;
};

export type OtelTraceStoreConfig = {
  rawResourceAttributes: boolean;
  rawSpanAttributes: boolean;
  rawEvents: boolean;
  rawLinks: boolean;
};

export type OtelTracesStreamProfile = {
  kind: "otel-traces";
  redactKeys?: string[];
  requestIdAttributes?: string[];
  attributeLimits?: Partial<OtelTraceAttributeLimits>;
  store?: Partial<OtelTraceStoreConfig>;
  dbStatementMode?: DbStatementMode;
  observability?: {
    request?: {
      eventsStream: string;
    };
  };
};

export type DecodedOtelEvent = {
  timeUnixNano: string | null;
  name: string;
  attributes: Record<string, unknown>;
  droppedAttributesCount?: number;
};

export type DecodedOtelLink = {
  traceId: string;
  spanId: string;
  traceState: string | null;
  attributes: Record<string, unknown>;
  droppedAttributesCount?: number;
};

export type DecodedOtelSpan = {
  traceId: string;
  spanId: string;
  parentSpanId?: string | null;
  traceState?: string | null;
  traceFlags?: number | null;
  name: string;
  kind?: number | string | null;
  startUnixNano?: string | null;
  endUnixNano?: string | null;
  timestamp?: string | null;
  status?: {
    code?: number | string | null;
    message?: string | null;
  };
  resourceSchemaUrl?: string | null;
  resourceAttributes: Record<string, unknown>;
  instrumentationScope?: {
    name?: string | null;
    version?: string | null;
    schemaUrl?: string | null;
    attributes?: Record<string, unknown>;
  };
  attributes: Record<string, unknown>;
  events: DecodedOtelEvent[];
  links: DecodedOtelLink[];
  droppedAttributesCount?: number;
  droppedEventsCount?: number;
  droppedLinksCount?: number;
  requestId?: string | null;
};

export type CanonicalOtelSpan = {
  schemaVersion: 1;
  signal: "trace.span";
  timestamp: string;
  endTimestamp: string | null;
  startUnixNano: string | null;
  endUnixNano: string | null;
  duration: number | null;
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  traceState: string | null;
  traceFlags: {
    sampled: boolean;
    raw: number | null;
  };
  name: string;
  kind: OTelSpanKind;
  status: {
    code: OTelStatusCode;
    message: string | null;
  };
  service: string | null;
  serviceNamespace: string | null;
  serviceInstanceId: string | null;
  environment: string | null;
  version: string | null;
  region: string | null;
  requestId: string | null;
  http: {
    method: string | null;
    route: string | null;
    path: string | null;
    target: string | null;
    url: string | null;
    statusCode: number | null;
    userAgent: string | null;
  };
  db: {
    system: string | null;
    name: string | null;
    operation: string | null;
    statement: string | null;
  };
  rpc: {
    system: string | null;
    service: string | null;
    method: string | null;
  };
  messaging: {
    system: string | null;
    destination: string | null;
    operation: string | null;
  };
  error: {
    isError: boolean;
    type: string | null;
    message: string | null;
    stacktrace: string | null;
  };
  instrumentationScope: {
    name: string | null;
    version: string | null;
    schemaUrl: string | null;
    attributes: Record<string, unknown>;
  };
  resource: {
    schemaUrl: string | null;
    attributes: Record<string, unknown>;
  };
  attributes: Record<string, unknown>;
  events: Array<{
    timestamp: string | null;
    timeUnixNano: string | null;
    name: string;
    attributes: Record<string, unknown>;
    droppedAttributesCount?: number;
  }>;
  eventNames: string[];
  links: Array<{
    traceId: string;
    spanId: string;
    traceState: string | null;
    attributes: Record<string, unknown>;
    droppedAttributesCount?: number;
  }>;
  dropped: {
    attributes: number;
    events: number;
    links: number;
  };
  redaction: {
    keys: string[];
  };
  identity: {
    spanKey: string;
    dedupeKey: string;
  };
};

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();
const REDACTED_VALUE = "[REDACTED]";

export const DEFAULT_OTEL_TRACE_REDACT_KEYS = [
  "password",
  "token",
  "secret",
  "authorization",
  "cookie",
  "apikey",
  "api_key",
  "set-cookie",
  "x-api-key",
] as const;

export const DEFAULT_REQUEST_ID_ATTRIBUTES = [
  "request.id",
  "http.request_id",
  "http.request.header.x_request_id",
  "http.request.header.x-request-id",
  "http.request.header.x_correlation_id",
  "http.request.header.x-correlation-id",
  "correlation.id",
] as const;

export const DEFAULT_ATTRIBUTE_LIMITS: OtelTraceAttributeLimits = {
  maxAttributeValueBytes: 8192,
  maxAttributesPerSpan: 256,
  maxEventsPerSpan: 128,
  maxLinksPerSpan: 128,
  maxStatementBytes: 4096,
};

export const DEFAULT_STORE_CONFIG: OtelTraceStoreConfig = {
  rawResourceAttributes: true,
  rawSpanAttributes: true,
  rawEvents: true,
  rawLinks: true,
};

function normalizeString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

function normalizeNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function normalizeInteger(value: unknown): number | null {
  const n = normalizeNumber(value);
  return n != null && Number.isInteger(n) ? n : null;
}

function normalizeNanoString(value: unknown): string | null {
  if (value == null) return null;
  if (typeof value === "bigint") return value >= 0n ? value.toString() : null;
  if (typeof value === "number" && Number.isFinite(value) && Number.isInteger(value) && value >= 0) {
    return BigInt(value).toString();
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^(0|[1-9][0-9]*)$/.test(trimmed)) return trimmed;
  }
  return null;
}

function isoFromUnixNano(nanoString: string | null): string | null {
  if (!nanoString) return null;
  try {
    const ms = BigInt(nanoString) / 1_000_000n;
    const date = new Date(Number(ms));
    if (Number.isNaN(date.getTime())) return null;
    return date.toISOString();
  } catch {
    return null;
  }
}

function durationMs(startUnixNano: string | null, endUnixNano: string | null): Result<number | null, { message: string }> {
  if (!startUnixNano || !endUnixNano) return Result.ok(null);
  const start = BigInt(startUnixNano);
  const end = BigInt(endUnixNano);
  if (end < start) return Result.err({ message: "endTimeUnixNano must be greater than or equal to startTimeUnixNano" });
  return Result.ok(Number(end - start) / 1_000_000);
}

function normalizeHexIdResult(raw: unknown, chars: number, field: string): Result<string, { message: string }> {
  const value = normalizeString(raw)?.toLowerCase() ?? "";
  if (!new RegExp(`^[0-9a-f]{${chars}}$`).test(value)) {
    return Result.err({ message: `${field} must be ${chars} lowercase hex characters` });
  }
  if (/^0+$/.test(value)) return Result.err({ message: `${field} must not be all zeroes` });
  return Result.ok(value);
}

function normalizeParentSpanIdResult(raw: unknown): Result<string | null, { message: string }> {
  const value = normalizeString(raw);
  if (!value) return Result.ok(null);
  const lowered = value.toLowerCase();
  if (/^0+$/.test(lowered)) return Result.ok(null);
  return normalizeHexIdResult(lowered, 16, "parentSpanId");
}

function normalizeSpanKind(value: unknown): OTelSpanKind {
  if (typeof value === "number") {
    if (value === 1) return "internal";
    if (value === 2) return "server";
    if (value === 3) return "client";
    if (value === 4) return "producer";
    if (value === 5) return "consumer";
    return "unspecified";
  }
  const raw = normalizeString(value)?.toLowerCase().replace(/^span_kind_/, "");
  if (raw === "internal" || raw === "server" || raw === "client" || raw === "producer" || raw === "consumer") return raw;
  return "unspecified";
}

function normalizeStatusCode(value: unknown): OTelStatusCode {
  if (typeof value === "number") {
    if (value === 1) return "ok";
    if (value === 2) return "error";
    return "unset";
  }
  const raw = normalizeString(value)?.toLowerCase().replace(/^status_code_/, "");
  if (raw === "ok" || raw === "error") return raw;
  return "unset";
}

function truncateUtf8(value: string, maxBytes: number): string {
  const bytes = TEXT_ENCODER.encode(value);
  if (bytes.byteLength <= maxBytes) return value;
  return TEXT_DECODER.decode(bytes.slice(0, Math.max(0, maxBytes)));
}

function sanitizeAttributeValue(value: unknown, redactKeys: Set<string>, path: string, maxBytes: number): { value: unknown; redacted: string[] } {
  if (typeof value === "string") return { value: truncateUtf8(value, maxBytes), redacted: [] };
  if (typeof value === "number") return { value: Number.isFinite(value) ? value : null, redacted: [] };
  if (typeof value === "boolean" || value === null) return { value, redacted: [] };
  if (typeof value === "bigint") return { value: value.toString(), redacted: [] };
  if (value instanceof Uint8Array) return { value: Buffer.from(value).toString("base64"), redacted: [] };
  if (Array.isArray(value)) {
    const out: unknown[] = [];
    const redacted: string[] = [];
    for (let i = 0; i < value.length; i++) {
      const child = sanitizeAttributeValue(value[i], redactKeys, `${path}.${i}`, maxBytes);
      out.push(child.value);
      redacted.push(...child.redacted);
    }
    return { value: out, redacted };
  }
  if (!isPlainObject(value)) return { value: null, redacted: [] };
  const out: Record<string, unknown> = {};
  const redacted: string[] = [];
  for (const [key, childValue] of Object.entries(value)) {
    const childPath = path === "" ? key : `${path}.${key}`;
    if (redactKeys.has(key.toLowerCase())) {
      out[key] = REDACTED_VALUE;
      redacted.push(childPath);
      continue;
    }
    const child = sanitizeAttributeValue(childValue, redactKeys, childPath, maxBytes);
    out[key] = child.value;
    redacted.push(...child.redacted);
  }
  return { value: out, redacted };
}

function limitAttributes(
  attrs: Record<string, unknown>,
  args: {
    maxAttributes: number;
    maxAttributeValueBytes: number;
    dropped: number;
    redactKeys: Set<string>;
    path: string;
  }
): { attributes: Record<string, unknown>; dropped: number; redacted: string[] } {
  const out: Record<string, unknown> = {};
  const redacted: string[] = [];
  let count = 0;
  let dropped = Math.max(0, Math.trunc(args.dropped));
  for (const [key, value] of Object.entries(attrs)) {
    if (count >= args.maxAttributes) {
      dropped += 1;
      continue;
    }
    count += 1;
    const keyPath = args.path === "" ? key : `${args.path}.${key}`;
    if (args.redactKeys.has(key.toLowerCase())) {
      out[key] = REDACTED_VALUE;
      redacted.push(keyPath);
      continue;
    }
    const sanitized = sanitizeAttributeValue(value, args.redactKeys, keyPath, args.maxAttributeValueBytes);
    out[key] = sanitized.value;
    redacted.push(...sanitized.redacted);
  }
  return { attributes: out, dropped, redacted };
}

function getString(attrs: Record<string, unknown>, ...keys: string[]): string | null {
  for (const key of keys) {
    const value = normalizeString(attrs[key]);
    if (value) return value;
  }
  return null;
}

function getInteger(attrs: Record<string, unknown>, ...keys: string[]): number | null {
  for (const key of keys) {
    const value = normalizeInteger(attrs[key]);
    if (value != null) return value;
  }
  return null;
}

function getRequestId(attrs: Record<string, unknown>, direct: string | null, requestIdAttributes: readonly string[]): string | null {
  if (direct) return direct;
  for (const key of requestIdAttributes) {
    const value = normalizeString(attrs[key]);
    if (value) return value;
  }
  return null;
}

function extractExceptionFromEvents(events: DecodedOtelEvent[]): { type: string | null; message: string | null; stacktrace: string | null } {
  for (const event of events) {
    if (event.name !== "exception") continue;
    return {
      type: getString(event.attributes, "exception.type"),
      message: getString(event.attributes, "exception.message"),
      stacktrace: getString(event.attributes, "exception.stacktrace"),
    };
  }
  return { type: null, message: null, stacktrace: null };
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function normalizeOtelDecodedSpanResult(
  profile: OtelTracesStreamProfile,
  input: DecodedOtelSpan
): Result<CanonicalOtelSpan, { message: string }> {
  const traceIdRes = normalizeHexIdResult(input.traceId, 32, "traceId");
  if (Result.isError(traceIdRes)) return traceIdRes;
  const spanIdRes = normalizeHexIdResult(input.spanId, 16, "spanId");
  if (Result.isError(spanIdRes)) return spanIdRes;
  const parentSpanIdRes = normalizeParentSpanIdResult(input.parentSpanId);
  if (Result.isError(parentSpanIdRes)) return parentSpanIdRes;

  const limits = { ...DEFAULT_ATTRIBUTE_LIMITS, ...(profile.attributeLimits ?? {}) };
  const store = { ...DEFAULT_STORE_CONFIG, ...(profile.store ?? {}) };
  const redactKeys = new Set([...DEFAULT_OTEL_TRACE_REDACT_KEYS, ...(profile.redactKeys ?? [])].map((key) => key.toLowerCase()));
  const requestIdAttributes = profile.requestIdAttributes ?? [...DEFAULT_REQUEST_ID_ATTRIBUTES];

  const resourceRes = limitAttributes(input.resourceAttributes, {
    maxAttributes: limits.maxAttributesPerSpan,
    maxAttributeValueBytes: limits.maxAttributeValueBytes,
    dropped: 0,
    redactKeys,
    path: "resource.attributes",
  });
  const scopeRes = limitAttributes(input.instrumentationScope?.attributes ?? {}, {
    maxAttributes: limits.maxAttributesPerSpan,
    maxAttributeValueBytes: limits.maxAttributeValueBytes,
    dropped: 0,
    redactKeys,
    path: "instrumentationScope.attributes",
  });
  const attrsRes = limitAttributes(input.attributes, {
    maxAttributes: limits.maxAttributesPerSpan,
    maxAttributeValueBytes: limits.maxAttributeValueBytes,
    dropped: input.droppedAttributesCount ?? 0,
    redactKeys,
    path: "attributes",
  });

  const startUnixNano = normalizeNanoString(input.startUnixNano);
  const endUnixNano = normalizeNanoString(input.endUnixNano);
  const durationRes = durationMs(startUnixNano, endUnixNano);
  if (Result.isError(durationRes)) return durationRes;
  const timestamp = isoFromUnixNano(startUnixNano) ?? normalizeString(input.timestamp) ?? new Date().toISOString();
  const endTimestamp = isoFromUnixNano(endUnixNano);

  const normalizedEvents: CanonicalOtelSpan["events"] = [];
  let droppedEvents = Math.max(0, Math.trunc(input.droppedEventsCount ?? 0));
  const eventNames: string[] = [];
  for (const event of input.events) {
    if (normalizedEvents.length >= limits.maxEventsPerSpan) {
      droppedEvents += 1;
      continue;
    }
    const eventAttrs = limitAttributes(event.attributes, {
      maxAttributes: limits.maxAttributesPerSpan,
      maxAttributeValueBytes: limits.maxAttributeValueBytes,
      dropped: event.droppedAttributesCount ?? 0,
      redactKeys,
      path: `events.${normalizedEvents.length}.attributes`,
    });
    const eventName = normalizeString(event.name) ?? "";
    eventNames.push(eventName);
    normalizedEvents.push({
      timestamp: isoFromUnixNano(normalizeNanoString(event.timeUnixNano)),
      timeUnixNano: normalizeNanoString(event.timeUnixNano),
      name: eventName,
      attributes: store.rawEvents ? eventAttrs.attributes : {},
      droppedAttributesCount: eventAttrs.dropped,
    });
    resourceRes.redacted.push(...eventAttrs.redacted);
  }

  const normalizedLinks: CanonicalOtelSpan["links"] = [];
  let droppedLinks = Math.max(0, Math.trunc(input.droppedLinksCount ?? 0));
  for (const link of input.links) {
    if (normalizedLinks.length >= limits.maxLinksPerSpan) {
      droppedLinks += 1;
      continue;
    }
    const linkTraceIdRes = normalizeHexIdResult(link.traceId, 32, "links.traceId");
    if (Result.isError(linkTraceIdRes)) {
      droppedLinks += 1;
      continue;
    }
    const linkSpanIdRes = normalizeHexIdResult(link.spanId, 16, "links.spanId");
    if (Result.isError(linkSpanIdRes)) {
      droppedLinks += 1;
      continue;
    }
    const linkAttrs = limitAttributes(link.attributes, {
      maxAttributes: limits.maxAttributesPerSpan,
      maxAttributeValueBytes: limits.maxAttributeValueBytes,
      dropped: link.droppedAttributesCount ?? 0,
      redactKeys,
      path: `links.${normalizedLinks.length}.attributes`,
    });
    normalizedLinks.push({
      traceId: linkTraceIdRes.value,
      spanId: linkSpanIdRes.value,
      traceState: normalizeString(link.traceState),
      attributes: store.rawLinks ? linkAttrs.attributes : {},
      droppedAttributesCount: linkAttrs.dropped,
    });
    resourceRes.redacted.push(...linkAttrs.redacted);
  }

  const resourceAttrs = resourceRes.attributes;
  const spanAttrs = attrsRes.attributes;
  const service = getString(resourceAttrs, "service.name");
  const statusCode = normalizeStatusCode(input.status?.code);
  const exception = extractExceptionFromEvents(normalizedEvents);
  const attrErrorType = getString(spanAttrs, "exception.type", "error.type");
  const attrErrorMessage = getString(spanAttrs, "exception.message", "error.message");
  const attrErrorStack = getString(spanAttrs, "exception.stacktrace", "error.stacktrace");
  const httpStatusCode = getInteger(spanAttrs, "http.response.status_code", "http.status_code");
  const errorMessage = attrErrorMessage ?? exception.message ?? normalizeString(input.status?.message);
  const traceFlagsRaw = normalizeInteger(input.traceFlags);
  const dbStatementRaw = getString(spanAttrs, "db.statement", "db.query.text");
  const dbStatement =
    profile.dbStatementMode === "raw" && dbStatementRaw
      ? truncateUtf8(dbStatementRaw, limits.maxStatementBytes)
      : null;

  const canonical: CanonicalOtelSpan = {
    schemaVersion: 1,
    signal: "trace.span",
    timestamp,
    endTimestamp,
    startUnixNano,
    endUnixNano,
    duration: durationRes.value,
    traceId: traceIdRes.value,
    spanId: spanIdRes.value,
    parentSpanId: parentSpanIdRes.value,
    traceState: normalizeString(input.traceState),
    traceFlags: {
      sampled: traceFlagsRaw == null ? false : (traceFlagsRaw & 1) === 1,
      raw: traceFlagsRaw,
    },
    name: normalizeString(input.name) ?? "",
    kind: normalizeSpanKind(input.kind),
    status: {
      code: statusCode,
      message: normalizeString(input.status?.message),
    },
    service,
    serviceNamespace: getString(resourceAttrs, "service.namespace"),
    serviceInstanceId: getString(resourceAttrs, "service.instance.id"),
    environment: getString(resourceAttrs, "deployment.environment.name", "deployment.environment"),
    version: getString(resourceAttrs, "service.version"),
    region: getString(resourceAttrs, "cloud.region"),
    requestId: getRequestId(spanAttrs, normalizeString(input.requestId), requestIdAttributes),
    http: {
      method: getString(spanAttrs, "http.request.method", "http.method"),
      route: getString(spanAttrs, "http.route"),
      path: getString(spanAttrs, "url.path", "http.target"),
      target: getString(spanAttrs, "http.target"),
      url: getString(spanAttrs, "url.full", "http.url"),
      statusCode: httpStatusCode,
      userAgent: getString(spanAttrs, "user_agent.original", "http.user_agent"),
    },
    db: {
      system: getString(spanAttrs, "db.system"),
      name: getString(spanAttrs, "db.name", "db.namespace"),
      operation: getString(spanAttrs, "db.operation", "db.operation.name"),
      statement: dbStatement,
    },
    rpc: {
      system: getString(spanAttrs, "rpc.system"),
      service: getString(spanAttrs, "rpc.service"),
      method: getString(spanAttrs, "rpc.method"),
    },
    messaging: {
      system: getString(spanAttrs, "messaging.system"),
      destination: getString(spanAttrs, "messaging.destination", "messaging.destination.name"),
      operation: getString(spanAttrs, "messaging.operation", "messaging.operation.name"),
    },
    error: {
      isError: statusCode === "error" || (httpStatusCode != null && httpStatusCode >= 500) || !!attrErrorType || !!exception.type,
      type: attrErrorType ?? exception.type,
      message: errorMessage,
      stacktrace: attrErrorStack ?? exception.stacktrace,
    },
    instrumentationScope: {
      name: normalizeString(input.instrumentationScope?.name),
      version: normalizeString(input.instrumentationScope?.version),
      schemaUrl: normalizeString(input.instrumentationScope?.schemaUrl),
      attributes: scopeRes.attributes,
    },
    resource: {
      schemaUrl: normalizeString(input.resourceSchemaUrl),
      attributes: store.rawResourceAttributes ? resourceAttrs : {},
    },
    attributes: store.rawSpanAttributes ? spanAttrs : {},
    events: store.rawEvents ? normalizedEvents : [],
    eventNames,
    links: store.rawLinks ? normalizedLinks : [],
    dropped: {
      attributes: attrsRes.dropped,
      events: droppedEvents,
      links: droppedLinks,
    },
    redaction: {
      keys: [...resourceRes.redacted, ...scopeRes.redacted, ...attrsRes.redacted].sort(),
    },
    identity: {
      spanKey: `${traceIdRes.value}:${spanIdRes.value}`,
      dedupeKey: sha256Hex(`${traceIdRes.value}\0${spanIdRes.value}\0${startUnixNano ?? ""}\0${service ?? ""}\0${normalizeString(input.name) ?? ""}`),
    },
  };

  return Result.ok(canonical);
}

function objectFromUnknown(value: unknown): Record<string, unknown> {
  return isPlainObject(value) ? structuredClone(value) : {};
}

function eventFromCanonical(value: unknown): DecodedOtelEvent | null {
  if (!isPlainObject(value)) return null;
  return {
    timeUnixNano: normalizeNanoString(value.timeUnixNano),
    name: normalizeString(value.name) ?? "",
    attributes: objectFromUnknown(value.attributes),
    droppedAttributesCount: normalizeInteger(value.droppedAttributesCount) ?? 0,
  };
}

function linkFromCanonical(value: unknown): DecodedOtelLink | null {
  if (!isPlainObject(value)) return null;
  const traceId = normalizeString(value.traceId);
  const spanId = normalizeString(value.spanId);
  if (!traceId || !spanId) return null;
  return {
    traceId,
    spanId,
    traceState: normalizeString(value.traceState),
    attributes: objectFromUnknown(value.attributes),
    droppedAttributesCount: normalizeInteger(value.droppedAttributesCount) ?? 0,
  };
}

function decodedSpanFromCanonicalLikeResult(value: unknown): Result<DecodedOtelSpan, { message: string }> {
  const objRes = expectPlainObjectResult(value, "otel-traces record");
  if (Result.isError(objRes)) return objRes;
  const obj = objRes.value;
  const traceId = normalizeString(obj.traceId);
  const spanId = normalizeString(obj.spanId);
  if (!traceId) return Result.err({ message: "traceId is required" });
  if (!spanId) return Result.err({ message: "spanId is required" });
  const resource = isPlainObject(obj.resource) ? obj.resource : {};
  const scope = isPlainObject(obj.instrumentationScope) ? obj.instrumentationScope : {};
  const status = isPlainObject(obj.status) ? obj.status : {};
  const traceFlags = isPlainObject(obj.traceFlags) ? obj.traceFlags : {};
  return Result.ok({
    traceId,
    spanId,
    parentSpanId: normalizeString(obj.parentSpanId),
    traceState: normalizeString(obj.traceState),
    traceFlags: normalizeInteger(traceFlags.raw),
    name: normalizeString(obj.name) ?? "",
    kind: obj.kind as number | string | null | undefined,
    startUnixNano: normalizeNanoString(obj.startUnixNano),
    endUnixNano: normalizeNanoString(obj.endUnixNano),
    timestamp: normalizeString(obj.timestamp),
    status: {
      code: status.code as number | string | null | undefined,
      message: normalizeString(status.message),
    },
    resourceSchemaUrl: normalizeString(resource.schemaUrl),
    resourceAttributes: objectFromUnknown(resource.attributes),
    instrumentationScope: {
      name: normalizeString(scope.name),
      version: normalizeString(scope.version),
      schemaUrl: normalizeString(scope.schemaUrl),
      attributes: objectFromUnknown(scope.attributes),
    },
    attributes: objectFromUnknown(obj.attributes),
    events: Array.isArray(obj.events) ? obj.events.map(eventFromCanonical).filter((event): event is DecodedOtelEvent => !!event) : [],
    links: Array.isArray(obj.links) ? obj.links.map(linkFromCanonical).filter((link): link is DecodedOtelLink => !!link) : [],
    droppedAttributesCount: isPlainObject(obj.dropped) ? (normalizeInteger(obj.dropped.attributes) ?? 0) : 0,
    droppedEventsCount: isPlainObject(obj.dropped) ? (normalizeInteger(obj.dropped.events) ?? 0) : 0,
    droppedLinksCount: isPlainObject(obj.dropped) ? (normalizeInteger(obj.dropped.links) ?? 0) : 0,
    requestId: normalizeString(obj.requestId),
  });
}

export function normalizeOtelTraceRecordResult(
  profile: OtelTracesStreamProfile,
  value: unknown
): Result<PreparedJsonRecord, { message: string }> {
  const decodedRes = decodedSpanFromCanonicalLikeResult(value);
  if (Result.isError(decodedRes)) return decodedRes;
  const normalizedRes = normalizeOtelDecodedSpanResult(profile, decodedRes.value);
  if (Result.isError(normalizedRes)) return normalizedRes;
  return Result.ok({
    value: normalizedRes.value,
    routingKey: normalizedRes.value.traceId,
  });
}
