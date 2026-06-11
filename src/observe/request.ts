import { Result } from "better-result";
import type { SearchHit, SearchResultBatch } from "../reader";
import type { UnifiedTimelineItem } from "../profiles";

export type ObserveRequestInput = {
  streams: {
    events?: string;
    traces?: string;
  };
  lookup: {
    requestId: string | null;
    traceId: string | null;
    spanId: string | null;
  };
  time: {
    from: string | null;
    to: string | null;
    paddingMs: number;
  };
  include: {
    events: boolean;
    trace: boolean;
    timeline: boolean;
    raw: boolean;
  };
  limits: {
    events: number;
    spans: number;
  };
};

export type ObserveSearchCoverage = {
  searched: boolean;
  complete: boolean;
  timed_out: boolean;
  limit_reached: boolean;
  hits: number;
  total: { value: number; relation: "eq" | "gte" };
  index_families_used: string[];
  scanned_tail_docs: number;
  scanned_segments: number;
  possible_missing_events_upper_bound: number;
};

export type TraceTreeNode = {
  spanId: string;
  parentSpanId: string | null;
  children: TraceTreeNode[];
  depth: number;
  service: string | null;
  name: string;
  kind: string;
  startTime: string;
  endTime: string | null;
  duration: number | null;
  statusCode: "unset" | "ok" | "error";
};

export type ServiceEdge = {
  from: string;
  to: string;
  count: number;
  errorCount: number;
  latency: {
    count: number;
    sum: number;
    min: number | null;
    max: number | null;
  };
};

export type TraceError = {
  spanId: string;
  service: string | null;
  name: string;
  time: string | null;
  type: string | null;
  message: string | null;
};

export type TraceDetails = {
  traceId: string | null;
  rootSpanId: string | null;
  spans: Record<string, unknown>[];
  tree: TraceTreeNode[];
  serviceMap: ServiceEdge[];
  criticalPath: string[];
  errors: TraceError[];
  partial: boolean;
  missingParents: string[];
  duplicateSpans: number;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function stringField(record: Record<string, unknown>, field: string): string | null {
  const value = record[field];
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

function numberField(record: Record<string, unknown>, field: string): number | null {
  const value = record[field];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function nestedObject(record: Record<string, unknown>, field: string): Record<string, unknown> {
  const value = record[field];
  return isPlainObject(value) ? value : {};
}

function parseOptionalString(raw: unknown, path: string): Result<string | null, { message: string }> {
  if (raw === undefined || raw === null) return Result.ok(null);
  if (typeof raw !== "string") return Result.err({ message: `${path} must be a string` });
  const trimmed = raw.trim();
  return Result.ok(trimmed === "" ? null : trimmed);
}

function parseBoolean(raw: unknown, fallback: boolean, path: string): Result<boolean, { message: string }> {
  if (raw === undefined) return Result.ok(fallback);
  if (typeof raw !== "boolean") return Result.err({ message: `${path} must be boolean` });
  return Result.ok(raw);
}

function parseLimit(raw: unknown, fallback: number, max: number, path: string): Result<number, { message: string }> {
  if (raw === undefined) return Result.ok(fallback);
  if (typeof raw !== "number" || !Number.isFinite(raw) || !Number.isInteger(raw) || raw <= 0 || raw > max) {
    return Result.err({ message: `${path} must be an integer between 1 and ${max}` });
  }
  return Result.ok(raw);
}

function parseTime(raw: unknown): Result<ObserveRequestInput["time"], { message: string }> {
  if (raw === undefined) return Result.ok({ from: null, to: null, paddingMs: 0 });
  if (!isPlainObject(raw)) return Result.err({ message: "time must be an object" });
  const fromRes = parseOptionalString(raw.from, "time.from");
  if (Result.isError(fromRes)) return fromRes;
  const toRes = parseOptionalString(raw.to, "time.to");
  if (Result.isError(toRes)) return toRes;
  for (const [path, value] of [
    ["time.from", fromRes.value],
    ["time.to", toRes.value],
  ] as const) {
    if (value != null && Number.isNaN(Date.parse(value))) return Result.err({ message: `${path} must be an ISO timestamp` });
  }
  const paddingRaw = raw.paddingMs ?? raw.padding_ms;
  if (paddingRaw === undefined) return Result.ok({ from: fromRes.value, to: toRes.value, paddingMs: 0 });
  if (typeof paddingRaw !== "number" || !Number.isFinite(paddingRaw) || paddingRaw < 0 || paddingRaw > 86_400_000) {
    return Result.err({ message: "time.paddingMs must be a non-negative number no greater than 86400000" });
  }
  return Result.ok({ from: fromRes.value, to: toRes.value, paddingMs: Math.trunc(paddingRaw) });
}

export function parseObserveRequestResult(raw: unknown): Result<ObserveRequestInput, { message: string }> {
  if (!isPlainObject(raw)) return Result.err({ message: "observe request must be an object" });
  const streamsRaw = raw.streams;
  if (!isPlainObject(streamsRaw)) return Result.err({ message: "streams must be an object" });
  const eventsStreamRes = parseOptionalString(streamsRaw.events, "streams.events");
  if (Result.isError(eventsStreamRes)) return eventsStreamRes;
  const tracesStreamRes = parseOptionalString(streamsRaw.traces, "streams.traces");
  if (Result.isError(tracesStreamRes)) return tracesStreamRes;

  const lookupRaw = raw.lookup;
  if (!isPlainObject(lookupRaw)) return Result.err({ message: "lookup must be an object" });
  const requestIdRes = parseOptionalString(lookupRaw.requestId, "lookup.requestId");
  if (Result.isError(requestIdRes)) return requestIdRes;
  const traceIdRes = parseOptionalString(lookupRaw.traceId, "lookup.traceId");
  if (Result.isError(traceIdRes)) return traceIdRes;
  const spanIdRes = parseOptionalString(lookupRaw.spanId, "lookup.spanId");
  if (Result.isError(spanIdRes)) return spanIdRes;
  const lookupCount = [requestIdRes.value, traceIdRes.value, spanIdRes.value].filter((value) => value != null).length;
  if (lookupCount !== 1) return Result.err({ message: "lookup must include exactly one of requestId, traceId, or spanId" });

  const includeRaw = isPlainObject(raw.include) ? raw.include : {};
  const includeEventsRes = parseBoolean(includeRaw.events, true, "include.events");
  if (Result.isError(includeEventsRes)) return includeEventsRes;
  const includeTraceRes = parseBoolean(includeRaw.trace, true, "include.trace");
  if (Result.isError(includeTraceRes)) return includeTraceRes;
  const includeTimelineRes = parseBoolean(includeRaw.timeline, true, "include.timeline");
  if (Result.isError(includeTimelineRes)) return includeTimelineRes;
  const includeRawRes = parseBoolean(includeRaw.raw, false, "include.raw");
  if (Result.isError(includeRawRes)) return includeRawRes;
  if (includeEventsRes.value && !eventsStreamRes.value) return Result.err({ message: "streams.events is required when include.events is true" });
  if (includeTraceRes.value && !tracesStreamRes.value) return Result.err({ message: "streams.traces is required when include.trace is true" });

  const limitsRaw = isPlainObject(raw.limits) ? raw.limits : {};
  const eventLimitRes = parseLimit(limitsRaw.events, 100, 500, "limits.events");
  if (Result.isError(eventLimitRes)) return eventLimitRes;
  const spanLimitRes = parseLimit(limitsRaw.spans, 5000, 10_000, "limits.spans");
  if (Result.isError(spanLimitRes)) return spanLimitRes;
  const timeRes = parseTime(raw.time);
  if (Result.isError(timeRes)) return timeRes;

  return Result.ok({
    streams: {
      events: eventsStreamRes.value ?? undefined,
      traces: tracesStreamRes.value ?? undefined,
    },
    lookup: {
      requestId: requestIdRes.value,
      traceId: traceIdRes.value,
      spanId: spanIdRes.value,
    },
    time: timeRes.value,
    include: {
      events: includeEventsRes.value,
      trace: includeTraceRes.value,
      timeline: includeTimelineRes.value,
      raw: includeRawRes.value,
    },
    limits: {
      events: eventLimitRes.value,
      spans: spanLimitRes.value,
    },
  });
}

export function quoteSearchValue(value: string): string {
  return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

export function buildTimeSearchClauses(time: ObserveRequestInput["time"]): string[] {
  const out: string[] = [];
  if (time.from) {
    const from = new Date(Date.parse(time.from) - time.paddingMs).toISOString();
    out.push(`timestamp:>=${quoteSearchValue(from)}`);
  }
  if (time.to) {
    const to = new Date(Date.parse(time.to) + time.paddingMs).toISOString();
    out.push(`timestamp:<=${quoteSearchValue(to)}`);
  }
  return out;
}

export function combineSearchClauses(...clauses: Array<string | null | undefined>): string {
  return clauses.filter((clause): clause is string => !!clause && clause.trim() !== "").join(" ");
}

function statusCode(record: Record<string, unknown>): "unset" | "ok" | "error" {
  const status = nestedObject(record, "status");
  const code = stringField(status, "code");
  return code === "ok" || code === "error" ? code : "unset";
}

function spanIsError(record: Record<string, unknown>): boolean {
  if (statusCode(record) === "error") return true;
  const error = nestedObject(record, "error");
  return error.isError === true;
}

function toTraceNode(record: Record<string, unknown>, depth: number): TraceTreeNode {
  return {
    spanId: stringField(record, "spanId") ?? "",
    parentSpanId: stringField(record, "parentSpanId"),
    children: [],
    depth,
    service: stringField(record, "service"),
    name: stringField(record, "name") ?? "",
    kind: stringField(record, "kind") ?? "unspecified",
    startTime: stringField(record, "timestamp") ?? "",
    endTime: stringField(record, "endTimestamp"),
    duration: numberField(record, "duration"),
    statusCode: statusCode(record),
  };
}

function compareSpans(left: Record<string, unknown>, right: Record<string, unknown>): number {
  const leftTs = stringField(left, "timestamp") ?? "";
  const rightTs = stringField(right, "timestamp") ?? "";
  if (leftTs !== rightTs) return leftTs < rightTs ? -1 : 1;
  const leftDuration = numberField(left, "duration") ?? -1;
  const rightDuration = numberField(right, "duration") ?? -1;
  if (leftDuration !== rightDuration) return rightDuration - leftDuration;
  return (stringField(left, "name") ?? "").localeCompare(stringField(right, "name") ?? "");
}

function sortTree(nodes: TraceTreeNode[]): void {
  nodes.sort((left, right) => {
    if (left.startTime !== right.startTime) return left.startTime < right.startTime ? -1 : 1;
    if ((left.duration ?? -1) !== (right.duration ?? -1)) return (right.duration ?? -1) - (left.duration ?? -1);
    return left.name.localeCompare(right.name);
  });
  for (const node of nodes) sortTree(node.children);
}

function cloneNodeAtDepth(node: TraceTreeNode, depth: number): TraceTreeNode {
  return {
    ...node,
    depth,
    children: node.children.map((child) => cloneNodeAtDepth(child, depth + 1)),
  };
}

function buildCriticalPath(rootNodes: TraceTreeNode[]): string[] {
  if (rootNodes.length === 0) return [];
  const score = (node: TraceTreeNode): number => (node.duration ?? 0) + (node.statusCode === "error" ? 1_000_000 : 0);
  let current = [...rootNodes].sort((a, b) => score(b) - score(a))[0]!;
  const out: string[] = [];
  while (current) {
    out.push(current.spanId);
    if (current.children.length === 0) break;
    current = [...current.children].sort((a, b) => score(b) - score(a))[0]!;
  }
  return out;
}

function buildServiceMap(spans: Record<string, unknown>[], bySpanId: Map<string, Record<string, unknown>>): ServiceEdge[] {
  const edges = new Map<string, ServiceEdge>();
  for (const span of spans) {
    const parentSpanId = stringField(span, "parentSpanId");
    if (!parentSpanId) continue;
    const parent = bySpanId.get(parentSpanId);
    if (!parent) continue;
    const from = stringField(parent, "service");
    const to = stringField(span, "service");
    if (!from || !to || from === to) continue;
    const key = `${from}\0${to}`;
    let edge = edges.get(key);
    if (!edge) {
      edge = {
        from,
        to,
        count: 0,
        errorCount: 0,
        latency: { count: 0, sum: 0, min: null, max: null },
      };
      edges.set(key, edge);
    }
    edge.count += 1;
    if (spanIsError(span)) edge.errorCount += 1;
    const duration = numberField(span, "duration");
    if (duration != null) {
      edge.latency.count += 1;
      edge.latency.sum += duration;
      edge.latency.min = edge.latency.min == null ? duration : Math.min(edge.latency.min, duration);
      edge.latency.max = edge.latency.max == null ? duration : Math.max(edge.latency.max, duration);
    }
  }
  return Array.from(edges.values()).sort((a, b) => `${a.from}:${a.to}`.localeCompare(`${b.from}:${b.to}`));
}

function buildTraceErrors(spans: Record<string, unknown>[]): TraceError[] {
  const errors: TraceError[] = [];
  for (const span of spans) {
    if (!spanIsError(span)) continue;
    const error = nestedObject(span, "error");
    errors.push({
      spanId: stringField(span, "spanId") ?? "",
      service: stringField(span, "service"),
      name: stringField(span, "name") ?? "",
      time: stringField(span, "timestamp"),
      type: stringField(error, "type"),
      message: stringField(error, "message") ?? (isPlainObject(span.status) ? stringField(span.status, "message") : null),
    });
  }
  return errors;
}

export function buildTraceDetails(spansRaw: unknown[], args?: { spanLimitReached?: boolean; coverageComplete?: boolean }): TraceDetails {
  const input = spansRaw.filter(isPlainObject).sort(compareSpans);
  const unique = new Map<string, Record<string, unknown>>();
  let duplicateSpans = 0;
  for (const span of input) {
    const traceId = stringField(span, "traceId");
    const spanId = stringField(span, "spanId");
    if (!traceId || !spanId) continue;
    const key = `${traceId}:${spanId}`;
    if (unique.has(key)) {
      duplicateSpans += 1;
      continue;
    }
    unique.set(key, span);
  }
  const spans = Array.from(unique.values()).sort(compareSpans);
  const bySpanId = new Map<string, Record<string, unknown>>();
  for (const span of spans) {
    const spanId = stringField(span, "spanId");
    if (spanId) bySpanId.set(spanId, span);
  }
  const nodeBySpanId = new Map<string, TraceTreeNode>();
  for (const span of spans) {
    const spanId = stringField(span, "spanId");
    if (spanId) nodeBySpanId.set(spanId, toTraceNode(span, 0));
  }

  const roots: TraceTreeNode[] = [];
  const missingParents = new Set<string>();
  for (const span of spans) {
    const spanId = stringField(span, "spanId");
    if (!spanId) continue;
    const node = nodeBySpanId.get(spanId);
    if (!node) continue;
    const parentSpanId = stringField(span, "parentSpanId");
    if (!parentSpanId) {
      roots.push(node);
      continue;
    }
    const parent = nodeBySpanId.get(parentSpanId);
    if (!parent) {
      missingParents.add(parentSpanId);
      roots.push(node);
      continue;
    }
    parent.children.push(node);
  }

  const setDepth = (node: TraceTreeNode, depth: number): TraceTreeNode => {
    node.depth = depth;
    node.children = node.children.map((child) => setDepth(child, depth + 1));
    return node;
  };
  const tree = roots.map((root) => setDepth(root, 0)).map((root) => cloneNodeAtDepth(root, 0));
  sortTree(tree);

  return {
    traceId: spans.length > 0 ? stringField(spans[0]!, "traceId") : null,
    rootSpanId: tree[0]?.spanId ?? null,
    spans,
    tree,
    serviceMap: buildServiceMap(spans, bySpanId),
    criticalPath: buildCriticalPath(tree),
    errors: buildTraceErrors(spans),
    partial: (args?.spanLimitReached ?? false) || args?.coverageComplete === false || missingParents.size > 0,
    missingParents: Array.from(missingParents).sort(),
    duplicateSpans,
  };
}

export function summarizeSearchCoverage(batches: SearchResultBatch[], hits: SearchHit[], limitReached: boolean): ObserveSearchCoverage {
  const families = new Set<string>();
  let complete = batches.length > 0;
  let timedOut = false;
  let scannedTailDocs = 0;
  let scannedSegments = 0;
  let possibleMissing = 0;
  let totalValue = 0;
  let totalRelation: "eq" | "gte" = "eq";
  for (const batch of batches) {
    complete = complete && batch.coverage.complete;
    timedOut = timedOut || batch.timedOut;
    scannedTailDocs += batch.coverage.scannedTailDocs;
    scannedSegments += batch.coverage.scannedSegments;
    possibleMissing += batch.coverage.possibleMissingEventsUpperBound;
    totalValue += batch.total.value;
    if (batch.total.relation === "gte") totalRelation = "gte";
    for (const family of batch.coverage.indexFamiliesUsed) families.add(family);
  }
  if (batches.length === 0) complete = true;
  return {
    searched: batches.length > 0,
    complete: complete && !timedOut && !limitReached,
    timed_out: timedOut,
    limit_reached: limitReached,
    hits: hits.length,
    total: { value: totalValue, relation: limitReached ? "gte" : totalRelation },
    index_families_used: Array.from(families).sort(),
    scanned_tail_docs: scannedTailDocs,
    scanned_segments: scannedSegments,
    possible_missing_events_upper_bound: possibleMissing,
  };
}

export function sortTimeline(items: UnifiedTimelineItem[]): UnifiedTimelineItem[] {
  return [...items].sort((left, right) => {
    if (left.time !== right.time) return left.time < right.time ? -1 : 1;
    return left.kind.localeCompare(right.kind);
  });
}

function evlogLevel(record: Record<string, unknown>): "debug" | "info" | "warn" | "error" | null {
  const level = stringField(record, "level");
  return level === "debug" || level === "info" || level === "warn" || level === "error" ? level : null;
}

function firstString(...values: Array<string | null>): string | null {
  return values.find((value) => value != null) ?? null;
}

function firstNumber(...values: Array<number | null>): number | null {
  return values.find((value) => value != null) ?? null;
}

export function buildObserveSummary(args: {
  lookup: ObserveRequestInput["lookup"];
  primaryEvent: Record<string, unknown> | null;
  trace: TraceDetails;
}): Record<string, unknown> {
  const rootSpan = args.trace.spans.find((span) => stringField(span, "spanId") === args.trace.rootSpanId) ?? args.trace.spans[0] ?? null;
  const event = args.primaryEvent;
  const http = rootSpan ? nestedObject(rootSpan, "http") : {};
  const error = rootSpan ? nestedObject(rootSpan, "error") : {};
  const eventStatus = event ? numberField(event, "status") : null;
  const spanStatus = numberField(http, "statusCode");
  const level = event ? evlogLevel(event) : null;
  const method = firstString(event ? stringField(event, "method") : null, stringField(http, "method"));
  const path = firstString(event ? stringField(event, "path") : null, stringField(http, "path"));
  const route = stringField(http, "route");
  const rootStart = rootSpan ? stringField(rootSpan, "timestamp") : null;
  const rootEnd = rootSpan ? stringField(rootSpan, "endTimestamp") : null;
  const eventMessage = event ? stringField(event, "message") : null;
  const spanName = rootSpan ? stringField(rootSpan, "name") : null;
  return {
    title:
      eventMessage ??
      ([method, route ?? path].filter(Boolean).join(" ") || spanName || args.lookup.requestId || args.lookup.traceId || args.lookup.spanId || "request"),
    service: firstString(event ? stringField(event, "service") : null, rootSpan ? stringField(rootSpan, "service") : null),
    environment: firstString(event ? stringField(event, "environment") : null, rootSpan ? stringField(rootSpan, "environment") : null),
    method,
    path,
    route,
    status: firstNumber(eventStatus, spanStatus),
    level,
    duration: firstNumber(event ? numberField(event, "duration") : null, rootSpan ? numberField(rootSpan, "duration") : null),
    startTime: firstString(event ? stringField(event, "timestamp") : null, rootStart),
    endTime: rootEnd,
    error: {
      isError: level === "error" || eventStatus != null && eventStatus >= 500 || args.trace.errors.length > 0 || error.isError === true,
      type: stringField(error, "type"),
      message: firstString(stringField(error, "message"), event ? stringField(event, "message") : null),
      why: event ? stringField(event, "why") : null,
      fix: event ? stringField(event, "fix") : null,
      link: event ? stringField(event, "link") : null,
    },
  };
}

export function choosePrimaryEvent(events: SearchHit[], traceId: string | null): SearchHit | null {
  if (events.length === 0) return null;
  if (traceId) {
    const matching = events.find((hit) => isPlainObject(hit.source) && stringField(hit.source, "traceId") === traceId);
    if (matching) return matching;
  }
  return events[0]!;
}
