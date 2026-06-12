import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gzipSync } from "node:zlib";
import { createProfileTestApp, fetchJsonApp } from "./profile_test_utils";

const TRACE_ID = "5b8efff798038103d269b633813fc60c";
const SPAN_ID = "086e83747d0e381e";
const CHILD_SPAN_ID = "186e83747d0e381f";

async function createOtelTraceStream(app: ReturnType<typeof createProfileTestApp>["app"], stream: string, profile: Record<string, unknown> = {}) {
  await app.fetch(
    new Request(`http://local/v1/stream/${stream}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  return fetchJsonApp(app, `http://local/v1/stream/${stream}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile: {
        kind: "otel-traces",
        ...profile,
      },
    }),
  });
}

function otlpJsonSpan(overrides: Record<string, unknown> = {}) {
  return {
    traceId: TRACE_ID,
    spanId: SPAN_ID,
    name: "GET /checkout",
    kind: 2,
    startTimeUnixNano: "1772020800000000000",
    endTimeUnixNano: "1772020800123000000",
    attributes: [
      { key: "request.id", value: { stringValue: "req_otel_1" } },
      { key: "http.request.method", value: { stringValue: "GET" } },
      { key: "http.route", value: { stringValue: "/checkout" } },
      { key: "http.response.status_code", value: { intValue: "500" } },
      { key: "authorization", value: { stringValue: "Bearer secret" } },
      { key: "db.system", value: { stringValue: "postgresql" } },
      { key: "db.statement", value: { stringValue: "SELECT * FROM users WHERE email = 'a@example.com'" } },
    ],
    events: [
      {
        timeUnixNano: "1772020800100000000",
        name: "exception",
        attributes: [
          { key: "exception.type", value: { stringValue: "Error" } },
          { key: "exception.message", value: { stringValue: "checkout failed" } },
          { key: "token", value: { stringValue: "secret-token" } },
        ],
      },
    ],
    status: { code: 2, message: "failed" },
    ...overrides,
  };
}

function otlpJsonRequest(spans = [otlpJsonSpan()]) {
  return {
    resourceSpans: [
      {
        resource: {
          attributes: [
            { key: "service.name", value: { stringValue: "checkout" } },
            { key: "deployment.environment.name", value: { stringValue: "prod" } },
            { key: "service.version", value: { stringValue: "1.2.3" } },
          ],
        },
        scopeSpans: [
          {
            scope: { name: "test-sdk", version: "1.0.0" },
            spans,
          },
        ],
      },
    ],
  };
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

function writeBytes(out: number[], field: number, bytes: Uint8Array): void {
  writeTag(out, field, 2);
  writeVarint(out, BigInt(bytes.byteLength));
  out.push(...bytes);
}

function writeMessage(out: number[], field: number, body: number[]): void {
  writeBytes(out, field, new Uint8Array(body));
}

function writeFixed64(out: number[], field: number, value: bigint): void {
  writeTag(out, field, 1);
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, value, true);
  out.push(...bytes);
}

function hexBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return bytes;
}

function anyString(value: string): number[] {
  const out: number[] = [];
  writeString(out, 1, value);
  return out;
}

function kvString(key: string, value: string): number[] {
  const out: number[] = [];
  writeString(out, 1, key);
  writeMessage(out, 2, anyString(value));
  return out;
}

function statusMessage(code: number, message: string): number[] {
  const out: number[] = [];
  writeString(out, 2, message);
  writeTag(out, 3, 0);
  writeVarint(out, BigInt(code));
  return out;
}

function makeOtlpProtoRequest(): Uint8Array {
  const span: number[] = [];
  writeBytes(span, 1, hexBytes(TRACE_ID));
  writeBytes(span, 2, hexBytes(CHILD_SPAN_ID));
  writeBytes(span, 4, hexBytes(SPAN_ID));
  writeString(span, 5, "SELECT cart");
  writeTag(span, 6, 0);
  writeVarint(span, 1n);
  writeFixed64(span, 7, 1772020800010000000n);
  writeFixed64(span, 8, 1772020800018000000n);
  writeMessage(span, 9, kvString("request.id", "req_proto_1"));
  writeMessage(span, 9, kvString("db.system", "postgresql"));
  writeMessage(span, 9, kvString("db.operation", "SELECT"));
  writeMessage(span, 15, statusMessage(1, "ok"));

  const scope: number[] = [];
  writeString(scope, 1, "proto-test");

  const scopeSpans: number[] = [];
  writeMessage(scopeSpans, 1, scope);
  writeMessage(scopeSpans, 2, span);

  const resource: number[] = [];
  writeMessage(resource, 1, kvString("service.name", "checkout"));
  writeMessage(resource, 1, kvString("deployment.environment.name", "prod"));

  const resourceSpans: number[] = [];
  writeMessage(resourceSpans, 1, resource);
  writeMessage(resourceSpans, 2, scopeSpans);

  const request: number[] = [];
  writeMessage(request, 1, resourceSpans);
  return new Uint8Array(request);
}

describe("otel-traces profile", () => {
  test("installs on json streams and exposes canonical schema/search defaults", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-otel-install-"));
    const { app } = createProfileTestApp(root);
    try {
      const res = await createOtelTraceStream(app, "otel-install", {
        redactKeys: ["sessionToken"],
        requestIdAttributes: ["request.id", "x-request-id"],
        attributeLimits: { maxAttributesPerSpan: 32 },
        store: { rawLinks: false },
        dbStatementMode: "raw",
        observability: {
          request: {
            eventsStream: "app-events",
          },
        },
      });
      expect(res.status).toBe(200);
      expect(res.body?.profile).toEqual({
        kind: "otel-traces",
        redactKeys: ["sessiontoken"],
        requestIdAttributes: ["request.id", "x-request-id"],
        attributeLimits: { maxAttributesPerSpan: 32 },
        store: { rawLinks: false },
        dbStatementMode: "raw",
        observability: {
          request: {
            eventsStream: "app-events",
          },
        },
      });

      const schemaRes = await fetchJsonApp(app, "http://local/v1/stream/otel-install/_schema", { method: "GET" });
      expect(schemaRes.status).toBe(200);
      expect(schemaRes.body?.search?.profile).toBe("otel-traces");
      expect(schemaRes.body?.search?.aliases?.trace).toBe("traceId");
      expect(schemaRes.body?.search?.aliases?.status).toBe("http.statusCode");
      expect(schemaRes.body?.search?.fields?.traceId?.kind).toBe("keyword");
      expect(schemaRes.body?.search?.fields?.duration?.kind).toBe("float");
      expect(schemaRes.body?.search?.fields?.["events.name"]?.bindings?.[0]?.jsonPointer).toBe("/eventNames");
      expect(schemaRes.body?.search?.rollups?.spans?.measures?.latency?.field).toBe("duration");

      const listRes = await fetchJsonApp(app, "http://local/v1/streams", { method: "GET" });
      expect(listRes.status).toBe(200);
      expect(listRes.body.find((row: any) => row.name === "otel-install")?.observability).toEqual({
        request: {
          events_stream: "app-events",
          traces_stream: "otel-install",
        },
      });

      const detailsRes = await fetchJsonApp(app, "http://local/v1/stream/otel-install/_details", { method: "GET" });
      expect(detailsRes.status).toBe(200);
      expect(detailsRes.body?.stream?.observability).toEqual({
        request: {
          events_stream: "app-events",
          traces_stream: "otel-install",
        },
      });
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("rejects non-json streams, invalid config, and late profile install", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-otel-validate-"));
    const { app } = createProfileTestApp(root);
    try {
      await app.fetch(new Request("http://local/v1/stream/otel-non-json", { method: "PUT", headers: { "content-type": "text/plain" } }));
      const nonJsonRes = await fetchJsonApp(app, "http://local/v1/stream/otel-non-json/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ profile: { kind: "otel-traces" } }),
      });
      expect(nonJsonRes.status).toBe(400);
      expect(nonJsonRes.body?.error?.message).toContain("application/json");

      await app.fetch(new Request("http://local/v1/stream/otel-invalid", { method: "PUT", headers: { "content-type": "application/json" } }));
      const invalidRes = await fetchJsonApp(app, "http://local/v1/stream/otel-invalid/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ profile: { kind: "otel-traces", dbStatementMode: "redact_literals" } }),
      });
      expect(invalidRes.status).toBe(400);
      expect(invalidRes.body?.error?.message).toContain("dbStatementMode");

      const invalidPairingRes = await fetchJsonApp(app, "http://local/v1/stream/otel-invalid/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          profile: {
            kind: "otel-traces",
            observability: {
              request: {
                eventsStream: "",
              },
            },
          },
        }),
      });
      expect(invalidPairingRes.status).toBe(400);
      expect(invalidPairingRes.body?.error?.message).toContain("profile.observability.request.eventsStream");

      await app.fetch(new Request("http://local/v1/stream/otel-late", { method: "PUT", headers: { "content-type": "application/json" } }));
      await app.fetch(
        new Request("http://local/v1/stream/otel-late", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ ok: true }),
        })
      );
      const lateRes = await fetchJsonApp(app, "http://local/v1/stream/otel-late/_profile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ profile: { kind: "otel-traces" } }),
      });
      expect(lateRes.status).toBe(400);
      expect(lateRes.body?.error?.message).toContain("before appending data");
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("normalizes canonical JSON appends, redacts attributes, and supports search aliases", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-otel-json-"));
    const { app } = createProfileTestApp(root, { searchWalOverlayQuietPeriodMs: 0 });
    try {
      await createOtelTraceStream(app, "otel-json", { dbStatementMode: "raw" });
      const appendRes = await app.fetch(
        new Request("http://local/v1/stream/otel-json", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            traceId: TRACE_ID,
            spanId: SPAN_ID,
            parentSpanId: null,
            name: "GET /checkout",
            kind: "server",
            startUnixNano: "1772020800000000000",
            endUnixNano: "1772020800123000000",
            status: { code: "error", message: "failed" },
            resource: {
              attributes: {
                "service.name": "checkout",
                "deployment.environment.name": "prod",
              },
            },
            attributes: {
              "request.id": "req_json_1",
              "http.request.method": "GET",
              "http.route": "/checkout",
              "http.response.status_code": 500,
              authorization: "Bearer secret",
              "db.system": "postgresql",
              "db.statement": "SELECT 1",
            },
            events: [
              {
                timeUnixNano: "1772020800100000000",
                name: "exception",
                attributes: {
                  "exception.type": "Error",
                  "exception.message": "checkout failed",
                  token: "secret-token",
                },
              },
            ],
          }),
        })
      );
      expect([200, 204]).toContain(appendRes.status);

      const readRes = await fetchJsonApp(app, "http://local/v1/stream/otel-json?format=json", { method: "GET" });
      expect(readRes.status).toBe(200);
      expect(readRes.body).toHaveLength(1);
      const span = readRes.body[0];
      expect(span).toMatchObject({
        schemaVersion: 1,
        signal: "trace.span",
        traceId: TRACE_ID,
        spanId: SPAN_ID,
        requestId: "req_json_1",
        service: "checkout",
        environment: "prod",
        duration: 123,
        http: { method: "GET", route: "/checkout", statusCode: 500 },
        db: { system: "postgresql", statement: "SELECT 1" },
        error: { isError: true, type: "Error", message: "checkout failed" },
        eventNames: ["exception"],
      });
      expect(span.attributes.authorization).toBe("[REDACTED]");
      expect(span.events[0].attributes.token).toBe("[REDACTED]");
      expect(span.redaction.keys).toEqual(expect.arrayContaining(["attributes.authorization", "events.0.attributes.token"]));

      const searchRes = await fetchJsonApp(app, "http://local/v1/stream/otel-json/_search", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          q: `trace:${TRACE_ID} req:req_json_1 status:>=500`,
          sort: ["timestamp:asc", "spanId:asc"],
        }),
      });
      expect(searchRes.status).toBe(200);
      expect(searchRes.body?.hits).toHaveLength(1);
      expect(searchRes.body?.hits?.[0]?.fields).toMatchObject({
        traceId: TRACE_ID,
        requestId: "req_json_1",
        service: "checkout",
        "http.statusCode": 500,
      });
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("ingests OTLP JSON over the default endpoint with gzip and auto-create", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-otel-otlp-json-"));
    const { app } = createProfileTestApp(root, {
      otlpTracesStream: "auto-traces",
      otlpAutoCreate: true,
      searchWalOverlayQuietPeriodMs: 0,
    });
    try {
      const body = gzipSync(JSON.stringify(otlpJsonRequest()));
      const res = await fetchJsonApp(app, "http://local/v1/traces", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "content-encoding": "gzip",
        },
        body,
      });
      expect(res.status).toBe(200);
      expect(res.body).toEqual({});
      expect(app.deps.db.getStream("auto-traces")?.profile).toBe("otel-traces");

      const readRes = await fetchJsonApp(app, "http://local/v1/stream/auto-traces?format=json", { method: "GET" });
      expect(readRes.status).toBe(200);
      expect(readRes.body).toHaveLength(1);
      expect(readRes.body[0]).toMatchObject({
        traceId: TRACE_ID,
        spanId: SPAN_ID,
        requestId: "req_otel_1",
        service: "checkout",
        http: { method: "GET", route: "/checkout", statusCode: 500 },
        db: { system: "postgresql", statement: null },
      });
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("ingests OTLP protobuf on explicit stream endpoint", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-otel-otlp-proto-"));
    const { app } = createProfileTestApp(root, { searchWalOverlayQuietPeriodMs: 0 });
    try {
      await createOtelTraceStream(app, "proto-traces");
      const res = await app.fetch(
        new Request("http://local/v1/stream/proto-traces/_otlp/v1/traces", {
          method: "POST",
          headers: { "content-type": "application/x-protobuf" },
          body: makeOtlpProtoRequest(),
        })
      );
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toBe("application/x-protobuf");
      expect((await res.arrayBuffer()).byteLength).toBe(0);

      const readRes = await fetchJsonApp(app, "http://local/v1/stream/proto-traces?format=json", { method: "GET" });
      expect(readRes.status).toBe(200);
      expect(readRes.body).toHaveLength(1);
      expect(readRes.body[0]).toMatchObject({
        traceId: TRACE_ID,
        spanId: CHILD_SPAN_ID,
        parentSpanId: SPAN_ID,
        requestId: "req_proto_1",
        service: "checkout",
        db: { system: "postgresql", operation: "SELECT" },
        status: { code: "ok", message: "ok" },
      });
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("returns OTLP partial success for rejected spans without dropping valid spans", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-otel-partial-"));
    const { app } = createProfileTestApp(root);
    try {
      await createOtelTraceStream(app, "partial-traces");
      const res = await fetchJsonApp(app, "http://local/v1/stream/partial-traces/_otlp/v1/traces", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(
          otlpJsonRequest([
            otlpJsonSpan(),
            otlpJsonSpan({
              traceId: "00000000000000000000000000000000",
              spanId: "0000000000000000",
            }),
          ])
        ),
      });
      expect(res.status).toBe(200);
      expect(res.body?.partialSuccess?.rejectedSpans).toBe(1);
      expect(res.body?.partialSuccess?.errorMessage).toContain("traceId");

      const readRes = await fetchJsonApp(app, "http://local/v1/stream/partial-traces?format=json", { method: "GET" });
      expect(readRes.body).toHaveLength(1);
      expect(readRes.body[0]?.traceId).toBe(TRACE_ID);
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
