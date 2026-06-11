import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createProfileTestApp, fetchJsonApp } from "./profile_test_utils";

const TRACE_ID = "5b8efff798038103d269b633813fc60c";
const ROOT_SPAN_ID = "086e83747d0e381e";
const DB_SPAN_ID = "186e83747d0e381f";
const CLIENT_SPAN_ID = "286e83747d0e3820";

async function createJsonStream(app: ReturnType<typeof createProfileTestApp>["app"], stream: string, profile: Record<string, unknown>) {
  await app.fetch(
    new Request(`http://local/v1/stream/${stream}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  const res = await fetchJsonApp(app, `http://local/v1/stream/${stream}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile,
    }),
  });
  expect(res.status).toBe(200);
}

function span(args: {
  spanId: string;
  parentSpanId?: string | null;
  service: string;
  name: string;
  kind?: string;
  start: string;
  end: string;
  statusCode?: "unset" | "ok" | "error";
  httpStatus?: number;
  requestId?: string;
  errorMessage?: string;
}) {
  return {
    traceId: TRACE_ID,
    spanId: args.spanId,
    parentSpanId: args.parentSpanId ?? null,
    name: args.name,
    kind: args.kind ?? "internal",
    startUnixNano: args.start,
    endUnixNano: args.end,
    status: { code: args.statusCode ?? "unset", message: args.errorMessage ?? null },
    resource: {
      attributes: {
        "service.name": args.service,
        "deployment.environment.name": "prod",
      },
    },
    attributes: {
      ...(args.requestId ? { "request.id": args.requestId } : {}),
      ...(args.httpStatus ? { "http.response.status_code": args.httpStatus } : {}),
      ...(args.name.startsWith("GET") ? { "http.request.method": "GET", "http.route": "/checkout" } : {}),
      ...(args.name.startsWith("SELECT") ? { "db.system": "postgresql", "db.operation": "SELECT" } : {}),
    },
    events: args.errorMessage
      ? [
          {
            timeUnixNano: args.end,
            name: "exception",
            attributes: {
              "exception.type": "Error",
              "exception.message": args.errorMessage,
            },
          },
        ]
      : [],
  };
}

async function seedObservabilityStreams(app: ReturnType<typeof createProfileTestApp>["app"]) {
  await createJsonStream(app, "app-events", { kind: "evlog" });
  await createJsonStream(app, "app-traces", { kind: "otel-traces" });

  const eventRes = await app.fetch(
    new Request("http://local/v1/stream/app-events", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        timestamp: "2026-03-27T10:00:00.250Z",
        level: "error",
        service: "checkout",
        environment: "prod",
        requestId: "req_obs_1",
        traceId: TRACE_ID,
        spanId: ROOT_SPAN_ID,
        method: "GET",
        path: "/checkout",
        status: 502,
        duration: 260,
        message: "Checkout failed",
        why: "Payment provider returned 502",
        fix: "Retry after provider recovery",
      }),
    })
  );
  expect([200, 204]).toContain(eventRes.status);

  const traceRes = await app.fetch(
    new Request("http://local/v1/stream/app-traces", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify([
        span({
          spanId: ROOT_SPAN_ID,
          service: "checkout",
          name: "GET /checkout",
          kind: "server",
          start: "1772020800000000000",
          end: "1772020800260000000",
          statusCode: "error",
          httpStatus: 502,
          requestId: "req_obs_1",
          errorMessage: "provider unavailable",
        }),
        span({
          spanId: DB_SPAN_ID,
          parentSpanId: ROOT_SPAN_ID,
          service: "checkout",
          name: "SELECT cart",
          start: "1772020800030000000",
          end: "1772020800040000000",
          statusCode: "ok",
        }),
        span({
          spanId: CLIENT_SPAN_ID,
          parentSpanId: ROOT_SPAN_ID,
          service: "payments",
          name: "POST payment",
          kind: "client",
          start: "1772020800100000000",
          end: "1772020800250000000",
          statusCode: "error",
          errorMessage: "provider unavailable",
        }),
      ]),
    })
  );
  expect([200, 204]).toContain(traceRes.status);
}

function observeBody(lookup: Record<string, string>, extra: Record<string, unknown> = {}) {
  return {
    streams: { events: "app-events", traces: "app-traces" },
    lookup,
    include: { events: true, trace: true, timeline: true },
    limits: { events: 20, spans: 100 },
    ...extra,
  };
}

describe("observe request API", () => {
  test("looks up by requestId and returns evlog context with trace tree", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-observe-request-id-"));
    const { app } = createProfileTestApp(root, { searchWalOverlayQuietPeriodMs: 0 });
    try {
      await seedObservabilityStreams(app);
      const res = await fetchJsonApp(app, "http://local/v1/observe/request", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(observeBody({ requestId: "req_obs_1" })),
      });

      expect(res.status).toBe(200);
      expect(res.body.lookup).toEqual({ requestId: "req_obs_1", traceId: TRACE_ID, spanId: null });
      expect(res.body.summary).toMatchObject({
        title: "Checkout failed",
        service: "checkout",
        environment: "prod",
        method: "GET",
        path: "/checkout",
        route: "/checkout",
        status: 502,
        level: "error",
        duration: 260,
        error: {
          isError: true,
          why: "Payment provider returned 502",
          fix: "Retry after provider recovery",
        },
      });
      expect(res.body.evlog.primary.requestId).toBe("req_obs_1");
      expect(res.body.evlog.matches).toHaveLength(1);
      expect(res.body.trace.traceId).toBe(TRACE_ID);
      expect(res.body.trace.rootSpanId).toBe(ROOT_SPAN_ID);
      expect(res.body.trace.spans).toHaveLength(3);
      expect(res.body.trace.tree).toHaveLength(1);
      expect(res.body.trace.tree[0].children.map((child: any) => child.spanId).sort()).toEqual([CLIENT_SPAN_ID, DB_SPAN_ID].sort());
      expect(res.body.trace.serviceMap).toEqual([
        {
          from: "checkout",
          to: "payments",
          count: 1,
          errorCount: 1,
          latency: { count: 1, sum: 150, min: 150, max: 150 },
        },
      ]);
      expect(res.body.trace.errors.map((error: any) => error.spanId).sort()).toEqual([CLIENT_SPAN_ID, ROOT_SPAN_ID].sort());
      expect(res.body.trace.criticalPath).toContain(ROOT_SPAN_ID);
      expect(res.body.timeline.length).toBeGreaterThanOrEqual(7);
      expect(res.body.coverage.events.searched).toBe(true);
      expect(res.body.coverage.traces.searched).toBe(true);
      expect(res.body.coverage.warnings).toEqual([]);
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("looks up by spanId, expands to the full trace, and correlates evlog by traceId", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-observe-span-id-"));
    const { app } = createProfileTestApp(root, { searchWalOverlayQuietPeriodMs: 0 });
    try {
      await seedObservabilityStreams(app);
      const res = await fetchJsonApp(app, "http://local/v1/observe/request", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(observeBody({ spanId: CLIENT_SPAN_ID })),
      });

      expect(res.status).toBe(200);
      expect(res.body.lookup).toEqual({ requestId: "req_obs_1", traceId: TRACE_ID, spanId: CLIENT_SPAN_ID });
      expect(res.body.trace.spans.map((item: any) => item.spanId).sort()).toEqual([ROOT_SPAN_ID, DB_SPAN_ID, CLIENT_SPAN_ID].sort());
      expect(res.body.evlog.primary.traceId).toBe(TRACE_ID);
      expect(res.body.trace.partial).toBe(false);
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("reports partial trace when span limit is reached", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-observe-limit-"));
    const { app } = createProfileTestApp(root, { searchWalOverlayQuietPeriodMs: 0 });
    try {
      await seedObservabilityStreams(app);
      const res = await fetchJsonApp(app, "http://local/v1/observe/request", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(observeBody({ traceId: TRACE_ID }, { limits: { events: 20, spans: 1 } })),
      });

      expect(res.status).toBe(200);
      expect(res.body.trace.spans).toHaveLength(1);
      expect(res.body.trace.partial).toBe(true);
      expect(res.body.coverage.traces.limit_reached).toBe(true);
      expect(res.body.coverage.warnings).toContain("span limit reached");
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("paginates trace lookup across more than one _search page", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-observe-scale-"));
    const { app } = createProfileTestApp(root, { searchWalOverlayQuietPeriodMs: 0 });
    try {
      await createJsonStream(app, "scale-traces", { kind: "otel-traces" });
      const rootSpanId = "0000000000000001";
      const spans = [
        span({
          spanId: rootSpanId,
          service: "api",
          name: "GET /bulk",
          kind: "server",
          start: "1772020800000000000",
          end: "1772020802000000000",
          statusCode: "ok",
          requestId: "req_scale_1",
        }),
      ];
      for (let i = 2; i <= 1200; i++) {
        const spanId = i.toString(16).padStart(16, "0");
        spans.push(
          span({
            spanId,
            parentSpanId: rootSpanId,
            service: i % 2 === 0 ? "api" : "worker",
            name: `child ${i}`,
            start: String(1772020800000000000n + BigInt(i) * 1_000_000n),
            end: String(1772020800000000000n + BigInt(i + 1) * 1_000_000n),
            statusCode: "ok",
          })
        );
      }

      const appendRes = await app.fetch(
        new Request("http://local/v1/stream/scale-traces", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(spans),
        })
      );
      expect([200, 204]).toContain(appendRes.status);

      const res = await fetchJsonApp(app, "http://local/v1/observe/request", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          streams: { traces: "scale-traces" },
          lookup: { traceId: TRACE_ID },
          include: { events: false, trace: true, timeline: false },
          limits: { spans: 1300 },
        }),
      });

      expect(res.status).toBe(200);
      expect(res.body.trace.spans).toHaveLength(1200);
      expect(res.body.trace.tree).toHaveLength(1);
      expect(res.body.trace.tree[0].children).toHaveLength(1199);
      expect(res.body.trace.partial).toBe(false);
      expect(res.body.coverage.traces.hits).toBe(1200);
      expect(res.body.coverage.traces.limit_reached).toBe(false);
      expect(res.body.coverage.warnings).toEqual([]);
    } finally {
      await app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
