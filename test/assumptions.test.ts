import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { STREAM_FLAG_DELETED, type SqliteDurableStore } from "../src/db/db";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { parseOffset } from "../src/offset";
import { streamHash16Hex } from "../src/util/stream_paths";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    ...overrides,
  };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

function seedAccelerationState(db: SqliteDurableStore, stream: string): void {
  const secret = new Uint8Array([1, 2, 3, 4]);
  db.upsertIndexState(stream, secret, 1);
  db.insertIndexRun({
    run_id: `${stream}-routing-run`,
    stream,
    level: 0,
    start_segment: 0,
    end_segment: 0,
    object_key: `streams/${stream}/routing.run`,
    size_bytes: 32,
    filter_len: 4,
    record_count: 1,
  });
  db.upsertSecondaryIndexState(stream, "repoName", secret, "cfg", 1);
  db.insertSecondaryIndexRun({
    run_id: `${stream}-secondary-run`,
    stream,
    index_name: "repoName",
    level: 0,
    start_segment: 0,
    end_segment: 0,
    object_key: `streams/${stream}/exact.run`,
    size_bytes: 32,
    filter_len: 4,
    record_count: 1,
  });
  db.upsertLexiconIndexState(stream, "routing_key", "", 1);
  db.insertLexiconIndexRun({
    run_id: `${stream}-lexicon-run`,
    stream,
    source_kind: "routing_key",
    source_name: "",
    level: 0,
    start_segment: 0,
    end_segment: 0,
    object_key: `streams/${stream}/lexicon.run`,
    size_bytes: 32,
    record_count: 1,
  });
  db.upsertSearchCompanionPlan(stream, 1, "hash", JSON.stringify({ generation: 1, sections: ["col"] }));
  db.upsertSearchSegmentCompanion(
    stream,
    0,
    `streams/${stream}/segments/0.cmp`,
    1,
    JSON.stringify(["col"]),
    JSON.stringify({ col: 32 }),
    32,
    null,
    null
  );
}

function seedObjectStoreCounters(db: SqliteDurableStore, stream: string): void {
  const streamHash = streamHash16Hex(stream);
  db.recordObjectStoreRequestByHash(streamHash, "segment", "put", 128, 2);
  db.recordObjectStoreRequestByHash(streamHash, "manifest", "put", 64, 1);
}

function seedAsyncIndexAction(db: SqliteDurableStore, stream: string): void {
  const seq = db.beginAsyncIndexAction({
    stream,
    actionKind: "routing_l0_build",
    targetKind: "routing_index",
    inputKind: "segment",
    inputCount: 1,
    inputSizeBytes: 128n,
    startSegment: 0,
    endSegment: 0,
    detailJson: JSON.stringify({ level: 0 }),
  });
  db.completeAsyncIndexAction({
    seq,
    status: "succeeded",
    outputCount: 1,
    outputSizeBytes: 64n,
    detailJson: JSON.stringify({ level: 0 }),
  });
}

function seedSegmentBuildAction(db: SqliteDurableStore, stream: string): void {
  const seq = db.beginSegmentBuildAction({
    stream,
    actionKind: "segment_build",
    inputKind: "wal",
    segmentIndex: 0,
    startOffset: 0n,
    detailJson: JSON.stringify({ pending_rows_before: 1 }),
  });
  db.completeSegmentBuildAction({
    seq,
    status: "succeeded",
    inputCount: 1,
    inputSizeBytes: 128n,
    outputCount: 1,
    outputSizeBytes: 64n,
    endOffset: 0n,
    detailJson: JSON.stringify({ block_count: 1 }),
  });
}

function expectAccelerationStateCleared(db: SqliteDurableStore, stream: string): void {
  expect(db.getIndexState(stream)).toBeNull();
  expect(db.listIndexRuns(stream)).toHaveLength(0);
  expect(db.listSecondaryIndexStates(stream)).toHaveLength(0);
  expect(db.listSecondaryIndexRuns(stream, "repoName")).toHaveLength(0);
  expect(db.listLexiconIndexStates(stream)).toHaveLength(0);
  expect(db.listLexiconIndexRuns(stream, "routing_key", "")).toHaveLength(0);
  expect(db.getSearchCompanionPlan(stream)).toBeNull();
  expect(db.listSearchSegmentCompanions(stream)).toHaveLength(0);
}

function expectObjectStoreCountersCleared(db: SqliteDurableStore, stream: string): void {
  const summary = db.getObjectStoreRequestSummaryByHash(streamHash16Hex(stream));
  expect(summary.puts).toBe(0n);
  expect(summary.reads).toBe(0n);
  expect(summary.gets).toBe(0n);
  expect(summary.heads).toBe(0n);
  expect(summary.lists).toBe(0n);
  expect(summary.deletes).toBe(0n);
  expect(summary.by_artifact).toHaveLength(0);
}

function expectAsyncIndexActionsCleared(db: SqliteDurableStore, stream: string): void {
  expect(db.listAsyncIndexActions(stream, 10)).toHaveLength(0);
}

function expectSegmentBuildActionsCleared(db: SqliteDurableStore, stream: string): void {
  expect(db.listSegmentBuildActions(stream, 10)).toHaveLength(0);
}

describe("assumptions", () => {
  test("append to missing stream returns 404", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    const r = await fetch(`${baseUrl}/v1/stream/missing`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1]),
    });
    expect(r.status).toBe(404);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("expired streams return 404", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/ttl`, { method: "PUT", headers: { "stream-ttl": "1s" } });
    await sleep(1100);
    let r = await fetch(`${baseUrl}/v1/stream/ttl?offset=-1`);
    expect(r.status).toBe(404);
    r = await fetch(`${baseUrl}/v1/stream/ttl`, { method: "HEAD" });
    expect(r.status).toBe(404);
    r = await fetch(`${baseUrl}/v1/stream/ttl`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1]),
    });
    expect(r.status).toBe(404);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("/v1/streams pagination", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/a`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/b`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/c`, { method: "PUT" });

    let r = await fetch(`${baseUrl}/v1/streams?limit=2&offset=0`);
    let arr = await r.json();
    expect(arr.length).toBe(2);

    r = await fetch(`${baseUrl}/v1/streams?limit=2&offset=2`);
    arr = await r.json();
    // Includes system streams like __stream_metrics__; total is 4 here.
    expect(arr.length).toBe(2);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("schema update accepts unencoded slashes in stream name", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    const streamName = "record/app.bsky.feed.like";
    const putRes = await fetch(`${baseUrl}/v1/stream/${streamName}`, { method: "PUT" });
    expect([200, 201]).toContain(putRes.status);

    const schemaRes = await fetch(`${baseUrl}/v1/stream/${streamName}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ schema: { type: "object", additionalProperties: true } }),
    });
    expect(schemaRes.status).toBe(200);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("schema update rejects registry-shaped payload", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    const streamName = "record/app.bsky.feed.like";
    const putRes = await fetch(`${baseUrl}/v1/stream/${streamName}`, { method: "PUT" });
    expect([200, 201]).toContain(putRes.status);

    const payload = {
      apiVersion: "durable.streams/schema-registry/v1",
      schema: streamName,
      currentVersion: 1,
      schemas: {
        "1": { type: "object", additionalProperties: true },
      },
      lenses: {},
    };

    const schemaRes = await fetch(`${baseUrl}/v1/stream/${streamName}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    expect(schemaRes.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("schema update accepts routingKey-only payload", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    const streamName = "record/app.bsky.feed.like";
    const putRes = await fetch(`${baseUrl}/v1/stream/${streamName}`, { method: "PUT" });
    expect([200, 201]).toContain(putRes.status);

    const payload = {
      routingKey: { jsonPointer: "/subject/uri", required: true },
    };
    const schemaRes = await fetch(`${baseUrl}/v1/stream/${streamName}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    expect(schemaRes.status).toBe(200);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("schema update rejects legacy routing-key aliases", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    const streamName = "record/app.bsky.feed.like";
    const putRes = await fetch(`${baseUrl}/v1/stream/${streamName}`, { method: "PUT" });
    expect([200, 201]).toContain(putRes.status);

    const schemaRes = await fetch(`${baseUrl}/v1/stream/${streamName}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        schema: { type: "object", additionalProperties: true },
        routing_key: "/subject/uri",
      }),
    });
    expect(schemaRes.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("decimal offset alias is rejected", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/alias`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/alias`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1]),
    });
    await fetch(`${baseUrl}/v1/stream/alias`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([2]),
    });

    const r = await fetch(`${baseUrl}/v1/stream/alias?offset=0`);
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("JSON Content-Type with charset is accepted", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/json`, { method: "PUT", headers: { "content-type": "application/json" } });
    const r = await fetch(`${baseUrl}/v1/stream/json`, {
      method: "POST",
      headers: { "content-type": "application/json; charset=utf-8" },
      body: JSON.stringify([{ ok: true }]),
    });
    expect(r.status).toBe(204);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("empty JSON array is rejected", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/empty`, { method: "PUT", headers: { "content-type": "application/json" } });
    const r = await fetch(`${baseUrl}/v1/stream/empty`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "[]",
    });
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("Stream-Key rejected when routingKey configured", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/rk`, { method: "PUT", headers: { "content-type": "application/json" } });
    await fetch(`${baseUrl}/v1/stream/rk/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        schema: { type: "object", properties: { id: { type: "string" } }, required: ["id"], additionalProperties: false },
        routingKey: { jsonPointer: "/id", required: true },
      }),
    });

    const r = await fetch(`${baseUrl}/v1/stream/rk`, {
      method: "POST",
      headers: { "content-type": "application/json", "stream-key": "x" },
      body: JSON.stringify([{ id: "x" }]),
    });
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("format=json on non-json payload returns 400", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/raw`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/raw`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1, 2, 3]),
    });
    const r = await fetch(`${baseUrl}/v1/stream/raw?offset=-1&format=json`);
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("unknown format returns 400", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/fmt`, { method: "PUT" });
    const r = await fetch(`${baseUrl}/v1/stream/fmt?offset=-1&format=xml`);
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("missing Content-Type on POST returns 400", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/noct`, { method: "PUT", headers: { "content-type": "application/octet-stream" } });
    const r = await fetch(`${baseUrl}/v1/stream/noct`, {
      method: "POST",
      body: new Uint8Array([1, 2, 3]),
    });
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("since= selects records at or after the timestamp", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/since`, { method: "PUT", headers: { "content-type": "text/plain" } });

    const t1Ms = Date.now() + 1000;
    const t2Ms = t1Ms + 1000;
    const t1 = new Date(t1Ms);
    const t2 = new Date(t2Ms);

    await fetch(`${baseUrl}/v1/stream/since`, {
      method: "POST",
      headers: { "content-type": "text/plain", "stream-timestamp": t1.toISOString() },
      body: new TextEncoder().encode("a"),
    });
    await fetch(`${baseUrl}/v1/stream/since`, {
      method: "POST",
      headers: { "content-type": "text/plain", "stream-timestamp": t2.toISOString() },
      body: new TextEncoder().encode("b"),
    });

    const since = new Date(t1Ms + 500).toISOString();
    const r = await fetch(`${baseUrl}/v1/stream/since?since=${encodeURIComponent(since)}`);
    expect(r.status).toBe(200);
    const text = await r.text();
    expect(text).toBe("b");

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("initial epoch is 0", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/epoch`, { method: "PUT" });
    const r = await fetch(`${baseUrl}/v1/stream/epoch`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1]),
    });
    const next = r.headers.get("stream-next-offset")!;
    const p = parseOffset(next);
    expect(p.kind).toBe("seq");
    if (p.kind === "seq") expect(p.epoch).toBe(0);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("delete is tombstone, listing excludes deleted, and acceleration state is scrubbed", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/del`, { method: "PUT" });
    seedAccelerationState(app.deps.db, "del");
    seedObjectStoreCounters(app.deps.db, "del");
    seedAsyncIndexAction(app.deps.db, "del");
    seedSegmentBuildAction(app.deps.db, "del");
    expect(app.deps.db.getIndexState("del")).not.toBeNull();
    expect(app.deps.db.listSecondaryIndexStates("del")).toHaveLength(1);
    expect(app.deps.db.listLexiconIndexStates("del")).toHaveLength(1);
    expect(app.deps.db.getSearchCompanionPlan("del")).not.toBeNull();
    expect(app.deps.db.getObjectStoreRequestSummaryByHash(streamHash16Hex("del")).puts).toBeGreaterThan(0n);

    await fetch(`${baseUrl}/v1/stream/del`, { method: "DELETE" });
    const r = await fetch(`${baseUrl}/v1/stream/del?offset=-1`);
    expect([404, 410]).toContain(r.status);

    const list = await fetch(`${baseUrl}/v1/streams`);
    const arr = await list.json();
    expect(arr.find((x: any) => x.name === "del")).toBeUndefined();
    const deletedRow = app.deps.db.getStream("del");
    expect(deletedRow).not.toBeNull();
    expect(deletedRow && app.deps.db.isDeleted(deletedRow)).toBe(true);
    expectAccelerationStateCleared(app.deps.db, "del");
    expectObjectStoreCountersCleared(app.deps.db, "del");
    expectAsyncIndexActionsCleared(app.deps.db, "del");
    expectSegmentBuildActionsCleared(app.deps.db, "del");

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("delete resets per-stream request counters even after tombstone manifest publication", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/reset-counters`, { method: "PUT" });
    seedObjectStoreCounters(app.deps.db, "reset-counters");
    expect(app.deps.db.getObjectStoreRequestSummaryByHash(streamHash16Hex("reset-counters")).puts).toBeGreaterThan(0n);

    const deleted = await fetch(`${baseUrl}/v1/stream/reset-counters`, { method: "DELETE" });
    expect(deleted.status).toBe(204);
    expectObjectStoreCountersCleared(app.deps.db, "reset-counters");

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("startup scrubs stale acceleration state for deleted streams", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    try {
      app.deps.db.ensureStream("stale");
      const now = app.deps.db.nowMs();
      app.deps.db.db
        .query(`UPDATE streams SET stream_flags = (stream_flags | ?), updated_at_ms=? WHERE stream=?;`)
        .run(STREAM_FLAG_DELETED, now, "stale");
      seedAccelerationState(app.deps.db, "stale");
      seedObjectStoreCounters(app.deps.db, "stale");
      seedAsyncIndexAction(app.deps.db, "stale");
      seedSegmentBuildAction(app.deps.db, "stale");
      expect(app.deps.db.getIndexState("stale")).not.toBeNull();
      expect(app.deps.db.listSecondaryIndexStates("stale")).toHaveLength(1);
      expect(app.deps.db.listLexiconIndexStates("stale")).toHaveLength(1);
      expect(app.deps.db.getSearchCompanionPlan("stale")).not.toBeNull();
      expect(app.deps.db.getObjectStoreRequestSummaryByHash(streamHash16Hex("stale")).puts).toBeGreaterThan(0n);
    } finally {
      app.close();
    }

    const restarted = createApp(cfg, new MockR2Store());
    try {
      const deletedRow = restarted.deps.db.getStream("stale");
      expect(deletedRow).not.toBeNull();
      expect(deletedRow && restarted.deps.db.isDeleted(deletedRow)).toBe(true);
      expectAccelerationStateCleared(restarted.deps.db, "stale");
      expectObjectStoreCountersCleared(restarted.deps.db, "stale");
      expectAsyncIndexActionsCleared(restarted.deps.db, "stale");
      expectSegmentBuildActionsCleared(restarted.deps.db, "stale");
    } finally {
      restarted.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("Stream-Seq is lexicographic and strictly increasing", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/seq`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/seq`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream", "stream-seq": "2" },
      body: new Uint8Array([1]),
    });
    const bad = await fetch(`${baseUrl}/v1/stream/seq`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream", "stream-seq": "10" },
      body: new Uint8Array([2]),
    });
    expect(bad.status).toBe(409);
    const ok = await fetch(`${baseUrl}/v1/stream/seq`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream", "stream-seq": "3" },
      body: new Uint8Array([3]),
    });
    expect(ok.status).toBe(204);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });
});
