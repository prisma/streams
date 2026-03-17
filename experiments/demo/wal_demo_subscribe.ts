/**
 * Demo: Live Query V2 wait loop (coarse vs realtime).
 *
 * Usage:
 *   bun run experiments/demo/wal_demo_subscribe.ts --query q1 --mode upgrade
 *   bun run experiments/demo/wal_demo_subscribe.ts --query q1 --mode coarse
 *   bun run experiments/demo/wal_demo_subscribe.ts --query q1 --mode realtime
 *
 * Options:
 *   --url http://127.0.0.1:8080
 *   --stream demo.wal
 *   --ttl-ms 3600000
 */

import { tableKeyFor, templateIdFor, watchKeyFor } from "../../src/touch/live_keys";
import { DEFAULT_BASE_URL, DEFAULT_STREAM, DEMO_ENTITY, argValue, parseIntArg, parseStringArg, sleep } from "./common";
import { dsError } from "../../src/util/ds_error.ts";

const ARGS = process.argv.slice(2);

type QueryId = "q1" | "q2" | "q3";
type Mode = "coarse" | "realtime" | "upgrade";

type QuerySpec = {
  id: QueryId;
  sql: string;
  entity: string;
  fields: string[];
  args: Record<string, string>;
};

const QUERIES: Record<QueryId, QuerySpec> = {
  q1: {
    id: "q1",
    sql: "SELECT id, title FROM todos WHERE tenantId='t1' AND userId='u1' AND status='open' ORDER BY id;",
    entity: DEMO_ENTITY,
    fields: ["tenantId", "userId", "status"],
    args: { tenantId: "t1", userId: "u1", status: "open" },
  },
  q2: {
    id: "q2",
    sql: "SELECT id, title FROM todos WHERE tenantId='t1' AND orgId='o2' AND status='done' ORDER BY id;",
    entity: DEMO_ENTITY,
    fields: ["tenantId", "orgId", "status"],
    args: { tenantId: "t1", orgId: "o2", status: "done" },
  },
  q3: {
    id: "q3",
    sql: "SELECT count(*) FROM todos WHERE tenantId='t2' AND status='open';",
    entity: DEMO_ENTITY,
    fields: ["tenantId", "status"],
    args: { tenantId: "t2", status: "open" },
  },
};

function parseQuery(args: string[]): QuerySpec {
  const raw = argValue(args, "--query");
  if (raw == null) throw dsError("missing --query (q1|q2|q3)");
  const q = raw.trim() as QueryId;
  const spec = QUERIES[q];
  if (!spec) throw dsError(`invalid --query: ${raw} (expected q1|q2|q3)`);
  return spec;
}

function parseMode(args: string[]): Mode {
  const raw = argValue(args, "--mode");
  if (!raw) return "upgrade";
  const m = raw.trim();
  if (m === "coarse" || m === "realtime" || m === "upgrade") return m;
  throw dsError(`invalid --mode: ${raw} (expected coarse|realtime|upgrade)`);
}

async function fetchJson(url: string, init: RequestInit): Promise<any> {
  const r = await fetch(url, init);
  const text = await r.text();
  if (!r.ok) {
    throw dsError(`HTTP ${r.status} ${url}: ${text}`);
  }
  if (text === "") return null;
  return JSON.parse(text);
}

async function waitForTouchEnabled(baseUrl: string, stream: string): Promise<void> {
  for (;;) {
    const r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
    if (r.status === 404) {
      await sleep(200);
      continue;
    }
    if (!r.ok) {
      const t = await r.text();
      throw dsError(`touch/meta failed: ${r.status} ${t}`);
    }
    return;
  }
}

async function activateTemplate(baseUrl: string, stream: string, entity: string, fieldsSorted: string[], ttlMs: number): Promise<any> {
  return fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/templates/activate`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      templates: [
        {
          entity,
          fields: fieldsSorted.map((name) => ({ name, encoding: "string" })),
        },
      ],
      inactivityTtlMs: ttlMs,
    }),
  });
}

async function waitOnce(baseUrl: string, stream: string, body: any): Promise<any> {
  return fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

async function main(): Promise<void> {
  const baseUrl = parseStringArg(ARGS, "--url", DEFAULT_BASE_URL);
  const stream = parseStringArg(ARGS, "--stream", DEFAULT_STREAM);
  const q = parseQuery(ARGS);
  const mode = parseMode(ARGS);
  const ttlMs = parseIntArg(ARGS, "--ttl-ms", 60 * 60 * 1000);

  const fieldsSorted = [...q.fields].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const templateId = templateIdFor(q.entity, fieldsSorted);
  const tableKey = tableKeyFor(q.entity);
  const argsInOrder = fieldsSorted.map((name) => {
    const v = q.args[name];
    if (v === undefined) throw dsError(`missing arg for field ${name}`);
    return v;
  });
  const watchKey = watchKeyFor(templateId, argsInOrder);

  // eslint-disable-next-line no-console
  console.log(`[demo][sub] stream=${stream}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] query=${q.id}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] mode=${mode}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] sql=${q.sql}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] entity=${q.entity}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] tableKey=${tableKey}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] template fields(sorted)=${JSON.stringify(fieldsSorted)}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] templateId=${templateId}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] watch args(sorted order)=${JSON.stringify(argsInOrder)}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] watchKey=${watchKey}`);

  await waitForTouchEnabled(baseUrl, stream);

  const meta = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
  const since0 = meta?.currentTouchOffset ?? "now";

  // Activate the template (idempotent).
  const act = await activateTemplate(baseUrl, stream, q.entity, fieldsSorted, ttlMs);
  // eslint-disable-next-line no-console
  console.log(`[demo][sub] activate: ${JSON.stringify(act)}`);

  let cursor = since0;
  let phase: Mode = mode === "upgrade" ? "coarse" : mode;
  let phaseStartMs = Date.now();

  for (;;) {
    if (mode === "upgrade" && phase === "coarse" && Date.now() - phaseStartMs >= 5000) {
      phase = "realtime";
      phaseStartMs = Date.now();
      // eslint-disable-next-line no-console
      console.log(`[demo][sub] upgrade: switching to realtime mode (watchKey only)`);
    }

    const keys = phase === "coarse" ? [tableKey] : [watchKey];

    const res = await waitOnce(baseUrl, stream, {
      sinceTouchOffset: cursor,
      timeoutMs: 30_000,
      keys,
      templateIdsUsed: [templateId],
    });

    if (res?.stale) {
      // eslint-disable-next-line no-console
      console.log(`[demo][sub] stale cursor; oldestAvailableTouchOffset=${res.oldestAvailableTouchOffset}`);
      cursor = res.currentTouchOffset ?? cursor;
      continue;
    }

    const nextCursor = res?.currentTouchOffset ?? res?.touchOffset ?? cursor;
    const touched = !!res?.touched;
    const touchedKeys = Array.isArray(res?.touchedKeys) ? res.touchedKeys : [];

    // eslint-disable-next-line no-console
    console.log(`[demo][sub] wait: touched=${touched} touchedKeys=${JSON.stringify(touchedKeys)} cursor=${cursor} -> ${nextCursor}`);

    cursor = nextCursor;
  }
}

await main();

