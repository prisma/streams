/**
 * Demo: simulate a Postgres wal2json-style change feed, map it into State
 * Protocol change events, and append those to a stream with touch enabled.
 *
 * Usage:
 *   bun run experiments/demo/wal_demo_ingest.ts --delay-ms 250
 *   bun run experiments/demo/wal_demo_ingest.ts --delay-ms 250 --count 100
 *   bun run experiments/demo/wal_demo_ingest.ts --delay-ms 250 --reset
 *   bun run experiments/demo/wal_demo_ingest.ts --url http://127.0.0.1:8080 --stream demo.wal --delay-ms 250
 */

import { defaultTouchStreamName } from "../../src/touch/naming";
import { DEFAULT_BASE_URL, DEFAULT_STREAM, DEMO_INTERPRETER, DEMO_SCHEMA, sleep, hasFlag, parseIntArg, parseStringArg } from "./common";
import { dsError } from "../../src/util/ds_error.ts";

const ARGS = process.argv.slice(2);

type TodoRow = {
  tenantId: string;
  id: string;
  userId: string;
  orgId: string;
  status: string;
  title: string;
};

const COLS: Array<keyof TodoRow> = ["tenantId", "id", "userId", "orgId", "status", "title"];

type WalChange =
  | {
      kind: "insert";
      schema: string;
      table: string;
      columnnames: string[];
      columnvalues: any[];
      pk?: { pknames: string[]; pktypes?: string[] };
    }
  | {
      kind: "update";
      schema: string;
      table: string;
      columnnames: string[];
      columnvalues: any[];
      oldkeys: { keynames: string[]; keyvalues: any[] };
      pk?: { pknames: string[]; pktypes?: string[] };
    }
  | {
      kind: "delete";
      schema: string;
      table: string;
      oldkeys: { keynames: string[]; keyvalues: any[] };
      pk?: { pknames: string[]; pktypes?: string[] };
    };

function rowToColumns(row: TodoRow): { names: string[]; values: any[] } {
  const names: string[] = [];
  const values: any[] = [];
  for (const c of COLS) {
    names.push(c);
    values.push(row[c]);
  }
  return { names, values };
}

type Wal2JsonV1Envelope = {
  xid: number;
  timestamp: string;
  change: WalChange[];
};

type StateProtocolChange = {
  type: string;
  key: string;
  value?: any;
  oldValue?: any;
  headers: {
    operation: "insert" | "update" | "delete";
    txid?: string;
    timestamp?: string;
  };
};

function zipObject(names: any[], values: any[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  const len = Math.min(names.length, values.length);
  for (let i = 0; i < len; i++) out[String(names[i])] = values[i];
  return out;
}

function pkNamesFromChange(change: WalChange): string[] {
  const raw = (change as any).pk?.pknames;
  if (Array.isArray(raw) && raw.length > 0 && raw.every((x: any) => typeof x === "string" && x.trim() !== "")) {
    return raw.map((x: any) => String(x));
  }
  throw dsError("wal2json change missing pk.pknames (require include-pk=on)");
}

function keyFromRow(pkNames: string[], row: any): string {
  if (!row || typeof row !== "object" || Array.isArray(row)) throw dsError("cannot derive key: row is not an object");
  const parts = pkNames.map((name) => {
    if (!Object.prototype.hasOwnProperty.call(row, name)) throw dsError(`cannot derive key: missing pk field ${name}`);
    const v = (row as any)[name];
    if (v === null) return "null";
    if (v === undefined) return "undefined";
    if (typeof v === "string") return v;
    if (typeof v === "number" && Number.isFinite(v)) return String(v);
    if (typeof v === "boolean") return v ? "true" : "false";
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  });
  if (pkNames.length === 1) return parts[0];
  return pkNames.map((n, i) => `${n}=${parts[i]}`).join("|");
}

function wal2jsonV1EnvelopeToStateProtocol(env: Wal2JsonV1Envelope): StateProtocolChange[] {
  const txid = String(env.xid);
  const timestamp = env.timestamp;

  const out: StateProtocolChange[] = [];
  for (const ch of env.change) {
    const kind = ch.kind;
    const type = `${ch.schema}.${ch.table}`;
    const pkNames = pkNamesFromChange(ch);

    const after =
      "columnnames" in ch && "columnvalues" in ch && Array.isArray(ch.columnnames) && Array.isArray(ch.columnvalues)
        ? zipObject(ch.columnnames, ch.columnvalues)
        : undefined;
    const before =
      "oldkeys" in ch && ch.oldkeys && Array.isArray(ch.oldkeys.keynames) && Array.isArray(ch.oldkeys.keyvalues)
        ? zipObject(ch.oldkeys.keynames, ch.oldkeys.keyvalues)
        : undefined;

    const keyBefore = before ? keyFromRow(pkNames, before) : null;
    const keyAfter = after ? keyFromRow(pkNames, after) : null;

    if (kind === "insert") {
      if (!keyAfter) throw dsError("insert missing pk values");
      out.push({
        type,
        key: keyAfter,
        value: after,
        headers: { operation: "insert", txid, timestamp },
      });
      continue;
    }
    if (kind === "delete") {
      if (!keyBefore) throw dsError("delete missing oldkeys identity");
      out.push({
        type,
        key: keyBefore,
        value: null,
        oldValue: before,
        headers: { operation: "delete", txid, timestamp },
      });
      continue;
    }

    // update
    const oldKey = keyBefore ?? keyAfter;
    const newKey = keyAfter ?? keyBefore;
    if (!oldKey || !newKey) throw dsError("update missing pk values");

    // If PK changed, represent as delete(old) + insert(new) for correct state materialization.
    if (oldKey !== newKey) {
      out.push({
        type,
        key: oldKey,
        value: null,
        oldValue: before,
        headers: { operation: "delete", txid, timestamp },
      });
      out.push({
        type,
        key: newKey,
        value: after,
        headers: { operation: "insert", txid, timestamp },
      });
      continue;
    }

    out.push({
      type,
      key: oldKey,
      value: after,
      oldValue: before,
      headers: { operation: "update", txid, timestamp },
    });
  }
  return out;
}

function makeEnvelope(change: WalChange): Wal2JsonV1Envelope {
  return {
    xid: Math.floor(Math.random() * 1_000_000_000),
    timestamp: new Date().toISOString(),
    change: [change],
  };
}

async function fetchJson(url: string, init: RequestInit): Promise<any> {
  const r = await fetch(url, init);
  const text = await r.text();
  if (!r.ok) {
    throw dsError(`HTTP ${r.status} ${url}: ${text}`);
  }
  if (text === "") return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function deleteStream(baseUrl: string, stream: string): Promise<void> {
  const r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, { method: "DELETE" });
  if (r.status === 404) return;
  if (!r.ok && r.status !== 204) {
    const t = await r.text();
    throw dsError(`failed to delete stream ${stream}: ${r.status} ${t}`);
  }
}

async function ensureStream(baseUrl: string, stream: string): Promise<void> {
  const r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
  });
  if (!r.ok && r.status !== 200 && r.status !== 201) {
    const t = await r.text();
    throw dsError(`failed to create stream ${stream}: ${r.status} ${t}`);
  }
}

async function ensureSchemaAndInterpreter(baseUrl: string, stream: string): Promise<void> {
  const reg = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, { method: "GET" });
  const currentVersion = typeof reg?.currentVersion === "number" ? reg.currentVersion : 0;
  if (currentVersion === 0) {
    await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ schema: DEMO_SCHEMA, interpreter: DEMO_INTERPRETER }),
    });
    return;
  }
  // Avoid lens complexity; just ensure interpreter is set.
  await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ interpreter: DEMO_INTERPRETER }),
  });
}

async function appendEvents(baseUrl: string, stream: string, events: any[]): Promise<void> {
  if (!Array.isArray(events) || events.length === 0) throw dsError("appendEvents requires a non-empty event array");
  const r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(events),
  });
  if (!r.ok && r.status !== 204 && r.status !== 200) {
    const t = await r.text();
    throw dsError(`append failed: ${r.status} ${t}`);
  }
}

function toggleStatus(s: string, a: string, b: string): string {
  return s === a ? b : a;
}

async function main(): Promise<void> {
  const baseUrl = parseStringArg(ARGS, "--url", DEFAULT_BASE_URL);
  const stream = parseStringArg(ARGS, "--stream", DEFAULT_STREAM);
  const delayMs = parseIntArg(ARGS, "--delay-ms", 250);
  const count = parseIntArg(ARGS, "--count", 0);
  const reset = hasFlag(ARGS, "--reset");

  if (reset) {
    const derived = defaultTouchStreamName(stream);
    // eslint-disable-next-line no-console
    console.log(`[demo][ingest] reset: deleting ${stream} and ${derived}`);
    await deleteStream(baseUrl, stream);
    await deleteStream(baseUrl, derived);
  }

  await ensureStream(baseUrl, stream);
  await ensureSchemaAndInterpreter(baseUrl, stream);

  // eslint-disable-next-line no-console
  console.log(`[demo][ingest] stream ready: ${stream} (touch companion at /v1/stream/${stream}/touch)`);
  // eslint-disable-next-line no-console
  console.log(`[demo][ingest] delayMs=${delayMs} count=${count === 0 ? "infinite" : String(count)}`);

  const rows = new Map<string, TodoRow>();
  rows.set("1", { tenantId: "t1", id: "1", userId: "u1", orgId: "o1", status: "open", title: "A" });
  rows.set("2", { tenantId: "t1", id: "2", userId: "u9", orgId: "o2", status: "done", title: "B" });
  rows.set("3", { tenantId: "t2", id: "3", userId: "u3", orgId: "o3", status: "open", title: "C" });
  rows.set("999", { tenantId: "t9", id: "999", userId: "u9", orgId: "o9", status: "noise", title: "NOISE" });

  // Seed initial inserts.
  for (const id of ["1", "2", "3", "999"]) {
    const row = rows.get(id)!;
    const { names, values } = rowToColumns(row);
    const change: WalChange = {
      kind: "insert",
      schema: "public",
      table: "todos",
      columnnames: names,
      columnvalues: values,
      pk: { pknames: ["id"], pktypes: ["text"] },
    };
    // eslint-disable-next-line no-console
    console.log(`[demo][ingest] insert id=${row.id} tenant=${row.tenantId} user=${row.userId} org=${row.orgId} status=${row.status}`);
    const env = makeEnvelope(change);
    const events = wal2jsonV1EnvelopeToStateProtocol(env);
    await appendEvents(baseUrl, stream, events);
    await sleep(delayMs);
  }

  let emitted = 0;
  let noiseTick = 0;

  // Deterministic 6-event cycle:
  // 1) noise title update (t9) - never matches q1/q2/q3
  // 2) status toggle on id=1 (t1,u1,*) - matches q1 each time (before/after has status=open)
  // 3) noise title update (t9)
  // 4) status toggle on id=2 (t1,*,o2) - matches q2 each time (before/after has status=done)
  // 5) noise title update (t9)
  // 6) status toggle on id=3 (t2,*,*) - matches q3 each time (before/after has status=open)
  //
  // This gives each query a steady stream of touches with non-matching events in between.
  for (;;) {
    const step = emitted % 6;

    if (step === 0 || step === 2 || step === 4) {
      const before = rows.get("999")!;
      const after: TodoRow = { ...before, title: `NOISE-${++noiseTick}` };
      rows.set("999", after);
      const b = rowToColumns(before);
      const a = rowToColumns(after);
      const change: WalChange = {
        kind: "update",
        schema: "public",
        table: "todos",
        columnnames: a.names,
        columnvalues: a.values,
        oldkeys: { keynames: b.names, keyvalues: b.values },
        pk: { pknames: ["id"], pktypes: ["text"] },
      };
      // eslint-disable-next-line no-console
      console.log(`[demo][ingest] update id=999 tenant=t9 title="${before.title}" -> "${after.title}"`);
      const env = makeEnvelope(change);
      const events = wal2jsonV1EnvelopeToStateProtocol(env);
      await appendEvents(baseUrl, stream, events);
    } else if (step === 1) {
      const before = rows.get("1")!;
      const after: TodoRow = { ...before, status: toggleStatus(before.status, "open", "done") };
      rows.set("1", after);
      const b = rowToColumns(before);
      const a = rowToColumns(after);
      const change: WalChange = {
        kind: "update",
        schema: "public",
        table: "todos",
        columnnames: a.names,
        columnvalues: a.values,
        oldkeys: { keynames: b.names, keyvalues: b.values },
        pk: { pknames: ["id"], pktypes: ["text"] },
      };
      // eslint-disable-next-line no-console
      console.log(`[demo][ingest] update id=1 tenant=t1 user=u1 status=${before.status} -> ${after.status}`);
      const env = makeEnvelope(change);
      const events = wal2jsonV1EnvelopeToStateProtocol(env);
      await appendEvents(baseUrl, stream, events);
    } else if (step === 3) {
      const before = rows.get("2")!;
      const after: TodoRow = { ...before, status: toggleStatus(before.status, "done", "open") };
      rows.set("2", after);
      const b = rowToColumns(before);
      const a = rowToColumns(after);
      const change: WalChange = {
        kind: "update",
        schema: "public",
        table: "todos",
        columnnames: a.names,
        columnvalues: a.values,
        oldkeys: { keynames: b.names, keyvalues: b.values },
        pk: { pknames: ["id"], pktypes: ["text"] },
      };
      // eslint-disable-next-line no-console
      console.log(`[demo][ingest] update id=2 tenant=t1 org=o2 status=${before.status} -> ${after.status}`);
      const env = makeEnvelope(change);
      const events = wal2jsonV1EnvelopeToStateProtocol(env);
      await appendEvents(baseUrl, stream, events);
    } else if (step === 5) {
      const before = rows.get("3")!;
      const after: TodoRow = { ...before, status: toggleStatus(before.status, "open", "done") };
      rows.set("3", after);
      const b = rowToColumns(before);
      const a = rowToColumns(after);
      const change: WalChange = {
        kind: "update",
        schema: "public",
        table: "todos",
        columnnames: a.names,
        columnvalues: a.values,
        oldkeys: { keynames: b.names, keyvalues: b.values },
        pk: { pknames: ["id"], pktypes: ["text"] },
      };
      // eslint-disable-next-line no-console
      console.log(`[demo][ingest] update id=3 tenant=t2 status=${before.status} -> ${after.status}`);
      const env = makeEnvelope(change);
      const events = wal2jsonV1EnvelopeToStateProtocol(env);
      await appendEvents(baseUrl, stream, events);
    }

    emitted++;
    if (count !== 0 && emitted >= count) break;
    await sleep(delayMs);
  }

  // eslint-disable-next-line no-console
  console.log(`[demo][ingest] done (emitted ${emitted})`);
}

await main();
