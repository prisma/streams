import { defaultTouchStreamName } from "../../src/touch/naming";
import { templateIdFor, watchKeyFor, watchKeyIdFor } from "../../src/touch/live_keys";
import { startLocalDurableStreamsServer, type DurableStreamsLocalServer } from "../../src/local";
import { getStatus } from "../../src/local/state";
import { dsError } from "../../src/util/ds_error.ts";

const ENTITY = "demo.live.fields";
const UPDATE_ENTITY = "demo.live.fields.update";
const STREAM = "demo.live.fields";
const LOCAL_SERVER_NAME = "live-fields-demo";
const FIELD_IDS = ["field1", "field2"] as const;

type FieldId = (typeof FIELD_IDS)[number];

type FieldUpdate = {
  field: FieldId;
  value: string;
};

type ParsedArgs = {
  port: number;
  dsPort: number;
  dsName: string;
  stream: string;
  updatesStream: string;
  reset: boolean;
};

const ARGS = process.argv.slice(2);

function argValue(flag: string): string | null {
  const idx = ARGS.indexOf(flag);
  if (idx === -1) return null;
  return ARGS[idx + 1] ?? null;
}

function hasFlag(flag: string): boolean {
  return ARGS.includes(flag);
}

function parseIntArg(flag: string, fallback: number): number {
  const raw = argValue(flag);
  if (raw == null) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) {
    throw dsError(`invalid ${flag}: ${raw}`);
  }
  return n;
}

function parseStringArg(flag: string, fallback: string): string {
  const raw = argValue(flag);
  return raw == null ? fallback : raw;
}

function parseArgs(): ParsedArgs {
  const stream = parseStringArg("--stream", STREAM);
  return {
    port: parseIntArg("--port", 3000),
    dsPort: parseIntArg("--ds-port", 0),
    dsName: parseStringArg("--ds-name", LOCAL_SERVER_NAME),
    stream,
    updatesStream: parseStringArg("--updates-stream", `${stream}.updates`),
    reset: hasFlag("--reset"),
  };
}

function asFieldId(value: string): FieldId | null {
  return (FIELD_IDS as readonly string[]).includes(value) ? (value as FieldId) : null;
}

async function fetchJson(url: string, init: RequestInit): Promise<any> {
  const res = await fetch(url, init);
  const text = await res.text();
  if (!res.ok) throw dsError(`HTTP ${res.status} ${url}: ${text}`);
  if (text === "") return null;
  return JSON.parse(text);
}

async function fetchText(url: string, init: RequestInit): Promise<string> {
  const res = await fetch(url, init);
  const text = await res.text();
  if (!res.ok) throw dsError(`HTTP ${res.status} ${url}: ${text}`);
  return text;
}

async function deleteStream(baseUrl: string, stream: string): Promise<void> {
  const res = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, { method: "DELETE" });
  if (res.status === 404 || res.status === 204) return;
  if (!res.ok) {
    const text = await res.text();
    throw dsError(`failed to delete stream ${stream}: ${res.status} ${text}`);
  }
}

async function ensureStream(baseUrl: string, stream: string): Promise<void> {
  const res = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
  });
  if (!res.ok && res.status !== 200 && res.status !== 201) {
    const text = await res.text();
    throw dsError(`failed to create stream ${stream}: ${res.status} ${text}`);
  }
}

async function ensureSourceSchemaAndInterpreter(baseUrl: string, stream: string): Promise<void> {
  const reg = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, { method: "GET" });
  const currentVersion = typeof reg?.currentVersion === "number" ? reg.currentVersion : 0;

  const interpreter = {
    apiVersion: "durable.streams/stream-interpreter/v1",
    format: "durable.streams/state-protocol/v1",
    touch: {
      enabled: true,
      retention: { maxAgeMs: 60 * 60 * 1000 },
      coarseIntervalMs: 20,
      touchCoalesceWindowMs: 20,
    },
  };

  const routingKey = { jsonPointer: "/key", required: true };

  if (currentVersion === 0) {
    await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        schema: { type: "object", additionalProperties: true },
        routingKey,
        interpreter,
      }),
    });
    return;
  }

  await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ routingKey, interpreter }),
  });
}

async function ensureUpdateStreamSchema(baseUrl: string, stream: string): Promise<void> {
  const reg = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, { method: "GET" });
  const currentVersion = typeof reg?.currentVersion === "number" ? reg.currentVersion : 0;
  const routingKey = { jsonPointer: "/key", required: true };

  if (currentVersion === 0) {
    await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ schema: { type: "object", additionalProperties: true }, routingKey }),
    });
    return;
  }

  await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ routingKey }),
  });
}

async function activateTemplate(baseUrl: string, stream: string): Promise<void> {
  const res = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/templates/activate`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      templates: [
        {
          entity: ENTITY,
          fields: [{ name: "field", encoding: "string" }],
        },
      ],
      inactivityTtlMs: 60 * 60 * 1000,
    }),
  });

  if (!Array.isArray(res?.denied) || res.denied.length > 0) {
    throw dsError(`template activation denied: ${JSON.stringify(res?.denied ?? [])}`);
  }
}

async function appendEvents(baseUrl: string, stream: string, events: any[]): Promise<void> {
  if (events.length === 0) return;
  const res = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(events),
  });
  if (!res.ok && res.status !== 200 && res.status !== 204) {
    const text = await res.text();
    throw dsError(`append failed to ${stream}: ${res.status} ${text}`);
  }
}

function parseLatestEventText(text: string): string | null {
  if (text.trim() === "") return null;
  let parsed: any;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  if (!Array.isArray(parsed) || parsed.length === 0) return null;
  const last = parsed[parsed.length - 1];
  if (!last || typeof last !== "object") return null;
  const valueObj = (last as any).value;
  if (!valueObj || typeof valueObj !== "object") return "";
  const rawText = (valueObj as any).text;
  return typeof rawText === "string" ? rawText : "";
}

async function readLatestFieldValue(baseUrl: string, stream: string, field: FieldId): Promise<string | null> {
  const text = await fetchText(
    `${baseUrl}/v1/stream/${encodeURIComponent(stream)}/pk/${encodeURIComponent(field)}?offset=-1&format=json`,
    { method: "GET" }
  );
  return parseLatestEventText(text);
}

async function ensureInitialRows(baseUrl: string, stream: string): Promise<Record<FieldId, string>> {
  const current = Object.fromEntries(FIELD_IDS.map((field) => [field, ""])) as Record<FieldId, string>;

  const missing: FieldId[] = [];
  for (const field of FIELD_IDS) {
    const latest = await readLatestFieldValue(baseUrl, stream, field);
    if (latest == null) {
      missing.push(field);
      continue;
    }
    current[field] = latest;
  }

  if (missing.length === 0) return current;

  const now = new Date().toISOString();
  const txid = `${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
  const events = missing.map((field) => ({
    type: ENTITY,
    key: field,
    value: {
      field,
      text: "",
    },
    headers: {
      operation: "insert",
      txid,
      timestamp: now,
    },
  }));

  await appendEvents(baseUrl, stream, events);
  return current;
}

function buildLiveQuerySummary(values: Record<FieldId, string>): {
  items: Array<{ field: FieldId; text: string; length: number }>;
  nonEmptyCount: number;
  totalChars: number;
} {
  const items = FIELD_IDS.map((field) => ({
    field,
    text: values[field] ?? "",
    length: (values[field] ?? "").length,
  })).filter((x) => x.text !== "");
  const totalChars = items.reduce((sum, x) => sum + x.length, 0);
  return {
    items,
    nonEmptyCount: items.length,
    totalChars,
  };
}

async function appendFieldUpdate(baseUrl: string, stream: string, update: FieldUpdate, previousText: string): Promise<void> {
  const now = new Date().toISOString();
  const txid = `${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

  await appendEvents(baseUrl, stream, [
    {
      type: ENTITY,
      key: update.field,
      value: {
        field: update.field,
        text: update.value,
      },
      oldValue: {
        field: update.field,
        text: previousText,
      },
      headers: {
        operation: "update",
        txid,
        timestamp: now,
      },
    },
  ]);
}

async function startOrReuseLocalDs(name: string, dsPort: number): Promise<{ baseUrl: string; close: () => Promise<void> }> {
  const status = await getStatus(name);
  if (status.running && status.dump?.http?.url) {
    return {
      baseUrl: status.dump.http.url,
      close: async () => {
        // Reused process owned elsewhere.
      },
    };
  }

  const localServer: DurableStreamsLocalServer = await startLocalDurableStreamsServer({
    name,
    port: dsPort,
    hostname: "127.0.0.1",
  });

  return {
    baseUrl: localServer.exports.http.url,
    close: async () => {
      await localServer.close();
    },
  };
}

function renderHtml(args: {
  stream: string;
  updatesStream: string;
  templateId: string;
  watchByField: Record<FieldId, { key: string; keyId: number }>;
}): string {
  const streamJson = JSON.stringify(args.stream);
  const updatesJson = JSON.stringify(args.updatesStream);
  const liveWaitJson = JSON.stringify({
    sourceStream: args.stream,
    templateId: args.templateId,
    keys: FIELD_IDS.map((field) => args.watchByField[field].key),
    keyIds: FIELD_IDS.map((field) => args.watchByField[field].keyId),
  });

  return String.raw`<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Durable Streams Live Fields Demo</title>
    <style>
      :root {
        --bg-0: #f3efe7;
        --bg-1: #fff9f0;
        --ink: #1d252b;
        --muted: #5a6771;
        --card: #ffffff;
        --line: #d8d2c7;
        --accent: #1f6a7a;
        --flash: #fff1a8;
        --ok: #2c8d5c;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        min-height: 100vh;
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        color: var(--ink);
        background: radial-gradient(1200px 600px at 10% 0%, var(--bg-1), var(--bg-0));
        display: grid;
        place-items: center;
        padding: 24px;
      }
      .panel {
        width: min(980px, 100%);
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 16px;
        box-shadow: 0 10px 30px rgba(37, 45, 54, 0.08);
        padding: 20px;
      }
      h1 {
        margin: 0 0 8px;
        font-size: 1.25rem;
      }
      p {
        margin: 0;
        color: var(--muted);
      }
      .meta {
        margin-top: 10px;
        font-size: 0.9rem;
        color: var(--muted);
      }
      .status-dot {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 999px;
        margin-right: 6px;
        background: var(--ok);
      }
      .grid {
        margin-top: 18px;
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }
      .card {
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 12px;
        background: #fff;
      }
      .card-head {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 8px;
      }
      label {
        font-size: 0.9rem;
        color: #3b4650;
      }
      .live-toggle {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        font-size: 0.85rem;
        color: #3b4650;
        user-select: none;
      }
      input[type="checkbox"] {
        width: 14px;
        height: 14px;
        accent-color: var(--accent);
      }
      input[type="text"] {
        width: 100%;
        border-radius: 10px;
        border: 1px solid #bcc4cb;
        padding: 10px 12px;
        font-size: 0.95rem;
        transition: border-color 0.2s ease, box-shadow 0.2s ease;
      }
      input[type="text"]:focus {
        outline: none;
        border-color: var(--accent);
        box-shadow: 0 0 0 3px rgba(31, 106, 122, 0.15);
      }
      input[type="text"].flash {
        animation: flash 500ms ease;
      }
      @keyframes flash {
        from { background: var(--flash); }
        to { background: #fff; }
      }
      .foot {
        margin-top: 14px;
        font-size: 0.85rem;
        color: var(--muted);
      }
      .live-query {
        margin-top: 18px;
        border: 1px dashed #b8b0a2;
        border-radius: 12px;
        padding: 12px;
        background: #fffdf8;
      }
      .live-query h2 {
        margin: 0 0 8px;
        font-size: 1rem;
      }
      .live-query p {
        margin: 0 0 10px;
        font-size: 0.9rem;
      }
      .query-stats {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        font-size: 0.88rem;
        color: #2c3a44;
        margin-bottom: 8px;
      }
      .query-status {
        font-size: 0.84rem;
        color: var(--muted);
        margin-bottom: 8px;
      }
      .query-list {
        margin: 0;
        padding-left: 18px;
        font-size: 0.9rem;
      }
      .query-list li {
        margin: 2px 0;
      }
      .query-controls {
        margin-top: 8px;
      }
      .query-controls button {
        border: 1px solid #9ea9b1;
        border-radius: 8px;
        background: #fff;
        color: #20313c;
        font-size: 0.85rem;
        padding: 6px 10px;
        cursor: pointer;
      }
      .query-controls button:hover {
        border-color: #60717f;
      }
      @media (max-width: 760px) {
        .grid {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <main class="panel">
      <h1>Durable Streams Local Live Demo</h1>
      <p>Client uses one Durable Streams long-poll per enabled field. Filtering happens by Durable Streams key.</p>
      <div class="meta">
        <span class="status-dot"></span>
        <span id="statusText">Connecting durable stream...</span>
      </div>

      <section class="grid">
        <article class="card">
          <div class="card-head">
            <label for="field1">Field 1</label>
            <label class="live-toggle"><input id="live-field1" data-live-field="field1" type="checkbox" checked /> live</label>
          </div>
          <input id="field1" data-field="field1" type="text" autocomplete="off" />
        </article>
        <article class="card">
          <div class="card-head">
            <label for="field2">Field 2</label>
            <label class="live-toggle"><input id="live-field2" data-live-field="field2" type="checkbox" checked /> live</label>
          </div>
          <input id="field2" data-field="field2" type="text" autocomplete="off" />
        </article>
      </section>

      <div class="foot">Each checked field maintains its own long poll to /pk/&lt;field&gt;. Unchecked fields do not have active subscriptions.</div>

      <section class="live-query">
        <h2>Live Query Subscription (LIVE V2)</h2>
        <p>This panel uses <code>/touch/wait</code> to subscribe for invalidation and reruns a derived query on touch.</p>
        <div class="query-stats">
          <span>Non-empty fields: <strong id="queryNonEmpty">0</strong></span>
          <span>Total chars: <strong id="queryChars">0</strong></span>
          <span>Waits: <strong id="queryWaits">0</strong></span>
          <span>Touched: <strong id="queryTouched">0</strong></span>
          <span>Timeouts: <strong id="queryTimeouts">0</strong></span>
          <span>Stale: <strong id="queryStale">0</strong></span>
        </div>
        <div id="queryStatus" class="query-status">Initializing live query…</div>
        <ul id="queryList" class="query-list"></ul>
        <div class="query-controls">
          <button id="queryRefresh" type="button">Rerun Query</button>
        </div>
      </section>
    </main>

    <script>
      const SOURCE_STREAM = ${streamJson};
      const UPDATES_STREAM = ${updatesJson};
      const LIVE_WAIT = ${liveWaitJson};
      const FIELD_IDS = ["field1", "field2"];

      const statusText = document.getElementById("statusText");
      const inputs = Object.fromEntries(FIELD_IDS.map((id) => [id, document.getElementById(id)]));
      const checkboxes = Object.fromEntries(FIELD_IDS.map((id) => [id, document.getElementById("live-" + id)]));
      const queryNonEmptyEl = document.getElementById("queryNonEmpty");
      const queryCharsEl = document.getElementById("queryChars");
      const queryWaitsEl = document.getElementById("queryWaits");
      const queryTouchedEl = document.getElementById("queryTouched");
      const queryTimeoutsEl = document.getElementById("queryTimeouts");
      const queryStaleEl = document.getElementById("queryStale");
      const queryStatusEl = document.getElementById("queryStatus");
      const queryListEl = document.getElementById("queryList");
      const queryRefreshButton = document.getElementById("queryRefresh");
      const SAME_ORIGIN = window.location.origin;

      function buildPollOrigins() {
        const seen = new Set();
        const out = [];
        const add = (origin) => {
          if (!origin || seen.has(origin)) return;
          seen.add(origin);
          out.push(origin);
        };

        add(SAME_ORIGIN);
        if (window.location.protocol === "http:") {
          const port = window.location.port;
          add("http://127.0.0.1:" + port);
          add("http://localhost:" + port);
          add("http://[::1]:" + port);
        }
        return out;
      }

      const POLL_ORIGINS = buildPollOrigins();
      const pollOriginByField = Object.fromEntries(FIELD_IDS.map((field, i) => [field, POLL_ORIGINS[i % POLL_ORIGINS.length]]));

      function currentLiveFields() {
        const out = [];
        for (const field of FIELD_IDS) {
          if (checkboxes[field] && checkboxes[field].checked) out.push(field);
        }
        return out;
      }

      function updateStatus() {
        const liveCount = currentLiveFields().length;
        statusText.textContent = "Durable Streams long polls active: " + liveCount + "/" + FIELD_IDS.length + " fields";
      }

      function flashInput(el) {
        el.classList.remove("flash");
        void el.offsetWidth;
        el.classList.add("flash");
      }

      async function sendEdit(field, value) {
        const res = await fetch("/api/edit", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ field, value }),
        });
        if (!res.ok) {
          const body = await res.text();
          console.error("edit failed", body);
        }
      }

      async function loadInitialState() {
        const res = await fetch("/api/state");
        const state = await res.json();
        for (const field of FIELD_IDS) {
          const input = inputs[field];
          if (!input) continue;
          const value = typeof state[field] === "string" ? state[field] : "";
          input.value = value;
        }
      }

      const subs = Object.fromEntries(
        FIELD_IDS.map((field) => [field, { field, offset: "now", running: false, abort: null, pollOrigin: pollOriginByField[field], failures: 0 }])
      );

      function applyFieldEvents(field, events) {
        const input = inputs[field];
        if (!input) return;
        for (const evt of events) {
          if (!evt || typeof evt !== "object") continue;
          const valueObj = evt.value;
          if (!valueObj || typeof valueObj !== "object") continue;
          const evtField = typeof valueObj.field === "string" ? valueObj.field : "";
          if (evtField !== field) continue;
          const nextValue = typeof valueObj.text === "string" ? valueObj.text : "";
          if (input.value !== nextValue) input.value = nextValue;
          flashInput(input);
        }
      }

      async function pollFieldLoop(field) {
        const sub = subs[field];
        if (!sub || sub.running) return;
        sub.running = true;
        try {
          for (;;) {
            const checkbox = checkboxes[field];
            if (!checkbox || !checkbox.checked) break;

            const controller = new AbortController();
            sub.abort = controller;
            const url =
              sub.pollOrigin +
              "/ds/v1/stream/" +
              encodeURIComponent(UPDATES_STREAM) +
              "/pk/" +
              encodeURIComponent(field) +
              "?offset=" +
              encodeURIComponent(sub.offset) +
              "&live=long-poll&timeout=30000&format=json";
            let res;
            try {
              res = await fetch(url, {
                method: "GET",
                cache: "no-store",
                signal: controller.signal,
              });
            } catch (err) {
              if (controller.signal.aborted) break;
              sub.failures = (sub.failures ?? 0) + 1;
              if (sub.failures >= 3 && sub.pollOrigin !== SAME_ORIGIN) {
                // Fallback to same-origin path if a loopback alias is unavailable.
                sub.pollOrigin = SAME_ORIGIN;
              }
              await new Promise((r) => setTimeout(r, 150));
              continue;
            } finally {
              if (sub.abort === controller) sub.abort = null;
            }

            sub.failures = 0;
            const nextOffset = res.headers.get("stream-next-offset");
            if (nextOffset && nextOffset.trim() !== "") sub.offset = nextOffset;
            if (res.status === 204) continue;
            if (!res.ok) {
              await new Promise((r) => setTimeout(r, 200));
              continue;
            }

            const body = await res.text();
            if (body.trim() === "") continue;
            let events;
            try {
              events = JSON.parse(body);
            } catch {
              continue;
            }
            if (!Array.isArray(events)) continue;
            applyFieldEvents(field, events);
          }
        } finally {
          sub.running = false;
          if (sub.abort) {
            sub.abort.abort();
            sub.abort = null;
          }
        }
      }

      function startEnabledPolls() {
        for (const field of FIELD_IDS) {
          const checkbox = checkboxes[field];
          if (checkbox && checkbox.checked) {
            pollFieldLoop(field).catch((err) => console.error(err));
          }
        }
      }

      const liveQuerySub = {
        cursor: "now",
        running: false,
        abort: null,
        waits: 0,
        touched: 0,
        timeouts: 0,
        stale: 0,
        lastWaitMs: 0,
      };

      function setQueryStatus(text) {
        if (queryStatusEl) queryStatusEl.textContent = text;
      }

      function renderLiveQuery(summary) {
        if (queryNonEmptyEl) queryNonEmptyEl.textContent = String(summary.nonEmptyCount ?? 0);
        if (queryCharsEl) queryCharsEl.textContent = String(summary.totalChars ?? 0);
        if (queryWaitsEl) queryWaitsEl.textContent = String(liveQuerySub.waits);
        if (queryTouchedEl) queryTouchedEl.textContent = String(liveQuerySub.touched);
        if (queryTimeoutsEl) queryTimeoutsEl.textContent = String(liveQuerySub.timeouts);
        if (queryStaleEl) queryStaleEl.textContent = String(liveQuerySub.stale);
        if (!queryListEl) return;

        queryListEl.innerHTML = "";
        const items = Array.isArray(summary.items) ? summary.items : [];
        if (items.length === 0) {
          const li = document.createElement("li");
          li.textContent = "(no non-empty fields)";
          queryListEl.appendChild(li);
          return;
        }
        for (const item of items) {
          const li = document.createElement("li");
          const field = typeof item?.field === "string" ? item.field : "?";
          const text = typeof item?.text === "string" ? item.text : "";
          li.textContent = field + " = " + JSON.stringify(text);
          queryListEl.appendChild(li);
        }
      }

      async function fetchLiveQuerySummary() {
        const res = await fetch("/api/live-query", { cache: "no-store" });
        if (!res.ok) {
          const text = await res.text();
          throw dsError("live query fetch failed: " + text);
        }
        return await res.json();
      }

      async function rerunLiveQuery(reason) {
        const summary = await fetchLiveQuerySummary();
        if (typeof summary.waitCursor === "string" && summary.waitCursor.trim() !== "" && liveQuerySub.cursor === "now") {
          liveQuerySub.cursor = summary.waitCursor;
        }
        renderLiveQuery(summary);
        setQueryStatus(
          "Live query rerun (" +
            reason +
            ") | nonEmpty=" +
            String(summary.nonEmptyCount ?? 0) +
            " | totalChars=" +
            String(summary.totalChars ?? 0)
        );
      }

      async function runLiveQueryLoop() {
        if (liveQuerySub.running) return;
        liveQuerySub.running = true;
        try {
          for (;;) {
            const controller = new AbortController();
            liveQuerySub.abort = controller;
            const startedAt = performance.now();
            let res;
            try {
              res = await fetch(
                "/ds/v1/stream/" + encodeURIComponent(LIVE_WAIT.sourceStream) + "/touch/wait",
                {
                  method: "POST",
                  headers: { "content-type": "application/json" },
                  signal: controller.signal,
                  body: JSON.stringify({
                    cursor: liveQuerySub.cursor,
                    timeoutMs: 30_000,
                    keys: LIVE_WAIT.keys,
                    keyIds: LIVE_WAIT.keyIds,
                    templateIdsUsed: [LIVE_WAIT.templateId],
                    interestMode: "fine",
                  }),
                }
              );
            } catch (err) {
              if (controller.signal.aborted) break;
              setQueryStatus("Live query wait error; retrying…");
              await new Promise((r) => setTimeout(r, 200));
              continue;
            } finally {
              if (liveQuerySub.abort === controller) liveQuerySub.abort = null;
            }

            liveQuerySub.waits += 1;
            liveQuerySub.lastWaitMs = Math.round(performance.now() - startedAt);
            if (!res.ok) {
              setQueryStatus("Live query wait HTTP " + res.status + "; retrying…");
              await new Promise((r) => setTimeout(r, 200));
              continue;
            }

            let payload;
            try {
              payload = await res.json();
            } catch {
              setQueryStatus("Live query wait returned invalid JSON; retrying…");
              await new Promise((r) => setTimeout(r, 200));
              continue;
            }

            if (typeof payload?.cursor === "string" && payload.cursor.trim() !== "") {
              liveQuerySub.cursor = payload.cursor;
            }

            if (payload?.stale === true) {
              liveQuerySub.stale += 1;
              await rerunLiveQuery("stale");
              setQueryStatus(
                "Live query stale -> reran (effective=" +
                  String(payload?.effectiveWaitKind ?? "?") +
                  ", wait=" +
                  String(liveQuerySub.lastWaitMs) +
                  "ms)"
              );
              continue;
            }

            if (payload?.touched === true) {
              liveQuerySub.touched += 1;
              await rerunLiveQuery("touched");
              setQueryStatus(
                "Live query touched -> reran (effective=" +
                  String(payload?.effectiveWaitKind ?? "?") +
                  ", wait=" +
                  String(liveQuerySub.lastWaitMs) +
                  "ms)"
              );
              continue;
            }

            liveQuerySub.timeouts += 1;
            if (queryWaitsEl) queryWaitsEl.textContent = String(liveQuerySub.waits);
            if (queryTimeoutsEl) queryTimeoutsEl.textContent = String(liveQuerySub.timeouts);
            setQueryStatus("Live query timeout (last wait " + String(liveQuerySub.lastWaitMs) + "ms), continuing…");
          }
        } finally {
          liveQuerySub.running = false;
          liveQuerySub.abort = null;
        }
      }

      for (const field of FIELD_IDS) {
        const input = inputs[field];
        const checkbox = checkboxes[field];
        if (!input || !checkbox) continue;

        input.addEventListener("input", () => {
          sendEdit(field, input.value).catch((err) => console.error(err));
        });

        checkbox.addEventListener("change", () => {
          const sub = subs[field];
          if (!checkbox.checked && sub && sub.abort) sub.abort.abort();
          updateStatus();
          if (checkbox.checked) {
            pollFieldLoop(field).catch((err) => console.error(err));
          }
        });
      }

      if (queryRefreshButton) {
        queryRefreshButton.addEventListener("click", () => {
          rerunLiveQuery("manual").catch((err) => {
            console.error(err);
            setQueryStatus("Manual rerun failed");
          });
        });
      }

      loadInitialState()
        .then(async () => {
          updateStatus();
          startEnabledPolls();
          await rerunLiveQuery("initial");
          runLiveQueryLoop().catch((err) => {
            console.error(err);
            setQueryStatus("Live query loop stopped");
          });
        })
        .catch((err) => {
          console.error(err);
          statusText.textContent = "Failed to load initial state";
          setQueryStatus("Failed to initialize live query");
        });

      window.addEventListener("beforeunload", () => {
        for (const field of FIELD_IDS) {
          const sub = subs[field];
          if (sub && sub.abort) sub.abort.abort();
        }
        if (liveQuerySub.abort) liveQuerySub.abort.abort();
      });
    </script>
  </body>
</html>`;
}

async function publishFieldUpdate(opts: {
  baseUrl: string;
  updateStream: string;
  field: FieldId;
  nextValue: string;
  previousValue: string;
}): Promise<void> {
  const now = new Date().toISOString();
  const txid = `${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
  await appendEvents(opts.baseUrl, opts.updateStream, [
    {
      type: UPDATE_ENTITY,
      key: opts.field,
      value: {
        field: opts.field,
        text: opts.nextValue,
      },
      oldValue: {
        field: opts.field,
        text: opts.previousValue,
      },
      headers: {
        operation: "update",
        txid,
        timestamp: now,
      },
    },
  ]);
}

async function runAppkitPublisher(opts: {
  baseUrl: string;
  sourceStream: string;
  updateStream: string;
  templateId: string;
  watchByField: Record<FieldId, { key: string; keyId: number }>;
  latestValues: Record<FieldId, string>;
  signal: AbortSignal;
}): Promise<void> {
  const cursors = Object.fromEntries(FIELD_IDS.map((field) => [field, "now"])) as Record<FieldId, string>;

  const loopForField = async (field: FieldId): Promise<void> => {
    while (!opts.signal.aborted) {
      const prevCursor = cursors[field];
      let res: any;
      try {
        res = await fetchJson(`${opts.baseUrl}/v1/stream/${encodeURIComponent(opts.sourceStream)}/touch/wait`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          signal: opts.signal,
          body: JSON.stringify({
            cursor: prevCursor,
            timeoutMs: 30_000,
            keys: [opts.watchByField[field].key],
            keyIds: [opts.watchByField[field].keyId],
            templateIdsUsed: [opts.templateId],
            interestMode: "fine",
          }),
        });
      } catch (err: any) {
        if (opts.signal.aborted || err?.name === "AbortError") return;
        await Bun.sleep(50);
        continue;
      }

      const nextCursor = typeof res?.cursor === "string" && res.cursor.trim() !== "" ? res.cursor : prevCursor;
      cursors[field] = nextCursor;
      if (res?.stale === true) continue;
      if (res?.touched !== true) continue;

      const latest = await readLatestFieldValue(opts.baseUrl, opts.sourceStream, field);
      const nextValue = latest ?? "";
      if (nextValue === opts.latestValues[field]) continue;

      const previous = opts.latestValues[field];
      opts.latestValues[field] = nextValue;
      await publishFieldUpdate({
        baseUrl: opts.baseUrl,
        updateStream: opts.updateStream,
        field,
        nextValue,
        previousValue: previous,
      });
    }
  };

  await Promise.all(FIELD_IDS.map((field) => loopForField(field)));
}

async function main(): Promise<void> {
  if (hasFlag("--help") || hasFlag("-h")) {
    // eslint-disable-next-line no-console
    console.log(`
Live Fields Demo (local Durable Streams + server-side AppKit waits)

Usage:
  bun run experiments/demo/live_fields_app.ts [options]

Options:
  --port <n>             Demo web server port (default: 3000)
  --ds-port <n>          Local Durable Streams port (default: 0 = ephemeral)
  --ds-name <name>       Local Durable Streams server name (default: live-fields-demo)
  --stream <name>        Source stream for field state events (default: demo.live.fields)
  --updates-stream <n>   Update stream for client long-poll subscriptions (default: <stream>.updates)
  --reset                Reset source + touch companion + updates stream before startup
`);
    process.exit(0);
  }

  const args = parseArgs();
  const ds = await startOrReuseLocalDs(args.dsName, args.dsPort);
  const baseUrl = ds.baseUrl;

  const touchStream = defaultTouchStreamName(args.stream);
  if (args.reset) {
    await deleteStream(baseUrl, args.stream);
    await deleteStream(baseUrl, touchStream);
    await deleteStream(baseUrl, args.updatesStream);
  }

  await ensureStream(baseUrl, args.stream);
  await ensureSourceSchemaAndInterpreter(baseUrl, args.stream);
  await activateTemplate(baseUrl, args.stream);

  await ensureStream(baseUrl, args.updatesStream);
  await ensureUpdateStreamSchema(baseUrl, args.updatesStream);

  const fieldValues = await ensureInitialRows(baseUrl, args.stream);

  const templateId = templateIdFor(ENTITY, ["field"]);
  const watchByField: Record<FieldId, { key: string; keyId: number }> = {
    field1: { key: watchKeyFor(templateId, ["field1"]), keyId: watchKeyIdFor(templateId, ["field1"]) },
    field2: { key: watchKeyFor(templateId, ["field2"]), keyId: watchKeyIdFor(templateId, ["field2"]) },
  };

  const appkitAbort = new AbortController();
  void runAppkitPublisher({
    baseUrl,
    sourceStream: args.stream,
    updateStream: args.updatesStream,
    templateId,
    watchByField,
    latestValues: fieldValues,
    signal: appkitAbort.signal,
  }).catch((err) => {
    if (!appkitAbort.signal.aborted) {
      // eslint-disable-next-line no-console
      console.error("[demo][live-fields] appkit publisher crashed:", err);
    }
  });

  const html = renderHtml({
    stream: args.stream,
    updatesStream: args.updatesStream,
    templateId,
    watchByField,
  });

  const server = Bun.serve({
    port: args.port,
    idleTimeout: 65,
    fetch: async (req) => {
      const url = new URL(req.url);

      if (req.method === "GET" && url.pathname === "/") {
        return new Response(html, {
          headers: {
            "content-type": "text/html; charset=utf-8",
            "cache-control": "no-store",
          },
        });
      }

      if (req.method === "GET" && url.pathname === "/health") {
        return Response.json({
          ok: true,
          stream: args.stream,
          updatesStream: args.updatesStream,
          dsUrl: baseUrl,
        });
      }

      if (req.method === "GET" && url.pathname === "/api/state") {
        return Response.json(fieldValues);
      }

      if (req.method === "GET" && url.pathname === "/api/live-query") {
        const summary = buildLiveQuerySummary(fieldValues);
        let waitCursor = "now";
        try {
          const meta = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(args.stream)}/touch/meta`, {
            method: "GET",
          });
          if (typeof meta?.cursor === "string" && meta.cursor.trim() !== "") {
            waitCursor = meta.cursor;
          }
        } catch {
          // keep best-effort behavior for demo
        }

        return Response.json({
          ...summary,
          waitCursor,
          queriedAtMs: Date.now(),
        });
      }

      if (req.method === "POST" && url.pathname === "/api/edit") {
        let body: any;
        try {
          body = await req.json();
        } catch {
          return new Response("invalid json", { status: 400 });
        }

        const fieldRaw = typeof body?.field === "string" ? body.field.trim() : "";
        const field = asFieldId(fieldRaw);
        if (!field) return new Response("invalid field", { status: 400 });

        const value = typeof body?.value === "string" ? body.value : "";
        const previousText = fieldValues[field] ?? "";
        if (value === previousText) return new Response(null, { status: 204 });

        await appendFieldUpdate(baseUrl, args.stream, { field, value }, previousText);
        fieldValues[field] = value;

        await publishFieldUpdate({
          baseUrl,
          updateStream: args.updatesStream,
          field,
          nextValue: value,
          previousValue: previousText,
        });

        return new Response(null, { status: 204 });
      }

      if (url.pathname.startsWith("/ds/")) {
        const corsHeaders = {
          "access-control-allow-origin": "*",
          "access-control-allow-methods": "GET,POST,PUT,DELETE,HEAD,OPTIONS",
          "access-control-allow-headers":
            "content-type,if-none-match,stream-key,stream-seq,stream-ttl,stream-expires-at,stream-format",
          "access-control-expose-headers":
            "stream-next-offset,stream-end-offset,stream-up-to-date,stream-closed,stream-cursor,stream-expires-at,etag,producer-epoch,producer-seq",
        };
        if (req.method === "OPTIONS") {
          return new Response(null, {
            status: 204,
            headers: corsHeaders,
          });
        }

        const target = `${baseUrl}${url.pathname.slice(3)}${url.search}`;
        const headers = new Headers(req.headers);
        headers.delete("host");

        const proxied = await fetch(target, {
          method: req.method,
          headers,
          body: req.method === "GET" || req.method === "HEAD" ? undefined : req.body,
          signal: req.signal,
        });

        if (proxied.headers.get("content-encoding")) {
          const outHeaders = new Headers(proxied.headers);
          outHeaders.delete("content-encoding");
          outHeaders.delete("content-length");
          for (const [k, v] of Object.entries(corsHeaders)) outHeaders.set(k, v);
          return new Response(proxied.body, {
            status: proxied.status,
            statusText: proxied.statusText,
            headers: outHeaders,
          });
        }

        const outHeaders = new Headers(proxied.headers);
        for (const [k, v] of Object.entries(corsHeaders)) outHeaders.set(k, v);
        return new Response(proxied.body, {
          status: proxied.status,
          statusText: proxied.statusText,
          headers: outHeaders,
        });
      }

      return new Response("not found", { status: 404 });
    },
  });

  // eslint-disable-next-line no-console
  console.log(`[demo][live-fields] local durable streams: ${baseUrl} (name=${args.dsName})`);
  // eslint-disable-next-line no-console
  console.log(`[demo][live-fields] source=${args.stream} updates=${args.updatesStream} templateId=${templateId}`);
  // eslint-disable-next-line no-console
  console.log(`[demo][live-fields] app: http://127.0.0.1:${server.port}`);

  const shutdown = async (): Promise<void> => {
    appkitAbort.abort();
    try {
      server.stop(true);
    } catch {
      // ignore
    }
    await ds.close();
    process.exit(0);
  };

  process.on("SIGINT", () => {
    void shutdown();
  });
  process.on("SIGTERM", () => {
    void shutdown();
  });
}

await main();
