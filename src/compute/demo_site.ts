import { dsError } from "../util/ds_error.ts";

type BuiltAsset = {
  bytes: Uint8Array | ArrayBuffer;
  contentType: string;
};

export type PrebuiltStudioAssets = {
  appScript: string;
  appStyles: string;
  builtAssets: Map<string, BuiltAsset>;
};

type GenerateJobStatus = "pending" | "running" | "succeeded" | "failed";

type GenerateJobState = {
  id: string;
  stream: string;
  total: number;
  inserted: number;
  batchSize: number;
  status: GenerateJobStatus;
  error: string | null;
  startedAt: string;
  finishedAt: string | null;
};

export type ComputeDemoSite = {
  close(): void;
  fetch(request: Request): Promise<Response>;
};

type GenerateStreamsTarget = {
  appendGenerateBatch?: (stream: string, events: Array<Record<string, unknown>>) => Promise<void>;
  beginGenerateJob?: (stream: string) => void;
  endGenerateJob?: (stream: string) => void;
  ensureGenerateStream?: (stream: string) => Promise<void>;
  fetch(request: Request): Promise<Response>;
};

type CreateComputeDemoSiteOptions = {
  bootId?: string;
  studioAssets: PrebuiltStudioAssets;
  streamsApp: GenerateStreamsTarget;
};

const APP_CACHE_CONTROL = "public, max-age=31536000, immutable";
const NO_STORE_CACHE_CONTROL = "no-cache, no-store, must-revalidate";
const STUDIO_ROOT_PATH = "/studio";
const STUDIO_STREAMS_PROXY_BASE_PATH = "/studio/api/streams";
const STUDIO_CONFIG_PATH = "/api/config";
const STUDIO_QUERY_PATH = "/api/query";
const STUDIO_AI_PATH = "/api/ai";
const GENERATE_ROOT_PATH = "/generate";
const GENERATE_JOBS_BASE_PATH = "/api/generate/jobs";
const JOB_RETENTION_MS = 10 * 60_000;
const GENERATE_BUTTON_COUNTS = new Set([1_000, 10_000, 100_000]);
const GENERATE_BATCH_TARGET_BYTES = 256 * 1024;
const GENERATE_BATCH_MIN_EVENTS = 100;
const GENERATE_BATCH_MAX_EVENTS = 500;
const GENERATE_GC_MIN_TOTAL_EVENTS = 100_000;
const GENERATE_GC_ROW_INTERVAL = 50_000;
const DEFAULT_GENERATE_STREAM = "demo-app";
const GENERATE_STREAM_NAME_MAX_LENGTH = 128;
const GENERATE_STREAM_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._/-]*$/;

const GENERATE_METHODS = ["GET", "POST", "PUT", "PATCH"] as const;
const GENERATE_PATHS = [
  "/api/orders",
  "/api/orders/:id",
  "/api/checkout",
  "/api/invoices",
  "/api/shipments/:id",
  "/api/catalog/search",
] as const;
const GENERATE_SERVICES = [
  "billing",
  "checkout",
  "fulfillment",
  "identity",
  "inventory",
  "search",
] as const;
const GENERATE_ENVIRONMENTS = ["prod", "staging"] as const;
const GENERATE_REGIONS = ["cdg", "fra", "iad"] as const;

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function jsonResponse(
  status: number,
  payload: unknown,
  headers?: HeadersInit,
): Response {
  return new Response(JSON.stringify(payload), {
    headers: {
      "cache-control": NO_STORE_CACHE_CONTROL,
      "content-type": "application/json; charset=utf-8",
      ...headers,
    },
    status,
  });
}

function methodNotAllowed(allow: string): Response {
  return new Response("Method Not Allowed", {
    headers: {
      Allow: allow,
      "cache-control": NO_STORE_CACHE_CONTROL,
    },
    status: 405,
  });
}

function collectGenerateBatchMemory(job: GenerateJobState, previousInserted: number): void {
  if (job.total < GENERATE_GC_MIN_TOTAL_EVENTS) return;
  const crossedBoundary =
    Math.floor(previousInserted / GENERATE_GC_ROW_INTERVAL) !==
    Math.floor(job.inserted / GENERATE_GC_ROW_INTERVAL);
  if (!crossedBoundary && job.inserted < job.total) return;

  const gc = (globalThis as { Bun?: { gc?: (force?: boolean) => void } }).Bun?.gc;
  if (typeof gc !== "function") return;

  try {
    gc(true);
  } catch {
    return;
  }
}

function responseBodyForMethod(
  request: Request,
  body: BodyInit | null,
): BodyInit | null {
  return request.method === "HEAD" ? null : body;
}

function textResponse(
  request: Request,
  body: string,
  init: ResponseInit & { contentType: string },
): Response {
  const headers = new Headers(init.headers);
  if (!headers.has("cache-control")) {
    headers.set("cache-control", NO_STORE_CACHE_CONTROL);
  }
  headers.set("content-type", init.contentType);

  return new Response(responseBodyForMethod(request, body), {
    headers,
    status: init.status,
  });
}

function normalizeBuiltAssetPath(pathname: string): string[] {
  const paths = [pathname];
  try {
    const decoded = decodeURIComponent(pathname);
    if (decoded !== pathname) {
      paths.push(decoded);
    }
  } catch {
    // Ignore malformed URI sequences and keep the raw pathname only.
  }

  for (const candidate of [...paths]) {
    if (candidate.startsWith(`${STUDIO_ROOT_PATH}/`)) {
      paths.push(candidate.slice(STUDIO_ROOT_PATH.length));
    } else if (candidate.startsWith("/") && candidate !== STUDIO_ROOT_PATH) {
      paths.push(`${STUDIO_ROOT_PATH}${candidate}`);
    }
  }
  return [...new Set(paths)];
}

function createEntropyHex(stream: string, index: number, byteLength = 64): string {
  let state = 0x811c9dc5;
  for (let position = 0; position < stream.length; position += 1) {
    state ^= stream.charCodeAt(position);
    state = Math.imul(state, 0x01000193);
  }
  state ^= index >>> 0;
  state = Math.imul(state, 0x9e3779b1);

  const alphabet = "0123456789abcdef";
  let out = "";
  for (let emitted = 0; emitted < byteLength; emitted += 1) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    const value = state >>> 0;
    out += alphabet[(value >>> 4) & 0x0f];
    out += alphabet[value & 0x0f];
  }
  return out;
}

function createStudioHtmlDocument(): string {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Streams Studio</title>
    <link rel="stylesheet" href="${STUDIO_ROOT_PATH}/app.css" />
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="${STUDIO_ROOT_PATH}/app.js"></script>
  </body>
</html>`;
}

function createLandingHtmlDocument(): string {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Streams Compute Demo</title>
    <style>
      :root {
        --bg: #071421;
        --bg-accent: #12304a;
        --panel: rgba(7, 20, 33, 0.82);
        --panel-border: rgba(148, 184, 210, 0.26);
        --text: #e6f1fb;
        --muted: #9fb6cb;
        --primary: #66d9ff;
        --primary-strong: #b5ffcc;
        --shadow: 0 24px 70px rgba(1, 8, 16, 0.45);
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        padding: 32px;
        color: var(--text);
        background:
          radial-gradient(circle at top left, rgba(102, 217, 255, 0.2), transparent 34%),
          radial-gradient(circle at bottom right, rgba(181, 255, 204, 0.18), transparent 30%),
          linear-gradient(135deg, var(--bg), var(--bg-accent));
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }
      main {
        width: min(980px, 100%);
        padding: 32px;
        border: 1px solid var(--panel-border);
        border-radius: 28px;
        background: var(--panel);
        backdrop-filter: blur(24px);
        box-shadow: var(--shadow);
      }
      h1 {
        margin: 0;
        font-size: clamp(2.4rem, 5vw, 4rem);
        line-height: 0.96;
        letter-spacing: -0.04em;
      }
      p {
        margin: 16px 0 0;
        max-width: 56ch;
        color: var(--muted);
        font-size: 1.05rem;
        line-height: 1.6;
      }
      .grid {
        display: grid;
        gap: 18px;
        margin-top: 28px;
        grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      }
      .card {
        display: block;
        padding: 22px;
        border-radius: 22px;
        border: 1px solid rgba(148, 184, 210, 0.18);
        background: linear-gradient(180deg, rgba(10, 28, 43, 0.92), rgba(6, 16, 28, 0.92));
        color: inherit;
        text-decoration: none;
        transition: transform 140ms ease, border-color 140ms ease, background 140ms ease;
      }
      .card:hover {
        transform: translateY(-2px);
        border-color: rgba(102, 217, 255, 0.5);
        background: linear-gradient(180deg, rgba(15, 38, 57, 0.96), rgba(7, 18, 30, 0.96));
      }
      .eyebrow {
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(102, 217, 255, 0.12);
        color: var(--primary);
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
      }
      .title {
        margin-top: 14px;
        font-size: 1.25rem;
        font-weight: 700;
      }
      .summary {
        margin-top: 10px;
        color: var(--muted);
        line-height: 1.55;
      }
      .go {
        margin-top: 18px;
        color: var(--primary-strong);
        font-weight: 700;
      }
    </style>
  </head>
  <body>
    <main>
      <span class="eyebrow">Compute Demo</span>
      <h1>Streams, with Studio and an evlog generator.</h1>
      <p>
        This deployment keeps the normal Streams API on <code>/v1/*</code>, serves Prisma Studio on <code>/studio</code>,
        and adds a write generator on <code>/generate</code> for bulk evlog ingestion against the same server.
      </p>
      <div class="grid">
        <a class="card" href="${STUDIO_ROOT_PATH}">
          <span class="eyebrow">Inspect</span>
          <div class="title">Open Studio</div>
          <div class="summary">
            Browse streams, inspect details, and query the live server through the bundled Studio UI.
          </div>
          <div class="go">Launch Studio</div>
        </a>
        <a class="card" href="${GENERATE_ROOT_PATH}">
          <span class="eyebrow">Ingest</span>
          <div class="title">Open Generator</div>
          <div class="summary">
            Create a fresh evlog stream and insert 1k, 10k, or 100k canonical request events with live progress.
          </div>
          <div class="go">Launch Generator</div>
        </a>
      </div>
    </main>
  </body>
</html>`;
}

function createGenerateHtmlDocument(): string {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Streams Evlog Generator</title>
    <style>
      :root {
        --page-top: #07121e;
        --page-bottom: #10283d;
        --panel: rgba(7, 18, 30, 0.84);
        --panel-border: rgba(150, 194, 220, 0.22);
        --text: #e9f3fb;
        --muted: #a6bccd;
        --button: #d9ff70;
        --button-hover: #f1ffb8;
        --button-text: #09131d;
        --track: rgba(163, 186, 201, 0.16);
        --fill-start: #66d9ff;
        --fill-end: #b5ffcc;
        --danger: #ff9f8e;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        min-height: 100vh;
        padding: 28px;
        color: var(--text);
        background:
          radial-gradient(circle at top left, rgba(102, 217, 255, 0.16), transparent 32%),
          radial-gradient(circle at bottom right, rgba(217, 255, 112, 0.14), transparent 24%),
          linear-gradient(180deg, var(--page-top), var(--page-bottom));
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }
      main {
        width: min(920px, 100%);
        margin: 0 auto;
        padding: 30px;
        border-radius: 28px;
        border: 1px solid var(--panel-border);
        background: var(--panel);
        backdrop-filter: blur(24px);
        box-shadow: 0 24px 80px rgba(0, 0, 0, 0.28);
      }
      h1 {
        margin: 0;
        font-size: clamp(2rem, 4vw, 3.4rem);
        line-height: 0.95;
        letter-spacing: -0.04em;
      }
      p {
        margin: 16px 0 0;
        color: var(--muted);
        line-height: 1.6;
        max-width: 58ch;
      }
      .actions {
        display: grid;
        gap: 12px;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        margin-top: 26px;
      }
      .field {
        display: grid;
        gap: 8px;
        margin-top: 24px;
      }
      .field label {
        color: var(--muted);
        font-size: 0.82rem;
        font-weight: 800;
        letter-spacing: 0.12em;
        text-transform: uppercase;
      }
      .field input {
        border: 1px solid rgba(255, 255, 255, 0.18);
        border-radius: 16px;
        background: rgba(255, 255, 255, 0.08);
        color: var(--text);
        font: inherit;
        font-size: 1rem;
        padding: 13px 15px;
        outline: none;
      }
      .field input:focus {
        border-color: rgba(181, 255, 204, 0.78);
        box-shadow: 0 0 0 4px rgba(181, 255, 204, 0.12);
      }
      .hint {
        color: var(--muted);
        font-size: 0.88rem;
      }
      button {
        border: 0;
        border-radius: 18px;
        padding: 18px 20px;
        font: inherit;
        font-weight: 800;
        letter-spacing: 0.02em;
        background: linear-gradient(135deg, var(--button), #9cf7ff);
        color: var(--button-text);
        cursor: pointer;
        transition: transform 140ms ease, filter 140ms ease, opacity 140ms ease;
      }
      button:hover:enabled {
        transform: translateY(-1px);
        filter: brightness(1.05);
      }
      button:disabled {
        cursor: not-allowed;
        opacity: 0.5;
      }
      .panel {
        margin-top: 24px;
        padding: 20px;
        border-radius: 22px;
        border: 1px solid rgba(150, 194, 220, 0.14);
        background: rgba(9, 25, 39, 0.72);
      }
      .label {
        font-size: 0.78rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #73dfff;
      }
      .status {
        margin-top: 10px;
        font-size: 1.2rem;
        font-weight: 700;
      }
      .meta {
        display: grid;
        gap: 10px 18px;
        margin-top: 16px;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        color: var(--muted);
      }
      .meta strong {
        display: block;
        margin-bottom: 4px;
        color: var(--text);
        font-size: 0.9rem;
      }
      .progress {
        margin-top: 18px;
        height: 16px;
        border-radius: 999px;
        overflow: hidden;
        background: var(--track);
      }
      .progress > div {
        height: 100%;
        width: 0%;
        background: linear-gradient(90deg, var(--fill-start), var(--fill-end));
        transition: width 160ms ease;
      }
      .numbers {
        margin-top: 12px;
        display: flex;
        justify-content: space-between;
        gap: 16px;
        color: var(--muted);
        font-variant-numeric: tabular-nums;
      }
      .links {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-top: 18px;
      }
      .links a {
        color: #b5ffcc;
        font-weight: 700;
        text-decoration: none;
      }
      .links a:hover {
        text-decoration: underline;
      }
      .error {
        margin-top: 16px;
        color: var(--danger);
        white-space: pre-wrap;
      }
    </style>
  </head>
  <body>
    <main>
      <div class="label">Evlog Ingest</div>
      <h1>Generate canonical request events.</h1>
      <p>
        Each run uses the selected <code>application/json</code> stream, installs the <code>evlog</code> profile,
        and appends the selected number of events in server-side chunks sized for steady progress updates.
      </p>

      <div class="field">
        <label for="streamInput">Stream name</label>
        <input id="streamInput" name="stream" type="text" value="${DEFAULT_GENERATE_STREAM}" autocomplete="off" spellcheck="false" maxlength="${GENERATE_STREAM_NAME_MAX_LENGTH}" />
        <div class="hint">Use letters, numbers, dot, underscore, slash, or hyphen. Default: <code>${DEFAULT_GENERATE_STREAM}</code>.</div>
      </div>

      <div class="actions">
        <button type="button" data-count="1000">Insert 1k</button>
        <button type="button" data-count="10000">Insert 10k</button>
        <button type="button" data-count="100000">Insert 100k</button>
      </div>

      <section class="panel">
        <div class="label">Progress</div>
        <div id="status" class="status">Ready to start.</div>
        <div class="progress"><div id="progressFill"></div></div>
        <div class="numbers">
          <div id="progressNumbers">0 / 0</div>
          <div id="progressPercent">0%</div>
        </div>
        <div class="meta">
          <div>
            <strong>Stream</strong>
            <span id="streamName">Not started</span>
          </div>
          <div>
            <strong>Batch Size</strong>
            <span id="batchSize">-</span>
          </div>
          <div>
            <strong>Status</strong>
            <span id="jobState">idle</span>
          </div>
        </div>
        <div class="links">
          <a href="${STUDIO_ROOT_PATH}">Open Studio</a>
          <a id="detailsLink" href="#" hidden>Open Stream Details</a>
        </div>
        <div id="error" class="error" hidden></div>
      </section>
    </main>

    <script>
      const buttons = Array.from(document.querySelectorAll("button[data-count]"));
      const statusEl = document.getElementById("status");
      const fillEl = document.getElementById("progressFill");
      const progressNumbersEl = document.getElementById("progressNumbers");
      const progressPercentEl = document.getElementById("progressPercent");
      const streamNameEl = document.getElementById("streamName");
      const streamInputEl = document.getElementById("streamInput");
      const batchSizeEl = document.getElementById("batchSize");
      const jobStateEl = document.getElementById("jobState");
      const errorEl = document.getElementById("error");
      const detailsLink = document.getElementById("detailsLink");

      let pollTimer = null;
      let currentJobId = null;

      const setButtonsDisabled = (disabled) => {
        for (const button of buttons) {
          button.disabled = disabled;
        }
        streamInputEl.disabled = disabled;
      };

      const readStreamName = () => streamInputEl.value.trim();

      const renderJob = (job) => {
        const total = job?.total ?? 0;
        const inserted = job?.inserted ?? 0;
        const percent = total > 0 ? Math.min(100, (inserted / total) * 100) : 0;
        statusEl.textContent =
          job == null
            ? "Ready to start."
            : job.status === "failed"
              ? "Insert failed."
              : job.status === "succeeded"
                ? "Insert finished."
                : "Insert running.";
        fillEl.style.width = percent.toFixed(2) + "%";
        progressNumbersEl.textContent = inserted.toLocaleString() + " / " + total.toLocaleString();
        progressPercentEl.textContent = percent.toFixed(1) + "%";
        streamNameEl.textContent = job?.stream ?? "Not started";
        batchSizeEl.textContent = job?.batchSize ? job.batchSize.toLocaleString() + " events" : "-";
        jobStateEl.textContent = job?.status ?? "idle";
        errorEl.hidden = !(job?.error);
        errorEl.textContent = job?.error ?? "";

        if (job?.stream) {
          detailsLink.hidden = false;
          detailsLink.href = "/v1/stream/" + encodeURIComponent(job.stream) + "/_details";
        } else {
          detailsLink.hidden = true;
          detailsLink.href = "#";
        }

        const active = job != null && (job.status === "pending" || job.status === "running");
        setButtonsDisabled(active);
      };

      const stopPolling = () => {
        if (pollTimer !== null) {
          clearTimeout(pollTimer);
          pollTimer = null;
        }
      };

      const schedulePoll = () => {
        stopPolling();
        if (!currentJobId) {
          return;
        }
        pollTimer = setTimeout(() => void pollJob(), 300);
      };

      const pollJob = async () => {
        if (!currentJobId) {
          return;
        }
        const response = await fetch("${GENERATE_JOBS_BASE_PATH}/" + encodeURIComponent(currentJobId), {
          cache: "no-store",
        });
        if (!response.ok) {
          renderJob({
            error: "Failed to read job status (" + response.status + " " + response.statusText + ").",
            status: "failed",
          });
          currentJobId = null;
          return;
        }
        const payload = await response.json();
        renderJob(payload.job);
        if (payload.job && (payload.job.status === "pending" || payload.job.status === "running")) {
          schedulePoll();
        } else {
          currentJobId = null;
        }
      };

      const startJob = async (count) => {
        const stream = readStreamName();
        if (!stream) {
          renderJob({
            error: "Enter a stream name before starting an insert.",
            status: "failed",
          });
          streamInputEl.focus();
          return;
        }

        stopPolling();
        currentJobId = null;
        renderJob({
          batchSize: 0,
          error: null,
          inserted: 0,
          status: "pending",
          stream,
          total: count,
        });
        const response = await fetch("${GENERATE_JOBS_BASE_PATH}", {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({ count, stream }),
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          renderJob({
            error: payload?.error ?? ("Failed to start job (" + response.status + " " + response.statusText + ")."),
            status: "failed",
          });
          return;
        }
        currentJobId = payload.job?.id ?? null;
        renderJob(payload.job);
        if (currentJobId) {
          schedulePoll();
        }
      };

      for (const button of buttons) {
        button.addEventListener("click", () => {
          const count = Number(button.dataset.count);
          void startJob(count);
        });
      }

      renderJob(null);
    </script>
  </body>
</html>`;
}

function createConfigPayload(bootId: string): {
  ai: { enabled: boolean };
  bootId: string;
  database: { enabled: boolean };
  streams: { url: string };
} {
  return {
    ai: { enabled: false },
    bootId,
    database: { enabled: false },
    streams: { url: STUDIO_STREAMS_PROXY_BASE_PATH },
  };
}

function buildGenerateEvent(stream: string, index: number, baseMs: number): Record<string, unknown> {
  const method = GENERATE_METHODS[index % GENERATE_METHODS.length];
  const service = GENERATE_SERVICES[index % GENERATE_SERVICES.length];
  const environment = GENERATE_ENVIRONMENTS[index % GENERATE_ENVIRONMENTS.length];
  const region = GENERATE_REGIONS[index % GENERATE_REGIONS.length];
  const path = GENERATE_PATHS[index % GENERATE_PATHS.length];
  const timestamp = new Date(baseMs + index * 1_000).toISOString();
  const status =
    index % 41 === 0 ? 503 : index % 17 === 0 ? 429 : index % 7 === 0 ? 404 : 200;
  const isError = status >= 500;
  const isWarn = !isError && status >= 400;

  const entropyHex = createEntropyHex(stream, index, 96);

  return {
    timestamp,
    requestId: `${stream}-req-${index.toString().padStart(8, "0")}`,
    traceContext: {
      traceId: `${stream}-trace-${Math.floor(index / 4)
        .toString()
        .padStart(6, "0")}`,
      spanId: `${service}-span-${index.toString().padStart(8, "0")}`,
    },
    method,
    path,
    service,
    environment,
    version: "compute-demo-v1",
    region,
    status,
    duration: 22 + (index % 900),
    message: isError
      ? "Upstream dependency timed out"
      : isWarn
        ? "Request rejected by policy"
        : "Request completed",
    why: isError
      ? "Payment provider exceeded deadline budget"
      : isWarn
        ? "Concurrency limiter rejected this request"
        : undefined,
    fix: isError
      ? "Retry this operation with exponential backoff"
      : isWarn
        ? "Reduce request rate or retry after the limiter window"
        : undefined,
    link:
      status >= 400
        ? `https://example.internal/runbooks/${service}/${status}`
        : undefined,
    sampling: {
      kept: true,
      source: "compute-demo-generate",
    },
    tenant: `tenant-${(index % 24).toString().padStart(2, "0")}`,
    host: `${service}-${region}-${index % 6}`,
    releaseChannel: environment === "prod" ? "stable" : "preview",
    context: {
      actor: {
        id: `user-${(index % 5_000).toString().padStart(5, "0")}`,
        plan: index % 13 === 0 ? "enterprise" : "pro",
      },
      fingerprint: entropyHex,
      request: {
        bytes: 512 + (index % 8_192),
        routeGroup: path.split("/")[2] ?? "root",
        traceToken: entropyHex.slice(0, 48),
      },
    },
  };
}

async function responseText(response: Response): Promise<string> {
  const text = await response.text();
  return text.trim();
}

function assertAllowedGenerateCount(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    return null;
  }
  return GENERATE_BUTTON_COUNTS.has(value) ? value : null;
}

function normalizeGenerateStreamName(value: unknown): string | null {
  if (value == null) {
    return DEFAULT_GENERATE_STREAM;
  }
  if (typeof value !== "string") {
    return null;
  }

  const stream = value.trim();
  if (
    stream.length === 0 ||
    stream.length > GENERATE_STREAM_NAME_MAX_LENGTH ||
    !GENERATE_STREAM_NAME_PATTERN.test(stream)
  ) {
    return null;
  }

  return stream;
}

export function createComputeDemoSite(
  options: CreateComputeDemoSiteOptions,
): ComputeDemoSite {
  const bootId = options.bootId ?? crypto.randomUUID();
  const jobs = new Map<string, GenerateJobState>();
  const cleanupTimers = new Set<Timer>();
  const studioAssets = options.studioAssets;

  const clearTimer = (timer: Timer): void => {
    cleanupTimers.delete(timer);
    clearTimeout(timer);
  };

  const scheduleCleanup = (jobId: string): void => {
    const timer = setTimeout(() => {
      jobs.delete(jobId);
      clearTimer(timer);
    }, JOB_RETENTION_MS);
    (timer as { unref?: () => void }).unref?.();
    cleanupTimers.add(timer);
  };

  const writeHtml = (request: Request, html: string): Response =>
    textResponse(request, html, {
      contentType: "text/html; charset=utf-8",
      headers: {
        "cache-control": NO_STORE_CACHE_CONTROL,
      },
      status: 200,
    });

  const writeAsset = (request: Request, body: BodyInit, contentType: string): Response =>
    new Response(responseBodyForMethod(request, body), {
      headers: {
        "cache-control": APP_CACHE_CONTROL,
        "content-type": contentType,
      },
      status: 200,
    });

  const proxyRequestToStreamsApp = async (
    request: Request,
    url: URL,
  ): Promise<Response> => {
    const proxyPathname = url.pathname.slice(STUDIO_STREAMS_PROXY_BASE_PATH.length);
    const normalizedPathname =
      proxyPathname.length > 0 ? proxyPathname : "/";
    const upstreamUrl = new URL(
      `${normalizedPathname}${url.search}`,
      "http://streams.internal/",
    );
    const headers = new Headers(request.headers);
    headers.delete("host");
    const body =
      request.method === "GET" || request.method === "HEAD"
        ? undefined
        : await request.arrayBuffer();

    const upstreamRequest = new Request(upstreamUrl, {
      body,
      headers,
      method: request.method,
      redirect: "manual",
      signal: request.signal,
    });
    const response = await options.streamsApp.fetch(upstreamRequest);
    const responseHeaders = new Headers(response.headers);

    responseHeaders.set("cache-control", NO_STORE_CACHE_CONTROL);

    return new Response(response.body, {
      headers: responseHeaders,
      status: response.status,
      statusText: response.statusText,
    });
  };

  const ensureGenerateStream = async (stream: string): Promise<void> => {
    if (options.streamsApp.ensureGenerateStream) {
      await options.streamsApp.ensureGenerateStream(stream);
      return;
    }

    const createResponse = await options.streamsApp.fetch(
      new Request(`http://streams.internal/v1/stream/${encodeURIComponent(stream)}`, {
        headers: {
          "content-type": "application/json",
        },
        method: "PUT",
      }),
    );
    if (!createResponse.ok && createResponse.status !== 204) {
      throw dsError(
        `stream create failed (${createResponse.status}): ${await responseText(createResponse)}`,
      );
    }

    const profileResponse = await options.streamsApp.fetch(
      new Request(
        `http://streams.internal/v1/stream/${encodeURIComponent(stream)}/_profile`,
        {
          body: JSON.stringify({
            apiVersion: "durable.streams/profile/v1",
            profile: {
              kind: "evlog",
            },
          }),
          headers: {
            "content-type": "application/json",
          },
          method: "POST",
        },
      ),
    );
    if (!profileResponse.ok) {
      throw dsError(
        `profile install failed (${profileResponse.status}): ${await responseText(profileResponse)}`,
      );
    }
  };

  const appendGenerateBatch = async (
    stream: string,
    events: Array<Record<string, unknown>>,
  ): Promise<void> => {
    if (options.streamsApp.appendGenerateBatch) {
      await options.streamsApp.appendGenerateBatch(stream, events);
      return;
    }

    const response = await options.streamsApp.fetch(
      new Request(`http://streams.internal/v1/stream/${encodeURIComponent(stream)}`, {
        body: JSON.stringify(events),
        headers: {
          "content-type": "application/json",
        },
        method: "POST",
      }),
    );
    if (!response.ok && response.status !== 204) {
      throw dsError(
        `append failed (${response.status}): ${await responseText(response)}`,
      );
    }
  };

  const runGenerateJob = async (job: GenerateJobState): Promise<void> => {
    const sampleEvent = buildGenerateEvent(job.stream, 0, Date.now());
    const estimatedBytes = new TextEncoder().encode(
      JSON.stringify(sampleEvent),
    ).byteLength;
    const batchSize = clamp(
      Math.floor(GENERATE_BATCH_TARGET_BYTES / Math.max(estimatedBytes, 1)),
      GENERATE_BATCH_MIN_EVENTS,
      GENERATE_BATCH_MAX_EVENTS,
    );

    job.batchSize = batchSize;
    job.status = "running";

    let beganGenerateJob = false;
    try {
      await ensureGenerateStream(job.stream);
      options.streamsApp.beginGenerateJob?.(job.stream);
      beganGenerateJob = true;
      const baseMs = Date.now() - Math.max(0, job.total - 1) * 1_000;

      while (job.inserted < job.total) {
        const remaining = job.total - job.inserted;
        const nextBatchSize = Math.min(job.batchSize, remaining);
        const events: Array<Record<string, unknown>> = [];

        for (let index = 0; index < nextBatchSize; index += 1) {
          events.push(
            buildGenerateEvent(job.stream, job.inserted + index, baseMs),
          );
        }

        await appendGenerateBatch(job.stream, events);
        const previousInserted = job.inserted;
        job.inserted += events.length;
        collectGenerateBatchMemory(job, previousInserted);

        await new Promise<void>((resolve) => setTimeout(resolve, 0));
      }

      job.status = "succeeded";
      job.finishedAt = new Date().toISOString();
      scheduleCleanup(job.id);
    } catch (error) {
      job.error = error instanceof Error ? error.message : String(error);
      job.status = "failed";
      job.finishedAt = new Date().toISOString();
      scheduleCleanup(job.id);
    } finally {
      if (beganGenerateJob) options.streamsApp.endGenerateJob?.(job.stream);
    }
  };

  const handleGenerateJobCreate = async (request: Request): Promise<Response> => {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          Allow: "POST,OPTIONS",
        },
        status: 204,
      });
    }
    if (request.method !== "POST") {
      return methodNotAllowed("POST,OPTIONS");
    }

    let payload: { count?: unknown; stream?: unknown };

    try {
      payload = (await request.json()) as { count?: unknown; stream?: unknown };
    } catch {
      return jsonResponse(400, { error: "Invalid JSON payload." });
    }

    const count = assertAllowedGenerateCount(payload.count);
    if (count == null) {
      return jsonResponse(400, {
        error: "count must be one of 1000, 10000, or 100000.",
      });
    }

    const stream = normalizeGenerateStreamName(payload.stream);
    if (stream == null) {
      return jsonResponse(400, {
        error:
          "stream must be 1-128 characters, start with a letter or number, and contain only letters, numbers, dot, underscore, slash, or hyphen.",
      });
    }

    const id = crypto.randomUUID();
    const job: GenerateJobState = {
      batchSize: 0,
      error: null,
      finishedAt: null,
      id,
      inserted: 0,
      startedAt: new Date().toISOString(),
      status: "pending",
      stream,
      total: count,
    };

    jobs.set(id, job);
    void runGenerateJob(job);

    return jsonResponse(202, { job });
  };

  const handleGenerateJobRead = (request: Request, url: URL): Response => {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          Allow: "GET,OPTIONS",
        },
        status: 204,
      });
    }
    if (request.method !== "GET") {
      return methodNotAllowed("GET,OPTIONS");
    }

    const suffix = url.pathname.slice(GENERATE_JOBS_BASE_PATH.length + 1);
    const jobId = decodeURIComponent(suffix);
    const job = jobs.get(jobId);
    if (!job) {
      return jsonResponse(404, { error: "Job not found." });
    }
    return jsonResponse(200, { job });
  };

  return {
    close(): void {
      for (const timer of cleanupTimers) {
        clearTimer(timer);
      }
    },

    async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url);

      if (url.pathname === "/") {
        return writeHtml(request, createLandingHtmlDocument());
      }

      if (url.pathname === STUDIO_ROOT_PATH || url.pathname === `${STUDIO_ROOT_PATH}/`) {
        return writeHtml(request, createStudioHtmlDocument());
      }

      if (url.pathname === `${STUDIO_ROOT_PATH}/app.js`) {
        return writeAsset(
          request,
          studioAssets.appScript,
          "application/javascript; charset=utf-8",
        );
      }

      if (url.pathname === `${STUDIO_ROOT_PATH}/app.css`) {
        return writeAsset(request, studioAssets.appStyles, "text/css; charset=utf-8");
      }

      if (
        url.pathname === STUDIO_STREAMS_PROXY_BASE_PATH ||
        url.pathname.startsWith(`${STUDIO_STREAMS_PROXY_BASE_PATH}/`)
      ) {
        return await proxyRequestToStreamsApp(request, url);
      }

      if (url.pathname === STUDIO_CONFIG_PATH) {
        return jsonResponse(200, createConfigPayload(bootId));
      }

      if (url.pathname === STUDIO_QUERY_PATH) {
        if (request.method === "OPTIONS") {
          return new Response(null, {
            headers: {
              Allow: "POST,OPTIONS",
            },
            status: 204,
          });
        }
        if (request.method !== "POST") {
          return methodNotAllowed("POST,OPTIONS");
        }
        return jsonResponse(503, {
          error: "Database access is disabled for this deployment.",
        });
      }

      if (url.pathname === STUDIO_AI_PATH) {
        if (request.method === "OPTIONS") {
          return new Response(null, {
            headers: {
              Allow: "POST,OPTIONS",
            },
            status: 204,
          });
        }
        if (request.method !== "POST") {
          return methodNotAllowed("POST,OPTIONS");
        }
        return jsonResponse(503, {
          code: "llm_disabled",
          message: "AI is disabled for this deployment.",
          ok: false,
        });
      }

      if (url.pathname === GENERATE_ROOT_PATH || url.pathname === `${GENERATE_ROOT_PATH}/`) {
        return writeHtml(request, createGenerateHtmlDocument());
      }

      if (url.pathname === GENERATE_JOBS_BASE_PATH) {
        return await handleGenerateJobCreate(request);
      }

      if (url.pathname.startsWith(`${GENERATE_JOBS_BASE_PATH}/`)) {
        return handleGenerateJobRead(request, url);
      }

      for (const assetPath of normalizeBuiltAssetPath(url.pathname)) {
        const asset = studioAssets.builtAssets.get(assetPath);
        if (asset) {
          const assetBytes = asset.bytes instanceof Uint8Array
            ? new Uint8Array(asset.bytes)
            : new Uint8Array(asset.bytes);
          return writeAsset(
            request,
            new Blob([assetBytes]),
            asset.contentType,
          );
        }
      }

      return await options.streamsApp.fetch(request);
    },
  };
}
