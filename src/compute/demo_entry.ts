import { bootstrapFromR2 } from "../bootstrap";
import { createApp, type App } from "../app";
import { loadConfig } from "../config";
import type { AppendRow } from "../ingest";
import { MockR2Store } from "../objectstore/mock_r2";
import { R2ObjectStore } from "../objectstore/r2";
import { resolveJsonIngestCapability } from "../profiles";
import type { SchemaRegistry } from "../schema/registry";
import { dsError } from "../util/ds_error.ts";
import { resolvePointerResult } from "../util/json_pointer";
import { initConsoleLogging } from "../util/log";
import { ensureComputeArgv } from "./entry";
import { createComputeDemoSite, type PrebuiltStudioAssets } from "./demo_site";
import { applyAutoTune, AutoTuneApplyError, parseAutoTuneArg } from "../server_auto_tune";
import { parseAuthConfigResult, withAuth } from "../auth";
import { Result } from "better-result";

initConsoleLogging();

export type StreamsFetchTarget = {
  appendGenerateBatch?: (stream: string, events: Array<Record<string, unknown>>) => Promise<void>;
  beginGenerateJob?: (stream: string) => void;
  endGenerateJob?: (stream: string) => void;
  ensureGenerateStream?: (stream: string) => Promise<void>;
  fetch(request: Request): Promise<Response>;
};

const EXTERNAL_STREAMS_URL_ENVS = [
  "COMPUTE_DEMO_STREAMS_SERVER_URL",
  "STREAMS_SERVER_URL",
] as const;

function fallbackStudioAssets(): PrebuiltStudioAssets {
  const message =
    "Studio assets were not bundled. Build this entrypoint with bun run build:compute-demo-bundle.";

  return {
    appScript: `const root = document.getElementById("root"); if (root) root.innerHTML = "<pre style=\\"white-space:pre-wrap;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;padding:24px\\">${message}</pre>";`,
    appStyles:
      "html,body{margin:0;background:#08111b;color:#e9f3fb;font-family:ui-sans-serif,system-ui,sans-serif;}",
    builtAssets: new Map(),
  };
}

async function loadStudioAssets(): Promise<PrebuiltStudioAssets> {
  try {
    return (await import("virtual:prebuilt-studio-assets")) as PrebuiltStudioAssets;
  } catch {
    return fallbackStudioAssets();
  }
}

function loadIdleTimeoutSeconds(): number {
  const raw = process.env.DS_HTTP_IDLE_TIMEOUT_SECONDS;
  if (raw == null || raw.trim() === "") return 180;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    console.error(`invalid DS_HTTP_IDLE_TIMEOUT_SECONDS: ${raw}`);
    process.exit(1);
  }
  return value;
}

function normalizeExternalStreamsServerUrl(value: string): string {
  const trimmed = value.trim();
  if (trimmed === "") {
    throw dsError("external Streams server URL must not be empty");
  }
  const withScheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(trimmed)
    ? trimmed
    : `https://${trimmed}`;
  return withScheme.endsWith("/") ? withScheme.slice(0, -1) : withScheme;
}

export function resolveExternalStreamsServerUrl(
  env: NodeJS.ProcessEnv = process.env,
): string | null {
  for (const name of EXTERNAL_STREAMS_URL_ENVS) {
    const raw = env[name];
    if (raw == null || raw.trim() === "") continue;
    return normalizeExternalStreamsServerUrl(raw);
  }
  return null;
}

export function createExternalStreamsTarget(baseUrl: string): StreamsFetchTarget {
  const normalizedBaseUrl = normalizeExternalStreamsServerUrl(baseUrl);

  return {
    async fetch(request: Request): Promise<Response> {
      const requestUrl = new URL(request.url);
      const upstreamUrl = new URL(
        `${requestUrl.pathname}${requestUrl.search}`,
        `${normalizedBaseUrl}/`,
      );
      const headers = new Headers(request.headers);
      headers.delete("host");
      const body =
        request.method === "GET" || request.method === "HEAD"
          ? undefined
          : await request.arrayBuffer();

      const response = await fetch(upstreamUrl, {
        body,
        headers,
        method: request.method,
        redirect: "manual",
        signal: request.signal,
      });

      return new Response(response.body, {
        headers: response.headers,
        status: response.status,
        statusText: response.statusText,
      });
    },
  };
}

export function applyColocatedComputeDemoArgv(
  argv: string[],
  env: NodeJS.ProcessEnv = process.env,
  opts: { log?: (message: string) => void } = {},
): string[] {
  const next = ensureComputeArgv(argv, env);
  const args = next.slice(2);
  const autoTune = parseAutoTuneArg(args);
  if (autoTune.enabled) {
    applyAutoTune(autoTune.valueMb, { env, log: opts.log });
  }
  return next;
}

const DIRECT_APPEND_TEXT_ENCODER = new TextEncoder();

function keyBytesFromString(value: string | null): Uint8Array | null {
  return value == null ? null : DIRECT_APPEND_TEXT_ENCODER.encode(value);
}

function extractRoutingKey(reg: SchemaRegistry, value: unknown): Uint8Array | null {
  if (!reg.routingKey) return null;
  const resolvedRes = resolvePointerResult(value, reg.routingKey.jsonPointer);
  if (Result.isError(resolvedRes)) {
    throw dsError(resolvedRes.error.message);
  }
  const resolved = resolvedRes.value;
  if (!resolved.exists) {
    if (reg.routingKey.required) throw dsError("routing key missing");
    return null;
  }
  if (typeof resolved.value !== "string") throw dsError("routing key must be string");
  return keyBytesFromString(resolved.value);
}

function appendErrorMessage(kind: string): string {
  if (kind === "not_found") return "stream not found";
  if (kind === "gone") return "stream expired";
  if (kind === "content_type_mismatch") return "content-type mismatch";
  if (kind === "overloaded") return "ingest queue full";
  if (kind === "closed") return "stream is closed";
  return "append failed";
}

function createColocatedStreamsTarget(streamsApp: App): StreamsFetchTarget {
  let activeGenerateJobs = 0;
  const generateStreamsToEnqueue = new Set<string>();

  return {
    appendGenerateBatch: async (stream, events) => {
      const { db, ingest, metrics, notifier, profiles, registry, stats, touch } = streamsApp.deps;
      const streamRow = db.getStream(stream);
      if (!streamRow || db.isDeleted(streamRow)) throw dsError("stream not found");

      const regRes = registry.getRegistryResult(stream);
      if (Result.isError(regRes)) throw dsError(regRes.error.message);
      const profileRes = profiles.getProfileResult(stream, streamRow);
      if (Result.isError(profileRes)) throw dsError(profileRes.error.message);
      const jsonIngest = resolveJsonIngestCapability(profileRes.value);
      const reg = regRes.value;
      const validator = reg.currentVersion > 0 ? registry.getValidatorForVersion(reg, reg.currentVersion) : null;
      if (reg.currentVersion > 0 && !validator) throw dsError("schema validator missing");

      const rows: AppendRow[] = [];
      let encodedBytes = 0;
      for (const event of events) {
        let value: unknown = event;
        let profileRoutingKey: Uint8Array | null = null;
        if (jsonIngest) {
          const preparedRes = jsonIngest.prepareRecordResult({ stream, profile: profileRes.value, value: event });
          if (Result.isError(preparedRes)) throw dsError(preparedRes.error.message);
          value = preparedRes.value.value;
          profileRoutingKey = keyBytesFromString(preparedRes.value.routingKey);
        }
        if (validator && !validator(value)) {
          const message = validator.errors ? validator.errors.map((error) => error.message).join("; ") : "schema validation failed";
          throw dsError(message);
        }
        const payload = DIRECT_APPEND_TEXT_ENCODER.encode(JSON.stringify(value));
        encodedBytes += payload.byteLength;
        rows.push({
          routingKey: reg.routingKey ? extractRoutingKey(reg, value) : profileRoutingKey,
          contentType: "application/json",
          payload,
        });
      }

      const appendRes = await ingest.append({
        stream,
        baseAppendMs: db.nowMs(),
        rows,
        contentType: "application/json",
      });
      if (Result.isError(appendRes)) {
        throw dsError(appendErrorMessage(appendRes.error.kind));
      }
      if (appendRes.value.appendedRows > 0) {
        metrics.recordAppend(encodedBytes, appendRes.value.appendedRows);
        notifier.notify(stream, appendRes.value.lastOffset);
        notifier.notifyDetailsChanged(stream);
        touch.notify(stream);
        stats?.recordStreamTouched(stream);
        stats?.recordIngested(encodedBytes);
      }
    },
    beginGenerateJob: (stream) => {
      activeGenerateJobs += 1;
      generateStreamsToEnqueue.add(stream);
      if (activeGenerateJobs !== 1) return;
      streamsApp.deps.indexer?.stop();
      streamsApp.deps.segmenter.stop(true);
    },
    endGenerateJob: (stream) => {
      generateStreamsToEnqueue.add(stream);
      activeGenerateJobs = Math.max(0, activeGenerateJobs - 1);
      if (activeGenerateJobs !== 0) return;
      streamsApp.deps.segmenter.start();
      streamsApp.deps.indexer?.start();
      for (const pendingStream of generateStreamsToEnqueue) {
        streamsApp.deps.indexer?.enqueue(pendingStream);
      }
      generateStreamsToEnqueue.clear();
    },
    fetch: (request) => streamsApp.fetch(request),
  };
}

async function main(): Promise<void> {
  const authConfigResult = parseAuthConfigResult(process.argv.slice(2));
  if (Result.isError(authConfigResult)) {
    console.error(authConfigResult.error.message);
    process.exit(1);
  }
  const authConfig = authConfigResult.value;
  const studioAssets = await loadStudioAssets();
  const externalStreamsServerUrl = resolveExternalStreamsServerUrl();
  let streamsTarget: StreamsFetchTarget;
  let closeStreamsTarget: (() => void) | null = null;
  let cfg: ReturnType<typeof loadConfig>;

  if (externalStreamsServerUrl) {
    cfg = loadConfig();
    streamsTarget = createExternalStreamsTarget(externalStreamsServerUrl);
    console.log(
      `prisma-streams compute demo using external Streams server ${externalStreamsServerUrl}`,
    );
  } else {
    try {
      process.argv = applyColocatedComputeDemoArgv(process.argv);
    } catch (error) {
      if (error instanceof AutoTuneApplyError) {
        console.error(error.message);
        process.exit(1);
      }
      throw error;
    }
    cfg = loadConfig();
    const args = process.argv.slice(2);

    const storeIdx = args.indexOf("--object-store");
    const storeChoice = storeIdx >= 0 ? args[storeIdx + 1] : null;
    if (!storeChoice || (storeChoice !== "r2" && storeChoice !== "local")) {
      console.error("missing or invalid --object-store (expected: r2 | local)");
      process.exit(1);
    }
    const bootstrapEnabled = args.includes("--bootstrap-from-r2");

    let store;
    if (storeChoice === "local") {
      const memBytesRaw = process.env.DS_MOCK_R2_MAX_INMEM_BYTES;
      const memMbRaw = process.env.DS_MOCK_R2_MAX_INMEM_MB;
      const putDelayRaw = process.env.DS_MOCK_R2_PUT_DELAY_MS;
      const getDelayRaw = process.env.DS_MOCK_R2_GET_DELAY_MS;
      const headDelayRaw = process.env.DS_MOCK_R2_HEAD_DELAY_MS;
      const listDelayRaw = process.env.DS_MOCK_R2_LIST_DELAY_MS;
      const memBytes = memBytesRaw
        ? Number(memBytesRaw)
        : memMbRaw
          ? Number(memMbRaw) * 1024 * 1024
          : null;
      const putDelayMs = putDelayRaw ? Number(putDelayRaw) : 0;
      const getDelayMs = getDelayRaw ? Number(getDelayRaw) : 0;
      const headDelayMs = headDelayRaw ? Number(headDelayRaw) : 0;
      const listDelayMs = listDelayRaw ? Number(listDelayRaw) : 0;
      if (memBytesRaw && !Number.isFinite(memBytes)) {
        console.error(`invalid DS_MOCK_R2_MAX_INMEM_BYTES: ${memBytesRaw}`);
        process.exit(1);
      }
      if (memMbRaw && !Number.isFinite(Number(memMbRaw))) {
        console.error(`invalid DS_MOCK_R2_MAX_INMEM_MB: ${memMbRaw}`);
        process.exit(1);
      }
      for (const [name, value] of [
        ["DS_MOCK_R2_PUT_DELAY_MS", putDelayMs],
        ["DS_MOCK_R2_GET_DELAY_MS", getDelayMs],
        ["DS_MOCK_R2_HEAD_DELAY_MS", headDelayMs],
        ["DS_MOCK_R2_LIST_DELAY_MS", listDelayMs],
      ] as const) {
        if (!Number.isFinite(value) || value < 0) {
          console.error(`invalid ${name}: ${process.env[name]}`);
          process.exit(1);
        }
      }
      const spillDir = process.env.DS_MOCK_R2_SPILL_DIR;
      store = new MockR2Store({
        maxInMemoryBytes: memBytes ?? undefined,
        spillDir,
        faults: {
          putDelayMs,
          getDelayMs,
          headDelayMs,
          listDelayMs,
        },
      });
    } else {
      const bucket = process.env.DURABLE_STREAMS_R2_BUCKET;
      const accountId = process.env.DURABLE_STREAMS_R2_ACCOUNT_ID;
      const accessKeyId = process.env.DURABLE_STREAMS_R2_ACCESS_KEY_ID;
      const secretAccessKey = process.env.DURABLE_STREAMS_R2_SECRET_ACCESS_KEY;
      const endpoint = process.env.DURABLE_STREAMS_R2_ENDPOINT;
      const region = process.env.DURABLE_STREAMS_R2_REGION;
      if (!bucket || !accountId || !accessKeyId || !secretAccessKey) {
        console.error(
          "missing R2 env vars: DURABLE_STREAMS_R2_BUCKET, DURABLE_STREAMS_R2_ACCOUNT_ID, DURABLE_STREAMS_R2_ACCESS_KEY_ID, DURABLE_STREAMS_R2_SECRET_ACCESS_KEY",
        );
        process.exit(1);
      }
      store = new R2ObjectStore({
        accessKeyId,
        accountId,
        bucket,
        secretAccessKey,
        endpoint,
        region,
      });
    }

    if (bootstrapEnabled) {
      await bootstrapFromR2(cfg, store, { clearLocal: true });
    }

    const streamsApp = createApp(cfg, store);
    streamsTarget = createColocatedStreamsTarget(streamsApp);
    closeStreamsTarget = () => streamsApp.close();
  }
  const demoSite = createComputeDemoSite({
    studioAssets,
    streamsApp: streamsTarget,
  });

  const server = Bun.serve({
    fetch: withAuth(authConfig, (request) => demoSite.fetch(request)),
    hostname: cfg.host,
    idleTimeout: loadIdleTimeoutSeconds(),
    port: cfg.port,
  });

  let shuttingDown = false;
  const shutdown = (signal: NodeJS.Signals): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`received ${signal}, shutting down prisma-streams compute demo`);
    try {
      server.stop(true);
    } catch (error) {
      console.error("failed to stop HTTP server cleanly", error);
    }
    try {
      demoSite.close();
    } catch (error) {
      console.error("failed to close compute demo cleanly", error);
    }
    if (closeStreamsTarget) {
      try {
        closeStreamsTarget();
      } catch (error) {
        console.error("failed to close streams application cleanly", error);
        process.exitCode = 1;
      }
    }
  };

  process.once("SIGINT", () => shutdown("SIGINT"));
  process.once("SIGTERM", () => shutdown("SIGTERM"));

  const listenTarget = cfg.host.includes(":")
    ? `[${cfg.host}]:${server.port}`
    : `${cfg.host}:${server.port}`;
  console.log(`prisma-streams compute demo listening on ${listenTarget}`);
}

if (import.meta.main) {
  await main();
}
