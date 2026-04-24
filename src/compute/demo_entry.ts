import { bootstrapFromR2 } from "../bootstrap";
import { createApp } from "../app";
import { loadConfig } from "../config";
import { MockR2Store } from "../objectstore/mock_r2";
import { R2ObjectStore } from "../objectstore/r2";
import { dsError } from "../util/ds_error.ts";
import { initConsoleLogging } from "../util/log";
import { ensureComputeArgv } from "./entry";
import { createComputeDemoSite, type PrebuiltStudioAssets } from "./demo_site";

initConsoleLogging();

export type StreamsFetchTarget = {
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

async function main(): Promise<void> {
  const cfg = loadConfig();
  const studioAssets = await loadStudioAssets();
  const externalStreamsServerUrl = resolveExternalStreamsServerUrl();
  let streamsTarget: StreamsFetchTarget;
  let closeStreamsTarget: (() => void) | null = null;

  if (externalStreamsServerUrl) {
    streamsTarget = createExternalStreamsTarget(externalStreamsServerUrl);
    console.log(
      `prisma-streams compute demo using external Streams server ${externalStreamsServerUrl}`,
    );
  } else {
    process.argv = ensureComputeArgv(process.argv);
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
      const memBytes = memBytesRaw
        ? Number(memBytesRaw)
        : memMbRaw
          ? Number(memMbRaw) * 1024 * 1024
          : null;
      if (memBytesRaw && !Number.isFinite(memBytes)) {
        console.error(`invalid DS_MOCK_R2_MAX_INMEM_BYTES: ${memBytesRaw}`);
        process.exit(1);
      }
      if (memMbRaw && !Number.isFinite(Number(memMbRaw))) {
        console.error(`invalid DS_MOCK_R2_MAX_INMEM_MB: ${memMbRaw}`);
        process.exit(1);
      }
      const spillDir = process.env.DS_MOCK_R2_SPILL_DIR;
      store =
        memBytes != null || spillDir
          ? new MockR2Store({
              maxInMemoryBytes: memBytes ?? undefined,
              spillDir,
            })
          : new MockR2Store();
    } else {
      const bucket = process.env.DURABLE_STREAMS_R2_BUCKET;
      const accountId = process.env.DURABLE_STREAMS_R2_ACCOUNT_ID;
      const accessKeyId = process.env.DURABLE_STREAMS_R2_ACCESS_KEY_ID;
      const secretAccessKey = process.env.DURABLE_STREAMS_R2_SECRET_ACCESS_KEY;
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
      });
    }

    if (bootstrapEnabled) {
      await bootstrapFromR2(cfg, store, { clearLocal: true });
    }

    const streamsApp = createApp(cfg, store);
    streamsTarget = streamsApp;
    closeStreamsTarget = () => streamsApp.close();
  }
  const demoSite = createComputeDemoSite({
    studioAssets,
    streamsApp: streamsTarget,
  });

  const server = Bun.serve({
    fetch: (request) => demoSite.fetch(request),
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
