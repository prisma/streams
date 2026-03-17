import { createLocalApp } from "../app_local";
import { buildLocalConfig } from "./config";
import { normalizeServerName } from "./paths";
import { acquireLock, getDbPath, writeServerDump, type DurableStreamsLocalServerDump } from "./state";
import { serveFetchHandler } from "./http";

export type DurableStreamsLocalExports = {
  http: { url: string; port: number };
  sqlite: { path: string };
  name: string;
  pid: number;
};

export type DurableStreamsLocalServer = {
  exports: DurableStreamsLocalExports;
  close(): Promise<void>;
};

export async function startLocalDurableStreamsServer(opts: {
  name?: string;
  port?: number;
  hostname?: string;
} = {}): Promise<DurableStreamsLocalServer> {
  const name = normalizeServerName(opts.name);
  const hostname = opts.hostname ?? "127.0.0.1";

  const releaseLock = await acquireLock(name);
  let app: ReturnType<typeof createLocalApp> | null = null;
  let http: { port: number; close(): Promise<void> } | null = null;
  let closed = false;

  try {
    const cfg = buildLocalConfig({ name, port: opts.port });
    app = createLocalApp(cfg);
    http = await serveFetchHandler(app.fetch, { hostname, port: cfg.port });

    const exportsPayload: DurableStreamsLocalExports = {
      name,
      pid: process.pid,
      http: {
        port: http.port,
        url: `http://${hostname}:${http.port}`,
      },
      sqlite: {
        path: getDbPath(name),
      },
    };

    const dump: DurableStreamsLocalServerDump = {
      version: 1,
      name,
      pid: process.pid,
      startedAt: new Date().toISOString(),
      http: exportsPayload.http,
      sqlite: exportsPayload.sqlite,
    };
    writeServerDump(name, dump);

    return {
      exports: exportsPayload,
      close: async () => {
        if (closed) return;
        closed = true;
        try {
          if (http) await http.close();
        } finally {
          try {
            app?.close();
          } finally {
            await releaseLock();
          }
        }
      },
    };
  } catch (err) {
    try {
      if (http) await http.close();
    } catch {
      // ignore
    }
    try {
      app?.close();
    } catch {
      // ignore
    }
    await releaseLock();
    throw err;
  }
}
