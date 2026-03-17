import { startLocalDurableStreamsServer, type DurableStreamsLocalExports } from "./server";

export async function runLocalServerProcess(
  opts: {
    name?: string;
    hostname?: string;
    port?: number;
  },
  cfg: {
    onReady(exportsPayload: DurableStreamsLocalExports): void;
    closeOnDisconnect?: boolean;
  }
): Promise<void> {
  const server = await startLocalDurableStreamsServer(opts);
  cfg.onReady(server.exports);

  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    try {
      await server.close();
    } finally {
      process.exit(0);
    }
  };

  process.on("SIGTERM", () => {
    void shutdown();
  });
  process.on("SIGINT", () => {
    void shutdown();
  });
  if (cfg.closeOnDisconnect) {
    process.on("disconnect", () => {
      void shutdown();
    });
  }
}
