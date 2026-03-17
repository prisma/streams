import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { Readable } from "node:stream";

function hasBody(method: string): boolean {
  const upper = method.toUpperCase();
  return upper !== "GET" && upper !== "HEAD";
}

function requestFromNode(req: IncomingMessage, opts: { hostname?: string; port: number }): Request {
  const method = (req.method ?? "GET").toUpperCase();
  const host = req.headers.host ?? `${opts.hostname ?? "127.0.0.1"}:${opts.port}`;
  const url = `http://${host}${req.url ?? "/"}`;

  const init: RequestInit & { duplex?: "half" } = {
    method,
    headers: req.headers as HeadersInit,
  };

  if (hasBody(method)) {
    init.body = Readable.toWeb(req as any) as unknown as BodyInit;
    init.duplex = "half";
  }

  return new Request(url, init);
}

async function writeNodeResponse(req: IncomingMessage, res: ServerResponse, response: Response): Promise<void> {
  res.statusCode = response.status;
  response.headers.forEach((value, key) => {
    res.setHeader(key, value);
  });

  if (!response.body || req.method?.toUpperCase() === "HEAD") {
    res.end();
    return;
  }

  const body = Readable.fromWeb(response.body as any);
  await new Promise<void>((resolve, reject) => {
    body.on("error", reject);
    res.on("error", reject);
    res.on("finish", () => resolve());
    body.pipe(res);
  });
}

export async function serveFetchHandler(
  fetchHandler: (req: Request) => Promise<Response> | Response,
  opts: { hostname?: string; port: number }
): Promise<{ port: number; close(): Promise<void> }> {
  if (typeof (globalThis as any).Bun !== "undefined") {
    const server = Bun.serve({
      hostname: opts.hostname,
      port: opts.port,
      // Long-poll endpoints routinely hold requests for 30s+.
      // Bun's default idle timeout (10s) is too low for this.
      idleTimeout: 65,
      fetch: fetchHandler,
    });

    return {
      port: server.port ?? opts.port,
      close: async () => {
        server.stop(true);
      },
    };
  }

  const hostname = opts.hostname ?? "127.0.0.1";
  const server = createServer(async (req, res) => {
    try {
      const request = requestFromNode(req, { hostname, port: opts.port });
      const response = await fetchHandler(request);
      await writeNodeResponse(req, res, response);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error("local fetch handler failed", err);
      res.statusCode = 500;
      res.setHeader("content-type", "application/json; charset=utf-8");
      res.end(JSON.stringify({ error: { code: "internal", message: "internal server error" } }));
    }
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(opts.port, hostname, () => {
      server.off("error", reject);
      resolve();
    });
  });

  const address = server.address();
  const port = typeof address === "object" && address ? address.port : opts.port;

  return {
    port,
    close: async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    },
  };
}
