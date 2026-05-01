import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { authenticateRequest, parseAuthConfigResult, withAuth } from "../src/auth";

const VALID_KEY = "0123456789";

const STREAMS_ENDPOINTS: Array<{ name: string; method: string; path: string; body?: BodyInit }> = [
  { name: "health", method: "GET", path: "/health" },
  { name: "metrics", method: "GET", path: "/metrics" },
  { name: "server details", method: "GET", path: "/v1/server/_details" },
  { name: "server memory", method: "GET", path: "/v1/server/_mem" },
  { name: "list streams", method: "GET", path: "/v1/streams?limit=10&offset=0" },
  { name: "create stream", method: "PUT", path: "/v1/stream/auth-test", body: "" },
  { name: "append stream", method: "POST", path: "/v1/stream/auth-test", body: "record" },
  { name: "read stream", method: "GET", path: "/v1/stream/auth-test?offset=-1" },
  { name: "read stream by pk path", method: "GET", path: "/v1/stream/auth-test/pk/key-1?offset=-1" },
  { name: "head stream", method: "HEAD", path: "/v1/stream/auth-test" },
  { name: "delete stream", method: "DELETE", path: "/v1/stream/auth-test" },
  { name: "get schema", method: "GET", path: "/v1/stream/auth-test/_schema" },
  { name: "update schema", method: "POST", path: "/v1/stream/auth-test/_schema", body: "{}" },
  { name: "get profile", method: "GET", path: "/v1/stream/auth-test/_profile" },
  { name: "update profile", method: "POST", path: "/v1/stream/auth-test/_profile", body: "{}" },
  { name: "stream details", method: "GET", path: "/v1/stream/auth-test/_details" },
  { name: "stream index status", method: "GET", path: "/v1/stream/auth-test/_index_status" },
  { name: "routing keys", method: "GET", path: "/v1/stream/auth-test/_routing_keys?limit=10" },
  { name: "search query", method: "GET", path: "/v1/stream/auth-test/_search?q=level:error&size=10" },
  { name: "search body", method: "POST", path: "/v1/stream/auth-test/_search", body: "{}" },
  { name: "aggregate", method: "POST", path: "/v1/stream/auth-test/_aggregate", body: "{}" },
  { name: "touch templates activate", method: "POST", path: "/v1/stream/auth-test/touch/templates/activate", body: "{}" },
  { name: "touch meta", method: "GET", path: "/v1/stream/auth-test/touch/meta?settle=flush&timeoutMs=1" },
  { name: "touch wait", method: "POST", path: "/v1/stream/auth-test/touch/wait", body: "{}" },
];

const COMPUTE_DEMO_ENDPOINTS: Array<{ name: string; method: string; path: string; body?: BodyInit }> = [
  { name: "compute demo landing", method: "GET", path: "/" },
  { name: "compute demo studio", method: "GET", path: "/studio" },
  { name: "compute demo studio script", method: "GET", path: "/studio/app.js" },
  { name: "compute demo studio style", method: "GET", path: "/studio/app.css" },
  { name: "compute demo streams proxy", method: "GET", path: "/studio/api/streams/v1/streams" },
  { name: "compute demo config", method: "GET", path: "/api/config" },
  { name: "compute demo query", method: "POST", path: "/api/query", body: "{}" },
  { name: "compute demo ai", method: "POST", path: "/api/ai", body: "{}" },
  { name: "compute demo generate page", method: "GET", path: "/generate" },
  { name: "compute demo create generate job", method: "POST", path: "/api/generate/jobs", body: "{}" },
  { name: "compute demo read generate job", method: "GET", path: "/api/generate/jobs/job-1" },
];

function requestForEndpoint(endpoint: { method: string; path: string; body?: BodyInit }, authorization?: string): Request {
  const headers = new Headers();
  if (authorization != null) headers.set("authorization", authorization);
  if (endpoint.body != null) headers.set("content-type", "application/json");
  return new Request(`http://local${endpoint.path}`, {
    body: endpoint.body,
    headers,
    method: endpoint.method,
  });
}

describe("server auth configuration", () => {
  test("requires an explicit auth mode", () => {
    const result = parseAuthConfigResult([], {});
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) {
      expect(result.error.message).toContain("--no-auth");
      expect(result.error.message).toContain("--auth-strategy api-key");
    }
  });

  test("rejects conflicting auth modes", () => {
    const result = parseAuthConfigResult(["--no-auth", "--auth-strategy", "api-key"], { API_KEY: VALID_KEY });
    expect(Result.isError(result)).toBe(true);
  });

  test("accepts explicit no-auth mode without API_KEY", () => {
    const result = parseAuthConfigResult(["--no-auth"], {});
    expect(Result.isOk(result)).toBe(true);
    if (Result.isOk(result)) {
      expect(result.value.mode).toBe("none");
    }
  });

  test("accepts api-key strategy with a sufficiently long API_KEY", () => {
    const result = parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: VALID_KEY });
    expect(Result.isOk(result)).toBe(true);
    if (Result.isOk(result)) {
      expect(result.value.mode).toBe("api-key");
    }
  });

  test("accepts equals-form api-key strategy", () => {
    const result = parseAuthConfigResult(["--auth-strategy=api-key"], { API_KEY: VALID_KEY });
    expect(Result.isOk(result)).toBe(true);
  });

  test("rejects unsupported auth strategies", () => {
    const result = parseAuthConfigResult(["--auth-strategy", "oauth"], { API_KEY: VALID_KEY });
    expect(Result.isError(result)).toBe(true);
  });

  test("rejects missing and too-short API_KEY values", () => {
    expect(Result.isError(parseAuthConfigResult(["--auth-strategy", "api-key"], {}))).toBe(true);
    expect(Result.isError(parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: "short" }))).toBe(true);
  });

  test("does not trim API_KEY during validation or comparison", () => {
    const key = " 123456789 ";
    const result = parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: key });
    expect(Result.isOk(result)).toBe(true);
    if (Result.isError(result)) return;

    const rejected = authenticateRequest(
      result.value,
      new Request("http://local/v1/streams", {
        headers: { authorization: "Bearer 123456789" },
      })
    );
    expect(rejected?.status).toBe(401);
  });
});

describe("server auth request enforcement", () => {
  test("allows requests in no-auth mode", async () => {
    const fetch = withAuth({ mode: "none" }, async () => new Response("ok"));
    const response = await fetch(new Request("http://local/v1/streams"));
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("ok");
  });

  test("rejects missing, malformed, and incorrect authorization headers", async () => {
    const result = parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: VALID_KEY });
    if (Result.isError(result)) throw new Error(result.error.message);

    let reached = 0;
    const fetch = withAuth(result.value, async () => {
      reached += 1;
      return new Response("ok");
    });

    for (const authorization of [null, "Basic 0123456789", "Bearer", "Bearer ", "Bearer wrongwrong1", "Bearer 0123456789 extra"]) {
      const headers = new Headers();
      if (authorization != null) headers.set("authorization", authorization);
      const response = await fetch(new Request("http://local/v1/streams", { headers }));
      expect(response.status).toBe(401);
      expect(response.headers.get("www-authenticate")).toBe("Bearer");
      expect(await response.json()).toEqual({
        error: {
          code: "unauthorized",
          message: "unauthorized",
        },
      });
    }

    expect(reached).toBe(0);
  });

  test("accepts bearer scheme case-insensitively", async () => {
    const result = parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: VALID_KEY });
    if (Result.isError(result)) throw new Error(result.error.message);

    const fetch = withAuth(result.value, async () => new Response("ok"));
    const response = await fetch(
      new Request("http://local/v1/streams", {
        headers: { authorization: `bEaReR ${VALID_KEY}` },
      })
    );

    expect(response.status).toBe(200);
    expect(await response.text()).toBe("ok");
  });

  test("authenticates before unknown routes and OPTIONS reach the app", async () => {
    const result = parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: VALID_KEY });
    if (Result.isError(result)) throw new Error(result.error.message);

    let reached = 0;
    const fetch = withAuth(result.value, async () => {
      reached += 1;
      return new Response("ok");
    });

    const unknown = await fetch(new Request("http://local/not-found"));
    const options = await fetch(new Request("http://local/v1/streams", { method: "OPTIONS" }));

    expect(unknown.status).toBe(401);
    expect(options.status).toBe(401);
    expect(reached).toBe(0);
  });

  test("requires auth before every production Streams API endpoint reaches routing", async () => {
    const result = parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: VALID_KEY });
    if (Result.isError(result)) throw new Error(result.error.message);

    const reached: string[] = [];
    const fetch = withAuth(result.value, async (request) => {
      reached.push(`${request.method} ${new URL(request.url).pathname}`);
      return new Response("handler reached");
    });

    for (const endpoint of STREAMS_ENDPOINTS) {
      const missing = await fetch(requestForEndpoint(endpoint));
      expect(missing.status, endpoint.name).toBe(401);
      expect(missing.headers.get("www-authenticate"), endpoint.name).toBe("Bearer");

      const incorrect = await fetch(requestForEndpoint(endpoint, "Bearer wrongwrong1"));
      expect(incorrect.status, endpoint.name).toBe(401);
      expect(incorrect.headers.get("www-authenticate"), endpoint.name).toBe("Bearer");
    }

    expect(reached).toEqual([]);
  });

  test("requires auth before every Compute demo endpoint reaches routing", async () => {
    const result = parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: VALID_KEY });
    if (Result.isError(result)) throw new Error(result.error.message);

    const reached: string[] = [];
    const fetch = withAuth(result.value, async (request) => {
      reached.push(`${request.method} ${new URL(request.url).pathname}`);
      return new Response("handler reached");
    });

    for (const endpoint of COMPUTE_DEMO_ENDPOINTS) {
      const missing = await fetch(requestForEndpoint(endpoint));
      expect(missing.status, endpoint.name).toBe(401);
      expect(missing.headers.get("www-authenticate"), endpoint.name).toBe("Bearer");

      const incorrect = await fetch(requestForEndpoint(endpoint, "Bearer wrongwrong1"));
      expect(incorrect.status, endpoint.name).toBe(401);
      expect(incorrect.headers.get("www-authenticate"), endpoint.name).toBe("Bearer");
    }

    expect(reached).toEqual([]);
  });

  test("passes every production Streams API endpoint through with a valid bearer token", async () => {
    const result = parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: VALID_KEY });
    if (Result.isError(result)) throw new Error(result.error.message);

    const reached: string[] = [];
    const fetch = withAuth(result.value, async (request) => {
      const url = new URL(request.url);
      reached.push(`${request.method} ${url.pathname}${url.search}`);
      return new Response("handler reached");
    });

    for (const endpoint of STREAMS_ENDPOINTS) {
      const response = await fetch(requestForEndpoint(endpoint, `Bearer ${VALID_KEY}`));
      expect(response.status, endpoint.name).toBe(200);
    }

    expect(reached).toEqual(STREAMS_ENDPOINTS.map((endpoint) => `${endpoint.method} ${endpoint.path}`));
  });

  test("passes every Compute demo endpoint through with a valid bearer token", async () => {
    const result = parseAuthConfigResult(["--auth-strategy", "api-key"], { API_KEY: VALID_KEY });
    if (Result.isError(result)) throw new Error(result.error.message);

    const reached: string[] = [];
    const fetch = withAuth(result.value, async (request) => {
      const url = new URL(request.url);
      reached.push(`${request.method} ${url.pathname}${url.search}`);
      return new Response("handler reached");
    });

    for (const endpoint of COMPUTE_DEMO_ENDPOINTS) {
      const response = await fetch(requestForEndpoint(endpoint, `Bearer ${VALID_KEY}`));
      expect(response.status, endpoint.name).toBe(200);
    }

    expect(reached).toEqual(COMPUTE_DEMO_ENDPOINTS.map((endpoint) => `${endpoint.method} ${endpoint.path}`));
  });
});
