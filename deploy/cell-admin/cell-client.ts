// The few product requests offboarding makes against a cell, under a
// token cell-admin mints in memory (never written, never printed):
// the catalog (`GET /v1/streams`, scope streams.catalog.read) and the
// stream delete (`DELETE /v1/streams/{name}`, scope
// streams.lifecycle.manage). A 429 or 503 is retried after its
// Retry-After (at most 5 s), so a project at its own request ceiling
// still drains.
import { AdminError } from "./errors";

export interface CellReply {
  status: number;
  code?: string;
  body?: unknown;
  /// Retry-After in seconds, clamped to 0..5; 1 when absent or unreadable.
  retryAfter: number;
}

const ATTEMPTS = 6;
const REQUEST_TIMEOUT_MS = 30_000;

/// The cell's base URL. A bearer token never crosses plain HTTP except to
/// this machine.
export function cellBase(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new AdminError(`--cell-url ${raw} is not a URL`, 2);
  }
  const local = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (url.protocol !== "https:" && !(url.protocol === "http:" && local)) {
    throw new AdminError(`--cell-url ${raw}: tokens go to a cell over https only`, 2);
  }
  if (url.pathname !== "/" || url.search !== "") {
    throw new AdminError(`--cell-url ${raw}: give the cell's origin, without a path`, 2);
  }
  return url.origin;
}

/// "403 credential_not_active", or just "200".
export const describeReply = (r: CellReply) => (r.code ? `${r.status} ${r.code}` : `${r.status}`);

const encName = (name: string) => name.split("/").map(encodeURIComponent).join("/");

/// The error code of either envelope: product `{"error":{"code"}}` or raw
/// `{"error":"code"}`.
function codeOf(body: unknown): string | undefined {
  const e = (body as { error?: unknown } | undefined)?.error;
  if (typeof e === "string") return e;
  const code = (e as { code?: unknown } | undefined)?.code;
  return typeof code === "string" ? code : undefined;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function retryAfterSecs(header: string | null): number {
  const n = Number(header ?? "1");
  return Number.isFinite(n) ? Math.max(0, Math.min(n, 5)) : 1;
}

async function once(base: string, method: string, path: string, token: string): Promise<CellReply> {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: { authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  const text = await res.text();
  let body: unknown;
  try {
    body = text === "" ? undefined : JSON.parse(text);
  } catch {
    body = undefined;
  }
  return {
    status: res.status,
    code: codeOf(body),
    body,
    retryAfter: retryAfterSecs(res.headers.get("retry-after")),
  };
}

/// One request, retried on 429, 503 and transport failures; the last
/// 429 or 503 is returned when the attempts run out.
export async function call(base: string, method: string, path: string, token: string): Promise<CellReply> {
  for (let attempt = 1; ; attempt++) {
    let reply: CellReply | undefined;
    let failure = "";
    try {
      reply = await once(base, method, path, token);
    } catch (e) {
      failure = (e as Error).name === "TimeoutError" ? "timed out" : "unreachable";
    }
    if (reply && reply.status !== 429 && reply.status !== 503) return reply;
    if (attempt >= ATTEMPTS) {
      if (reply) return reply;
      throw new AdminError(`a ${method} to ${base}: ${failure} after ${ATTEMPTS} attempts`);
    }
    await sleep((reply?.retryAfter ?? 1) * 1000);
  }
}

export interface Walk {
  listed: number;
  deleted: number;
}

/// One full catalog walk that deletes every stream it lists.
export async function walkAndDelete(
  base: string,
  token: string,
  log: (line: string) => void,
): Promise<Walk> {
  const walk: Walk = { listed: 0, deleted: 0 };
  let cursor: string | undefined;
  do {
    const q = cursor ? `limit=1000&cursor=${encodeURIComponent(cursor)}` : "limit=1000";
    const page = await call(base, "GET", `/v1/streams?${q}`, token);
    if (page.status !== 200) {
      throw new AdminError(`catalog walk answered ${describeReply(page)}`);
    }
    const body = page.body as { streams?: { name: string }[]; cursor?: string };
    for (const s of body.streams ?? []) {
      walk.listed += 1;
      const del = await call(base, "DELETE", `/v1/streams/${encName(s.name)}`, token);
      if (del.status === 204) walk.deleted += 1;
      else if (del.status !== 404 && del.status !== 410) {
        throw new AdminError(`DELETE of a listed stream answered ${describeReply(del)}`);
      }
    }
    cursor = body.cursor;
    log(`  walked a page: ${walk.listed} listed, ${walk.deleted} deleted so far`);
  } while (cursor);
  return walk;
}
