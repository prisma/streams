// The few product requests offboarding makes against a cell, under
// tokens cell-admin mints in memory (never written, never printed):
// the catalog (`GET /v1/streams`, scope streams.catalog.read) and the
// stream delete (`DELETE /v1/streams/{name}`, scope
// streams.lifecycle.manage). A 429 or 503 is retried after its
// Retry-After (at most 5 s, or a caller's own bound: a drain waits out the
// project's whole debt), so a project at its own request ceiling still
// drains.
import { AdminError } from "./errors";

export interface CellReply {
  status: number;
  code?: string;
  body?: unknown;
  /// Retry-After in seconds, clamped to 0..maxRetryAfterSecs; 1 when
  /// absent or unreadable.
  retryAfter: number;
}

/// How a caller waits: the longest Retry-After it honours (5 s unless
/// set), and how it sleeps (a test's clock).
export interface Waits {
  maxRetryAfterSecs?: number;
  sleep?: (ms: number) => Promise<void>;
}

const ATTEMPTS = 6;
const REQUEST_TIMEOUT_MS = 30_000;
const RETRY_AFTER_MAX_SECS = 5;

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

function retryAfterSecs(header: string | null, max: number): number {
  const n = Number(header ?? "1");
  return Number.isFinite(n) ? Math.max(0, Math.min(n, max)) : 1;
}

async function once(base: string, method: string, path: string, token: string, max: number): Promise<CellReply> {
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
    retryAfter: retryAfterSecs(res.headers.get("retry-after"), max),
  };
}

/// One request, retried on 429, 503 and transport failures; the last
/// 429 or 503 is returned when the attempts run out.
export async function call(
  base: string,
  method: string,
  path: string,
  token: string,
  waits: Waits = {},
): Promise<CellReply> {
  const pause = waits.sleep ?? sleep;
  for (let attempt = 1; ; attempt++) {
    let reply: CellReply | undefined;
    let failure = "";
    try {
      reply = await once(base, method, path, token, waits.maxRetryAfterSecs ?? RETRY_AFTER_MAX_SECS);
    } catch (e) {
      failure = (e as Error).name === "TimeoutError" ? "timed out" : "unreachable";
    }
    if (reply && reply.status !== 429 && reply.status !== 503) return reply;
    if (attempt >= ATTEMPTS) {
      if (reply) return reply;
      throw new AdminError(`a ${method} to ${base}: ${failure} after ${ATTEMPTS} attempts`);
    }
    await pause((reply?.retryAfter ?? 1) * 1000);
  }
}

export interface Walk {
  listed: number;
  deleted: number;
}

/// The largest catalog page a walk asks for.
export const WALK_PAGE_MAX = 1000;

/// One full catalog walk that deletes every stream it lists, a page of at
/// most `pageLimit` at a time, each page deleted before the next is listed.
/// `token` mints the bearer of each request (each page's listing and each
/// delete), so a walk may outlast any one token: at the free tier's 10
/// requests a second under phase B's store bucket a walk takes about
/// 1.25 s a stream, and an operator token is usable for 29.5 minutes. One
/// request's own retries wait at most 5 x 120 s at a drain's bound.
export async function walkAndDelete(
  base: string,
  token: () => string,
  log: (line: string) => void,
  opts: Waits & { pageLimit?: number } = {},
): Promise<Walk> {
  const walk: Walk = { listed: 0, deleted: 0 };
  const limit = `limit=${opts.pageLimit ?? WALK_PAGE_MAX}`;
  let cursor: string | undefined;
  do {
    const q = cursor ? `${limit}&cursor=${encodeURIComponent(cursor)}` : limit;
    const page = await call(base, "GET", `/v1/streams?${q}`, token(), opts);
    if (page.status !== 200) {
      throw new AdminError(`catalog walk answered ${describeReply(page)}`);
    }
    const body = page.body as { streams?: { name: string }[]; cursor?: string };
    for (const s of body.streams ?? []) {
      walk.listed += 1;
      const del = await call(base, "DELETE", `/v1/streams/${encName(s.name)}`, token(), opts);
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
