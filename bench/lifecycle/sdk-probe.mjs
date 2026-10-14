#!/usr/bin/env node
// The restart leg's SDK probe (bench/lifecycle/README.md, leg 3; the
// owner's round-3 call 10, 2026-10-13): the shipping SDK, sdk/dist as built
// from this tree, calls a read, a plain append and a producer append in
// turn, one call at a time, on one stream of the cell. Each call is one
// JSON line in --out: its path, start and end (ms), ok, and every HTTP
// attempt the SDK made for it (status and Retry-After, or the transport
// failure); a failed call adds the SDK's error class, its status and code,
// and the answer or cause that ended it. lifecycle.py judges the lines.
//
//   node bench/lifecycle/sdk-probe.mjs --url URL --stream NAME --headers-file FILE
//     --out CALLS.jsonl --producer-id ID --state STATE.json [--pause-ms 250]
//
// FILE is a k2gen header file (mode 0600): its `authorization: Bearer`
// line is the SDK's token and its `prisma-encryption-key` line the
// stream's key. It is read again for every token the SDK asks for (its
// first request, and after a 401), so a rotated file is picked up; a file
// others can read is refused. Neither value is ever written out.
//
// The first line is {"setup":"ok"} once the stream exists (created or
// already there), or {"setup":"failed", error} and exit 2. On SIGTERM the
// call in flight, if any, is written with "stopped": true, and the probe
// exits 0.
import { appendFileSync, readFileSync, renameSync, statSync, writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { StreamsClient, StreamsError } from "../../sdk/dist/index.js";

const { values: args } = parseArgs({
  options: {
    url: { type: "string" },
    stream: { type: "string" },
    "headers-file": { type: "string" },
    out: { type: "string" },
    "producer-id": { type: "string" },
    state: { type: "string" },
    "pause-ms": { type: "string", default: "250" },
  },
  strict: true,
});
for (const k of ["url", "stream", "headers-file", "out", "producer-id", "state"]) {
  if (!args[k]) {
    console.error(`sdk-probe: --${k} is required`);
    process.exit(2);
  }
}
const pauseMs = Number(args["pause-ms"]);
const write = (rec) => appendFileSync(args.out, JSON.stringify(rec) + "\n");

/** The header file's lines, refused when anyone but its owner may read it. */
function headers() {
  const file = args["headers-file"];
  if (statSync(file).mode & 0o077) throw new Error(`${file} is readable by others (chmod 600)`);
  const map = {};
  for (const raw of readFileSync(file, "utf8").split(/\r?\n/)) {
    const line = raw.trim();
    const i = line.indexOf(":");
    if (line && !line.startsWith("#") && i > 0) map[line.slice(0, i).trim().toLowerCase()] = line.slice(i + 1).trim();
  }
  return map;
}

function token() {
  const m = /^Bearer\s+(\S+)$/i.exec(headers()["authorization"] ?? "");
  if (!m) throw new Error(`${args["headers-file"]} has no "authorization: Bearer <token>" line`);
  return m[1];
}

/** An error and its causes, one line: what ended a call that got no answer. */
function causes(e) {
  const parts = [];
  for (let c = e, n = 0; c !== undefined && c !== null && n < 4; c = c.cause, n++) {
    if (typeof c !== "object") {
      parts.push(String(c));
      break;
    }
    const code = typeof c.code === "string" && !(c instanceof StreamsError) ? ` (${c.code})` : "";
    parts.push(`${c.name ?? c.constructor?.name ?? "Error"}: ${c.message ?? ""}${code}`);
  }
  return parts.join("; cause: ").slice(0, 400);
}

/** The SDK's error: its class, status and code, and the server's answer
 * that ended the call (its own, or the one it wraps) or else the cause. */
function failure(e) {
  const out = { class: e?.name ?? typeof e, message: String(e?.message ?? e).slice(0, 300) };
  if (e instanceof StreamsError) Object.assign(out, { status: e.status, code: e.code });
  const answer = [e, e?.cause].find((x) => x instanceof StreamsError && x.status > 0);
  if (answer) out.answer = `HTTP ${answer.status} ${answer.code}`;
  else out.cause = causes(e instanceof StreamsError ? e.cause ?? e : e);
  return out;
}

// Every HTTP attempt of the call in flight (the SDK's retries included).
let inflight = null;
async function recordingFetch(url, init) {
  const a = { t: Date.now() };
  inflight?.attempts.push(a);
  try {
    const res = await fetch(url, init);
    Object.assign(a, { end: Date.now(), status: res.status });
    const ra = res.headers.get("retry-after");
    if (ra !== null) a.retry_after = ra;
    return res;
  } catch (e) {
    Object.assign(a, { end: Date.now(), status: 0, error: causes(e) });
    throw e;
  }
}

// Producer state in a file of the run: a durable store, as a real client's.
const store = {
  async load() {
    try {
      return JSON.parse(readFileSync(args.state, "utf8"));
    } catch {
      return undefined;
    }
  },
  async save(_scope, st) {
    writeFileSync(`${args.state}.tmp`, JSON.stringify(st));
    renameSync(`${args.state}.tmp`, args.state);
  },
};

process.on("SIGTERM", () => {
  if (inflight) write({ ...inflight, end: Date.now(), ok: false, stopped: true });
  process.exit(0);
});

const client = new StreamsClient({ url: args.url, tokenProvider: token, fetch: recordingFetch });
let stream;
try {
  const encryptionKey = headers()["prisma-encryption-key"];
  if (!encryptionKey) throw new Error(`${args["headers-file"]} has no "prisma-encryption-key" line`);
  stream = await client.createStream(args.stream, { encryptionKey, format: { kind: "json" } });
} catch (e) {
  write({ setup: "failed", t: Date.now(), error: failure(e) });
  process.exit(2);
}
write({ setup: "ok", t: Date.now() });
const producer = stream.producer(args["producer-id"], { state: store });
let cursor = "now";
const calls = {
  // From the tail, then on from each page's cursor: the probe reads back its own appends.
  read: async (call) => {
    const page = await stream.read({ from: cursor });
    cursor = page.cursor || cursor;
    call.records = page.records.length;
  },
  append: (call, n) => stream.append({ lifecycle: "restart-sdk", path: "append", n }),
  producer: (call, n) => producer.append({ lifecycle: "restart-sdk", path: "producer", n }),
};
const paths = Object.keys(calls);
for (let n = 0; ; n++) {
  const path = paths[n % paths.length];
  const call = (inflight = { path, start: Date.now(), attempts: [] });
  try {
    await calls[path](call, n);
    call.ok = true;
  } catch (e) {
    Object.assign(call, { ok: false, error: failure(e) });
  }
  inflight = null;
  write({ ...call, end: Date.now() });
  await new Promise((resolve) => setTimeout(resolve, pauseMs));
}
