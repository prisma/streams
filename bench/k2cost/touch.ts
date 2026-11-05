// touch: a population of streams touched one after another (tuning P1, the
// habanero scale bench's population mode), so each touch finds its stream
// cold once the population outlasts the descriptor cache's TTL and sweep:
// 100k streams at 300 touches a second touch each every 5.5 minutes, the
// shape of 1M touched once an hour. A touch appends one record (unless
// --append 0) and reads the stream's records (naming --max-bytes only when
// given, as a client without maxBytes does). Open loop at --rate touches a
// second, at most --concurrency in flight; --rate 0 sends every touch of a
// pass at once (--concurrency of them), the cold-read burst.
//
//   k2gen touch --base URL --streams N --prefix P [--rate 300] [--passes 1]
//               [--concurrency 512] [--append 1] [--record-bytes 64]
//               [--max-bytes B] [--duration S] [--abort-on-refusal 1]
//               [--read-from start|tail]
//
// Its ledger counts every answer by status (status_counts) and the touch
// latency; s3lite's stats count the store requests by cell and status, so
// the registry's GETs answered 304 against those answered 200 are read
// there (counts.py), not here.
import { type Ctx } from "./produce.ts";
import { encName, pool, productHeader, sleep, stop, streamName } from "./common.ts";

export async function touch(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const { o, http, meter } = ctx;
  const rate = o.num("rate", 300);
  const passes = o.int("passes", 1);
  const concurrency = o.int("concurrency", 512);
  const append = o.int("append", 1) !== 0;
  const recordBytes = o.int("record-bytes", 64);
  const maxBytes = o.has("max-bytes") ? o.int("max-bytes", 0) : undefined;
  // --abort-on-refusal ends the run at the first answer that is not a 200: a
  // touch leg that is refused measures nothing, however long it would go on.
  const abortOnRefusal = o.has("abort-on-refusal");
  const refused = (name: string, status: number): void => {
    failed++;
    if (!abortOnRefusal) return;
    process.stderr.write(`[k2gen touch] abort: ${name} answered ${status}\n`);
    process.exitCode = 3;
    stop.abort();
  };
  if (!(rate >= 0)) throw new Error("--rate must be >= 0");
  if (passes < 1 || concurrency < 1) throw new Error("--passes and --concurrency must be >= 1");
  const record = JSON.stringify([{ pad: "x".repeat(Math.max(0, recordBytes - 12)) }]);
  const query = maxBytes === undefined ? "" : `?maxBytes=${maxBytes}`;
  // --read-from start reads each stream from its beginning, a catch-up reader
  // whose depth grows by a chunk a pass; tail reads on from where this stream's
  // last read ended, as a client holding its cursor does.
  const readFrom = o.str("read-from", "start");
  if (readFrom !== "start" && readFrom !== "tail") throw new Error("--read-from must be start or tail");
  const cursors = new Map<string, string>();
  const total = ctx.streams * passes;
  let touched = 0, failed = 0;
  const started = performance.now();
  const one = async (i: number) => {
    if (rate > 0) await sleep(started + (i * 1000) / rate - performance.now(), signal);
    if (signal.aborted || stop.signal.aborted) return;
    const name = encName(streamName(ctx.prefix, i % ctx.streams));
    if (append) {
      const sent = performance.now();
      const w = await http.call("POST", `/v1/streams/${name}/records:batch`, { body: record, contentType: "application/json" });
      if (w.status !== 200) return refused(name, w.status);
      meter.appendMs(performance.now() - sent);
    }
    const asked = performance.now();
    const held = readFrom === "tail" ? cursors.get(name) : undefined;
    const from = held === undefined ? query : `${query ? `${query}&` : "?"}cursor=${encodeURIComponent(held)}`;
    const r = await http.call("GET", `/v1/streams/${name}/records${from}`);
    if (r.status === 200) meter.readMs(performance.now() - asked);
    const next = readFrom === "tail" ? productHeader(r.headers, "next-cursor") : null;
    if (next) cursors.set(name, next);
    meter.add(r.status === 200 ? "touches_served" : "touches_refused");
    if (r.status === 200) touched++;
    else refused(name, r.status);
  };
  await pool(total, concurrency, one, signal);
  return { touch: { streams: ctx.streams, passes, rate, concurrency, append, max_bytes: maxBytes ?? null, touched, failed } };
}
