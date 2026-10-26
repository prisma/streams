// mux: a tenant's whole read side through ONE multiplexed wait (layout 6A
// package 2, `POST /v1/projects/{p}/live:wait`): a tail on every stream and
// a consumer group on the first --group-streams of them, in one long-poll
// loop, as an SDK client with `liveMux` waits. It replaces a `tail` phase
// plus a `group` phase of the same tenant (profile S: 20 tails, a group on
// 5 streams, 25 held connections) with one held connection.
//
// Each answer is counted where the client sees it: a tail entry's records
// (JSON array) and payload bytes, a group entry's messages, each checked by
// the oracle as the per-stream modes check theirs. A group batch is settled
// (`:settle`, ack each) before the next wait, as `group --settle every`
// settles before its next pull; the next wait carries every subscription
// again with its cursor. Tails start from `now` (the first answer pins
// every cursor at once), so a tail sees every record acked after it.

import {
  type Http, type Meter, backoffMs, b64Len, encName, errCode, parseJson, pool, sleep, stop, streamName,
} from "./common.ts";
import { countOracle } from "./oracle.ts";
import { Producer, produceSpec, type Ctx } from "./produce.ts";

interface Entry {
  id: string;
  records?: unknown[];
  cursor?: string;
  backlog?: number;
  messages?: Array<{ leaseToken: string; attempts: number; value: unknown }>;
  status?: number;
  error?: { code: string };
}

interface Sub {
  id: string;
  kind: "tail" | "group";
  stream: string;
  cursor?: string;
}

/** The consumer every group stream needs (idempotent PUT, as `group`). */
async function consumers(http: Http, names: string[], group: string, cfg: string, signal: AbortSignal): Promise<Set<string>> {
  const bytes = new Set<string>();
  await pool(names.length, 8, async (i) => {
    const name = names[i]!;
    const md = await http.call("GET", `/v1/streams/${encName(name)}`);
    if (md.status === 200) {
      const j = JSON.parse(new TextDecoder().decode(md.body)) as { contentType?: string };
      if (!(j.contentType ?? "application/json").startsWith("application/json")) bytes.add(name);
    }
    for (let a = 0; a < 6 && !signal.aborted; a++) {
      const r = await http.call("PUT", `/v1/streams/${encName(name)}/consumers/${encodeURIComponent(group)}`, { body: cfg, contentType: "application/json" });
      if (r.status === 200 || r.status === 201 || (r.status === 409 && errCode(r) === "consumer_config_conflict")) return;
      await sleep(backoffMs(r), stop.signal);
    }
  });
  return bytes;
}

export async function mux(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const { o, http, meter } = ctx;
  const project = o.str("project", ctx.owner?.project ?? "");
  if (!project) throw new Error("mux needs --project (or a tenants project)");
  const waitMs = o.int("wait-ms", 25_000);
  const group = o.str("group", "s-cg");
  const groupStreams = o.int("group-streams", 0);
  const pull = o.int("pull", 10);
  const settle = o.str("settle", "every");
  if (settle !== "every" && settle !== "none") throw new Error("--settle must be every|none");
  const visibilityMs = o.int("visibility-ms", 30_000);
  const key = ctx.http.hdrs.key();
  const names = Array.from({ length: ctx.streams }, (_, i) => streamName(ctx.prefix, i));
  const grouped = names.slice(0, Math.min(groupStreams, names.length));
  const cfg = JSON.stringify({ maxBatchRecords: pull, visibilityTimeoutMs: visibilityMs });
  const bytesStreams = grouped.length ? await consumers(http, grouped, group, cfg, signal) : new Set<string>();
  const subs: Sub[] = [
    ...names.map((stream, i): Sub => ({ id: `t${i}`, kind: "tail", stream, cursor: "now" })),
    ...grouped.map((stream, i): Sub => ({ id: `g${i}`, kind: "group", stream })),
  ];
  const byId = new Map(subs.map((s) => [s.id, s]));
  const spec = produceSpec(o, false);
  const prod = spec ? new Producer(ctx, spec) : null;
  const produced = prod?.run(signal);
  const path = `/v1/projects/${encodeURIComponent(project)}/live:wait`;
  meter.connect(1);
  try {
    while (!signal.aborted) {
      const body = JSON.stringify({
        waitMs,
        subs: subs.map((s) => s.kind === "tail"
          ? { id: s.id, kind: "tail", stream: s.stream, key, cursor: s.cursor }
          : { id: s.id, kind: "group", stream: s.stream, key, group, max: pull, visibilityMs }),
      });
      // Not cancelled at the deadline: an answer that leased messages is
      // always read and settled, so the run's delivery count is exact.
      const r = await http.call("POST", path, { body, contentType: "application/json", timeoutMs: waitMs + 30_000 });
      if (r.status !== 200) {
        if (signal.aborted) return done();
        await sleep(backoffMs(r), signal);
        continue;
      }
      meter.readMs(r.ms);
      meter.add("live_waits");
      const answer = parseJson<{ results: Entry[] }>(r, meter, "live_wait");
      const results = answer?.results ?? [];
      if (results.length === 0) meter.add("empty_polls");
      for (const entry of results) {
        const sub = byId.get(entry.id);
        if (!sub) continue;
        if (entry.error) {
          meter.errorCode(`${entry.status ?? 0}:${entry.error.code}`);
          continue;
        }
        if (sub.kind === "tail") {
          sub.cursor = entry.cursor ?? sub.cursor;
          const recs = entry.records ?? [];
          if (ctx.owner) countOracle(meter, ctx.owner, recs);
          deliver(meter, sub.stream, recs.length, recs.reduce<number>((n, v) => n + Buffer.byteLength(JSON.stringify(v)), 0));
          meter.add("tail_answers");
          continue;
        }
        const msgs = entry.messages ?? [];
        if (msgs.length === 0) continue;
        meter.add("pulls");
        const isBytes = bytesStreams.has(sub.stream);
        if (ctx.owner && !isBytes) countOracle(meter, ctx.owner, msgs.map((m) => m.value));
        let n = 0;
        for (const m of msgs) {
          n += isBytes && typeof m.value === "string" ? b64Len(m.value) : Buffer.byteLength(JSON.stringify(m.value));
          if (m.attempts > 1) meter.add("redelivered_records");
        }
        deliver(meter, sub.stream, msgs.length, n);
        if (settle !== "every") continue;
        const sb = JSON.stringify({ acks: msgs.map((m) => ({ leaseToken: m.leaseToken })), retries: [], extends: [] });
        const s = await http.call("POST", `/v1/streams/${encName(sub.stream)}/consumers/${encodeURIComponent(group)}:settle`, { body: sb, contentType: "application/json" });
        meter.add("settles");
        if (s.status === 200) {
          const sj = parseJson<Record<string, number>>(s, meter, "settle") ?? {};
          for (const k of ["acked", "retried", "extended", "dlq", "stale"]) meter.add(`settle_${k}`, sj[k] ?? 0);
        }
      }
    }
  } finally {
    meter.connect(-1);
  }
  return done();

  async function done(): Promise<Record<string, unknown>> {
    const producer = produced ? await produced : {};
    return { mux: { tails: names.length, groups: grouped.length, group, pull, settle, wait_ms: waitMs, visibility_ms: visibilityMs }, ...producer };
  }
}

function deliver(meter: Meter, stream: string, records: number, bytes: number): void {
  if (records === 0 && bytes === 0) return;
  meter.add("delivered_records", records);
  meter.add("delivered_payload_bytes", bytes);
  const tl = meter.tally(stream);
  tl.delivered_records += records;
  tl.delivered_bytes += bytes;
}
