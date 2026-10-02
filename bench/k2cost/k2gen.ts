#!/usr/bin/env bun
// k2gen: the K2 cost experiment's load generator (target/k2-design.md
// C3). Product surface only: /v1/streams/{name}, …/records,
// …/records:batch, …/records:long-poll, …/records:sse,
// …/consumers/{c}, …:pull, …:settle.
//
//   bun bench/k2cost/k2gen.ts <mode> [flags]       (bun k2gen.ts help)
//
// Output: --out FILE.jsonl gets one JSON line per 10 s window (and a
// final partial one), --ledger FILE.json the exact totals at exit, and
// stdout one summary line. Exit 0; 3 when a --expect verification
// fails; 2 on a usage error.

import { writeFileSync } from "node:fs";
import {
  DEFAULT_KEY_B64, HeaderSource, badJson, Http, Meter, Opts, Reporter, deadlineSignal, installSignals, parseFlags, stop,
} from "./common.ts";
import { corpusSample, corpusStats } from "./corpus.ts";
import { produce, setup, type Ctx } from "./produce.ts";
import { group, subs, tail, walk } from "./consume.ts";
import { churn } from "./churn.ts";

const USAGE = `k2gen <mode> [flags]

Modes
  setup         create --streams streams (--format json|bytes, --ttl SECS idle expiry, --parallel 8)
  produce       appends; --rate R (open-loop Poisson req/s) | --mbps X (payload MB/s) | --concurrency C
  walk          catch-up reads: --from earliest|lag:SECS --readers K --stagger SECS --parallel P
                --max-bytes B --expect a.ledger.json[,b...] (verify reader 0 against acked counts)
  tail          live subscription per stream: --via long-poll|sse --readers K/stream --wait-ms 25000 --from now|beginning
  group         consumer groups: --pull N (1..1000) --consumers K/stream --settle every|none --group NAME
                --wait-ms 1000 --visibility-ms 30000 --retry-frac F --extend-frac F --until-empty 1
                --dlq STREAM --max-attempts N (dead-letter after N deliveries; the DLQ stream must exist)
                (a pull leases one message per routing key: give the producer --routing-keys K)
  churn         lifecycles: --count N --size-kib S --end delete|ttl:SECS [--rate L/s | --concurrency C]
  subs          --count N idle subscribers over the streams: --via sse|long-poll --connect-rate 20
  corpus-stats  per-record zstd-1 ratio (+55 B frame header) at --sizes 128,256,1024,2048,16384
                for --payloads corpus,b64rand,bytes (--samples 2000, --sample N prints N records)

Common
  --base URL  --headers-file FILE ("Name: value" lines, re-read on change and after a 401)
  --streams N (1)  --prefix STR (k2/s; stream i is PREFIX%05d)  --duration SECS  --seed N (1)
  --out FILE.jsonl  --ledger FILE.json  --key B64 (Prisma-Encryption-Key; the header file may set it)
  --window SECS (10)  --port P (serve the latest window as JSON)  --routing-keys K (keys k0..k{K-1})

Producer (produce; walk/tail/group/subs run one in-process when given --rate/--mbps/--concurrency)
  --records N per request (1 = POST /records; >1 = POST /records:batch, JSON only)
  --record-bytes B (1024)  --payload corpus|b64rand|bytes  --zipf S (0 = uniform stream activity)
  --pool N (cycle N pregenerated records)  --timeout-ms 60000  --drain-secs 60
`;

const PRODUCER = ["rate", "mbps", "concurrency", "records", "record-bytes", "payload", "zipf", "routing-keys", "pool", "timeout-ms", "drain-secs"];
const COMMON = ["base", "headers-file", "streams", "prefix", "duration", "seed", "out", "ledger", "key", "window", "port"];
const KNOWN: Record<string, string[]> = {
  setup: [...COMMON, "format", "ttl", "parallel", "payload"],
  produce: [...COMMON, ...PRODUCER],
  walk: [...COMMON, ...PRODUCER, "from", "readers", "stagger", "parallel", "max-bytes", "expect"],
  tail: [...COMMON, ...PRODUCER, "via", "readers", "wait-ms", "from"],
  group: [...COMMON, ...PRODUCER, "pull", "consumers", "group", "settle", "wait-ms", "visibility-ms", "retry-frac", "extend-frac", "until-empty", "dlq", "max-attempts"],
  churn: [...COMMON, "count", "size-kib", "end", "record-bytes", "payload", "concurrency", "rate", "records"],
  subs: [...COMMON, ...PRODUCER, "count", "via", "connect-rate", "wait-ms"],
  "corpus-stats": ["seed", "samples", "sizes", "payloads", "out", "sample"],
};
// An earliest walk, setup and churn run to completion; --duration caps them.
const UNTIL_DONE = 7 * 86_400;

const { positional, flags } = parseFlags(process.argv.slice(2));
const mode = positional[0] ?? "";
if (!mode || mode === "help" || flags.has("help")) {
  process.stdout.write(USAGE);
  process.exit(mode ? 0 : 2);
}
if (!(mode in KNOWN) || positional.length > 1) {
  process.stderr.write(`k2gen: unknown mode or stray argument: ${positional.join(" ")}\n\n${USAGE}`);
  process.exit(2);
}
const unknown = [...flags.keys()].filter((k) => !KNOWN[mode]!.includes(k));
if (unknown.length) {
  process.stderr.write(`k2gen ${mode}: unknown flag(s) ${unknown.map((k) => `--${k}`).join(" ")}\n`);
  process.exit(2);
}

// Bun's fetch queues requests beyond BUN_CONFIG_MAX_HTTP_REQUESTS (256
// by default) inside the runtime, which would silently close the open
// loop; the variable is read once at start, so re-exec with it raised.
const MAX_HTTP = 16384;
if (mode !== "corpus-stats" && process.env.K2GEN_CHILD !== "1" && Number(process.env.BUN_CONFIG_MAX_HTTP_REQUESTS ?? 0) < MAX_HTTP) {
  const child = Bun.spawn([process.execPath, ...(Bun.main.startsWith("/$bunfs") ? [] : [process.argv[1]!]), ...process.argv.slice(2)], {
    env: { ...process.env, BUN_CONFIG_MAX_HTTP_REQUESTS: String(MAX_HTTP), K2GEN_CHILD: "1" },
    stdio: ["inherit", "inherit", "inherit"],
  });
  for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => child.kill(sig));
  process.exit(await child.exited);
}
installSignals();

const o = new Opts(flags);
try {
  if (mode === "corpus-stats") {
    if (o.has("sample")) {
      for (const r of corpusSample(o.int("seed", 1), Number(o.str("sizes", "1024").split(",")[0]), o.int("sample", 3))) {
        process.stdout.write(r + "\n");
      }
      process.exit(0);
    }
    const rows = corpusStats(o);
    const text = rows.map((r) => JSON.stringify(r)).join("\n") + "\n";
    process.stdout.write(text);
    const out = o.str("out");
    if (out) writeFileSync(out, text);
    process.exit(0);
  }

  const base = o.str("base");
  if (!base) throw new Error("--base URL is required");
  const hdrs = new HeaderSource(o.str("headers-file"), { "prisma-encryption-key": o.str("key", DEFAULT_KEY_B64) });
  const meter = new Meter();
  const http = new Http(base.replace(/\/+$/, ""), hdrs, meter);
  const walkLag = mode === "walk" && o.str("from", "earliest").startsWith("lag:");
  const runsToCompletion = mode === "setup" || mode === "churn" || (mode === "walk" && !walkLag);
  const ctx: Ctx = {
    http,
    meter,
    o,
    prefix: o.str("prefix", "k2/s"),
    streams: o.int("streams", 1),
    seed: o.int("seed", 1),
    duration: o.num("duration", runsToCompletion ? UNTIL_DONE : 60),
  };
  if (ctx.streams < 1) throw new Error("--streams must be >= 1");
  const rep = new Reporter(mode, meter, o.str("out"), o.str("ledger"), o.num("window", 10), o.asObject(), o.has("port") ? o.int("port", 0) : undefined);
  const signal = deadlineSignal(ctx.duration);
  rep.start();
  let result: Record<string, unknown>;
  switch (mode) {
    case "setup": result = { setup: await setup(ctx) }; break;
    case "produce": result = await produce(ctx, signal); break;
    case "walk": result = await walk(ctx, signal); break;
    case "tail": result = await tail(ctx, signal); break;
    case "group": result = await group(ctx, signal); break;
    case "churn": result = await churn(ctx, signal); break;
    case "subs": result = await subs(ctx, signal); break;
    default: throw new Error(`unreachable mode ${mode}`);
  }
  const ledger = await rep.finish({
    base: http.base,
    prefix: ctx.prefix,
    streams: ctx.streams,
    seed: ctx.seed,
    stopped_by_signal: stop.signal.aborted,
    bad_json: badJson,
    ...result,
  });
  const summary: Record<string, unknown> = {
    mode,
    duration_s: ledger.duration_s,
    acked_requests: ledger.acked_requests,
    acked_records: ledger.acked_records,
    payload_bytes: ledger.payload_bytes,
    delivered_records: ledger.delivered_records,
    delivered_payload_bytes: ledger.delivered_payload_bytes,
    status_counts: ledger.status_counts,
    append_p99_ms: ledger.append_p99_ms,
  };
  const v = result.verify as { ok?: boolean } | undefined;
  if (v) summary.verify = v;
  process.stdout.write(JSON.stringify(summary) + "\n");
  process.exit(v && v.ok === false ? 3 : 0);
} catch (e) {
  process.stderr.write(`k2gen ${mode}: ${process.env.K2GEN_DEBUG ? (e as Error).stack : (e as Error).message}\n`);
  process.exit(2);
}
