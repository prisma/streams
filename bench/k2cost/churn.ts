// churn: stream lifecycles. Each lifecycle is
//   PUT create -> append S KiB -> read it all back once -> DELETE
// or, with --end ttl:SECS, a create carrying {"expiry":{"idle":"SECSs"}}
// and no delete (the stream expires after SECS idle).
//
// Lifecycle i uses stream `${prefix}${i:06}` (give each churn run a
// fresh prefix). Lifecycles start open loop at --rate per second, or
// closed loop over --concurrency workers (default 4).

import { backoffMs, encName, parseJson, pool, sleep, stop, type Resp } from "./common.ts";
import type { Ctx } from "./produce.ts";
import { PayloadGen, payloadKind } from "./corpus.ts";

export async function churn(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const { o, http, meter } = ctx;
  const count = o.int("count", 100);
  const sizeKiB = o.num("size-kib", 10);
  const end = o.str("end", "delete");
  const recordBytes = o.int("record-bytes", 1024);
  const payload = payloadKind(o.str("payload", "corpus"));
  const concurrency = o.int("concurrency", 4);
  const rate = o.num("rate", 0);
  let ttl = 0;
  if (end.startsWith("ttl:")) {
    ttl = Number(end.slice(4));
    if (!(ttl > 0) || !Number.isInteger(ttl)) throw new Error("--end ttl:SECS needs a positive integer");
  } else if (end !== "delete") throw new Error("--end must be delete or ttl:SECS");
  const isBytes = payload === "bytes";
  // A batch carries up to --records records (JSON only) and 1 MiB.
  const perReq = isBytes ? 1 : Math.max(1, Math.min(o.int("records", 100), Math.floor((1 << 20) / recordBytes)));
  const recsPerLife = Math.max(1, Math.round((sizeKiB * 1024) / recordBytes));
  const gen = new PayloadGen(payload, recordBytes, ctx.seed, 4);
  const doc: Record<string, unknown> = { format: { kind: isBytes ? "bytes" : "json" } };
  if (ttl) doc.expiry = { idle: `${ttl}s` };
  const createBody = JSON.stringify(doc);
  const name = (i: number) => `${ctx.prefix}${String(i).padStart(6, "0")}`;
  let completed = 0, failed = 0, verifyMismatch = 0;
  const failures: Record<string, number> = {};
  const fail = (step: string, r: Resp) => {
    const k = `${step}:${r.status}`;
    failures[k] = (failures[k] ?? 0) + 1;
    failed++;
  };
  /** One request with up to 5 retries on 429/503/transport errors. */
  const retrying = async (f: () => Promise<Resp>): Promise<Resp> => {
    let r = await f();
    for (let a = 0; a < 5 && (r.status === 429 || r.status === 503 || r.status === 0) && !stop.signal.aborted; a++) {
      meter.add("retries");
      await sleep(backoffMs(r), stop.signal);
      r = await f();
    }
    return r;
  };

  const life = async (i: number): Promise<void> => {
    const n = name(i);
    const base = `/v1/streams/${encName(n)}`;
    let r = await retrying(() => http.call("PUT", base, { body: createBody, contentType: "application/json" }));
    if (r.status !== 201 && r.status !== 200) return fail("create", r);
    meter.add("streams_created");
    let sent = 0, sentBytes = 0;
    while (sent < recsPerLife) {
      const k = Math.min(perReq, recsPerLife - sent);
      let body: string | Uint8Array, pbytes = 0, path = "/records";
      if (isBytes) {
        body = gen.raw();
        pbytes = body.byteLength;
      } else if (k === 1) {
        body = gen.json();
        pbytes = body.length;
      } else {
        const rs = Array.from({ length: k }, () => gen.json());
        pbytes = rs.reduce((a, s) => a + s.length, 0);
        body = `[${rs.join(",")}]`;
        path = "/records:batch";
      }
      const ts = performance.now();
      r = await retrying(() => http.call("POST", base + path, { body, contentType: isBytes ? "application/octet-stream" : "application/json" }));
      if (r.status !== 200) return fail("append", r);
      meter.appendMs(performance.now() - ts);
      const count = parseJson<{ count?: number }>(r, meter, "append")?.count ?? k;
      meter.add("acked_requests");
      meter.add("acked_records", count);
      meter.add("payload_bytes", pbytes);
      const tl = meter.tally(n);
      tl.acked_records += count;
      tl.acked_bytes += pbytes;
      sent += k;
      sentBytes += pbytes;
    }
    // Read it all back once.
    let cursor = "beginning", got = 0, gotBytes = 0;
    for (;;) {
      r = await retrying(() => http.call("GET", `${base}/records?cursor=${encodeURIComponent(cursor)}`));
      if (r.status !== 200 && r.status !== 204) return fail("read", r);
      meter.readMs(r.ms);
      if (r.status === 200 && r.body.byteLength > 0) {
        if (isBytes) {
          gotBytes += r.body.byteLength;
          got += Math.round(r.body.byteLength / recordBytes);
        } else {
          const arr = parseJson<unknown[]>(r, meter, "read") ?? [];
          got += arr.length;
          gotBytes += arr.length ? r.body.byteLength - 2 - (arr.length - 1) : 0;
        }
      }
      const next = r.headers?.get("prisma-next-cursor");
      if (r.headers?.get("prisma-up-to-date") === "true" || r.status === 204 || !next || next === cursor) break;
      cursor = next;
    }
    meter.add("delivered_records", got);
    meter.add("delivered_payload_bytes", gotBytes);
    const tl = meter.tally(n);
    tl.delivered_records += got;
    tl.delivered_bytes += gotBytes;
    if (gotBytes !== sentBytes) verifyMismatch++;
    if (!ttl) {
      r = await retrying(() => http.call("DELETE", base));
      if (r.status !== 204) return fail("delete", r);
      meter.add("streams_deleted");
    }
    meter.add("lifecycles");
    completed++;
  };

  if (rate > 0) {
    // Open loop: lifecycle i starts at i / rate seconds.
    const t0 = performance.now();
    const running: Promise<void>[] = [];
    for (let i = 0; i < count && !signal.aborted; i++) {
      const due = t0 + (i * 1000) / rate;
      const wait = due - performance.now();
      if (wait > 0) await sleep(wait, signal);
      if (signal.aborted) break;
      running.push(life(i));
    }
    await Promise.all(running);
  } else {
    await pool(count, concurrency, life, signal);
  }
  return {
    churn: {
      count, size_kib: sizeKiB, end, ttl_secs: ttl || null, record_bytes: recordBytes, payload,
      records_per_lifecycle: recsPerLife, records_per_request: perReq,
      completed, failed, failures, verify_mismatched_lifecycles: verifyMismatch,
    },
  };
}
