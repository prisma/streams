#!/usr/bin/env python3
"""L6 spike M4: summarize one short L6S point of arm b (audit + read path).

usage: python3 m4-audit-summary.py <results dir> [...] [--out file.json]

For each run directory:
- the two audits (phase 4 right after the load, phase 6 after the tail):
  k2gen's verify block (every acked record read back exactly once, in
  order: blocks/records missing, unexpected, mismatched, incomplete blocks,
  offset errors; order inversions are concurrent requests committed in
  another order than sent, not an error) and the HTTP status counts;
- s3lite's requests per (tier, kind, op) cell during each audit, from the
  phase boundary snapshots (boundaries.jsonl): hseg GETs are the read
  path's ranged GETs, shard/sst GETs its index-row (and tail) reads;
- the sealer's and the reader's counters (/v1/debug/store l6_spill) at
  each audit's end, from the 10 s scrape (scrape.jsonl; no read happens
  outside the audits), and at the run's end (store-end.json);
- PUT bytes by tier over the run (hist must be 0 on arm b) and the
  process-wide count of refused keyed history reads (must be 0: a run that
  counts one is void).
Python 3.9 compatible.
"""
import json
import sys


def jl(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def load(path):
    with open(path) as f:
        return json.load(f)


def phases(d):
    """phase index -> (mode, start_ms, end_ms) from marks.jsonl."""
    out = {}
    for m in jl(f"{d}/marks.jsonl"):
        if m.get("mark") == "phase_start":
            out.setdefault(m["i"], {})["mode"] = m.get("mode")
            out[m["i"]]["start"] = m.get("t_ms") or m.get("t")
        elif m.get("mark") == "phase_end" and "mode" in m:
            out.setdefault(m["i"], {})["end"] = m.get("t_ms") or m.get("t")
            out[m["i"]]["rc"] = m.get("rc")
    return out


def boundary(d, label):
    for b in jl(f"{d}/boundaries.jsonl"):
        if b.get("label") == label:
            return b
    return None


def cell_counts(snap):
    """cell -> total requests (all statuses)."""
    out = {}
    for cell, statuses in (snap or {}).get("s3lite", {}).get("cells", {}).items():
        if isinstance(statuses, dict):
            out[cell] = sum(v for v in statuses.values() if isinstance(v, (int, float)))
    return out


def cells_between(d, a, b):
    ca, cb = cell_counts(boundary(d, a)), cell_counts(boundary(d, b))
    diff = {k: cb.get(k, 0) - ca.get(k, 0) for k in set(ca) | set(cb)}
    return {k: v for k, v in sorted(diff.items()) if v}


def spill_at(d, t_ms):
    """The l6_spill body of the first store scrape at or after t_ms (an idle
    phase follows each audit, so nothing reads between the two), or of the
    run's end snapshot when no scrape follows."""
    for line in jl(f"{d}/scrape.jsonl"):
        if line.get("src") == "store" and line.get("t", 0) >= t_ms:
            return line.get("body", {}).get("l6_spill", {})
    end = load(f"{d}/store-end.json")
    return next(iter(end.get("servers", {}).values()), {}).get("store", {}).get("l6_spill", {})


def shard_view(spill):
    shards = spill.get("shards", {})
    out = {}
    for path, s in shards.items():
        r = s.get("reads", {})
        out[path] = {
            "seals_started": s.get("seals_started"),
            "seals_completed": s.get("seals_completed"),
            "seal_failures": s.get("seal_failures"),
            "objects_put": s.get("objects_put"),
            "pages_sealed": s.get("pages_sealed"),
            "page_bytes_sealed": s.get("page_bytes_sealed"),
            "index_rows": s.get("index_rows"),
            "leak_bytes": s.get("leak_bytes"),
            "deadline_flushes": s.get("deadline_flushes"),
            "ring_evicted_unabsorbed": s.get("ring_evicted_unabsorbed"),
            "reads": {k: r.get(k) for k in (
                "calls", "completed", "rows", "gets", "get_bytes", "whole_chunks",
                "partial_chunks", "pages", "budget_partials", "missing_row_partials",
                "not_found_retryable", "get_retries", "get_errors", "in_flight_max",
                "keyed_unsupported", "lane_reads", "lane_skipped_pages", "race_adoptions")},
            "read_ms": r.get("read_ms"),
        }
    return out


def audit(d, i):
    led = load(f"{d}/phase-{i}.ledger.json")
    v = led.get("verify", {})
    keep = ("ok", "expected_records", "read_records", "read_bytes", "pages", "blocks_expected",
            "blocks_found", "blocks_missing", "records_missing", "unexpected_records",
            "mismatched_records", "incomplete_blocks", "offset_errors", "order_inversions",
            "failed_requests", "sent_requests")
    return {
        "verify": {k: v.get(k) for k in keep},
        "status_counts": led.get("status_counts"),
        "duration_s": led.get("duration_s"),
        "read_p50_ms": led.get("read_p50_ms"),
        "read_p99_ms": led.get("read_p99_ms"),
        "requests": led.get("requests"),
    }


def put_by_tier(d):
    s0, s1 = load(f"{d}/s3lite-start.json"), load(f"{d}/s3lite-end.json")
    p0 = {t: v.get("put", 0) for t, v in s0["put_bytes"]["by_tier"].items()}
    p1 = {t: v.get("put", 0) for t, v in s1["put_bytes"]["by_tier"].items()}
    return {t: p1.get(t, 0) - p0.get(t, 0) for t in sorted(set(p0) | set(p1))}


def summarize(d):
    ph = phases(d)
    audits = {i: p for i, p in ph.items() if p.get("mode") == "audit"}
    res = {"run": d, "phases": {i: ph[i] for i in sorted(ph)}, "audits": {}}
    for i in sorted(audits):
        a = audit(d, i)
        a["s3lite_requests"] = cells_between(d, f"phase-{i}-start", f"phase-{i}-end")
        a["l6_spill_at_end"] = shard_view(spill_at(d, audits[i].get("end") or 0))
        res["audits"][i] = a
    load_end = ph.get(3, {}).get("end") or 0
    res["l6_spill_at_load_end"] = shard_view(spill_at(d, load_end))
    end = load(f"{d}/store-end.json")
    server = next(iter(end.get("servers", {}).values()), {})
    spill = server.get("store", {}).get("l6_spill", {})
    res["l6_spill_at_run_end"] = shard_view(spill)
    res["keyed_history_reads_unsupported"] = spill.get("keyed_history_reads_unsupported")
    res["put_bytes_by_tier"] = put_by_tier(d)
    res["clean"] = (
        all(a["verify"].get("ok") is True for a in res["audits"].values())
        and len(res["audits"]) == 2
        and res["keyed_history_reads_unsupported"] == 0
        and res["put_bytes_by_tier"].get("hist", 0) == 0
    )
    return res


def main():
    args = sys.argv[1:]
    out = None
    if "--out" in args:
        k = args.index("--out")
        out = args[k + 1]
        args = args[:k] + args[k + 2:]
    res = [summarize(d.rstrip("/")) for d in args]
    text = json.dumps(res, indent=1, sort_keys=True)
    if out:
        with open(out, "w") as f:
            f.write(text + "\n")
    for r in res:
        print(r["run"])
        for i, a in sorted(r["audits"].items()):
            v = a["verify"]
            reads = {p: s["reads"] for p, s in a["l6_spill_at_end"].items()}
            print(f"  audit phase {i}: ok={v['ok']} expected={v['expected_records']} read={v['read_records']} "
                  f"missing={v['blocks_missing']}/{v['records_missing']} unexpected={v['unexpected_records']} "
                  f"mismatched={v['mismatched_records']} incomplete={v['incomplete_blocks']} offset_errors={v['offset_errors']} "
                  f"inversions={v['order_inversions']} status={a['status_counts']}")
            print(f"    s3lite: {a['s3lite_requests']}")
            print(f"    reads (cumulative at the audit's end): {reads}")
        print(f"  keyed refused: {r['keyed_history_reads_unsupported']}; PUT bytes by tier: {r['put_bytes_by_tier']}; clean: {r['clean']}")


if __name__ == "__main__":
    main()
