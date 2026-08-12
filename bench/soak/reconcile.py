#!/usr/bin/env python3
"""EXACT accepted-vs-durable reconciliation (R25-G, exact mode R26-8).

Unabsorbed records are DURABLE — absorption moves them from the shard
tier to history; it is not the durability boundary. So this check runs
regardless of absorber lag, every campaign.

Two layers, and the second is the one that proves integrity:

1. COUNT check: generator-acknowledged records vs the durable tail's
   next offset (HTTP HEAD — see the trap note below).
2. LEDGER check (R26-8): the generator serves /ledger — every request's
   op id filed under exactly one disposition (acked / rejected /
   ambiguous, as compressed ranges) — and every record embeds
   {"op": request_seq, "b": batch_pos}. The full stream is walked and
   verified:
     * every acked op appears EXACTLY once, with every batch position;
     * definitively rejected ops are absent;
     * ambiguous ops (transport failure after send) appear 0 or 1
       times — and when they appear, completely;
     * nothing else appears, and nothing appears twice.
   This closes the two holes the count check cannot see: a lost acked
   write masked by an uncounted late completion, and a duplicate
   masked by a shortfall elsewhere.
"""
import json, os, sys, urllib.request

S = os.environ["SOAK_HOME"]
AUTH = open(f"{S}/auth.txt").read().strip()
BATCH = int(os.environ.get("BENCH_BATCH", "10"))

# Crockford-style base32 offset token (src/offsets.rs): 26 chars encode
# a 130-bit value ((raw_seq << 32) << 2), where raw_seq is the NEXT
# offset (record count). The 2026-08-11 probe run failed here by
# treating the token as a plain integer.
_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

def decode_next_offset(token):
    if not token or len(token) != 26:
        return 0
    value = 0
    for ch in token:
        value = (value << 5) | _ALPHABET.index(ch)
    return (value >> 2) >> 32  # strip padding, then epoch/in_block low bits

def get(url, headers=None, timeout=120, method="GET"):
    req = urllib.request.Request(url, method=method, headers={
        "Authorization": f"Bearer {AUTH}", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        # r.headers stays an email.message.Message: case-INSENSITIVE
        # lookup. dict(r.headers) silently made every header read
        # case-sensitive — fine against an edge that capitalizes,
        # zero against a local HTTP/1.1 axum that lowercases.
        return r.status, r.headers, r.read()

def expand(ranges):
    out = set()
    for a, b in ranges:
        out.update(range(a, b + 1))
    return out

def walk_stream(server, name, skey):
    """Yield every record of one stream, paged from the horizon.

    HTTP HEAD is the tail probe: Stream-Next-Offset = the durable tail's
    next offset, no body. A GET with `?head=1` is NOT that — the raw
    route ignores the unknown param and serves a full page from the
    horizon, so its Stream-Next-Offset is merely the end of that page
    (the 2026-08-11 campaign "lost" 97% of records to that misread).
    """
    _, hdrs, _ = get(f"{server}/v1/stream/{name}",
                     headers={"Stream-Encryption-Key": skey}, method="HEAD")
    tail_tok = hdrs.get("Stream-Next-Offset", "")
    tail = decode_next_offset(tail_tok)
    seen = 0
    tok = None
    while seen < tail:
        url = f"{server}/v1/stream/{name}"
        if tok:
            url += f"?offset={tok}"
        _, hdrs, body = get(url, headers={"Stream-Encryption-Key": skey})
        page = json.loads(body)
        if not page:
            raise RuntimeError(
                f"{name}: empty page at {seen}/{tail} — walk stalled")
        for rec in page:
            yield rec
        seen += len(page)
        tok = hdrs.get("Stream-Next-Offset", "")
    if seen != tail:
        raise RuntimeError(f"{name}: walked {seen} records, tail says {tail}")

def reconcile(region):
    server = open(f"{S}/url-server-{region}.txt").read().strip()
    gen = open(f"{S}/url-gen-{region}.txt").read().strip()
    _, _, body = get(f"{gen}/")
    stats = json.loads(body)
    tiers = stats if isinstance(stats, list) else stats.get("tiers", [])
    # Lines carry CUMULATIVE counters; the LAST is the post-join final
    # record (R26-8) — exact, not a snapshot missing in-flight requests.
    last = tiers[-1] if tiers else {}
    ok_requests = last.get("ok", 0)
    acked_records = ok_requests * BATCH

    # The op ledger (absent on pre-R26-8 generators -> count-only mode).
    ledger = None
    try:
        _, _, lbody = get(f"{gen}/ledger")
        ledger = json.loads(lbody)
    except Exception:
        pass

    # Enumerate campaign streams via the catalog; sum durable heads.
    skey = open(f"{S}/skey.txt").read().strip()
    durable = 0
    names = []
    cursor = ""
    while True:
        url = f"{server}/v1/streams?limit=1000&prefix=soak-{region}"
        if cursor:
            url += f"&cursor={cursor}"
        _, _, body = get(url)
        page = json.loads(body)
        page_names = [s["name"] if isinstance(s, dict) else s
                      for s in page.get("streams", page if isinstance(page, list) else [])]
        # The server's prefix param is not a plain startswith filter —
        # verified locally: prefix=soak-local-2 returned soak-local-1
        # too. Filter client-side; trust nothing.
        page_names = [n for n in page_names if n.startswith(f"soak-{region}")]
        # Multi-stream campaigns write ONLY the -N suffixed streams; a
        # bare-base stream alongside them is residue from an earlier
        # (pre-ledger) writer in the same namespace and would fail the
        # op-identity check for records this campaign never wrote.
        # Excluded LOUDLY, never silently.
        if any(n.startswith(f"soak-{region}-") for n in page_names):
            bare = [n for n in page_names if n == f"soak-{region}"]
            for n in bare:
                print(f"  {region}: EXCLUDING pre-campaign residue stream {n!r}")
            page_names = [n for n in page_names if n != f"soak-{region}"]
        for name in page_names:
            _, hdrs, _ = get(f"{server}/v1/stream/{name}",
                headers={"Stream-Encryption-Key": skey}, method="HEAD")
            durable += decode_next_offset(hdrs.get("Stream-Next-Offset", ""))
            names.append(name)
        cursor = page.get("cursor", "") if isinstance(page, dict) else ""
        if not cursor:
            break

    row = {
        "region": region, "acked_records": acked_records,
        "durable_records": durable, "streams": len(names),
        "ambiguous_requests": last.get("ambiguous", 0),
    }

    if ledger is None:
        # Legacy generator: one-sided count bound only.
        row["mode"] = "count-only"
        row["verdict"] = "OK" if durable >= acked_records else "LOSS"
        print(f"  {region}: acked={acked_records} durable={durable} "
              f"(count-only) -> {row['verdict']}")
        return row["verdict"] == "OK", row

    acked = expand(ledger.get("acked", []))
    rejected = expand(ledger.get("rejected", []))
    ambiguous = expand(ledger.get("ambiguous", []))
    problems = []
    if len(acked) != ok_requests:
        problems.append(
            f"ledger acked {len(acked)} != stats ok {ok_requests}")

    # Walk every stream, tally (op, b) exactly-once across the union.
    seen = {}
    walked = 0
    for name in names:
        for rec in walk_stream(server, name, skey):
            walked += 1
            op, b = rec.get("op"), rec.get("b")
            if op is None or b is None:
                problems.append(f"record without op identity in {name}")
                continue
            key = (op, b)
            if key in seen:
                problems.append(f"duplicate record op={op} b={b}")
            seen[key] = seen.get(key, 0) + 1

    landed_ambiguous = 0
    for op in acked:
        missing = [b for b in range(BATCH) if (op, b) not in seen]
        if missing:
            problems.append(f"ACKED op {op} missing positions {missing[:3]}")
    for op in rejected:
        if any((op, b) in seen for b in range(BATCH)):
            problems.append(f"REJECTED op {op} present in the stream")
    for op in ambiguous:
        present = [b for b in range(BATCH) if (op, b) in seen]
        if present and len(present) != BATCH:
            problems.append(f"ambiguous op {op} landed PARTIALLY: {len(present)}/{BATCH}")
        if len(present) == BATCH:
            landed_ambiguous += 1
    known = acked | ambiguous
    for (op, b), n in seen.items():
        if op not in known:
            problems.append(f"record from UNKNOWN op {op}")
        if n > 1:
            problems.append(f"op {op} b {b} appears {n} times")

    expected = (len(acked) + landed_ambiguous) * BATCH
    if walked != expected:
        problems.append(f"walked {walked} records, ledger explains {expected}")

    row["mode"] = "exact-ledger"
    row["walked_records"] = walked
    row["landed_ambiguous"] = landed_ambiguous
    row["problems"] = problems[:20]
    row["verdict"] = "OK" if not problems else "LOSS"
    print(f"  {region}: acked_ops={len(acked)} walked={walked} "
          f"ambig_landed={landed_ambiguous}/{len(ambiguous)} "
          f"problems={len(problems)} -> {row['verdict']}")
    for p in problems[:5]:
        print(f"    !! {p}")
    return row["verdict"] == "OK", row

if __name__ == "__main__":
    regions = sys.argv[1:] or (os.environ.get("SOAK_REGIONS", "").split())
    results, all_ok = [], True
    for r in regions:
        ok, row = reconcile(r)
        results.append(row)
        all_ok = all_ok and ok
    run_id = os.environ.get("SOAK_RUN_ID", "adhoc")
    os.makedirs(f"{S}/results/{run_id}", exist_ok=True)
    json.dump(results, open(f"{S}/results/{run_id}/reconcile.json", "w"), indent=1)
    if not all_ok:
        sys.exit("RECONCILE FAILED — exact op-ledger verification found violations")
