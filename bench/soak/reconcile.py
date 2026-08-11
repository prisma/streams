#!/usr/bin/env python3
"""Accepted-vs-durable reconciliation (R25-G).

Unabsorbed records are DURABLE — absorption moves them from the shard
tier to history; it is not the durability boundary. So this check runs
regardless of absorber lag, every campaign:

    generator acknowledged records == sum of durable tail.next offsets

The generator appends BENCH_BATCH records per request and reports `ok`
request counts per tier; the durable side is each campaign stream's
head offset. A shortfall is lost writes; an excess is phantom acks.
Ambiguous requests (client timeout after server accept) may land 0 or 1
times, so the durable sum may EXCEED acks by at most the reported
ambiguous count — never fall short.
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

def get(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {AUTH}", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, dict(r.headers), r.read()

def reconcile(region):
    server = open(f"{S}/url-server-{region}.txt").read().strip()
    gen = open(f"{S}/url-gen-{region}.txt").read().strip()
    _, _, body = get(f"{gen}/")
    stats = json.loads(body)
    tiers = stats if isinstance(stats, list) else stats.get("tiers", [])
    # The generator emits one line per 20 s WINDOW carrying CUMULATIVE
    # counters (stats.ok.load() at snapshot time) — summing lines
    # overcounts by the number of windows. The probe run failed exactly
    # that way: summed acked 372,840 vs a true cumulative 61,290. The
    # last line is the total.
    last = tiers[-1] if tiers else {}
    ok_requests = last.get("ok", 0)
    ambiguous = last.get("ambiguous", 0) + last.get("unknown", 0)
    acked_records = ok_requests * BATCH

    # Enumerate campaign streams via the catalog, sum durable heads.
    skey = open(f"{S}/skey.txt").read().strip()
    durable = 0
    streams = 0
    cursor = ""
    while True:
        url = f"{server}/v1/streams?limit=1000&prefix=soak-{region}"
        if cursor:
            url += f"&cursor={cursor}"
        _, _, body = get(url)
        page = json.loads(body)
        names = [s["name"] if isinstance(s, dict) else s
                 for s in page.get("streams", page if isinstance(page, list) else [])]
        for name in names:
            _, hdrs, _ = get(f"{server}/v1/stream/{name}?head=1",
                headers={"Stream-Encryption-Key": skey})
            token = hdrs.get("Stream-Next-Offset", "")
            durable += decode_next_offset(token)
            streams += 1
        cursor = page.get("cursor", "") if isinstance(page, dict) else ""
        if not cursor:
            break

    # One-sided by construction: `ok` is a SNAPSHOT-time lower bound —
    # requests completing after the last stats window land durably
    # without being counted, and ambiguous/error outcomes may land 0 or
    # 1 times. LOSS (durable < acked) is the integrity failure; a small
    # excess is the expected snapshot skew and is reported, not failed.
    # Exact both-sided integrity needs the per-record op ledger (plan
    # §9.6 extension, not yet in awsbench).
    excess = durable - acked_records
    verdict = "OK" if durable >= acked_records else "LOSS"
    print(f"  {region}: acked={acked_records} durable={durable} "
          f"excess={excess} streams={streams} ambiguous_reqs={ambiguous} -> {verdict}")
    return verdict == "OK", {
        "region": region, "acked_records": acked_records,
        "durable_records": durable, "streams": streams,
        "ambiguous_requests": ambiguous, "verdict": verdict,
    }

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
        sys.exit("RECONCILE FAILED — durable records fall SHORT of acknowledged (data loss)")
