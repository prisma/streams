#!/usr/bin/env python3
"""Keyed-read acceptance campaign (ROUTING-V3 spec §15) — one arm.

Drives the million-key workload scaled to the local rig against ONE
server build and emits a machine-readable measurement file; the
covering-vs-postings comparison and gates run in keyed-compare.py over
two arm outputs.

Phases: ingest K keys x R rounds x B records (1 KiB) -> absorb settle
-> W read windows (ACTIVE random keys: 1 cold + 2 warm reads each,
full drain, exact-count verification) -> snapshot everything.

Pricing for the COGS gate (documented assumptions, public-Tigris-shaped
+ Compute-shaped CPU): Class A $4.50/M, Class B $0.36/M, storage
$0.02/GiB-month, CPU $0.03/vCPU-hour.
"""
import json
import os
import random
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "http://127.0.0.1:8090"
AUTH = os.environ["AUTH_TOKEN"]
KEY = os.environ["STREAM_KEY"]
OUT = os.environ["KEYED_OUT"]
LABEL = os.environ["KEYED_LABEL"]
SRV_PID = int(os.environ["SRV_PID"])

K = int(os.environ.get("KEYED_KEYS", "20000"))
B = int(os.environ.get("KEYED_BATCH", "1"))
R = int(os.environ.get("KEYED_ROUNDS", "2"))
ACTIVE = int(os.environ.get("KEYED_ACTIVE", "200"))
WINDOWS = int(os.environ.get("KEYED_WINDOWS", "3"))
CONC = int(os.environ.get("KEYED_CONC", "48"))
REC = 1024
STREAM = f"kc-{LABEL}"

random.seed(42)


def req(method, path, body=None, headers=None, timeout=30):
    r = urllib.request.Request(f"{BASE}{path}", method=method, data=body)
    r.add_header("authorization", f"Bearer {AUTH}")
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return resp.status, dict(resp.headers), resp.read()


def sreq(method, path, body=None, headers=None, attempts=6):
    last = None
    for a in range(attempts):
        try:
            return req(method, path, body, headers)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(0.05 * (2**a))
    raise RuntimeError(f"{method} {path}: {last}")


def debug(path):
    _, _, b = sreq("GET", path)
    return json.loads(b)


def cpu_seconds():
    out = os.popen(f"ps -o cputime= -p {SRV_PID}").read().strip()
    if not out:
        return 0.0
    parts = out.replace("-", ":").split(":")
    parts = [float(x) for x in parts]
    secs = 0.0
    for p in parts:
        secs = secs * 60 + p
    return secs


def s3lite(path):
    with urllib.request.urlopen(f"http://127.0.0.1:9500{path}", timeout=10) as r:
        return json.loads(r.read())


HDRS = {"stream-encryption-key": KEY, "content-type": "application/json"}


def key_name(i):
    return f"k{i:07d}"


def ingest_round(args):
    i, rnd = args
    body = json.dumps(
        [{"k": i, "r": rnd, "j": j, "pad": "x" * (REC - 60)} for j in range(B)]
    ).encode()
    sreq(
        "POST",
        f"/v1/stream/{STREAM}",
        body,
        {**HDRS, "stream-key": key_name(i)},
    )


def read_key_full(i):
    """Full keyed drain; returns (records, pages, wall_ms)."""
    tok, total, pages = None, 0, 0
    t0 = time.time()
    for _ in range(4096):
        path = f"/v1/stream/{STREAM}?key={key_name(i)}"
        if tok is not None:
            path += f"&offset={tok}"
        st, h, b = sreq("GET", path, None, HDRS)
        pages += 1
        body = b.strip()
        if body:
            recs = json.loads(body)
            total += len(recs) if isinstance(recs, list) else 1
        nxt = h.get("Stream-Next-Offset")
        if h.get("Stream-Up-To-Date", "").lower() == "true" or not nxt or nxt == tok:
            break
        tok = nxt
    return total, pages, (time.time() - t0) * 1000.0


def snap(tag):
    row = {
        "tag": tag,
        "t": round(time.time(), 1),
        "cpu_s": cpu_seconds(),
        "stats2": s3lite("/_s3lite/stats2"),
        "load": debug("/v1/debug/load"),
    }
    with open(f"{OUT}/snap-{tag}.json", "w") as f:
        json.dump(row, f)
    return row


t0 = time.time()
print(f"KEYED[{LABEL}]: K={K} B={B} R={R} rec={REC}B active={ACTIVE}x{WINDOWS}w")
sreq("PUT", f"/v1/stream/{STREAM}", b"", HDRS)
snap("t0")

# ---- ingest -----------------------------------------------------------
work = [(i, rnd) for rnd in range(R) for i in range(K)]
with ThreadPoolExecutor(CONC) as ex:
    done = 0
    for _ in ex.map(ingest_round, work):
        done += 1
        if done % 10000 == 0:
            print(f"  ingest {done}/{len(work)} ({int(time.time() - t0)}s)")
print(f"  ingest done in {int(time.time() - t0)}s")
snap("ingested")

# ---- absorb settle ----------------------------------------------------
deadline = time.time() + 1200
settled = 0
while time.time() < deadline:
    u = debug("/v1/debug/usage")
    b = u["absorb_backlog"]
    if b["streams"] == 0 and b["eligible"] == 0 and u["deferred_sparse"]["streams"] == 0:
        settled += 1
        if settled >= 3:
            break
    else:
        settled = 0
    time.sleep(2)
else:
    print("FATAL: absorption never settled")
    sys.exit(2)
print(f"  absorbed at {int(time.time() - t0)}s")
snap("absorbed")

# ---- read windows -----------------------------------------------------
expected = B * R
cold_ms, warm_ms = [], []
read_errors = 0
for w in range(WINDOWS):
    keys = random.sample(range(K), ACTIVE)
    for i in keys:
        n, _pages, ms = read_key_full(i)
        cold_ms.append(ms)
        if n != expected:
            read_errors += 1
            print(f"    MISMATCH key {i}: {n} != {expected}")
        for _ in range(2):
            n2, _p, ms2 = read_key_full(i)
            warm_ms.append(ms2)
            if n2 != expected:
                read_errors += 1
    print(f"  window {w + 1}/{WINDOWS} done ({int(time.time() - t0)}s)")
snap("read")


def pct(v, p):
    if not v:
        return 0.0
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))]


report = {
    "label": LABEL,
    "keys": K,
    "batch": B,
    "rounds": R,
    "record_bytes": REC,
    "expected_per_key": expected,
    "read_errors": read_errors,
    "cold_p50_ms": round(pct(cold_ms, 0.50), 2),
    "cold_p99_ms": round(pct(cold_ms, 0.99), 2),
    "warm_p50_ms": round(pct(warm_ms, 0.50), 2),
    "warm_p99_ms": round(pct(warm_ms, 0.99), 2),
    "cold_reads": len(cold_ms),
    "warm_reads": len(warm_ms),
    "wall_s": int(time.time() - t0),
}
with open(f"{OUT}/keyed-report.json", "w") as f:
    json.dump(report, f, indent=2)
print(json.dumps(report, indent=2))
sys.exit(1 if read_errors else 0)
