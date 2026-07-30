#!/usr/bin/env python3
"""Driver + gates for the mature second-absorption stress (run-mature.sh).

Phases: seed N x DEPTH (wave 1) -> settle -> +1 record each (wave 2) ->
watch trim telemetry until convergence -> integrity read-back.
Stdlib only. Gates documented in run-mature.sh.
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
OUT = os.environ["MATURE_OUT"]
LABEL = os.environ["MATURE_LABEL"]
SRV_PID = int(os.environ["SRV_PID"])

N = int(os.environ.get("MATURE_STREAMS", "1024"))
DEPTH = int(os.environ.get("MATURE_DEPTH", "2048"))
BATCH = int(os.environ.get("MATURE_BATCH", "256"))
# Wave 2 must CROSS the sparse-deferral bar (ABSORB_MIN_BYTES_FOR_AGE,
# 256 KiB at the wide posture) or nothing re-absorbs and the trim wave
# this test exists to bound never fires: 6 x 64 KiB = 384 KiB/stream.
WAVE2_RECS = int(os.environ.get("MATURE_WAVE2_RECS", "6"))
WAVE2_BYTES = int(os.environ.get("MATURE_WAVE2_BYTES", str(64 * 1024)))
BUDGET = int(os.environ.get("MATURE_TRIM_BUDGET", "65536"))
# Gauge gate: the drain oscillates in a compaction sawtooth (4 shard DBs
# each retire budget-sized tombstone batches per tick, and compaction
# rewrites the record-laden SSTs they cover) — measured peak 1,168 MB at
# the wide posture, settling back to ~920. The gate sits below the
# 1,400 MB shed line so a genuinely unbounded delete batch (the failure
# class this test exists for: +190 MB per 2.1M-op batch, multi-GiB at
# the 67M fleet shape) still fails while legitimate churn does not.
RSS_GATE = int(os.environ.get("MATURE_RSS_GATE_MB", "1300"))
CONC = int(os.environ.get("MATURE_CONC", "48"))
PAD = "x" * 160

series = open(f"{OUT}/mature.jsonl", "w")


def req(method, path, body=None, headers=None, timeout=30):
    r = urllib.request.Request(f"{BASE}{path}", method=method, data=body)
    r.add_header("authorization", f"Bearer {AUTH}")
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return resp.status, dict(resp.headers), resp.read()


def sreq(method, path, body=None, headers=None, attempts=6):
    for a in range(attempts):
        try:
            st, h, b = req(method, path, body, headers)
            if st < 300:
                return st, h, b
        except Exception as e:  # noqa: BLE001 - retry every transport error
            if a == attempts - 1:
                raise
        time.sleep(0.1 * (2**a) + random.random() * 0.05)
    raise RuntimeError(f"{method} {path} kept failing")


def debug(path):
    _, _, b = sreq("GET", path)
    return json.loads(b)


def rss_kb():
    out = os.popen(f"ps -o rss= -p {SRV_PID}").read().strip()
    return int(out) if out else 0


def name(i):
    return f"m{LABEL}-{i}"


def batch_body(count, start_seq):
    return json.dumps(
        [{"i": start_seq + j, "pad": PAD} for j in range(count)]
    ).encode()


HDRS = {"stream-encryption-key": KEY, "content-type": "application/json"}


def create_and_seed(i):
    sreq("PUT", f"/v1/stream/{name(i)}", b"", HDRS)
    seq = 0
    while seq < DEPTH:
        n = min(BATCH, DEPTH - seq)
        sreq("POST", f"/v1/stream/{name(i)}", batch_body(n, seq), HDRS)
        seq += n
    return i


def wave2(i):
    body = json.dumps(
        [{"i": DEPTH + j, "pad": "y" * WAVE2_BYTES} for j in range(WAVE2_RECS)]
    ).encode()
    sreq("POST", f"/v1/stream/{name(i)}", body, HDRS)
    return i


def sample(tag):
    load = debug("/v1/debug/load")
    row = {
        "t": round(time.time(), 1),
        "tag": tag,
        "rss_mb": load.get("rss_mb"),
        "ps_rss_mb": round(rss_kb() / 1024, 1),
        "trim": load.get("trim"),
        "admit_shed": load.get("admit_shed"),
        "absorb_lag_max": load.get("absorb_lag_max_secs"),
    }
    series.write(json.dumps(row) + "\n")
    series.flush()
    return load


def settle_absorption(deadline_s, tag):
    """Wait until the aggregate absorb backlog reads zero, 3 in a row."""
    consecutive = 0
    end = time.time() + deadline_s
    while time.time() < end:
        u = debug("/v1/debug/usage")
        b = u["absorb_backlog"]
        sample(tag)
        if b["streams"] == 0 and b["eligible"] == 0:
            consecutive += 1
            if consecutive >= 3:
                return True
        else:
            consecutive = 0
        time.sleep(2)
    return False


def read_count(i):
    """Paginate a full read of stream i, return records seen. The first
    page carries no offset (start-of-stream); every subsequent page
    follows the server's Stream-Next-Offset token verbatim."""
    tok, total = None, 0
    for _ in range(4096):
        path = f"/v1/stream/{name(i)}"
        if tok is not None:
            path += f"?offset={tok}"
        st, h, b = sreq("GET", path, None, HDRS)
        body = b.strip()
        n = 0
        if body:
            recs = json.loads(body)
            n = len(recs) if isinstance(recs, list) else 1
        total += n
        nxt = h.get("Stream-Next-Offset")
        if h.get("Stream-Up-To-Date", "").lower() == "true" or not nxt or nxt == tok:
            break
        if n == 0 and nxt == tok:
            break
        tok = nxt
    return total


fails = []


def gate(ok, label):
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    if not ok:
        fails.append(label)


t0 = time.time()
print(
    f"MATURE: {N} streams x {DEPTH} records + wave2 {WAVE2_RECS}x{WAVE2_BYTES}B, "
    f"budget {BUDGET}, wide posture"
)

with ThreadPoolExecutor(CONC) as ex:
    done = 0
    for _ in ex.map(create_and_seed, range(N)):
        done += 1
        if done % 128 == 0:
            print(f"  seeded {done}/{N} ({int(time.time() - t0)}s)")
            sample("seed")
print(f"  wave-1 ingest done in {int(time.time() - t0)}s")

if not settle_absorption(900, "settle1"):
    print("FATAL: wave-1 absorption never settled")
    sys.exit(2)
base = sample("settled1")
base_total = base["trim"]["deletes_total"]
print(
    f"  wave-1 settled at {int(time.time() - t0)}s; "
    f"trim so far: {base['trim']}"
)

# ---- wave 2: the dangerous transition ---------------------------------
t2 = time.time()
with ThreadPoolExecutor(CONC) as ex:
    list(ex.map(wave2, range(N)))
print(f"  wave-2 appended in {int(time.time() - t2)}s; watching trim drain")

max_batch_seen = 0
max_rss = 0.0
debt_seen = 0
stable = 0
prev_total = -1
deadline = time.time() + 1200
converged = False
while time.time() < deadline:
    load = sample("drain")
    tr = load["trim"]
    max_batch_seen = max(max_batch_seen, tr["deletes_max_batch"])
    max_rss = max(max_rss, load["rss_mb"] or 0.0)
    debt_seen = max(debt_seen, tr["debt_streams"])
    u = debug("/v1/debug/usage")
    b = u["absorb_backlog"]
    if (
        tr["debt_streams"] == 0
        and b["streams"] == 0
        and tr["deletes_total"] == prev_total
    ):
        stable += 1
        if stable >= 3:
            converged = True
            break
    else:
        stable = 0
    prev_total = tr["deletes_total"]
    time.sleep(2)

final = sample("final")
tr = final["trim"]
owed = N * DEPTH
print(f"  drain finished at {int(time.time() - t0)}s: {tr}")
print("gates:")
gate(converged, "convergence: debt drained to zero and totals went quiet")
gate(
    max_batch_seen <= BUDGET,
    f"G1 max deletes in any commit {max_batch_seen} <= budget {BUDGET}",
)
gate(
    debt_seen > 0,
    f"G2 trim debt observed after wave 2 (decoupling engaged): peak {debt_seen} streams",
)
gate(
    tr["deletes_total"] == owed,
    f"G3 owed offsets trimmed exactly once: total {tr['deletes_total']} == {owed}",
)
gate(
    max_rss < RSS_GATE,
    f"G4 rss gauge bounded: peak {max_rss:.0f} MB < {RSS_GATE} MB (shed 1400)",
)

expected = DEPTH + WAVE2_RECS
sampled = random.sample(range(N), 32)
bad = 0
with ThreadPoolExecutor(16) as ex:
    for i, cnt in zip(sampled, ex.map(read_count, sampled)):
        if cnt != expected:
            print(f"    integrity: stream {i} read {cnt} != {expected}")
            bad += 1
gate(bad == 0, f"G5 integrity: 32 sampled streams read back {expected} records each")

report = {
    "streams": N,
    "depth": DEPTH,
    "budget": BUDGET,
    "owed_deletes": owed,
    "trim_final": tr,
    "max_batch_seen": max_batch_seen,
    "peak_debt_streams": debt_seen,
    "max_rss_mb": max_rss,
    "wall_s": int(time.time() - t0),
    "fails": fails,
}
with open(f"{OUT}/mature-report.json", "w") as f:
    json.dump(report, f, indent=2)
print(json.dumps(report, indent=2))
sys.exit(1 if fails else 0)
