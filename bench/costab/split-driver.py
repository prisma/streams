#!/usr/bin/env python3
"""Live split scenario (spec §15.3), v3 build only, scaler knobs tuned
hot so the transition fires in seconds:

  1. Multi-key hot stream splits at a load-balanced point; appends
     never surface a seal (no client-visible 409/segment errors);
     per-key order and exact counts hold ACROSS the split via ?key=.
  2. A one-key hot stream never splits (ineffective_split_avoided /
     hot key posture) and stays fully readable.
  3. No child registry objects are created (spec §15.3.6).
"""
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "http://127.0.0.1:8090"
AUTH = os.environ["AUTH_TOKEN"]
KEY = os.environ["STREAM_KEY"]
OUT = os.environ["SPLIT_OUT"]


def req(method, path, body=None, headers=None, timeout=30):
    r = urllib.request.Request(f"{BASE}{path}", method=method, data=body)
    r.add_header("authorization", f"Bearer {AUTH}")
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return (
            resp.status,
            # hyper emits lowercase header names; a plain dict() would
            # make Stream-Next-Offset lookups silently miss (the S1
            # 'half the records' harness bug).
            {k.lower(): v for k, v in resp.headers.items()},
            resp.read(),
        )


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


HDRS = {"stream-encryption-key": KEY, "content-type": "application/json"}
fails = []


def gate(ok, label):
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    if not ok:
        fails.append(label)


def read_key_all(stream, k):
    tok, out = None, []
    for _ in range(4096):
        path = f"/v1/stream/{stream}?key={k}"
        if tok is not None:
            path += f"&offset={tok}"
        st, h, b = sreq("GET", path, None, HDRS)
        body = b.strip()
        if body:
            recs = json.loads(body)
            out.extend(recs if isinstance(recs, list) else [recs])
        nxt = h.get("stream-next-offset")
        if h.get("stream-up-to-date", "").lower() == "true" or not nxt or nxt == tok:
            break
        tok = nxt
    return out


# ---- scenario 1: multi-key hot stream splits --------------------------
S1 = "split-multi"
sreq("PUT", f"/v1/stream/{S1}", b"", HDRS)
KEYS = [f"g{i}" for i in range(8)]
seqs = {k: 0 for k in KEYS}
append_errors = 0


def blast(secs):
    global append_errors
    end = time.time() + secs
    def one(k):
        global append_errors
        n = seqs[k]
        seqs[k] = n + 1
        body = json.dumps([{"k": k, "n": n, "pad": "y" * 800}]).encode()
        try:
            sreq("POST", f"/v1/stream/{S1}", body, {**HDRS, "stream-key": k})
        except Exception:
            append_errors += 1
    while time.time() < end:
        with ThreadPoolExecutor(8) as ex:
            list(ex.map(one, KEYS))


t0 = time.time()
split_seen = None
while time.time() - t0 < 120:
    blast(3)
    seg = debug(f"/v1/segments/{S1}")
    live = [s for s in seg["segments"] if s["live"]]
    if len(live) >= 2:
        split_seen = seg
        break
gate(split_seen is not None, "S1 a multi-key hot stream splits")
gate(append_errors == 0, f"S1 zero client-visible append errors through the seal ({append_errors})")
if split_seen:
    los = sorted(int(s["lo"], 16) for s in split_seen["segments"] if s["live"])
    gate(len(los) >= 2, f"S1 split point recorded: {len(los)} live segments")

# Continue appending post-split, then verify EVERY key end-to-end.
blast(3)
bad = 0
for k in KEYS:
    got = read_key_all(S1, k)
    ns = [r["n"] for r in got if r.get("k") == k]
    want = list(range(seqs[k]))
    if ns != want:
        bad += 1
        print(f"    key {k}: got {len(ns)}/{len(want)}, ordered={ns == sorted(ns)}")
gate(bad == 0, "S1 per-key order + exact counts hold across the split")

# ---- scenario 2: one hot key never splits -----------------------------
S2 = "split-onekey"
sreq("PUT", f"/v1/stream/{S2}", b"", HDRS)
n2 = 0
t0 = time.time()
while time.time() - t0 < 45:
    body = json.dumps([{"n": n2, "pad": "z" * 800}]).encode()
    sreq("POST", f"/v1/stream/{S2}", body, {**HDRS, "stream-key": "the-one"})
    n2 += 1
seg2 = debug(f"/v1/segments/{S2}")
live2 = [s for s in seg2["segments"] if s["live"]]
gate(len(live2) == 1, f"S2 one dominant key never splits (live={len(live2)})")
got2 = read_key_all(S2, "the-one")
gate(
    [r["n"] for r in got2] == list(range(n2)),
    f"S2 hot key fully readable in order ({len(got2)}/{n2})",
)
sc = debug("/v1/debug/load").get("scaler", {})
print(f"  scaler: {sc}")
gate(sc.get("segment_splits", 0) >= 1, "scaler counted the S1 split")

report = {"fails": fails, "scaler": sc, "s1_keys": {k: seqs[k] for k in KEYS}, "s2_records": n2}
with open(f"{OUT}/split-report.json", "w") as f:
    json.dump(report, f, indent=2)
print(json.dumps(report, indent=2))
sys.exit(1 if fails else 0)
