#!/usr/bin/env python3
"""#131 cross-owner fan-out battery. Against the local two-instance rig
(local-fanout.sh) or any fleet LB + direct instance URLs:

1. Hammers one keyed stream (8 keys x 250, parallel) counting ACKS —
   run 1 counted attempts and misread silent append failures as read
   truncation. Splits fire within seconds under the field-gate knobs.
2. Verdicts, all requiring the lineage to span owners (8 shards over 2
   instances makes single-owner draws vanishingly unlikely at 2 splits):
     reads   — every key walks to EXACTLY its acked count via the LB
               and DIRECTLY on each instance (instance-side fan-out)
     scan    — full snapshot pages to the acked total on all bases
     pull    — messages deliver through a cross-owner lineage
     settle  — acks land (single-segment batch through the router)
     saga    — versioned DELETE completes (relayed per-segment sweep)

Exit 0 = all pass. Env: AUTH_TOKEN, STREAM_KEY, LB, A, B.
"""
import json, os, sys, threading, time, urllib.error, urllib.request

AUTH = os.environ["AUTH_TOKEN"]
KEY = os.environ["STREAM_KEY"]
LB = os.environ["LB"]
BASES = {"LB": LB, "s1": os.environ["A"], "s2": os.environ["B"]}
H = {"authorization": f"Bearer {AUTH}", "prisma-encryption-key": KEY}
JH = {**H, "content-type": "application/json"}
S = f"fan/probe-{int(time.time())}"
KEYS = "abcdefgh"

def req(method, url, body=None, headers=None, timeout=30):
    r = urllib.request.Request(url, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers or H)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:
        return -1, {}, str(e).encode()

def hget(h, n):
    for k, v in h.items():
        if k.lower() == n.lower():
            return v

verdicts, rc = {}, 0
def verdict(name, ok, detail=""):
    global rc
    verdicts[name] = ("PASS" if ok else f"FAIL {detail}")
    if not ok:
        rc = 1

# ---- 1. hammer with ack counting ----
st, _, _ = req("PUT", f"{LB}/v1/streams/{S}", {"format": {"kind": "json"}}, JH)
if st != 201:
    print(f"[fanout] RIG-FAIL create: {st}"); sys.exit(2)
acked = {k: 0 for k in KEYS}
lock = threading.Lock()
# Cloud-leg findings: the hot detector needs SUSTAINED windowed rate.
# urllib opens a TLS connection per request, so one WAN thread manages
# ~1 req/s — 8 threads sit exactly at the 1% hot threshold and never
# cross it. FAN_THREADS_PER_KEY raises aggregate rate; FAN_FLOOR_SECS
# keeps the hammer running long enough for the eval windows.
TPK = int(os.environ.get("FAN_THREADS_PER_KEY", "1"))
FLOOR = float(os.environ.get("FAN_FLOOR_SECS", "0"))
t_end = time.time() + FLOOR
def worker(k, quota):
    i = 0
    while i < quota or time.time() < t_end:
        st, _, _ = req("POST", f"{LB}/v1/streams/{S}/records", {"i": i, "k": k},
                       {**JH, "prisma-routing-key": f"key-{k}"}, timeout=15)
        if st == 200:
            with lock:
                acked[k] += 1
        i += 1
ts = []
for k in KEYS:
    base_q, rem = divmod(250, TPK)
    for t in range(TPK):
        ts.append(threading.Thread(target=worker, args=(k, base_q + (1 if t < rem else 0))))
for t in ts: t.start()
for t in ts: t.join()
print(f"[fanout] {S}: {sum(acked.values())} acked ({dict(acked)})")
time.sleep(8)
st, _, b = req("GET", f"{LB}/v1/segments/{S}")
nseg = len(json.loads(b).get("segments", [])) if st == 200 else 0
print(f"[fanout] {nseg} segments")
if nseg < 3:
    print("[fanout] RIG-FAIL: no split (check SCALE_HOT knobs)"); sys.exit(2)

# ---- 2a. keyed walks: exact acked counts on every base ----
def walk(base, key):
    tok, tot, hops = None, 0, 0
    while hops < 150:
        hops += 1
        q = f"?routingKey=key-{key}&maxBytes=65536" + (f"&cursor={tok}" if tok else "")
        st, h, b = req("GET", f"{base}/v1/streams/{S}/records{q}")
        if st != 200:
            return tot, f"{st}@hop{hops}"
        d = json.loads(b) if b.strip() else []
        tot += len(d) if isinstance(d, list) else len(d.get("records", []))
        nxt = hget(h, "Prisma-Next-Cursor")
        if hget(h, "Prisma-Up-To-Date") or not nxt or nxt == tok:
            return tot, None
        tok = nxt
    return tot, "no-convergence"
bad = []
for key in KEYS:
    for nm, base in BASES.items():
        n, err = walk(base, key)
        if err or n != acked[key]:
            bad.append(f"{key}@{nm}:{n}/{acked[key]} {err}")
verdict("reads-all-bases-exact", not bad, str(bad[:4]))

# ---- 2b. scan snapshot on every base ----
def scan_all(base):
    tok, tot, hops = None, 0, 0
    while hops < 300:
        hops += 1
        q = "?maxBytes=200000" + (f"&cursor={tok}" if tok else "")
        st, h, b = req("GET", f"{base}/v1/streams/{S}:scan{q}")
        if st != 200:
            return tot, f"{st}@hop{hops}:{b[:80]!r}"
        d = json.loads(b) if b.strip() else []
        items = d if isinstance(d, list) else d.get("items", d.get("records", []))
        tot += len(items)
        nxt = (d.get("cursor") if isinstance(d, dict) else None) or hget(h, "Prisma-Next-Cursor")
        done = (isinstance(d, dict) and (d.get("complete") or d.get("upToDate"))) or hget(h, "Prisma-Up-To-Date")
        if done or not nxt or nxt == tok:
            return tot, None
        tok = nxt
    return tot, "no-convergence"
total = sum(acked.values())
sbad = []
for nm, base in BASES.items():
    n, err = scan_all(base)
    if err or n != total:
        sbad.append(f"{nm}:{n}/{total} {err}")
verdict("scan-all-bases-exact", not sbad, str(sbad))

# ---- 2c. pull + settle + saga through the LB ----
st, h, _ = req("PUT", f"{LB}/v1/streams/{S}/consumers/cfan",
               {"visibilityTimeoutMs": 30000}, JH)
ver = hget(h, "Prisma-Consumer-Version")
st2, _, b2 = req("POST", f"{LB}/v1/streams/{S}/consumers/cfan:pull", {"max": 5}, JH)
msgs = json.loads(b2).get("messages", []) if st2 == 200 else []
verdict("pull-delivers", st2 == 200 and len(msgs) > 0, f"{st2}/{len(msgs)}")
if msgs:
    st4, _, b4 = req("POST", f"{LB}/v1/streams/{S}/consumers/cfan:settle",
                     {"acks": [{"leaseToken": m["leaseToken"]} for m in msgs]}, JH)
    verdict("settle-acks", st4 == 200, f"{st4}:{b4[:100]!r}")
del_st, db = None, b""
for _ in range(20):
    del_st, _, db = req("DELETE", f"{LB}/v1/streams/{S}/consumers/cfan",
                        None, {**H, "prisma-consumer-version": ver})
    if del_st == 503:
        time.sleep(1); continue
    break
verdict("saga-delete", del_st == 204, f"{del_st}:{db[:120]!r}")
st3, _, _ = req("GET", f"{LB}/v1/streams/{S}/consumers/cfan")
verdict("saga-post-delete-404", st3 == 404, str(st3))

print("\n[fanout] ===== verdicts =====")
for k, v in verdicts.items():
    print(f"[fanout]   {k}: {v}")
sys.exit(rc)
