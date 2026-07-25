"""Docker-ladder load driver: routed appends with replay-to following.
usage: driver.py <stream> <keyfile> <recs_per_sec> <duration_secs> [payload_pad] [nkeys]
Writes sent-seq state to /tmp/ladder-seqs-<stream>.json (cumulative across calls).
"""
import json, sys, time, threading, urllib.request, urllib.error, random, os

PORTS = {"streams-1": 8101, "streams-2": 8102, "streams-3": 8103}
# Cluster mode: one front-door URL for all instances (the platform LB
# routes; a replay-to just means "re-roll until you land on the owner").
CLUSTER_AUTH = os.environ.get("CLUSTER_AUTH")
# Cluster mode: {"streams-1": "https://...", ...}. Four Compute services
# have four URLs and NO shared front door, so replay-to must select the
# owner's URL the same way the docker map selects its port — retrying one
# fixed URL would spin forever on shards that instance does not own.
CLUSTER_URLS = json.loads(os.environ["CLUSTER_URLS"]) if os.environ.get("CLUSTER_URLS") else None
CLUSTER_URL = (sorted(CLUSTER_URLS.values())[0] if CLUSTER_URLS else os.environ.get("CLUSTER_URL"))
stream, keyfile = sys.argv[1], sys.argv[2]
rate, dur = float(sys.argv[3]), float(sys.argv[4])
pad = int(sys.argv[5]) if len(sys.argv) > 5 else 900
nkeys = int(sys.argv[6]) if len(sys.argv) > 6 else 16
KEY = open(keyfile).read().strip()
BATCH = int(os.environ.get("BATCH", "20"))
statef = f"/tmp/ladder-seqs-{stream.replace('#','_')}.json"
seqs = json.load(open(statef)) if os.path.exists(statef) else {}
lock = threading.Lock()
stats = {"ok":0, "err":0, "retries":0, "redirects":0, "codes":{}}
affinity = {}  # key -> preferred entry port (learned from replay-to)

def append_batch(k, batch, entry, pseq):
    body = json.dumps([{"k":k,"seq":s,"pad":"z"*pad} for s in batch]).encode()
    if CLUSTER_URLS:
        # `entry` carries the instance NAME in cluster mode.
        base = CLUSTER_URLS.get(entry) or sorted(CLUSTER_URLS.values())[0]
        url = f"{base}/v1/stream/{urllib.parse.quote(stream, safe='')}"
    elif CLUSTER_URL:
        url = f"{CLUSTER_URL}/v1/stream/{urllib.parse.quote(stream, safe='')}"
    else:
        url = f"http://127.0.0.1:{entry}/v1/stream/{urllib.parse.quote(stream, safe='')}"
    hdrs_auth = {"Authorization": f"Bearer {CLUSTER_AUTH}"} if CLUSTER_AUTH else {}
    rq = urllib.request.Request(url, data=body, method="POST", headers={
        **hdrs_auth,
        "Stream-Encryption-Key": KEY, "Stream-Key": k,
        "Content-Type": "application/json",
        # Idempotent producer: retries of an ambiguous outcome (408/5xx/
        # timeout) resend the SAME producer-seq; the server dedups (204).
        "Producer-Id": f"drv-{k}", "Producer-Epoch": "0",
        "Producer-Seq": str(pseq)})
    return urllib.request.urlopen(rq, timeout=30)

def worker(keys, per_key_rate, stop_at):
    interval = BATCH / (per_key_rate * len(keys))
    nxt = time.time()
    pseqs = {k: 0 for k in keys}
    while time.time() < stop_at:
        for k in keys:
            if time.time() >= stop_at: break
            with lock: base = seqs.get(k, 0); seqs[k] = base + BATCH
            batch = list(range(base, base + BATCH))
            entry = affinity.get(k, "streams-1" if CLUSTER_URLS else PORTS["streams-1"])
            sent = False
            # With producer idempotence a batch may be retried until
            # unambiguously acked; a producer-seq is never reused for
            # different content, so give-up means poisoning the key.
            for attempt in range(60):
                try:
                    r = append_batch(k, batch, entry, pseqs[k])
                    r.read()
                    with lock: stats["ok"] += BATCH
                    affinity[k] = entry
                    pseqs[k] += 1
                    sent = True
                    break
                except urllib.error.HTTPError as e:
                    code = e.code
                    with lock:
                        stats["codes"][code] = stats["codes"].get(code, 0) + 1
                    tgt = e.headers.get("streams-replay-to")
                    if code in (409, 503) and tgt:
                        if CLUSTER_URLS and tgt in CLUSTER_URLS:
                            entry = tgt
                        elif not CLUSTER_URLS and not CLUSTER_URL and tgt in PORTS:
                            entry = PORTS[tgt]
                        with lock: stats["redirects"] += 1
                        continue
                    exp = e.headers.get("producer-expected-seq")
                    if code == 409 and exp is not None:
                        # Fresh segment after a split (expects 0) or a
                        # resync: adopt the server's expectation and
                        # resend this batch under it.
                        pseqs[k] = int(exp)
                        with lock: stats["pseq_resync"] = stats.get("pseq_resync", 0) + 1
                        continue
                    if code == 429:
                        ra = float(e.headers.get("retry-after", "0.5") or 0.5)
                        time.sleep(min(ra, 2.0))
                        with lock: stats["retries"] += 1
                        continue
                    if code in (408, 409, 503, 500, 502):
                        time.sleep(min(1.5 * (attempt + 1), 4.0))
                        with lock: stats["retries"] += 1
                        continue
                    break
                except Exception:
                    time.sleep(min(0.5 * (attempt + 1), 4.0))
                    with lock: stats["retries"] += 1
            if not sent:
                with lock:
                    stats["err"] += BATCH
                    seqs[k] = base  # roll back; pseq NOT advanced
            nxt += interval
            d = nxt - time.time()
            if d > 0: time.sleep(d)

keys = [f"key-{i}" for i in range(nkeys)]
nw = 32
stop_at = time.time() + dur
groups = [keys[i::nw] for i in range(nw)]
threads = [threading.Thread(target=worker, args=(g, rate/nkeys, stop_at)) for g in groups if g]
t0 = time.time()
for t in threads: t.start()
for t in threads: t.join()
el = time.time() - t0
json.dump(seqs, open(statef, "w"))
print(json.dumps({"elapsed": round(el,1), "rate": round(stats['ok']/el,1), **stats}))
