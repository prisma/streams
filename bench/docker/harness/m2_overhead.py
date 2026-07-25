"""M2: routing-wrapper overhead. Plain vs scaled(pre-split) append latency
against the local :8095 e2e world. Batch=1 isolates per-request cost."""
import json, time, threading, urllib.request, sys, statistics as st
BASE = "http://127.0.0.1:8095/v1/stream/"
KEY = open(sys.argv[1]).read().strip()
N, THREADS = 400, 4

def create(name, scaling):
    h = {"Stream-Encryption-Key": KEY, "Content-Type": "application/json"}
    if scaling: h["Stream-Scaling"] = "auto"
    rq = urllib.request.Request(BASE+name, method="PUT", headers=h)
    try: urllib.request.urlopen(rq, timeout=30).read()
    except urllib.error.HTTPError as e:
        if e.code != 409: raise

def bench(name, scaled):
    lats = []
    lock = threading.Lock()
    def w(tid):
        for i in range(N//THREADS):
            h = {"Stream-Encryption-Key": KEY, "Content-Type": "application/json"}
            if scaled: h["Stream-Key"] = f"k-{tid}-{i%8}"
            body = json.dumps({"k": f"k-{tid}", "seq": i, "pad": "z"*100}).encode()
            t0 = time.perf_counter()
            rq = urllib.request.Request(BASE+name, data=body, method="POST", headers=h)
            urllib.request.urlopen(rq, timeout=30).read()
            with lock: lats.append((time.perf_counter()-t0)*1000)
    ts = [threading.Thread(target=w, args=(t,)) for t in range(THREADS)]
    t0 = time.time()
    for t in ts: t.start()
    for t in ts: t.join()
    el = time.time()-t0
    lats.sort()
    return dict(n=len(lats), rps=round(len(lats)/el,1),
                p50=round(lats[len(lats)//2],1),
                p95=round(lats[int(len(lats)*.95)],1),
                p99=round(lats[int(len(lats)*.99)],1))

create("m2plain", False); create("m2scaled", True)
# warm both paths
bench("m2plain", False); bench("m2scaled", True)
a = bench("m2plain", False)
b = bench("m2scaled", True)
print("plain :", a)
print("scaled:", b)
print(f"p50 delta: {b['p50']-a['p50']:.1f} ms  p99 delta: {b['p99']-a['p99']:.1f} ms")
