#!/bin/bash
# Review round 3 V6: the LOADED matched promotion comparison. Two arms
# with IDENTICAL streams, subscribers, append schedule, subscribed-write
# overlap, payload size, duration and storage state; only the promotion
# threshold differs:
#   A: STREAMS x 1 sub, SSE_HUB_PROMOTE_AT=2 (all direct)
#   B: STREAMS x 1 sub, SSE_HUB_PROMOTE_AT=1 (all hub)
# Load: WPS appends/s round-robin over ACTIVE subscribed streams for
# SECS, 1 KiB payloads with a send timestamp; measured: append shed
# (429/503), append p50/p99, delivery lag p50/p99 (first subscriber per
# written stream), server CPU (ps sampling), RSS before/after, teardown
# residual. Decision rule (review): promote-on-first wins only with a
# clear shed/CPU reduction and no material memory/latency regression.
set -e
HERE=$(cd "$(dirname "$0")/../.." && pwd)
OUT=${OUT:-/tmp/sse-matched-loaded}; STREAMS=${STREAMS:-4000}
ACTIVE=${ACTIVE:-400}; WPS=${WPS:-400}; SECS=${SECS:-180}
rm -rf "$OUT" && mkdir -p "$OUT"
AUTH=probe-token
cleanup() { kill $S3_PID $SRV_PID 2>/dev/null; wait 2>/dev/null || true; }
trap cleanup EXIT
run_arm() {
  local ARM=$1 THRESH=$2
  "$HERE/bench/costab/bin/s3lite-ab" --listen 127.0.0.1:9500 --latency-ms 1 > "$OUT/s3lite-$ARM.log" 2>&1 &
  S3_PID=$!
  sleep 1
  env SLATE_S3_ENDPOINT=http://127.0.0.1:9500 SLATE_S3_BUCKET=ml$ARM \
    SLATE_S3_REGION=local SLATE_S3_ACCESS_KEY_ID=test SLATE_S3_SECRET_ACCESS_KEY=test \
    AUTH_TOKEN=$AUTH PATH_PREFIX=ml$ARM INSTANCE_NAME=streams-1 INITIAL_SHARDS=1 \
    TAIL_RING_BYTES=0 SHARED_CACHE_BYTES=67108864 ADMIT_RSS_SHED_MB=1400 \
    SSE_MAX_CONNECTIONS=0 SSE_HUB_PROMOTE_AT=$THRESH RUST_LOG=warn \
    "$HERE/target/release/streams-slate" --listen 127.0.0.1:8090 > "$OUT/server-$ARM.log" 2>&1 &
  SRV_PID=$!
  for i in $(seq 1 60); do
    curl -sf -o /dev/null -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/store && break
    sleep 1
  done
  KEY=$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip('='))")
  SRV_PID_ENV=$SRV_PID python3 - "$STREAMS" "$ACTIVE" "$WPS" "$SECS" "$AUTH" "$KEY" "$ARM" <<'PY'
import asyncio, json, os, subprocess, sys, time, urllib.request
S, ACTIVE, WPS, SECS, AUTH, KEY, ARM = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], sys.argv[6], sys.argv[7]
SRV = os.environ["SRV_PID_ENV"]
def snap():
    r = urllib.request.Request("http://127.0.0.1:8090/v1/debug/load", headers={"authorization": f"Bearer {AUTH}"})
    d = json.load(urllib.request.urlopen(r, timeout=10))
    c = d.get("sse_canary", {})
    return {"rss": d.get("rss_mb"), "conns": d.get("sse_connections"), "hubs": d.get("sse_live_hubs"),
            "shed_in": d.get("admit_shed_inflight"), "shed_rss": d.get("admit_shed_rss"),
            "prepared": c.get("prepared_records"), "delivered": c.get("delivered_records")}
def cpu():
    o = subprocess.run(["ps","-o","%cpu=","-p",SRV], capture_output=True, text=True).stdout.strip()
    try: return float(o)
    except: return -1.0
async def create(i):
    req = urllib.request.Request(f"http://127.0.0.1:8090/v1/streams/m{i}", data=b'{"format":{"kind":"json"}}',
        headers={"authorization": f"Bearer {AUTH}", "prisma-encryption-key": KEY, "content-type": "application/json"}, method="PUT")
    await asyncio.get_event_loop().run_in_executor(None, lambda: urllib.request.urlopen(req, timeout=15).read())
async def sub(i):
    r, w = await asyncio.open_connection("127.0.0.1", 8090)
    w.write((f"GET /v1/streams/m{i}/records:sse?cursor=now HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\n"
             f"authorization: Bearer {AUTH}\r\nprisma-encryption-key: {KEY}\r\n\r\n").encode())
    await w.drain()
    head = await asyncio.wait_for(r.read(64), 15)
    assert b" 200" in head[:16], head[:40]
    return (r, w)
def pct(v, p):
    if not v: return -1
    v = sorted(v); return v[min(len(v)-1, int(p*len(v)))]
async def main():
    base = snap()
    sem = asyncio.Semaphore(64)
    async def c(i):
        async with sem: await create(i)
    await asyncio.gather(*[c(i) for i in range(S)])
    conns = [await sub(i) for i in range(S)]
    parked = snap()
    print(f"{ARM} parked: conns={parked['conns']} hubs={parked['hubs']} rss={parked['rss']:.0f}MB", flush=True)
    # Readers: drain every subscriber, recording lag from the payload
    # timestamp (only ACTIVE streams receive writes).
    lags = []
    async def reader(r):
        try:
            while True:
                b = await r.read(4096)
                if not b: return
                s = b.decode(errors="replace")
                for m in s.split('"t":')[1:]:
                    num = m.split("}")[0].split(",")[0]
                    try: lags.append(time.time() - float(num))
                    except: pass
        except Exception: return
    readers = [asyncio.create_task(reader(r)) for r, _ in conns]
    # Writers: WPS round-robin over the first ACTIVE streams.
    ok = thr = err = 0; lat = []
    pad = "x" * 900
    deadline = time.time() + SECS; i = 0
    interval = 1.0 / WPS
    body_hdr = {"authorization": f"Bearer {AUTH}", "prisma-encryption-key": KEY, "content-type": "application/json"}
    def post(i):
        req = urllib.request.Request(f"http://127.0.0.1:8090/v1/streams/m{i % ACTIVE}/records",
            data=json.dumps({"t": time.time(), "pad": pad}).encode(), headers=body_hdr, method="POST")
        t0 = time.time()
        try:
            urllib.request.urlopen(req, timeout=10); return ("ok", time.time()-t0)
        except urllib.error.HTTPError as e:
            return ("thr" if e.code in (429, 503) else "err", time.time()-t0)
        except Exception:
            return ("err", time.time()-t0)
    loop = asyncio.get_event_loop()
    cpus = []
    next_cpu = time.time() + 5
    while time.time() < deadline:
        t0 = time.time()
        res = await asyncio.gather(*[loop.run_in_executor(None, post, i + k) for k in range(8)])
        i += 8
        for kind, d in res:
            if kind == "ok": ok += 1; lat.append(d)
            elif kind == "thr": thr += 1
            else: err += 1
        if time.time() >= next_cpu:
            cpus.append(cpu()); next_cpu = time.time() + 5
        await asyncio.sleep(max(0, 8*interval - (time.time()-t0)))
    under = snap()
    await asyncio.sleep(2)
    for t in readers: t.cancel()
    for _, w in conns: w.close()
    await asyncio.sleep(8)
    after = snap()
    offered = ok + thr + err
    print(f"{ARM} VERDICT thr={thr} ({100*thr/max(1,offered):.3f}%) ok={ok} err={err} "
          f"append p50={pct(lat,.5)*1000:.0f}ms p99={pct(lat,.99)*1000:.0f}ms "
          f"lag p50={pct(lags,.5)*1000:.0f}ms p99={pct(lags,.99)*1000:.0f}ms n={len(lags)} "
          f"cpu avg={sum(cpus)/max(1,len(cpus)):.1f}% rss parked={parked['rss']:.0f} under={under['rss']:.0f} after={after['rss']:.0f}MB "
          f"hubs={under['hubs']} prepared={under['prepared']} delivered={under['delivered']}", flush=True)
asyncio.run(main())
PY
  kill $S3_PID $SRV_PID 2>/dev/null; wait 2>/dev/null || true
  sleep 1
}
echo "== ARM A: ${STREAMS} x 1, threshold 2 (all direct), ${WPS} wps over ${ACTIVE} subscribed streams, ${SECS}s"; run_arm A 2
echo "== ARM B: ${STREAMS} x 1, threshold 1 (all hub), same load";                                            run_arm B 1
