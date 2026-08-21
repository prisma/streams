#!/bin/bash
# Review V6: MATCHED-SHAPE promotion experiment. Identical stream and
# subscriber shapes across arms; only the promotion threshold varies:
#   A: STREAMS x 1 sub, SSE_HUB_PROMOTE_AT=2  (current default: all direct)
#   B: STREAMS x 1 sub, SSE_HUB_PROMOTE_AT=1  (promote-on-first: all hub)
#   C: STREAMS/2 x 2,   SSE_HUB_PROMOTE_AT=2  (density-2 control: all hub)
# Snapshots: baseline / post-create / post-park / +90s idle (with CPU
# sampling) / under controlled appends / post-disconnect.
# Repo-relative (review: no workstation-specific paths).
set -e
HERE=$(cd "$(dirname "$0")/../.." && pwd)
OUT=${OUT:-/tmp/sse-matched}; STREAMS=${STREAMS:-4000}
rm -rf "$OUT" && mkdir -p "$OUT"
AUTH=probe-token
cleanup() { kill $S3_PID $SRV_PID 2>/dev/null; wait 2>/dev/null || true; }
trap cleanup EXIT
run_arm() {
  local ARM=$1 NSTREAMS=$2 PER=$3 THRESH=$4
  "$HERE/bench/costab/bin/s3lite-ab" --listen 127.0.0.1:9500 --latency-ms 1 > "$OUT/s3lite-$ARM.log" 2>&1 &
  S3_PID=$!
  sleep 1
  env SLATE_S3_ENDPOINT=http://127.0.0.1:9500 SLATE_S3_BUCKET=m$ARM \
    SLATE_S3_REGION=local SLATE_S3_ACCESS_KEY_ID=test SLATE_S3_SECRET_ACCESS_KEY=test \
    AUTH_TOKEN=$AUTH PATH_PREFIX=m$ARM INSTANCE_NAME=streams-1 INITIAL_SHARDS=1 \
    TAIL_RING_BYTES=0 SHARED_CACHE_BYTES=67108864 ADMIT_RSS_SHED_MB=1400 \
    SSE_MAX_CONNECTIONS=0 SSE_HUB_PROMOTE_AT=$THRESH RUST_LOG=warn \
    "$HERE/target/release/streams-slate" --listen 127.0.0.1:8090 > "$OUT/server-$ARM.log" 2>&1 &
  SRV_PID=$!
  for i in $(seq 1 60); do
    curl -sf -o /dev/null -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/store && break
    sleep 1
  done
  KEY=$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip('='))")
  SRV_PID_ENV=$SRV_PID python3 - "$NSTREAMS" "$PER" "$AUTH" "$KEY" "$ARM" <<'PY'
import asyncio, json, os, subprocess, sys, time, urllib.request
S, PER, AUTH, KEY, ARM = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
SRV = os.environ["SRV_PID_ENV"]
def snap(tag):
    r = urllib.request.Request("http://127.0.0.1:8090/v1/debug/load?walk=1", headers={"authorization": f"Bearer {AUTH}"})
    d = json.load(urllib.request.urlopen(r, timeout=10))
    out = {k: d.get(k) for k in ("rss_mb","sse_connections","sse_live_hubs","sse_hub_total_bytes","sse_hub_logical_bytes","admit_shed_inflight","admit_shed_rss")}
    print(f"{ARM} {tag}: {out}", flush=True)
    return out
def cpu_pct(samples=5):
    vals = []
    for _ in range(samples):
        o = subprocess.run(["ps","-o","%cpu=","-p",SRV], capture_output=True, text=True).stdout.strip()
        try: vals.append(float(o))
        except: pass
        time.sleep(2)
    return round(sum(vals)/max(1,len(vals)), 1)
async def create(i):
    req = urllib.request.Request(f"http://127.0.0.1:8090/v1/streams/m{i}",
        data=b'{"format":{"kind":"json"}}',
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
async def main():
    base = snap("baseline")
    sem = asyncio.Semaphore(64)
    async def c(i):
        async with sem: await create(i)
    t0 = time.time()
    await asyncio.gather(*[c(i) for i in range(S)])
    created = snap("post-create")
    print(f"{ARM} create: {S} in {time.time()-t0:.0f}s, +{(created['rss_mb']-base['rss_mb']):.0f}MB", flush=True)
    conns = []
    for i in range(S):
        for _ in range(PER):
            conns.append(await sub(i))
    parked = snap("post-park")
    print(f"{ARM} park: {len(conns)} conns, +{(parked['rss_mb']-created['rss_mb'])*1024/len(conns):.1f}KB/sub", flush=True)
    await asyncio.sleep(90)
    idle = snap("idle+90s")
    cpu = cpu_pct()
    print(f"{ARM} idle: slope {(idle['rss_mb']-parked['rss_mb'])*1024/1.5:.0f}KB/min, cpu {cpu}%", flush=True)
    # Controlled appends: 1 record to every 20th stream; time delivery.
    t_send = time.time()
    hits = 0
    for i in range(0, S, 20):
        req = urllib.request.Request(f"http://127.0.0.1:8090/v1/streams/m{i}/records",
            data=json.dumps({"t": time.time()}).encode(),
            headers={"authorization": f"Bearer {AUTH}", "prisma-encryption-key": KEY, "content-type": "application/json"}, method="POST")
        try:
            urllib.request.urlopen(req, timeout=10); hits += 1
        except Exception: pass
    async def drain_one(idx):
        r, _ = conns[idx * PER]
        try:
            await asyncio.wait_for(r.read(256), 10)
            return time.time()
        except Exception:
            return None
    arrivals = [a for a in await asyncio.gather(*[drain_one(i) for i in range(0, S, 20)]) if a]
    lat = sorted(a - t_send for a in arrivals)
    p50 = lat[len(lat)//2] if lat else -1
    under = snap("under-appends")
    print(f"{ARM} appends: {hits} ok, delivered {len(arrivals)}, p50 {p50*1000:.0f}ms", flush=True)
    for r, w in conns:
        w.close()
    await asyncio.sleep(10)
    after = snap("post-disconnect")
    print(f"{ARM} VERDICT: park {(parked['rss_mb']-created['rss_mb'])*1024/len(conns):.1f}KB/sub, "
          f"hubs {parked['sse_live_hubs']}, idle-cpu {cpu}%, deliver-p50 {p50*1000:.0f}ms, "
          f"residual {(after['rss_mb']-base['rss_mb']):.0f}MB", flush=True)
asyncio.run(main())
PY
  kill $S3_PID $SRV_PID 2>/dev/null; wait 2>/dev/null || true
  sleep 1
}
echo "== ARM A: ${STREAMS} x 1, threshold 2 (all direct)"; run_arm A "$STREAMS" 1 2
echo "== ARM B: ${STREAMS} x 1, threshold 1 (all hub)";    run_arm B "$STREAMS" 1 1
echo "== ARM C: $((STREAMS/2)) x 2, threshold 2 (all hub)"; run_arm C "$((STREAMS/2))" 2 2
