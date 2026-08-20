#!/bin/bash
# #275 leg 15: 1-sub-per-stream residency probe. Arm D: N streams x 1
# sub (all direct under F8 even with the hub flag on). Arm H: N/2
# streams x 2 subs (all hub-promoted) at the SAME connection count.
# Reports RSS at park, 90 s idle slope, future-bytes gauges, and
# post-disconnect residual. Local: no edge in the way.
set -e
HERE=/Users/sorenschmidt/code/streams
OUT=/tmp/sse-1per; N=${N:-1000}
rm -rf "$OUT" && mkdir -p "$OUT"
AUTH=probe-token
cleanup() { kill $S3_PID $SRV_PID 2>/dev/null; wait 2>/dev/null || true; }
trap cleanup EXIT
run_arm() {
  local ARM=$1 STREAMS=$2 PER=$3
  "$HERE/bench/costab/bin/s3lite-ab" --listen 127.0.0.1:9500 --latency-ms 1 > "$OUT/s3lite-$ARM.log" 2>&1 &
  S3_PID=$!
  sleep 1
  env SLATE_S3_ENDPOINT=http://127.0.0.1:9500 SLATE_S3_BUCKET=p$ARM \
    SLATE_S3_REGION=local SLATE_S3_ACCESS_KEY_ID=test SLATE_S3_SECRET_ACCESS_KEY=test \
    AUTH_TOKEN=$AUTH PATH_PREFIX=p$ARM INSTANCE_NAME=streams-1 INITIAL_SHARDS=1 \
    TAIL_RING_BYTES=0 SHARED_CACHE_BYTES=67108864 ADMIT_RSS_SHED_MB=1400 \
    SSE_MAX_CONNECTIONS=0 SSE_LIVE_HUB=1 SSE_H1_MAX_BUF=${H1BUF:-65536} RUST_LOG=warn \
    "$HERE/target/release/streams-slate" --listen 127.0.0.1:8090 > "$OUT/server-$ARM.log" 2>&1 &
  SRV_PID=$!
  for i in $(seq 1 60); do
    curl -sf -o /dev/null -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/store && break
    sleep 1
  done
  KEY=$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip('='))")
  python3 - "$STREAMS" "$PER" "$AUTH" "$KEY" "$ARM" "$OUT" <<'PY'
import asyncio, json, sys, time, urllib.request
S, PER, AUTH, KEY, ARM, OUT = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]
def snap():
    r = urllib.request.Request("http://127.0.0.1:8090/v1/debug/load", headers={"authorization": f"Bearer {AUTH}"})
    d = json.load(urllib.request.urlopen(r, timeout=10))
    return {k: d.get(k) for k in ("rss_mb","sse_connections","sse_future_bytes","sse_hub_future_bytes","sse_live_hubs","sse_hub_total_bytes")}
async def create(i):
    req = urllib.request.Request(f"http://127.0.0.1:8090/v1/streams/p{i}",
        data=b'{"format":{"kind":"json"}}',
        headers={"authorization": f"Bearer {AUTH}", "prisma-encryption-key": KEY, "content-type": "application/json"}, method="PUT")
    await asyncio.get_event_loop().run_in_executor(None, lambda: urllib.request.urlopen(req, timeout=10).read())
async def sub(i):
    r, w = await asyncio.open_connection("127.0.0.1", 8090)
    req = (f"GET /v1/streams/p{i}/records:sse?cursor=now HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\n"
           f"authorization: Bearer {AUTH}\r\nprisma-encryption-key: {KEY}\r\n\r\n")
    w.write(req.encode()); await w.drain()
    head = await asyncio.wait_for(r.read(64), 10)
    assert b" 200" in head[:16], head[:40]
    return (r, w)
async def main():
    before = snap(); print(f"{ARM} BEFORE: {before}", flush=True)
    sem = asyncio.Semaphore(64)
    async def c(i):
        async with sem: await create(i)
    await asyncio.gather(*[c(i) for i in range(S)])
    conns = []
    for i in range(S):
        for _ in range(PER):
            conns.append(await sub(i))
    at_park = snap(); print(f"{ARM} PARKED n={len(conns)}: {at_park}", flush=True)
    t0 = time.time(); await asyncio.sleep(90)
    idle = snap(); print(f"{ARM} IDLE+90s: {idle}", flush=True)
    slope_kb = (idle["rss_mb"] - at_park["rss_mb"]) * 1024 / max(1,(time.time()-t0)/60)
    for r, w in conns:
        w.close()
    await asyncio.sleep(8)
    after = snap(); print(f"{ARM} POST-DISCONNECT: {after}", flush=True)
    per_sub_kb = (at_park["rss_mb"] - before["rss_mb"]) * 1024 / len(conns)
    print(f"{ARM} VERDICT: per-sub {per_sub_kb:.1f} KB, idle slope {slope_kb:.0f} KB/min, "
          f"hubs {at_park['sse_live_hubs']}, hub_future {at_park['sse_hub_future_bytes']}, "
          f"residual {after['rss_mb']-before['rss_mb']:.0f} MB", flush=True)
asyncio.run(main())
PY
  kill $S3_PID $SRV_PID 2>/dev/null; wait 2>/dev/null || true
  sleep 1
}
echo "== ARM D: $N x 1 (direct)"; run_arm D "$N" 1
echo "== ARM H: $((N/2)) x 2 (hub)"; run_arm H "$((N/2))" 2
