#!/bin/bash
# #268 hub smoke: delivery correctness + parked slope with SSE_LIVE_HUB=1.
set -e
HERE=/Users/sorenschmidt/code/streams
OUT=/tmp/hub-smoke
N=${N:-2000}
rm -rf "$OUT" && mkdir -p "$OUT"
AUTH=probe-token
cleanup() { kill $S3_PID $SRV_PID 2>/dev/null; wait 2>/dev/null || true; }
trap cleanup EXIT
"$HERE/bench/costab/bin/s3lite-ab" --listen 127.0.0.1:9500 --latency-ms 1 > "$OUT/s3lite.log" 2>&1 &
S3_PID=$!
sleep 1
env SLATE_S3_ENDPOINT=http://127.0.0.1:9500 SLATE_S3_BUCKET=hubsmoke \
  SLATE_S3_REGION=local SLATE_S3_ACCESS_KEY_ID=test SLATE_S3_SECRET_ACCESS_KEY=test \
  AUTH_TOKEN=$AUTH PATH_PREFIX=hubsmoke INSTANCE_NAME=streams-1 INITIAL_SHARDS=1 \
  TAIL_RING_BYTES=33554432 SHARED_CACHE_BYTES=67108864 ADMIT_RSS_SHED_MB=1400 \
  SSE_MAX_CONNECTIONS=0 SSE_LIVE_HUB=1 RUST_LOG=warn \
  "$HERE/target/release/streams-slate" --listen 127.0.0.1:8090 > "$OUT/server.log" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  curl -sf -o /dev/null -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/store && break
  sleep 1
  kill -0 $SRV_PID 2>/dev/null || { echo "server died"; tail -5 "$OUT/server.log"; exit 1; }
done
KEY=$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip('='))")
curl -sf -X PUT -H "authorization: Bearer $AUTH" -H "prisma-encryption-key: $KEY" \
  -H "content-type: application/json" -d '{"format":{"kind":"json"}}' \
  http://127.0.0.1:8090/v1/streams/hub > /dev/null
snap() { curl -s -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/load | \
  python3 -c "import json,sys;d=json.load(sys.stdin);print(d.get('rss_mb'),d.get('sse_connections'),d.get('sse_live_hubs'),d.get('sse_hub_future_bytes'))"; }
echo "BEFORE: $(snap)"
python3 - "$N" "$AUTH" "$KEY" <<'PY' &
import asyncio, sys, json, urllib.request
N, AUTH, KEY = int(sys.argv[1]), sys.argv[2], sys.argv[3]
async def sub(i):
    r, w = await asyncio.open_connection("127.0.0.1", 8090)
    req = (f"GET /v1/streams/hub/records:sse HTTP/1.1\r\n"
           f"host: x\r\nauthorization: Bearer {AUTH}\r\nprisma-encryption-key: {KEY}\r\n\r\n")
    w.write(req.encode()); await w.drain()
    await r.read(300)  # headers + connect-at-tail status control
    return r, w
def append(body):
    rq = urllib.request.Request("http://127.0.0.1:8090/v1/streams/hub/records",
        data=json.dumps(body).encode(), method="POST",
        headers={"authorization": f"Bearer {AUTH}", "prisma-encryption-key": KEY,
                 "content-type": "application/json"})
    urllib.request.urlopen(rq).read()
async def main():
    conns = [await sub(i) for i in range(N)]
    print(f"PARKED {len(conns)}", flush=True)
    await asyncio.sleep(20)
    print("SNAP-POINT", flush=True)
    await asyncio.sleep(10)
    # DELIVERY: append 3 records; every sampled subscriber must see all 3
    for k in range(3):
        await asyncio.get_event_loop().run_in_executor(None, append, {"k": k})
    ok = 0
    samples = conns[:5] + conns[-5:]
    async def drain(r):
        acc = ""
        end = asyncio.get_event_loop().time() + 8
        while asyncio.get_event_loop().time() < end:
            try:
                b = await asyncio.wait_for(r.read(4096), 1)
            except asyncio.TimeoutError:
                continue
            if not b: break
            acc += b.decode(errors="replace")
            if acc.count("event: control") >= 3 and '"k":2' in acc.replace(" ", ""):
                return acc, True
        return acc, False
    for r, w in samples:
        acc, good = await drain(r)
        if good: ok += 1
        else: print("SAMPLE-MISS:", repr(acc[-200:]), flush=True)
    print(f"DELIVERY {ok}/{len(samples)}", flush=True)
    for r, w in conns: w.close()
    print("CLOSED", flush=True)
    await asyncio.sleep(1)
asyncio.run(main())
PY
PYPID=$!
sleep 27
echo "PARKED: $(snap)"
wait $PYPID
sleep 5
echo "AFTER: $(snap)"
grep -c "panic" "$OUT/server.log" || true
