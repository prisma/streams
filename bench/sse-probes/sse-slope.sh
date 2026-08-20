#!/bin/bash
# #267 idle-slope probe: N parked :sse subscribers, cgroup-honest RSS
# slope from /v1/debug/load, plus sse_future_bytes + mass-disconnect
# residual. Local server against s3lite-ab.
set -e
HERE=/Users/sorenschmidt/code/streams
OUT=/tmp/sse-slope
N=${N:-2000}
rm -rf "$OUT" && mkdir -p "$OUT"
AUTH=probe-token
cleanup() { kill $S3_PID $SRV_PID 2>/dev/null; wait 2>/dev/null || true; }
trap cleanup EXIT
"$HERE/bench/costab/bin/s3lite-ab" --listen 127.0.0.1:9500 --latency-ms 1 > "$OUT/s3lite.log" 2>&1 &
S3_PID=$!
sleep 1
env SLATE_S3_ENDPOINT=http://127.0.0.1:9500 SLATE_S3_BUCKET=sseprobe \
  SLATE_S3_REGION=local SLATE_S3_ACCESS_KEY_ID=test SLATE_S3_SECRET_ACCESS_KEY=test \
  AUTH_TOKEN=$AUTH PATH_PREFIX=sseprobe INSTANCE_NAME=streams-1 INITIAL_SHARDS=1 \
  TAIL_RING_BYTES=0 SHARED_CACHE_BYTES=67108864 ADMIT_RSS_SHED_MB=1400 \
  SSE_MAX_CONNECTIONS=0 RUST_LOG=warn \
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
  http://127.0.0.1:8090/v1/streams/slope > /dev/null
snap() { curl -s -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/load | \
  python3 -c "import json,sys;d=json.load(sys.stdin);print(d.get('rss_mb'),d.get('sse_connections'),d.get('sse_future_bytes'))"; }
echo "BEFORE: $(snap)"
python3 - "$N" "$AUTH" "$KEY" <<'PY' &
import asyncio, sys
N, AUTH, KEY = int(sys.argv[1]), sys.argv[2], sys.argv[3]
async def sub(i):
    r, w = await asyncio.open_connection("127.0.0.1", 8090)
    req = (f"GET /v1/streams/slope/records:sse HTTP/1.1\r\n"
           f"host: x\r\nauthorization: Bearer {AUTH}\r\nprisma-encryption-key: {KEY}\r\n\r\n")
    w.write(req.encode()); await w.drain()
    await r.read(256)  # headers + first control
    return r, w
async def main():
    conns = []
    for i in range(N):
        try: conns.append(await sub(i))
        except Exception as e:
            print("conn fail at", i, e); break
    print(f"PARKED {len(conns)}", flush=True)
    await asyncio.sleep(40)  # hold parked
    for r, w in conns: w.close()
    print("CLOSED", flush=True)
    await asyncio.sleep(1)
asyncio.run(main())
PY
PYPID=$!
sleep 25
echo "PARKED: $(snap)"
wait $PYPID
sleep 15
echo "AFTER-DISCONNECT: $(snap)"
