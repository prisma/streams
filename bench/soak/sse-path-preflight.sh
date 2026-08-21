#!/bin/bash
# Review round 3, Phase 2: PATH PREFLIGHT before every field SSE run.
# The edge buffers streaming responses unless told not to, and the
# in-region hairpin ignores the opt-out: a run whose path produces
# zombies or silent 200s measures the edge, not the server. Five steps:
#   1. open N SSE requests (cursor=now) from THIS host to the server
#   2. every one must deliver its initial control bytes promptly
#   3. every one must deliver TWO consecutive 15 s heartbeats
#   4. client-held connections must equal the server's sse_connections
#      delta (no zombies: origin legs that died behind live client legs)
#   5. abort (exit 1) on any zombie or silent 200
# Usage: sse-path-preflight.sh <server-url> <ops-bearer> <tenant-bearer>
#        <stream-key> <stream-name> [N=8]
set -e
URL=$1; OPS=$2; TOK=$3; KEY=$4; NAME=$5; N=${6:-8}
[ -n "$NAME" ] || { echo "usage: $0 <url> <ops-bearer> <tenant-bearer> <key> <stream> [N]"; exit 2; }
python3 - "$URL" "$OPS" "$TOK" "$KEY" "$NAME" "$N" <<'PY'
import json, socket, ssl, sys, time, urllib.request
url, ops, tok, key, name, n = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], int(sys.argv[6])
host = url.split("//")[1].split("/")[0]
tls = url.startswith("https")
port = 443 if tls else int(host.split(":")[1]) if ":" in host else 80
hostname = host.split(":")[0]
def server_conns():
    r = urllib.request.Request(f"{url}/v1/debug/load", headers={"authorization": f"Bearer {ops}"})
    return json.load(urllib.request.urlopen(r, timeout=10)).get("sse_connections", -1)
before = server_conns()
ctx = ssl.create_default_context()
conns = []
for i in range(n):
    raw = socket.create_connection((hostname, port), timeout=8)
    s = ctx.wrap_socket(raw, server_hostname=hostname) if tls else raw
    s.sendall((f"GET /v1/streams/{name}/records:sse?cursor=now HTTP/1.1\r\nhost: {hostname}\r\n"
               f"authorization: Bearer {tok}\r\nprisma-encryption-key: {key}\r\n\r\n").encode())
    conns.append({"s": s, "bytes": 0, "status": None, "hb": 0, "last": time.time()})
    time.sleep(0.25)   # paced establishment
# step 2: initial control bytes within 5 s
deadline = time.time() + 5
for c in conns:
    c["s"].settimeout(max(0.1, deadline - time.time()))
    try:
        head = c["s"].recv(4096)
        c["bytes"] += len(head); c["last"] = time.time()
        c["status"] = head.split(b" ")[1][:3].decode() if b" " in head[:16] else "none"
        c["ctl"] = b"event: control" in head
    except Exception:
        c["status"] = "timeout"; c["ctl"] = False
silent = [i for i, c in enumerate(conns) if c["status"] != "200" or not c.get("ctl")]
print(f"step 2: {n - len(silent)}/{n} delivered 200 + initial control")
# step 3: two consecutive heartbeats (15 s cadence) within 40 s
end = time.time() + 40
while time.time() < end and any(c["hb"] < 2 for c in conns):
    for c in conns:
        if c["hb"] >= 2: continue
        c["s"].settimeout(0.2)
        try:
            b = c["s"].recv(4096)
            if b:
                c["bytes"] += len(b); c["last"] = time.time()
                c["hb"] += b.count(b": keep-alive")
        except socket.timeout:
            pass
        except Exception:
            c["status"] = "error"
    time.sleep(0.1)
hb_ok = sum(1 for c in conns if c["hb"] >= 2)
print(f"step 3: {hb_ok}/{n} received two consecutive heartbeats")
# step 4: server-side truth
after = server_conns()
held = sum(1 for c in conns if c["status"] == "200")
print(f"step 4: client-held={held} server-delta={after - before} (before={before} after={after})")
zombies = held - max(0, after - before)
ok = not silent and hb_ok == n and zombies <= 0
for c in conns:
    try: c["s"].close()
    except Exception: pass
if ok:
    print("PATH_PREFLIGHT_OK")
    sys.exit(0)
print(f"PATH_PREFLIGHT_FAIL silent={len(silent)} no-heartbeat={n - hb_ok} zombies={max(0, zombies)}")
sys.exit(1)
PY
