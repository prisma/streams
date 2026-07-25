"""Ladder order checker: discovers segments from segmap objects in s3lite,
drains every segment via HTTP (following replay-to), and verifies per-key
gapless 0..N-1 across the segment lineage.
usage: checker.py <stream> <keyfile> [--expect-segments N] [--prefix d1]
"""
import json, sys, time, urllib.request, urllib.parse, re

PORTS = {"streams-1": 8101, "streams-2": 8102, "streams-3": 8103}
import os
CLUSTER_AUTH = os.environ.get("CLUSTER_AUTH")
CLUSTER_URLS = json.loads(os.environ["CLUSTER_URLS"]) if os.environ.get("CLUSTER_URLS") else None
CLUSTER_URL = (sorted(CLUSTER_URLS.values())[0] if CLUSTER_URLS else os.environ.get("CLUSTER_URL"))
stream, keyfile = sys.argv[1], sys.argv[2]
prefix = "d1"
expect_min_segs = None
for i, a in enumerate(sys.argv):
    if a == "--expect-segments": expect_min_segs = int(sys.argv[i+1])
    if a == "--prefix": prefix = sys.argv[i+1]
KEY = open(keyfile).read().strip()

def list_keys(pfx):
    out, token = [], None
    while True:
        u = f"http://127.0.0.1:9500/ladder/?list-type=2&prefix={urllib.parse.quote(pfx)}&max-keys=1000"
        if token: u += "&continuation-token=" + urllib.parse.quote(token)
        xml = urllib.request.urlopen(u, timeout=30).read().decode()
        out += re.findall(r"<Key>([^<]+)</Key>", xml)
        m = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", xml)
        if not m: break
        token = m.group(1)
    return out

# authoritative map via the /segments endpoint (any instance).
def get_map():
    last = None
    urls = (sorted(CLUSTER_URLS.values()) if CLUSTER_URLS
            else ([CLUSTER_URL] if CLUSTER_URL else [f"http://127.0.0.1:{p}" for p in PORTS.values()]))
    for base in urls:
        try:
            h = {"Stream-Encryption-Key": KEY}
            if CLUSTER_AUTH: h["Authorization"] = f"Bearer {CLUSTER_AUTH}"
            rq = urllib.request.Request(
                f"{base}/v1/stream/{urllib.parse.quote(stream, safe='')}/segments",
                headers=h)
            with urllib.request.urlopen(rq, timeout=30) as r:
                return json.load(r)
        except Exception as e:
            last = e
    raise last
m = get_map()
print(f"segmap v{m['version']} — {len(m['segments'])} segment(s) total")

def seg_ids(m):
    ids = set()
    for s in m["segments"]: ids.add(s["seg_id"] if isinstance(s, dict) else s[0])
    for s in (m.get("sealed") or []): ids.add(s["seg_id"] if isinstance(s, dict) else s[0])
    return sorted(ids)

ids = seg_ids(m)
print("segment ids:", ids)
if expect_min_segs and len(ids) < expect_min_segs:
    print(f"FAIL: expected >= {expect_min_segs} segments, got {len(ids)}"); sys.exit(1)

drain_retries = {}
def drain(name):
    recs, off, pages = [], None, 0
    entry = PORTS["streams-1"]
    while True:
        if CLUSTER_URL:
            url = f"{CLUSTER_URL}/v1/stream/{urllib.parse.quote(name, safe='')}?limit=1000"
        else:
            url = f"http://127.0.0.1:{entry}/v1/stream/{urllib.parse.quote(name, safe='')}?limit=1000"
        if off: url += "&offset=" + urllib.parse.quote(off, safe="")
        h = {"Stream-Encryption-Key": KEY}
        if CLUSTER_AUTH: h["Authorization"] = f"Bearer {CLUSTER_AUTH}"
        rq = urllib.request.Request(url, headers=h)
        try:
            with urllib.request.urlopen(rq, timeout=120) as r:
                batch = json.load(r)
                nxt = r.headers.get("stream-next-offset")
                upd = r.headers.get("stream-up-to-date")
        except urllib.error.HTTPError as e:
            tgt = e.headers.get("streams-replay-to")
            if e.code in (409, 503) and (tgt in PORTS or CLUSTER_URL):
                if not CLUSTER_URL: entry = PORTS[tgt]
                continue
            if e.code == 404:
                return []  # segment stream never created (no records routed)
            if e.code == 503:
                time.sleep(0.5); continue
            raise
        except (TimeoutError, OSError):
            # transient (busy absorber, fencing): retry the page
            slow = drain_retries.get(name, 0) + 1
            drain_retries[name] = slow
            if slow > 12: raise
            time.sleep(2.0)
            continue
        pages += 1
        got = [b for b in batch if b and "k" in b]
        recs.extend(got)
        if not got and (upd == "true" or nxt == off): break
        off = nxt
        if pages > 4000: raise RuntimeError("too many pages")
    return recs

sent = json.load(open(f"/tmp/ladder-seqs-{stream.replace('#','_')}.json"))
per_seg = {}
total = 0
for sid in ids:
    nm = f"{stream}#{sid}"
    rs = drain(nm)
    per_seg[sid] = rs
    total += len(rs)
    print(f"  {nm}: {len(rs)} records")

# lineage order = seg_id ascending is NOT generally creation order; use created_ms if present else seg_id
def created(sid):
    for pool in (m["segments"], m.get("sealed") or []):
        for s in pool:
            if (s["seg_id"] if isinstance(s, dict) else s[0]) == sid:
                return (s.get("created_ms", 0) if isinstance(s, dict) else 0, sid)
    return (0, sid)
order = sorted(ids, key=created)

perkey = {}
for sid in order:
    for r in per_seg[sid]:
        perkey.setdefault(r["k"], []).append((sid, r["seq"]))

fails = []
for k, want_n in sorted(sent.items(), key=lambda x: int(x[0].split("-")[1])):
    got = [s for _, s in perkey.get(k, [])]
    segs_of_k = sorted(set(s for s, _ in perkey.get(k, [])))
    ok = got == list(range(want_n))
    if not ok:
        i = next((i for i, (a, b) in enumerate(zip(got, range(want_n))) if a != b), min(len(got), want_n))
        fails.append(f"{k}: len {len(got)}/{want_n} div@{i}")
    print(f"  {k:8s} n={len(got):5d}/{want_n:5d} segs={segs_of_k} {'OK' if ok else 'FAIL'}")
print(f"TOTAL drained {total} / sent {sum(sent.values())}")
if sum(sent.values()) == 0:
    print("ORDER CHECK: FAIL (vacuous - zero records were ever acknowledged)"); sys.exit(1)
if fails or total != sum(sent.values()):
    print("ORDER CHECK: FAIL"); [print(" ", f) for f in fails]; sys.exit(1)
print("ORDER CHECK: PASS")
