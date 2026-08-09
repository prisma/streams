#!/usr/bin/env python3
"""Crash-consistency gate: every ACKed append must survive SIGKILL.

The contract under test is the one a customer actually relies on: a 200
on append means the record is durable. A 408 is the documented ambiguous
case (APPEND_TIMEOUT elapsed with the commit possibly in flight), so
408s are recorded and allowed to land or not — but they may never
produce a GAP or a DUPLICATE, and they may never appear out of order.

Usage:
  kill9-consistency.py <base-url> <bearer> <key-b64> <server-pid-file> \
                       [--streams N] [--writers N] [--kill-after SECS] \
                       [--restart-cmd CMD]

Exit 0 only if, after the kill and restart:
  - every ACKed record is present exactly once
  - records appear in append order per stream
  - no record the server never ACKed appears (no phantom writes)
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("url")
ap.add_argument("token")
ap.add_argument("key")
ap.add_argument("pidfile")
ap.add_argument("--streams", type=int, default=4)
ap.add_argument("--writers", type=int, default=8)
ap.add_argument("--kill-after", type=float, default=8.0)
ap.add_argument("--restart-cmd", default="")
ap.add_argument("--settle", type=float, default=25.0)
A = ap.parse_args()

HDR = {"Authorization": f"Bearer {A.token}", "Prisma-Encryption-Key": A.key}
names = [f"k9-{os.getpid()}-{i}" for i in range(A.streams)]

# acked[stream] = ordered list of payload ints the server returned 200 for.
# ambiguous[stream] = set of payloads whose fate the protocol leaves open.
acked = {n: [] for n in names}
ambiguous = {n: set() for n in names}
lock = threading.Lock()
stop = threading.Event()
counter = {"n": 0}


def req(method, path, body=None, timeout=30):
    r = urllib.request.Request(A.url + path, data=body, headers=dict(HDR), method=method)
    if body is not None:
        r.add_header("content-type", "application/octet-stream")
    return urllib.request.urlopen(r, timeout=timeout)


def create(name):
    r = urllib.request.Request(
        f"{A.url}/v1/streams/{name}",
        data=json.dumps({"format": {"kind": "bytes"}}).encode(),
        headers={**HDR, "content-type": "application/json"},
        method="PUT",
    )
    urllib.request.urlopen(r, timeout=30).read()


def writer(wid):
    """Append records tagged (writer, seq), recording each verdict.

    Payload is WWWSSSSSSSSS: 3 digits of writer id, 9 of that writer's
    own sequence. Concurrent writers race, so the stream's order is
    arrival order and there is NO global order to assert. What the
    server does promise is per-producer FIFO: one writer is sequential
    (it awaits each response), so ITS records must appear in ITS order.
    """
    seq = 0
    while not stop.is_set():
        seq += 1
        with lock:
            counter["n"] += 1
            v = counter["n"]
        name = names[v % len(names)]
        rec = (wid, seq)
        payload = f"{wid:03d}{seq:09d}".encode()
        try:
            resp = req("POST", f"/v1/streams/{name}/records", payload, timeout=20)
            code = resp.getcode()
            resp.read()
            if code == 200:
                with lock:
                    acked[name].append(rec)
            else:
                with lock:
                    ambiguous[name].add(rec)
        except urllib.error.HTTPError:
            # 408 = documented ambiguity. 5xx/429 during a kill are
            # expected too, but they are NOT acks, so the record may or
            # may not exist; either way it must not corrupt the log.
            with lock:
                ambiguous[name].add(rec)
        except Exception:
            with lock:
                ambiguous[name].add(rec)


def read_all(name):
    """Read the whole stream back, returning the payload ints in order."""
    out, cursor, guard = [], None, 0
    while guard < 10000:
        guard += 1
        q = f"?maxBytes=1000000" + (f"&cursor={cursor}" if cursor else "")
        try:
            resp = req("GET", f"/v1/streams/{name}/records{q}", timeout=60)
            body = resp.read()
            nxt = resp.headers.get("prisma-next-cursor")
        except Exception as e:
            print(f"  read error on {name}: {e}")
            return out, False
        if not body:
            break
        for i in range(0, len(body), 12):
            chunk = body[i : i + 12]
            if len(chunk) == 12 and chunk.isdigit():
                out.append((int(chunk[:3]), int(chunk[3:])))
        if not nxt or nxt == cursor:
            break
        cursor = nxt
    return out, True


print(f"creating {len(names)} streams")
for n in names:
    create(n)

print(f"starting {A.writers} writers")
threads = [threading.Thread(target=writer, args=(w,), daemon=True) for w in range(A.writers)]
for t in threads:
    t.start()

time.sleep(A.kill_after)
pid = int(open(A.pidfile).read().strip())
with lock:
    acked_at_kill = sum(len(v) for v in acked.values())
print(f"SIGKILL pid {pid} after {A.kill_after}s ({acked_at_kill} acked so far)")
os.kill(pid, signal.SIGKILL)
time.sleep(1.5)
stop.set()
for t in threads:
    t.join(timeout=10)

with lock:
    total_acked = sum(len(v) for v in acked.values())
    total_amb = sum(len(v) for v in ambiguous.values())
print(f"writers stopped: {total_acked} acked, {total_amb} ambiguous")

if A.restart_cmd:
    print("restarting server")
    subprocess.Popen(A.restart_cmd, shell=True)

deadline = time.time() + A.settle
while time.time() < deadline:
    try:
        if urllib.request.urlopen(A.url + "/health", timeout=5).getcode() == 200:
            break
    except Exception:
        pass
    time.sleep(1)
else:
    print("FAIL: server did not come back healthy")
    sys.exit(1)
print("server healthy again")
time.sleep(3)

failures = []
for name in names:
    got, ok = read_all(name)
    if not ok:
        failures.append(f"{name}: read failed")
        continue
    with lock:
        want = sorted(acked[name])
        amb = ambiguous[name]
    gotset = set(got)

    missing = [v for v in want if v not in gotset]
    if missing:
        failures.append(
            f"{name}: {len(missing)} ACKED records LOST after SIGKILL "
            f"(first {missing[:5]})"
        )
    if len(got) != len(gotset):
        dupes = [v for v in gotset if got.count(v) > 1]
        failures.append(f"{name}: {len(dupes)} DUPLICATED records (first {dupes[:5]})")
    phantom = [v for v in got if v not in set(want) and v not in amb]
    if phantom:
        failures.append(
            f"{name}: {len(phantom)} PHANTOM records never acked "
            f"(first {phantom[:5]})"
        )
    # Per-producer FIFO: each writer is sequential, so its own records
    # must appear in its own order. Across writers the order is arrival
    # order and carries no promise.
    for w in range(A.writers):
        mine = [seq for (wid, seq) in got if wid == w]
        if mine != sorted(mine):
            bad = next(
                (i for i in range(1, len(mine)) if mine[i] < mine[i - 1]), None
            )
            failures.append(
                f"{name}: writer {w} records OUT OF ORDER at index {bad} "
                f"({mine[max(0,(bad or 1)-2):(bad or 1)+2]})"
            )
    landed_amb = len([v for v in got if v in amb])
    print(
        f"  {name}: acked={len(want)} present={len(got)} "
        f"ambiguous_landed={landed_amb}/{len(amb)}"
    )

print()
if failures:
    print("KILL9 CONSISTENCY: FAIL")
    for f in failures:
        print("  " + f)
    sys.exit(1)
print(f"KILL9 CONSISTENCY: PASS ({total_acked} acked records all survived SIGKILL)")
