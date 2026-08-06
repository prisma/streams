#!/usr/bin/env python3
"""#113 fleet observer: polls the fleet's own coordination state — the
same objects the instances and the LB act on — and prints/logs the
scale-up story: desired count, live set, per-instance load vector, and
shard ownership. Every desired.json epoch change is captured verbatim
(epoch + reason) — run 1 lost the 1->2 and 2->3 trigger reasons because
only the count was recorded and later publishes overwrote the file.
Usage:

    SOAK_HOME=~/.streams-soak python3 bench/fleet/observe-fleet.py [secs] [csv]

Transitions (desired epoch or ownership changes) are also appended to
<csv>.transitions with the verbatim reason string.
"""
import json, os, sys, time
import boto3, botocore.config

S = os.path.expanduser(os.environ.get("SOAK_HOME", "~/.streams-soak"))
d = json.load(open(os.path.join(S, "bkey-fleet.json")))["data"]
s3 = boto3.client("s3", endpoint_url=d["endpoint"],
    aws_access_key_id=d["accessKeyId"], aws_secret_access_key=d["secretAccessKey"],
    region_name="auto",
    config=botocore.config.Config(retries={"max_attempts": 2}, connect_timeout=10, read_timeout=20))
B = d["bucketName"]
DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 1500
CSV = sys.argv[2] if len(sys.argv) > 2 else "/tmp/fleet-observe.csv"

def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None

def heartbeats():
    out = {}
    try:
        r = s3.list_objects_v2(Bucket=B, Prefix="fleetops/fleet/")
        for o in r.get("Contents", []):
            k = o["Key"]
            if k.endswith(".json") and "desired" not in k and "overrides" not in k:
                hb = get_json(k)
                if hb:
                    out[hb.get("instance", k)] = hb
    except Exception:
        pass
    return out

start = time.time()
csv = open(CSV, "a")
if csv.tell() == 0:
    csv.write("t_secs,desired,live,instances,per_instance_rps,per_instance_cpu,owned_shards,desired_epoch,overrides\n")
tlog = open(CSV + ".transitions", "a")
print(f"observing fleet for {DURATION}s -> {CSV}")
last_live = -1
last_epoch = None
last_owned = None
while time.time() - start < DURATION:
    t = int(time.time() - start)
    des = get_json("fleetops/fleet/desired.json") or {}
    desired = des.get("desired", des.get("count", "?"))
    epoch = des.get("epoch")
    ov = get_json("fleetops/fleet/overrides.json") or {}
    ov_entries = ov.get("entries", {}) or {}
    ov_str = "|".join(f"{p}->{e.get('to','?')}" for p, e in sorted(ov_entries.items())) or "-"
    now_ms = time.time() * 1000
    hbs = heartbeats()
    live = {k: v for k, v in hbs.items() if now_ms - v.get("ts_ms", 0) < 10_000}
    names = sorted(live)
    rps = "|".join(f"{live[n].get('rps', 0):.0f}" for n in names)
    cpu = "|".join(f"{live[n].get('cpu_pct', 0):.0f}" for n in names)
    owned = "|".join(f"{n}:{len(live[n].get('owned_shards', []))}" for n in names)
    line = f"{t},{desired},{len(live)},{'|'.join(names)},{rps},{cpu},{owned},{epoch},{ov_str}"
    csv.write(line + "\n"); csv.flush()
    stamp = time.strftime("%H:%M:%S")
    print(f"[{stamp} t+{t:4}s] desired={desired} live={len(live)} [{' '.join(names)}] rps=[{rps}] cpu%=[{cpu}] shards=[{owned}] ov=[{ov_str}]")
    if epoch != last_epoch:
        reason = des.get("reason", "?")
        msg = f"t+{t}s desired.json epoch {last_epoch} -> {epoch}: count={desired} reason: {reason}"
        print(f"  *** {msg}")
        tlog.write(msg + "\n"); tlog.flush()
        last_epoch = epoch
    if owned != last_owned:
        msg = f"t+{t}s ownership: {owned} (overrides: {ov_str})"
        if last_owned is not None:
            print(f"  *** {msg}")
        tlog.write(msg + "\n"); tlog.flush()
        last_owned = owned
    if len(live) != last_live:
        print(f"  *** live set changed: {last_live} -> {len(live)}")
        last_live = len(live)
    time.sleep(10)
