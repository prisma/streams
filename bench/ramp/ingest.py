#!/usr/bin/env python3
"""ingest.py: one queue's sustained send rate on one shard of a 1 GiB server.

The single-queue throughput cap the scaling design publishes (SCALING-DESIGN
§6.6, §7 row 11): how many messages and MB a second one queue on one shard
takes on Compute's 1 GiB class, and how far that is from the 13.9 MB/s of
100 GB in 2 h. For each message size (--sizes, default 1 KiB, 16 KiB,
64 KiB) a fresh cell (s3lite on the host and one server, INITIAL_SHARDS=1, the posture
--posture, default `on`: the dedicated cell profile with auth enforced)
takes a rate ladder (--mb-steps, MB a second of message bodies) of open-loop
senders (--producers, batches of up to 1 MiB, at most 1,000 messages) and no
consumer, each step a new queue (maxQueuedBytes --max-queued, 8 GiB, above
today's 256 MiB default) on the same shard for --secs, until a step is
refused (send errors: the queue's or the shard's, e.g. 503
shard_l0_backpressure at the shard log's L0 watermark) or serves under 98%
of its target. The highest step held is the sustained single-queue rate at
that size; closed-loop senders only find where the shard refuses.

Every 5 s it samples the server's resident set (/proc/1/statm), its own
rss_mb and shed counters (/v1/debug/load), and s3lite's /_s3lite/stats2:
WAL PUTs (shard/wal/put) and the live bytes of each object class (the
shard log's WAL and SSTs, the segment objects). Per size it reports sent
messages and MB a second, send latency, RSS (median, max), the WAL PUT
rate, and the store's growth per class in MB a second.

The sender is habanero's queue mode (population-mode.patch), with the
token from rig.Posture. A measurement: run it under the machine lock.
"""
import argparse
import json
import os
import statistics
import sys
import threading
import time
import urllib.request

import rig

URL = "http://streams-1:8080"
S3_STATS = "http://127.0.0.1:8299/_s3lite/stats2"
MIB = 1 << 20


class Sampler(threading.Thread):
    def __init__(self, posture, path):
        super().__init__(daemon=True)
        self.posture, self.out = posture, open(path, "a")
        self.rows, self.stop = [], threading.Event()

    def sample(self):
        row = {"t": round(time.time(), 1)}
        try:
            out = rig.sh("docker", "exec", rig.server(), "sh", "-c",
                         "cat /proc/1/statm; grep usage_usec /sys/fs/cgroup/cpu.stat").split("\n")
            row["rss_mb"] = int(out[0].split()[1]) * 4096 / MIB
            row["cpu_usec"] = int(out[1].split()[1])
        except (SystemExit, ValueError, IndexError) as e:
            row["error"] = str(e)[:160]
        try:
            load = self.posture.debug(1, "/v1/debug/load")
            row["self_rss_mb"], row["shed"] = load.get("rss_mb"), load.get("admit_shed")
            # The shard log's L0 count: the refusal engages at 3/4 of its line, releases at 1/2.
            row["l0"] = (self.posture.debug(1, "/operator/data.json").get("local") or {}).get("l0_max")
        except OSError as e:
            row["load_error"] = str(e)[:160]
        try:
            with urllib.request.urlopen(S3_STATS, timeout=5) as r:
                s3 = json.loads(r.read())
            row["wal_puts"] = ((s3.get("cells") or {}).get("shard/wal/put") or {}).get("2xx", 0)
            row["live"] = {k: v.get("bytes", 0) for k, v in ((s3.get("live_bytes") or {}).get("cells") or {}).items()}
        except OSError as e:
            row["s3_error"] = str(e)[:160]
        return row

    def run(self):
        while True:
            row = self.sample()
            self.rows.append(row)
            self.out.write(json.dumps(row) + "\n")
            self.out.flush()
            if self.stop.wait(5):
                return


def rates(rows, t0, t1):
    """The server's CPU, WAL PUTs a second and live-byte growth per class (MB/s) between t0 and t1."""
    win = [r for r in rows if t0 <= r["t"] <= t1 and "wal_puts" in r]
    if len(win) < 2:
        return {}
    a, b = win[0], win[-1]
    dt = b["t"] - a["t"]
    classes = sorted(set(a["live"]) | set(b["live"]))
    l0 = [r["l0"] for r in win if r.get("l0") is not None]
    return {
        "l0_median_max": [statistics.median(l0), max(l0)] if l0 else None,
        "cpu_cores": round((b.get("cpu_usec", 0) - a.get("cpu_usec", 0)) / 1e6 / dt, 3),
        "wal_puts_per_sec": round((b["wal_puts"] - a["wal_puts"]) / dt, 2),
        "growth_mb_per_sec": {c: round((b["live"].get(c, 0) - a["live"].get(c, 0)) / dt / 1e6, 3) for c in classes},
        "live_mb_end": {c: round(b["live"].get(c, 0) / 1e6, 1) for c in classes},
    }


def run_step(posture, size, batch, mb, a, tag):
    """One rung: open-loop sends at MB MB/s of SIZE-byte bodies for a.secs."""
    rate = mb * 1e6 / size
    token = open(posture.tokens).readline().strip() if posture.tokens else None
    benv = ["BENCH_QUEUE_CONSUMERS=0", f"BENCH_QUEUE_RATE={rate}", f"BENCH_QUEUE_MAX_BYTES={a.max_queued}"]
    if token:
        benv.append(f"BENCH_QUEUE_TOKEN={token}")
    res = rig.bench("--mode", "queue", "--streams", "1", "--concurrency", str(a.producers),
                    "--entries", str(batch), "--payload-bytes", str(size), "--prefix", tag, "--label", tag,
                    "--warmup-secs", "5", "--duration-secs", str(a.secs), env=benv, url=URL,
                    timeout=a.secs + 600)
    sent = res.get("sent_per_sec") or 0
    errors = sum((res.get("errors") or {}).values())
    return res, sent, errors, rate


def run_size(posture, size, a, out_dir):
    batch = max(1, min(1000, MIB // size))
    env = posture.compose_env(INITIAL_SHARDS=1, **rig.HostStore.ENV)
    rig.compose("down", "-v", "--remove-orphans", env=env)
    store = rig.HostStore()
    rig.compose("up", "-d", "--no-deps", "streams-1", env=env)
    rig.wait_healthy(8201, "streams-1")
    sampler = Sampler(posture, os.path.join(out_dir, f"samples-{size}.jsonl"))
    sampler.start()
    steps, held, store_full = [], None, False
    try:
        for mb in a.mb_steps:
            if store.live_gb() > a.store_max_gb:  # the rig's limit, not the server's
                store_full = True
                break
            t0 = time.time() + 5
            res, sent, errors, rate = run_step(posture, size, batch, mb, a, f"ingest-{size}-{mb}")
            t1 = time.time()
            rss = [r["rss_mb"] for r in sampler.rows if "rss_mb" in r and t0 <= r["t"] <= t1]
            ok = errors == 0 and not res.get("error") and sent >= 0.98 * rate
            step = {
                "mb_target": mb, "msgs_target": round(rate, 1), "held": ok,
                "sent_per_sec": round(sent, 1), "mb_per_sec": round(sent * size / 1e6, 2),
                "send_latency_ms": (res.get("latency_ms") or {}).get("send"),
                "send_errors": errors, "first_error": res.get("first_error"), "bench_error": res.get("error"),
                "rss_mb": {"median": round(statistics.median(rss), 1) if rss else None,
                           "max": round(max(rss), 1) if rss else None},
                **rates(sampler.rows, t0, t1),
            }
            steps.append(step)
            print(json.dumps({"size": size, **{k: step[k] for k in ("mb_target", "held", "mb_per_sec", "send_errors",
                                                                    "first_error", "rss_mb")},
                              "wal_puts_per_sec": step.get("wal_puts_per_sec")})[:700], flush=True)
            if not ok:
                break
            held = step
    finally:
        sampler.stop.set()
        sampler.join()
        open(os.path.join(out_dir, f"server-{size}.log"), "w").write(
            rig.sh("docker", "logs", "--tail", "200", rig.server(), check=False))
        oom = rig.sh("docker", "inspect", "-f", "{{.State.OOMKilled}}", rig.server(), check=False).strip()
        rig.compose("down", "-v", "--remove-orphans", env=env)
        store.stop()
    return {"size": size, "instance": [posture.cpus, posture.gb], "knobs": posture.knobs,
            "batch": batch, "producers": a.producers, "secs_per_step": a.secs, "posture": posture.name,
            "store_full": store_full,
            "sustained_mb_per_sec": held["mb_per_sec"] if held else 0.0,
            "sustained_msgs_per_sec": held["sent_per_sec"] if held else 0.0,
            # what refused first: the rung that did not hold and its first error
            # (on layout 7 the shard log's L0 watermark, 503 shard_l0_backpressure)
            "first_refusal": ({"mb_target": steps[-1]["mb_target"], "error": steps[-1]["first_error"],
                               "served_mb_per_sec": steps[-1]["mb_per_sec"]}
                              if steps and not steps[-1]["held"] else None),
            "sheds": max((r.get("shed") or 0) for r in sampler.rows) if sampler.rows else None,
            "oom_killed": oom, "steps": steps}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", type=int, nargs="+", default=[1024, 16384, 65536])
    ap.add_argument("--producers", type=int, default=8)
    ap.add_argument("--mb-steps", type=float, nargs="+", default=[1, 2, 4, 8, 14, 20, 30])
    ap.add_argument("--secs", type=int, default=120, help="seconds a rung")
    ap.add_argument("--max-queued", type=int, default=8 << 30)
    ap.add_argument("--store-max-gb", type=float, default=30, help="the host store's live bytes that end a ladder")
    ap.add_argument("--posture", choices=["on", "off"], default="on")
    ap.add_argument("--size", default="1/1", help="the instance, VCPU/GB (memory knobs scale with GB: rig.SCALED)")
    ap.add_argument("--out", default=os.path.join(rig.HERE, "results",
                                                  time.strftime("ingest-%Y%m%dT%H%M%SZ", time.gmtime())))
    ap.add_argument("--set", nargs="*", default=[], metavar="KEY=VALUE", help="server settings on top of the size's (recorded)")
    ap.add_argument("--dry-run", action="store_true", help="print the plan (DRY lines), start nothing")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    posture = rig.Posture(a.posture, a.out, size=a.size, shards=1, overrides=a.set)
    if a.dry_run:
        for size in a.sizes:
            print(f"DRY ingest posture {a.posture} size {a.size} message {size} B batch {max(1, min(1000, MIB // size))} "
                  f"rungs {a.mb_steps} MB/s, {a.secs} s each, knobs {posture.knobs}")
        return
    results = []
    for size in a.sizes:
        print(f"== {size} B messages {time.strftime('%H:%MZ', time.gmtime())}", flush=True)
        r = run_size(posture, size, a, a.out)
        results.append(r)
        open(os.path.join(a.out, "sizes.jsonl"), "a").write(json.dumps(r) + "\n")
        print(json.dumps({k: r[k] for k in ("size", "sustained_mb_per_sec", "sustained_msgs_per_sec", "first_refusal",
                                            "oom_killed")}),
              flush=True)
    print("INGEST_DONE", a.out, flush=True)


if __name__ == "__main__":
    sys.exit(main())
