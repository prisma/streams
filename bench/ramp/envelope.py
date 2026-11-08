#!/usr/bin/env python3
"""envelope.py: the append envelope of one server, per instance size.

The cell envelope (`CELL_ENVELOPE_*`: requests/s and append bytes/s one
instance carries, 1,411 req/s and 2.06 MB/s on the 1 GiB class, "measured on
another shape") for each instance size the scaling design prices: a fresh
cell of one server (--size VCPU/GB, the profile's memory budgets scaled with
GB, rig.SCALED; --shards, default one a vCPU; posture `on`: the dedicated
cell, auth enforced, billing and the rollup on) takes an open-loop ladder of
appends (--rates, appends a second in total, of --payload-bytes each, over
64 streams a shard, on the product surface with the tenant's token), each
rung for --secs after a 10 s warm-up. A rung holds when at least 98% of its
appends were served, with no error, a p99 of at most 2 s, and no shed; the
ladder stops at the first rung that does not. The envelope at that size is
the highest rung held, in requests and MB a second, beside the server's
CPU (cgroup cpu.stat usage over the rung) and RSS.

Usage: envelope.py [--size 1/1] [--shards N] [--rates 250 500 ...]
                   [--payload-bytes 1024] [--secs 120] [--out DIR]
A measurement: run it under the machine lock.
"""
import argparse
import json
import os
import statistics
import sys
import threading
import time

import rig

URL = "http://streams-1:8080"


class Sampler(threading.Thread):
    """Every 2 s: resident set, cgroup CPU usage (µs), shed counter."""

    def __init__(self, posture, path):
        super().__init__(daemon=True)
        self.posture, self.out = posture, open(path, "a")
        self.rows, self.stop = [], threading.Event()

    def run(self):
        while True:
            row = {"t": round(time.time(), 2)}
            try:
                out = rig.sh("docker", "exec", rig.server(), "sh", "-c",
                             "cat /proc/1/statm; grep usage_usec /sys/fs/cgroup/cpu.stat").split("\n")
                row["rss_mb"] = int(out[0].split()[1]) * 4096 / 1048576
                row["cpu_usec"] = int(out[1].split()[1])
            except (SystemExit, ValueError, IndexError) as e:
                row["error"] = str(e)[:160]
            try:
                row["shed"] = self.posture.debug(1, "/v1/debug/load").get("admit_shed")
            except OSError as e:
                row["load_error"] = str(e)[:160]
            self.rows.append(row)
            self.out.write(json.dumps(row) + "\n")
            self.out.flush()
            if self.stop.wait(2):
                return

    def window(self, t0, t1):
        rows = [r for r in self.rows if t0 <= r["t"] <= t1 and "cpu_usec" in r]
        if len(rows) < 2:
            return {}
        a, b = rows[0], rows[-1]
        return {"cpu_cores": round((b["cpu_usec"] - a["cpu_usec"]) / 1e6 / (b["t"] - a["t"]), 3),
                "rss_mb_median": round(statistics.median(r["rss_mb"] for r in rows), 1),
                "rss_mb_max": round(max(r["rss_mb"] for r in rows), 1),
                "sheds": (b.get("shed") or 0) - (a.get("shed") or 0)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", default="1/1", help="the instance, VCPU/GB")
    ap.add_argument("--shards", type=int, help="default: one a vCPU")
    ap.add_argument("--rates", type=float, nargs="+", default=[250, 500, 1000, 2000, 4000, 8000, 16000])
    ap.add_argument("--payload-bytes", type=int, default=1024)
    ap.add_argument("--secs", type=int, default=120, help="seconds a rung")
    ap.add_argument("--out", default=os.path.join(rig.HERE, "results",
                                                  time.strftime("envelope-%Y%m%dT%H%M%SZ", time.gmtime())))
    ap.add_argument("--set", nargs="*", default=[], metavar="KEY=VALUE", help="server settings on top of the size's (recorded)")
    ap.add_argument("--dry-run", action="store_true", help="print the plan (DRY lines), start nothing")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    shards = a.shards or max(1, int(rig.parse_size(a.size)[0]))
    posture = rig.Posture("on", a.out, size=a.size, shards=shards, overrides=a.set)
    if a.dry_run:
        print(f"DRY envelope size {a.size} shards {shards} rates {a.rates} payload {a.payload_bytes} B, "
              f"{a.secs} s a rung, knobs {posture.knobs}")
        return
    env = posture.compose_env(INITIAL_SHARDS=shards, **rig.HostStore.ENV)
    rig.compose("down", "-v", "--remove-orphans", env=env)
    store = rig.HostStore()
    rig.compose("up", "-d", "--no-deps", "streams-1", env=env)
    rig.wait_healthy(8201, "streams-1")
    sampler = Sampler(posture, os.path.join(a.out, "samples.jsonl"))
    sampler.start()
    rungs, held = [], None
    try:
        for rate in a.rates:
            label = f"env-{int(rate)}"
            t0 = time.time() + 10
            pop = rig.population(64 * shards, rate, a.secs, label, posture.tokens, warmup=10, url=URL,
                                 payload_bytes=a.payload_bytes, appends_only=True)
            t1 = time.time()
            served = pop.get("touches_per_sec") or 0
            p99 = (pop.get("latency_ms") or {}).get("p99")
            win = sampler.window(t0, t1)
            ok = (not pop.get("error") and pop.get("touch_errors") == 0 and served >= 0.98 * rate
                  and p99 is not None and p99 <= 2000 and not win.get("sheds"))
            rung = {"rate": rate, "held": ok, "requests_per_sec": round(served, 1),
                    "mb_per_sec": round(served * a.payload_bytes / 1e6, 3), "p99_ms": p99,
                    "touch_errors": pop.get("touch_errors"), "late": pop.get("late"),
                    "errors": dict(list((pop.get("errors") or {}).items())[:3]) or pop.get("error"), **win}
            rungs.append(rung)
            print(json.dumps(rung), flush=True)
            if not ok:
                break
            held = rung
    finally:
        sampler.stop.set()
        sampler.join()
        oom = rig.sh("docker", "inspect", "-f", "{{.State.OOMKilled}}", rig.server(), check=False).strip()
        rig.compose("down", "-v", "--remove-orphans", env=env)
        store.stop()
    result = {"size": [posture.cpus, posture.gb], "shards": shards, "knobs": posture.knobs,
              "payload_bytes": a.payload_bytes, "secs_per_rung": a.secs, "oom_killed": oom,
              "envelope": {k: held[k] for k in ("requests_per_sec", "mb_per_sec", "p99_ms", "cpu_cores",
                                                "rss_mb_max")} if held else None,
              "rungs": rungs}
    json.dump(result, open(os.path.join(a.out, "envelope.json"), "w"), indent=1)
    print(json.dumps({"size": a.size, "shards": shards, "envelope": result["envelope"]}), flush=True)
    print("ENVELOPE_DONE", a.out, flush=True)


if __name__ == "__main__":
    sys.exit(main())
