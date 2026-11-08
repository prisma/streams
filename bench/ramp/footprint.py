#!/usr/bin/env python3
"""footprint.py: the memory of a resident shard on one 1 GiB Linux server.

For each posture (--postures: `off` = auth, billing and the rollup off;
`on` = the dedicated cell profile, auth enforced for one tenant, billing
required, the rollup on: see rig.py) and each shard count N (--shards,
default 1 8 32) it starts the rig's s3lite and one server with
INITIAL_SHARDS=N (compose.yml, a fresh store), then runs fixed phases:

  boot   the server serving, no stream yet
  open   creates 64 streams a shard and touches them, so every shard's
         engine opens (the transient: the peak while N shard logs open)
  idle   no traffic: what N resident shards hold at rest
  light  about 5 touches (an append and a read) a second a shard
  rest   no traffic again
  busy   2,000 touches a second over 64 streams a shard: past what the
         instance's one vCPU takes (the touches it served are reported)
  hold   (--hold-mins, the largest N only) busy for M minutes: does resident
         memory grow while the load stays flat (the soak's F-RSS question)
  cool   no traffic after the load

The load is habanero's population mode (rig.population), with the tenant's
token when auth is enforced. Every SAMPLE_SECS it reads the server
process's resident set from /proc/1/statm (what the admission shed line and
Compute's memory line read), the container's cgroup memory.current (what
the OOM killer charges, page cache included), and /v1/debug/load (the
server's own rss_mb, its shed counters and its open shard engines). Per
phase it reports the settled median and peak of each, and per posture and
N the MB per resident shard over that run's boot baseline.

Usage: footprint.py [--postures off on] [--shards 1 8 32] [--scale 1.0]
                    [--hold-mins 0] [--out DIR]
Needs the image from build.sh. A measurement: run it under the machine lock.
"""
import argparse
import json
import os
import statistics
import sys
import threading
import time

import rig

SAMPLE_SECS = 0.5
URL = "http://streams-1:8080"
# name, seconds, load: None, or (streams a shard, (per_shard|total, touches a second))
PHASES = [
    ("boot", 30, None),
    ("open", 15, (64, ("total", 64))),
    ("idle", 120, None),
    ("light", 120, (4, ("per_shard", 5))),
    ("rest", 60, None),
    ("busy", 180, (64, ("total", 2000))),
    ("cool", 60, None),
]


class Sampler(threading.Thread):
    """Samples the server until stopped; each sample is tagged with the phase."""

    def __init__(self, out_path, page, posture):
        super().__init__(daemon=True)
        self.out = open(out_path, "a")
        self.page, self.posture = page, posture
        self.phase = "start"
        self.samples = []
        self.stop = threading.Event()

    def sample(self):
        s = {"t": round(time.time(), 3), "phase": self.phase}
        try:
            statm, cg = rig.sh("docker", "exec", rig.server(), "sh", "-c",
                               "cat /proc/1/statm; cat /sys/fs/cgroup/memory.current").split("\n")[:2]
            s["rss_mb"] = int(statm.split()[1]) * self.page / 1048576
            s["cgroup_mb"] = int(cg) / 1048576
        except (SystemExit, ValueError, IndexError) as e:
            s["error"] = str(e)[:200]
        try:
            load = self.posture.debug(1, "/v1/debug/load")
            s["self_rss_mb"] = load.get("rss_mb")
            s["shed"] = load.get("admit_shed")
            s["shed_line_mb"] = load.get("shed_line_mb")
            s["engines"] = len((load.get("maintenance_shards") or {}).get("shards") or [])
            s["tick_gap_ms"] = load.get("runtime_max_tick_gap_ms")
        except OSError as e:
            s["load_error"] = str(e)[:200]
        return s

    def run(self):
        while not self.stop.is_set():
            s = self.sample()
            self.samples.append(s)
            self.out.write(json.dumps(s) + "\n")
            self.out.flush()
            self.stop.wait(SAMPLE_SECS)


def summarize(samples, phase):
    rows = [s for s in samples if s.get("phase") == phase and "rss_mb" in s]
    if not rows:
        return {}
    tail = rows[len(rows) // 2:]  # the settled half

    def med(key):
        vals = [r[key] for r in tail if isinstance(r.get(key), (int, float))]
        return round(statistics.median(vals), 1) if vals else None

    span_min = max(1e-9, (rows[-1]["t"] - rows[0]["t"]) / 60)
    return {
        "samples": len(rows),
        "rss_mb_median": med("rss_mb"), "rss_mb_max": round(max(r["rss_mb"] for r in rows), 1),
        "cgroup_mb_median": med("cgroup_mb"), "cgroup_mb_max": round(max(r.get("cgroup_mb", 0) for r in rows), 1),
        "self_rss_mb_median": med("self_rss_mb"),
        "engines": max((r.get("engines") or 0) for r in rows),
        "shed": max((r.get("shed") or 0) for r in rows),
        "tick_gap_ms_max": max((r.get("tick_gap_ms") or 0) for r in rows),
        # growth over the phase, which a fixed footprint would not show
        "rss_mb_per_min": round((rows[-1]["rss_mb"] - rows[0]["rss_mb"]) / span_min, 2),
    }


def phases_for(shards, hold_mins, largest):
    if not hold_mins or shards != largest:
        return PHASES
    i = [p[0] for p in PHASES].index("cool")
    return PHASES[:i] + [("hold", hold_mins * 60, (64, ("total", 2000)))] + PHASES[i:]


def run_point(posture, shards, out_dir, scale, phases):
    env = posture.compose_env(INITIAL_SHARDS=shards)
    rig.compose("down", "-v", "--remove-orphans", env=env)
    rig.compose("up", "-d", "s3lite", "streams-1", env=env)
    rig.wait_healthy(8201, "streams-1")
    page = int(rig.sh("docker", "exec", rig.server(), "getconf", "PAGESIZE").strip())
    tag = f"{posture.name}-{shards}"
    sampler = Sampler(os.path.join(out_dir, f"samples-{tag}.jsonl"), page, posture)
    sampler.start()
    loads = {}
    try:
        for name, secs, load in phases:
            secs = secs if name == "hold" else max(5, int(secs * scale))
            sampler.phase = name
            if load is None:
                time.sleep(secs)
                continue
            per_shard_streams, (kind, rate) = load
            rate = rate * shards if kind == "per_shard" else rate
            loads[name] = rig.population(per_shard_streams * shards, rate, secs, f"fp-{tag}-{name}",
                                         tokens=posture.tokens, url=URL)
            if loads[name].get("error"):  # no load: the phase measured nothing
                raise SystemExit(f"{tag} {name}: {loads[name]['error'][:200]}")
    finally:
        sampler.stop.set()
        sampler.join()
        open(os.path.join(out_dir, f"server-{tag}.log"), "w").write(
            rig.sh("docker", "logs", "--tail", "200", rig.server(), check=False))
        oom = rig.sh("docker", "inspect", "-f", "{{.State.OOMKilled}}", rig.server(), check=False).strip()
        rig.compose("down", "-v", "--remove-orphans", env=env)
    return {"posture": posture.name, "shards": shards, "size": [posture.cpus, posture.gb], "knobs": posture.knobs,
            "phases": {name: summarize(sampler.samples, name) for name, _, _ in phases},
            "loads": {k: {f: v.get(f) for f in ("created", "create_errors", "target_per_sec", "touches_per_sec",
                                                "touch_errors", "late", "latency_ms", "error")}
                      for k, v in loads.items()},
            "oom_killed": oom}


def per_shard(points):
    """Per resident shard: (phase median - boot median) / N, per posture."""
    out = {}
    for p in points:
        boot = p["phases"]["boot"].get("rss_mb_median") or 0
        row = {"boot_mb": boot}
        for ph in ("idle", "light", "busy", "hold", "cool"):
            if p["phases"].get(ph):
                row[ph] = round(((p["phases"][ph].get("rss_mb_median") or 0) - boot) / p["shards"], 2)
        row["open_peak_over_boot_mb"] = round((p["phases"]["open"].get("rss_mb_max") or 0) - boot, 1)
        out[f'{p["posture"]}-{p["shards"]}'] = row
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--postures", nargs="+", choices=["off", "on"], default=["off", "on"])
    ap.add_argument("--shards", type=int, nargs="+", default=[1, 8, 32])
    ap.add_argument("--scale", type=float, default=1.0, help="phase lengths x SCALE (a smoke run: 0.1)")
    ap.add_argument("--hold-mins", type=int, default=0, help="a busy hold of M minutes on the largest N")
    ap.add_argument("--size", default="1/1", help="the instance, VCPU/GB (memory knobs scale with GB: rig.SCALED)")
    ap.add_argument("--out", default=os.path.join(rig.HERE, "results",
                                                  time.strftime("footprint-%Y%m%dT%H%M%SZ", time.gmtime())))
    ap.add_argument("--dry-run", action="store_true", help="print the plan (DRY lines), start nothing")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    if a.dry_run:
        for name in a.postures:
            posture = rig.Posture(name, a.out, size=a.size)
            for n in a.shards:
                phases = [(ph, secs if ph == "hold" else max(5, int(secs * a.scale)))
                          for ph, secs, _ in phases_for(n, a.hold_mins, max(a.shards))]
                print(f"DRY footprint posture {name} size {a.size} shards {n} phases {phases} knobs {posture.knobs}")
        return
    points = []
    for name in a.postures:
        posture = rig.Posture(name, a.out, size=a.size)
        for n in a.shards:
            print(f"== {name} {n} shards, size {a.size} {time.strftime('%H:%MZ', time.gmtime())}", flush=True)
            p = run_point(posture, n, a.out, a.scale, phases_for(n, a.hold_mins, max(a.shards)))
            points.append(p)
            print(json.dumps({"posture": name, "shards": n, "phases": p["phases"], "loads": p["loads"]}), flush=True)
    result = {"points": points, "per_shard_mb": per_shard(points)}
    json.dump(result, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    print(json.dumps(result["per_shard_mb"]), flush=True)
    print("FOOTPRINT_DONE", a.out, flush=True)


if __name__ == "__main__":
    sys.exit(main())
