#!/usr/bin/env python3
"""ramp.py: prisma/streams#149's tiny-stream ramp, compressed 60x, on the Mac.

Each step is a fresh cell (s3lite and INSTANCES servers of compose.yml,
INITIAL_SHARDS=SHARDS, FLEET_MIN=FLEET_MAX=INSTANCES behind the router)
holding COUNT tiny streams, each touched once a PERIOD (60 s instead of
#149's hour) by one 400-byte append and one read from its start, on the
product surface. The driver is Streams' bench with habanero's population
mode (prisma/habanero#771's bench-modes.patch, built in by build.sh), run
in a container on the rig's network: it creates the streams, then touches
them open-loop at COUNT/PERIOD a second for STEP seconds after a 30 s
warm-up.

With --fleet-min and --fleet-max the ramp keeps one cell for every step
instead (FLEET_MIN servers at first, INITIAL_SHARDS=SHARDS) and adds each
step's streams to the last step's; a Scaler plays Compute's scaler, starting
and stopping servers as the fleet's desired count says, and reloading the
router (the scaling design's interim posture: 8 shards, a fleet of 1 to 8).

A step holds, as #149's ramp, when every stream was created, at least 98%
of the touches due were served with no touch error, their p99 is at most
2 s, and no server shed anything (admission's shed counters, read every
5 s) or was OOM-killed. The ramp stops at the first step that does not
hold. Fixed INSTANCES and SHARDS today (the fleet does not resize on this
workload); the same rig measures the tuning packages as they land and,
later, dynamic split and merge (P5) with FLEET_MIN=1.

Tenancy, with auth enforced from cell-admin feed files (tenants.sh, cached
per count under --tenants-cache): `one` gives every stream one project's
token; `each` gives stream i project i's token. `--auth off` runs `one`
with no tokens and auth off (the footprint posture).

Usage: ramp.py [--instances 1] [--shards N] [--counts 10 100 ...]
               [--period 60] [--step-secs 180] [--out DIR]
"""
import argparse
import json
import os
import sys
import threading
import time

import rig
from rig import HERE, sh

WARMUP_SECS = 30


def router_conf(instances):
    """nginx: `compute-instance: k` pins to streams-(k+1); otherwise round robin."""
    pins = "\n".join(f'    "{k}" streams-{k + 1}:8080;' for k in range(instances))
    rr = "\n".join(f"    server streams-{k + 1}:8080;" for k in range(instances))
    return f"""events {{ worker_connections 16384; }}
worker_rlimit_nofile 65536;
http {{
  access_log off;
  resolver 127.0.0.11 valid=30s ipv6=off;
  upstream spread {{
{rr}
    keepalive 256;
  }}
  map $http_compute_instance $pinned {{
    default "";
{pins}
  }}
  server {{
    listen 8080;
    client_max_body_size 8m;
    location / {{
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_read_timeout 60s;
      if ($pinned != "") {{ proxy_pass http://$pinned; break; }}
      proxy_pass http://spread;
    }}
  }}
}}
"""


class Watch(threading.Thread):
    """Every 5 s: each server's resident set (statm), shed counters, engines."""

    def __init__(self, instances, path, posture, live=None):
        super().__init__(daemon=True)
        self.instances, self.out, self.posture = instances, open(path, "a"), posture
        self.live = live  # a Scaler: poll the servers it runs, else 1..INSTANCES
        self.stop = threading.Event()
        self.first, self.last, self.rss_max = {}, {}, {}

    def poll(self, k):
        name = f"streams-{k}"
        row = {"t": round(time.time(), 1), "server": name}
        try:
            statm = sh("docker", "exec", rig.server(k), "cat", "/proc/1/statm")
            row["rss_mb"] = int(statm.split()[1]) * 4096 / 1048576
        except SystemExit as e:
            row["error"] = str(e)[:160]
        try:
            load = self.posture.debug(k, "/v1/debug/load")
            row["shed"] = load.get("admit_shed")
            row["engines"] = len((load.get("maintenance_shards") or {}).get("shards") or [])
        except OSError as e:
            row["load_error"] = str(e)[:160]
        return row

    def run(self):
        while True:
            for k in range(1, (self.live.live if self.live else self.instances) + 1):
                row = self.poll(k)
                self.out.write(json.dumps(row) + "\n")
                name = row["server"]
                if "shed" in row:
                    self.first.setdefault(name, row["shed"])
                    self.last[name] = row["shed"]
                if "rss_mb" in row:
                    self.rss_max[name] = max(self.rss_max.get(name, 0), row["rss_mb"])
            self.out.flush()
            if self.stop.wait(5):
                return

    def sheds(self):
        return sum(self.last[n] - self.first.get(n, 0) for n in self.last)


def fresh_cell(instances, shards, conf_path, posture):
    env = posture.compose_env(INITIAL_SHARDS=shards, FLEET_MIN=instances, FLEET_MAX=instances,
                              ROUTER_CONF=conf_path)
    rig.compose("down", "-v", "--remove-orphans", env=env)
    rig.compose("up", "-d", "s3lite", *[f"streams-{k}" for k in range(1, instances + 1)], env=env)
    for k in range(1, instances + 1):
        rig.wait_healthy(8200 + k, f"streams-{k}")
    # The router resolves its upstreams at start: only once every server is up.
    rig.compose("up", "-d", "router", env=env)
    rig.wait_healthy(8200, "the router", tries=60)
    return env


def gate(instances, shards, log, posture, stable_secs=30, timeout_secs=600):
    """Habanero's gate2.sh: under light warm traffic, wait until every ring
    holds INSTANCES, every instance owns at most the fair share and they own
    SHARDS in all, and that placement is stable for STABLE_SECS."""
    share = -(-shards // instances)
    start, stable_since, last, rings, counts = time.time(), None, None, set(), []
    while time.time() - start < timeout_secs:
        rig.population(shards * 4, instances * 20, 5, f"gate-{int(time.time())}", posture.tokens)
        try:
            views = [posture.debug(k, "/operator/data.json") for k in range(1, instances + 1)]
        except OSError:
            continue
        rings = {len((v.get("local") or {}).get("ring_active") or []) for v in views}
        now = time.time() * 1000
        beats = (views[0].get("fleet") or {}).get("heartbeats") or []
        owned = {h.get("instance"): sorted(h.get("owned_shards") or []) for h in beats
                 if now - (h.get("ts_ms") or 0) < 15000}
        counts = [len(owned.get(f"streams-{k}", [None] * 999)) for k in range(1, instances + 1)]
        ok = rings == {instances} and max(counts) <= share and sum(counts) == shards
        placement = json.dumps(sorted(owned.items()))
        stable_since = (stable_since or time.time()) if ok and placement == last else None
        last = placement
        log.write(json.dumps({"t": round(time.time()), "rings": sorted(rings), "counts": counts,
                              "ok": ok, "stable": round(time.time() - stable_since) if stable_since else 0}) + "\n")
        log.flush()
        if stable_since and time.time() - stable_since >= stable_secs:
            return counts
    raise SystemExit(f"the gate did not pass in {timeout_secs} s: rings {rings} counts {counts}")


class Scaler(threading.Thread):
    """Compute's scaler on the Mac: every 5 s it reads the fleet's desired
    count (`fleet.desired.count` on /operator/data.json, from streams-1, which
    it never stops) and starts servers up to it, or stops the newest after
    the desired count has stayed below the live count for 30 s, within
    FLEET_MIN..FLEET_MAX; then it reloads the router with the live servers."""

    def __init__(self, a, env, posture, conf, log):
        super().__init__(daemon=True)
        self.a, self.env, self.posture, self.conf, self.log = a, env, posture, conf, log
        self.live, self.below_since = a.fleet_min, None
        self.stop = threading.Event()

    def resize(self, n):
        if n > self.live:
            rig.compose("up", "-d", *[f"streams-{k}" for k in range(self.live + 1, n + 1)], env=self.env)
            for k in range(self.live + 1, n + 1):
                rig.wait_healthy(8200 + k, f"streams-{k}")
        else:
            # SIGTERM: the server drains its shards within its drain budget (144 s)
            rig.compose("stop", "-t", "180", *[f"streams-{k}" for k in range(n + 1, self.live + 1)], env=self.env)
        self.live = n
        open(self.conf, "w").write(router_conf(n))
        sh("docker", "exec", f"{rig.PROJECT}-router-1", "nginx", "-s", "reload", check=False)

    def run(self):
        while not self.stop.wait(5):
            try:
                desired = ((self.posture.debug(1, "/operator/data.json").get("fleet") or {}).get("desired") or {})
            except OSError:
                continue
            want = max(self.a.fleet_min, min(self.a.fleet_max, int(desired.get("count") or self.live)))
            now = time.time()
            self.below_since = (self.below_since or now) if want < self.live else None
            row = {"t": round(now), "desired": desired.get("count"), "reason": desired.get("reason"), "live": self.live}
            if want > self.live or (want < self.live and now - self.below_since >= 30):
                row["resize_to"] = want
                self.resize(want)
                self.below_since = None
            self.log.write(json.dumps(row) + "\n")
            self.log.flush()


def run_continuous(a, out_dir, results):
    """One cell for every step (FLEET_MIN servers at first, Scaler-sized), the
    streams of each step added to the last's: the design's interim posture."""
    conf = os.path.abspath(os.path.join(out_dir, "router.conf"))
    open(conf, "w").write(router_conf(a.fleet_min))
    if a.auth == "off":
        posture = rig.Posture("off", out_dir, fleet=True)
    else:
        posture = rig.Posture("on", out_dir, max(a.counts) if a.tenancy == "each" else 1, a.tenants_cache, fleet=True)
    env = posture.compose_env(INITIAL_SHARDS=a.shards, FLEET_MIN=a.fleet_min, FLEET_MAX=a.fleet_max,
                              ROUTER_CONF=conf)
    rig.compose("down", "-v", "--remove-orphans", env=env)
    rig.compose("up", "-d", "s3lite", *[f"streams-{k}" for k in range(1, a.fleet_min + 1)], env=env)
    for k in range(1, a.fleet_min + 1):
        rig.wait_healthy(8200 + k, f"streams-{k}")
    rig.compose("up", "-d", "router", env=env)
    rig.wait_healthy(8200, "the router", tries=60)
    scaler = Scaler(a, env, posture, conf, open(os.path.join(out_dir, "scaler.jsonl"), "a"))
    scaler.start()
    prev = 0
    try:
        for count in a.counts:
            print(f"== {count} streams, {scaler.live} instance(s) live, {a.shards} shard(s) "
                  f"{time.strftime('%H:%MZ', time.gmtime())}", flush=True)
            watch = Watch(a.fleet_max, os.path.join(out_dir, f"watch-{count}.jsonl"), posture, live=scaler)
            watch.start()
            started, live_start = time.time(), scaler.live
            try:
                pop = rig.population(count, count / a.period, a.step_secs, "ramp", posture.tokens,
                                     warmup=WARMUP_SECS, create_from=prev)
            finally:
                watch.stop.set()
                watch.join()
            ooms = [f"streams-{k}" for k in range(1, a.fleet_max + 1)
                    if sh("docker", "inspect", "-f", "{{.State.OOMKilled}}", rig.server(k), check=False).strip() == "true"]
            ok, checks, held = verdict(count, a.period, a.step_secs, pop, watch.sheds(), ooms)
            step = {"count": count, "shards": a.shards, "fleet": [a.fleet_min, a.fleet_max],
                    "live": [live_start, scaler.live], "tenancy": a.tenancy, "auth": a.auth,
                    "period_secs": a.period, "step_secs": a.step_secs, "wall_secs": round(time.time() - started),
                    "held": round(held, 4) if held is not None else None, "pass": ok, "checks": checks,
                    "sheds": watch.sheds(), "oom_killed": ooms,
                    "rss_mb_max": {k: round(v, 1) for k, v in sorted(watch.rss_max.items())}, "population": pop}
            open(results, "a").write(json.dumps(step) + "\n")
            print(json.dumps({k: step[k] for k in ("count", "pass", "held", "live", "checks", "sheds", "rss_mb_max")}
                             | {"p99_ms": (pop.get("latency_ms") or {}).get("p99"),
                                "errors": dict(list((pop.get("errors") or {}).items())[:3]) or pop.get("error")}),
                  flush=True)
            prev = count
            if not ok:
                print(f"RAMP_STOP at {count}", flush=True)
                return
        print("RAMP_HELD every step", flush=True)
    finally:
        scaler.stop.set()
        scaler.join()


def verdict(count, period, step_secs, pop, sheds, ooms):
    target = count / period
    held = (pop.get("touches_per_sec") or 0) / target if target else None
    p99 = (pop.get("latency_ms") or {}).get("p99")
    checks = {
        "created": pop.get("created") == count and pop.get("create_errors") == 0,
        "touched": pop.get("touch_errors") == 0 and ((held or 0) >= 0.98 or target * step_secs < 50),
        "p99": p99 is not None and p99 <= 2000,
        "no_shed": sheds == 0,
        "no_oom": not ooms,
    }
    return all(checks.values()), checks, held


def run_step(a, count, out_dir):
    label = f"ramp-{count}"
    conf = os.path.abspath(os.path.join(out_dir, "router.conf"))
    open(conf, "w").write(router_conf(a.instances))
    if a.auth == "off":
        posture = rig.Posture("off", out_dir, fleet=a.instances > 1)
    else:
        posture = rig.Posture("on", out_dir, count if a.tenancy == "each" else 1, a.tenants_cache,
                              fleet=a.instances > 1)
    fresh_cell(a.instances, a.shards, conf, posture)
    placement = None
    if a.instances > 1:
        placement = gate(a.instances, a.shards, open(os.path.join(out_dir, f"gate-{count}.jsonl"), "a"), posture)
    watch = Watch(a.instances, os.path.join(out_dir, f"watch-{count}.jsonl"), posture)
    watch.start()
    started = time.time()
    try:
        pop = rig.population(count, count / a.period, a.step_secs, label, posture.tokens, warmup=WARMUP_SECS)
    finally:
        watch.stop.set()
        watch.join()
    ooms = [f"streams-{k}" for k in range(1, a.instances + 1)
            if sh("docker", "inspect", "-f", "{{.State.OOMKilled}}", rig.server(k), check=False).strip() == "true"]
    ok, checks, held = verdict(count, a.period, a.step_secs, pop, watch.sheds(), ooms)
    return {
        "count": count, "instances": a.instances, "shards": a.shards, "tenancy": a.tenancy, "auth": a.auth,
        "period_secs": a.period, "step_secs": a.step_secs, "wall_secs": round(time.time() - started),
        "held": round(held, 4) if held is not None else None, "pass": ok, "checks": checks,
        "sheds": watch.sheds(), "oom_killed": ooms, "placement": placement,
        "rss_mb_max": {k: round(v, 1) for k, v in sorted(watch.rss_max.items())},
        "population": pop,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--instances", type=int, default=1, help="fixed fleet: a fresh cell of this many per step")
    ap.add_argument("--fleet-min", type=int, help="with --fleet-max: one cell for every step, sized by the fleet")
    ap.add_argument("--fleet-max", type=int)
    ap.add_argument("--shards", type=int)
    ap.add_argument("--tenancy", choices=["one", "each"], default="one")
    ap.add_argument("--auth", choices=["enforce", "off"], default="enforce")
    ap.add_argument("--tenants-cache", default=rig.TENANTS_CACHE)
    ap.add_argument("--counts", type=int, nargs="+", default=[10, 100, 1000, 10000, 100000, 1000000])
    ap.add_argument("--period", type=float, default=60.0, help="seconds between two touches of one stream")
    ap.add_argument("--step-secs", type=int, default=180)
    ap.add_argument("--out", default=os.path.join(HERE, "results", time.strftime("ramp-%Y%m%dT%H%M%SZ", time.gmtime())))
    ap.add_argument("--dry-run", action="store_true", help="print the plan (DRY lines), start nothing")
    a = ap.parse_args()
    if (a.fleet_min is None) != (a.fleet_max is None):
        raise SystemExit("--fleet-min and --fleet-max go together")
    if a.fleet_max is not None and not 1 <= a.fleet_min <= a.fleet_max <= 8:
        raise SystemExit("1 <= --fleet-min <= --fleet-max <= 8 (compose.yml's servers)")
    a.shards = a.shards or a.fleet_max or a.instances
    if a.tenancy == "each" and a.auth == "off":
        raise SystemExit("--tenancy each needs --auth enforce")
    if not 1 <= a.instances <= 8:
        raise SystemExit("--instances is 1 to 8 (compose.yml's servers)")
    os.makedirs(a.out, exist_ok=True)
    results = os.path.join(a.out, "steps.jsonl")
    if a.dry_run:
        fleet = f"fleet {a.fleet_min}-{a.fleet_max} (one cell)" if a.fleet_max else f"{a.instances} instance(s) (a cell a step)"
        print(f"DRY ramp {fleet} shards {a.shards} tenancy {a.tenancy} auth {a.auth} counts {a.counts} "
              f"period {a.period} s, {a.step_secs} s a step")
        return
    try:
        if a.fleet_max is not None:
            run_continuous(a, a.out, results)
            return
        for count in a.counts:
            print(f"== {count} streams on {a.instances} instance(s), {a.shards} shard(s) "
                  f"{time.strftime('%H:%MZ', time.gmtime())}", flush=True)
            step = run_step(a, count, a.out)
            open(results, "a").write(json.dumps(step) + "\n")
            pop = step["population"]
            print(json.dumps({k: step[k] for k in ("count", "pass", "held", "checks", "sheds", "rss_mb_max")}
                             | {"p99_ms": (pop.get("latency_ms") or {}).get("p99"),
                                "errors": dict(list((pop.get("errors") or {}).items())[:3]) or pop.get("error")}),
                  flush=True)
            if not step["pass"]:
                print(f"RAMP_STOP at {count}", flush=True)
                break
        else:
            print("RAMP_HELD every step", flush=True)
    finally:
        rig.compose("down", "-v", "--remove-orphans",
                    env=dict(os.environ, ROUTER_CONF=os.path.abspath(os.path.join(a.out, "router.conf"))))
        print("RAMP_DONE", a.out, flush=True)


if __name__ == "__main__":
    sys.exit(main())
