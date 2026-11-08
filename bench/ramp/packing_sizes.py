#!/usr/bin/env python3
"""packing_sizes.py: the standard packing points per Compute instance size.

"Should the shared cells use larger instances?" For each size (--sizes
VCPU/GB: 1/1, 2/4, 4/8, 8/16) and each point (--points: t = PACK-T,
s = PACK-S-MUX, m = the m-shared mix of ML.env), bench/k2cost/run-local.sh
runs the point with its server in a sized Linux container
(bench/ramp/k2-server.sh, K2_SERVER_DOCKER=1): the tenant count scaled by
--density x the vCPUs (PACK-T 1,000, PACK-S-MUX 200, m-shared 48 s + 1 m at
1 vCPU and density 1), TOKIO_WORKERS at the vCPUs, the profile's per-instance
memory budgets scaled with GB (rig.SCALED, recorded). Prices: k2's ab50 with
compute at $0.064 per measured (cgroup) vCPU-hour and $0.006 per GB-hour of
the size (K2_INSTANCE_GB).

Per size and point it reports $/tenant-month at ab50 (and owner prices),
whether the point was valid (k2's checks: no refusal of a compliant tenant,
no shed, audits and meters), the steady window's append p99, the server's
peak RSS against its shed line, the measured vCPU, and the shards it ran.
Fail early (--first-only): one point per size first, the rest only where
informative. Decision rule (the lead's): the size that minimises $/tenant
within the p99 and memory bars, blast radius as the tie-break.

A measurement: run it under the machine lock, from the worktree the image
was built from (run-local checks the server's commit).
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

import rig

POINTS = {  # name: (point file, base manifest, env selecting it)
    "t": ("PACK-T.env", "pack-t-1000.json", {"PACK_N": "1000"}),
    "s": ("PACK-S.env", "pack-s-mux-0200.json", {"PACK_N": "200", "PACK_MUX": "1"}),
    "m": ("ML.env", "ml-m-shared.json", {"ML_CELL": "m-shared"}),
}


def scaled_manifest(base, factor, out_dir, tag):
    """BASE's classes with their counts x FACTOR (a class holds at most 9,999)."""
    m = json.load(open(os.path.join(rig.ROOT, "bench", "k2cost", "tenants", base)))
    m["name"] = f"{m['name']}-x{factor:g}".replace(".", "p")
    for c in m["classes"]:
        c["count"] = min(9999, max(1, round(c["count"] * factor)))
        if "pool" in c:
            c["pool"] = min(8192, max(c["pool"], round(c["pool"] * factor)))  # k2's limit on a class's sockets
    path = os.path.join(out_dir, f"manifest-{tag}.json")
    json.dump(m, open(path, "w"), indent=1)
    return path, sum(c["count"] for c in m["classes"])


def point_copy(point_file, manifest, out_dir, tag):
    """The point file with TENANTS_MANIFEST naming MANIFEST and a POINT_NAME suffix."""
    src = open(os.path.join(rig.ROOT, "bench", "k2cost", "points", point_file)).read()
    path = os.path.join(out_dir, f"point-{tag}.env")
    open(path, "w").write(src + f"\nTENANTS_MANIFEST={manifest}\nPOINT_NAME=$POINT_NAME-{tag}\n")
    return path


def run_point(a, size, key, out_dir, port_base):
    cpus, gb = rig.parse_size(size)
    point_file, base, sel = POINTS[key]
    factor = cpus * a.density
    tag = f"{key}-{size.replace('/', 'x')}-d{a.density:g}" + ("-sv" if a.shard_a_vcpu else "")
    manifest, tenants = scaled_manifest(base, factor, out_dir, tag)
    point = point_copy(point_file, manifest, out_dir, tag)
    # The points run the profile's one shard, so the per-shard lines scale too.
    shards = max(1, int(cpus)) if a.shard_a_vcpu else 1
    knobs = rig.scaled_knobs(rig.profile_lines(["compute-1g.env", "shared-cell.env"]), gb, shards=shards)
    if a.shard_a_vcpu:  # the instance's cap and shard count follow the vCPUs (the project ceilings stay)
        knobs.update(INITIAL_SHARDS=str(shards), ADMIT_MAX_INFLIGHT=str(512 * shards))
    knobs.update(dict(o.split("=", 1) for o in a.set))  # recorded with the point's knobs
    extra = " ".join([f"TOKIO_WORKERS={max(1, int(cpus))}", f"K2_DOCKER_CPUS={cpus:g}",
                      f"K2_DOCKER_MEM={int(gb * 1024)}m"] + [f"{k}={v}" for k, v in knobs.items()])
    env = dict(os.environ, **sel, K2_SERVER_DOCKER="1", K2_SERVER_BIN=os.path.join(rig.HERE, "k2-server.sh"),
               K2_INSTANCE_GB=f"{gb:g}", K2_HOME=a.k2_home, K2_PORT_BASE=str(port_base),
               K2_RUN_TAG=f"size{size.replace('/', 'x')}", SERVER_ENV_EXTRA=extra,
               K2_S3LITE_BIN=a.s3lite)
    if a.smoke:
        env.update(PACK_SMOKE="1", ML_SMOKE="1")
    if a.dry_run:
        out = subprocess.run(["bench/k2cost/run-local.sh", point, "--dry-run"], cwd=rig.ROOT, env=env,
                             capture_output=True, text=True)
        lines = (out.stdout + out.stderr).splitlines()
        print(f"DRY sizes {size} point {key} tenants {tenants} rc {out.returncode}: " + " | ".join(
            ln for ln in lines if ln.startswith("DRY_RUN") and "env:" not in ln)[:900], flush=True)
        return {"size": size, "point": key, "tenants": tenants, "dry_run": True, "rc": out.returncode}
    started = time.time()
    console = os.path.join(out_dir, f"console-{tag}.log")
    with open(console, "w") as f:
        proc = subprocess.Popen(["bench/k2cost/run-local.sh", point], cwd=rig.ROOT, env=env,
                                stdout=f, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            rc = proc.wait(timeout=a.point_timeout_mins * 60)
        except subprocess.TimeoutExpired:
            # Past its bound the point is stopped whole (run-local tears down on TERM) and the leg fails.
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
            subprocess.run(["docker", "rm", "-f", f"k2srv-{port_base + 2}"], capture_output=True)
            rc = "timeout"
    run_dir = next((ln.split(" -> ")[1].split(" (")[0] for ln in open(console)
                    if "] point " in ln and " -> " in ln), None)
    row = {"size": size, "point": key, "density": a.density, "tenants": tenants, "rc": rc,
           "wall_secs": round(time.time() - started), "knobs": knobs, "run": run_dir}
    if run_dir and os.path.exists(os.path.join(run_dir, "packing.json")):
        p = json.load(open(os.path.join(run_dir, "packing.json")))
        classes = json.load(open(os.path.join(run_dir, "tenants", "classes.json")))
        p99 = max(((c.get("periods") or {}).get("steady") or {}).get("append_p99_ms") or 0
                  for c in classes.values())
        lim = p.get("limits") or {}
        row.update({
            "valid": (p.get("validity") or {}).get("valid"),
            "failed_checks": [c["check"] for c in (p.get("validity") or {}).get("checks", []) if not c.get("ok")],
            "ab50_per_tenant": (p.get("cases") or {}).get("ab50", {}).get("per_tenant_month"),
            "owner_per_tenant": (p.get("cases") or {}).get("owner", {}).get("per_tenant_month"),
            "append_p99_ms": p99,
            "rss_mb": (lim.get("rss_mb") or {}).get("value"),
            "shed_line_mb": int(knobs["ADMIT_RSS_SHED_MB"]),
            "vcpu_used": (lim.get("vcpu") or {}).get("value"),
            "shards": shards,
            "terms_month": p.get("terms_month"),
        })
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", nargs="+", default=["1/1", "2/4", "4/8", "8/16"])
    ap.add_argument("--points", nargs="+", choices=sorted(POINTS), default=["t", "s", "m"])
    ap.add_argument("--density", type=float, default=1.0, help="tenants x vCPUs x DENSITY")
    ap.add_argument("--first-only", action="store_true", help="fail early: the first point per size only")
    ap.add_argument("--set", nargs="*", default=[], metavar="KEY=VALUE", help="server settings on top (recorded)")
    ap.add_argument("--point-timeout-mins", type=float, default=75,
                    help="a point past this is stopped and the leg fails (a full point takes 40-45 min)")
    ap.add_argument("--shard-a-vcpu", action="store_true",
                    help="one shard a vCPU and ADMIT_MAX_INFLIGHT 512 a vCPU (default: the profile's one shard and 512)")
    ap.add_argument("--smoke", action="store_true", help="the points' smoke shape (0.15 time scale)")
    ap.add_argument("--k2-home", default=os.path.expanduser("~/.streams-k2/p5-sizes-bench"))
    ap.add_argument("--port-base", type=int, default=9780)
    ap.add_argument("--dry-run", action="store_true", help="run-local's --dry-run per point (DRY lines), start nothing")
    ap.add_argument("--s3lite", default=os.path.expanduser("~/.streams-k2/queues-bench/bin/s3lite-6aB-abcbb934"),
                    help="the native s3lite run-local starts (the store, on the host)")
    ap.add_argument("--out", default=os.path.join(rig.HERE, "results",
                                                  time.strftime("sizes-%Y%m%dT%H%M%SZ", time.gmtime())))
    a = ap.parse_args()
    a.out = os.path.abspath(a.out)  # run-local runs from the repository's root
    os.makedirs(a.out, exist_ok=True)
    rows = []
    for size in a.sizes:
        for key in (a.points[:1] if a.first_only else a.points):
            print(f"== {size} {key} density {a.density:g} {time.strftime('%H:%MZ', time.gmtime())}", flush=True)
            row = run_point(a, size, key, a.out, a.port_base)
            rows.append(row)
            open(os.path.join(a.out, "points.jsonl"), "a").write(json.dumps(row) + "\n")
            print(json.dumps({k: row.get(k) for k in ("size", "point", "tenants", "valid", "ab50_per_tenant",
                                                      "append_p99_ms", "rss_mb", "shed_line_mb", "vcpu_used",
                                                      "failed_checks", "rc")}), flush=True)
            if row.get("rc") == "timeout":
                print("STOP", size, key, f"ran past {a.point_timeout_mins:g} min", flush=True)
                return 3
    print("SIZES_DONE", a.out, flush=True)


if __name__ == "__main__":
    sys.exit(main())
