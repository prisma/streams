#!/usr/bin/env python3
"""Deploy an in-region k2gen generator into a K2 field cell. Entry point:
bench/k2cost/field/gen.sh <run-id> <cell> <plan> [--replace].

The generator is deploy/app-gen running the k2gen binary uploaded by
bins.py (AWSBENCH_S3_KEY). app-gen hands its binary no arguments, so the
plan travels as GEN_PLAN_JSON and deploy/app-gen/plan.ts runs it (stages
in order, ` ;; `-separated invocations of a stage concurrently) and serves
the window lines on $PORT for observe.py.

Plan files (bench/k2cost/field/plans/*.plan): one stage per line, each a
k2gen invocation (`bun k2gen.ts help`) without --base, --headers-file,
--out, --ledger, --port or --key; `idle SECS` waits. Every measurement
plan starts with `idle 300`: a pre-load baseline for observe.py, which runs
before gen.sh. This tool appends `--base <target> --headers-file @HEADERS`
to every invocation. Targets are SERVICE URLs (stable across versions, so
a rolling deploy or a revive never cuts the generator off), never a
version's preview URL. With routers, as clients enter production (the LB
domain, docs/STAGING.md §2), invocations alternate over every router in
plan order, and a `subs` invocation without producer flags is split over
all of them (count and connect rate divided; idle subscribers on the same
streams are interchangeable), so no phase measures one router's edge
limit. Without routers the target is streams-1. GEN_TARGET (streams-N or
router-N) sends everything to one instance. gen-g<k>.json records each
invocation's target.

One generator per cell at a time: while an earlier g<k> of the cell is
still in the ledger, gen.sh refuses, unless --replace, which destroys it
first through `teardown.py <run> --service <name> --yes` (same guards).

Credentials: a customer JWT for the cell's customer project (feeds.mjs,
signed with the cell's feed key; lifetime = the plan's length plus an
hour, at most ~24 h: a longer plan needs a redeploy) and the cell's data
key, written as header lines to secrets/ (0600) and to the artifact
bucket under k2c/<run>/<cell>/ (TOKENS_S3_KEY; recorded for teardown).

Campaign mode (campaign.json): the generator is deployed into the campaign
project like every cell service (deploy.deploy_service, same guards); its
S3_* are the campaign's artifact bucket (<campaign>-artifacts, e.g.
k2c-cost-artifacts), read from the field home's artifact files.

Env (whole, restated; every other project variable is unset):
AWSBENCH_S3_KEY, S3_* (artifact bucket), TOKENS_S3_KEY, GEN_PLAN_JSON,
RESOLV_OVERRIDE, CPU_LOG_SECS, KEEP_AWAKE=1 with an expiry in
resources.json (GEN_TTL_MIN, default: the plan's length plus 15 min, at
least 30) that the wrapper enforces (KEEP_AWAKE_UNTIL_MS), and
GEN_START_BY_MS = that expiry less the plan's length: plan.ts never starts
the plan later (a cold-booted finished generator must not run its load
again), and releases the guard 2 min after the plan ends. The downloaded
k2gen's sha256 must equal bins.json's.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys

import deploy as D
import fieldlib as F

MODES = {"setup", "produce", "walk", "tail", "group", "churn", "subs", "idle"}
PRODUCER_FLAGS = {"--rate", "--mbps", "--concurrency"}
RESERVED = {"--base", "--headers-file", "--out", "--ledger", "--port", "--key"}
UNTIL_DONE = {"setup", "churn"}


def k2gen_flags(src_dir: str) -> dict:
    """Each mode's flags, from the KNOWN table of the k2gen.ts the uploaded
    binary was built from, so a plan typo fails here, not in the field."""
    text = open(os.path.join(src_dir, "k2gen.ts"), encoding="utf-8").read()

    def arr(name: str) -> list:
        return re.findall(r'"([a-z0-9-]+)"', re.search(rf"const {name} = \[(.*?)\];", text, re.S).group(1))
    common, producer = arr("COMMON"), arr("PRODUCER")
    block = re.search(r"const KNOWN[^=]*= \{(.*?)\n\};", text, re.S).group(1)
    out = {}
    for m in re.finditer(r'"?([a-z-]+)"?:\s*\[(.*?)\]', block):
        body = m.group(2)
        out[m.group(1)] = set(re.findall(r'"([a-z0-9-]+)"', body)) | \
            (set(common) if "...COMMON" in body else set()) | (set(producer) if "...PRODUCER" in body else set())
    return out


def check_flags(stages: list, known: dict) -> None:
    for stage in stages:
        for argv in stage:
            if argv[0] == "idle":
                continue
            bad = [a for a in argv[1:] if a.startswith("--") and a[2:].split("=", 1)[0] not in known.get(argv[0], set())]
            if bad:
                F.die(f"k2gen {argv[0]} does not take {bad}")


def parse_plan(path: str) -> tuple:
    stages, est = [], 0.0
    with open(path, encoding="utf-8") as f:
        for n, raw in enumerate(f, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            stage, longest = [], 0.0
            for part in line.split(" ;; "):
                argv = shlex.split(part)
                if not argv or argv[0] not in MODES:
                    F.die(f"{path}:{n}: unknown mode {argv[:1]}")
                bad = RESERVED.intersection(a.split("=", 1)[0] for a in argv)
                if bad:
                    F.die(f"{path}:{n}: {sorted(bad)} are set by gen.sh")
                if argv[0] == "idle":
                    secs = float(argv[1])
                else:
                    secs = 0.0
                    if "--duration" in argv:
                        secs = float(argv[argv.index("--duration") + 1])
                    elif argv[0] not in UNTIL_DONE:
                        secs = 60.0
                stage.append(argv)
                longest = max(longest, secs + 60)
            stages.append(stage)
            est += longest
    if not stages:
        F.die(f"{path}: empty plan")
    return stages, est


def flag(argv: list, name: str, default: str) -> str:
    for j, a in enumerate(argv):
        if a == name and j + 1 < len(argv):
            return argv[j + 1]
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return default


def with_flag(argv: list, name: str, value: str) -> list:
    out, skip = [], False
    for j, a in enumerate(argv):
        if skip:
            skip = False
            continue
        if a == name:
            skip = True
            continue
        if not a.startswith(name + "="):
            out.append(a)
    return out + [name, value]


def targets_of(run: str, cell: str, st: dict) -> list:
    """[(instance, service URL)] the plan's invocations go to."""
    svcs = (F.load_resources(run)["cells"].get(cell) or {}).get("services") or {}
    by_inst = {r.get("instance"): r.get("service_url") for r in svcs.values() if r.get("status") != "deleted"}
    urls = {**st.get("server_service_urls", {}), **st.get("router_service_urls", {})}

    def url(name: str) -> str:
        u = urls.get(name) or by_inst.get(name)
        if not u:
            F.die(f"no service URL for {name} (redeploy the cell with this deploy.py)")
        return u
    if os.environ.get("GEN_TARGET"):
        return [(os.environ["GEN_TARGET"], url(os.environ["GEN_TARGET"]))]
    routers = sorted(st.get("router_urls") or {}, key=lambda r: int(r.split("-")[1]))
    return [(r, url(r)) for r in routers] or [("streams-1", url("streams-1"))]


def spread(stages: list, targets: list) -> tuple:
    """Append --base/--headers-file to every invocation: alternate over the
    targets in plan order; split a pure `subs` over all of them."""
    out, assigned, k, m = [], [], 0, len(targets)
    for si, stage in enumerate(stages):
        new = []
        for argv in stage:
            if argv[0] == "idle":
                new.append(argv)
                assigned.append({"stage": si, "mode": "idle", "target": None})
                continue
            if argv[0] == "subs" and m > 1 and not PRODUCER_FLAGS.intersection(a.split("=", 1)[0] for a in argv):
                count, rate = int(flag(argv, "--count", "100")), float(flag(argv, "--connect-rate", "20"))
                for j, (name, url) in enumerate(targets):
                    part = with_flag(with_flag(argv, "--count", str(count // m + (1 if j < count % m else 0))),
                                     "--connect-rate", f"{rate / m:g}")
                    new.append(part + ["--base", url, "--headers-file", "@HEADERS"])
                    assigned.append({"stage": si, "mode": "subs", "target": name, "split": f"{j + 1}/{m}"})
                continue
            name, url = targets[k % m]
            k += 1
            new.append(argv + ["--base", url, "--headers-file", "@HEADERS"])
            assigned.append({"stage": si, "mode": argv[0], "target": name})
        out.append(new)
    for n, a in enumerate(assigned):  # plan.ts numbers invocations in this order
        a["inv"] = n
    return out, assigned


def replace_earlier(run: str, cell: str, replace: bool) -> None:
    svcs = (F.load_resources(run)["cells"].get(cell) or {}).get("services") or {}
    live = sorted(n for n, r in svcs.items() if r.get("role") == "gen" and r.get("status") != "deleted")
    if live and not replace:
        F.die(f"generator(s) {live} of {cell} are still in the ledger: tear them down first, or pass --replace")
    for name in live:
        F.say(f"  --replace: destroying {name} (teardown.py --service)")
        p = subprocess.run([sys.executable, os.path.join(F.HERE, "teardown.py"), run, "--service", name, "--yes"])
        if p.returncode != 0:
            F.die(f"could not destroy {name}; nothing deployed")


def main() -> None:
    F.banner("gen")
    args = [a for a in sys.argv[1:] if a != "--replace"]
    if len(args) != 3:
        F.die("usage: gen.sh <run-id> <cell> <plan file or name> [--replace]", 2)
    run, cell, plan = args
    F.check_names(run, cell)
    if not os.path.exists(plan):
        plan = os.path.join(F.HERE, "plans", f"{plan}.plan")
    stages, est = parse_plan(plan)
    project = F.run_project(run, cell)  # campaign mode: the campaign project, live-checked
    if not project or not (F.load_resources(run)["cells"].get(cell) or {}).get("bucket"):
        F.die(f"cell {cell} is not provisioned in run {run}")
    if project == F.artifact_project_id() and not F.campaign():
        F.die("refusing to deploy into the artifact project")
    st = F.read_json(os.path.join(F.cell_dir(run, cell), "cell.json"))
    if not st or not st.get("server_urls"):
        F.die(f"cell {cell} has no deployed servers (deploy-cell.sh first)")
    bins = (F.read_json(os.path.join(F.FIELD, "bins.json")) or {}).get("_latest") or {}
    if not bins.get("k2gen"):
        F.die("no uploaded k2gen: run bins.py first")
    manifest = F.read_json(os.path.join(F.FIELD, "bins.json"))
    check_flags(stages, k2gen_flags(manifest[bins["k2gen"]]["source"]))
    ttl_min = int(os.environ.get("GEN_TTL_MIN") or max(30, est / 60 + 15))
    if ttl_min * 60 - est < 180:  # a generator deploy and boot take ~2.5 min
        F.die(f"GEN_TTL_MIN={ttl_min} leaves under 3 min to deploy before the plan's latest start "
              f"(plan ~{est / 60:.1f} min)")
    targets = targets_of(run, cell, st)
    stages, assigned = spread(stages, targets)
    replace_earlier(run, cell, "--replace" in sys.argv[1:])
    gens = st.setdefault("gens", {})
    k = len(gens) + 1
    svc = f"{F.base_name(run, cell)}-g{k}"
    sdir = F.secrets_dir(run, cell)
    ttl = int(min(24 * 3600 - 300, est + 3600))
    jwt_path = os.path.join(sdir, f"gen-{k}.jwt")
    subprocess.run(["node", os.path.join(F.HERE, "feeds.mjs"), "token", "--dir", sdir, "--cell-id", st["cell_id"],
                    "--project", st["customer_project_id"], "--workspace", st["workspace_id"],
                    "--credential", st["credential_id"], "--ttl", str(ttl), "--sub", svc, "--out", jwt_path], check=True)
    data_key = F.ensure_secret(run, cell, "data-key", F.b64key)
    with open(jwt_path, encoding="utf-8") as f:
        jwt = f.read().strip()
    headers = f"authorization: Bearer {jwt}\nprisma-encryption-key: {data_key}\n"
    F.write_secret(os.path.join(sdir, f"gen-{k}-headers.txt"), headers)
    hkey = f"k2c/{run}/{cell}/gen-{k}-headers.txt"
    F.record_artifact_object(run, cell, hkey)
    client, bucket = F.artifact_s3()
    F.put_verified(client, bucket, hkey, headers.encode())
    env = {
        "AWSBENCH_S3_KEY": bins["k2gen"], "S3_ENDPOINT": F.field_file("artifact-endpoint.txt"),
        "S3_BUCKET": F.field_file("artifact-bucket.txt"), "S3_REGION": "auto",
        "S3_ACCESS_KEY_ID": F.field_file("binid.txt"), "S3_SECRET_ACCESS_KEY": F.field_file("binsec.txt"),
        "TOKENS_S3_KEY": hkey, "GEN_PLAN_JSON": json.dumps(stages, separators=(",", ":")),
        "RESOLV_OVERRIDE": D.RESOLV, "KEEP_AWAKE": "1", "CPU_LOG_SECS": D.CPU_LOG_SECS,
        "KEEP_AWAKE_UNTIL_MS": "0", "GEN_START_BY_MS": "0",
    }
    unknown = D.check_env_names("gen", env, D.known_names())
    if unknown:
        F.die(f"env names app-gen does not read: {unknown}")
    F.say(f"== {cell}: generator {svc} -> {sorted({a['target'] for a in assigned if a['target']})}; plan "
          f"{os.path.basename(plan)}: {sum(len(s) for s in stages)} invocation(s) in {len(stages)} stage(s), "
          f"~{est / 60:.0f} min; KEEP_AWAKE expiry +{ttl_min} min")
    D.stage("app-gen")
    env["GEN_START_BY_MS"] = str(F.now_ms() + ttl_min * 60_000 - int(est * 1000))
    d = D.deploy_service(run, cell, project, svc, "gen", "app-gen", env, ttl_min)

    def plan_up(body: bytes) -> bool:
        try:
            return "invs" in json.loads(body)
        except ValueError:
            return False
    d["gate"] = D.gate_http(d["url"], "/", plan_up, secs=420)
    # A binary the instance cannot exec (a libc mismatch) fails its first
    # invocation at once: say so now, not when the windows stay empty.
    status, body, _ = F.http_get(d["url"] + "/", timeout=15)
    try:
        state = json.loads(body)
        failed = [i for i in state.get("invs", []) if i.get("error") or i.get("rc") not in (None, 0, 3)]
    except ValueError:
        state, failed = {}, [{"status": status}]
    if failed:
        F.say(f"    WARNING: plan invocation(s) failed early: {json.dumps(failed)[:600]}")
    d["early_failures"] = failed
    gens[f"g{k}"] = {"service": svc, "url": d["url"], "service_url": d["service_url"],
                     "targets": [a["target"] for a in assigned], "plan": os.path.abspath(plan),
                     "deployed": F.utc(), "token_ttl_s": ttl}
    D.save_state(run, cell, st)
    report = {"service": svc, "plan": os.path.abspath(plan), "stages": stages, "targets": assigned,
              "estimate_s": est, "env": F.redact(env), "instances": {f"gen-g{k}": d}}
    D.finish(run, cell, report, bins, f"gen-g{k}.json")
    if state.get("refused"):
        F.die(f"{svc}: plan refused: {state['refused']} (teardown.py {run} --service {svc} --yes)")
    F.say(f"    {svc}: plan running at {d['url']} (memory class {d.get('memory_gib_class')} GiB, "
          f"shape {d.get('shape')}, k2gen sha256 ok {d.get('binary_ok')})")


if __name__ == "__main__":
    main()
