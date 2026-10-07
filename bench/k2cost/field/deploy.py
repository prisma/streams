#!/usr/bin/env python3
"""Deploy one K2 field cell (target/k2-design.md §6, §9.2). Entry points:

    bench/k2cost/field/deploy-cell.sh <run-id> <cell>            # the whole cell
    bench/k2cost/field/deploy-cell.sh <run-id> <cell> --kill N   # stop streams-N's version
    bench/k2cost/field/deploy-cell.sh <run-id> <cell> --only N   # (re)deploy streams-N alone

The cell file $K2_FIELD_HOME/cells/<cell>.env says SERVERS, FLEET_MIN,
ROUTERS, KEEP_AWAKE (+ KEEP_AWAKE_TTL_MIN), SCRAPE, WAL_POSTURE,
INITIAL_SHARDS and SERVER_ENV_EXTRA. Topology follows
bench/fleet/deploy-fleet.sh and RUNBOOK §7: SERVERS ordinal servers
streams-1..N in fleet mode (FLEET_PREFIX set), every one of them in the
routers' UPSTREAMS, with FLEET_MAX=N: the scaler can grow the ring to every
deployed ordinal and no further, and the ordinals above FLEET_MIN (default
min(2,N)) are spares that idle and sleep until the scaler wants them (the
pilot routes to the first `desired` upstreams only and wakes a desired
ordinal that sleeps). KEEP_AWAKE applies to ordinals 1..FLEET_MIN and the
routers, never to a spare. Fleet mode needs FLEET_MAX > 1, so a T-single
cell (N=1) gets FLEET_MAX=2 with no second ordinal: it is labelled
`capped` (it cannot scale out; its heartbeats still carry cpu_pct). Then
ROUTERS pilot load balancers (MODE=lb) router-1..M.

KEEP_AWAKE is bounded twice: the ledger expiry (now + KEEP_AWAKE_TTL_MIN)
travels to the instance as KEEP_AWAKE_UNTIL_MS, where the wrapper's guard
releases itself at that time, and observe.py destroys any service past its
expiry (teardown.py --expired). Every instance logs its cumulative CPU every
CPU_LOG_SECS (15) awake seconds; every instance's downloaded binary sha256
(the wrapper's `binary sha256` line) must equal bins.json's, or the deploy
fails (target/k2-design.md §6: refuse a run of another build).

--kill N stops ordinal N's running version (`compute versions stop`, a
ledger-listed service of this run only) and records when; --only N deploys
ordinal N alone (a new version; the stopped or old one is destroyed),
health-gates it, republishes <FLEET_PREFIX>/fleet/urls.json, which the
routers adopt without a redeploy (src/bin/pilot/lb.rs), and records the gap
since the kill and the revived boot. A rolling deploy is --only 1..N in
turn; the routers and the generators (which use service URLs) stay up.

Sequence: refuse unknown env names (checked against src/config/*.rs, the
pilot sources and the deploy/app-* wrappers); stage deploy/app-server and
deploy/app-lb with bench/stage-app.sh; mint the cell's identities, keys
and auth feeds; deploy each server and health-gate it on /health before
the next; publish fleet/urls.json; wait for the ring in the bucket (every
server has beaten, desired count and boots unchanged for 60 s, and with
KEEP_AWAKE the ring membership too: see ring_gate);
deploy each router and gate it on /stats (never /health: a router proxies
it to a server); read each instance's `instance shape:` boot line from
`compute logs` for its memory size. Each deploy restates the service's
whole environment and unsets every other project variable (RUNBOOK §7.3:
Compute env is project-scoped and merged, and every deploy snapshots the
union); the variable listing and the deploy run under one CLI lock, so no
other field tool's deploy sets a variable in between. Values travel in a
0600 env file that is deleted after the call, never on a command line.
Right after each deploy, before its health gate, `compute versions show`
must list no variable the deploy did not state (system-managed names
aside), or the deploy stops (teardown.sh removes what it made); deploy.json
also records each version's env names.

The env listing refuses the deploy if any row names another project (or
none): the compute CLI sets and unsets each variable by its own lookup on
the same filter and then PATCHes or DELETEs the first row by id
(compute-sdk #applyEnvVars), which no gate sees.

Campaign mode (campaign.json): every service goes into the campaign
project (names unchanged). `--project` is always that project and every
`--unset-env` key comes from GET /v1/environment-variables?projectId=
<campaign>, so the snapshot of each version is exact although several
cells share the project; under the same CLI lock every key the deploy sets
or unsets is probed with the CLI's exact lookup (`&key=K`), which must
return at most one row, the campaign project's (probe_env_keys). A redeploy's
`--service` id, and --kill's service and version, are first proven live in
the campaign project (fieldlib.campaign_owns); fieldlib.gate_cli refuses
any other.

Outputs: resources.json (services, versions, URLs, KEEP_AWAKE expiries),
runs/<run>/<cell>/cell.json (ids, preview and service URLs, no secret) and
results/<run>/<cell>/deploy.json (each instance's redacted environment,
the platform's env-name list for its version, shape, memory GiB, gates,
binary identity), deploy-only-s<N>-<utc>.json and kill-s<N>-<utc>.json.
"""
from __future__ import annotations

import calendar
import concurrent.futures as cf
import glob
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse

import fieldlib as F

APPS = os.path.join(F.FIELD, "apps")
STAGE = os.path.join(F.ROOT, "bench", "stage-app.sh")
PROFILE = os.path.join(F.ROOT, "deploy", "profiles", "compute-1g.env")
# Literal backslash-n: the deploy CLI passes it through and the wrapper
# unescapes it (deploy/app-server/index.ts, RESOLV_OVERRIDE).
RESOLV = "nameserver 108.61.10.10\\nnameserver 8.8.8.8"
FEEDS_DIR = "/tmp/feeds"
# §6: STAGING §4 engine knobs except the polls; binary poll defaults.
ENGINE = {
    "WAL_GROUP_COMMIT": "1", "FLUSH_INTERVAL_MS": "25", "WAL_POST_ACK_GATHER_MS": "6",
    "FRAME_COMPRESS": "1", "ABSORB_BYTES": "4194304", "ABSORB_AGE_SECS": "60",
    "TRIM_PER_OP": "65536", "TRIM_GLOBAL_BUDGET": "65536",
    "ADMIT_MAX_INFLIGHT": "512", "ADMIT_MAX_INFLIGHT_PER_STREAM": "256", "TAIL_RING_BYTES": "0",
}
LIMITS = {"LIMIT_BYTES_PER_SEC": "5000000", "LIMIT_REQS_PER_SEC": "1000", "LIMIT_RECS_PER_SEC": "5000",
          "LIMIT_BURST_SECS": "2"}
# STAGING §4 "autoscaling: PRODUCTION defaults" and "fleet scaling", stated
# explicitly so a binary default change cannot move the posture silently.
SCALE = {
    "SCALE_EVAL_SECS": "10", "SCALE_RATE_WINDOW_SECS": "120", "SCALE_HOT_PCT": "75",
    "SCALE_HOT_EVALS": "2", "SCALE_COOLDOWN_SECS": "600", "REBALANCE_LAG_SECS": "60",
    "REBALANCE_MOVE_COOLDOWN_SECS": "60", "REBALANCE_RETURN_SECS": "300",
    "SCALE_OUT_CPU_PCT": "75", "SCALE_IN_CPU_PCT": "50", "SCALE_CPU_SUSTAIN_SECS": "20",
    "SCALE_LATENCY_MS": "250", "SCALE_EDGE_SLOTS": "140", "SCALE_EDGE_LATENCY_MS": "1000",
    "SCALE_IN_SECS": "60",
}
# P-100 is the binary's default gap, the one write tier since 2026-10-07. P-exp
# pins the 10 ms gap the cells recorded as P-exp ran (the default until then).
WAL_POSTURES = {"P-100": {}, "P-exp": {"WAL_FLUSH_GAP_MS": "10"}, "P-500": {"WAL_FLUSH_GAP_MS": "500"},
                "P-1000": {"WAL_FLUSH_GAP_MS": "1000"}}
# Wrapper CPU samples every 15 awake seconds: a boot loses at most that much
# CPU after its last line (smoke sm1002f: 60 s left a 16 s-awake server none).
CPU_LOG_SECS = "15"


def topology(cfg: dict) -> dict:
    """Deployed ordinals, FLEET_MIN, FLEET_MAX and whether the cell is capped."""
    n = F.cell_int(cfg, "SERVERS", 1)
    lo = F.cell_int(cfg, "FLEET_MIN", min(2, n))
    if n < 1 or not 1 <= lo <= n:
        F.die(f"cell needs SERVERS >= 1 and 1 <= FLEET_MIN <= SERVERS (got {n}, {lo})")
    if "FLEET_MAX" in cfg:
        F.die("FLEET_MAX is not a cell key: it is SERVERS (every ordinal up to FLEET_MAX is deployed)")
    return {"servers": n, "fleet_min": lo, "fleet_max": max(2, n), "capped": n == 1}


# ---------------------------------------------------------------- env names

def known_names() -> dict:
    """Every env name each role's process reads, from the source tree."""
    def grep(paths, pats):
        out = set()
        for p in paths:
            text = open(p, encoding="utf-8").read()
            for pat in pats:
                out.update(re.findall(pat, text))
        return out
    cfg = [p for p in glob.glob(os.path.join(F.ROOT, "src", "config", "*.rs")) if "test" not in os.path.basename(p)]
    binary = grep(cfg, [r'env\s*=\s*"([A-Z][A-Z0-9_]+)"', r'\.get\(\s*"([A-Z][A-Z0-9_]+)"\s*\)',
                        r'env_parse(?:::<[^>]+>)?\(\s*env,\s*"([A-Z][A-Z0-9_]+)"', r'envf\(\s*"([A-Z][A-Z0-9_]+)"'])
    pilot = grep([os.path.join(F.ROOT, "src", "bin", "pilot.rs")] + glob.glob(os.path.join(F.ROOT, "src", "bin", "pilot", "*.rs")),
                 [r'(?:env|required)\("([A-Z][A-Z0-9_]+)"\)'])
    wrap = {}
    for app in ("app-server", "app-lb", "app-gen"):
        wrap[app] = grep(glob.glob(os.path.join(F.ROOT, "deploy", app, "*.ts")),
                         [r'process\.env\.([A-Z][A-Z0-9_]+)', r'\benv\.([A-Z][A-Z0-9_]+)'])
    return {"binary": binary, "pilot": pilot, "wrapper": wrap}


def check_env_names(role: str, env: dict, names: dict) -> list:
    if role == "server":
        allowed = names["binary"] | names["wrapper"]["app-server"]
    elif role == "router":
        allowed = names["pilot"] | names["wrapper"]["app-lb"] | {"MODE"}
    else:
        allowed = names["wrapper"]["app-gen"]
    return sorted(k for k in env if k not in allowed)


def profile_env() -> dict:
    out = {}
    with open(PROFILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            k, v = line.split("=", 1)
            out[k] = v
    return out


# ---------------------------------------------------------------- cell state

def cell_state(run: str, cell: str) -> dict:
    """Real-shaped identities, minted once per cell (§6), plus the
    customer project the generator's credentials belong to."""
    path = os.path.join(F.cell_dir(run, cell), "cell.json")
    st = F.read_json(path)
    if not st:
        st = {
            "run": run, "cell": cell,
            "account_id": F.rand_id("acct_"), "deploy_project_id": F.rand_id("proj_"),
            "cell_id": F.rand_id("cell_"), "customer_project_id": F.rand_id("proj_"),
            "workspace_id": F.rand_id("wksp_"), "credential_id": F.rand_id("strcred_"),
            "path_prefix": F.PATH_PREFIX, "fleet_prefix": F.FLEET_PREFIX, "created": F.utc(),
        }
        F.write_json(path, st)
    return st


def save_state(run: str, cell: str, st: dict) -> None:
    F.write_json(os.path.join(F.cell_dir(run, cell), "cell.json"), st)


def cell_secrets(run: str, cell: str) -> dict:
    tok = lambda: __import__("secrets").token_urlsafe(32)  # noqa: E731
    return {
        "AUTH_TOKEN": F.ensure_secret(run, cell, "auth-token", tok),
        "FLEET_INTERNAL_TOKEN": F.ensure_secret(run, cell, "fleet-internal-token", tok),
        "USAGE_STREAM_KEY": F.ensure_secret(run, cell, "usage-stream-key", F.b64key),
        "STREAMS_CURSOR_KEY": F.ensure_secret(run, cell, "cursor-key", F.b64key),
    }


def ensure_feeds(run: str, cell: str, st: dict) -> str:
    sdir = F.secrets_dir(run, cell)
    bundle = os.path.join(sdir, "feeds-bundle.json")
    if not os.path.exists(bundle):
        subprocess.run(["node", os.path.join(F.HERE, "feeds.mjs"), "init", "--dir", sdir,
                        "--cell-id", st["cell_id"], "--project", st["customer_project_id"],
                        "--workspace", st["workspace_id"], "--credential", st["credential_id"]], check=True)
    key = f"k2c/{run}/{cell}/feeds.json"
    F.record_artifact_object(run, cell, key)
    client, bucket = F.artifact_s3()
    F.put_verified(client, bucket, key, open(bundle, "rb").read())
    return key


# ---------------------------------------------------------------- env

def bin_s3_env() -> dict:
    return {"BIN_S3_ENDPOINT": F.field_file("artifact-endpoint.txt"), "BIN_S3_BUCKET": F.field_file("artifact-bucket.txt"),
            "BIN_S3_REGION": "auto", "BIN_S3_ACCESS_KEY_ID": F.field_file("binid.txt"),
            "BIN_S3_SECRET_ACCESS_KEY": F.field_file("binsec.txt")}


def server_env(run, cell, cfg, st, sec, bins, feeds_key, i) -> dict:
    k = F.bucket_keys(run, cell)
    topo = topology(cfg)
    env = dict(profile_env())
    env.update(ENGINE)
    env.update(LIMITS)
    env.update(SCALE)
    env.update(WAL_POSTURES[cfg.get("WAL_POSTURE", "P-100")])
    env.update({
        "SERVER_BINARY_S3_KEY": bins["streams"], **bin_s3_env(), "RESOLV_OVERRIDE": RESOLV,
        "SLATE_S3_ENDPOINT": k["endpoint"], "SLATE_S3_BUCKET": k["bucketName"], "SLATE_S3_REGION": "auto",
        "SLATE_S3_ACCESS_KEY_ID": k["accessKeyId"], "SLATE_S3_SECRET_ACCESS_KEY": k["secretAccessKey"],
        "PATH_PREFIX": st["path_prefix"], "FLEET_PREFIX": st["fleet_prefix"], "INSTANCE_NAME": f"streams-{i}",
        "FLEET_MIN": str(topo["fleet_min"]), "FLEET_MAX": str(topo["fleet_max"]), "FLEET_PEER_DOMAINS": "prisma.build",
        "INITIAL_SHARDS": cfg.get("INITIAL_SHARDS", "4"),
        "STREAMS_AUTH_MODE": "enforce", "STREAMS_AUTH_ISSUER": "https://auth.prisma.io",
        "STREAMS_AUTH_KEYS_FILE": f"{FEEDS_DIR}/keys.json", "STREAMS_AUTH_POLICY_FILE": f"{FEEDS_DIR}/policies.json",
        "STREAMS_AUTH_GRANTS_FILE": f"{FEEDS_DIR}/grants.json", "STREAMS_AUTH_REFRESH_SECS": "60",
        "FEEDS_S3_KEY": feeds_key, "FLEET_AUTH_MODE": "static",
        "BILLING_MODE": "required", "ACCOUNT_ID": st["account_id"], "PROJECT_ID": st["deploy_project_id"],
        "CELL_ID": st["cell_id"], "REGION": F.REGION, "CPU_LOG_SECS": CPU_LOG_SECS, **sec,
    })
    if i == 1:
        env["ROLLUP"] = "1"
    if cfg.get("KEEP_AWAKE") == "1" and i <= topo["fleet_min"]:  # never a spare
        env["KEEP_AWAKE"] = "1"
    for item in cfg.get("SERVER_ENV_EXTRA", "").split():
        if item.startswith("-"):
            env.pop(item[1:], None)
        else:
            kk, vv = item.split("=", 1)
            env[kk] = vv
    return env


def router_env(run, cell, cfg, st, bins, j, upstreams) -> dict:
    k = F.bucket_keys(run, cell)
    env = {
        "LB_BINARY_S3_KEY": bins["pilot"], **bin_s3_env(), "RESOLV_OVERRIDE": RESOLV,
        "S3_ENDPOINT": k["endpoint"], "S3_BUCKET": k["bucketName"], "S3_REGION": "auto",
        "S3_ACCESS_KEY_ID": k["accessKeyId"], "S3_SECRET_ACCESS_KEY": k["secretAccessKey"],
        "FLEET_PREFIX": st["fleet_prefix"], "DATA_PREFIX": st["path_prefix"], "PILOT_MODE": "lb",
        "ROUTER_NAME": f"router-{j}", "UPSTREAMS": ",".join(upstreams), "CPU_LOG_SECS": CPU_LOG_SECS,
    }
    if cfg.get("KEEP_AWAKE") == "1":
        env["KEEP_AWAKE"] = "1"
    return env


# ---------------------------------------------------------------- deploy

def project_env(project: str) -> tuple:
    """(the project's own production variables, its system-managed names).
    A row of another project, or of none, refuses the deploy before
    anything is sent (fieldlib.project_env_rows): dropping it would protect
    only this list, not the CLI's own per-key writes."""
    rows = F.project_env_rows(project)
    return ({r["key"] for r in rows if not r.get("isManagedBySystem")},
            {r["key"] for r in rows if r.get("isManagedBySystem")})


def probe_env_keys(project: str, keys) -> None:
    """Campaign mode, under the deploy's CLI lock: the exact lookup the
    compute CLI makes for every variable it sets or unsets (GET
    /environment-variables?projectId=<campaign>&class=production&key=K, then
    PATCH/DELETE existing[0] by id, or POST) must return at most one row,
    of key K and the campaign project. Those writes are outside gate_api;
    this probe is their only guard."""
    def rows(k: str) -> tuple:
        return k, F.api_list(f"/environment-variables?projectId={project}&class=production"
                             f"&key={urllib.parse.quote(k)}")
    with cf.ThreadPoolExecutor(4) as ex:
        got = list(ex.map(rows, sorted(keys)))
    bad = {k: [(r.get("key"), r.get("projectId")) for r in rs] for k, rs in got
           if len(rs) > 1 or any(r.get("key") != k or r.get("projectId") != project for r in rs)}
    if bad:
        raise F.ScopeError(f"the CLI's per-key env lookups in {project} would address other rows: "
                           f"{dict(list(bad.items())[:5])}: refusing the deploy")


def snapshot_extra(version: str, env: dict, system: set) -> list:
    """Names in a version's env snapshot (`compute versions show`) that its
    deploy did not state, system-managed names aside. An inherited variable
    (RUNBOOK §7.3; KEEP_AWAKE on a spare or an f1b server bills until
    teardown while the ledger says keep_awake false) stops the deploy here,
    before its health gate. Values are never read out of the answer."""
    err = ""
    for _ in range(3):
        try:
            shown = (F.cli_json(["versions", "show", version]).get("data") or {}).get("envVars")
            if not isinstance(shown, dict) or not shown:
                raise RuntimeError("the answer holds no envVars")
            return sorted(set(shown) - set(env) - set(system))
        except F.ScopeError:
            raise
        except Exception as e:  # noqa: BLE001 - retried, then fatal: an unread snapshot is not exact
            err = str(e)[:300]
            time.sleep(5)
    F.die(f"version {version}: env snapshot unreadable ({err})")
    return []


def deploy_target(project: str, svc: str, sid: str | None) -> str | None:
    """Campaign mode: the service id to redeploy, verified in the campaign
    project (live project, live name), or None for a new service. The name
    is first looked up in the campaign project, so a lost deploy answer
    never makes a second service of that name; a ledger id that is gone
    (404 and not listed) is replaced by a new service of the same name.
    Per-cell mode: `sid` unchanged."""
    if not F.campaign():
        return sid
    found = [s for s in F.services_in(project) if s.get("name") == svc]
    if len(found) > 1 or (sid and found and found[0].get("id") != sid):
        F.die(f"{svc}: live {[s.get('id') for s in found]} in {project} is not the ledger's {sid}: resolve by hand")
    sid = sid or (found[0].get("id") if found else None)
    if not sid:
        return None
    state, why = F.campaign_owns("service", sid, name=svc)
    if state == "gone" and not found:
        return None
    if state != "live":
        F.die(f"refusing to deploy {svc} over service {sid}: {why or 'GET answered 404 but the project lists it'}")
    return sid


def write_env_file(path: str, env: dict) -> None:
    lines = []
    for k2, v in sorted(env.items()):
        if "'" in v or "\n" in v or "\r" in v:
            F.die(f"env value of {k2} cannot be written to an env file (quote or newline)")
        lines.append(f"{k2}='{v}'")
    F.write_secret(path, "\n".join(lines) + "\n")


def deploy_service(run, cell, project, svc, role, app, env, keep_awake_ttl_min=None) -> dict:
    """One CLI deploy with the service's whole env. Records the service in
    resources.json before the call (pending) and its ids after. With
    KEEP_AWAKE=1 the ledger expiry is also put in the env as
    KEEP_AWAKE_UNTIL_MS, which the wrapper's guard enforces."""
    if env.get("KEEP_AWAKE") == "1":
        env["KEEP_AWAKE_UNTIL_MS"] = str(F.now_ms() + int(keep_awake_ttl_min or 30) * 60_000)
    with F.resources(run) as doc:
        rec = F.cell_res(doc, cell)["services"].setdefault(svc, {"name": svc, "role": role, "status": "pending",
                                                                  "created": F.utc()})
        sid = rec.get("id")
        if env.get("KEEP_AWAKE") == "1":
            exp = int(env["KEEP_AWAKE_UNTIL_MS"])
            rec["keep_awake"] = True
            rec["keep_awake_expires"] = F.utc(exp)
            ka = F.cell_res(doc, cell)["keep_awake"]
            ka[:] = [x for x in ka if x["service"] != svc] + [{"service": svc, "expires": F.utc(exp)}]
        else:
            rec["keep_awake"] = False
            rec.pop("keep_awake_expires", None)
    tmp = os.path.join(F.cell_dir(run, cell), "tmp")
    os.makedirs(tmp, mode=0o700, exist_ok=True)
    envfile = os.path.join(tmp, f"{svc}.env")
    write_env_file(envfile, env)
    t0 = time.time()
    data, err, unset, system = None, "", [], set()
    try:
        # Under the CLI lock from the variable listing to the deploy: no
        # other field tool's deploy (another cell in the same campaign
        # project) can set a variable in between, so `unset` is exactly the
        # project's variables this service's env does not restate.
        with F.cli_lock():
            for attempt in range(1, 5):
                sid = deploy_target(project, svc, sid)
                own, system = project_env(project)
                unset = sorted(own - set(env))
                if F.campaign():
                    probe_env_keys(project, set(env) | set(unset))
                args = ["deploy", "--project", project, "--path", os.path.join(APPS, app), "--http-port", "8080",
                        "--env", envfile, "--timeout", "300"]
                # A redeploy stops and deletes the version it replaces: a version
                # left running keeps billing (and, with KEEP_AWAKE, never sleeps).
                args += ["--service", sid, "--destroy-old-version"] if sid else ["--service-name", svc, "--region", F.REGION]
                for u in unset:
                    args += ["--unset-env", u]
                try:
                    data = F.cli_json(args, timeout=900)["data"]
                    break
                except F.ScopeError:
                    raise
                except Exception as e:  # noqa: BLE001 - retried, then fatal
                    err = str(e)
                    F.say(f"    deploy attempt {attempt} for {svc} failed: {err[:300]}")
                    found = [s for s in F.services_in(project) if s.get("name") == svc]
                    if found:
                        sid = found[0]["id"]
                        with F.resources(run) as doc:
                            F.cell_res(doc, cell)["services"][svc]["id"] = sid
                    time.sleep(20)
    finally:
        os.remove(envfile)
    if not data:
        F.die(f"deploy of {svc} failed after 4 attempts: {err[:400]} "
              f"(remove what this cell deployed: teardown.sh {run} {cell})")
    url = data.get("deploymentEndpointDomain") or ""
    url = url if url.startswith("http") else f"https://{url}"
    svc_url = data.get("appEndpointDomain") or ""
    with F.resources(run) as doc:
        rec = F.cell_res(doc, cell)["services"][svc]
        rec.update({"id": data["appId"], "status": "deployed", "version": data["deploymentId"],
                    "preview_url": url, "service_url": (svc_url if svc_url.startswith("http") or not svc_url
                                                        else f"https://{svc_url}"),
                    "deployed": F.utc(), "deploy_secs": round(time.time() - t0, 1)})
        rec.setdefault("versions", []).append(data["deploymentId"])
    extra = snapshot_extra(data["deploymentId"], env, system)
    if extra:
        with F.resources(run) as doc:
            F.cell_res(doc, cell)["services"][svc]["platform_env_extra"] = extra
        F.die(f"{svc}: version {data['deploymentId']} holds variables this deploy did not state: {extra} "
              f"(inherited from the project; remove what this cell deployed: teardown.sh {run} {cell})")
    F.say(f"    {svc}: version {data['deploymentId']} at {url} ({time.time() - t0:.0f}s, unset {len(unset)})")
    return {"id": data["appId"], "version": data["deploymentId"], "url": url, "unset": unset,
            "service_url": rec["service_url"], "deploy_secs": round(time.time() - t0, 1),
            "deploy_started_ms": int(t0 * 1000)}


def upstreams_of(body: bytes):
    try:
        return json.loads(body).get("upstreams")
    except ValueError:
        return None


def gate_http(url: str, path: str, ok, secs: float = 300) -> dict:
    t0, last = time.time(), ""
    while time.time() - t0 < secs:
        status, body, _ = F.http_get(url + path, timeout=15)
        if status == 200 and ok(body):
            return {"path": path, "secs": round(time.time() - t0, 1)}
        last = f"{status} {body[:200]!r}"
        time.sleep(5)
    F.die(f"{url}{path} not healthy after {secs}s: {last} (teardown.sh removes what was deployed)")
    return {}


def instance_shape(version: str) -> dict:
    lines = F.compute_logs(version, 45, stop_when=lambda ls: any("instance shape:" in l for l in ls)
                           and any("binary sha256" in l for l in ls))
    if not any("binary sha256" in l for l in lines):  # a slow first log: one more read
        lines = F.compute_logs(version, 45, stop_when=lambda ls: any("binary sha256" in l for l in ls))
    out: dict = {}
    for l in lines:
        if "instance shape:" in l and "shape" not in out:
            try:
                out["shape"] = json.loads(l.split("instance shape:", 1)[1].strip())
            except ValueError:
                pass
        m = re.search(r"binary sha256 ([0-9a-f]{64})", l)
        if m:
            out["binary_sha256"] = m.group(1)
    sh = out.get("shape") or {}
    cg = sh.get("cgroup_memory_max")
    mem = int(cg) if cg and str(cg).isdigit() else sh.get("mem_total_bytes")
    out["memory_gib"] = round(mem / 2 ** 30, 3) if mem else None
    out["memory_basis"] = "cgroup memory.max" if cg and str(cg).isdigit() else ("os.totalmem" if mem else None)
    # The kernel reports the VM's memory less its own reservations (a 1 GiB
    # instance shows ~0.955 GiB): price the provisioned class, the smallest
    # standard size at or above what the kernel sees.
    out["memory_gib_class"] = next((c for c in (0.25, 0.5, 1, 2, 4, 8, 16, 32) if mem and mem <= c * 2 ** 30), None)
    return out


def get_doc(s3, bucket, key):
    try:
        o = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(o["Body"].read()), o
    except Exception:  # noqa: BLE001 - absent or unreadable is "no document yet"
        return None, None


def ring_gate(run, cell, st, n, urls: dict, keep_awake: bool, timeout=900) -> dict:
    """RUNBOOK §7.4b read from the bucket. Every server must have beaten
    (seq advancing) since its deploy, and the desired count must hold for
    60 s with no server rebooting. With KEEP_AWAKE the ring itself (the
    first `desired` beating ordinals) must also hold unchanged for those
    60 s. Without it an idle server sleeps within a minute or two (smoke
    sm1002c), so liveness cannot hold still; a server that has not beaten
    yet gets the routers' wake ping, GET /health, as at deploy time."""
    s3, bucket = F.data_s3(run, cell)
    fp = st["fleet_prefix"]
    t0 = time.time()
    seqs: dict = {}
    seen: set = set()
    still: dict = {}
    sig_prev, stable_since, reads, pings = None, None, 0, 0
    while time.time() - t0 < timeout:
        beating, boots = [], []
        for i in range(1, n + 1):
            hb, _ = get_doc(s3, bucket, f"{fp}/fleet/streams-{i}.json")
            reads += 1
            key = (hb.get("boot_id"), hb.get("seq")) if hb else None
            boots.append(key[0] if key else None)
            if hb and i in seqs and key != seqs[i] and not hb.get("draining") and not hb.get("withdrawn"):
                beating.append(i)
                seen.add(i)
            if i not in seen:
                still[i] = still.get(i, 0) + 1
                if still[i] % 4 == 3:
                    F.http_get(urls[f"streams-{i}"] + "/health", timeout=20)
                    pings += 1
            seqs[i] = key
        desired, _ = get_doc(s3, bucket, f"{fp}/fleet/desired.json")
        reads += 1
        count = int((desired or {}).get("count") or 0)
        ring = tuple(beating[:max(1, count)])
        sig = (count, tuple(boots), ring if keep_awake else None)
        ready = len(seen) == n and count >= 1 and (not keep_awake or len(ring) == min(count, n))
        if ready and sig == sig_prev:
            stable_since = stable_since or time.time()
            if time.time() - stable_since >= 60:
                return {"desired": count, "ring": [f"streams-{i}" for i in ring], "beating_at_end": len(beating),
                        "keep_awake": keep_awake, "secs": round(time.time() - t0, 1), "bucket_reads": reads,
                        "wake_pings": pings}
        else:
            stable_since = None
        sig_prev = sig
        time.sleep(5)
    F.die(f"ring not stable after {timeout}s (seen {sorted(seen)}, last {sig_prev}) "
          f"(teardown.sh {run} {cell} removes what was deployed)")
    return {}


def publish_urls(run, cell, st, urls: dict) -> None:
    s3, bucket = F.data_s3(run, cell)
    s3.put_object(Bucket=bucket, Key=f"{st['fleet_prefix']}/fleet/urls.json", Body=json.dumps(urls).encode())


def check_identity(report: dict, bins: dict, manifest: dict) -> list:
    """Every instance's logged binary sha256 against bins.json (§6)."""
    bad = []
    for name, d in report["instances"].items():
        key = bins["pilot"] if name.startswith("router-") else bins["k2gen"] if name.startswith("gen") else bins["streams"]
        want = (manifest.get(key) or {}).get("sha256")
        d["binary_expected"] = {"key": key, "sha256": want}
        d["binary_ok"] = bool(want) and d.get("binary_sha256") == want
        if not d["binary_ok"]:
            bad.append(f"{name}: logged {d.get('binary_sha256') or 'no sha256 line'}, bins.json {key} = {want}")
    return bad


def shapes(report: dict) -> None:
    for name, d in report["instances"].items():
        d.update(instance_shape(d["version"]))
        try:
            shown = F.cli_json(["versions", "show", d["version"]]).get("data") or {}
            d["platform_env_names"] = sorted((shown.get("envVars") or {}).keys())
            # A name in the version's snapshot that this deploy did not state:
            # deploy_service already stopped on any but a system-managed one;
            # recorded here for the report.
            stated = d.get("env") or report.get("env") or {}
            d["platform_env_extra"] = sorted(set(d["platform_env_names"]) - set(stated))
            if d["platform_env_extra"]:
                F.say(f"    WARNING {name}: version env has names this deploy did not state: {d['platform_env_extra']}")
        except Exception as e:  # noqa: BLE001 - informational
            d["platform_env_names_error"] = str(e)[:200]
        F.say(f"    {name}: memory class {d.get('memory_gib_class')} GiB (kernel {d.get('memory_gib')} GiB, "
              f"{d.get('memory_basis')}), shape {d.get('shape')}, sha256 {str(d.get('binary_sha256'))[:16]}")


def record_instances(run: str, cell: str, report: dict) -> None:
    with F.resources(run) as doc:
        for name, d in report["instances"].items():
            for rec in F.cell_res(doc, cell)["services"].values():
                if rec.get("version") == d["version"]:
                    rec["memory_gib"] = d.get("memory_gib_class")
                    rec["memory_gib_kernel"] = d.get("memory_gib")
                    rec["instance"] = name
                    rec["binary_ok"] = d.get("binary_ok")


def finish(run: str, cell: str, report: dict, bins: dict, out_name: str) -> None:
    """Shapes and identity of the instances just deployed, the ledger, the
    report; a binary of another build fails the deploy (after the report)."""
    shapes(report)
    manifest = F.read_json(os.path.join(F.FIELD, "bins.json")) or {}
    bad = check_identity(report, bins, manifest)
    report["binary_identity"] = "ok" if not bad else bad
    record_instances(run, cell, report)
    report["finished"] = F.utc()
    F.write_json(os.path.join(F.results_dir(run, cell), out_name), report)
    if bad:
        F.die(f"binary identity check failed: {bad} (teardown.sh {run} {cell} removes this cell)")


def context(run: str, cell: str) -> tuple:
    F.check_names(run, cell)
    cfg = F.read_cell_file(cell)
    if cfg.get("WAL_POSTURE", "P-100") not in WAL_POSTURES:
        F.die(f"WAL_POSTURE must be one of {sorted(WAL_POSTURES)}")
    topology(cfg)
    res = F.load_resources(run)["cells"].get(cell) or {}
    project = F.run_project(run, cell)  # campaign mode: the campaign project, live-checked
    if not project or not (res.get("bucket") or {}).get("id"):
        F.die(f"cell {cell} is not provisioned in run {run}: run provision.py first")
    art = F.artifact_receipt()
    if (project == art["projectId"] and not F.campaign()) or res["bucket"]["id"] == art["bucketId"]:
        F.die("refusing to deploy into the artifact project or bucket")
    bins = (F.read_json(os.path.join(F.FIELD, "bins.json")) or {}).get("_latest")
    if not bins:
        F.die("no uploaded binaries: run bins.py first")
    return cfg, project, bins


def stage(*apps: str) -> None:
    with F.cli_lock():  # `bun install` shares bunx's package cache
        for app in apps:
            subprocess.run([STAGE, app, os.path.join(APPS, app)], check=True)


def deploy_all(run: str, cell: str) -> None:
    cfg, project, bins = context(run, cell)
    topo = topology(cfg)
    n, m = topo["servers"], F.cell_int(cfg, "ROUTERS", 0)
    st = cell_state(run, cell)
    sec = cell_secrets(run, cell)
    names = known_names()
    ttl = cfg.get("KEEP_AWAKE_TTL_MIN", "30")
    # Check every env before anything deploys (routers with placeholder upstreams).
    envs = {i: server_env(run, cell, cfg, st, sec, bins, "k2c/x", i) for i in range(1, n + 1)}
    envs_r = {j: router_env(run, cell, cfg, st, bins, j, ["https://x"]) for j in range(1, m + 1)}
    for e in list(envs.values()) + list(envs_r.values()):
        if e.get("KEEP_AWAKE") == "1":
            e["KEEP_AWAKE_UNTIL_MS"] = "0"  # what deploy_service adds
    bad = {f"streams-{i}": check_env_names("server", e, names) for i, e in envs.items()}
    bad.update({f"router-{j}": check_env_names("router", e, names) for j, e in envs_r.items()})
    bad = {k: v for k, v in bad.items() if v}
    if bad:
        F.die(f"env names no process reads: {bad}")
    F.say(f"== {cell}: {n} server ordinal(s) (FLEET_MIN {topo['fleet_min']}, FLEET_MAX {topo['fleet_max']}"
          f"{', capped' if topo['capped'] else ''}), {m} router(s), KEEP_AWAKE={cfg.get('KEEP_AWAKE', '0')}, "
          f"SCRAPE={cfg.get('SCRAPE', '0')}, {cfg.get('WAL_POSTURE', 'P-100')}; env names verified")
    stage(*(["app-server"] + (["app-lb"] if m else [])))
    feeds_key = ensure_feeds(run, cell, st)
    report = {"run": run, "cell": cell, "cell_file": cfg, "topology": topo, "state": st, "binaries": bins,
              "env_names_checked": True, "instances": {}, "started": F.utc()}
    base = F.base_name(run, cell)
    urls, svc_urls = {}, {}
    for i in range(1, n + 1):
        env = server_env(run, cell, cfg, st, sec, bins, feeds_key, i)
        d = deploy_service(run, cell, project, f"{base}-s{i}", "server", "app-server", env, ttl)
        d["health"] = gate_http(d["url"], "/health", lambda b: True)
        urls[f"streams-{i}"], svc_urls[f"streams-{i}"] = d["url"], d["service_url"]
        report["instances"][f"streams-{i}"] = {**d, "env": F.redact(env)}
    publish_urls(run, cell, st, urls)
    st.update({"server_urls": urls, "server_service_urls": svc_urls, "topology": topo})
    save_state(run, cell, st)
    report["ring"] = ring_gate(run, cell, st, n, urls, cfg.get("KEEP_AWAKE") == "1")
    F.say(f"    ring stable: {report['ring']}")
    for j in range(1, m + 1):
        env = router_env(run, cell, cfg, st, bins, j, [urls[f"streams-{i}"] for i in range(1, n + 1)])
        d = deploy_service(run, cell, project, f"{base}-r{j}", "router", "app-lb", env, ttl)
        d["stats"] = gate_http(d["url"], "/stats", lambda b: upstreams_of(b) == n)
        st.setdefault("router_urls", {})[f"router-{j}"] = d["url"]
        st.setdefault("router_service_urls", {})[f"router-{j}"] = d["service_url"]
        report["instances"][f"router-{j}"] = {**d, "env": F.redact(env)}
    save_state(run, cell, st)
    finish(run, cell, report, bins, "deploy.json")
    F.say(f"== {cell} deployed: servers {urls}, routers {st.get('router_urls', {})}")


def ledger_server(run: str, cell: str, i: int) -> dict:
    svc = f"{F.base_name(run, cell)}-s{i}"
    rec = ((F.load_resources(run)["cells"].get(cell) or {}).get("services") or {}).get(svc)
    if not rec or rec.get("status") == "deleted" or not rec.get("id"):
        F.die(f"{svc} is not a deployed service of run {run}")
    return rec


def kill(run: str, cell: str, i: int) -> None:
    """Stop ordinal i's running version: an instance kill (design §9.2 F2)."""
    _, project, _ = context(run, cell)
    rec = ledger_server(run, cell, i)
    live = [s for s in F.services_in(project) if s.get("name") == rec["name"]]
    if not live or live[0].get("id") != rec["id"] or rec["id"] in F.foreign_ids(run):
        F.die(f"{rec['name']}: the live service does not match the ledger ({live[:1]})")
    if F.campaign():  # the service, then its version, live in the campaign project
        for kind, rid, kw in (("service", rec["id"], {"name": rec["name"]}),
                              ("deployment", rec["version"], {"service_id": rec["id"]})):
            state, why = F.campaign_owns(kind, rid, **kw)
            if state != "live":
                F.die(f"--kill {i}: {why or kind + ' ' + rid + ' is gone (404)'}")
    t0 = F.utc()
    p = F.cli(["versions", "stop", rec["version"]], timeout=600)
    out = {"service": rec["name"], "version": rec["version"], "requested": t0, "stopped": F.utc(),
           "rc": p.returncode, "tail": (p.stderr or p.stdout)[-300:]}
    with F.resources(run) as doc:
        F.cell_res(doc, cell)["services"][rec["name"]].setdefault("kills", []).append(out)
    F.write_json(os.path.join(F.results_dir(run, cell), f"kill-s{i}-{t0.replace(':', '')}.json"), out)
    F.say(f"  {rec['name']}: versions stop {rec['version']} rc={p.returncode} at {out['stopped']}")
    if p.returncode != 0:
        F.die(f"versions stop failed: {out['tail']}")


def only(run: str, cell: str, i: int) -> None:
    """Deploy ordinal i alone (a revive after --kill, or one step of a
    rolling deploy) and republish urls.json for the routers to adopt."""
    cfg, project, bins = context(run, cell)
    topo = topology(cfg)
    if not 1 <= i <= topo["servers"]:
        F.die(f"--only {i}: the cell has ordinals 1..{topo['servers']}")
    st = F.read_json(os.path.join(F.cell_dir(run, cell), "cell.json"))
    if not st or not st.get("server_urls"):
        F.die(f"cell {cell} was never deployed: run deploy-cell.sh {run} {cell} first")
    rec = ledger_server(run, cell, i)
    kills = rec.get("kills") or []
    stage("app-server")
    feeds_key = f"k2c/{run}/{cell}/feeds.json"
    env = server_env(run, cell, cfg, st, cell_secrets(run, cell), bins, feeds_key, i)
    bad = check_env_names("server", env, known_names())
    if bad:
        F.die(f"env names no process reads: {bad}")
    report = {"run": run, "cell": cell, "only": i, "instances": {}, "started": F.utc(), "kill": kills[-1] if kills else None}
    s3, bucket = F.data_s3(run, cell)
    hb_key = f"{st['fleet_prefix']}/fleet/streams-{i}.json"
    old_boot = ((get_doc(s3, bucket, hb_key)[0]) or {}).get("boot_id")
    d = deploy_service(run, cell, project, rec["name"], "server", "app-server", env, cfg.get("KEEP_AWAKE_TTL_MIN", "30"))
    d["health"] = gate_http(d["url"], "/health", lambda b: True)
    st["server_urls"][f"streams-{i}"] = d["url"]
    st.setdefault("server_service_urls", {})[f"streams-{i}"] = d["service_url"]
    publish_urls(run, cell, st, st["server_urls"])
    save_state(run, cell, st)
    hb, t_end = None, time.time() + 60
    while time.time() < t_end:  # the revived process's first beat (a new boot_id)
        hb, _ = get_doc(s3, bucket, hb_key)
        if hb and hb.get("boot_id") != old_boot:
            break
        time.sleep(2)
    report["revived"] = {"boot_id": (hb or {}).get("boot_id"), "previous_boot_id": old_boot,
                         "new_boot": bool(hb) and hb.get("boot_id") != old_boot, "seq": (hb or {}).get("seq"),
                         "first_beat_ts_ms": (hb or {}).get("ts_ms"), "utc": F.utc()}
    if kills:
        stopped = calendar.timegm(time.strptime(kills[-1]["stopped"], "%Y-%m-%dT%H:%M:%SZ"))
        report["gap_s"] = round(time.time() - stopped, 1)
    report["instances"][f"streams-{i}"] = {**d, "env": F.redact(env)}
    finish(run, cell, report, bins, f"deploy-only-s{i}-{report['started'].replace(':', '')}.json")
    F.say(f"== {cell}: streams-{i} redeployed at {d['url']}; urls.json republished; revived {report['revived']}"
          f"{'; gap since kill ' + str(report.get('gap_s')) + ' s' if kills else ''}")


def main() -> None:
    F.banner("deploy-cell")
    a = sys.argv[1:]
    if len(a) == 2:
        deploy_all(a[0], a[1])
    elif len(a) == 4 and a[2] in ("--kill", "--only") and a[3].isdigit():
        (kill if a[2] == "--kill" else only)(a[0], a[1], int(a[3]))
    else:
        F.die("usage: deploy-cell.sh <run-id> <cell> [--kill N | --only N]", 2)


if __name__ == "__main__":
    main()
