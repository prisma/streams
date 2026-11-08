"""rig.py: what footprint.py, ingest.py and ramp.py share.

The compose project (compose.yml), the servers' posture, the tenants a
posture enforces, and the bench runs from a container on the rig's network.

Postures:
- `off`: compute-1g.env with auth, billing and the rollup off.
- `on`: compute-1g.env plus a cell profile (dedicated-cell.env for one
  tenant, shared-cell.env for a tenant each): auth enforced from cell-admin
  feed files (tenants.sh), billing required, the rollup on, the debug routes
  behind a bearer. The secrets it needs (usage key, debug bearer) are made
  per run into the run's directory (mode 0600), never into the repository.
"""
import base64
import json
import os
import secrets
import subprocess
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
PROJECT = "streams-ramp"
NETWORK = f"{PROJECT}_default"
TENANTS_CACHE = os.path.expanduser("~/.streams-k2/ramp-tenants")
# The raw and product surfaces encrypt every stream under its caller's key.
KEY = "-"
while KEY.startswith("-"):  # the bench's argument parser reads a leading dash as a flag
    KEY = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()


def sh(*cmd, env=None, check=True, timeout=None):
    """CMD's output; past TIMEOUT seconds it is killed and the run stops (a leg never hangs)."""
    try:
        r = subprocess.run(cmd, cwd=HERE, env=env, text=True, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except subprocess.TimeoutExpired:
        raise SystemExit(f"{' '.join(cmd[:4])} ... ran past its {timeout} s bound")
    if check and r.returncode != 0:
        raise SystemExit(f"{' '.join(cmd)} failed ({r.returncode}): {r.stdout[-800:]}")
    return r.stdout


def compose(*args, env):
    return sh("docker", "compose", "-p", PROJECT, "-f", "compose.yml", *args, env=env)


def server(k=1):
    return f"{PROJECT}-streams-{k}-1"


# The profile's per-instance memory budgets, scaled with the instance's RAM
# (GB / 1): the shed line, the caches, the bulk budget, the unabsorbed and
# project-pressure lines, the SSE connection cap. The feed budgets stay: the
# release posture refuses a feed above the 64 MiB a campaign certified, on every
# profile (config::validation::profile_feed_budget_max). Per-database
# knobs (memtable, L0 size, the per-shard unabsorbed line) and the CPU knobs
# (SLATEDB_RT_THREADS, the compactor's concurrency) stay; tokio sizes its
# workers from the container's CPU quota. PER_SHARD holds the two budget lines
# set per shard (clean handles, tail ring): with the shard count known they
# scale with GB per shard, never below the profile's, so the instance's lines
# keep their share of the shed line; with it unknown (the footprint leg, which
# measures what a shard costs) they stay the profile's. Off the profile's values
# the server must not claim compute-1g's certificate: MEMPROFILE_CERT=compute-1g
# refuses to start on any other bulk or compactor setting, so a scaled or
# overridden posture sets OFF_CERT, which names no certificate.
OFF_CERT = "rig-uncertified"
SCALED = {
    "ADMIT_RSS_SHED_MB": 500, "SHARED_CACHE_BYTES": None, "TELEMETRY_CACHE_BYTES": None,
    "STORE_BULK_INFLIGHT_MAX_BYTES": None,
    "MAX_UNABSORBED_BYTES_PER_INSTANCE": None, "PROJECT_MEMORY_PRESSURE_BYTES": None, "SSE_MAX_CONNECTIONS": None,
}
PER_SHARD = ("HANDLE_MAX_RESIDENT", "TAIL_RING_BYTES")


def parse_size(spec):
    """'VCPU/GB' -> (vcpus, gb), e.g. '4/8'."""
    cpus, gb = spec.split("/")
    return float(cpus), float(gb)


def profile_lines(names):
    """KEY=VALUE lines of deploy/profiles/NAMES, later files winning."""
    out = {}
    for name in names:
        for ln in open(os.path.join(ROOT, "deploy", "profiles", name)).read().splitlines():
            if ln[:1].isupper() and "=" in ln:
                k, v = ln.split("=", 1)
                out[k] = v
    return out


def scaled_knobs(base, gb, shards=None):
    """The SCALED knobs at GB, from BASE (the profiles' values), and with
    SHARDS the PER_SHARD lines at GB per shard."""
    out = {}
    for k, default in SCALED.items():
        v = base.get(k, default)
        if v is None:
            continue
        out[k] = str(int(int(v) * gb))
    if shards:
        for k in PER_SHARD:
            if k in base:
                out[k] = str(int(int(base[k]) * max(1.0, gb / shards)))
    if gb != 1:
        out["MEMPROFILE_CERT"] = OFF_CERT
    return out


def tenants(count, cache=TENANTS_CACHE, max_age_secs=6 * 3600):
    """cell-admin projects for COUNT tenants (tenants.sh): the admitted cell is
    cached, its 12 h tokens and workload JWT re-minted once older than MAX_AGE."""
    d = os.path.join(cache, str(count))
    tokens = os.path.join(d, "tokens.txt")
    if not os.path.exists(tokens) or time.time() - os.path.getmtime(tokens) > max_age_secs:
        sh(os.path.join(HERE, "tenants.sh"), str(count), d)
    return d


class HostStore:
    """s3lite on the host for a cell, at the compose store's port and latency.

    s3lite holds every object in memory and the container VM has 16 GiB, so a
    write ladder's store gets the VM's OOM killer (a 13 GB ladder lost it twice
    on 2026-11-06) and swaps before that. On the host it has room; a driver
    bounds it by reading live_gb."""

    ENV = {"S3_ENDPOINT": "http://host.docker.internal:8299"}

    def __init__(self, latency_ms=20):
        self.proc = subprocess.Popen(
            [os.path.join(HERE, "bin", "s3lite-host"), "--listen", "127.0.0.1:8299", "--latency-ms", str(latency_ms)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            try:
                self.live_gb()
                return
            except OSError:
                time.sleep(0.2)
        self.stop()
        raise SystemExit("host s3lite did not answer on 8299")

    @staticmethod
    def live_gb():
        with urllib.request.urlopen("http://127.0.0.1:8299/_s3lite/stats2", timeout=5) as r:
            cells = (json.loads(r.read()).get("live_bytes") or {}).get("cells") or {}
        return sum(c.get("bytes", 0) for c in cells.values()) / 1e9

    def stop(self):
        self.proc.kill()
        self.proc.wait()


class Posture:
    """The servers' environment for one run: env file, feeds, bearer, tokens.

    FLEET: more than one server, so the peers need an identity: the static
    token with auth off, the workload JWT (tenants.sh) with auth on (the
    release posture refuses the static token)."""

    def __init__(self, name, out_dir, tenant_count=1, cache=TENANTS_CACHE, fleet=False, size="1/1", shards=None,
                 overrides=None):
        self.name = name
        self.cpus, self.gb = parse_size(size)
        self.bearer = None
        self.tokens = None
        self.feeds = os.path.join(HERE, "bin")
        if name == "off":
            base = profile_lines(["compute-1g.env"])
            lines = ["STREAMS_AUTH_MODE=off", "ADMIT_MAX_INFLIGHT=512"]
            if fleet:
                lines += [f"FLEET_INTERNAL_TOKEN={secrets.token_hex(24)}", "FLEET_ALLOW_HTTP_PEERS=1"]
        else:
            tdir = tenants(tenant_count, cache)
            self.tokens = os.path.join(tdir, "tokens.txt")
            self.feeds = os.path.join(tdir, "feeds")
            profile = "dedicated-cell.env" if tenant_count == 1 else "shared-cell.env"
            base = profile_lines(["compute-1g.env", profile])
            lines = [f"{k}={v}" for k, v in base.items() if k not in SCALED and not (shards and k in PER_SHARD)]
            self.bearer = secrets.token_hex(24)
            lines += [
                "STREAMS_AUTH_MODE=enforce", "STREAMS_AUTH_ISSUER=https://auth.prisma.io",
                "STREAMS_AUTH_KEYS_FILE=/feeds/keys.json", "STREAMS_AUTH_POLICY_FILE=/feeds/policies.json",
                "STREAMS_AUTH_GRANTS_FILE=/feeds/grants.json",
                # the cell's identity as tenants.sh admitted it
                "PROJECT_ID=proj_ramp_deploy", "CELL_ID=ramp-cell",
                f"USAGE_STREAM_KEY={base64.b64encode(os.urandom(32)).decode()}",
                f"AUTH_TOKEN={self.bearer}",
            ]
            if fleet:
                lines += ["FLEET_AUTH_MODE=workload", "WORKLOAD_TOKEN_FILE=/feeds/workload.jwt",
                          "FLEET_ALLOW_HTTP_PEERS=1"]
        # The instance size's memory budgets (recorded beside the env file).
        # OVERRIDES: KEY=VALUE settings on top, for a run that varies a knob.
        self.knobs = scaled_knobs(base, self.gb, shards)
        if overrides:
            self.knobs.update(dict(o.split("=", 1) for o in overrides), MEMPROFILE_CERT=OFF_CERT)
        lines += [f"{k}={v}" for k, v in self.knobs.items()]
        tag = f"{name}-{tenant_count}-{self.cpus:g}x{self.gb:g}"
        json.dump({"size": {"vcpus": self.cpus, "gb": self.gb}, "scaled": self.knobs},
                  open(os.path.join(out_dir, f"size-{tag}.json"), "w"), indent=1)
        self.env_file = os.path.abspath(os.path.join(out_dir, f"posture-{tag}.env"))
        fd = os.open(self.env_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            # One line a key, the last setting winning, so the file reads as what the server runs.
            f.write("\n".join(f"{k}={v}" for k, v in dict(ln.split("=", 1) for ln in lines).items()) + "\n")

    def compose_env(self, **extra):
        env = dict(os.environ, AUTH_ENV=self.env_file, FEEDS_DIR=self.feeds,
                   SERVER_CPUS=f"{self.cpus:g}", SERVER_MEM=f"{int(self.gb * 1024)}m")
        env.update({k: str(v) for k, v in extra.items()})
        return env

    def debug(self, k, path, timeout=5):
        req = urllib.request.Request(f"http://127.0.0.1:{8200 + k}{path}")
        if self.bearer:
            req.add_header("authorization", f"Bearer {self.bearer}")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())


def wait_healthy(port, what, tries=120):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.5)
    raise SystemExit(f"{what} did not become healthy")


def bench(*args, env=(), mounts=(), url="http://router:8080", timeout=None):
    """One bench run in a container on the rig's network; its JSON summary.
    TIMEOUT: seconds past which the container is removed and the run stops."""
    name = f"{PROJECT}-bench-{secrets.token_hex(4)}"
    cmd = ["docker", "run", "--rm", "--name", name, "--network", NETWORK, "--ulimit", "nofile=65536:65536"]
    for host, inside in mounts:
        cmd += ["-v", f"{host}:{inside}:ro"]
    for kv in env:
        cmd += ["-e", kv]
    try:
        out = sh(*cmd, "streams-ramp:latest", "bench", "--url", url, "--key", KEY, "--json", *args, check=False,
                 timeout=timeout)
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    lines = [ln for ln in out.splitlines() if ln.startswith("{")]
    return json.loads(lines[-1]) if lines else {"error": out[-800:]}


def population(count, rate, secs, label, tokens=None, warmup=0, url="http://router:8080", creators=None,
               create_from=0, payload_bytes=400, appends_only=False):
    """Creates COUNT streams, then touches them (an append and a read) at RATE
    a second for SECS after WARMUP: habanero's population mode. TOKENS: a
    tokens.txt (stream i takes line i, modulo its length) or None."""
    workers = max(4, min(2000, int(rate * 8) + 4))
    env, mounts = [f"BENCH_POP_RATE={rate}", "BENCH_POP_START=0"], []
    if creators:
        env.append(f"BENCH_POP_CREATORS={creators}")
    if create_from:
        env.append(f"BENCH_POP_CREATE_FROM={create_from}")
    if appends_only:
        env.append("BENCH_POP_NO_READ=1")
    if tokens:
        mounts.append((os.path.dirname(tokens), "/tokens"))
        env.append(f"BENCH_POP_TOKENS=/tokens/{os.path.basename(tokens)}")
    return bench("--mode", "population", "--streams", str(count), "--concurrency", str(workers),
                 "--payload-bytes", str(payload_bytes), "--warmup-secs", str(warmup), "--duration-secs", str(secs),
                 "--prefix", label, "--label", label, env=env, mounts=mounts, url=url, timeout=secs + warmup + 600)
