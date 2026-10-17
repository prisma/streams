#!/bin/bash
# ONE top-level release-candidate certification (R30 review): either a
# gate runs here, or its immutable cloud-campaign manifest is verified
# against the EXACT release binary. No gate is attributed to a script
# that does not prove it.
#
# Two shapes (docs/OPS-RELEASE.md §7). The fleet shape is the default:
#   SOAK_HOME=~/.streams-soak scripts/rc-certify.sh [--shape fleet] \
#     --capacity-run <cap-run-id> --handoff-run <handoff-run-id> \
#     --livefeed-fleet-run <manifest-path-or-run-id> \
#     --livefeed-canary-run <manifest-path-or-run-id>
# The single-cell shape certifies a single-server, fleet-off RC (the
# lead's call 7 of 2026-10-11, under the owner's "you decide"):
#   SOAK_HOME=~/.streams-soak scripts/rc-certify.sh --shape single-cell \
#     --artifact-run <build-upload-run-id> \
#     --provider-contract-log <path> \
#     --layer-b-run <m> --envelope-run <m> --sc0-1s-run <m> --sc1-run <m> \
#     --stream-count-run <m> --battery-run <m>     # <m>: manifest path or run id
# Each shape refuses the other's manifests, before the gate runs.
#
# Single-cell verifies, in order:
#   1. local gate, as below (the capacity mechanism test runs alone in it)
#   2. release identity from build-upload.sh's binaries.json in the
#      --artifact-run directory: server gitCommit == HEAD; its sha256 is
#      the RC identity
#   3. the real-provider contract log (docs/PROVIDER-CONTRACT.md):
#      re-judged by tests_ran.py, one qualifying line naming a Tigris
#      endpoint over https, the passed line, no SKIPPED
#   4-9. the arm manifests layer-b, envelope, sc0-1s, sc1, stream-count
#      and battery (<arm>-manifest.json in a run directory): arm == its
#      name, server sha == RC sha, verdict PASS, every leg PASS, the legs
#      call 7 names present (layer-b: H9, H9b, H11t; battery: seven), the
#      run's window (started_unix, finished_unix) at least 72 h for sc0-1s,
#      30 h for sc1 and non-empty for the rest, at least 1,000 projects for
#      layer-b and battery, and the stream-count arm's `streams`, the scale
#      it certifies; the profile fence (triage B9): each profile file the
#      manifest names is byte for byte the tag tree's deploy/profiles/<file>,
#      their lines hold INITIAL_SHARDS=1, KEEP_AWAKE=1,
#      STREAMS_RELEASE_POSTURE=1 and PROJECT_SHARE_K=8 and none sets
#      FLEET_PREFIX; every field arm (all but layer-b) names one and the
#      same cell_env_sha256
# then prints each fleet-only leg it waives (capacity campaign, handoff,
# LiveFeed fleet, LiveFeed canary).
#
# Fleet verifies, in order:
#   1. local gate (fmt, clippy fingerprints, Rust/DST suite, cargo deny)
#   2. the x86_64-musl release binary builds; its sha256 is the RC identity
#   3. capacity manifest: binaries.json server sha == RC sha,
#      capacity-verdict.json PASS, reconcile.json verdict OK
#   4. handoff manifest: binary.sha == RC sha, restore-verdict.json
#      restored_over_prekill >= 1.0, reconcile OK in its results dir
#   5. livefeed FLEET manifest (round 11.5): commit == HEAD, verdict
#      PASS, every leg PASS/COVERED_INPROC. Tree-identity, not
#      artifact-identity: the fleet battery runs the host-built binary
#      of THIS tree (CI runs it on every push as livefeed-fleet-cert).
#   6. livefeed CANARY manifest (round 11.6): server sha == RC sha
#      (exact release artifact), verdict PASS, every leg PASS.
# Prints RC_CERTIFY_OK only when every leg holds.
set -euo pipefail
cd "$(dirname "$0")/.."
S=${SOAK_HOME:?set SOAK_HOME}
# The checks below are Python `assert`s, which PYTHONOPTIMIZE strips. Each
# runs as `python3 -E`, so no PYTHON* variable of the caller's environment
# (PYTHONOPTIMIZE, PYTHONPATH, ...) can switch a check off.

SHAPE=fleet
CAP_RUN=""; HANDOFF_RUN=""; LF_FLEET_RUN=""; LF_CANARY_RUN=""
ARTIFACT_RUN=""; PROVIDER_LOG=""; LAYER_B_RUN=""; ENVELOPE_RUN=""; SC0_RUN=""; SC1_RUN=""
STREAMS_RUN=""; BATTERY_RUN=""
while [ $# -gt 0 ]; do
  case "$1" in
    --shape) SHAPE=$2; shift 2;;
    --capacity-run) CAP_RUN=$2; shift 2;;
    --handoff-run)  HANDOFF_RUN=$2; shift 2;;
    --livefeed-fleet-run)  LF_FLEET_RUN=$2; shift 2;;
    --livefeed-canary-run) LF_CANARY_RUN=$2; shift 2;;
    --artifact-run) ARTIFACT_RUN=$2; shift 2;;
    --provider-contract-log) PROVIDER_LOG=$2; shift 2;;
    --layer-b-run) LAYER_B_RUN=$2; shift 2;;
    --envelope-run) ENVELOPE_RUN=$2; shift 2;;
    --sc0-1s-run) SC0_RUN=$2; shift 2;;
    --sc1-run) SC1_RUN=$2; shift 2;;
    --stream-count-run) STREAMS_RUN=$2; shift 2;;
    --battery-run) BATTERY_RUN=$2; shift 2;;
    *) echo "unknown arg: $1"; exit 2;;
  esac
done
need() { # <flag> <value>: this shape requires the flag
  [ -n "$2" ] || { echo "need $1"; exit 2; }
}
takes_no() { # <flag> <value> <why>: this shape refuses the other shape's evidence
  [ -z "$2" ] || { echo "a $SHAPE certification takes no $1: $3"; exit 2; }
}
case "$SHAPE" in
  fleet)
    need --capacity-run "$CAP_RUN"
    need --handoff-run "$HANDOFF_RUN"
    need --livefeed-fleet-run "$LF_FLEET_RUN"
    need --livefeed-canary-run "$LF_CANARY_RUN"
    SINGLE="that is single-cell evidence (--shape single-cell)"
    takes_no --artifact-run "$ARTIFACT_RUN" "$SINGLE"
    takes_no --provider-contract-log "$PROVIDER_LOG" "$SINGLE"
    takes_no --layer-b-run "$LAYER_B_RUN" "$SINGLE"
    takes_no --envelope-run "$ENVELOPE_RUN" "$SINGLE"
    takes_no --sc0-1s-run "$SC0_RUN" "$SINGLE"
    takes_no --sc1-run "$SC1_RUN" "$SINGLE"
    takes_no --stream-count-run "$STREAMS_RUN" "$SINGLE"
    takes_no --battery-run "$BATTERY_RUN" "$SINGLE"
    STEPS=6; IDENTITY_RUN=$CAP_RUN;;
  single-cell)
    need --artifact-run "$ARTIFACT_RUN"
    need --provider-contract-log "$PROVIDER_LOG"
    need --layer-b-run "$LAYER_B_RUN"
    need --envelope-run "$ENVELOPE_RUN"
    need --sc0-1s-run "$SC0_RUN"
    need --sc1-run "$SC1_RUN"
    need --stream-count-run "$STREAMS_RUN"
    need --battery-run "$BATTERY_RUN"
    WAIVED="that fleet leg is waived for a single-cell, fleet-off RC"
    takes_no --capacity-run "$CAP_RUN" "$WAIVED"
    takes_no --handoff-run "$HANDOFF_RUN" "$WAIVED"
    takes_no --livefeed-fleet-run "$LF_FLEET_RUN" "$WAIVED"
    takes_no --livefeed-canary-run "$LF_CANARY_RUN" "$WAIVED"
    STEPS=9; IDENTITY_RUN=$ARTIFACT_RUN;;
  *) echo "unknown shape: $SHAPE (fleet | single-cell)"; exit 2;;
esac

# A manifest argument is either a manifest file path or a run id
# under $SOAK_HOME/results/<id>/ holding the named manifest.
resolve_manifest() { # <arg> <default-basename>
  if [ -f "$1" ]; then echo "$1"; else echo "$S/results/$1/$2"; fi
}

echo "== 1/$STEPS local gate =="
./scripts/release-gate.sh

echo "== 2/$STEPS release identity: campaign artifact + commit provenance =="
# The RC identity is the CAMPAIGN'S uploaded artifact (built by
# build-upload.sh with STREAMS_GIT_COMMIT + SOURCE_DATE_EPOCH pinned);
# certify that its manifest commit IS the tagged tree, then require
# every other campaign to have run the identical sha. The fleet shape
# reads the capacity campaign's binaries.json; the single-cell shape
# reads the one build-upload.sh wrote for --artifact-run.
HEAD_COMMIT=$(git rev-parse HEAD)
RC_SHA=$(python3 -E - "$S/results/$IDENTITY_RUN" "$HEAD_COMMIT" <<'PY'
import json, sys
d, head = sys.argv[1], sys.argv[2]
bins = json.load(open(f"{d}/binaries.json"))
srv = [(k, v) for k, v in bins.items() if "streams-" in k]
assert srv, "no server binary in manifest"
key, meta = srv[0]
commit = meta.get("gitCommit", "")
assert commit == head, f"manifest commit {commit[:12]} != HEAD {head[:12]} — campaign did not run this tree"
print(meta["sha256"])
PY
)
echo "RC binary sha256 (campaign artifact): $RC_SHA"

# One single-cell arm: <arm>-manifest.json, re-checked against the RC sha.
ARMS=()
verify_arm() { # <step> <arm> <manifest-path-or-run-id>
  local manifest
  manifest=$(resolve_manifest "$3" "$2-manifest.json")
  echo "== $1/$STEPS $2 manifest ($3) =="
  python3 -E - "$2" "$manifest" "$RC_SHA" "$HEAD_COMMIT" <<'PY'
import hashlib, json, os, subprocess, sys
arm, path, rc, head = sys.argv[1:5]
def refuse(why):
    sys.exit(f"{arm}: {why}")
# The legs and scale the lead's call 7 (2026-10-11) names for an arm.
REQUIRED_LEGS = {
    "layer-b": ("H9", "H9b", "H11t"),
    "battery": ("hostile-tenants", "cost-hostile-tenants", "parked-consumers", "ch-probe",
                "suspension-under-load", "revocation-under-load", "billing-reconcile"),
}
MIN_PROJECTS = {"layer-b": 1000, "battery": 1000}
# The run's least length in seconds: SC0-1s 72 h, SC1 30 h (the lead's
# reading of call 7's "about 30 h": its probe schedule's length). Any
# other arm: a non-empty window.
MIN_SECS = {"sc0-1s": 72 * 3600, "sc1": 30 * 3600}
if not os.path.isfile(path):
    refuse(f"no manifest at {path}")
m = json.load(open(path))
if m.get("arm") != arm:
    refuse(f"the manifest names arm {m.get('arm')!r}")
sha = str(m.get("server_sha256", "")).removeprefix("sha256:")
if sha != rc:
    refuse(f"ran {sha[:16]}, RC is {rc[:16]} — NOT the release binary")
if m.get("verdict") != "PASS":
    refuse(f"verdict {m.get('verdict')!r}")
legs = m.get("legs")
if not isinstance(legs, dict) or not legs:
    refuse("no legs")
bad = {k: v for k, v in legs.items() if v != "PASS"}
if bad:
    refuse(f"legs not green: {bad}")
missing = [leg for leg in REQUIRED_LEGS.get(arm, ()) if leg not in legs]
if missing:
    refuse(f"required legs missing: {missing}")
started, finished = m.get("started_unix"), m.get("finished_unix")
if type(started) is not int or type(finished) is not int:
    refuse(f"started_unix {started!r}, finished_unix {finished!r}: "
           "the manifest names its run's window in unix seconds")
secs, floor = finished - started, MIN_SECS.get(arm, 1)
if secs < floor:
    refuse(f"ran {secs} s, under the arm's floor of {floor} s"
           + (f" ({floor // 3600} h)" if floor >= 3600 else ""))
# The profile fence (deploy-q5 triage B9, under the lead's call 7): the
# arm's cell ran deploy profiles that are, byte for byte, the tag tree's,
# whose layered lines hold the single-cell pins and no FLEET_PREFIX (a
# fleet-pinned run cannot certify a fleet-off RC). The lines are read from
# the tag tree, not taken from the manifest's own `pins`.
PINS = ("INITIAL_SHARDS=1", "KEEP_AWAKE=1", "STREAMS_RELEASE_POSTURE=1", "PROJECT_SHARE_K=8")
prof = m.get("profile")
if (not isinstance(prof, dict) or not isinstance(prof.get("files"), list) or not prof["files"]
        or not isinstance(prof.get("sha256"), dict)):
    refuse("no profile: the manifest names the deploy profiles its cell ran (files, sha256)")
lines, fleet = set(), []
for f in prof["files"]:
    if not isinstance(f, str) or not f or "/" in f or f.startswith("."):
        refuse(f"profile file {f!r} is not a deploy/profiles/ name")
    shown = subprocess.run(["git", "show", f"{head}:deploy/profiles/{f}"], capture_output=True)
    if shown.returncode != 0:
        refuse(f"profile deploy/profiles/{f} is not in the tag tree")
    want = hashlib.sha256(shown.stdout).hexdigest()
    got = str(prof["sha256"].get(f, "")).removeprefix("sha256:")
    if got != want:
        refuse(f"profile {f} has sha256 {got[:16]}, the tag tree's {want[:16]}")
    text = [line.strip() for line in shown.stdout.decode(errors="replace").splitlines()]
    lines |= set(text)
    if any(line.startswith("FLEET_PREFIX=") for line in text):
        fleet.append(f)
missing = [p for p in PINS if p not in lines]
if missing:
    refuse(f"the profile's files lack the single-cell pins {missing}")
if fleet:
    refuse(f"the profile sets FLEET_PREFIX ({', '.join(fleet)}): a fleet-pinned run")
# A field arm names the cell environment it ran; one across them (below).
if arm != "layer-b":
    cell_env = m.get("cell_env_sha256")
    if not isinstance(cell_env, str) or not cell_env:
        refuse(f"cell_env_sha256 {cell_env!r}: a field arm names the cell environment it ran")
scale = ""
if arm in MIN_PROJECTS:
    projects = m.get("projects")
    if type(projects) is not int or projects < MIN_PROJECTS[arm]:
        refuse(f"projects {projects!r}: the arm runs at least {MIN_PROJECTS[arm]} projects")
    scale = f", {projects} projects"
if arm == "stream-count":
    streams = m.get("streams")
    if type(streams) is not int or streams < 1:
        refuse(f"streams {streams!r}: the arm names the stream count it certifies")
    scale = f", {streams} streams"
print(f"  {arm}: PASS ({len(legs)} legs{scale}, {secs / 3600:.1f} h) on {sha[:16]}")
PY
  ARMS+=("$2=$manifest")
}

if [ "$SHAPE" = single-cell ]; then
  echo "== 3/$STEPS provider contract ($PROVIDER_LOG) =="
  # The real-provider runner's log as scripts/test-leg.sh kept it
  # (docs/PROVIDER-CONTRACT.md), re-judged here. The log names no commit:
  # running it on the tagged tree is the operator's step (OPS-RELEASE §7).
  python3 -E scripts/quality/tests_ran.py "$PROVIDER_LOG" \
    --exact bootstrap::tests::provider_contract::real_provider_meets_the_provider_contract
  python3 -E - "$PROVIDER_LOG" <<'PY'
import re, sys
from urllib.parse import urlsplit
log = open(sys.argv[1], errors="replace").read()
def refuse(why):
    sys.exit(f"provider contract: {why}")
if re.search(r"^SKIPPED:", log, re.M):
    refuse("the log says SKIPPED: the runner had no opt-in and qualified nothing")
lines = re.findall(r"^qualifying endpoint (\S+) bucket \S+ prefix .* region \S+$", log, re.M)
if len(lines) != 1:
    refuse(f"{len(lines)} `qualifying endpoint ...` lines; a qualifying run prints one")
endpoint = lines[0]
url = urlsplit(endpoint)
host = url.hostname or ""
if url.scheme != "https" or not any(host == d or host.endswith("." + d)
                                    for d in ("storage.dev", "tigris.dev")):
    refuse(f"qualified {endpoint}: not a Tigris endpoint over https")
if not re.search(r"^real-provider: provider contract passed; ", log, re.M):
    refuse("no `real-provider: provider contract passed` line")
print(f"  provider contract: passed against {endpoint}")
PY
  verify_arm 4 layer-b "$LAYER_B_RUN"
  verify_arm 5 envelope "$ENVELOPE_RUN"
  verify_arm 6 sc0-1s "$SC0_RUN"
  verify_arm 7 sc1 "$SC1_RUN"
  verify_arm 8 stream-count "$STREAMS_RUN"
  verify_arm 9 battery "$BATTERY_RUN"
  # The field arms ran one cell: one cell environment across them (triage B9).
  python3 -E - ${ARMS[@]+"${ARMS[@]}"} <<'PY'
import json, sys
envs = {}
for pair in sys.argv[1:]:
    arm, path = pair.split("=", 1)
    if arm != "layer-b":
        envs.setdefault(json.load(open(path))["cell_env_sha256"], []).append(arm)
if len(envs) != 1:
    sys.exit(f"the field arms ran {len(envs)} cell environments: "
             + "; ".join(f"{', '.join(a)} on {e[:16]}" for e, a in envs.items()))
print(f"  one cell environment across the field arms: {next(iter(envs))[:16]}")
PY

  echo "== waived for a single-cell, fleet-off RC (the lead's call 7, 2026-10-11) =="
  mkdir -p target
  python3 -E - "$RC_SHA" "$HEAD_COMMIT" "$ARTIFACT_RUN" "$PROVIDER_LOG" ${ARMS[@]+"${ARMS[@]}"} <<'PY'
import hashlib, json, re, sys
rc, head, artifact_run, log_path = sys.argv[1:5]
def sha256(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()
WAIVED = {
    "capacity_campaign": "post-split capacity scaling as product evidence needs a fleet; "
                         "the release gate still ran the capacity mechanism test alone",
    "handoff": "owner movement between instances; one server owns every shard",
    "livefeed_fleet": "the LiveFeed certification of a three-instance fleet "
                      "(promote-rc.sh still requires CI's livefeed-fleet-cert at the SHA)",
    "livefeed_canary": "the LiveFeed canary battery runs a three-instance fleet",
}
for name, why in WAIVED.items():
    print(f"  WAIVED {name.replace('_', '-')}: {why}")
arms = {}
for pair in sys.argv[5:]:
    arm, path = pair.split("=", 1)
    m = json.load(open(path))
    arms[arm] = {"manifest": path, "manifest_sha256": sha256(path), "legs": sorted(m["legs"]),
                 **{k: m[k] for k in ("projects", "streams", "started_unix", "finished_unix") if k in m}}
def hours(arm):
    return f"{(arms[arm]['finished_unix'] - arms[arm]['started_unix']) / 3600:.1f} h"
endpoint = re.search(r"^qualifying endpoint (\S+) ", open(log_path, errors="replace").read(),
                     re.M).group(1)
provider = {"log": log_path, "log_sha256": sha256(log_path), "endpoint": endpoint}
tag_lines = [
    "single-cell, fleet-off manifest set verified via rc-certify.sh --shape single-cell: "
    f"provider contract against {endpoint} (log sha256 {provider['log_sha256']}), "
    f"layer-b, envelope, sc0-1s ({hours('sc0-1s')}), sc1 ({hours('sc1')}), "
    f"stream-count ({arms['stream-count']['streams']} streams), "
    f"battery ({arms['battery']['projects']} projects)",
    "WAIVED for this single-cell, fleet-off RC (the lead's call 7 of 2026-10-11, under the "
    "owner's delegation): " + ", ".join(n.replace("_", " ") for n in WAIVED)
    + "; this tag certifies no multi-server operation",
]
json.dump({
    "commit": head,
    "server_sha256": rc,
    "shape": "single-cell",
    "artifact_run": artifact_run,
    "provider_contract": provider,
    "arms": arms,
    "waived": WAIVED,
    "tag_lines": tag_lines,
}, open("target/rc-certify-manifest.json", "w"), indent=2)
PY
  echo "manifest -> target/rc-certify-manifest.json"
  echo "RC_CERTIFY_OK  binary=$RC_SHA"
  exit 0
fi

echo "== 3/6 capacity manifest ($CAP_RUN) =="
CAPD="$S/results/$CAP_RUN"
python3 -E - "$CAPD" "$RC_SHA" <<'PY'
import json, sys
d, rc = sys.argv[1], sys.argv[2]
v = json.load(open(f"{d}/capacity-verdict.json"))
assert v.get("PASS") is True, f"capacity verdict not PASS: {v}"
rec = json.load(open(f"{d}/reconcile.json"))
assert all(r.get("verdict") == "OK" for r in rec), f"reconcile not OK: {rec}"
print(f"  capacity: PASS on {rc[:16]} (peak {v['peak_ledger_bytes']>>20}MB, "
      f"shed {v['maintenance_shed_total']}, catchup {v['catchup_over_ingest']})")
PY

echo "== 4/6 handoff manifest ($HANDOFF_RUN) =="
HD="$S/results/$HANDOFF_RUN"
python3 -E - "$HD" "$RC_SHA" <<'PY'
import json, sys
d, rc = sys.argv[1], sys.argv[2]
sha = open(f"{d}/binary.sha").read().strip()
assert sha == rc, f"handoff ran {sha[:16]}, RC is {rc[:16]} — NOT the release binary"
v = json.load(open(f"{d}/restore-verdict.json"))
r = v.get("restored_over_prekill")
assert r is not None and r >= 1.0, f"restore ratio {r} < 1.0"
rec = json.load(open(f"{d}/reconcile.json"))
assert rec and all(row.get("verdict") == "OK" for row in rec), f"handoff reconcile not OK: {rec}"
print(f"  handoff: restore {r}, reconcile OK, on {sha[:16]}")
PY

echo "== 5/6 livefeed fleet manifest ($LF_FLEET_RUN) =="
LF_FLEET_MANIFEST=$(resolve_manifest "$LF_FLEET_RUN" livefeed-cert-manifest.json)
python3 -E - "$LF_FLEET_MANIFEST" "$HEAD_COMMIT" <<'PY'
import json, sys
m, head = json.load(open(sys.argv[1])), sys.argv[2]
assert m.get("commit") == head, \
    f"fleet manifest commit {m.get('commit', '')[:12]} != HEAD {head[:12]}"
assert m.get("verdict") == "PASS", f"fleet verdict: {m.get('verdict')}"
bad = {k: v for k, v in m.get("legs", {}).items() if v not in ("PASS", "COVERED_INPROC")}
assert not bad, f"fleet legs not green: {bad}"
print(f"  livefeed fleet: PASS ({len(m.get('legs', {}))} legs) on tree {head[:12]}")
PY

echo "== 6/6 livefeed canary manifest ($LF_CANARY_RUN) =="
LF_CANARY_MANIFEST=$(resolve_manifest "$LF_CANARY_RUN" livefeed-canary-manifest.json)
python3 -E - "$LF_CANARY_MANIFEST" "$RC_SHA" <<'PY'
import json, sys
m, rc = json.load(open(sys.argv[1])), sys.argv[2]
sha = m.get("server_sha256", "").removeprefix("sha256:")
assert sha == rc, f"canary ran {sha[:16]}, RC is {rc[:16]} — NOT the release binary"
assert m.get("verdict") == "PASS", f"canary verdict: {m.get('verdict')}"
bad = {k: v for k, v in m.get("legs", {}).items() if v != "PASS"}
assert not bad, f"canary legs not green: {bad}"
print(f"  livefeed canary: PASS ({len(m.get('legs', {}))} legs) on {sha[:16]}")
PY

# Review round 3: the certification result as data — promote-rc.sh
# records THIS server_sha256 as the tag's artifact identity.
mkdir -p target
python3 -E - "$RC_SHA" "$CAP_RUN" "$HANDOFF_RUN" "$LF_FLEET_RUN" "$LF_CANARY_RUN" <<'PY'
import json, subprocess, sys
sha, cap, hand, lfleet, lcanary = sys.argv[1:6]
head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
json.dump({
    "commit": head,
    "server_sha256": sha,
    "capacity_run": cap,
    "handoff_run": hand,
    "livefeed_fleet_run": lfleet,
    "livefeed_canary_run": lcanary,
    "capacity": "pass",
    "handoff": "pass",
    "livefeed_fleet": "pass",
    "livefeed_canary": "pass",
    "shape": "fleet",
    # promote-rc.sh prints these in the tag (the single-cell shape writes its own).
    "tag_lines": ["capacity + handoff manifests verified via rc-certify.sh"],
}, open("target/rc-certify-manifest.json", "w"), indent=2)
PY
echo "manifest -> target/rc-certify-manifest.json"
echo "RC_CERTIFY_OK  binary=$RC_SHA"
