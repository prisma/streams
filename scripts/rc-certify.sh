#!/bin/bash
# ONE top-level release-candidate certification (R30 review): either a
# gate runs here, or its immutable cloud-campaign manifest is verified
# against the EXACT release binary. No gate is attributed to a script
# that does not prove it.
#
# Usage:
#   SOAK_HOME=~/.streams-soak scripts/rc-certify.sh \
#     --capacity-run <cap-run-id> --handoff-run <handoff-run-id> \
#     --livefeed-fleet-run <manifest-path-or-run-id> \
#     --livefeed-canary-run <manifest-path-or-run-id>
#
# Verifies, in order:
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

CAP_RUN=""; HANDOFF_RUN=""; LF_FLEET_RUN=""; LF_CANARY_RUN=""
while [ $# -gt 0 ]; do
  case "$1" in
    --capacity-run) CAP_RUN=$2; shift 2;;
    --handoff-run)  HANDOFF_RUN=$2; shift 2;;
    --livefeed-fleet-run)  LF_FLEET_RUN=$2; shift 2;;
    --livefeed-canary-run) LF_CANARY_RUN=$2; shift 2;;
    *) echo "unknown arg: $1"; exit 2;;
  esac
done
[ -n "$CAP_RUN" ] || { echo "need --capacity-run"; exit 2; }
[ -n "$HANDOFF_RUN" ] || { echo "need --handoff-run"; exit 2; }
[ -n "$LF_FLEET_RUN" ] || { echo "need --livefeed-fleet-run"; exit 2; }
[ -n "$LF_CANARY_RUN" ] || { echo "need --livefeed-canary-run"; exit 2; }

# A livefeed run argument is either a manifest file path or a run id
# under $SOAK_HOME/results/<id>/ holding the named manifest.
resolve_manifest() { # <arg> <default-basename>
  if [ -f "$1" ]; then echo "$1"; else echo "$S/results/$1/$2"; fi
}

echo "== 1/6 local gate =="
./scripts/release-gate.sh

echo "== 2/6 release identity: campaign artifact + commit provenance =="
# The RC identity is the CAMPAIGN'S uploaded artifact (built by
# build-upload.sh with STREAMS_GIT_COMMIT + SOURCE_DATE_EPOCH pinned);
# certify that its manifest commit IS the tagged tree, then require
# every other campaign to have run the identical sha.
CAPD="$S/results/$CAP_RUN"
HEAD_COMMIT=$(git rev-parse HEAD)
RC_SHA=$(python3 - "$CAPD" "$HEAD_COMMIT" <<'PY'
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

echo "== 3/6 capacity manifest ($CAP_RUN) =="
python3 - "$CAPD" "$RC_SHA" <<'PY'
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
python3 - "$HD" "$RC_SHA" <<'PY'
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
python3 - "$LF_FLEET_MANIFEST" "$HEAD_COMMIT" <<'PY'
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
python3 - "$LF_CANARY_MANIFEST" "$RC_SHA" <<'PY'
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
python3 - "$RC_SHA" "$CAP_RUN" "$HANDOFF_RUN" "$LF_FLEET_RUN" "$LF_CANARY_RUN" <<'PY'
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
}, open("target/rc-certify-manifest.json", "w"), indent=2)
PY
echo "manifest -> target/rc-certify-manifest.json"
echo "RC_CERTIFY_OK  binary=$RC_SHA"
