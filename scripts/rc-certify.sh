#!/bin/bash
# ONE top-level release-candidate certification (R30 review): either a
# gate runs here, or its immutable cloud-campaign manifest is verified
# against the EXACT release binary. No gate is attributed to a script
# that does not prove it.
#
# Usage:
#   SOAK_HOME=~/.streams-soak scripts/rc-certify.sh \
#     --capacity-run <cap-run-id> --handoff-run <handoff-run-id>
#
# Verifies, in order:
#   1. local gate (fmt, clippy fingerprints, Rust/DST suite, cargo deny)
#   2. the x86_64-musl release binary builds; its sha256 is the RC identity
#   3. capacity manifest: binaries.json server sha == RC sha,
#      capacity-verdict.json PASS, reconcile.json verdict OK
#   4. handoff manifest: binary.sha == RC sha, restore-verdict.json
#      restored_over_prekill >= 1.0, reconcile OK in its results dir
# Prints RC_CERTIFY_OK only when every leg holds.
set -euo pipefail
cd "$(dirname "$0")/.."
S=${SOAK_HOME:?set SOAK_HOME}

CAP_RUN=""; HANDOFF_RUN=""
while [ $# -gt 0 ]; do
  case "$1" in
    --capacity-run) CAP_RUN=$2; shift 2;;
    --handoff-run)  HANDOFF_RUN=$2; shift 2;;
    *) echo "unknown arg: $1"; exit 2;;
  esac
done
[ -n "$CAP_RUN" ] || { echo "need --capacity-run"; exit 2; }
[ -n "$HANDOFF_RUN" ] || { echo "need --handoff-run"; exit 2; }

echo "== 1/4 local gate =="
./scripts/release-gate.sh

echo "== 2/4 release binary identity =="
cargo zigbuild --release --target x86_64-unknown-linux-musl 2>&1 | tail -1
RC_SHA=$(shasum -a 256 target/x86_64-unknown-linux-musl/release/streams-slate | cut -d' ' -f1)
echo "RC binary sha256: $RC_SHA"

echo "== 3/4 capacity manifest ($CAP_RUN) =="
CAPD="$S/results/$CAP_RUN"
python3 - "$CAPD" "$RC_SHA" <<'PY'
import json, sys
d, rc = sys.argv[1], sys.argv[2]
bins = json.load(open(f"{d}/binaries.json"))
srv = [v["sha256"] for k, v in bins.items() if "/streams-" in k or k.startswith("bin/streams-")]
assert srv, "no server binary in manifest"
assert srv[0] == rc, f"capacity ran {srv[0][:16]}, RC is {rc[:16]} — NOT the release binary"
v = json.load(open(f"{d}/capacity-verdict.json"))
assert v.get("PASS") is True, f"capacity verdict not PASS: {v}"
rec = json.load(open(f"{d}/reconcile.json"))
assert all(r.get("verdict") == "OK" for r in rec), f"reconcile not OK: {rec}"
print(f"  capacity: PASS on {srv[0][:16]} (peak {v['peak_ledger_bytes']>>20}MB, "
      f"shed {v['maintenance_shed_total']}, catchup {v['catchup_over_ingest']})")
PY

echo "== 4/4 handoff manifest ($HANDOFF_RUN) =="
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

echo "RC_CERTIFY_OK  binary=$RC_SHA"
