#!/bin/bash
# L6 seal spike: run one rig point and log it (SPIKE-PLAN.md §2.4).
#
#   spike-run.sh <milestone> KEY=VALUE...
#     e.g. spike-run.sh M1 ARM=base SHARDS=1 STREAMS=8 MBPS=1 WAL_GAP=100 \
#            PAYLOAD=b64rand RECORDS=10 RECORD_BYTES=1024
#
# Wrapper-only keys (never passed to the rig):
#   ROOT=<checkout>   the checkout whose rig and target/release run the point
#                     (default the l6-spike worktree; the M1 identity arm
#                     uses a pristine 50fab9d9 clone)
#   POINT=<file>      point file, absolute or under ROOT/bench/k2cost/points
#                     (default L6S.env)
#   WAIT=1            wait for another heavy leg to finish instead of refusing
#   QUIET=<secs>      with WAIT=1: start only after no heavy leg has been seen
#                     for this long (default 0). The identity rerun uses 600:
#                     the first unmodified run was voided when a token-refresh
#                     `sleep 240` in the rig was SIGTERMed right after another
#                     rig finished (2026-10-11 13:23Z).
#   OVERLAP=1         start even though another heavy leg runs (no wait, no
#                     refusal); the legs seen at the start are logged and
#                     recorded in runs.jsonl as overlapping_legs_at_start.
#                     Byte and request counts stay comparable; RSS, CPU and
#                     latency do not. Used once, for the M1 identity pair, when
#                     a sibling session ran q0 rigs back to back for hours.
#   MIN_FREE_GIB=<n>  refuse below this much free disk (default 40, the plan's
#                     line). M4's short points (a few MB of results, s3lite in
#                     memory) ran at 15 when sibling sessions held the volume
#                     at 16-27 GiB free; the value is recorded in runs.jsonl.
#   GUARD=0|1         run tools/token-guard.sh beside the rig (default 1): it
#                     re-mints the rig's token only if the rig's own refresh
#                     loop died, and logs to <log>.guard.
# Every other KEY=VALUE goes into the rig's environment (the point file
# reads them as ${VAR:-default}).
#
# Refuses: another heavy leg running (gate/quality/mutants/run-local/
# formal_batch/the q0 rigs rig/q0-*.sh), a dirty ROOT, < 40 GiB free, the l6-spike target
# dir over 45 GB. Appends one line to $L6/runs.jsonl (milestone, point,
# results dir, git sha, binary sha256s, host before/after, start/end, rc,
# env) and deletes $TMPDIR/l6-* scratch stores afterwards.
set -euo pipefail

L6=/Users/sorenschmidt/.streams-k2/analysis/public-beta/l6
W=/Users/sorenschmidt/.streams-k2/wt/l6-spike
MILESTONE=${1:?usage: spike-run.sh <milestone> KEY=VALUE...}
shift
ROOT=$W
POINT=L6S.env
WAIT=0
QUIET=0
OVERLAP=0
GUARD=1
MIN_FREE_GIB=40
ENV=()
for kv in "$@"; do
  case "$kv" in
    ROOT=*) ROOT=${kv#ROOT=} ;;
    POINT=*) POINT=${kv#POINT=} ;;
    WAIT=*) WAIT=${kv#WAIT=} ;;
    QUIET=*) QUIET=${kv#QUIET=} ;;
    GUARD=*) GUARD=${kv#GUARD=} ;;
    MIN_FREE_GIB=*) MIN_FREE_GIB=${kv#MIN_FREE_GIB=} ;;
    OVERLAP=*) OVERLAP=${kv#OVERLAP=} ;;
    [A-Z_]*=*) ENV+=("$kv") ;;
    *) echo "spike-run: not KEY=VALUE: $kv" >&2; exit 2 ;;
  esac
done
case "$POINT" in /*) PF=$POINT ;; *) PF=$ROOT/bench/k2cost/points/$POINT ;; esac
[ -f "$PF" ] || { echo "spike-run: no point file $PF" >&2; exit 2; }
mkdir -p "$L6/logs"

HEAVY='gate\.sh|quality\.sh|cargo-mutants|run-local\.sh|formal_batch|q0-[a-z0-9-]*\.sh|mutations\.sh'
heavy() { pgrep -fl "$HEAVY" | grep -v "pgrep" || true; }
waited=0
quiet=0
OVERLAPPING=""
if [ "$OVERLAP" = 1 ]; then
  OVERLAPPING=$(heavy | { grep -v 'zsh -c' || true; } | awk '{print $2"/"$3}' | sort | uniq -c | awk '{print $2"x"$1}' | tr '\n' ' ')
  echo "[spike-run $(date +%H:%M:%S)] OVERLAP=1: starting beside: ${OVERLAPPING:-nothing}"
fi
while [ "$OVERLAP" != 1 ]; do
  if [ -n "$(heavy)" ]; then
    if [ "$WAIT" != 1 ]; then
      echo "spike-run: another heavy leg runs; refusing:" >&2
      heavy >&2
      exit 3
    fi
    [ "$quiet" -gt 0 ] && echo "[spike-run $(date +%H:%M:%S)] quiet broken after ${quiet} s by: $(heavy | head -3 | cut -c1-160 | tr '\n' ' ')"
    quiet=0
    [ $((waited % 600)) = 0 ] && echo "[spike-run $(date +%H:%M:%S)] waiting for: $(heavy | head -3 | cut -c1-160 | tr '\n' ' ')"
  else
    [ "$quiet" -ge "$QUIET" ] && break
    [ "$quiet" = 0 ] && [ "$QUIET" -gt 0 ] && echo "[spike-run $(date +%H:%M:%S)] no heavy leg; waiting ${QUIET} s of quiet"
    quiet=$((quiet + 60))
  fi
  sleep 60
  waited=$((waited + 60))
done
if [ -n "$(git -C "$ROOT" status --porcelain)" ]; then
  echo "spike-run: $ROOT is dirty; commit first" >&2
  git -C "$ROOT" status --porcelain | head >&2
  exit 3
fi
free_g=$(df -g "$HOME" | awk 'NR==2 {print $4}')
[ "$free_g" -ge "$MIN_FREE_GIB" ] || { echo "spike-run: only ${free_g} GiB free (< $MIN_FREE_GIB)" >&2; exit 3; }
target_g=$(du -sg "$W/target" 2>/dev/null | awk '{print $1}')
[ "${target_g:-0}" -le 45 ] || { echo "spike-run: $W/target is ${target_g} GB (> 45): run cargo clean --release" >&2; exit 3; }

host() { # label -> JSON object of the host state
  python3 - "$1" <<'PY'
import json, subprocess, sys
def sh(*a):
    return subprocess.run(a, capture_output=True, text=True).stdout.strip()
vm = sh("vm_stat")
free_pages = next((int(l.split(":")[1].strip().rstrip(".")) for l in vm.splitlines() if l.startswith("Pages free")), None)
page = int(sh("sysctl", "-n", "hw.pagesize") or 16384)
df = sh("df", "-g", "/System/Volumes/Data").splitlines()[-1].split()
print(json.dumps({"label": sys.argv[1], "uptime": sh("uptime"), "free_mem_mb": round((free_pages or 0) * page / 2**20),
                  "disk_free_gib": int(df[3]) if len(df) > 3 else None}))
PY
}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG=$L6/logs/$MILESTONE-$STAMP.log
HOST_BEFORE=$(host before)
T0=$(python3 -c 'import time; print(int(time.time() * 1000))')
echo "[spike-run $(date +%H:%M:%S)] $MILESTONE ${ENV[*]+"${ENV[*]}"} (root $ROOT, point $PF) -> $LOG"
rc=0
PID_GUARD=
if [ "$GUARD" = 1 ]; then
  "$L6/tools/token-guard.sh" "$LOG.guard" < /dev/null > /dev/null 2>&1 &
  PID_GUARD=$!
fi
(cd "$ROOT" && env ${ENV[@]+"${ENV[@]}"} bench/k2cost/run-local.sh "$PF") > "$LOG" 2>&1 || rc=$?
[ -n "$PID_GUARD" ] && { kill "$PID_GUARD" 2>/dev/null || true; }
GUARD_INTERVENED=$(grep -c INTERVENED "$LOG.guard" 2>/dev/null || true)
T1=$(python3 -c 'import time; print(int(time.time() * 1000))')
HOST_AFTER=$(host after)
RUN=$(sed -n 's/.*results in \(.*\)$/\1/p' "$LOG" | tail -1)
echo "[spike-run $(date +%H:%M:%S)] rc=$rc results $RUN"

rm -rf "${TMPDIR:-/tmp}"/l6-* 2>/dev/null || true
DU=$(du -sh "$W/target" "$HOME/.streams-k2/results" 2>/dev/null | tr '\t\n' ' =')

python3 - "$L6/runs.jsonl" <<PY
import hashlib, json, os, subprocess, sys
def sha(p):
    try:
        h = hashlib.sha256()
        with open(p, "rb") as f:
            for c in iter(lambda: f.read(1 << 20), b""):
                h.update(c)
        return h.hexdigest()
    except OSError:
        return None
root = "$ROOT"
rec = {
    "milestone": "$MILESTONE",
    "point": os.path.basename("$RUN").rsplit("-", 1)[0] if "$RUN" else None,
    "results": "$RUN" or None,
    "log": "$LOG",
    "root": root,
    "point_file": "$PF",
    "point_file_sha256": sha("$PF"),
    "git_sha": subprocess.run(["git", "-C", root, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
    "binaries_sha256": {b: sha(os.path.join(root, "target", "release", b)) for b in ("streams-slate", "s3lite", "pilot")},
    "host": {"hw": subprocess.run(["sysctl", "-n", "hw.model", "hw.ncpu", "hw.memsize"], capture_output=True, text=True).stdout.split(),
             "before": json.loads('''$HOST_BEFORE'''), "after": json.loads('''$HOST_AFTER''')},
    "waited_for_other_legs_s": $waited,
    "overlapping_legs_at_start": """$OVERLAPPING""".split() if "$OVERLAP" == "1" else None,
    "start_ms": $T0, "end_ms": $T1, "rc": $rc,
    "env": """${ENV[*]+"${ENV[*]}"}""".split(),
    "disk_after": """$DU""",
    "token_guard": {"on": "$GUARD" == "1", "interventions": int("${GUARD_INTERVENED:-0}" or 0)},
    "min_free_gib": $MIN_FREE_GIB,
}
with open(sys.argv[1], "a") as f:
    f.write(json.dumps(rec, separators=(",", ":")) + "\n")
PY
exit $rc
