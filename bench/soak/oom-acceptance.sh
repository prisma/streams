#!/bin/bash
# OOM acceptance-campaign driver — the release gate before
# preview.7-class posture is restored (docs/OBSERVABILITY-BILLING-
# STATUS.md "Acceptance gate"). One driver, all legs.
#
#   oom-acceptance.sh verify <server-url> <auth-token-file>
#   oom-acceptance.sh leg <name> <server-url> <gen-url> <auth-file> \
#       <target_gib> <bytes_per_op> <max_minutes> \
#       [stall_ms] [stop_load_cmd] [kill_line_mb]
#   oom-acceptance.sh reconcile <server-url> <auth-file> <key-file> \
#       <stream-prefix> <streams> <expected_ok> <batch>
#
# verify — reads the RESOLVED config from GET /v1/debug/absorb and
# requires EXACT equality with EVERY memory knob in
# deploy/profiles/compute-1g.env (a four-of-twelve check proves
# nothing about the categories the incident implicated). Budget is
# exact-equality too: a LARGER budget than intended also invalidates
# the envelope.
#
# leg — verify, optional stall injection (always cleared by trap),
# then sample every 5 s. Targets ACCEPTED BYTES (target_gib), derived
# as ok x bytes_per_op — an op-count target silently changes meaning
# when batch/record size changes. Per-sample checks: reservation
# invariant, death detection (normalized health codes), errs == 0
# (deliberate shedding is retried by the closed-loop gen and never
# lands in errs), stall visibility on stall legs. On reaching the byte
# target the leg enters the RECOVERY phase: stop_load_cmd runs, then
# ingest must go quiet, absorption must catch up to ingest, history L0
# must decline, the spool must drain, and the cgroup peak must sit
# >= 100 MiB under the kill line. Only then PASS. Failed legs preserve
# their namespace and samples.
#
# reconcile — exact acknowledged-operation accounting: pages every
# campaign stream's records (product surface) and requires
# total records == expected_ok x batch.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PROFILE="$ROOT/deploy/profiles/compute-1g.env"

pval() { awk -F= -v k="$1" '$1==k {print $2}' "$PROFILE"; }

norm_http() { case "${1:-}" in [0-9][0-9][0-9]) printf '%s' "$1" ;; *) printf '0' ;; esac; }

case "${1:-}" in
verify)
  URL=$2; AUTH=$(cat "$3")
  J=$(curl -sf --max-time 15 -H "Authorization: Bearer $AUTH" "$URL/v1/debug/absorb")
  python3 - "$J" "$PROFILE" <<'PY'
import json, sys
j = json.loads(sys.argv[1])
prof = {}
for line in open(sys.argv[2]):
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    prof[k] = int(v)
cfg = j.get("config") or {}
if not cfg:
    print("VERIFY_FAIL: server exposes no resolved config (old binary?)"); sys.exit(1)
# EXACT equality for every profile knob the server resolves.
checks = [
    ("ABSORB_GATHER_MAX_BYTES", "gatherPackingLimitBytes"),
    ("ABSORB_GLOBAL_BUDGET_BYTES", "absorbBudgetBytes"),
    ("ABSORB_GLOBAL_GATHERS", "gatherSlots"),
    ("SLATEDB_RT_THREADS", "slatedbRuntimeThreads"),
    ("SHARED_CACHE_BYTES", "sharedCacheBytes"),
    ("HISTORY_CACHE_BYTES", "historyCacheBytes"),
    ("POSTINGS_CACHE_BYTES", "postingsCacheBytes"),
    ("TELEMETRY_CACHE_BYTES", "telemetryCacheBytes"),
    ("MAX_UNFLUSHED_BYTES", "maxUnflushedBytes"),
    ("L0_SST_SIZE_BYTES", "l0SstSizeBytes"),
    ("L0_MAX_SSTS", "l0MaxSsts"),
    ("ADMIT_RSS_SHED_MB", "shedLineMb"),
]
errs = []
for env_key, cfg_key in checks:
    if env_key not in prof:
        errs.append(f"profile missing {env_key}")
        continue
    if cfg.get(cfg_key) != prof[env_key]:
        errs.append(f"{cfg_key}={cfg.get(cfg_key)} != profile {env_key}={prof[env_key]}")
eff = cfg.get("effectiveGatherConcurrency")
if errs:
    print("VERIFY_FAIL: " + "; ".join(errs)); sys.exit(1)
print(f"VERIFY_OK all {len(checks)} knobs exact; effective gather concurrency={eff}")
PY
  ;;
leg)
  NAME=$2; URL=$3; GENURL=$4; AUTHFILE=$5; AUTH=$(cat "$AUTHFILE")
  TARGET_GIB=${6:?target_gib}; BYTES_PER_OP=${7:?bytes_per_op}; MAXMIN=${8:?max_minutes}
  STALL=${9:-0}; STOPCMD=${10:-}; KILL_LINE_MB=${11:-740}
  TARGET_BYTES=$(python3 -c "print(int(float('$TARGET_GIB') * 1024**3))")
  OUT="${OOM_RESULTS_DIR:-/tmp}/oom-leg-$NAME.jsonl"; : > "$OUT"
  "$0" verify "$URL" "$AUTHFILE"
  # The stall is ALWAYS cleared on exit — an interrupted stall leg must
  # not leave the service deliberately degraded for later legs.
  clear_stall() {
    curl -s --max-time 10 -X POST -H "Authorization: Bearer $AUTH" \
      "$URL/v1/debug/history-stall?ms=0" >/dev/null 2>&1 || true
  }
  trap clear_stall EXIT INT TERM
  if [ "$STALL" != "0" ]; then
    curl -sf --max-time 10 -X POST -H "Authorization: Bearer $AUTH" \
      "$URL/v1/debug/history-stall?ms=$STALL" >/dev/null
    echo "stalled-history-flush leg: injected ${STALL}ms per flush"
  fi
  PHASE=load
  for i in $(seq 1 $(( MAXMIN * 12 ))); do
    ts=$(date +%s)
    g=$(curl -s --max-time 10 "$GENURL/" || echo '{}')
    ab=$(curl -s --max-time 10 -H "Authorization: Bearer $AUTH" "$URL/v1/debug/absorb" || echo '{}')
    hc=$(norm_http "$(curl -s --max-time 10 -o /dev/null -w '%{http_code}' "$URL/health" || true)")
    origin=$(curl -s --max-time 10 -D- -o /dev/null "$URL/health" 2>/dev/null | grep -ci 'prisma-streams-origin' || true)
    line=$(python3 - "$ts" "$i" "$hc" "$origin" "$PHASE" <<PY
import json, sys
ts, i, hc, origin, phase = sys.argv[1:6]
def safe(s):
    try:
        return json.loads(s)
    except Exception:
        return {}
def latest(v):
    # awsbench serves a JSON ARRAY of cumulative rows; the pilot gen
    # serves one object. Normalize to the latest row either way.
    if isinstance(v, list):
        return v[-1] if v else {}
    return v if isinstance(v, dict) else {}
g = latest(safe('''$g''')); ab = safe('''$ab''')
print(json.dumps({"ts": int(ts), "i": int(i), "health": int(hc),
                  "origin": int(origin or 0), "phase": phase,
                  "gen": g if isinstance(g, dict) else {},
                  "absorb": ab if isinstance(ab, dict) else {}}))
PY
)
    # Never append an unparseable sample.
    if ! python3 -c "import json,sys; json.loads(sys.argv[1])" "$line" 2>/dev/null; then
      echo "sample $i unparseable — skipped" >&2
      sleep 5; continue
    fi
    echo "$line" >> "$OUT"
    VERDICT=$(python3 - "$OUT" "$TARGET_BYTES" "$BYTES_PER_OP" "$STALL" "$KILL_LINE_MB" "$PHASE" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
target_bytes, bpo, stall, kill_mb = (int(x) for x in sys.argv[2:6])
phase = sys.argv[6]
r = rows[-1]
gen = r.get("gen") or {}
ab = r.get("absorb") or {}
budget = ab.get("budget") or {}
absorber = ab.get("absorber") or {}
proc = ab.get("process") or {}
ok = gen.get("ok") or 0
accepted = ok * bpo
# Death: 6 consecutive dead samples (normalized health + no origin).
if sum(1 for x in rows[-6:] if x["health"] != 200 and not x.get("origin")) >= 6:
    print(f"FAIL_DEAD accepted={accepted}"); sys.exit()
# Error policy: the closed-loop gen retries deliberate shed; a nonzero
# errs count means NON-retryable failures leaked to the client.
errs = gen.get("errs", gen.get("errors"))
if errs:
    print(f"FAIL_ERRORS errs={errs} accepted={accepted}"); sys.exit()
# Invariant: declared reservation never exceeds capacity.
res = absorber.get("reservedBytes")
if budget and res is not None and res > budget.get("capacityBytes", 1 << 62):
    print("FAIL_INVARIANT reserved>capacity"); sys.exit()
# Cgroup headroom whenever the platform exposes it.
peak = proc.get("cgroupPeakMb")
if peak is not None and peak > kill_mb - 100:
    print(f"FAIL_HEADROOM cgroupPeak={peak}MB > killLine-100={kill_mb-100}MB"); sys.exit()
if proc.get("oomKillTotal"):
    print(f"FAIL_OOMKILL count={proc['oomKillTotal']}"); sys.exit()
# Stall legs: the injected delay must be visible in flush timing.
if phase == "load" and stall and len(rows) > 24:
    fl = [((x.get("absorb") or {}).get("absorber") or {}).get("lastFlushMs") or 0
          for x in rows[-24:]]
    if max(fl) < stall:
        print("FAIL_METRIC stall invisible in lastFlushMs"); sys.exit()
if phase == "load":
    if accepted >= target_bytes:
        print(f"TARGET accepted={accepted}"); sys.exit()
else:
    # RECOVERY: ingest quiet, absorption caught up, L0 declining,
    # spool drained.
    recov = [x for x in rows if x.get("phase") == "recovery"]
    if len(recov) >= 6:
        cur, prev = recov[-1], recov[-6]
        def a(x, k): return ((x.get("absorb") or {}).get("absorber") or {}).get(k) or 0
        ingest_quiet = a(cur, "ingestBytesTotal") == a(prev, "ingestBytesTotal")
        caught_up = a(cur, "absorbedBytesTotal") >= a(cur, "ingestBytesTotal") * 0.999
        def l0(x):
            parts = (x.get("absorb") or {}).get("historyPartitions") or []
            return max((p.get("l0SstCount") or 0 for p in parts), default=0)
        l0_ok = l0(cur) <= max(l0(prev), 2)
        spool = ((cur.get("absorb") or {}).get("telemetry") or {}).get("spool") or {}
        spool_ok = (spool.get("pendingRows") or 0) == 0
        idle = a(cur, "reservedBytes") == 0
        if ingest_quiet and caught_up and l0_ok and spool_ok and idle:
            print(f"PASS accepted={accepted}"); sys.exit()
print("CONTINUE")
PY
)
    case "$VERDICT" in
      CONTINUE) ;;
      TARGET*)
        echo "byte target reached ($VERDICT) — entering recovery phase"
        [ -n "$STOPCMD" ] && bash -c "$STOPCMD"
        clear_stall
        PHASE=recovery
        ;;
      PASS*)
        echo "LEG_PASS $NAME: $VERDICT (samples: $OUT)"
        echo "NOTE: run '$0 reconcile ...' for the exact-accounting check before recording PASS."
        exit 0 ;;
      *)
        echo "LEG_FAIL $NAME: $VERDICT — namespace and samples PRESERVED ($OUT)"
        exit 1 ;;
    esac
    sleep 5
  done
  echo "LEG_FAIL $NAME: TIMECAP in phase=$PHASE — samples preserved ($OUT)"
  exit 1
  ;;
reconcile)
  URL=$2; AUTH=$(cat "$3"); KEY=$(cat "$4"); PREFIX=$5
  STREAMS=${6:?streams}; EXPECTED_OK=${7:?expected_ok}; BATCH=${8:?batch}
  python3 - "$URL" "$AUTH" "$KEY" "$PREFIX" "$STREAMS" "$EXPECTED_OK" "$BATCH" <<'PY'
import json, sys, urllib.request
url, auth, key, prefix, streams, expected_ok, batch = sys.argv[1:8]
streams, expected_ok, batch = int(streams), int(expected_ok), int(batch)
expected_records = expected_ok * batch
total = 0
for s in range(streams):
    name = f"{prefix}-{s}"
    cursor = None
    while True:
        u = f"{url}/v1/streams/{name}/records?limit=1000"
        if cursor:
            u += f"&cursor={cursor}"
        req = urllib.request.Request(u, headers={
            "Authorization": f"Bearer {auth}",
            "prisma-encryption-key": key,
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.load(resp)
            total += len(body) if isinstance(body, list) else 0
            cursor = resp.headers.get("prisma-next-cursor")
            utd = (resp.headers.get("prisma-up-to-date") or "").lower() == "true"
        if utd or not cursor:
            break
print(f"reconcile: durable records={total} expected={expected_records} "
      f"({expected_ok} ok x {batch})")
sys.exit(0 if total == expected_records else 1)
PY
  ;;
*)
  echo "usage: $0 verify|leg|reconcile ... (see header)" >&2
  exit 1
  ;;
esac
