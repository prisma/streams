# Sourced by ladder.sh, d2run.sh and d4run.sh. A server in fleet mode
# splits no stream (the split gate, edge change #119), so a rung that
# needs splits takes them on streams-1 outside fleet mode, then forms the
# fleet over the same PATH_PREFIX before its order check:
#   ladder_solo [-f overlay.yml ...]   streams-1 alone, FLEET_PREFIX unset
#   ladder_fleet                       all three in fleet mode, every ring
#                                      lists all three
# Both append to $LOG when it is set.
LADDER_CD=${LADDER_CD:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
ladder_log() { if [ -n "${LOG:-}" ]; then cat >>"$LOG"; else cat >/dev/null; fi; }
ladder_solo() {
  (cd "$LADDER_CD" && docker compose stop streams-2 streams-3) 2>&1 | ladder_log
  (cd "$LADDER_CD" && docker compose -f compose.yml "$@" -f compose.solo.yml \
    up -d --force-recreate streams-1) 2>&1 | ladder_log
  for _ in $(seq 1 60); do
    curl -sf -o /dev/null -m 5 http://127.0.0.1:8101/health && break
    sleep 1
  done
  sleep 5
}
ladder_fleet() {
  (cd "$LADDER_CD" && docker compose up -d --force-recreate streams-1 streams-2 streams-3) 2>&1 | ladder_log
  for _ in $(seq 1 90); do
    ok=1
    for port in 8101 8102 8103; do
      curl -s -m 5 "http://127.0.0.1:$port/v1/debug/load" | python3 -c '
import json, sys
try:
    active = json.load(sys.stdin)["ring"]["active"]
except Exception:
    sys.exit(1)
sys.exit(0 if all(f"streams-{n}" in active for n in (1, 2, 3)) else 1)' || ok=0
    done
    [ "$ok" = 1 ] && return 0
    sleep 2
  done
  echo "ladder_fleet: the ring never listed all three servers" >&2
  return 1
}
