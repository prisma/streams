#!/bin/bash
# L6 spike: a token-refresh guard for one rig point (bench/k2cost/run-local.sh).
#
# The rig re-mints its bearer token every 240 s in a `while sleep 240` loop
# and tokens live 600 s. A SIGTERM to that `sleep` from outside the rig ends
# the loop, and every request 600 s after the last mint gets `401 expired`:
# that voided the first unmodified identity run (2026-10-11 ~13:25Z, half of
# the load refused). This guard acts only when the rig's own refresh has
# stopped: every 30 s it looks at the rig's secrets directory for this run
# (the newest $K2/secrets/L6S-* created after the guard started); when its
# `headers` file is older than 330 s it re-mints exactly as the rig's
# mint_headers does (credential exchange at the emulator, atomic replace of
# the headers file) and logs one line. It never reads or prints the token
# beyond writing the headers file the rig itself writes. It exits once the
# rig has deleted the secrets directory (teardown), or when killed.
#
#   token-guard.sh <guard-log>
set -u
K2=$HOME/.streams-k2
GLOG=${1:?usage: token-guard.sh <guard-log>}
EMU=http://127.0.0.1:9580   # run-local.sh EMU_PORT
START=$(date +%s)
glog() { echo "[token-guard $(date -u +%H:%M:%S)] $*" >> "$GLOG"; }
S=""
glog "started; waiting for a secrets dir newer than $START"
while :; do
  if [ -z "$S" ]; then
    for d in $(ls -dt "$K2"/secrets/L6S-* 2>/dev/null | head -3); do
      if [ "$(stat -f %m "$d")" -ge "$START" ]; then S=$d; glog "watching $(basename "$d")"; break; fi
    done
  elif [ ! -d "$S" ]; then
    glog "secrets dir gone (rig torn down); exiting"
    exit 0
  elif [ -f "$S/headers" ] && [ -f "$S/exchange.hdr" ]; then
    age=$(( $(date +%s) - $(stat -f %m "$S/headers") ))
    if [ "$age" -gt 330 ]; then
      tok=$(curl -sf -X POST -H @"$S/exchange.hdr" "$EMU/v1/token/streams" | jq -r .accessToken) || tok=""
      if [ -n "$tok" ] && [ "$tok" != null ]; then
        ( umask 077
          printf 'authorization: Bearer %s\nprisma-encryption-key: %s\n' "$tok" "$(cat "$S/stream_key")" > "$S/headers.guard.tmp" )
        mv -f "$S/headers.guard.tmp" "$S/headers"
        glog "INTERVENED: headers were ${age} s old (the rig's refresh loop is dead); re-minted"
      else
        glog "headers ${age} s old; re-mint FAILED"
      fi
      unset tok
    fi
  fi
  sleep 30 || true
done
