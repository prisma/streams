#!/usr/bin/env bash
# CI verdict for one pushed commit: the only evidence for "CI is green".
#
#   scripts/dev/ci-status.sh [SHA]          # default HEAD; prints each workflow
#   scripts/dev/ci-status.sh [SHA] --wait   # blocks until every run finishes
#
# A push to slate runs three workflows: ci.yml, rust-quality.yml and
# workflow-lint.yml. Exit 0 only when all three have a run for exactly this
# commit and each concluded success; a run still in progress is not
# evidence. Runs are selected by workflow file, not display name (GitHub
# names a workflow from the default branch). Run --wait in the background
# (rust-quality takes 12 min to 4 h); read failures with
# `gh run view <id> --log-failed` before any rerun, and report every rerun.
set -euo pipefail
wait_for_runs=false
rev=HEAD
for arg in "$@"; do
  if [[ "$arg" = --wait ]]; then wait_for_runs=true; else rev=$arg; fi
done
sha=$(git rev-parse "$rev")
workflows=(ci.yml rust-quality.yml workflow-lint.yml)
# One line per workflow file: file, run id, status, conclusion, url (the
# newest run of that file for this commit; empty when there is none yet).
run_of() {
  gh run list --workflow "$1" --commit "$sha" --limit 5 \
    --json databaseId,status,conclusion,url \
    --jq ".[0] | select(. != null) | \"$1\t\(.databaseId)\t\(.status)\t\(.conclusion)\t\(.url)\""
}
if $wait_for_runs; then
  for _ in $(seq 1 30); do
    missing=0
    for workflow in "${workflows[@]}"; do
      [[ -n "$(run_of "$workflow")" ]] || missing=1
    done
    (( missing == 0 )) && break
    sleep 10
  done
  for workflow in "${workflows[@]}"; do
    line=$(run_of "$workflow")
    [[ -n "$line" ]] || continue
    if [[ "$(printf '%s' "$line" | cut -f3)" != completed ]]; then
      gh run watch "$(printf '%s' "$line" | cut -f2)" --interval 60 > /dev/null 2>&1 || true
    fi
  done
fi
echo "commit $sha"
ok=true
for workflow in "${workflows[@]}"; do
  line=$(run_of "$workflow")
  if [[ -z "$line" ]]; then
    echo "CI_PENDING: no $workflow run for this commit yet"
    ok=false
    continue
  fi
  printf '%s\n' "$line" | awk -F'\t' '{ printf "  %-18s %-12s %-10s %s\n", $1, $3, $4, $5 }'
  status=$(printf '%s' "$line" | cut -f3)
  conclusion=$(printf '%s' "$line" | cut -f4)
  if [[ "$status" != completed ]]; then
    echo "CI_PENDING: $workflow is $status"
    ok=false
  elif [[ "$conclusion" != success ]]; then
    echo "CI_FAIL: $workflow concluded $conclusion (gh run view $(printf '%s' "$line" | cut -f2) --log-failed)"
    ok=false
  fi
done
if $ok; then
  echo CI_GREEN
else
  exit 1
fi
