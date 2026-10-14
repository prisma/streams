#!/bin/bash
# The CI evidence of a release candidate, read by one rule (the owner's
# round-3 call 8, 2026-10-13): every attempt of every required job in the
# push runs at the commit succeeded. scripts/promote-rc.sh (step 1, before
# it mints the tag) and scripts/publish-sdk.sh (step 1, before it
# publishes) both run it, so publishing reads exactly what certified.
#
#   scripts/required-checks.sh <sha>
#
# * Push runs only (`event=push`): ci.yml and rust-quality.yml also run
#   nightly on slate's head, so an RC that stays the head gains a scheduled
#   run every night. Those runs are not its evidence: a red nightly neither
#   blocks nor certifies it. workflow-lint's `actionlint` is a required job
#   of its own workflow, so the query is over every workflow's push runs; a
#   workflow GitHub cannot parse runs no job, and its names are missing.
# * Every attempt (`filter=all`): the jobs API answers only a run's latest
#   attempt by default, so a green rerun would hide a red attempt. A red
#   attempt refuses; such a commit needs a new commit, not a rerun.
# * Every page (`--paginate`): gh refuses `--slurp` with `--jq`, so the
#   slurped pages go to Python whole.
# The Actions API lists only this repository's workflows, so no other
# app's check run can satisfy a required name. Prints `ok   <name>` per
# required job and ends `REQUIRED_CHECKS_GREEN <sha>: <names>`; otherwise a
# `FAIL:` line per missing name and per attempt that did not succeed, then
# `REQUIRED_CHECKS_FAIL <sha>`, and exits 1.
set -euo pipefail
SHA=${1:?usage: required-checks.sh <sha>}
REQUIRED_CHECKS=(rust livefeed livefeed-fleet-cert platform-e2e
  mt-cert-1000 durable-streams-server-conformance product-field-gate sdk-package
  actionlint)
DIR=$(mktemp -d "${TMPDIR:-/tmp}/required-checks.XXXXXX")
trap 'rm -rf "$DIR"' EXIT
gh api "repos/{owner}/{repo}/actions/runs?head_sha=$SHA&event=push&per_page=100" \
  --paginate --slurp > "$DIR/runs.json"
RUN_IDS=$(python3 -c '
import json, sys
for page in json.load(open(sys.argv[1])):
    for run in page["workflow_runs"]:
        print(run["id"])' "$DIR/runs.json")
for id in $RUN_IDS; do
  gh api "repos/{owner}/{repo}/actions/runs/$id/jobs?filter=all&per_page=100" \
    --paginate --slurp > "$DIR/jobs-$id.json"
done
python3 - "$SHA" "$DIR" "${REQUIRED_CHECKS[@]}" <<'PY'
import json, sys
from pathlib import Path
sha, folder, required = sys.argv[1], Path(sys.argv[2]), sys.argv[3:]
runs = [run for page in json.loads((folder / 'runs.json').read_text()) for run in page['workflow_runs']]
jobs = [(run, job) for run in runs
        for page in json.loads((folder / f"jobs-{run['id']}.json").read_text())
        for job in page['jobs']]
green = True
for name in required:
    mine = [(run, job) for run, job in jobs if job['name'] == name]
    bad = [(run, job) for run, job in mine
           if job['status'] != 'completed' or job['conclusion'] != 'success']
    if not mine:
        print(f'FAIL: {name}: no job of that name in the push runs at {sha}')
    for run, job in bad:
        where = f"attempt {job['run_attempt']} of push run {run['id']} ({run['path']})"
        if job['status'] != 'completed':
            print(f"FAIL: {name}: {where} is {job['status']}")
        else:
            print(f"FAIL: {name}: {where} concluded {job['conclusion']} "
                  f"(gh run view {run['id']} --attempt {job['run_attempt']} --log-failed)")
    if mine and not bad:
        print(f'ok   {name}')
    green = green and bool(mine) and not bad
if not green:
    print(f'REQUIRED_CHECKS_FAIL {sha}')
    sys.exit(1)
print(f"REQUIRED_CHECKS_GREEN {sha}: {' '.join(required)}")
PY
