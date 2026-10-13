#!/bin/bash
# Deploy a release to a shared cell's stage (public-beta deploy-q5 triage
# B5, design §2.3): the only way a release reaches a cell. A rollback is
# this command with the previous release directory.
#
#   PRISMA_TOKEN_FILE=F deploy/composer/deploy-release.sh <stage> <release-dir>
#     --project ID [--branch ID] --workspace ID --state DIR|s3://...
#     [--s3-credentials F] [--cell-url URL --bearer-file F
#     --canary-token-file T [--restart]] [--management-api URL]
#     [--issuer-release-dir DIR --issuer-project ID [--issuer-branch ID]]
#   PRISMA_TOKEN_FILE=F deploy/composer/deploy-release.sh <scratch|cert>
#     <release-dir> --project ID --branch ID --workspace ID --destroy
#
# <stage> is production (the app's default stage) or scratch or cert (named
# stages: Branches of the project, --branch). <release-dir> is one
# scripts/cut-release.sh made, in the directory of its tag; deploy-log.jsonl
# beside it records every deploy. --state (and --s3-credentials) is the
# cell's cell-admin state; --cell-url, the service's address, makes the run
# read the running cell's identity first and cell-check it after. The
# Management API token is read from the file PRISMA_TOKEN_FILE names.
#
# Refused, in this order, before anything deploys: a release directory
# that is not exactly what was cut (a dotfile, a stray, a changed byte); a
# secret the release's app binds set in this shell's environment
# (set-secrets is the only path, D10); a stage the release carries no
# cells file for; KEEP_AWAKE_UNTIL_MS in production's; a cells file or
# profile the release's own entry would refuse; a --project or --branch
# other than the one `prisma deploy` picks for the app; a project outside
# the cells file's REGION; a secret name missing from the stage's own
# scope; a release whose layout_version, keys_contract or
# MAX_REQUEST_BODY_BYTES (each under its own release's rules) differs
# from the running cell's release (the cell's /v1/debug/load sha256,
# found among the releases beside this one; or, when no cell answers, the
# release deploy-log.jsonl last deployed there; neither is "no cell yet",
# accepted); and profiles
# `cell-admin check-sizing` refuses. Then the release's own pinned `prisma
# deploy`, from the release root, then cell-check within 5 minutes. The
# deploy's CELL_DEPLOY_NONCE is the release's tag, or the nonce
# deploy-log.jsonl recorded for the stage's last live deploy when that was
# this release, so running a deploy again is the same input (a no-op).
#
# --issuer-release-dir (the interim issuer's <tag>-issuer release, Q6 (b))
# deploys the issuer's app in the same run, after the cell's deploy and
# cell-check passed: its release cut from the cell's commit and under the
# same rules, its stages/<stage>.env naming the cell's CELL_ID, its project
# in the region holding its nine secrets, all checked before the cell
# deploys; its settings are the envParams of its `prisma deploy`.
#
# --restart restarts the release the cell at --cell-url runs (refused when
# it runs another, or none answers): the same checks, a new nonce
# `<tag>.r<unix secs>` so the deploy is a new version of the same release,
# and a cell-check that passes only an instance whose x-streams-boot-id
# differs from the one that answered before (lead decision deploy-q5 #6
# (2)). It takes no --issuer-release-dir. A plain deploy after it keeps
# the restart's nonce.
#
# --destroy (scratch or cert only, never production) runs the release's
# destroy.ts instead: Composer's destroy of that named stage, whose
# buckets stay for `provision-cell.py <stage> --teardown`.
#
# Exit codes: 0 deployed and cell-check passed (or destroyed); 1 refused, or failed (the
# message says whether anything deployed); 2 usage; 3 deployed, cell-check
# not run (no --cell-url: run it with the service's address). The work is
# deploy-release.ts's; this name is the operator's command.
exec bun "$(dirname "$0")/deploy-release.ts" "$@"
