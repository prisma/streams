#!/bin/bash
# Deploy a release to a shared cell's stage (public-beta deploy-q5 triage
# B5, design §2.3): the only way a release reaches a cell. A rollback is
# this command with the previous release directory.
#
#   PRISMA_TOKEN_FILE=F deploy/composer/deploy-release.sh <stage> <release-dir>
#     --project ID [--branch ID] --workspace ID --state DIR|s3://...
#     [--s3-credentials F] [--cell-url URL --bearer-file F
#     --canary-token-file T] [--management-api URL]
#
# <stage> is production (the app's default stage) or scratch or cert (named
# stages: Branches of the project, --branch). <release-dir> is one
# scripts/cut-release.sh made, in the directory of its tag; deploy-log.jsonl
# beside it records every deploy. --state (and --s3-credentials) is the
# cell's cell-admin state; --cell-url, the service's address, makes the run
# read the running cell's identity first and cell-check it after. The
# Management API token is read from the file PRISMA_TOKEN_FILE names.
#
# Refused, in this order, before anything deploys: a secret the cell's app
# binds set in this shell's environment (set-secrets is the only path,
# D10); a release directory that is not exactly what was cut (a dotfile, a
# stray, a changed byte); a stage the release carries no cells file for;
# KEEP_AWAKE_UNTIL_MS in production's; a cells file or profile the entry
# would refuse; a project outside the cells file's REGION; a secret name
# missing from the stage's own scope; a release whose layout_version,
# keys_contract or MAX_REQUEST_BODY_BYTES differs from the running cell's
# release (the cell's /v1/debug/load sha256, found among the releases
# beside this one; or, when no cell answers, the release deploy-log.jsonl
# last deployed there; neither is "no cell yet", accepted); and profiles
# `cell-admin check-sizing` refuses. Then the release's own pinned `prisma
# deploy`, from the release root, then cell-check within 5 minutes.
#
# Exit codes: 0 deployed and cell-check passed; 1 refused, or failed (the
# message says whether anything deployed); 2 usage; 3 deployed, cell-check
# not run (no --cell-url: run it with the service's address). The work is
# deploy-release.ts's; this name is the operator's command.
exec bun "$(dirname "$0")/deploy-release.ts" "$@"
