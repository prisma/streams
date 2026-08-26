#!/bin/bash
# Round-11.6: the LiveFeed field-canary battery (livefeed-canary-rc1).
# Runs the RELEASE-POSTURE binary with the pinned 1-GiB profile
# (deploy/profiles/compute-1g.env) on a real 3-instance fleet, then
# drives the Stage-8 canaries: A (1000x1), B (500x2), C (10x100) and
# the failure campaigns (owner movement, blackholed owner, widened
# seal-publication window, cross-project retention pressure, largest
# LEGAL record incl. worst text framing). Produces
# target/livefeed-canary-manifest.json; exits nonzero on any failed leg.
set -euo pipefail
cd "$(dirname "$0")/../.."
cargo build --release --bin streams-slate --bin s3lite
exec node bench/canary/livefeed-canary.mjs
