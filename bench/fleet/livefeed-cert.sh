#!/bin/bash
# Round-11.4: the REAL two-process LiveFeed fleet certification.
# Three streams-slate processes over one shared s3lite store, real
# TCP listeners, real fleet heartbeats/overrides, enforce-mode auth
# from the platform emulator, workload fleet identity, the livefeed
# engine — and the exact release binary. Produces
# target/livefeed-cert-manifest.json; exits nonzero on any failed leg.
set -euo pipefail
cd "$(dirname "$0")/../.."
cargo build --release --bin streams-slate --bin s3lite
exec node bench/fleet/livefeed-cert.mjs
