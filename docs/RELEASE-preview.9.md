# v0.2.0-preview.9 — provenance and evidence

## Provenance
- Tag commit: (this commit; see git tag v0.2.0-preview.9)
- SlateDB pin: 0717cc1e4e9bad10a4773760f66bac4264ecf05e (upstream slatedb)
- Release binary (x86_64-musl) sha256: 4e3e52acd3fb6aab88af018c1a0663969ec45130a012dc74c09000863b520d5f
- Build identity embedded: STREAMS_GIT_COMMIT via build.rs; verified in
  the field by verify-running.py (binary sha + git commit + build time
  + /readyz headers + boot-id consistency).

## Field evidence (three-commit form)
- **R27-4 incompressible capacity gate: PASS** —
  cap-20260813T085718Z-55853, binary 4da313a1 (commit 7acd6f01 era).
  Peak ledger 540 MB = 100.6% of cap, held; 16,613 typed maintenance
  sheds; drained 0.8 s; catch-up 1.545x; NO process exit across
  ~7.5 GB incompressible ingest; exact op-ledger reconcile OK; the six
  errors were origin-less platform-edge 502s, none landed.
- **R27-5 fleet handoff at peak backlog: PASS** — handoff-fh185257,
  binary 503bc59f (commit 5efc5790). SIGABRT at 365 MB durable backlog;
  every shard restored at >= 100% of pre-kill exactly (1.0017) in 37 s;
  catch-up under continued load; absolute drain 120->3 MB in 73 s;
  exact reconcile through the LB (all 2,879 kill-window ambiguous ops
  zero-landed). Kill-and-replace exercised (dead ordinal redeployed,
  LB refreshed).
- **Local latency-rig attribution chain** — toxiproxy 35 ms rigs
  reproduce SIN kills faithfully; final posture survived >7 GB with
  peak RSS 723 MB (was 886/OOM).

## Cost on the final profile
w10k A/B: +4.7% cost units vs upstream-default compaction (details in
CAPACITY-R27.md); LIST economy preserved.

## Release battery (this commit)
scripts/release-gate.sh: PASS (fmt, clippy vs cold baseline 228, suite
358/0, conformance + battery legs). Full suite green.

## OPEN platform blockers (explicitly gating GA, not this preview)
1. **Stale-build serving**: the platform can run a months-old binary
   under a new version. Application-side identity (digest + commit +
   boot id on /readyz and /v1/debug/load, verified by the campaign
   manifest) is our half; the platform must gate readiness on digest
   match and route only matching-ready versions.
2. **cgroup memory.peak**: exported by the binary but the sandbox does
   not expose cgroupfs; kernel-peak capture is platform-blocked.
3. Product-scope decisions (multitenant principals/quotas, invoice
   billing sign-off, SLO region policy, external security review)
   remain Phase 3/4 items per the readiness table.
