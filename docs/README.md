# Documentation map

Use this page to find the right document quickly. The repository tracks about
250 Markdown files, and most of `docs/` is dated campaign or review evidence.
This page sorts them into live references, gate inputs and historical records.
When sources disagree, trust them in this order: the code, then
[AGENTS.md](../AGENTS.md), then the live references below, then history. A
historical document records what was true on its date. Do not build or
operate from it.

Entries marked *(hidden)* are left out of default `rg` searches by the root
[`.ignore`](../.ignore). Read them by path, or search them with
`rg --no-ignore-dot`. The filter is reliable only when you search the root or
a single directory: with several path arguments, ripgrep applies it
intermittently.

## Start here

| Document | What it is |
|---|---|
| [AGENTS.md](../AGENTS.md) | The agents' operating manual: working agreement, environment, verification ladder, ratchets, formal and mutation recipes, commit records, known traps. It is loaded automatically. |
| [README.md](../README.md) | Product overview, the two HTTP surfaces (`/v1/streams` product, `/v1/stream` raw Durable Streams), quick start. |
| [SPEC.md](../SPEC.md) | Live architecture: decision log (§2), guarantees G1-G10 (§4), limitations. Where the API differs, the product-surface spec below wins (see its header). |
| [NEXT-WORK.md](reviews/2026-09-hardening/NEXT-WORK.md) | The queue of open work, in the owner's priority order. |

## Very large documents: read by anchor

Do not read these whole. Find the heading first, then read only that range
(`sed -n '<from>,<to>p' <file>`, or Read with an offset).

| Document | Size | Find a section with |
|---|---|---|
| [Formal-verification roadmap](PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md) | 268 KB, 3,040 lines | `rg -n '^### KANI-047' docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md`. §0 is the implementation record and §2 the mandatory agent behaviour. Its paths are relative to the repository root. |
| [edge-changes.md](reviews/2026-09-hardening/edge-changes.md) | 230 KB | `rg -n '^### #57 ' docs/reviews/2026-09-hardening/edge-changes.md`. The index table comes first, and records are grouped by risk, not by number. |
| [verification/manifest.json](../verification/manifest.json) | 186 KB | `rg -n '"id": "KANI-047"' verification/manifest.json` |
| [TLA+ group READMEs](../verification/tla/history/README.md) | 150 / 81 / 69 KB (history / durability / seal) | `rg -n '^### TLA-016' verification/tla/history/README.md` |
| [Product-surface spec, consolidated](../handover/prisma_streams_surface_spec_prelaunch_hard_cutover/PRISMA_STREAMS_PRODUCT_SURFACE_SPEC.md) | 140 KB | Read the matching chapter file `00-`..`09-` in the same directory instead. |
| [exception-audit-2026-09.md](quality/exception-audit-2026-09.md) | 95 KB | `rg -n '^## ' docs/quality/exception-audit-2026-09.md` (the owner decisions D1-D9 are under "Decisions for the owner") |
| [WIRE-MATRIX.md](refactor/WIRE-MATRIX.md) | 66 KB in 269 very long lines | `rg -n '^### 2\.7 ' docs/refactor/WIRE-MATRIX.md`, or search for the route |
| [RUNBOOK.md](../RUNBOOK.md) | 57 KB | `rg -n '^## ' RUNBOOK.md` |
| [verification/assumptions.md](../verification/assumptions.md) | 51 KB, 43 entries | `rg -n '^### ASM-' verification/assumptions.md` |

## Live references by topic

**Architecture and contracts**
- [SPEC.md](../SPEC.md): engine, storage, guarantees. [COMPUTE-SPEC.md](../COMPUTE-SPEC.md): the fleet, routing and storage tiers on Prisma Compute (cited by `src/fleet.rs`).
- [Product-surface spec](../handover/prisma_streams_surface_spec_prelaunch_hard_cutover/README.md): the API authority (pre-launch hard cutover; chapters 00-09).
- [ROUTING-V3.md](ROUTING-V3.md) (routing keys, segments, compact postings), [HISTORY-V2.md](HISTORY-V2.md) (shared partitioned history), [SCALING.md](SCALING.md) (segment split/merge, host rebalancing), [LIVE-FEED.md](LIVE-FEED.md) (SSE subscription contract), [MAINTENANCE-BACKPRESSURE.md](MAINTENANCE-BACKPRESSURE.md) (the shipped R25/R26 system).
- Application-owner contracts, cited from `src/` and the formal models: [append-transitions.md](append-transitions.md), [creation-transitions.md](creation-transitions.md), [seal-transitions.md](seal-transitions.md), [runtime-resources.md](runtime-resources.md), [crypto-frame-v4.md](crypto-frame-v4.md) (frame v4/v5 encryption).
- [PROVIDER-CONTRACT.md](PROVIDER-CONTRACT.md): the object-store provider suite behind ASM-OBJSTORE-CAS.
- [OBSERVABILITY-BILLING.md](OBSERVABILITY-BILLING.md): the normative billing, usage and telemetry design.
- [read-experiments/final-disposition.md](read-experiments/final-disposition.md): reads have one permanent production path, with no runtime switches.
- [CONFORMANCE.md](../CONFORMANCE.md): the pinned Durable Streams suite. [sdk/README.md](../sdk/README.md): the TypeScript SDK.

**Wire contract, edge changes and decisions**
- [refactor/WIRE-MATRIX.md](refactor/WIRE-MATRIX.md): the per-route wire contract, first characterized at 685ea035. Update it with every client-visible change. Its `file:line` references are from 685ea035 and have drifted, so find code by symbol.
- [edge-changes.md](reviews/2026-09-hardening/edge-changes.md): before/after records #1-#60. Each new edge change adds a record in the same format.
- [reviews/2026-09-hardening/README.md](reviews/2026-09-hardening/README.md): the hardening acceptance record, the owner's decisions from the second external review (2026-09-25), the split-500 release hold and the deployment gates.
- [effective-config-diff.md](reviews/2026-09-hardening/effective-config-diff.md): rc.4 vs HEAD effective configuration for each deployment family. This is a deploy prerequisite.
- [config-simplification.md](reviews/2026-09-hardening/config-simplification.md): the audit of every setting the binary reads (2026-09-29) and the five packages of simplifications proposed to the owner. Its per-setting detail is `reviews/2026-09-hardening/evidence/config-audit-2026-09-29/detail.md` (170 KB, hidden from `rg`; read one item by its `#### ` heading).

**Operations and release**
- [RUNBOOK.md](../RUNBOOK.md): build, configure, deploy, monitor, debug. It is compiled into the binary (`include_str!` in `src/operator.rs`), so an edit changes the build.
- [OPERATIONS.md](../OPERATIONS.md) (durability dependencies, backup, identity, SLOs), [SECURITY.md](../SECURITY.md), [OPS-RELEASE.md](OPS-RELEASE.md) (standing release policies and the fork ledger).
- [STAGING.md](STAGING.md) (staging plan, not yet executed), [GUIDE-COMPOSER.md](GUIDE-COMPOSER.md) (standalone deploy with Prisma Composer), [deploy/README.md](../deploy/README.md) (Compute wrapper apps), [deploy/cell-admin/README.md](../deploy/cell-admin/README.md) (the operator guide of a single-server shared cell: profile, admission, tokens, offboarding).

**Multitenancy**
- [MULTITENANCY.md](MULTITENANCY.md): the frozen shared-cell contract (revision 5). [CONTROL-PLANE-INTEGRATION.md](CONTROL-PLANE-INTEGRATION.md) is the proposed platform contract, and [platform-demo/README.md](../platform-demo/README.md) is its executable emulator (used by `scripts/platform-e2e.mjs`).
- The conversion is enforced by `scripts/multitenancy-audit.sh` against `scripts/mt-audit-baseline.txt`. The old map is historical (below).

**Formal verification (Kani and TLA+)**
- [verification/README.md](../verification/README.md): toolchain setup, commands, receipts, what stales what.
- [manifest.json](../verification/manifest.json) (the implemented obligations) and [assumptions.md](../verification/assumptions.md) (the assumption ledger).
- Group mappings: [seal](../verification/tla/seal/README.md), [durability](../verification/tla/durability/README.md), [history](../verification/tla/history/README.md). Kani harnesses live next to their owners as `src/**/proofs.rs`. Regression seeds are in `verification/regressions/<ID>/`.
- [The roadmap](PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md) is the full planned catalog, not a set of results. Read it by anchor.

**Quality policy**
- [RUST-QUALITY.md](RUST-QUALITY.md): the normative policy, including exceptions and exception growth. The pinned review skill is [SKILL.md](../.agents/skills/thermo-nuclear-code-quality-review/SKILL.md) (hash-pinned in `quality/review-skill-pin.json`; never edit it).
- [quality/exception-audit-2026-09.md](quality/exception-audit-2026-09.md): the exception-reason audit and owner decisions D1-D9.

**Testing**
- [DST.md](DST.md): the simulation model, invariants and fault model, and how to run it. [src/dst/tests/README.md](../src/dst/tests/README.md) maps each contract to its DST module.
- [dst/DST-EXPANSION-SPEC.md](dst/DST-EXPANSION-SPEC.md) is the normative DST spec. [dst/SCENARIO-CATALOG.md](dst/SCENARIO-CATALOG.md) is a gate input (see below).
- Harness READMEs: [bench/soak](../bench/soak/README.md), [bench/costab](../bench/costab/README.md) with [COST-METHODOLOGY.md](COST-METHODOLOGY.md), [bench/livefeed-perf](../bench/livefeed-perf/README.md), [bench/sse-probes](../bench/sse-probes/README.md), [bench/docker/harness](../bench/docker/harness/README.md).

## Gate inputs: edit only through their tools

The gates read these files. A hand edit either fails a gate or weakens it.

| Path | Read by | How it changes |
|---|---|---|
| `docs/quality/legacy-diagnostics{,-linux}.json` *(hidden)*, `legacy-source.json`, `syntax-fragments.json` | `scripts/quality/gate.py`, `source_gate.py` | Never. Each is sha256-pinned in `docs/quality/policy.json`. |
| `docs/quality/policy.json` | `gate.py`, `source_gate.py` | Owner decision only. |
| `docs/quality/owners.json` | `source_gate.py` | Hand-add a reasoned row in place (the file is not sorted). |
| `docs/quality/source-allowances.json`, `diagnostic-allowances{,-linux}.json` | `source_gate.py`, `common.py` | Shrink only: `. scripts/dev/env.sh && python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl --prune`, after a successful `scripts/quality.sh` clippy step on the current tree. |
| `docs/quality/exception-growth.json` | `source_gate.py`, `source_rules.py` | Owner-approved rows only. An agent proposes a row and never adds one. |
| `docs/quality/review-skill-pin.json` | `scripts/quality/config.py` | Never. It pins the review skill. |
| `docs/refactor/architecture-policy.json` | `scripts/architecture-gate.py`, `source_gate.py` | Budget exceptions and SSE core files are owner decisions. |
| `docs/refactor/architecture-review-baseline.json` | `architecture-gate.py` | Never. Hash-pinned; `--capture-review-baseline` refuses to overwrite it. |
| `docs/refactor/architecture-baseline.json` | `scripts/architecture-report.py` | Never. The WP-00 baseline. |
| `docs/refactor/test-inventory.json` | `scripts/test-inventory.py`, `gate.sh`, CI | `python3 scripts/test-inventory.py --write` in the reviewed commit that changes a test. |
| `docs/refactor/test-scenario-map.json`, `docs/dst/SCENARIO-CATALOG.md` | `scripts/scenario-map-report.py --check`; the map also by `test-inventory.py` and `review-evidence.py` | By hand. IDs and status labels must match between the two files. |
| `docs/refactor/SCENARIO-MAP.md` | generated | `python3 scripts/scenario-map-report.py` without `--check` rewrites it. |
| `docs/refactor/review-mechanisms.json`, `scenario-dispositions.json`, `clippy-review-dispositions.json`, `review-unit-relocations.json` | `scripts/review-evidence.py --check` | Re-pin by hand after editing a pinned test. |
| `docs/refactor/test-inventory-before.json`, `test-relocations.json`, `test-adaptations.json`, `test-additions.json` | `test-inventory.py --compare` (evidence only) | Never. Frozen extraction evidence. |
| `scripts/mt-audit-baseline.txt` | `scripts/multitenancy-audit.sh` | `bash scripts/multitenancy-audit.sh --regen`, in the commit that moves pinned lines. |
| `verification/manifest.json` | `scripts/quality/formal.py` | By hand. Any edit makes CI run every obligation. |
| `verification/receipts/*.json` | `formal.py check` | Only `formal.py run ... --record` writes them ([commands](../verification/README.md#commands)). |
| `RUNBOOK.md` | `include_str!` in `src/operator.rs`; `scripts/quality/compiler_fixtures.py` | Normal edits, but each one changes the server binary. |
| `conformance/{package,expected}.json` | the `src/protocol_pin.rs` test (`include_str!`) | Change them only together with the protocol pin. |

## Historical record

These documents record what happened and must not be edited to match today.
Each entry says what supersedes it, where something does.

**Root**
- [DESIGN.md](../DESIGN.md): the July rewrite design (it still names slatedb 0.14). Superseded by SPEC.md.
- [VERIFICATION.md](../VERIFICATION.md): July 2026 results from live Tigris tests (the SPEC §8 V-items, `src/bin/verify.rs`). This is not formal verification, which is [verification/](../verification/README.md).
- [REPORT.md](../REPORT.md) and [EXPERIMENT-PILOT.md](../EXPERIMENT-PILOT.md): the pilot and hardening record from July, both self-labelled HISTORICAL. EXPERIMENT-PILOT is cited from `src/bootstrap.rs`, a formal input, so it stays in place.
- [PER-KEY-ORDERING.md](../PER-KEY-ORDERING.md): the static per-key segment design, replaced by ROUTING-V3 (see its §0). It is cited from `src/offsets.rs`, a formal input, so it stays in place.
- [AUTOSCALING-DESIGN.md](../AUTOSCALING-DESIGN.md) (a scaling-group proposal to Compute), [PLATFORM-EDGE-REPORT.md](../PLATFORM-EDGE-REPORT.md) (edge admission concurrency), [AWS-readyness.md](../AWS-readyness.md) (the slate-codex disposition), [BENCHMARKS.md](../BENCHMARKS.md) (the rewrite against the old Bun server). All are from July 2026.
- [handover-plan.md](../handover-plan.md): the July product-surface handover. Superseded by [RELEASE-PRODUCT-SURFACE.md](RELEASE-PRODUCT-SURFACE.md) and the code.
- `codereview1.md` *(hidden)*: the WP-00 restructuring work package, written against 685ea035 (192 KB). It was executed through `docs/refactor/`.

**Field campaigns and reports in `docs/`**
- Soaks: SOAK-REGIONS (the 2026-07-26 baseline), SOAK5/6/7/9-REPORT, SOAK-R25H-REPORT (the corrected six-region soak, 2026-08-11), and PUMP-AB-REPORT (the pump-versus-tick A/B on Tigris in eu-central-1, 2026-09-30: the field evidence for edge record #77).
- Capacity and chaos: CAPACITY-R26 and CAPACITY-R27 (the OOM fix and its gate). CHAOS-CAMPAIGN is superseded in part by CHAOS-R23, CHAOS-R23's R23-1 by CHAOS-R24, and CHAOS-R24 by [MAINTENANCE-BACKPRESSURE.md](MAINTENANCE-BACKPRESSURE.md).
- Cost: COST-AB1, COST-WIDE1, COST-WIDE2, COST-CAMPAIGN-1 and COST-CAMPAIGN-2. Their `ABSORB_CONCURRENCY` is a retired knob; the current controls are in [RUNBOOK §3.2](../RUNBOOK.md#32-engine-shard-log).
- Fleet, performance and platform: FLEET-CAMPAIGN, PERF-LIVEFEED (round 12), PLATFORM-SIN-404-REPORT and its -VERIFICATION (fixed 2026-07-30), TIGRIS-404-COST, TIGRIS-REGION-CENSUS, and BUCKETS-SINGLE-REGION (its finding still holds: a bucket inherits its project's region).
- Releases and verdicts: RELEASE-preview.9, RELEASE-rc.1, R30-RESPONSE and RELEASE-PRODUCT-SURFACE (the product-surface release gate). [READINESS.md](READINESS.md) is the launch matrix as of R30 (2026-08-14); the newer 2026-09 hardening decisions withhold production approval. OBSERVABILITY-BILLING-STATUS is the implementation matrix as of 2026-08-09.
- `MULTITENANCY-MAP.md` *(hidden)*: the August conversion map (190 KB). Its line anchors have drifted; the audit script replaces it.

**Review programs**
- R-review remediation against a7e2070f (September 6): [review-resolution.md](review-resolution.md), the ledger of 24 findings, plus eleven `review-*-evidence.md` files beside it.
- [review-followup/](review-followup/README.md): follow-up ledgers and contract notes R03-A to R24-A from September 6-7. The code is now authoritative.
- [read-experiments/](read-experiments/results.md): experiments O1-O5, their ledgers and results, and `followup/`, whose `*.json` measurements are *(hidden)*. Only final-disposition.md is live. The reproduction harness is `scripts/read-experiments/followup/`, and its three `.rs` templates are sha256-pinned in `docs/quality/syntax-fragments.json`: never edit or move them.
- [refactor/BASELINE.md](refactor/BASELINE.md) and [refactor/COMMIT-ORDER.md](refactor/COMMIT-ORDER.md): WP-00 deliverables. Their line references are stale.
- [quality/adoption.md](quality/adoption.md), [dependencies.md](quality/dependencies.md) and [pr19-merge-review.md](quality/pr19-merge-review.md): the quality adoption and PR #19 records (2026-09-08/09). The standing holds they record still apply. `quality/verification.json` and `linux-adoption-receipt.json` are adoption-time receipts that no gate reads.
- `reviews/2026-09-hardening/plans/` *(hidden)*: 77 implementation plans in `plans`..`plans20`. Each names the tree it was written against.
- `reviews/2026-09-hardening/evidence/` *(hidden)*: the raw effective-config evidence, the failed capacity-leg gate log, the split investigation and the refused-chain probe.
- `reviews/2026-09-hardening/report/` *(hidden)*: the end-of-batch report at 24c4c77a (ledger, decisions, remaining, process, external-review claims, report.html). `reviews/2026-07-27-perf-diff.patch` is also *(hidden)*.
- [dst/STATUS.md](dst/STATUS.md) (2026-08-22) and [dst/IMPLEMENTATION-PLAN.md](dst/IMPLEMENTATION-PLAN.md) (2026-08-02): the DST expansion status and plan. [history/](history/README.md): removed designs such as the queue and state profiles.

**Benchmarks and platform repros outside `docs/`**
- `bench/`: WORKLOAD-CERT-PLAN (2026-08-19), aws-comparison-plan, sinmax-report, mt-tenants-report, tigris-observatory-report-1, tigris-message, fra-ab-baseline, docker/LADDER-LOG and `results-2026-07-22/`.
- Platform tickets: [repro-edge-404/](../repro-edge-404/README.md) and [repro-no-restart/](../repro-no-restart/README.md) (both cited by RUNBOOK) and [bench/edge-repro/](../bench/edge-repro/README.md). `charts/` holds the figures for EXPERIMENT-PILOT and REPORT.
