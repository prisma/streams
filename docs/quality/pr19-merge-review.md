# PR #19 final tooling corrections and integration decision

On 9 September 2026, the repository owner requested: “Address the two findings
in this review. Then merge the PR. Then address all the 3524 inherited warnings
in individual chunks as PRs on slate branch.” This explicitly authorizes source
integration after Q01/Q02 and final validation. It supersedes the earlier draft
and merge hold. It does not grant performance parity, deployment, cryptographic
or raw-evidence publication acceptance.

The supplied final review of `892d7bb96fb17533888b05f56ec469266a1bdc30`
accepts the permanent O1–O4 implementations, O5 removal, compatible scan-end
omission and mandatory physical identity at source/wire-contract scope. This
follow-up changes tooling and documentation only; the accepted runtime remains
`cf209dacc4915141a01fb63acc3802de4f612faf`.

| Finding | Correction | Executed controls |
| --- | --- | --- |
| Q01 | `23f4d00`: restore explicit `openssl` and `native-tls` package bans; accurately describe the two maintenance-advisory exceptions; configuration regression protects each unconditional ban. | cargo-deny 0.20.2 accepts the locked workspace. Separate temporary openssl 0.10 and native-tls 0.2 graphs fail with `banned` diagnostics naming the intended packages. A rustls 0.23 graph passes. The workspace lock contains neither package nor `openssl-sys`. |
| Q02 | `79d33df`: bare actionlint in the common entry point; version/missing-tool checks; checksum-pinned installer wired into local setup and the common CI job. Independent workflow-lint CI retained. | Real actionlint discovers and rejects malformed `.yml` and `.yaml` controls separately and accepts valid controls. Each malformed extension also fails `scripts/quality.sh` with `QUALITY_FAIL: workflow lint (actionlint)`. Missing/wrong-version controls pass. |

The Python suite now has 16 cases. Tooling controls do not substitute for final
source tests: `scripts/gate.sh` and every ordinary job in `ci`, `rust-quality`
and `workflow-lint` must succeed on the final correction/report revision before
merge. [PR checks](https://github.com/prisma/streams/pull/19/checks) retain exact
commit and job identities; scheduled-only campaigns are distinguished from
ordinary jobs. Raw logs and temporary dependency graphs remain local under the
existing evidence-upload hold.

The PR title/body now describe permanent read ownership/postings validation,
removed O5, peer compatibility and adopted quality gates. Cache gains in the
historical experiment report are not performance claims for the current tree.

## Integration boundary

Repository API inspection on 9 September reports that `slate` is unprotected,
there are no repository rulesets, zero repository webhooks, and no GitHub
deployment records. The three checked-in workflows perform validation; none
publishes or deploys a release. The documented Prisma Compute deployment paths
use explicit deployment commands. No deployment action is part of this work.
These observations establish the repository-side integration boundary; they do
not attest to an independently configured external system absent from the
repository/API. Because GitHub does not enforce required statuses on `slate`,
all ordinary jobs are checked explicitly before the authorized merge. Branch
protection settings are not silently changed.

Use a normal merge, preserving the lint-adoption anchor and source-pinned
experiment ancestors. The merge base inspected for this review remains
`dac9e908ec7bbf50d9b6479ce27991d198269bfb`. The owner retains the separate
acceptance decisions for workload-specific performance, independent crypto,
actual release-binary rolling upgrade/drain/rollback, deployed fleet behavior
and raw evidence. Local tests and source integration do not discharge them.

## Inherited warning series

The cleanup starts after PR #19 integration, from 3,524 occurrence-level
allowances on each supported host. Each focused PR targets `slate`, fixes the
source cause, prunes corresponding active allowances and validates the affected
owner. Immutable adoption inventories and lint thresholds remain unchanged.
No blanket suppression or replacement runtime experiment is part of this work.
