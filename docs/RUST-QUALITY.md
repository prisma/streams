# Rust quality-gate policy

**Status: adopted normative policy, 8 September 2026.** Scope: hand-written Rust and its verification tooling. MUST is a merge requirement; SHOULD requires a documented reason to deviate. Adoption is authorized by the repository owner's request. The implementation/migration record is in [quality/adoption.md](quality/adoption.md).

The objective is the structural approval bar in the [pinned Cursor review skill](../.agents/skills/thermo-nuclear-code-quality-review/SKILL.md): simpler state models, canonical ownership, fewer special cases and no gratuitous abstractions. A clean tool run cannot substitute for that review. The numerical limits below are this project's policy, not numerical requirements attributed to Cursor. The upstream skill's exact content is retained and [pinned](quality/review-skill-pin.json); both Codex and Cursor discover that same repository-local file.

## Toolchain and required entry point

Root `rust-toolchain.toml` MUST pin an exact release plus Clippy and rustfmt. Local checks and every CI Rust job MUST use that pin, never floating stable. Additional tools are version-pinned in `quality-tools.toml`; nightly tools have a separate dated pin. Toolchain or baseline migrations require an explicit review and new provenance, rather than an automatic update.

`scripts/quality.sh` is the common local/CI entry point for workflow linting, formatting, Clippy, dependency checks and architecture, multitenancy, scenario, inventory and evidence gates. Pinned actionlint MUST run without a file glob so both `.yml` and `.yaml` workflows are checked. Every subprocess failure MUST propagate, including commands in pipelines. The existing Rust, protocol, SDK and capacity jobs remain required. All workspace members, targets and supported feature configurations MUST be checked. New feature declarations require an explicit compatible matrix; mutually exclusive features MUST NOT be combined indiscriminately.

## Lint profile

Cargo's canonical profile is `[workspace.lints]`; every member MUST opt in with `[lints] workspace = true`. The root `clippy.toml` owns thresholds and type/method paths. These MUST be validated using the pinned compiler, including a failing example and legitimate control for configured type-dependent rules.

Rust denies `unsafe_op_in_unsafe_fn`, `unused_must_use` and `unfulfilled_lint_expectations`, and warns on `unreachable_pub`.

Clippy enables `all` at warning priority -2 and `correctness` at deny priority -1. The focused additional warnings are `excessive_nesting`, `too_many_lines`, `too_many_arguments`, `type_complexity`, `fn_params_excessive_bools`, `struct_excessive_bools`, `option_option`, `unnecessary_wraps`, `match_same_arms`, `let_underscore_future`, `let_underscore_must_use`, `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro`, `cast_possible_truncation`, `cast_sign_loss`, `redundant_clone`, `needless_pass_by_value`, `disallowed_methods` and `allow_attributes_without_reason`. `await_holding_lock`, `await_holding_refcell_ref`, `await_holding_invalid_type` and `undocumented_unsafe_blocks` are denied.

Thresholds are 100 function lines, five arguments, nesting depth four, one boolean parameter, three boolean fields and type complexity 200. Exported-API compatibility does not disable these structural checks. Test unwrap/expect/panic allowances apply only where Clippy identifies tests; they are not blanket production suppressions. Do not enable whole pedantic, restriction or nursery groups. Do not gate on cognitive complexity: Clippy itself cautions that it does not measure actual understandability.

`RunWindow` is a planning-only owner. It MUST be released before asynchronous scans; the disallowed-holding-type lint is an additional guard, not the ownership proof. Decoder and admission owners MUST additionally enable `indexing_slicing` and `arithmetic_side_effects`; lifecycle state machines MUST enable `wildcard_enum_match_arm`. The owner registry identifies their scope. Validated hot loops may use a narrow justified expectation; no global suppression is permitted.

Primitive spawning belongs only to registered task owners. The denylist covers Tokio free functions, runtime handles, local/blocking tasks, standard threads and `mem::forget`; it MUST also inventory JoinSet and builder APIs. A denylist alone does not prove lifecycle ownership. Runtime environment reads and process-wide state also require explicit owners. Process instrumentation/executors need their own named entries; there is no blanket runtime exemption.

## Diagnostics and exceptions

Run `cargo fmt --all -- --check` and `cargo clippy --locked --workspace --all-targets --message-format=json` for the default configuration, and each declared additional configuration. Fail every warning except a specifically approved legacy occurrence. Errors and denied lints ALWAYS fail, regardless of the legacy baseline. Once no legacy allowance remains, add `-- -D warnings`.

Diagnostic identity MUST include lint ID, normalized repository path, qualified item, source-occurrence fingerprint and multiplicity. Repeat compilations of the same source location are collapsed; distinct locations are not. A second identical warning needs a second allowance. Unknown diagnostics fail closed. Human-rendered `(message, file)` sets are not an acceptance mechanism.

The adoption inventory is frozen against commit `5bdaf9684197ff84bd544fd0fcd69520001ea196` and the pinned lint profile. Initial legacy debt is explicit; it is not a claim that the old code meets every new standard. Normal PRs MUST NOT regenerate or grow the baseline. Obsolete allowances MUST be removed, and subsequent merge-base comparisons MUST prevent removed debt from returning. The old historical gate anchors remain intact.

Exceptions MUST be statement/item-scoped `#[expect(clippy::lint_name, reason = "owner; invariant/issue; why the simpler alternative is wrong")]`. Narrow `allow(..., reason = "...")` is permitted when conditional compilation makes expectations unreliable. Test-only modules may exempt size/argument rules. Blanket lint-group suppression is forbidden. Unfulfilled expectations expose stale exceptions. Existing legacy exceptions are inventoried during adoption and cannot authorize new sites.

Never replace a deliberate poisoned-lock failure with silent recovery, swallow an error, or introduce a wrapper solely to satisfy a lint. A flags/options bag does not discharge argument complexity; a pass-through module does not discharge canonical ownership.

## Architecture requirements

The syntax-aware gate extends the existing architecture check and keeps its historical anchors. Merge-base comparisons and the adoption inventory provide a non-growing limit after a file has shrunk.

| Gate | Mandatory behavior |
| --- | --- |
| File growth | New hand-written `.rs` files MUST be at most 1,000 physical lines. Crossing from at/below 1,000 to above it is blocked. An existing oversized file MUST NOT grow without an owner-approved bounded exception. Tests count. Only explicitly generated, reproducible output is excluded. |
| Canonical boundaries | Preserve the ban on transport/state dependencies in extracted owners. Peer/wire adapters need explicit entries. Domain decisions MUST NOT be reconstructed from HTTP responses, JSON blobs or error-display strings. |
| Effect ownership | No new raw spawning, runtime environment reads or mutable process-global service state outside registered owners. Intentional process-wide metrics/executors get separate entries. |
| Proof-bearing types | Checked frames, validated postings and charged batches keep invariant fields private to their owner. Construction/mutation goes through that owner. Compiler fixtures MUST attempt external construction and mutation, with legitimate owner/API controls. |

Source rules use parsed Rust syntax. Type-dependent rules use actual compiler diagnostics. Each new gate MUST include violating fixtures and legitimate controls, including import aliases and relevant cfg variants. Unknown syntax/macros/import forms MUST be reported, never silently counted as compliant. Compiler coverage and any unresolved syntax inventory MUST be distinguished explicitly.

Splitting a transaction into context-heavy helpers, hiding flags in options bags or adding pass-through modules is not acceptance. Review the whole affected owner under the pinned skill, including opportunities to delete unnecessary state or layers.

## Verification selected by the changed invariant

| Trigger | Required verification and acceptance |
| --- | --- |
| Every PR | Run pinned `cargo machete` and `cargo deny check`. Fail unexplained unused dependencies, unapproved advisories, sources and licenses. Unknown Git/registry sources are denied; the existing exact-revision SlateDB source is explicitly allowed. Duplicate versions are reviewed signals, not a blanket ban. False positives require a rationale rather than automatic dependency deletion. |
| Codec, index or admission changes | Property tests exercise the production decoder/admission path with both valid and malformed data: zero counts, overflow, overlapping intervals and corruption. Run at least 1,024 cases for each affected property and commit minimized failures. Replay the saved fuzz corpus on relevant PRs; run bounded fuzzing with the pinned nightly on scheduled jobs. |
| Synchronization or retirement changes | Loom exercises the actual small state-transition implementation through instrumented primitives and records exploration bounds. Keep held-WAL and cancellation integration tests. Neither a rewritten model nor a bounded pass certifies the server. |
| Unsafe or low-level buffer changes | Run compatible affected unit tests under Miri with the dated nightly. Unsupported FFI/system paths remain explicitly untested. Retained buffers additionally assert backing-capacity/reservation accounting after subset selection, eviction and cancellation; Miri is not a memory-budget proof. |
| Changed critical invariants | Run `cargo mutants --in-diff <saved-diff>` against the actual PR merge base, with explicit owner/test scope. Require a passing unmutated baseline and a disposition for every surviving viable mutant. Timeouts and incomplete runs are not assertion-detected failures. Preserve exact discovered failures as regressions. |

Before requesting review, the author MUST identify the canonical owner, account for every warning/exception, provide a falsifiable regression for each changed invariant and explain what complexity was removed—or why further abstraction would be worse. Required test/protocol jobs and scoped external acceptance remain independent of the quality gate. These standards do not lift the existing performance, cryptographic, merge, deployment or evidence-upload holds.

## Primary references

- [Pinned Cursor skill](https://github.com/cursor/plugins/blob/2b8ae2ee306f823d54879d3da7f8496b73c31d5d/cursor-team-kit/skills/thermo-nuclear-code-quality-review/SKILL.md), content blob `ac76a2bc88bb2d895e83ab1788aa584a82346cfc`.
- [Cargo lint configuration](https://doc.rust-lang.org/cargo/reference/lints.html), [Clippy configuration](https://doc.rust-lang.org/clippy/configuration.html), [lint catalog](https://rust-lang.github.io/rust-clippy/master/index.html), [Rust diagnostic attributes](https://doc.rust-lang.org/reference/attributes/diagnostics.html).
- [cargo-machete](https://github.com/bnjbvr/cargo-machete), [cargo-deny sources](https://embarkstudios.github.io/cargo-deny/checks/sources/cfg.html), [dependency bans](https://embarkstudios.github.io/cargo-deny/checks/bans/cfg.html).
- [Proptest](https://proptest-rs.github.io/proptest/intro.html), [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html), [Loom](https://docs.rs/loom/latest/loom/), [Miri](https://github.com/rust-lang/miri), [cargo-mutants diff scope](https://mutants.rs/in-diff.html), [mutation timeouts](https://mutants.rs/timeouts.html).
