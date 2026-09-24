# Rust quality-gate policy

**Status: adopted normative policy, 8 September 2026.** Scope: hand-written Rust and its verification tooling. MUST is a merge requirement; SHOULD requires a documented reason to deviate. Adoption is authorized by the repository owner's request. The implementation/migration record is in [quality/adoption.md](quality/adoption.md).

The objective is the structural approval bar in the [pinned Cursor review skill](../.agents/skills/thermo-nuclear-code-quality-review/SKILL.md): simpler state models, canonical ownership, fewer special cases and no gratuitous abstractions. A clean tool run cannot substitute for that review. The numerical limits below are this project's policy, not numerical requirements attributed to Cursor. The upstream skill's exact content is retained and [pinned](quality/review-skill-pin.json); both Codex and Cursor discover that same repository-local file.

## Toolchain and required entry point

Root `rust-toolchain.toml` MUST pin an exact release plus Clippy and rustfmt. Local checks and every CI Rust job MUST use that pin, never floating stable. Additional tools are version-pinned in `quality-tools.toml`; nightly tools have a separate dated pin. Toolchain or baseline migrations require an explicit review and new provenance, rather than an automatic update.

`scripts/quality.sh` is the common local/CI entry point for workflow linting, formatting, Clippy, rustdoc, dependency checks and architecture, multitenancy, scenario, inventory and evidence gates. Pinned actionlint MUST run without a file glob so both `.yml` and `.yaml` workflows are checked. Every subprocess failure MUST propagate, including commands in pipelines. The ordinary Clippy command ends with `-- -D warnings`; the empty active diagnostic ledgers are not a reason to leave warnings merely advisory. The existing Rust, protocol, SDK and capacity jobs remain required. Every job MUST declare `timeout-minutes`, at least twice its observed green maximum, so a hung leg fails instead of holding a runner for the six-hour default; every workflow MUST declare a `concurrency` group that cancels a superseded pull-request run and never a branch push or a scheduled run (each commit on `slate` keeps its own verdict). All workspace members, targets and supported feature configurations MUST be checked. New feature declarations require an explicit compatible matrix; mutually exclusive features MUST NOT be combined indiscriminately.

A gate leg that selects tests by filter or by `--exact` name MUST prove that it ran them. `cargo test` exits 0 and prints `test result: ok. 0 passed` when the selection matches nothing, so neither the exit code nor a search for `test result: ok` notices that a reviewed rename removed the test a leg names. Such legs run through `scripts/test-leg.sh`, which keeps the log and has `scripts/quality/tests_ran.py` judge it: every result line reads ok, the passed count reaches the leg's floor, and each named test has its own `... ok` line. The full suite's floor is the test inventory, less the capacity test that runs alone. A floor states that the selection still exists; it is not a test count to maintain.

## Lint profile

Cargo's canonical profile is `[workspace.lints]`; every member MUST opt in with `[lints] workspace = true`. The root `clippy.toml` owns thresholds and type/method paths. These MUST be validated using the pinned compiler, including a failing example and legitimate control for configured type-dependent rules.

Rust denies `unsafe_op_in_unsafe_fn`, `unused_must_use` and `unfulfilled_lint_expectations`, and warns on `unreachable_pub`.

Clippy enables `all` at warning priority -2 and `correctness` at deny priority -1. The focused additional warnings are `excessive_nesting`, `too_many_lines`, `too_many_arguments`, `type_complexity`, `fn_params_excessive_bools`, `struct_excessive_bools`, `option_option`, `unnecessary_wraps`, `match_same_arms`, `let_underscore_future`, `let_underscore_must_use`, `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro`, `cast_possible_truncation`, `cast_sign_loss`, `redundant_clone`, `needless_pass_by_value`, `disallowed_methods` and `allow_attributes_without_reason`. `await_holding_lock`, `await_holding_refcell_ref`, `await_holding_invalid_type` and `undocumented_unsafe_blocks` are denied.

Thresholds are 100 function lines, five arguments, nesting depth four, one boolean parameter, three boolean fields and type complexity 200. Exported-API compatibility does not disable these structural checks. Test unwrap/expect/panic allowances apply only where Clippy identifies tests; they are not blanket production suppressions. Do not enable whole pedantic, restriction or nursery groups. Do not gate on cognitive complexity: Clippy itself cautions that it does not measure actual understandability.

`RunWindow` is a planning-only owner. It MUST be released before asynchronous scans; the disallowed-holding-type lint is an additional guard, not the ownership proof. Decoder and admission owners MUST additionally enable `indexing_slicing` and `arithmetic_side_effects`; lifecycle state machines MUST enable `wildcard_enum_match_arm`. The owner registry identifies their scope. Validated hot loops may use a narrow justified expectation; no global suppression is permitted.

Primitive spawning belongs only to registered task owners. The denylist covers Tokio free functions, runtime handles, local/blocking tasks, standard threads and `mem::forget`; it MUST also inventory JoinSet and builder APIs. A denylist alone does not prove lifecycle ownership. Runtime environment reads and process-wide state also require explicit owners. Process instrumentation/executors need their own named entries; there is no blanket runtime exemption.

## Diagnostics and exceptions

Run `cargo fmt --all -- --check` and `cargo clippy --locked --workspace --all-targets --message-format=json` for the default configuration, and each declared additional configuration. Fail every warning except a specifically approved legacy occurrence. Errors and denied lints ALWAYS fail, regardless of the legacy baseline. Once no legacy allowance remains, add `-- -D warnings`.

Rustdoc is held to the same bar: `cargo doc --locked --workspace --no-deps --document-private-items` MUST pass under `RUSTDOCFLAGS='-D warnings'`. Private items are included because most of the crate is `pub(crate)`; a public-only build would leave nearly all of its prose unchecked. Byte layouts and keyspace diagrams belong in fenced `text` blocks, not in prose where `<name>` parses as HTML and `[name]` as a link. A public item names a private one in plain code, never as a link.

Diagnostic identity MUST include lint ID, normalized repository path, qualified item, source-occurrence fingerprint and multiplicity. Repeat compilations of the same source location are collapsed; distinct locations are not. A second identical warning needs a second allowance. Unknown diagnostics fail closed. Human-rendered `(message, file)` sets are not an acceptance mechanism.

The adoption inventory is frozen against commit `5bdaf9684197ff84bd544fd0fcd69520001ea196` and the pinned lint profile. Initial legacy debt is explicit; it is not a claim that the old code meets every new standard. Normal PRs MUST NOT regenerate or grow the baseline. Obsolete allowances MUST be removed, and subsequent merge-base comparisons MUST prevent removed debt from returning. The old historical gate anchors remain intact.

Exceptions MUST be statement/item-scoped `#[expect(clippy::lint_name, reason = "owner; invariant/issue; why the simpler alternative is wrong")]`. Narrow `allow(..., reason = "...")` is permitted when conditional compilation makes expectations unreliable. Test-only modules may exempt size/argument rules. Blanket lint-group suppression is forbidden. Unfulfilled expectations expose stale exceptions. Existing legacy exceptions are inventoried during adoption and cannot authorize new sites.

Every reasoned source exception also has a merge-base structural contract: the
code under it may not grow. Scope is measured as code: lines that are not
blank, comments or attributes, the nested-item count, and the syntax facts
outside attributes. An exception on an out-of-line `mod x;`, or a file-level
`#![...]` in a file that declares one, covers the module file too, and is
measured over it; a declared module file the ratchet does not read fails, as
does a `#[path]` module outside the ratcheted source set. `unwrap_used` and
`expect_used` retain normalized direct-method and associated-call
fingerprints, including panic methods written inside a macro's arguments.
Lexical import aliases are resolved; ordinary callee and path sites under the
exceptional scope are also retained conservatively so a local alias cannot
hide a same-size replacement. `dead_code` retains exact field fingerprints.
Fingerprints are keyed below the exception's owner, so renaming the owner
keeps them. These are source ceilings, not reimplementations of Clippy; the
pinned compiler fixtures remain the typed authority for method and
associated-function lint truth. This preserves deliberate poisoned-lock
failure while preventing an impl-wide expectation from silently covering an
unrelated panic site or compatibility field.

A contract is identified by its file, owning item, scope kind and one lint; the
reason text is not part of it. Editing a reason is an explanation update only:
it keeps every ceiling and never admits growth. Each lint of a multi-lint
attribute keeps its own contract, and several attributes on one scope measure
that scope once, so splitting, merging or deleting a redundant attribute
neither resets nor loosens a ceiling. A contract that disappears from one file
while the same owner, scope kind and lint appears in another has moved and is
compared with the contract it left. One that appears under another owner, in
the same file or a related module (a file and its submodules), with the same
scope kind and lint as a disappearing one is a rename or a narrowing onto an
extracted item, and is compared the same way. Among several candidates an
exact match is the origin; otherwise each metric is held to the candidates'
smallest value.

Growth is first answered by narrowing the exception, moving code out of its
scope or restructuring. Growth that remains is admitted only by a row in
`docs/quality/exception-growth.json` naming the contract (`path`, `owner`,
`scope`, `lint`), the grown `metrics` at exactly the values the gate reports
(each `metric before -> after` becomes `"metric": after`), a `rationale` and the
`approver`. A row records the contract's current state: it stays valid while
the contract holds exactly those values and fails as stale once the contract
changes, so it must then be removed or re-approved. Rows carry the repository
owner's approval. A coding agent may propose a row, with the gate's reported
values, as a decision for the owner, but never adds its own approval rows. A
plan never re-decides an exception's reason, and never renames, re-wraps,
splits or re-attaches an exception's owner, to absorb growth; a new exception
on code that left an existing exception's scope is growth, proposed as a row.
The gate checks each row's shape; it cannot check who approved it.

Never replace a deliberate poisoned-lock failure with silent recovery, swallow an error, or introduce a wrapper solely to satisfy a lint. A flags/options bag does not discharge argument complexity; a pass-through module does not discharge canonical ownership.

## Architecture requirements

The syntax-aware gate extends the existing architecture check and keeps its historical anchors. Merge-base comparisons and the adoption inventory provide a non-growing limit after a file has shrunk. On a push the source ratchet's comparison base is the event's previous revision (`QUALITY_BEFORE_SHA`), because `origin/<branch>` is the pushed commit itself; a push without it, or a branch-creating push with no previous revision, fails closed rather than comparing HEAD with HEAD. A base file that does not parse (a red push this one repairs) is left out of the comparison.

| Gate | Mandatory behavior |
| --- | --- |
| File growth | New hand-written `.rs` files MUST be at most 1,000 physical lines. Crossing from at/below 1,000 to above it is blocked. An existing oversized file MUST NOT grow without an owner-approved bounded exception. Tests count. Only explicitly generated, reproducible output is excluded. |
| Canonical boundaries | Preserve the ban on transport/state dependencies in extracted owners. Peer/wire adapters need explicit entries. Domain decisions MUST NOT be reconstructed from HTTP responses, JSON blobs or error-display strings. |
| Effect ownership | No new raw spawning, runtime environment reads or mutable process-global service state outside registered owners. Intentional process-wide metrics/executors get separate entries. |
| Proof-bearing types | Checked frames, validated postings and charged batches keep invariant fields private to their owner. Construction/mutation goes through that owner. Compiler fixtures MUST attempt external construction and mutation, with legitimate owner/API controls. |

Source rules use parsed Rust syntax. Type-dependent rules use actual compiler diagnostics. Each new gate MUST include violating fixtures and legitimate controls, including import aliases and relevant cfg variants. Unknown syntax/macros/import forms MUST be reported, never silently counted as compliant. Compiler coverage and any unresolved syntax inventory MUST be distinguished explicitly.

Splitting a transaction into context-heavy helpers, hiding flags in options bags or adding pass-through modules is not acceptance. Review the whole affected owner under the pinned skill, including opportunities to delete unnecessary state or layers.

## Verification selected by the changed invariant

The comparison range is event-specific and is written into `plan.json` with
the event, checked-out revision, comparison revision/kind and selected mutation
owners. Pull requests use the actual target-branch merge base. Pushes use the
exact `before` revision from the event—even for a non-ancestor force update;
CI fetches that object if necessary and fails closed if it remains unavailable.
A branch-creation push compares against Git's empty tree. Scheduled runs do not
pretend to have a PR diff: a stable seven-night hash rotation selects complete
registered owners and runs their whole mutation scope. The rotation slot and
source files are recorded in the same receipt.

`scripts/quality/mutation_owners.py` is the single exact owner table for source
paths, packages/targets and test filters. Registered paths participate directly
even when they are outside the planner's broader critical prefixes. Change
selection consumes NUL-delimited, rename-aware Git records and carries prior
registered/prefix criticality to a live destination; a missing destination row
therefore fails before discovery. A dissimilar move represented as delete/add
conservatively carries new Rust paths as possible replacements. True deletions
remain explicit receipt entries and are never presented to `cargo-mutants`.
The resolved owner/source handoff is recorded once and validated unchanged by
the driver before discovery.

The planner records `visibility_only_files` when the only changes narrow parsed
`pub` visibility to `pub(crate)`, `pub(super)` or `pub(self)` and every other
source character matches. It separately records `production_unchanged_files`
when parsed production tokens match after removing direct lint `allow`/`expect`
annotations and items with their own explicit `#[cfg(test)]` attribute. Entire
files require an actual `#![cfg(test)]`; filenames and enclosing function names
are not proof. Visibility may only narrow as above. Compiler, source, dependency
and ordinary test jobs remain mandatory for both classifications.

The second comparison preserves signatures, expressions, literal spellings,
documentation attributes, configuration and macro tokens. Opaque item macros,
custom attributes/derives and source-introspection macros retain checks. Mixed
or indirect configuration is not erased. Parsed statement attributes cannot
mark their containing function as test-only. Unknown syntax or a failed Git or
parser read cannot establish unchanged production source. Executable positive
and negative controls cover these boundaries in the planner test suite.

An unchanged outer custom attribute or derive may also retain its exact input
while an unrelated item changes only lint annotations. This requires the entire
annotated declaration, including nested tokens and all attributes, to remain
byte-identical at identical line, column and UTF-8 byte positions. No macro name
is treated as inherently safe. Changes inside that declaration, moved inputs,
custom inner or conditionally constructed attributes, opaque item macros and
source introspection retain checks. The remaining production source must still pass the parsed token
comparison. Positive and negative controls cover these boundaries.

A separate, stricter byte comparison may remove only trailing root items with
an explicit `#[cfg(test)]` while preserving the entire production prefix and
all its item locations. Unchanged custom attributes and derives are eligible
only under this proof: their complete input bytes and spans remain fixed.
Nested test items inside a production macro input cannot use this exception.
Custom crate-level inner attributes, opaque item macros and source-introspection
macros still retain checks.

The planner separately records `formatted_visibility_files` for narrowed
visibility plus token-preserving formatting, including parser-identified
trailing parameter commas on the narrowed functions. This does **not** prove
unchanged macro expansion: unrelated derive spans may move. All otherwise
selected property-corpus and Miri checks remain selected. Only mutation
selection omits these non-executable edits; every other critical changed source
still needs an actual-diff mutation scope. Changed opaque macro inputs, source
introspection, expressions, types, tuple commas and other functions' parameter
commas are ineligible. No zero-mutant execution is reported as a passing test.

The `visibility_only_files` and `production_unchanged_files` classifications do
not select mutation, Miri or property-corpus checks by themselves. Tooling changes still exercise the verification harness. This is
a selection decision, not a passing zero-mutation experiment; other critical
changes still require a registered executable mutation scope.

`plan.json` selects exactly three checks, `properties_fuzz`, `miri` and
`mutants`, and each gates one `rust-quality` step; a planner test refuses a
check no step reads. Nothing selects the rest, because it runs on every change:
compiler, Clippy, the `--lib quality_` leg (the lib property and Loom models)
and ci.yml's full `cargo test --release`, which also carries the two pilot Loom
models (`src/bin/pilot/benchmark/tests.rs` and `tests/pilot_membership.rs`).

A scheduled bucket bypasses diff-oriented prefix selection entirely. Its full
owners, complete discovery source set and rotation slot must agree in the plan,
driver receipt and actual cargo-mutants file arguments. The union of seven
buckets covers every active registered owner exactly once; zero discovery is
reported only after that owner's discovery command actually ran.

| Trigger | Required verification and acceptance |
| --- | --- |
| Every PR | Run pinned `cargo machete` and `cargo deny check`. Fail unexplained unused dependencies, unapproved advisories, sources and licenses. Unknown Git/registry sources are denied; the existing exact-revision SlateDB source is explicitly allowed. Duplicate versions are reviewed signals, not a blanket ban. False positives require a rationale rather than automatic dependency deletion. |
| Codec, index or admission changes | Property tests exercise the production decoder/admission path with both valid and malformed data: zero counts, overflow, overlapping intervals and corruption. Run at least 1,024 cases for each affected property and commit minimized failures. Replay the saved fuzz corpus on relevant PRs; run bounded fuzzing with the pinned nightly on scheduled jobs. |
| Synchronization or retirement changes | Loom exercises the actual small state-transition implementation through instrumented primitives and records exploration bounds. Keep held-WAL and cancellation integration tests. Neither a rewritten model nor a bounded pass certifies the server. |
| Unsafe or low-level buffer changes | Run compatible affected unit tests under Miri with the dated nightly. Unsupported FFI/system paths remain explicitly untested. Retained buffers additionally assert backing-capacity/reservation accounting after subset selection, eviction and cancellation; Miri is not a memory-budget proof. |
| Changed critical invariants | Run `cargo mutants --in-diff <saved-diff>` against the actual PR merge base, with explicit owner/test scope. Require a passing unmutated baseline and a disposition for every surviving viable mutant. Timeouts and incomplete runs are not assertion-detected failures. Preserve exact discovered failures as regressions. A diff whose executable critical files all belong to registered owners but selects no mutant reports that explicitly and claims no experiment; an unregistered owner still fails. |

Before requesting review, the author MUST identify the canonical owner, account for every warning/exception, provide a falsifiable regression for each changed invariant and explain what complexity was removed—or why further abstraction would be worse. Required test/protocol jobs and scoped external acceptance remain independent of the quality gate. These standards do not lift the existing performance, cryptographic, merge, deployment or evidence-upload holds.

## Primary references

- [Pinned Cursor skill](https://github.com/cursor/plugins/blob/2b8ae2ee306f823d54879d3da7f8496b73c31d5d/cursor-team-kit/skills/thermo-nuclear-code-quality-review/SKILL.md), content blob `ac76a2bc88bb2d895e83ab1788aa584a82346cfc`.
- [Cargo lint configuration](https://doc.rust-lang.org/cargo/reference/lints.html), [Clippy configuration](https://doc.rust-lang.org/clippy/configuration.html), [lint catalog](https://rust-lang.github.io/rust-clippy/master/index.html), [Rust diagnostic attributes](https://doc.rust-lang.org/reference/attributes/diagnostics.html).
- [cargo-machete](https://github.com/bnjbvr/cargo-machete), [cargo-deny sources](https://embarkstudios.github.io/cargo-deny/checks/sources/cfg.html), [dependency bans](https://embarkstudios.github.io/cargo-deny/checks/bans/cfg.html).
- [Proptest](https://proptest-rs.github.io/proptest/intro.html), [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html), [Loom](https://docs.rs/loom/latest/loom/), [Miri](https://github.com/rust-lang/miri), [cargo-mutants diff scope](https://mutants.rs/in-diff.html), [mutation timeouts](https://mutants.rs/timeouts.html).
