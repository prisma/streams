# Exception reason edits in the September 2026 hardening program

The external review of `slate` at 24c4c77a (section 7, adopted by the
repository owner) found that the source ratchet let a change grow an existing
`#[expect]`/`#[allow]` exception by editing its reason, and asked for every
such edit in the program to be classified. The program's commits were written
by a coding agent; none of the growth below carried the owner's approval when
it landed. The ratchet now keys contracts without the reason and admits growth
only through an owner-approved row (docs/RUST-QUALITY.md,
`docs/quality/exception-growth.json`). Everything below is already in the
base, so it passes; this record lists it for the owner's decision.

## Scope and method

- Range: `git rev-list --first-parent --reverse adc2cdc5..95bfabc6`, 125
  commits, from the program's first commit on 2026-09-21 to the last commit
  before the ratchet change. Each commit is compared with its first parent;
  the merge 7a6ecd23 is compared with its first parent too.
- Files: the Rust files each commit changed (`git diff --no-renames`), in the
  directories the ratchet reads, without unparsed templates. Deleted and
  added files both load, so moves are visible.
- Contracts: `scripts/quality/source_rules.py` as of the ratchet change, run
  with the pinned extractor (`tools/quality-syntax`): per lint, keyed
  (path, owner, scope kind, lint), measured as code (lines that are not
  blank, comments or attributes, and syntax facts outside attributes), with
  moves and renames paired as the gate pairs them.
- A **reason edit** is a contract whose reason texts differ from its
  predecessor's. A **move/rename** is a contract paired with a predecessor
  under another path or owner.
- Classes: **scope expansion** when any size or site metric grew (code lines,
  nested items, syntax facts, unwrap/expect sites, fields); **scope expansion
  (fingerprint-only)** when only call/path/field fingerprints grew (a call's
  text changed, or a callee moved and its resolved path changed); **scope
  reduction** when nothing grew and something fell; **explanation update**
  when every metric is equal.
- **New gate** is `exception_growth` with no rows: `row needed` means the
  change would now fail without an owner-approved row.
- Reproduce: `python3 scripts/exception-audit.py adc2cdc5 95bfabc6` (about a
  minute). It prints Tables 1-3, the roll-up and Appendix A below.

## Summary

- 72 reason-edit contracts in 30 commits: 36 scope expansions, 19
  fingerprint-only expansions, 16 scope reductions and 1 explanation update.
  The measure is code, so every size expansion is code that grew under an
  existing exception.
- 16 move/rename contracts in 7 commits: 7 scope expansions, 1
  fingerprint-only expansion, 6 explanation updates and 2 reductions. The two
  moves labelled "verbatim" (0eb8a38d, df9ff212) kept their code lines and
  gained `pub(super)` (2 syntax facts each); d902a85b and ca36a7f4 also added
  2 syntax facts.
- By commit: 15 commits edited a reason while the measured size grew, and 11
  more while only fingerprints grew. 30 of the 36 commits in the tables
  would need a row under the new gate.
- The external census found 13 commits with growth and 12 with shrink. All 13
  are size expansions here. Of the 12 it counted as shrink, 2 are reductions,
  8 grew fingerprints while their size fell, and 2 grew in syntax facts
  (d93b421b, 07db91a7). So the census undercounted: "shrink" in lines is not
  "no growth" in the ratchet's metrics. 063a674d, which the census did not
  count, consolidated four `Registry` functions' unwrap exceptions into one on
  `DescriptorCache::slots`; paired as a rename, it grew fingerprints.
- Table 3 lists the pairs the gate still does not match: 6ef3bc64 narrowed an
  impl-wide exception on `SingleSource` onto two methods, a change of scope
  kind (D3).

## Findings

1. The reason edit was the program's routine way to absorb growth. The 26
   commits with any expansion under a reason edit span the whole program,
   including correctness fixes (527d3d3a, f1c9e77e, 6ef3bc64, 714abcc2) and
   test-rig changes (82095942, 46d4b7df). The largest single growth is
   f1c9e77e's `product_consumer_pull` (62→78 code lines, 96→118 facts).
2. Fingerprint-only expansion is common: 19 contracts. Every call and path
   under an `unwrap_used`/`expect_used` exception is fingerprinted, so a
   changed argument or a callee that moved (its import-resolved path changes)
   reads as growth in every excepted caller (D5).
3. Moves did not move only: "verbatim" moves widened visibility inside the
   measured scope.
4. The last commit before the change (95bfabc6, this program's F-G fix)
   moved its test exception onto an extracted helper; paired, it is a
   reduction and passes.

## Census comparison

| Commit | Census | Audit: reason edits (worst) | Audit: moves/renames (worst) | Agrees | Note |
| --- | --- | --- | --- | --- | --- |
| 6faf4757 | growth | scope expansion | — | yes |  |
| 82095942 | growth | scope expansion | — | yes |  |
| eb52b970 | growth | scope expansion | — | yes |  |
| 527d3d3a | growth | scope expansion | — | yes |  |
| f1c9e77e | growth | scope expansion | — | yes |  |
| b9f3cd7c | growth | scope expansion | — | yes |  |
| a1cf29f3 | growth | scope expansion | — | yes |  |
| 729c52ac | growth | scope expansion | — | yes |  |
| 796211d7 | growth | scope expansion | — | yes |  |
| a0185c3b | growth | scope expansion | — | yes |  |
| 714abcc2 | growth | scope expansion | scope reduction | yes | the set-aside spawn moved into tasks/refusal.rs (paired, a reduction) |
| 6ef3bc64 | growth | scope expansion | — | yes |  |
| 46d4b7df | growth | scope expansion | — | yes |  |
| 1994fc25 | shrink | scope reduction | — | yes |  |
| 5292e3e3 | shrink | scope reduction | — | yes |  |
| 668bc80c | shrink | scope expansion (fingerprint-only) | — | no |  |
| 71345c03 | shrink | scope expansion (fingerprint-only) | — | no |  |
| ac9ab99e | shrink | scope expansion (fingerprint-only) | — | no |  |
| d93b421b | shrink | scope expansion | — | no | code lines fell, syntax facts grew |
| 4bb51c0d | shrink | scope expansion (fingerprint-only) | — | no |  |
| 5c0e62d6 | shrink | scope expansion (fingerprint-only) | — | no |  |
| 6515a15d | shrink | scope expansion (fingerprint-only) | — | no |  |
| 69d8bc95 | shrink | scope expansion (fingerprint-only) | — | no |  |
| 07db91a7 | shrink | scope expansion | — | no | syntax facts grew (+2, +6) |
| ed9238da | shrink | scope expansion (fingerprint-only) | — | no |  |
| d2e87129 | not counted | scope expansion (fingerprint-only) | — | — | rename spawn_load → finish_load; new call fingerprints |
| 063a674d | not counted | scope expansion (fingerprint-only) | — | — | four Registry functions consolidated into DescriptorCache::slots, paired as a rename |
| 33fbd10e | not counted | scope reduction | — | — | rename router → debug_routes |
| 58569eb7 | not counted | explanation update | — | — |  |
| d902a85b | not counted | — | scope expansion | — | move added 2 syntax facts |
| ca36a7f4 | not counted | — | scope expansion | — | move added 2 syntax facts; get changed fingerprints |
| 0eb8a38d | not counted | — | scope expansion | — | "verbatim" move added pub(super) (2 facts) |
| df9ff212 | not counted | — | scope expansion | — | "verbatim" move added pub(super) (2 facts) |
| f0c072b3 | not counted | — | explanation update | — |  |
| 7a6ecd23 | — | scope expansion (fingerprint-only) | — | — | merge commit, compared with its first parent |
| 95bfabc6 | — | — | scope reduction | — | after the census; paired onto the extracted rig helper |

## Adversarial review of the gate

Before it landed, the gate change was reviewed by three independent reviewers
(bypasses; crashes and CI correctness; false positives on ordinary work),
and every finding was given to a separate skeptic to reproduce or refute.
Of 25 findings, 19 were confirmed and fixed in the gate or recorded as
decisions below; 6 were refuted as the adopted design.

| Finding | Verdict | Disposition |
| --- | --- | --- |
| An exception moved into a `#[path]` module outside the scanned set (a non-`.rs` file or a hidden directory) left every ceiling behind | confirmed, high | fixed: a by-path module must resolve to a ratcheted source |
| An exception on an out-of-line `mod x;` (or a file-level `#![…]` in its parent) covered the child file but measured none of it | confirmed, high | fixed: the child module files are part of the scope; one the ratchet cannot resolve fails |
| An unwrap/expect written inside a macro's arguments was not counted | confirmed, high | fixed: counted and fingerprinted from the macro's tokens |
| A module directory named `target` or `node_modules` at any depth was not scanned | confirmed, high | fixed: `target` is skipped only at the root |
| An unprefixed lint name (`unwrap_used`) with `renamed_and_removed_lints` allowed escaped the `clippy::` contract | confirmed, high | fixed: `renamed_and_removed_lints` cannot be suppressed |
| A shim under the old name in another file took a renamed exception's origin | confirmed, high | fixed: a vanished contract can be the origin of both |
| A move combined with an owner change was a new exception | confirmed, high | fixed for related modules (a file and its submodules); D3 for unrelated ones |
| A pure rename or narrowing of an unwrap/expect/dead_code exception read as all-new fingerprints | confirmed, medium | fixed: fingerprints are keyed below the owner |
| Unrelated same-named contracts in other files were paired as moves (`crate::start`, `crate::handle`) | confirmed, medium | fixed where a related module or an exact match exists; D9 for the rest |
| The exception's own attribute, doc and blank lines were scope: splitting an attribute read as growth, a shorter reason freed budget, `expect` → `allow` changed fingerprints | confirmed, medium | fixed: measured as code (D4) |
| A push repairing an unparseable base file crashed the gate | confirmed, medium | fixed: a base file that does not parse is left out |
| A non-UTF-8 path anywhere in the base tree crashed the listing | confirmed, low | fixed |
| A branch-creating push compared HEAD with HEAD (from 24c4c77a) | confirmed, low | fixed: it fails closed |
| A symlinked `.rs` file crashed the base parse | confirmed, low | fixed by the unparseable-base rule |
| The ratchet commit said a slate-to-main promotion would report historical growth | confirmed, low | corrected: main's merge base holds no exceptions (D8) |
| Hoisting an item exception to its impl or file widens it unreported | refuted | a hoisted exception is a new exception, reviewed in source like any other |
| An unrelated deletion made a rename ambiguous | refuted as documented | now held to the smallest candidate anyway |
| A same-file deletion plus a new exception is paired as a rename | refuted | adopted rule; the note now says "paired with vanished" |
| Merging same-owner files is not credited | refuted | adopted per-contract rule |
| Renaming a local binding under an unwrap exception is growth | refuted | D5 |
| Exact-state rows need re-approval for less growth | refuted | adopted design (D2) |

## Decisions for the owner

- **D1 (implemented; confirm).** Contracts are keyed per lint, not per lint
  set: keying by the set leaves merging `#[expect(a)]` and `#[expect(b)]` into
  `#[expect(a, b)]` (or splitting them) as a zero-code reset.
- **D2 (implemented; confirm).** Rows are exact-state: a row stays valid while
  its contract holds exactly the recorded values and fails as stale when the
  contract changes. The rejected alternatives were rows consumed by the push
  that lands the growth (breaks the local run after that push, a push carrying
  the growth and the row removal together, and any later comparison across
  several pushes) and rows bound to a base revision (break on every rebase).
- **D3 (residual pairing).** Moves (same owner, scope kind and lint) and
  renames or narrowings (same scope kind and lint, in the same file or a
  related module) are paired. Not paired: a rename into an unrelated module,
  and a change of scope kind (impl-wide → per-method, as 6ef3bc64 did, or the
  reverse). The documented rule forbids using either to absorb growth.
- **D4 (implemented; confirm).** Scope is measured as code: lines that are not
  blank, comments or attributes, and syntax facts outside attributes. Before,
  the item's physical span and its attribute facts counted, so a doc comment
  read as growth and a shorter or re-wrapped reason freed body budget.
- **D5 (fingerprint breadth).** Under `unwrap_used`/`expect_used` every call
  and path is fingerprinted, so any argument change, any new call, and any
  move of a callee counts as growth in each excepted caller; about 320 such
  attributes exist. Option: fingerprint only sites whose resolved callee ends
  in `unwrap`/`unwrap_err`/`expect`/`expect_err`, keeping import-alias
  resolution. That changes normative text, and decides how many rows the
  owner will be asked for.
- **D6 (the historical expansions above).** They are in the base and pass.
  For each Table 1/2 expansion: accept as is, or schedule a remediation that
  narrows the exception.
- **D7 (approval authenticity).** The gate checks a row's shape; it cannot
  check who approved it. The rule relies on review of commits that touch the
  ledger.
- **D8 (promotion).** main's merge base with slate (8e3aa50b) has 21 Rust
  files and no exceptions, so a slate-to-main pull request compares nothing:
  every exception on slate is new to main and passes. The per-push record on
  slate, and this audit, are the evidence; the promotion adds none.
- **D9 (conservative false positives).** A new exception is compared with an
  unrelated vanished one when their owner names coincide across unrelated
  modules, or when it appears in a file or related module where same-kind,
  same-lint exceptions vanished; ambiguous candidates hold it to their
  smallest values. The escape is to land the deletion in a separate push, or
  a row.

## Tables

### Table 1 — reason edits

| # | Commit | Subject | Contract | Reason | Code lines | Items | Facts | Sites | Fingerprints | Class | New gate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 7a6ecd23 | Merge review follow-ups, rustdoc to zero, and the rustls ... | `src/bootstrap.rs` · `crate::run` · `function` · `clippy::expect_used` | A-1 | 591→591 | 1→1 | 977→976 | 1→1 | +1 −2 | scope expansion (fingerprint-only) | row needed |
| 2 | 7a6ecd23 | Merge review follow-ups, rustdoc to zero, and the rustls ... | `src/bootstrap.rs` · `crate::run` · `function` · `clippy::unwrap_used` | A-2 | 591→591 | 1→1 | 977→976 | 4→4 | +1 −2 | scope expansion (fingerprint-only) | row needed |
| 3 | d2e87129 | Postings cache: a store load claims only to its absorbed ... | `src/postings_cache.rs` · `crate::PostingsCache::finish_load` · `function` · `clippy::unwrap_used` | A-3 (paired with vanished crate::PostingsCache::spawn_load) | 88→27 | 1→1 | 164→60 | 1→1 | +7 −56 | scope expansion (fingerprint-only) | row needed |
| 4 | 1994fc25 | A consumer pull loops back only on settled poison, and al... | `src/application/consumer/delivery.rs` · `crate::dlq_and_settle` · `function` · `clippy::too_many_arguments` | A-4 | 132→125 | 1→1 | 191→187 | — | = | scope reduction | pass |
| 5 | 6faf4757 | A watch wait proves shard ownership before it parks: a no... | `src/application/watch.rs` · `crate::WatchService::new` · `function` · `clippy::too_many_arguments` | A-5 | 22→24 | 1→1 | 29→31 | — | = | scope expansion | row needed |
| 6 | 5292e3e3 | The feed's retry task never reads a subscriber's records | `src/sse/feed/drive.rs` · `crate::LiveFeed::schedule_transition_retry` · `function` · `clippy::disallowed_methods` | A-6 | 36→26 | 1→1 | 51→36 | — | = | scope reduction | pass |
| 7 | 5292e3e3 | The feed's retry task never reads a subscriber's records | `src/sse/feed/drive.rs` · `crate::LiveFeed::schedule_transition_retry` · `function` · `clippy::excessive_nesting` | A-7 | 36→26 | 1→1 | 51→36 | — | = | scope reduction | pass |
| 8 | 5292e3e3 | The feed's retry task never reads a subscriber's records | `src/sse/feed/drive.rs` · `crate::LiveFeed::bump_version` · `function` · `clippy::let_underscore_must_use` | A-8 (paired with vanished crate::LiveFeed::drive_under_permit) | 78→9 | 1→1 | 135→16 | — | = | scope reduction | pass |
| 9 | 668bc80c | A topology transition never reaches a writer as closure | `src/application/append/close.rs` · `crate::closed_tail_failure` · `function` · `clippy::unwrap_used` | A-9 | 27→23 | 1→1 | 44→43 | 1→1 | +2 −3 | scope expansion (fingerprint-only) | row needed |
| 10 | 063a674d | The descriptor cache never publishes a read that raced a ... | `src/registry/cache.rs` · `crate::DescriptorCache::slots` · `function` · `clippy::unwrap_used` | A-10 (paired with vanished crate::Registry::cache_insert, crate::Registry::cache_len, crate::Registry::get, crate::Registry::invalidate) | 3→3 | 1→1 | 11→8 | 1→1 | +3 −1 | scope expansion (fingerprint-only) | row needed |
| 11 | 71345c03 | Every h1 connection is served under a request-head deadline | `src/bin/livebench.rs` · `crate::client` · `function` · `clippy::unwrap_used` | A-11 | 9→9 | 1→1 | 19→19 | 1→1 | +2 −2 | scope expansion (fingerprint-only) | row needed |
| 12 | 71345c03 | Every h1 connection is served under a request-head deadline | `src/bootstrap.rs` · `crate::run` · `function` · `clippy::expect_used` | A-12 | 591→590 | 1→1 | 976→975 | 1→1 | +1 −2 | scope expansion (fingerprint-only) | row needed |
| 13 | 71345c03 | Every h1 connection is served under a request-head deadline | `src/bootstrap.rs` · `crate::run` · `function` · `clippy::unwrap_used` | A-13 | 591→590 | 1→1 | 976→975 | 4→4 | +1 −2 | scope expansion (fingerprint-only) | row needed |
| 14 | ac9ab99e | A sealed span's page resolves its engine on every read, n... | `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::excessive_nesting` | A-14 | 140→131 | 1→1 | 235→215 | — | = | scope reduction | pass |
| 15 | ac9ab99e | A sealed span's page resolves its engine on every read, n... | `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::too_many_arguments` | A-15 | 140→131 | 1→1 | 235→215 | — | = | scope reduction | pass |
| 16 | ac9ab99e | A sealed span's page resolves its engine on every read, n... | `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::too_many_lines` | A-16 | 140→131 | 1→1 | 235→215 | — | = | scope reduction | pass |
| 17 | ac9ab99e | A sealed span's page resolves its engine on every read, n... | `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::unwrap_used` | A-17 | 140→131 | 1→1 | 235→215 | 2→2 | +1 −9 | scope expansion (fingerprint-only) | row needed |
| 18 | d93b421b | A live tail whose engine retired under the same owner is ... | `src/sse/source.rs` · `crate::<SingleSource as super::feed::FeedSourceRead>` · `impl` · `clippy::unwrap_used` | A-18 | 95→93 | 12→12 | 158→162 | 2→2 | +11 −5 | scope expansion | row needed |
| 19 | d93b421b | A live tail whose engine retired under the same owner is ... | `src/sse/source.rs` · `crate::<LineageSource as super::feed::FeedSourceRead>` · `impl` · `clippy::too_many_lines` | A-19 | 197→195 | 12→12 | 306→311 | — | = | scope expansion | row needed |
| 20 | d93b421b | A live tail whose engine retired under the same owner is ... | `src/sse/source.rs` · `crate::<LineageSource as super::feed::FeedSourceRead>` · `impl` · `clippy::unwrap_used` | A-20 | 197→195 | 12→12 | 306→311 | 3→3 | +12 −7 | scope expansion | row needed |
| 21 | 82095942 | A durability-barrier test proves its request entered the ... | `src/dst/tests/durability_fences.rs` · `crate::a_fence_waits_for_durability_before_reporting_closed` · `function` · `clippy::disallowed_methods` | A-21 | 135→135 | 1→1 | 230→232 | — | = | scope expansion | row needed |
| 22 | 82095942 | A durability-barrier test proves its request entered the ... | `src/dst/tests/durability_fences.rs` · `crate::a_fence_waits_for_durability_before_reporting_closed` · `function` · `clippy::too_many_lines` | A-22 | 135→135 | 1→1 | 230→232 | — | = | scope expansion | row needed |
| 23 | eb52b970 | A sweep release is not a strike: only a judged departure ... | `src/sharddir.rs` · `crate::OpenGate::notify_closed` · `function` · `clippy::unwrap_used` | A-23 | 18→18 | 1→1 | 61→62 | 3→3 | +2 −1 | scope expansion | row needed |
| 24 | eb52b970 | A sweep release is not a strike: only a judged departure ... | `src/sharddir.rs` · `crate::OpenGate::retire_resident` · `function` · `clippy::unwrap_used` | A-24 | 18→19 | 1→1 | 59→63 | 2→2 | +5 −1 | scope expansion | row needed |
| 25 | 4bb51c0d | An unreadable router report defers only the desired publi... | `src/fleet.rs` · `crate::start` · `function` · `clippy::unwrap_used` | A-25 | 543→534 | 2→2 | 1024→1017 | 4→4 | +3 −6 | scope expansion (fingerprint-only) | row needed |
| 26 | 527d3d3a | A scan page draws on the project read-byte quota like any... | `src/product.rs` · `crate::product_entry` · `function` · `clippy::unwrap_used` | A-26 | 303→296 | 1→1 | 490→447 | 1→1 | +4 −19 | scope expansion (fingerprint-only) | row needed |
| 27 | 527d3d3a | A scan page draws on the project read-byte quota like any... | `src/product.rs` · `crate::product_read` · `function` · `clippy::too_many_arguments` | A-27 | 244→244 | 1→1 | 311→315 | — | = | scope expansion | row needed |
| 28 | 527d3d3a | A scan page draws on the project read-byte quota like any... | `src/product.rs` · `crate::product_read` · `function` · `clippy::too_many_lines` | A-28 | 244→244 | 1→1 | 311→315 | — | = | scope expansion | row needed |
| 29 | 527d3d3a | A scan page draws on the project read-byte quota like any... | `src/product.rs` · `crate::render_product_read` · `function` · `clippy::unwrap_used` | A-29 | 45→47 | 1→1 | 95→104 | 1→1 | +7 −0 | scope expansion | row needed |
| 30 | 527d3d3a | A scan page draws on the project read-byte quota like any... | `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::expect_used` | A-30 | 191→192 | 1→1 | 272→277 | 1→1 | +9 −4 | scope expansion | row needed |
| 31 | 527d3d3a | A scan page draws on the project read-byte quota like any... | `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::unwrap_used` | A-31 | 191→192 | 1→1 | 272→277 | 1→1 | +9 −4 | scope expansion | row needed |
| 32 | 527d3d3a | A scan page draws on the project read-byte quota like any... | `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::too_many_lines` | A-32 | 191→192 | 1→1 | 272→277 | — | = | scope expansion | row needed |
| 33 | f1c9e77e | A consumer pull draws on the project read-byte quota like... | `src/product.rs` · `crate::product_entry` · `function` · `clippy::unwrap_used` | A-33 | 296→297 | 1→1 | 447→450 | 1→1 | +2 −1 | scope expansion | row needed |
| 34 | f1c9e77e | A consumer pull draws on the project read-byte quota like... | `src/product.rs` · `crate::product_entry` · `function` · `clippy::excessive_nesting` | A-34 | 296→297 | 1→1 | 447→450 | — | = | scope expansion | row needed |
| 35 | f1c9e77e | A consumer pull draws on the project read-byte quota like... | `src/product.rs` · `crate::product_entry` · `function` · `clippy::too_many_arguments` | A-35 | 296→297 | 1→1 | 447→450 | — | = | scope expansion | row needed |
| 36 | f1c9e77e | A consumer pull draws on the project read-byte quota like... | `src/product.rs` · `crate::product_entry` · `function` · `clippy::too_many_lines` | A-36 | 296→297 | 1→1 | 447→450 | — | = | scope expansion | row needed |
| 37 | f1c9e77e | A consumer pull draws on the project read-byte quota like... | `src/product/consumer_pull.rs` · `crate::product_consumer_pull` · `function` · `clippy::too_many_arguments` | A-37 | 62→78 | 1→1 | 96→118 | — | = | scope expansion | row needed |
| 38 | 5c0e62d6 | An internal receiver tells an unreadable registry apart f... | `src/product.rs` · `crate::internal_sweep_segment` · `function` · `clippy::expect_used` | A-38 | 81→81 | 6→6 | 114→111 | 1→1 | +4 −5 | scope expansion (fingerprint-only) | row needed |
| 39 | 5c0e62d6 | An internal receiver tells an unreadable registry apart f... | `src/product.rs` · `crate::internal_queue_cursor` · `function` · `clippy::expect_used` | A-39 | 70→70 | 1→1 | 117→114 | 1→1 | +4 −5 | scope expansion (fingerprint-only) | row needed |
| 40 | 58569eb7 | Only decrypt_frame is dead in the service build, so only ... | `src/crypto.rs` · `crate::decrypt_frame` · `function` · `dead_code` | A-40 | 9→9 | 1→1 | 23→23 | 0→0 | = | explanation update | pass |
| 41 | 796211d7 | A serving descriptor has one epoch and it is never absent | `src/application/creation.rs` · `crate::fresh_desc` · `function` · `clippy::too_many_arguments` | A-41 | 36→37 | 1→1 | 65→68 | — | = | scope expansion | row needed |
| 42 | b9f3cd7c | Cursor verdicts are typed; the scan page answers each one... | `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::expect_used` | A-42 | 192→192 | 1→1 | 277→280 | 1→1 | +3 −0 | scope expansion | row needed |
| 43 | b9f3cd7c | Cursor verdicts are typed; the scan page answers each one... | `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::unwrap_used` | A-43 | 192→192 | 1→1 | 277→280 | 1→1 | +3 −0 | scope expansion | row needed |
| 44 | b9f3cd7c | Cursor verdicts are typed; the scan page answers each one... | `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::too_many_lines` | A-44 | 192→192 | 1→1 | 277→280 | — | = | scope expansion | row needed |
| 45 | a1cf29f3 | Shard-open counters belong to the gate that counts them | `src/sharddir.rs` · `crate::OpenGate::get_or_open` · `function` · `clippy::unwrap_used` | A-45 | 150→146 | 1→1 | 395→379 | 6→6 | +5 −9 | scope expansion (fingerprint-only) | row needed |
| 46 | a1cf29f3 | Shard-open counters belong to the gate that counts them | `src/store_timing/observations.rs` · `crate::snapshot` · `function` · `clippy::unwrap_used` | A-46 | 69→70 | 1→1 | 92→93 | 1→1 | +1 −0 | scope expansion | row needed |
| 47 | 729c52ac | A merge allocates its child id before either parent seals | `src/segmap.rs` · `crate::SegmentMap::merge` · `function` · `clippy::too_many_arguments` | A-47 | 54→55 | 1→1 | 117→124 | — | = | scope expansion | row needed |
| 48 | 729c52ac | A merge allocates its child id before either parent seals | `src/segmap.rs` · `crate::SegmentMap::merge` · `function` · `clippy::unwrap_used` | A-48 | 54→55 | 1→1 | 117→124 | 1→1 | +3 −0 | scope expansion | row needed |
| 49 | 6515a15d | A rebalance move names only a ring member, and an overrid... | `src/fleet.rs` · `crate::start` · `function` · `clippy::unwrap_used` | A-49 | 534→532 | 2→2 | 1017→1017 | 4→4 | +4 −6 | scope expansion (fingerprint-only) | row needed |
| 50 | 33fbd10e | Every /v1/debug path answers through one bearer gate, not... | `src/http.rs` · `crate::debug_routes` · `function` · `clippy::disallowed_methods` | A-50 (paired with vanished crate::router) | 243→151 | 1→1 | 380→201 | — | = | scope reduction | pass |
| 51 | 33fbd10e | Every /v1/debug path answers through one bearer gate, not... | `src/http.rs` · `crate::debug_routes` · `function` · `clippy::too_many_lines` | A-51 (paired with vanished crate::router) | 243→151 | 1→1 | 380→201 | — | = | scope reduction | pass |
| 52 | a0185c3b | A panicking request handler is logged and counted on its ... | `src/http/serve.rs` · `crate::serve_h1` · `function` · `clippy::disallowed_methods` | A-52 | 45→47 | 1→1 | 65→68 | — | = | scope expansion | row needed |
| 53 | a0185c3b | A panicking request handler is logged and counted on its ... | `src/http/serve.rs` · `crate::serve_h1` · `function` · `clippy::let_underscore_must_use` | A-53 | 45→47 | 1→1 | 65→68 | — | = | scope expansion | row needed |
| 54 | 714abcc2 | A task a closing runtime refuses is dropped after the reg... | `src/tasks.rs` · `crate::TaskSupervisor::spawn` · `function` · `clippy::unwrap_used` | A-54 | 29→33 | 1→1 | 55→60 | 1→1 | +6 −2 | scope expansion | row needed |
| 55 | 714abcc2 | A task a closing runtime refuses is dropped after the reg... | `src/tasks/tests.rs` · `crate::a_spawn_refused_by_a_closing_runtime_cannot_deadlock_its_supervisor` · `function` · `clippy::disallowed_methods` | A-55 (paired with vanished src/tasks.rs crate::TaskSupervisor::spawn) | 29→39 | 1→9 | 55→67 | — | = | scope expansion | row needed |
| 56 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::cast_possible_truncation` | A-56 | 118→104 | 1→1 | 196→164 | — | = | scope reduction | pass |
| 57 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::excessive_nesting` | A-57 | 118→104 | 1→1 | 196→164 | — | = | scope reduction | pass |
| 58 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::expect_used` | A-58 | 118→104 | 1→1 | 196→164 | 1→1 | +0 −17 | scope reduction | pass |
| 59 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::too_many_lines` | A-59 | 118→104 | 1→1 | 196→164 | — | = | scope reduction | pass |
| 60 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::wildcard_enum_match_arm` | A-60 | 118→104 | 1→1 | 196→164 | — | = | scope reduction | pass |
| 61 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::excessive_nesting` | A-61 | 131→97 | 1→1 | 215→172 | — | = | scope reduction | pass |
| 62 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::too_many_arguments` | A-62 | 131→97 | 1→1 | 215→172 | — | = | scope reduction | pass |
| 63 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::unwrap_used` | A-63 | 131→97 | 1→1 | 215→172 | 2→2 | +14 −30 | scope expansion (fingerprint-only) | row needed |
| 64 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/source.rs` · `crate::<LineageSource as super::feed::FeedSourceRead>` · `impl` · `clippy::too_many_lines` | A-64 | 195→201 | 12→12 | 311→317 | — | = | scope expansion | row needed |
| 65 | 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/source.rs` · `crate::<LineageSource as super::feed::FeedSourceRead>` · `impl` · `clippy::unwrap_used` | A-65 | 195→201 | 12→12 | 311→317 | 3→3 | +11 −7 | scope expansion | row needed |
| 66 | 69d8bc95 | The sweep custody words move behind a private-field Sweep... | `src/shard.rs` · `crate::ShardEngine::start` · `function` · `clippy::unwrap_used` | A-66 | 289→288 | 1→1 | 586→584 | 5→5 | +3 −3 | scope expansion (fingerprint-only) | row needed |
| 67 | 07db91a7 | A cooldown past the millisecond range holds for ever inst... | `src/scaler3.rs` · `crate::State::prune` · `function` · `clippy::unwrap_used` | A-67 | 26→25 | 1→1 | 72→74 | 1→1 | +2 −0 | scope expansion | row needed |
| 68 | 07db91a7 | A cooldown past the millisecond range holds for ever inst... | `src/scaler3.rs` · `crate::evaluate_state` · `function` · `clippy::too_many_lines` | A-68 | 127→127 | 1→1 | 303→309 | — | = | scope expansion | row needed |
| 69 | 46d4b7df | BILLING_MODE and ROLLUP have one reader: health, the bill... | `src/dst/tests/fixture_http.rs` · `crate::http_rig_build` · `function` · `clippy::too_many_lines` | A-69 | 165→171 | 1→1 | 295→296 | — | = | scope expansion | row needed |
| 70 | 46d4b7df | BILLING_MODE and ROLLUP have one reader: health, the bill... | `src/dst/tests/fixture_http.rs` · `crate::http_rig_build` · `function` · `clippy::let_underscore_must_use` | A-70 | 165→171 | 1→1 | 295→296 | — | = | scope expansion | row needed |
| 71 | ed9238da | The absorber holds only what it reads: the store and key ... | `src/bootstrap.rs` · `crate::run` · `function` · `clippy::expect_used` | A-71 | 590→582 | 1→1 | 975→967 | 1→1 | +3 −5 | scope expansion (fingerprint-only) | row needed |
| 72 | ed9238da | The absorber holds only what it reads: the store and key ... | `src/bootstrap.rs` · `crate::run` · `function` · `clippy::unwrap_used` | A-72 | 590→582 | 1→1 | 975→967 | 4→4 | +3 −5 | scope expansion (fingerprint-only) | row needed |

### Table 2 — moves and renames without a reason edit

| # | Commit | Subject | Contract | Moved or renamed from | Code lines | Items | Facts | Sites | Fingerprints | Class | New gate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | d902a85b | Move the feed's permit-held drive and its retry task into... | `src/sse/feed/drive.rs` · `crate::LiveFeed::schedule_transition_retry` · `function` · `clippy::disallowed_methods` | moved from src/sse/feed.rs | 36→36 | 1→1 | 51→51 | — | = | explanation update | pass |
| 2 | d902a85b | Move the feed's permit-held drive and its retry task into... | `src/sse/feed/drive.rs` · `crate::LiveFeed::schedule_transition_retry` · `function` · `clippy::excessive_nesting` | moved from src/sse/feed.rs | 36→36 | 1→1 | 51→51 | — | = | explanation update | pass |
| 3 | d902a85b | Move the feed's permit-held drive and its retry task into... | `src/sse/feed/drive.rs` · `crate::LiveFeed::drive_under_permit` · `function` · `clippy::excessive_nesting` | moved from src/sse/feed.rs | 78→78 | 1→1 | 133→135 | — | = | scope expansion | row needed |
| 4 | d902a85b | Move the feed's permit-held drive and its retry task into... | `src/sse/feed/drive.rs` · `crate::LiveFeed::drive_under_permit` · `function` · `clippy::let_underscore_must_use` | moved from src/sse/feed.rs | 78→78 | 1→1 | 133→135 | — | = | scope expansion | row needed |
| 5 | ca36a7f4 | Move the registry's descriptor cache into registry/cache.... | `src/registry/cache.rs` · `crate::Registry::cache_insert` · `function` · `clippy::unwrap_used` | moved from src/registry.rs | 17→17 | 2→2 | 50→52 | 1→1 | +2 −1 | scope expansion | row needed |
| 6 | ca36a7f4 | Move the registry's descriptor cache into registry/cache.... | `src/registry/cache.rs` · `crate::Registry::cache_len` · `function` · `clippy::unwrap_used` | moved from src/registry.rs | 3→3 | 1→1 | 11→11 | 1→1 | = | explanation update | pass |
| 7 | ca36a7f4 | Move the registry's descriptor cache into registry/cache.... | `src/registry/cache.rs` · `crate::Registry::get` · `function` · `clippy::unwrap_used` | moved from src/registry.rs | 63→63 | 1→1 | 137→137 | 2→2 | +6 −6 | scope expansion (fingerprint-only) | row needed |
| 8 | ca36a7f4 | Move the registry's descriptor cache into registry/cache.... | `src/registry/cache.rs` · `crate::Registry::invalidate` · `function` · `clippy::unwrap_used` | moved from src/registry.rs | 3→3 | 1→1 | 12→12 | 1→1 | = | explanation update | pass |
| 9 | 0eb8a38d | Move the product scan page into product/scan.rs, verbatim | `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::expect_used` | moved from src/product.rs | 191→191 | 1→1 | 270→272 | 1→1 | +21 −20 | scope expansion | row needed |
| 10 | 0eb8a38d | Move the product scan page into product/scan.rs, verbatim | `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::unwrap_used` | moved from src/product.rs | 191→191 | 1→1 | 270→272 | 1→1 | +21 −20 | scope expansion | row needed |
| 11 | 0eb8a38d | Move the product scan page into product/scan.rs, verbatim | `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::too_many_lines` | moved from src/product.rs | 191→191 | 1→1 | 270→272 | — | = | scope expansion | row needed |
| 12 | df9ff212 | Move the product consumer pull handler into product/consu... | `src/product/consumer_pull.rs` · `crate::product_consumer_pull` · `function` · `clippy::too_many_arguments` | moved from src/product.rs | 62→62 | 1→1 | 94→96 | — | = | scope expansion | row needed |
| 13 | f0c072b3 | serve_h1 moves into http/serve.rs, beside the posture it ... | `src/http/serve.rs` · `crate::serve_h1` · `function` · `clippy::disallowed_methods` | moved from src/http.rs | 45→45 | 1→1 | 65→65 | — | = | explanation update | pass |
| 14 | f0c072b3 | serve_h1 moves into http/serve.rs, beside the posture it ... | `src/http/serve.rs` · `crate::serve_h1` · `function` · `clippy::let_underscore_must_use` | moved from src/http.rs | 45→45 | 1→1 | 65→65 | — | = | explanation update | pass |
| 15 | 714abcc2 | A task a closing runtime refuses is dropped after the reg... | `src/tasks/refusal.rs` · `crate::spawn_set_aside` · `function` · `clippy::disallowed_methods` | paired with vanished src/tasks.rs crate::TaskSupervisor::spawn | 29→17 | 1→1 | 55→43 | — | = | scope reduction | pass |
| 16 | 95bfabc6 | A spawn that unwinds closes its set-aside slot and drops ... | `src/tasks/tests.rs` · `crate::assert_teardown_completes` · `function` · `clippy::disallowed_methods` | paired with vanished crate::a_spawn_refused_by_a_closing_runtime_cannot_deadlock_its_supervisor | 39→29 | 9→1 | 67→48 | — | = | scope reduction | pass |

### Table 3 — unmatched vanish/appear pairs (same file and lint)

| Commit | Subject | Vanished contract | Appeared contract |
| --- | --- | --- | --- |
| 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/source.rs` · `crate::<SingleSource as super::feed::FeedSourceRead>` · `impl` · `clippy::unwrap_used` | `src/sse/source.rs` · `crate::<SingleSource as super::feed::FeedSourceRead>::frontier` · `function` · `clippy::unwrap_used` |
| 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | `src/sse/source.rs` · `crate::<SingleSource as super::feed::FeedSourceRead>` · `impl` · `clippy::unwrap_used` | `src/sse/source.rs` · `crate::<SingleSource as super::feed::FeedSourceRead>::closed` · `function` · `clippy::unwrap_used` |

### Roll-up — worst class per commit

| Commit | Subject | Reason edits (worst) | Moves/renames (worst) | New gate |
| --- | --- | --- | --- | --- |
| 7a6ecd23 | Merge review follow-ups, rustdoc to zero, and the rustls ... | scope expansion (fingerprint-only) | — | row needed |
| d2e87129 | Postings cache: a store load claims only to its absorbed ... | scope expansion (fingerprint-only) | — | row needed |
| 1994fc25 | A consumer pull loops back only on settled poison, and al... | scope reduction | — | pass |
| 6faf4757 | A watch wait proves shard ownership before it parks: a no... | scope expansion | — | row needed |
| d902a85b | Move the feed's permit-held drive and its retry task into... | — | scope expansion | row needed |
| 5292e3e3 | The feed's retry task never reads a subscriber's records | scope reduction | — | pass |
| 668bc80c | A topology transition never reaches a writer as closure | scope expansion (fingerprint-only) | — | row needed |
| ca36a7f4 | Move the registry's descriptor cache into registry/cache.... | — | scope expansion | row needed |
| 063a674d | The descriptor cache never publishes a read that raced a ... | scope expansion (fingerprint-only) | — | row needed |
| 71345c03 | Every h1 connection is served under a request-head deadline | scope expansion (fingerprint-only) | — | row needed |
| ac9ab99e | A sealed span's page resolves its engine on every read, n... | scope expansion (fingerprint-only) | — | row needed |
| d93b421b | A live tail whose engine retired under the same owner is ... | scope expansion | — | row needed |
| 82095942 | A durability-barrier test proves its request entered the ... | scope expansion | — | row needed |
| eb52b970 | A sweep release is not a strike: only a judged departure ... | scope expansion | — | row needed |
| 4bb51c0d | An unreadable router report defers only the desired publi... | scope expansion (fingerprint-only) | — | row needed |
| 0eb8a38d | Move the product scan page into product/scan.rs, verbatim | — | scope expansion | row needed |
| 527d3d3a | A scan page draws on the project read-byte quota like any... | scope expansion | — | row needed |
| df9ff212 | Move the product consumer pull handler into product/consu... | — | scope expansion | row needed |
| f1c9e77e | A consumer pull draws on the project read-byte quota like... | scope expansion | — | row needed |
| 5c0e62d6 | An internal receiver tells an unreadable registry apart f... | scope expansion (fingerprint-only) | — | row needed |
| 58569eb7 | Only decrypt_frame is dead in the service build, so only ... | explanation update | — | pass |
| 796211d7 | A serving descriptor has one epoch and it is never absent | scope expansion | — | row needed |
| b9f3cd7c | Cursor verdicts are typed; the scan page answers each one... | scope expansion | — | row needed |
| a1cf29f3 | Shard-open counters belong to the gate that counts them | scope expansion | — | row needed |
| 729c52ac | A merge allocates its child id before either parent seals | scope expansion | — | row needed |
| 6515a15d | A rebalance move names only a ring member, and an overrid... | scope expansion (fingerprint-only) | — | row needed |
| 33fbd10e | Every /v1/debug path answers through one bearer gate, not... | scope reduction | — | pass |
| f0c072b3 | serve_h1 moves into http/serve.rs, beside the posture it ... | — | explanation update | pass |
| a0185c3b | A panicking request handler is logged and counted on its ... | scope expansion | — | row needed |
| 714abcc2 | A task a closing runtime refuses is dropped after the reg... | scope expansion | scope reduction | row needed |
| 6ef3bc64 | A source read fails as a typed SourceReadError, never an ... | scope expansion | — | row needed |
| 69d8bc95 | The sweep custody words move behind a private-field Sweep... | scope expansion (fingerprint-only) | — | row needed |
| 07db91a7 | A cooldown past the millisecond range holds for ever inst... | scope expansion | — | row needed |
| 46d4b7df | BILLING_MODE and ROLLUP have one reader: health, the bill... | scope expansion | — | row needed |
| ed9238da | The absorber holds only what it reads: the store and key ... | scope expansion (fingerprint-only) | — | row needed |
| 95bfabc6 | A spawn that unwinds closes its set-aside slot and drops ... | — | scope reduction | pass |

### Appendix A — reason texts

**A-1** 7a6ecd23 `src/bootstrap.rs` · `crate::run` · `function` · `clippy::expect_used`

- before: "run; the runtime's task supervisor is fresh at boot, so it accepts the maintenance worker; a fallible spawn would leave the process serving without maintenance"
- after: "run; covers exactly the maintenance-worker spawn: the runtime's task supervisor is fresh at boot, so it accepts that worker; a fallible spawn would leave the process serving without maintenance"

**A-2** 7a6ecd23 `src/bootstrap.rs` · `crate::run` · `function` · `clippy::unwrap_used`

- before: "run; a poisoned cache lock at boot would mean a half-built shared cache, and the auth file paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"
- after: "run; covers exactly the shared-cache lock and the three auth file paths: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"

**A-3** d2e87129 `src/postings_cache.rs` · `crate::PostingsCache::finish_load` · `function` · `clippy::unwrap_used`

- before: "PostingsCache::spawn_load; a poisoned cache index may hold a partially installed slice or in-flight load; recovering it could serve a truncated postings slice or miscount resident bytes"
- after: "PostingsCache::finish_load; a poisoned cache index may hold a partially installed slice or in-flight load; recovering it could serve a truncated postings slice or miscount resident bytes"

**A-4** 1994fc25 `src/application/consumer/delivery.rs` · `crate::dlq_and_settle` · `function` · `clippy::too_many_arguments`

- before: "dlq_and_settle; the dead-letter path takes the stream, consumer, key, epoch, identity, route and segment separately as settlement resolved them; a context struct would exist only for this signature"
- after: "dlq_and_settle; the dead-letter path takes the stream, consumer, key, epoch, identity, engine and segment separately as settlement resolved them; a context struct would exist only for this signature"

**A-5** 6faf4757 `src/application/watch.rs` · `crate::WatchService::new` · `function` · `clippy::too_many_arguments`

- before: "WatchService::new; the service takes its registry, clock, key and limit collaborators separately as composition resolved them; a builder would exist for this single call site"
- after: "WatchService::new; the service takes its registry, shard directory, clock, key and limit collaborators separately as composition resolved them, the directory because a wait must prove route ownership before it reads a process-local journal; a builder would exist for this single call site"

**A-6** 5292e3e3 `src/sse/feed/drive.rs` · `crate::LiveFeed::schedule_transition_retry` · `function` · `clippy::disallowed_methods`

- before: "LiveFeed::schedule_transition_retry; the retry is a bare task the feed owns and cancels through its own flag, nesting the abandoned and superseded verdicts inside the wake it awaits; a supervised task would tie a feed-scoped retry to the runtime supervisor and flattening the verdicts would separate them from the wake"
- after: "LiveFeed::schedule_transition_retry; the retry is a bare task the feed owns and cancels through its own flag, nesting the abandoned and settled verdicts inside the wake it awaits; a supervised task would tie a feed-scoped retry to the runtime supervisor and flattening the verdicts would separate them from the wake"

**A-7** 5292e3e3 `src/sse/feed/drive.rs` · `crate::LiveFeed::schedule_transition_retry` · `function` · `clippy::excessive_nesting`

- before: "LiveFeed::schedule_transition_retry; the retry is a bare task the feed owns and cancels through its own flag, nesting the abandoned and superseded verdicts inside the wake it awaits; a supervised task would tie a feed-scoped retry to the runtime supervisor and flattening the verdicts would separate them from the wake"
- after: "LiveFeed::schedule_transition_retry; the retry is a bare task the feed owns and cancels through its own flag, nesting the abandoned and settled verdicts inside the wake it awaits; a supervised task would tie a feed-scoped retry to the runtime supervisor and flattening the verdicts would separate them from the wake"

**A-8** 5292e3e3 `src/sse/feed/drive.rs` · `crate::LiveFeed::bump_version` · `function` · `clippy::let_underscore_must_use`

- before: "LiveFeed::drive_under_permit; the drive nests the install and incompatibility verdicts inside the transition arm of the permit-held loop and re-publishes through a watch whose send fails only when every session is gone; flattening it or a handled send would separate the verdicts from the permit that serialises them"
- after: "LiveFeed::bump_version; the version watch fails to send only when every session is gone; a handled result would only restate that nobody waits"

**A-9** 668bc80c `src/application/append/close.rs` · `crate::closed_tail_failure` · `function` · `clippy::unwrap_used`

- before: "closed_tail_failure; a poisoned stream state may hold a half-advanced durable frontier; recovering it could report a closed length never made durable"
- after: "closed_tail_failure; the stream-state lock read for a declared closure's tail may be poisoned while it holds a half-advanced durable frontier; recovering it could report a closed length never made durable"

**A-10** 063a674d `src/registry/cache.rs` · `crate::DescriptorCache::slots` · `function` · `clippy::unwrap_used`

- before: "Registry::cache_insert; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
- before: "Registry::cache_len; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
- before: "Registry::get; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
- before: "Registry::invalidate; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
- after: "DescriptorCache::slots; a poisoned descriptor cache may hold a partially filled or invalidated slot; recovering it could serve a stale incarnation as current"

**A-11** 71345c03 `src/bin/livebench.rs` · `crate::client` · `function` · `clippy::unwrap_used`

- before: "client; the builder holds only static timeouts and pool sizes, so building the bench client cannot fail; a fallible build would only restate the panic at startup"
- after: "client; the builder holds only static timeouts and a pool that idles out inside the server keep-alive deadline, so building the bench client cannot fail; a fallible build would only restate the panic at startup"

**A-12** 71345c03 `src/bootstrap.rs` · `crate::run` · `function` · `clippy::expect_used`

- before: "run; covers exactly the maintenance-worker spawn: the runtime's task supervisor is fresh at boot, so it accepts that worker; a fallible spawn would leave the process serving without maintenance"
- after: "run; covers exactly one site, the maintenance-worker spawn: the runtime's task supervisor is fresh at boot, so it accepts that worker; a fallible spawn would leave the process serving without maintenance"

**A-13** 71345c03 `src/bootstrap.rs` · `crate::run` · `function` · `clippy::unwrap_used`

- before: "run; covers exactly the shared-cache lock and the three auth file paths: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"
- after: "run; covers exactly four sites, the shared-cache lock and the three auth file paths: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"

**A-14** ac9ab99e `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::excessive_nesting`

- before: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner, serves locally or through one redirect and caches the reader and owner hint it used, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution or a recovered hint would separate the page from the owner resolution it must repeat"
- after: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"

**A-15** ac9ab99e `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::too_many_arguments`

- before: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner, serves locally or through one redirect and caches the reader and owner hint it used, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution or a recovered hint would separate the page from the owner resolution it must repeat"
- after: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"

**A-16** ac9ab99e `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::too_many_lines`

- before: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner, serves locally or through one redirect and caches the reader and owner hint it used, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution or a recovered hint would separate the page from the owner resolution it must repeat"
- after: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"

**A-17** ac9ab99e `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::unwrap_used`

- before: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner, serves locally or through one redirect and caches the reader and owner hint it used, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution or a recovered hint would separate the page from the owner resolution it must repeat"
- after: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"

**A-18** d93b421b `src/sse/source.rs` · `crate::<SingleSource as super::feed::FeedSourceRead>` · `impl` · `clippy::unwrap_used`

- before: "SingleSource; a poisoned stream state may hold a half-advanced durable frontier; recovering it could serve a length never made durable"
- after: "SingleSource; a poisoned stream state may hold a half-advanced durable frontier and the live tail pins one engine incarnation whose close is a typed cutoff; recovering the state could serve a length never made durable and a stale read could serve a retired engine"

**A-19** d93b421b `src/sse/source.rs` · `crate::<LineageSource as super::feed::FeedSourceRead>` · `impl` · `clippy::too_many_lines`

- before: "LineageSource; one batch walks the span chain until the budget or the frontier stops it, and a poisoned stream state may hold a half-advanced durable frontier; splitting the walk would separate it from its budget and recovering the state could serve a length never made durable"
- after: "LineageSource; one batch walks the span chain until the budget or the frontier stops it, the live tail pins one engine incarnation whose close is a typed cutoff, and a poisoned stream state may hold a half-advanced durable frontier; splitting the walk would separate it from its budget, a stale read could serve a retired engine and recovering the state could serve a length never made durable"

**A-20** d93b421b `src/sse/source.rs` · `crate::<LineageSource as super::feed::FeedSourceRead>` · `impl` · `clippy::unwrap_used`

- before: "LineageSource; one batch walks the span chain until the budget or the frontier stops it, and a poisoned stream state may hold a half-advanced durable frontier; splitting the walk would separate it from its budget and recovering the state could serve a length never made durable"
- after: "LineageSource; one batch walks the span chain until the budget or the frontier stops it, the live tail pins one engine incarnation whose close is a typed cutoff, and a poisoned stream state may hold a half-advanced durable frontier; splitting the walk would separate it from its budget, a stale read could serve a retired engine and recovering the state could serve a length never made durable"

**A-21** 82095942 `src/dst/tests/durability_fences.rs` · `crate::a_fence_waits_for_durability_before_reporting_closed` · `function` · `clippy::disallowed_methods`

- before: "durability fence fixture; the held close and the competing takeover are both joined after dispatch is released; running either inline would deadlock behind the held durability barrier"
- after: "durability fence fixture; the held close and the competing takeover are proven to have entered the engine before their pending checks and are both joined after dispatch is released; running either inline would deadlock behind the held durability barrier"

**A-22** 82095942 `src/dst/tests/durability_fences.rs` · `crate::a_fence_waits_for_durability_before_reporting_closed` · `function` · `clippy::too_many_lines`

- before: "durability fence scenario; the held close, the pending takeover observation and the durable postconditions describe one causal interleaving; splitting the phases into pass-through helpers would hide which state the fence answered from"
- after: "durability fence scenario; the held close, the entered-and-pending takeover observation and the durable postconditions describe one causal interleaving; splitting the phases into pass-through helpers would hide which state the fence answered from"

**A-23** eb52b970 `src/sharddir.rs` · `crate::OpenGate::notify_closed` · `function` · `clippy::unwrap_used`

- before: "OpenGate::notify_closed; a poisoned gate state or serving map may hold a half-recorded retirement, and the resident matched under the same write guard is still present; recovering the former could retire the wrong incarnation and a fallible remove would deny a resident the guard just proved"
- after: "OpenGate::notify_closed; a poisoned gate state or serving map may hold a half-recorded retirement, the resident matched under the same write guard is still present and the close it reports is judged as evidence under those same guards; recovering the former could retire the wrong incarnation and a fallible remove would deny a resident the guard just proved"

**A-24** eb52b970 `src/sharddir.rs` · `crate::OpenGate::retire_resident` · `function` · `clippy::unwrap_used`

- before: "OpenGate::retire_resident; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff; recovering either could serve, reopen or reap the wrong incarnation"
- after: "OpenGate::retire_resident; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff and the stated reason is judged under the same guards that removed the resident; recovering either could serve, reopen or reap the wrong incarnation"

**A-25** 4bb51c0d `src/fleet.rs` · `crate::start` · `function` · `clippy::unwrap_used`

- before: "start; a poisoned timing ring may hold a half-recorded wait, and the fleet documents serialise infallibly as plain data; recovering the former or handling the latter would add branches no tick reaches"
- after: "start; a poisoned timing ring may hold a half-recorded wait, the fleet documents serialise infallibly as plain data, and an unreadable router snapshot is a typed deferral of the desired CAS rather than a panic site; recovering the ring, handling the serialisation or aborting the tick on the snapshot would add branches no tick reaches"

**A-26** 527d3d3a `src/product.rs` · `crate::product_entry` · `function` · `clippy::unwrap_used`

- before: "product_entry; the response builder holds a fixed status and literal ASCII header values, so building it cannot fail; mapping a builder error into a substitute response would report a wire status the handler never decided"
- after: "product_entry; the preflight response builder holds a fixed status and literal ASCII header values, so building it cannot fail; mapping a builder error into a substitute response would report a wire status the handler never decided"

**A-27** 527d3d3a `src/product.rs` · `crate::product_read` · `function` · `clippy::too_many_arguments`

- before: "product_read; the read takes every extractor and authorization part the entry resolved and dispatches raw, keyed and long-poll reads from one place; a request struct or a split would separate the dispatch from the parts it needs"
- after: "product_read; the read takes every extractor and the verified principal the entry resolved, dispatches raw, keyed and long-poll reads from one place and hands the principal to the page render that debits it; a request struct or a split would separate the dispatch from the parts it needs"

**A-28** 527d3d3a `src/product.rs` · `crate::product_read` · `function` · `clippy::too_many_lines`

- before: "product_read; the read takes every extractor and authorization part the entry resolved and dispatches raw, keyed and long-poll reads from one place; a request struct or a split would separate the dispatch from the parts it needs"
- after: "product_read; the read takes every extractor and the verified principal the entry resolved, dispatches raw, keyed and long-poll reads from one place and hands the principal to the page render that debits it; a request struct or a split would separate the dispatch from the parts it needs"

**A-29** 527d3d3a `src/product.rs` · `crate::render_product_read` · `function` · `clippy::unwrap_used`

- before: "render_product_read; the status is fixed and every header value was validated when the descriptor and cursor were produced, so building the response cannot fail; mapping a builder error into a substitute response would report a wire status the handler never decided"
- after: "render_product_read; the status is fixed and every header value was validated when the descriptor and cursor were produced, so building the response cannot fail once the served bytes are debited; mapping a builder error into a substitute response would report a wire status the handler never decided"

**A-30** 527d3d3a `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::expect_used`

- before: "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail; mapping either into a substitute response would report a wire status the handler never decided"
- after: "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail once the page is debited; mapping either into a substitute response would report a wire status the handler never decided"

**A-31** 527d3d3a `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::unwrap_used`

- before: "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail; mapping either into a substitute response would report a wire status the handler never decided"
- after: "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail once the page is debited; mapping either into a substitute response would report a wire status the handler never decided"

**A-32** 527d3d3a `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::too_many_lines`

- before: "product_scan; the scan resolves, admits and pages the frozen cursor in one sequence; splitting it would separate the page from the cursor it advances"
- after: "product_scan; the scan resolves, decodes and pages the frozen cursor and debits the page it frames in one sequence; splitting it would separate the page from the cursor it advances and the bytes it charges"

**A-33** f1c9e77e `src/product.rs` · `crate::product_entry` · `function` · `clippy::unwrap_used`

- before: "product_entry; the preflight response builder holds a fixed status and literal ASCII header values, so building it cannot fail; mapping a builder error into a substitute response would report a wire status the handler never decided"
- after: "product_entry; the preflight response builder holds a fixed status and literal ASCII header values, so building it cannot fail, and every handler the entry dispatches to (the consumer pull now with its identity resolved here) decides its own wire status; mapping a builder error into a substitute response would report a status the handler never decided"

**A-34** f1c9e77e `src/product.rs` · `crate::product_entry` · `function` · `clippy::excessive_nesting`

- before: "product_entry; the product entry takes every extractor axum resolved and dispatches every method and sub-path from one match whose admission refusal nests inside the tagged audit; a request struct, a split or a flattened refusal would separate the dispatch from the extractors and the refusal from the audit it tags"
- after: "product_entry; the product entry takes every extractor axum resolved, resolves the stream identity the typed handlers take and dispatches every method and sub-path from one match whose admission refusal nests inside the tagged audit; a request struct, a split or a flattened refusal would separate the dispatch from the extractors and the refusal from the audit it tags"

**A-35** f1c9e77e `src/product.rs` · `crate::product_entry` · `function` · `clippy::too_many_arguments`

- before: "product_entry; the product entry takes every extractor axum resolved and dispatches every method and sub-path from one match whose admission refusal nests inside the tagged audit; a request struct, a split or a flattened refusal would separate the dispatch from the extractors and the refusal from the audit it tags"
- after: "product_entry; the product entry takes every extractor axum resolved, resolves the stream identity the typed handlers take and dispatches every method and sub-path from one match whose admission refusal nests inside the tagged audit; a request struct, a split or a flattened refusal would separate the dispatch from the extractors and the refusal from the audit it tags"

**A-36** f1c9e77e `src/product.rs` · `crate::product_entry` · `function` · `clippy::too_many_lines`

- before: "product_entry; the product entry takes every extractor axum resolved and dispatches every method and sub-path from one match whose admission refusal nests inside the tagged audit; a request struct, a split or a flattened refusal would separate the dispatch from the extractors and the refusal from the audit it tags"
- after: "product_entry; the product entry takes every extractor axum resolved, resolves the stream identity the typed handlers take and dispatches every method and sub-path from one match whose admission refusal nests inside the tagged audit; a request struct, a split or a flattened refusal would separate the dispatch from the extractors and the refusal from the audit it tags"

**A-37** f1c9e77e `src/product/consumer_pull.rs` · `crate::product_consumer_pull` · `function` · `clippy::too_many_arguments`

- before: "product_consumer_pull; the parameters are the request's typed context parts, not tunables; a bundle struct for this single call site would only rename the same positional list"
- after: "product_consumer_pull; the parameters are the request's typed context parts (resolved stream identity, consumer, headers, body, access), not tunables; a bundle struct for this single call site would only rename the same positional list"

**A-38** 5c0e62d6 `src/product.rs` · `crate::internal_sweep_segment` · `function` · `clippy::expect_used`

- before: "internal_sweep_segment; the outcome derives Serialize with plain fields, so converting it to a JSON value cannot fail; a fallible conversion would turn a completed operation into a spurious wire error"
- after: "internal_sweep_segment; the outcome is rendered only after the typed registry prelude and the incarnation check admitted the target, and it derives Serialize with plain fields, so converting it to a JSON value cannot fail; a fallible conversion would turn a completed operation into a spurious wire error"

**A-39** 5c0e62d6 `src/product.rs` · `crate::internal_queue_cursor` · `function` · `clippy::expect_used`

- before: "internal_queue_cursor; the outcome derives Serialize with plain fields, so converting it to a JSON value cannot fail; a fallible conversion would turn a completed operation into a spurious wire error"
- after: "internal_queue_cursor; the position is rendered only after the typed registry prelude and the incarnation check admitted the target, and it derives Serialize with plain fields, so converting it to a JSON value cannot fail; a fallible conversion would turn a completed operation into a spurious wire error"

**A-40** 58569eb7 `src/crypto.rs` · `crate::decrypt_frame` · `function` · `dead_code`

- before: "crypto owner; the by-path side bins and the fuzz harness include this module and use only the hashing or codec half, so the service's other entry points are unused there; a cfg gate per item would fork the module's surface between builds"
- after: "decrypt_frame; the whole-record offline decoder capped at MAX_RECORD_PLAINTEXT serves the keys CLI, cryptobench and tests while every service read decrypts through FrameDecryptor under its page limit, so the service library has no caller; expect(dead_code) would be unfulfilled in the test build and in the by-path bins that call it"

**A-41** 796211d7 `src/application/creation.rs` · `crate::fresh_desc` · `function` · `clippy::too_many_arguments`

- before: "fresh_desc; a fresh descriptor is built from the resolved name, epoch, policy and fork parts separately as creation decided them; a builder would restate the descriptor's own fields"
- after: "fresh_desc; a fresh descriptor is built from the resolved name, key, content type and expiry policy as creation decided them, and returns the epoch it minted; a builder would restate the descriptor's own fields"

**A-42** b9f3cd7c `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::expect_used`

- before: "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail once the page is debited; mapping either into a substitute response would report a wire status the handler never decided"
- after: "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail once each typed cursor verdict is answered and the page is debited; mapping either into a substitute response would report a wire status the handler never decided"

**A-43** b9f3cd7c `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::unwrap_used`

- before: "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail once the page is debited; mapping either into a substitute response would report a wire status the handler never decided"
- after: "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail once each typed cursor verdict is answered and the page is debited; mapping either into a substitute response would report a wire status the handler never decided"

**A-44** b9f3cd7c `src/product/scan.rs` · `crate::product_scan` · `function` · `clippy::too_many_lines`

- before: "product_scan; the scan resolves, decodes and pages the frozen cursor and debits the page it frames in one sequence; splitting it would separate the page from the cursor it advances and the bytes it charges"
- after: "product_scan; the scan resolves the collection, answers each typed cursor verdict, pages the frozen cursor and debits the page it frames in one sequence; splitting it would separate the page from the cursor it advances and the bytes it charges"

**A-45** a1cf29f3 `src/sharddir.rs` · `crate::OpenGate::get_or_open` · `function` · `clippy::unwrap_used`

- before: "OpenGate::get_or_open; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff; recovering either could serve, reopen or reap the wrong incarnation"
- after: "OpenGate::get_or_open; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff beside this gate's own open counters; recovering either could serve, reopen or reap the wrong incarnation"

**A-46** a1cf29f3 `src/store_timing/observations.rs` · `crate::snapshot` · `function` · `clippy::unwrap_used`

- before: "process slow-operation ring; poison may follow an interrupted sample update; recovery would present partial diagnostic state as valid"
- after: "process slow-operation ring read beside the caller's shard-open counters; poison may follow an interrupted sample update; recovery would present partial diagnostic state as valid"

**A-47** 729c52ac `src/segmap.rs` · `crate::SegmentMap::merge` · `function` · `clippy::too_many_arguments`

- before: "SegmentMap::merge; a merge names both segments, the new segment's id, epoch and owner and the transition version separately as the rebalancer decided them; a request struct would exist for this single call site"
- after: "SegmentMap::merge; phase B supplies both parents, each parent's frozen next offset, the child's route and the clock as separate facts it proved; a request struct would exist for this single call site"

**A-48** 729c52ac `src/segmap.rs` · `crate::SegmentMap::merge` · `function` · `clippy::unwrap_used`

- before: "SegmentMap::merge; both segment ids were validated live and adjacent before the merge, so the lookup finds them; a fallible find would add a branch no validated merge reaches"
- after: "SegmentMap::merge; both parents were found live and adjacent and the child id allocated before either seals, so the lookup finds them; a fallible find would add a branch no validated merge reaches"

**A-49** 6515a15d `src/fleet.rs` · `crate::start` · `function` · `clippy::unwrap_used`

- before: "start; a poisoned timing ring may hold a half-recorded wait, the fleet documents serialise infallibly as plain data, and an unreadable router snapshot is a typed deferral of the desired CAS rather than a panic site; recovering the ring, handling the serialisation or aborting the tick on the snapshot would add branches no tick reaches"
- after: "start; a poisoned timing ring may hold a half-recorded wait, the fleet documents serialise infallibly as plain data, an unreadable router snapshot is a typed deferral of the desired CAS, and the move target and the eager move-in are ring decisions over the view this tick published rather than panic sites; recovering the ring, handling the serialisation or aborting the tick on the snapshot would add branches no tick reaches"

**A-50** 33fbd10e `src/http.rs` · `crate::debug_routes` · `function` · `clippy::disallowed_methods`

- before: "router; the route table is one declaration so every path is visible in one place, and the debug abort spawns a bare task that ends the process itself; splitting the table or supervising the abort would separate the routes from the table and the abort from the death it causes"
- after: "debug_routes; the operator debug table is one declaration behind one gate, and the debug abort spawns a bare task that ends the process itself; splitting the table would scatter what the gate covers and supervising the abort would separate it from the death it causes"

**A-51** 33fbd10e `src/http.rs` · `crate::debug_routes` · `function` · `clippy::too_many_lines`

- before: "router; the route table is one declaration so every path is visible in one place, and the debug abort spawns a bare task that ends the process itself; splitting the table or supervising the abort would separate the routes from the table and the abort from the death it causes"
- after: "debug_routes; the operator debug table is one declaration behind one gate, and the debug abort spawns a bare task that ends the process itself; splitting the table would scatter what the gate covers and supervising the abort would separate it from the death it causes"

**A-52** a0185c3b `src/http/serve.rs` · `crate::serve_h1` · `function` · `clippy::disallowed_methods`

- before: "serve_h1; each accepted connection is served by a task the listener's own JoinSet owns and joins at shutdown, and nodelay and connection errors are routine client behaviour; a supervised task per connection and handled results would restate what the JoinSet already owns"
- after: "serve_h1; each accepted connection is served by a task the listener's own JoinSet owns, reaps (counting a panicked one) and joins at shutdown, and nodelay and connection errors are routine client behaviour; a supervised task per connection and handled connection results would restate what the JoinSet already owns"

**A-53** a0185c3b `src/http/serve.rs` · `crate::serve_h1` · `function` · `clippy::let_underscore_must_use`

- before: "serve_h1; each accepted connection is served by a task the listener's own JoinSet owns and joins at shutdown, and nodelay and connection errors are routine client behaviour; a supervised task per connection and handled results would restate what the JoinSet already owns"
- after: "serve_h1; each accepted connection is served by a task the listener's own JoinSet owns, reaps (counting a panicked one) and joins at shutdown, and nodelay and connection errors are routine client behaviour; a supervised task per connection and handled connection results would restate what the JoinSet already owns"

**A-54** 714abcc2 `src/tasks.rs` · `crate::TaskSupervisor::spawn` · `function` · `clippy::unwrap_used`

- before: "Supervisor registration; a poisoned phase may contain an incomplete task insertion; recovering and spawning again could leave a task outside the eventual drain"
- after: "Supervisor registration; a poisoned phase may contain an incomplete task insertion, and a task a closing runtime refused is dropped only after the lock is released; recovering and spawning again could leave a task outside the eventual drain"

**A-55** 714abcc2 `src/tasks/tests.rs` · `crate::a_spawn_refused_by_a_closing_runtime_cannot_deadlock_its_supervisor` · `function` · `clippy::disallowed_methods`

- before: "TaskSupervisor worker owner; the registration lock retains each handle before shutdown can take the map; spawning through another supervisor would recursively delegate this canonical owner"
- after: "F-G deadlock rig; the runtime drop under test may never return, so it runs on a detached thread the bounded receive below observes; a runtime task cannot host the drop of its own runtime"
- after: "F-G deadlock rig; the task exists only to be dropped by the runtime teardown under test; supervising it would put the supervisor under test in its own teardown path"

**A-56** 6ef3bc64 `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::cast_possible_truncation`

- before: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, handling the watch send, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"
- after: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"

**A-57** 6ef3bc64 `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::excessive_nesting`

- before: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, handling the watch send, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"
- after: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"

**A-58** 6ef3bc64 `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::expect_used`

- before: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, handling the watch send, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"
- after: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"

**A-59** 6ef3bc64 `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::too_many_lines`

- before: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, handling the watch send, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"
- after: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"

**A-60** 6ef3bc64 `src/sse/feed.rs` · `crate::LiveFeed::read_and_publish` · `function` · `clippy::wildcard_enum_match_arm`

- before: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, handling the watch send, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"
- after: "LiveFeed::read_and_publish; one read publishes its batch, charges retention, evicts within the ring budget from a pre-counted eviction set and answers every other reserve outcome alike, with payload lengths that fit u32 by the record ceiling; splitting it, checking the length, flattening the eviction, naming every outcome or a fallible pop would separate the publication from the budget it must honour"

**A-61** 6ef3bc64 `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::excessive_nesting`

- before: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"
- after: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"

**A-62** 6ef3bc64 `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::too_many_arguments`

- before: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"
- after: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"

**A-63** 6ef3bc64 `src/sse/source.rs` · `crate::LineageSource::sealed_span_page` · `function` · `clippy::unwrap_used`

- before: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"
- after: "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"

**A-64** 6ef3bc64 `src/sse/source.rs` · `crate::<LineageSource as super::feed::FeedSourceRead>` · `impl` · `clippy::too_many_lines`

- before: "LineageSource; one batch walks the span chain until the budget or the frontier stops it, the live tail pins one engine incarnation whose close is a typed cutoff, and a poisoned stream state may hold a half-advanced durable frontier; splitting the walk would separate it from its budget, a stale read could serve a retired engine and recovering the state could serve a length never made durable"
- after: "LineageSource; one batch walks the span chain until the budget or the frontier stops it and names each failure's typed verdict, and a poisoned stream state may hold a half-advanced durable frontier; splitting the walk would separate it from its budget and recovering the state could serve a length never made durable"

**A-65** 6ef3bc64 `src/sse/source.rs` · `crate::<LineageSource as super::feed::FeedSourceRead>` · `impl` · `clippy::unwrap_used`

- before: "LineageSource; one batch walks the span chain until the budget or the frontier stops it, the live tail pins one engine incarnation whose close is a typed cutoff, and a poisoned stream state may hold a half-advanced durable frontier; splitting the walk would separate it from its budget, a stale read could serve a retired engine and recovering the state could serve a length never made durable"
- after: "LineageSource; one batch walks the span chain until the budget or the frontier stops it and names each failure's typed verdict, and a poisoned stream state may hold a half-advanced durable frontier; splitting the walk would separate it from its budget and recovering the state could serve a length never made durable"

**A-66** 69d8bc95 `src/shard.rs` · `crate::ShardEngine::start` · `function` · `clippy::unwrap_used`

- before: "ShardEngine::start; a poisoned in-flight queue or trim-debt set may hold a half-recorded group or debt; recovering either could acknowledge a group that never committed or trim a stream that still owes data"
- after: "ShardEngine::start; the pump and trim tickers unwrap only the in-flight queue and trim-debt locks, and a poisoned one may hold a half-recorded group or debt; recovering either could acknowledge a group that never committed or trim a stream that still owes data"

**A-67** 07db91a7 `src/scaler3.rs` · `crate::State::prune` · `function` · `clippy::unwrap_used`

- before: "State::prune; the sketch table just exceeded its cap, so at least one entry exists to pick as the victim; a fallible pick would turn the cap into a no-op"
- after: "State::prune; the cooldown table just exceeded its cap, so at least one entry exists to pick as the victim; a fallible pick would turn the cap into a no-op"

**A-68** 07db91a7 `src/scaler3.rs` · `crate::evaluate_state` · `function` · `clippy::too_many_lines`

- before: "evaluate_state; one pass ranks every sketched segment against the same policy and limits; splitting it would hide which rule chose each split or merge"
- after: "evaluate_state; one pass ranks every sketched segment against the same policy, limits and cooldown clock; splitting it would hide which rule chose each split or merge"

**A-69** 46d4b7df `src/dst/tests/fixture_http.rs` · `crate::http_rig_build` · `function` · `clippy::too_many_lines`

- before: "HTTP rig builder; every runtime owner is wired in one place so the fixture's dependency order stays visible to scenario authors; pass-through steps would hide which owner a scenario option changed"
- after: "HTTP rig builder; every runtime owner and the scenario's command-line edit are wired in one place so the fixture's dependency order stays visible to scenario authors; pass-through steps would hide which owner a scenario option changed"

**A-70** 46d4b7df `src/dst/tests/fixture_http.rs` · `crate::http_rig_build` · `function` · `clippy::let_underscore_must_use`

- before: "http_rig_build; the supervisor rejects a spawn only while it is stopping, when the rig is being torn down; a rejected rig task has nothing left to serve"
- after: "http_rig_build; the supervisor rejects a spawn only while it is stopping, when the rig is being torn down, whatever command line the scenario configured; a rejected rig task has nothing left to serve"

**A-71** ed9238da `src/bootstrap.rs` · `crate::run` · `function` · `clippy::expect_used`

- before: "run; covers exactly one site, the maintenance-worker spawn: the runtime's task supervisor is fresh at boot, so it accepts that worker; a fallible spawn would leave the process serving without maintenance"
- after: "run; covers exactly one site, the maintenance-worker spawn, and none in the shard opener: the runtime's task supervisor is fresh at boot, so it accepts that worker; a fallible spawn would leave the process serving without maintenance"

**A-72** ed9238da `src/bootstrap.rs` · `crate::run` · `function` · `clippy::unwrap_used`

- before: "run; covers exactly four sites, the shared-cache lock and the three auth file paths: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"
- after: "run; covers exactly four sites, the shared-cache lock and the three auth file paths, and none in the shard opener: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"

<!-- commits: 125, reason edits: 72, moves/renames: 16, unmatched: 2 -->
