# Item 71: remove crypto.rs's redundant per-item `allow(dead_code)`

Tree: `slate` @ `fb18840d` (origin/slate = `7c4f8606`; none of the 4 unpushed commits touch `src/crypto.rs`, the five by-path includers or any ledger named below).
Reviewer text: robustness-maintainability-review.md §71 (Where `src/crypto.rs:138-147, 209-226, 641`; Change "delete the 10 redundant allows, keep one narrow allow on decrypt_frame with a corrected reason, prune source-allowances via gate.py --prune").

**Verdict.** The finding holds on the current tree, and the change can be built. One correction to the reviewer: there is **nothing to prune**. `docs/quality/source-allowances.json` has no rows for these reasoned allowances, and the only rows that name them are in the immutable adoption ceiling `docs/quality/legacy-source.json`, which must not be edited. The change is one commit to one file: `src/crypto.rs`, 736 → 696 lines, 1 insertion and 41 deletions. No function body changes and no mutants are generated.

---

## 1 Problem (verified)

### 1.1 The eleven per-item allowances (all present, all with the same copied reason)

Each is the same 4-line block (top-level form shown; the impl-method copies are indented by 4 more spaces):

```rust
#[allow(
    dead_code,
    reason = "crypto owner; the by-path side bins and the fuzz harness include this module and use only the hashing or codec half, so the service's other entry points are unused there; a cfg gate per item would fork the module's surface between builds"
)]
```

| # | lines (HEAD) | item | other exceptions on the same item |
|---|---|---|---|
| 1 | 138-141 | `WATCH_CAP_MAX_LIFETIME_SECS` (const, l.142) | none |
| 2 | 143-146 | `WATCH_CAP_SKEW_SECS` (const, l.147) | none |
| 3 | 164-167 | `watch_capability_sig` (fn, l.172) | sits between `#[expect(clippy::too_many_arguments)]` (160-163) and the fn-wide `#[expect(clippy::expect_used)]` (168-171) |
| 4 | 209-212 | `watch_capability_project` (fn, l.213) | none |
| 5 | 222-225 | `verify_watch_capability` (fn, l.226) | `#[expect(clippy::too_many_arguments)]` (218-221) |
| 6 | 318-321 | `RouteHash::for_stream` (l.322) | none |
| 7 | 328-331 | `RouteHash::for_child` (l.332) | none |
| 8 | 360-363 | `SegmentHash::for_stream` (l.364) | none |
| 9 | 374-377 | `SegmentHash::for_segment` (l.378) | none |
| 10 | 641-644 | `decrypt_frame` (fn, l.645) | none; preceded by `// Shared by the standalone keys/cryptobench tools; server pages use bounded reads.` (l.640) |
| 11 | 722-725 | `secret_eq` (fn, l.726) | none |

The reviewer's line ranges (138-147, 209-226, 641) are a subset of these. The count "10 redundant + decrypt_frame" is correct.

### 1.2 Every by-path includer already allows dead_code on the whole module

| includer | lines | module-wide attribute on `mod crypto;` |
|---|---|---|
| `src/bin/keys.rs` | 8-13 | `#[allow(dead_code, reason = "Key CLI crypto inclusion; …")]` |
| `src/bin/livebench.rs` | 34-39 | `#[allow(dead_code, reason = "benchmark module selection; …")]` |
| `src/bin/cryptobench.rs` | 4-9 | `#[allow(dead_code, reason = "crypto benchmark module selection; …")]` |
| `tools/quality-invariants/src/lib.rs` | 5-12 | `#[allow(dead_code, unreachable_pub, unused_imports, reason = "invariant harness; …")]` |
| `fuzz/fuzz_targets/postings.rs` | 5-12 | `#[allow(dead_code, unreachable_pub, unused_imports, reason = "postings fuzz target; …")]` |

A lint level on `mod crypto;` is inherited by every item in the file, so the per-item allows do nothing in these five builds. `git log -S dead_code` shows the three bin allows date from `961513af` (2026-08-15) and the harness/fuzz allows from `a1d9dabf` and `404bc1f3` (2026-09-08). All of them predate `647802f8` (2026-09-11), which converted the old per-item comments (`// unused only in the crypto-sharing side bins`, `// consumed at MT Stage 3`) into the current reason text. The reason drifted: MT Stage 3 has landed, and every one of these items now has a service caller.

The only other copy of the module is the service library (`src/lib.rs:30 mod crypto;`, a private module). That is the only build in which the per-item allows take effect. In rustc's dead-code pass, an item whose effective `dead_code` level is allow or expect is a **root**. So each allowance (a) hides the item's own deadness and (b) keeps everything it calls alive too: `tenant::route_hash_input`, `route_child_hash_input`, `storage_hash_input`, `segment_identity_input`, `encode_hash_input`, `HashDomain::WatchCapabilityV1`, `ProjectId::new`, the two consts and `watch_capability_sig`.

### 1.3 Use sites (grep `-w` over `src tools fuzz`; production means outside `#[cfg(test)]`, in the lib)

| item | production callers in the service lib (enclosing fn) | test / by-path users |
|---|---|---|
| `WATCH_CAP_MAX_LIFETIME_SECS` | `src/crypto.rs:253` (`verify_watch_capability`) | none |
| `WATCH_CAP_SKEW_SECS` | `src/crypto.rs:250, 253` (`verify_watch_capability`) | none |
| `watch_capability_sig` | `src/crypto.rs:256` (`verify_watch_capability`) | `src/crypto.rs:690` (`capability_vector::watch_capability_vector_is_pinned`), `src/dst/review_security.rs:127`, `src/dst/tests/quota_enforcement.rs:693`, `src/dst/tests/security_routes.rs:243`, `src/dst/tests/watch_observation.rs:330,355,374,456,522,633,700`; comment only: `src/bin/livebench.rs:131` |
| `watch_capability_project` | `src/application/watch.rs:198` (`WatchService::authenticate`, called from `src/product.rs:3828`) | none |
| `verify_watch_capability` | `src/application/watch.rs:240` (`WatchService::authenticate`) | none |
| `RouteHash::for_stream` | `src/registry.rs:726` (`segment_route`), `:737` (`segment_route_by_id`), `:747` (`resolve_segment`); `src/http/read.rs:417` (`render_raw_read`); `src/sse/session.rs:265, 312`; `src/application/append.rs:337`, `append/admission.rs:18`, `watch.rs:146, 324`, `consumer.rs:398`, `creation/anchor.rs:30`, `creation/product.rs:34`, `creation/raw.rs:20, 93`, `creation/initialization.rs:99, 132` | `src/dst/tests/{runtime_usage,runtime_sweep,watch_observation,runtime_isolation,reads_raw,seal_convergence,fixture_http}.rs`, `src/registry/tests.rs:166-167`, `src/registry/resolution_tests.rs:22, 94` |
| `RouteHash::for_child` | `src/application/topology.rs:510` (`resume_incarnation`, called at 313/315/393/395/402/410) | none |
| `SegmentHash::for_stream` | `src/registry.rs:675` (`PersistedDescriptor::storage_hash`) | none directly |
| `SegmentHash::for_segment` | `src/registry.rs:702` (`PersistedDescriptor::dynamic_segment_identity`, called from `registry.rs:773`, `billing.rs:2032`, `http.rs:3231`) | none directly |
| `secret_eq` | `src/deployment_bearer.rs:30` (`DeploymentBearer::authorizes` ← `http.rs:356`), `src/peer.rs:113` (`PeerClient::inbound_static_ok` ← `http.rs:451, 517`) | `deployment_bearer::tests`, `peer::tests` |
| `decrypt_frame` | **none** | by-path bins: `src/bin/keys.rs:127`, `src/bin/cryptobench.rs:17, 92`; tests: `src/crypto/tests.rs:28, 33`, `src/crypto/compress_tests.rs:36, 90, 109, 122`, `src/crypto/invocation_tests.rs:46, 57, 92, 103, 115, 134`, `src/shard/record/checked.rs:121, 158, 189` (inside `#[cfg(test)] mod tests` at 118), `src/shard/transaction_tests.rs:184` |

None of the production callers is under a `#[cfg(test)]` statement attribute (the cfg(test) failpoint statements at `topology.rs:452`, `sse/session.rs:249` and `http/read.rs:566` are separate statements). None of the enclosing functions carries its own `allow(dead_code)`: the only other lib-level dead_code exceptions are in `history.rs:754`, `http.rs:1750`, `postings_cache.rs:61`, `project_policy.rs:16` (module-wide, does not reach these callers), `queue.rs:407` and `sse/source.rs:170`. The current tree passes `clippy -D warnings`, so every enclosing caller is live in the lib's non-test build. That makes ten of the eleven items live there without their allowances.

### 1.4 decrypt_frame's reason states the opposite of the facts

The reason says the by-path side bins "use only the hashing or codec half, so the service's other entry points are unused there". For `decrypt_frame` it is the reverse: the service never calls it, because service reads decrypt through `FrameDecryptor` (`src/application/read_keys.rs:2, 68, 78`) under a page limit. The by-path bins `keys` and `cryptobench` are the users. The `//` comment on line 640 already states this correctly, so the attribute contradicts the comment right above it.

`decrypt_frame_limited` (l.657) has exactly one caller, `decrypt_frame` (l.651). No other file names it. It is alive in the service build only because the `decrypt_frame` allowance roots it. Keeping that allowance keeps this unchanged (see §8).

### 1.5 Why `expect` / `cfg_attr` is not the remedy for decrypt_frame

- `#[expect(dead_code)]` would be unfulfilled in the lib **test** build, where tests call it. With the `unfulfilled_lint_expectations` lint denied, that is an error.
- `#[cfg_attr(not(test), expect(dead_code))]` would still be unfulfilled in the non-test builds of `keys` and `cryptobench`, which call it. The inner `expect` overrides their module-wide allow. It would also stop the planner's `production_unchanged` normaliser from recognising the attribute as a direct lint annotation.
- RUST-QUALITY.md: "Narrow `allow(..., reason = "...")` is permitted when conditional compilation makes expectations unreliable." This is that case, so the allow stays.

---

## 2 Contract decision

**No wire change and no typed-contract change.** Lint-level attributes affect only diagnostics. They change no type, signature, visibility, expression or codegen, so everything below stays byte-identical:

- the watch capability wire form `<project>.<exp>.<sig>`
- its HMAC input and pinned vector `381d52c5438c2b10393c4697a001e5a5`
- the route, storage and segment hashes
- the frame codec
- the constant-time bearer comparison

Nothing reaches the product or raw edge, so no WIRE-MATRIX row and no decision on the backward-compatible alternative are needed.

The one contract that does change is the **lint contract**. After this commit, the service library's dead-code analysis covers the ten items and, transitively, their `tenant.rs` hash-input callees. If a future change removes the last production caller of any of them, it fails `clippy -D warnings` instead of passing silently. `decrypt_frame` keeps its one exemption, with a reason that states the actual direction.

---

## 3 Red tests / pinning tests

**This is a pure refactor with no behaviour change, so there is no red test.** A red test cannot exist because rustc lint levels do not reach codegen. What follows are the proof that the change compiles and the existing tests that pin behaviour.

### 3.1 Falsifiable compile control (red on a scratch variant, never committed)

Delete **all eleven** allowances, including decrypt_frame's, and run `cargo clippy --locked -p streams-slate --lib -- -D warnings`. The expected output is exactly two errors:

```
error: function `decrypt_frame` is never used
   --> src/crypto.rs:605:15
    |
605 | pub(crate) fn decrypt_frame(
    |               ^^^^^^^^^^^^^
    |
    = note: `-D dead-code` implied by `-D warnings`
    = help: to override `-D warnings` add `#[allow(dead_code)]`

error: function `decrypt_frame_limited` is never used
   --> src/crypto.rs:617:15
    |
617 | pub(crate) fn decrypt_frame_limited(
    |               ^^^^^^^^^^^^^^^^^^^^^

error: could not compile `streams-slate` (lib) due to 2 previous errors
```

The line numbers were computed on a simulated copy with all eleven blocks deleted (692 lines). Rustc reports free functions one diagnostic each, matching `scripts/clippy-baseline-fingerprints.txt` (`function `touch_token` is never used`, `function `unhex` is never used` …); the note/help wording is the pinned toolchain's (1.98.1) and not load-bearing. This control shows three things:

- **(a)** The lib's non-test dead-code pass does reach `crypto.rs` items, so the deletions are checked rather than masked by some enclosing allow.
- **(b)** No dead-code error names the other nine functions or constants, so the ten are live.
- **(c)** `decrypt_frame`, with its sole callee, is the only dead item.

Treat any additional error as a stop signal: it means a presumed-live item is dead, and the commit must keep that item's allowance with a new, true reason.

### 3.2 Green compile proof (the committed tree)

`cargo clippy --locked --workspace --all-targets -- -D warnings` exits 0. That one run compiles `crypto.rs` in seven configurations:

- lib non-test and lib test
- `keys`, `livebench`, `cryptobench` (bin and bin-test)
- `streams-quality-invariants` (lib and lib-test)
- `streams-quality-fuzz`

### 3.3 Existing tests that pin the touched items (sanity only; codegen is identical)

- `crypto::capability_vector::watch_capability_vector_is_pinned` (`src/crypto.rs`): `watch_capability_sig` byte-for-byte.
- `crypto::tests::round_trip_and_fresh_invocation_nonce`, `crypto::tests::fingerprint_stable`, `crypto::compress_tests::{new_compressed_frame_round_trip,new_uncompressed_frame_round_trip,unknown_version_rejected}`, `crypto::invocation_tests::{r01_rfc8452_aes256_empty_plaintext_vector,r01_segments_and_reused_offsets_have_safe_invocation_domains,r01_retained_legacy_frames_and_new_versions_read_together,r01_frame_golden_header_and_ciphertext}`: `decrypt_frame`.
- `deployment_bearer::tests::{unset_bearer_opens_only_the_off_mode,set_bearer_compares_in_every_mode}`, `peer::tests::credential_modes_are_exclusive`: `secret_eq`.
- `dst_tests::watch_observation::{watch_urls_verify_on_a_process_that_never_saw_the_key,watch_capability_respects_project_suspension,watch_capability_waits_occupy_project_admission,stale_policy_fails_watch_capabilities_closed}`, `dst::review_security::r16_unauthorized_watch_shapes_are_independent_of_existence`: `verify_watch_capability`, `watch_capability_project` and the lifetime constants.
- `registry::tests`, `registry::resolution_tests`, `dst_tests::watch_observation::touch_close_shard_matches_route_hash_not_storage_hash`: `RouteHash` / `SegmentHash` constructors.

---

## 4 Edits, in commit order

**One commit.** No verbatim-move commit is needed, because nothing moves and no file over 1,000 lines is touched.

### Ceilinged files (current `wc -l`; this change's budget = 0, none touched)

| file | wc -l | touched |
|---|---|---|
| src/http.rs | 3,369 | no |
| src/product.rs | 4,205 | no |
| src/shard.rs | 3,196 | no |
| src/billing.rs | 2,201 | no |
| src/history.rs | 1,713 | no |
| src/auth.rs | 1,676 | no |
| src/registry.rs | 1,501 | no |
| src/sse/feed.rs | 1,195 | no |
| src/fleet.rs | 1,143 | no |
| **src/crypto.rs** (not ceilinged) | 736 → **696** | yes |

### Commit 1: `src/crypto.rs` only

1. Delete the 4-line allow blocks #1-#9 and #11 (HEAD lines 138-141, 143-146, 164-167, 209-212, 222-225, 318-321, 328-331, 360-363, 374-377, 722-725). Nothing else in those neighbourhoods changes: doc comments, the two `expect`s on `watch_capability_sig`, the `expect` on `verify_watch_capability`, and all signatures and bodies stay as they are.
2. In the decrypt_frame block (HEAD 641-644), replace **only the reason line** (HEAD 643). Keep the `// Shared by the standalone keys/cryptobench tools; …` comment on 640, which is already true. The new block:

```rust
// Shared by the standalone keys/cryptobench tools; server pages use bounded reads.
#[allow(
    dead_code,
    reason = "decrypt_frame; the unbounded offline decoder serves the keys CLI, cryptobench and tests while every service read decrypts through FrameDecryptor under its page limit, so the service library has no caller; expect(dead_code) would be unfulfilled in the test build and in the by-path bins that call it"
)]
pub(crate) fn decrypt_frame(
```

The reason has exactly two `;` and no internal `"`, so it matches the owner; invariant; alternative regex `"[^";]+;[^";]+;[^";]+"` in `scripts/quality/source_rules.py` (checked by simulation). The first segment names the owner item, following the file's other reasons (`StreamKey::fingerprint; …`, `watch_capability_sig; …`).

Simulated result: `1 file changed, 1 insertion(+), 41 deletions(-)`, 9 hunks, 696 lines.

**Suggested commit subject** (repo style): `Only decrypt_frame is dead in the service build, so only it keeps a dead-code allowance`. The body should say:

- every by-path includer allows dead_code on the whole module
- an allowed item is a dead-code root, so the ten allowances hid live items and their tenant hash-input callees from the service build
- decrypt_frame's reason was reversed
- the change is annotation-only, and the crypto owner's in-diff mutation selection is empty
- no source-allowance rows existed

End with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

### Exception ratchets touched (`scripts/quality/source_rules.py::exception_contracts` and `exception_growth`, both computed from merge-base source at run time, not stored in a baseline file)

| function | exceptions on it | effect of the edit | remedy |
|---|---|---|---|
| `watch_capability_sig` | `expect(clippy::too_many_arguments)`, **fn-wide `expect(clippy::expect_used)`** | the item span (starts at doc line 149) shrinks by 4: `scope_lines` 50 → 46; `syntax_facts` drops by ≥1 (the removed attribute); `expect_site:ordinary-call:*` / `expect_site:path:*` fingerprints unchanged or fewer (the body's calls `encode_hash_input`, `<Hmac<Sha256> as Mac>::new_from_slice`, `.expect`, `update`, `finalize`, `hex` are untouched) | none; every metric is equal or lower, and `exception_growth` fails only on growth |
| `verify_watch_capability` | `expect(clippy::too_many_arguments)` | span 218-272: `scope_lines` 55 → 51; `syntax_facts` −≥1 | none |
| `decrypt_frame` | the rewritten allow | new identity (value changed) → `exception_growth` skips it ("A new/changed reason is the explicit review decision"); `violations` checks the owner;invariant;alternative shape → passes | Søren approves the wording (see decisions) |
| the ten deleted allows | — | their contract identities disappear; `exception_growth` iterates only current identities, and no vacated row exists anywhere | none |

**Callers carrying fn-wide unwrap/expect exceptions whose call paths name these items:**

- `WatchService::authenticate` (`src/application/watch.rs`, `expect(clippy::unwrap_used)`): calls `crate::crypto::watch_capability_project` / `verify_watch_capability`
- `PersistedDescriptor::resolve_segment` (`src/registry.rs:746`, `expect(clippy::expect_used)` at 742): calls `crate::crypto::RouteHash::for_stream`, `RoutingKeyHash::of`
- `resume_incarnation` (`src/application/topology.rs:421`, fn-wide `expect(clippy::expect_used)` at 413 plus `excessive_nesting` at 417): calls `crate::crypto::RouteHash::for_child`

No path moves or is re-aliased, so none of these fingerprints changes, and none of those files is edited.

**Other clippy rules.** No function changes length, argument count, nesting or bool parameters. No new unwrap/expect/panic is added. No expectation becomes unfulfilled: only `allow`s are removed, and each removed allow was on a live item. No `name: String` parameter or `.stream_ref(` call is added, so there are no mt_lint markers. The existing `// mt-lint: allow(name-param-shared-core)` marker at l.287 stays adjacent to `stream_hash`.

---

## 5 Mutation analysis

`src/crypto.rs` is both under a critical prefix (`src/crypto`) and registered (`scripts/quality/mutation_owners.py:79`: `owner('crypto', 'src/crypto.rs', 'crypto::')`).

- **Planner classification.** `production_changes.unchanged_production` will **not** classify the file as production-unchanged. Its lint-annotation normaliser would erase the allows, but `#[derive(...)]` is not in the builtin attribute set, and the derives on `RouteHash` (l.311), `SegmentHash` (l.350), `RoutingKeyHash` (l.396) and `FrameHeader` (l.431) move up 16-40 lines. They are therefore not "fixed" inputs, and `normalized_source` returns `None`. The trailing-test-prefix proof also fails, because production `secret_eq` sits between test modules. The plan therefore selects `mutants: true` (crypto owner), `properties_fuzz: true` (CODEC_PREFIXES) and `miri: true` (BUFFER_PREFIXES) for this commit. The last two just re-run existing corpus and Miri tests.
- **Changed fn bodies: none.**
- **In-diff selection.** In cargo-mutants 27.1.0, `in_diff.rs::affected_lines` marks every inserted line plus, for each deleted run, the new-file line before and the line after. `visit.rs::function_body_span` makes a FnValue mutant span run from the first statement to the last. Applied to the simulated diff, the affected new-file lines are:
  - `137` doc, `138` const, `139` const
  - `155` `)]`, `156` `#[expect(`
  - `196` doc, `197` `pub(crate) fn watch_capability_project(… ) {` signature
  - `205` `)]`, `206` `pub(crate) fn verify_watch_capability(` signature
  - `297` doc, `298` `for_stream` signature
  - `303` doc, `304` `for_child(` signature
  - `331` doc, `332` `SegmentHash::for_stream` signature
  - `341` doc, `342` `for_segment(` signature
  - `606` `dead_code,`, `607` new reason
  - `685` doc, `686` `secret_eq` signature

  Every one is a doc comment, attribute, const or signature line. Each signature's body span starts on the following line. Consts are not mutated. If git slides an identical `)]` line at hunks 3 and 5, the affected lines move by one but stay in the attribute region. **Zero mutants intersect.**
- **Expected driver output** (`scripts/quality/mutation_driver.py`): `crypto: no executable mutants in the selected scope`, then `No executable mutations in the registered owners of this selection; no mutation experiment is claimed.` This matches the precedent `647802f8`, which registered the crypto owner so that "the runner reports the empty selection this annotation-only diff produces".
- **Owner rows and filters:** unchanged. No new files.
- **Push batching.** If this commit is pushed together with the four unpushed commits, the push comparison (`QUALITY_BEFORE_SHA` = origin/slate) also selects those commits' own mutants. This commit adds none to that set.

---

## 6 Ledgers

| ledger | change | why |
|---|---|---|
| `docs/quality/source-allowances.json` | **none** | Has no `exception` rows for `src/crypto.rs` (its only crypto.rs row is the `by-path-module` `crypto/decrypt.rs` row, which stays). Reasoned exceptions are skipped by `violations` and are never recorded, so `stale = active − current` is unchanged. **Do not run `--prune`**: it would rewrite the file for no content change. |
| `docs/quality/legacy-source.json` | **none (immutable)** | Holds adoption-time rows such as `("exception","src/crypto.rs","crate::decrypt_frame","allow (dead_code)")` as a ceiling. It is sha-pinned in `docs/quality/policy.json` `immutable_sha256`, and lower counts are fine. |
| `docs/quality/diagnostic-allowances*.json` | none | Empty warning ledgers; no warning is added. |
| `docs/quality/owners.json` | none | Only by-path/test-module rows for crypto; no exception rows. |
| `docs/refactor/test-inventory.json` | none | No DST test body changes. |
| `docs/refactor/review-mechanisms.json` | none | The only crypto pins are on `src/crypto/invocation_tests.rs` bodies, which are unchanged. |
| `docs/refactor/architecture-policy.json`, `WIRE-MATRIX.md`, test-scenario-map / scenario-dispositions | none | Not referenced or not affected; no wire change, no renames. |
| `scripts/mt-audit-baseline.txt` | none | Text rows without line numbers; crypto.rs is excluded from the stream-hash category (`multitenancy-audit.sh:82`). |
| `docs/quality/verification.json`, `docs/refactor/architecture-baseline.json`, `scripts/clippy-baseline-fingerprints.txt` | none | Historical receipt, warning-only report and release fingerprints, respectively. None is a current-HEAD gate for this file's hash or lines, and no new warning is produced. |

---

## 7 Controls

Run these after the in-tree mutation and gate runs finish, because they need the CPU. Commands are from the repo root.

1. **Scratch red control (§3.1)** on the working tree before committing. Delete all 11 blocks, then run:
   `cargo clippy --locked -p streams-slate --lib -- -D warnings`
   Expect the two `never used` errors at `src/crypto.rs:605:15` and `617:15`, then `could not compile … due to 2 previous errors`. Nothing else is allowed. Then restore the decrypt_frame block with the new reason (§4 step 2).
2. `cargo fmt --all -- --check`: no output, exit 0.
3. `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl`: exit 0.
4. `python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl`: expect the last line `quality ratchets: OK; <n> Rust files; 0 emitted warning occurrences; <S−10> accepted exception scopes; base 7c4f860638e6`. There should be no `obsolete source allowances`, no `accepted exception grew`, no `exception needs owner; invariant; alternative`, and no `file growth` lines. (S is the count at HEAD; ten identities disappear, and decrypt_frame's is replaced one-for-one.)
5. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`: exit 0.
6. `cargo build --locked -p streams-quality-syntax && QUALITY_BASE_REF=HEAD~1 python3 scripts/quality/verification_plan.py --out target/quality-plan-71`: `plan.json` should have `changed_rust_files == ["src/crypto.rs"]`, `mutation_source_files == ["src/crypto.rs"]`, `production_unchanged_files == []`, `mutants/properties_fuzz/miri == true`.
7. `QUALITY_BASE_REF=HEAD~1 QUALITY_MUTANTS_OUT=target/quality-mutations-71 scripts/quality/mutations.sh`: expect `crypto: no executable mutants in the selected scope` and `No executable mutations in the registered owners of this selection; no mutation experiment is claimed.`
8. `cargo test --locked --lib crypto::`, `cargo test --locked --lib deployment_bearer:: peer::`, `cargo test --locked -p streams-quality-invariants`: all ok, with the same pass counts as at HEAD.
9. `python3 scripts/architecture-gate.py --check`, `python3 scripts/test-inventory.py --check`, `bash scripts/multitenancy-audit.sh`: unchanged OK.
10. `git show --stat HEAD`: `src/crypto.rs | 42 +-` and `1 file changed, 1 insertion(+), 41 deletions(-)`. `wc -l src/crypto.rs` gives `696`.
11. Before claiming green on slate, run `gh run view` on the pushed run (memory rule: never claim CI green without it).

---

## 8 Out of scope (noted, not done)

- **`decrypt_frame_limited` has one caller (`decrypt_frame`).** Its doc comment ("None means the authenticated payload crosses this page's remaining limit") describes a page caller that no longer exists, since service pages call `FrameDecryptor` directly. Folding it into `decrypt_frame` would edit fn bodies in a critical file (new mutants need killing tests), so it is a separate item. The `decrypt_frame` allowance keeps it rooted either way.
- **Stale doc on `WATCH_CAP_MAX_LIFETIME_SECS` (l.131-137).** It describes the retired `GET /touch/key/{watchKeyHex}?sig={this}` URL design; `watch_capability_sig`'s own doc calls `?sig=` retired. The "Replaces … at MT Stage 3" tails on the `SegmentHash` constructors (l.358-359, 373) are also stale. These are doc-only fixes and belong in a separate commit.
- **The five includers' module-wide allows** are the real mechanism and stay as they are. The reviewer's leaf-crate extraction (rejected; it would reverse the private-module decision at `src/lib.rs:4-9`) is not pursued.
- **Deleting `decrypt_frame`** and having `keys` / `cryptobench` call `FrameDecryptor::new(..).decrypt(.., MAX_RECORD_PLAINTEXT)` directly would touch about 20 test call sites. Not proposed.

## Decisions for Søren

1. **Approve the rewritten `decrypt_frame` allowance reason** (§4 step 2). Under RUST-QUALITY, a changed reason is the explicit reviewed decision. Nothing else needs approval: there is no wire, contract or ledger change.
2. *(Optional, follow-ups)* Whether to schedule the §8 items: folding `decrypt_frame_limited` into `decrypt_frame`, and correcting the stale `?sig=` / "MT Stage 3" doc comments.

---

## Skeptic corrections (C1..C8)

I checked the plan against `fb18840d`, reading the files only. Confirmed as written:

- all 11 quoted allow blocks and their line numbers (`src/crypto.rs:138-147, 164-167, 209-212, 222-225, 318-321, 328-331, 360-363, 374-377, 641-644, 722-725`)
- the five includers' module-wide `allow(dead_code)`: `src/bin/keys.rs:8-13`, `livebench.rs:34-39`, `cryptobench.rs:4-9`, `tools/quality-invariants/src/lib.rs:5-12`, `fuzz/fuzz_targets/postings.rs:5-12`. `tenant.rs` is also included with a module-wide allow in all five, so the tenant callees are covered too.
- every use site in §1.3 (I re-grepped `src tools fuzz examples`; `scripts/read-experiments/*.rs` is not a cargo target)
- `wc -l`: 736 for crypto.rs, and the nine ceilinged files match
- `docs/quality/source-allowances.json` has no crypto.rs exception rows, so there is nothing to prune
- `legacy-source.json` is sha-pinned in `docs/quality/policy.json`
- the planner verdict (`#[derive]` at 311/350/396/431 moves, so `normalized_source` returns None, so `mutants/properties_fuzz/miri = true`)
- cargo-mutants 27.1.0 behaviour: `in_diff.rs:213-258 affected_lines` and `visit.rs:757 function_body_span` do what the plan says. The `pr.diff` from `verification_plan.py:377` is a default 3-context diff. Even under `-U0` every marked line is still a doc, attribute, const or signature line. Result: zero mutants.
- exception ratchets only shrink (`source_rules.py:204-216` fails only on growth). The new `decrypt_frame` value is a new identity and passes the `"[^";]+;[^";]+;[^";]+"` check at `source_rules.py:258`.
- the §3.1 control really expects exactly two errors. `FrameDecryptor::decrypt` stays live through `decrypt_append` (`src/crypto/decrypt.rs:47-49`, which `src/application/read_keys.rs:77-79` calls). `MAX_RECORD_PLAINTEXT` stays live through `src/application/read_budget.rs:7,39,70`. No third dead item appears.

**C1 (header claim false; outcome unaffected).** Line 3 says none of the 4 unpushed commits touch a named ledger or caller. `git diff --name-only 7c4f8606 fb18840d` shows they touch:
- `docs/quality/source-allowances.json`: a `TEST_ASSERT_KEYED_DENSE` global row removed
- `docs/refactor/architecture-policy.json` and `docs/refactor/test-inventory.json`: both named in §6
- `src/sse/session.rs`: a §1.3 `RouteHash::for_stream` caller
- `src/shard/transaction_tests.rs`: a §1.3 `decrypt_frame` test user

The cited lines (`sse/session.rs:265,312`, `transaction_tests.rs:184`) are still correct at HEAD. The ratchet merge base is origin/slate. Fix the sentence to say the overlap exists and is disjoint from this edit.

**C2 (§1.3 list of lib dead_code exceptions is incomplete).** The list misses every `cfg_attr` form:
- `src/touch_keys.rs:31-37` and `:72-78`: `cfg_attr(not(test), allow(dead_code))`
- `src/shard_directory.rs:64-69` and `:91-96`: the same, on enum variants
- `src/shard.rs:424-429`, `:1783-1788`, `:1989-1994`, `:2229-2234`, `:2657-2662`: `cfg_attr(not(test), expect(dead_code))`
- `src/product.rs:94-99`: `canonical_stream_name`, `cfg_attr(not(test), expect(dead_code))`

None of these rooted items is on any caller chain of the ten items. The callers are in registry.rs, sse/session.rs, http/read.rs, application/*, deployment_bearer.rs, peer.rs and topology.rs. So the conclusion stands, but the list must be complete.

§1.5 should also say why decrypt_frame does not follow the `touch_keys.rs` precedent `cfg_attr(not(test), allow(dead_code, …))`. The reason: a direct `#[allow]` is the form that `production_changes.normalized_source` (it requires the exact `#[` wrapper) and the reasoned-exception regex checks (`source_rules.py:122-124, 256-259`) are written against. Keep the direct allow.

**C3 (the proposed decrypt_frame reason contains a false statement).** It calls decrypt_frame "the unbounded offline decoder". decrypt_frame is bounded: `src/crypto.rs:651-652` calls `decrypt_frame_limited(..., MAX_RECORD_PLAINTEXT)` and errors `"decoded record exceeds 32 MiB"`. What separates it from the service path is the record cap, not a page limit. Replacement text (exactly two `;`, no `"`, checked):

`reason = "decrypt_frame; the whole-record offline decoder capped at MAX_RECORD_PLAINTEXT serves the keys CLI, cryptobench and tests while every service read decrypts through FrameDecryptor under its page limit, so the service library has no caller; expect(dead_code) would be unfulfilled in the test build and in the by-path bins that call it"`

**C4 (§6 ledger table misnames a ledger).** `docs/refactor/review-mechanisms.json` contains no crypto entries at all; a grep for `crypto` finds nothing. The `src/crypto/invocation_tests.rs` sha pins the plan refers to are in `docs/refactor/review-unit-relocations.json:20-55` (`function_sha256` per relocated test). `scripts/review-evidence.py:195-201` checks them against the `to_file` only. This edit does not change them. Add the row "review-unit-relocations.json: none (pins invocation_tests.rs function bodies, untouched)" and correct the review-mechanisms row.

**C5 (control 8 cannot run as written).** `cargo test --locked --lib deployment_bearer:: peer::` passes two positional TESTNAMEs, and cargo accepts one (`cargo test [TESTNAME] [-- ARGS]`). Use `cargo test --locked --lib -- deployment_bearer:: peer::`, which is the same `--` form `mutation_driver.py:32-38` uses. `cargo test --locked --lib crypto::` is fine.

**C6 (control 9 skips gates that CI runs).** `scripts/quality.sh:36-42` also runs:
- `review-evidence.py --check`, which reads review-unit-relocations (C4)
- `scenario-map-report.py --check`
- `verify-rc-evidence.py --self-test`
- `python3 -m unittest discover -s scripts/quality`
- the `mt_lint::multitenancy_identity_lint` leg

Replace controls 2-5 and 9 with one `scripts/quality.sh` run, which includes all of them. Expect `QUALITY_OK`, then check the gate.py line from control 4.

**C7 (the red control does not prove the lint-contract change).** §2 claims that after the commit, removing an item's last production caller fails `clippy -D warnings`. The §3.1 control only proves the ten are live. It does not show that deadness is now detected where HEAD masked it. Add a scratch control with a predictable red/green split, never committed:
1. Replace `crate::crypto::secret_eq(v, t)` at `src/deployment_bearer.rs:30` and `crate::crypto::secret_eq(value, token)` at `src/peer.rs:113` with plain `==`.
2. Run `cargo clippy --locked -p streams-slate --lib -- -D warnings`.
3. On HEAD it passes: allow `crypto.rs:722-725` masks the deadness.
4. On the committed tree it fails, and the only error is ``function `secret_eq` is never used`` at `src/crypto.rs:686:15`.
5. Revert the scratch edit.

This works because `deployment_bearer::tests` and `peer::tests` call `authorizes` / `inbound_static_ok`, never `secret_eq` directly, and the non-test `--lib` build has no other user.

**C8 (the plan leaves out a side effect of the change).** Once the allows are gone, an item whose only callers are in another owner's body can become dead when a mutant replaces that body. Examples: the `WATCH_CAP_*` consts and `watch_capability_project` under a body mutant of `verify_watch_capability` or `WatchService::authenticate`. Today that is only a warning, because no `RUSTFLAGS -D warnings` is set: `.cargo/config.toml` is absent and `config.py:32` forbids RUSTFLAGS. So mutants stay viable and nothing changes. Record this in §5 so that a later move to `-D warnings` in the quality profile is not surprised by unviable mutants.

**Verdict: ready-with-corrections.** The edit is annotation-only in one file under the ceiling, with zero mutants and no ledger writes. Apply C3 (reason wording) and C5 (broken control) before execution. C1, C2, C4, C6, C7 and C8 fix documentation or strengthen the controls.
