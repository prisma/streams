# Item 59: delete the `TEST_ASSERT_KEYED_DENSE` bisect lever

Repo `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD `7c4f8606` (origin/slate = merge base = `9b2f53bc`).
Everything below was read on this tree. The review's line numbers are stale; current ones are given.

**Short version.** The reviewer's Change is right and builds, with one citation that would break the
build and three consequences the review does not mention:

1. **Wrong line in the review.** "the http.rs:2867 re-export" is now `read_merged`'s re-export, and
   `src/dst/runtime.rs:506` uses it. Delete **http.rs:2864-2865** only.
2. **An architecture exception must go.** The deletion shrinks `serve` from 718 to 679
   architecture-gate lines, below its fixed baseline of 715. `scripts/architecture-gate.py --check`
   then fails with `obsolete budget exception: function:src/sse/session.rs::serve`, so that
   exception has to be removed in the same commit.
3. **Mutation scope.** cargo-mutants `--in-diff` counts the lines on **both sides** of every
   deletion as changed. That puts the operators of `serve`'s two `at_head` predicates in scope:
   8 mutants that already exist, plus the `serve` function-body mutant. By analysis, 4 of them
   (M2-M5) survive the `sse_session` owner's current test filters, so CI would fail on a pure
   deletion. The plan adds one RAW-pairing test for multi-record windows in its own commit (C1).
   It pins behaviour the code already has and kills all 9.
4. **Stale path in the re-attached doc.** The doc comment names `src/dst.rs`, which no longer
   exists. The caller is `src/dst/runtime.rs`. Fixing it is optional.

---

## 1 Problem (verified, with quotes)

**No writer exists.** `git grep -n TEST_ASSERT_KEYED_DENSE` finds the definition, one re-export,
three `.load(` sites, one comment and some JSON ledger rows. Nothing calls `.store(`, `.swap(` or
`fetch_*` on it anywhere. The static is always `false`.

- `src/application/read.rs:116-126`. The doc comment belongs to `execute_segment`, but it is
  attached to the static:
  ```
  116: /// The merge itself, free of `AppState` so the simulation harness can call
  117: /// the production reader instead of reimplementing the history/tail split
  118: /// (`src/dst.rs`). A second copy of this boundary logic would be a copy
  119: /// that can drift, and drift here means the oracle stops testing what
  120: /// production does.
  121: /// Round-13 CODE-RED bisect: the repro's stream carries ONLY rk=""
  122: /// records, so keyed reads must be dense too — armed by the test.
  123: #[cfg(test)]
  124: pub(crate) static TEST_ASSERT_KEYED_DENSE: std::sync::atomic::AtomicBool =
  125:     std::sync::atomic::AtomicBool::new(false);
  126:
  127: #[expect(
  ```
  So `execute_segment` (line 142) has no doc, and rustdoc gives the test-only static the merge
  reader's documentation. The attachment was already wrong at the adoption anchor `5bdaf968`:
  `#[allow(clippy::too_many_arguments)]` sat between that doc and the static, so it applied to the
  static. The immutable rows `legacy-source.json:2107` and `legacy-diagnostics.json:398` record
  that `allow` under `crate::TEST_ASSERT_KEYED_DENSE`.
- `src/application/read.rs:135-141`. This expectation exists only because of the audit block:
  ```
  #[cfg_attr(
      test,
      expect(
          clippy::excessive_nesting,
          reason = "execute_segment; the keyed-dense audit nests the offset walk inside the test-armed check of the history leg; flattening it would separate the audit from the leg it inspects"
      )
  )]
  ```
- `src/application/read.rs:241-256`. Audit 1 never runs:
  `#[cfg(test)] if TEST_ASSERT_KEYED_DENSE.load(std::sync::atomic::Ordering::Relaxed) { let mut expect = scan_from; for r in &out.recs { assert!(r.off <= expect, "HISTORY leg gap: ...") ... } assert!(hist_upto <= expect, "HISTORY leg over-claim: ...") }`
- `src/http.rs:2864-2865` (the review says 2867):
  ```
  2864: #[cfg(test)]
  2865: pub(crate) use crate::application::read::TEST_ASSERT_KEYED_DENSE;
  2866: #[cfg(test)]
  2867: pub(crate) use crate::application::read::read_merged;   <- KEEP (src/dst/runtime.rs:506)
  ```
- `src/sse/session.rs`, inside `serve` (the review says 397-412 and 530-543). This state exists
  only for the lever:
  - `320: #[cfg(test)]` / `321: let task_rk_dbg = rk_filter.clone();`
  - `343-347`: the comment `// Round-13 CODE-RED bisect (test builds): every emitted record ...`
    (3 lines), `#[cfg(test)]`, `let mut bisect_last_off: Option<u64> = None;`
  - `402-417`, audit 2 (catch-up):
    `#[cfg(test)] if crate::http::TEST_ASSERT_KEYED_DENSE.load(..Relaxed) { if let Some(l) = bisect_last_off { assert!(r.off <= l + 1, "SESSION SKIP in CATCH-UP: ...", r.off, task_rk_dbg, batch.scan_from, batch.scan_to); } bisect_last_off = Some(r.off.max(bisect_last_off.unwrap_or(0))); }`
  - `536-551`, audit 3 (live): the same shape, with `"SESSION SKIP in LIVE: ..."` and
    `feed.floor_for_test()`.
- `src/sse/feed.rs:810-818`. `floor_for_test` duplicates `floor()`, and audit 3 is its only caller:
  ```
  810:     #[cfg(test)]
  811:     pub(crate) fn floor(&self) -> u64 {
  812:         self.st.lock().unwrap().floor
  813:     }
  815:     #[cfg(test)]
  816:     pub(crate) fn floor_for_test(&self) -> u64 {
  817:         self.st.lock().unwrap().floor
  818:     }
  ```
- `src/dst/tests/persistence_faults.rs:368-372`. The note records that arming the lever breaks
  other tests:
  `// NOTE: TEST_ASSERT_KEYED_DENSE stays DISARMED in-suite — it is a / process-global bisect lever and the parallel suite runs / legitimate sparse keyed lanes concurrently (arming it here / failed five unrelated tests). ...`
  The test's own doc (lines 341-349) already says the leg reconciles "EXACTLY against the acked
  set".
- Ledger rows that exist only for the lever:
  - `docs/quality/source-allowances.json:1753-1759`: the `global` row for
    `crate::TEST_ASSERT_KEYED_DENSE` (`std :: sync :: atomic :: AtomicBool`).
  - `docs/refactor/architecture-policy.json:92`: `"TEST_ASSERT_KEYED_DENSE"` in
    `adapter_http_exports["src/sse/session.rs"]`.
  - `legacy-source.json:2107,2807` and `legacy-diagnostics{,-linux}.json:398` are immutable
    adoption inventories, sha256-pinned in `docs/quality/policy.json`. They stay as they are.
- History: the lever was introduced in `df4deda7` (2026-08-31, round-13.4 CODE-RED). That commit
  message already says "the TEST_ASSERT_KEYED_DENSE bisect lever stays DISARMED in-suite, it is
  process-global". The protection that actually works stays in place: the always-on unfiltered
  `read_merged` audit (read.rs:301-322) and the DST hunt leg's exact reconciliation.

**Also verified.** The reviewer's first step says the compiler must list exactly read.rs:242,
session.rs:398 and session.rs:530. On the current tree the sites are read.rs:242, session.rs:403
and session.rs:537, and the errors appear **only in test-cfg builds**, because every use site is
`#[cfg(test)]`. A non-test `cargo check --lib` stays green.

## 2 Contract decision

- **Typed contract / wire codes: none.** Every deleted line is `#[cfg(test)]`. In test builds the
  blocks only ever loaded a `false`. Production binaries are byte-for-byte unaffected. Nothing
  changes at the product or raw edge, and `docs/refactor/WIRE-MATRIX.md` needs no edit.
- The new test in C1 pins wire behaviour that already exists and is already documented (round
  11.8 RAW pairing, `docs/LIVE-FEED.md` / `WIRE-MATRIX.md` from `4247421f`). It does not change it.
- **Backward-compatible alternative:** keep a lever, but scope it per read plan or per feed
  instead of process-wide. Not recommended: it keeps a test-only branch inside the production
  merge path and in `serve`, and nothing arms it.
- Synchronization: the only atomic removed had no writer and ordered nothing. No Loom or
  held-commit obligation (item 48) applies.

## 3 Red tests and controls

This item changes no behaviour, so there is no behavioural red test. Three falsifiable controls
take its place.

**R0: compile red (the reviewer's first step; do not commit).** Delete only read.rs:121-126 (the
lever's 2 doc lines, its `#[cfg(test)]`, the 2-line static and the following blank line) and
http.rs:2864-2865. Then run
`cargo check --locked -p streams-slate --all-targets 2>&1 | grep -E '^error|-->'`. Expected output
(the read.rs site has moved up 6 lines, from 242 to 236):
```
error[E0425]: cannot find value `TEST_ASSERT_KEYED_DENSE` in this scope
   --> src/application/read.rs:236:16
error[E0425]: cannot find value `TEST_ASSERT_KEYED_DENSE` in module `crate::http`
   --> src/sse/session.rs:403:45
error[E0425]: cannot find value `TEST_ASSERT_KEYED_DENSE` in module `crate::http`
   --> src/sse/session.rs:537:45
error: could not compile `streams-slate` (lib test) due to 3 previous errors
```
Also expected: `cargo check --locked -p streams-slate --lib` reports `Finished` (the non-test
build is unaffected).

**R1: mutation red, which is why C1 exists.** Apply C2 without C1 and run the owner's leg,
restricted to the two predicate lines:
```
git diff HEAD > target/item59-c2.diff      # C2 staged/committed on a scratch branch without C1
cargo mutants --in-diff target/item59-c2.diff --file src/sse/session.rs --package streams-slate \
  --profile quality --jobs 1 --timeout 90 --baseline run \
  --cargo-test-arg=sse:: --cargo-test-arg=-- --cargo-test-arg=dst_tests::sse_delivery:: \
  --cargo-test-arg=dst_tests::livefeed_swap:: --cargo-test-arg=livefeed_engine_retired \
  --re 'session\.rs:(394|512):'
```
Predicted output (mutant names; the timing suffix is omitted):
```
MISSED   src/sse/session.rs:394:36: delete ! in serve
MISSED   src/sse/session.rs:512:53: replace && with || in serve
MISSED   src/sse/session.rs:512:68: replace + with - in serve
MISSED   src/sse/session.rs:512:68: replace + with * in serve
```
M1 and M6-M8 are expected CAUGHT; section 5 gives the reasons. This is a prediction from reading
the tests. Record what the run actually prints in the C1 commit message. Any single MISSED line
fails CI's leg, because `mutation_driver.py` runs with `check=True` and keeps no disposition ledger.

**C1 test: green on the current tree and after C2, by design.**
`src/dst/tests/sse_delivery.rs::raw_up_to_date_rides_only_the_last_record_of_a_multi_record_window`.
Expected on both trees: `test dst::dst_tests::sse_delivery::raw_up_to_date_rides_only_the_last_record_of_a_multi_record_window ... ok`.
The failure message it produces under each mutant is listed in section 5.

**Negative controls for the ledgers** (the gates must fail as shown when a step is skipped):
- `cfg_attr(test, expect(clippy::excessive_nesting ...))` left in place: clippy `--all-targets`
  reports `error: this lint expectation is unfulfilled` at `src/application/read.rs` on the
  `clippy::excessive_nesting` line. `unfulfilled_lint_expectations` is denied.
- Serve budget exception left in place: `architecture-gate.py --check` reports
  `obsolete budget exception: function:src/sse/session.rs::serve: the default limit 715 already covers it`.
- Source allowance not pruned: `gate.py` and `architecture-gate.py --check` both report
  `1 obsolete source allowances; run the quality ratchet with --prune`.
- Inventory not regenerated: `test-inventory.py --check` reports
  `new test requires inventory: raw_up_to_date_rides_only_the_last_record_of_a_multi_record_window`.

## 4 Edits, file by file, in commit order

No verbatim-move commit is needed: no file grows past a ceiling, and nothing moves.

### Line budgets (current `wc -l` compared with the merge-base ceiling)

| File | Now | Ceiling | After | Note |
|---|---|---|---|---|
| src/http.rs | 3,371 | 3,371 | **3,369** | −2 (C2) |
| src/sse/feed.rs | 1,200 | 1,200 | **1,195** | −5 (C2) |
| src/product.rs | 4,205 | 4,205 | 4,205 | untouched |
| src/shard.rs | 3,197 | 3,197 | 3,197 | untouched (task text said 3,232; tree says 3,197) |
| src/billing.rs | 2,201 | 2,201 | 2,201 | untouched |
| src/history.rs | 1,713 | 1,713 | 1,713 | untouched |
| src/auth.rs | 1,676 | 1,676 | 1,676 | untouched |
| src/registry.rs | 1,501 | 1,501 | 1,501 | untouched (task text said 1,509) |
| src/fleet.rs | 1,143 | 1,143 | 1,143 | untouched |
| src/application/read.rs | 938 | ≤1,000 | 909 | −29 |
| src/sse/session.rs | 950 | ≤1,000 (arch baseline 1,060) | 911 | −39 |
| src/dst/tests/persistence_faults.rs | 524 | ≤1,000 | 519 | −5 (DST file) |
| src/dst/tests/sse_delivery.rs | 599 | ≤1,000 | ~690 | +~90 (C1, DST file) |

### C1: "RAW pairing is pinned on multi-record windows: only the last record's control carries upToDate"

1. `src/dst/tests/sse_delivery.rs`, lines 341-353. Make `raw_sse_connect` delegate, and add an
   offset-taking sibling. It is a helper, so it is not inventoried and no pinned hash changes.
   ```rust
   /// A RAW live subscription on `name` from the current frontier
   /// (`offset=now`): the raw surface always rides the default lane.
   async fn raw_sse_connect(addr: std::net::SocketAddr, name: &str) -> tokio::net::TcpStream {
       raw_sse_connect_at(addr, name, "now").await
   }

   /// A RAW live subscription from a raw `offset` token: a session that
   /// starts below the frontier catches up privately before it goes live.
   async fn raw_sse_connect_at(
       addr: std::net::SocketAddr,
       name: &str,
       offset: &str,
   ) -> tokio::net::TcpStream {
       use tokio::io::AsyncWriteExt;
       let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
       let req = format!(
           "GET /v1/stream/{name}?live=sse&offset={offset} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
       );
       sck.write_all(req.as_bytes()).await.unwrap();
       sck
   }
   ```
2. `src/dst/tests/sse_delivery.rs`. Insert after the closing `}` of
   `shared_raw_subscriber_gets_up_to_date_after_a_match_free_batch` (line 510) and before
   `collect_session`'s doc:
   ```rust
   // ------------------------------------------------------------------
   // RAW pairing on multi-record windows: the catch-up read and a shared
   // batch fold upToDate into the LAST record's paired control only.
   // ------------------------------------------------------------------

   /// One RAW window: everything through the upToDate control naming
   /// `next`, then one quiet beat, so a standalone duplicate after the
   /// paired control is in the text the assertions count. Both collects
   /// are bounded: a hung collect would read as a mutation timeout.
   async fn raw_window(raw: &mut tokio::net::TcpStream, next: u64) -> String {
       let (head, _) = hub_sse_collect(raw, 8, |t| status_at(t, next)).await;
       let (quiet, _) = hub_sse_collect(raw, 2, |_| false).await;
       format!("{head}{quiet}")
   }

   /// The pinned RAW protocol for a window of `records` records ending at
   /// `next`: ONE paired control per data event, no standalone status, and
   /// upToDate on the last record's control only.
   fn assert_raw_pairing(window: &str, records: usize, next: u64, leg: &str) {
       assert_eq!(
           window.matches("event: data").count(),
           records,
           "{leg}: every record of the window:\n{window}"
       );
       assert_eq!(
           window.matches("event: control").count(),
           records,
           "{leg}: ONE paired control per data event, no standalone status:\n{window}"
       );
       let flagged = up_to_date_controls(window);
       assert_eq!(flagged.len(), 1, "{leg}: upToDate rides exactly one control:\n{window}");
       let last = raw_next_tok(next);
       assert!(
           flagged.iter().all(|d| d.contains(&last)),
           "{leg}: upToDate rides the LAST record's control (next={next}):\n{window}"
       );
   }

   /// RAW pairing on the session's two multi-record paths, the private
   /// catch-up read and a shared live batch: upToDate folds into the paired
   /// control of the last record at the frontier, never an earlier one, and
   /// no standalone status follows. The rank-13 test above sends one record
   /// per window, which cannot tell the last record from the first.
   #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
   async fn raw_up_to_date_rides_only_the_last_record_of_a_multi_record_window() {
       let store = mem();
       let (state, addr) = http_rig(store).await;
       let ct = ("content-type", "application/json");
       let body = br#"[{"a":0},{"a":1}]"#;
       let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawpair", &[ct], body).await;
       assert!(st == 200 || st == 201, "create {st}");

       // CATCH-UP: both existing records are read privately from START.
       let start = crate::offsets::encode_ep(0, crate::offsets::Offset::START);
       let mut raw = raw_sse_connect_at(addr, "rawpair", &start).await;
       assert_raw_pairing(&raw_window(&mut raw, 2).await, 2, 2, "raw catch-up");

       // SHARED: a product session joins the default-lane feed, so the
       // next append reaches the raw session as one retained batch.
       let mut prod = lf_connect(addr, "rawpair", "?cursor=now").await;
       let (p0, _) = hub_sse_collect(&mut prod, 8, |t| t.contains("\"upToDate\":true")).await;
       assert!(p0.contains("\"upToDate\":true"), "product parks at the head:\n{p0}");
       assert_eq!(
           default_lane_feed(&state, "rawpair").await.subscriber_count(),
           2,
           "raw and product sessions share ONE feed"
       );

       // LIVE: one append of two records is one shared batch of two.
       let body = br#"[{"a":2},{"a":3}]"#;
       let (st, _, _) = hreq(addr, "POST", "/v1/stream/rawpair", &[ct], body).await;
       assert!(st == 200 || st == 204, "raw append {st}");
       assert_raw_pairing(&raw_window(&mut raw, 4).await, 2, 4, "raw shared batch");
       drop(raw);
       drop(prod);
       engine_shutdown(&state).await;
   }
   ```
   Why the two windows take the intended paths:
   - The feed's head starts at `src.frontier()` (feed.rs:633), so `join_head` is 2 and the raw
     session reads [0,2) through the catch-up arm (session.rs:394 after C2).
   - Two subscribers make `read_and_publish` retain a SHARED batch (feed.rs:971,
     `solo = subscribers <= 1`). One POST is one commit, so records [2,4) arrive as one
     `Take::Batch` with `start_index` 0 and `last_i` 2 (session.rs:512 after C2).
   - Raw folds `at_head` into its control (`record_ctl`, session.rs:80-86). Product ignores it.
   - The helpers already exist: `hreq`/`lf_connect`/`hub_sse_collect` are bounded fixtures;
     `default_lane_feed`, `status_at`, `up_to_date_controls` and `raw_next_tok` are in this file.
     The `worker_threads` config follows the rank-13 test.
3. `docs/refactor/test-inventory.json`: `python3 scripts/test-inventory.py --write` takes it from
   502 to 503 tests. The only new entry is the test above, with `"scenarios": []`.

### C2: "The round-13 keyed-dense bisect lever is gone: no test ever armed it"

1. `src/application/read.rs`
   - Delete 121-126 (bisect doc ×2, `#[cfg(test)]`, the static ×2, the blank line). Doc lines
     116-120 now sit directly on `execute_segment`'s first `#[expect(`.
   - Optional doc fix, no line change: line 118, change `` (`src/dst.rs`) `` to
     `` (`src/dst/runtime.rs`) ``. The DST caller is `src/dst/runtime.rs:506`
     (`crate::http::read_merged`); `src/dst.rs` does not exist.
   - Delete 135-141, the `#[cfg_attr(test, expect(clippy::excessive_nesting, ...))]`. It would
     become unfulfilled: the non-test build already stays within nesting depth 4, and the only
     other `cfg(test)` code left in the function (lines 301-322) nests 2 deep.
   - Delete 241-256, audit 1 (`#[cfg(test)] if TEST_ASSERT_KEYED_DENSE.load(...) { ... }`).
     `cursor = hist_upto;` now follows the `if hist_upto > 0 { ... }` block directly.
2. `src/http.rs`: delete 2864-2865 only. **Keep 2866-2867** (`read_merged`).
3. `src/sse/session.rs` (inside `serve`): delete 320-321 (`task_rk_dbg`), 343-347 (bisect comment
   and `bisect_last_off`), 402-417 (audit 2) and 536-551 (audit 3). After the deletion the test-cfg
   body of `serve` is token-identical to the non-test body that clippy already accepts. `rk_filter`
   keeps its uses at lines 201, 209 and 210. The file has no imports to drop, because it names
   `Relaxed` by full path.
4. `src/sse/feed.rs`: delete 815-819 (`#[cfg(test)] fn floor_for_test` and the blank line after
   it). The `cfg(test)` `floor()` at 810-813 stays; `feed/tests.rs` uses it 8 times.
5. `src/dst/tests/persistence_faults.rs`: delete 368-372 (the NOTE). The test's
   `function_sha256` does not change, because `test-inventory.py` `canonical_tokens` strips `//`
   comments. So neither the `review-mechanisms.json:1036` pin (`after_sha256 6708f111...`) nor
   the inventory changes. None of the 5 lines match the inventory's `configuration` regex.
6. Ledgers go in the same commit (section 6): source-allowances prune, and two
   architecture-policy edits.

### Every ratcheted `#[expect]` scope the diff touches, and what happens to it

A ratchet fails only on growth: `source_rules.exception_growth` compares
`count > before`. Every scope below shrinks, so no reason text needs a new decision.

| Scope | Exceptions | Effect of this change |
|---|---|---|
| `execute_segment` (read.rs) | `too_many_lines`; **`unwrap_used` (function-wide, every call/path fingerprinted)**; `cfg_attr(test, excessive_nesting)` | The `cfg_attr` is **deleted** (it would be unfulfilled). The item span goes from 127-324 (198 lines) to 116-295 (180 lines): +5 doc lines, −7 for the `cfg_attr`, −16 for the audit. `syntax_facts` drops: +5 doc attributes against −2 attributes and the audit's paths, method calls and macros. The path fingerprints for `TEST_ASSERT_KEYED_DENSE` and `std::sync::atomic::Ordering::Relaxed` disappear. `unwrap_sites` is unchanged, because the audit had no unwrap. `too_many_lines` stays fulfilled at about 128 counted lines. |
| `impl LiveFeed` (feed.rs:604-607) | **`unwrap_used` (impl-wide, fingerprinted)** | One `unwrap_site` (the `lock().unwrap()` in `floor_for_test`) and its fingerprints are removed; scope_lines −5. It stays fulfilled through `head()` and other methods. |
| `serve` (session.rs:161-180) | `too_many_arguments`, `too_many_lines`, `excessive_nesting`, `let_underscore_must_use`, `disallowed_methods` (no unwrap/expect) | scope_lines −39 and syntax_facts shrink. All five stay fulfilled: the non-test build already has exactly this shape. |
| `cut_resume_never_skips_a_durable_record` (persistence_faults.rs:350-361) | `disallowed_methods`, `too_many_lines`, `excessive_nesting` | scope_lines −5 (comments only); facts unchanged. Clippy's `too_many_lines` ignores comments, so it stays fulfilled. |
| C1 helpers and test (sse_delivery.rs) | none | The file has no inner `#![...]` and its `mod` declaration has no expect, so no scope grows. |

## 5 Mutation-kill analysis

**Selection.** The push's `git diff --binary <before>` is used as-is (3 context lines).
cargo-mutants 27.1.0's `affected_lines` (checked in
`~/.cargo/registry/src/.../cargo-mutants-27.1.0/src/in_diff.rs:213-257`) marks the new-file line
**before and after** every deleted run. A mutant is selected when its span intersects one of those
lines.

- `src/sse/session.rs` has critical prefix `src/sse` and owner `sse_session`, with filters
  `sse:: dst_tests::sse_delivery:: dst_tests::livefeed_swap:: livefeed_engine_retired`. It is
  **production-changed**: `production_changes.normalized_source` erases only
  `cfg(test)` *items*, and the deleted `cfg(test)` code here is statements. Affected new lines are
  319/320, 340/341, **394**/395 and **512**/513.
- `src/http.rs` is production-changed, because a `use` is not an erasable item kind. Its affected
  lines 2863/2864 hold no mutants, so the log shows `http: no executable mutants in the selected scope`.
- `src/sse/feed.rs` is expected in `production_unchanged_files`: the removed item is a `cfg(test)`
  function. If it were selected, lines 814/815 still hold no mutant.
- `src/application/read.rs` is not selected: `'src/application/read.rs'` does not start with the
  critical prefix `src/application/read_`, and the file has no owner row.
- `persistence_faults.rs` and `sse_delivery.rs` are not critical and not registered.

**The 9 in-diff mutants.** Line and column refer to the file after C2; columns are 1-based
operator positions (old line 401 → new 394, old 535 → new 512). CI's "without C1" prediction comes
from the current filter set. The C1 kill column gives the failure the new test prints.

| # | Mutant | Without C1 | C1 kill (failure message) |
|---|---|---|---|
| M0 | `session.rs:194:5: replace serve -> axum::response::Response with Default::default()` | CAUGHT (every SSE test) | `raw catch-up: every record of the window` left 0, right 2 |
| M1 | `394:33: replace && with || in serve`, giving `(A&&B&&C&&D) \|\| !csrc.closed()` | likely CAUGHT (product catch-ups lose their standalone upToDate) | catch-up record 0 also flagged: `raw catch-up: upToDate rides exactly one control` left 2, right 1 |
| M2 | `394:36: delete ! in serve`, giving `... && csrc.closed()` | **MISSED** (no raw catch-up-of-records control count in scope; `livefeed_basics` has one but sits outside the filters) | no fold, so a standalone status follows: `raw catch-up: ONE paired control per data event, no standalone status` left 3, right 2 |
| M3 | `512:53: replace && with || in serve` | **MISSED** (in-scope tests only use single-record windows at head) | both records flagged: `raw shared batch: upToDate rides exactly one control` left 2, right 1 |
| M4 | `512:68: replace + with - in serve` (`start_index - i`) | **MISSED** (single record: 0-0+1 == 1) | i=1 overflows (`attempt to subtract with overflow`), the session task dies after record 2: `raw shared batch: every record of the window` left 1, right 2 |
| M5 | `512:68: replace + with * in serve` (`start_index * i`) | **MISSED** (0*0+1 == 1) | never at head, so a standalone status follows: `... no standalone status` left 3, right 2 |
| M6 | `512:72: replace + with - in serve` (`... - 1`) | CAUGHT (rank-13 test: underflow at i=0, record missing from `a3`) | `raw shared batch: every record of the window` left 0, right 2 |
| M7 | `512:72: replace + with * in serve` (`(..)*1`) | CAUGHT (rank-13 `a3`: 2 controls) | `... no standalone status` left 3, right 2 |
| M8 | `512:76: replace == with != in serve` | CAUGHT (rank-13 `a3`: 2 controls) | flag lands on next=3: `raw shared batch: upToDate rides the LAST record's control (next=4)` |

- **Equivalent mutants:** none. Each variant has a distinguishing execution above.
- **Timeouts:** every wait in C1 is bounded: 8 s + 2 s per window, `hreq` 60 s. Under M4/M6 the
  panicking session drops `tx`, and the body ends with the chunked terminator. The worst case is
  about 30 s, which is under `--timeout 90`.
- **Owner rows and filters:** unchanged. The test sits in `dst_tests::sse_delivery::`, which is
  already one of `sse_session`'s filters. There are no new files, so no new owner rows.
- **Why widening the filters is not enough:** adding `dst_tests::livefeed_basics::` would kill
  M1 and M2 (its exact-framing legs catch up one record), but M3-M5 need a *multi-record* shared
  batch, and no existing test sends one.

## 6 Ledgers (all in C2 except the inventory, which goes in C1)

| Ledger | Change | How |
|---|---|---|
| `docs/quality/source-allowances.json` | Remove the `global` row at 1753-1759 (`crate::TEST_ASSERT_KEYED_DENSE`, `src/application/read.rs`, `std :: sync :: atomic :: AtomicBool`) | `gate.py --prune` (section 7). The diff must show exactly those 7 lines. |
| `docs/refactor/architecture-policy.json` | Delete `budget_exceptions["function:src/sse/session.rs::serve"]` (lines 54-60, limit 721) | By hand. This is **forced**: `serve` drops to 679, below the baseline default of 715. |
| `docs/refactor/architecture-policy.json` | Delete `"TEST_ASSERT_KEYED_DENSE"` (line 92) and the trailing comma on line 91 | By hand. The gate checks only extra symbols, so this is a tightening. |
| `docs/refactor/test-inventory.json` | +1 entry (C1) | `scripts/test-inventory.py --write` |
| `docs/quality/owners.json` | none | The static never had an owners row; `serve`'s `tokio::spawn` effect row is unchanged; there are no new macros, spawns or globs. |
| `docs/quality/diagnostic-allowances{,-linux}.json` | none | Both are empty; `--prune` rewrites the macOS one with identical content. |
| `docs/quality/legacy-*.json`, `syntax-fragments.json` | **must not change** | Pinned by sha256 in `docs/quality/policy.json`. |
| `docs/refactor/review-mechanisms.json` | none | The `cut_resume...` hash ignores comments. |
| `docs/refactor/WIRE-MATRIX.md`, `src/dst/tests/README.md`, `mutation_owners.py` | none | No wire change, no new DST module, no new file. |

## 7 Controls (exact commands and expected outputs)

Before editing:
```
git grep -n -e TEST_ASSERT_KEYED_DENSE -e floor_for_test -e task_rk_dbg -e bisect_last_off
```
Expected: 21 lines. That is 6 in JSON ledgers (legacy-diagnostics ×2, legacy-source ×2,
source-allowances, architecture-policy) and 15 in `src`: read.rs:124 and 242,
persistence_faults.rs:368, http.rs:2865, feed.rs:816, and session.rs:321, 347, 403, 406, 411,
416, 537, 540, 546 and 550.

After C1 and C2:

1. `git grep -n -e TEST_ASSERT_KEYED_DENSE -e floor_for_test -e task_rk_dbg -e bisect_last_off -e keyed-dense`.
   Expected: only the 4 immutable rows, `legacy-diagnostics-linux.json:398`,
   `legacy-diagnostics.json:398`, `legacy-source.json:2107` and `legacy-source.json:2807`.
2. `cargo fmt --all -- --check` produces no output.
3. `cargo clippy --locked --workspace --all-targets -- -D warnings` finishes with no warnings. It
   is the negative control for the deleted `cfg_attr`.
4. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`
   finishes. `execute_segment` now carries the merge-reader doc.
5. Ratchet and prune:
   ```
   mkdir -p target/quality
   cargo build --locked -p streams-quality-syntax
   cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl
   python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl
   ```
   Expected before the prune: `1 obsolete source allowances; run the quality ratchet with --prune`
   and `quality ratchets: FAIL; ...`.
   Then run `python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl --prune` and
   `git diff --stat docs/quality`. Expected: `source-allowances.json | 7 -------` and nothing else.
   Rerun without `--prune`; expected: `quality ratchets: OK; ...`.
6. `python3 scripts/architecture-gate.py --check`. Before the policy edit it reports
   `obsolete budget exception: function:src/sse/session.rs::serve: the default limit 715 already covers it`.
   After the edit: `architecture-gate: OK (... Rust files; fixed baseline a7e2070f3b43)`.
7. `python3 scripts/test-inventory.py --check` reports `test-inventory: OK (503 tests, ...)`.
   `python3 scripts/review-evidence.py --check` reports OK.
   `python3 scripts/scenario-map-report.py --check` reports OK.
8. Tests:
   ```
   cargo test --locked -p streams-slate --lib -- --exact dst::dst_tests::sse_delivery::raw_up_to_date_rides_only_the_last_record_of_a_multi_record_window
   cargo test --locked -p streams-slate --lib -- dst_tests::sse_delivery:: dst_tests::livefeed_swap:: livefeed_engine_retired dst_tests::livefeed_basics:: dst_tests::persistence_faults:: sse::
   ```
   Expected: all `ok`. The first run prints `1 passed`. Check that the module path matches
   `cargo test -- --list`.
9. `scripts/quality.sh` ends with `QUALITY_OK`.
10. Commit both, then run CI's own selection before pushing:
    ```
    export QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) \
           QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate
    python3 scripts/quality/verification_plan.py --out target/quality-plan
    scripts/quality/mutations.sh
    ```
    Expected:
    - `plan.json` has `mutation_source_files` ⊇ {`src/sse/session.rs`, `src/http.rs`} and
      `src/sse/feed.rs` in `production_unchanged_files`.
    - The run prints `http: no executable mutants in the selected scope`.
    - The `sse_session` receipt shows the 9 mutants of section 5, all caught (plus any mutants
      from the not-yet-pushed `517f4fa1`/`7c4f8606`, if they go in the same push; those are
      their own obligations).
    - The last line is `Mutation verification executed N selected mutant(s) across M registered owner(s).`
      with exit 0.
    - To list item 59's scope on its own:
      `git diff 7c4f8606 HEAD > target/item59.diff && cargo mutants --list --in-diff target/item59.diff --file src/sse/session.rs --package streams-slate`
      lists exactly the 9 names in the table.
11. After the push, `gh run list --branch slate` and `gh run view <id>`: every job green. Never
    claim green from memory.

Suggested commit messages (each ends with
`Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`):
- C1: "RAW pairing is pinned on multi-record windows: only the last record's control carries upToDate". The body records the R1 MISSED list observed without the test.
- C2: "The round-13 keyed-dense bisect lever is gone: no test ever armed it". The body states the
  ratchet deltas: read.rs 938→909, session.rs 950→911, http.rs 3,371→3,369, feed.rs 1,200→1,195;
  serve 718→679 architecture lines so its R10 exception retires; 1 source allowance pruned.

## 8 Out of scope

- The always-armed `cfg(test)` unfiltered audit in `execute_segment` (read.rs:301-322). Its
  comment says "Round-13 CODE-RED bisect (test builds)", but it runs in every test build and is
  real coverage, so it stays.
- Restructuring `serve`'s `at_head` predicates into a pure function that unit tests could kill.
  It would be cleaner, but it is a larger change to a ratcheted 679-line function.
- Item 58 (`fail_next_list` in registry.rs) and item 60 (`retained_charge`, `source_reads` and
  `_key` in feed.rs). Item 60 touches feed.rs in different hunks, and the two items can land in
  either order.
- The immutable adoption inventories: `legacy-source.json` and `legacy-diagnostics{,-linux}.json`.
- The mutation obligations of the unpushed commits `517f4fa1` and `7c4f8606`, if they share the push.

---

## Skeptic corrections (C1..C6)

Everything below was checked on the tree at `7c4f8606`: the source, `scripts/quality/*.py`, `scripts/architecture-gate.py`,
`tools/quality-syntax/src/scan.rs`, and cargo-mutants 27.1.0 (`src/in_diff.rs`, `src/visit.rs`). These claims hold up:

- the grep inventory (21 lines: 6 JSON, 15 src)
- the http.rs:2864-2865 citation, and keeping 2866-2867 for `src/dst/runtime.rs:506`
- `serve` at 718 architecture lines (fn 181 to `}` 898), falling to 679, with baseline 715. The `obsolete budget exception` text matches architecture-gate.py:143-145.
- the 39/29/5/2 line deltas
- the cfg_attr `excessive_nesting` expect becoming unfulfilled (fn body 1, for 2, if 3, `if TEST_…` 4, `for r` 5, over threshold 4)
- the `affected_lines` both-sides rule (in_diff.rs:213-257)
- the new-file mutant positions: 394:33 `&&`, 394:36 `!`, 512:53/68/72/76; M0 is 194:5
- M2-M5 surviving the `sse_session` filters (no in-scope test counts RAW catch-up controls or sends a multi-record shared batch; livefeed_swap's raw legs only wait for `upToDate`)
- C1's paths: feed head = `src.frontier()` at feed.rs:633, `solo` at feed.rs:971, JSON "messages ARE records" at creation/fork.rs:242
- the 7-line source-allowance prune
- the comment-only persistence_faults edit leaving `function_sha256` and the review-mechanisms pin unchanged
- the inventory going from 502 to 503

**C1 (BLOCKER): re-attaching the `///` doc to `execute_segment` fails the source ratchet.**
`execute_segment` carries a function-wide `#[expect(clippy::unwrap_used, …)]` (read.rs:131-134). Its contract fingerprints
every `path` fact in the item's scope (source_rules.py:176-183, `unwrap_site:path:<qualified>:<digest>`). The scope is the item span, and the span includes outer attributes.

The scanner makes a `path` fact for every `syn::Path` (scan.rs:222-224). That includes the `doc` path of each `#[doc = "…"]`, which it reaches through `visit::visit_attribute` → `visit_meta_name_value` (scan.rs:273-276). With lines 116-120 on `execute_segment`, the metric
`unwrap_site:path:crate::execute_segment:<sha16("crate::execute_segment\0doc")>` goes from 0 at the merge base to 5.

`exception_growth` (source_rules.py:204-214) fails on any per-metric increase:
`accepted exception grew without a new decision: ('src/application/read.rs', 'crate::execute_segment', 'function', 'expect (clippy :: unwrap_used , reason = "execute_segment; …")'): unwrap_site:path:crate::execute_segment:… 0 -> 5`.
It fails in both `gate.py` and `architecture-gate.py --check`, because the latter calls `source_gate.check()` at architecture-gate.py:216. It also stops `--prune` from writing, since gate.py:63-64 and source_gate.py:69 prune only when there are no other problems. So §7 step 5's "7 lines pruned, then OK" cannot happen as written.

The plan's §4 table is correct that the net `syntax_facts` falls (+10 facts, −~19). It misses this per-key fingerprint.

Fix:
- Delete read.rs:121-126 only.
- Either (a) turn lines 116-120 into plain `//` comments above `#[expect(`, which produces no facts and sits outside the span, or (b) move them as `///` onto `ReadPlan::execute` (read.rs:57). That item has no doc and no expect, and `impl<'a> ReadPlan<'a>` has no impl-wide expect.
- Do NOT move them onto `read_merged`. That grows its `too_many_arguments` contract's `scope_lines` (read.rs:62-65).
- Correct the stale `src/dst.rs` path to `src/dst/runtime.rs` at the same time.
- Read.rs budget: with (a), 938 → 909 still holds. With (b), 909 plus 0: the lines move and none are added.
- Add a negative control: "doc left as `///` on execute_segment → the `unwrap_site:path … 0 -> 5` failure above".

**C2: `src/sse/feed.rs` will NOT be in `production_unchanged_files`.** This corrects §5 bullet 3 and §7 step 10.
`normalized_source` returns None when any non-builtin attribute (`derive` is not in `builtins`) is not in `fixed_attribute_inputs` (production_changes.py:118-128). That set requires identical line, column and byte positions (lines 17-57). `#[derive(Debug, Clone, Copy, PartialEq, Eq)]` at feed.rs:1147 moves up 5 lines, so the file stays conservative. The file also has no trailing-test-prefix match, because the edit is at line 815.

Result: feed.rs is in `mutation_source_files` and owner `sse_feed` is selected. Its affected new lines are 814 (blank) and 815 (the first `///` of `take_visible`; `function_body_span` starts at the first stmt, visit.rs:757-760). No mutant is selected, and the log prints `sse_feed: no executable mutants in the selected scope`. The outcome is harmless, but the stated expectation is wrong. The same derive reasoning applies to http.rs (derives at 3010/3200), which the plan already treats as changed.

**C3: R1 command.**
- With C2 already committed, `git diff HEAD > target/item59-c2.diff` is empty. Use `git diff HEAD~1 HEAD` (C2 committed directly on 7c4f8606 on the scratch branch), or `git diff 7c4f8606` with C2 uncommitted.
- Mirror `mutation_driver.mutation_command` (mutation_driver.py:48-60): add `--cargo-arg=--locked --cargo-arg=--lib --build-timeout 600`. Without `--lib`, cargo test also builds and runs the bins and integration targets under the filters, so the run is not CI's.

**C4: ceiling table.** The ceiling is against merge base `9b2f53bc`:
- `src/shard.rs` = **3,232** (now 3,197), not 3,197.
- `src/sse/session.rs` merge base = 948, so the ceiling is 1,000.

Neither changes the plan (neither file grows past it).

**C5: §4 C2 step 3 wording.** "The test-cfg body of `serve` is token-identical to the non-test body" is not literally true. The `#[cfg(test)] crate::failpoints::pause(…)` statements remain (for example session.rs:216-217, 249-250 and the SseBeforeSend and SseFeedBeforeDrive pauses). The conclusion still holds: they are expression statements that add no nesting, so all five `serve` expects stay fulfilled and the contracts only shrink.

**C6: trivia.**
- `raw_sse_connect` spans sse_delivery.rs:341-350 (doc 341-342, fn 343-350), not 341-353.
- The C1 insertion point, after the `}` at line 510 and before `collect_session`'s doc at 512, is correct.
- In CI terms, the http.rs change also turns on the Miri leg (`BUFFER_PREFIXES` contains `src/http`) and src/sse turns on the Loom leg (verification_plan.py:27-30). This adds no new obligation, but expect both legs in the run.

**Verdict: ready-with-corrections.** Only C1 affects the build: the plan as written fails `gate.py` and `architecture-gate.py --check`. The fix is to take the doc lines off the unwrap-fingerprinted `execute_segment`. The C1 mutation-kill test is sound and buildable, and its kill table holds, including M4 panicking under the `quality` profile, which inherits dev overflow checks.
