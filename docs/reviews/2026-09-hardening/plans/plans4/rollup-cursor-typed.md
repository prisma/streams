# Plan: rollup cursors fail closed (review item 49)

Repository `/Users/sorenschmidt/code/streams`, branch `slate @ 8dabca7f`. Read-only
verification; nothing in the repo was touched or run. Every line number below was
re-found by content on the current tree (the reviewer's numbers are stale).

**Verdict on the review claim: CORRECT and current, with one precision.** `ops_cursor`
really does swallow read errors (and non-UTF-8 checkpoints) into "no checkpoint";
`system_read` really does default a missing `Stream-Next-Offset` to `""`; both `hdrs`
blocks really are dead. The precision: on the shipped read path a 2xx page can never
lack the header (`render_raw_read` sets it unconditionally on both the local and the
relay leg), so the `system_read` half is a latent contract hole with no reachable
end-to-end failure today; the `ops_cursor` half is a live fail-open whose consequence is
a double merge of the `_ops_metrics` minute tier (`samples += 1` is not idempotent).
The reviewer's Change is buildable as stated and is the smallest correct change; no
verbatim-move commit is needed because the edit SHRINKS `billing.rs` (2,300 -> 2,295).

---

## 1. Problem (verified, with quotes)

### 1.1 `ops_cursor` fails open — `src/rollup.rs:733-740`

```rust
733    pub(crate) async fn ops_cursor(&self) -> Option<String> {
734        self.db
735            .get(K_OPS_CURSOR)
736            .await
737            .ok()
738            .flatten()
739            .and_then(|v| String::from_utf8(v.to_vec()).ok())
740    }
```

Three things collapse into `None`: a SlateDB read error (`.ok()`), a missing row, and a
checkpoint whose bytes are not UTF-8 (`.ok()` on `from_utf8`). `None` means "start of
the ledger" to the consumer (`src/billing.rs:1318-1319`):

```rust
1318    let cursor = rollup.ops_cursor().await.filter(|c| !c.is_empty());
1319    let Some((body, next)) = system_read(state, OPS_METRICS_STREAM, &key, cursor).await? else {
```

so one transient read error re-reads `_ops_metrics` from offset `None` and
`apply_ops_page` (`src/rollup.rs:744-775`) re-merges every snapshot: `counters` (last
wins) and `gauges_max` (max) are idempotent, but `agg.samples += 1` (`:766`) is not, and
the raw tier is re-put. The first replayed page then commits its own `next_cursor`
(`:772`), rewinding the durable checkpoint. It also bypasses the repository read
(`read_bytes`, `:395-405`), which is where the test fault hook lives, so the failure
cannot even be exercised.

The twin it should mirror, `src/rollup.rs:510-515`, already fails closed:

```rust
510    pub(crate) async fn cursor(&self) -> anyhow::Result<Option<String>> {
511        read_bytes(&self.db, K_CURSOR)
512            .await?
513            .map(|v| String::from_utf8(v.to_vec()).map_err(Into::into))
514            .transpose()
515    }
```

and `r14_required_read_failures_leave_every_row_and_checkpoint_unchanged`
(`src/rollup/accounting_failure_tests.rs:81-86`) pins it with `read_faults()` on
`K_CURSOR`. Nothing pins `ops_cursor`; `apply_ops_page` and `sweep_ops_raw` have no unit
test at all (`grep` of `src/rollup/`).

### 1.2 `system_read` defaults a missing position to `""` — `src/billing.rs:2228-2239` and `2274-2283`

```rust
2228    if resp.status().is_success() {
2229        let next = resp
2230            .headers()
2231            .get("Stream-Next-Offset")
2232            .and_then(|v| v.to_str().ok())
2233            .unwrap_or_default()
2234            .to_string();
...
2274        Ok(resp) if resp.status().is_success() => {
2275            let next = resp
2276                .headers()
2277                .get("stream-next-offset")
2278                .and_then(|v| v.to_str().ok())
2279                .unwrap_or_default()
2280                .to_string();
```

`next` is what `rollup_step` / `ops_rollup_step` commit as the checkpoint
(`apply_page(&envelopes, &next)` `:1291`, `apply_ops_page(&snaps, &next)` `:1331`). An
absent or non-visible-ASCII header therefore checkpoints `""`, which the next step maps
back to `None` via `.filter(|c| !c.is_empty())` (`:1277`, `:1318`) — a replay from the
start of the ledger, silently. Reachability today: `src/http/read.rs:397-403` sets
`stream-next-offset` on every 2xx raw page, and the relay leg
(`/v1/internal/segment-read`, `src/http.rs:3281-3364`) ends in the same `read_inner`, so
the default is never taken on this build. It is a contract hole (a foreign build or a
header-stripping proxy on the relay leg, or a future `read_inner` exit that forgets the
header), not a live bug.

### 1.3 The two `hdrs` blocks are dead — `src/billing.rs:1267-1272` and `1312-1317`

```rust
1267    use axum::http::{HeaderMap, HeaderValue};
1268    let mut hdrs = HeaderMap::new();
1269    hdrs.insert(
1270        "stream-encryption-key",
1271        HeaderValue::from_str(&key).map_err(|_| "bad usage key".to_string())?,
1272    );
```

`hdrs` is never read (lines `1273-1298` and `1318-1334` do not mention it);
`system_read` builds its own map (`:2205-2209`) and performs the identical validation
(`"bad system key"`). The only surviving effect of the dead blocks is the error string
`"bad usage key"` for a key that is not a valid header value — no test or doc references
it (`grep -rn "bad usage key" src/ docs/ scripts/` is empty).

---

## 2. Contract decision

**No wire change.** The header contract stays: every 2xx page of `GET /v1/stream/{name}`
(raw, `src/http/read.rs:403`) and of `GET /v1/internal/segment-read/{name}`
(`WIRE-MATRIX.md:220`) carries `stream-next-offset`. No status code, header or body
changes; `docs/refactor/WIRE-MATRIX.md` needs no row.

**Client-side contract (`billing::system_read`)** — a page IS its position:

| Response | Before | After |
|---|---|---|
| 404 | `Ok(None)` | `Ok(None)` (unchanged) |
| 2xx with non-empty visible-ASCII `stream-next-offset` | `Ok(Some((body, next)))` | unchanged |
| 2xx without the header, or empty, or not `to_str()`-able | `Ok(Some((body, "")))` -> checkpoint `""` -> replay from start | `Err("system read {stream}: page without stream-next-offset")` |
| other status | relay once / `Err(...)` | unchanged |

Same rule on both legs (local `axum` response and relayed `reqwest` response); both
expose `&http::HeaderMap` (one `http 1.4.2` in `Cargo.lock`), so one helper serves both.

**Checkpoint contract (`UsageRollup::ops_cursor`)** becomes the twin of `cursor()`:
`anyhow::Result<Option<String>>` — `Ok(None)` = no checkpoint yet (ledger from start),
`Ok(Some(c))` = resume after `c`, `Err` = the checkpoint could not be read or is not
UTF-8. `ops_rollup_step` propagates `Err(e.to_string())`; the existing supervisor loop
(`spawn_rollup`, `src/billing.rs:1522-1531`) already logs `ops rollup step: {e}`, counts
0 and sleeps 2 s, so the failure mode is "retry from the durable checkpoint", never
"restart the ledger". Both `cursor()` and `ops_cursor()` read through one `read_text`
repository read (through `read_bytes`, hence through the test fault hook).

**Durable `""` tolerance is kept.** `.filter(|c| !c.is_empty())` in both steps stays:
it protects against a `""` a previous build might have checkpointed (a `""` passed as
`offset=` would be `400 invalid_offset` forever). After this change nothing can write
`""` again; a `LedgerCursor` newtype that makes it unrepresentable is a follow-up (§8).

**Error strings.** `"bad usage key"` disappears with the dead blocks; `system_read`'s
`"bad system key"` is the single validation. The consumer never matches on these strings
(it logs them), so no typed-verdict rule is bent.

**Why not a typed `SystemReadError` enum now.** Nothing decides on the string; adding an
enum to a ceilinged file to carry one new arm is the "wrapper to satisfy a rule" the
policy forbids. Revisit if a caller ever needs to branch on it.

---

## 3. Red tests

Work order for the red run: (i) apply the two test-support visibility words in
`src/rollup.rs` (`pub(crate) const K_OPS_CURSOR`, `pub(crate) fn read_faults`) and add
the DST test only; run it -> RED; (ii) add the fix and the two unit tests -> GREEN. The
two unit tests cannot be runtime-red because the return type (`Option<String>`) and the
missing helper are the contract: they are compile-red, stated exactly below. All three
land in one commit.

### 3a. DST — the behavioural red (compiles on both trees)

File: `src/dst/tests/billing_controller.rs` (505 lines; theme "bounded billing passes",
where the `r09_*` tests already assert `rollup.cursor()` unchanged after a cancelled
pass). Append at the end of the file.

```rust
async fn rollup_rows(db: &slatedb::Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut iter = db.scan(..).await.unwrap();
    while let Some(kv) = iter.next().await.unwrap() {
        rows.push((kv.key.to_vec(), kv.value.to_vec()));
    }
    rows
}

/// Review item 49: an unreadable `_ops_metrics` checkpoint fails the ops
/// step. It never reads as the start of the ledger, which would merge
/// every snapshot into the minute tier a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreadable_ops_checkpoint_fails_the_step_instead_of_restarting_the_ledger() {
    let (state, _) = http_rig(mem()).await;
    let rollup = Arc::new(
        crate::rollup::UsageRollup::open(mem(), "", &state.config)
            .await
            .unwrap(),
    );
    assert!(state.rollup.install(rollup.clone()).is_ok());
    crate::ops::emit_metrics_once(&state).await.expect("emit");
    let mut absorbed = 0;
    for _ in 0..20 {
        let n = crate::billing::ops_rollup_step(&state)
            .await
            .expect("ops rollup");
        absorbed += n;
        if n == 0 {
            break;
        }
    }
    assert!(absorbed >= 1, "the rig's snapshot reached the minute tier");
    let before = rollup_rows(&rollup.db).await;
    assert!(
        before
            .iter()
            .any(|(key, _)| key.as_slice() == crate::rollup::K_OPS_CURSOR),
        "the drain committed its checkpoint"
    );
    crate::rollup::read_faults().lock().unwrap().insert((
        Arc::as_ptr(&rollup.db) as usize,
        crate::rollup::K_OPS_CURSOR.to_vec(),
    ));
    let step = crate::billing::ops_rollup_step(&state).await;
    assert_eq!(
        step,
        Err("injected rollup repository read failure".to_string()),
        "an unreadable checkpoint fails the step; it never restarts the ledger"
    );
    assert_eq!(
        rollup_rows(&rollup.db).await,
        before,
        "a failed required read moves no row and no checkpoint"
    );
    assert_eq!(
        crate::billing::ops_rollup_step(&state).await.expect("ops rollup"),
        0,
        "the next step resumes at the committed checkpoint"
    );
    assert_eq!(rollup_rows(&rollup.db).await, before);
    rollup.db.close().await.unwrap();
    engine_shutdown(&state).await;
}
```

Deterministic: no spawned rollup task, every wait is a bounded `for`, one in-memory
store per DB, the fault is keyed by this DB's pointer. `rollup.db` is `pub`
(`src/rollup.rs:368`), `http_rig`/`mem`/`engine_shutdown`/`Arc` are already imported in
this file.

**Expected red on the current tree** (scaffolding present, `ops_cursor` still `db.get`;
the fault is never consumed, the intact checkpoint is read, the page after it is empty):

```
---- dst::dst_tests::billing_controller::an_unreadable_ops_checkpoint_fails_the_step_instead_of_restarting_the_ledger stdout ----
thread 'dst::dst_tests::billing_controller::an_unreadable_ops_checkpoint_fails_the_step_instead_of_restarting_the_ledger' panicked at src/dst/tests/billing_controller.rs:<L>:5:
assertion `left == right` failed: an unreadable checkpoint fails the step; it never restarts the ledger
  left: Ok(0)
 right: Err("injected rollup repository read failure")
```

### 3b. Unit — the reviewer's first step (`ops_cursor` via `read_faults()` on `K_OPS_CURSOR`)

File: `src/rollup/accounting_failure_tests.rs` (196 lines). Add `K_OPS_CURSOR` to the
`use super::{...}` list; append:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ops_checkpoint_read_failure_is_an_error_never_the_start_of_the_ledger() {
    let db = Arc::new(
        Db::builder("ops-cursor", Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let r = UsageRollup {
        db: db.clone(),
        close_rows_visited: Default::default(),
    };
    assert_eq!(r.ops_cursor().await.unwrap(), None, "no checkpoint yet");
    r.apply_ops_page(&[], "c0").await.unwrap();
    assert_eq!(r.ops_cursor().await.unwrap().as_deref(), Some("c0"));
    read_faults()
        .lock()
        .unwrap()
        .insert((Arc::as_ptr(&db) as usize, K_OPS_CURSOR.to_vec()));
    assert_eq!(
        r.ops_cursor().await.unwrap_err().to_string(),
        "injected rollup repository read failure"
    );
    assert_eq!(
        r.ops_cursor().await.unwrap().as_deref(),
        Some("c0"),
        "one injected failure; the checkpoint is intact"
    );
    db.put(K_OPS_CURSOR, [0xff, 0xfe]).await.unwrap();
    assert!(
        r.ops_cursor().await.is_err(),
        "a checkpoint that is not UTF-8 is corruption, not the start of the ledger"
    );
    db.close().await.unwrap();
}
```

`apply_ops_page(&[], "c0")` is the real commit path with zero snapshots: only the
checkpoint is written, no `OpsSnapshot` fixture is needed.

**Expected red on the current tree:** does not compile, because the type is the
contract. `ops_cursor()` returns `Option<String>`, so
`error[E0599]: no method named `unwrap_err` found for enum `Option<String>` in the current scope`
and `error[E0599]: no method named `is_err` found for enum `Option<String>`` (the first
`assert_eq!` also fails to type-check: `error[E0277]: can't compare `String` with `Option<_>``).

### 3c. Unit — a system page is its position (`page_position`)

File: `src/billing/tests.rs` (216 lines, `use super::*;` reaches the private helper).
Append:

```rust
/// A system page is its position: `system_read` commits the page's
/// `stream-next-offset` as the rollup checkpoint, so a success carrying
/// none is refused, never checkpointed as "".
#[test]
fn a_system_page_without_a_position_is_refused_never_checkpointed_as_empty() {
    use axum::http::{HeaderMap, HeaderValue};
    let refused = Err("system read _usage: page without stream-next-offset".to_string());
    let mut headers = HeaderMap::new();
    assert_eq!(page_position("_usage", &headers), refused);
    headers.insert("stream-next-offset", HeaderValue::from_static(""));
    assert_eq!(page_position("_usage", &headers), refused);
    headers.insert("stream-next-offset", HeaderValue::from_bytes(b"\xff").unwrap());
    assert_eq!(page_position("_usage", &headers), refused);
    headers.insert(
        "stream-next-offset",
        HeaderValue::from_static("00000000000000000000000004"),
    );
    assert_eq!(
        page_position("_usage", &headers).as_deref(),
        Ok("00000000000000000000000004")
    );
}
```

`b"\xff"` is a legal header byte (obs-text) that `to_str()` refuses, so the third case
exercises the `to_str().ok()` arm, not only absence/emptiness.

**Expected red on the current tree:**
`error[E0425]: cannot find function `page_position` in this scope`.

There is no end-to-end oracle for a positionless page: `render_raw_read` always sets the
header and no DST fake peer exists to strip it on the relay leg (§8). The pure helper is
the whole decision, so the unit test is the verdict.

---

## 4. Edits, file by file

No new files, no move commit. Budgets are the merge-base ratchet
(`scripts/quality/source_rules.py:226`: `limit = max(1000, lines at origin/slate)`),
measured on `8dabca7f`. All counts are post-`cargo fmt`.

### 4.1 `src/rollup.rs` — 822 lines, limit 1,000. After: ~826.

No `#[expect]` anywhere in this file (the two in `src/rollup/tests.rs` are another file
and untouched). No `crate::http`/`crate::product` edges (baseline `{}`; none added).

(a) After `read_bytes` (`:405`), add the shared repository read (+9 incl. blank and
doc):

```rust
/// Checkpoints are opaque offset tokens. One that is not UTF-8 is a
/// corrupt checkpoint, never "no checkpoint yet".
async fn read_text(db: &Db, key: &[u8]) -> anyhow::Result<Option<String>> {
    read_bytes(db, key)
        .await?
        .map(|v| String::from_utf8(v.to_vec()).map_err(Into::into))
        .transpose()
}
```

(b) `cursor()` (`:510-515`, 6 lines -> 3):

```rust
    pub(crate) async fn cursor(&self) -> anyhow::Result<Option<String>> {
        read_text(&self.db, K_CURSOR).await
    }
```

(c) `read_faults` (`:460`): `fn read_faults()` -> `pub(crate) fn read_faults()` (0 lines;
`#[cfg(test)]` stays; needed by §3a).

(d) `K_OPS_CURSOR` (`:709`): `const` -> `pub(crate) const` (0 lines; needed by §3a).

(e) `ops_cursor()` (`:733-740`, 8 lines -> 6):

```rust
    /// The `_ops_metrics` checkpoint. An unreadable one fails the ops
    /// step; it never reads as the start of the ledger, which would
    /// merge every snapshot into the minute tier again.
    pub(crate) async fn ops_cursor(&self) -> anyhow::Result<Option<String>> {
        read_text(&self.db, K_OPS_CURSOR).await
    }
```

Complexity removed: two hand-rolled byte->text decoders become one; the ops checkpoint
now takes the same repository path (and the same fault hook) as the usage checkpoint.

### 4.2 `src/billing.rs` — 2,300 lines, CEILINGED at 2,300. After: 2,295 (net −5).

`#[expect]`-ratcheted functions in this file: `utc_year_month` (`:111`), `drain_once`
(`:762`,`:766`), `spawn_rollup` (`:1483`, **`clippy::unwrap_used`** — every call-site and
path fact under it is fingerprinted), `tombstone_walk` (`:2082`). **None is touched**, and
no `use` alias is added or removed anywhere in the file, so `spawn_rollup`'s callee
fingerprints (`rollup_step(&state)`, `ops_rollup_step(&state)`, `open_rollup`,
`sweep_ops_raw`, `close_months_due`, `publish_artifacts`) resolve exactly as before.
This is also why `rollup_step`/`ops_rollup_step` are NOT moved to a sub-module: a
`pub(crate) use consumer::rollup_step;` would change the resolved call-site digest under
`spawn_rollup`'s `unwrap_used` scope (`tools/quality-syntax/src/scan.rs:240-253` resolves
the first path segment through the file's imports) and force a re-decision of that
exception for no gain. `crate::http` edges: 35 today, baseline ceiling 43, none added.

(a) `rollup_step` (`:1267-1272`): delete the six lines quoted in §1.3 (the `use`, the
`let mut hdrs`, the `insert(...)`). Nothing else in the function changes. **−6.**

(b) `ops_rollup_step` (`:1312-1318`): delete the six dead lines; replace `:1318` with

```rust
    let cursor = rollup
        .ops_cursor()
        .await
        .map_err(|e| e.to_string())?
        .filter(|c| !c.is_empty());
```

(1 -> 5 lines). **Net −2.** Now byte-for-byte the shape of `rollup_step:1273-1277`.

(c) `system_read` local leg (`:2229-2234`): replace the six-line extraction with

```rust
        let next = page_position(stream, resp.headers())?;
```

**−5.** Relay leg (`:2275-2280`): same replacement, **−5.**

(d) New private helper between `system_read` and `urlencode` (after `:2287`; +13 incl.
blank, 3 doc lines, the mt-lint marker):

```rust

/// A page IS its position: the offset the rollup commits with the rows.
/// A success without one would be checkpointed as "" and replayed from
/// the start of the ledger, so it is refused here, never defaulted.
// mt-lint: allow(name-param-shared-core): system ledger name, used only in the refusal text
fn page_position(stream: &str, headers: &axum::http::HeaderMap) -> Result<String, String> {
    headers
        .get("stream-next-offset")
        .and_then(|v| v.to_str().ok())
        .filter(|next| !next.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("system read {stream}: page without stream-next-offset"))
}
```

The marker is required: `"stream"` is in `mt_lint::NAME_PARAMS`
(`src/mt_lint.rs:95`) and `billing.rs` is not a `SURFACE_FILES` ingress; the marker walk
(`src/mt_lint.rs:112-133`) accepts it on the comment line directly above the `fn`, as on
`system_read` itself (`:2197`). `axum::http::HeaderMap` and `reqwest::Response::headers()`
are the same `http::HeaderMap` (single `http 1.4.2` in `Cargo.lock`; `reqwest 0.12`).
Architecture report: `HeaderMap` regex hits in `billing.rs` go 4 down (two deleted
`use`s, two deleted `HeaderMap::new()`) and 1 up — a SHRINK, not growth.

Arithmetic: −6 −2 −5 −5 +13 = **−5 -> 2,295 ≤ 2,300.** `system_read` is ~80 lines
(<100), `ops_rollup_step` ~31, no nesting change.

### 4.3 `src/rollup/accounting_failure_tests.rs` — 196 -> ~229. `#![cfg(test)]`.

§3b test + `K_OPS_CURSOR` in the `use super::{...}` list. No `#[expect]` in the file.

### 4.4 `src/billing/tests.rs` — 216 -> ~238. `#![cfg(test)]`.

§3c test appended after the last test. The one `#[expect(clippy::cast_sign_loss)]`
(`:38`, `storage_clock_splits_at_month_boundaries`) is item-scoped and untouched, so its
`scope_lines`/`syntax_facts` are unchanged.

### 4.5 `src/dst/tests/billing_controller.rs` — 505 -> ~560. DST, under the 1,000 ceiling.

§3a test + `rollup_rows` helper appended. No `#[expect]` in the file. Test-only for the
architecture gate (`src/dst/` prefix), so its `crate::http` references are exempt.

### 4.6 Nothing else in `src/`

No change to `src/http.rs` (3,371/3,372), `src/http/read.rs`, `src/ops.rs`,
`src/dst/dst_tests.rs`, `src/dst/tests/billing_usage.rs` (961/1,000 — deliberately not
used for the new test), `src/billing/system_append.rs`.

---

## 5. Mutation-kill analysis

**Selection fact first.** Under the current planner none of the touched production files
is a mutation source: `scripts/quality/verification_plan.py:22-31` lists
`CRITICAL_PREFIXES` (`src/rollup/allocation`, `src/rollup/storage`,
`src/billing/read_accumulator`, `src/billing/read_spool`, `src/http`, ...) and
`scripts/quality/mutation_owners.py:53-169` registers no row for `src/rollup.rs` or
`src/billing.rs`; `plan()` (`:88-90`) selects only forced/registered/prefixed paths. For
this diff `plan.json` will read `"mutants": false, "mutation_source_files": []` — a
selection decision, not a passing experiment (`RUST-QUALITY.md`, "Verification selected
by the changed invariant"). Registering `src/billing.rs` or `src/rollup.rs` now is NOT
recommended: it would put 2,300 / 826 lines of unrelated arithmetic (`64 << 20` body cap,
month close, publication) under the fail-on-any-survivor leg with no bounding tests, the
exact trap `dd220be8` fixed for `fleet`. The in-diff adjacency trap (reflowed neighbours
entering scope) is therefore moot here. The tests are nevertheless written to kill every
mutant cargo-mutants 27.1 would generate, so the analysis holds if the files are
registered later:

| Site | Mutant | Killed by |
|---|---|---|
| `page_position` body | `Ok(String::new())` / `Ok("xyzzy".into())` | §3c last assert expects `Ok("0000…04")` (and the three `refused` asserts) |
| `page_position` body | `Err(String::new())` / `Err("xyzzy".into())` | §3c last assert |
| `!next.is_empty()` in `.filter` | `next.is_empty()` | §3c: `""` header -> `Ok("")` ≠ `refused`; `"0000…04"` -> `Err` ≠ `Ok` |
| `ops_cursor` body | `Ok(None)` / `Ok(Some(String::new()))` / `Ok(Some("xyzzy"))` / `Err(anyhow!(..))` | §3b `Some("c0")` asserts; §3a "resumes at the checkpoint == 0" (a `None` would re-read the snapshot -> `1`) |
| `read_text` body | same four shapes | §3b (`ops_cursor`) and existing `r14_required_read_failures…` (`cursor()` `is_err`), `r09_active_rollup_cancels_entered_read…` (`cursor() == Some(before.1)`) |
| `cursor` body | same | existing `r09_active_rollup_cancels_entered_read_and_replays_ledger_once` |
| `ops_rollup_step` body | `Ok(0)` | existing `ops_metrics_and_alerts_flow` (`total >= 2`) |
| `ops_rollup_step` body | `Ok(1)` / `Ok(usize::MAX)` | §3a (`Err` expected after the fault; `0` expected after) |
| `ops_rollup_step` body | `Err(String::new())` | `ops_metrics_and_alerts_flow` (`expect("ops rollup")`) |
| `system_read` local leg `?` on `page_position` | (no operator mutant; `?` is not mutated) | contract pinned by §3c |
| deleted `hdrs` blocks | none | — |

No new `match` guard, no `_ =>` arm, no new binary operator, no new boolean parameter.

---

## 6. Ledgers (same commit)

| Ledger | Action |
|---|---|
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write` — one new DST entry (§3a). Unit tests outside `src/dst/` are not inventoried (`scripts/test-inventory.py:138`). |
| `docs/quality/owners.json` | **none** — no new static, no `tokio::spawn`, no `#[path]` module, no `json!`/`select!` (the DST test uses none; `format!`/`assert*` are expression macros). |
| `docs/quality/source-allowances.json` | **none** — `crate::read_faults::FAULTS` keeps its identity (visibility is not part of it). |
| `docs/refactor/architecture-policy.json` | **none** — no new file, no new `crate::http` edge. |
| `docs/refactor/WIRE-MATRIX.md` | **none** — no wire change. |
| `src/dst/tests/README.md` | **none** — no new DST module (`billing_*` row already covers `billing_controller`). |
| `scripts/quality/mutation_owners.py` | **none** — see §5. |
| `docs/refactor/test-scenario-map.json` | **none** — catalogue-driven (`scripts/scenario-map-report.py:173-178`); a new test needs no scenario. |
| `docs/MULTITENANCY-MAP.md:246` | prose mentions `ops_cursor()`; still true, leave. |

Commit message must state: the red output of §3a verbatim; that §3b/§3c are compile-red
by construction (type/symbol are the contract); `billing.rs 2,300 -> 2,295`,
`rollup.rs 822 -> 826`; `"bad usage key"` retired in favour of `system_read`'s
`"bad system key"`; that neither file is a mutation source under the planner (with the
`plan.json` fields quoted).

---

## 7. Controls

Run from `/Users/sorenschmidt/code/streams`, after the tree's current gate finishes.

1. **Red** (after §3 step (i): visibility words + DST test only):
   ```
   cargo test --locked --release --lib dst_tests::billing_controller::an_unreadable_ops_checkpoint -- --nocapture 2>&1 | tee target/legs/item49-red.log
   ```
   Expect exactly the panic in §3a (`left: Ok(0)`, `right: Err("injected rollup repository read failure")`), `test result: FAILED. 0 passed; 1 failed`.
2. **Green** (after §3 step (ii): fix + unit tests), each leg proving it ran:
   ```
   scripts/test-leg.sh target/legs/item49-dst.log --exact dst::dst_tests::billing_controller::an_unreadable_ops_checkpoint_fails_the_step_instead_of_restarting_the_ledger -- --locked --release --lib dst_tests::billing_controller::
   scripts/test-leg.sh target/legs/item49-rollup.log --exact rollup::accounting_failure_tests::an_ops_checkpoint_read_failure_is_an_error_never_the_start_of_the_ledger -- --locked --release --lib rollup::
   scripts/test-leg.sh target/legs/item49-billing.log --exact billing::tests::a_system_page_without_a_position_is_refused_never_checkpointed_as_empty -- --locked --release --lib billing::tests::
   ```
3. **Neighbours that consume `system_read` / the ops step** (all must stay green):
   ```
   cargo test --locked --release --lib dst_tests::billing_usage::ops_metrics_and_alerts_flow
   cargo test --locked --release --lib dst_tests::security_audit::
   cargo test --locked --release --lib dst_tests::runtime_journals::
   cargo test --locked --release --lib dst_tests::security_workload::
   cargo test --locked --release --lib dst_tests::billing_
   ```
4. **Budgets** — must print ≤ 2300 / ≤ 1000 / ≤ 1000 / ≤ 1000 / ≤ 1000:
   ```
   wc -l src/billing.rs src/rollup.rs src/dst/tests/billing_controller.rs src/rollup/accounting_failure_tests.rs src/billing/tests.rs
   ```
5. **Quality entry point** (fmt, clippy `-D warnings`, rustdoc `-D warnings`, machete/deny, architecture report+gate, scenario map, `test-inventory --check`, review evidence, mt-audit, mt-lint):
   ```
   bash scripts/quality.sh
   ```
   In particular `python3 scripts/test-inventory.py --check` must pass after the `--write`, and mt-lint must report no `name-param-shared-core` finding for `page_position`.
6. **Planner receipt** (with the commit made, `origin/slate` = `8dabca7f`):
   ```
   python3 scripts/quality/verification_plan.py --out target/quality-plan && python3 -c 'import json;p=json.load(open("target/quality-plan/plan.json"));print(p["mutants"],p["mutation_source_files"],p["unregistered_mutation_source_files"])'
   ```
   Expect `False [] []`. (If it prints a path, a file under a critical prefix was touched by mistake.)
7. **Ratchet facts to confirm on the diff** (no growth): `git diff --stat` shows no change to `src/http.rs`; `grep -c "crate::http" src/billing.rs` ≤ 43 (baseline) — today 35; no `#[expect(` line appears in the diff.
8. **Whole gate before push**, as this branch requires: `OUT=/tmp/gate-item49.txt bash scripts/gate.sh`; then `gh run view` after the push — never claim CI green from the local gate alone.

---

## 8. Out of scope / follow-ups

- **`LedgerCursor` newtype** (non-empty by construction) so both `.filter(|c| !c.is_empty())`
  and `page_position`'s emptiness check disappear; belongs with the MULTITENANCY-MAP cursor
  reset note (`docs/MULTITENANCY-MAP.md:267`), which already plans a clean cursor cut.
- **`rollup_step` / `ops_rollup_step` are twins**; a shared `ledger_step` would dedupe them
  but every callee name under `spawn_rollup`'s `unwrap_used` scope would change — do it only
  when that exception is re-decided for its own reason.
- **End-to-end oracle for a positionless relay page** needs a DST fake peer (or a
  header-stripping proxy on the relay leg); none exists. The pure helper is the decision
  point, so §3c is the verdict for now.
- **`ops_m1` (test-only, `src/rollup.rs:778-785`)** keeps `.ok().flatten()`; it is a
  test accessor, not a checkpoint.
- **`sweep_ops_raw` and `apply_ops_page` have no unit tests**; `sweep_ops_raw`'s
  `unwrap_or("")` on a non-UTF-8 key is a separate (harmless) lenience.
- **`docs/MULTITENANCY-MAP.md`** line references for `system_read`/`rollup_step` are stale
  (`:2763`, `:1420`, `:1460`); a doc sweep, not this commit.
- Registering `src/billing.rs`/`src/rollup.rs` as mutation owners (§5) is a separate,
  deliberate decision with its own bounding tests.

---

## Skeptic corrections (C1..C9)

Verified read-only against the working tree at `df9ff212` (HEAD; `origin/slate` = `8dabca7f`,
the merge base the ratchets use). Every quoted line in §1, §4 re-found at the stated number;
`wc -l` matches (rollup.rs 822, billing.rs 2,300, http.rs 3,371, billing_controller.rs 505,
accounting_failure_tests.rs 196, billing/tests.rs 216, billing_usage.rs 961). The five
`#[expect]` sites in billing.rs are exactly `utc_year_month:111`, `drain_once:762,766`,
`spawn_rollup:1483` (`clippy::unwrap_used`), `tombstone_walk:2082`; none in rollup.rs; the
ratchet (`scripts/quality/source_rules.py:109-201`) keys on `qualified\0value` digests and
span LENGTH, never on absolute line numbers, so the −6/−2 deletions above `spawn_rollup`
do not move its fingerprint. Single `http 1.4.2` in `Cargo.lock`; the crate pins
`reqwest = "0.12"` (`Cargo.toml:23`), so one `&http::HeaderMap` helper serves both legs.
Planner: `src/rollup.rs` / `src/billing.rs` match no `CRITICAL_PREFIXES` entry
(`verification_plan.py:22-31`) and have no `mutation_owners.py` row (the nearest rows are
`src/billing/system_append.rs:68` and `src/ops.rs:71`, both untouched), so `mutants: false`
is a correct selection fact. `docs/quality/diagnostic-allowances.json` is empty; the
`crate::UsageRollup::ops_cursor` entry in `legacy-diagnostics.json:21376` is the frozen
adoption inventory (`unreachable_pub` from when it was `pub`), not an active allowance.
`review-mechanisms.json` pins only the four `r09_*` DST tests and two `r14_*` unit tests in
the touched files — the plan appends after them and changes only the file-level `use` list,
which `function_hash` does not cover. Source-allowance identity
`(global, src/rollup.rs, crate::read_faults::FAULTS, ...)` is visibility-blind
(`source_rules.py:66-73`). mt-lint: `"stream"` is in `NAME_PARAMS` (`src/mt_lint.rs:95`),
`billing.rs` is not in `SURFACE_FILES` (`:106`), the marker walk (`:112-133`) accepts a
`//` line under `///` docs. No unbuildable control: none of the controls use `hreq`, enforce
mode or stream creation in the deployment tenant; `_ops_metrics` is created by
`system_append` under the system project exactly as `ops_metrics_and_alerts_flow` already
does on a plain `http_rig(mem())`, and `http_rig` spawns neither `spawn_telemetry` nor
`spawn_rollup` (only `bootstrap.rs:888-890` and the two explicit DST sites do), so the
manual-step test is deterministic.

### C1. The tree is not clean: another item's work is in flight, and `test-inventory.py --write` will swallow it

`git status` at review time: `M src/dst/tests/fixture_auth.rs`, `M src/dst/tests/quota_read_volume.rs`,
`M src/product.rs`, `M src/product/consumer_pull.rs` (rank-28 step B, a new DST test
`consumer_pulls_draw_on_the_read_byte_quota` in `quota_read_volume.rs:+95`). The plan's §6
step "python3 scripts/test-inventory.py --write" regenerates the WHOLE manifest from
`src/dst` (`scripts/test-inventory.py:132-146`), so run on this tree it writes TWO new
entries; committing that manifest with only the item-49 files makes CI's
`test-inventory.py --check` (`.github/workflows/ci.yml:56`) fail with
`new test requires inventory: consumer_pulls_draw_on_the_read_byte_quota` reversed
(`missing test: ...`) at that commit. **Correction:** either land after the rank-28 commit,
or stage only the item-49 hunk of `docs/refactor/test-inventory.json` and confirm
`git diff --cached docs/refactor/test-inventory.json | grep '^+ *"name"'` prints exactly
`an_unreadable_ops_checkpoint_fails_the_step_instead_of_restarting_the_ledger`. Also
`git add` only the five source files listed in §4 plus the manifest; `git diff --cached --stat`
must not show `product.rs`, `consumer_pull.rs`, `fixture_auth.rs` or `quota_read_volume.rs`.
(§0 "slate @ 8dabca7f" should read HEAD `df9ff212`; the difference is a verbatim
`consumer_pull.rs` move that touches none of the plan's files or budgets.)

### C2. §3b and §3c are only compile-red; both can be runtime-red with a predictable message at no cost

The task's red-first bar is a test that FAILS on the current tree with a predictable message.
A missing symbol (`E0425`) or a mismatched type (`E0599`/`E0277`) is satisfied by ANY body,
so it pins nothing about behaviour. The plan's two-step work order (§3 "(i) … (ii)") already
exists for §3a; extend it so the red step stages the OLD semantics behind the NEW shape:

- **§3b red step:** change only the signature and wrap the existing body:
  `pub(crate) async fn ops_cursor(&self) -> anyhow::Result<Option<String>> { Ok(self.db.get(K_OPS_CURSOR).await.ok().flatten().and_then(|v| String::from_utf8(v.to_vec()).ok())) }`
  and adjust `billing.rs:1318` to the §4.2(b) form. Then §3b compiles and fails at
  `r.ops_cursor().await.unwrap_err()` with
  ```
  thread 'rollup::accounting_failure_tests::an_ops_checkpoint_read_failure_is_an_error_never_the_start_of_the_ledger' panicked at src/rollup/accounting_failure_tests.rs:<L>:33:
  called `Result::unwrap_err()` on an `Ok` value: Some("c0")
  ```
  (the fault is never consumed because `db.get` bypasses `read_bytes`; the checkpoint reads
  intact). The green step then replaces the body with `read_text(&self.db, K_OPS_CURSOR).await`.
- **§3c red step:** stage `page_position` with the old extraction
  (`Ok(headers.get("stream-next-offset").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string())`).
  `clippy::unnecessary_wraps` would fire on that body but the red step runs `cargo test`
  only, never `scripts/quality.sh`. Expected red at the FIRST assert (empty map):
  ```
  thread 'billing::tests::a_system_page_without_a_position_is_refused_never_checkpointed_as_empty' panicked at src/billing/tests.rs:<L>:5:
  assertion `left == right` failed
    left: Ok("")
   right: Err("system read _usage: page without stream-next-offset")
  ```
  The green step substitutes the §4.2(d) body. Record both red outputs in the commit message
  beside §3a's.

Both stagings are transient (same commit); the final diff is exactly §4.

### C3. Prefer renaming the parameter over adding a reviewed mt-lint exemption marker

`page_position(stream: &str, …)` needs the marker only because the parameter is spelled
`stream` (`src/mt_lint.rs:95`, `:154-170`: the rule matches on the parameter NAME being in
`NAME_PARAMS` with a `str`-shaped type). The value is used once, inside the refusal text, and
is never an identity. Every marker is printed as a "reviewed exemption" for certification
eyeballing (`src/mt_lint.rs:412-424`); adding one for a formatting argument is noise.
**Correction (preferred):** `fn page_position(ledger: &str, headers: &axum::http::HeaderMap)`
with `format!("system read {ledger}: …")` — no marker, no lint finding, and §4.2(d) is +12
instead of +13, so billing.rs lands at **2,294**. If the marker is kept, the plan as written
is correct (`system_read:2197` uses the same placement).

### C4. §1.2 precision: there IS a 2xx exit in `read_inner` without the header, and `""` can never be a legitimate position

`src/http/read.rs:241-251`: when `params.internal` and the request carries
`streams-internal-read-page: 1`, `read_inner` returns `axum::Json(WireReadPage)` with NO
`stream-next-offset` header. It is unreachable for `system_read` today because only
`src/application/read_remote.rs:361` sends that header and `system_read`'s local leg passes
`params.internal = false` (`billing.rs:2210-2213`) while the relay `mk` (`:2257-2272`) adds
only the `InternalTarget` headers — but it is the concrete in-tree shape of "a future exit
that forgets the header", so name it in §1.2 instead of the hypothetical. Conversely,
`raw_position` (`read.rs:320-327`) always yields 26 characters — `Offset::encode`
(`src/offsets.rs:25-38`) and `encode_ep` (`:83-95`) pad to 26 unconditionally, and a
`ReadPosition.after == 0` encodes as `raw_seq = 0`, not `""` — so the `!next.is_empty()`
refusal cannot reject a real page of an empty ledger. State this: it is the reason the
emptiness check is safe, and the reason no existing DST test (`ops_metrics_and_alerts_flow`,
the `r09_*` rollup tests, `security_audit::`, `runtime_journals::`) can turn red.

### C5. §3a red run leaves an unconsumed fault in the process-global set — harmless, but say so

On the current tree `ops_cursor` bypasses `read_bytes`, so the entry
`(Arc::as_ptr(&rollup.db), K_OPS_CURSOR)` inserted at §3a stays in `read_faults()` after the
panic. Nothing else reads `K_OPS_CURSOR` through `read_bytes` on that tree and the red run is
a single `--lib dst_tests::billing_controller::an_unreadable_ops_checkpoint` invocation, so
no other test can inherit it; on the green tree the entry is consumed by the very next
`ops_cursor()` call. Keep the red invocation single-test as §7.1 already writes it.

### C6. Verified claims the plan can cite as settled (no change needed)

- Red trace §3a: `emit_metrics_once` (`src/ops.rs:600-610`) appends one snapshot via
  `system_append`; `ops_rollup_step` drains it and commits `K_OPS_CURSOR`
  (`rollup.rs:772`); the next step reads the intact checkpoint, `system_read` at the tail
  returns a 2xx page with an empty body → `Ok(0)`; `assert_eq!(step, Err(..))` prints
  exactly the §3a message (`left: Ok(0)` / `right: Err("injected rollup repository read failure")`,
  `anyhow::bail!` at `rollup.rs:402` is a literal, so `to_string()` is byte-exact).
- Green trace §3a: `read_text` → `read_bytes` removes the entry keyed by the inner `Db`
  pointer (`&self.db` deref-coerces to the same `*const Db` as `Arc::as_ptr`), errors, and
  `ops_rollup_step` returns before `system_read`; rows unchanged; the following step is `0`.
- Budgets: billing.rs limit is `min(legacy 3,039, max(1000, merge-base 2,300)) = 2,300`
  (`source_gate.py:27-43`, `source_rules.py:226-228`, `legacy-source.json`); rollup.rs limit
  is `max(1000, 822) = 1,000`; the architecture gate's own file budget for billing.rs is the
  3,051 exception (`architecture-policy.json`), functions over 200 lines only. Reverse
  `crate::http` edges 35 now vs baseline 43 (`architecture-review-baseline.json`), 0 in rollup.rs.
- Ledgers: `test-inventory` covers `src/dst` only (`test-inventory.py:138`); scenario map is
  catalogue-driven (`scenario-map-report.py:166-178`); `src/dst/tests/README.md:17`
  `billing_*` row covers the module; owners.json needs no row (no static, spawn, `#[path]`,
  or non-expression macro — `format!`/`assert*` are in `EXPRESSION_MACROS`,
  `source_rules.py:15-19`); mt-audit categories (`multitenancy-audit.sh:52-62`) are not
  touched; WIRE-MATRIX has no change (header contract unchanged on both routes).
- Test path names in §7 are right: `src/dst/mod.rs:158 mod dst_tests;`,
  `src/dst/dst_tests.rs:25-26 #[path = "tests/billing_controller.rs"] mod billing_controller;`.
- `test-leg.sh` CLI shape (`<log> [tests_ran options] -- <cargo args>`) matches §7.2.
- No name collisions for `read_text`, `page_position`, `rollup_rows` in the touched files.

### C7. Optional: one doc line on `system_read`

`billing.rs:2193-2196` says "Returns (json body, next cursor)". After the change a success
without a position is refused; add "a 2xx page without `stream-next-offset` is an error,
never a `""` cursor" to that doc (+1 line; with C3 the file lands at 2,295, without C3 at
2,296 — both ≤ 2,300). Not required; the helper's own doc carries the invariant.

### C8. Ratchet/ledger sweep result

No missed ledger or ratchet found beyond C1's sequencing hazard. Specifically checked and
clean: `docs/quality/owners.json`, `docs/quality/source-allowances.json`,
`docs/refactor/architecture-policy.json`, `docs/refactor/WIRE-MATRIX.md`,
`src/dst/tests/README.md`, `scripts/quality/mutation_owners.py`,
`docs/refactor/review-mechanisms.json` (pinned hashes untouched),
`docs/refactor/review-unit-relocations.json`, `scripts/mt-audit-baseline.txt`,
`docs/quality/policy.json` immutable files, `scripts/clippy-baseline-fingerprints.txt`.

### C9. Verdict

**ready-with-corrections.** The analysis of the defect, the fix shape, the line budgets, the
expect-ratchet reasoning and the planner selection are all correct on the current tree.
Apply C1 (commit/inventory sequencing against the in-flight rank-28 work) and C2 (make the
two unit tests runtime-red in the staged red step) before executing; C3 is preferred, C4/C5/C7
are precision notes.
