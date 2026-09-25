# Bug #7: rollup segment keys embed the payer account — migration plan

Repo `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD `46f668b3` (= `origin/slate`). This plan is read-only work: nothing was edited and no cargo command was run.

Inputs:
- the diagnosis in `scratchpad/bugs/bug-7.md`;
- the earlier plan in `scratchpad/bugs/plan-7.md`, which re-keyed rows silently inside `open_with_cache`;
- the external reviewer's conditions, adopted by Søren on 2026-09-24:
  - no silent rewrite at startup;
  - either a new key namespace with an explicit activation point, or a resumable migration with durable phase markers;
  - a rehearsal on a consistent copy of a real rollup DB, which is the acceptance condition for the cutover;
  - the operator's `pendingArtifacts` split into publishable, blocked-corrupt and total counts.

**Shape of the answer.** The plan uses both options the reviewer offered.
- **A new namespace.** The physically keyed rollup gets its own SlateDB path, `telemetry/usage-rollup/v3/p0`, next to the untouched v2 path.
- **An explicit activation point.** The new clap knob `ROLLUP_SEGMENT_LAYOUT=payer|physical` defaults to `payer`, which leaves every deployment exactly as it is today.
- **A resumable copy with durable phase markers.** Under `physical`, the rollup opens v3, and it copies v2 into v3 whenever v3's marker does not yet say `done`. The v2 → v3 copy commits its marker in the same batch as its rows.
- **v2 is never written.** Rollback and abort are therefore "set `payer` again": v2 resumes from its own ledger cursor.

There are four commits:
- C1: split `pendingArtifacts`;
- C2: layout-aware keying, selected by the marker;
- C3: the knob and the copy;
- C4: the rehearsal harness and the cutover runbook.

---

## 1. Problem (verified on 46f668b3)

### 1.1 The defect, quoted

**The emitter re-stamps after it builds the closed month's final** (`src/shard/transaction/append.rs:283-290`):
```rust
            let finals_before = finals.len();
            bm.advance_storage_clock(bts, |closed| {
                finals.push(closed.to_snapshot(true));
            });
            if finals.len() > finals_before && bm.account_id != bref.identity.account_id {
                bm.account_id = bref.identity.account_id.clone();
            }
```
`to_snapshot` copies the old payer: `account_id: self.account_id.clone()` at `src/billing.rs:304`.
- The closed month's final therefore goes out under A.
- Every later snapshot, including the zero-gauge one from `billing_close`, goes out under B.
- This ordering is deliberate: commit `98db1f4b` bills each month to the workspace that owned it. It stays unchanged.

**The rollup keys the carried state by payer** (`src/rollup.rs:40-42`):
```rust
fn k_segment(account: &str, project: &str, stream_id: &str, seg: u32) -> Vec<u8> {
    format!("segment/{account}/{project}/{stream_id}/{seg}").into_bytes()
}
```
`Page::apply_snapshot` builds that key from the snapshot's payer (`src/rollup/page.rs:325-330`):
```rust
        let skey = k_segment(
            &id.account_id,
            &id.project_id,
            &id.stream_id,
            snap.segment_id,
        );
```
It then advances only that row (`page.rs:384-393`, `st.account_id = id.account_id.clone(); self.segs.insert(skey, st);`). A snapshot under B never reaches `segment/A/...`.

**The carry bills every row it finds, forever:**
- `MonthClose::carry` scans the whole prefix: `.read_page::<SegmentState>(b"segment/", after.as_deref())` (`src/rollup/close.rs:149`).
- `CarryPage::segment` (`close.rs:266-322`) checks the key against the row's own attribute: `state.account_id == parts[1]` (`:273-276`), which an orphan always passes.
- It skips a row only when `state.storage_accounted_through_ms >= self.close.boundary` (`:277`).
- Otherwise it adds `byte_ms(state.owned_frame_bytes_current, from, boundary)` to `month/{M}/A/...` and to both aggregates (`:296-311`). It then advances only the clock: `state.storage_accounted_through_ms = self.close.boundary; self.batch.put(key, ...)` (`:319-320`). The gauge is never zeroed.
- `finalize_row` freezes the phantom row and stages an immutable `artifact-pending/...` (`close.rs:245-253`).

**The provisional view shows the same phantom.** `stream_segment_states` scans `format!("segment/{account}/{project}/{stream_id}/")` (`src/rollup.rs:579`). Its caller, `src/product/usage.rs:101-103`, extrapolates the orphan's gauge into A's current-month usage.

**Nothing deletes `segment/` rows.** The only rollup deletes are:
- close cursors (`close.rs:108-109`);
- published pending rows (`rollup.rs:636`, `:690`);
- expired ops points (`rollup.rs:822`).

### 1.2 Consequence

The trigger is auth mode shadow or enforce, `ROLLUP=1`, and a workspace change before a month-crossing append (a real transfer, or a feed-lag fallback stamp).
- **From the close of M+1 onward**, every month close bills A the gauge frozen at the boundary. This continues after the stream is deleted.
- **B is billed correctly**, and month M is billed correctly to A.
- **The realistic ordering cannot be fixed with a "final retires the state" repair.** Month M usually closes before the first M+1 append. A's final then hits the finalized early return at `page.rs:351-354` and never touches the state.

Only one state per physical segment fixes both orderings. This is plan-7's analysis, re-verified line by line on this HEAD.

### 1.3 Why a format change needs a migration

Persisted `segment/{account}/...` rows hold the only copy of each idle segment's gauge and clock.
- A fresh namespace without a copy would drop idle gauges, and with them storage billing for idle streams.
- It would also drop finalized rows, the outbox and the cursor.
- Plan-7 re-keyed silently inside `open_with_cache` (`plan-7.md` "Migration"). It had no marker and no activation choice, and it deleted the old rows in place.
- The reviewer rejected that. The mechanism below replaces it.

**The deployed rc.4 binary cannot be relied on to fail closed.** Its carry (`git show 685ea035:src/rollup.rs`, lines ~1320-1360) skips a malformed key (`if parts.len() != 5 { continue; }`) and parses the segment id with `unwrap_or(0)`.
- A same-DB re-key would therefore make a rolled-back rc.4 silently under-bill.
- Plan-7's "the old binary fails its carry closed" holds only for post-hardening binaries.
- This is the decisive reason for a separate v3 path. An old binary opens v2 and never sees v3.

### 1.4 Full use-site list

**Segment-state keys and scans**

| Site | What | Changes in |
|---|---|---|
| `src/rollup.rs:40-42` | `k_segment` payer builder | stays (payer layout); physical builder added in `layout.rs` (C2) |
| `src/rollup/page.rs:9` | import `k_segment` | C2 (import removed) |
| `src/rollup/page.rs:325-330` | key built in `apply_snapshot` | C2 → `self.layout.key(id, snap.segment_id)` |
| `src/rollup/page.rs:110-112` | `commit` writes `segs` | unchanged |
| `src/rollup/close.rs:149` | carry scans `b"segment/"` | unchanged (same prefix in both layouts; v3 is its own DB) |
| `src/rollup/close.rs:266-285` | 5-part parse, `parts[4]` segment, `parts[1]` identity, `parts[2]`/`parts[3]` | C2 → `self.close.layout.parse(&key, &state)` |
| `src/rollup/close.rs:103,146,165,108` | `meta/close-seg-cursor/{month}` holds a segment key | unchanged; the copy refuses a v2 that holds one (C3) |
| `src/rollup.rs:376-388` | `close_scan_range` keeps a cursor inside its prefix | unchanged |
| `src/rollup.rs:573-586` | `stream_segment_states` payer prefix | C2 → layout prefix + payer filter |
| `src/product/usage.rs:101-103` | only caller of `stream_segment_states` | unchanged (signature kept) |
| `src/rollup.rs:28-31,492-517` | `ROLLUP_PATH` v2, `open_with_cache` | C3 (v3 path + copy under the knob) |

**Tests that name the keyspace, all unchanged**
- `src/rollup/close_seek_tests.rs:3,56` uses `k_segment("a","p","s",..)` with no marker, so the payer layout applies. Its arity is kept on purpose (plan-7 had to edit it).
- `src/rollup/accounting_failure_tests.rs:133` is `b"segment/a/p/s/not-a-number"`, in the pinned `r14_corrupt_watermarks_rows_and_close_keys_block_progress` (sha `9cc3ac…`, `docs/refactor/review-mechanisms.json:139`). The payer parse keeps today's check order, so the pin still errs on `parts[4].parse()` and stays byte-unchanged.
- `src/rollup/close/tests.rs:25-80` covers `read_page` over `segment/a|b|c|z` and is layout-agnostic.
- `src/rollup/tests.rs:216,457` call `stream_segment_states("acct", "proj", ..)` with no marker, so the payer layout applies.
- The pinned r14 tests and r23 build `UsageRollup { db, close_rows_visited }` directly. **No field may be added to `UsageRollup`**: the pinned bodies would change. The layout is therefore read from the DB marker, never stored on the struct.

**Pending-artifact counting, for the split**
- `src/http.rs:1719-1723`: `let pending = r.pending_artifacts(1000).await.map(|v| v.len()).unwrap_or(0);`, reported as `"pendingArtifacts": pending` at `:1740`.
- `src/rollup.rs:592-626`, `pending_artifacts`: a row whose key does not split into five parts, or whose body does not decode as `MonthRow`, is logged, skipped and **never counted**. The corrupt count is invisible on the surface today.
- `src/billing.rs:1300-1301`: the publisher uses `pending_artifacts(64)`.
- Tests: `rollup/tests.rs:35,402,564,705,803`, `accounting_failure_tests.rs:399`, `dst/tests/billing_controller.rs:551,558,578`.

**Docs**
- `docs/OBSERVABILITY-BILLING.md:503` says `v1` (stale) and `:512` shows the payer key.
- `docs/MULTITENANCY.md:1434-1435` says "segment identity re-stamps split-safe at month boundaries".
- `docs/MULTITENANCY-MAP.md:384-388` is a dated audit and is not edited.
- `RUNBOOK.md:686-707` covers the readiness surface.
- `docs/refactor/WIRE-MATRIX.md:204` covers `/operator/billing.json`.

**Size constraints verified with `wc -l`**
- Oversized files, which must not grow: `src/billing.rs` 2151, `src/http.rs` 3153.
- The plan does **not** touch `billing.rs`, `bootstrap.rs` or `append.rs`.
- `http.rs` shrinks, in C1 only.
- No `#[expect]` scope is touched in production. Every edited function was checked: `billing_readiness_axum` and all edited rollup functions carry no exception. The rollup's only exceptions are on `tests.rs:65` (`snap`) and `tests.rs:477`, and neither is edited.

---

## 2. Contract decision

### 2.1 Mechanism

**Keyspaces.** Which layout a rollup DB holds is decided by the DB's marker, not by its path or by a struct field.
```text
telemetry/usage-rollup/v2/p0   segment/<account>/<project>/<stream-id>/<segment-id>   payer layout (today; no marker)
telemetry/usage-rollup/v3/p0   segment/<project>/<stream-id>/<segment-id>             physical layout
v3 only:                       meta/segment-migration -> {"phase": "copying"|"wiping"|"done", ...}
```
- `SegmentLayout::of(db)` reads `meta/segment-migration`:
  - absent → `Payer`;
  - `done` → `Physical`;
  - `copying` or `wiping` → `Err("segment layout migration incomplete")`;
  - unreadable → `Err`.
- Every segment-key user reads the layout once per call: `page::apply`, `close_month` and `stream_segment_states`. An incomplete or corrupt v3 therefore fails every path closed.
- Only v3 ever gets a marker. The tests plant one in a mem DB to exercise the physical layout before the copy exists (C2).

**The physical layout.**
- The key is `segment/{project}/{stream_id}/{seg}`. `stream_id` is `desc.stream_epoch` (`billing.rs:576`), the incarnation's 16-byte epoch minted from runtime entropy (`runtime.rs:265-269`, called at `application/creation.rs:217`). No payer is needed for uniqueness.
- The payer is the existing `SegmentState.account_id` attribute. `apply_snapshot`'s version/clock fence rewrites it whenever it advances, so a re-stamp moves the one gauge to B in either ordering.
- The carry bills `state.account_id` and refuses an empty payer before any write.
- `stream_segment_states(account, ..)` scans the physical stream prefix and keeps states whose payer is `account`. The provisional view therefore shows a workspace exactly what the close will invoice it.

**The activation point** is `--rollup-segment-layout` / `ROLLUP_SEGMENT_LAYOUT`, a clap value parser over exactly `payer` or `physical`, defaulting to `payer`.
- **`payer`**: `open_with_cache` opens v2, which is today's code path. The bug persists until activation.
- **`physical`**: `open_with_cache` opens v3.
  - If v3's marker is not `done`, it first opens v2 as a SlateDB writer, which fences any payer writer still alive.
  - It then runs `layout::complete`, which copies until `done`, and closes v2.
  - Only then does it return the rollup. A failed copy fails the open.

**The copy.** It runs one durable v3 batch per step, and each batch carries the marker.
1. **Start step.** v3 must be empty (`Err("the physical rollup holds rows but no migration marker")` otherwise). The step records `copying{basis, after: null}`.
2. **Basis, read from v2 at every step.** The basis is `meta/usage-cursor`, `meta/ops-cursor` and `meta/oldest-unclosed-month`.
   - Any `meta/close-seg-cursor/*` or `meta/close-fin-cursor/*` present fails the step with `Err("a payer-layout month close is in progress; finish it with ROLLUP_SEGMENT_LAYOUT=payer before activating the physical layout")`.
   - This refusal matters for correctness. A mid-close v2 carries partial orphan charges into non-finalized month rows, and its segment cursor names a payer key.
3. **Copy step.** It reads up to one page of v2 rows after `after`: 1,000 rows or 1 MB, the close's own bounds.
   - Every key outside `segment/` is `put` verbatim.
   - Every `segment/` row is parsed with the payer checks (the key must have 5 parts, and the key's account must equal the attribute). It is folded into its physical key as the **newest by (usage_version, storage_accounted_through_ms)**. On a tie the state folded first is kept, mirroring `apply_snapshot`'s strict advance.
   - Targets folded in an earlier batch are re-read from v3.
   - The step then writes `copying{basis, after: <last key>, tally}`.
   - An empty page writes `done{basis, tally}`.
4. **Basis change.** When a resumed `copying` marker's basis differs from v2's current basis, v2 moved on in between (someone ran `payer`, or an old binary). The copy then writes `wiping{basis, after: null}` and deletes v3's non-marker keys cursor by cursor. It then restarts at `copying{basis, after: null}`.
   - The wipe is what removes stale keys: a published `artifact-pending` row, or a deleted close cursor.
   - At most one wipe may start per `complete` call. A second one fails with `Err("the payer rollup changed while this copy held it")`. v2 is fenced for the whole call, so a second wipe is impossible in a correct run. The guard also turns a would-be infinite mutant into a fast failure.
5. **Invariants.** v2 is never written. v3 is only ever installed at `done`. A kill anywhere loses at most the unflushed tail, because the rollup DBs run `wal_enabled: false` (`billing.rs:1510`) and `Db::write` returns before durability (slatedb `db.rs:1666-1670`). Marker and rows share a batch, so the resume point always matches the data.

**Rollback and abort**

| When | Action | Result |
|---|---|---|
| Before `done` (copy failing or refused) | set `ROLLUP_SEGMENT_LAYOUT=payer` | v2 reopens untouched. The partial v3 is inert. The next activation sees a basis change, wipes, and recopies. |
| After `done` | set `payer` | v2 reopens at its frozen cursor and re-consumes the `_usage` ledger from there. v3 is retained. Re-activation opens v3 (`done`), which catches up from its own cursor. Each DB is self-consistent with the ledger. |
| After `done`, across a month close | same, with a caveat | Both DBs close that month. Publication is create-only (`billing.rs:1256-1296`: `PutMode::Create`, then a byte comparison on `AlreadyExists`), so the second publisher records `ARTIFACT_MISMATCHES` and its row stays pending. That is fail-closed with an alarm, never a silent overwrite. The runbook says not to roll back across a month boundary. |
| Discard v3 | delete the objects under `<prefix>/telemetry/usage-rollup/v3/p0/` while nothing has it open | The next activation copies afresh. |
| Rolled-back pre-change binary (rc.4 or current slate) | none | It opens v2 and never sees v3, so there is no silent under-billing path. |

### 2.2 Server behaviour before, during and after

| | Before (knob `payer`, the default) | During (knob `physical`, v3 not `done`) | After (knob `physical`, v3 `done`) |
|---|---|---|---|
| Rollup DB installed | v2 | none yet (`open_rollup` has not returned) | v3 |
| Ledger apply / month close / publication | v2, payer keys (bug persists) | not running | v3, physical keys |
| Usage API (`/v1/streams/{name}/usage`, project usage) | v2 | 503 `usage_unavailable` "the usage rollup is not running on this instance" (`product/usage.rs:45-47`), retryable, as on any instance without a rollup | v3; the old payer stops accruing phantom storage |
| Required-mode readiness (`BILLING_MODE=required`, `ROLLUP=1`) | unchanged | `bootstrap.rs:685-693` awaits `open_rollup` **before** binding the listener, so the rollup owner does not listen until the copy is done. A refused or failed copy fails boot with `BILLING_MODE=required: rollup DB must open before serving: …`. Non-required mode: the rollup task runs the copy, the instance serves, and a failed copy ends the critical `usage-rollup` task, which is today's `TaskResult::Failed` behaviour. | unchanged (`rollup=true`) |
| `/operator/billing.json` `rollup` | as today plus the C1 split, `segmentLayout: "payer"` | `{"running": false}` (non-required), no listener (required) | `segmentLayout: "physical"` |
| Logs | none new | info `rollup segment layout: copying the payer rollup into the physical layout` (phase, basis); warn `…: the payer rollup moved on since the copy began; wiping the partial copy` | info `…: physical layout active` (tally, elapsed_ms) |

### 2.3 What changes at the edge

- **E1 (C1, operator-debug, approved: the reviewer's split).** `/operator/billing.json` `rollup` gains `pendingArtifactsBlockedCorrupt` and `pendingArtifactsTotal`.
  - `pendingArtifacts` keeps its exact value, the publishable rows counted up to 1,000 publishable, including its cap rule.
  - The rollup section is now serialized from a typed `RollupReadiness` struct in the rollup module. serde_json has no `preserve_order` (`Cargo.lock` shows no indexmap), so key order and every existing value are byte-identical.
  - The scan-failure behaviour (reported as `0`) is preserved. Changing it is D6.
  - Each readiness GET still logs every corrupt pending row, exactly as today (edge record #51); the shared parser logs.
- **E2 (C3, process).** New flag `--rollup-segment-layout` / env `ROLLUP_SEGMENT_LAYOUT`, values `payer|physical`, default `payer`. Any other value fails to parse (clap), so boot fails.
- **E3 (C3, operator-debug).** `rollup.segmentLayout`, one of `"payer"`, `"physical"`, or `null` when the marker is unreadable.
- **E4 (C3, process, `physical` only).** The one-time copy blocks the rollup owner's open (table above). A v2 with a close in flight refuses activation.
- **E5 (C2/C3, product/usage, `physical` only; this is the fix).**
  - After a re-stamp the old payer gets no carried storage in later months.
  - Its provisional fallback no longer shows the orphan.
  - The new payer is billed exactly as today.
  - Month rows, name and project aggregates, and invoices for payers without orphans are byte-identical. The rehearsal proves this (§3.4).
- **E6 (C2, operator/billing).** An incomplete or corrupt marker fails apply, close and usage reads closed. It is reachable only on a v3 DB.

**Nothing changes** for:
- the default configuration;
- the emitter (`append.rs`, `maintenance.rs`);
- month, name and project keys;
- artifact paths;
- the ledger format.

---

## 3. Red tests, acceptance checks, pins and controls

**Red-first procedure.** No red commit lands on `slate`, because `scripts/gate.sh` runs there. Each red is observed in the working tree, saved as a log, and then committed together with its fix. The tests below are written against the existing APIs wherever possible, so the red is an assertion rather than a compile error.

### 3.1 C1: the outbox split

**`dst::dst_tests::billing_readiness::the_rollup_report_splits_pending_artifacts_by_publishability`** (new, `src/dst/tests/billing_readiness.rs`)

Setup:
- a default `http_rig_build` rig;
- `UsageRollup::open(rig.state.data_store.clone(), "", &rig.state.config)`;
- `rollup.db.put` of four rows:

| Row | Body |
|---|---|
| `artifact-pending/2026-07/a/p/s1` | `{}` |
| `artifact-pending/2026-07/a/p/s2` | `{}` |
| `artifact-pending/2026-07/a/p/not-json` | `not json` |
| `artifact-pending/2026-07/short` | `{}` |

- then `install_rollup` and a GET of `/operator/billing.json` with no headers (the default rig authorizes, as at `:17-20`).

Assertions, in this order:
- `pendingArtifacts == 2` (pin: today's value);
- `pendingArtifactsBlockedCorrupt == 2`;
- `pendingArtifactsTotal == 4`;
- `running == true`, `lastApplyMs == 0`, `lastApplyAgeSecs == -1`, `oldestUnclosedMonth == null` and `pendingCorrectionArtifacts == 0`, which pins the move of the existing fields;
- `report["rollup"].as_object().unwrap().keys()` equals exactly the eight sorted names.

No `json!` is used in the test (a new `json!` in test code needs an owners.json row).

Red today:
```
assertion `left == right` failed: rows the publisher skips because they do not decode
  left: Null
 right: Number(2)
```

**`rollup::accounting_failure_tests::the_outbox_count_stops_where_the_publisher_scan_stops`** (new, beside the item-27 test)

Rows in key order:

| Key suffix | Body | Status |
|---|---|---|
| `…/a/p/s1` | `{}` | publishable |
| `…/a/p/s2` | `not json` | blocked |
| `…/a/p/s3` | `{}` | publishable |
| `…/a/p/s4` | `{}` | publishable |
| `…/short` | `{}` | blocked |

Expected:
- `artifact_outbox(2)` = `{publishable: 2, blocked_corrupt: 1, total: 3}`, and `pending_artifacts(2).len() == 2`, so the two scans stop at the same row;
- `artifact_outbox(64)` = `{3, 2, 5}`.

Red: compile error `error[E0599]: no method named \`artifact_outbox\` found for struct \`UsageRollup\``. This is a new API. Its assertions are pinned green and controlled by §3.5.

### 3.2 C2: layout-aware keying (new file `src/rollup/layout/tests.rs`, module `rollup::layout::tests`)

**How to see the red.** Stage `src/rollup/layout.rs` holding only `#[cfg(test)] mod tests;`, add `mod layout;` to `rollup.rs`, and run `cargo test --locked --lib rollup::layout::tests`. The tests use only existing APIs and plant the marker with `r.db.put(b"meta/segment-migration", br#"{"phase":"done"}"#)`.

**Fixtures**
- From `rollup/tests.rs`, made `pub(super)` with zero line delta: `ClockGuard`, `ClockGuard::at`, `mem_store`, `test_cfg`, `id`. `snap` is **not** touched (it carries an `#[expect]`).
- Local helpers:
  - `snapshot(account, version, month, gauge, through) -> SegmentSnapshot` (5 args);
  - `envelope(SegmentSnapshot) -> UsageEnvelope`;
  - `restamp_at_august(r, july_closed_first)`, which is plan-7's scenario:
    - a July live snapshot under `acct`: v1, gauge 100, clock Jul 15;
    - then one page holding the July final under `acct` (v1, byte-ms 17 d × 100, clock Aug 1) and the August live snapshot under `acct2` (v2, gauge 400, clock Aug 2, byte-ms 1 d × 100);
    - the July close runs before or after that page, per the bool.

Constants used below: d = 86,400,000 ms.

| Test | Scenario | Asserts (green) | Red today |
|---|---|---|---|
| `restamped_segment_stops_accruing_to_its_previous_payer` | marker `done`, final applied before the July close | `close_month(2026,8,0) == 1`; `month_row("2026-08","acct",..)`, `project_row`, `name_row` all `None`; acct2 August = 1 d×100 + 30 d×400 = **1,045,440,000,000**; acct July = 17 d×100 = **146,880,000,000**; `stream_segment_states("acct")` empty; `("acct2")` = one state with gauge 400; September close = 1 with acct2 30 d×400 = **1,036,800,000,000**, acct none | `the previous payer must not be carried into a month it never owned` |
| `restamp_after_the_month_closed_still_retires_the_previous_payer` | same, July closed first (the final hits `page.rs:351-354`) | same | same message |
| `the_payer_layout_still_carries_a_restamped_orphan` (**pin + non-vacuity control**) | no marker, final-first ordering | `close_month(2026,8,0) == 2`; acct August = 31 d×100 = **267,840,000,000**; `stream_segment_states("acct")` gauge 100 | green today; stays green until the payer code is deleted. It proves the scenario reaches the bug and that the marker alone changes the outcome. |
| `an_incomplete_or_unreadable_layout_fails_every_rollup_path_closed` | for marker in `{"phase":"copying"}`, `{"phase":"wiping"}`, `not json`: take a DB snapshot, then call `apply_page` (one read batch), `close_month(2026,7,0)` and `stream_segment_states` | each `Err`; for the two phases the text is exactly `segment layout migration incomplete`; the snapshot is byte-identical | ``called `Result::unwrap_err()` on an `Ok` value: ()`` |
| `a_payer_keyed_state_blocks_a_physical_carry` | marker `done` + `segment/acct/proj/7/0` (payer `acct`, gauge 1, clock Jul 1). `parts[3]` is numeric on purpose, so a `&&`→`||` mutant cannot fail by accident on the parse. | `close_month(2026,7,0)` errs with `invalid segment accounting key`; snapshot unchanged | `a payer-keyed state must block a physical carry` (today it carries and returns `Ok(1)`) |
| `a_physical_state_without_a_payer_blocks_the_carry` | marker `done` + `segment/proj/s/0` with `account_id: ""` | `Err` text `segment accounting state has no payer`; snapshot unchanged | `left: "invalid segment accounting key" right: "segment accounting state has no payer"` |

**Pins that must stay green, unchanged**
- `rollup::accounting_failure_tests::r14_*`: the payer parse keeps the check order.
- `rollup::close_seek_tests::r23_*`: `close_rows_visited` counts only `read_page` rows, and the marker read is not counted.
- All of `rollup::tests` and `rollup::close::tests`.

### 3.3 C3: knob and copy

**Stage 1, open-level tests.** Stage the `CliArgs` field, its deterministic default `"payer"` and the surface row, but no behaviour. `physical_cfg()` sets `cli.rollup_segment_layout = "physical"` on `CliArgs::deterministic()`. Run `cargo test --locked --lib rollup::layout::copy_tests`.

**The C3 fixture v2**, built through the payer API. It lives in `copy_tests.rs` as `payer_rollup(store, prefix)`.
- Streams:
  - `S`, `"aa"×8`, named `orders`;
  - `T`, `"bb"×8`, named `audit`.
- Page `c1`: July live `S@acct` (v1, gauge 100, Jul 15) and `T@acct` (v1, gauge 50, Jul 15).
- `close_month(2026,7,0)`.
- Page `c2`: `S` July final `@acct` (a no-op against the finalized row) and `S` August live `@acct2` (v2, gauge 400, Aug 2).
- `db.close()`.

Rows by construction:

| Class | Rows |
|---|---|
| month | 3 (Jul S/acct, Jul T/acct, Aug S/acct2) |
| name | 3 |
| project | 2 |
| artifact-pending | 2 |
| meta/usage-cursor | 1 |
| segment | 3 (acct/S, acct/T, acct2/S) |

That gives **11 non-segment rows, 3 legacy states and 2 physical states.**

| Test | Asserts (green) | Stage-1 red |
|---|---|---|
| `a_physical_open_copies_the_payer_rollup_and_retires_its_orphans` | **(a)** `segment_layout() == "physical"`; **(b)** every non-segment `(key, value)` of v3 equals v2's (11 rows), excluding the marker; **(c)** the segment keys of v3 are exactly `segment/proj/<S>/0` (v2, 400, Aug 2, `acct2`) and `segment/proj/<T>/0` (v1, 50, Aug 1, `acct`); **(d)** the marker is exactly `done{basis:{usage_cursor:"c2", ops_cursor:null, oldest_unclosed:null}, tally:{rows:11, legacy_states:3, physical_states:2}}`; **(e)** reopening under `payer` gives a full snapshot equal to the pre-copy snapshot of v2; **(f)** `close_month(2026,8,0) == 2`: no `(08, acct, S)` row, `(08, acct, T)` = 31 d×50 = **133,920,000,000**, `(08, acct2, S)` = 1 d×100 + 30 d×400 | ``assertion `left == right` failed: the activation opens the physical rollup  left: "payer" right: "physical"`` |
| `a_copy_refuses_a_payer_rollup_with_a_month_close_in_flight` | plant `meta/close-seg-cursor/2026-08` in v2; the physical open errs with the exact refusal text; the v3 marker is absent (the refusal comes before the start step writes) | `the activation must refuse a payer rollup whose month close is in flight` |
| `a_corrupt_payer_keyed_state_refuses_the_copy` | v2 holds `segment/a/p/s/not-a-number` (payer `a`); the open error chain contains `invalid digit found in string`; the v3 marker is `copying{after:null}` (resumable after repair) | `a corrupt payer-keyed state must refuse the copy` |
| `a_physical_rollup_holding_rows_without_a_marker_fails_closed` | a raw `Db` at the v3 path gets one `month/x` row; the physical open errs with `the physical rollup holds rows but no migration marker` | `a v3 with rows and no marker must fail closed` |
| `a_done_physical_rollup_opens_without_its_payer_source` | after `done` and close, delete every object under `<prefix>/telemetry/usage-rollup/v2/p0/`; the physical open succeeds; `segment_layout()=="physical"`; listing the v2 path afterwards is empty (no empty v2 is created) | layout red as above |
| `the_default_layout_keeps_the_payer_rollup` (pin) | default cfg: `segment_layout()=="payer"`; listing `<prefix>/telemetry/usage-rollup/v3/p0/` is empty | green today (pin) |

**Stage 2, internal-API tests.** These are written together with `layout::copy`. Their red is compile error `E0425` on `super::copy`. Their non-vacuity comes from the §5 mutants they kill.
- **`an_interrupted_copy_resumes_to_the_uninterrupted_result`.** A reference run uses `copy(v3, v2, usize::MAX, PageBound{rows: 4, bytes: 1_000_000})` over the 14-row fixture. That is **6 steps**: start, 4 copy pages (4+4+4+2) and done.
  - For every k in 0..=5: fresh store, raw `copy(.., k, ..)`, drop both DBs **without flush**, then resume through `UsageRollup::open(physical_cfg())`.
  - Then an every-step trial: open, step 1, `flush()`, drop, until `done`.
  - Each final v3 snapshot equals the reference (marker included).
- **`a_copy_restarts_from_empty_when_the_payer_rollup_moved_on`.**
  - Copy 3 steps and kill with a flush.
  - Reopen v2 under `payer`: apply page `c3` (T August live, v2, gauge 60) and `mark_artifact_published` for T's July artifact. That deletes `artifact-pending/2026-07/acct/proj/<T>` and adds `artifact-done/…`.
  - Step one at a time and record the phases. The first transition is `wiping`, followed by `wiping` steps, then `copying` steps, then `done`. Exactly one `copying→wiping` transition occurs.
  - The final v3 equals the fold of the **modified** v2, and `artifact-pending/2026-07/acct/proj/<T>` is absent from v3.
- **`one_copy_batch_reads_at_most_its_row_and_byte_bound`.**
  - v2 is raw: keys `k01`..`k09`, each with a 7-byte value (10 bytes per row).
  - `copy(.., 2, PageBound{rows: 3, bytes: 1_000_000})` gives marker `copying{after:"k03", tally.rows:3}`.
  - Then `copy(.., 1, PageBound{rows: 100, bytes: 20})` gives `after:"k05"`, `rows:5`.
- **`the_fold_keeps_the_newest_observation_in_either_scan_order`.** v2 is raw, and every state's payer equals its key's payer.

  | Pair | Rows | Kept |
  |---|---|---|
  | s1 | `a_old`(v1, 100, Sep 1) + `b_new`(v2, 400, Aug 2) | b_new |
  | s2 | `a_new`(v2, 400, Aug 2) + `b_old`(v1, 100, Sep 1) | a_new |
  | s3 (tie) | `a_tie`(v5, 10, Aug 1) + `b_tie`(v5, 20, Aug 1) | a_tie, gauge 10 |
  | s4 | `a_clk`(v3, 30, Aug 1) + `b_clk`(v3, 40, Sep 1) | b_clk |
  | s5 | `a_clk`(v3, 50, Sep 1) + `b_clk`(v3, 60, Aug 1) | a_clk |

  Tally `{legacy_states: 10, physical_states: 5}`.

**`config::tests::the_rollup_segment_layout_accepts_only_payer_or_physical`** (new)
- It uses `CliArgs::try_parse_from(["streams-slate","--s3-endpoint","http://127.0.0.1:1","--rollup-segment-layout", v])`: `physical` and `payer` parse, `physicall` is `Err`, and `test_cli().rollup_segment_layout == "payer"`.
- Red, observed in two steps:
  1. The test alone gives `error[E0609]: no field \`rollup_segment_layout\``.
  2. With the field but no `value_parser`, it gives `a misspelled layout must refuse to parse`.

**`config::tests::cli_surface_is_pinned`** gets the row `("rollup-segment-layout", "ROLLUP_SEGMENT_LAYOUT", "payer")`. With the row and no field, it fails with `CLI surface drifted; a rename/default change is a product decision, not a refactor`.

**`dst::dst_tests::billing_readiness::the_rollup_report_names_its_segment_layout`** (new)
- The default rig with a payer rollup installed reports `"payer"`.
- A rig with `HttpRigOptions { cli: |cli| cli.rollup_segment_layout = "physical".into(), .. }` and `UsageRollup::open(.., &rig.state.config)` installed reports `"physical"`.
- Red: `left: Null right: String("payer")`.
- The C1 key-set assertion gains `segmentLayout` in this commit.

### 3.4 C4: the rehearsal harness (the acceptance condition)

New file `src/rollup/rehearsal.rs` (`#![cfg(test)]`), module `rollup::rehearsal`, with two tests:
- `a_synthetic_payer_rollup_rehearses_the_cutover_exactly`, which runs in CI;
- `a_real_rollup_copy_rehearses_the_cutover`, marked `#[ignore = "needs an owner-supplied rollup copy at target/rollup-rehearsal/source"]`.

**Input.** The harness takes a directory holding a consistent copy of `<prefix>/telemetry/usage-rollup/v2/p0/**`, laid out as `<dir>/telemetry/usage-rollup/v2/p0/...`.
- It loads every object through `object_store::local::LocalFileSystem::new_with_prefix(dir)` (the default `fs` feature of object_store 0.14.1) into memory.
- Every trial runs on a fresh `InMemory` store seeded with those objects, so the copy on disk is never modified.
- The real test's directory is `concat!(env!("CARGO_MANIFEST_DIR"), "/target/rollup-rehearsal/source")`. These are compile-time macros, so there is no runtime environment read and no allowance row. `target/` is already gitignored.
- The report is written to `target/rollup-rehearsal/report.json` and printed.
- With no copy present, the ignored test **fails** with `no rollup copy at …/target/rollup-rehearsal/source`. It cannot pass vacuously.

**Checks.** Each check is an `anyhow::ensure!` with the exact numbers in its message. Each maps to a reviewer condition.

1. **Accounting conservation (static).**
   - v3's non-segment rows equal v2's, compared by class (source, month, name, project, artifact-pending, artifact-done, corr-pending, corr-done, meta, ops, other) on row count, byte count and sha256 over the sorted (key, value) pairs.
   - v3's segment rows equal an **independent** fold of v2's segment rows. The harness sorts each physical key's legacy rows by (version, clock), ties in scan order. It never calls `layout::newest`.
   - Reported: `legacy_states`, `physical_states`, `retired_orphans` (= L − P), `orphan_gauge_bytes`, and the number of distinct orphan payers. Only counts are reported, never identifiers.
2. **Source untouched and rollback isolation.** v2's census before and after the copy is equal. Reopening under `payer` after `done` reproduces the pristine census.
3. **Interruption safety.** An uninterrupted reference run records `steps` and `copy_ms`.
   - Single kills: at `min(steps−1, 64)` evenly spaced k, always including k=1 and k=steps−1, run `copy(k)`, drop **without flush**, and resume through the production `UsageRollup::open(physical)`.
   - One every-step trial: step, flush, drop, repeat.
   - One abort trial:
     1. kill at k = steps/2;
     2. apply a synthetic page to v2 through `payer`: a read batch from a fresh boot on an existing stream, which moves `meta/usage-cursor`;
     3. resume under `physical` and check that `wiping` happens exactly once.
   - Every final v3 census must equal the reference census (the abort trial is compared with the fold of the modified v2).
4. **Replay conservation.** For every month row and every `SegMonth` in it, the harness builds that segment's last-applied snapshot:

   | Snapshot field | Source |
   |---|---|
   | identity | the key's account, project and stream, plus the row's `stream_name` |
   | `usage_version` | `sm.usage_version` |
   | `month_final` | `sm.final_seen` |
   | ingest bytes and records | `sm.ingest_bytes`, `sm.ingest_records` |
   | gauge | `sm.gauge_bytes` |
   | storage byte-ms | `sm.storage_byte_ms` |
   | storage clock | `sm.accounted_through_ms` |

   - It applies them in pages of 500, with `next_cursor` set to the DB's current `meta/usage-cursor`, to v2 (payer) and to v3 (physical).
   - **Both census digests must be unchanged**, including every segment state. This is §9.3's replay contract, checked on the migrated DB.
   - Control: re-apply one non-final current-month snapshot with `usage_version + 1`. The census must change in exactly that month row and one segment state. This proves the page really applied.
5. **Invoice and recovery preservation.**
   - `artifact_outbox(usize::MAX)` is equal on v2 and v3 (publishable, blocked and total).
   - `billing::publish_artifacts` is looped to 0 into two fresh stores. The `(path, bytes)` sets must be equal, and so must the resulting `artifact-done/` rows (published_ms comes from the ClockGuard).
6. **Accounting conservation (forward close).**
   - The harness sets `ClockGuard::at(T)`, with T = start of the month after the latest month holding a month row, + `month_close_grace_ms` + 1.
   - It runs `close_months_due(grace)` on the pristine v2 and on v3.
   - For every `(month, account, project)`: `v2.project.storage − v3.project.storage == phantom`, and the same holds per name row and per month row.
   - `phantom` is computed by the harness from the retired orphans alone, with the legacy carry rule:
     - add `byte_ms(g, max(clock, start_m), boundary_m)` only when the pristine month row for (m, payer, project, stream) is absent or has `final_seen == false` for that segment, and `clock < boundary_m`;
     - the clock then advances to `boundary_m`.
   - All ingest, read, queue and append counters are equal. Every month row not involving an orphan is **byte-identical**.
   - `payer_closed − physical_closed` equals the number of orphan-only rows. After publication, the artifact sets differ by exactly those rows.
7. **Planted-orphan control (real copy only).**
   - A second trial plants, into the pristine v2, a legacy row for the first physical segment whose winner has `usage_version ≥ 1` and gauge > 0.
   - The planted row goes under payer `rehearsal-planted`, with `usage_version = winner − 1`, the winner's gauge and the winner's clock.
   - Checks 1 and 6 must then report one extra retired orphan, with phantom = that gauge × the closed span.
   - This makes the real rehearsal non-vacuous even when the real DB holds no orphan.

**Exact numbers for the synthetic test.** The fixture is built through the payer API under `ClockGuard::at(month_start_ms(2026,9) + 86_400_001)`. Stream names are `orders`, `audit`, `events` and `ledger`.

| Stream | Payers | July | August |
|---|---|---|---|
| S (`aa`×8) | acct → acct2 | live @acct: v1, gauge 100, Jul 15 | S final July @acct, then S live @acct2: v2, gauge 400, Aug 2 |
| T (`bb`×8) | acct | live: v1, gauge 50, Jul 15 | none |
| U (`cc`×8) | acct3, segments 0 and 1 | v1, gauges 10 and 20, Jul 15 | U seg0 live: v2, gauge 10, Aug 2 |
| V (`dd`×8) | ws_z → ws_a | live @ws_z: v1, gauge 70, Jul 15 | V live @ws_a: v2, gauge 80, Aug 2 |

V's orphan `ws_z` sorts **after** its winner `ws_a`, so a last-wins fold fails.

Build order:
1. The July live snapshots above.
2. A read batch (boot `boot`, seq 1, July, T, 101 bytes).
3. `close_month(2026,7,0)`.
4. The August snapshots above.
5. A `UsageCorrection` for `(2026-07, acct, proj, S)` (ingest +5), which lands in `corr-pending` because July is finalized.
6. `apply_ops_page` with one `OpsSnapshot` (instance `i1`).
7. `mark_artifact_published` for T's July artifact.
8. A planted `artifact-pending/2026-07/short` `{}`.

Expected rows by construction:

| Class | Rows |
|---|---|
| month | 7 |
| name | 7 |
| project | 6 |
| source | 1 |
| artifact-pending | 4 (3 July publishable + 1 blocked) |
| artifact-done | 1 |
| corr-pending | 1 |
| ops | 2 |
| meta | 2 |
| segment | 7 |

Expected results:
- v2 has 38 rows (31 non-segment).
- `legacy_states 7`, `physical_states 5`, `retired_orphans 2`, `orphan_gauge_bytes 170`.
- Steps at `PageBound{rows: 4, …}`: 1 + ⌈38/4⌉ + 1 = **12**.
- Replayed snapshots: 8.
- Outbox: publishable 3, blocked 1, total 4.
- Forward close of August:
  - `payer_closed 6`, `physical_closed 4`;
  - phantom `(2026-08, acct, proj)` = 31 d×100 = **267,840,000,000**;
  - phantom `(2026-08, ws_z, proj)` = 31 d×70 = **187,488,000,000**;
  - T August = 31 d×50 = **133,920,000,000** in both;
  - the artifact sets differ by exactly `acct/proj/<S>/2026-08.json` and `ws_z/proj/<V>/2026-08.json`.

These are derive-then-pin numbers. If the first green run prints a different count, the fixture is wrong and must be explained. The number must never simply be copied.

**What the owner supplies (D8).**
1. **A consistent copy of a real v2 rollup DB.** Stop its only writer: set `ROLLUP=0` on, or stop, that cell's `ROLLUP=1` instance. Copy with the Tigris endpoint, for example `aws s3 sync s3://<bucket>/<PATH_PREFIX>/telemetry/usage-rollup/v2/p0/ target/rollup-rehearsal/source/telemetry/usage-rollup/v2/p0/`, then restart the writer. A SlateDB checkpoint and clone would allow a copy without stopping, but that needs tooling (`src/bin/verify.rs` "clone") and is out of scope.
2. The choice of which cell and bucket.
3. A run of `cargo test --locked --release --lib rollup::rehearsal::a_real_rollup_copy_rehearses_the_cutover -- --ignored --nocapture` on a machine with enough memory for the copy.
4. Review of the report, which holds only counts, digests and months. It is committed as `docs/reviews/2026-09-hardening/evidence/rollup-rehearsal-<cell>-<date>.json`, with a short verdict in the acceptance record.

### 3.5 Non-vacuity controls (local; each saved as a log)

| Control | Edit | Expected failure |
|---|---|---|
| K1 | C2: `pays` returns `true` for Physical | `restamped_*`: `the previous payer keeps no provisional gauge` |
| K2 | C2: `Physical` arm of `key()` uses the payer builder | both `restamped_*` red with the §3.2 message |
| K3 | C3: `complete` returns `Ok(())` without copying | `a_physical_open_copies_*` (b): 0 vs 11 rows |
| K4 | C3: fold keeps the later-scanned state (swap `newest` args) | `the_fold_keeps_*` s2/s3/s5 and the synthetic rehearsal: `fold mismatch at segment/proj/<V>/0` |
| K5 | C3: skip the basis comparison | `a_copy_restarts_*`: stale `artifact-pending/…/<T>` present |
| K6 | C4: harness `fold` replaced by a call to `layout::newest` | not a failure, which is why the harness must not call it; reviewed by grep in C4 (`grep -n "newest" src/rollup/rehearsal.rs` → no output) |
| K7 | C4: forward-close phantom computed as 0 | synthetic: `phantom mismatch at (2026-08, acct, proj): 267840000000 vs 0` |
| K8 | C1: `artifact_outbox` counts only publishable into `total` | DST: `pendingArtifactsTotal 2 vs 4` |

---

## 4. Edits by file, in commit order

### C1: "The rollup readiness report counts publishable and corrupt pending artifacts"

- **`src/rollup/readiness.rs` (NEW, ~95 lines).** No `json!`, no effects, no transport imports.
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub(crate) struct ArtifactOutbox { publishable: usize, blocked_corrupt: usize, total: usize }`.
  - `impl UsageRollup { pub(crate) async fn artifact_outbox(&self, max_publishable: usize) -> anyhow::Result<ArtifactOutbox> }`. It scans `artifact-pending/` and classifies each row through the shared `pending_artifact_row`, so it logs exactly as today. It stops once `publishable == max_publishable`, which is `pending_artifacts`' own stopping rule.
  - `#[derive(Debug, Serialize)] #[serde(rename_all = "camelCase")] pub(crate) struct RollupReadiness { running: bool, last_apply_ms: i64, last_apply_age_secs: i64, oldest_unclosed_month: Option<String>, pending_artifacts: usize, pending_artifacts_blocked_corrupt: usize, pending_artifacts_total: usize, pending_correction_artifacts: usize }`.
  - `pub(crate) async fn readiness(&self, now_ms: i64, last_apply_ms: i64) -> RollupReadiness`. Its body is `http.rs:1719-1741` moved verbatim: `.unwrap_or(0)`, `.ok().flatten()`, and the age rule `if last_apply > 0 { (now - last_apply) / 1000 } else { -1 }`. The only change is that `pending` comes from `artifact_outbox(1000).await.unwrap_or_default()`.
- **`src/rollup.rs` (837 → ~845).**
  - Add `mod readiness;` after `mod reconciliation;` (`:20`) and re-export `pub(crate) use readiness::RollupReadiness;` only if clippy's `private_interfaces` asks for it.
  - Add `const K_OLDEST_UNCLOSED: &[u8] = b"meta/oldest-unclosed-month";` after `K_CURSOR` (`:63`).
  - Extract the body of `pending_artifacts`' loop (`:598-620`) into `fn pending_artifact_row(key: &[u8], value: &[u8]) -> Option<(String, String, String, MonthRow)>`, which logs the same two `tracing::error!` lines with the same fields and messages. `pending_artifacts` keeps its signature and tuple.
- **`src/rollup/close.rs` (371 → 370).** In `close_months_due`, replace the local `const MARKER: &[u8] = b"meta/oldest-unclosed-month";` (`:33`) with `super::K_OLDEST_UNCLOSED`. The function has no `#[expect]`.
- **`src/http.rs` (3153 → ~3131; oversized, shrinks).** Replace `:1718-1743`, keeping the `json!` invocation count of `billing_readiness_axum` at 3 so the macro-dsl allowance count is unchanged:
  ```rust
  if let Some(r) = state.rollup.get() {
      rollup_info = serde_json::json!(r.readiness(now, last_apply).await);
  }
  ```
- **`src/rollup/accounting_failure_tests.rs` (418 → ~450).** The outbox test (§3.1). Imports are unchanged apart from nothing new.
- **`src/dst/tests/billing_readiness.rs` (100 → ~140).** The DST test (§3.1).
- **Docs.**
  - `RUNBOOK.md:702`: the row covers the three counts, and adds that `pendingArtifactsBlockedCorrupt > 0` means corrupt rows that will never publish (operator repair).
  - `docs/refactor/WIRE-MATRIX.md:204`: list the `rollup` fields and name `RollupReadiness` as their owner.

### C2: "Segment states are keyed by physical segment in a rollup whose migration marker says done"

- **`src/rollup/layout.rs` (NEW, ~120 lines at C2).**
  - Module doc with the `text` keyspace block from §2.1 (rustdoc-safe).
  - `pub(super) const MARKER: &[u8] = b"meta/segment-migration";`
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(super) enum SegmentLayout { Payer, Physical }` with these methods. None has more than 4 args; there are no bool params and no `_ =>` arm.
    - `async fn of(db: &Db) -> anyhow::Result<Self>`: via `read_json::<Migration>(db, MARKER)`, so a test read fault reaches it.
    - `fn as_str(self) -> &'static str`.
    - `fn key(self, id: &BillingIdentity, segment: u32) -> Vec<u8>`.
    - `fn stream_prefix(self, account: &str, project: &str, stream_id: &str) -> Vec<u8>`.
    - `fn pays(self, state: &SegmentState, account: &str) -> bool`.
    - `fn parse<'k>(self, key: &'k [u8], state: &SegmentState) -> anyhow::Result<(&'k str, &'k str, u32)>`. The payer arm is the verbatim check sequence of `close.rs:267-276`. The physical arm checks 4 parts, `"segment"`, non-empty parts, `parts[3].parse()`, then `ensure!(!state.account_id.is_empty(), "segment accounting state has no payer")`.
  - `fn physical_key(project: &str, stream_id: &str, segment: u32) -> Vec<u8>`.
  - The `Migration` marker type, with serde defaults on every field so `{"phase":"done"}` decodes:
    - `#[serde(tag = "phase", rename_all = "snake_case")] pub(super) enum Migration { Wiping { basis, after: Option<String> }, Copying { basis, after: Option<String>, tally }, Done { basis, tally } }`;
    - `Basis { usage_cursor, ops_cursor, oldest_unclosed: Option<String> }`;
    - `Tally { rows, legacy_states, physical_states: u64 }`.
  - `#[cfg(test)] mod tests;`
- **`src/rollup.rs` (~845 → ~858).**
  - `mod layout;`.
  - `stream_segment_states` (`:573-586`): `let layout = layout::SegmentLayout::of(&self.db).await?; let pfx = layout.stream_prefix(account, project, stream_id);`, and in the loop `let state: SegmentState = decode_json(&kv.value)?; if layout.pays(&state, account) { out.push(state); }` (+4).
  - `pub(crate) async fn segment_layout(&self) -> anyhow::Result<&'static str>` (+4).
- **`src/rollup/page.rs` (480 → ~477).**
  - `apply` (`:17-28`): `let mut page = Page::new(db, super::layout::SegmentLayout::of(db).await?);`.
  - `Page` gets a `layout` field and `new` gets a parameter.
  - `apply_snapshot` `:325-330` becomes `let skey = self.layout.key(id, snap.segment_id);`, so the function goes from 96 to 91 lines.
  - Drop `k_segment` from the `use` list (`:9`).
- **`src/rollup/close.rs` (~370 → ~364).**
  - `MonthClose` gets `layout: SegmentLayout`. `close_month` reads it before building the struct (`:93`).
  - `CarryPage::segment` `:267-276` becomes `let (project, stream_id, segment) = self.close.layout.parse(&key, &state)?;`. The identity uses `project.to_owned()` and `stream_id.to_owned()` (`:282-283`). The order is still parse, then the clock check.
- **`src/rollup/tests.rs` (807, 0 net).** `pub(super)` on `ClockGuard`, `ClockGuard::at`, `mem_store`, `test_cfg`, `id`. `snap` and the `:477` test are not touched.
- **`src/rollup/layout/tests.rs` (NEW, ~300).** §3.2.
- **Docs.** `docs/MULTITENANCY.md:1434-1435` gets a clause: "split-safe in the cell; the rollup carries one state per physical segment once ROLLUP_SEGMENT_LAYOUT=physical is active (bug 7), and the payer layout still orphans the previous payer until then".

### C3: "ROLLUP_SEGMENT_LAYOUT=physical copies the payer rollup into a physically keyed rollup and serves from it"

- **`src/config/cli.rs` (686 → ~695).**
  - After `rollup` (`:414-416`):
    ```rust
    /// Rollup segment-state layout (bug 7). `payer` keeps the v2 rollup;
    /// `physical` opens the v3 rollup, copying v2 into it first until its
    /// migration marker says done (RUNBOOK "Rollup segment-layout cutover").
    #[arg(long, env = "ROLLUP_SEGMENT_LAYOUT", default_value = "payer", value_parser = ["payer", "physical"])]
    pub(crate) rollup_segment_layout: String,
    ```
  - `deterministic()` gets `rollup_segment_layout: "payer".into(),` (`:616`).
  - `pub(crate) fn physical_segment_layout(&self) -> bool { self.rollup_segment_layout == "physical" }` after `runs_rollup` (`:671`).
- **`src/config/tests.rs` (720 → ~735).** The surface row after `:468` and the new test (§3.3).
- **`src/rollup/layout.rs` (~120 → ~330).**
  - `#[derive(Clone, Copy)] pub(super) struct PageBound { rows: usize, bytes: usize }` and `pub(super) const PAGE: PageBound = PageBound { rows: 1000, bytes: 1_000_000 };`.
  - `pub(super) async fn complete(v3: &Db, payer: impl Future<Output = anyhow::Result<Db>>) -> anyhow::Result<()>`:
    1. return early on `Done`;
    2. await `payer`, which fences v2;
    3. log;
    4. `copy(v3, &v2, usize::MAX, PAGE)`;
    5. `v2.close()` (on both paths; the copy error wins, and a close error is returned when the copy succeeded);
    6. `ensure!` Done;
    7. log the tally and `elapsed_ms`.
  - `pub(super) async fn copy(v3: &Db, v2: &Db, batches: usize, page: PageBound) -> anyhow::Result<Migration>`. It loops `step`, counts `Copying→Wiping` transitions, and fails on the second with `the payer rollup changed while this copy held it`.
  - `async fn step(v3, v2, page) -> anyhow::Result<Migration>`: basis first (including the close-cursor refusal), then the match on the marker (`None` / `Copying` with a stale basis / `Copying` / `Wiping` / `Done`, all explicit), then one `v3.write(batch)` with the marker.
  - `async fn copy_page(...)`, `async fn wipe_page(...)` (cursor-driven, deleting non-marker keys after `after`), and `async fn read_rows(db, after, page)`. `read_rows` loops `while rows.len() < page.rows && bytes < page.bytes`, with `bytes += key.len() + value.len()`.
  - `fn newest(current: Option<SegmentState>, candidate: SegmentState) -> SegmentState`, matching on `Some(held) if (held.usage_version, held.storage_accounted_through_ms) >= (candidate.usage_version, candidate.storage_accounted_through_ms) => held` and `Some(_) | None => candidate`.
  - `impl Basis { async fn read(v2: &Db) -> anyhow::Result<Self> }` and `async fn ensure_empty(v3: &Db)`.
  - `#[cfg(test)] mod copy_tests;`. Every function is ≤ 60 lines, nesting ≤ 3, and there is no unwrap, expect, panic, `as`, spawn, env read, `static` or `json!`.
- **`src/rollup.rs` (~858 → ~885).**
  - Header comment `:28-30`: v2 is the payer layout and v3 the physical one, selected by `ROLLUP_SEGMENT_LAYOUT`.
  - Add `pub(crate) const ROLLUP_PATH_PHYSICAL: &str = "telemetry/usage-rollup/v3/p0";`.
  - `fn rollup_path(prefix: &str, path: &str) -> String`, lifted from `:498-502`.
  - `async fn open_db(store, path: String, settings, cache) -> anyhow::Result<Db>`, lifted from `:505-512`.
  - `open_with_cache` becomes:
    ```rust
    let physical = cfg.cli.physical_segment_layout();
    let settings = crate::billing::telemetry_settings(&cfg.billing, &cfg.engine.compactor_options());
    let path = if physical { ROLLUP_PATH_PHYSICAL } else { ROLLUP_PATH };
    let db = open_db(store.clone(), rollup_path(prefix, path), settings.clone(), cache.clone()).await?;
    if physical {
        let payer = open_db(store, rollup_path(prefix, ROLLUP_PATH), settings, cache);
        if let Err(error) = layout::complete(&db, payer).await {
            if let Err(close) = db.close().await {
                tracing::warn!(%close, "rollup v3 close after a failed segment copy");
            }
            return Err(error);
        }
    }
    ```
    The function is about 32 lines with no `#[expect]`.
  - `readiness.rs` gains `segment_layout: Option<&'static str>` (`self.segment_layout().await.ok()`).
- **`src/rollup/readiness.rs` (~95 → ~98).** The field above.
- **`src/rollup/layout/copy_tests.rs` (NEW, ~420).** §3.3.
- **`src/dst/tests/billing_readiness.rs` (~140 → ~175).** The segment-layout test, plus the C1 key set gaining `segmentLayout`.
- **`scripts/quality/mutation_owners.py`** (only if D7 is accepted): one row, `owner('rollup_layout', 'src/rollup/layout.rs', 'rollup::'),` after `rollup_storage` (`:63`).
- **Docs.**
  - `docs/OBSERVABILITY-BILLING.md`: §9.1 `:503` lists v2 and v3; §9.2 `:512` adds the v3 key line `segment/<project>/<stream-id>/<segment-id> → latest absolute segment state (payer attribute)` and the marker line.
  - `docs/STAGING.md:131`: a comment line for `ROLLUP_SEGMENT_LAYOUT` (payer until the cutover).
  - `docs/refactor/WIRE-MATRIX.md:204`: `segmentLayout`.

### C4: "A rehearsal harness proves the segment-layout cutover on a copy of a real rollup"

- **`src/rollup/rehearsal.rs` (NEW, ≤ 900; split `rehearsal/fixture.rs` if it passes 900).**
  - `DbCopy { objects }` with `load(dir)`, `from_store(store)` and `store()`.
  - `Census` and `ClassCensus`, derived `Serialize`, using `sha2` (already a dependency).
  - `fold` (independent), `phantom`, `replay_page`, and the trials of §3.4.
  - The `Report` struct (Serialize, counts only).
  - The two tests.
- **`src/rollup.rs` (~885 → ~887).** `#[cfg(test)] mod rehearsal;` next to `mod close_seek_tests;` (`:836-837`).
- **`RUNBOOK.md`.** A new section "Rollup segment-layout cutover (bug 7)":
  1. Preconditions:
     - the rehearsal record is accepted;
     - the new binary is deployed under `payer`;
     - `rollup.segmentLayout == "payer"`;
     - `oldestUnclosedMonth` is the previous month, and it is not within `MONTH_CLOSE_GRACE_MS` after a month boundary;
     - the `_usage` retention covers the planned rollback window.
  2. Set `ROLLUP_SEGMENT_LAYOUT=physical` on the `ROLLUP=1` instance only.
  3. Expected log lines, with the tally equal to the rehearsal's.
  4. Checks after: `segmentLayout == "physical"` and `lastApplyAgeSecs` recovers.
  5. Abort, rollback and discard, from the §2.1 table.
  6. Never roll back across a month boundary.
- **`docs/reviews/2026-09-hardening/README.md`.** An acceptance-record stub: "Bug #7 cutover rehearsal: pending owner-supplied copy (D8)".

---

## 5. Mutation analysis

**Planner selection over C1..C4, measured against `origin/slate`:**
- `src/http.rs` is under `BUFFER_PREFIXES` and is the registered owner `http`, so `miri: true`, `mutants: true` and `selected_mutation_owners ⊇ ["http"]`.
- `src/rollup/{layout,readiness,rehearsal}.rs`, `page.rs`, `close.rs`, `rollup.rs`, `src/config/*` and `src/dst/tests/*` match no critical prefix. Within `src/rollup`, only `allocation` and `storage` are critical (`verification_plan.py:22-24`), and none of these files is a registered owner unless D7 is accepted.
- With D7, `rollup_layout` is also selected, and the `scripts/quality/` edit sets `properties_fuzz` and `miri` true (tooling).

**`http` (C1 diff inside `billing_readiness_axum`).** The in-diff mutant is `replace billing_readiness_axum -> Response with Default::default()`. It is killed by every `dst_tests::billing_readiness::` test, whose `serde_json::from_slice(&body).unwrap()` fails on an empty body. The changed line has no binary operator. Expected: 1 caught, 0 missed. C3 does not touch `http.rs`.

**`rollup_layout` (if D7 is accepted; filter `rollup::`).** Each viable mutant is listed with the test that kills it.

| Mutant | Killed by |
|---|---|
| `of` → `Ok(Default)` | unviable (no `Default`) |
| `as_str` → `""`, `"xyzzy"` | `copy_tests::a_physical_open_*` (a), `the_default_layout_*` |
| `key` → `vec![]`, `vec![0]`, `vec![1]` | `restamped_*` |
| `stream_prefix` → `vec![]` | `restamped_*` (a scan of everything fails to decode month rows as `SegmentState`) |
| `pays` → `true` / `false` | `restamped_*` (acct empty / acct2 one state) |
| `parse` → `Ok(Default::default())` | `restamped_*` (identity with empty project) |
| `parse`: `==`→`!=` (part counts, `"segment"`), `&&`→`\|\|` | `a_payer_keyed_state_blocks_*` (numeric `parts[3]` on purpose), `restamped_*` |
| `physical_key` → `vec![]` etc. | `a_physical_open_*` (c) |
| `newest` → `Default::default()` | `the_fold_*` |
| `newest`: `>=` → `<`, `>`, `==`, `<=` | `the_fold_*`: s2 kills `<`, `<=`, `==`; s3 (tie) kills `>` |
| `Basis::read` → `Ok(Default)` | `a_copy_restarts_*` (stale pending row survives) |
| basis `!=` → `==` | the plain copy wipes twice and hits the second-wipe guard, so `a_physical_open_*` fails fast; **no TIMEOUT** |
| `read_rows` → `Ok(vec![])` | `a_physical_open_*` (b) |
| `read_rows` `<` (rows) → `<=`, `==`, `>`; `&&` → `\|\|`; `<` (bytes) → `<=`, `==`, `>` | `one_copy_batch_*` |
| `+=` → `-=`, `*=` and `+` → `-`, `*` on `bytes` | `one_copy_batch_*` (bytes test: 10-byte rows, bound 20) |
| `tally.*` `+= 1` → `-= 1` (underflow panic in debug), `*= 1` | exact tally in `a_physical_open_*` (d) and `the_fold_*` |
| wipe-count guard `>` → `>=`, `<` | `a_copy_restarts_*` (legit single wipe) and `a_physical_open_*` |
| `complete` → `Ok(())` | `a_physical_open_*` |
| `ensure_empty` → `Ok(())` | `a_physical_rollup_holding_rows_*` |

**Why nothing times out.** The loops cannot run forever: copy and wipe are cursor-driven, `after` strictly advances, and the second-wipe guard ends the only cycle a mutant could create. `step`, `copy`, `copy_page` and `wipe_page` return `Migration` or a `(Migration, WriteBatch)` tuple, which has no `Default`, so their replacements are unviable.

**Equivalence traps avoided.** The row bound is written as `while rows.len() < page.rows …`, not `if rows.len() >= page.rows { break }`, because `>=`→`==` is equivalent under +1 steps. The bytes bound is tested with sizes that never hit it exactly.

**Stop conditions.** Any MISSED or TIMEOUT in `rollup_layout` blocks the push. The fix is a test or a restructure, never a new exception. Record the counts in `scratchpad/mutants-bug7.log`.

**Unregistered changes without a mutation gate** (`page.rs`, `close.rs`, `rollup.rs`, `readiness.rs`) are covered by the K-controls in §3.5 and by the rehearsal. The release-wide mutation run (review work package 4) should include `src/rollup/**`.

---

## 6. Ledgers (in the same commit as the change)

**C1**
- `docs/refactor/test-inventory.json`: `python3 scripts/test-inventory.py --write` gives +1 entry, `the_rollup_report_splits_pending_artifacts_by_publishability`.
- `docs/refactor/WIRE-MATRIX.md:204` and `RUNBOOK.md:702`.
- No `owners.json` or `source-allowances.json` rows:
  - no new `json!`;
  - `billing_readiness_axum` keeps 3 invocations;
  - `readiness.rs` has no macro-dsl or effect.
- No `exception-growth.json` row. No mechanism re-pin; the r14 pins are byte-unchanged.

**C2**
- No inventory change. `rollup::` unit tests are not inventoried; only `src/dst/tests/**` is.
- `docs/MULTITENANCY.md`.
- No re-pin: `review-mechanisms.json:132-139` shas `a8ceb2…` and `9cc3ac…` still match.

**C3**
- `config/tests.rs` surface row (the pinned CLI surface).
- `test-inventory.json` (`--write`): the new DST test, and the C1 test's `function_sha256` changes (its key set gains `segmentLayout`).
- `docs/OBSERVABILITY-BILLING.md`, `docs/STAGING.md`, `WIRE-MATRIX.md`.
- The `mutation_owners.py` row, if D7.
- **Edge record:** append records E2-E6 to `docs/reviews/2026-09-hardening/edge-changes.md`, or open a release-safety edge log if the owner prefers (D12). E1 goes with C1 in the same way.

**C4**
- `RUNBOOK.md` cutover section.
- The acceptance-record stub.
- No inventory change (unit tests).

**All commits**
- Every file touched stays within its ceiling:
  - `http.rs` 3153 → ~3131;
  - `rollup.rs` ≤ ~890;
  - new files ≤ 1,000 each;
  - `config/tests.rs` ~735;
  - `billing_readiness.rs` ~175.
- The DST file stays ≤ 1,000.

---

## 7. Controls (exact commands and expected outputs)

Use Python 3.11 or newer. Build the scanner first: `cargo build --locked -p streams-quality-syntax`.

1. **C1 red:** `cargo test --locked --lib -- --exact dst::dst_tests::billing_readiness::the_rollup_report_splits_pending_artifacts_by_publishability`
   - Expected: `1 failed`, with the §3.1 `Null` vs `Number(2)` message.
   - Save as `scratchpad/bug7-c1-red.log`.
2. **C1 green, floored:** `scripts/test-leg.sh target/quality/bug7-c1.log --exact dst::dst_tests::billing_readiness::the_rollup_report_splits_pending_artifacts_by_publishability --exact rollup::accounting_failure_tests::the_outbox_count_stops_where_the_publisher_scan_stops -- --locked --lib billing_readiness accounting_failure_tests rollup::tests`
   - Expected: every result `ok`, including `an_undecodable_pending_artifact_is_logged_and_stays_pending` and both `r14_*`.
   - Then `cargo test --locked --lib dst::dst_tests::billing_controller::`: every result `ok`.
3. **C2 red** (tests-only `layout.rs`): `cargo test --locked --lib rollup::layout::tests`
   - Expected: **5 failed, 1 passed**, where the pass is `the_payer_layout_still_carries_a_restamped_orphan`.
   - The failure messages are those of §3.2.
4. **C2 green:** `cargo test --locked --lib rollup::`
   - Expected: every result `ok`.
   - Then `git diff --stat src/rollup/accounting_failure_tests.rs src/rollup/close_seek_tests.rs` must print nothing.
5. **C3 red, stage 1:** `cargo test --locked --lib rollup::layout::copy_tests config::tests::the_rollup_segment_layout_accepts_only_payer_or_physical`
   - Expected: every open-level test except `the_default_layout_keeps_the_payer_rollup` fails with its §3.3 message.
   - The config test fails with `a misspelled layout must refuse to parse`.
6. **C3 green:** `cargo test --locked --lib rollup:: config:: dst::dst_tests::billing_readiness:: dst::dst_tests::billing_controller:: dst::dst_tests::billing_usage::`
   - Expected: every result `ok`.
7. **C4:** `scripts/test-leg.sh target/quality/bug7-c4.log --exact rollup::rehearsal::a_synthetic_payer_rollup_rehearses_the_cutover_exactly -- --locked --release --lib rollup::rehearsal::`
   - Expected: `1 passed; 0 failed; 1 ignored`.
   - The printed report shows the §3.4 synthetic numbers:
     - `legacy_states: 7`, `physical_states: 5`, `retired_orphans: 2`, `steps: 12`, `replayed_snapshots: 8`, `payer_closed: 6`, `physical_closed: 4`;
     - phantoms `267840000000` and `187488000000`.
8. **Controls K1-K8** (§3.5): each gives its named failure. Revert each after recording `scratchpad/bug7-K<n>.log`.
9. **Size and scope, after `cargo fmt --all`:**
   - `wc -l src/http.rs src/billing.rs src/rollup.rs src/rollup/*.rs src/rollup/layout/*.rs src/config/cli.rs src/config/tests.rs src/dst/tests/billing_readiness.rs`.
   - Expected: `http.rs ≤ 3153`, `billing.rs = 2151`, and every other file ≤ 1000.
   - `git diff origin/slate --stat -- src/billing.rs src/bootstrap.rs src/shard/` prints nothing.
10. **Quality on each commit:** `scripts/quality.sh`. Expected:
    - final line `QUALITY_OK`;
    - `test-inventory: OK`;
    - `architecture-gate: OK`;
    - no `file growth`;
    - no `accepted exception grew without an approved growth row`;
    - no `unregistered source occurrence`;
    - clippy and rustdoc `-D warnings` clean;
    - `mt_lint::multitenancy_identity_lint` ok.
11. **Gate:** `OUT=$PWD/target/gate-bug7.txt scripts/gate.sh`. The last line of `$OUT` is `GATEDONE`. The file is `$OUT`, not `$1`.
12. **CI's selection and mutation run, before the push:**
    - `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan`
    - Expected `plan.json`: `mutation_source_files` = `["src/http.rs"]`, plus `"src/rollup/layout.rs"` with D7; `unregistered_mutation_source_files: []`; `mutants: true`; `miri: true`.
    - Then `scripts/quality/mutations.sh` with the same environment: zero `MISSED`, zero `TIMEOUT`, and the §5 unviable list. Save as `scratchpad/mutants-bug7.log`.
13. **After the push:** `gh run list --branch slate --limit 5 --json databaseId,headSha,status,conclusion`, match the SHA, then `gh run view <id>`. Never claim CI is green from memory.
14. **Acceptance (owner, D8/D9):** the ignored real-copy test on the supplied copy. Its expected output is `1 passed`, and `target/rollup-rehearsal/report.json` shows every check `ok: true`. Only after that is the cutover (step 2 of the RUNBOOK section) scheduled.

---

## 8. Out of scope

- **The emitter ordering** in `bill_append`. It is deliberate, and `bill_append` sits under an `unwrap_used` exception (`append.rs:257-260`).
- **Reversing phantom rows and artifacts already finalized for old payers** before the cutover. The rehearsal reports `retired_orphans` and their gauges, and the owner decides on `UsageCorrection` records (D5). v3 copies the finalized rows verbatim.
- **Zero-gauge states of deleted streams**, which still make the carry synthesize an empty month row and artifact every month (`close.rs:296-318`). This has the same "never retired" shape and needs its own red test.
- **Deleting the payer layout code and v2 objects** and flipping the default to `physical`. Later commits, after acceptance and one clean month close on v3 in every cell (D4, D10).
- **Dropping the account parameter** of `stream_segment_states` (MULTITENANCY-MAP.md:388 product item).
- **Mid-month transfer split**, and boundaries crossed through `billing_close` (no re-stamp). These belong to MULTITENANCY.md Stage 7 platform work.
- **Ledger-replay equivalence.** Rebuilding the rollup from the `_usage` ledger needs a copy of the shard DB and the usage key. The harness proves idempotent replay of the materialized state instead.
- **A live copy without stopping the writer** (SlateDB checkpoint and clone tooling).
- **Publication throughput.** Observed while verifying: `publish_artifacts` takes at most 64 monthly and 64 correction artifacts per hourly tick (`billing.rs:1301,1326`), so a large cell's outbox drains slowly. The new counts will make this visible. It is not fixed here.
- **The correction outbox** (`pending_correction_artifacts`) has its own silent skip of keys that do not split into 6 parts (`rollup.rs:664`). It is not part of the approved split.
- **The effective-config comparison tool** (review work package 3) must include the new knob. That tool is separate work.

---

## 9. Decisions for the owner

- **D1 (activation point).** A new clap knob, `ROLLUP_SEGMENT_LAYOUT=payer|physical`, defaulting to `payer`, with strict parsing. **Recommend** as specified. The alternatives, an operator HTTP route or an offline subcommand, need growth of `http.rs` or `main.rs` and the lib facade.
- **D2 (namespace).** A separate v3 DB path with a copy, rather than a new prefix in the same DB. **Recommend v3.** A rolled-back rc.4 silently skips malformed keys (§1.3), so only a separate DB isolates old binaries. Each DB stays consistent with the ledger, so rollback and abort need no reconcile pass. The cost is a one-time copy (duration measured by the rehearsal) and duplicate storage until v2 is deleted.
- **D3 (required-mode readiness during the copy).** The rollup owner does not bind its listener until the copy is done.
  - The rehearsal's `copy_ms` on the real copy tells whether that is seconds or minutes.
  - `ops/raw` (7 days of 15-second points per instance) may dominate the bytes. The report gives bytes per class, and the owner may choose to leave `ops/raw` out of the copy, a one-line change plus an exclusion in check 1.
  - **Recommend** accepting it for a scheduled cutover. The alternative, serving without the rollup, relaxes item 10's readiness contract.
- **D4 (rollback policy after `done`).** Rollback is supported through `payer`, as in §2.1, but never across a month close. Artifact conflicts then fail closed with `ARTIFACT_MISMATCHES`. Owner decisions:
  - how long to keep v2 (recommend at least one clean month close on v3);
  - the `_usage` retention needed for catch-up;
  - when the payer code path is deleted.
- **D5 (existing damage).** Orphans that already produced finalized phantom rows or published artifacts before the cutover. Decide from the rehearsal's counts whether to issue `UsageCorrection` records.
- **D6 (operator surface shape).**
  - The fields: flat siblings `pendingArtifactsBlockedCorrupt` and `pendingArtifactsTotal`, with `pendingArtifacts` unchanged (as planned), or one nested object.
  - Whether a scan failure should report `null` instead of today's `0`. This would be an edge change; the default keeps `0`.
  - Whether to add a `…Truncated` flag at the 1,000-row cap.
- **D7 (mutation owner).** Register `src/rollup/layout.rs` as `rollup_layout` with filter `rollup::`. **Recommend yes.** It gates the copy code in CI, and §5 lists how every viable mutant dies.
- **D8 (the real copy).** Which cell and bucket, the stop-copy procedure (§3.4), who runs the rehearsal, and where the report is kept (aggregates only). Nothing can be accepted without this.
- **D9 (cutover scheduling).**
  - Per cell, not within `MONTH_CLOSE_GRACE_MS` after a month boundary.
  - At low traffic (the instance does not serve during the copy in required mode).
  - After the new binary has run under `payer`.
  - A refused copy (close in flight) fails boot in required mode, and the remedy is `payer` for at least one hourly tick.
- **D10 (fresh cells and the default).** Under the default `payer`, new cells start on v2 and need their own cutover. **Recommend** flipping the default to `physical` in a follow-up commit once the rehearsal is accepted; a fresh cell then copies an empty v2.
- **D11 (knob naming).** `ROLLUP_SEGMENT_LAYOUT` with values `payer` and `physical` are the proposed names. Changing them later is a pinned CLI-surface change.
- **D12 (edge ledger location).** Whether E1-E6 are appended to `docs/reviews/2026-09-hardening/edge-changes.md` or start a release-safety edge log.
- **D13 (release provenance).** The next release record's `rollup_schema` (RELEASE-PRODUCT-SURFACE.md:1135 style) should read "v2 (payer) or v3 (physical) per ROLLUP_SEGMENT_LAYOUT".

---

## Skeptic corrections (C1..C17)

Verified read-only against `46f668b3`: every `wc -l` figure in §1.4 and §4, the quoted code in §1.1 (append.rs:283-290, billing.rs:304, rollup.rs:40-42 and 579, page.rs:325-330, 351-354 and 384-393, close.rs:149, 266-322 and 245-253), the rollup delete sites (close.rs:108-109, rollup.rs:636/690/822), the rc.4 carry (`git show 685ea035:src/rollup.rs`: `parts.len() != 5 { continue; }` and `parts[4].parse().unwrap_or(0)` at 1363-1367), bootstrap.rs:684-694, product/usage.rs:45-46 and 101-103, the cli.rs anchors (414-416, `deterministic()` 551, `runs_rollup` 669-671), the config surface row (tests.rs:468), the r14 pins (review-mechanisms.json:132-139), and the `billing_readiness_axum` json! allowance (count 3, source-allowances.json:2783-2788). Slatedb `Db::write` returns before durability (checkout 0717cc1, db.rs:1663-1667). I checked the synthetic numbers by hand: 38 rows, 31 non-segment, 7→5 states, 170 orphan bytes, 12 steps, 8 replays, 6 vs 4 closes, and the phantoms 267,840,000,000 and 187,488,000,000 all follow from the fixture. The C3 fixture's 14 rows and 6 steps and the §3.2 figures (146.88e9, 1,045.44e9, 1,036.8e9) also check out.

**C1: A test file that has to change is listed as unchanged (the build breaks).** `src/rollup/close/tests.rs:33-39` builds `MonthClose { rollup, month, start, boundary, now }` as a struct literal. Adding `layout: SegmentLayout` to `MonthClose` (§4 C2) is a compile error there.
- **Fix:** add `layout: crate::rollup::layout::SegmentLayout::Payer,` (+1 line; the file is not pinned or inventoried). Move `close/tests.rs` from §1.4 "Tests that name the keyspace, all unchanged" into the C2 edit list. `SegmentLayout` is `pub(super)` in `rollup::layout`, so `rollup::close::tests`, as a descendant of `rollup`, can name it.

**C2: The http.rs line numbers are wrong, and the edit as written would delete the auth check.** `billing_readiness_axum` *starts* at http.rs:1719. The rollup block is `:1736` (`let mut rollup_info = json!({ "running": false })`) through `:1762` (the closing `}`): `let pending` is at `:1738`, `rollup_info = serde_json::json!({` at `:1754`, and `"pendingArtifacts"` at `:1759`.
- §4 C1 says "Replace `:1718-1743`". That range covers the doc comment, the signature and the `authorized()` 401 guard.
- **Fix:** replace `:1737-1762`, the `if let Some(r) = state.rollup.get() { … }` block, and keep `:1736` unchanged. That keeps the json! count at 3. Fix §1.4 (`:1738-1742`, `:1759`) and §4 C1's "body is `http.rs:1719-1741`" (really `:1738-1761`) to match.

**C3: The rehearsal's byte comparisons are nondeterministic, and they expose an existing publication defect.**
- `MonthRow.segments` is a `std::collections::HashMap<u32, SegMonth>` (rollup.rs:112-113, RandomState). `publish_artifacts` publishes a **re-serialization** of the decoded row, `let body = serde_json::to_vec(&row)` (billing.rs:1313), not the stored bytes. So any row with 2 or more segments serializes in a random key order every time it is decoded and re-encoded.
- **Broken checks:**
  - §3.4 check 5 ("(path, bytes) sets must be equal"): the synthetic July `U@acct3` artifact has 2 segments, so the check fails about half the time.
  - Check 6 ("every month row not involving an orphan is **byte-identical**"): `U`'s August row is re-finalized separately in v2 and v3.
  - "The artifact sets differ by exactly those rows."
  - E5's "byte-identical".
- Verbatim-copied rows are fine: check 1 and copy test (b).
- **Fix:** compare `serde_json::from_slice::<serde_json::Value>` values. Without `preserve_order`, `Value` maps are BTreeMaps and therefore canonical. Alternatively compare decoded rows with sorted segments. Keep sha256 digests only for rows copied verbatim.
- **Existing defect, to go to the owner, not fixed here:**
  - A publish retry after a crash between the PUT and `mark_artifact_published` compares a *fresh* re-serialization with the stored object (billing.rs:1270-1273).
  - For a multi-segment row, that raises a false `ARTIFACT_MISMATCHES` about half the time and leaves the row pending.
  - The comment at close.rs:245-246 ("identical bytes") does not hold on the publish side.
  - Add this as D15. The rehearsal must not report it as a migration difference.

**C4: The single-kill trials do not simulate a kill, and they may never test resume.**
- The rollup settings run `wal_enabled: false` (billing.rs:1510), `l0_sst_size_bytes: 2 MiB` (:1516) and `max_unflushed_bytes: 8 MiB` (:1515). A write is durable only once its memtable is flushed. A copy of a few KB that is dropped without `flush()` usually leaves v3 **empty** on reopen. The resume then takes the *start* step, so the "for every k in 0..=5" trials, and the rehearsal's `min(steps−1, 64)` kills, pass without ever resuming from `copying{after}`.
- Slatedb has no `impl Drop for Db` (checkout 0717cc1). A dropped handle's background flush, compactor and GC tasks keep running against the same store the resumed writer uses. That is not a kill, the durable prefix is nondeterministic, and the tasks pile up (thousands in the rehearsal's every-step trial).
- **Fix: deterministic kills.**
  1. Run k steps, then `flush()`.
  2. Run j more steps without a flush.
  3. Snapshot the object store by copying its objects into a fresh `InMemory` (`DbCopy::from_store` already exists in C4).
  4. `close()` the abandoned handles on the old store.
  5. Resume on the snapshot.
  6. **Assert the resumed marker equals the marker flushed at k** before resuming, which proves a resume and not a restart. Then assert the final snapshot equals the reference.
- **Also cover the abort path.** Nothing kills inside `wiping` or at the `wiping→copying` transition today. Add one every-step trial (step, flush, snapshot, reopen) over the abort path. The every-step trial should `close()` each handle, not drop it.

**C5: "v2 is never written" and "fences any payer writer" are overstated.**
- `open_db` for v2 uses `telemetry_settings`, which includes `compactor_options: Some(co)` (billing.rs:1507-1519) and a GC (:1485-1498). Opening v2 as a writer therefore writes a new manifest epoch and runs an embedded compactor and GC that can rewrite or delete v2 objects during the copy.
- The copier never writes to v2, so it never learns that a later opener re-fenced it: a second `ROLLUP=1` instance, or an old binary restarted after its fenced task failed. Its per-step basis re-read comes from the stale fenced handle.
- **Fix:**
  - Open v2 for the copy with `compactor_options: None` and no GC (a local settings override in `layout::complete`).
  - Before writing `done`, re-read the basis through a *fresh* read-only handle on v2 and require it to equal the marker's basis.
  - Reword the §2.1 invariant to "v2's rows are never written; opening it bumps its writer epoch".
  - Add the runbook precondition "exactly one `ROLLUP=1` instance in the cell, and the rollout is complete".

**C6: The basis does not detect every change to v2.**
- `mark_artifact_published` and `mark_correction_published` (rollup.rs:625-705) and `sweep_ops_raw` (:805-830) change v2 without moving `meta/usage-cursor`, `meta/ops-cursor` or `meta/oldest-unclosed-month`. A payer run between two activation attempts that only publishes or sweeps goes unseen. v3 then brings back already-published `artifact-pending/…` rows, which publish again and hit the C3 false mismatch, and it brings back swept `ops/raw` rows.
- In practice the ops cursor moves every tick. But §2.1 step 4 ("the wipe is what removes … a published `artifact-pending` row") depends on that, and K5 / `a_copy_restarts_*` always pairs the publication with a cursor move (page `c3`), so it cannot catch this.
- **Fix, one of:**
  - (a) add to `Basis` the count and last key of `artifact-pending/`, `corr-pending/` and `ops/raw/`, all bounded by the outbox or retention;
  - (b) run one verification pass comparing v3's non-segment rows to v2's before `done`, at the cost of one more full read.
- Add a publication-only variant of `a_copy_restarts_*`.

**C7: The phase markers are not durable when their phase begins.** Nothing in `complete` flushes.
- The rollup loop runs `close_months_due` and `publish_artifacts` on its **first** iteration, because `last_close` is `None` (billing.rs:1427-1460). A kill after "physical layout active" can therefore lose `done`, the `artifact-done` rows and the pending retirements, then re-copy and publish again.
- **Fix:** `v3.flush()` after the start step, after each `copying→wiping` transition, and after `done`, before `complete` returns. Optionally also flush every 8 MiB to bound the re-copy after a platform kill. The owner condition is "durable phases"; the current wording, "loses at most the unflushed tail", describes something weaker.

**C8: Mutant gap in the payer parse (applies if D7 is accepted).**
- The payer arm moves into `layout.rs`, a new file, so all of it is in the diff. Its `&&`→`||` mutants on the `ensure!` (close.rs:268-271 moved) only show up with a payer-layout key of other than 5 parts, where the mutant then indexes `parts[4]` and panics.
- No payer-layout test plants one: r14's key has 5 parts (`not-a-number`), and `close/tests.rs` only calls `read_page`. The result is MISSED, which fails CI.
- **Fix:** add `a_malformed_payer_key_blocks_the_carry` to `layout/tests.rs`: no marker, `segment/a/p/s` with payer `a`, `close_month(2026,7,0)` errs with exactly `invalid segment accounting key`, snapshot unchanged. Green today, so it is a pin.
- Also specify `SegmentLayout::pays` for `Payer` = `true`, so the default path stays identical. Its `true→false` mutant is killed by rollup/tests.rs:216 and :457, which run under the `rollup::` filter.
- Measure the wall time of `cargo test --profile quality --lib rollup::` (it now includes the synthetic rehearsal and the copy tests) against the driver's 90 s per-mutant `--timeout` (mutation_driver.py:52-54), and record it in the plan.

**C9: The owner condition "no startup rewrite".** The copy runs inside `open_with_cache` at boot. In required mode that happens before the listener binds (bootstrap.rs:684-694).
- It is explicit (a knob), not silent, and it writes a new namespace. But it is literally a startup rewrite.
- Add **D14**: the owner confirms that a knob-gated boot copy satisfies the condition, or picks the offline alternative that D1 rejected.
- D3 must also compare the measured copy time with the platform's start and health deadline. Resumability guarantees forward progress only if each attempt flushes (C7).

**C10: The rehearsal's cost figures are not representative, and its run time is not bounded.**
- `copy_ms` is measured on `InMemory`. Production writes 2 MiB L0 SSTs to Tigris, with compaction running and the 8 MiB unflushed back-pressure. D3's "seconds or minutes" cannot rest on that number.
- **Fix:** also time one reference copy on a `LocalFileSystem`-backed store, or on a staging bucket. Report bytes read and written, the SST count and peak RSS. Add v3's total bytes, the duplicate storage until v2 is deleted, to the report.
- **State the budget.** Each single kill costs about 2 full copies, up to 64 of them. The every-step trial costs about (v2 bytes / 1 MB) opens. Memory is about 2-3× v2. Cap the every-step trial (for example the first and last 200 steps plus the abort path) so the real rehearsal ends in bounded time.

**C11: The C1 red cannot be observed as written.** `the_outbox_count_stops_where_the_publisher_scan_stops` (compile error `E0599`) and the DST test share the lib test binary. With both staged, §7 step 1 gets a compile failure, not the `Null` vs `Number(2)` red.
- **Fix:** stage the DST test alone for step 1, then add the outbox test and observe its `E0599`.

**C12: The red messages depend on assertion order.** In the §3.2 table, `close_month(2026,8,0) == 1` comes first, but today's value is 2. An unlabelled `assert_eq!` would print `left: 2 right: 1`, not `the previous payer must not be carried into a month it never owned`.
- **Fix:** either put the `month_row("2026-08","acct",..).is_none()` assertion, with that message, first, or give the count assertion the message.
- The same applies to `a_physical_open_copies_*`: (a) must be the first assertion.
- The fixtures need two successive `ClockGuard`s (August close, then September close). Drop the first before taking the second, because it holds a write lock (rollup/tests.rs:16-17).

**C13: Factual fixes.**
- `Cargo.lock` **does** contain `indexmap` (line 1504); serde_json 1.0.150 just does not depend on it (Cargo.lock:2768-2778). The key-order conclusion still holds.
- MULTITENANCY.md's "split-safe" sentence is at :1438, not :1434-1435.
- `docs/reviews/2026-09-hardening/edge-changes.md` is already 1,049 lines. Docs are outside the Rust growth ceiling (source_rules.py:431-442 applies to sources), but D12 should note its size.

**C14: Edge approvals.** Only E1 (the reviewer's split) and E5 (the fix) are owner-approved.
- E2 (the knob) is covered by D1 and D11, and E4 by D3.
- **E3 (`rollup.segmentLayout`) has no decision entry.** Add it to D6, or as a new D item, before C3 lands.

**C15: Wording fixes in the rehearsal checks.**
- **Check 4's control, "one non-final current-month snapshot":** under `ClockGuard` at Sep 2 the synthetic DB has no current-month (September) rows. It means "a non-finalized month row" (August).
- **What the control really changes:**
  - It also rewrites that row's name and project aggregates with +0. Assert those are unchanged in content (C3).
  - The month row's `updated_ms` comes from `crate::shard::now_ms()` (page.rs:50), not from the ClockGuard.
- **Check 7's phantom:** the planted orphan takes the winner's clock, which on a real DB usually lies inside the last month the forward close covers. So phantom = gauge × (boundary − max(clock, start)), not "gauge × closed span".

**C16: An empty v2 is created silently.** Under `physical` with v3 not `done`, a missing v2 (fresh cell, wrong `PATH_PREFIX`, or v2 deleted by mistake) is created empty by the lazy open. The copy completes to `done` with no state, which drops every idle gauge if v2 was really lost.
- **Fix:** log `rows=0, legacy_states=0` at warn level, and make the runbook's "tally equals the rehearsal's" an explicit go/no-go check. D10's fresh-cell path accepts this deliberately.

**C17: Handling the real copy.** `target/rollup-rehearsal/source` will hold account ids, project ids and stream names (in month and artifact rows) on a workstation.
- D8 must set how the copy is stored and deleted.
- `ensure!` messages such as `fold mismatch at segment/proj/<V>/0` print identifiers. Keep failure logs out of the committed evidence, or print only hashed keys.

**What holds.**
- The design meets the reviewer's shape: a separate v3 path (the rc.4 skip-on-malformed carry makes an in-place re-key unsafe to roll back), an explicit knob defaulting to `payer`, and a marker in the same batch as its rows.
- The fold is correct for the append re-stamp. The final is snapshotted at version v and `usage_version += 1` follows (append.rs:285-296), so the new payer's version is strictly higher. A tie can only come from the `billing_close` or maintenance paths, which do not re-stamp.
- `UsageRollup` gets no new field, so the r14 and r23 pins stay byte-unchanged. The marker read uses keyed `read_faults`, so the r14 fault injection is unaffected.
- No `#[expect]` scope is edited, and no exception-growth row is needed.
- `rollup.rs` stays ≤ 1,000 lines (about 887); `http.rs` and `billing.rs` do not grow.
- The mutation analysis holds for the physical arm, `newest`, `read_rows` and the wipe guard.

**Verdict: ready-with-corrections.** C1, C2, C3 and C4 are blocking corrections within the plan: a compile break, a destructive line range, flaky acceptance checks, and kill trials that are vacuous or nondeterministic. C5, C6 and C7 are design tightenings needed to meet "durable phases" and "resumable after a kill at every phase" literally. C8 prevents a CI mutation failure if D7 is accepted. C9 and C14 are owner decisions to add before C3 lands.
