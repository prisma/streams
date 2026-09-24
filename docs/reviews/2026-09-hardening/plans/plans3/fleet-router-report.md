# Plan: one unreadable `routers/*.json` must defer only the desired publication

Repository `/Users/sorenschmidt/code/streams`, branch `slate` @ `82095942`. Read-only verification; nothing edited.

## 0. Verdict on the review claim

**Correct and current.** `src/fleet.rs:642-646` reads the router reports and, on `Err`, logs
"router snapshot deferred; scale decision deferred" and `continue`s the `'ticks` loop
(`src/fleet.rs:481`). That `continue` sits *before* the desired-state read (`:735`), the
ownership publication `state.ownership.set_view` / `state.peer.set_peers` (`:799-800`), the
parked-session wake (`:810-818`), the tick yield (`:828-845`), the eager open (`:847-884`),
return-home (`:886-971`), the rebalancer (`:973-1061`) and the desired CAS (`:1064-1124`).
None of those steps consumes the router reports; only the edge dimension does
(`edge_hot` `:692`, `need_edge` `:693`, `need` `:706-712`, `need_shrink` `:718-733`, and the
`reason` string `:1103`). So one corrupt/oversized/slow router document freezes ring
publication, peer table, yield and rebalance on every instance of the cell, every 2 s, for as
long as it persists — while the log claims only the scale decision was deferred.

## 1. Mechanism, with evidence

- `src/fleet/repository.rs:77-79` `read_router_reports` → `read_population("routers", false)`.
  `read_population` (`:85-155`) fails the WHOLE population on: one object over 128 KiB
  (`:118-121`), more than `MAX_MEMBERS` objects (`:105-108`), one GET error (`:127`), one body
  over budget (`:131-134`), one document that is not JSON (`:148`, `serde_json::from_slice(&raw)?`),
  or a 10 s deadline (`:152-154`). Fail-closed is right for a *population* read (the ring must
  never be published from an incomplete set) and must stay.
- `src/fleet.rs:598-602`: the heartbeat-set read is ALSO fail-closed with `continue`
  ("fleet snapshot deferred; ownership view retained") — correct, because `active`
  (`:755`, `planning::active_members(cur_count, instance, hb_age_ms)`) and `peer_urls`
  (`:595-619`) are derived from it. Likewise `read_desired_state` (`:735-742`, `cur_count` feeds
  `active`) and `read_overrides` (`:783-790`, the map half of the view). The router read is the
  one read in the tick whose failure gates nothing the view needs.
- What each downstream step needs (answering "does `set_view` depend on the router reports?"):
  - `set_view(active, map)` `:799`: `active` from desired count + heartbeat ages (`:755`);
    `map` from `ov.entries` (`:792-796`). **No router dependency.**
  - `set_peers(peer_urls)` `:800`: heartbeats `:610-618` + `fleet/urls.json` `:763-775`. **None.**
  - wake `:810-818`: the view. **None.** Yield `:828-845`: `effective_owner` (view). **None.**
    Eager open `:852-884`: `ov.entries`. **None.** Return-home `:891-971`: `ov`, `peer_load`,
    usage. **None.** Rebalancer `:973-1061`: `hb`, `peer_load`, usage. **None.**
  - Desired CAS `:1064-1124`: `need`/`need_shrink` → `edge_hot` → `edge_p50`. **The only consumer.**
- The router tier's reports are bucket-writable input (`src/fleet.rs:127-128` says so for URLs;
  `routers/*.json` is the same class). A partially written or malformed report from one router
  therefore freezes ownership convergence cell-wide until someone deletes the file.
- Existing proof that fail-closed reads are a deliberate contract for the *authority*
  documents: `src/dst/tests/fleet_controller.rs:159-164` ("unread overrides must not publish a
  new ring"). The change below preserves that and only re-scopes the router read.

## 2. Designs

### 2a. Smallest correct design (RECOMMENDED)

Keep the router read where it is (section 2b of the tick), stop `continue`-ing on `Err`, fold
the reports through a pure freshness predicate, and put an `is_err()` guard at the head of the
scale-decision chain so only the desired CAS is withheld. Concretely:

1. `src/fleet.rs:638-653` → read with `.inspect_err(warn)`, then
   `let edge_p50 = fresh_edge_p50(reports.as_deref().unwrap_or(&[]), now_ms());`
2. New private `fn fresh_edge_p50(reports: &[serde_json::Value], now_ms: i64) -> f64` next to
   the other pure helpers (after `pick_victim_shard`, `src/fleet.rs:211`). This is the
   "wall-clock boundary extracted into a predicate with explicit inputs" the mutation leg wants,
   and it removes 11 syntax facts from `start`.
3. `src/fleet.rs:1068`: `let (publish, publish_count) = if reports.is_err() { (false, need) } else if cur.is_none() { ...`.
   The head guard keeps `below_since` frozen during a router outage exactly as today (today the
   whole tick aborts before touching it), so scale-in sustain semantics are unchanged.
4. Re-decide the `clippy::unwrap_used` reason on `start` (the only ratchet contract this edit
   necessarily changes — see §4). All other five contracts on `start` shrink.

Why in-place rather than the item's "move the read + need/need_shrink to just before the
publication block":
- Moving `need`/`need_shrink` (`:706-733`) puts ~20 arithmetic mutants into the diff scope
  (`cargo mutants --in-diff` selects any mutant whose span touches an added line). Several are
  not killable deterministically: `total_cores_used / cfg.scale_in_util` (`:727`) and
  `total_cores_used / cfg.target_util` are dominated by the test process's own CPU (self
  `cpu_pct` is `getrusage` of the whole `cargo test` process), and every `need_shrink` mutant is
  only observable after `cfg.scale_in` = `scale_in_secs` = 60 s (`src/config/cli.rs:627`), which
  the rig cannot shorten (`fixture_config`, `src/dst/tests/fixture_http.rs:332-354`, takes
  `CliArgs::deterministic()` with no scaling knobs). The mutation driver fails on ANY missed
  mutant (`scripts/quality/mutation_driver.py:152`, `check=True`). So the move design is red in
  CI unless it also grows the rig fixture and adds a per-dimension scale table — out of scope.
- A "pure verbatim-move commit" does not escape this: an in-file move changes token order, so
  `production_unchanged_files` does not apply, and a push's diff (event `before` revision) still
  carries the moved arithmetic as added lines.
- `src/fleet.rs` is ceilinged at 1,143 lines; the in-place design is line-neutral (§4).

### 2b. Owner-first alternative (NOT now)

Give the scale decision its own owner: read the router reports inside a `scale_decision(...)`
step that runs strictly after ownership publication/yield/rebalance, taking a typed load
snapshot (`FleetLoad { live, total_rps, total_cores_used, total_inflight, max_loaded_p50,
max_loaded_cpu }`) plus `Option<f64>` edge p50, returning `Option<(count, reason)>`. That is the
structurally right shape (SCALING.md §4: "the scaler runs strictly after the rebalancer each
tick") and would let a table test kill every dimension's mutants without the rig. Prerequisites
it needs first: (i) `HttpRigOptions` (or the typed snapshot) must make `scale_in`,
`cpu_sustain`, `latency_sustain` test-settable; (ii) the self-CPU noise has to be removed from
the decision inputs under test (inject the load vector, don't measure it); (iii) a new file
(`src/fleet/scale.rs`) under the `src/fleet` critical prefix needs its own
`owner('fleet_scale', ...)` row, and registering it commits the WHOLE file to the seven-night
full-owner mutation rotation, so every branch in it must be assertable (no log-only
`if !x { warn }` branches — those are equivalent mutants and fail the nightly). Recommend
recording this as a follow-up and shipping 2a now.

### 2c. Rejected variants
- Keep the fold inline and add a flag (no helper): +5 syntax facts in `start` → all six
  `#[expect]` contracts on `start` grow → six reasons to re-decide for a bug fix. Worse than 2a.
- Put `fresh_edge_p50` in `src/fleet/planning.rs`: the file is under the `src/fleet` critical
  prefix and has NO `mutation_owners.py` row, so changing it fails the plan ("register every
  changed critical mutation owner", `mutation_owners.py:277-281`); registering it drags
  `active_members`/`trusted_urls` into the nightly rotation, and `trusted_urls` has a log-only
  `if !valid { warn }` (`planning.rs:31-34`) whose `!`-deletion mutant is equivalent → unkillable.
  Keeping the helper in `src/fleet.rs` (already owned) avoids all of that; it fits because
  the edit frees 9 lines.

## 3. Red tests

### 3a. DST scenario — fails on current code (the regression)

File: `src/dst/tests/fleet_controller.rs` (224 lines now; ~+95). Reuses `mem()`,
`http_rig_build`, `HttpRigOptions`, `engine_shutdown`, `RigRuntime::first()`. No `json!`,
`select!` or `spawn` (each would need a per-function source allowance). Every wait is bounded.

Add to the imports at the top of the file:

```rust
use crate::shard::now_ms;
```

Append:

```rust
/// Poll `ready` until it holds or `budget` elapses. Every wait in this
/// module is bounded so a regression fails by assertion, never by a hang.
async fn settled(budget: Duration, mut ready: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + budget;
    while !ready() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A non-draining heartbeat for `instance` with a trusted https origin and
/// `inflight` admitted requests, stamped 20 s ahead so it stays inside the
/// 10 s live window for the whole scenario however the ticks are scheduled.
async fn peer_heartbeat(store: &Arc<dyn ObjectStore>, instance: &str, inflight: i64) {
    let ts_ms = now_ms() + 20_000;
    let body = format!(
        r#"{{"instance":"{instance}","ts_ms":{ts_ms},"rps":0.0,"inflight":{inflight},"owned_shards":[],"draining":false,"url":"https://{instance}.invalid"}}"#
    );
    store
        .put(&Path::from(format!("fleet/{instance}.json")), PutPayload::from(body))
        .await
        .unwrap();
}

async fn desired_doc(state: &Arc<crate::http::AppState>) -> crate::fleet::Desired {
    state
        .fleet
        .read_desired_state()
        .await
        .unwrap()
        .0
        .expect("the seeded desired document is always present")
}

/// Router reports are a bucket-writable input that only the scale decision
/// consumes. One unreadable `routers/*.json` used to abandon the whole tick:
/// the ring and peer table stayed stale, a shard the ring had moved away was
/// never yielded, return-home never ran — cell-wide, for as long as the file
/// persisted. Only the desired CAS may wait on that read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreadable_router_report_defers_only_the_desired_publication() {
    let inner = mem();
    // Ring of two, shard 00 overridden to streams-2 long enough ago that
    // return-home may hand it back, and one router report that is not JSON.
    let aged = now_ms() - 301_000;
    for (path, body) in [
        ("fleet/desired.json", r#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#.to_string()),
        ("fleet/overrides.json", format!(r#"{{"entries":{{"00":{{"to":"streams-2","ms":{aged}}}}}}}"#)),
        ("routers/edge-1.json", "not json".to_string()),
    ] {
        inner.put(&Path::from(path), PutPayload::from(body)).await.unwrap();
    }
    peer_heartbeat(&inner, "streams-2", 300).await;
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(inner.clone()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    // Possession before the ring exists: this instance serves 00.
    assert!(matches!(
        rig.state.shards.open_or_wait("00", Duration::from_secs(5)).await,
        crate::sharddir::OpenOutcome::Ready(_)
    ));
    assert!(crate::fleet::start_configured(rig.state.clone(), &rig.tasks));

    // Tick 1 publishes the view, yields 00 and returns the aged override
    // home (CAS); tick 2 mirrors the emptied override map.
    let ring = vec!["streams-1".to_string(), "streams-2".to_string()];
    settled(Duration::from_secs(10), || {
        rig.state.ownership.ring_active() == ring
            && rig.state.shards.held_prefixes().is_empty()
            && rig.state.peer.has_peer("streams-2")
            && rig.state.ownership.overrides().is_empty()
    })
    .await;
    assert_eq!(
        rig.state.ownership.ring_active(),
        ring,
        "an unreadable router report must not freeze ownership publication"
    );
    assert!(
        rig.state.shards.held_prefixes().is_empty(),
        "possession must still yield the moved shard at the tick"
    );
    assert!(rig.state.peer.has_peer("streams-2"), "the peer table must still be published");
    assert!(
        rig.state.ownership.overrides().is_empty(),
        "return-home must still run and commit while a router report is unreadable"
    );
    // Tick 1 reached its publication site (its override CAS precedes it)
    // with the report unreadable: the desired document is untouched.
    let desired = desired_doc(&rig.state).await;
    assert_eq!((desired.epoch, desired.count), (1, 2), "only the desired publication is deferred");

    // Readable again: 300 in flight over 105 admitted slots wants a third
    // instance, and the next tick publishes it.
    inner.delete(&Path::from("routers/edge-1.json")).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut desired = desired_doc(&rig.state).await;
    while desired.epoch < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        desired = desired_doc(&rig.state).await;
    }
    assert_eq!(desired.epoch, 2, "the desired publication resumes once the report is readable");
    assert!(desired.count >= 3, "edge-slot dimension must scale out: {}", desired.count);

    rig.tasks.shutdown(Duration::from_secs(3)).await;
    engine_shutdown(&rig.state).await;
}
```

Why these numbers (all from defaults in `src/config/cli.rs:614-630` and `src/config/model.rs:462-468`):
`fleet_max = 4`, `target_util = 0.75`, `edge_slots = 140`, `fleet_min = 1`,
`rebalance_return_secs = 300`, `allow_http_peers = false` (so the peer URL must be https;
`.invalid` is fine because `peer_domains_raw` is unset). `need_slots = ceil(300 / (0.75·140)) = 3
> cur_count 2` regardless of the test process's CPU (which can only push `need_util` up, hence
`count >= 3`, never exact). The override is aged past 300 s so return-home drops it in tick 1
(`return_home_allowed(owned = 0, gain = 1, total = 1, active_n = 2)` → `1 <= 1`), which is what
proves the rebalancer-side block ran and CAS'd. Yield happens in the same tick because the
mirror that tick still carried the override (`effective_owner("00") == "streams-2"`).

**Expected failure on current code** (`82095942`): the tick hits `src/fleet.rs:645` and
`continue`s before `set_view`; after the 10 s bound the first assertion fails:

```
thread 'dst::dst_tests::fleet_controller::an_unreadable_router_report_defers_only_the_desired_publication' panicked at src/dst/tests/fleet_controller.rs:NNN:
assertion `left == right` failed: an unreadable router report must not freeze ownership publication
  left: []
 right: ["streams-1", "streams-2"]
```

(`held_prefixes()` would still be `["00"]`, `has_peer` false, `overrides()` empty-by-default,
and `fleet/desired.json` untouched — but the ring assertion fires first.) Run it with
`cargo test --lib dst_tests::fleet_controller::an_unreadable_router_report -- --nocapture`;
record that red run in the commit message (the test and fix land in ONE commit — slate takes
no red commit).

Wall time on the fixed code: tick 1 at ~2 s, tick 2 at ~4 s, phase 2 one more tick → ~7 s.
Adds ~7 s to the fleet owner's per-mutant test time (13 s today per `dd220be8`; cap 90 s).

### 3b. Unit test for the extracted predicate

File: `src/fleet/tests.rs` (`#![cfg(test)]`, 145 lines; `use super::*;` already registered as
an `unresolved-glob` in `docs/quality/owners.json`). Append:

```rust
fn router_report(ts_ms: Option<i64>, client_p50_ms: f64) -> serde_json::Value {
    let stamp = ts_ms.map_or(String::new(), |ts| format!(r#""ts_ms":{ts},"#));
    serde_json::from_str(&format!(r#"{{{stamp}"client_p50_ms":{client_p50_ms}}}"#)).unwrap()
}

// The edge signal is the worst p50 among reports stamped within the last
// 10 s of `now`: exactly 10 s old is stale, unstamped is ancient, a stamp
// from the future is fresh.
#[test]
fn edge_p50_is_the_worst_fresh_router_report() {
    let now = 1_000_000;
    let reports = [
        router_report(Some(now - 9_999), 400.0),
        router_report(Some(now - 10_000), 900.0),
        router_report(Some(now - 5_000), 250.0),
        router_report(None, 700.0),
    ];
    assert_eq!(fresh_edge_p50(&reports, now), 400.0);
    assert_eq!(fresh_edge_p50(&[], now), 0.0);
    assert_eq!(fresh_edge_p50(&[router_report(Some(now + 1), 50.0)], now), 50.0);
}
```

"Red" here is honest but weaker: `fresh_edge_p50` does not exist on current code (compile
error); the behaviour it pins is the verbatim inline fold from `:648-650`. Its value is the
mutation table below.

Mutant kill table for `fresh_edge_p50` (cargo-mutants 27.1.0, `--in-diff`):

| mutant | result on the table above |
| --- | --- |
| body → `0.0` / `1.0` / `-1.0` | `400.0` expected → killed |
| `now_ms - ts` → `+` | every age huge → `0.0` ≠ `400.0` → killed |
| `now_ms - ts` → `/` | ages ≈ 1 → 10 000-old counts → `900.0` → killed (and `now / 0` panics on the unstamped report) |
| `< 10_000` → `==` | only the 10 000-old report → `900.0` → killed |
| `< 10_000` → `>` | stale + unstamped only → `900.0` → killed |
| `< 10_000` → `<=` (if generated) | 10 000-old becomes fresh → `900.0` → killed |
| `start` body → `()` | fleet task never spawned → `r09_fleet_cancels_…` `expect("target storage operation must be entered")` after 5 s, and 3a → killed |

The new lines inside `start` (`inspect_err`, `as_deref().unwrap_or(&[])`, `if reports.is_err()`)
contain no binary/unary operator, so they add no other in-diff mutant.

## 4. Code change, per file, with budgets

### `src/fleet.rs` — CEILINGED at 1,143 lines (merge-base ratchet; `wc -l` = 1143). Net **0**.

Line budget (verify with `wc -l src/fleet.rs` after `cargo fmt --all`; must read ≤ 1143):

| edit | lines |
| --- | --- |
| replace `:638-653` (16 lines) with the 5-line block below | −11 |
| head guard at `:1068` (2 new lines, the third reuses the existing `if cur.is_none()` line) | +2 |
| `fresh_edge_p50` after `:211` (1 blank + 1 doc + 7) | +9 |
| re-decided `unwrap_used` reason (`:432-435`, same 4 lines, longer text) | 0 |
| **net** | **0 → 1143** |

If rustfmt reflows the `.inspect_err(...)` chain onto two extra lines (it does not touch this
closure today — `:601`/`:645` one-line arms and the >100-column chain at `:765` survive
`cargo fmt --check`), take the two lines back by folding the helper's doc onto the `fn` line's
preceding comment or by dropping the blank line before it; re-run `wc -l`.

(a) `:638-653` →

```rust
            // 2b. Router reports: worst client-observed p50 (edge congestion
            // is invisible to acks). Unreadable reports defer ONLY the desired CAS.
            let reports = fleet_io!(repository.read_router_reports())
                .inspect_err(|error| tracing::warn!(%error, "router snapshot unreadable; desired publication deferred"));
            let edge_p50 = fresh_edge_p50(reports.as_deref().unwrap_or(&[]), now_ms());
```

`edge_p50` is no longer `mut` (it was only assigned inside the removed loop; `:692` and `:1103`
read it). `fleet_io!` invocation count in `start` stays 9 (matches the `macro-dsl` allowance
row `crate::start`/`fleet_io` count 9 in `docs/quality/source-allowances.json`).

(b) `:1068` →

```rust
            let (publish, publish_count) = if reports.is_err() {
                (false, need) // unreadable router snapshot: no scale decision this tick
            } else if cur.is_none() {
```

(c) after `:211` (`pick_victim_shard`), before the `rss_bytes` doc:

```rust

/// Worst client-observed p50 among router reports stamped within 10 s of `now_ms`.
fn fresh_edge_p50(reports: &[serde_json::Value], now_ms: i64) -> f64 {
    reports
        .iter()
        .filter(|v| now_ms - v["ts_ms"].as_i64().unwrap_or(0) < 10_000)
        .map(|v| v["client_p50_ms"].as_f64().unwrap_or(0.0))
        .fold(0.0, f64::max)
}
```

(d) `:432-435` — re-decide the `clippy::unwrap_used` reason (format `owner; invariant; alternative`,
no `;` or `"` inside a part):

```rust
#[expect(
    clippy::unwrap_used,
    reason = "start; a poisoned timing ring may hold a half-recorded wait, the fleet documents serialise infallibly as plain data, and the router snapshot is folded through fresh_edge_p50 without an unwrap because an unreadable one defers only the desired publication; recovering the former or handling the latter would add branches no tick reaches"
)]
```

Ratchet accounting for `start` (`scripts/quality/source_rules.py:116-216`, contracts keyed by
`(path, qualified, kind, attribute text)`; `exception_growth` fails on any metric that grows
under an UNCHANGED attribute text, and skips an identity whose text changed):

| `#[expect]` on `start` | metrics ratcheted | effect of this edit |
| --- | --- | --- |
| `too_many_lines` | scope_lines, nested_items, syntax_facts | scope 680 → 671; items unchanged (only `macro(fleet_io)`); facts −7 → all shrink, text unchanged, OK |
| `excessive_nesting` | same | same, OK (the removed `{ … }` block is not the deepest nest) |
| `cast_sign_loss` | same | same, OK |
| `cast_possible_truncation` | same | same, OK |
| `let_underscore_must_use` | same | same, OK |
| `unwrap_used` | same **+** `unwrap_sites` (4, unchanged: `:537`, `:959`, `:1033`, `:1111`) **+** fingerprints of every `call-site` (whole tokens) and every `path` in scope | the new `fresh_edge_p50(...)` call and the new `fresh_edge_p50` path are new fingerprint keys → would read "accepted exception grew" → **re-decide this reason** (text in (d)); the new identity is not in the merge base, so it is skipped |

Fact arithmetic (scanner rules in `tools/quality-syntax/src/scan.rs:222-286`; macro bodies are
opaque except attribute groups, so the log text is free): old block 28 facts (`fleet_io` 3,
`reports` 1, `Ok` 1, `items` 1, `Err` 1, `tracing::warn` 3, `reports` 1, `now_ms()` 2, `v` 1,
`as_i64` 2, `unwrap_or` 2, `fresh` 1, `edge_p50` 2, `max` 2, `v` 1, `as_f64` 2, `unwrap_or` 2);
new block 17 (`fleet_io` 3, `inspect_err` 2, `tracing::warn` 3, call 1 + path 1, `reports` 1,
`as_deref` 2, `unwrap_or` 2, `now_ms()` 2); guard +4 (`reports` 1, `is_err` 2, `need` 1). Net −7.
All six `#[expect]`s still fire (`unfulfilled_lint_expectations` is denied): the four unwraps,
the casts at `:663-676`/`:727`, the nesting and length remain.

Other gates on this file: architecture-gate `file:src/fleet.rs` limit 1317 and
`function:src/fleet.rs::start` limit 687 — both shrink. `forbidden_edges` (`crate::http` count 3)
unchanged. `serde_json::Value` in a private fn signature adds no reverse edge.

### `src/fleet/tests.rs` — 145 → ~168 lines. `#![cfg(test)]` file → `production_unchanged`
for the planner; no owner row, no mutation. Content in §3b.

### `src/dst/tests/fleet_controller.rs` — 224 → ~320 lines (< 1,000). Not under a critical
prefix; not a mutation source. Content in §3a. Do not touch
`r09_fleet_cancels_entered_documents_without_partial_authority_or_lost_retry` (pinned by
function sha256 in `docs/refactor/review-mechanisms.json:702-705`; the pin hashes the function
text only, so adding imports and new functions elsewhere in the file is safe).

### Nothing else in `src/`. No new files, no `HttpRigOptions` change, no `planning.rs` change.

## 5. Ledger / doc rows

| artefact | action |
| --- | --- |
| `docs/refactor/test-inventory.json` | regenerate: `python3 scripts/test-inventory.py --write` (adds the new DST test entry with its `function_sha256`; name must be unique across `src/dst/**`). The `tests_ran.py` floor follows the inventory. |
| `docs/refactor/review-mechanisms.json` | **no change** — the pinned fleet test is untouched. |
| `docs/refactor/test-scenario-map.json` | **no change** (the fleet tests are not scenario-mapped; the map gate only validates existing references). |
| `scripts/quality/mutation_owners.py` | **no change** — `src/fleet.rs` is already `owner('fleet', …, 'fleet:: dst_tests::fleet_controller:: dst_tests::runtime_isolation::')`, whose filter reaches both new tests. |
| `docs/quality/owners.json`, `docs/quality/source-allowances.json` | **no change** — no new spawn/env/global/glob/macro-DSL sites; the `fleet_io` count stays 9; the changed `#[expect]` is in-source (exceptions with `reason =` are never ledger rows). Run `scripts/quality/gate.py --prune` only if the gate reports an obsolete allowance (it should not). |
| `docs/refactor/WIRE-MATRIX.md`, `docs/LIVE-FEED.md` | **no change** — no HTTP/SSE/document-format change; `fleet/desired.json` keeps its shape and `reason` string. |
| `docs/SCALING.md` §7 "Rebalancer (implemented)" (`:218-219`) | optional one sentence after "Everyone mirrors overrides.json into routing each fleet tick": *"A tick that cannot read the router tier's `routers/*.json` still mirrors the ring and overrides, yields and rebalances; it withholds only the desired-count CAS, the one decision that consumes those reports."* |
| commit message | one commit, fix + both tests + inventory; quote the red run (§3a failure) and the local `scripts/quality.sh` + `scripts/quality/mutations.sh` receipts (fleet owner: N mutants tested, N caught). |

Local verification order (CI's plan first, per the session's traps):
1. `cargo build --locked -p streams-quality-syntax && scripts/quality.sh` — watch for
   "accepted exception grew" (would name the metric), "file growth: src/fleet.rs", and the
   test-inventory gate.
2. `cargo test --lib fleet::` and `cargo test --lib dst_tests::fleet_controller::` (the new DST
   test alone first, on the unfixed tree, to record the red).
3. `python3 scripts/quality/verification_plan.py --out target/quality-plan` then
   `scripts/quality/mutations.sh` — expect the `fleet` owner with the `fresh_edge_p50` mutants
   and `start → ()`, all caught, under the 90 s per-mutant cap.
4. `wc -l src/fleet.rs` = 1143 after `cargo fmt --all`.

## 6. What could go wrong

- **Wire compatibility**: none. No request/response, SSE, or coordination-document format
  changes. The only externally visible difference is the log line text
  ("router snapshot deferred; scale decision deferred" → "router snapshot unreadable; desired
  publication deferred"); no script or doc in the repo greps the old text.
- **Fleet skew during a rolling deploy**: old binaries still freeze their view while a bad
  router file exists; new ones publish, yield and rebalance. Ownership is derived from the same
  authority documents on every instance, and a shard an old instance fails to yield is fenced
  the moment the new owner opens it (eager open or first routed request) — the pre-existing
  lazy-fence path. No new hazard; the skew only exists while the fault persists.
- **Scale-decision semantics during a router outage**: `below_since` and the desired CAS are
  frozen exactly as today. Change worth stating in the commit: `lat_breach_since` /
  `cpu_breach_since` (`:680-705`) now keep running during the outage (previously the aborted
  tick did not touch them). They are edge-independent, so a CPU/latency breach sustained
  across a router outage scales out promptly when reports return instead of restarting its
  sustain window — the correct behaviour, and no test depends on the old one.
- **Nightly full-owner rotation**: `src/fleet.rs` is already in the seven-night rotation; the
  only new mutants are `fresh_edge_p50`'s, all killed by §3b. (Independent of this change: if
  the `fleet` bucket has surviving mutants in `start`'s arithmetic today, that is a
  pre-existing nightly condition — check `gh run list --workflow rust-quality.yml
  --event schedule` before attributing a red nightly to this commit.)
- **Flakiness of 3a**: real-time 2 s ticks (bounded 10 s + 8 s waits, ~7 s typical); peer
  heartbeat stamped 20 s ahead so the 10 s live window cannot expire mid-test; self CPU noise
  only raises `need_util`, hence `count >= 3` not `== 3`; `RigRuntime::first()` like its
  neighbours; the aged override cannot be dropped *before* the first mirror because the mirror
  and return-home happen in the same tick in that order (`:799` then `:886-971`). If a CI runner
  is so slow that tick 2 lands after 10 s, the ring/held/peer assertions have already been met
  at tick 1; only `overrides().is_empty()` would lag — keep the 10 s bound generous rather than
  tightening it.
- **rustfmt vs the line ceiling**: the whole `tasks.spawn(...)` closure in `start` is currently
  left alone by rustfmt (evidence above). The budget has zero slack by design; the doc line of
  the helper and the blank line before it are the adjustable margin. Never let `wc -l` exceed
  1143.
- **Ratchet surprise**: if `scripts/quality.sh` reports `syntax_facts` growing for any of the
  five unchanged `#[expect]`s, my hand count is off by the reported delta — remove that many
  facts inside `start` (e.g. the redundant `Option<std::collections::HashMap<String, String>>`
  annotation at `:763` is 4 path facts) rather than re-deciding five reasons.
- **Mutation timing**: fleet owner per-mutant test time ≈ 13 s + 7 s; well under 90 s.

## Skeptic corrections

Checked read-only against `slate` @ `82095942`. Verdict: **sound with corrections** — the
2a design, the line budget, the ratchet accounting and the red DST test all hold up
first-hand; the items below are the places where the plan is wrong in detail, silent
about a trap, or needs a cheap hardening.

### S1. Kill table: the `<` → `>` row's value is wrong (still killed)

`src/fleet.rs` helper, mutant `now_ms - ts > 10_000`: ages are 9 999 (no), 10 000 (no),
5 000 (no), 1 000 000 for the unstamped report (yes) → the only "fresh" report is the
unstamped one → result **700.0**, not 900.0. `700.0 != 400.0` so the mutant still dies;
fix the table so the commit-message receipt does not misstate it. The other seven rows
verified against cargo-mutants 27.1.0's actual operator table
(`~/.cargo/registry/src/*/cargo-mutants-27.1.0/src/visit.rs:589-597`): `Lt → ==, >, <=`;
`Sub → +, /`; f64 body → `0.0, 1.0, -1.0` (`fnvalue.rs:76`). Nothing else in 27.1.0
mutates the new lines: there is no if-condition, method-call or literal mutation genre,
and match-arm deletion only fires on a `match` with a `_` arm (`visit.rs:654-660`) —
the removed `match reports { Ok.. Err.. }` had none, and nothing new is a match.

### S2. `unwrap_used` re-decision is unavoidable for a second reason; use a cleaner text

`scripts/quality/source_rules.py:176-188` fingerprints, under `clippy::unwrap_used`
only, every `call-site` (function calls with a path callee, NOT method calls) and every
`path` fact in `start`, keyed `unwrap_site:ordinary-call:<digest>` / `unwrap_site:path:<digest>`
with a **count**. Two keys grow, not one:

- new `call-site` `fresh_edge_p50(...)` and new `path` `fresh_edge_p50` (plan's reason), and
- the `path` `need`: today `start` has 5 `need` path facts (`:747` ×1, `:1069`, `:1070`,
  `:1072`, `:1078`); the guard's `(false, need)` makes 6 → `count > before` → "grew".

(`reports` stays at 2 — `match reports`/`for v in reports` become
`reports.as_deref()`/`reports.is_err()`; `now_ms` stays at 10 — `:648` moves into the
helper and the new call re-adds one; `edge_p50` drops 3 → 1 because `:1103` is inside
`format!` tokens and is not a path fact.) So there is no fingerprint-neutral spelling of the
guard either; re-deciding the reason is the sanctioned route (`exception_growth`,
`source_rules.py:207-208`, skips an identity whose attribute text changed;
`test_source_rules.py:131-134` pins that). The plan's proposed text reads badly once the
middle part has three clauses ("the former … the latter"). Use, at `src/fleet.rs:432-435`:

```rust
#[expect(
    clippy::unwrap_used,
    reason = "start; a poisoned timing ring may hold a half-recorded wait, the fleet documents serialise infallibly as plain data, and an unreadable router snapshot is a typed deferral of the desired CAS rather than a panic site; recovering the ring, handling the serialisation or aborting the tick on the snapshot would add branches no tick reaches"
)]
```

Format check (`source_rules.py:258`, regex `"[^";]+;[^";]+;[^";]+"`): exactly two `;`,
no `"` inside a part — verified. Reasoned in-source exceptions are never ledger rows
(`:263-264`), so `source-allowances.json` is untouched, as the plan says.

### S3. Do NOT reflow the lines adjacent to the head guard (in-diff adjacency trap)

cargo-mutants' `--in-diff` marks as affected every **inserted** line plus the line
immediately after a run of deletions (`in_diff.rs:205-250`, "if a line is deleted then the
range will span from the line before to the line after"). The plan's (b) edit is safe
only because the lines that follow the three new ones — `(true, need) // bootstrap …`,
`} else if need > cur_count {` (`:1070`), `} else if need_shrink < cur_count {` (`:1073`)
— stay byte-identical. If the implementer reflows or re-indents `:1070`, the `>` mutants
enter scope and `need >= cur_count` **survives** the DST test (3 ≥ 2 publishes exactly as
3 > 2 does; the only distinguishing state, `need == cur_count`, is reached after phase 2
but the test stops polling at `epoch >= 2`). Same for `:1073` (`<=`). Concretely: the diff
hunk for (b) must be `-1 line / +3 lines` with `:1069` onward as context. Likewise (a)
must not touch `:655-663` (the `need_util` line carries `/` and `as u64`; `/`→`*` and
`/`→`%` are CPU-noise mutants, exactly the class the plan's §2a says it is avoiding).

### S4. DST test hardening (cheap, avoids two flake modes)

`src/dst/tests/fleet_controller.rs`, plan §3a:

1. Stamp the peer heartbeat further ahead. With `ts_ms = now + 20_000` streams-2 is live
   for 30 s from setup (`src/fleet.rs:605`, `now - ts < 10_000`) and ring-active for 50 s
   (`planning.rs:16`, `age < 30_000`). Phase 2 needs streams-2 **live** (its
   `inflight: 300` is the only thing that makes `need_slots = 3`); if a loaded runner
   spends most of the 10 s `settled` bound plus the 8 s phase-2 bound, tick 3 can land
   after the 30 s window with `total_inflight = 0`, `need = need_util` (possibly 1) and
   the `epoch == 2` assertion fails for the wrong reason. Use `now_ms() + 60_000`
   (negative ages are fine everywhere: `:605`, `planning.rs:16`); the live window becomes
   70 s, comfortably past the bounds below.
2. Widen the bounds to `settled(Duration::from_secs(20), …)` and a 12 s phase-2 deadline.
   They only change failure latency on the unfixed tree (still bounded; a mutant
   `start → ()` is then killed at ~20 s, well under the 90 s per-mutant timeout in
   `mutation_driver.py:50`).
3. Keep the shutdown parity with `r09`: `let report = rig.tasks.shutdown(Duration::from_secs(3)).await;`
   `assert!(report.aborted.is_empty(), "fleet loop must cancel cooperatively: {report:?}");`
   (`ShutdownReport` is not `#[must_use]`, `src/tasks.rs:121`, so the plan's bare statement
   compiles; the assertion is what makes a wedged tick visible.)
4. Expected red on the current tree, exact: thread
   `dst::dst_tests::fleet_controller::an_unreadable_router_report_defers_only_the_desired_publication`
   panics with
   ``assertion `left == right` failed: an unreadable router report must not freeze ownership publication`` /
   `left: []` / `right: ["streams-1", "streams-2"]` — after the full `settled` bound (20 s
   with item 2). Verified mechanism: `src/fleet.rs:642-646` `continue`s every tick while
   `routers/edge-1.json` exists, so `set_view` (`:799`) never runs and `ring_active()` stays
   `[]` (`OwnershipView::default()`).

The rest of the scenario arithmetic checks out first-hand: rig defaults `prefixes = ["00"]`
(`fixture_http.rs:44`) so `return_home_allowed(0, 1, 1, 2)` holds whichever way
`ring_pick("00", [s1, s2])` lands (home == streams-2 is the no-op-override branch at
`:905-907`; home == streams-1 is healthy because the own heartbeat carries lag 0 and an
idle engine reports `wedge_ms() == 0`, `src/shard.rs:1940-1981`); `CliArgs::deterministic()`
gives `scale_edge_slots = 140`, `scale_out_cpu_pct = 75`, `fleet_max = 4`
(`src/config/cli.rs:615-630`) and the rig's empty `MapEnvironment` leaves
`allow_http_peers = false`, `peer_domains_raw = None`, `fleet_min = 1`,
`rebalance_return_secs = 300` (`src/config/load.rs:213-230`, `model.rs:460-469`), so
`https://streams-2.invalid` passes `valid_peer_url` and `ceil(300 / 105) = 3 > 2`.
`has_peer`/`set_peers` (`src/peer.rs:68,77`), `overrides()`/`ring_active()`
(`src/ownership.rs:105,118`), `held_prefixes`/`open_or_wait`/`prefixes`
(`src/shard_directory.rs:302,218,370`) and `crate::sharddir::OpenOutcome::Ready`
(`src/sharddir.rs:398`) all exist with the used signatures; `get_or_open` does not consult
ownership, so opening `00` before the ring exists is `Ready`.

### S5. Helper: rename the `now_ms` parameter

`fn fresh_edge_p50(reports: &[serde_json::Value], now_ms: i64)` shadows the module import
`crate::shard::now_ms` (`src/fleet.rs:14`). No lint fires, but the syntax scanner resolves
the parameter's uses through the lexical import table (`scan.rs:222-224`) and records them
as `crate::shard::now_ms` path facts — misleading in any future fingerprint diff. Use `now`:

```rust
/// Worst client-observed p50 among router reports stamped within 10 s of `now`.
fn fresh_edge_p50(reports: &[serde_json::Value], now: i64) -> f64 {
    reports
        .iter()
        .filter(|v| now - v["ts_ms"].as_i64().unwrap_or(0) < 10_000)
        .map(|v| v["client_p50_ms"].as_f64().unwrap_or(0.0))
        .fold(0.0, f64::max)
}
```

Same 9 lines (blank + doc + 7), same mutants, same kill table. The unit test in §3b is
unchanged (`fresh_edge_p50(&reports, now)`).

### S6. Residuals the commit message must state (no test possible or wanted)

- **Slow (not corrupt) router reads still delay the view.** The plan keeps the read at
  section 2b, before `read_desired_state`/`read_overrides`/`set_view`. A store that hangs
  on one `routers/*.json` GET only errors at the population deadline
  (`repository.rs:152-154`, 10 s), so the view/peers/yield of that tick are published up to
  10 s late — no longer skipped, but not "before the router read" as the item's placement
  would give. Accepted because moving `edge_hot`/`need_edge` (`:692-693`) after the
  rebalancer drags `>`, `&&`, `>=`, `+` into the in-diff scope (7 mutants) and killing
  them needs a three-variant DST matrix (hot p50 with rps ≥ 5; p50 == threshold; stale
  report with rps ≥ 5). Record as a follow-up under the plan's 2b.
- **Bootstrap is deferred too.** With `cur.is_none()` and an unreadable report the guard
  withholds the first `desired.json` as today (the whole tick aborted). Same behaviour,
  now explicit; state it.
- **`lat_breach_since`/`cpu_breach_since` keep running through a router outage.**
  Disclosed in §6; there is no deterministic test (20 s sustain windows and the test
  process's own CPU). Say so in the commit rather than claiming coverage.
- The test exercises return-home (the rebalancer block's CAS at `:957`), not a lag-driven
  move (`:984-1061`); the item's "run … rebalance" is proven for the block, not for the
  move path. Fine, but do not overstate it.

### S7. Verified gate facts (so the implementer need not re-derive them)

- **Ceiling 1143 is right.** `source_rules.violations` (`:226-230`) uses
  `min(legacy 1248 + 4 adoption, max(1000, merge-base 1143))`; `merge_base()` is
  `origin/slate` locally and the push's `before` in CI (`common.py:20-29`,
  `rust-quality.yml:19-20`), i.e. `82095942` → 1143. `architecture-gate.py:126-129`
  caps `file:src/fleet.rs` at 1317 and `function:src/fleet.rs::start` at 687; the new
  helper gets `max(200, 0)`.
- **`syntax_facts` −7 reproduces from the scanner rules** (`tools/quality-syntax/src/scan.rs`):
  path (every `syn::Path`, incl. locals and `Ok`/`Err` patterns), `method-call` +
  `method-call-site` (2 per method call), `call-site` (1 per path-callee call, plus its
  path), `macro` + `macro-tokens` + the macro's path (3 per invocation; tokens opaque).
  Old block 28, new block 17, guard 4. All six `#[expect]`s share `start`'s scope
  (attributes are inside the item span — `test_source_rules.py:125-129` pins that), so a
  miscount of even +1 would fail five reasons; the count above is exact under those rules.
- **`fleet_io` must stay at exactly 9.** Growth fails `violations` (`:265`); a drop makes the
  row *stale* and `source_gate.check` fails with "obsolete source allowances; run … --prune"
  (`source_gate.py:60-62`) unless the ledger is pruned in the same commit. The plan's
  replacement keeps one invocation → 9. `tokio::select` (2) and `serde_json::json` (2)
  rows unchanged.
- **`unwrap_or(&[])` does not trip `clippy::unwrap_or_default`** under the pinned 1.98.1:
  precedent `src/postings/validated.rs:114` passes today with `-D warnings` and an empty
  active ledger (`docs/quality/diagnostic-allowances.json`: `warnings: 0`).
- **`src/fleet/tests.rs` is exempt from mutation selection** as `production_unchanged`:
  `#![cfg(test)]` → `test_only_file` → normalized source `''`
  (`production_changes.py:66-67`), even though `src/fleet` is a critical prefix
  (`verification_plan.py:29-31`). `src/dst/tests/**` is not critical. `src/fleet/planning.rs`
  would indeed be an unregistered critical source (`mutation_owners.py:277-281`) — the
  plan's 2c rejection is correct.
- **Test inventory.** `ci.yml:56` runs `test-inventory.py --check`, which rejects new
  entries; `--write` in the same commit is mandatory, as the plan says. The
  `review-mechanisms.json` pin on `r09_…` is a per-function token hash
  (`review-evidence.py:178-181`), so the new import and new functions in the same file are
  safe; keep the new test names unique (grep confirms none of `an_unreadable_router_report…`,
  `edge_p50_is_the_worst…`, `settled`, `peer_heartbeat`, `desired_doc`, `fresh_edge_p50`
  exist today).
- **rustfmt.** No `rustfmt.toml`; the whole `start` body is currently emitted verbatim
  (`:601`, `:645`, `:765`, `:853-855`, `:864-870` are all non-rustfmt shapes while
  `cargo fmt --check` is green), so the 130-column `.inspect_err(...)` line is safe and
  the (a)/(b) budgets hold. The helper is outside `start` and formats to exactly the
  7 lines shown. Still run `cargo fmt --all && wc -l src/fleet.rs` before committing.
- **Mutation timing.** `start → ()` is killed by three bounded failures in parallel
  (`r09` at 5 s via `expect("target storage operation must be entered")`,
  `two_runtimes_never_share_fleet_state` at ~2.6 s, the new test at the `settled` bound);
  no test can hang.
