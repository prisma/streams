//! Closure debts of replaced incarnations, end to end (NEXT-WORK §2, a
//! billing release blocker). Recreating a name over a dead incarnation
//! records a debt before the write that replaces its descriptor; every
//! instance's billing sweep closes the segments it owns at the debt's
//! persisted instant and forgets the debt once every segment has settled.
//! These scenarios cover the raw surface, a month crossing, a crash at each
//! handoff boundary, a recreation that loses its CAS to a renewal, a split
//! incarnation and fork retention. Two instances are in
//! `billing_closure_owners.rs`.

use super::fixture_http::{
    HttpRigOptions, engine_shutdown, http_rig, http_rig_build, install_rollup,
};
use super::fixture_livefeed::{hub_append_lf, split_and_await};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, skey};
use crate::billing::replaced::settle_replaced;
use crate::billing::{SegmentBillingMetaV1 as Meta, billing_now_ms, month_start_ms};
use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
use crate::registry::replaced::DebtEntry;
use std::sync::Arc;
use std::time::Duration;

type State = Arc<crate::http::AppState>;
type Ref = crate::tenant::TenantStreamRef;
type Desc = crate::registry::StreamDesc;
type Engine = Arc<crate::shard::ShardEngine>;
type Addr = std::net::SocketAddr;

pub(super) const JSON: (&str, &str) = ("content-type", "application/json");
/// One 26-byte record.
pub(super) const ROW: &[u8] = br#"[{"n":1,"pad":"aaaaaaaaaa"}]"#;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;

/// Creates, or recreates, `name` on the raw surface.
pub(super) async fn raw_put(addr: Addr, name: &str, headers: &[(&str, &str)], body: &[u8]) {
    let (st, _, b) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), headers, body).await;
    assert_eq!(st, 201, "raw PUT {name}: {}", String::from_utf8_lossy(&b));
}

/// Creates, or recreates, `name` on the product surface.
pub(super) async fn product_put(addr: Addr, name: &str) {
    let (st, _, b) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{name}"),
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st,
        201,
        "product PUT {name}: {}",
        String::from_utf8_lossy(&b)
    );
}

/// Appends one record under routing key `key` on the product surface.
pub(super) async fn keyed_append(addr: Addr, name: &str, key: &str) {
    let (st, _, b) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records"),
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", key),
        ],
        br#"{"n":1,"pad":"aaaaaaaaaa"}"#,
    )
    .await;
    assert_eq!(st, 200, "append to {name}: {}", String::from_utf8_lossy(&b));
}

/// A product stream `name` with two records in segment 0, then split into a
/// sealed segment 0 and live children 1 and 2.
pub(super) async fn hub_split(addr: Addr, state: &State, name: &str) {
    product_put(addr, name).await;
    hub_append_lf(addr, name, r#"{"h":0}"#).await;
    hub_append_lf(addr, name, r#"{"h":1}"#).await;
    split_and_await(state, name, 0).await;
}

/// A routing key that segment `seg` of `desc` owns.
pub(super) fn key_for(desc: &Desc, seg: u32) -> String {
    (0..10_000)
        .map(|i| format!("k{i}"))
        .find(|key| desc.resolve_segment(key).seg_id == seg)
        .expect("a routing key for every live segment")
}

/// The stored descriptor, read past this instance's cache.
pub(super) async fn stored(state: &State, sref: &Ref) -> Desc {
    state.registry.invalidate(sref);
    state
        .registry
        .get(sref)
        .await
        .unwrap()
        .expect("a stored descriptor")
}

/// The engine this instance serves segment `seg` of `desc` from.
pub(super) async fn engine_of(state: &State, desc: &Desc, seg: u32) -> Engine {
    let route = desc.segment_route_by_id(seg).expect("a routed segment");
    let Ok(engine) = state.engine_for_quiet(&route).await else {
        panic!("this instance does not own segment {seg}'s shard");
    };
    engine
}

/// Expires `sref` at `at` with nothing observing it (an idle expiry), and
/// drops it from every instance's cache.
pub(super) async fn expire(states: &[&State], sref: &Ref, at: i64) {
    let lapsed = states[0]
        .registry
        .cas_update(sref, |d| {
            d.expires_at_ms = Some(at);
            true
        })
        .await
        .unwrap();
    assert!(lapsed, "{sref} did not expire");
    for state in states {
        state.registry.invalidate(sref);
    }
}

/// Drains until none of `ids` is dirty. A row acked CLEAN is never revisited
/// by the dirty-row reconciler: only the walk or the debt pass can close it.
pub(super) async fn ack_clean(state: &State, engine: &Engine, ids: &[[u8; 16]]) {
    for _ in 0..200 {
        crate::billing::drain_once(state).await.expect("drain");
        let dirty = engine.usage_dirty_scan().await.unwrap();
        if dirty.iter().all(|(hash, _)| !ids.contains(hash)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("rows were never acked clean");
}

/// The row once its gauge reads zero within five seconds: a close is a
/// committer op, so it lands shortly after it is submitted, and a loaded
/// host (two instances' committers) gets room before a close counts as lost.
pub(super) async fn closed(engine: &Engine, id: [u8; 16]) -> Option<Meta> {
    for _ in 0..250 {
        let meta = engine.billing_meta(id).await;
        if meta
            .as_ref()
            .is_some_and(|m| m.owned_frame_bytes_current == 0)
        {
            return meta;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

/// What closing `before` at `close_ms` leaves: the storage integral advanced
/// to exactly that instant (split at any month boundary on the way), the
/// gauge zero, and one more version.
pub(super) fn expected_close(before: &Meta, close_ms: i64) -> Meta {
    let mut want = before.clone();
    want.advance_storage_clock(close_ms, |_| {});
    want.owned_frame_bytes_current = 0;
    want.usage_version += 1;
    want
}

/// Asserts that `got` bills exactly what `want` does.
pub(super) fn assert_billed(got: &Meta, want: &Meta, what: &str) {
    let billed = |m: &Meta| {
        (
            m.owned_frame_bytes_current,
            m.storage_accounted_through_ms,
            m.month_byte_ms(),
            (m.month_year, m.month_month),
            m.usage_version,
        )
    };
    assert_eq!(billed(got), billed(want), "{what}");
}

pub(super) async fn debts(state: &State) -> Vec<DebtEntry> {
    state.registry.replaced_page(None, 64).await.unwrap()
}

/// The one closure debt the cell holds, at the step `when` names.
pub(super) async fn single_debt(state: &State, when: &str) -> DebtEntry {
    let mut all = debts(state).await;
    assert_eq!(all.len(), 1, "exactly one closure debt {when}");
    all.remove(0)
}

/// The segments a debt records as settled, in order.
pub(super) fn settled(entry: &DebtEntry) -> Vec<u32> {
    let mut segments = entry.debt.settled.clone();
    segments.sort_unstable();
    segments
}

/// Runs the production sweep (discovery, the tombstone walk, then the debt
/// pass) until `id`'s gauge closes.
async fn sweep_until_closed(state: &State, engine: &Engine, id: [u8; 16]) -> Meta {
    for _ in 0..10 {
        crate::billing::sweep_owned_outboxes(state).await;
        if let Some(meta) = closed(engine, id).await {
            return meta;
        }
    }
    panic!("the replaced incarnation kept its storage gauge through every sweep");
}

/// Runs the production sweep until no closure debt is left.
async fn sweep_until_settled(state: &State) {
    for _ in 0..20 {
        crate::billing::sweep_owned_outboxes(state).await;
        if debts(state).await.is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("a closure debt never settled");
}

/// A raw stream with a one-hour TTL and one billed record: its descriptor,
/// segment-0 engine and row.
async fn raw_ttl_stream(state: &State, addr: Addr, name: &str) -> (Desc, Engine, Meta) {
    raw_put(addr, name, &[JSON, ("stream-ttl", "3600")], ROW).await;
    let old = stored(state, &state.deployment.raw_adapter_sref(name)).await;
    let engine = engine_of(state, &old, 0).await;
    let before = engine
        .billing_meta(old.dynamic_segment_identity(0))
        .await
        .expect("a billed row");
    assert!(before.owned_frame_bytes_current > 0, "no gauge to leak");
    (old, engine, before)
}

/// Owner requirement "raw recreation": an idle-expired incarnation replaced
/// through `PUT /v1/stream/{name}` (`CreationService::create` ->
/// `claim::resolve` -> `Registry::recreate`) leaves a debt naming it, and
/// the PRODUCTION sweep (`sweep_owned_outboxes`: walk, then the debt pass)
/// closes its acked-clean row exactly at the persisted expiry, once, then
/// forgets the debt. The new incarnation's gauge is not the debt's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_raw_recreation_over_an_idle_expired_incarnation_closes_at_its_expiry() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    let sref = state.deployment.raw_adapter_sref("raw-idle");
    let (old, engine, _) = raw_ttl_stream(&state, addr, "raw-idle").await;
    let id = old.dynamic_segment_identity(0);
    ack_clean(&state, &engine, &[id]).await;
    let before = engine.billing_meta(id).await.unwrap();
    // Idle expiry: nothing observes it before the name is recreated.
    let expired_at = billing_now_ms();
    expire(&[&state], &sref, expired_at).await;
    // The recreation must judge the incarnation expired, not at its expiry.
    while billing_now_ms() < expired_at + 100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    raw_put(addr, "raw-idle", &[JSON], b"").await;
    let fresh = stored(&state, &sref).await;
    assert_ne!(fresh.stream_epoch, old.stream_epoch, "a new incarnation");
    let debt = single_debt(&state, "after the raw recreation").await;
    assert_eq!(
        (
            debt.debt.descriptor.stream_epoch.as_str(),
            debt.debt.close_ms,
            settled(&debt)
        ),
        (old.stream_epoch.as_str(), expired_at, vec![]),
        "the raw recreation recorded the replaced incarnation at its expiry"
    );
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/raw-idle",
        &[JSON],
        br#"[{"n":2}]"#,
    )
    .await;
    assert_eq!(st, 204);
    let fresh_id = fresh.dynamic_segment_identity(0);
    let live = engine.billing_meta(fresh_id).await.expect("the new row");

    let got = sweep_until_closed(&state, &engine, id).await;
    assert_billed(
        &got,
        &expected_close(&before, expired_at),
        "the replaced row closes exactly at its persisted expiry",
    );
    sweep_until_settled(&state).await;
    let after = engine.billing_meta(id).await.unwrap();
    assert_billed(
        &after,
        &got,
        "closed once: later sweeps leave the row alone",
    );
    let now_live = engine.billing_meta(fresh_id).await.unwrap();
    assert_eq!(
        (now_live.owned_frame_bytes_current, now_live.usage_version),
        (live.owned_frame_bytes_current, live.usage_version),
        "the new incarnation's gauge is not the debt's to close"
    );
    engine_shutdown(&state).await;
}

/// The same recreation over a row that was still DIRTY (appended, never
/// acked; a crash or an unowned drain). The drain runs every ~2 s and the
/// sweep far less often, so the drain meets the replaced row first. The debt
/// carries the persisted expiry, and the owner's requirement is that
/// storage closes exactly there; whichever closer runs first must honour it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dirty_row_of_a_replaced_incarnation_still_closes_at_its_expiry() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    let sref = state.deployment.raw_adapter_sref("raw-dirty");
    let (old, engine, before) = raw_ttl_stream(&state, addr, "raw-dirty").await;
    let id = old.dynamic_segment_identity(0);
    let expired_at = billing_now_ms();
    expire(&[&state], &sref, expired_at).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    raw_put(addr, "raw-dirty", &[JSON], b"").await;
    let debt = single_debt(&state, "after the raw recreation").await;
    assert_eq!(debt.debt.close_ms, expired_at);
    // The drain's cadence, then the sweep's. The drain's close lands before
    // the sweep reads the row, so the drain is the one that closes it.
    crate::billing::drain_once(&state).await.expect("drain");
    closed(&engine, id)
        .await
        .expect("the drain closes the replaced row");
    let got = sweep_until_closed(&state, &engine, id).await;
    sweep_until_settled(&state).await;
    assert_billed(
        &got,
        &expected_close(&before, expired_at),
        "a dirty row of a replaced incarnation was closed at the drain's clock, \
         not at the expiry its closure debt persisted",
    );
    engine_shutdown(&state).await;
}

/// Resets the injected billing clock, even on panic.
struct ClockReset;
impl Drop for ClockReset {
    fn drop(&mut self) {
        set_clock(0);
    }
}

fn set_clock(ms: i64) {
    crate::billing::BILLING_CLOCK_OVERRIDE.store(ms, std::sync::atomic::Ordering::Relaxed);
}

/// Consumes the `_usage` ledger to its end.
async fn roll_up(state: &State) {
    for _ in 0..50 {
        if crate::billing::rollup_step(state).await.expect("rollup") == 0 {
            return;
        }
    }
    panic!("the rollup never reached the end of the ledger");
}

/// Moves the billing clock to `at` and closes every month due there.
async fn closed_months(state: &State, at: i64) -> Vec<String> {
    set_clock(at);
    let rollup = state.rollup.get().expect("the rig's rollup");
    let grace = state.config.billing.month_close_grace_ms;
    let closed = rollup.close_months_due(grace).await.unwrap();
    closed.into_iter().map(|(month, _)| month).collect()
}

/// What `month` invoices for incarnation `epoch`: its frozen storage
/// byte-ms plus corrections (zero without a row).
async fn invoiced(state: &State, month: &str, epoch: &str) -> u128 {
    let rollup = state.rollup.get().expect("the rig's rollup");
    let row = rollup
        .month_row(month, "acct_test", "proj-test", epoch)
        .await
        .unwrap();
    row.map_or(0, |row| {
        let frozen = row.frozen.expect("a finalized month").storage_byte_ms;
        crate::rollup::eff_u128(frozen.parse().unwrap(), &row.corr.storage_byte_ms_delta)
    })
}

/// The gauge the rollup carries onto every later month for `epoch`.
async fn carried_gauge(state: &State, epoch: &str) -> u64 {
    let rollup = state.rollup.get().expect("the rig's rollup");
    let segments = rollup
        .stream_segment_states("acct_test", "proj-test", epoch)
        .await
        .unwrap();
    segments.iter().map(|s| s.owned_frame_bytes_current).sum()
}

/// January 2026 on the billing clock: `mx` bills one record from Jan 15,
/// acked clean and rolled up, then expires idle at Jan 31 12:00. Creation
/// judges liveness on the wall clock, months past both instants.
struct IdleJanuary {
    state: State,
    addr: Addr,
    sref: Ref,
    old: Desc,
    engine: Engine,
    before: Meta,
    expired_at: i64,
    /// Storage January owes: the gauge from Jan 15 to the expiry.
    owed: u128,
}

async fn idle_january() -> IdleJanuary {
    let (state, addr) = http_rig(mem()).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);
    let jan15 = month_start_ms(2026, 1) + 14 * DAY;
    set_clock(jan15);
    raw_put(addr, "mx", &[JSON], ROW).await;
    let sref = state.deployment.raw_adapter_sref("mx");
    let old = stored(&state, &sref).await;
    let engine = engine_of(&state, &old, 0).await;
    let id = old.dynamic_segment_identity(0);
    ack_clean(&state, &engine, &[id]).await;
    roll_up(&state).await;
    let before = engine.billing_meta(id).await.expect("a billed row");
    assert_eq!(before.storage_accounted_through_ms, jan15);
    let expired_at = month_start_ms(2026, 2) - 12 * HOUR;
    expire(&[&state], &sref, expired_at).await;
    assert!(
        crate::shard::now_ms() > expired_at,
        "creation judges the wall clock: it must be past the expiry"
    );
    let owed =
        u128::from(before.owned_frame_bytes_current) * u128::try_from(expired_at - jan15).unwrap();
    IdleJanuary {
        state,
        addr,
        sref,
        old,
        engine,
        before,
        expired_at,
        owed,
    }
}

/// Recreates `mx` at the current billing clock, appends to the new
/// incarnation, settles the debt through the production sweep, and rolls
/// the old row's close up. Returns the closed row.
async fn recreate_and_settle(case: &IdleJanuary) -> Meta {
    raw_put(case.addr, "mx", &[JSON], b"").await;
    let (st, _, _) = hreq(case.addr, "POST", "/v1/stream/mx", &[JSON], br#"[{"n":2}]"#).await;
    assert_eq!(st, 204);
    let id = case.old.dynamic_segment_identity(0);
    let got = sweep_until_closed(&case.state, &case.engine, id).await;
    sweep_until_settled(&case.state).await;
    ack_clean(&case.state, &case.engine, &[id]).await;
    roll_up(&case.state).await;
    got
}

/// Owner requirement "month crossing": expiry in January, recreation and
/// settlement in February (inside January's close grace). The shard row
/// closes inside January at the expiry; January's invoice bills exactly the
/// gauge up to the expiry, and February's carry bills nothing for the
/// replaced incarnation. A close at settlement time would have rolled the
/// row into February and billed January to its boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_month_crossing_recreation_bills_storage_only_up_to_the_expiry_instant() {
    let _clock = crate::billing::billing_clock_lock().write().await;
    let _reset = ClockReset;
    let case = idle_january().await;
    set_clock(month_start_ms(2026, 2) + 2 * HOUR);
    let got = recreate_and_settle(&case).await;
    assert_billed(
        &got,
        &expected_close(&case.before, case.expired_at),
        "the replaced row closes at its January expiry",
    );
    assert_eq!(
        ((got.month_year, got.month_month), got.month_byte_ms()),
        ((2026, 1), case.owed),
        "the close stays in January"
    );
    let jan_close = month_start_ms(2026, 2) + DAY + HOUR;
    assert_eq!(closed_months(&case.state, jan_close).await, ["2026-01"]);
    let feb_close = month_start_ms(2026, 3) + DAY + HOUR;
    assert_eq!(closed_months(&case.state, feb_close).await, ["2026-02"]);
    let epoch = case.old.stream_epoch.as_str();
    assert_eq!(
        (
            invoiced(&case.state, "2026-01", epoch).await,
            invoiced(&case.state, "2026-02", epoch).await,
            carried_gauge(&case.state, epoch).await,
        ),
        (case.owed, 0, 0),
        "January bills exactly up to the expiry; February carries nothing"
    );
    let fresh = stored(&case.state, &case.sref).await;
    assert!(
        invoiced(&case.state, "2026-02", &fresh.stream_epoch).await > 0,
        "the new incarnation bills February"
    );
    engine_shutdown(&case.state).await;
}

/// A raw stream name whose segment 0 lives on the same shard as `desc`'s:
/// an append to it is a barrier behind every op submitted to that
/// committer before it.
fn sibling_on_shard(state: &State, desc: &Desc) -> String {
    let shard = |name: &str| {
        let sref = state.deployment.raw_adapter_sref(name);
        state
            .shards
            .prefix_for(&crate::crypto::RouteHash::for_stream(&sref).0)
    };
    let want = state
        .shards
        .prefix_for(&desc.segment_route_by_id(0).expect("segment 0"));
    (0..256)
        .map(|i| format!("sib{i}"))
        .find(|name| shard(name) == want)
        .expect("a name on the same shard")
}

/// Owner requirement "crash between the debt write and the replacing
/// write": the debt is written (`record_replaced`, the first durable step of
/// `Registry::recreate`) and nothing replaces the incarnation. While it is
/// stored and dead the debt waits and the debt pass closes nothing; the
/// walk closes it at its expiry; the debt still waits. A later real
/// recreation keeps the existing debt, and a new process (no in-memory
/// cursor) settles and deletes it without a second close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_debt_written_before_a_crashed_replacement_waits_for_the_walk_and_settles_later() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let store = mem();
    let rig = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let sref = state.deployment.raw_adapter_sref("crash4");
    raw_put(addr, "crash4", &[JSON], ROW).await;
    let old = stored(&state, &sref).await;
    let id = old.dynamic_segment_identity(0);
    let engine = engine_of(&state, &old, 0).await;
    ack_clean(&state, &engine, &[id]).await;
    let before = engine.billing_meta(id).await.expect("a billed row");
    let sibling = sibling_on_shard(&state, &old);
    raw_put(addr, &sibling, &[JSON], b"").await;
    let expired_at = billing_now_ms();
    expire(&[&state], &sref, expired_at).await;
    let dead = stored(&state, &sref).await;
    state.registry.record_replaced(&dead).await.unwrap();
    let written = single_debt(&state, "after the crashed recreation's first step").await;
    assert_eq!(
        (written.debt.close_ms, settled(&written)),
        (expired_at, vec![])
    );
    for _ in 0..5 {
        settle_replaced(&state).await;
    }
    // Committer barrier: any close those passes submitted has landed.
    let (st, _, _) = hreq(addr, "POST", &format!("/v1/stream/{sibling}"), &[JSON], ROW).await;
    assert_eq!(st, 204, "the barrier append");
    let waiting = single_debt(&state, "while the incarnation is stored and dead").await;
    assert_eq!(
        (waiting.key.as_str(), settled(&waiting)),
        (written.key.as_str(), vec![]),
        "a debt of a stored, dead incarnation waits for the walk"
    );
    let open = engine.billing_meta(id).await.unwrap();
    assert_billed(&open, &before, "the debt pass closed a stored incarnation");
    let mut walked = None;
    for _ in 0..10 {
        crate::billing::tombstone_walk(&state).await;
        walked = closed(&engine, id).await;
        if walked.is_some() {
            break;
        }
    }
    let walked = walked.expect("the walk never closed the stored, dead incarnation");
    assert_billed(&walked, &expected_close(&before, expired_at), "walk close");
    for _ in 0..3 {
        settle_replaced(&state).await;
    }
    let waiting = single_debt(&state, "after the walk closed the incarnation").await;
    assert_eq!(waiting.key, written.key);
    raw_put(addr, "crash4", &[JSON], b"").await;
    let rerecorded = single_debt(&state, "after the real recreation").await;
    assert_eq!(
        (
            rerecorded.key.as_str(),
            rerecorded.debt.close_ms,
            settled(&rerecorded)
        ),
        (written.key.as_str(), expired_at, vec![]),
        "the real recreation keeps the existing debt"
    );
    drop(engine);
    rig.shutdown().await;
    let rig = http_rig_build(store, RigRuntime::incarnation(1), HttpRigOptions::default()).await;
    let state = rig.state.clone();
    for _ in 0..20 {
        settle_replaced(&state).await;
        if debts(&state).await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        debts(&state).await.is_empty(),
        "a new process never settled the debt"
    );
    let engine = engine_of(&state, &old, 0).await;
    let after = engine.billing_meta(id).await.unwrap();
    assert_billed(&after, &walked, "settlement closed the row a second time");
    drop(engine);
    rig.shutdown().await;
}

/// Waits until the store has parked the held operation.
async fn parked_within(engaged: &std::sync::atomic::AtomicU64, bound: Duration) {
    let deadline = std::time::Instant::now() + bound;
    while engaged.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the held write never parked"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Owner requirement "CAS losers": a recreation whose predicate judged the
/// incarnation dead writes its debt, then parks at its replacing write; a
/// real append renews the TTL through its own CAS meanwhile. The recreation
/// loses, re-reads, finds the incarnation live and replaces nothing. Once
/// the debt's own descriptor reads expired but the stored one does not, the
/// pass drops the debt and leaves the live gauge untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreation_that_loses_its_cas_to_a_ttl_renewal_leaves_a_debt_the_pass_drops() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let store = FaultStore::new(mem(), 505, FaultProfile::clean());
    let rig = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let sref = state.deployment.raw_adapter_sref("cas5");
    raw_put(addr, "cas5", &[JSON, ("stream-ttl", "120")], ROW).await;
    // Six seconds left of a two-minute window: the next append renews, and
    // the renewed incarnation stays live long past the lapse the pass waits
    // for, however slow the host.
    let lapse = crate::shard::now_ms() + 6000;
    expire(&[&state], &sref, lapse).await;
    let d0 = stored(&state, &sref).await;
    let id = d0.dynamic_segment_identity(0);
    let engine = engine_of(&state, &d0, 0).await;
    let hex = crate::crypto::hex;
    let path = format!(
        "registry/v4/projects/{}/streams/{}.json",
        hex(sref.project_id().as_bytes()),
        hex(sref.name().as_str().as_bytes())
    );
    let engaged = store.hold_class_under(StoreOp::Put, ObjClass::Other, &path, 1);
    let service = state.creation_service();
    let fresh = crate::application::creation::fresh_desc(
        &service,
        &sref,
        &skey(),
        "application/json".to_string(),
        None,
        None,
    );
    // A recreator whose instant is already past the expiry (a slow or skewed peer).
    let recreation = state.registry.recreate(&sref, fresh.0, |d| {
        crate::application::creation::recreatable(d, lapse + 1)
    });
    let renewal = async {
        parked_within(&engaged, Duration::from_secs(3)).await;
        let parked = single_debt(&state, "while the replacing write is parked").await;
        assert_eq!(
            parked.debt.close_ms, lapse,
            "the debt precedes the replacing write"
        );
        let (st, _, _) = hreq(addr, "POST", "/v1/stream/cas5", &[JSON], br#"[{"n":2}]"#).await;
        assert_eq!(st, 204);
        let renewed = stored(&state, &sref).await;
        let live = engine.billing_meta(id).await.unwrap();
        store.release_hold();
        (renewed, live)
    };
    let (outcome, (renewed, live)) = futures_util::future::join(recreation, renewal).await;
    let (created, winner) = outcome.unwrap();
    assert!(!created, "a renewed incarnation was replaced");
    assert_eq!(winner.stream_epoch, d0.stream_epoch);
    assert!(
        renewed.expires_at_ms > Some(lapse + 1),
        "the append renewed the TTL"
    );
    while billing_now_ms() <= lapse + 50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let lost = single_debt(&state, "after the lost recreation").await;
    assert_eq!(lost.debt.close_ms, lapse, "the debt outlives the lost CAS");
    for _ in 0..3 {
        settle_replaced(&state).await;
    }
    let after = engine.billing_meta(id).await.unwrap();
    assert!(
        after.owned_frame_bytes_current > 0,
        "the pass closed a live gauge"
    );
    assert_billed(&after, &live, "the pass touched a live row");
    assert!(
        debts(&state).await.is_empty(),
        "a renewed incarnation's debt stayed"
    );
    let now = stored(&state, &sref).await;
    assert_eq!(
        (now.stream_epoch.as_str(), now.expires_at_ms),
        (d0.stream_epoch.as_str(), renewed.expires_at_ms)
    );
    drop(engine);
    rig.shutdown().await;
}

/// Owner requirement "multi-segment incarnation": a split stream (sealed
/// segment 0 with data, live children 1 with data and 2 never billed)
/// replaced through the product surface. The first pass closes segments 0
/// and 1 and settles only 2, never a segment in the pass that submitted its
/// close; the next pass settles 0 and 1, and only that last settlement
/// deletes the debt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_split_incarnation_settles_segment_by_segment_and_only_then_forgets_its_debt() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    hub_split(addr, &state, "ms6").await;
    let sref = state.deployment.raw_adapter_sref("ms6");
    let old = stored(&state, &sref).await;
    keyed_append(addr, "ms6", &key_for(&old, 1)).await;
    let ids = [0, 1, 2].map(|s| old.dynamic_segment_identity(s));
    let engine = engine_of(&state, &old, 0).await;
    ack_clean(&state, &engine, &ids[..2]).await;
    let before = [
        engine.billing_meta(ids[0]).await.expect("segment 0 billed"),
        engine.billing_meta(ids[1]).await.expect("segment 1 billed"),
    ];
    assert!(
        before.iter().all(|m| m.owned_frame_bytes_current > 0),
        "no gauge to leak"
    );
    assert!(
        engine.billing_meta(ids[2]).await.is_none(),
        "segment 2 never billed"
    );
    let expired_at = billing_now_ms();
    expire(&[&state], &sref, expired_at).await;
    product_put(addr, "ms6").await;
    let debt = single_debt(&state, "after the product recreation").await;
    assert_eq!(
        (debt.debt.segments().unwrap(), settled(&debt)),
        (vec![0, 1, 2], vec![])
    );

    settle_replaced(&state).await;
    let debt = single_debt(&state, "after the first pass").await;
    assert_eq!(
        settled(&debt),
        [2],
        "one pass settles what was never billed, and never a segment it just closed"
    );
    let zero = closed(&engine, ids[0])
        .await
        .expect("the pass closes segment 0");
    let one = closed(&engine, ids[1])
        .await
        .expect("the pass closes segment 1");
    settle_replaced(&state).await;
    assert!(
        debts(&state).await.is_empty(),
        "the last settlement deletes the debt"
    );
    assert_billed(&zero, &expected_close(&before[0], expired_at), "segment 0");
    assert_billed(&one, &expected_close(&before[1], expired_at), "segment 1");
    for (id, want) in [(ids[0], &zero), (ids[1], &one)] {
        let after = engine.billing_meta(id).await.unwrap();
        assert_billed(&after, want, "a settled segment was closed again");
    }
    assert!(engine.billing_meta(ids[2]).await.is_none());
    engine_shutdown(&state).await;
}

/// A source `src` with one record and a fork `kid` reading through it, the
/// source row acked clean; `unforked` is the source as an instance that
/// never saw the fork holds it. The source's one-hour TTL gives it a
/// persisted close instant even while retained, so a debt written for it
/// would be visible.
struct Forked {
    unforked: Desc,
    kid: &'static str,
    engine: Engine,
    before: Meta,
}

async fn forked_source(state: &State, addr: Addr, src: &str, kid: &'static str) -> Forked {
    raw_put(addr, src, &[JSON, ("stream-ttl", "3600")], br#"[{"n":0}]"#).await;
    let unforked = stored(state, &state.deployment.raw_adapter_sref(src)).await;
    raw_put(addr, kid, &[JSON, ("stream-forked-from", src)], b"").await;
    let engine = engine_of(state, &unforked, 0).await;
    let id = unforked.dynamic_segment_identity(0);
    ack_clean(state, &engine, &[id]).await;
    let before = engine.billing_meta(id).await.expect("a billed source");
    assert!(before.owned_frame_bytes_current > 0);
    Forked {
        unforked,
        kid,
        engine,
        before,
    }
}

/// Neither surface replaces a source its fork still reads: no debt is
/// written, its incarnation and fork children are unchanged, and the fork
/// still reads through it.
async fn assert_never_replaced(state: &State, addr: Addr, forked: &Forked) {
    let sref = forked.unforked.sref();
    let name = sref.name().as_str().to_string();
    let retained = stored(state, &sref).await;
    assert_eq!(retained.fork_children.len(), 1, "the fork pins its source");
    // A snapshot from before the fork: the raw surface reaches the recreate CAS.
    let mut stale = forked.unforked.to_persisted();
    stale.expires_at_ms = Some(crate::shard::now_ms() - 1);
    state
        .registry
        .test_poison_cache(&sref, Desc::try_from(stale).unwrap());
    let (st, _, b) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), &[JSON], b"").await;
    let body = String::from_utf8_lossy(&b).to_string();
    assert!(st == 409 && body.contains("gone"), "raw: {st} {body}");
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let spec = br#"{"format":{"kind":"json"}}"#;
    let (st, _, b) = preq(addr, "PUT", &format!("/v1/streams/{name}"), &key, spec).await;
    let body = String::from_utf8_lossy(&b).to_string();
    assert!(st == 409 && body.contains("gone"), "product: {st} {body}");
    assert!(
        debts(state).await.is_empty(),
        "a retained incarnation was given a debt"
    );
    let after = stored(state, &sref).await;
    assert_eq!(
        (&after.stream_epoch, &after.fork_children),
        (&retained.stream_epoch, &retained.fork_children)
    );
    let (st, _, b) = hreq(addr, "GET", &format!("/v1/stream/{}", forked.kid), &[], b"").await;
    assert_eq!(st, 200, "the fork lost its source");
    assert_eq!(
        serde_json::from_slice::<Vec<serde_json::Value>>(&b)
            .unwrap()
            .len(),
        1
    );
}

/// A source its fork still reads is never replaced, and its storage keeps
/// billing, flagged retained, through the sweeps.
async fn assert_retained_and_billed(state: &State, addr: Addr, forked: &Forked) {
    assert_never_replaced(state, addr, forked).await;
    for _ in 0..3 {
        crate::billing::sweep_owned_outboxes(state).await;
        crate::billing::drain_once(state).await.expect("drain");
    }
    let id = forked.unforked.dynamic_segment_identity(0);
    let mut meta = forked.engine.billing_meta(id).await.unwrap();
    for _ in 0..20 {
        if meta.retained_by_forks || meta.owned_frame_bytes_current == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        meta = forked.engine.billing_meta(id).await.unwrap();
    }
    assert_eq!(
        (meta.owned_frame_bytes_current, meta.retained_by_forks),
        (forked.before.owned_frame_bytes_current, true),
        "the storage its fork still reads stopped billing"
    );
}

/// Owner requirement "fork retention", soft-deleted source: deleting a
/// source its fork reads retains it; neither surface replaces it, no debt is
/// written, and its storage line stays active with `retained_by_forks`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_soft_deleted_source_is_never_replaced_and_keeps_billing() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    let forked = forked_source(&state, addr, "ret7", "ret7-kid").await;
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/ret7", &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    let soft = stored(&state, &forked.unforked.sref()).await;
    assert!(soft.soft_deleted && !soft.deleted, "retained, not deleted");
    assert_retained_and_billed(&state, addr, &forked).await;
    engine_shutdown(&state).await;
}

/// Owner decision (2026-09-29), expired source: a source that expires while
/// its fork reads through it is retained for recreation
/// (`creation::retained_for_forks`), so neither surface replaces it and no
/// debt is written, and the fork still reads. Its storage, unlike a
/// soft-deleted source's (above), stops billing at the expiry: the walk
/// closes the row exactly at the persisted instant and does not flag it
/// retained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_source_its_fork_reads_is_never_replaced_and_stops_billing_at_expiry() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    let forked = forked_source(&state, addr, "exp7", "exp7-kid").await;
    let expired_at = crate::shard::now_ms() - 1;
    let lapsed = state
        .registry
        .cas_update_retry(&forked.unforked.sref(), |d| {
            d.expires_at_ms = Some(expired_at);
            true
        })
        .await
        .unwrap();
    assert!(lapsed);
    assert_never_replaced(&state, addr, &forked).await;
    for _ in 0..3 {
        crate::billing::sweep_owned_outboxes(&state).await;
        crate::billing::drain_once(&state).await.expect("drain");
    }
    let id = forked.unforked.dynamic_segment_identity(0);
    let got = closed(&forked.engine, id)
        .await
        .expect("the walk closes the expired source's storage");
    assert_billed(
        &got,
        &expected_close(&forked.before, expired_at),
        "the expired source stops billing at its expiry",
    );
    assert!(
        !got.retained_by_forks,
        "an expired source is not retained for billing"
    );
    engine_shutdown(&state).await;
}
