//! The tombstone walk's shard custody across a page: a shard the walk
//! cold-opens for one segment is handed back before the walk moves on,
//! whatever it found there, so an open that had nothing to close never
//! holds a slot of the sweep's resident budget against the segments after
//! it on the page; and a shard the walk or the closure-debt pass enqueued
//! a close or a retention flag on is kept until its committer applies it.

use super::billing_closure_debts::{
    JSON, ROW, assert_billed, closed, debts as closure_debts, engine_of, expected_close, expire,
    raw_put, settled, single_debt, stored,
};
use super::fixture_failpoints::sweep_lock;
use super::fixture_http::{
    HttpRig, HttpRigOptions, engine_shutdown, http_rig_build, http_rig_opts,
};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::billing::replaced::settle_replaced;
use crate::billing::{SegmentBillingMetaV1 as Meta, WALK_CLOSE_SUBMITS, billing_now_ms};
use crate::shard::ShardConfig;
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

type State = Arc<crate::http::AppState>;
type Ref = crate::tenant::TenantStreamRef;
type Desc = crate::registry::StreamDesc;
type Engine = Arc<crate::shard::ShardEngine>;

const TTL: (&str, &str) = ("stream-ttl", "3600");

/// Sixteen shards (the 4-bit prefix code): twice the reach of one sweep's
/// discovery (`sweep_discovery_max`, 8), so the shards past it stay cold
/// for the walk to open itself.
fn prefixes() -> Vec<String> {
    (0..16).map(|i| format!("{i:04b}")).collect()
}

/// The shard `name`'s stream lives on.
fn shard_of(state: &State, name: &str) -> String {
    let sref = state.deployment.raw_adapter_sref(name);
    state
        .shards
        .prefix_for(&crate::crypto::RouteHash::for_stream(&sref).0)
}

/// One name per stem, each on its own shard past the first sweep's
/// discovery: a fresh rig's first sweep rotates from the first configured
/// prefix and opens `sweep_discovery_max` shards, so it never reaches these
/// and the walk opens them itself.
fn names_past_discovery(state: &State, stems: [&str; 3]) -> [(String, String); 3] {
    let reach = state.config.billing.sweep_discovery_max;
    let all = state.shards.prefixes();
    assert!(all.len() > reach, "discovery must not reach every shard");
    let past = &all[reach..];
    let mut taken: Vec<String> = Vec::new();
    stems.map(|stem| {
        let name = (0..4096)
            .map(|i| format!("{stem}{i:04}"))
            .find(|n| {
                let shard = shard_of(state, n);
                past.contains(&shard) && !taken.contains(&shard)
            })
            .expect("a name on a free shard past discovery");
        let shard = shard_of(state, &name);
        taken.push(shard.clone());
        (name, shard)
    })
}

/// Whether any open shard carries (billing, maintenance) debt: what the
/// sweep keeps a resident for (dirty rows or month finals; unabsorbed
/// frames or trim debt).
async fn debts(state: &State) -> (bool, bool) {
    let (mut billing, mut maintenance) = (false, false);
    for engine in state.shards.engines() {
        billing |= engine.has_billing_debt().await.unwrap_or(true);
        maintenance |=
            engine.maintenance_snapshot().unabsorbed_frame_bytes > 0 || engine.trim_stats().0 > 0;
    }
    (billing, maintenance)
}

/// Drains billing and waits for absorption until no shard carries any debt,
/// so every residency in the sweeps under test is the walk's own.
async fn quiesce(state: &State) {
    for _ in 0..200 {
        match debts(state).await {
            (false, false) => return,
            (true, _) => {
                crate::billing::drain_once(state).await.expect("drain");
            }
            (false, true) => {}
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the shards never quiesced");
}

/// Process incarnation `n` over `store`, with the sixteen shards and
/// `shard` as every engine's configuration.
async fn rig_over(store: &Arc<dyn ObjectStore>, n: u64, shard: ShardConfig) -> HttpRig {
    let opts = HttpRigOptions {
        prefixes: prefixes(),
        shard,
        ..HttpRigOptions::default()
    };
    http_rig_build(store.clone(), RigRuntime::incarnation(n), opts).await
}

/// The lever that fixes the losing order: a committer that takes even one
/// op gathers company for two seconds before it commits, so a close the
/// sweep enqueues is still unapplied when the sweep settles the shard, and
/// a group whose engine closed meanwhile is rejected whole.
fn gathering() -> ShardConfig {
    ShardConfig {
        pace_min_reqs: 1,
        gather_window: Duration::from_secs(2),
        ..ShardConfig::default()
    }
}

/// `id`'s row on `desc`'s segment-0 engine once no shard carries any debt:
/// open (a gauge to close) and clean (acked, so the drain never revisits it
/// and only the walk or the debt pass can close it).
async fn clean_open_row(state: &State, desc: &Desc, id: [u8; 16]) -> Meta {
    quiesce(state).await;
    let engine = engine_of(state, desc, 0).await;
    let row = engine.billing_meta(id).await.expect("a billed row");
    assert!(row.owned_frame_bytes_current > 0, "no gauge to close");
    let dirty = engine.usage_dirty_scan().await.unwrap();
    assert!(
        dirty.iter().all(|(hash, _)| *hash != id),
        "the row is clean: only the walk or the debt pass can close it"
    );
    row
}

/// `id`'s row on `engine` as soon as `applied` holds for it within five
/// seconds (an enqueued op lands within one two-second gather, or two when
/// a group was already gathering), otherwise as it stands.
async fn row_once(engine: &Engine, id: [u8; 16], applied: fn(&Meta) -> bool) -> Meta {
    let mut row = engine.billing_meta(id).await.expect("the row");
    for _ in 0..250 {
        if applied(&row) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        row = engine.billing_meta(id).await.expect("the row");
    }
    row
}

/// A row whose storage gauge is closed.
fn gauge_closed(row: &Meta) -> bool {
    row.owned_frame_bytes_current == 0
}

/// Fable review: the walk returned early for a segment with no billing row
/// (or another incarnation's row) and skipped handing back the shard it had
/// cold-opened for it. Two such dead streams on two cold shards filled the
/// default resident budget (2), the gauged dead stream after them on the
/// page was deferred, and every later sweep replayed the page from the
/// start: phase 1 closed the debt-free leftovers, the walk re-opened them
/// for the same segments or stopped at their holdoff, and never reached it.
/// The production sweep must close that gauge at its persisted expiry, and
/// must not keep the empty streams' shards after the walk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shard_the_walk_opened_for_nothing_to_close_is_handed_back_before_the_next_segment() {
    let _serial = sweep_lock().lock().await;
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) =
        http_rig_opts(mem(), prefixes(), crate::shard::ShardConfig::default()).await;
    let [(xa, pa), (xb, pb), (y, py)] =
        names_past_discovery(&state, ["gone-a", "gone-b", "held-y"]);
    let sref = |name: &str| state.deployment.raw_adapter_sref(name);
    let (sa, sb, sy) = (sref(&xa), sref(&xb), sref(&y));
    // Created with a TTL and never appended to: no billing row.
    raw_put(addr, &xa, &[JSON, TTL], b"").await;
    raw_put(addr, &xb, &[JSON, TTL], b"").await;
    raw_put(addr, &y, &[JSON, TTL], ROW).await;
    for empty in [&sa, &sb] {
        let desc = stored(&state, empty).await;
        let engine = engine_of(&state, &desc, 0).await;
        let row = engine.billing_meta(desc.dynamic_segment_identity(0)).await;
        assert!(row.is_none(), "{empty} has no billing row");
    }
    let oy = stored(&state, &sy).await;
    let iy = oy.dynamic_segment_identity(0);
    quiesce(&state).await;
    let before = engine_of(&state, &oy, 0)
        .await
        .billing_meta(iy)
        .await
        .unwrap();
    assert!(before.owned_frame_bytes_current > 0, "no gauge to close");
    let expired_at = billing_now_ms();
    for dead in [&sa, &sb, &sy] {
        expire(&[&state], dead, expired_at).await;
    }
    // Every shard cold, as after a restart; the retirement holdoffs cleared.
    engine_shutdown(&state).await;
    let page = state.registry.reconciliation_page(None, 256).await.unwrap();
    let at = |s: &Ref| {
        let on_page = page.streams.iter().position(|d| &d.sref() == s);
        on_page.expect("every dead stream is on the walk's first page")
    };
    assert!(
        at(&sa) < at(&sy) && at(&sb) < at(&sy),
        "the walk meets both empty streams before the gauged one"
    );

    crate::billing::sweep_owned_outboxes(&state).await;
    let mut held = state.billing.sweep_custody_prefixes();
    held.retain(|p| *p == pa || *p == pb);
    crate::billing::sweep_owned_outboxes(&state).await;
    crate::billing::sweep_owned_outboxes(&state).await;
    state.shards.clear_holdoff(&py);
    let engine = engine_of(&state, &oy, 0).await;
    let row = match closed(&engine, iy).await {
        Some(row) => row,
        None => engine.billing_meta(iy).await.unwrap(),
    };
    assert_eq!(
        (
            held,
            (
                row.owned_frame_bytes_current,
                row.storage_accounted_through_ms
            )
        ),
        (Vec::<String>::new(), (0, expired_at)),
        "the walk kept the empty streams' shards after their segments and \
         deferred the gauged stream behind them on every sweep"
    );
    assert_billed(
        &row,
        &expected_close(&before, expired_at),
        "the gauged row closes exactly at its persisted expiry",
    );
    drop(engine);
    engine_shutdown(&state).await;
}

/// The lost close (NEXT-WORK, confirmed at 88015cc5): the walk enqueued the
/// close of a dead incarnation's open, clean row on a shard it cold-opened,
/// then settled that shard at once. The debt probe reads only the durable
/// dirty index, which a close still in the committer queue has not written,
/// so the walk retired the engine and its committer dropped the close: the
/// gauge stayed open, carried onto every later month, until a revisit a
/// whole catalog pass later, which could lose it the same way. A committer
/// that gathers for two seconds makes that order certain. The walk must keep
/// the shard until the close is applied, once, at the persisted expiry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_the_walk_submits_on_a_shard_it_opened_is_applied_before_the_shard_is_handed_back()
{
    let _serial = sweep_lock().lock().await;
    let _clock = crate::billing::billing_clock_lock().read().await;
    let store = mem();
    let setup = rig_over(&store, 0, ShardConfig::default()).await;
    let y = "lone-close-y";
    let sy = setup.state.deployment.raw_adapter_sref(y);
    raw_put(setup.addr, y, &[JSON, TTL], ROW).await;
    let oy = stored(&setup.state, &sy).await;
    let iy = oy.dynamic_segment_identity(0);
    let before = clean_open_row(&setup.state, &oy, iy).await;
    let expired_at = billing_now_ms();
    expire(&[&setup.state], &sy, expired_at).await;
    // A new process: every shard cold, so the walk opens `y`'s itself.
    setup.shutdown().await;
    let rig = rig_over(&store, 1, gathering()).await;
    let state = &rig.state;

    let submits = WALK_CLOSE_SUBMITS.load(Relaxed);
    crate::billing::tombstone_walk(state).await;
    assert!(
        WALK_CLOSE_SUBMITS.load(Relaxed) > submits,
        "the walk enqueued the close"
    );
    let held = state.billing.sweep_custody_prefixes();
    let py = shard_of(state, y);
    state.shards.clear_holdoff(&py);
    let engine = engine_of(state, &oy, 0).await;
    let row = row_once(&engine, iy, gauge_closed).await;
    assert_billed(
        &row,
        &expected_close(&before, expired_at),
        "the walk handed back the shard it opened before its committer \
         applied the close it enqueued there",
    );
    let dirty = engine.usage_dirty_scan().await.unwrap();
    assert!(
        dirty.contains(&(iy, before.usage_version + 1)),
        "the terminal observation waits for the drain: {dirty:?}"
    );
    assert_eq!(held, vec![py], "the walk kept the shard it closed on");
    let d = stored(state, &sy).await;
    assert_eq!(
        (d.expires_at_ms, d.deleted, d.stream_epoch.as_str()),
        (Some(expired_at), false, oy.stream_epoch.as_str()),
        "the walk closes billing only; the descriptor is untouched"
    );
    drop(engine);
    rig.shutdown().await;
}

/// The same lost op for the fork-retention flag: the walk enqueued
/// `retained_by_forks` on the clean row of a soft-deleted source its fork
/// still reads, on a shard it cold-opened, and handed the shard back before
/// the committer applied it. The walk must keep the shard until the flag is
/// on the row, once, moving no billed figure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retention_flag_the_walk_submits_on_a_shard_it_opened_is_applied_before_the_shard_is_handed_back()
 {
    let _serial = sweep_lock().lock().await;
    let _clock = crate::billing::billing_clock_lock().read().await;
    let store = mem();
    let setup = rig_over(&store, 0, ShardConfig::default()).await;
    let (src, kid) = ("lone-source-r", "lone-source-r-kid");
    let sr = setup.state.deployment.raw_adapter_sref(src);
    raw_put(setup.addr, src, &[JSON], ROW).await;
    raw_put(setup.addr, kid, &[JSON, ("stream-forked-from", src)], b"").await;
    let or = stored(&setup.state, &sr).await;
    let ir = or.dynamic_segment_identity(0);
    let before = clean_open_row(&setup.state, &or, ir).await;
    let path = format!("/v1/stream/{src}");
    let (st, _, _) = hreq(setup.addr, "DELETE", &path, &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    let soft = stored(&setup.state, &sr).await;
    assert!(soft.soft_deleted && !soft.deleted, "retained, not deleted");
    let unflagged = clean_open_row(&setup.state, &or, ir).await;
    assert_eq!(
        (unflagged.retained_by_forks, unflagged.usage_version),
        (false, before.usage_version),
        "nothing flagged the row yet: only the walk can"
    );
    setup.shutdown().await;
    let rig = rig_over(&store, 1, gathering()).await;
    let state = &rig.state;

    crate::billing::tombstone_walk(state).await;
    state.shards.clear_holdoff(&shard_of(state, src));
    let engine = engine_of(state, &or, 0).await;
    let row = row_once(&engine, ir, |row| row.retained_by_forks).await;
    assert_eq!(
        (row.retained_by_forks, row.usage_version),
        (true, before.usage_version + 1),
        "the walk handed back the shard it opened before its committer \
         applied the retention flag it enqueued there"
    );
    let mut flagged = before.clone();
    flagged.usage_version += 1;
    assert_billed(&row, &flagged, "the flag moves no billed figure");
    drop(engine);
    rig.shutdown().await;
}

/// The same lost close in the closure-debt pass (`replaced.rs`): it enqueued
/// the close of a replaced incarnation's open, clean row on a shard it
/// cold-opened and settled that shard at once, so the committer dropped it.
/// The debt survives to retry it, and every retry met the same hand-back.
/// The pass must keep the shard until the close is applied at the debt's
/// instant; the next pass then settles the debt and forgets it without a
/// second close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_the_debt_pass_submits_on_a_shard_it_opened_is_applied_before_the_shard_is_handed_back()
 {
    let _serial = sweep_lock().lock().await;
    let _clock = crate::billing::billing_clock_lock().read().await;
    let store = mem();
    let setup = rig_over(&store, 0, ShardConfig::default()).await;
    let z = "lone-debt-z";
    let sz = setup.state.deployment.raw_adapter_sref(z);
    raw_put(setup.addr, z, &[JSON, TTL], ROW).await;
    let old = stored(&setup.state, &sz).await;
    let id = old.dynamic_segment_identity(0);
    let before = clean_open_row(&setup.state, &old, id).await;
    let expired_at = billing_now_ms();
    expire(&[&setup.state], &sz, expired_at).await;
    // The recreation must judge the incarnation expired, not at its expiry.
    while billing_now_ms() < expired_at + 100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    raw_put(setup.addr, z, &[JSON], b"").await;
    let fresh = stored(&setup.state, &sz).await;
    assert_ne!(fresh.stream_epoch, old.stream_epoch, "a new incarnation");
    let unsettled = clean_open_row(&setup.state, &old, id).await;
    assert_billed(&unsettled, &before, "nothing closed the replaced row yet");
    let debt = single_debt(&setup.state, "after the recreation").await;
    assert_eq!((debt.debt.close_ms, settled(&debt)), (expired_at, vec![]));
    setup.shutdown().await;
    let rig = rig_over(&store, 1, gathering()).await;
    let state = &rig.state;

    let submits = WALK_CLOSE_SUBMITS.load(Relaxed);
    settle_replaced(state).await;
    assert!(
        WALK_CLOSE_SUBMITS.load(Relaxed) > submits,
        "the debt pass enqueued the close"
    );
    state.shards.clear_holdoff(&shard_of(state, z));
    let engine = engine_of(state, &old, 0).await;
    let row = row_once(&engine, id, gauge_closed).await;
    assert_billed(
        &row,
        &expected_close(&before, expired_at),
        "the debt pass handed back the shard it opened before its committer \
         applied the close it enqueued there",
    );
    let waiting = single_debt(state, "after the pass that closed the row").await;
    assert_eq!(
        (waiting.key.as_str(), settled(&waiting)),
        (debt.key.as_str(), vec![]),
        "the debt waits for its owner to find the row closed"
    );
    settle_replaced(state).await;
    assert!(
        closure_debts(state).await.is_empty(),
        "the next pass forgot the debt"
    );
    let after = engine.billing_meta(id).await.unwrap();
    assert_billed(&after, &row, "settlement closed the row a second time");
    drop(engine);
    rig.shutdown().await;
}
