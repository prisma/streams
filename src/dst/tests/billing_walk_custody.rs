//! The tombstone walk's shard custody across a page: a shard the walk
//! cold-opens for one segment is handed back before the walk moves on,
//! whatever it found there, so an open that had nothing to close never
//! holds a slot of the sweep's resident budget against the segments after
//! it on the page.

use super::billing_closure_debts::{
    JSON, ROW, assert_billed, closed, engine_of, expected_close, expire, raw_put, stored,
};
use super::fixture_failpoints::sweep_lock;
use super::fixture_http::{engine_shutdown, http_rig_opts};
use super::fixture_storage::mem;
use crate::billing::billing_now_ms;
use std::sync::Arc;
use std::time::Duration;

type State = Arc<crate::http::AppState>;
type Ref = crate::tenant::TenantStreamRef;

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
