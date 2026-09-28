//! Closure debts across two instances (NEXT-WORK §2, owner movement): each
//! instance closes only the replaced segments it owns, the debt outlives
//! every unsettled segment, debts owned by different instances all settle,
//! and a debt waiting for the tombstone walk is reached on the instance that
//! owns its incarnation. One contract with `billing_closure_debts.rs`, whose
//! helpers it shares.

use super::billing_closure_debts::{
    JSON, ROW, ack_clean, assert_billed, closed, debts, engine_of, expected_close, expire,
    hub_split, key_for, keyed_append, product_put, raw_put, settled, single_debt, stored,
};
use super::fixture_http::{HttpRig, http_rig_owner_whole};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::billing::billing_now_ms;
use crate::billing::replaced::settle_replaced;
use std::collections::HashMap;
use std::time::Duration;

const PREFIXES: [&str; 4] = ["00", "01", "10", "11"];

/// Two instances over one store, every shard owned by inst-a until a
/// scenario moves one.
async fn owner_pair() -> (HttpRig, HttpRig) {
    let store = mem();
    let a = http_rig_owner_whole(store.clone(), "inst-a", RigRuntime::first()).await;
    let b = http_rig_owner_whole(store, "inst-b", RigRuntime::incarnation(1)).await;
    let active = vec!["inst-a".to_string(), "inst-b".to_string()];
    let on_a: HashMap<String, String> = PREFIXES
        .iter()
        .map(|p| ((*p).to_string(), "inst-a".to_string()))
        .collect();
    for rig in [&a, &b] {
        rig.state.ownership.set_view(active.clone(), on_a.clone());
    }
    a.state
        .peer
        .set_peer("inst-b", &format!("http://{}", b.addr));
    b.state
        .peer
        .set_peer("inst-a", &format!("http://{}", a.addr));
    (a, b)
}

/// Moves shard `prefix` to instance `to` in both instances' views.
fn move_prefix(rigs: [&HttpRig; 2], prefix: &str, to: &str) {
    for rig in rigs {
        rig.state.ownership.set_override(prefix, to);
    }
}

/// Owner requirement "owner movement": a split incarnation whose sealed
/// segment 0 and low child 1 live on inst-a's shard and whose high child 2
/// lives on another shard, replaced through the product surface. Before
/// settlement, segment 2's shard moves to inst-b, which opens it (a yields
/// it). inst-a closes and settles 0 and 1 and never touches 2; the debt
/// stays while 2 is unsettled; inst-b then closes 2 at the same persisted
/// expiry and its settlement deletes the debt. Every segment closes once.
///
/// The debt pass (`settle_replaced`) is driven directly rather than through
/// the production sweep: the replaced descriptor is gone from the catalog,
/// so the pass is the only closer here, and driving it pins which instance
/// runs each step and lets the settled set be read between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_instance_closes_only_the_replaced_segments_it_owns() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (a, b) = owner_pair().await;
    hub_split(a.addr, &a.state, "mv").await;
    let sref = a.state.deployment.raw_adapter_sref("mv");
    let old = stored(&a.state, &sref).await;
    let route = |s: u32| old.segment_route_by_id(s).unwrap();
    let (p0, p2) = (
        a.state.shards.prefix_for(&route(0)),
        a.state.shards.prefix_for(&route(2)),
    );
    assert_eq!(
        a.state.shards.prefix_for(&route(1)),
        p0,
        "the low child keeps the parent's shard"
    );
    assert_ne!(p0, p2, "the high child is salted onto another shard");
    for seg in [1, 2] {
        keyed_append(a.addr, "mv", &key_for(&old, seg)).await;
    }
    let ids = [0, 1, 2].map(|s| old.dynamic_segment_identity(s));
    let e0 = engine_of(&a.state, &old, 0).await;
    let e2 = engine_of(&a.state, &old, 2).await;
    ack_clean(&a.state, &e0, &ids[..2]).await;
    ack_clean(&a.state, &e2, &ids[2..]).await;
    let before = [
        e0.billing_meta(ids[0]).await.expect("segment 0 billed"),
        e0.billing_meta(ids[1]).await.expect("segment 1 billed"),
        e2.billing_meta(ids[2]).await.expect("segment 2 billed"),
    ];
    drop(e2);
    let expired_at = billing_now_ms();
    expire(&[&a.state, &b.state], &sref, expired_at).await;
    product_put(a.addr, "mv").await;
    b.state.registry.invalidate(&sref);
    let debt = single_debt(&a.state, "after the product recreation").await;
    assert_eq!(debt.debt.segments().unwrap(), [0, 1, 2]);

    move_prefix([&a, &b], &p2, "inst-b");
    assert!(
        a.state.engine_for_quiet(&route(2)).await.is_err(),
        "inst-a kept the moved shard"
    );
    assert!(
        a.state.shards.open(&p2).is_none(),
        "inst-a still holds the moved shard resident"
    );
    let Ok(b2) = b.state.engine_for_quiet(&route(2)).await else {
        panic!("inst-b did not open the shard it now owns");
    };
    for _ in 0..3 {
        settle_replaced(&a.state).await;
        closed(&e0, ids[0]).await.expect("inst-a closes segment 0");
        closed(&e0, ids[1]).await.expect("inst-a closes segment 1");
    }
    let debt = single_debt(&a.state, "after inst-a's passes").await;
    assert_eq!(settled(&debt), [0, 1], "inst-a settles only what it owns");
    let open = b2.billing_meta(ids[2]).await.unwrap();
    assert_billed(&open, &before[2], "inst-a closed a segment it does not own");

    settle_replaced(&b.state).await;
    closed(&b2, ids[2]).await.expect("inst-b closes segment 2");
    let debt = single_debt(&a.state, "while segment 2 is closed but unsettled").await;
    assert_eq!(settled(&debt), [0, 1]);
    settle_replaced(&b.state).await;
    assert!(
        debts(&a.state).await.is_empty(),
        "the last settlement deletes the debt"
    );
    for (engine, seg) in [(&e0, 0), (&e0, 1), (&b2, 2)] {
        let got = engine.billing_meta(ids[seg]).await.unwrap();
        assert_billed(
            &got,
            &expected_close(&before[seg], expired_at),
            "segment close",
        );
    }
    drop((e0, b2));
    a.shutdown().await;
    b.shutdown().await;
}

/// Two names whose streams live on different shards.
fn names_on_two_shards(a: &HttpRig) -> [(String, String); 2] {
    let prefix = |name: &str| {
        let sref = a.state.deployment.raw_adapter_sref(name);
        a.state
            .shards
            .prefix_for(&crate::crypto::RouteHash::for_stream(&sref).0)
    };
    let x = "dx".to_string();
    let y = (0..64)
        .map(|i| format!("dy{i}"))
        .find(|y| prefix(y) != prefix(&x))
        .expect("a name on another shard");
    let (px, py) = (prefix(&x), prefix(&y));
    [(x, px), (y, py)]
}

/// Owner requirement "owner movement", two debts: one replaced incarnation
/// on each instance's shard. Every instance runs the pass over the whole
/// cell's debts, so each must get past the other's debt to reach and settle
/// its own; both old gauges must close at the expiry and both debts settle.
///
/// The debt pass is driven directly rather than through the production
/// sweep: both descriptors are replaced, so the walk cannot find them, and
/// no drain runs in these rigs, which leaves the pass their only closer;
/// driving it alone keeps the walk's own handling of foreign shards (the
/// last test here) out of this verdict.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closure_debts_owned_by_different_instances_all_settle() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (a, b) = owner_pair().await;
    let [(x, _), (y, py)] = names_on_two_shards(&a);
    move_prefix([&a, &b], &py, "inst-b");
    raw_put(a.addr, &x, &[JSON], ROW).await;
    raw_put(b.addr, &y, &[JSON], ROW).await;
    let (sx, sy) = (
        a.state.deployment.raw_adapter_sref(&x),
        a.state.deployment.raw_adapter_sref(&y),
    );
    let (ox, oy) = (stored(&a.state, &sx).await, stored(&b.state, &sy).await);
    let (ex, ey) = (
        engine_of(&a.state, &ox, 0).await,
        engine_of(&b.state, &oy, 0).await,
    );
    let (ix, iy) = (
        ox.dynamic_segment_identity(0),
        oy.dynamic_segment_identity(0),
    );
    let expired_at = billing_now_ms();
    expire(&[&a.state, &b.state], &sx, expired_at).await;
    expire(&[&a.state, &b.state], &sy, expired_at).await;
    raw_put(a.addr, &x, &[JSON], b"").await;
    raw_put(b.addr, &y, &[JSON], b"").await;
    for sref in [&sx, &sy] {
        a.state.registry.invalidate(sref);
        b.state.registry.invalidate(sref);
    }
    assert_eq!(debts(&a.state).await.len(), 2);
    for _ in 0..20 {
        settle_replaced(&a.state).await;
        settle_replaced(&b.state).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let (gx, gy) = (
        ex.billing_meta(ix).await.unwrap(),
        ey.billing_meta(iy).await.unwrap(),
    );
    let left = debts(&a.state).await.len();
    assert_eq!(
        (
            (
                gx.owned_frame_bytes_current,
                gx.storage_accounted_through_ms
            ),
            (
                gy.owned_frame_bytes_current,
                gy.storage_accounted_through_ms
            ),
            left,
        ),
        ((0, expired_at), (0, expired_at), 0),
        "each instance stops its pass at the other's debt (a segment it may not \
         open), so one instance never settles the debt it closed and the other \
         never reaches its own: a replaced gauge stays open forever"
    );
    drop((ex, ey));
    a.shutdown().await;
    b.shutdown().await;
}

/// Owner requirement "crash between the debt write and the replacing
/// write", across two instances: the debt of a stored, dead incarnation
/// waits for the tombstone walk to close it, so every instance's walk must
/// reach the dead incarnations it owns. inst-a's stream `dx` and inst-b's
/// `dy*` both expire idle; inst-b's recreation of `dy*` crashes after its
/// debt write. The production sweeps on both instances must close both
/// gauges at the expiry, and the crash-left debt keeps waiting (its
/// incarnation is still stored).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_left_debt_is_walked_closed_behind_another_instances_dead_stream() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (a, b) = owner_pair().await;
    let [(x, _), (y, py)] = names_on_two_shards(&a);
    move_prefix([&a, &b], &py, "inst-b");
    raw_put(a.addr, &x, &[JSON], ROW).await;
    raw_put(b.addr, &y, &[JSON], ROW).await;
    let (sx, sy) = (
        a.state.deployment.raw_adapter_sref(&x),
        a.state.deployment.raw_adapter_sref(&y),
    );
    let (ox, oy) = (stored(&a.state, &sx).await, stored(&b.state, &sy).await);
    let (ex, ey) = (
        engine_of(&a.state, &ox, 0).await,
        engine_of(&b.state, &oy, 0).await,
    );
    let (ix, iy) = (
        ox.dynamic_segment_identity(0),
        oy.dynamic_segment_identity(0),
    );
    let expired_at = billing_now_ms();
    expire(&[&a.state, &b.state], &sx, expired_at).await;
    expire(&[&a.state, &b.state], &sy, expired_at).await;
    let dead = stored(&b.state, &sy).await;
    b.state.registry.record_replaced(&dead).await.unwrap();
    for _ in 0..20 {
        crate::billing::sweep_owned_outboxes(&a.state).await;
        crate::billing::sweep_owned_outboxes(&b.state).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let (gx, gy) = (
        ex.billing_meta(ix).await.unwrap(),
        ey.billing_meta(iy).await.unwrap(),
    );
    let waiting = single_debt(&b.state, "while its incarnation is stored and dead").await;
    assert_eq!(waiting.debt.descriptor.stream_epoch, oy.stream_epoch);
    assert_eq!(
        (
            (
                gx.owned_frame_bytes_current,
                gx.storage_accounted_through_ms
            ),
            (
                gy.owned_frame_bytes_current,
                gy.storage_accounted_through_ms
            ),
        ),
        ((0, expired_at), (0, expired_at)),
        "inst-b's walk stops at inst-a's dead stream (a shard it may not open) on \
         every sweep and never reaches its own: the debt waits for a walk that \
         never comes, and the gauge stays open"
    );
    drop((ex, ey));
    a.shutdown().await;
    b.shutdown().await;
}
