#![cfg(test)]
//! Physical reclamation of dead incarnations end to end (layout 6,
//! package 6): the production sweep (tombstone walk, then the closure-debt
//! pass) asks each owned shard to purge an irrevocably dead incarnation's
//! segments, and the shard's collector purges and reclaims them once the
//! billing close reached the ledger.
//! - A hard-deleted stream: its tail goes, its sealed chunks are
//!   decremented and its objects deleted, and its handle never loads again.
//! - An expired stream becomes a tombstone once its expiry is
//!   `EXPIRY_TOMBSTONE_DELAY_MS` old (edge #231), and is purged then: a
//!   renewal that landed before the tombstone's CAS keeps it alive, one that
//!   lands after is refused. Within the delay nothing is purged; a
//!   recreation of its name purges the replaced incarnation and leaves the
//!   new one alone.
//! - A source a fork reads through is not purged until the fork is gone.

use super::billing_closure_debts::{JSON, ROW, engine_of, expire, raw_put, stored};
use super::fixture_billing::{DrainWait, ack_clean, billing_clock_past};
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use crate::billing::billing_now_ms;
use crate::spill::gc::HeadState;
use crate::spill::object::ObjectId;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

type State = Arc<crate::http::AppState>;
type Engine = Arc<crate::shard::ShardEngine>;
type Addr = std::net::SocketAddr;

/// The collector of `engine` driven by the test, with no purge delay or
/// grace.
fn drive(engine: &Engine) {
    engine.gc_state().configure(|s| {
        s.auto = false;
        s.purge_delay = Duration::ZERO;
        s.grace = Duration::ZERO;
    });
}

/// Five appends to `name`, then a seal of its shard: its records in a
/// segment object.
async fn sealed_records(addr: Addr, engine: &Engine, name: &str) {
    for _ in 0..5 {
        let (st, _, _) = hreq(addr, "POST", &format!("/v1/stream/{name}"), &[JSON], ROW).await;
        assert_eq!(st, 204, "append to {name}");
    }
    engine.seal_requested().await.expect("seal");
}

/// Runs the production sweep, a drain round (the purge waits until the
/// close reached the ledger) and the collector until `id` is purged and
/// reclaimed on `engine`.
async fn sweep_until_purged(state: &State, engine: &Engine, id: [u8; 16]) {
    let mut drain = DrainWait::default();
    for _ in 0..40 {
        crate::billing::sweep_owned_outboxes(state).await;
        drain.round(state).await;
        engine.gc_pass().await;
        let head = engine.head_row(&id).await.unwrap();
        if head.is_some_and(|h| h.state == HeadState::Purged && !h.owes()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the dead incarnation was never purged");
}

/// Each object's counted chunks (its `'R'` row).
async fn refcounts(engine: &Engine) -> BTreeMap<ObjectId, u64> {
    engine
        .refcount_rows()
        .await
        .unwrap()
        .into_iter()
        .map(|(id, r)| (id, r.expect("a decodable row").live_chunks))
        .collect()
}

/// Runs the sweep and the drain until `id`'s billing row is closed and
/// acknowledged; the collector is not driven, so no purge runs here.
async fn loop_closed(
    state: &State,
    engine: &Engine,
    id: [u8; 16],
) -> crate::billing::SegmentBillingMetaV1 {
    let mut drain = DrainWait::default();
    for _ in 0..40 {
        crate::billing::sweep_owned_outboxes(state).await;
        drain.round(state).await;
        let dirty = engine.usage_dirty_scan().await.unwrap();
        let clean = dirty.iter().all(|(hash, _)| *hash != id);
        if let Some(meta) = engine.billing_meta(id).await
            && meta.owned_frame_bytes_current == 0
            && clean
        {
            return meta;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the close never reached the ledger");
}

/// Whether any sealed chunk of `id` is still counted (unreclaimed).
async fn counted_chunks(engine: &Engine, id: [u8; 16]) -> usize {
    let reclaimed = engine
        .head_row(&id)
        .await
        .unwrap()
        .map_or(0, |h| h.reclaimed);
    engine
        .live_chunks(id)
        .await
        .unwrap()
        .iter()
        .filter(|e| e.first >= reclaimed)
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_stream_is_purged_and_its_objects_reclaimed() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    raw_put(addr, "gc-del", &[JSON], ROW).await;
    let sref = state.deployment.raw_adapter_sref("gc-del");
    let desc = stored(&state, &sref).await;
    let engine = engine_of(&state, &desc, 0).await;
    drive(&engine);
    sealed_records(addr, &engine, "gc-del").await;
    let id = desc.dynamic_segment_identity(0);
    assert!(
        counted_chunks(&engine, id).await > 0,
        "sealed chunks to reclaim"
    );
    // Each object holding the stream's chunks, and what its row counts.
    let mut mine: BTreeMap<ObjectId, u64> = BTreeMap::new();
    for e in engine.live_chunks(id).await.unwrap() {
        *mine.entry(e.object).or_default() += 1;
    }
    let before = refcounts(&engine).await;
    // A purge asked for while the billing gauge is open waits for the close
    // (after a purge the close could not load the stream).
    ack_clean(&state, &engine, &[id]).await;
    engine.request_purge(id, 0).await.unwrap();
    assert_eq!(
        engine.purge_stream(id).await.unwrap(),
        crate::shard::seal::gc::purge::PurgeOutcome::Refused,
        "the gauge is open"
    );
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/gc-del", &[], b"").await;
    assert_eq!(st, 204, "delete");
    // The request is there (the delete asks for it itself; the walk is the
    // fallback).
    assert_eq!(
        engine.head_row(&id).await.unwrap().map(|h| h.state),
        Some(HeadState::Requested),
        "the purge is asked for"
    );
    // The close reaches the ledger before the purge runs (the usage
    // counters ride the tail, f3, and the purge deletes the tail).
    let closed_row = loop_closed(&state, &engine, id).await;
    sweep_until_purged(&state, &engine, id).await;
    assert_eq!(
        counted_chunks(&engine, id).await,
        0,
        "every chunk reclaimed"
    );
    // The static billing row stays as the close left it; the counters went
    // with the tail after the ledger acknowledged them, and nothing marks
    // the stream for the drain again (no re-emission at a lower version).
    let billed = engine
        .billing_meta(id)
        .await
        .expect("the billing row stays");
    assert_eq!(
        (
            &billed.stream_id,
            &billed.project_id,
            billed.owned_frame_bytes_current
        ),
        (&closed_row.stream_id, &closed_row.project_id, 0),
        "the static billing row is untouched"
    );
    let mut drain = DrainWait::default();
    for _ in 0..3 {
        crate::billing::sweep_owned_outboxes(&state).await;
        drain.round(&state).await;
    }
    let dirty = engine.usage_dirty_scan().await.unwrap();
    assert!(
        dirty.iter().all(|(hash, _)| *hash != id),
        "a purged stream is never emitted again"
    );
    engine.collect().await.unwrap();
    let after = refcounts(&engine).await;
    for (object, n) in &mine {
        let left = before[object] - n;
        assert_eq!(
            after.get(object).copied(),
            (left > 0).then_some(left),
            "object {object:?} counts the chunks of other streams only"
        );
    }
    assert!(
        engine.garbage_rows().await.unwrap().is_empty(),
        "every zeroed object was deleted"
    );
    assert!(
        engine.stream_handle(id).await.is_err(),
        "a purged stream never loads again"
    );
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_stream_is_not_purged_within_its_tombstone_delay_and_a_recreation_purges_it() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    raw_put(addr, "gc-ttl", &[JSON, ("stream-ttl", "3600")], ROW).await;
    let sref = state.deployment.raw_adapter_sref("gc-ttl");
    let old = stored(&state, &sref).await;
    let engine = engine_of(&state, &old, 0).await;
    drive(&engine);
    sealed_records(addr, &engine, "gc-ttl").await;
    let id = old.dynamic_segment_identity(0);
    let expired_at = billing_now_ms();
    expire(&[&state], &sref, expired_at).await;
    billing_clock_past(expired_at + 99).await;
    for _ in 0..4 {
        crate::billing::sweep_owned_outboxes(&state).await;
        engine.gc_pass().await;
    }
    assert_eq!(
        engine.head_row(&id).await.unwrap(),
        None,
        "an expired incarnation is not irrevocably dead: no purge is asked for"
    );
    assert!(counted_chunks(&engine, id).await > 0);
    // Recreated: the replaced incarnation is purged; the new one is not.
    raw_put(addr, "gc-ttl", &[JSON], ROW).await;
    let fresh = stored(&state, &sref).await;
    assert_ne!(fresh.stream_epoch, old.stream_epoch);
    sealed_records(addr, &engine, "gc-ttl").await;
    sweep_until_purged(&state, &engine, id).await;
    assert_eq!(counted_chunks(&engine, id).await, 0);
    let fresh_id = fresh.dynamic_segment_identity(0);
    assert_eq!(engine.head_row(&fresh_id).await.unwrap(), None);
    assert!(
        counted_chunks(&engine, fresh_id).await > 0,
        "the new incarnation keeps its chunks"
    );
    let (st, _, body) = hreq(addr, "GET", "/v1/stream/gc-ttl?offset=-1", &[], b"").await;
    assert_eq!(
        st,
        200,
        "the new incarnation reads: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(!body.is_empty());
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_its_fork_reads_is_never_purged_until_the_fork_is_deleted() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    raw_put(addr, "gc-src", &[JSON], ROW).await;
    let sref = state.deployment.raw_adapter_sref("gc-src");
    let src = stored(&state, &sref).await;
    let engine = engine_of(&state, &src, 0).await;
    drive(&engine);
    sealed_records(addr, &engine, "gc-src").await;
    raw_put(
        addr,
        "gc-kid",
        &[JSON, ("stream-forked-from", "gc-src")],
        b"",
    )
    .await;
    let id = src.dynamic_segment_identity(0);
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/gc-src", &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    let soft = stored(&state, &sref).await;
    assert!(soft.soft_deleted && !soft.deleted, "retained for its fork");
    let mut drain = DrainWait::default();
    for _ in 0..4 {
        crate::billing::sweep_owned_outboxes(&state).await;
        drain.round(&state).await;
        engine.gc_pass().await;
    }
    assert_eq!(
        engine.head_row(&id).await.unwrap(),
        None,
        "a retained source is not purged"
    );
    assert!(counted_chunks(&engine, id).await > 0);
    let (st, _, body) = hreq(addr, "GET", "/v1/stream/gc-kid?offset=-1", &[], b"").await;
    assert_eq!(
        st,
        200,
        "the fork reads through its source: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        !body.is_empty() && body != b"[]".as_slice(),
        "the source's records"
    );
    // The fork's delete releases the source, which cascades to a tombstone:
    // now it is purged.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/gc-kid", &[], b"").await;
    assert!(st == 204 || st == 200, "delete the fork: {st}");
    assert!(
        stored(&state, &sref).await.deleted,
        "the cascade tombstoned the source"
    );
    sweep_until_purged(&state, &engine, id).await;
    assert_eq!(counted_chunks(&engine, id).await, 0);
    engine_shutdown(&state).await;
}

const HOUR_MS: i64 = 3_600_000;

/// A raw stream with a one-hour TTL and sealed records, then expired
/// `ago_ms` ago: its sref, the expired descriptor the walk would read, the
/// engine and the segment id.
async fn expired_stream(
    state: &State,
    addr: Addr,
    name: &str,
    ago_ms: i64,
) -> (
    crate::tenant::TenantStreamRef,
    crate::registry::StreamDesc,
    Engine,
    [u8; 16],
) {
    raw_put(addr, name, &[JSON, ("stream-ttl", "3600")], ROW).await;
    let sref = state.deployment.raw_adapter_sref(name);
    let desc = stored(state, &sref).await;
    let engine = engine_of(state, &desc, 0).await;
    drive(&engine);
    sealed_records(addr, &engine, name).await;
    expire(&[state], &sref, crate::shard::now_ms() - ago_ms).await;
    let seen = stored(state, &sref).await;
    let id = seen.dynamic_segment_identity(0);
    (sref, seen, engine, id)
}

/// The renewal an append admitted before the expiry submits (`touch_ttl`),
/// landing now.
async fn land_renewal(state: &State, seen: &crate::registry::StreamDesc) {
    crate::application::creation::TtlMutation::for_tests(
        state.registry.clone(),
        seen.sref(),
        seen.stream_epoch.clone(),
        3600,
        crate::shard::now_ms() + HOUR_MS,
    )
    .run()
    .await
    .expect("the renewal's mutation runs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_renewal_that_lands_before_the_expiry_tombstone_keeps_the_stream_alive() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    let delay = crate::application::creation::EXPIRY_TOMBSTONE_DELAY_MS;
    let (sref, seen, engine, id) = expired_stream(&state, addr, "gc-renew", delay + 60_000).await;
    // The walk read `seen` expired; the renewal lands before its CAS.
    land_renewal(&state, &seen).await;
    let made = state
        .creation_service()
        .tombstone_expired(&seen)
        .await
        .unwrap();
    assert!(!made, "the CAS re-checks the expiry and declines");
    let live = stored(&state, &sref).await;
    assert!(
        !live.deleted && crate::http::desc_alive(&live),
        "renewed, alive"
    );
    let mut drain = DrainWait::default();
    for _ in 0..3 {
        crate::billing::sweep_owned_outboxes(&state).await;
        drain.round(&state).await;
        engine.gc_pass().await;
    }
    assert_eq!(
        engine.head_row(&id).await.unwrap(),
        None,
        "nothing is purged"
    );
    assert!(counted_chunks(&engine, id).await > 0);
    let (st, _, body) = hreq(addr, "GET", "/v1/stream/gc-renew?offset=-1", &[], b"").await;
    assert_eq!(
        st,
        200,
        "the renewed stream reads: {}",
        String::from_utf8_lossy(&body)
    );
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_renewal_that_lands_after_the_expiry_tombstone_is_refused_and_the_stream_purged() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    let delay = crate::application::creation::EXPIRY_TOMBSTONE_DELAY_MS;
    let (sref, seen, engine, id) = expired_stream(&state, addr, "gc-late", delay + 60_000).await;
    // The production sweep makes it a tombstone and asks for its purge.
    sweep_until_purged(&state, &engine, id).await;
    let dead = stored(&state, &sref).await;
    assert!(
        dead.deleted,
        "the walk made the expired incarnation a tombstone"
    );
    assert_eq!(
        dead.logical_close_ms, seen.expires_at_ms,
        "closed at its expiry"
    );
    assert_eq!(
        counted_chunks(&engine, id).await,
        0,
        "and it is fully reclaimed"
    );
    // A renewal admitted before the expiry lands now: refused.
    land_renewal(&state, &seen).await;
    let still = stored(&state, &sref).await;
    assert!(still.deleted, "a late renewal does not revive a tombstone");
    assert_eq!(still.expires_at_ms, seen.expires_at_ms);
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/gc-late?offset=-1", &[], b"").await;
    assert!(st == 404 || st == 410, "gone: {st}");
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_anchored_before_the_expiry_tombstone_retains_its_source() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    let delay = crate::application::creation::EXPIRY_TOMBSTONE_DELAY_MS;
    let (sref, seen, engine, id) = expired_stream(&state, addr, "gc-anchor", delay + 60_000).await;
    // The walk read `seen` expired with no child; a fork prepared before
    // the expiry installs its reference before the CAS (`anchor::install`).
    let installed = state
        .registry
        .mutate_incarnation(&sref, &seen.stream_epoch, |current| {
            let mut next = current.to_persisted();
            next.fork_children.push("fork-late".to_string());
            crate::registry::Mutation::Write(next, ())
        })
        .await
        .unwrap();
    assert!(matches!(
        installed,
        crate::registry::MutationResult::Applied(())
    ));
    let made = state
        .creation_service()
        .tombstone_expired(&seen)
        .await
        .unwrap();
    assert!(!made, "the CAS re-checks the children and declines");
    let kept = stored(&state, &sref).await;
    assert!(
        !kept.deleted && kept.fork_children.len() == 1,
        "retained for its fork"
    );
    let mut drain = DrainWait::default();
    for _ in 0..3 {
        crate::billing::sweep_owned_outboxes(&state).await;
        drain.round(&state).await;
        engine.gc_pass().await;
    }
    assert_eq!(
        engine.head_row(&id).await.unwrap(),
        None,
        "a retained source is not purged"
    );
    assert!(counted_chunks(&engine, id).await > 0);
    engine_shutdown(&state).await;
}
