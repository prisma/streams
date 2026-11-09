#![cfg(test)]
//! A purge request while a split holds the engine's maintenance gate (S4-DESIGN §5.1, S6-DESIGN
//! §7.1): `request_purge` answers `Busy` at once, and each caller falls back without waiting out
//! the pause.
//! - The client DELETE answers as before and leaves the purge to the tombstone walk.
//! - The walk stops its page and asks again on its next pass, once the gate is free.
//! - The closure-debt pass keeps a recreation's debt until its purge is asked for.

use super::billing_closure_debts::{JSON, ROW, engine_of, expire, raw_put, stored};
use super::fixture_billing::billing_clock_past;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use crate::billing::billing_now_ms;
use std::sync::Arc;
use std::time::Duration;

type State = Arc<crate::http::AppState>;
type Engine = Arc<crate::shard::ShardEngine>;

const BOUND: Duration = Duration::from_secs(10);

/// The collector of `engine` driven by the test, with no purge delay.
fn drive(engine: &Engine) {
    engine.gc_state().configure(|s| {
        s.auto = false;
        s.purge_delay = Duration::ZERO;
    });
}

/// One production sweep (the tombstone walk, then the closure-debt pass), bounded.
async fn sweep(state: &State) {
    tokio::time::timeout(BOUND, crate::billing::sweep_owned_outboxes(state))
        .await
        .expect("the sweep waits for no split");
}

/// Sweeps until `engine` holds a purge request for `id`, at most 40 rounds.
async fn sweep_until_asked(state: &State, engine: &Engine, id: [u8; 16]) {
    for _ in 0..40 {
        sweep(state).await;
        if engine.head_row(&id).await.unwrap().is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the purge was never asked for");
}

/// **A DELETE during a split answers as before and leaves the purge to the walk.** With the
/// gate held, the DELETE answers `204` and asks for no purge; the walk's pass asks for none
/// either. Once the gate is free, the walk asks for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_delete_during_a_split_leaves_its_purge_to_the_walk() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    raw_put(addr, "gate-del", &[JSON], ROW).await;
    let sref = state.deployment.raw_adapter_sref("gate-del");
    let desc = stored(&state, &sref).await;
    let engine = engine_of(&state, &desc, 0).await;
    drive(&engine);
    let id = desc.dynamic_segment_identity(0);
    let hold = engine.gc_state().gate.exclusive(BOUND).await.expect("free");
    let delete = hreq(addr, "DELETE", "/v1/stream/gate-del", &[], b"");
    let (st, _, _) = tokio::time::timeout(BOUND, delete).await.expect("answered");
    assert_eq!(st, 204, "delete");
    assert_eq!(
        engine.head_row(&id).await.unwrap(),
        None,
        "the DELETE's ask"
    );
    sweep(&state).await;
    assert_eq!(engine.head_row(&id).await.unwrap(), None, "the walk's ask");
    drop(hold);
    sweep_until_asked(&state, &engine, id).await;
    engine_shutdown(&state).await;
}

/// **A recreation's closure debt stays until its purge is asked for.** An expired stream is
/// recreated while the gate is held: the closure-debt pass meets `Busy` and keeps the debt,
/// with no purge asked for. Once the gate is free, the next passes ask for it and settle the
/// debt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_recreation_during_a_split_keeps_its_closure_debt_until_its_purge_is_asked() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (state, addr) = http_rig(mem()).await;
    raw_put(addr, "gate-ttl", &[JSON, ("stream-ttl", "3600")], ROW).await;
    let sref = state.deployment.raw_adapter_sref("gate-ttl");
    let old = stored(&state, &sref).await;
    let engine = engine_of(&state, &old, 0).await;
    drive(&engine);
    let id = old.dynamic_segment_identity(0);
    let expired_at = billing_now_ms();
    expire(&[&state], &sref, expired_at).await;
    billing_clock_past(expired_at + 99).await;
    let hold = engine.gc_state().gate.exclusive(BOUND).await.expect("free");
    raw_put(addr, "gate-ttl", &[JSON], ROW).await;
    for _ in 0..3 {
        sweep(&state).await;
    }
    let debts = state.registry.replaced_page(None, 10).await.unwrap();
    assert_eq!(debts.len(), 1, "the recreation's debt stays");
    assert_eq!(engine.head_row(&id).await.unwrap(), None);
    drop(hold);
    sweep_until_asked(&state, &engine, id).await;
    for _ in 0..40 {
        if state
            .registry
            .replaced_page(None, 10)
            .await
            .unwrap()
            .is_empty()
        {
            break;
        }
        sweep(&state).await;
    }
    let debts = state.registry.replaced_page(None, 10).await.unwrap();
    assert!(debts.is_empty(), "settled once asked: {}", debts.len());
    engine_shutdown(&state).await;
}
