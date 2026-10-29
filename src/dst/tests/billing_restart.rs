//! Layout 6 (DESIGN-6A f3): a billed segment's usage reaches the ledger
//! from its tail's counters and the engine's in-memory dirty set, which a
//! fresh process rebuilds from the tails. The rebuild re-emits every billed
//! segment once; the rollup's version rule makes the re-emission a no-op,
//! so the totals stay exact across a restart and the writes after it.
#![cfg(test)]
use super::fixture_http::{engine_shutdown, http_rig_at, install_rollup};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::sync::Arc;

const KEY: [(&str, &str); 1] = [("prisma-encryption-key", PRISMA_KEY)];
const RECORD: &[u8] = br#"{"n":1,"pad":"aaaaaaaaaa"}"#; // 26 B

/// Drain until a round emits nothing, then roll the ledger up to its end.
/// The records the drain emitted.
async fn settle(state: &Arc<crate::http::AppState>) -> usize {
    let mut drained = 0;
    for _ in 0..100 {
        let n = crate::billing::drain_once(state).await.expect("drain");
        drained += n;
        if n == 0 {
            break;
        }
    }
    for _ in 0..50 {
        if crate::billing::rollup_step(state).await.expect("rollup") == 0 {
            break;
        }
    }
    drained
}

/// `name`'s current usage answer.
async fn usage(addr: std::net::SocketAddr, name: &str) -> serde_json::Value {
    let path = format!("/v1/streams/{name}/usage/current");
    let (st, _, body) = preq(addr, "GET", &path, &KEY, b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

async fn append(addr: std::net::SocketAddr, name: &str) {
    let path = format!("/v1/streams/{name}/records");
    let (st, _, _) = preq(addr, "POST", &path, &KEY, RECORD).await;
    assert_eq!(st, 200);
}

/// Two billed streams, drained and rolled up; a restart over the same store
/// re-emits both (the drain emits again with no new write) and every total
/// is unchanged; one more write after the restart adds exactly its bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_re_emits_every_billed_segment_and_the_rollup_bills_it_once() {
    let _xr = crate::billing::billing_clock_lock().read().await;
    let store = mem();
    let (state, addr) = http_rig_at(store.clone(), RigRuntime::first()).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup).await;
    for (name, appends) in [("rs-a", 2), ("rs-b", 1)] {
        let path = format!("/v1/streams/{name}");
        let (st, _, _) = preq(addr, "PUT", &path, &KEY, br#"{"format":{"kind":"json"}}"#).await;
        assert_eq!(st, 201);
        for _ in 0..appends {
            append(addr, name).await;
        }
    }
    assert!(settle(&state).await >= 2, "both segments' snapshots");
    assert_eq!(settle(&state).await, 0, "the dirty set is clear");
    let before = [usage(addr, "rs-a").await, usage(addr, "rs-b").await];
    assert_eq!(before[0]["ingestPayloadBytes"], 2 * RECORD.len() as u64);
    assert_eq!(before[1]["ingestRecords"], 1);
    engine_shutdown(&state).await;

    let (state, addr) = http_rig_at(store.clone(), RigRuntime::incarnation(1)).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup).await;
    // A read opens the shard; the first drain after it re-marks both.
    for name in ["rs-a", "rs-b"] {
        let path = format!("/v1/streams/{name}/records");
        let (st, _, _) = preq(addr, "GET", &path, &KEY, b"").await;
        assert_eq!(st, 200);
    }
    assert!(
        settle(&state).await >= 2,
        "the fresh process re-emits every billed segment"
    );
    let rolled = [usage(addr, "rs-a").await, usage(addr, "rs-b").await];
    for (b, r) in before.iter().zip(&rolled) {
        for f in ["ingestPayloadBytes", "ingestRecords", "ownedStoredBytesNow"] {
            assert_eq!(r[f], b[f], "{f} after the re-emission");
        }
    }
    append(addr, "rs-a").await;
    settle(&state).await;
    let last = usage(addr, "rs-a").await;
    assert_eq!(last["ingestPayloadBytes"], 3 * RECORD.len() as u64);
    assert_eq!(last["ingestRecords"], 3);
    engine_shutdown(&state).await;
}
