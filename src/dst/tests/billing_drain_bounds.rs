//! The usage drain's bounds: how many of a shard's dirty rows one drain
//! round takes (the owner's decision of 2026-10-08, T9).

use super::fixture_http::{HttpRigOptions, cold_absorber, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::time::Duration;

/// T9(a), edge change #121: one drain round takes up to 256 of each
/// shard's dirty rows (64 before), so a shard that dirties N segments
/// within one cadence has every snapshot in `_usage` after ceil(N / 256)
/// rounds, not ceil(N / 64). 400 streams with one append each over the
/// rig's shards, one of which holds more than 256 of them: the first
/// round emits exactly min(rows, 256) snapshots of each shard and, once
/// its acknowledgements apply, the second round the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_drain_round_takes_up_to_256_dirty_rows_of_each_shard() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    // The absorber is not the subject (and the rig's fast one keeps a
    // shard of this many segments from closing at the rig's stop).
    let options = HttpRigOptions {
        absorber: Some(cold_absorber()),
        ..HttpRigOptions::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    let json = [("content-type", "application/json")];
    for n in 0..400 {
        let path = format!("/v1/stream/page-{n}");
        assert_eq!(hreq(rig.addr, "PUT", &path, &json, b"").await.0, 201);
        let appended = hreq(rig.addr, "POST", &path, &json, br#"[{"n":1}]"#).await;
        assert_eq!(appended.0, 204);
    }
    let dirty = dirty_rows(&rig.state).await;
    assert_eq!(dirty.iter().sum::<usize>(), 400);
    assert!(dirty.iter().any(|rows| *rows > 256), "{dirty:?}");
    let first: usize = dirty.iter().map(|rows| (*rows).min(256)).sum();
    let rest: usize = dirty.iter().map(|rows| rows.saturating_sub(256)).sum();
    assert_eq!(
        crate::billing::drain_once(&rig.state).await,
        Ok(first),
        "one round takes up to 256 dirty rows of each shard {dirty:?}"
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while dirty_rows(&rig.state).await.iter().sum::<usize>() != rest {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the first round's acknowledgements apply");
    assert_eq!(crate::billing::drain_once(&rig.state).await, Ok(rest));
    rig.shutdown().await;
}

/// Each open shard's dirty rows, for the shards that hold any.
async fn dirty_rows(state: &crate::http::AppState) -> Vec<usize> {
    let mut dirty = Vec::new();
    for engine in state.shards.engines() {
        let rows = engine.usage_dirty_scan().await.unwrap().len();
        if rows > 0 {
            dirty.push(rows);
        }
    }
    dirty
}
