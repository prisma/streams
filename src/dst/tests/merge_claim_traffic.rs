#![cfg(test)]
//! A claimant's roll forward under client traffic on the other source (TLA-048 option (b), change 1:
//! the hold before the open). A merge's claimant fences its other source by a maintenance open and
//! writes that source's `'T'` through it. Every reopen of the source by the instance that serves it
//! bumps the writer epoch and fences that maintenance writer, so under steady traffic the roll
//! forward's `'T'` write fails attempt after attempt (TLA-048's `nc_open_path_570cce46b`). With the
//! hold an open of a source waits, without opening, while a live intent naming it is fresh by the
//! opener's own observations, so the claimant's writer stays the source's current one.

use super::fixture_http::engine_shutdown;
use super::merge_k::{Rig, append_to, two_instances};
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use crate::topology::intent::{self, Intent, State};
use std::time::Duration;

/// `M`'s log at `path`, complete: its `'Y'` naming `home` (`recover::merge`'s completeness test).
async fn complete_merged_log(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    path: &str,
    home: u32,
) {
    let db = crate::keyspace::db_builder(path, store.clone())
        .build()
        .await
        .unwrap();
    let (key, value) = crate::shard::open_index::identity_row(home);
    db.put(&key, &value).await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();
}

/// `'T'` on the log at `path`.
async fn retire(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    path: &str,
    t: &crate::topology::fence::Retired,
) {
    let db = crate::keyspace::db_builder(path, store.clone())
        .build()
        .await
        .unwrap();
    let row = serde_json::to_vec(t).unwrap();
    db.put(crate::topology::fence::retired_key(), row)
        .await
        .unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();
}

/// **A claimant's roll forward completes while the other source's instance keeps taking traffic.**
/// A merge of `000` (inst-a) and `001` (inst-b) whose actor died after `000`'s `'T'`, with `M`
/// complete, lapses. inst-a's reopen of `000` claims it and rolls forward; the claimant is parked
/// at its read of `M`, after its claim and its maintenance open of `001` and before `001`'s `'T'`.
/// Meanwhile appends keep arriving at inst-b for `001`, whose engine the maintenance open fenced:
/// its reopen must wait while the claim is fresh. Released, the claimant writes `001`'s `'T'`,
/// publishes the merge and ends the intent `Reclaimed` within 5 s, under its own claim (lease
/// epoch 2): no reopen fenced its maintenance writer, so no second claimant had to take over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_claimants_roll_forward_completes_under_traffic_on_the_other_source() {
    let faults = FaultStore::uniform(super::fixture_storage::mem(), 7, FaultPlan::CLEAN);
    let (rig, state_b, addr_b, version) = two_instances(&faults).await;
    let Rig {
        store,
        addr,
        low,
        high,
        ..
    } = &rig;
    let snapshot = state_b.shards.prefixes();
    let homes = [snapshot.shards.leaves["000"], snapshot.shards.leaves["001"]];
    let merged_home = snapshot.shards.next_home;
    let merged_path = crate::topology::model::born_home_path("00", merged_home);
    // The merge's home CAS, as its start made it: `M`'s home reserved in the stored topology.
    let (stored, etag) = crate::topology::store::read(&**store).await.unwrap();
    let (reserved, home) = stored.reserve_home();
    assert_eq!(home, merged_home);
    crate::topology::store::replace(&**store, &etag, &reserved)
        .await
        .unwrap();
    complete_merged_log(store, &merged_path, merged_home).await;
    let t = crate::topology::fence::Retired {
        version: version + 1,
        op_id: "op-merge".to_string(),
        children: vec![("00".to_string(), merged_home)],
    };
    let path_of = |home: u32| snapshot.shards.homes[&home].path.clone();
    retire(store, &path_of(homes[0]), &t).await;
    let merge = Intent {
        kind: "merge".to_string(),
        op_id: "op-merge".to_string(),
        sources: vec![("000".to_string(), homes[0]), ("001".to_string(), homes[1])],
        children: vec![merged_home],
        version_seen: version,
        state: State::Preparing,
        lease_owner: "inst-elsewhere".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    intent::create(&**store, &merge).await.unwrap();
    let manifest = format!("{merged_path}/manifest/");
    let parked = faults.hold_class_under(StoreOp::Get, ObjClass::Manifest, &manifest, 1);
    let t0 = std::time::Instant::now();
    while parked.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        drop(append_to(*addr, low, 5).await);
        assert!(
            t0.elapsed() < Duration::from_secs(40),
            "inst-a claims the lapsed merge"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let claimed = intent::read(&**store).await.unwrap().unwrap().intent;
    assert_eq!((claimed.state, claimed.lease_epoch), (State::Preparing, 2));
    // Traffic at inst-b for 001 past its fenced engine's holdoff (6 s), so it reopens if it may.
    let traffic = std::time::Instant::now();
    while traffic.elapsed() < Duration::from_secs(10) {
        drop(append_to(addr_b, high, 6).await);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    faults.release_hold();
    let released = std::time::Instant::now();
    let ended = loop {
        let held = intent::read(&**store).await.unwrap().unwrap();
        if held.intent.is_terminal() || released.elapsed() >= Duration::from_secs(5) {
            break held.intent;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let owner = ended
        .lease_owner
        .split('@')
        .next()
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        (
            ended.state,
            ended.outcome.map(|o| o.result),
            ended.lease_epoch,
            owner
        ),
        (
            State::Reclaimed,
            Some("published".to_string()),
            2,
            "opener:000".to_string()
        ),
        "inst-a's claim rolls forward within 5 s of the release, under its own claim"
    );
    let (stored, _) = crate::topology::store::read(&**store).await.unwrap();
    assert_eq!(stored.shards.leaves.get("00"), Some(&merged_home));
    engine_shutdown(&rig.state).await;
    engine_shutdown(&state_b).await;
}
