#![cfg(test)]
//! The fleet tick's two commits (`fleet::moves`), each held to its contract on a real rig: a move
//! is mirrored and acted on here only when its CAS of the overrides document committed. A version
//! read before another write is stale, so its CAS loses; the current one wins.

use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::fleet::{FleetDocument, OverrideEntry, Overrides};
use std::time::Duration;

/// `streams-1` of a ring of two over `00` and `01`, its view published by hand (no tick runs), with
/// `00` (whose ring home it is) open.
async fn member() -> HttpRig {
    let options = HttpRigOptions {
        fleet_store: Some(mem()),
        instance: Some("streams-1".to_string()),
        prefixes: vec!["00".to_string(), "01".to_string()],
        ..Default::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    let ring = vec!["streams-1".to_string(), "streams-2".to_string()];
    rig.state.ownership.set_view(ring, Default::default());
    let opened = rig
        .state
        .shards
        .open_or_wait("00", Duration::from_secs(5))
        .await;
    assert!(
        matches!(opened, crate::sharddir::OpenOutcome::Ready(_)),
        "00 opens"
    );
    rig
}

/// Write `doc` over the stored overrides document as of `version`; it must commit.
async fn write(rig: &HttpRig, doc: &Overrides, version: Option<object_store::UpdateVersion>) {
    let body = serde_json::to_vec(doc).unwrap();
    let doc = FleetDocument::Overrides;
    assert!(
        rig.state.fleet.replace_document(doc, body, version).await,
        "the write commits"
    );
}

/// **A lag move hands its shard off giver first** (`Overrides::lag_move`, F6). With this
/// runtime's handoff runner started, the lag move parks, flushes and closes `00` here, then the
/// overrides name the target: it reports the move, `00` is no longer open here, the mirror names
/// the target, and the stored document holds the entry. (Under F6 the move rereads the document
/// itself, so the version the tick read matters only to a wedged victim's evacuation.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_lag_move_hands_its_shard_off_giver_first() {
    let rig = member().await;
    let state = &rig.state;
    crate::fleet::handoff::start(state.clone(), &rig.tasks);
    let (doc, version) = state.fleet.read_overrides().await.unwrap();
    let pair = ("00".to_string(), "streams-2".to_string());
    assert!(
        doc.lag_move(&pair, state, version).await,
        "the move is made"
    );
    let owner = state.ownership.effective_owner("00");
    assert_eq!(
        (state.shards.is_open("00"), owner.as_deref()),
        (false, Some("streams-2"))
    );
    let stored = state.fleet.read_overrides().await.unwrap().0;
    assert_eq!(
        stored.entries.get("00").map(|e| e.to.as_str()),
        Some("streams-2")
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(state).await;
}

/// **A return reports its document only when its CAS commits** (`Overrides::return_home`). With a
/// stale version nothing is returned and the stored entry stays; with the current one the
/// document without the entry is returned, and stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_return_home_reports_its_document_only_when_its_cas_commits() {
    let rig = member().await;
    let state = &rig.state;
    let mut seeded = Overrides::default();
    let entry = OverrideEntry {
        to: "streams-2".to_string(),
        ms: 1,
    };
    seeded.entries.insert("00".to_string(), entry);
    write(&rig, &seeded, None).await;
    let (doc, stale) = state.fleet.read_overrides().await.unwrap();
    write(&rig, &doc, stale.clone()).await;
    let drops = ["00".to_string()];
    assert!(
        doc.return_home(&drops, state, stale).await.is_none(),
        "a lost CAS returns nothing"
    );
    let stored = |doc: Overrides| doc.entries.into_keys().collect::<Vec<_>>();
    assert_eq!(
        stored(state.fleet.read_overrides().await.unwrap().0),
        ["00"]
    );
    let (doc, current) = state.fleet.read_overrides().await.unwrap();
    let returned = doc
        .return_home(&drops, state, current)
        .await
        .expect("the CAS commits");
    assert!(
        returned.entries.is_empty(),
        "the returned document drops 00"
    );
    assert!(
        stored(state.fleet.read_overrides().await.unwrap().0).is_empty(),
        "and is stored"
    );
    engine_shutdown(state).await;
}
