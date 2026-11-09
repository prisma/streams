#![cfg(test)]
//! The fleet and the topology (S4-DESIGN §10.1): every member's heartbeat names the cell's live
//! topology intent, which the fleet's planner reads to hold off a scale-in while a split or merge
//! runs (`fleet::planning`'s shrink sentinel).

use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::topology::intent::{self, Intent, State};
use std::time::{Duration, Instant};

type App = std::sync::Arc<crate::http::AppState>;

/// How long a beat naming the intent's change may take: a few 2 s beat periods.
const BOUND: Duration = Duration::from_secs(10);

/// The first heartbeat `streams-1` publishes after `since_ms` whose `topology_intent` is `want`.
async fn beat_naming(state: &App, since_ms: i64, want: Option<&str>) {
    let t0 = Instant::now();
    loop {
        let beats = state.fleet.peek_heartbeat_set().await.unwrap_or_default();
        let found = beats.into_iter().any(|b| {
            b.instance == "streams-1" && b.ts_ms > since_ms && b.topology_intent.as_deref() == want
        });
        if found {
            return;
        }
        assert!(t0.elapsed() < BOUND, "no beat named {want:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **A member's heartbeat names the live topology intent until it ends.** A fleet instance beats
/// with no intent; once a live split intent is written its beats name the operation, and once the
/// intent is rolled back they name none again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_members_heartbeat_names_the_live_topology_intent_until_it_ends() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(mem()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    let state = &rig.state;
    beat_naming(state, 0, None).await;
    let live = Intent {
        kind: "split".to_string(),
        op_id: "op-beat".to_string(),
        sources: vec![("00".to_string(), 0)],
        children: vec![90, 91],
        version_seen: 1,
        state: State::Preparing,
        lease_owner: "inst-a".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    let held = intent::create(&*state.data_store, &live).await.unwrap();
    beat_naming(state, crate::shard::now_ms(), Some("op-beat")).await;
    let mut ended = live;
    ended.state = State::RolledBack;
    intent::replace(&*state.data_store, &held, ended)
        .await
        .unwrap();
    beat_naming(state, crate::shard::now_ms(), None).await;
    engine_shutdown(state).await;
}
