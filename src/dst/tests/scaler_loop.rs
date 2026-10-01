//! The scaler's own loop over a running server (NEXT-WORK section 10):
//! `scaler3::start` evaluates the sketches on its cadence and runs each
//! decision in the pass that follows, inside that pass's deadline. The unit
//! tests drive `evaluate` and `scaler_controller` drives a pass; this module
//! runs the loop that joins them.
use super::fixture_http::{HttpRigOptions, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::time::Duration;

/// Where the loop must split: the load sits 60:40 in the segment's first
/// and 48th sixty-fourths, so the load-weighted median is the end of the
/// first bin (the split the scaler's unit tests establish).
const SPLIT_AT: u64 = u64::MAX / 64;

/// The loop's first evaluation comes one cadence after it starts (the
/// default 10 s: the rig overlays no environment). The bound leaves room for
/// that and two retried passes; a loop that never splits fails here.
const SPLIT_BOUND: Duration = Duration::from_secs(30);

/// Feed the parent's admission sketch as the append path does, far above
/// the default hot thresholds: 600 GB under one routing key at the
/// segment's first point and 400 GB under another at three quarters. The
/// second key's 40% makes the segment plural, so it splits instead of
/// surfacing the first key as an unsplittable hot key.
fn feed_heat(state: &crate::http::AppState, desc: &crate::registry::StreamDesc) {
    let parent = desc.resolve_segment("");
    for (point, key, bytes) in [
        (1, 1, 600_000_000_000),
        (u64::MAX / 4 * 3, 2, 400_000_000_000),
    ] {
        let route = crate::registry::SegRoute {
            point,
            key_hash: crate::crypto::RoutingKeyHash([key; 16]),
            ..parent.clone()
        };
        state.runtime.scaler.note_append(desc, &route, bytes, 1);
    }
}

/// The `split_committed` events the loop's controller emitted for `name`.
fn split_events(state: &crate::http::AppState, name: &str) -> Vec<crate::ops::OpsEvent> {
    state
        .runtime
        .ops
        .recent(crate::ops::RECENT_CAP)
        .into_iter()
        .filter(|event| {
            event.event_type == "split_committed" && event.stream_name.as_deref() == Some(name)
        })
        .collect()
}

/// The loop's first `split_committed` event for `name`, within the bound. On
/// a timeout the scaler's own evaluation names the half of the loop that
/// failed: a decision still owed means the loop never evaluated; none means
/// it evaluated (its decision holds the stream's cooldown) and no pass ran it.
async fn await_split_committed(state: &crate::http::AppState, name: &str) -> crate::ops::OpsEvent {
    let waited = tokio::time::timeout(SPLIT_BOUND, async {
        loop {
            if let Some(event) = split_events(state, name).pop() {
                return event;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    if let Ok(event) = waited {
        return event;
    }
    let (owed, _) = state.runtime.scaler.evaluate();
    assert!(
        owed.is_empty(),
        "the scaler loop never evaluated within {SPLIT_BOUND:?}: its decision is still owed: {owed:?}"
    );
    panic!(
        "the scaler loop evaluated within {SPLIT_BOUND:?} (its decision holds the stream's cooldown) but no pass ran it"
    );
}

/// The published map: the parent sealed at its three records with both
/// children as successors, the children live on either side of the median.
async fn assert_split_published(
    state: &crate::http::AppState,
    sref: &crate::tenant::TenantStreamRef,
    epoch: &str,
) {
    state.registry.invalidate(sref);
    let after = state.registry.get(sref).await.unwrap().unwrap();
    assert_eq!(after.stream_epoch, epoch, "the split keeps the incarnation");
    let map = after
        .segments
        .as_ref()
        .expect("the split materializes the map");
    assert!(map.pending.is_none(), "the split is published, not pending");
    let shape: Vec<_> = map
        .segments
        .iter()
        .map(|s| {
            let edges = (s.predecessors.clone(), s.successors.clone());
            (
                s.seg_id,
                s.lo,
                s.hi,
                s.is_live(),
                edges,
                s.sealed_next_offset,
            )
        })
        .collect();
    assert_eq!(
        (map.next_seg_id, shape),
        (
            3,
            vec![
                (0, 0, u64::MAX, false, (vec![], vec![1, 2]), Some(3)),
                (1, 0, SPLIT_AT, true, (vec![0], vec![]), None),
                (2, SPLIT_AT, u64::MAX, true, (vec![0], vec![]), None),
            ]
        )
    );
}

/// The loop splits a hot segment at its load-weighted median one cadence
/// after it starts: the evaluation it runs on its own takes the decision,
/// and the pass that follows executes it before the pass deadline. A loop
/// that never starts, or whose pass deadline is already behind it when the
/// pass begins, never splits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_loop_splits_a_hot_segment_at_its_load_weighted_median() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let state = &rig.state;
    let json = [("content-type", "application/json")];
    let created = hreq(rig.addr, "PUT", "/v1/stream/loop-hot", &json, b"").await;
    assert_eq!(created.0, 201);
    let records = br#"[{"n":1},{"n":2},{"n":3}]"#;
    let appended = hreq(rig.addr, "POST", "/v1/stream/loop-hot", &json, records).await;
    assert_eq!(appended.0, 204);
    let sref = state.deployment.raw_adapter_sref("loop-hot");
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    assert!(
        desc.segments.is_none(),
        "an unsplit stream has no stored map"
    );
    feed_heat(state, &desc);
    // The policy splits after two hot evaluations; the test takes the first
    // itself, so the loop's own first evaluation is the deciding one.
    assert_eq!(state.runtime.scaler.evaluate(), (vec![], vec![]));
    let started = tokio::time::Instant::now();
    crate::scaler3::start(std::sync::Arc::downgrade(&rig.state), &rig.tasks);
    let event = await_split_committed(state, "loop-hot").await;
    let cadence = Duration::from_secs(state.config.scaler.eval_secs);
    assert!(
        started.elapsed() >= cadence,
        "the loop evaluates one cadence ({cadence:?}) after it starts, not before"
    );
    let epoch = desc.stream_epoch.as_str();
    assert_eq!(
        (
            event.event_id,
            event.project_id,
            event.stream_id,
            event.fields
        ),
        (
            format!("split/{epoch}/0"),
            Some("proj-test".to_owned()),
            Some(epoch.to_owned()),
            serde_json::json!({"segId": 0, "splitAt": SPLIT_AT, "projectId": "proj-test"}),
        )
    );
    assert_split_published(state, &sref, epoch).await;
    // The split retired the parent's sketch: no heat is left to decide on.
    let stats = state.runtime.scaler.stats_json();
    assert_eq!(
        (&stats["sketches"], &stats["hot_keys"]),
        (&serde_json::json!(0), &serde_json::json!([]))
    );
    // Cancellation stops the loop cooperatively (the rig asserts every task
    // finished), and the loop ran this split exactly once.
    rig.shutdown().await;
    assert_eq!(split_events(state, "loop-hot").len(), 1);
}
