//! Entered fleet I/O cancellation and complete ownership-view publication.
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::shard::now_ms;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

#[path = "fleet_desired.rs"]
mod desired;
#[path = "fleet_reads.rs"]
mod reads;

#[derive(Debug)]
struct HeldDocument {
    inner: Arc<dyn ObjectStore>,
    path: &'static str,
    write: bool,
    entered: AtomicU64,
    gate: tokio::sync::Semaphore,
    /// Every GET and PUT first waits this long: a store brownout.
    delay: Duration,
}
impl std::fmt::Display for HeldDocument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "held-fleet-document")
    }
}
impl HeldDocument {
    /// Parks every PUT (`write`) or GET of `path` until the test closes the
    /// gate; every other operation passes at once.
    fn held(inner: Arc<dyn ObjectStore>, path: &'static str, write: bool) -> Self {
        HeldDocument {
            inner,
            path,
            write,
            entered: AtomicU64::new(0),
            gate: tokio::sync::Semaphore::new(0),
            delay: Duration::ZERO,
        }
    }

    #[expect(
        clippy::let_underscore_must_use,
        reason = "HeldDocument::enter; the permit is the park itself and is released the moment the test grants it; a held permit would keep the document entered after the test released it"
    )]
    async fn enter(&self, path: &Path, write: bool) {
        if path.as_ref() == self.path && write == self.write {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let _ = self.gate.acquire().await;
        }
    }
}
#[async_trait::async_trait]
impl ObjectStore for HeldDocument {
    async fn put_opts(
        &self,
        path: &Path,
        body: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        tokio::time::sleep(self.delay).await;
        self.enter(path, true).await;
        self.inner.put_opts(path, body, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(
        &self,
        path: &Path,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        tokio::time::sleep(self.delay).await;
        self.enter(path, false).await;
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "fleet cancellation scenario; entering documents, cancelling under partial authority and checking the retained retry form one causal sequence; helper phases would hide which document lost its retry"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "r09_fleet_cancels_entered_documents_without_partial_authority_or_lost_retry; the fixture nests the entered wait and the desired-state poll inside the timeouts that bound them; flattening them would separate the waits from the bounds they must respect"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09_fleet_cancels_entered_documents_without_partial_authority_or_lost_retry() {
    for (path, write) in [
        ("fleet/streams-1.json", true),
        ("fleet/overrides.json", false),
        ("fleet/desired.json", true),
    ] {
        let inner = mem();
        if path != "fleet/desired.json" {
            inner
                .put(
                    &Path::from("fleet/desired.json"),
                    PutPayload::from(
                        r#"{"count":1,"epoch":1,"reason":"source","computed_at_ms":0}"#,
                    ),
                )
                .await
                .unwrap();
        }
        inner
            .put(
                &Path::from("fleet/overrides.json"),
                PutPayload::from(r#"{"entries":{}}"#),
            )
            .await
            .unwrap();
        let store = Arc::new(HeldDocument::held(inner.clone(), path, write));
        let rig = http_rig_build(
            mem(),
            RigRuntime::first(),
            HttpRigOptions {
                fleet_store: Some(store.clone()),
                instance: Some("streams-1".into()),
                ..Default::default()
            },
        )
        .await;
        rig.state.ownership.set_view(
            vec!["prior-owner".into()],
            std::collections::HashMap::from([("00".into(), "prior-owner".into())]),
        );
        let prior = rig.state.ownership.view();
        assert!(crate::fleet::start_configured(
            rig.state.clone(),
            &rig.tasks
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.entered.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("target storage operation must be entered");
        if path == "fleet/overrides.json" {
            assert_eq!(
                rig.state.ownership.view(),
                prior,
                "unread overrides must not publish a new ring"
            );
        }
        let report = rig.tasks.shutdown(Duration::from_secs(4)).await;
        assert!(
            report.aborted.is_empty(),
            "active fleet I/O must cancel cooperatively: {report:?}"
        );
        assert!(
            report
                .outcomes
                .iter()
                .any(|(name, outcome)| *name == "fleet"
                    && *outcome == crate::tasks::TaskOutcome::Finished)
        );
        if path == "fleet/desired.json" {
            assert!(
                matches!(
                    inner.get(&Path::from(path)).await,
                    Err(object_store::Error::NotFound { .. })
                ),
                "held CAS cannot claim publication"
            );
        }
        store.gate.close();
        let retry = crate::tasks::TaskSupervisor::new();
        assert!(crate::fleet::start_configured(rig.state.clone(), &retry));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let desired = rig.state.fleet.read_desired_state().await.unwrap().0;
                if rig.state.ownership.ring_active() == vec!["streams-1".to_string()]
                    && desired.is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("fresh authoritative retry must complete after release");
        let desired = rig
            .state
            .fleet
            .read_desired_state()
            .await
            .unwrap()
            .0
            .unwrap();
        if path == "fleet/desired.json" {
            assert_eq!(desired.pending_events.len(), 1);
            assert_eq!(desired.pending_events[0].event_id, "desired/1");
        }
        assert!(
            retry
                .shutdown(Duration::from_millis(300))
                .await
                .aborted
                .is_empty()
        );
        engine_shutdown(&rig.state).await;
    }
}

/// Poll `ready` until it holds or `budget` elapses. Every wait in this
/// module is bounded so a regression fails by assertion, never by a hang.
async fn settled(budget: Duration, mut ready: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + budget;
    while !ready() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A non-draining heartbeat for `instance` with a trusted https origin,
/// `inflight` admitted requests and `cpu_pct` load, stamped 60 s ahead so it
/// stays inside the 10 s live window for the whole scenario however the
/// ticks are scheduled.
async fn peer_heartbeat(store: &Arc<dyn ObjectStore>, instance: &str, inflight: i64, cpu_pct: f64) {
    let ts_ms = now_ms() + 60_000;
    let body = format!(
        r#"{{"instance":"{instance}","ts_ms":{ts_ms},"rps":0.0,"cpu_pct":{cpu_pct},"inflight":{inflight},"owned_shards":[],"draining":false,"url":"https://{instance}.invalid"}}"#
    );
    store
        .put(
            &Path::from(format!("fleet/{instance}.json")),
            PutPayload::from(body),
        )
        .await
        .unwrap();
}

async fn desired_doc(state: &Arc<crate::http::AppState>) -> crate::fleet::Desired {
    state
        .fleet
        .read_desired_state()
        .await
        .unwrap()
        .0
        .expect("the seeded desired document is always present")
}

/// The reason the scaler publishes ends with the measured rate and the live
/// count: no need computed from an assumed capacity stands between them.
fn assert_reason_ends_with_rate_and_live(reason: &str) {
    let mut names = reason.rsplit(' ').map(|token| token.split('=').next());
    assert_eq!(
        (names.next().flatten(), names.next().flatten()),
        (Some("live"), Some("rps")),
        "the reason ends with the measured rate and the live count: {reason}"
    );
}

/// Router reports are a bucket-writable input that only the scale decision
/// consumes. One unreadable `routers/*.json` used to abandon the whole tick:
/// the ring and peer table stayed stale, a shard the ring had moved away was
/// never yielded, return-home never ran, cell-wide, for as long as the file
/// persisted. Only the desired CAS may wait on that read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreadable_router_report_defers_only_the_desired_publication() {
    let inner = mem();
    // Ring of two, shard 00 overridden to streams-2 long enough ago that
    // return-home may hand it back, and one router report that is not JSON.
    let aged = now_ms() - 301_000;
    for (path, body) in [
        (
            "fleet/desired.json",
            r#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#.to_string(),
        ),
        (
            "fleet/overrides.json",
            format!(r#"{{"entries":{{"00":{{"to":"streams-2","ms":{aged}}}}}}}"#),
        ),
        ("routers/edge-1.json", "not json".to_string()),
    ] {
        inner
            .put(&Path::from(path), PutPayload::from(body))
            .await
            .unwrap();
    }
    peer_heartbeat(&inner, "streams-2", 300, 0.0).await;
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(inner.clone()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    // Possession before the ring exists: this instance serves 00.
    assert!(matches!(
        rig.state
            .shards
            .open_or_wait("00", Duration::from_secs(5))
            .await,
        crate::sharddir::OpenOutcome::Ready(_)
    ));
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));

    // Tick 1 publishes the view, yields 00 and returns the aged override
    // home (CAS); tick 2 mirrors the emptied override map.
    let ring = vec!["streams-1".to_string(), "streams-2".to_string()];
    settled(Duration::from_secs(20), || {
        rig.state.ownership.ring_active() == ring
            && rig.state.shards.held_prefixes().is_empty()
            && rig.state.peer.has_peer("streams-2")
            && rig.state.ownership.overrides().is_empty()
    })
    .await;
    assert_eq!(
        rig.state.ownership.ring_active(),
        ring,
        "an unreadable router report must not freeze ownership publication"
    );
    assert!(
        rig.state.shards.held_prefixes().is_empty(),
        "possession must still yield the moved shard at the tick"
    );
    assert!(
        rig.state.peer.has_peer("streams-2"),
        "the peer table must still be published"
    );
    assert!(
        rig.state.ownership.overrides().is_empty(),
        "return-home must still run and commit while a router report is unreadable"
    );
    // Tick 1 reached its publication site (its override CAS precedes it)
    // with the report unreadable: the desired document is untouched.
    let desired = desired_doc(&rig.state).await;
    assert_eq!(
        (desired.epoch, desired.count),
        (1, 2),
        "only the desired publication is deferred"
    );

    // Readable again: 300 in flight over 105 admitted slots wants a third
    // instance, and the next tick publishes it.
    inner
        .delete(&Path::from("routers/edge-1.json"))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    let mut desired = desired_doc(&rig.state).await;
    while desired.epoch < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        desired = desired_doc(&rig.state).await;
    }
    assert_eq!(
        desired.epoch, 2,
        "the desired publication resumes once the report is readable"
    );
    assert!(
        desired.count >= 3,
        "edge-slot dimension must scale out: {}",
        desired.count
    );
    assert_reason_ends_with_rate_and_live(&desired.reason);

    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(
        report.aborted.is_empty(),
        "fleet loop must cancel cooperatively: {report:?}"
    );
    engine_shutdown(&rig.state).await;
}

/// A ring of two (streams-1, streams-2) and the given shard overrides,
/// written as the fleet writes them.
async fn seed_ring_of_two(store: &Arc<dyn ObjectStore>, entries: &[(&str, &str)]) {
    let overrides = crate::fleet::Overrides {
        entries: entries
            .iter()
            .map(|(prefix, to)| {
                let entry = crate::fleet::OverrideEntry {
                    to: (*to).to_string(),
                    ms: now_ms(),
                };
                ((*prefix).to_string(), entry)
            })
            .collect(),
        ..Default::default()
    };
    for (path, body) in [
        (
            "fleet/desired.json",
            br#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#.to_vec(),
        ),
        (
            "fleet/overrides.json",
            serde_json::to_vec(&overrides).unwrap(),
        ),
    ] {
        store
            .put(&Path::from(path), PutPayload::from(body))
            .await
            .unwrap();
    }
}

/// Item 34: the ring ignores an override whose target is not a member
/// (`effective_owner`, the router mirror), so that target must never open
/// the shard: the open would fence the ring's real owner, the next tick
/// yields it, and after the holdoff the open fires again, indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_override_the_ring_ignores_is_never_opened_by_its_target() {
    let inner = mem();
    seed_ring_of_two(&inner, &[("00", "streams-9")]).await;
    peer_heartbeat(&inner, "streams-1", 0, 0.0).await;
    peer_heartbeat(&inner, "streams-2", 0, 0.0).await;
    // Never parks (its gate is open): it counts the tick's overrides reads,
    // one per pass.
    let store = Arc::new(HeldDocument {
        gate: tokio::sync::Semaphore::new(tokio::sync::Semaphore::MAX_PERMITS),
        ..HeldDocument::held(inner.clone(), "fleet/overrides.json", false)
    });
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(store.clone()),
            instance: Some("streams-9".into()),
            ..Default::default()
        },
    )
    .await;
    let opened = rig.state.shards.open_stats()["started"].clone();
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    let ring = vec!["streams-1".to_string(), "streams-2".to_string()];
    settled(Duration::from_secs(20), || {
        rig.state.ownership.ring_active() == ring
    })
    .await;
    assert_eq!(
        rig.state.ownership.ring_active(),
        ring,
        "the tick must publish the ring"
    );
    assert_eq!(
        rig.state.ownership.effective_owner("00").as_deref(),
        Some("streams-1"),
        "the ring ignores an override to a non-member"
    );
    // The pass that published the ring has read the overrides; a later
    // pass's read proves that pass, and its eager opens, ran to their end.
    let published = store.entered.load(Ordering::SeqCst);
    settled(Duration::from_secs(10), || {
        store.entered.load(Ordering::SeqCst) > published
    })
    .await;
    assert!(
        store.entered.load(Ordering::SeqCst) > published,
        "a second tick must run"
    );
    assert_eq!(
        rig.state.shards.open_stats()["started"],
        opened,
        "an override the ring ignores must never open the shard on its target"
    );
    assert!(
        rig.state.shards.held_prefixes().is_empty(),
        "the target must hold nothing the ring assigns elsewhere"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// Item 34: the rebalancer's target must be a member of the active ring.
/// The idlest heartbeat outside it (a scale-in leftover, a non-ordinal
/// name) would receive an override every reader ignores, so the move would
/// only evict the shard from the laggard and strike its holdoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lagging_owner_moves_its_shard_only_to_an_active_member() {
    let inner = mem();
    seed_ring_of_two(&inner, &[]).await;
    peer_heartbeat(&inner, "streams-2", 0, 50.0).await;
    peer_heartbeat(&inner, "streams-9", 0, 0.0).await;
    let ring = ["streams-1".to_string(), "streams-2".to_string()];
    assert_eq!(
        crate::ownership::ring_pick("00", &ring),
        0,
        "the ring gives 00 to streams-1"
    );
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(inner.clone()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(
        rig.state
            .shards
            .open_or_wait("00", Duration::from_secs(5))
            .await,
        crate::sharddir::OpenOutcome::Ready(_)
    ));
    // Lag no absorber clears (no stream owns this segment), well over the
    // default 60 s threshold, attributed to shard 00.
    let usage = &rig.state.runtime.usage;
    usage.set_absorb_lag(crate::crypto::SegmentHash([0x34; 16]), 120);
    usage.set_shard_lag("00", 120);
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    settled(Duration::from_secs(20), || {
        rig.state.ownership.overrides().contains_key("00")
    })
    .await;
    assert_eq!(
        rig.state
            .ownership
            .overrides()
            .get("00")
            .map(String::as_str),
        Some("streams-2"),
        "a move target must be a member of the active ring"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// Item 34: the instance the ring assigns an overridden shard to opens it at
/// its next tick (the eager handoff fences the previous holder's db), both
/// for a move-in the ring honours and for a shard whose override names a
/// non-member, which the ring gives to its rendezvous home.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_rings_owner_opens_every_overridden_shard_it_is_assigned_at_the_tick() {
    let inner = mem();
    seed_ring_of_two(&inner, &[("00", "streams-9"), ("10", "streams-1")]).await;
    peer_heartbeat(&inner, "streams-2", 0, 0.0).await;
    let ring = ["streams-1".to_string(), "streams-2".to_string()];
    assert_eq!(
        (
            crate::ownership::ring_pick("00", &ring),
            crate::ownership::ring_pick("10", &ring)
        ),
        (0, 1),
        "the ring gives 00 to streams-1 and 10 to streams-2"
    );
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(inner.clone()),
            instance: Some("streams-1".into()),
            prefixes: vec!["00".into(), "10".into()],
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    let shards = &rig.state.shards;
    settled(Duration::from_secs(10), || {
        shards.is_open("00") && shards.is_open("10")
    })
    .await;
    assert!(
        shards.is_open("10"),
        "a move-in the ring honours opens at the tick, not at the first routed request"
    );
    assert!(
        shards.is_open("00"),
        "the ring's owner opens a shard whose override it ignores at the tick"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// A single-instance ring's coordination documents: desired count 1, no
/// overrides.
async fn seed_ring_of_one(store: &Arc<dyn ObjectStore>) {
    for (path, body) in [
        (
            "fleet/desired.json",
            r#"{"count":1,"epoch":1,"reason":"seed","computed_at_ms":0}"#,
        ),
        ("fleet/overrides.json", r#"{"entries":{}}"#),
    ] {
        store
            .put(&Path::from(path), PutPayload::from(body))
            .await
            .unwrap();
    }
}

/// Item 40, the owner's contract: process liveness and controller progress
/// are separate facts. The heartbeat has its own supervised task, so a fleet
/// tick stuck in a coordination read (a wedged controller, or a store that
/// never answers that document) keeps publishing its liveness every beat,
/// while the progress it publishes stays at the tick's last completed pass:
/// peers see a live process whose controller is not progressing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_tick_keeps_its_heartbeat_but_not_its_progress() {
    let inner = mem();
    seed_ring_of_one(&inner).await;
    let store = Arc::new(HeldDocument::held(
        inner.clone(),
        "fleet/overrides.json",
        false,
    ));
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(store.clone()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    settled(Duration::from_secs(5), || {
        store.entered.load(Ordering::SeqCst) > 0
    })
    .await;
    assert_eq!(
        store.entered.load(Ordering::SeqCst),
        1,
        "the tick must reach the held overrides read"
    );
    let mut beats = std::collections::BTreeMap::new();
    let window = tokio::time::Instant::now() + Duration::from_secs(7);
    while tokio::time::Instant::now() < window {
        if let Ok(result) = inner.get(&Path::from("fleet/streams-1.json")).await {
            let doc: serde_json::Value =
                serde_json::from_slice(&result.bytes().await.unwrap()).unwrap();
            beats.insert(
                doc["ts_ms"].as_i64().unwrap(),
                doc["progress_age_ms"].as_i64(),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        store.entered.load(Ordering::SeqCst),
        1,
        "the tick must stay parked in the held read for the whole window"
    );
    assert!(
        beats.len() >= 3,
        "a stuck tick must not stop the heartbeat: {} distinct beats in 7 s",
        beats.len()
    );
    let ages: Vec<i64> = beats
        .values()
        .map(|age| age.expect("every beat publishes its controller's progress"))
        .collect();
    let (first, last) = (beats.keys().next().unwrap(), beats.keys().last().unwrap());
    assert!(
        ages.windows(2).all(|pair| pair[0] < pair[1]),
        "no pass completed, so the published progress age only grows: {beats:?}"
    );
    assert!(
        ages[ages.len() - 1] - ages[0] + 250 >= last - first,
        "the progress age must grow with the wall clock between beats: {beats:?}"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// Item 40: the heartbeat PUT is no longer the tick's first step, so a
/// publication the store holds cannot keep the controller from reading its
/// authority and publishing its ownership view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_heartbeat_does_not_stop_the_tick() {
    let inner = mem();
    seed_ring_of_one(&inner).await;
    let store = Arc::new(HeldDocument::held(
        inner.clone(),
        "fleet/streams-1.json",
        true,
    ));
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(store.clone()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    rig.state
        .ownership
        .set_view(vec!["prior-owner".into()], std::collections::HashMap::new());
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    settled(Duration::from_secs(5), || {
        store.entered.load(Ordering::SeqCst) > 0
    })
    .await;
    let ring = vec!["streams-1".to_string()];
    settled(Duration::from_secs(6), || {
        rig.state.ownership.ring_active() == ring
    })
    .await;
    assert_eq!(
        store.entered.load(Ordering::SeqCst),
        1,
        "the heartbeat PUT must still be held"
    );
    assert_eq!(
        rig.state.ownership.ring_active(),
        ring,
        "the tick must publish the ring while its heartbeat PUT is held"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(5)).await;
    assert!(
        report.aborted.is_empty(),
        "a held heartbeat PUT must cancel cooperatively: {report:?}"
    );
    engine_shutdown(&rig.state).await;
}

/// Item 40, the owner's brownout requirement: a slow store stretches every
/// fleet pass to several tick periods, and that is not a stuck controller.
/// The heartbeat keeps its own cadence, the progress deadline is derived
/// from the budgets a pass may use, so neither instance ever leaves the
/// other's ring.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_brownout_slows_the_tick_without_churning_the_ring() {
    let inner = mem();
    seed_ring_of_two(&inner, &[]).await;
    let rigs = start_two(Arc::new(HeldDocument {
        delay: Duration::from_millis(600),
        ..HeldDocument::held(inner.clone(), "", false)
    }))
    .await;
    let ring = vec!["streams-1".to_string(), "streams-2".to_string()];
    let both = || {
        rigs.iter()
            .all(|rig| rig.state.ownership.ring_active() == ring)
    };
    settled(Duration::from_secs(25), both).await;
    assert!(
        both(),
        "both instances must converge on the two-member ring"
    );
    let mut beats = std::collections::BTreeMap::new();
    let window = tokio::time::Instant::now() + Duration::from_secs(12);
    while tokio::time::Instant::now() < window {
        assert!(both(), "a brownout must not churn the ring");
        for instance in ["streams-1", "streams-2"] {
            let path = Path::from(format!("fleet/{instance}.json"));
            let bytes = inner.get(&path).await.unwrap().bytes().await.unwrap();
            let beat: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            beats.insert(
                (instance, beat["ts_ms"].as_i64()),
                beat["progress_age_ms"].as_u64(),
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for instance in ["streams-1", "streams-2"] {
        let ages: Vec<Option<u64>> = beats
            .iter()
            .filter(|((name, _), _)| *name == instance)
            .map(|(_, age)| *age)
            .collect();
        assert!(
            ages.len() >= 5,
            "{instance} must keep its heartbeat cadence through the brownout: {} beats in 12 s",
            ages.len()
        );
        let passes = ages.windows(2).filter(|pair| pair[1] < pair[0]).count();
        assert!(
            passes >= 1,
            "{instance}'s controller must keep completing passes: {ages:?}"
        );
    }
    for rig in &rigs {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        engine_shutdown(&rig.state).await;
    }
}

/// `streams-1` and `streams-2`, each its own runtime with its fleet loop
/// running, sharing the coordination `store`.
async fn start_two(store: Arc<dyn ObjectStore>) -> Vec<HttpRig> {
    let mut rigs = Vec::new();
    for (incarnation, instance) in [(0, "streams-1"), (1, "streams-2")] {
        let rig = http_rig_build(
            mem(),
            RigRuntime::incarnation(incarnation),
            HttpRigOptions {
                fleet_store: Some(store.clone()),
                instance: Some(instance.into()),
                ..Default::default()
            },
        )
        .await;
        assert!(crate::fleet::start_configured(
            rig.state.clone(),
            &rig.tasks
        ));
        rigs.push(rig);
    }
    rigs
}

/// `instance`'s published heartbeat document, as every peer reads it.
async fn published(store: &Arc<dyn ObjectStore>, instance: &str) -> serde_json::Value {
    let path = Path::from(format!("fleet/{instance}.json"));
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Item 40, instance-wide withdrawal: a runtime whose supervisor lost a
/// Critical loop publishes its withdrawal at its next beat, and every ring,
/// its own included, drops it within a pass, not after the 30 s liveness
/// window. (Under the process root the same loss cancels every task; the
/// heartbeat's last beat then carries the withdrawal, as the next test pins.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_critical_loop_failure_withdraws_the_instance_from_every_ring() {
    let inner = mem();
    seed_ring_of_two(&inner, &[]).await;
    let rigs = start_two(inner.clone()).await;
    let rings_are = |ring: &[&str]| {
        rigs.iter()
            .all(|rig| rig.state.ownership.ring_active() == ring)
    };
    settled(Duration::from_secs(20), || {
        rings_are(&["streams-1", "streams-2"])
    })
    .await;
    assert!(
        rings_are(&["streams-1", "streams-2"]),
        "both rings must converge first"
    );
    assert!(
        rigs[0]
            .tasks
            .spawn("probe", crate::tasks::Policy::Critical, |_| async {
                crate::tasks::TaskResult::Failed("probe lost".into())
            })
            .is_ok()
    );
    settled(Duration::from_secs(10), || rings_are(&["streams-2"])).await;
    assert!(
        rings_are(&["streams-2"]),
        "a lost Critical loop must withdraw its instance from every ring within a pass"
    );
    let withdrawn = published(&inner, "streams-1").await["withdrawn"].clone();
    assert!(
        withdrawn
            .as_str()
            .is_some_and(|reason| reason.contains("probe")),
        "the heartbeat names the withdrawal: {withdrawn}"
    );
    for rig in &rigs {
        assert!(
            rig.tasks
                .shutdown(Duration::from_secs(5))
                .await
                .aborted
                .is_empty()
        );
        engine_shutdown(&rig.state).await;
    }
}

/// Item 40: a stopping runtime does not simply go quiet and leave its peers
/// to wait out the liveness window. Its heartbeat's last beat, bounded,
/// withdraws it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopping_runtime_publishes_its_withdrawal() {
    let inner = mem();
    seed_ring_of_one(&inner).await;
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(inner.clone()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    let path = Path::from("fleet/streams-1.json");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while inner.head(&path).await.is_err() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        published(&inner, "streams-1").await["withdrawn"].is_null(),
        "a running instance is not withdrawn"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(5)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    let withdrawn = published(&inner, "streams-1").await["withdrawn"].clone();
    assert!(
        withdrawn.is_string(),
        "the last beat must withdraw the stopping instance: {withdrawn}"
    );
    engine_shutdown(&rig.state).await;
}
