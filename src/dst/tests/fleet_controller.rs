//! Entered fleet I/O cancellation and complete ownership-view publication.
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::shard::now_ms;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

#[derive(Debug)]
struct HeldDocument {
    inner: Arc<dyn ObjectStore>,
    path: &'static str,
    write: bool,
    entered: AtomicU64,
    gate: tokio::sync::Semaphore,
}
impl std::fmt::Display for HeldDocument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "held-fleet-document")
    }
}
impl HeldDocument {
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
        let store = Arc::new(HeldDocument {
            inner: inner.clone(),
            path,
            write,
            entered: AtomicU64::new(0),
            gate: tokio::sync::Semaphore::new(0),
        });
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
        if path != "fleet/desired.json" {
            assert_eq!(
                rig.state.ownership.view(),
                prior,
                "unread overrides must not publish a new ring"
            );
        }
        let report = rig.tasks.shutdown(Duration::from_millis(300)).await;
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

/// `instance`'s own heartbeat stamp. A tick publishes its heartbeat before
/// its first read, so a newer stamp proves the previous tick ran to its end.
async fn heartbeat_stamp(store: &Arc<dyn ObjectStore>, instance: &str) -> i64 {
    let path = Path::from(format!("fleet/{instance}.json"));
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    serde_json::from_slice::<crate::fleet::Heartbeat>(&bytes)
        .unwrap()
        .ts_ms
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
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(inner.clone()),
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
    let published = heartbeat_stamp(&inner, "streams-9").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while heartbeat_stamp(&inner, "streams-9").await <= published
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        heartbeat_stamp(&inner, "streams-9").await > published,
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
