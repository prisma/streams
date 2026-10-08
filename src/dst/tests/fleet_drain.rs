//! Item 40, planned drain: a stopping instance hands its ownership off
//! while it keeps beating and keeps its fencing, and a drain that cannot
//! finish says so.
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::fleet::drain::{DrainOutcome, drain};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// The coordination documents of a ring of `count`, no overrides.
async fn seed_ring(fleet: &Arc<dyn ObjectStore>, count: u64) {
    let desired = format!(r#"{{"count":{count},"epoch":1,"reason":"seed","computed_at_ms":0}}"#);
    for (path, body) in [
        ("fleet/desired.json", desired),
        ("fleet/overrides.json", r#"{"entries":{}}"#.to_string()),
    ] {
        fleet
            .put(&Path::from(path), PutPayload::from(body))
            .await
            .unwrap();
    }
}

/// `instance`, its fleet loop running, over the shared `data` and `fleet`
/// stores.
async fn start(
    data: &Arc<dyn ObjectStore>,
    fleet: &Arc<dyn ObjectStore>,
    incarnation: u64,
    instance: &str,
) -> HttpRig {
    let rig = http_rig_build(
        data.clone(),
        RigRuntime::incarnation(incarnation),
        HttpRigOptions {
            fleet_store: Some(fleet.clone()),
            instance: Some(instance.into()),
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    rig
}

/// Polls `ready` until it holds or `budget` elapses.
async fn settled(budget: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    while !ready() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ready()
}

/// `instance`'s published heartbeat document.
async fn published(fleet: &Arc<dyn ObjectStore>, instance: &str) -> serde_json::Value {
    let path = Path::from(format!("fleet/{instance}.json"));
    let bytes = fleet.get(&path).await.unwrap().bytes().await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// A stream the ring gives `rig`, created there with one record.
async fn stream_owned_by(rig: &HttpRig) -> (String, String) {
    let ct = [("content-type", "application/json")];
    for index in 0..64 {
        let name = format!("drain/s{index}");
        let (status, _, _) = hreq(rig.addr, "PUT", &format!("/v1/stream/{name}"), &ct, b"").await;
        if status == 409 {
            continue;
        }
        assert!(status == 200 || status == 201, "create {name}: {status}");
        let (status, _, _) = hreq(
            rig.addr,
            "POST",
            &format!("/v1/stream/{name}"),
            &ct,
            br#"[{"n":1}]"#,
        )
        .await;
        assert!(status == 200 || status == 204, "append {name}: {status}");
        let held = rig.state.shards.held_prefixes();
        assert_eq!(held.len(), 1, "one shard holds the new stream: {held:?}");
        return (name, held[0].clone());
    }
    panic!("the ring gave this instance none of 64 streams");
}

/// The owner's multi-instance drain contract: the draining instance keeps
/// beating (it announces the drain; it never goes dark first), its peer
/// takes the shard over and serves the record it acknowledged while the
/// drain still runs (the peer's open fences the draining writer: fencing,
/// not heartbeat freshness, is what rules out two writers), and the outcome
/// is a handoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_drain_hands_every_shard_to_its_peer_before_the_stop() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 2).await;
    let draining = start(&data, &fleet, 0, "streams-1").await;
    let peer = start(&data, &fleet, 1, "streams-2").await;
    let both = ["streams-1".to_string(), "streams-2".to_string()];
    let converged = settled(Duration::from_secs(20), || {
        [&draining, &peer]
            .iter()
            .all(|rig| rig.state.ownership.ring_active() == both)
    })
    .await;
    assert!(converged, "both rings must hold both instances first");
    let (name, _) = stream_owned_by(&draining).await;

    let done = AtomicBool::new(false);
    let (outcome, (beats, served)) = tokio::join!(
        drain_then_mark(&draining, &done),
        watch(&done, &fleet, &peer, &name)
    );
    assert_eq!(outcome, DrainOutcome::HandedOff);
    assert!(
        !beats.is_empty(),
        "the draining instance must announce its drain while it runs"
    );
    assert!(served, "the peer serves the stream before the drain ends");
    assert!(draining.state.shards.held_prefixes().is_empty());
    assert_eq!(peer.state.ownership.ring_active(), ["streams-2"]);
    assert_eq!(draining.state.ownership.ring_active(), ["streams-2"]);
    let (status, _, body) = hreq(
        peer.addr,
        "GET",
        &format!("/v1/stream/{name}?offset=-1"),
        &[],
        b"",
    )
    .await;
    assert_eq!(status, 200, "the peer serves the handed-off stream");
    assert!(
        String::from_utf8_lossy(&body).contains(r#""n":1"#),
        "the record the draining instance acknowledged is the peer's to serve: {}",
        String::from_utf8_lossy(&body)
    );
    let json = [("content-type", "application/json")];
    let append = format!("/v1/stream/{name}");
    let refused = hreq(draining.addr, "POST", &append, &json, br#"[{"n":2}]"#)
        .await
        .0;
    assert_eq!(
        refused, 409,
        "the drained instance refuses a write it no longer owns"
    );
    let (_, _, body) = hreq(peer.addr, "GET", &format!("{append}?offset=-1"), &[], b"").await;
    assert!(
        !String::from_utf8_lossy(&body).contains(r#""n":2"#),
        "no write lands through the drained instance"
    );
    for rig in [&draining, &peer] {
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

/// A drain whose peer never publishes a view without it runs out of budget
/// and says who still routes to it; it is never reported as a handoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_a_peer_never_acknowledges_times_out_and_names_it() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 2).await;
    let document = format!(
        r#"{{"instance":"streams-2","ts_ms":{},"rps":0.0,"owned_shards":[],"draining":false,"seq":5}}"#,
        crate::shard::now_ms()
    );
    fleet
        .put(
            &Path::from("fleet/streams-2.json"),
            PutPayload::from(document),
        )
        .await
        .unwrap();
    let draining = start(&data, &fleet, 0, "streams-1").await;
    let outcome = drain(&draining.state, "streams-1", Duration::from_secs(3)).await;
    let DrainOutcome::TimedOut { pending } = outcome else {
        panic!("a drain its peer never acknowledged must time out: {outcome:?}");
    };
    assert!(
        pending.contains(&"streams-2 has not read this instance's drain".to_string()),
        "{pending:?}"
    );
    assert!(
        draining
            .tasks
            .shutdown(Duration::from_secs(5))
            .await
            .aborted
            .is_empty()
    );
    engine_shutdown(&draining.state).await;
}

/// A peer of an earlier version publishes no beat sequence and would keep
/// routing to a draining instance, so no drain begins: the drain says so at
/// once and never announces itself, and the instance stops as it did before
/// drains existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_of_an_earlier_version_means_no_drain() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 2).await;
    let document = format!(
        r#"{{"instance":"streams-2","ts_ms":{},"rps":0.0,"owned_shards":[],"draining":false}}"#,
        crate::shard::now_ms()
    );
    fleet
        .put(
            &Path::from("fleet/streams-2.json"),
            PutPayload::from(document),
        )
        .await
        .unwrap();
    let draining = start(&data, &fleet, 0, "streams-1").await;
    let outcome = drain(&draining.state, "streams-1", Duration::from_secs(30)).await;
    assert_eq!(outcome, DrainOutcome::NoPeer);
    assert!(
        !draining.state.fleet.standing().draining(),
        "no drain is announced to a peer that would not honour it"
    );
    assert!(
        draining
            .tasks
            .shutdown(Duration::from_secs(5))
            .await
            .aborted
            .is_empty()
    );
    engine_shutdown(&draining.state).await;
}

/// A fleet of one has no peer to take its ownership: its drain says so at
/// once instead of waiting out its budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fleet_of_one_has_no_peer_to_hand_off_to() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 1).await;
    let alone = start(&data, &fleet, 0, "streams-1").await;
    let started = tokio::time::Instant::now();
    let outcome = drain(&alone.state, "streams-1", Duration::from_secs(30)).await;
    assert_eq!(outcome, DrainOutcome::NoPeer);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(
        alone
            .tasks
            .shutdown(Duration::from_secs(5))
            .await
            .aborted
            .is_empty()
    );
    engine_shutdown(&alone.state).await;
}

/// The wiring, in production's shape: a signal loop requests the stop and
/// ends, the runtime's drain runs before any loop is cancelled, the draining
/// heartbeat is published while the runtime still runs (it also says
/// withdrawn: the ended signal loop is a critical loop), its peer drops it,
/// and only then does the stop follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_requested_stop_drains_the_fleet_runtime_first() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 2).await;
    let draining = start(&data, &fleet, 0, "streams-1").await;
    let peer = start(&data, &fleet, 1, "streams-2").await;
    let both = ["streams-1".to_string(), "streams-2".to_string()];
    let converged = settled(Duration::from_secs(20), || {
        [&draining, &peer]
            .iter()
            .all(|rig| rig.state.ownership.ring_active() == both)
    })
    .await;
    assert!(converged, "both rings must hold both instances first");
    signal_loop(&draining);
    let cancellation = draining.tasks.cancellation();
    let mut announced_while_running = false;
    let mut withdrawn = serde_json::Value::Null;
    let mut health = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !cancellation.is_cancelled() && tokio::time::Instant::now() < deadline {
        let beat = published(&fleet, "streams-1").await;
        if beat["draining"] == true {
            announced_while_running |= !cancellation.is_cancelled();
            withdrawn = beat["withdrawn"].clone();
        }
        if beat["draining"] == true && health.is_none() {
            health = probe_health(draining.addr).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        cancellation.is_cancelled(),
        "the drain's end requests the stop"
    );
    assert!(
        announced_while_running,
        "the drain is announced before any loop is cancelled"
    );
    assert_eq!(withdrawn, "runtime draining");
    assert_eq!(
        health,
        Some((503, "runtime draining".to_string())),
        "while it drains the instance serves and says it drains"
    );
    assert_eq!(peer.state.ownership.ring_active(), ["streams-2"]);
    assert_eq!(
        draining.state.fleet.standing().drain_outcome(),
        Some(DrainOutcome::HandedOff),
        "the drain the stop ran is recorded as a handoff"
    );
    for rig in [&draining, &peer] {
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

/// Drains `streams-1` with a 30 s budget, then marks `done`.
async fn drain_then_mark(draining: &HttpRig, done: &AtomicBool) -> DrainOutcome {
    let outcome = drain(&draining.state, "streams-1", Duration::from_secs(30)).await;
    done.store(true, Ordering::SeqCst);
    outcome
}

/// Until `done`: the draining beats `streams-1` publishes, and whether the
/// peer served `name` (reading it routes it there: the peer opens the shard
/// once its view names it owner).
async fn watch(
    done: &AtomicBool,
    fleet: &Arc<dyn ObjectStore>,
    peer: &HttpRig,
    name: &str,
) -> (std::collections::BTreeSet<Option<i64>>, bool) {
    let (mut beats, mut served) = (std::collections::BTreeSet::new(), false);
    let read = format!("/v1/stream/{name}?offset=-1");
    while !done.load(Ordering::SeqCst) {
        let beat = published(fleet, "streams-1").await;
        if beat["draining"] == true {
            beats.insert(beat["ts_ms"].as_i64());
        }
        served |=
            hreq(peer.addr, "GET", &read, &[], b"").await.0 == 200 && !done.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (beats, served)
}

/// The signal loop's shape (`bootstrap::run`): it requests the stop and
/// ends.
fn signal_loop(rig: &HttpRig) {
    let request = rig.tasks.shutdown_request();
    let spawned = rig.tasks.spawn(
        "signal",
        crate::tasks::Policy::Critical,
        move |_| async move {
            request.request();
            crate::tasks::TaskResult::Done
        },
    );
    assert!(spawned.is_ok());
}

/// `GET /health` on `addr`: its status and body, or `None` when the
/// listener has already closed (a drain that ended first).
async fn probe_health(addr: std::net::SocketAddr) -> Option<(u16, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.ok()?;
    let request = format!("GET /health HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.ok()?;
    let response = String::from_utf8_lossy(&response);
    let status = response.split(' ').nth(1)?.parse().ok()?;
    let body = response.split("\r\n\r\n").nth(1)?.to_string();
    Some((status, body))
}

/// A fleet store the test rigs: reads of its `parked` document never answer
/// (a tick parked on `fleet/overrides.json` never publishes a view, while
/// the heartbeat keeps beating), and writes of its `pinned` document always
/// lose their CAS (the count the test seeded holds).
#[derive(Debug)]
struct RiggedFleet {
    inner: Arc<dyn ObjectStore>,
    /// A coordination document whose reads never answer.
    parked: Option<&'static str>,
    /// A coordination document whose writes always lose their CAS.
    pinned: Option<&'static str>,
    /// While set, every read and listing fails (an unreachable store).
    unreachable: Arc<AtomicBool>,
}

impl RiggedFleet {
    fn refusal(&self) -> Option<object_store::Error> {
        self.unreachable
            .load(Ordering::SeqCst)
            .then(|| object_store::Error::Generic {
                store: "rigged-fleet",
                source: "unreachable".into(),
            })
    }
}

impl std::fmt::Display for RiggedFleet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rigged-fleet")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RiggedFleet {
    async fn put_opts(
        &self,
        path: &Path,
        body: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self.pinned == Some(path.as_ref()) {
            return Err(object_store::Error::Precondition {
                path: path.to_string(),
                source: "pinned by the test".into(),
            });
        }
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
        if let Some(refusal) = self.refusal() {
            return Err(refusal);
        }
        if self.parked == Some(path.as_ref()) {
            std::future::pending::<()>().await;
        }
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
        match self.refusal() {
            Some(refusal) => Box::pin(futures_util::stream::once(async move { Err(refusal) })),
            None => self.inner.list(prefix),
        }
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        if let Some(refusal) = self.refusal() {
            return Err(refusal);
        }
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

/// The owner's contract, with a real second instance: a peer whose tick
/// never publishes a view that read the drain (parked in a coordination
/// read, heartbeat still live) keeps the drain from completing, and the
/// drain says so when its budget runs out; it never reports a handoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_a_live_peer_never_reads_times_out_and_names_it() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 2).await;
    let draining = start(&data, &fleet, 0, "streams-1").await;
    let parked: Arc<dyn ObjectStore> = Arc::new(RiggedFleet {
        inner: fleet.clone(),
        parked: Some("fleet/overrides.json"),
        pinned: None,
        unreachable: Arc::default(),
    });
    let peer = start(&data, &parked, 1, "streams-2").await;
    let beating = tokio::time::timeout(Duration::from_secs(10), async {
        while fleet
            .head(&Path::from("fleet/streams-2.json"))
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok();
    assert!(beating, "the parked peer still publishes its heartbeat");
    let outcome = drain(&draining.state, "streams-1", Duration::from_secs(3)).await;
    let DrainOutcome::TimedOut { pending } = outcome else {
        panic!("a drain its live peer never read must time out: {outcome:?}");
    };
    assert!(
        pending.contains(&"streams-2 has not read this instance's drain".to_string()),
        "{pending:?}"
    );
    for rig in [&draining, &peer] {
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

/// With a desired count of one, a live peer above the count cannot take the
/// sole member's ownership (every ring falls back to it), so the drain says
/// there is no peer at once, announces nothing, and does not wait out its
/// budget. The count is pinned: the rigs report the whole test process's
/// CPU, so a busy run would otherwise scale the fleet out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sole_member_within_the_count_has_no_peer_to_hand_off_to() {
    let data = mem();
    let seeded = mem();
    seed_ring(&seeded, 1).await;
    let fleet: Arc<dyn ObjectStore> = Arc::new(RiggedFleet {
        inner: seeded,
        parked: None,
        pinned: Some("fleet/desired.json"),
        unreachable: Arc::default(),
    });
    let member = start(&data, &fleet, 0, "streams-1").await;
    let above = start(&data, &fleet, 1, "streams-2").await;
    let beating = tokio::time::timeout(Duration::from_secs(10), async {
        while fleet
            .head(&Path::from("fleet/streams-2.json"))
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok();
    assert!(beating, "the peer above the count publishes its heartbeat");
    let started = tokio::time::Instant::now();
    let outcome = drain(&member.state, "streams-1", Duration::from_secs(30)).await;
    assert_eq!(outcome, DrainOutcome::NoPeer);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(
        !member.state.fleet.standing().draining(),
        "nothing is announced"
    );
    for rig in [&member, &above] {
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

/// A fleet of one over a store the test can make unreachable, its desired
/// count pinned at one.
async fn reachable_until_told(data: &Arc<dyn ObjectStore>) -> (HttpRig, Arc<AtomicBool>) {
    let seeded = mem();
    seed_ring(&seeded, 1).await;
    let unreachable = Arc::new(AtomicBool::new(false));
    let fleet: Arc<dyn ObjectStore> = Arc::new(RiggedFleet {
        inner: seeded,
        parked: None,
        pinned: Some("fleet/desired.json"),
        unreachable: unreachable.clone(),
    });
    (start(data, &fleet, 0, "streams-1").await, unreachable)
}

/// A fleet that cannot be read before anything is announced: the drain gives
/// up at one document deadline, well inside its budget, announces nothing,
/// and names what it could not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreadable_fleet_times_out_before_anything_is_announced() {
    let data = mem();
    let (member, unreachable) = reachable_until_told(&data).await;
    unreachable.store(true, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let outcome = drain(&member.state, "streams-1", Duration::from_secs(25)).await;
    let DrainOutcome::TimedOut { pending } = outcome else {
        panic!("an unreadable fleet must time out: {outcome:?}");
    };
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "gave up at one document deadline"
    );
    assert!(
        pending.iter().any(|p| p.contains("unreadable")),
        "{pending:?}"
    );
    assert!(
        !member.state.fleet.standing().draining(),
        "nothing is announced"
    );
    unreachable.store(false, Ordering::SeqCst);
    assert!(
        member
            .tasks
            .shutdown(Duration::from_secs(5))
            .await
            .aborted
            .is_empty()
    );
    engine_shutdown(&member.state).await;
}

/// A read error before the document deadline is retried: once the fleet
/// reads again, the drain reaches its real outcome (a fleet of one has no
/// peer), not a timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_passing_read_error_before_announcing_is_retried() {
    let data = mem();
    let (member, unreachable) = reachable_until_told(&data).await;
    unreachable.store(true, Ordering::SeqCst);
    let readable_again = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        unreachable.store(false, Ordering::SeqCst);
    };
    let (outcome, ()) = futures_util::future::join(
        drain(&member.state, "streams-1", Duration::from_secs(25)),
        readable_again,
    )
    .await;
    assert_eq!(outcome, DrainOutcome::NoPeer);
    assert!(
        member
            .tasks
            .shutdown(Duration::from_secs(5))
            .await
            .aborted
            .is_empty()
    );
    engine_shutdown(&member.state).await;
}

/// Whether `instance` publishes its heartbeat within ten seconds.
async fn beats(fleet: &Arc<dyn ObjectStore>, instance: &str) -> bool {
    let path = Path::from(format!("fleet/{instance}.json"));
    tokio::time::timeout(Duration::from_secs(10), async {
        while fleet.head(&path).await.is_err() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok()
}

/// An instance no desired count names (an ordinal above the count and above
/// its peers' FLEET_MAX; a name that is not an ordinal is the same case)
/// drains like any other: its peer's tick found it by a listing and reads it
/// at every pass, so the peer publishes a view that read its drain, and the
/// drain hands off instead of waiting out its budget. The count is pinned:
/// the rigs report the whole test process's CPU.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_beyond_the_count_is_read_by_its_peer() {
    let data = mem();
    let seeded = mem();
    seed_ring(&seeded, 1).await;
    let fleet: Arc<dyn ObjectStore> = Arc::new(RiggedFleet {
        inner: seeded,
        parked: None,
        pinned: Some("fleet/desired.json"),
        unreachable: Arc::default(),
    });
    let above = start(&data, &fleet, 1, "streams-5").await;
    assert!(beats(&fleet, "streams-5").await, "streams-5 beats first");
    let member = start(&data, &fleet, 0, "streams-1").await;
    assert_eq!(member.state.config.cli.fleet_max, 4);
    assert!(beats(&fleet, "streams-1").await, "its peer beats");
    let outcome = drain(&above.state, "streams-5", Duration::from_secs(20)).await;
    assert_eq!(outcome, DrainOutcome::HandedOff);
    for rig in [&above, &member] {
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
