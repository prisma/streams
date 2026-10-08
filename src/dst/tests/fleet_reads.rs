//! The fleet tick reads its members by name (E7): the ordinals the desired
//! count it reads names, every member a read found before, and every router
//! report a listing discovered, by one GET, which a member whose document has
//! not changed answers 304 Not Modified. It lists only to discover members no
//! count names, on its first pass and every 30th after it. A member that
//! boots or stops is read by the first pass that starts after its document
//! changes, as it was when the tick listed them, and an ordinal a count names
//! is read by the pass that reads that count, whatever FLEET_MAX says.
use super::super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::super::fixture_runtime::RigRuntime;
use super::super::fixture_storage::mem;
use super::{peer_heartbeat, settled};
use crate::dst::StoreOp;
use crate::dst::trace_store::{TraceOutcome, TraceStore};
use crate::shard::now_ms;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// The tick reads the URL map once per pass, and nothing else reads it.
const PASS_MARKER: &str = "fleet/urls.json";

/// A fleet store whose desired count only the test writes: the controller's
/// writes of it lose their CAS (the rigs report the whole test process's
/// CPU, so a busy run would otherwise scale the fleet out). Its overrides
/// document answers only the first `answered` reads; a later one waits, so
/// the view of the pass before it stays published.
#[derive(Debug)]
struct Gated {
    inner: Arc<dyn ObjectStore>,
    answered: AtomicU64,
    asked: AtomicU64,
}

impl std::fmt::Display for Gated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gated-fleet")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Gated {
    async fn put_opts(
        &self,
        path: &Path,
        body: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if path.as_ref() == "fleet/desired.json" {
            return Err(object_store::Error::Precondition {
                path: path.to_string(),
                source: "the test holds the desired count".into(),
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
        if path.as_ref() == "fleet/overrides.json" {
            let read = self.asked.fetch_add(1, Ordering::SeqCst);
            while read >= self.answered.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
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

impl Gated {
    /// The store the test writes, the gated store over it, and the traced
    /// store the tick reads.
    fn over_mem() -> (Arc<dyn ObjectStore>, Arc<Gated>, Arc<TraceStore>) {
        let inner = mem();
        let gated = Arc::new(Gated {
            inner: inner.clone(),
            answered: AtomicU64::new(u64::MAX),
            asked: AtomicU64::new(0),
        });
        let trace = TraceStore::verbatim(gated.clone());
        (inner, gated, trace)
    }

    /// Waits until the tick has asked for `reads` overrides reads: each pass
    /// the gate answered has published its view, and the next one waits.
    async fn asked(&self, reads: u64) {
        let asked = || self.asked.load(Ordering::SeqCst) >= reads;
        settled(Duration::from_secs(20), asked).await;
        assert!(asked(), "the tick must reach overrides read {reads}");
    }
}

/// A fleet whose published desired count is `count`, with no overrides.
async fn seed_count(store: &Arc<dyn ObjectStore>, count: u64) {
    let desired = format!(r#"{{"count":{count},"epoch":1,"reason":"seed","computed_at_ms":0}}"#);
    for (path, body) in [
        ("fleet/desired.json", desired),
        ("fleet/overrides.json", r#"{"entries":{}}"#.to_string()),
    ] {
        store
            .put(&Path::from(path), PutPayload::from(body))
            .await
            .unwrap();
    }
}

/// `instance`'s heartbeat stamped `ts_ms`, withdrawn when `withdrawn` says
/// why.
async fn beat(store: &Arc<dyn ObjectStore>, instance: &str, ts_ms: i64, withdrawn: Option<&str>) {
    let withdrawn = withdrawn.map_or(String::new(), |why| format!(r#","withdrawn":"{why}""#));
    let body = format!(
        r#"{{"instance":"{instance}","ts_ms":{ts_ms},"rps":0.0,"owned_shards":[],"draining":false{withdrawn}}}"#
    );
    store
        .put(
            &Path::from(format!("fleet/{instance}.json")),
            PutPayload::from(body),
        )
        .await
        .unwrap();
}

/// `streams-1` with its fleet loop running over `fleet`, configured by `cli`.
async fn tick_over(fleet: Arc<dyn ObjectStore>, cli: fn(&mut crate::config::CliArgs)) -> HttpRig {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some("streams-1".into()),
            cli,
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

async fn stop(rig: HttpRig) {
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// The outcomes of the GETs of `path` that have completed, in order.
fn reads(trace: &TraceStore, path: &str) -> Vec<TraceOutcome> {
    trace
        .events()
        .into_iter()
        .filter(|event| event.op == StoreOp::Get && event.path == path)
        .map(|event| event.outcome)
        .filter(|outcome| *outcome != TraceOutcome::Pending)
        .collect()
}

/// How many passes read the URL map between the latest PUT of `member` and
/// the first GET that returned that document: only a pass already in flight
/// when it landed may miss it.
fn passes_before_read(trace: &TraceStore, member: &str) -> usize {
    let events = trace.events();
    let put = events
        .iter()
        .rev()
        .find(|event| event.op == StoreOp::Put && event.path == member)
        .map(|event| event.seq)
        .expect("the test wrote the member's document");
    let read = events
        .iter()
        .find(|event| {
            event.op == StoreOp::Get
                && event.path == member
                && event.seq > put
                && event.outcome == TraceOutcome::Ok
        })
        .map(|event| event.seq)
        .expect("the tick read the member's changed document");
    events
        .iter()
        .filter(|event| {
            event.op == StoreOp::Get
                && event.path == PASS_MARKER
                && (put..read).contains(&event.seq)
        })
        .count()
}

/// The idle floor's largest request class: the tick listed `fleet/` and
/// `routers/` at every pass (3,467 LISTs an hour per server, each priced as
/// ten GETs). It now lists both once, on its first pass, to discover its
/// members, and names them at every pass after it: the ordinals the desired
/// count names, and the members a read found before (a woken spare above the
/// count, an asleep one, and the router report). Each is read every pass, a
/// member whose document has not changed answers 304 at every pass after the
/// one that first read it, and a FLEET_MAX of 4096 with four members booted
/// asks for no other heartbeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_tick_lists_its_members_only_to_discover_them() {
    let (inner, _, trace) = Gated::over_mem();
    seed_count(&inner, 2).await;
    peer_heartbeat(&inner, "streams-2", 0, 0.0).await;
    peer_heartbeat(&inner, "streams-3", 0, 0.0).await;
    beat(&inner, "streams-4", now_ms() - 3_600_000, None).await;
    let report = format!(
        r#"{{"router":"router-1","ts_ms":{},"client_p50_ms":1.0}}"#,
        now_ms()
    );
    inner
        .put(
            &Path::from("routers/router-1.json"),
            PutPayload::from(report),
        )
        .await
        .unwrap();
    let rig = tick_over(trace.clone(), |cli| cli.fleet_max = 4096).await;
    settled(Duration::from_secs(20), || {
        reads(&trace, PASS_MARKER).len() >= 5
    })
    .await;
    assert!(
        reads(&trace, PASS_MARKER).len() >= 5,
        "five passes must complete their reads"
    );
    let present = BTreeSet::from(
        ["streams-1", "streams-2", "streams-3", "streams-4"]
            .map(|instance| format!("fleet/{instance}.json")),
    );
    let asked: BTreeSet<String> = trace
        .events()
        .into_iter()
        .filter(|event| event.op == StoreOp::Get && event.path.starts_with("fleet/streams-"))
        .map(|event| event.path)
        .collect();
    let absent: Vec<&String> = asked.difference(&present).collect();
    assert!(
        absent.is_empty(),
        "FLEET_MAX names no member, yet {} absent heartbeats were asked for, {:?} among them",
        absent.len(),
        absent.first()
    );
    assert_eq!(asked, present, "every member that exists is read");
    let lists: Vec<String> = trace
        .events()
        .into_iter()
        .filter(|event| event.op == StoreOp::List)
        .map(|event| event.path)
        .collect();
    assert_eq!(
        lists,
        ["fleet", "routers"],
        "the tick lists its members and the router reports once, on its first pass, to discover them"
    );
    for member in [
        "fleet/streams-2.json",
        "fleet/streams-3.json",
        "fleet/streams-4.json",
        "routers/router-1.json",
    ] {
        let outcomes = reads(&trace, member);
        assert!(
            outcomes.len() >= 5,
            "{member} is read every pass: {outcomes:?}"
        );
        assert_eq!(outcomes[0], TraceOutcome::Ok, "{member}: {outcomes:?}");
        assert!(
            outcomes[1..]
                .iter()
                .all(|outcome| *outcome == TraceOutcome::NotModified),
            "an unchanged member answers 304 at every later pass: {member} {outcomes:?}"
        );
    }
    assert_eq!(
        rig.state.ownership.ring_active(),
        ["streams-1", "streams-2"],
        "the ring is the desired count's ordinals"
    );
    stop(rig).await;
}

/// A desired count beyond this runtime's FLEET_MAX (a peer configured larger
/// published it, or FLEET_MIN exceeds FLEET_MAX) names its ordinals in the
/// pass that reads it: the first pass's ring holds every live member the
/// count names, and so does the ring of the first pass that reads a rise.
/// Each ring is the one its pass published: the next pass waits at its
/// overrides read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_beyond_fleet_max_names_its_members_in_the_pass_that_reads_it() {
    let (inner, gated, trace) = Gated::over_mem();
    gated.answered.store(1, Ordering::SeqCst);
    seed_count(&inner, 5).await;
    for peer in ["streams-2", "streams-5"] {
        peer_heartbeat(&inner, peer, 0, 0.0).await;
    }
    let rig = tick_over(trace, |_| {}).await;
    assert_eq!(rig.state.config.cli.fleet_max, 4);
    let ring = || rig.state.ownership.ring_active();
    gated.asked(2).await;
    assert_eq!(
        ring(),
        ["streams-1", "streams-2", "streams-5"],
        "the first pass's ring"
    );

    peer_heartbeat(&inner, "streams-7", 0, 0.0).await;
    seed_count(&inner, 7).await;
    gated.answered.store(3, Ordering::SeqCst);
    gated.asked(4).await;
    assert_eq!(
        ring(),
        ["streams-1", "streams-2", "streams-5", "streams-7"],
        "the ring of the first pass that read the rise"
    );
    stop(rig).await;
}

/// A member that boots (its first beat) or stops (its last beat withdraws
/// it) changes the ring at the first pass that starts after its document
/// lands, the bound the listing gave: only a pass already in flight may miss
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_boots_or_stops_is_read_by_the_next_pass() {
    let trace = TraceStore::verbatim(mem());
    let fleet: Arc<dyn ObjectStore> = trace.clone();
    seed_count(&fleet, 3).await;
    peer_heartbeat(&fleet, "streams-2", 0, 0.0).await;
    let rig = tick_over(fleet.clone(), |_| {}).await;
    let ring = || rig.state.ownership.ring_active();
    settled(Duration::from_secs(10), || {
        ring() == ["streams-1", "streams-2"]
    })
    .await;
    assert_eq!(ring(), ["streams-1", "streams-2"], "streams-3 never booted");

    peer_heartbeat(&fleet, "streams-3", 0, 0.0).await;
    let booted = ["streams-1", "streams-2", "streams-3"];
    settled(Duration::from_secs(10), || ring() == booted).await;
    assert_eq!(ring(), booted, "the booted ordinal joins the ring");
    let missed = passes_before_read(&trace, "fleet/streams-3.json");
    assert!(missed <= 1, "{missed} passes missed the booted ordinal");

    beat(
        &fleet,
        "streams-2",
        now_ms() + 60_000,
        Some("runtime stopping"),
    )
    .await;
    settled(Duration::from_secs(10), || {
        ring() == ["streams-1", "streams-3"]
    })
    .await;
    assert_eq!(
        ring(),
        ["streams-1", "streams-3"],
        "the stopped member leaves the ring"
    );
    let missed = passes_before_read(&trace, "fleet/streams-2.json");
    assert!(missed <= 1, "{missed} passes missed the stopped member");
    stop(rig).await;
}
