//! The store requests a cell's first creates and appends put in flight on an instance that has
//! served none of them (Addendum 25 of the forward-wedge ruling, 2026-11-20). The combined
//! scouting leg (one project, `--tenancy one`) saw about 250 listings of the other class in flight
//! 2 s after a handoff open of a shard with 578 unsealed streams, filling the general pool beside
//! 185 sheds. What issues them is not known; this DST names it. The gauge of the pile DST
//! (`split_store_inflight`) sits beneath the production store wrapper (`TimingStore`) on a store
//! 20 ms away and reports the requests in flight by kind and path, their call sites, and the count
//! bound's pools: for the leg's case, one project whose streams are created and appended, then the
//! server restarts over the store (an instance that has served none of them, as after a handoff)
//! and the project creates and appends at once; and for many projects' first creates at once.
//! The same case under the instance's in-flight cap at its production 512 (Addendum 26) reports
//! the requests in flight at their peak, the sheds, and each answered request's time by stage,
//! from the store calls its own task made above the production wrapper.

use super::fixture_cell::{CREATE, Cell, CellSpec, error_code, open_cell};
use super::fixture_http::engine_shutdown;
use super::split_store_inflight::{FOLLOW, Gauge, GaugeStore, resolved, sites_in};
use crate::dst::fault_store::{FaultPlan, FaultStore};
use crate::project_policy::ProjectQuotas;
use crate::store_timing::{StoreResources, TimingStore};
use futures_util::FutureExt;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Instant;

/// A project whose first create seeds its stream count, with a request rate no create reaches.
fn seeded(_: usize) -> ProjectQuotas {
    ProjectQuotas {
        requests_per_sec: 100_000,
        max_streams: 10_000,
        ..ProjectQuotas::default()
    }
}

/// A store 20 ms away behind the gauge, beneath the production store wrapper and its own count
/// bound.
fn gauged() -> (Arc<dyn ObjectStore>, Arc<Gauge>, Arc<StoreResources>) {
    let plan = FaultPlan {
        latency_pct: 100,
        latency_ms: (20, 20),
        ..FaultPlan::CLEAN
    };
    let fault = FaultStore::uniform(super::fixture_storage::mem(), 0x5419, plan);
    let gauge = Arc::new(Gauge::default());
    let gauged = GaugeStore {
        inner: fault,
        gauge: gauge.clone(),
    };
    let io = Arc::new(StoreResources::new(&crate::config::StorageConfig::default()));
    (Arc::new(TimingStore::new(gauged, io.clone())), gauge, io)
}

/// What `calls` (each one request's status) put in flight at the store from `begun`, on `cell`.
async fn measured<F: std::future::Future<Output = u16>>(
    what: &str,
    cell: &Cell,
    (gauge, io): (&Gauge, &StoreResources),
    calls: Vec<F>,
) -> String {
    let state = cell.state.clone();
    *gauge.requests.lock().unwrap() = Some(Arc::new(move || state.admission.active()));
    let begun = gauge.start();
    let statuses = futures_util::future::join_all(calls).await;
    let answered = begun.elapsed();
    tokio::time::sleep_until((begun + FOLLOW).into()).await;
    let peak = gauge.window();
    engine_shutdown(&cell.state).await;
    drop(gauge.requests.lock().unwrap().take());
    let sites = resolved(std::mem::take(&mut *gauge.samples.lock().unwrap()));
    let mut answers: Vec<(u16, usize)> = Vec::new();
    for status in statuses {
        match answers.iter_mut().find(|(s, _)| *s == status) {
            Some((_, n)) => *n += 1,
            None => answers.push((status, 1)),
        }
    }
    format!(
        "FIRST_CREATES {what}: answered {answers:?} in {} ms; {}; pools (peak, waits) general \
         {:?} commit {:?}\nFIRST_CREATES   call sites: {}",
        answered.as_millis(),
        peak.shown(6),
        io.count_seen()[0],
        io.count_seen()[1],
        sites_in(&sites, begun, FOLLOW, 8),
    )
}

/// One record appended to project `i`'s stream `name`: its status.
async fn append(cell: &Cell, i: usize, name: String) -> u16 {
    let path = format!("/v1/streams/{name}/records");
    cell.call(i, "POST", &path, br#"{"n":1}"#).await.0
}

/// One stream `name` created by project `i`: its status.
async fn create(cell: &Cell, i: usize, name: String) -> u16 {
    cell.call(i, "PUT", &format!("/v1/streams/{name}"), CREATE)
        .await
        .0
}

/// The leg's case: one project creates `streams` streams and appends to each, the server
/// restarts over its store, and the project then appends to every stream and creates `creates`
/// more at once.
async fn after_a_restart(streams: usize, creates: usize) -> String {
    let (store, gauge, io) = gauged();
    let cell = restarted_cell(&store, streams, 1).await;
    let mut calls: Vec<futures_util::future::BoxFuture<'_, u16>> = (0..streams)
        .map(|n| append(&cell, 0, format!("s{n}")).boxed())
        .collect();
    calls.extend((0..creates).map(|n| create(&cell, 0, format!("new{n}")).boxed()));
    let what = format!(
        "one project, {streams} streams appended and {creates} created at once after a restart"
    );
    measured(&what, &cell, (&gauge, &io), calls).await
}

/// Project 0 of `projects` over `store` creates `streams` streams and appends to each, and the
/// server restarts over the store: the restarted cell, which has served none of them.
async fn restarted_cell(store: &Arc<dyn ObjectStore>, streams: usize, projects: usize) -> Cell {
    let spec = || CellSpec {
        store: store.clone(),
        quotas: seeded,
        ..CellSpec::open(projects)
    };
    let before = open_cell(spec()).await;
    for chunk in (0..streams).collect::<Vec<_>>().chunks(64) {
        let made = chunk.iter().map(|n| create(&before, 0, format!("s{n}")));
        assert!(
            futures_util::future::join_all(made)
                .await
                .iter()
                .all(|s| *s == 201)
        );
        let touched = chunk.iter().map(|n| append(&before, 0, format!("s{n}")));
        drop(futures_util::future::join_all(touched).await);
    }
    engine_shutdown(&before.state).await;
    before.state.rollup.get().unwrap().db.close().await.unwrap();
    open_cell(CellSpec {
        incarnation: 1,
        ..spec()
    })
    .await
}

/// `projects` projects' first creates at once on a cell that has served none of them.
async fn many_projects(projects: usize) -> String {
    let (store, gauge, io) = gauged();
    let cell = open_cell(CellSpec {
        store,
        quotas: seeded,
        ..CellSpec::open(projects)
    })
    .await;
    let calls: Vec<_> = (0..projects)
        .map(|i| create(&cell, i, "first".into()))
        .collect();
    let what = format!("{projects} projects' first creates at once");
    measured(&what, &cell, (&gauge, &io), calls).await
}

/// **The store requests of a cell's first creates and appends on an instance that has served
/// none of them** (a diagnosis for Addendum 25 of the forward-wedge ruling, run under the
/// machine's lock; ignored in the suite): the leg's case, one project of 600 streams appended and
/// 100 created at once after a restart, then 64 and 256 projects' first creates at once. Each
/// reports the peak of store requests in flight by kind and path, their call sites and the count
/// bound's pools. Nothing is asserted of the peaks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a diagnosis, run under the machine's lock"]
async fn store_requests_in_flight_for_first_creates_and_appends() {
    let started = Instant::now();
    println!("{}", after_a_restart(600, 100).await);
    for projects in [64, 256] {
        println!("{}", many_projects(projects).await);
    }
    println!(
        "FIRST_CREATES   ({} ms in all)",
        started.elapsed().as_millis()
    );
}

/// One store call seen above the production store wrapper: the task that made it, its kind and
/// path, and when it began and answered (its count permit's wait included).
type Call = (
    Option<tokio::task::Id>,
    &'static str,
    String,
    Instant,
    Instant,
);

/// A store over `inner` that records every call above `TimingStore`, so a call's time includes
/// its wait for a permit, with the task that made it: a request's own calls run in its
/// connection's task.
#[derive(Debug)]
struct Stages {
    inner: Arc<dyn ObjectStore>,
    calls: std::sync::Mutex<Vec<Call>>,
}

impl std::fmt::Display for Stages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stages({})", self.inner)
    }
}

impl Stages {
    fn note(&self, kind: &'static str, path: &object_store::path::Path, began: Instant) {
        let call = (
            tokio::task::try_id(),
            kind,
            path.to_string(),
            began,
            Instant::now(),
        );
        self.calls.lock().unwrap().push(call);
    }
}

#[async_trait::async_trait]
impl ObjectStore for Stages {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let began = Instant::now();
        let put = self.inner.put_opts(location, payload, opts).await;
        self.note("PUT", location, began);
        put
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let kind = if options.head { "HEAD" } else { "GET" };
        let began = Instant::now();
        let got = self.inner.get_opts(location, options).await;
        self.note(kind, location, began);
        got
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }
}

/// The stream a descriptor GET reads, from its key (`registry::desc_path`: hex of the name).
fn read_name(path: &str) -> Option<String> {
    let hex = path.rsplit_once("/streams/")?.1.strip_suffix(".json")?;
    String::from_utf8(crate::crypto::unhex(hex)?).ok()
}

/// p50, p99 and the most of `values` in milliseconds.
fn quantiles(mut values: Vec<u128>) -> String {
    values.sort_unstable();
    let at = |q: usize| values.get((values.len() * q / 100).min(values.len().saturating_sub(1)));
    let ms = |v: Option<&u128>| v.copied().unwrap_or(0);
    format!(
        "p50 {} p99 {} max {} ms",
        ms(at(50)),
        ms(at(99)),
        ms(values.last())
    )
}

/// Each answered request's own store time by stage, from the calls its task made: the
/// descriptor reads (`registry/`), the engine's reads (`shards/`, the handle load and the tail),
/// and the rest of its answer (admission, the commit and everything not at the store). Calls
/// made by other tasks (the committer, the pump, gc) are counted by stage apart.
fn by_stage(calls: &[Call], answers: &[Answer]) -> String {
    let mut task_of = std::collections::HashMap::new();
    for (task, kind, path, _, _) in calls {
        if let (Some(task), "GET", Some(name)) = (task, *kind, read_name(path)) {
            task_of.entry(name).or_insert(*task);
        }
    }
    let stage = |path: &str| {
        if path.starts_with("registry/") {
            0
        } else if path.starts_with("shards/") {
            1
        } else {
            2
        }
    };
    let mut own: std::collections::HashMap<tokio::task::Id, [u128; 3]> =
        std::collections::HashMap::new();
    let mut others = [0usize; 3];
    let requests: std::collections::HashSet<_> = task_of.values().copied().collect();
    for (task, _, path, began, ended) in calls {
        match task.filter(|t| requests.contains(t)) {
            Some(t) => {
                own.entry(t).or_default()[stage(path)] += ended.duration_since(*began).as_millis()
            }
            None => others[stage(path)] += 1,
        }
    }
    let (mut descriptor, mut engine, mut rest) = (Vec::new(), Vec::new(), Vec::new());
    for (name, _, _, took) in answers.iter().filter(|(_, status, _, _)| *status < 300) {
        let [d, e, _] = task_of
            .get(name)
            .and_then(|t| own.get(t))
            .copied()
            .unwrap_or_default();
        descriptor.push(d);
        engine.push(e);
        rest.push(took.as_millis().saturating_sub(d + e));
    }
    format!(
        "per answered request: descriptor reads {}; engine reads {}; the rest {} ({} of {} joined \
         to their task); calls by other tasks: registry {}, shards {}, other {}",
        quantiles(descriptor),
        quantiles(engine),
        quantiles(rest),
        task_of.len(),
        answers.len(),
        others[0],
        others[1],
        others[2],
    )
}

/// One request, timed: the stream it names, its status and how long it took to answer.
/// One request, answered: the stream it names, its status, its error code and how long it took.
type Answer = (String, u16, Option<String>, std::time::Duration);

/// Project 0's request `method` on `path` with `body`, timed: its answer.
async fn timed(cell: &Cell, name: String, method: &str, path: String, body: &[u8]) -> Answer {
    let sent = Instant::now();
    let (status, _, answer) = cell.call(0, method, &path, body).await;
    (name, status, error_code(&answer), sent.elapsed())
}

/// The leg's case under the instance's in-flight cap at `cap`, its production value (512): project
/// 0 of `projects` creates `streams` streams and appends to each, the server restarts over the
/// store, and project 0 appends to every stream and creates `creates` more at once. With one
/// project its requests are the fair share's and spared up to a quarter over the cap
/// (`fair_shed::inflight_hard_line`); with a second, idle project they are past their share. The
/// instance's requests in flight at their peak, the answers (a `429 rate_limited` is a shed), and each answered
/// request's time by stage.
async fn under_the_cap(streams: usize, creates: usize, cap: i64, projects: usize) -> String {
    let (timing, _, io) = gauged();
    let stages = Arc::new(Stages {
        inner: timing,
        calls: std::sync::Mutex::default(),
    });
    let store: Arc<dyn ObjectStore> = stages.clone();
    let cell = restarted_cell(&store, streams, projects).await;
    cell.state.admission.set_max_inflight(cap);
    stages.calls.lock().unwrap().clear();
    let mut calls: Vec<futures_util::future::BoxFuture<'_, _>> = (0..streams)
        .map(|n| {
            let path = format!("/v1/streams/s{n}/records");
            timed(&cell, format!("s{n}"), "POST", path, br#"{"n":1}"#).boxed()
        })
        .collect();
    calls.extend((0..creates).map(|n| {
        timed(
            &cell,
            format!("new{n}"),
            "PUT",
            format!("/v1/streams/new{n}"),
            CREATE,
        )
        .boxed()
    }));
    let (stop, peak) = (
        std::sync::atomic::AtomicBool::new(false),
        std::sync::atomic::AtomicI64::new(0),
    );
    let sample = async {
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            peak.fetch_max(
                cell.state.admission.active(),
                std::sync::atomic::Ordering::Relaxed,
            );
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    };
    let burst = async {
        let answers = futures_util::future::join_all(calls).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        answers
    };
    let ((), answers) = futures_util::future::join(sample, burst).await;
    engine_shutdown(&cell.state).await;
    // An in-flight shed on the product surface: `429 rate_limited` (`WriteRefusal::Overloaded`).
    let shed: Vec<u128> = answers
        .iter()
        .filter(|(_, s, code, _)| *s == 429 && code.as_deref() == Some("rate_limited"))
        .map(|(_, _, _, t)| t.as_millis())
        .collect();
    let mut statuses = std::collections::BTreeMap::new();
    for (_, status, code, _) in &answers {
        *statuses.entry((*status, code.clone())).or_insert(0usize) += 1;
    }
    let calls = std::mem::take(&mut *stages.calls.lock().unwrap());
    format!(
        "UNDER_CAP project 0 of {projects}, {streams} appended and {creates} created at once after \
         a restart, in-flight cap {cap}: statuses {statuses:?}, requests in flight at their peak {}, the sheds (429 rate_limited) \
         {} answered {}; pools (peak, waits) general {:?} commit {:?}\nUNDER_CAP   {}",
        peak.load(std::sync::atomic::Ordering::Relaxed),
        shed.len(),
        quantiles(shed),
        io.count_seen()[0],
        io.count_seen()[1],
        by_stage(&calls, &answers),
    )
}

/// **A cold engine under the instance's in-flight cap** (a diagnosis for Addendum 26 of the
/// forward-wedge ruling, run under the machine's lock; ignored in the suite): the leg's case,
/// 600 appends and 100 creates at once on one project after a restart, at the production cap of
/// 512 requests in flight; then 1,000 appends (past the spared line of 640), and 600 from one of
/// two projects (past its fair share, so not spared). It reports the peak of requests in flight, the sheds and, for each
/// answered request, the time its own task spent in descriptor reads and engine reads and the
/// rest. Nothing is asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a diagnosis, run under the machine's lock"]
async fn a_cold_engine_under_the_in_flight_cap() {
    for (streams, projects) in [(600, 1), (1_000, 1), (600, 2)] {
        println!("{}", under_the_cap(streams, 100, 512, projects).await);
    }
}
