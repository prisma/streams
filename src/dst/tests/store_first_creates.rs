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

use super::fixture_cell::{CREATE, Cell, CellSpec, open_cell};
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
    let spec = || CellSpec {
        store: store.clone(),
        quotas: seeded,
        ..CellSpec::open(1)
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
    let cell = open_cell(CellSpec {
        incarnation: 1,
        ..spec()
    })
    .await;
    let mut calls: Vec<futures_util::future::BoxFuture<'_, u16>> = (0..streams)
        .map(|n| append(&cell, 0, format!("s{n}")).boxed())
        .collect();
    calls.extend((0..creates).map(|n| create(&cell, 0, format!("new{n}")).boxed()));
    let what = format!(
        "one project, {streams} streams appended and {creates} created at once after a restart"
    );
    measured(&what, &cell, (&gauge, &io), calls).await
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
