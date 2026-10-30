//! The stream-count seed's store work (the owner's round-5 call 2 of
//! 2026-10-15, edge change #143 as amended): a project with a `max_streams`
//! quota counts its streams once per boot, on its first create, by walking
//! its catalog (a LIST, 10, and a GET, 1, of every descriptor under its
//! prefix, tombstones included). A walk that completes is charged to no
//! project, so every served create weighs its own 11 (a 404 and a PUT),
//! whenever the cell booted and however many names the project has held. A
//! walk that fails its page stays charged to the create that made it.

use super::fixture_cell::{CREATE, Cell, CellSpec, error_code, open_cell, project};
use super::fixture_http::engine_shutdown;
use super::fixture_storage::mem;
use crate::dst::fault_store::{FaultPlan, FaultProfile, FaultStore};
use crate::dst::trace_store::TraceStore;
use crate::dst::{ObjClass, StoreOp};
use crate::project_policy::ProjectQuotas;
use crate::store_timing::{StoreResources, TimingStore};
use crate::tenant::ProjectId;
use object_store::ObjectStoreExt;
use std::sync::Arc;

/// A rate whose budget a create's weight leaves far from debt, so each
/// create's weight reads off the level it leaves a full budget at.
const MEASURE: u64 = 1_000;
/// The rate of a project whose second create is admitted only if its first
/// weighed at most 11.
const ELEVEN: u64 = 11;

/// One project with a stream quota, so its first create seeds the count,
/// at `MEASURE` requests a second.
fn measured() -> CellSpec {
    CellSpec {
        quotas: |_| quotas(MEASURE),
        ..CellSpec::open(1)
    }
}

fn quotas(requests_per_sec: u64) -> ProjectQuotas {
    ProjectQuotas {
        requests_per_sec,
        max_streams: 16,
        ..ProjectQuotas::default()
    }
}

/// A store that weighs the registry work of the requests it serves, over
/// `inner`.
fn timed(inner: Arc<dyn object_store::ObjectStore>) -> Arc<dyn object_store::ObjectStore> {
    let io = Arc::new(StoreResources::new(&crate::config::StorageConfig::default()));
    Arc::new(TimingStore::new(inner, io))
}

/// Project 0's store budget now, at `rate` requests a second.
fn level(cell: &Cell, rate: u64) -> f64 {
    let id = ProjectId::new(&project(0)).unwrap();
    let share = cell.state.quotas.park_share(&id, &quotas(rate)).unwrap();
    share.store_ops_level()
}

/// Project 0's catalog prefix, under which its seed walk lists.
fn catalog_prefix() -> String {
    let hex: String = project(0).bytes().map(|b| format!("{b:02x}")).collect();
    format!("registry/v4/projects/{hex}/streams")
}

/// Project 0 creates `name` once its budget has refilled whole: the status,
/// the error code and the weight the create was debited.
async fn weighed_create(cell: &Cell, name: &str) -> (u16, Option<String>, f64) {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (status, _, body) = cell
        .call(0, "PUT", &format!("/v1/streams/{name}"), CREATE)
        .await;
    let weight = MEASURE as f64 - level(cell, MEASURE);
    (status, error_code(&body), weight)
}

/// Three creates on a fresh cell: the first, which walks the empty catalog
/// to seed the count (its LIST weighs 10), weighs 11 like the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn every_served_create_weighs_11_its_seed_walk_included() {
    let cell = open_cell(CellSpec {
        store: timed(mem()),
        ..measured()
    })
    .await;
    let mut weights = Vec::new();
    for name in ["w0", "w1", "w2"] {
        let (status, code, weight) = weighed_create(&cell, name).await;
        assert_eq!((status, code), (201, None), "create {name}");
        weights.push(weight);
    }
    assert_eq!(
        weights, [11.0; 3],
        "three creates' weights from a full budget"
    );
    engine_shutdown(&cell.state).await;
}

/// A project creates and deletes six streams, and the server restarts over
/// the same store: its first create walks six tombstones past an empty
/// descriptor cache and still weighs 11, so at 11 requests a second its next
/// create, right after it, is admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_first_create_after_a_restart_weighs_11_whatever_names_the_project_held() {
    let store = timed(mem());
    let cell = open_cell(CellSpec {
        store: store.clone(),
        ..measured()
    })
    .await;
    for n in 0..6 {
        let path = format!("/v1/streams/gone{n}");
        assert_eq!(cell.call(0, "PUT", &path, CREATE).await.0, 201, "{path}");
        assert_eq!(cell.call(0, "DELETE", &path, b"").await.0, 204, "{path}");
    }
    engine_shutdown(&cell.state).await;
    cell.state.rollup.get().unwrap().db.close().await.unwrap();
    let cell = open_cell(CellSpec {
        store,
        incarnation: 1,
        quotas: |_| quotas(ELEVEN),
        ..CellSpec::open(1)
    })
    .await;
    let first = cell.call(0, "PUT", "/v1/streams/fresh0", CREATE).await.0;
    let after_first = level(&cell, ELEVEN);
    let (second, _, body) = cell.call(0, "PUT", "/v1/streams/fresh1", CREATE).await;
    assert_eq!(
        (first, after_first, second, error_code(&body)),
        (201, 0.0, 201, None),
        "the first two creates since the restart at {ELEVEN} a second"
    );
    engine_shutdown(&cell.state).await;
}

/// A seed walk that fails its page (a key under the project's catalog
/// prefix that no descriptor has, which the page refuses after its LIST)
/// answers `503 catalog_unavailable` and stays charged: the create weighs
/// its 404 and the walk's LIST. Once the key is gone the next create's walk
/// completes, and that create weighs 11.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_seed_walk_that_fails_its_page_stays_charged() {
    let inner = mem();
    let stray = object_store::path::Path::from(format!("{}/stray", catalog_prefix()));
    inner.put(&stray, b"{}".to_vec().into()).await.unwrap();
    let cell = open_cell(CellSpec {
        store: timed(inner.clone()),
        ..measured()
    })
    .await;
    let failed = weighed_create(&cell, "w0").await;
    let unavailable = Some("catalog_unavailable".to_string());
    assert_eq!(
        failed,
        (503, unavailable, 11.0),
        "a create whose walk failed"
    );
    inner.delete(&stray).await.unwrap();
    let next = weighed_create(&cell, "w1").await;
    assert_eq!(
        next,
        (201, None, 11.0),
        "the create once its walk completes"
    );
    engine_shutdown(&cell.state).await;
}

/// Eight first creates of one project sent at once, while each catalog LIST
/// takes 200 ms, walk the project's catalog once between them: the first
/// walks and seeds the count, and the others wait for its result instead of
/// each making the cell walk, uncharged, again. All eight are created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn concurrent_first_creates_walk_the_catalog_once() {
    let slow_list = FaultPlan {
        latency_pct: 100,
        latency_ms: (200, 200),
        ..FaultPlan::CLEAN
    };
    let profile = FaultProfile::clean().with_op_class(StoreOp::List, ObjClass::Other, slow_list);
    let trace = TraceStore::verbatim(FaultStore::new(mem(), 7, profile));
    let cell = open_cell(CellSpec {
        store: timed(trace.clone()),
        ..measured()
    })
    .await;
    let catalog = catalog_prefix();
    let walks = || {
        let listed = |e: &&crate::dst::trace_store::StoreTraceEvent| {
            e.op == StoreOp::List && e.path.starts_with(&catalog)
        };
        trace.events().iter().filter(listed).count()
    };
    let before = walks();
    let creates = (0..8).map(|n| {
        let path = format!("/v1/streams/burst{n}");
        let cell = &cell;
        async move { cell.call(0, "PUT", &path, CREATE).await.0 }
    });
    let statuses = futures_util::future::join_all(creates).await;
    assert_eq!(
        (statuses, walks() - before),
        (vec![201; 8], 1),
        "eight concurrent first creates' statuses and catalog walks"
    );
    engine_shutdown(&cell.state).await;
}
