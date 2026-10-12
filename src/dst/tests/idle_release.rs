//! Idle release (Q0): an instance that has seen no data-plane request for two
//! minutes, with nothing left to absorb, releases its block caches and its
//! postings slices; health and operator requests do not hold it back, and the
//! next read returns the same records and starts a new quiet period.

use super::fixture_http::{HttpRigOptions, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::dst::trace_store::TraceStore;
use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
use crate::runtime::ManualClock;
use std::sync::atomic::Ordering;
use std::time::Duration;

const TICK: Duration = Duration::from_secs(2);

/// Real time between two manual ticks, for the loop to read each one.
const SPACING: Duration = Duration::from_millis(50);

/// Advances the manual clock `ticks` loop ticks, `SPACING` apart.
async fn tick(clock: &ManualClock, ticks: u32) {
    for _ in 0..ticks {
        clock.advance(TICK);
        tokio::time::sleep(SPACING).await;
    }
}

/// Advances the manual clock one loop tick at a time until `done` holds,
/// giving the loop up to 2 s of real time after each tick, at most `ticks`
/// ticks: a loaded host delays the loop's reading, never the outcome.
/// Whether it held.
async fn tick_until(clock: &ManualClock, ticks: u32, done: impl Fn() -> bool) -> bool {
    for _ in 0..ticks {
        clock.advance(TICK);
        for _ in 0..40 {
            if done() {
                return true;
            }
            tokio::time::sleep(SPACING).await;
        }
    }
    false
}

/// Both streams' records, as a reader sees them.
async fn records(addr: std::net::SocketAddr) -> Vec<Vec<u8>> {
    let mut all = Vec::new();
    for stream in ["idle-a", "idle-b"] {
        let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{stream}"), &[], b"").await;
        assert_eq!(status, 200, "read {stream}");
        all.push(body);
    }
    all
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_instance_releases_its_caches_and_the_next_read_refills_them() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let (state, addr, clock) = (rig.state.clone(), rig.addr, rig.clock.clone());
    let ct = [("content-type", "application/json")];
    for stream in ["idle-a", "idle-b"] {
        let path = format!("/v1/stream/{stream}");
        let (status, _, _) = hreq(addr, "PUT", &path, &ct, b"").await;
        assert!(status == 200 || status == 201, "create {stream}: {status}");
        for n in 0..8 {
            let body = format!("[{{\"n\":{n},\"pad\":\"{}\"}}]", "x".repeat(2048));
            let (status, _, _) = hreq(addr, "POST", &path, &ct, body.as_bytes()).await;
            assert!(status == 200 || status == 204, "append {stream}: {status}");
        }
    }
    let runtime = &state.runtime;
    let absorbed = tokio::time::timeout(Duration::from_secs(30), async {
        while crate::backpressure::snapshot(&state.shards).unabsorbed_bytes_instance > 0
            || runtime.history.cache.inserted_bytes() == 0
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    absorbed
        .await
        .expect("the absorber flushed both streams into history, cached on write");
    let before = records(addr).await;
    assert_ne!(
        runtime.postings.stats()["entries"],
        0,
        "the absorber installed postings"
    );

    crate::bootstrap::idle::start(&state, &rig.tasks);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let released = || runtime.history.cache.releases();
    // Health and operator requests every 30 s do not count as arrivals:
    // nothing is released by 118 s, and the release follows.
    for ticks in [15, 15, 15, 14] {
        for path in ["/health", "/v1/debug/load"] {
            let (status, _, _) = hreq(addr, "GET", path, &[], b"").await;
            assert_eq!(status, 200, "{path}");
        }
        tick(&clock, ticks).await;
        assert_eq!(released(), 0, "released before the quiet period ended");
    }
    assert!(tick_until(&clock, 30, || released() > 0).await, "released");
    assert_eq!(released(), 1);
    assert_eq!(runtime.shared_cache.releases(), 1);
    assert_eq!(runtime.telemetry.cache.releases(), 1);
    assert_eq!(runtime.history.cache.inserted_bytes(), 0);
    assert_eq!(runtime.postings.stats()["entries"], 0);

    assert_eq!(records(addr).await, before, "a read after the release");
    tick(&clock, 58).await;
    assert_eq!(released(), 1, "the read started a new quiet period");
    assert!(
        tick_until(&clock, 30, || released() > 1).await,
        "released again"
    );
    rig.shutdown().await;
}

/// The engine's flush ticker flushes a shard's memtable only after appends,
/// so what the absorber writes once appends stop (its maintenance deletes)
/// stays in memory at idle: about 20 MB over four shards in the Q0 rig's F3
/// posture. The idle release flushes every shard's memtable to an L0 table
/// before it empties the caches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_release_flushes_what_the_shard_memtables_hold() {
    let trace = TraceStore::verbatim(mem());
    let rig = http_rig_build(
        trace.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let (state, addr, clock) = (rig.state.clone(), rig.addr, rig.clock.clone());
    let ct = [("content-type", "application/json")];
    let (status, _, _) = hreq(addr, "PUT", "/v1/stream/idle-flush", &ct, b"").await;
    assert!(status == 200 || status == 201, "create: {status}");
    let (status, _, _) = hreq(addr, "POST", "/v1/stream/idle-flush", &ct, b"[{\"n\":1}]").await;
    assert!(status == 200 || status == 204, "append: {status}");
    let engine = state
        .shards
        .engines()
        .pop()
        .expect("the stream's shard is open");
    // Past the flush ticker's last tick after the append (5 s), a write the
    // engine does not count as an append stays in the memtable.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let probe = b"\xffq0-idle-flush-probe".as_slice();
    engine.db.put(probe, b"held").await.unwrap();
    let flushed_tables = || {
        trace
            .events()
            .iter()
            .filter(|e| e.op == StoreOp::Put && e.path.contains("shard/compacted/"))
            .count()
    };
    let before = flushed_tables();

    crate::bootstrap::idle::start(&state, &rig.tasks);
    tokio::time::sleep(SPACING).await;
    tick(&clock, 59).await;
    let released = || state.runtime.history.cache.releases() > 0;
    assert!(tick_until(&clock, 30, released).await, "released");
    assert!(
        flushed_tables() > before,
        "the release wrote no L0 table for the shard's memtable"
    );
    let held = engine.db.get(probe).await.unwrap();
    assert_eq!(held.as_deref(), Some(b"held".as_slice()));
    rig.shutdown().await;
}

/// A request that arrives while a release flushes the shard memtables ends
/// the quiet period: the release is dropped, so the caches are not emptied
/// under that client's first requests, and the next quiet period releases.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_during_the_release_flush_drops_that_release() {
    let store = FaultStore::new(mem(), 7, FaultProfile::clean());
    let rig = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let (state, addr, clock) = (rig.state.clone(), rig.addr, rig.clock.clone());
    let ct = [("content-type", "application/json")];
    let path = "/v1/stream/idle-flush-read";
    let (status, _, _) = hreq(addr, "PUT", path, &ct, b"").await;
    assert!(status == 200 || status == 201, "create: {status}");
    let (status, _, _) = hreq(addr, "POST", path, &ct, b"[{\"n\":1}]").await;
    assert!(status == 200 || status == 204, "append: {status}");
    let engine = state
        .shards
        .engines()
        .pop()
        .expect("the stream's shard is open");
    // Past the flush ticker's last tick, a row the release must flush.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let probe = b"\xffq0-idle-flush-read-probe".as_slice();
    engine.db.put(probe, b"held").await.unwrap();
    // The release's L0 table PUT parks (the Db lives at `{prefix}/shard`).
    let held = state.shards.held_prefixes();
    assert_eq!(held.len(), 1, "one shard serves the stream: {held:?}");
    let tables = format!(
        "{}/compacted/",
        object_store::path::Path::from(format!("{}/shard", held[0]))
    );
    let parked = store.hold_class_under(StoreOp::Put, ObjClass::Sst, &tables, 1);

    crate::bootstrap::idle::start(&state, &rig.tasks);
    tokio::time::sleep(SPACING).await;
    tick(&clock, 59).await;
    let flushing = || parked.load(Ordering::SeqCst) > 0;
    assert!(
        tick_until(&clock, 30, flushing).await,
        "the release's flush parked"
    );
    let (status, _, _) = hreq(addr, "GET", path, &[], b"").await;
    assert_eq!(status, 200, "a read while the release flushes");
    store.release_hold();
    let released = || state.runtime.history.cache.releases();
    tick(&clock, 58).await;
    assert_eq!(released(), 0, "the caches were released under the read");
    assert!(
        tick_until(&clock, 30, || released() > 0).await,
        "the next quiet period released"
    );
    let held = engine.db.get(probe).await.unwrap();
    assert_eq!(held.as_deref(), Some(b"held".as_slice()));
    rig.shutdown().await;
}
