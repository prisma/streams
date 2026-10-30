//! Idle release (Q0): an instance that has seen no data-plane request for two
//! minutes, with less than 1 MiB unsealed, seals every open shard and releases
//! its shared and telemetry block caches; health and operator requests do not
//! hold it back, and the next read returns the same records and starts a new
//! quiet period.

use super::fixture_http::{HttpRigOptions, cold_sealer, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
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

/// A raw stream's (sealed boundary, durable next) on its engine.
async fn sealed_through(state: &crate::http::AppState, stream: &str) -> (u64, u64) {
    let sref = state.deployment.raw_adapter_sref(stream);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let route = desc.segment_route_by_id(0).unwrap();
    let engine = state.engine_for(&route).await.unwrap();
    let handle = engine.stream_handle(desc.storage_hash()).await.unwrap();
    let state = handle.state.lock().unwrap();
    (state.durable.absorbed, state.durable.next)
}

/// A rig whose sealer never seals by itself: what is sealed, the idle
/// release sealed, and the release's forced flush is the only one.
fn idle_rig_options() -> HttpRigOptions {
    HttpRigOptions {
        seal: Some(cold_sealer),
        ..HttpRigOptions::default()
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_idle_instance_releases_its_caches_and_the_next_read_refills_them() {
    let store = FaultStore::new(mem(), 7, FaultProfile::clean());
    let rig = http_rig_build(store.clone(), RigRuntime::first(), idle_rig_options()).await;
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
    // Eight one-record appends per stream, all unsealed: a backlog under the
    // release's 1 MiB drain line.
    for stream in ["idle-a", "idle-b"] {
        assert_eq!(sealed_through(&state, stream).await, (0, 8), "{stream}");
    }
    let before = records(addr).await;

    crate::bootstrap::idle::start(&state, &rig.tasks);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let released = || runtime.shared_cache.releases();
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
    assert_eq!(runtime.telemetry.cache.releases(), 1);
    // The release sealed every record before it emptied the caches, so the
    // read below is served from the segment objects.
    for stream in ["idle-a", "idle-b"] {
        assert_eq!(sealed_through(&state, stream).await, (8, 8), "{stream}");
    }
    assert_eq!(
        crate::backpressure::snapshot(&state.shards).unabsorbed_bytes_instance,
        0
    );

    assert_eq!(records(addr).await, before, "a read after the release");
    tick(&clock, 58).await;
    assert_eq!(released(), 1, "the read started a new quiet period");
    assert!(
        tick_until(&clock, 30, || released() > 1).await,
        "released again"
    );
    rig.shutdown().await;
}

/// What a shard writes once appends stop (the queue collector, the billing
/// drain) stays in its memtable at idle unless something flushes it. The idle
/// release's seal flushes every shard's memtable to an L0 table before it
/// empties the caches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_idle_release_flushes_what_the_shard_memtables_hold() {
    let trace = TraceStore::verbatim(mem());
    let rig = http_rig_build(trace.clone(), RigRuntime::first(), idle_rig_options()).await;
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
    // The sealer never flushes by itself here: a write the engine does not
    // count as an append stays in the memtable.
    let probe = b"S\xffq0-idle-flush-probe".as_slice();
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
    let released = || state.runtime.shared_cache.releases() > 0;
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_read_during_the_release_flush_drops_that_release() {
    let store = FaultStore::new(mem(), 7, FaultProfile::clean());
    let rig = http_rig_build(store.clone(), RigRuntime::first(), idle_rig_options()).await;
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
    // The sealer never flushes by itself here: a row the release must flush.
    let probe = b"S\xffq0-idle-flush-read-probe".as_slice();
    engine.db.put(probe, b"held").await.unwrap();
    // The release seal's forced flush parks on its L0 table PUT (the Db lives
    // at `{prefix}/shard`; segment objects go under `hseg/`).
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
    let released = || state.runtime.shared_cache.releases();
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

/// One key's records of stream `press` as a product reader pages them: the
/// `n` of each, in order.
async fn keyed(addr: std::net::SocketAddr, routing: &str) -> Vec<u64> {
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (mut cursor, mut seen) = ("beginning".to_string(), Vec::new());
    for _ in 0..64 {
        let path = format!("/v1/streams/press/records?routingKey={routing}&cursor={cursor}");
        let (status, headers, body) = preq(addr, "GET", &path, &key, b"").await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        let page: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        seen.extend(page.iter().map(|v| v["n"].as_u64().unwrap()));
        if headers.get("prisma-up-to-date").map(String::as_str) == Some("true") {
            return seen;
        }
        cursor = headers["prisma-next-cursor"].clone();
    }
    panic!("{routing}: no end after 64 pages");
}

/// Seals every open shard, then waits until nothing is unsealed, at most 30 s.
async fn all_sealed(state: &crate::http::AppState) {
    seal_every_shard(state).await;
    let drained = async {
        while crate::backpressure::snapshot(&state.shards).unabsorbed_bytes_instance > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(30), drained)
        .await
        .expect("everything sealed");
}

/// One requested seal of every open shard.
async fn seal_every_shard(state: &crate::http::AppState) {
    for engine in state.shards.engines() {
        engine.seal_requested().await.expect("a requested seal");
    }
}

/// The routing keys of stream `press`.
const KEYS: [&str; 3] = ["ka", "kb", "kc"];

/// Append records `ns` to every key of stream `press`, each served.
async fn append_keyed(addr: std::net::SocketAddr, ns: std::ops::Range<u64>) {
    for n in ns {
        for routing in KEYS {
            let headers = [
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", routing),
            ];
            let body = format!(r#"{{"n":{n},"pad":"{}"}}"#, "x".repeat(4096));
            let path = "/v1/streams/press/records";
            let (status, _, _) = preq(addr, "POST", path, &headers, body.as_bytes()).await;
            assert_eq!(status, 200, "append {routing} {n}");
        }
    }
}

/// Read every key until `done`, each read a gapless prefix: the reads made.
async fn read_prefixes(addr: std::net::SocketAddr, done: &std::sync::atomic::AtomicBool) -> usize {
    let mut reads = 0;
    while !done.load(Ordering::Relaxed) {
        for routing in KEYS {
            let seen = keyed(addr, routing).await;
            let prefix: Vec<u64> = (0..seen.len() as u64).collect();
            assert_eq!(seen, prefix, "{routing} mid-load");
            reads += 1;
        }
    }
    reads
}

/// Pressure releases (RSS over the shed line: block caches dropped, registry
/// slots swept) run beside live keyed appends, keyed readers and seals that
/// move every shard's sealed boundary each round, and every reader sees
/// exactly its key's records, in order, with no gap or repeat: during the
/// load a prefix, after it all of them (mem2 review L5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn pressure_releases_beside_live_keyed_reads_keep_every_record() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let (state, addr, clock) = (rig.state.clone(), rig.addr, rig.clock.clone());
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let format = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(
        preq(addr, "PUT", "/v1/streams/press", &key, format).await.0,
        201
    );
    crate::bootstrap::idle::start(&state, &rig.tasks);
    state.admission.set_rss_shed_mb(500);
    let (rounds, per) = (6_u64, 8_u64);
    let done = std::sync::atomic::AtomicBool::new(false);
    let releases = || state.runtime.shared_cache.releases();
    let load = async {
        for round in 0..rounds {
            state.admission.record_rss_mb(0);
            append_keyed(addr, round * per..(round + 1) * per).await;
            seal_every_shard(&state).await;
            state.admission.record_rss_mb(501);
            let before = releases();
            tick(&clock, 14).await;
            let released = tick_until(&clock, 4, || releases() > before).await;
            assert!(released, "round {round}");
        }
        state.admission.record_rss_mb(0);
        all_sealed(&state).await;
        done.store(true, Ordering::Relaxed);
    };
    let ((), reads) = futures_util::future::join(load, read_prefixes(addr, &done)).await;
    assert!(reads > 0, "the readers read during the load");
    let all: Vec<u64> = (0..rounds * per).collect();
    for routing in KEYS {
        assert_eq!(keyed(addr, routing).await, all, "{routing} after the load");
    }
    assert!(releases() >= rounds, "a release each round");
    rig.shutdown().await;
}
