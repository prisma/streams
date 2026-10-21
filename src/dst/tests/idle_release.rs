//! Idle release (Q0): an instance that has seen no data-plane request for two
//! minutes, with nothing left to absorb, releases its block caches and its
//! postings slices; health and operator requests do not hold it back, and the
//! next read returns the same records and starts a new quiet period.

use super::fixture_http::{HttpRigOptions, http_rig_build};
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
    let store = FaultStore::new(mem(), 7, FaultProfile::clean());
    let rig = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
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
    // The history partitions' compactors run on real time, not the manual
    // clock: one that writes a table between the release and the reading
    // below caches its index (48 bytes) in the new generation, as idle.rs
    // allows (`REFILL_FLOOR`). History tables are written only once the
    // release is read; the release's own flush writes shard tables.
    store.hold_class_containing(
        StoreOp::Put,
        ObjClass::Sst,
        "/history2/compacted/",
        u64::MAX,
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
    store.release_hold();

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

/// Waits until the absorber owes nothing, at most 30 s.
async fn absorbed(state: &crate::http::AppState) {
    let drained = async {
        while crate::backpressure::snapshot(&state.shards).unabsorbed_bytes_instance > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(30), drained)
        .await
        .expect("absorbed");
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

/// Pressure releases (RSS over the shed line: block caches dropped, postings
/// swept at a zero horizon, registry slots swept) run beside live keyed
/// appends, keyed readers and the absorber's installs, and every reader
/// sees exactly its key's records, in order, with no gap or repeat: during
/// the load a prefix, after it all of them (mem2 review L5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
            state.admission.record_rss_mb(501);
            let before = releases();
            tick(&clock, 14).await;
            let released = tick_until(&clock, 4, || releases() > before).await;
            assert!(released, "round {round}");
        }
        state.admission.record_rss_mb(0);
        absorbed(&state).await;
        done.store(true, Ordering::Relaxed);
    };
    let ((), reads) = futures_util::future::join(load, read_prefixes(addr, &done)).await;
    assert!(reads > 0, "the readers read during the load");
    let all: Vec<u64> = (0..rounds * per).collect();
    for routing in KEYS {
        assert_eq!(keyed(addr, routing).await, all, "{routing} after the load");
    }
    assert!(releases() >= rounds, "a release each round");
    let entries = &state.runtime.postings.stats()["entries"];
    assert_ne!(
        entries, 0,
        "the absorber installed postings the readers used"
    );
    rig.shutdown().await;
}
