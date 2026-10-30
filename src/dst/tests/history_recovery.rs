//! History recovery: what the sealer and the handle cache owe a stream
//! across a restart (layout 6). The sealer's open loads every stream whose
//! durable tail holds unsealed pages, so a pre-restart stream converges on
//! a fresh owner with no request; a failed open is retried; a seal is one
//! object for every unsealed stream and leaves appends beside it bounded.

use super::fixture_storage::{
    SealCadence, append_sized, mem, open_engine_with_settings, skey, wait_all_sealed,
};
use crate::dst::{FaultPlan, FaultStore};
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// The scenarios' shard-log settings: a 5 ms flush, a 50 ms manifest poll.
fn log_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(5)),
        manifest_poll_interval: Duration::from_millis(50),
        ..Default::default()
    }
}

/// An engine over `prefix` on the production open path (a reopened log
/// restores its durable backlog), its sealer on production settings.
async fn open(store: &Arc<dyn ObjectStore>, prefix: &str) -> Arc<crate::shard::ShardEngine> {
    open_engine_with_settings(
        store.clone(),
        prefix,
        crate::shard::ShardConfig::default(),
        log_settings(),
    )
    .await
}

/// Owner A: one acked append of each size on `hash` with its sealer
/// stopped (a crash before any seal), then A terminates. Returns A's
/// unsealed ledger, which the next owner must restore exactly.
async fn crash_before_seal(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    hash: [u8; 16],
    sizes: &[usize],
) -> u64 {
    let engine = open(store, prefix).await;
    engine.configure_seal(|s| s.auto = false);
    let key = skey();
    for size in sizes {
        append_sized(&engine, hash, &key, "", *size).await;
    }
    let backlog = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert!(backlog > 0, "owner A's appends left no unsealed backlog");
    engine
        .await_terminated(Duration::from_secs(30))
        .await
        .expect("owner A terminates");
    backlog
}

/// The (sealed boundary, next) watermarks readers see: the handle's
/// published durable tail, never the applied one.
async fn published_tail(engine: &crate::shard::ShardEngine, hash: [u8; 16]) -> (u64, u64) {
    let handle = engine.stream_handle(hash).await.unwrap();
    let s = handle.state.lock().unwrap();
    (s.durable.absorbed, s.durable.next)
}

/// Polls the published tail every 20 ms for up to 30 s (the release suite
/// saturates every core) until it reads `want`; returns the last reading.
async fn wait_published(
    engine: &crate::shard::ShardEngine,
    hash: [u8; 16],
    want: (u64, u64),
) -> (u64, u64) {
    let mut last = published_tail(engine, hash).await;
    for _ in 0..1500 {
        if last == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        last = published_tail(engine, hash).await;
    }
    last
}

/// Polls the shard's unsealed ledger every 20 ms for up to 30 s until it
/// is empty, without touching any stream; whether it emptied.
async fn wait_ledger_empty(engine: &crate::shard::ShardEngine) -> bool {
    for _ in 0..1500 {
        if engine.maintenance_snapshot().unabsorbed_frame_bytes == 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// Memory finding (static audit): resident StreamHandles lived forever —
/// a wide shard held one per stream ever touched. Idle handles with no
/// outside references must evict, and a later touch must reload the
/// same durable state from the shard DB. A handle holding unsealed pages
/// is held by the sealer's unsealed set, so the streams are sealed first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn idle_stream_handles_evict_and_reload() {
    let store: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 95, FaultPlan::new(0, 0, 0));
    let key = skey();
    let engine = open(&store, "dst-evict").await;
    let hashes: Vec<[u8; 16]> = (0u8..8).map(|i| [0xB0 + i; 16]).collect();
    for h in &hashes {
        append_sized(&engine, *h, &key, "", 512).await;
    }
    assert!(engine.resident_streams() >= 8);
    let sealed = engine.seal_requested().await.expect("seal");
    assert_eq!(sealed.streams, 8, "one seal takes every unsealed stream");

    // Give the pipeline a beat so no committer batch still holds clones,
    // then evict with a zero idle threshold: everything unreferenced goes.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let evicted = engine.evict_idle_handles(Duration::from_millis(1), 0);
    assert!(
        evicted >= 8,
        "expected all idle handles evicted, got {evicted}"
    );
    assert_eq!(engine.resident_streams(), 0);

    // Reload: durable state must be intact from the shard DB.
    assert_eq!(
        published_tail(&engine, hashes[0]).await,
        (1, 1),
        "reloaded handle lost durable state"
    );

    // A held reference is untouchable by construction.
    let _held = engine.stream_handle(hashes[1]).await.unwrap();
    let evicted = engine.evict_idle_handles(Duration::from_millis(1), 0);
    assert!(
        engine.resident_streams() >= 1,
        "held handle must survive, evicted={evicted}"
    );
    engine.begin_close();
}

/// Preserve the existing lower order statistic: floor((30 - 1) * p).
/// The fixed 30-probe episode uses indices 14 (p50) and 28 (p99).
async fn paced_append_p99_ms(
    engine: &Arc<crate::shard::ShardEngine>,
    key: &crate::crypto::StreamKey,
    tag: &str,
) -> u128 {
    let probe = [0xA7; 16];
    let mut latencies = [0; 30];
    for sample in &mut latencies {
        let started = std::time::Instant::now();
        append_sized(engine, probe, key, "", 1024).await;
        *sample = started.elapsed().as_millis();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    latencies.sort_unstable();
    eprintln!("{tag}: p50={}ms p99={}ms", latencies[14], latencies[28]);
    latencies[28]
}

/// WC #266, layout 6: one seal over 192 sparse streams (one object, one
/// commit) concurrent with paced appends on the shared 2-worker runtime
/// keeps append p99 within 4x the baseline plus 150 ms, on a store with
/// Tigris-like latency. The sealer's own triggers are off, so the
/// baseline is clean and the wave is the one seal the test starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn sparse_absorption_wave_bounds_append_latency() {
    let plan = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 100,
        latency_ms: (5, 15),
    };
    let store: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 4242, plan);
    let key = skey();
    const N: u8 = 192;
    let hashes: Vec<[u8; 16]> = (0..N)
        .map(|i| [0xA6, i, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        .collect();
    let engine = open(&store, "dst-wave").await;
    engine.configure_seal(|s| s.auto = false);
    // Sparse seed: ~2 KiB per stream — the 10k-tenant field shape in
    // miniature.
    for h in &hashes {
        append_sized(&engine, *h, &key, "", 2048).await;
    }
    // Probe stream must exist before measuring.
    append_sized(&engine, [0xA7; 16], &key, "", 1024).await;
    let base_p99 = paced_append_p99_ms(&engine, &key, "baseline").await;

    // The WAVE: one seal sweeping every sparse stream (and the probe),
    // concurrent with the paced appends.
    #[expect(
        clippy::disallowed_methods,
        reason = "sparse_absorption_wave_bounds_append_latency; this test owns the concurrent seal and joins its actual result before asserting latency; replacing the spawned seal with sequential work would stop exercising cross-worker contention"
    )]
    let wave_task = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.seal_requested().await.expect("wave seal") })
    };
    let wave_p99 = paced_append_p99_ms(&engine, &key, "during-wave").await;
    let wave = wave_task
        .await
        .expect("wave task must complete without panicking");
    assert_eq!(
        wave.streams,
        usize::from(N) + 1,
        "the wave sealed every sparse stream and the probe in one seal"
    );
    assert!(
        wave_p99 <= base_p99 * 4 + 150,
        "append p99 under a sparse seal wave must stay bounded: \
         baseline {base_p99}ms vs wave {wave_p99}ms"
    );
    engine.begin_close();
}

/// Static-audit P1, layout 6: a stream acked but never sealed before a
/// crash converges on a fresh owner with not a single request for it. The
/// sealer's open loads every stream whose durable tail holds unsealed
/// pages, and the fast cadence seals it; the test waits on the shard's
/// ledger, which it can read without touching the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn untouched_streams_absorb_after_restart() {
    let store: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 94, FaultPlan::new(0, 0, 0));
    let hash = [0xA4u8; 16];
    let backlog = crash_before_seal(&store, "dst-restart", hash, &[2 * 1024; 5]).await;

    // Owner B: a fresh engine, an empty key cache (the sealer holds no
    // key), and — the point — no request for the stream.
    let engine_b = open(&store, "dst-restart").await;
    assert_eq!(
        engine_b.maintenance_snapshot().unabsorbed_frame_bytes,
        backlog,
        "the new owner restores the durable backlog exactly"
    );
    let _cadence = SealCadence::start(&engine_b);
    assert!(
        wait_ledger_empty(&engine_b).await,
        "untouched pre-crash stream never sealed after restart"
    );
    // Only now touch the stream. The ledger is published when the seal's
    // group applies, the boundary readers see once it is durable.
    let last = wait_published(&engine_b, hash, (5, 5)).await;
    assert_eq!(last.1, 5, "durable next lost across restart");
    assert_eq!(last.0, last.1, "the sealed boundary must reach next");
    engine_b.begin_close();
}

/// Review round 4, P1, layout 6: restart recovery of one large record
/// whose only due trigger is its age (5 MiB is under the default unsealed
/// and memtable triggers; the test shortens only the age, since the
/// default is 45 s). No request arrives.
///
/// The sealed boundary readers see is published only when the acker
/// dispatches the seal's group after remote durability (G2), and a seal
/// starts by waiting on that dispatch. The dispatch gate is held until the
/// age trigger is long due, so the published tail must stay (0, 1), and
/// must reach (1, 1) once dispatch resumes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_large_record_absorbs_after_restart_under_default_policy() {
    let store: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 98, FaultPlan::new(0, 0, 0));
    let hash = [0xA7u8; 16];
    let backlog = crash_before_seal(&store, "dst-bigrec", hash, &[5 * 1024 * 1024]).await;

    let engine_b = open(&store, "dst-bigrec").await;
    let held = engine_b.test_hold_dispatch().await;
    let age = Duration::from_millis(20);
    engine_b.configure_seal(|s| {
        s.age_seal = age;
        s.tick = age;
    });
    engine_b.seal_ready().await.expect("the sealer opens");
    let due = |m: crate::shard::seal::Measure| m.oldest.is_some_and(|o| o >= age * 5);
    for _ in 0..1500 {
        if due(engine_b.seal_measure()) {
            break;
        }
        tokio::time::sleep(age).await;
    }
    assert!(
        due(engine_b.seal_measure()),
        "the open loaded no unsealed page of the pre-restart stream"
    );
    assert_eq!(
        engine_b.maintenance_snapshot().unabsorbed_frame_bytes,
        backlog
    );
    assert_eq!(published_tail(&engine_b, hash).await, (0, 1));
    drop(held);
    assert_eq!(wait_published(&engine_b, hash, (1, 1)).await, (1, 1));
    assert!(
        wait_ledger_empty(&engine_b).await,
        "the sealed record left unsealed bytes in the ledger"
    );
    engine_b.begin_close();
}

/// Review round 4, P1, layout 6: the sealer's open must RETRY until it
/// succeeds. A failed startup discovery used to strand pre-restart
/// streams for good (no signal, no handle, no rediscovery path). The first
/// two opens on this shard fail; the third loads the stream, which then
/// seals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn dirty_scan_retries_until_it_succeeds() {
    let store: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 99, FaultPlan::new(0, 0, 0));
    let hash = [0xA9u8; 16];
    crash_before_seal(&store, "dst-scanretry", hash, &[2 * 1024]).await;

    crate::shard::inject_seal_open_faults("dst-scanretry", 2);
    let engine_b = open(&store, "dst-scanretry").await;
    let _cadence = SealCadence::start(&engine_b);
    engine_b
        .seal_ready()
        .await
        .expect("the sealer's open recovered from its failures");
    assert_eq!(
        engine_b.seal_json()["open_failures"],
        2,
        "exactly the two injected opens failed"
    );
    wait_all_sealed(&engine_b, &[hash]).await;
    assert_eq!(published_tail(&engine_b, hash).await, (1, 1));
    assert!(wait_ledger_empty(&engine_b).await);
    engine_b.begin_close();
}

/// R26-1 (rewritten from the round-4 deferral companion), layout 6: a tiny
/// record found after restart must AGE-SEAL like anything else. A
/// permanently deferred residual stalls the no-progress clock into the
/// instance latch. Every byte trigger is out of reach, so only the age
/// trigger can seal the 512 B record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn sparse_records_rediscovered_after_restart_are_absorbed() {
    let store: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 100, FaultPlan::new(0, 0, 0));
    let hash = [0xABu8; 16];
    crash_before_seal(&store, "dst-sparse", hash, &[512]).await;

    let engine_b = open(&store, "dst-sparse").await;
    engine_b.configure_seal(|s| {
        s.memtable_bytes = u64::MAX;
        s.unsealed_bytes = u64::MAX;
        s.wal_objects_seal = u64::MAX;
        s.age_seal = Duration::from_millis(1);
        s.tick = Duration::from_millis(50);
    });
    assert!(
        wait_ledger_empty(&engine_b).await,
        "a rediscovered tiny record must age-seal, not defer forever"
    );
    assert_eq!(wait_published(&engine_b, hash, (1, 1)).await, (1, 1));
    engine_b.begin_close();
}

/// Review round 4 (memory): time-based handle eviction alone lets a
/// cardinality burst hold rate × idle-window handles. Past
/// handle_max_resident the ticker must evict oldest-touched
/// unreferenced handles immediately — referenced ones never. The burst
/// is sealed first: a handle holding unsealed pages is never evictable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn handle_capacity_cap_evicts_oldest_first() {
    let store: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 102, FaultPlan::new(0, 0, 0));
    let key = skey();
    let engine = open(&store, "dst-handlecap").await;
    let hashes: Vec<[u8; 16]> = (0u8..12).map(|i| [0xE0 + i; 16]).collect();
    for (i, h) in hashes.iter().enumerate() {
        append_sized(&engine, *h, &key, "", 256).await;
        // Distinct last_touch ordering (ms granularity).
        if i % 3 == 2 {
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
    }
    let sealed = engine.seal_requested().await.expect("seal");
    assert_eq!(sealed.streams, 12, "one seal takes every unsealed stream");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Hold the OLDEST handle: the cap must skip it (referenced) and
    // evict the oldest UNREFERENCED instead.
    let held = engine.stream_handle(hashes[0]).await.unwrap();
    // A fresh burst is NOT idle — the idle pass alone (10 min default)
    // would evict nothing; only the capacity cap can.
    let evicted = engine.evict_idle_handles(Duration::from_secs(600), 4);
    assert!(
        evicted >= 8,
        "cap must evict down toward the bound, got {evicted}"
    );
    assert!(
        engine.resident_streams() <= 4,
        "resident handles above the cap: {}",
        engine.resident_streams()
    );
    // The held handle survived.
    let still = engine.stream_handle(hashes[0]).await.unwrap();
    assert!(
        Arc::ptr_eq(&held, &still),
        "referenced handle must never evict"
    );
    engine.begin_close();
}
