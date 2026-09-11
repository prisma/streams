//! History recovery.

use super::fixture_storage::{append_n, append_sized, mem, skey, wait_all_absorbed};
use crate::dst::{FaultPlan, FaultStore};
use object_store::ObjectStore;
use std::sync::Arc;

/// Memory finding (static audit): resident StreamHandles lived forever —
/// a wide shard held one per stream ever touched. Idle handles with no
/// outside references must evict, and a later touch must reload the
/// same durable state from the shard DB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_stream_handles_evict_and_reload() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 95, FaultPlan::new(0, 0, 0));
    let key = skey();

    let db = slatedb::Db::builder("dst-evict", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-evict".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let hashes: Vec<[u8; 16]> = (0u8..8).map(|i| [0xB0 + i; 16]).collect();
    for h in &hashes {
        append_sized(&engine, *h, &key, "", 512).await;
    }
    assert!(engine.resident_streams() >= 8);

    // Give the pipeline a beat so no committer batch still holds clones,
    // then evict with a zero idle threshold: everything unreferenced goes.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let evicted = engine.evict_idle_handles(std::time::Duration::from_millis(1), 0);
    assert!(
        evicted >= 8,
        "expected all idle handles evicted, got {evicted}"
    );
    assert_eq!(engine.resident_streams(), 0);

    // Reload: durable state must be intact from the shard DB.
    let st = engine.stream_handle(hashes[0]).await.unwrap();
    let n = { st.state.lock().unwrap().durable.next };
    assert_eq!(n, 1, "reloaded handle lost durable state");

    // A held reference is untouchable by construction.
    let _held = engine.stream_handle(hashes[1]).await.unwrap();
    let evicted = engine.evict_idle_handles(std::time::Duration::from_millis(1), 0);
    assert!(
        engine.resident_streams() >= 1,
        "held handle must survive, evicted={evicted}"
    );
    engine.begin_close();
}

/// Release blocker (review round 4, P0): a second absorption wave across
/// many mature streams used to expand into ONE WriteBatch of
/// streams × max_trim_per_op deletes (67M at the wide posture — a
/// multi-GiB batch). Boundary publication and physical trimming are now
/// decoupled: the advance batch trims at most TRIM_GLOBAL_BUDGET
/// deletes, the remainder becomes trim debt, and TrimTick maintenance
/// drains it a budgeted slice per commit — including via the 5 s flush
/// ticker with no test involvement.
/// One look at every stream's durable tail: true when `ready` holds for all.
async fn all_tails(
    engine: &crate::shard::ShardEngine,
    hashes: &[[u8; 16]],
    ready: impl Fn(&crate::shard::TailFields) -> bool,
) -> bool {
    for hash in hashes {
        let handle = engine.stream_handle(*hash).await.unwrap();
        let tail = handle.state.lock().unwrap().durable.clone();
        if !ready(&tail) {
            return false;
        }
    }
    true
}

#[expect(
    clippy::too_many_lines,
    reason = "budgeted trim scenario; seeding two waves, draining under the global budget and checking every stream's convergence form one causal sequence on one engine; helper phases would hide which wave exceeded the budget"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_absorption_wave_trims_under_a_global_budget() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 96, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hashes: Vec<[u8; 16]> = (0u8..24).map(|i| [0xC0u8.wrapping_add(i); 16]).collect();
    const RECS: u64 = 200;
    const BUDGET: u64 = 512;

    let db = slatedb::Db::builder("dst-maturewave", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-maturewave".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig {
            // The per-stream cap is deliberately HUGE: only the global
            // budget may bound the wave (the wide posture runs
            // TRIM_PER_OP=65536, where per-stream capping alone still
            // permitted the 67M-delete batch).
            max_trim_per_op: 65_536,
            trim_global_budget: BUDGET,
            ..Default::default()
        },
        absorb_tx,
        None,
        __maint,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );

    // Wave 1: build mature streams (deep absorbed prefixes). A FIRST
    // absorption sets trim_safe_to to the previous boundary (0), so it
    // owes no trims — which is exactly why the earlier 100k-stream run
    // never caught this bug.
    for h in &hashes {
        append_n(&engine, *h, &key, usize::try_from(RECS).unwrap(), 512).await;
    }
    wait_all_absorbed(&engine, &hashes).await;
    let (debt0, _, max0, _) = engine.trim_stats();
    assert_eq!(debt0, 0, "first absorption must owe no trims");
    assert_eq!(max0, 0, "first absorption must delete nothing");

    // Wave 2: one new record each, then absorption advances every
    // boundary and RECS offsets per stream become trimmable at once.
    for h in &hashes {
        append_sized(&engine, *h, &key, "", 512).await;
    }
    let mut ok = false;
    // 30 s: same suite-saturation allowance as the restart-seed test.
    for _ in 0..1500 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        if all_tails(&engine, &hashes, |t| t.absorbed > RECS).await {
            ok = true;
            break;
        }
    }
    assert!(ok, "wave-2 boundaries never advanced");

    // The decoupling proof: boundaries are published but the bulk of the
    // physical trim work is DEBT, not one giant batch. (The old code
    // trimmed all 24 × 200 = 4,800 offsets inline in the advance batch.)
    let (debt, _, max_batch, _) = engine.trim_stats();
    assert!(
        debt > 0,
        "trim work must be deferred as debt, not done inline in the advance batch"
    );
    assert!(
        max_batch <= BUDGET,
        "a commit group exceeded the global trim budget: {max_batch} > {BUDGET}"
    );

    // Drain most of the debt with explicit pulses (fast), asserting the
    // bound holds throughout.
    for _ in 0..200 {
        engine.pump_trim_tick();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let (d, _, m, _) = engine.trim_stats();
        assert!(m <= BUDGET, "budget violated mid-drain: {m}");
        if d <= 2 {
            break;
        }
    }
    // Leave the tail of the debt to the PRODUCTION driver: the 5 s flush
    // ticker must finish the job with no help from the test.
    //
    // Wait on the INVARIANT (every stream trimmed to its safe target),
    // not on the debt set being momentarily empty: the debt set is a
    // work queue, and a stream whose handle is evicted and reloaded
    // re-enters it, so `trim_stats().0 == 0` is a transient the
    // maintenance pass can show while work remains — that proxy made
    // this test flake roughly 1 run in 3 under full-suite load.
    let mut drained = false;
    for _ in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let trimmed = all_tails(&engine, &hashes, |t| t.trimmed >= t.trim_safe_to).await;
        if trimmed && engine.trim_stats().0 == 0 {
            drained = true;
            break;
        }
    }
    assert!(
        drained,
        "flush-ticker trim maintenance never drained the debt"
    );

    // Convergence: every stream fully advanced AND fully trimmed, and
    // the maintenance markers are gone.
    for h in &hashes {
        let st = engine.stream_handle(*h).await.unwrap();
        let f = { st.state.lock().unwrap().durable.clone() };
        assert_eq!(f.absorbed, RECS + 1);
        assert_eq!(f.next, RECS + 1);
        assert_eq!(
            f.trimmed, f.trim_safe_to,
            "trim cursor must reach the safe target"
        );
        assert_eq!(f.trim_safe_to, RECS, "safe target is the previous boundary");
    }
    let (_, _, max_final, total) = engine.trim_stats();
    assert!(max_final <= BUDGET);
    assert_eq!(
        total,
        24 * RECS,
        "every owed offset must be trimmed exactly once"
    );
    let dirty = engine.scan_dirty_streams().await.unwrap();
    assert!(
        !dirty.iter().any(|(h, _, _)| hashes.contains(h)),
        "maintenance markers must clear once absorb and trim both catch up"
    );
    engine.begin_close();
}

/// Review round 4, P1: a stream skipped by the gather's byte budget must
/// STAY pending and absorb on a later tick — with the resident-handle
/// sweep disabled, nothing else can rediscover it. The old pump removed
/// every lane member from pending, stranding budget-deferred streams
/// for up to a sweep period and blinding the lag view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_deferred_streams_absorb_on_the_next_tick() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 97, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hashes: Vec<[u8; 16]> = (0u8..6).map(|i| [0xD0 + i; 16]).collect();

    let db = slatedb::Db::builder("dst-defer", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-defer".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    // PRODUCTION pump (not direct gather calls): tiny budget packs ~2
    // streams per gather, so full convergence REQUIRES deferred streams
    // surviving in pending across ticks. No sweep, no extra signals.
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            sweep_every: u32::MAX,
            gather_max_bytes: 40 * 1024,
            ..Default::default()
        },
        absorb_rx,
    );
    for h in &hashes {
        append_sized(&engine, *h, &key, "", 16 * 1024).await;
    }
    // All six must absorb across the NEXT FEW ticks off the ORIGINAL
    // signals alone — ~3 gathers at 2 streams each, so well under 1 s
    // at a 20 ms tick. The deadline is deliberately far below the
    // periodic durable-index rescan (tick 120 ≈ 2.4 s here), which
    // would otherwise re-find dropped streams and mask exactly the bug
    // this test exists to catch (proven by mutation: removing deferred
    // streams from pending converges at rescan time, not tick time).
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(1_200);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "budget-deferred streams did not absorb within the tick horizon"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        if all_tails(&engine, &hashes, |t| t.absorbed == t.next && t.next > 0).await {
            break;
        }
    }
    engine.begin_close();
}

/// Review round 4, P1: restart rediscovery under the TRUE default
/// policy. A single large record used to be estimated at 1 KiB
/// (records × 1 KiB), below every default threshold — never absorbed
/// again without a customer request. The tail now carries exact
/// unabsorbed bytes and the seed reads them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_record_absorbs_after_restart_under_default_policy() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 98, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xA7u8; 16];

    {
        let db = slatedb::Db::builder("dst-bigrec", store.clone() as Arc<dyn ObjectStore>)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                manifest_poll_interval: std::time::Duration::from_millis(50),
                ..Default::default()
            })
            .build()
            .await
            .expect("open db A");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        // R25-A: tests use the REAL load path — a fresh DB rebuilds to
        // zero; a reopened DB restores its durable backlog, exactly as
        // the production opener does.
        let __maint = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        let engine_a = crate::shard::ShardEngine::start(
            "dst-bigrec".to_string(),
            Arc::new(db),
            store.clone(),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
            __maint,
        );
        // One 5 MiB record: above the default 4 MiB byte threshold in
        // truth, 1 KiB in the old estimate.
        append_sized(&engine_a, hash, &key, "", 5 * 1024 * 1024).await;
        engine_a.begin_close();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    let db = slatedb::Db::builder("dst-bigrec", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db B");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine_b = crate::shard::ShardEngine::start(
        "dst-bigrec".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    // THE POINT: pure AbsorberConfig::default() — production thresholds,
    // production tick, production sweep cadence. No requests arrive.
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine_b.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig::default(),
        absorb_rx,
    );
    let mut cleared = false;
    for _ in 0..300 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let dirty = engine_b.scan_dirty_streams().await.unwrap();
        if !dirty.iter().any(|(h, _, _)| *h == hash) {
            cleared = true;
            break;
        }
    }
    assert!(
        cleared,
        "a 5 MiB pre-restart record never absorbed under the default policy"
    );
    let st = engine_b.stream_handle(hash).await.unwrap();
    let (a, n) = {
        let s = st.state.lock().unwrap();
        (s.durable.absorbed, s.durable.next)
    };
    assert_eq!((a, n), (1, 1));
    engine_b.begin_close();
}

/// Review round 4, P1: the dirty-index scan must RETRY until it
/// succeeds. A failed startup scan used to log-and-forget, permanently
/// stranding pre-restart streams (no signal, no handle, no pending
/// entry, no rediscovery path).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dirty_scan_retries_until_it_succeeds() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 99, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xA9u8; 16];

    {
        let db = slatedb::Db::builder("dst-scanretry", store.clone() as Arc<dyn ObjectStore>)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                manifest_poll_interval: std::time::Duration::from_millis(50),
                ..Default::default()
            })
            .build()
            .await
            .expect("open db A");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        // R25-A: tests use the REAL load path — a fresh DB rebuilds to
        // zero; a reopened DB restores its durable backlog, exactly as
        // the production opener does.
        let __maint = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        let engine_a = crate::shard::ShardEngine::start(
            "dst-scanretry".to_string(),
            Arc::new(db),
            store.clone(),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
            __maint,
        );
        append_sized(&engine_a, hash, &key, "", 2 * 1024).await;
        engine_a.begin_close();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // The first TWO scans on this shard fail; the third succeeds.
    crate::shard::inject_dirty_scan_faults("dst-scanretry", 2);

    let db = slatedb::Db::builder("dst-scanretry", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db B");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine_b = crate::shard::ShardEngine::start(
        "dst-scanretry".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine_b.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(50),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    // Backoff schedule: attempt at tick 1 (fail), tick 3 (fail), tick 7
    // (succeeds) — then absorption converges. The marker poll here uses
    // the same scan, so consume-faults also proves the injection is
    // per-prefix (this poll runs against engine_b's prefix only after
    // the absorber has burned the injected failures... the poll itself
    // would otherwise eat them; poll starts after a delay for that).
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let mut cleared = false;
    for _ in 0..300 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // Tolerate an injected fault if the absorber hasn't burned both
        // yet on a slow runner — the poll must not eat the absorber's
        // schedule into a panic.
        let Ok(dirty) = engine_b.scan_dirty_streams().await else {
            continue;
        };
        if !dirty.iter().any(|(h, _, _)| *h == hash) {
            cleared = true;
            break;
        }
    }
    assert!(
        cleared,
        "absorber never recovered from failed startup dirty scans"
    );
    engine_b.begin_close();
}

/// R26-1 (rewritten from the round-4 deferral companion): a tiny record
/// REDISCOVERED after restart must be reported in the shard's pending
/// summary and then AGE-ABSORB like anything else. The old assertion —
/// that it stays deferred under the sparse policy — is the exact
/// behavior R26-1 deleted, because a permanently deferred residual
/// stalls the durable no-progress clock into the instance latch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_records_rediscovered_after_restart_are_absorbed() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 100, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xABu8; 16];

    {
        let db = slatedb::Db::builder("dst-sparse", store.clone() as Arc<dyn ObjectStore>)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                manifest_poll_interval: std::time::Duration::from_millis(50),
                ..Default::default()
            })
            .build()
            .await
            .expect("open db A");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        // R25-A: tests use the REAL load path — a fresh DB rebuilds to
        // zero; a reopened DB restores its durable backlog, exactly as
        // the production opener does.
        let __maint = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        let engine_a = crate::shard::ShardEngine::start(
            "dst-sparse".to_string(),
            Arc::new(db),
            store.clone(),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
            __maint,
        );
        append_sized(&engine_a, hash, &key, "", 512).await;
        engine_a.begin_close();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    let db = slatedb::Db::builder("dst-sparse", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db B");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine_b = crate::shard::ShardEngine::start(
        "dst-sparse".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    // Byte threshold out of reach (the 512 B record must go through the
    // AGE trigger), age immediate, fast tick.
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine_b.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(50),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    // The rediscovered record must AGE-ABSORB: its durable dirty mark
    // clears instead of sitting as a permanent residual feeding the
    // no-progress latch.
    let mut absorbed = false;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let Ok(dirty) = engine_b.scan_dirty_streams().await else {
            continue;
        };
        if !dirty.iter().any(|(h, _, _)| *h == hash) {
            absorbed = true;
            break;
        }
    }
    assert!(
        absorbed,
        "a rediscovered tiny record must age-absorb, not defer forever"
    );
    // And the durable tail must agree it fully absorbed (the v2 gather
    // lane is keyless, so the empty KeyCache above is no obstacle).
    let h = engine_b.stream_handle(hash).await.expect("handle");
    {
        let st = h.state.lock().unwrap();
        assert!(st.durable.absorbed > 0 && st.durable.absorbed == st.durable.next);
    }
    engine_b.begin_close();
}

/// Review round 4, P1: an absorber's pending-summary row must clear on
/// shard departure — the frozen row otherwise double-counts against the
/// new owner's and the fleet rollup reports phantom backlog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_summary_clears_on_shard_close() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 101, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xADu8; 16];

    let db = slatedb::Db::builder("dst-sumclear", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-sumclear".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            tick: std::time::Duration::from_millis(50),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    // A record under the default age threshold (300 s — far past this
    // test's horizon) stays pending, keeping the row populated for as
    // long as we need it.
    append_sized(&engine, hash, &key, "", 512).await;
    let mut published = false;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if engine
            .usage
            .absorb_pending_summary_for("dst-sumclear")
            .is_some()
        {
            published = true;
            break;
        }
    }
    assert!(published, "summary row never published");

    engine.begin_close();
    let mut cleared = false;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if engine
            .usage
            .absorb_pending_summary_for("dst-sumclear")
            .is_none()
        {
            cleared = true;
            break;
        }
    }
    assert!(
        cleared,
        "pending-summary row survived shard close (phantom fleet backlog)"
    );
}

/// Review round 4 (memory): time-based handle eviction alone lets a
/// cardinality burst hold rate × idle-window handles. Past
/// handle_max_resident the ticker must evict oldest-touched
/// unreferenced handles immediately — referenced ones never.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handle_capacity_cap_evicts_oldest_first() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 102, FaultPlan::new(0, 0, 0));
    let key = skey();

    let db = slatedb::Db::builder("dst-handlecap", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-handlecap".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let hashes: Vec<[u8; 16]> = (0u8..12).map(|i| [0xE0 + i; 16]).collect();
    for (i, h) in hashes.iter().enumerate() {
        append_sized(&engine, *h, &key, "", 256).await;
        // Distinct last_touch ordering (ms granularity).
        if i % 3 == 2 {
            tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Hold the OLDEST handle: the cap must skip it (referenced) and
    // evict the oldest UNREFERENCED instead.
    let held = engine.stream_handle(hashes[0]).await.unwrap();
    // A fresh burst is NOT idle — the idle pass alone (10 min default)
    // would evict nothing; only the capacity cap can.
    let evicted = engine.evict_idle_handles(std::time::Duration::from_secs(600), 4);
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
