//! Runtime open gate.

use super::fixture_storage::{mem, open_engine, skey};
use crate::dst::{FaultPlan, FaultStore, ObjClass, OpLog, StoreOp, Workload, mech};
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

// ---- the eu-central-1 reopen storm ----------------------------------

/// Seed a shard prefix with many WAL SSTs and no L0 flush, so every open
/// must replay all of them from the store. This is the state eu-central-1
/// was in when its engine first died: a WAL the boundary had not caught up
/// with, behind a slow, partially cross-routed store.
async fn seed_untrimmed_wal(store: Arc<dyn ObjectStore>, prefix: &str, records: u64) {
    let db = slatedb::Db::builder(prefix, store)
        .with_settings(slatedb::config::Settings {
            // Mint a WAL SST per write...
            flush_interval: Some(std::time::Duration::from_millis(1)),
            // ...and never flush the memtable to L0, so replay_after_wal_id
            // stays at zero and every subsequent open replays everything.
            // (0.15 validates max_unflushed > l0_sst_size, so raise both.)
            l0_sst_size_bytes: 1 << 30,
            max_unflushed_bytes: 2 << 30,
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("seed db");
    for i in 0..records {
        db.put_with_options(
            format!("k{i:06}").as_bytes(),
            vec![7u8; 256].as_slice(),
            &slatedb::config::PutOptions::default(),
            &slatedb::config::WriteOptions::default(),
        )
        .await
        .expect("seed put")
        .await_durable()
        .await
        .expect("seed durable");
    }
    // Drop WITHOUT close: close() would flush the memtable to L0 and
    // advance the replay boundary, which is exactly what must not happen.
    drop(db);
}

/// The OLD `engine_for` semantics, verbatim in miniature: hold a lock,
/// await the open inline in the caller's task, insert into the map from
/// the caller's task. The inner Db open is spawned (as `on_slatedb_rt`
/// does in production), so abandoning the await detaches it.
#[expect(
    clippy::disallowed_methods,
    reason = "reopen storm reproduction; the detached open is the defect being reproduced and its abandonment is counted through the fenced opens and the empty serving map; owning the open would remove the storm the scenario exists to reproduce"
)]
async fn naive_get_or_open(
    lock: &tokio::sync::Mutex<()>,
    shards: &std::sync::RwLock<HashMap<String, Arc<crate::shard::ShardEngine>>>,
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    fenced_opens: &Arc<std::sync::atomic::AtomicU64>,
) -> Option<Arc<crate::shard::ShardEngine>> {
    if let Some(e) = shards.read().unwrap().get(prefix) {
        return Some(e.clone());
    }
    let _g = lock.lock().await;
    if let Some(e) = shards.read().unwrap().get(prefix) {
        return Some(e.clone());
    }
    // Mimic on_slatedb_rt: the REAL open runs in a spawned task; the
    // caller awaits a oneshot. Dropping this future abandons the rx but
    // not the open.
    let (tx, rx) = tokio::sync::oneshot::channel();
    let st = store.clone();
    let p = prefix.to_string();
    let fenced = fenced_opens.clone();
    tokio::spawn(async move {
        // Non-panicking open: a detached replay that loses the epoch war
        // gets `Fenced` from the winner — count those, they are the
        // zombies of the real incident.
        let st2 = st.clone();
        let db = slatedb::Db::builder(p.as_str(), st)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                manifest_poll_interval: std::time::Duration::from_millis(50),
                ..Default::default()
            })
            .build()
            .await;
        match db {
            Ok(db) => {
                let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
                // R25-A: tests use the REAL load path — a fresh DB rebuilds to
                // zero; a reopened DB restores its durable backlog, exactly as
                // the production opener does.
                let __maint = crate::shard::load_or_rebuild_maintenance(&db)
                    .await
                    .expect("load maintenance");
                let eng = crate::shard::ShardEngine::start(
                    p,
                    Arc::new(db),
                    st2,
                    crate::shard::ShardConfig::default(),
                    absorb_tx,
                    None,
                    __maint,
                );
                let _ = tx.send(eng);
            }
            Err(e) => {
                if format!("{e}").contains("newer DB client") {
                    fenced.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    });
    let engine = rx.await.ok()?;
    shards
        .write()
        .unwrap()
        .insert(prefix.to_string(), engine.clone());
    Some(engine)
}

/// **The eu-central-1 wedge, reproduced.**
///
/// WAL replay on open is slower than the callers' patience (slow store,
/// paused time), callers time out and disconnect exactly as the soak
/// clients did at 30 s, and the old open path turns each disconnection
/// into a fresh, detached, full-WAL replay. The assertions are the
/// storm's signature from docs/SOAK-REGIONS.md, scaled down: WAL read
/// amplification ≥ 3× the WAL itself, multiple writers opened and fenced,
/// and — the wedge — the serving map STILL empty when the dust settles.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reopen_storm_reproduces_the_eu_central_wedge() {
    let inner = mem();
    seed_untrimmed_wal(inner.clone(), "dst-storm", 120).await;

    // Every store op costs 40–80 ms simulated — the fra profile with a
    // quarter of requests cross-routed. 120 WAL SSTs × ~50 ms ≫ the 1 s
    // caller patience below, which is the 30 s client timeout scaled to
    // the test's magnitudes.
    let plan = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 100,
        latency_ms: (40, 80),
    };
    let store = FaultStore::uniform(inner.clone(), 61, plan);

    let lock = tokio::sync::Mutex::new(());
    let shards: std::sync::RwLock<HashMap<String, Arc<crate::shard::ShardEngine>>> =
        Default::default();
    let fenced_opens = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Twelve successive clients, each timing out and disconnecting —
    // dropping the future, exactly what axum does — then the next arrives.
    for _ in 0..12 {
        let fut = naive_get_or_open(&lock, &shards, store.clone(), "dst-storm", &fenced_opens);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), fut).await;
    }
    // Let the detached replays grind to completion so the storm's full
    // cost is on the ledger.
    tokio::time::sleep(std::time::Duration::from_secs(300)).await;

    // Measured on this exact setup: 7,503 GETs for a 120-SST WAL — a 62×
    // amplification, 11 of 12 opens fenced. The floor leaves a wide margin
    // while staying an order of magnitude above any legitimate cost.
    let wal_gets = store.count(StoreOp::Get, ObjClass::Wal);
    assert!(
        wal_gets >= 2_000,
        "expected a WAL read storm (measured 7,503 on this setup; floor 2,000), \
         got {wal_gets} — the reproduction has gone vacuous"
    );
    assert!(
        shards.read().unwrap().is_empty(),
        "the naive path actually populated the map — the wedge did not reproduce"
    );
    assert!(
        fenced_opens.load(Ordering::SeqCst) >= 1,
        "no detached open was fenced by a later one — the writer-epoch war \
         did not reproduce"
    );
}

/// OpenGate counters are process-global too; its three counter-asserting
/// tests serialize here for the same reason as the reader-cache tests.
fn gate_lock() -> &'static tokio::sync::Mutex<()> {
    static L: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// **The fix.** Same sick store, same impatient clients, through
/// `OpenGate`: one open, started once, owning its own completion. Clients
/// get retryable 503s while it runs; the engine lands in the serving map
/// even though every client that asked for it had already given up; WAL
/// read amplification is ~1×.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn open_gate_survives_impatient_clients_without_a_storm() {
    use crate::sharddir::{OpenGate, OpenOutcome};
    let _serial = gate_lock().lock().await;
    let inner = mem();
    seed_untrimmed_wal(inner.clone(), "dst-gate", 120).await;

    let plan = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 100,
        latency_ms: (40, 80),
    };
    let store = FaultStore::uniform(inner.clone(), 61, plan);

    OpenGate::reset_counters_for_tests();
    let shards = Arc::new(std::sync::RwLock::new(HashMap::new()));
    let st = store.clone();
    let gate = OpenGate::new(
        shards.clone(),
        Box::new(
            move |prefix: String, _inc: crate::sharddir::EngineIncarnation| {
                let st = st.clone();
                Box::pin(async move {
                    let s: Arc<dyn ObjectStore> = st;
                    Ok(open_engine(s, &prefix).await)
                })
            },
        ),
        std::time::Duration::from_secs(180),
    );

    // The same twelve impatient clients. Each gets a Wait (503) — and
    // their timeouts must NOT abandon or restart the open.
    let mut waits = 0;
    for _ in 0..12 {
        match gate
            .get_or_open("dst-gate", std::time::Duration::from_secs(1))
            .await
        {
            OpenOutcome::Wait { .. } => waits += 1,
            OpenOutcome::Ready(_) => {}
            OpenOutcome::Failed(e) => panic!("open failed: {e}"),
        }
    }
    assert!(waits > 0, "callers were never made to wait — vacuous");

    // The single open finishes on its own and inserts itself.
    for _ in 0..600 {
        if !shards.read().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    assert!(
        !shards.read().unwrap().is_empty(),
        "the open never completed into the serving map"
    );

    let (started, completed, failed, coalesced) = gate.instance_counters();
    assert_eq!(started, 1, "exactly one open may start (got {started})");
    assert_eq!(completed, 1);
    assert_eq!(failed, 0);
    assert!(coalesced >= 10, "later callers must join the first open");

    // One replay costs ~5 store ops per WAL SST (existence probes arrive
    // as HEAD-flavoured GETs, plus content reads, plus noise from the
    // fenced seeding db's background tasks) — measured 616 here against
    // the naive path's 7,503. The ceiling is 8/SST: an order of magnitude
    // under the storm, comfortably above one honest replay.
    let wal_gets = store.count(StoreOp::Get, ObjClass::Wal);
    assert!(
        wal_gets <= 8 * 120,
        "reopen budget violated: {wal_gets} WAL GETs for a 120-SST WAL (≤{} allowed; \
         one replay measures ~616, the storm measures ~7,503)",
        8 * 120
    );

    // And the engine works: appends through it are acknowledged.
    let engine = match gate
        .get_or_open("dst-gate", std::time::Duration::from_secs(5))
        .await
    {
        OpenOutcome::Ready(e) => e,
        other => panic!(
            "expected Ready after completion, got {}",
            match other {
                OpenOutcome::Wait { code, .. } => code,
                OpenOutcome::Failed(_) => "failed",
                OpenOutcome::Ready(_) => unreachable!(),
            }
        ),
    };
    let cov = store.coverage();
    let mut log = OpLog::default();
    let mut w = Workload::new(cov);
    w.append(&engine, [9u8; 16], &skey(), "k", false, &mut log)
        .await;
    assert_eq!(log.total_acked(), 1, "append through the opened engine");
}

/// The idle-cost pin behind docs/TIGRIS-404-COST.md: an open-but-idle
/// engine's only store traffic is the manifest/compactions poll cadence.
/// Every such poll is a live probe-GET that MISSES — the 404 Tigris
/// measures at ~200-240 ms of internal work and bills as Class B — so
/// the production default cadences, asserted here against the very
/// constants the binary ships (`crate::DEFAULT_*_POLL_MS`), are a cost
/// posture. This fails if the cadence tightens back toward the old
/// 1000/500 deploy pins (~3× the ceiling), if a background task starts
/// chattering at idle, if idle flushes begin minting WAL objects, or if
/// per-poll directory LISTs return (the Class-A regression cost
/// campaign 2 removed). Faults off, realistic store latency on: latency
/// shapes timer interleaving but cannot suppress or add a poll.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn idle_engine_store_traffic_is_bounded_by_the_poll_cadence() {
    // The posture VALUES are part of the pin — the budget below scales
    // with the constants, so without this a reverted default would
    // silently re-price every idle instance and still pass. The dollar
    // math in docs/TIGRIS-404-COST.md was accepted against exactly
    // these; redo it there before changing either number.
    assert_eq!(crate::DEFAULT_MANIFEST_POLL_MS, 2000);
    assert_eq!(crate::DEFAULT_COMPACTOR_POLL_MS, 2500);

    let inner = mem();
    let store = FaultStore::uniform(inner, 11, FaultPlan::new(0, 0, 100));
    let db = slatedb::Db::builder("dst-idlepoll", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            // Production shape: pump on ⇒ SlateDB's own flush timer is a
            // 1 s failsafe (same stretch open_engine_cfg mirrors).
            flush_interval: Some(std::time::Duration::from_secs(1)),
            manifest_poll_interval: std::time::Duration::from_millis(
                crate::DEFAULT_MANIFEST_POLL_MS,
            ),
            compactor_options: {
                let mut co = slatedb::config::CompactorOptions::default();
                co.poll_interval =
                    std::time::Duration::from_millis(crate::DEFAULT_COMPACTOR_POLL_MS);
                Some(co)
            },
            ..Default::default()
        })
        .build()
        .await
        .expect("open idle db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-idlepoll".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig {
            wal_group_commit: true,
            ..Default::default()
        },
        absorb_tx,
        None,
        __maint,
    );

    // One acked append makes this a real, used instance (not a fresh-open
    // special case), then a long settle clears open probes, the first
    // flush, and every first-fire timer before the measured window.
    let cov = store.coverage();
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.append(&engine, [4u8; 16], &skey(), "warm", false, &mut log)
        .await;
    assert_eq!(log.total_acked(), 1, "warm-up append must ack");
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;

    let gets = |st: &Arc<FaultStore>| -> (u64, u64, u64) {
        let total: u64 = [
            ObjClass::Wal,
            ObjClass::Manifest,
            ObjClass::Sst,
            ObjClass::Fleet,
            ObjClass::Other,
        ]
        .into_iter()
        .map(|c| st.count(StoreOp::Get, c))
        .sum();
        (
            total,
            st.count(StoreOp::Get, ObjClass::Manifest),
            st.count(StoreOp::Get, ObjClass::Wal),
        )
    };
    let lists = |st: &Arc<FaultStore>| -> u64 {
        [
            ObjClass::Wal,
            ObjClass::Manifest,
            ObjClass::Sst,
            ObjClass::Fleet,
            ObjClass::Other,
        ]
        .into_iter()
        .map(|c| st.count(StoreOp::List, c))
        .sum()
    };
    let puts_wal = |st: &Arc<FaultStore>| st.count(StoreOp::Put, ObjClass::Wal);

    let (g0, gm0, gw0) = gets(&store);
    let l0 = lists(&store);
    let pw0 = puts_wal(&store);

    const WINDOW_SECS: u64 = 120;
    tokio::time::sleep(std::time::Duration::from_secs(WINDOW_SECS)).await;

    let (g1, gm1, gw1) = gets(&store);
    let l1 = lists(&store);
    let pw1 = puts_wal(&store);
    let (dg, dgm, dgw, dl, dpw) = (g1 - g0, gm1 - gm0, gw1 - gw0, l1 - l0, pw1 - pw0);

    let manifest_polls = WINDOW_SECS * 1000 / crate::DEFAULT_MANIFEST_POLL_MS;
    let compactor_polls = WINDOW_SECS * 1000 / crate::DEFAULT_COMPACTOR_POLL_MS;
    // Per tick the writer's manifest poll costs 2 GETs (miss-probe +
    // anchor revalidation) and the compactor's costs ~5 (compactions
    // probe + revalidate, plus its own manifest reads) — measured 363
    // total here at the shipped defaults. Budget 3/manifest + 6/compactor
    // tick; the old 1000/500 deploy posture measures ~3× this ceiling.
    let ceiling = manifest_polls * 3 + compactor_polls * 6;
    assert!(
        dg <= ceiling,
        "idle store GETs {dg} exceed the poll-cadence budget {ceiling} \
         ({manifest_polls} manifest + {compactor_polls} compactions polls / {WINDOW_SECS}s)"
    );
    assert!(
        dgm >= manifest_polls / 2,
        "manifest polling looks stopped ({dgm} probe-GETs in {WINDOW_SECS}s; \
         expected ≈{manifest_polls}) — the budget assertion above is vacuous"
    );
    assert_eq!(dgw, 0, "an idle engine has no business reading the WAL");
    assert_eq!(dpw, 0, "an idle pump must not mint WAL objects");
    assert!(
        dl <= 2,
        "idle LISTs returned ({dl} in {WINDOW_SECS}s) — the per-poll \
         directory-LIST regression cost campaign 2 removed"
    );
    if let Err(e) = cov.require(&[mech::STORE_LATENCY]) {
        panic!("{e}");
    }
}

/// CHAOS-2: an instance whose shards can NEVER open must stop calling
/// itself healthy. Invalid engine config, a wrong bucket, bad
/// credentials and an unreachable endpoint all land here — the process
/// binds, `/health` says `ok`, and every append 500s forever while the
/// load balancer keeps sending traffic.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn health_reports_unready_when_no_shard_has_ever_opened() {
    use crate::sharddir::OpenGate;
    let _serial = gate_lock().lock().await;
    OpenGate::reset_counters_for_tests();

    let shards = Arc::new(std::sync::RwLock::new(HashMap::new()));
    let gate = OpenGate::new(
        shards.clone(),
        Box::new(
            move |_prefix: String, _inc: crate::sharddir::EngineIncarnation| {
                Box::pin(async move {
                    anyhow::bail!(
                        "invalid configuration: max_unflushed_bytes (16777216) must be \
                     greater than l0_sst_size_bytes (33554432)"
                    )
                })
            },
        ),
        std::time::Duration::from_secs(180),
    );

    // Distinct prefixes: this is the whole data plane failing, not one
    // poison stream.
    for (i, prefix) in ["shards/root", "shards/1", "shards/2"].iter().enumerate() {
        loop {
            match gate
                .get_or_open(prefix, std::time::Duration::from_secs(30))
                .await
            {
                crate::sharddir::OpenOutcome::Failed(_) => break,
                crate::sharddir::OpenOutcome::Wait {
                    retry_after_secs, ..
                } => tokio::time::sleep(std::time::Duration::from_secs(retry_after_secs + 1)).await,
                crate::sharddir::OpenOutcome::Ready(_) => panic!("open must not succeed"),
            }
        }
        // Below the strike line the instance stays in rotation: a
        // single failure is a store blip, not a broken deploy.
        if i < 2 {
            assert!(
                gate.unready_reason().is_none(),
                "evicted from rotation after only {} failure(s)",
                i + 1
            );
        }
    }

    let reason = gate
        .unready_reason()
        .expect("three failures, zero successes => unready");
    assert!(
        reason.contains("max_unflushed_bytes"),
        "readiness must carry the diagnosis, got: {reason}"
    );
}

/// An engine that keeps dying young must meet an escalating holdoff, not
/// an eager reopen: rapid open→die cycles against a sick store ARE the
/// storm, whatever kills the engine.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn open_gate_escalates_holdoff_for_engines_that_die_young() {
    use crate::sharddir::{OpenGate, OpenOutcome};
    let _serial = gate_lock().lock().await;
    let inner = mem();
    OpenGate::reset_counters_for_tests();
    let shards = Arc::new(std::sync::RwLock::new(HashMap::new()));
    let st = inner.clone();
    let gate = OpenGate::new(
        shards.clone(),
        Box::new(
            move |prefix: String, _inc: crate::sharddir::EngineIncarnation| {
                let st = st.clone();
                Box::pin(async move {
                    let s: Arc<dyn ObjectStore> = st.clone();
                    Ok(open_engine(s, &prefix).await)
                })
            },
        ),
        std::time::Duration::from_secs(180),
    );

    // Open, die young, repeat. Holdoffs must grow: 3s, 6s, 12s.
    let mut observed = Vec::new();
    for _ in 0..3 {
        let eng = loop {
            match gate
                .get_or_open("dst-flap", std::time::Duration::from_secs(30))
                .await
            {
                OpenOutcome::Ready(e) => break e,
                OpenOutcome::Wait {
                    retry_after_secs, ..
                } => {
                    tokio::time::sleep(std::time::Duration::from_secs(retry_after_secs)).await;
                }
                OpenOutcome::Failed(e) => panic!("open failed: {e}"),
            }
        };
        let inc = gate.resident_incarnation("dst-flap").expect("resident");
        drop(eng);
        assert!(gate.notify_closed("dst-flap", inc)); // died young (lifetime ≈ 0)
        match gate
            .get_or_open("dst-flap", std::time::Duration::from_secs(1))
            .await
        {
            OpenOutcome::Wait {
                retry_after_secs, ..
            } => observed.push(retry_after_secs),
            OpenOutcome::Ready(_) => panic!("reopened with no holdoff after dying young"),
            OpenOutcome::Failed(e) => panic!("open failed: {e}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(70)).await; // clear holdoff
    }
    assert!(
        observed.windows(2).all(|w| w[1] > w[0]),
        "holdoff must escalate for engines that die young, got {observed:?}"
    );
}

/// A hung open must not hold the shard hostage: the deadline fails it,
/// the holdoff arms, and — critically — the abandoned open is *reaped*,
/// not detached. Its late engine gets closed, never installed. Detached
/// late completions were the zombie writers of the original storm; this
/// is the guard that keeps the deadline from reintroducing them.
///
/// Observed live before this existed: the soak2 campaign's final run left
/// eu-central-1 with an open looping in slatedb compactions recovery for
/// 20+ minutes. One open, 648 coalesced waiters, zero storm — and an
/// unavailable shard with no path back.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_hung_open_is_deadlined_and_its_late_engine_reaped() {
    use crate::sharddir::{OpenGate, OpenOutcome};
    let _serial = gate_lock().lock().await;
    let inner = mem();
    OpenGate::reset_counters_for_tests();

    // The opener parks on a test-controlled gate until released — a stand-in
    // for "slatedb open looping in recovery".
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let opened: Arc<Mutex<Vec<Arc<crate::shard::ShardEngine>>>> = Arc::new(Mutex::new(Vec::new()));
    let shards = Arc::new(std::sync::RwLock::new(HashMap::new()));
    let st = inner.clone();
    let rel = release.clone();
    let op = opened.clone();
    let gate = OpenGate::new(
        shards.clone(),
        Box::new(
            move |prefix: String, _inc: crate::sharddir::EngineIncarnation| {
                let st = st.clone();
                let rel = rel.clone();
                let op = op.clone();
                Box::pin(async move {
                    let _ = rel.acquire().await; // park here until the test releases
                    let s: Arc<dyn ObjectStore> = st.clone();
                    let e = open_engine(s, &prefix).await;
                    op.lock().unwrap().push(e.clone());
                    Ok(e)
                })
            },
        ),
        std::time::Duration::from_secs(30),
    );

    // First caller starts the open and times out waiting.
    match gate
        .get_or_open("dst-hang", std::time::Duration::from_secs(1))
        .await
    {
        OpenOutcome::Wait { code, .. } => assert_eq!(code, "shard_opening"),
        other => panic!(
            "expected Wait, got {}",
            match other {
                OpenOutcome::Ready(_) => "Ready",
                OpenOutcome::Failed(_) => "Failed",
                OpenOutcome::Wait { .. } => unreachable!(),
            }
        ),
    }

    // Let the 30 s deadline pass. The open task must fail the attempt and
    // arm the holdoff without any help from callers.
    tokio::time::sleep(std::time::Duration::from_secs(35)).await;
    let (_started, completed, failed, _coalesced) = gate.instance_counters();
    assert_eq!(failed, 1, "the hung open must be failed by its deadline");
    assert_eq!(completed, 0);
    assert!(
        shards.read().unwrap().is_empty(),
        "nothing may be installed by a deadlined open"
    );

    // The abandoned open now completes late. The reaper must close its
    // engine, not install it.
    release.add_permits(1);
    let mut reaped = false;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let engines = opened.lock().unwrap().clone();
        if let Some(e) = engines.first()
            && e.is_closed()
        {
            reaped = true;
            break;
        }
    }
    assert!(reaped, "the late engine was never closed by the reaper");
    assert!(
        shards.read().unwrap().is_empty(),
        "a reaped engine must never appear in the serving map"
    );

    // After the holdoff, a fresh open (opener no longer parks: permits
    // remain) must succeed and install.
    release.add_permits(10);
    tokio::time::sleep(std::time::Duration::from_secs(10)).await; // clear holdoff
    let eng = loop {
        match gate
            .get_or_open("dst-hang", std::time::Duration::from_secs(30))
            .await
        {
            OpenOutcome::Ready(e) => break e,
            OpenOutcome::Wait {
                retry_after_secs, ..
            } => tokio::time::sleep(std::time::Duration::from_secs(retry_after_secs)).await,
            OpenOutcome::Failed(e) => panic!("recovery open failed: {e}"),
        }
    };
    assert!(!eng.is_closed(), "the recovery engine must be live");
    assert!(!shards.read().unwrap().is_empty());
}
