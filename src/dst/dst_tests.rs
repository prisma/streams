//! Scenarios. See `src/dst.rs` for the model and docs/DST.md for scope.
//!
//! Every scenario that claims to exercise a mechanism `require`s the
//! corresponding coverage counter. A scenario that silently stops
//! triggering its mechanism is a scenario that has stopped testing
//! anything, and this project has paid for that lesson twice.

use super::*;
use std::sync::atomic::Ordering;

fn mem() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

fn skey() -> crate::crypto::StreamKey {
    crate::crypto::StreamKey([7u8; 32])
}

// ---- the fault substrate --------------------------------------------

/// The claim that makes replay possible: an operation's fate depends on
/// `(seed, path, op, occurrence)` and NOT on when it ran relative to other
/// operations.
///
/// The test issues the same set of paths in two different orders. A shared
/// sequential RNG — the obvious implementation, and the one this harness
/// used before — fails this outright, because the Nth random number goes
/// to whichever operation arrived Nth.
#[tokio::test]
async fn fault_placement_is_a_pure_function_of_the_seed() {
    async fn decisions(seed: u64, order: &[u64]) -> Vec<(u64, bool)> {
        let s = FaultStore::uniform(mem(), seed, FaultPlan::new(25, 15, 30));
        let mut out = Vec::new();
        for i in order {
            let p = ObjPath::from(format!("shards/x/wal/{i}.sst"));
            let ok = s
                .put_opts(&p, PutPayload::from(vec![1u8; 8]), PutOptions::default())
                .await
                .is_ok();
            out.push((*i, ok));
        }
        out.sort();
        out
    }
    let forward: Vec<u64> = (0..64).collect();
    let reverse: Vec<u64> = (0..64).rev().collect();

    assert_eq!(
        decisions(42, &forward).await,
        decisions(42, &reverse).await,
        "fault placement must not depend on the order operations arrive in"
    );
    assert_ne!(
        decisions(42, &forward).await,
        decisions(43, &forward).await,
        "different seeds must explore different schedules"
    );
}

/// A fault store that never injects proves nothing.
#[tokio::test]
async fn faults_actually_fire() {
    let s = FaultStore::uniform(mem(), 7, FaultPlan::new(20, 20, 30));
    for i in 0..300u64 {
        let _ = s
            .put_opts(
                &ObjPath::from(format!("shards/x/wal/{i}.sst")),
                PutPayload::from(vec![0u8; 4]),
                PutOptions::default(),
            )
            .await;
    }
    assert!(s.injected_errors() > 0, "no errors injected");
    assert!(s.injected_lost() > 0, "no lost responses injected");
    assert!(s.injected_latency() > 0, "no latency injected");
}

/// The ambiguity fault must be genuinely ambiguous: the caller sees an
/// error, and the object is nonetheless there. If this ever regressed to
/// "error and no write", every ambiguity scenario below would quietly
/// become a plain-error scenario.
#[tokio::test]
async fn a_lost_response_still_wrote_the_object() {
    let inner = mem();
    // 100% lost-response: every put applies and every caller sees an error.
    let s = FaultStore::uniform(inner.clone(), 1, FaultPlan::new(0, 100, 0));
    let p = ObjPath::from("shards/x/wal/1.sst");
    let err = s
        .put_opts(&p, PutPayload::from(vec![9u8; 4]), PutOptions::default())
        .await;
    assert!(err.is_err(), "caller must see a failure");
    let got = inner.get_opts(&p, GetOptions::default()).await;
    assert!(got.is_ok(), "the write must nonetheless have landed");
}

/// Faults must reach every verb we can fault. Deletes are the GC path —
/// the one that removed live SSTs under a zombie DB in ladder pass 3 — and
/// they went unfaulted in the first version of this harness.
#[tokio::test]
async fn deletes_and_reads_are_faulted_too() {
    use futures_util::StreamExt;
    let inner = mem();
    let s = FaultStore::uniform(inner.clone(), 3, FaultPlan::new(100, 0, 0));

    let p = ObjPath::from("shards/x/wal/1.sst");
    inner
        .put_opts(&p, PutPayload::from(vec![1u8; 4]), PutOptions::default())
        .await
        .unwrap();

    assert!(
        s.get_opts(&p, GetOptions::default()).await.is_err(),
        "reads must be faultable for availability"
    );

    let del = s
        .delete_stream(futures_util::stream::once(async move { Ok(p.clone()) }).boxed())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(del.len(), 1);
    assert!(del[0].is_err(), "deletes must be faultable");
    assert!(
        inner
            .get_opts(&ObjPath::from("shards/x/wal/1.sst"), GetOptions::default())
            .await
            .is_ok(),
        "a delete that failed before dispatch must leave the object in place"
    );
}

/// The classifier is shared with production telemetry; if it stops
/// agreeing, a scenario that targets "the WAL" stops targeting what
/// `/v1/debug/store` reports as the WAL.
#[test]
fn object_classes_match_production_telemetry() {
    assert_eq!(ObjClass::of("soak/shards/10/wal/0001.sst"), ObjClass::Wal);
    assert_eq!(
        ObjClass::of("shards/10/manifest/0001.manifest"),
        ObjClass::Manifest
    );
    assert_eq!(ObjClass::of("streams/ab/compacted/0001.sst"), ObjClass::Sst);
    assert_eq!(ObjClass::of("stg1/fleet/streams-1.json"), ObjClass::Fleet);
    assert_eq!(ObjClass::of("stg1/topology.json"), ObjClass::Other);
}

// ---- scenarios over the real engine ---------------------------------

/// Open the engine WITHOUT an absorber: reads come from the shard log
/// only. Used by scenarios that are about the commit path.
async fn open_engine(store: Arc<dyn ObjectStore>, prefix: &str) -> Arc<crate::shard::ShardEngine> {
    let db = slatedb::Db::builder(prefix, store)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    crate::shard::ShardEngine::start(
        prefix.to_string(),
        Arc::new(db),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    )
}

/// I1+I2+I3 for a single writer under the full fault set — errors, lost
/// responses and realistic latency, not latency alone.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn acked_records_survive_store_faults() {
    for seed in [1u64, 7, 99] {
        let inner = mem();
        // Errors and ambiguity on the WAL (the ack path); manifest and SST
        // get latency only, so compaction still makes progress.
        let profile = FaultProfile::uniform(FaultPlan::new(0, 0, 40))
            .with_class(ObjClass::Wal, FaultPlan::new(12, 8, 40));
        let store = FaultStore::new(inner.clone(), seed, profile);
        let cov = store.coverage();
        let engine = open_engine(store.clone(), &format!("dst-faults-{seed}")).await;
        let key = skey();
        let hash = [3u8; 16];

        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        w.run(&engine, hash, &key, &["a", "b", "c"], 12, false, &mut log)
            .await;
        assert!(log.total_acked() > 0, "seed {seed}: nothing acked");

        let ds: Arc<dyn ObjectStore> = store.clone();
        let observed = drain_observed(&ds, &engine, hash, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("seed {seed}: {e}\ncoverage={:?}", cov.snapshot());
        }
        if let Err(e) = cov.require(&[
            mech::STORE_LATENCY,
            mech::STORE_ERROR,
            mech::STORE_LOST_RESPONSE,
            mech::APPEND_ACKED,
        ]) {
            panic!("seed {seed}: {e}");
        }
    }
}

/// Characterisation, not aspiration: **object-store faults do not reach the
/// client as failures.** SlateDB retries them, so a store that is flaky but
/// eventually available makes appends *slow*, never failed or ambiguous.
///
/// This is worth pinning because it defines the ambiguity surface. If it
/// ever fails, retries have stopped somewhere and clients can now see
/// unknown outcomes from plain store flakiness — which changes what the
/// producer-idempotence contract has to cover. It is also the mechanism
/// that made the eu-central-1 wedge invisible until client timeouts fired:
/// nothing failed, everything just took longer than anyone would wait
/// (docs/SOAK-REGIONS.md).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn store_errors_surface_as_latency_not_as_failed_appends() {
    let inner = mem();
    // 95 % of WAL writes fail before dispatch.
    let profile = FaultProfile::uniform(FaultPlan::new(0, 0, 20))
        .with_class(ObjClass::Wal, FaultPlan::new(95, 0, 5));
    let store = FaultStore::new(inner.clone(), 5, profile);
    let cov = store.coverage();
    let engine = open_engine(store.clone(), "dst-flaky").await;
    let key = skey();
    let hash = [12u8; 16];

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.max_attempts = 1; // no client-side retry: this is about the server
    for _ in 0..20 {
        w.append(&engine, hash, &key, "p", false, &mut log).await;
    }

    assert!(
        store.injected_errors() > 100,
        "expected a storm of injected errors, got {}",
        store.injected_errors()
    );
    assert_eq!(
        log.total_acked(),
        20,
        "every append should still have been acknowledged; unknown={} rejected={}",
        log.unknown.len(),
        log.rejected.len()
    );
    let ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&ds, &engine, hash, &key, &cov).await;
    log.audit(&observed).expect("audit");
}

/// **I6 across a shard handoff.**
///
/// Store faults alone cannot produce client-visible ambiguity here: SlateDB
/// retries object-store errors until they succeed, so a flaky store yields
/// *slow* appends, never failed or ambiguous ones. We measured that
/// directly — 20/20 appends acknowledged with a 95 % injected error rate
/// and 2,329 injected errors — and it is the same mechanism that made the
/// eu-central-1 wedge invisible to clients until their own timeouts fired
/// (docs/SOAK-REGIONS.md).
///
/// So the ambiguity a real client actually meets comes from **fencing**: a
/// shard moves, the in-flight append returns `Moved`, and the client cannot
/// know whether it committed. It retries the same logical operation — same
/// producer sequence — against the new owner, exactly as a client following
/// `Streams-Replay-To` does. If producer state did not survive the handoff,
/// that retry double-writes.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn idempotent_retries_commit_at_most_once_across_a_handoff() {
    for seed in [5u64, 21] {
        let inner = mem();
        let store = FaultStore::uniform(inner.clone(), seed, FaultPlan::new(0, 10, 30));
        let cov = store.coverage();
        let key = skey();
        let hash = [4u8; 16];
        let prefix = format!("dst-idem-{seed}");

        let a = open_engine(store.clone(), &prefix).await;
        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        w.max_attempts = 2;

        // A settled producer sequence through the original owner.
        for _ in 0..10 {
            w.append_to(&[&a], hash, &key, "p", true, &mut log).await;
        }
        assert!(log.total_acked() > 0, "seed {seed}: nothing acked");

        // The move. From here the client's first attempt goes to the fenced
        // owner and its retry to the new one, carrying the same sequence.
        let b = open_engine(store.clone(), &prefix).await;
        for _ in 0..8 {
            w.append_to(&[&a, &b], hash, &key, "p", true, &mut log).await;
        }

        let ds: Arc<dyn ObjectStore> = store.clone();
        let observed = drain_observed(&ds, &b, hash, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("seed {seed}: {e}\ncoverage={:?}", cov.snapshot());
        }
        // Non-vacuity: the handoff must actually have produced ambiguous
        // outcomes and failover retries, or this tested nothing.
        if let Err(e) = cov.require(&[
            mech::APPEND_ACKED,
            mech::APPEND_UNKNOWN,
            mech::APPEND_RETRIED,
        ]) {
            panic!("seed {seed}: {e}");
        }
    }
}

/// **I4, asserted.** The previous version of this test built a "ghost"
/// ledger of writes attempted through the fenced owner and then never
/// looked at it, so an old owner that acknowledged every one of them would
/// still have passed. The assertion is the test.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_fenced_owner_acknowledges_nothing() {
    for seed in [2u64, 13] {
        let inner = mem();
        let store = FaultStore::uniform(inner.clone(), seed, FaultPlan::new(0, 0, 25));
        let cov = store.coverage();
        let key = skey();
        let hash = [5u8; 16];
        let prefix = format!("dst-fence-{seed}");

        let a = open_engine(store.clone(), &prefix).await;
        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        w.run(&a, hash, &key, &["x", "y"], 8, false, &mut log).await;
        let before = log.total_acked();
        assert!(before > 0, "seed {seed}: nothing acked pre-handoff");

        // The move: a new owner opens the same shard log, fencing A.
        let b = open_engine(store.clone(), &prefix).await;

        // Everything A acknowledges from here is an I4 violation.
        let mut ghost = OpLog::default();
        let mut gw = Workload::new(cov.clone());
        gw.max_attempts = 1; // a fenced owner gets one shot, not three
        gw.run(&a, hash, &key, &["x", "y"], 5, false, &mut ghost).await;

        assert_eq!(
            ghost.total_acked(),
            0,
            "I4 violated (seed {seed}): the fenced owner acknowledged {} write(s) \
             after the new owner opened",
            ghost.total_acked()
        );
        assert!(
            a.is_closed(),
            "seed {seed}: the fenced owner never observed that it lost the shard, \
             so its background tasks are still live"
        );
        cov.hit(mech::OLD_OWNER_FENCED);

        // I1 across the move: what A acked is still readable through B.
        w.run(&b, hash, &key, &["x", "y"], 8, false, &mut log).await;
        let ds: Arc<dyn ObjectStore> = store.clone();
        let observed = drain_observed(&ds, &b, hash, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("seed {seed}: after handoff (pre-handoff acks={before}): {e}");
        }
    }
}

/// The dangerous window the previous test could not reach: an append that
/// is **already in flight** when the new owner opens.
///
/// The store gate parks the WAL PUT — after the engine has staged the
/// batch, before durability, therefore before any acknowledgment — which
/// is the `after_db_write_before_durable_ack` fault point without adding a
/// hook to production code. The contract: that request either acks and is
/// readable through the new owner, or fails; it may not ack and vanish.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handoff_with_an_append_in_flight_resolves_safely() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 31, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [6u8; 16];
    let prefix = "dst-inflight".to_string();

    let a = open_engine(store.clone(), &prefix).await;
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&a, hash, &key, &["k"], 4, false, &mut log).await;
    let settled = log.total_acked();
    assert!(settled > 0, "nothing acked before the handoff");

    // Park the next WAL write, then start an append that will block in it.
    // Park exactly one WAL write: the in-flight append's. The new
    // owner's open writes to the WAL too, and an unbounded hold would
    // park the handoff itself.
    let engaged = store.hold_class(StoreOp::Put, ObjClass::Wal, 1);
    let a2 = a.clone();
    let key2 = key.clone();
    let cov2 = cov.clone();
    let inflight = tokio::spawn(async move {
        let mut log = OpLog::default();
        let mut w = Workload::new(cov2);
        w.max_attempts = 1;
        let outcome = w.append(&a2, hash, &key2, "k", false, &mut log).await;
        (outcome, log)
    });

    // Wait for the append to actually park in the WAL write.
    for _ in 0..2000 {
        if engaged.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(
        engaged.load(Ordering::SeqCst) > 0,
        "no append ever parked in the WAL write — the scenario is vacuous"
    );
    cov.hit(mech::IN_FLIGHT_AT_FENCE);

    // Hand the shard over while that request is stuck mid-write.
    let b = open_engine(store.clone(), &prefix).await;
    store.release_hold();

    let (outcome, inflight_log) = inflight.await.expect("in-flight task panicked");

    // Whatever happened, it must not be "acknowledged but unreadable".
    let ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&ds, &b, hash, &key, &cov).await;
    for (rk, attempts) in &inflight_log.acked {
        log.acked.entry(rk.clone()).or_default().extend(attempts);
    }
    log.rejected.extend(inflight_log.rejected.iter().copied());
    if let Err(e) = log.audit(&observed) {
        panic!("in-flight handoff ({outcome:?}): {e}");
    }
    if let Err(e) = cov.require(&[mech::IN_FLIGHT_AT_FENCE]) {
        panic!("{e}");
    }
}



// ---- the tiered read path -------------------------------------------

/// Open the engine WITH a real absorber, so records migrate into the
/// history tier and reads exercise the production merge.
///
/// Runs multi-threaded on purpose: the absorber reaches
/// `crate::on_slatedb_rt` (a process-global multi-threaded runtime) and
/// `spawn_blocking`, so it cannot be driven from a paused current-thread
/// test. That is precisely the coupling docs/DST.md's roadmap has to break
/// before whole-scenario replay is possible.
async fn open_engine_with_absorber(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
) -> (Arc<crate::shard::ShardEngine>, tokio::task::JoinHandle<()>) {
    let db = slatedb::Db::builder(prefix, store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    let engine = crate::shard::ShardEngine::start(
        prefix.to_string(),
        Arc::new(db),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let keys = Arc::new(crate::history::KeyCache::default());
    // The absorber derives subkeys from (key, epoch); the workload uses the
    // stream hash as the epoch, so the cache must agree or nothing decodes.
    keys.put(hash, key.clone(), hash);
    let cfg = crate::history::AbsorberConfig {
        threshold_bytes: 1,
        threshold_age: std::time::Duration::from_millis(1),
        tick: std::time::Duration::from_millis(20),
        batch_puts: 256,
        pass_bytes: 8 * 1024 * 1024,
    };
    let handle = crate::history::Absorber::start(store, engine.clone(), keys, cfg, absorb_rx);
    (engine, handle)
}

/// I1 across the tier boundary. Records acknowledged on the shard log must
/// still be readable — through the production merged reader — after the
/// absorber has moved them into the history tier and the shard log has
/// been trimmed behind them.
///
/// The first version of this harness discarded the absorber channel
/// entirely and read straight off the shard log, so history DB creation,
/// block encryption, the absorbed-boundary publication, trimming and the
/// merge were all untested.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acked_records_survive_absorption_into_history() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 17, FaultPlan::new(0, 0, 10));
    let cov = store.coverage();
    let key = skey();
    let hash = [8u8; 16];
    let (engine, absorber) = open_engine_with_absorber(store.clone(), "dst-hist", hash, &key).await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["h1", "h2"], 40, false, &mut log)
        .await;
    assert!(log.total_acked() > 0, "nothing acked");

    // Wait for the absorbed boundary to advance past zero.
    let mut absorbed = 0u64;
    for _ in 0..400 {
        if let Ok(h) = engine.stream_handle(hash).await {
            absorbed = h.state.lock().unwrap().durable.absorbed;
            if absorbed > 0 {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        absorbed > 0,
        "the absorber never advanced the boundary — the scenario would only \
         have tested the shard log again"
    );

    let ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&ds, &engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("absorbed={absorbed}: {e}\ncoverage={:?}", cov.snapshot());
    }
    if let Err(e) = cov.require(&[mech::READ_FROM_HISTORY]) {
        panic!("{e}");
    }
    absorber.abort();
}

/// A fenced owner must not leave a **zombie absorber** behind.
///
/// This is not hypothetical: a fenced shard's absorber that keeps retrying
/// against a dead DB evicts the rightful owner's history handle in a
/// ping-pong — "the absorption war" (2026-07-20), which `history.rs`
/// guards against explicitly. Asserting that the old engine *reports*
/// itself closed is not the same as asserting its tasks actually exited,
/// and only the second one catches a leak.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_owners_absorber_exits() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 23, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [11u8; 16];
    let prefix = "dst-zombie";

    let (a, absorber_a) = open_engine_with_absorber(store.clone(), prefix, hash, &key).await;
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&a, hash, &key, &["z"], 10, false, &mut log).await;
    assert!(log.total_acked() > 0, "nothing acked before the handoff");
    assert!(
        !absorber_a.is_finished(),
        "the absorber exited before the shard was even fenced"
    );

    // The handoff: a new owner opens the same shard log.
    let (b, absorber_b) = open_engine_with_absorber(store.clone(), prefix, hash, &key).await;

    // Give the old engine a reason to notice: its next commit attempt is
    // what discovers the fence.
    let mut ghost = OpLog::default();
    let mut gw = Workload::new(cov.clone());
    gw.max_attempts = 1;
    gw.run(&a, hash, &key, &["z"], 3, false, &mut ghost).await;
    assert_eq!(
        ghost.total_acked(),
        0,
        "I4 violated: the fenced owner acknowledged writes"
    );

    // The old absorber must now exit of its own accord.
    let mut exited = false;
    for _ in 0..400 {
        if absorber_a.is_finished() {
            exited = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        exited,
        "the fenced owner's absorber is still running — a zombie that will \
         fight the new owner for its history DB"
    );

    absorber_b.abort();
    let _ = b;
}

// ---- the oracle itself must be able to fail -------------------------

fn obs(pairs: &[(&str, &[AttemptId])]) -> HashMap<String, Vec<AttemptId>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_vec()))
        .collect()
}

#[test]
fn oracle_accepts_a_faithful_read() {
    let mut log = OpLog::default();
    let acked: Vec<AttemptId> = (0..10).map(|i| (i, 0)).collect();
    log.acked.insert("k".into(), acked.clone());
    assert!(log.audit(&obs(&[("k", &acked)])).is_ok());
}

#[test]
fn oracle_catches_loss() {
    // The C3 shape: acknowledged but unreadable.
    let mut log = OpLog::default();
    let acked: Vec<AttemptId> = (0..10).map(|i| (i, 0)).collect();
    log.acked.insert("k".into(), acked);
    let err = log.audit(&obs(&[("k", &[(0, 0), (1, 0), (2, 0)])])).unwrap_err();
    assert!(err.starts_with("I1"), "expected I1, got: {err}");
}

#[test]
fn oracle_catches_duplicates() {
    let mut log = OpLog::default();
    log.acked.insert("k".into(), vec![(0, 0), (1, 0)]);
    let err = log
        .audit(&obs(&[("k", &[(0, 0), (1, 0), (1, 0)])]))
        .unwrap_err();
    assert!(err.starts_with("I3"), "expected I3, got: {err}");
}

#[test]
fn oracle_catches_reordering() {
    let mut log = OpLog::default();
    log.acked.insert("k".into(), vec![(0, 0), (1, 0), (2, 0)]);
    let err = log
        .audit(&obs(&[("k", &[(0, 0), (2, 0), (1, 0)])]))
        .unwrap_err();
    assert!(err.starts_with("I2"), "expected I2, got: {err}");
}

/// I5's negative control: a write the server refused must not turn up.
#[test]
fn oracle_catches_a_rejected_write_that_committed() {
    let mut log = OpLog::default();
    log.acked.insert("k".into(), vec![(0, 0)]);
    log.rejected.insert((1, 0));
    let err = log.audit(&obs(&[("k", &[(0, 0), (1, 0)])])).unwrap_err();
    assert!(err.starts_with("I5"), "expected I5, got: {err}");
}

/// I6's negative control: two attempts of one idempotent operation both
/// stored. This is the shape a broken producer-dedupe path would produce,
/// and the shape a retrying client would have caused during the
/// eu-central-1 wedge if it had not been using idempotence.
#[test]
fn oracle_catches_an_idempotent_operation_stored_twice() {
    let mut log = OpLog::default();
    log.idempotent.insert(9);
    log.acked.insert("k".into(), vec![(9, 0)]);
    log.unknown.insert((9, 1));
    let err = log.audit(&obs(&[("k", &[(9, 0), (9, 1)])])).unwrap_err();
    assert!(err.starts_with("I6"), "expected I6, got: {err}");
}

/// A non-idempotent retry after an unknown outcome may legitimately commit
/// twice. The oracle must NOT call that a bug — if it did, every ambiguity
/// scenario would fail for the wrong reason and the suite would be tuned
/// until it stopped testing ambiguity at all.
#[test]
fn oracle_permits_a_non_idempotent_retry_committing_twice() {
    let mut log = OpLog::default();
    log.unknown.insert((9, 0));
    log.acked.insert("k".into(), vec![(9, 1)]);
    assert!(
        log.audit(&obs(&[("k", &[(9, 0), (9, 1)])])).is_ok(),
        "a client without producer idempotence is allowed to double-write"
    );
}

/// The ledger must not be able to contradict itself unnoticed.
#[test]
fn oracle_catches_a_self_contradictory_ledger() {
    let mut log = OpLog::default();
    log.acked.insert("k".into(), vec![(1, 0)]);
    log.rejected.insert((1, 0));
    let err = log.audit(&obs(&[("k", &[(1, 0)])])).unwrap_err();
    assert!(err.starts_with("harness bug"), "got: {err}");
}
