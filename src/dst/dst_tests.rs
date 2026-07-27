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

    // Streaming list() must be faulted too — it used to delegate straight
    // through, so a scenario could "fault listings" while GC and recovery
    // walked an untouched store.
    let listed = s
        .list(Some(&ObjPath::from("shards/x/wal")))
        .collect::<Vec<_>>()
        .await;
    assert!(
        listed.iter().any(|r| r.is_err()),
        "streaming list() must be faultable (got {} items, all ok)",
        listed.len()
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


/// Anti-vacuity for the anti-vacuity mechanism: `STORE_LOST_RESPONSE` may
/// only increment when a response was *actually* discarded. Counting the
/// decision at roll time (the previous behaviour) let a scenario satisfy
/// `require(STORE_LOST_RESPONSE)` on a verb that ignored the decision.
#[tokio::test]
async fn lost_response_counter_tracks_applied_behaviour_only() {
    let inner = mem();
    // 100 % lost-response, but only multipart is exercised — and multipart
    // deliberately cannot apply it (it would leak an undrivable upload).
    let s = FaultStore::uniform(inner.clone(), 11, FaultPlan::new(0, 100, 0));
    let before = s.injected_lost();
    let _ = s
        .put_multipart_opts(
            &ObjPath::from("shards/x/wal/mpu.sst"),
            PutMultipartOptions::default(),
        )
        .await;
    assert_eq!(
        s.injected_lost(),
        before,
        "multipart cannot apply a lost response, so it must not count one"
    );

    // A plain PUT can apply it, and must count exactly then.
    let r = s
        .put_opts(
            &ObjPath::from("shards/x/wal/1.sst"),
            PutPayload::from(vec![1u8; 4]),
            PutOptions::default(),
        )
        .await;
    assert!(r.is_err(), "the caller must see the lost response");
    assert_eq!(
        s.injected_lost(),
        before + 1,
        "an applied lost response must count exactly once"
    );
    assert!(
        inner
            .get_opts(&ObjPath::from("shards/x/wal/1.sst"), GetOptions::default())
            .await
            .is_ok(),
        "and the write must nonetheless have landed"
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
    open_engine_cfg(store, prefix, crate::shard::ShardConfig::default()).await
}

async fn open_engine_cfg(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    cfg: crate::shard::ShardConfig,
) -> Arc<crate::shard::ShardEngine> {
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
    crate::shard::ShardEngine::start(prefix.to_string(), Arc::new(db), cfg, absorb_tx, None)
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

/// **I6, properly: duplicate suppression across a handoff.**
///
/// The previous version of this scenario could pass without ever
/// exercising duplicate suppression — the fenced owner could reject
/// everything and the new owner commit each retry as a fresh append. This
/// one forces the real path:
///
///   1. owner A durably commits producer P sequence N (acked, offset
///      recorded);
///   2. owner B opens and fences A — the handoff;
///   3. the client, which has an ambiguous view of N, retries **the exact
///      same bytes** with the same P/N against B;
///   4. B must answer `duplicate = true` **at the original offset**, and
///      the stream must contain that operation exactly once;
///   5. sequence N+1 must then succeed through B.
///
/// Producer state lives in the shard log (`producer_key`), so it survives
/// the handoff by being read back from storage by the new owner — that is
/// the property under test, and it is only observable via the duplicate
/// response.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn producer_state_survives_a_handoff_and_suppresses_duplicates() {
    for seed in [5u64, 21] {
        let inner = mem();
        let store = FaultStore::uniform(inner.clone(), seed, FaultPlan::new(0, 0, 30));
        let cov = store.coverage();
        let key = skey();
        let hash = [4u8; 16];
        let prefix = format!("dst-idem-{seed}");
        let pid = format!("producer-{seed}");
        // The exact request bytes a retry must resend verbatim.
        let body_n = format!("{{\"op\":\"n\",\"seed\":{seed}}}");

        let a = open_engine(store.clone(), &prefix).await;
        let w = Workload::new(cov.clone());

        // Warm the log so the tail is NOT at the committed offset — this is
        // what makes "original offset" a real assertion rather than a
        // coincidence.
        for i in 0..4u64 {
            let o = w
                .attempt_with_deadline(&a, hash, &key, "p", &format!("warm{i}"), None, None)
                .await;
            assert!(matches!(o, Outcome::Acked { .. }), "warm-up append");
        }

        // Sequence N (0 for a fresh producer epoch) commits through A.
        let first = w
            .attempt_with_deadline(
                &a, hash, &key, "p", &body_n,
                Some(crate::shard::ProducerReq { id: pid.clone(), epoch: 1, seq: 0 }),
                None,
            )
            .await;
        let orig_offset = match first {
            Outcome::Acked { last_offset, duplicate } => {
                assert!(!duplicate, "the first commit is not a duplicate");
                last_offset
            }
            other => panic!("seed {seed}: producer seq 0 must commit, got {other:?}"),
        };

        // More traffic, so the tail moves past the committed offset.
        for i in 0..3u64 {
            let _ = w
                .attempt_with_deadline(&a, hash, &key, "p", &format!("after{i}"), None, None)
                .await;
        }

        // The handoff.
        let b = open_engine(store.clone(), &prefix).await;
        cov.hit(mech::OLD_OWNER_FENCED);

        // The retry: identical bytes, identical producer identity.
        let retry = w
            .attempt_with_deadline(
                &b, hash, &key, "p", &body_n,
                Some(crate::shard::ProducerReq { id: pid.clone(), epoch: 1, seq: 0 }),
                None,
            )
            .await;
        match retry {
            Outcome::Acked { last_offset, duplicate } => {
                assert!(
                    duplicate,
                    "seed {seed}: the new owner did not recognise the retry as a \
                     duplicate — producer state did not survive the handoff"
                );
                assert_eq!(
                    last_offset, orig_offset,
                    "seed {seed}: duplicate ack must carry the ORIGINAL offset"
                );
            }
            other => panic!("seed {seed}: retry must be an acked duplicate, got {other:?}"),
        }

        // Exactly one copy in the stream, and no offset consumed by the
        // duplicate.
        let ds: Arc<dyn ObjectStore> = store.clone();
        let handle = b.stream_handle(hash).await.expect("handle");
        let before_next = handle.state.lock().unwrap().durable.next;
        let res = crate::http::read_merged(&ds, &key, &hash, &handle, &b, 0, None, 8 * 1024 * 1024)
            .await
            .expect("read back");
        let copies = res
            .recs
            .iter()
            .filter(|r| r.payload.as_ref() == body_n.as_bytes())
            .count();
        assert_eq!(
            copies, 1,
            "seed {seed}: the logical operation must appear exactly once (found {copies})"
        );
        let after_next = handle.state.lock().unwrap().durable.next;
        assert_eq!(
            before_next, after_next,
            "seed {seed}: a duplicate must not consume an offset"
        );

        // And the producer can continue: N+1 succeeds through the new owner.
        let next = w
            .attempt_with_deadline(
                &b, hash, &key, "p", "seq-1",
                Some(crate::shard::ProducerReq { id: pid.clone(), epoch: 1, seq: 1 }),
                None,
            )
            .await;
        assert!(
            matches!(next, Outcome::Acked { duplicate: false, .. }),
            "seed {seed}: sequence N+1 must commit after the handoff, got {next:?}"
        );

        if let Err(e) = cov.require(&[
            mech::APPEND_ACKED,
            mech::PRODUCER_DUPLICATE,
            mech::OLD_OWNER_FENCED,
        ]) {
            panic!("seed {seed}: {e}");
        }
    }
}

/// **Storage faults DO produce client-visible ambiguity — via deadlines.**
///
/// The earlier characterisation ("store errors surface as latency, not
/// failures") was measured with an infinitely patient caller, which made
/// it narrower than it sounded. Real callers have deadlines. Under heavy
/// injected store latency the append outlives the client's deadline: the
/// client records `Unknown` while the server commits anyway — ambiguity
/// with no fencing event anywhere.
///
/// The resolution is the same contract as everywhere else: retry
/// idempotently once storage heals, and exactly one operation exists.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn storage_latency_creates_client_ambiguity_resolved_by_idempotence() {
    let inner = mem();
    // Every WAL write takes 3-6 simulated seconds: far beyond the client
    // deadline below, nowhere near a failure.
    let slow = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 100,
        latency_ms: (3_000, 6_000),
    };
    let profile = FaultProfile::uniform(FaultPlan::new(0, 0, 20)).with_class(ObjClass::Wal, slow);
    let store = FaultStore::new(inner.clone(), 71, profile);
    let cov = store.coverage();
    let key = skey();
    let hash = [25u8; 16];
    let engine = open_engine(store.clone(), "dst-deadline").await;
    let w = Workload::new(cov.clone());
    let pid = "deadline-producer".to_string();
    let body = "{\"op\":\"deadline\"}".to_string();

    // The client gives up after 1 s while the append is still in flight.
    let first = w
        .attempt_with_deadline(
            &engine, hash, &key, "d", &body,
            Some(crate::shard::ProducerReq { id: pid.clone(), epoch: 1, seq: 0 }),
            Some(std::time::Duration::from_secs(1)),
        )
        .await;
    assert_eq!(
        first,
        Outcome::Unknown,
        "the client must time out while the server keeps working — \
         no ambiguity means the scenario is vacuous"
    );

    // Storage heals; the server's original append completes on its own.
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

    // The client resolves its ambiguity by retrying idempotently.
    let retry = w
        .attempt_with_deadline(
            &engine, hash, &key, "d", &body,
            Some(crate::shard::ProducerReq { id: pid.clone(), epoch: 1, seq: 0 }),
            None,
        )
        .await;
    match retry {
        Outcome::Acked { duplicate, .. } => assert!(
            duplicate,
            "the retry must be recognised as a duplicate — otherwise a \
             deadline-driven retry double-writes"
        ),
        other => panic!("retry should ack as a duplicate, got {other:?}"),
    }

    let ds: Arc<dyn ObjectStore> = store.clone();
    let handle = engine.stream_handle(hash).await.expect("handle");
    let res = crate::http::read_merged(&ds, &key, &hash, &handle, &engine, 0, None, 1 << 20)
        .await
        .expect("read back");
    let copies = res
        .recs
        .iter()
        .filter(|r| r.payload.as_ref() == body.as_bytes())
        .count();
    assert_eq!(copies, 1, "exactly one copy after the ambiguous retry");

    if let Err(e) = cov.require(&[
        mech::CLIENT_DEADLINE_EXPIRED,
        mech::APPEND_UNKNOWN,
        mech::PRODUCER_DUPLICATE,
        mech::STORE_LATENCY,
    ]) {
        panic!("{e}");
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

    // EVERY task the old engine owns must terminate — not just the
    // absorber, and not merely "is_finished" (which is also true after a
    // panic). await_terminated joins each handle and names stragglers.
    //
    // The committer is the one that used to be unable to exit at all: it
    // held the engine, the engine held its channel sender, so the channel
    // could never close. One resident committer + engine allocation per
    // shard move, forever.
    match a.await_terminated(std::time::Duration::from_secs(30)).await {
        Ok(()) => {}
        Err(e) => panic!("fenced owner left tasks behind: {e}"),
    }

    // The absorber is a separately-owned task; join it explicitly too.
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
    absorber_a.await.expect("absorber must exit cleanly, not panic");

    absorber_b.abort();
    let _ = b;
}

/// Queued-but-uncommitted appends must be answered when the shard closes,
/// not left to hang until each client's own timeout. `begin_close` drains
/// what is in flight; the committer drains what is still queued behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_an_engine_answers_queued_appends_and_ends_every_task() {
    let inner = mem();
    // Slow WAL writes so requests pile up behind the in-flight commit.
    let slow = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 100,
        latency_ms: (400, 800),
    };
    let store = FaultStore::new(
        inner.clone(),
        73,
        FaultProfile::uniform(FaultPlan::CLEAN).with_class(ObjClass::Wal, slow),
    );
    let cov = store.coverage();
    let key = skey();
    let hash = [26u8; 16];
    let engine = open_engine(store.clone(), "dst-drain").await;

    // Fire a burst without awaiting; they queue behind the slow commit.
    let mut waiters = Vec::new();
    for i in 0..16u64 {
        let e = engine.clone();
        let k = key.clone();
        let c = cov.clone();
        waiters.push(tokio::spawn(async move {
            let w = Workload::new(c);
            w.attempt_with_deadline(&e, hash, &k, "q", &format!("drain{i}"), None, None)
                .await
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    engine.begin_close();

    // Every caller must get an answer — acked (it made it) or Unknown
    // (fenced/moved) — and none may hang.
    let mut answered = 0;
    for w in waiters {
        match tokio::time::timeout(std::time::Duration::from_secs(20), w).await {
            Ok(Ok(_outcome)) => answered += 1,
            Ok(Err(e)) => panic!("waiter task panicked: {e}"),
            Err(_) => panic!("a queued append never received a response after close"),
        }
    }
    assert_eq!(answered, 16, "every queued append must be answered");

    engine
        .await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("all engine tasks must terminate after close");
}



/// **The group-commit pump with the post-ACK barrier and gather window,
/// under faults.** The gather path moves ack dispatch from the acker task
/// into the pump (an ordering change on the hottest path in the system),
/// so it gets the full treatment: WAL errors, lost responses, latency,
/// concurrent producers, retries, and the complete I1–I7 audit through
/// the production merged reader. The acker stays live as the failsafe —
/// this scenario must pass with BOTH dispatchers running, proving the
/// dispatch_gate keeps them from interleaving tail-state updates.
///
/// Also pins the idle contract: after quiet, a single append must not
/// wait a gather window it has no herd to gather (regression guard for
/// "gather only after a flush that dispatched work").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gather_pump_preserves_invariants_under_faults() {
    let mut total_gathers = 0u64;
    for seed in [3u64, 17, 41] {
        let inner = mem();
        let profile = FaultProfile::uniform(FaultPlan::new(0, 0, 40))
            .with_class(ObjClass::Wal, FaultPlan::new(8, 6, 40));
        let store = FaultStore::new(inner.clone(), seed, profile);
        let cov = store.coverage();
        let key = skey();
        let hash = [29u8; 16];
        let cfg = crate::shard::ShardConfig {
            wal_group_commit: true,
            wal_flush_gap: std::time::Duration::from_millis(2),
            wal_post_ack_gather: std::time::Duration::from_millis(3),
            ..Default::default()
        };
        let engine = open_engine_cfg(store.clone(), &format!("dst-gather-{seed}"), cfg).await;

        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        // Two closed-loop waves with concurrent keys — the shape the
        // gather window exists for.
        for _ in 0..4 {
            w.run(&engine, hash, &key, &["g1", "g2", "g3"], 10, false, &mut log)
                .await;
        }

        // The pump must be flushing, and across the seeds the drift
        // path (gather windows) must have been exercised. Per-seed
        // barrier_acked would flake: the acker fires on the same watch
        // change and legitimately wins most dispatch races — that is by
        // design, not a defect.
        let flushes = engine.pump_flushes.load(Ordering::Relaxed);
        assert!(flushes > 0, "seed {seed}: the pump never flushed");
        total_gathers += engine.pump_gathers.load(Ordering::Relaxed)
            + engine.pump_barrier_acked.load(Ordering::Relaxed);

        let ds: Arc<dyn ObjectStore> = store.clone();
        let observed = drain_observed(&ds, &engine, hash, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("seed {seed}: {e}");
        }

        // Idle contract: no herd, no gather tax. One append after quiet
        // completes in well under (gather + PUT + margin) of virtual/real
        // time; mainly this asserts it completes at all without waiting
        // for a second flush cycle.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let t0 = std::time::Instant::now();
        let o = w
            .attempt_with_deadline(&engine, hash, &key, "idle", "idle-probe", None, None)
            .await;
        assert!(
            matches!(o, Outcome::Acked { .. }),
            "seed {seed}: idle append must ack, got {o:?}"
        );
        let took = t0.elapsed();
        assert!(
            took < std::time::Duration::from_secs(5),
            "seed {seed}: idle append took {took:?} — the gather window is \
             taxing the idle path"
        );

        engine.begin_close();
        engine
            .await_terminated(std::time::Duration::from_secs(30))
            .await
            .expect("pump + committer + acker + ticker all terminate");
    }
    assert!(
        total_gathers > 0,
        "no seed ever exercised the gather/barrier path — the scenario \
         has degraded to re-testing the acker"
    );
}

/// **The tiering invariant, not just "the absorber ran".**
///
/// The absorption scenario waits for `absorbed > 0`, which proves records
/// moved but not that the split is real. This one waits for `trimmed > 0`
/// and then asserts the three-way structure directly:
///
///   [0, trimmed)          gone from the shard log
///   [0, absorbed)         readable from history
///   [absorbed, next)      readable from the shard tail
///
/// and that the merged read equals the canonical stream exactly — no gap
/// at the boundary, no overlap, nothing lost to the trim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_and_tail_partition_the_stream_after_trim() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 83, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [27u8; 16];
    let (engine, absorber) =
        open_engine_with_absorber(store.clone(), "dst-tier", hash, &key).await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    // Enough traffic, in waves, that absorption AND trim both advance
    // (trim lands one absorb round behind the boundary by design).
    for _ in 0..6 {
        w.run(&engine, hash, &key, &["t1", "t2"], 12, false, &mut log).await;
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    }

    let (mut trimmed, mut absorbed, mut next) = (0u64, 0u64, 0u64);
    for _ in 0..400 {
        if let Ok(h) = engine.stream_handle(hash).await {
            let st = h.state.lock().unwrap();
            trimmed = st.durable.trimmed;
            absorbed = st.durable.absorbed;
            next = st.durable.next;
        }
        if trimmed > 0 && absorbed < next {
            break;
        }
        // keep the stream moving so the absorber has work
        w.run(&engine, hash, &key, &["t1"], 2, false, &mut log).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        trimmed > 0,
        "trim never advanced (absorbed={absorbed}, next={next}) — this \
         scenario would only be re-testing absorption"
    );
    assert!(absorbed >= trimmed, "absorbed {absorbed} must cover trimmed {trimmed}");
    assert!(
        absorbed < next,
        "no live tail above the boundary (absorbed={absorbed}, next={next}) — \
         the merged read would be history-only and prove nothing"
    );

    // 1. Below the trim: physically gone from the shard log.
    let handle = engine.stream_handle(hash).await.expect("handle");
    let below = crate::shard::read_frames_range(&engine, &handle, 0, trimmed, 1 << 20)
        .await
        .expect("scan below trim");
    assert!(
        below.frames.is_empty(),
        "{} frames still in the shard log below the trim boundary",
        below.frames.len()
    );

    // 2. History serves [0, absorbed).
    let ds: Arc<dyn ObjectStore> = store.clone();
    let hist = crate::history::read_history(&ds, &hash, &key, 0, absorbed, None, 8 << 20)
        .await
        .expect("history read");
    assert!(
        !hist.records.is_empty(),
        "history returned nothing for [0, {absorbed})"
    );
    assert!(
        hist.records.iter().all(|(off, _)| *off < absorbed),
        "history returned an offset at or above the absorbed boundary"
    );

    // 3. The tail serves [absorbed, next).
    let tail = crate::shard::read_frames_range(&engine, &handle, absorbed, next, 8 << 20)
        .await
        .expect("tail scan");
    assert!(
        !tail.frames.is_empty(),
        "the shard tail is empty above the absorbed boundary"
    );

    // 4. The merged read is exactly the canonical stream: every acked
    //    record, once, in order — across the boundary.
    let observed = drain_observed(&ds, &engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("merged read is not the canonical stream (trimmed={trimmed}, absorbed={absorbed}, next={next}): {e}");
    }
    let merged_total: usize = observed.values().map(|v| v.len()).sum();
    assert!(
        merged_total >= hist.records.len(),
        "merged read ({merged_total}) returned fewer records than history alone ({})",
        hist.records.len()
    );
    if let Err(e) = cov.require(&[mech::READ_FROM_HISTORY]) {
        panic!("{e}");
    }
    absorber.abort();
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


/// I7's negative control: a record that belongs to no issued attempt must
/// be caught. Previously the audit only checked that acked attempts were
/// present, so a fabricated record could pass unnoticed.
#[test]
fn oracle_catches_a_record_that_was_never_issued() {
    let mut log = OpLog::default();
    log.issued.insert((1, 0));
    log.acked.insert("k".into(), vec![(1, 0)]);
    let err = log.audit(&obs(&[("k", &[(1, 0), (9, 9)])])).unwrap_err();
    assert!(err.starts_with("I7"), "expected I7, got: {err}");
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
            l0_sst_size_bytes: 1 << 30,
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
            &slatedb::config::WriteOptions {
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed put");
    }
    // Drop WITHOUT close: close() would flush the memtable to L0 and
    // advance the replay boundary, which is exactly what must not happen.
    drop(db);
}

/// The OLD `engine_for` semantics, verbatim in miniature: hold a lock,
/// await the open inline in the caller's task, insert into the map from
/// the caller's task. The inner Db open is spawned (as `on_slatedb_rt`
/// does in production), so abandoning the await detaches it.
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
                let eng = crate::shard::ShardEngine::start(
                    p,
                    Arc::new(db),
                    crate::shard::ShardConfig::default(),
                    absorb_tx,
                    None,
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
        Box::new(move |prefix: String| {
            let st = st.clone();
            Box::pin(async move {
                let s: Arc<dyn ObjectStore> = st;
                Ok(open_engine(s, &prefix).await)
            })
        }),
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

    let (started, completed, failed, coalesced) = OpenGate::counters_for_tests();
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
        Box::new(move |prefix: String| {
            let st = st.clone();
            Box::pin(async move {
                let s: Arc<dyn ObjectStore> = st.clone();
                Ok(open_engine(s, &prefix).await)
            })
        }),
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
        drop(eng);
        gate.notify_closed("dst-flap"); // died young (lifetime ≈ 0)
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
    let gate = OpenGate::with_deadline(
        shards.clone(),
        Box::new(move |prefix: String| {
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
        }),
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
    let (_started, completed, failed, _coalesced) = OpenGate::counters_for_tests();
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
        if let Some(e) = engines.first() {
            if e.is_closed() {
                reaped = true;
                break;
            }
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

// ---- the metadata-read surface (history reader cache + compactions GC)


// The history reader cache and its counters are process-global, so the
// tests below serialize on one lock (they pass solo but race each other
// under the parallel test harness), and poll-pinning uses an RAII guard
// so a panicking test cannot leave the hour-long poll behind for others.
fn hrc_lock() -> &'static tokio::sync::Mutex<()> {
    static L: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct PollPin;
impl PollPin {
    fn hour() -> Self {
        crate::history::set_reader_poll_ms_for_tests(3_600_000);
        PollPin
    }
}
impl Drop for PollPin {
    fn drop(&mut self) {
        crate::history::set_reader_poll_ms_for_tests(5_000);
    }
}

/// Protocol-cost budget: after the first read warms the cache, repeated
/// history reads must not open new DbReaders — the per-request manifest
/// GETs and checkpoint writes are exactly the small-metadata operations
/// Tigris sometimes serves from a remote region (the "metadata trickle",
/// docs/SOAK-REGIONS.md), so each cold open is a chance at a
/// transcontinental round trip on the user-visible read path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_reads_reuse_a_cached_reader() {
    let _serial = hrc_lock().lock().await;
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 41, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [21u8; 16];
    let (engine, absorber) =
        open_engine_with_absorber(store.clone(), "dst-hrc", hash, &key).await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["h"], 30, false, &mut log).await;
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
    assert!(absorbed > 0, "absorber never advanced — nothing to read from history");

    let ds: Arc<dyn ObjectStore> = store.clone();
    let (h0, m0, _p0, s0, _e0) = crate::history::reader_cache_counters();

    // First drain: opens (or reuses a prior test's) reader.
    let obs = drain_observed(&ds, &engine, hash, &key, &cov).await;
    log.audit(&obs).expect("first drain audit");
    let manifest_gets_after_warm = store.count(StoreOp::Get, ObjClass::Manifest);

    // Nineteen more drains: all served by the cached reader.
    for _ in 0..19 {
        let obs = drain_observed(&ds, &engine, hash, &key, &cov).await;
        log.audit(&obs).expect("cached drain audit");
    }

    let (h1, m1, _p1, s1, _e1) = crate::history::reader_cache_counters();
    assert!(
        m1 - m0 <= 1,
        "at most one cache miss across 20 drains (got {})",
        m1 - m0
    );
    assert!(
        h1 - h0 >= 19,
        "the cached reader must serve the repeat drains (hits {})",
        h1 - h0
    );
    // The absorber may advance between drains; each advance is allowed one
    // stale reopen — but reopens must be bounded by absorb cadence, never
    // by request count.
    assert!(
        s1 - s0 <= 3,
        "stale reopens must track absorb cadence, not request rate (got {})",
        s1 - s0
    );
    // Store-level corroboration: 19 cached drains must not multiply the
    // manifest traffic the warm-up produced. Uncached, every drain paid
    // the open cost again.
    let manifest_gets_final = store.count(StoreOp::Get, ObjClass::Manifest);
    let cached_drain_cost = manifest_gets_final - manifest_gets_after_warm;
    assert!(
        cached_drain_cost <= manifest_gets_after_warm.max(20),
        "19 cached drains cost {cached_drain_cost} manifest GETs vs {manifest_gets_after_warm} \
         for the entire warm-up — the cache is not being used"
    );
    absorber.abort();
}

/// The correctness edge the cache must not soften: the absorbed boundary
/// advances, and a read arrives BEFORE the cached reader's own poll has
/// caught up. The poll interval is pinned absurdly high so the staleness
/// is deterministic, not a race: the cache must detect non-coverage via
/// its one-row probe, reopen fresh, and return every acknowledged record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_cached_reader_is_detected_and_replaced() {
    let _serial = hrc_lock().lock().await;
    let _poll = PollPin::hour();
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 43, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [22u8; 16];
    let (engine, absorber) =
        open_engine_with_absorber(store.clone(), "dst-hrc-stale", hash, &key).await;

    let ds: Arc<dyn ObjectStore> = store.clone();
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());

    let mut wait_absorbed_past = |target: u64| {
        let engine = engine.clone();
        async move {
            for _ in 0..400 {
                if let Ok(h) = engine.stream_handle(hash).await {
                    let a = h.state.lock().unwrap().durable.absorbed;
                    if a > target {
                        return a;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            panic!("absorbed never advanced past {target}");
        }
    };

    // Batch 1 → absorb → drain (cache now holds a reader whose view ends
    // at the first boundary).
    w.run(&engine, hash, &key, &["s"], 15, false, &mut log).await;
    let a1 = wait_absorbed_past(0).await;
    let obs = drain_observed(&ds, &engine, hash, &key, &cov).await;
    log.audit(&obs).expect("drain 1");

    // Batch 2 → absorb further. The cached reader CANNOT know (its poll
    // is an hour away); only the probe-and-reopen path can serve this.
    w.run(&engine, hash, &key, &["s"], 15, false, &mut log).await;
    let a2 = wait_absorbed_past(a1).await;
    assert!(a2 > a1);

    let (_, _, _, s0, _) = crate::history::reader_cache_counters();
    let obs = drain_observed(&ds, &engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&obs) {
        panic!("stale-reader drain lost records: {e}");
    }
    let (_, _, _, s1, _) = crate::history::reader_cache_counters();
    assert!(
        s1 > s0,
        "the stale reader was never detected — the scenario is vacuous \
         (poll should have been pinned too high for it to self-heal)"
    );

    // And the replacement is itself cached: one more drain, no reopen.
    let (_, _, _, s2a, _) = crate::history::reader_cache_counters();
    let obs = drain_observed(&ds, &engine, hash, &key, &cov).await;
    log.audit(&obs).expect("drain 3");
    let (_, _, _, s2b, _) = crate::history::reader_cache_counters();
    assert_eq!(s2b, s2a, "the fresh reader must be cached, not reopened again");

    absorber.abort();
}

/// Key-filtered reads cannot verify coverage by offset contiguity (the
/// filter legitimately skips offsets), which is why coverage is proven by
/// probe. Same staleness setup, filtered read path directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_history_reads_survive_a_stale_reader() {
    let _serial = hrc_lock().lock().await;
    let _poll = PollPin::hour();
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 47, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [23u8; 16];
    let (engine, absorber) =
        open_engine_with_absorber(store.clone(), "dst-hrc-filt", hash, &key).await;

    let ds: Arc<dyn ObjectStore> = store.clone();
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["fa", "fb"], 10, false, &mut log).await;
    let mut a1 = 0;
    for _ in 0..400 {
        if let Ok(h) = engine.stream_handle(hash).await {
            a1 = h.state.lock().unwrap().durable.absorbed;
            if a1 > 0 {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(a1 > 0);
    // Warm the cache at boundary a1.
    let _ = crate::history::read_history(&ds, &hash, &key, 0, a1, Some("fa"), 1 << 20)
        .await
        .expect("warm filtered read");

    // Advance the boundary past the cached view.
    w.run(&engine, hash, &key, &["fa", "fb"], 10, false, &mut log).await;
    let mut a2 = a1;
    for _ in 0..400 {
        if let Ok(h) = engine.stream_handle(hash).await {
            a2 = h.state.lock().unwrap().durable.absorbed;
            if a2 > a1 {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(a2 > a1, "second absorb never landed");

    let res = crate::history::read_history(&ds, &hash, &key, 0, a2, Some("fa"), 1 << 20)
        .await
        .expect("filtered read at new boundary");
    assert!(
        res.completed,
        "filtered read must be coverage-proven complete after the fallback"
    );
    // Every "fa" record acked into [0, a2) must be present.
    let acked_fa = log.acked.get("fa").map(|v| v.len()).unwrap_or(0);
    let in_history = res.records.len();
    assert!(
        in_history >= acked_fa.saturating_sub(5),
        "filtered read returned {in_history} records for {acked_fa} acked \
         (allowing a small unabsorbed tail)"
    );
    absorber.abort();
}

/// The cache is bounded: streams beyond the cap evict the oldest reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_reader_cache_evicts_beyond_its_cap() {
    let _serial = hrc_lock().lock().await;
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 53, FaultPlan::CLEAN);
    let key = skey();
    let ds: Arc<dyn ObjectStore> = store.clone();
    let (_, _, _, _, e0) = crate::history::reader_cache_counters();

    // Twelve distinct minimal history DBs. A DbReader cannot open a
    // nonexistent prefix (production never asks it to: reads only happen
    // once the absorber has created the db), so seed each with one row.
    for i in 0..12u8 {
        let mut hash = [30u8; 16];
        hash[15] = i;
        let path = crate::history::history_db_path(&hash);
        let db = slatedb::Db::builder(path.as_str(), ds.clone())
            .build()
            .await
            .expect("seed history db");
        db.put(b"seed", b"1").await.expect("seed row");
        db.close().await.expect("close seed db");
        let _ = crate::history::reader_cache()
            .acquire(&ds, &hash, &key, 0)
            .await
            .expect("acquire");
    }
    let (_, _, _, _, e1) = crate::history::reader_cache_counters();
    assert!(
        e1 - e0 >= 3,
        "12 streams past a cap of 8 must evict (evictions {})",
        e1 - e0
    );
    assert!(
        crate::history::reader_cache().len_for_tests().await <= 9,
        "cache size must stay near its cap"
    );
}

/// Reopen cost after compactor churn is a protocol budget: the
/// compactions log is a versioned object where every compactor state
/// change mints another file, and open pages through the survivors — at
/// cross-region latency this class made the eu-central-1 open crawl. With
/// GC reaping superseded versions (min_age floored at 0 here), a reopen
/// after heavy churn must cost a bounded number of small-object reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopen_cost_is_bounded_after_compactions_churn() {
    let inner = mem();
    let key = skey();
    let hash = [24u8; 16];

    let settings = || slatedb::config::Settings {
        flush_interval: Some(std::time::Duration::from_millis(2)),
        manifest_poll_interval: std::time::Duration::from_millis(25),
        // Tiny L0 SSTs force constant compactor activity → many
        // `.compactions` versions.
        l0_sst_size_bytes: 4 * 1024,
        compactor_options: Some(slatedb::config::CompactorOptions {
            poll_interval: std::time::Duration::from_millis(20),
            ..Default::default()
        }),
        garbage_collector_options: Some(slatedb::config::GarbageCollectorOptions {
            compactions_options: Some(slatedb::config::GarbageCollectorDirectoryOptions {
                interval: Some(std::time::Duration::from_millis(200)),
                min_age: std::time::Duration::from_secs(0),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Churn phase on the raw store (counted separately from the reopen).
    let churn_store = FaultStore::uniform(inner.clone(), 59, FaultPlan::CLEAN);
    {
        let db = slatedb::Db::builder("dst-compact", churn_store.clone())
            .with_settings(settings())
            .build()
            .await
            .expect("open churn db");
        for i in 0..400u32 {
            db.put_with_options(
                format!("k{i:05}").as_bytes(),
                vec![7u8; 512].as_slice(),
                &slatedb::config::PutOptions::default(),
                &slatedb::config::WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .expect("churn put");
            if i % 50 == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        }
        // Let compactor + GC cycles run, then close cleanly.
        tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
        db.close().await.expect("close churn db");
    }
    // Non-vacuity: the churn must actually have minted compactions-log
    // versions (they are Put:Other in our classifier).
    let churn_other_puts = churn_store.count(StoreOp::Put, ObjClass::Other);
    assert!(
        churn_other_puts >= 5,
        "churn phase minted only {churn_other_puts} small-object versions — \
         the compactor never ran and this budget test is vacuous"
    );
    let _ = hash;
    let _ = key;

    // Reopen through a fresh counting store: the budget under test.
    let reopen_store = FaultStore::uniform(inner.clone(), 61, FaultPlan::CLEAN);
    let db2 = slatedb::Db::builder("dst-compact", reopen_store.clone())
        .with_settings(settings())
        .build()
        .await
        .expect("reopen db");
    let other_reads = reopen_store.count(StoreOp::Get, ObjClass::Other)
        + reopen_store.count(StoreOp::List, ObjClass::Other);
    assert!(
        other_reads <= 40,
        "reopen paged through {other_reads} small-object reads — the \
         compactions log is not being reaped (budget 40)"
    );
    db2.close().await.ok();
}
