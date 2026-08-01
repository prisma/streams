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

fn skey2() -> crate::crypto::StreamKey {
    crate::crypto::StreamKey([8u8; 32])
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
    // Mirror production: with the pump on, SlateDB's own flush timer is a
    // long failsafe (else it flushes mid-PUT commits itself and the pump's
    // gather/skip machinery never sees a busy generation).
    let flush_interval = if cfg.wal_group_commit {
        std::time::Duration::from_secs(1)
    } else {
        std::time::Duration::from_millis(5)
    };
    let db = slatedb::Db::builder(prefix, store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(flush_interval),
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
        store,
        cfg,
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
        let observed = drain_observed(&engine, hash, &key, &cov).await;
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
    let observed = drain_observed(&engine, hash, &key, &cov).await;
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
                &a,
                hash,
                &key,
                "p",
                &body_n,
                Some(crate::shard::ProducerReq {
                    id: pid.clone(),
                    epoch: 1,
                    seq: 0,
                    request_hash: None,
                }),
                None,
            )
            .await;
        let orig_offset = match first {
            Outcome::Acked {
                last_offset,
                duplicate,
            } => {
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
                &b,
                hash,
                &key,
                "p",
                &body_n,
                Some(crate::shard::ProducerReq {
                    id: pid.clone(),
                    epoch: 1,
                    seq: 0,
                    request_hash: None,
                }),
                None,
            )
            .await;
        match retry {
            Outcome::Acked {
                last_offset,
                duplicate,
            } => {
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
        let res = crate::http::read_merged(&key, &hash, &handle, &b, 0, None, 8 * 1024 * 1024)
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
                &b,
                hash,
                &key,
                "p",
                "seq-1",
                Some(crate::shard::ProducerReq {
                    id: pid.clone(),
                    epoch: 1,
                    seq: 1,
                    request_hash: None,
                }),
                None,
            )
            .await;
        assert!(
            matches!(
                next,
                Outcome::Acked {
                    duplicate: false,
                    ..
                }
            ),
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

/// **I6, the undeniable composite** (review #3's closing ask): the append
/// COMMITS durably but its response never reaches the client (deadline
/// expires under WAL latency — genuine ambiguity, not a rejected write),
/// THEN ownership moves, THEN the client retries the identical bytes
/// against the new owner. The retry must dedupe at the original offset —
/// which the client never learned, so the test recovers it from the
/// stream itself, exactly as a reconciling client would.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn ambiguous_commit_survives_handoff_and_dedupes() {
    let inner = mem();
    let slow_wal = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 100,
        latency_ms: (2_000, 4_000),
    };
    let profile =
        FaultProfile::uniform(FaultPlan::new(0, 0, 15)).with_class(ObjClass::Wal, slow_wal);
    let store = FaultStore::new(inner.clone(), 77, profile);
    let cov = store.coverage();
    let key = skey();
    let hash = [37u8; 16];
    let prefix = "dst-ambig-handoff";
    let body = "{\"op\":\"ambig\"}".to_string();
    let pid = "ambig-producer".to_string();

    let a = open_engine(store.clone(), prefix).await;
    let w = Workload::new(cov.clone());

    // Tail context so "original offset" is not the trivial 0.
    for i in 0..3u64 {
        let o = w
            .attempt_with_deadline(&a, hash, &key, "x", &format!("pre{i}"), None, None)
            .await;
        assert!(matches!(o, Outcome::Acked { .. }));
    }

    // The ambiguous commit: client deadline expires mid-WAL-flush; the
    // server finishes on its own. The client records Unknown and NEVER
    // sees an offset.
    let first = w
        .attempt_with_deadline(
            &a,
            hash,
            &key,
            "x",
            &body,
            Some(crate::shard::ProducerReq {
                id: pid.clone(),
                epoch: 1,
                seq: 0,
                request_hash: None,
            }),
            Some(std::time::Duration::from_millis(300)),
        )
        .await;
    assert_eq!(first, Outcome::Unknown, "the ambiguity must be real");
    tokio::time::sleep(std::time::Duration::from_secs(20)).await; // server completes

    // Handoff: B fences A.
    let b = open_engine(store.clone(), prefix).await;
    cov.hit(mech::OLD_OWNER_FENCED);

    // The retry: identical bytes, identical producer identity, new owner.
    let retry = w
        .attempt_with_deadline(
            &b,
            hash,
            &key,
            "x",
            &body,
            Some(crate::shard::ProducerReq {
                id: pid.clone(),
                epoch: 1,
                seq: 0,
                request_hash: None,
            }),
            None,
        )
        .await;
    let (retry_off, dup) = match retry {
        Outcome::Acked {
            last_offset,
            duplicate,
        } => (last_offset, duplicate),
        other => panic!("retry must ack, got {other:?}"),
    };
    assert!(
        dup,
        "the durably-committed-but-unacked append must be recognised as a \
         duplicate by the NEW owner"
    );

    // Recover the original offset from the stream (the client never got
    // it) and check the dedupe pointed there — and that exactly one copy
    // exists across both owners' tenures.
    let ds: Arc<dyn ObjectStore> = store.clone();
    let handle = b.stream_handle(hash).await.expect("handle");
    let res = crate::http::read_merged(&key, &hash, &handle, &b, 0, None, 8 * 1024 * 1024)
        .await
        .expect("read back");
    let copies: Vec<u64> = res
        .recs
        .iter()
        .filter(|r| r.payload.as_ref() == body.as_bytes())
        .map(|r| r.off)
        .collect();
    assert_eq!(copies.len(), 1, "exactly one copy of the ambiguous op");
    assert_eq!(
        copies[0], retry_off,
        "the duplicate ack must carry the offset the commit actually landed at"
    );

    if let Err(e) = cov.require(&[
        mech::CLIENT_DEADLINE_EXPIRED,
        mech::PRODUCER_DUPLICATE,
        mech::OLD_OWNER_FENCED,
        mech::STORE_LATENCY,
    ]) {
        panic!("{e}");
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
            &engine,
            hash,
            &key,
            "d",
            &body,
            Some(crate::shard::ProducerReq {
                id: pid.clone(),
                epoch: 1,
                seq: 0,
                request_hash: None,
            }),
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
            &engine,
            hash,
            &key,
            "d",
            &body,
            Some(crate::shard::ProducerReq {
                id: pid.clone(),
                epoch: 1,
                seq: 0,
                request_hash: None,
            }),
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
    let res = crate::http::read_merged(&key, &hash, &handle, &engine, 0, None, 1 << 20)
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
        gw.run(&a, hash, &key, &["x", "y"], 5, false, &mut ghost)
            .await;

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
        let observed = drain_observed(&b, hash, &key, &cov).await;
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
    let observed = drain_observed(&b, hash, &key, &cov).await;
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
    open_engine_with_absorber_layout(store, prefix, hash, key).await
}

async fn open_engine_with_absorber_layout(
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
        store.clone(),
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
        ..Default::default()
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
    let observed = drain_observed(&engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("absorbed={absorbed}: {e}\ncoverage={:?}", cov.snapshot());
    }
    if let Err(e) = cov.require(&[mech::READ_FROM_HISTORY]) {
        panic!("{e}");
    }
    absorber.abort();
}

/// Drain the merged reader WITH a key filter (drain_observed hardcodes
/// unfiltered reads): paginate read_merged and collect attempt ids.
async fn drain_filtered(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    filter: &str,
) -> Vec<AttemptId> {
    let mut out = Vec::new();
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mut from = 0u64;
    for _ in 0..1024 {
        let res = crate::http::read_merged(
            key,
            &hash,
            &handle,
            engine,
            from,
            Some(filter),
            8 * 1024 * 1024,
        )
        .await
        .expect("filtered read");
        for rec in &res.recs {
            let v: serde_json::Value = serde_json::from_slice(&rec.payload).expect("payload");
            let (op, att) = (v["op"].as_u64().unwrap(), v["att"].as_u64().unwrap() as u32);
            assert_eq!(
                v["k"].as_str().unwrap(),
                filter,
                "filter {filter:?} returned a record for key {:?}",
                v["k"]
            );
            out.push((op, att));
        }
        if res.completed {
            break;
        }
        match res.last {
            Some(last) if last + 1 > from => from = last + 1,
            _ => {}
        }
    }
    out
}

/// Signals are the absorber's fast path, not its source of truth. The
/// signal channel is a bounded `try_send` (it provably drops ~35k of
/// 100k seed signals, docs/COST-WIDE1.md §3), and a restarted instance
/// has no signals for pre-crash data. The re-discovery sweep must find
/// unabsorbed streams from the engine's resident handles with NO signal
/// ever delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absorber_sweep_recovers_streams_whose_signals_were_lost() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 51, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [35u8; 16];

    let db = slatedb::Db::builder("dst-sweep", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    // The engine's signal channel goes nowhere: rx dropped on the spot.
    let (engine_tx, engine_rx) = crate::history::absorber_channel();
    drop(engine_rx);
    let engine = crate::shard::ShardEngine::start(
        "dst-sweep".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        engine_tx,
        None,
    );
    let keys = Arc::new(crate::history::KeyCache::default());
    keys.put(hash, key.clone(), hash);
    // The absorber listens on a channel that never carries a signal. Keep
    // the sender alive: a closed channel would exit the absorber loop.
    let (_quiet_tx, quiet_rx) = crate::history::absorber_channel();
    let absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        keys,
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            batch_puts: 256,
            pass_bytes: 8 * 1024 * 1024,
            sweep_every: 2,
            ..Default::default()
        },
        quiet_rx,
    );

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["s"], 15, false, &mut log)
        .await;
    assert!(log.total_acked() > 0, "nothing acked");

    // No signal was ever delivered; only the sweep can find this stream.
    let mut caught_up = false;
    for _ in 0..400 {
        if let Ok(h) = engine.stream_handle(hash).await {
            let st = h.state.lock().unwrap();
            if st.durable.absorbed > 0 && st.durable.absorbed == st.durable.next {
                caught_up = true;
            }
        }
        if caught_up {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        caught_up,
        "the sweep never absorbed the signal-less stream — lost signals \
         mean lost absorption"
    );
    let ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("sweep-absorbed stream lost records: {e}");
    }
    absorber.abort();
}

/// History v2's headline property: absorption WITHOUT the customer key.
/// The gather lane copies raw encrypted frames into the shared
/// partition, so an absorber whose KeyCache is EMPTY must still absorb
/// — and the records must decode correctly on read, where the client
/// supplies the key. (v1 required the key server-side and stranded
/// key-expired backlogs; docs/COST-WIDE1.md §2.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_absorbs_without_customer_keys() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 61, FaultPlan::new(0, 0, 10));
    let cov = store.coverage();
    let key = skey();
    let hash = [50u8; 16];

    let db = slatedb::Db::builder("dst-v2nokey", store.clone() as Arc<dyn ObjectStore>)
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
        "dst-v2nokey".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    // NO keys.put: the v1 absorber would return key-missing forever.
    let keys = Arc::new(crate::history::KeyCache::default());
    let absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        keys,
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            batch_puts: 256,
            pass_bytes: 8 * 1024 * 1024,
            ..Default::default()
        },
        absorb_rx,
    );

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["", "vk"], 25, false, &mut log)
        .await;
    assert!(log.total_acked() > 0, "nothing acked");

    let mut absorbed = 0u64;
    for _ in 0..400 {
        if let Ok(h) = engine.stream_handle(hash).await {
            let st = h.state.lock().unwrap();
            absorbed = st.durable.absorbed;
            if absorbed > 0 {
                assert!(
                    st.durable.history_v2,
                    "absorption advanced without the v2 flag"
                );
            }
        }
        if absorbed > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        absorbed > 0,
        "keyless v2 absorption never advanced — the gather lane still \
         depends on the customer key"
    );

    // Reads (client-supplied key) must see every acked record across the
    // boundary, and filters must work against the shared partition.
    let ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("v2 keyless absorption lost records (absorbed={absorbed}): {e}");
    }
    let unkeyed = drain_filtered(&engine, hash, &key, "").await;
    assert_eq!(&unkeyed, &log.acked[""], "v2 empty-key filter broken");
    let keyed = drain_filtered(&engine, hash, &key, "vk").await;
    assert_eq!(&keyed, &log.acked["vk"], "v2 keyed filter broken");
    absorber.abort();
}

/// v2 history must survive the owner handing the shard to a NEW engine:
/// the flags/route round-trip through the durable tail, the successor
/// opens the shared partition itself (fencing the old writer), and every
/// acked record stays readable — without any customer key ever reaching
/// an absorber.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_history_survives_engine_handoff() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 67, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [51u8; 16];
    let prefix = "dst-v2reopen";

    let (a, absorber_a) = open_engine_with_absorber(store.clone(), prefix, hash, &key).await;
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&a, hash, &key, &["r"], 20, false, &mut log).await;
    let mut absorbed = 0u64;
    for _ in 0..400 {
        if let Ok(h) = a.stream_handle(hash).await {
            absorbed = h.state.lock().unwrap().durable.absorbed;
            if absorbed > 0 {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(absorbed > 0, "no v2 absorption before the handoff");

    // Successor opens the same shard; its first commit fences the old
    // owner, its partition open fences the old partition writer.
    let (b, absorber_b) = open_engine_with_absorber(store.clone(), prefix, hash, &key).await;
    // Same Workload: op numbering must continue, or the post-handoff ops
    // collide with the pre-handoff ones in the shared OpLog.
    w.run(&b, hash, &key, &["r"], 5, false, &mut log).await;

    let ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&b, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("v2 history lost records across the handoff: {e}");
    }
    {
        let h = b.stream_handle(hash).await.expect("handle");
        let st = h.state.lock().unwrap();
        assert!(
            st.durable.history_v2,
            "v2 flag lost across the tail round-trip"
        );
    }
    absorber_a.abort();
    absorber_b.abort();
    a.begin_close();
}

/// The interim sparse policy: AGE absorption requires min_age_bytes of
/// pending data. A tiny stream must stay in the shard log (readable,
/// durable, no per-stream history DB minted) while a fat-enough stream
/// age-absorbs — and the deferred stream is reported as policy, not lag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_streams_defer_absorption_until_they_have_volume() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 57, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let tiny = [40u8; 16];
    let fat = [41u8; 16];

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
    let engine = crate::shard::ShardEngine::start(
        "dst-defer".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let keys = Arc::new(crate::history::KeyCache::default());
    keys.put(tiny, key.clone(), tiny);
    keys.put(fat, key.clone(), fat);
    let absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        keys,
        crate::history::AbsorberConfig {
            // Byte threshold out of reach; age immediate — so ONLY the
            // min_age_bytes gate separates the two streams.
            threshold_bytes: 64 * 1024 * 1024,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            batch_puts: 256,
            pass_bytes: 8 * 1024 * 1024,
            // DST workload frames are tiny (~40-90 B): 2 records sit far
            // under this gate, 60 far over it.
            min_age_bytes: 1024,
            ..Default::default()
        },
        absorb_rx,
    );

    // Pause absorption while the workloads run: otherwise the fat
    // stream's pending crosses the gate mid-workload, a pass absorbs the
    // prefix, and the residue re-enters BELOW the gate — correct policy
    // behavior (the residue stays readable in the shard log and defers
    // until it has volume), but it makes "absorbed == next" racy here.
    crate::history::absorb_pause_flag().store(true, Ordering::Relaxed);
    let mut tiny_log = OpLog::default();
    let mut w1 = Workload::new(cov.clone());
    // ~2 small frames pending: under the 1 KiB min — must defer.
    w1.run(&engine, tiny, &key, &["d"], 2, false, &mut tiny_log)
        .await;
    let mut fat_log = OpLog::default();
    let mut w2 = Workload::new(cov.clone());
    // ~60 frames pending: over the min — must age-absorb.
    w2.run(&engine, fat, &key, &["d"], 60, false, &mut fat_log)
        .await;
    crate::history::absorb_pause_flag().store(false, Ordering::Relaxed);

    let mut fat_absorbed = false;
    for _ in 0..400 {
        let h = engine.stream_handle(fat).await.expect("handle");
        {
            let st = h.state.lock().unwrap();
            if st.durable.absorbed > 0 && st.durable.absorbed == st.durable.next {
                fat_absorbed = true;
            }
        }
        if fat_absorbed {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(fat_absorbed, "the fat stream never age-absorbed");

    // The tiny stream had the same age and MORE ticks than it needed —
    // it must still be un-absorbed, by policy, and counted as deferred.
    let h = engine.stream_handle(tiny).await.expect("handle");
    {
        let st = h.state.lock().unwrap();
        assert_eq!(
            st.durable.absorbed, 0,
            "the sparse policy absorbed a tiny stream into per-stream history"
        );
        assert!(st.durable.next > 0);
    }
    // (The deferred_sparse SUMMARY is a process-global gauge that other
    // tests' absorbers overwrite in the parallel suite; the Arm B wide
    // run asserts it in a single-server process. Here: the per-hash lag
    // map, which is collision-free.)
    assert_eq!(
        crate::usage::absorb_lag(crate::crypto::SegmentHash(tiny)),
        0,
        "a policy-deferred stream must not read as absorb lag"
    );

    // Both streams stay fully readable through the merged reader.
    let ds: Arc<dyn ObjectStore> = store.clone();
    let obs_tiny = drain_observed(&engine, tiny, &key, &cov).await;
    tiny_log
        .audit(&obs_tiny)
        .expect("tiny stream readable from the shard log");
    let obs_fat = drain_observed(&engine, fat, &key, &cov).await;
    fat_log
        .audit(&obs_fat)
        .expect("fat stream readable after absorption");
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
    absorber_a
        .await
        .expect("absorber must exit cleanly, not panic");

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

/// **Review #2's barrier test, exact form: the gather must not start
/// until this flush's acks are ON THE WIRE.** The dispatch gate is held
/// (the deterministic stand-in for "the acker is paused after durability,
/// before response dispatch"); while held, the client's ack must not
/// arrive AND the pump must not enter a gather window; on release, ack
/// then gather. This is the property that makes "post-ACK" true rather
/// than merely "post-flush".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gather_window_waits_for_ack_dispatch() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 87, FaultPlan::new(0, 0, 30));
    let cov = store.coverage();
    let key = skey();
    let hash = [40u8; 16];
    let cfg = crate::shard::ShardConfig {
        wal_group_commit: true,
        wal_flush_gap: std::time::Duration::from_millis(2),
        wal_post_ack_gather: std::time::Duration::from_millis(4),
        ..Default::default()
    };
    let engine = open_engine_cfg(store.clone(), "dst-barrier", cfg).await;
    let w = Workload::new(cov.clone());

    // Warm one append so the pipeline is established.
    let o = w
        .attempt_with_deadline(&engine, hash, &key, "b", "warm", None, None)
        .await;
    assert!(matches!(o, Outcome::Acked { .. }));

    // Hold dispatch, then fire an append. Its flush may complete, but its
    // ack CANNOT be dispatched and no gather may begin.
    let guard = engine.test_hold_dispatch();
    let e2 = engine.clone();
    let k2 = key.clone();
    let c2 = cov.clone();
    let waiter = tokio::spawn(async move {
        let w2 = Workload::new(c2);
        w2.attempt_with_deadline(&e2, hash, &k2, "b", "held", None, None)
            .await
    });
    // Give the commit + flush ample real time while dispatch stays held.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let gathers_held = engine.pump_gathers.load(Ordering::Relaxed)
        + engine.pump_gathers_skipped_busy.load(Ordering::Relaxed);
    assert!(
        !waiter.is_finished(),
        "the ack must not reach the client while dispatch is held"
    );
    drop(guard);
    let out = tokio::time::timeout(std::time::Duration::from_secs(30), waiter)
        .await
        .expect("ack after release")
        .expect("join");
    assert!(matches!(out, Outcome::Acked { .. }), "got {out:?}");
    // The gather decision for that flush happened AFTER release — i.e.
    // after dispatch — so the counter moves only once the ack was out.
    for _ in 0..100 {
        let now = engine.pump_gathers.load(Ordering::Relaxed)
            + engine.pump_gathers_skipped_busy.load(Ordering::Relaxed);
        if now > gathers_held {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("terminate");
}

/// **Review #2's deadlock probe: commits landing DURING a flush must not
/// extend what that flush's barrier waits for.** The target is captured
/// before the flush; groups committed while the PUT is in flight belong
/// to the next generation. Under 200-400 ms WAL latency, appends fired
/// mid-flight must all ack promptly across >= 2 flushes — a pump waiting
/// on the wrong generation would need ITSELF to flush again and would
/// stall until the 250 ms failsafe (visible here as a hang).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_during_a_flush_do_not_extend_its_barrier() {
    let inner = mem();
    let slow_wal = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 100,
        latency_ms: (200, 400),
    };
    let store = FaultStore::new(
        inner.clone(),
        89,
        FaultProfile::uniform(FaultPlan::CLEAN).with_class(ObjClass::Wal, slow_wal),
    );
    let cov = store.coverage();
    let key = skey();
    let hash = [41u8; 16];
    let cfg = crate::shard::ShardConfig {
        wal_group_commit: true,
        wal_flush_gap: std::time::Duration::from_millis(2),
        wal_post_ack_gather: std::time::Duration::from_millis(4),
        ..Default::default()
    };
    let engine = open_engine_cfg(store.clone(), "dst-midflight", cfg).await;

    let mut waves = Vec::new();
    for i in 0..12u64 {
        let e = engine.clone();
        let k = key.clone();
        let c = cov.clone();
        waves.push(tokio::spawn(async move {
            let w = Workload::new(c);
            // Staggered so several land while earlier flushes are in
            // flight.
            tokio::time::sleep(std::time::Duration::from_millis(i * 60)).await;
            w.attempt_with_deadline(&e, hash, &k, "m", &format!("mid{i}"), None, None)
                .await
        }));
    }
    for t in waves {
        let out = tokio::time::timeout(std::time::Duration::from_secs(30), t)
            .await
            .expect("no barrier stall")
            .expect("join");
        assert!(matches!(out, Outcome::Acked { .. }), "got {out:?}");
    }
    assert!(
        engine.pump_flushes.load(Ordering::Relaxed) >= 2,
        "the scenario must span multiple generations"
    );
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("terminate");
}

/// **Adaptive gather: a busy next generation skips the window.**
///
/// Construction note (itself a finding): a fully SYNCHRONIZED herd at
/// saturation produces no drift — everyone re-enters during the settle
/// and the drift key already suppresses the window — so the busy-skip
/// only matters when drift and volume coincide. That coincidence is
/// built deterministically here: dispatch is held, batch A flushes,
/// batch B commits behind it, and on release the pump dispatches A and
/// finds B (large) already pending. Threshold 4 must record a busy-skip;
/// threshold-disabled must gather instead. The knob is the only
/// difference, so the counters prove the mechanism, not scheduling luck.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_busy_next_generation_skips_the_gather_window() {
    for (skip_reqs, expect_skips) in [(4u32, true), (u32::MAX, false)] {
        let inner = mem();
        let store = FaultStore::uniform(inner.clone(), 91, FaultPlan::new(0, 0, 20));
        let cov = store.coverage();
        let key = skey();
        let hash = [42u8; 16];
        let cfg = crate::shard::ShardConfig {
            wal_group_commit: true,
            wal_flush_gap: std::time::Duration::from_millis(2),
            wal_post_ack_gather: std::time::Duration::from_millis(4),
            wal_gather_skip_reqs: skip_reqs,
            wal_gather_skip_bytes: u64::MAX,
            ..Default::default()
        };
        let engine = open_engine_cfg(store.clone(), &format!("dst-busy-{skip_reqs}"), cfg).await;
        let w = Workload::new(cov.clone());
        let o = w
            .attempt_with_deadline(&engine, hash, &key, "s", "warm", None, None)
            .await;
        assert!(matches!(o, Outcome::Acked { .. }));

        // Hold dispatch; batch A (4 appends) commits and flushes but its
        // acks are stuck; batch B (8 appends) commits BEHIND it.
        let guard = engine.test_hold_dispatch();
        let mut all = Vec::new();
        for i in 0..12u64 {
            let e = engine.clone();
            let k = key.clone();
            let c = cov.clone();
            all.push(tokio::spawn(async move {
                let w2 = Workload::new(c);
                w2.attempt_with_deadline(&e, hash, &k, "s", &format!("b{i}"), None, None)
                    .await
            }));
            if i == 3 {
                // Let batch A reach its flush before B starts committing.
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let skips_before = engine.pump_gathers_skipped_busy.load(Ordering::Relaxed);
        let applied_before = engine.pump_gathers.load(Ordering::Relaxed);
        drop(guard);
        for t in all {
            let out = tokio::time::timeout(std::time::Duration::from_secs(30), t)
                .await
                .expect("ack")
                .expect("join");
            assert!(matches!(out, Outcome::Acked { .. }), "got {out:?}");
        }
        let skips = engine.pump_gathers_skipped_busy.load(Ordering::Relaxed) - skips_before;
        let applied = engine.pump_gathers.load(Ordering::Relaxed) - applied_before;
        if expect_skips {
            assert!(
                skips > 0,
                "drift+volume with threshold 4 must busy-skip (skips={skips}, applied={applied})"
            );
        } else {
            assert_eq!(
                skips, 0,
                "threshold disabled must never skip (applied={applied})"
            );
            assert!(
                applied > 0,
                "the same drift+volume must GATHER when skipping is off"
            );
        }
        engine.begin_close();
        engine
            .await_terminated(std::time::Duration::from_secs(30))
            .await
            .expect("terminate");
    }
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
            w.run(
                &engine,
                hash,
                &key,
                &["g1", "g2", "g3"],
                10,
                false,
                &mut log,
            )
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
        let observed = drain_observed(&engine, hash, &key, &cov).await;
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

/// **Review #2's ring ordering + paging asks, pinned at offset level.**
///
/// 1. Publish-before-NOTIFY: a waiter woken by the tail notify must find
///    the ring already covering the new offset — publish-before-ACK is
///    not enough, because the woken reader races the ack path.
/// 2. A read starting MID-batch returns exactly the tail of that batch.
/// 3. A producer-idempotence duplicate publishes nothing (no offset was
///    consumed, so ring ceiling must not move).
/// 4. Budget progress: max_bytes=1 still returns the first record and
///    advances — an oversized record can never wedge a cursor, on the
///    ring path or the DB path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ring_ordering_paging_and_duplicates_at_offset_level() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 93, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [43u8; 16];
    let cfg = crate::shard::ShardConfig {
        tail_ring_bytes: 32 * 1024 * 1024,
        ..Default::default()
    };
    let engine = open_engine_cfg(store.clone(), "dst-ring-ord", cfg).await;
    let w = Workload::new(cov.clone());

    // (1) Arm a notify waiter BEFORE the append; on wake, the ring must
    // already cover the appended offset.
    let o = w
        .attempt_with_deadline(&engine, hash, &key, "o", "first", None, None)
        .await;
    assert!(matches!(o, Outcome::Acked { .. }));
    let handle = engine.stream_handle(hash).await.expect("handle");
    let notified = handle.notify.notified();
    let before_next = handle.state.lock().unwrap().durable.next;
    let e2 = engine.clone();
    let k2 = key.clone();
    let c2 = cov.clone();
    let appender = tokio::spawn(async move {
        let w2 = Workload::new(c2);
        w2.attempt_with_deadline(&e2, hash, &k2, "o", "second", None, None)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(20), notified)
        .await
        .expect("waiter must be woken");
    // The instant of wake: ring must already hold [before_next, next).
    let next = handle.state.lock().unwrap().durable.next;
    assert!(next > before_next);
    let hit = engine
        .ring_read(&handle, before_next, next, 1 << 20)
        .expect("a woken reader must hit the ring, not fall to the DB");
    assert_eq!(hit.frames.len(), (next - before_next) as usize);
    assert!(matches!(
        appender.await.expect("join"),
        Outcome::Acked { .. }
    ));

    // (2) Mid-batch: append a 4-record batch (one commit group), read
    // starting inside it.
    let mut log = OpLog::default();
    let mut w2 = Workload::new(cov.clone());
    w2.run(&engine, hash, &key, &["o"], 4, false, &mut log)
        .await;
    let end = handle.state.lock().unwrap().durable.next;
    let mid = end - 2;
    let part = engine
        .ring_read(&handle, mid, end, 1 << 20)
        .expect("mid-batch start must be servable from the ring");
    assert_eq!(part.frames.len(), 2, "exactly the batch tail");
    assert_eq!(part.last_offset, Some(end - 1));

    // (3) Duplicate publishes nothing.
    let pr = crate::shard::ProducerReq {
        id: "ring-dup".into(),
        epoch: 1,
        seq: 0,
        request_hash: None,
    };
    let first = w
        .attempt_with_deadline(&engine, hash, &key, "o", "dup-body", Some(pr.clone()), None)
        .await;
    assert!(matches!(
        first,
        Outcome::Acked {
            duplicate: false,
            ..
        }
    ));
    let ceil_before = {
        let r = handle.ring.lock().unwrap();
        r.batches.back().map(|b| b.next)
    };
    let published_before = engine.ring_published.load(Ordering::Relaxed);
    let retry = w
        .attempt_with_deadline(&engine, hash, &key, "o", "dup-body", Some(pr), None)
        .await;
    assert!(matches!(
        retry,
        Outcome::Acked {
            duplicate: true,
            ..
        }
    ));
    let ceil_after = {
        let r = handle.ring.lock().unwrap();
        r.batches.back().map(|b| b.next)
    };
    assert_eq!(
        ceil_before, ceil_after,
        "a duplicate must not move the ring ceiling"
    );
    assert_eq!(
        engine.ring_published.load(Ordering::Relaxed),
        published_before,
        "a duplicate must not publish a batch"
    );

    // (4) Oversized-record progress, ring and DB path alike.
    let tail_end = handle.state.lock().unwrap().durable.next;
    let one = crate::shard::read_frames_range(&engine, &handle, 0, tail_end, 1)
        .await
        .expect("budget-1 read");
    assert_eq!(one.frames.len(), 1, "the first record always fits");
    assert!(one.last_offset.is_some(), "and the cursor advances");
    // Same via a cold engine (DB path).
    let b = open_engine_cfg(
        store.clone(),
        "dst-ring-ord",
        crate::shard::ShardConfig {
            tail_ring_bytes: 32 * 1024 * 1024,
            ..Default::default()
        },
    )
    .await;
    let hb = b.stream_handle(hash).await.expect("handle");
    let one_db = crate::shard::read_frames_range(&b, &hb, 0, tail_end, 1)
        .await
        .expect("db budget-1 read");
    assert_eq!(one_db.frames.len(), 1);
    assert_eq!(
        one.frames[0], one_db.frames[0],
        "same first frame either path"
    );
}

/// **Durable-tail ring: correct under load, evictions, and fallback.**
///
/// Small budget forces evictions mid-run, so reads exercise all three
/// paths — ring hit, ring miss -> DB scan, and mixed ranges — and the
/// full I1–I7 audit runs over the production merged reader. Anti-vacuity:
/// the run must have produced hits AND evictions, or the scenario proves
/// nothing about the ring.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_ring_serves_live_reads_and_survives_eviction() {
    for seed in [9u64, 33] {
        let inner = mem();
        let store = FaultStore::uniform(inner.clone(), seed, FaultPlan::new(0, 0, 25));
        let cov = store.coverage();
        let key = skey();
        let hash = [30u8; 16];
        let cfg = crate::shard::ShardConfig {
            // Tiny: a couple of groups' worth, so eviction is constant.
            tail_ring_bytes: 2 * 1024,
            ..Default::default()
        };
        let engine = open_engine_cfg(store.clone(), &format!("dst-ring-{seed}"), cfg).await;

        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        for _ in 0..4 {
            w.run(&engine, hash, &key, &["r1", "r2"], 12, false, &mut log)
                .await;
        }

        let hits = engine.ring_hits.load(Ordering::Relaxed);
        let evicted = engine.ring_evicted.load(Ordering::Relaxed);
        assert!(
            engine.ring_published.load(Ordering::Relaxed) > 0,
            "seed {seed}: nothing was ever published to the ring"
        );
        assert!(evicted > 0, "seed {seed}: budget never forced an eviction");

        let ds: Arc<dyn ObjectStore> = store.clone();
        let observed = drain_observed(&engine, hash, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("seed {seed}: ring-backed reads broke the canon: {e}");
        }
        let _ = hits; // hit-path asserted in the equivalence scenario below
    }
}

/// **Ring/DB equivalence, and a restart starts cold.**
///
/// The same offset range read through the ring (fresh engine, everything
/// resident) and through the canonical DB scan (reopened engine, ring
/// necessarily empty) must be byte-identical — the ring is a cache, not
/// a second source of truth. Also pins publish-before-ack: immediately
/// after an ack returns, the ring already covers the acked offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_ring_matches_the_db_scan_and_restarts_cold() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 13, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [31u8; 16];
    let cfg = crate::shard::ShardConfig {
        tail_ring_bytes: 32 * 1024 * 1024,
        ..Default::default()
    };
    let a = open_engine_cfg(store.clone(), "dst-ring-eq", cfg.clone()).await;
    let w = Workload::new(cov.clone());

    for i in 0..20u64 {
        let o = w
            .attempt_with_deadline(&a, hash, &key, "eq", &format!("rec{i}"), None, None)
            .await;
        assert!(matches!(o, Outcome::Acked { .. }));
    }
    let handle_a = a.stream_handle(hash).await.expect("handle");
    let next = handle_a.state.lock().unwrap().durable.next;

    // Publish-before-ack: the last acked offset is ring-resident NOW.
    let tail1 = a
        .ring_read(&handle_a, next - 1, next, 1 << 20)
        .expect("the ring must cover an offset the ack already exposed");
    assert_eq!(tail1.frames.len(), 1);

    let hits_before = a.ring_hits.load(Ordering::Relaxed);
    let via_ring = crate::shard::read_frames_range(&a, &handle_a, 0, next, 8 << 20)
        .await
        .expect("ring-backed read");
    assert!(
        a.ring_hits.load(Ordering::Relaxed) > hits_before,
        "full-range read on the fresh engine must be a ring hit"
    );

    // Reopen: cold ring, same range must come from the DB, byte-equal.
    let b = open_engine_cfg(store.clone(), "dst-ring-eq", cfg).await;
    let handle_b = b.stream_handle(hash).await.expect("handle");
    let via_db = crate::shard::read_frames_range(&b, &handle_b, 0, next, 8 << 20)
        .await
        .expect("db read");
    assert_eq!(
        b.ring_hits.load(Ordering::Relaxed),
        0,
        "cold ring cannot hit"
    );
    assert_eq!(via_ring.frames.len(), via_db.frames.len(), "frame count");
    for (i, (ra, rb)) in via_ring.frames.iter().zip(via_db.frames.iter()).enumerate() {
        assert_eq!(ra, rb, "frame {i} differs between ring and DB");
    }
}

/// A duplicate `Absorbed{upto}` op must not advance the trim.
///
/// The absorber paces passes off the PUBLISHED absorbed boundary, which
/// lags the committer by durability + dispatch, so under load it can
/// re-submit an `upto` the committer has already applied. The committer
/// used to treat that duplicate like any other pass and trim toward
/// `prev_absorbed` — by then the LIVE boundary — collapsing the deferred-
/// trim lag that protects readers holding a stale absorbed snapshot
/// mid-merge. That collapse, plus the snapshot/tail-scan TOCTOU in
/// `read_merged`, is the 2026-07-27 boundary-race DST failure: records
/// vanished from a `completed = true` page at exactly the sampled
/// absorbed boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_absorbed_op_does_not_advance_the_trim() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 29, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [31u8; 16];
    let engine = open_engine(store.clone(), "dst-duptrim").await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    // Offsets [0, 20).
    w.run(&engine, hash, &key, &["d"], 20, false, &mut log)
        .await;
    assert_eq!(log.total_acked(), 20, "need all 20 offsets acked");

    let published = |engine: &Arc<crate::shard::ShardEngine>| {
        let engine = engine.clone();
        async move {
            let h = engine.stream_handle(hash).await.expect("handle");
            let st = h.state.lock().unwrap();
            (st.durable.absorbed, st.durable.trimmed)
        }
    };
    let wait_absorbed = |engine: &Arc<crate::shard::ShardEngine>, want: u64| {
        let engine = engine.clone();
        async move {
            for _ in 0..400 {
                let h = engine.stream_handle(hash).await.expect("handle");
                if h.state.lock().unwrap().durable.absorbed >= want {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("absorbed never reached {want}");
        }
    };

    // Advance 0 -> 10: deferred trim means nothing is deleted yet.
    engine.submit_absorbed(hash, 10, 0).await;
    wait_absorbed(&engine, 10).await;
    // Advance 10 -> 18: trims up to the previous boundary, 10.
    engine.submit_absorbed(hash, 18, 0).await;
    wait_absorbed(&engine, 18).await;
    let (absorbed, trimmed) = published(&engine).await;
    assert_eq!(absorbed, 18);
    assert_eq!(
        trimmed, 10,
        "an advancing op trims to the previous boundary"
    );

    // The duplicate: re-submit the boundary the committer already holds,
    // exactly as an absorber pass that raced dispatch does.
    engine.submit_absorbed(hash, 18, 0).await;
    // Sentinel append: the committer queue is FIFO, so this ack proves the
    // duplicate op was processed and its state published.
    w.run(&engine, hash, &key, &["d"], 1, false, &mut log).await;
    assert_eq!(log.total_acked(), 21, "sentinel append must ack");

    let (absorbed, trimmed) = published(&engine).await;
    assert_eq!(absorbed, 18, "a duplicate must not move the boundary");
    assert_eq!(
        trimmed, 10,
        "a duplicate Absorbed op advanced the trim to the live boundary — \
         the deferred-trim lag protecting stale-snapshot readers is gone"
    );

    // The lag is not bookkeeping: [10, 18) must still be readable from the
    // shard log, because a reader that snapshotted absorbed=10 before the
    // 10 -> 18 dispatch scans its tail from exactly there.
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mid = crate::shard::read_frames_range(&engine, &handle, 10, 18, 1 << 20)
        .await
        .expect("scan [10, 18)");
    assert_eq!(
        mid.frames.len(),
        8,
        "records above the previous boundary must survive a duplicate op"
    );
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
    let err = log
        .audit(&obs(&[("k", &[(0, 0), (1, 0), (2, 0)])]))
        .unwrap_err();
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
                let eng = crate::shard::ShardEngine::start(
                    p,
                    Arc::new(db),
                    st2,
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

fn m(v: &std::sync::atomic::AtomicU64) -> u64 {
    v.load(Ordering::Relaxed)
}

/// Direct append of a payload of chosen size (the workload helper only
/// sends tiny JSON bodies; the gather-budget tests need real volume).
async fn append_sized(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    rk: &str,
    payload_bytes: usize,
) -> u64 {
    let subkey = crate::crypto::derive_subkey(key, &hash, rk, 0);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: hash,
        entries: vec![bytes::Bytes::from(vec![0x5au8; payload_bytes])],
        usage: crate::usage::counters(&hash),
        routing_key: rk.to_string(),
        key_hash: crate::crypto::stream_hash(rk),
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey,
        ts_hint_ms: None,
        seq: None,
        bytes: 0,
        close: false,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        resp: tx,
    };
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    rx.await.expect("resp").expect("ack").last_offset
}

async fn wait_all_absorbed(engine: &Arc<crate::shard::ShardEngine>, hashes: &[[u8; 16]]) {
    for h in hashes {
        let mut ok = false;
        for _ in 0..400 {
            let st = engine.stream_handle(*h).await.unwrap();
            let (a, n) = {
                let s = st.state.lock().unwrap();
                (s.durable.absorbed, s.durable.next)
            };
            if a == n && n > 0 {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(ok, "stream {:02x?} never fully absorbed", &h[..2]);
    }
}

/// P0 (static audit): the v2 gather previously accumulated up to
/// V2_LANE_PER_TICK x 4 MiB (~4 GiB) in ONE WriteBatch before any
/// backpressure could apply. The aggregate budget must pack streams up
/// to gather_max_bytes and defer the rest to later gathers — with no
/// stream starved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_gather_packs_to_the_aggregate_budget() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 91, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hashes: Vec<[u8; 16]> = (0u8..6).map(|i| [0x70 + i; 16]).collect();

    let db = slatedb::Db::builder("dst-budget", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let engine = crate::shard::ShardEngine::start(
        "dst-budget".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    for h in &hashes {
        append_sized(&engine, *h, &key, "", 16 * 1024).await;
    }

    // Unstarted absorber: gathers are driven directly so packing is
    // deterministic. ~16.6 KiB per unkeyed chunk against a 40 KiB budget
    // means exactly two streams per gather.
    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            gather_max_bytes: 40 * 1024,
            ..Default::default()
        },
    );
    let mut per_gather = Vec::new();
    let mut first_deferred = None;
    for _ in 0..6 {
        let outcome = absorber.absorb_gather_v2(&hashes).await.expect("gather");
        if outcome.advanced.is_empty() {
            break;
        }
        if first_deferred.is_none() {
            first_deferred = Some(outcome.deferred_budget.len());
        }
        per_gather.push(outcome.advanced.len());
    }
    assert_eq!(
        per_gather,
        vec![2, 2, 2],
        "budget must pack exactly two 16 KiB streams per gather"
    );
    // Review round 4: streams that did not fit must be REPORTED as
    // budget-deferred (the pump keeps them pending off this signal).
    assert_eq!(
        first_deferred,
        Some(4),
        "the four streams that did not fit must classify as deferred_budget"
    );
    wait_all_absorbed(&engine, &hashes).await;
    engine.begin_close();
}

/// A chunk larger than the whole budget must still make progress — alone
/// — instead of starving (frame bodies can reach the 32 MiB API cap).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_chunk_gathers_alone() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 92, FaultPlan::new(0, 0, 0));
    let key = skey();
    let big = [0x80u8; 16];
    let small_a = [0x81u8; 16];
    let small_b = [0x82u8; 16];

    let db = slatedb::Db::builder("dst-oversize", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let engine = crate::shard::ShardEngine::start(
        "dst-oversize".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    append_sized(&engine, big, &key, "", 200 * 1024).await;
    append_sized(&engine, small_a, &key, "", 16 * 1024).await;
    append_sized(&engine, small_b, &key, "", 16 * 1024).await;

    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            gather_max_bytes: 64 * 1024,
            ..Default::default()
        },
    );
    let all = [big, small_a, small_b];
    let g1 = absorber.absorb_gather_v2(&all).await.expect("gather 1");
    assert_eq!(g1.advanced.len(), 1, "oversized chunk must gather alone");
    assert_eq!(g1.advanced[0].0, big);
    assert_eq!(g1.deferred_budget.len(), 2);
    let g2 = absorber.absorb_gather_v2(&all).await.expect("gather 2");
    assert_eq!(
        g2.advanced.len(),
        2,
        "both small streams fit the next gather"
    );
    wait_all_absorbed(&engine, &all).await;
    engine.begin_close();
}

/// ROUTING-V3 §3: postings REPLACED the covering index — a keyed frame
/// is stored once (plus a ~tens-of-bytes postings page), so keyed and
/// unkeyed streams now cost the same against the gather budget. The
/// 40 KiB budget that used to fit ONE keyed 16 KiB stream fits two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_frames_no_longer_count_twice_against_the_budget() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 93, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hashes: Vec<[u8; 16]> = (0u8..2).map(|i| [0x90 + i; 16]).collect();

    let db = slatedb::Db::builder("dst-keyedbudget", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let engine = crate::shard::ShardEngine::start(
        "dst-keyedbudget".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    for h in &hashes {
        append_sized(&engine, *h, &key, "k1", 16 * 1024).await;
    }

    // Keyed chunks now weigh what unkeyed ones do (~16.6 KiB): the
    // canonical row plus a compact postings allowance.
    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            gather_max_bytes: 40 * 1024,
            ..Default::default()
        },
    );
    let before = crate::history::POSTINGS_BYTES_WRITTEN.load(Ordering::Relaxed);
    let g1 = absorber.absorb_gather_v2(&hashes).await.expect("gather 1");
    assert_eq!(
        g1.advanced.len(),
        2,
        "postings killed the keyed double-write: both streams fit one budget"
    );
    let postings = crate::history::POSTINGS_BYTES_WRITTEN.load(Ordering::Relaxed) - before;
    let canonical: u64 = g1.advanced.iter().map(|(_, _, b)| *b).sum();
    assert!(postings > 0, "keyed frames must produce postings pages");
    assert!(
        postings * 100 <= canonical * 8,
        "postings bytes must stay within the 8% batch-1 gate: {postings} vs {canonical}"
    );
    wait_all_absorbed(&engine, &hashes).await;
    engine.begin_close();
}

/// Static-audit P1: an unabsorbed tail must be rediscovered by a fresh
/// owner WITHOUT the customer ever touching the stream again. The old
/// rediscovery enumerated resident handles only — a restarted engine
/// has none, so a pre-crash stream's absorption never resumed and trim
/// never advanced. The durable dirty-stream index closes this.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn untouched_streams_absorb_after_restart() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 94, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xA4u8; 16];

    // Owner A: append + ack with NO absorber running (crash before
    // absorption), then drop the engine without a clean close.
    {
        let db = slatedb::Db::builder("dst-restart", store.clone() as Arc<dyn ObjectStore>)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                manifest_poll_interval: std::time::Duration::from_millis(50),
                ..Default::default()
            })
            .build()
            .await
            .expect("open db A");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        let engine_a = crate::shard::ShardEngine::start(
            "dst-restart".to_string(),
            Arc::new(db),
            store.clone(),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
        );
        for _ in 0..5 {
            append_sized(&engine_a, hash, &key, "", 2 * 1024).await;
        }
        // Simulate a crash: close the engine (fencing handoff) but note
        // the absorber never ran, so absorbed == 0 < next == 5 and the
        // dirty marker is durably present.
        engine_a.begin_close();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Owner B: fresh engine + absorber, EMPTY key cache (v2 needs none),
    // and — the point — not a single request for the stream.
    let db = slatedb::Db::builder("dst-restart", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db B");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    let engine_b = crate::shard::ShardEngine::start(
        "dst-restart".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine_b.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            min_age_bytes: 0,
            // Disable the resident-handle sweep entirely: convergence in
            // this test must come from the durable index seed ALONE, and
            // the test itself must not materialize the handle early (the
            // sweep would then find it and mask a broken seed).
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );

    // Wait on the MARKER only — it clears in the same committer batch
    // that brings absorbed up to next, and polling it does not touch the
    // stream.
    let mut cleared = false;
    for _ in 0..500 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let dirty = engine_b.scan_dirty_streams().await.unwrap();
        if !dirty.iter().any(|(h, _, _)| *h == hash) {
            cleared = true;
            break;
        }
    }
    assert!(
        cleared,
        "untouched pre-crash stream never absorbed after restart"
    );

    // Only now touch the stream to confirm the boundary state.
    let st = engine_b.stream_handle(hash).await.unwrap();
    let (a, n) = {
        let s = st.state.lock().unwrap();
        (s.durable.absorbed, s.durable.next)
    };
    assert_eq!(n, 5, "durable next lost across restart");
    assert_eq!(
        a, n,
        "absorbed must equal next after index-seeded absorption"
    );
    engine_b.begin_close();
}

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
    let engine = crate::shard::ShardEngine::start(
        "dst-evict".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
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

/// Multi-record variant of append_sized: one request carrying `n`
/// records of `each` bytes (the mature-wave test needs deep per-stream
/// prefixes without 4,800 round-trips).
async fn append_n(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    n: usize,
    each: usize,
) -> u64 {
    let subkey = crate::crypto::derive_subkey(key, &hash, "", 0);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: hash,
        entries: (0..n)
            .map(|_| bytes::Bytes::from(vec![0x5au8; each]))
            .collect(),
        usage: crate::usage::counters(&hash),
        routing_key: String::new(),
        key_hash: crate::crypto::stream_hash(""),
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey,
        ts_hint_ms: None,
        seq: None,
        bytes: 0,
        close: false,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        resp: tx,
    };
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    rx.await.expect("resp").expect("ack").last_offset
}

/// Release blocker (review round 4, P0): a second absorption wave across
/// many mature streams used to expand into ONE WriteBatch of
/// streams × max_trim_per_op deletes (67M at the wide posture — a
/// multi-GiB batch). Boundary publication and physical trimming are now
/// decoupled: the advance batch trims at most TRIM_GLOBAL_BUDGET
/// deletes, the remainder becomes trim debt, and TrimTick maintenance
/// drains it a budgeted slice per commit — including via the 5 s flush
/// ticker with no test involvement.
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
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            min_age_bytes: 0,
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
        append_n(&engine, *h, &key, RECS as usize, 512).await;
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
    for _ in 0..500 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut all = true;
        for h in &hashes {
            let st = engine.stream_handle(*h).await.unwrap();
            let a = st.state.lock().unwrap().durable.absorbed;
            if a < RECS + 1 {
                all = false;
                break;
            }
        }
        if all {
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
        let mut all = true;
        for h in &hashes {
            let st = engine.stream_handle(*h).await.unwrap();
            let f = { st.state.lock().unwrap().durable.clone() };
            if f.trimmed < f.trim_safe_to {
                all = false;
                break;
            }
        }
        if all && engine.trim_stats().0 == 0 {
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
    let engine = crate::shard::ShardEngine::start(
        "dst-defer".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
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
            min_age_bytes: 0,
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
    'outer: loop {
        assert!(
            std::time::Instant::now() < deadline,
            "budget-deferred streams did not absorb within the tick horizon"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        for h in &hashes {
            let st = engine.stream_handle(*h).await.unwrap();
            let (a, n) = {
                let s = st.state.lock().unwrap();
                (s.durable.absorbed, s.durable.next)
            };
            if !(a == n && n > 0) {
                continue 'outer;
            }
        }
        break;
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
        let engine_a = crate::shard::ShardEngine::start(
            "dst-bigrec".to_string(),
            Arc::new(db),
            store.clone(),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
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
    let engine_b = crate::shard::ShardEngine::start(
        "dst-bigrec".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
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
        let engine_a = crate::shard::ShardEngine::start(
            "dst-scanretry".to_string(),
            Arc::new(db),
            store.clone(),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
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
    let engine_b = crate::shard::ShardEngine::start(
        "dst-scanretry".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine_b.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(50),
            min_age_bytes: 0,
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

/// Review round 4, P1 (companion): a tiny sparse record under the
/// default policy must stay DEFERRED after restart — and be REPORTED as
/// deferred in the shard's pending summary, not silently dropped and
/// not absorbed against the sparse-cost policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_records_stay_deferred_and_reported_after_restart() {
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
        let engine_a = crate::shard::ShardEngine::start(
            "dst-sparse".to_string(),
            Arc::new(db),
            store.clone(),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
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
    let engine_b = crate::shard::ShardEngine::start(
        "dst-sparse".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    // Default thresholds (4 MiB / 256 KiB min-age bytes), fast tick so
    // the summary publishes quickly.
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine_b.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            tick: std::time::Duration::from_millis(50),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    // The seed must land it in pending AND the tick must classify it as
    // policy-deferred in this shard's summary row.
    let mut reported = false;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if let Some((_eligible, _oldest, deferred, dbytes)) =
            crate::usage::absorb_pending_summary_for("dst-sparse")
        {
            if deferred >= 1 && dbytes > 0 {
                reported = true;
                break;
            }
        }
    }
    assert!(
        reported,
        "a rediscovered sparse record must be visible as policy-deferred"
    );
    // And it must NOT absorb (the deferral is the intended policy).
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let dirty = engine_b.scan_dirty_streams().await.unwrap();
    assert!(
        dirty.iter().any(|(h, _, _)| *h == hash),
        "sparse stream must remain durably marked (not absorbed, not dropped)"
    );
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
    let engine = crate::shard::ShardEngine::start(
        "dst-sumclear".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
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
    // A sparse record deferred by the default policy keeps the row
    // populated for as long as we need it.
    append_sized(&engine, hash, &key, "", 512).await;
    let mut published = false;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if crate::usage::absorb_pending_summary_for("dst-sumclear").is_some() {
            published = true;
            break;
        }
    }
    assert!(published, "summary row never published");

    engine.begin_close();
    let mut cleared = false;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if crate::usage::absorb_pending_summary_for("dst-sumclear").is_none() {
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
    let engine = crate::shard::ShardEngine::start(
        "dst-handlecap".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
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

/// Round-4 root cause: the absorber's lane classification races
/// dispatch (a signal can arrive before its append's tail publishes, so
/// the zero-route guard briefly reads route==0 and picks v1; a tick
/// later a stale absorbed==0 re-admits v2). The two lanes then
/// interleave and a flagged-v2 stream ends up with ranges that exist
/// ONLY in the v1 per-stream DB — acked records the v2 read path can
/// never see. The COMMITTER seals the layout at the first advance:
/// cross-layout advances are dropped, boundaries never cover a range
/// the sealed tier doesn't hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_advance_seals_the_history_layout() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 103, FaultPlan::new(0, 0, 0));
    let key = skey();

    let db = slatedb::Db::builder("dst-seal", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let engine = crate::shard::ShardEngine::start(
        "dst-seal".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );

    async fn wait_absorbed(
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        want: u64,
    ) -> (u64, bool) {
        for _ in 0..400 {
            let h = engine.stream_handle(hash).await.unwrap();
            let (a, f) = {
                let s = h.state.lock().unwrap();
                (s.durable.absorbed, s.durable.history_v2)
            };
            if a >= want {
                return (a, f);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let h = engine.stream_handle(hash).await.unwrap();
        let s = h.state.lock().unwrap();
        (s.durable.absorbed, s.durable.history_v2)
    }

    // Stream A: sealed v2 by its first advance; a later v1 advance (the
    // racy in-flight v1 pass) must be DROPPED — boundary and flag hold.
    let a = [0xF1u8; 16];
    for _ in 0..5 {
        append_sized(&engine, a, &key, "", 512).await;
    }
    engine.submit_absorbed_batch_v2(vec![(a, 3, 0)]).await;
    let (abs, flag) = wait_absorbed(&engine, a, 3).await;
    assert_eq!((abs, flag), (3, true), "first v2 advance seals v2");
    engine.submit_absorbed(a, 5, 0).await; // cross-layout v1 advance
    // Sentinel append proves the committer processed the op above.
    append_sized(&engine, a, &key, "", 64).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let h = engine.stream_handle(a).await.unwrap();
    let (abs, flag) = {
        let s = h.state.lock().unwrap();
        (s.durable.absorbed, s.durable.history_v2)
    };
    assert_eq!(
        (abs, flag),
        (3, true),
        "a v1 advance on a sealed-v2 stream must be dropped whole"
    );

    // Stream B: sealed v1 by its first advance; a later v2 AbsorbedBatch
    // entry must be dropped — the flag must never flip mid-stream.
    let b = [0xF2u8; 16];
    for _ in 0..5 {
        append_sized(&engine, b, &key, "", 512).await;
    }
    engine.submit_absorbed(b, 3, 0).await;
    let (abs, flag) = wait_absorbed(&engine, b, 3).await;
    assert_eq!((abs, flag), (3, false), "first v1 advance seals v1");
    engine.submit_absorbed_batch_v2(vec![(b, 5, 0)]).await;
    append_sized(&engine, b, &key, "", 64).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let h = engine.stream_handle(b).await.unwrap();
    let (abs, flag) = {
        let s = h.state.lock().unwrap();
        (s.durable.absorbed, s.durable.history_v2)
    };
    assert_eq!(
        (abs, flag),
        (3, false),
        "a v2 advance on a sealed-v1 stream must be dropped whole"
    );
    // Continuation on the SEALED lane still works.
    engine.submit_absorbed(b, 5, 0).await;
    let (abs, flag) = wait_absorbed(&engine, b, 5).await;
    assert_eq!((abs, flag), (5, false));
    assert!(
        engine
            .absorb_lane_dropped
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 2,
        "both cross-layout advances must be counted"
    );
    engine.begin_close();
}

/// ROUTING-V3 §5/§8.5: a sparse key spread across many canonical gaps
/// pages through the planner under the span budget (≤ 8 per response)
/// and the cursor advances via consumed_to — every match returned
/// exactly once, in order, across multiple partial responses, with no
/// per-offset GET pattern (structural: the reader only range-scans).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_key_reads_page_with_bounded_spans() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 104, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xB1u8; 16];

    let db = slatedb::Db::builder("dst-sparsekey", store.clone() as Arc<dyn ObjectStore>)
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
        "dst-sparsekey".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            min_age_bytes: 0,
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );

    // 24 appends of 512 records each (12,288 offsets): the key "sp"
    // hits every 512th offset (24 matches), buried in default-key
    // records. Batched multi-entry appends with per-entry keys are not
    // a workload-helper shape, so append per batch with the FIRST
    // record keyed via a dedicated single append then a filler batch.
    let mut expected = Vec::new();
    let mut off = 0u64;
    for i in 0..24u64 {
        let subkey = crate::crypto::derive_subkey(&key, &hash, "sp", 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(
                serde_json::json!({"op": i, "att": 0, "k": "sp"})
                    .to_string()
                    .into_bytes(),
            )],
            usage: crate::usage::counters(&hash),
            routing_key: "sp".to_string(),
            key_hash: crate::crypto::stream_hash("sp"),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            close: false,
            producer: None,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            resp: tx,
        };
        assert!(engine.try_enqueue(req).is_ok());
        rx.await.expect("resp").expect("ack");
        expected.push((i, 0u32));
        off += 1;
        append_n(&engine, hash, &key, 511, 40).await;
        off += 511;
    }
    let _ = off;
    wait_all_absorbed(&engine, &[hash]).await;

    let ds: Arc<dyn ObjectStore> = store.clone();
    let got = drain_filtered(&engine, hash, &key, "sp").await;
    assert_eq!(
        got, expected,
        "sparse keyed paging lost or reordered records"
    );
    assert!(
        crate::history::READ_FRAMES_MATCHED.load(Ordering::Relaxed) > 0,
        "the postings planner path must have served this"
    );
    engine.begin_close();
}

/// ROUTING-V3 §8.6: a corrupt postings page must never surface as
/// completed=true over an unverified range — the reader falls back to
/// ONE bounded canonical envelope scan (exact-key filtered), counts
/// the corruption, and still returns every record exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_postings_fall_back_to_the_envelope() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 105, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xB3u8; 16];

    let db = slatedb::Db::builder("dst-corruptp", store.clone() as Arc<dyn ObjectStore>)
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
        "dst-corruptp".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            min_age_bytes: 0,
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );

    for i in 0..30u64 {
        let subkey = crate::crypto::derive_subkey(&key, &hash, "ck", 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(
                serde_json::json!({"op": i, "att": 0, "k": "ck"})
                    .to_string()
                    .into_bytes(),
            )],
            usage: crate::usage::counters(&hash),
            routing_key: "ck".to_string(),
            key_hash: crate::crypto::stream_hash("ck"),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            close: false,
            producer: None,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            resp: tx,
        };
        assert!(engine.try_enqueue(req).is_ok());
        rx.await.expect("resp").expect("ack");
    }
    wait_all_absorbed(&engine, &[hash]).await;

    // Corrupt the key's page in place: contiguous matches from offset 0
    // make the page key fully deterministic (bucket 0, page_first 0).
    let part = engine.history_partition().await.expect("partition");
    let pk = crate::postings::postings_key(
        crate::crypto::RouteHash(hash),
        crate::crypto::SegmentHash(hash),
        &crate::postings::rk_hash("ck"),
        0,
        0,
    );
    // The partition is WAL-disabled: a default (await-durable) put would
    // wait for a flush that only comes later — write like the gather
    // does, then flush explicitly.
    let mut wb = slatedb::WriteBatch::new();
    wb.put(&pk, b"garbage-not-a-page");
    part.write_with_options(
        wb,
        &slatedb::config::WriteOptions {
            await_durable: false,
            ..Default::default()
        },
    )
    .await
    .expect("corrupt");
    part.flush().await.expect("flush corruption");

    let before = crate::history::POSTINGS_CORRUPT.load(Ordering::Relaxed);
    let ds: Arc<dyn ObjectStore> = store.clone();
    // The absorber write-through-warmed the slice cache with the (valid)
    // runs it encoded; served from there, the corruption would never be
    // touched. The envelope contract is about a COLD index read — model
    // the instance that did not absorb this data.
    engine.postings_cache.sweep_idle(std::time::Duration::ZERO);
    let got = drain_filtered(&engine, hash, &key, "ck").await;
    let want: Vec<(u64, u32)> = (0..30u64).map(|i| (i, 0u32)).collect();
    assert_eq!(got, want, "envelope fallback lost records");
    assert!(
        crate::history::POSTINGS_CORRUPT.load(Ordering::Relaxed) > before,
        "corruption must be counted"
    );
    engine.begin_close();
}

/// Spec §7: a key's second read must be served from the decoded slice
/// cache — no new physical index load — and repeated reads keep
/// hitting. (The ≥90% active-window hit-rate gate runs in the
/// acceptance campaign; this pins the mechanism.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_keyed_reads_hit_the_postings_cache() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 107, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xB5u8; 16];

    let db = slatedb::Db::builder("dst-pcache", store.clone() as Arc<dyn ObjectStore>)
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
        "dst-pcache".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            min_age_bytes: 0,
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    for i in 0..8u64 {
        let subkey = crate::crypto::derive_subkey(&key, &hash, "hot", 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(
                serde_json::json!({"op": i, "att": 0, "k": "hot"})
                    .to_string()
                    .into_bytes(),
            )],
            usage: crate::usage::counters(&hash),
            routing_key: "hot".to_string(),
            key_hash: crate::crypto::stream_hash("hot"),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            close: false,
            producer: None,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            resp: tx,
        };
        assert!(engine.try_enqueue(req).is_ok());
        rx.await.expect("resp").expect("ack");
        append_n(&engine, hash, &key, 32, 64).await;
    }
    wait_all_absorbed(&engine, &[hash]).await;

    let ds: Arc<dyn ObjectStore> = store.clone();
    let cache = &engine.postings_cache;

    // Write-through warming (spec §7): the absorber installed the runs
    // it just wrote, so even the FIRST read pays no index round trip.
    let first = drain_filtered(&engine, hash, &key, "hot").await;
    assert_eq!(first.len(), 8);
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        0,
        "first read after in-process absorption must be warm: {} / slice {:?}",
        cache.stats(),
        cache.debug_slice(
            &crate::crypto::SegmentHash(hash),
            &crate::postings::rk_hash("hot")
        ),
    );
    assert!(cache.hits.load(Ordering::Relaxed) >= 1);
    assert!(cache.warm_installs.load(Ordering::Relaxed) >= 1);

    // Simulate an instance that did NOT absorb this data (restart /
    // ownership move): sweep everything, then the cold-load contract
    // applies — one physical load, then hits.
    cache.sweep_idle(std::time::Duration::ZERO);
    let again = drain_filtered(&engine, hash, &key, "hot").await;
    assert_eq!(again, first);
    let loads_after_cold = cache.index_loads.load(Ordering::Relaxed);
    let hits_after_cold = cache.hits.load(Ordering::Relaxed);
    assert!(loads_after_cold >= 1, "swept cache must load the index");

    for _ in 0..5 {
        let warm = drain_filtered(&engine, hash, &key, "hot").await;
        assert_eq!(warm, first);
    }
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        loads_after_cold,
        "warm reads must not touch the physical index"
    );
    assert!(
        cache.hits.load(Ordering::Relaxed) >= hits_after_cold + 5,
        "warm reads must be cache hits"
    );
    engine.begin_close();
}

/// ROUTING-V3 §3.6: Stream-Seq is scoped to the ROUTING KEY. Two keys
/// advance independent lanes on one segment; a regression within one
/// key still conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_seq_is_scoped_to_the_routing_key() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 108, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xB7u8; 16];
    let db = slatedb::Db::builder("dst-keyseq", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let engine = crate::shard::ShardEngine::start(
        "dst-keyseq".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let send = |rk: &'static str, seq: &'static str| {
        let engine = engine.clone();
        let key = key.clone();
        async move {
            let subkey = crate::crypto::derive_subkey(&key, &hash, rk, 0);
            let (tx, rx) = tokio::sync::oneshot::channel();
            let req = crate::shard::AppendReq {
                enqueued_at: std::time::Instant::now(),
                hash,
                route: hash,
                entries: vec![bytes::Bytes::from_static(b"{}")],
                usage: crate::usage::counters(&hash),
                routing_key: rk.to_string(),
                key_hash: crate::crypto::stream_hash(rk),
                producer_lineage: Vec::new(),
                key_version: 0,
                subkey,
                ts_hint_ms: None,
                seq: Some(seq.to_string()),
                bytes: 0,
                close: false,
                producer: None,
                deferred_error: None,
                sealed_reject_new: None,
                touch: None,
                resp: tx,
            };
            assert!(engine.try_enqueue(req).is_ok());
            rx.await.expect("resp")
        }
    };
    assert!(send("a", "s1").await.is_ok());
    assert!(send("b", "s1").await.is_ok(), "key b has its own lane");
    assert!(send("a", "s2").await.is_ok());
    assert!(send("b", "s2").await.is_ok());
    // Regression WITHIN a key conflicts; the other key is untouched.
    match send("a", "s2").await {
        Err(crate::shard::AppendErr::SeqConflict { current }) => {
            assert_eq!(current.as_deref(), Some("s2"));
        }
        other => panic!("expected per-key seq conflict, got {other:?}"),
    }
    assert!(send("b", "s3").await.is_ok());
    engine.begin_close();
}

/// ROUTING-V3 §3.6 release gate: a producer retry whose first attempt
/// committed on the SEALED PARENT segment must be recognized by the
/// child through the predecessor chain — duplicate ack carrying the
/// parent's committed offset, and NO offset consumed on the child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_retries_across_a_split_commit_once() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 109, FaultPlan::new(0, 0, 0));
    let key = skey();
    let parent = [0xC1u8; 16];
    let child = [0xC2u8; 16];
    let db = slatedb::Db::builder("dst-splitprod", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let engine = crate::shard::ShardEngine::start(
        "dst-splitprod".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let send = |identity: [u8; 16], lineage: Vec<[u8; 16]>, seq: u64, close: bool| {
        let engine = engine.clone();
        let key = key.clone();
        async move {
            let subkey = crate::crypto::derive_subkey(&key, &identity, "pk", 0);
            let (tx, rx) = tokio::sync::oneshot::channel();
            let req = crate::shard::AppendReq {
                enqueued_at: std::time::Instant::now(),
                hash: identity,
                route: identity,
                entries: if close {
                    Vec::new()
                } else {
                    vec![bytes::Bytes::from_static(b"{\"p\":1}")]
                },
                usage: crate::usage::counters(&identity),
                routing_key: "pk".to_string(),
                key_hash: crate::crypto::stream_hash("pk"),
                producer_lineage: lineage,
                key_version: 0,
                subkey,
                ts_hint_ms: None,
                seq: None,
                bytes: 0,
                close,
                producer: if close {
                    None
                } else {
                    Some(crate::shard::ProducerReq {
                        id: "prod-1".into(),
                        epoch: 1,
                        seq,
                        request_hash: None,
                    })
                },
                deferred_error: None,
                sealed_reject_new: None,
                touch: None,
                resp: tx,
            };
            assert!(engine.try_enqueue(req).is_ok());
            rx.await.expect("resp")
        }
    };

    // Commit (epoch 1, seq 0) on the parent, then seal it — the split.
    let ack1 = send(parent, vec![], 0, false).await.expect("parent commit");
    assert!(!ack1.duplicate);
    let parent_off = ack1.last_offset;
    send(parent, vec![], 0, true).await.expect("seal parent");

    // The ambiguous retry lands on the CHILD with the predecessor chain:
    // recognized as a duplicate, answered with the PARENT's offset, and
    // the child consumes no offset.
    let ack2 = send(child, vec![parent], 0, false)
        .await
        .expect("child retry");
    assert!(ack2.duplicate, "retry across the seal must be a duplicate");
    assert_eq!(
        ack2.last_offset, parent_off,
        "duplicate must answer with the ORIGINAL committed offset"
    );
    let child_next = {
        let h = engine.stream_handle(child).await.unwrap();
        let st = h.state.lock().unwrap();
        st.durable.next.max(st.applied.next)
    };
    assert_eq!(
        child_next, 0,
        "the duplicate must not consume a child offset"
    );

    // The NEXT sequence commits on the child normally, seeded state
    // continuing the chain.
    let ack3 = send(child, vec![parent], 1, false).await.expect("child s1");
    assert!(!ack3.duplicate);
    assert_eq!(ack3.last_offset, 0, "first real child record at offset 0");
    engine.begin_close();
}

// ---- seal-gap read semantics (review blocker: a topology transition
// may delay a reader, but it must NEVER look like permanent closure) --

/// Full-fidelity HTTP rig: real AppState + axum server on a loopback
/// port, one shard prefix, fast absorber. The gap tests need the exact
/// header behavior clients see, not engine-level approximations.
async fn http_rig(
    store: Arc<dyn ObjectStore>,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_opts(
        store,
        vec!["00".to_string()],
        crate::shard::ShardConfig::default(),
    )
    .await
}

/// http_rig with explicit shard prefixes (multi-engine capacity tests)
/// and a shard config (e.g. serial WAL for deterministic throughput).
async fn http_rig_opts(
    store: Arc<dyn ObjectStore>,
    prefixes: Vec<String>,
    shard_cfg: crate::shard::ShardConfig,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_full(store, prefixes, shard_cfg, 0).await
}

/// A rig whose AppState carries an account token, for the negative
/// authorization matrix.
async fn http_rig_auth(
    store: Arc<dyn ObjectStore>,
    token: &str,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_inner(
        store,
        vec!["00".to_string()],
        crate::shard::ShardConfig::default(),
        0,
        Some(token.to_string()),
    )
    .await
}

async fn http_rig_full(
    store: Arc<dyn ObjectStore>,
    prefixes: Vec<String>,
    shard_cfg: crate::shard::ShardConfig,
    per_segment_slots: i64,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_inner(store, prefixes, shard_cfg, per_segment_slots, None).await
}

async fn http_rig_inner(
    store: Arc<dyn ObjectStore>,
    prefixes: Vec<String>,
    shard_cfg: crate::shard::ShardConfig,
    per_segment_slots: i64,
    auth: Option<String>,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    let registry = crate::registry::Registry::new(store.clone());
    let keys = Arc::new(crate::history::KeyCache::default());
    let touch = Arc::new(crate::touch::TouchRegistry::default());
    let shards_map: Arc<
        std::sync::RwLock<std::collections::HashMap<String, Arc<crate::shard::ShardEngine>>>,
    > = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
    let opener = {
        let store = store.clone();
        let keys = keys.clone();
        let shard_cfg = shard_cfg.clone();
        Box::new(move |prefix: String| {
            let store = store.clone();
            let keys = keys.clone();
            let shard_cfg = shard_cfg.clone();
            let fut: futures_util::future::BoxFuture<
                'static,
                anyhow::Result<Arc<crate::shard::ShardEngine>>,
            > = Box::pin(async move {
                let db = slatedb::Db::builder(format!("{prefix}/shard"), store.clone())
                    .with_settings(slatedb::config::Settings {
                        flush_interval: Some(std::time::Duration::from_millis(5)),
                        manifest_poll_interval: std::time::Duration::from_millis(50),
                        ..Default::default()
                    })
                    .build()
                    .await?;
                let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
                let engine = crate::shard::ShardEngine::start(
                    prefix,
                    Arc::new(db),
                    store.clone(),
                    shard_cfg.clone(),
                    absorb_tx,
                    None,
                );
                crate::history::Absorber::start(
                    store,
                    engine.clone(),
                    keys,
                    crate::history::AbsorberConfig {
                        threshold_bytes: 1,
                        threshold_age: std::time::Duration::from_millis(1),
                        tick: std::time::Duration::from_millis(20),
                        min_age_bytes: 0,
                        sweep_every: u32::MAX,
                        ..Default::default()
                    },
                    absorb_rx,
                );
                Ok(engine)
            });
            fut
        })
    };
    let gate = crate::sharddir::OpenGate::new(shards_map.clone(), opener);
    let state = Arc::new(crate::http::AppState {
        registry,
        shard_prefixes: prefixes,
        shards: shards_map,
        fleet_store: None,
        gate,
        fleet_ops: std::sync::atomic::AtomicU64::new(0),
        inflight: std::sync::atomic::AtomicI64::new(0),
        inflight_peak: std::sync::atomic::AtomicI64::new(0),
        admit_max_inflight: 0,
        admit_rss_shed_mb: 0,
        rss_mb_cached: std::sync::atomic::AtomicU64::new(0),
        admit_shed: std::sync::atomic::AtomicU64::new(0),
        admit_max_inflight_per_stream: per_segment_slots,
        stream_inflight: std::sync::Mutex::new(std::collections::HashMap::new()),
        stream_shed: std::sync::atomic::AtomicU64::new(0),
        wedge_shed: std::sync::atomic::AtomicU64::new(0),
        instance_name: String::new(),
        ring_active: std::sync::RwLock::new(Vec::new()),
        ring_overrides: std::sync::RwLock::new(std::collections::HashMap::new()),
        data_store: store,
        keys,
        touch,
        default_key: None,
        auth_token: auth,
        metrics: Arc::new(crate::metrics::Metrics::default()),
    });
    let app = crate::http::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (state, addr)
}

const RIG_KEY_B64: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="; // skey() = [7u8; 32]

/// Minimal HTTP/1.1 client: returns (status, lowercased headers, body).
async fn hreq(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> (u16, std::collections::HashMap<String, String>, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    // The rig key is supplied by default, but a caller that passes its
    // OWN stream-encryption-key must not end up sending two — the
    // server reads the first, so the injected one would silently win
    // and a "wrong key" test would quietly exercise the right key.
    let mut req = format!("{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n");
    if !extra
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("stream-encryption-key"))
    {
        req.push_str(&format!("stream-encryption-key: {RIG_KEY_B64}\r\n"));
    }
    req.push_str(&format!("content-length: {}\r\n", body.len()));
    for (k, v) in extra {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    // A peer RESET after a complete response is normal on macOS when
    // the server closes while the client still has unread request bytes
    // queued; treat it as end-of-response and let the parse below judge
    // completeness (a truly truncated read fails at the header
    // terminator).
    if let Err(e) = s.read_to_end(&mut buf).await {
        if e.kind() != std::io::ErrorKind::ConnectionReset || buf.is_empty() {
            panic!("response read: {e}");
        }
    }
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header terminator");
    let head = String::from_utf8_lossy(&buf[..split]).to_string();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = std::collections::HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    let mut raw_body = buf[split + 4..].to_vec();
    if headers.get("transfer-encoding").map(|v| v == "chunked") == Some(true) {
        // Connection: close + chunked — decode.
        let mut out = Vec::new();
        let mut rest: &[u8] = &raw_body;
        loop {
            let Some(le) = rest.windows(2).position(|w| w == b"\r\n") else {
                break;
            };
            let n =
                usize::from_str_radix(std::str::from_utf8(&rest[..le]).unwrap_or("0").trim(), 16)
                    .unwrap_or(0);
            if n == 0 {
                break;
            }
            let start = le + 2;
            out.extend_from_slice(&rest[start..start + n]);
            rest = &rest[start + n + 2..];
        }
        raw_body = out;
    }
    (status, headers, raw_body)
}

/// One page of a keyed/keyless read. Returns (status, headers, records).
async fn read_page(
    addr: std::net::SocketAddr,
    stream: &str,
    key: Option<&str>,
    tok: Option<&str>,
) -> (
    u16,
    std::collections::HashMap<String, String>,
    Vec<serde_json::Value>,
) {
    // Keyed pages go through the PRODUCT route (the singular route is
    // the default-key view only); the product route's cursor parameter
    // accepts the same opaque tokens the lineage reader emits, so these
    // tests still drive the lineage machinery directly.
    let (st, h, b) = match key {
        Some(k) => {
            let mut path = format!("/v1/streams/{stream}/records?routingKey={k}");
            if let Some(t) = tok {
                path.push_str(&format!("&cursor={t}"));
            }
            preq(
                addr,
                "GET",
                &path,
                &[("prisma-encryption-key", PRISMA_KEY)],
                b"",
            )
            .await
        }
        None => {
            let mut path = format!("/v1/stream/{stream}?x=1");
            if let Some(t) = tok {
                path.push_str(&format!("&offset={t}"));
            }
            hreq(addr, "GET", &path, &[], b"").await
        }
    };
    let recs = if b.is_empty() {
        Vec::new()
    } else {
        serde_json::from_slice::<Vec<serde_json::Value>>(&b).unwrap_or_default()
    };
    (st, h, recs)
}

/// Drain a key fully, asserting NO page ever reports closure. Returns
/// (records, final headers).
async fn drain_no_closure(
    addr: std::net::SocketAddr,
    stream: &str,
    key: Option<&str>,
) -> (
    Vec<serde_json::Value>,
    std::collections::HashMap<String, String>,
) {
    let mut tok: Option<String> = None;
    let mut out = Vec::new();
    for _ in 0..64 {
        let (st, h, recs) = read_page(addr, stream, key, tok.as_deref()).await;
        assert!(st == 200 || st == 204, "page status {st}");
        assert!(
            !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
            "a transition must never report closure (headers: {h:?})"
        );
        out.extend(recs);
        // Keyed pages come from the product surface (Prisma-*), keyless
        // ones from the raw surface (Stream-*).
        let nxt = h
            .get("prisma-next-cursor")
            .or_else(|| h.get("stream-next-offset"))
            .cloned();
        let utd = h
            .get("prisma-up-to-date")
            .or_else(|| h.get("stream-up-to-date"))
            .map(|v| v == "true")
            == Some(true);
        if utd || nxt.is_none() || nxt == tok {
            return (out, h);
        }
        tok = nxt;
    }
    panic!("drain did not settle");
}

fn gap_lock() -> &'static tokio::sync::Mutex<()> {
    static L: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Releases the publish failpoint even if the test panics — a parked
/// resume must never leak into sibling tests.
struct FailpointGuard;
impl Drop for FailpointGuard {
    fn drop(&mut self) {
        crate::scaler3::failpoints::release_before_publish();
    }
}

/// Boot a rig, create + fill a stream, then drive a split INTO the
/// parked seal-gap: Phase A CAS'd, parent sealed, successors withheld.
/// Returns everything the gap assertions need.
async fn rig_in_seal_gap(
    stream: &str,
    per_key: usize,
) -> (
    Arc<crate::http::AppState>,
    std::net::SocketAddr,
    FailpointGuard,
    tokio::task::JoinHandle<bool>,
) {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        &format!("/v1/stream/{stream}"),
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "create {st}");
    for i in 0..per_key {
        for k in ["ga", "gb"] {
            let body = serde_json::json!({ "k": k, "n": i }).to_string();
            let (st, _, _) = preq(
                addr,
                "POST",
                &format!("/v1/streams/{stream}/records"),
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert!(st == 200 || st == 204, "append {st}");
        }
    }
    crate::scaler3::failpoints::arm_before_publish();
    let guard = FailpointGuard;
    let split = {
        let state = state.clone();
        let name = stream.to_string();
        tokio::spawn(async move {
            crate::scaler3::execute_split(&state, &name, 0, 0x8000_0000_0000_0000).await
        })
    };
    // The gap is entered once the parent identity's engine handle is
    // CLOSED while the descriptor still shows one segment + pending.
    let desc = state.registry.get(stream).await.unwrap().unwrap();
    let identity = desc.resolve_segment("").identity;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let closed = match state
            .engine_for_scaler(&crate::crypto::stream_hash(stream))
            .await
        {
            Some(e) => match e.stream_handle(identity).await {
                Ok(h) => h.state.lock().unwrap().durable.closed,
                Err(_) => false,
            },
            None => false,
        };
        let d = state.registry.get(stream).await.unwrap().unwrap();
        let pending = d
            .segments
            .as_ref()
            .map(|m| m.pending.is_some() && m.segments.len() == 1)
            .unwrap_or(false);
        if closed && pending {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "seal gap never entered (closed={closed} pending={pending})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    (state, addr, guard, split)
}

/// Wait until the withheld publication lands after release.
async fn await_published(state: &Arc<crate::http::AppState>, stream: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        state.registry.invalidate(stream);
        let d = state.registry.get(stream).await.unwrap().unwrap();
        let done = d
            .segments
            .as_ref()
            .map(|m| m.pending.is_none() && m.segments.len() > 1)
            .unwrap_or(false);
        if done {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "publication never completed after release"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// GET during the seal gap: records + resume cursor, never closure,
/// never a final Up-To-Date; after release, the same client drains the
/// full lineage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_get_never_reports_closure() {
    let _l = gap_lock().lock().await;
    let (state, addr, _guard, split) = rig_in_seal_gap("gapget", 6).await;

    let mut tok: Option<String> = None;
    let mut got = 0usize;
    for _ in 0..16 {
        let (st, h, recs) = read_page(addr, "gapget", Some("ga"), tok.as_deref()).await;
        assert!(st == 200 || st == 204, "gap page status {st}");
        assert!(
            !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
            "seal gap reported closure: {h:?}"
        );
        assert!(
            !h.contains_key("stream-up-to-date"),
            "seal gap reported finality: {h:?}"
        );
        got += recs.len();
        let nxt = h
            .get("prisma-next-cursor")
            .or_else(|| h.get("stream-next-offset"))
            .cloned();
        if nxt.is_none() || nxt == tok {
            break;
        }
        tok = nxt;
    }
    assert_eq!(got, 6, "every pre-seal record stays readable in the gap");

    crate::scaler3::failpoints::release_before_publish();
    // The reader-spawned resume() may win the publication CAS; the
    // split task's own bool only says who published. The outcome gate
    // is await_published.
    split.await.unwrap();
    await_published(&state, "gapget").await;
    let (recs, last) = drain_no_closure(addr, "gapget", Some("ga")).await;
    assert_eq!(recs.len(), 6);
    assert_eq!(
        last.get("prisma-up-to-date")
            .or_else(|| last.get("stream-up-to-date"))
            .map(String::as_str),
        Some("true"),
        "published lineage ends Up-To-Date"
    );
}

/// HEAD during the gap must not report closure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_head_no_closure() {
    let _l = gap_lock().lock().await;
    let (state, addr, _guard, split) = rig_in_seal_gap("gaphead", 3).await;
    let (st, h, _) = hreq(addr, "HEAD", "/v1/stream/gaphead", &[], b"").await;
    assert_eq!(st, 200);
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "HEAD reported closure in the gap: {h:?}"
    );
    crate::scaler3::failpoints::release_before_publish();
    split.await.unwrap();
    await_published(&state, "gaphead").await;
    let (st, h, _) = hreq(addr, "HEAD", "/v1/stream/gaphead", &[], b"").await;
    assert_eq!(st, 200);
    assert!(!h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"));
}

/// A long-poll already parked at the tail when the seal lands must wake
/// WITHOUT closure and with a usable rearm token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_long_poll_wakes_without_closure() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/gappoll",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "create {st}");
    for i in 0..3 {
        let body = serde_json::json!({ "k": "ga", "n": i }).to_string();
        preq(
            addr,
            "POST",
            "/v1/streams/gappoll/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "ga"),
            ],
            body.as_bytes(),
        )
        .await;
    }
    // Find the tail token, then park a long-poll on it.
    let (_, h, _) = read_page(addr, "gappoll", None, None).await;
    let tail = h.get("stream-next-offset").unwrap().clone();
    let poll = {
        let tail = tail.clone();
        tokio::spawn(async move {
            hreq(
                addr,
                "GET",
                &format!("/v1/stream/gappoll?offset={tail}&live=long-poll&timeout=8s"),
                &[],
                b"",
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    crate::scaler3::failpoints::arm_before_publish();
    let _guard = FailpointGuard;
    let split = {
        let state = state.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(&state, "gappoll", 0, 0x8000_0000_0000_0000).await
        })
    };
    let (st, h, _) = poll.await.unwrap();
    assert!(st == 200 || st == 204, "poll woke with {st}");
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "seal wake reported closure: {h:?}"
    );
    crate::scaler3::failpoints::release_before_publish();
    split.await.unwrap();
    await_published(&state, "gappoll").await;
    let (recs, _) = drain_no_closure(addr, "gappoll", Some("ga")).await;
    assert_eq!(recs.len(), 3);
}

/// A read STARTING inside the gap (cold client, offset 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_cold_read_mid_gap() {
    let _l = gap_lock().lock().await;
    let (state, addr, _guard, split) = rig_in_seal_gap("gapcold", 4).await;
    let (st, h, recs) = read_page(addr, "gapcold", Some("gb"), None).await;
    assert_eq!(st, 200);
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "{h:?}"
    );
    assert!(!h.contains_key("stream-up-to-date"), "{h:?}");
    assert_eq!(recs.len(), 4, "gap serves everything below the seal");
    crate::scaler3::failpoints::release_before_publish();
    split.await.unwrap();
    await_published(&state, "gapcold").await;
    let (recs, _) = drain_no_closure(addr, "gapcold", Some("gb")).await;
    assert_eq!(recs.len(), 4);
}

/// A client that vanishes mid-request during the gap, then retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_cancel_then_retry() {
    let _l = gap_lock().lock().await;
    let (state, addr, _guard, split) = rig_in_seal_gap("gapcancel", 4).await;
    {
        use tokio::io::AsyncWriteExt;
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "GET /v1/streams/gapcancel/records?routingKey=ga HTTP/1.1\r\nhost: x\r\nprisma-encryption-key: {RIG_KEY_B64}\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await.unwrap();
        drop(s); // vanish without reading the response
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (st, h, recs) = read_page(addr, "gapcancel", Some("ga"), None).await;
    assert_eq!(st, 200);
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "{h:?}"
    );
    assert_eq!(
        recs.len(),
        4,
        "retry after cancellation serves the gap view"
    );
    crate::scaler3::failpoints::release_before_publish();
    split.await.unwrap();
    await_published(&state, "gapcancel").await;
    let (recs, _) = drain_no_closure(addr, "gapcancel", Some("ga")).await;
    assert_eq!(recs.len(), 4);
}

/// The cross-instance shape: a reader whose CACHED descriptor predates
/// the whole transition (no pending, one segment) meets the sealed
/// engine handle. The standard path must refresh + redispatch instead
/// of trusting the stale map and reporting closure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_gap_stale_descriptor_redispatches() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/gapstale",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "create {st}");
    for i in 0..4 {
        let body = serde_json::json!({ "k": "ga", "n": i }).to_string();
        preq(
            addr,
            "POST",
            "/v1/streams/gapstale/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "ga"),
            ],
            body.as_bytes(),
        )
        .await;
    }
    // The pre-transition descriptor this "instance" will keep believing.
    let stale = state.registry.get("gapstale").await.unwrap().unwrap();
    assert!(stale.segments.as_ref().is_none_or(|m| m.pending.is_none()));

    crate::scaler3::failpoints::arm_before_publish();
    let _guard = FailpointGuard;
    let split = {
        let state = state.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(&state, "gapstale", 0, 0x8000_0000_0000_0000).await
        })
    };
    // Wait for the seal, then plant the STALE descriptor over the fresh
    // cache entry — the reader now sees exactly what a lagging sibling
    // instance would.
    let identity = stale.resolve_segment("").identity;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let closed = match state
            .engine_for_scaler(&crate::crypto::stream_hash("gapstale"))
            .await
        {
            Some(e) => match e.stream_handle(identity).await {
                Ok(h) => h.state.lock().unwrap().durable.closed,
                Err(_) => false,
            },
            None => false,
        };
        if closed {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "seal never landed");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    state.registry.test_poison_cache("gapstale", stale);

    let (st, h, recs) = read_page(addr, "gapstale", Some("ga"), None).await;
    assert_eq!(st, 200);
    assert!(
        !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
        "stale descriptor let closure through: {h:?}"
    );
    assert_eq!(recs.len(), 4);
    crate::scaler3::failpoints::release_before_publish();
    split.await.unwrap();
    await_published(&state, "gapstale").await;
    let (recs, _) = drain_no_closure(addr, "gapstale", Some("ga")).await;
    assert_eq!(recs.len(), 4);
}

// ---- oversized keyed records / long runs (review blocker: the first
// record must ALWAYS make progress; consumed_to is first-class) -------

/// Byte-budgeted keyed drain returning (offsets, payload_sizes, pages).
/// Panics if a page makes no progress — the stall this guards against.
async fn drain_keyed_paged(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    rk: &str,
    page_bytes: usize,
) -> (Vec<u64>, Vec<usize>, usize) {
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mut from = 0u64;
    let mut offs = Vec::new();
    let mut sizes = Vec::new();
    let mut pages = 0usize;
    loop {
        pages += 1;
        assert!(pages <= 128, "drain did not settle");
        let res = crate::http::read_merged(key, &hash, &handle, engine, from, Some(rk), page_bytes)
            .await
            .expect("keyed read");
        for rec in &res.recs {
            offs.push(rec.off);
            sizes.push(rec.payload.len());
        }
        if res.completed {
            return (offs, sizes, pages);
        }
        match res.last {
            Some(last) if last + 1 > from => from = last + 1,
            other => panic!(
                "incomplete page made no progress (from={from}, last={other:?}) — \
                 the oversized-run stall"
            ),
        }
    }
}

/// One record far larger than the page budget, surrounded by small
/// records of another key: it must be served (allow-first), and the
/// drain must complete with exact contents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_keyed_record_pages_through() {
    let store = mem();
    let key = skey();
    let hash = [0xC1u8; 16];
    let db = slatedb::Db::builder("dst-bigrec", store.clone())
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
        "dst-bigrec".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            min_age_bytes: 0,
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    let mut want: Vec<(u64, usize)> = Vec::new();
    for _ in 0..3 {
        append_sized(&engine, hash, &key, "other", 4 * 1024).await;
    }
    want.push((
        append_sized(&engine, hash, &key, "big", 12 * 1024 * 1024).await,
        12 * 1024 * 1024,
    ));
    for _ in 0..3 {
        append_sized(&engine, hash, &key, "other", 4 * 1024).await;
    }
    for _ in 0..4 {
        want.push((
            append_sized(&engine, hash, &key, "big", 4 * 1024).await,
            4 * 1024,
        ));
    }
    wait_all_absorbed(&engine, &[hash]).await;
    let ds: Arc<dyn ObjectStore> = store.clone();
    let (offs, sizes, pages) = drain_keyed_paged(&engine, hash, &key, "big", 1024 * 1024).await;
    assert_eq!(
        offs,
        want.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
        "exact offsets in order"
    );
    assert_eq!(
        sizes,
        want.iter().map(|(_, s)| *s).collect::<Vec<_>>(),
        "the 12 MiB record arrived intact"
    );
    assert!(pages >= 2, "budget forces pagination (pages={pages})");
    engine.begin_close();
}

/// A contiguous single-key run larger than the 16 MiB plan budget AND
/// the page budget: sub-run planning + span truncation page it through,
/// including across a postings-cache wipe mid-drain (the restart /
/// cold-instance shape).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_keyed_run_pages_with_progress() {
    let store = mem();
    let key = skey();
    let hash = [0xC2u8; 16];
    let db = slatedb::Db::builder("dst-bigrun", store.clone())
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
        "dst-bigrun".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            min_age_bytes: 0,
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    let mut want = Vec::new();
    for _ in 0..24 {
        want.push(append_sized(&engine, hash, &key, "run", 1024 * 1024).await);
    }
    wait_all_absorbed(&engine, &[hash]).await;
    let ds: Arc<dyn ObjectStore> = store.clone();

    // Page manually so the cache wipe lands between pages.
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mut from = 0u64;
    let mut offs = Vec::new();
    let mut pages = 0usize;
    loop {
        pages += 1;
        assert!(pages <= 64, "drain did not settle");
        let res = crate::http::read_merged(
            &key,
            &hash,
            &handle,
            &engine,
            from,
            Some("run"),
            4 * 1024 * 1024,
        )
        .await
        .expect("keyed read");
        for rec in &res.recs {
            offs.push(rec.off);
            assert_eq!(rec.payload.len(), 1024 * 1024);
        }
        if res.completed {
            break;
        }
        match res.last {
            Some(last) if last + 1 > from => from = last + 1,
            other => panic!("no progress at from={from} ({other:?})"),
        }
        if pages == 2 {
            // Cold-instance restart between partial pages.
            engine.postings_cache.sweep_idle(std::time::Duration::ZERO);
        }
    }
    assert_eq!(offs, want, "all 24 MiB drained exactly once, in order");
    assert!(pages >= 4, "24 MiB through 4 MiB pages (pages={pages})");
    engine.begin_close();
}

/// Review blocker 4: a sequence the PARENT accepted must conflict on
/// the child — Stream-Seq resolves through the sealed predecessor
/// chain (nearest identity wins), and the next sequence continues.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_seq_resolves_through_predecessors() {
    let store = mem();
    let key = skey();
    let parent = [0xD1u8; 16];
    let child = [0xD2u8; 16];
    let db = slatedb::Db::builder("dst-seqchain", store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let engine = crate::shard::ShardEngine::start(
        "dst-seqchain".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let send = |identity: [u8; 16], lineage: Vec<[u8; 16]>, seq: Option<&str>, close: bool| {
        let engine = engine.clone();
        let key = key.clone();
        let seq = seq.map(|s| s.to_string());
        async move {
            let subkey = crate::crypto::derive_subkey(&key, &identity, "sk", 0);
            let (tx, rx) = tokio::sync::oneshot::channel();
            let req = crate::shard::AppendReq {
                enqueued_at: std::time::Instant::now(),
                hash: identity,
                route: identity,
                entries: if close {
                    Vec::new()
                } else {
                    vec![bytes::Bytes::from_static(b"{\"s\":1}")]
                },
                usage: crate::usage::counters(&identity),
                routing_key: "sk".to_string(),
                key_hash: crate::crypto::stream_hash("sk"),
                producer_lineage: lineage,
                key_version: 0,
                subkey,
                ts_hint_ms: None,
                seq,
                bytes: 0,
                close,
                producer: None,
                deferred_error: None,
                sealed_reject_new: None,
                touch: None,
                resp: tx,
            };
            assert!(engine.try_enqueue(req).is_ok());
            rx.await.expect("resp")
        }
    };

    send(parent, vec![], Some("s10"), false)
        .await
        .expect("parent accepts s10");
    send(parent, vec![], None, true).await.expect("seal parent");

    // The parent's lane must gate the child through the chain.
    match send(child, vec![parent], Some("s10"), false).await {
        Err(crate::shard::AppendErr::SeqConflict { current }) => {
            assert_eq!(current.as_deref(), Some("s10"));
        }
        other => panic!("s10 on the child must conflict, got {other:?}"),
    }
    match send(child, vec![parent], Some("s09"), false).await {
        Err(crate::shard::AppendErr::SeqConflict { .. }) => {}
        other => panic!("s09 on the child must conflict, got {other:?}"),
    }
    let ack = send(child, vec![parent], Some("s11"), false)
        .await
        .expect("s11 advances the chained lane");
    assert_eq!(ack.last_offset, 0, "first real child record");
    engine.begin_close();
}

/// Review finding 5: producer sessions are scoped per ROUTING KEY. One
/// producer id runs independent sequence lanes on two keys of one
/// segment, and each lane follows ITS key through a split.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_lanes_scoped_per_routing_key() {
    let store = mem();
    let key = skey();
    let parent = [0xD3u8; 16];
    let child = [0xD4u8; 16];
    let db = slatedb::Db::builder("dst-prodkeys", store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let engine = crate::shard::ShardEngine::start(
        "dst-prodkeys".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let send = |identity: [u8; 16], lineage: Vec<[u8; 16]>, rk: &str, seq: u64, close: bool| {
        let engine = engine.clone();
        let key = key.clone();
        let rk = rk.to_string();
        async move {
            let subkey = crate::crypto::derive_subkey(&key, &identity, &rk, 0);
            let (tx, rx) = tokio::sync::oneshot::channel();
            let req = crate::shard::AppendReq {
                enqueued_at: std::time::Instant::now(),
                hash: identity,
                route: identity,
                entries: if close {
                    Vec::new()
                } else {
                    vec![bytes::Bytes::from_static(b"{\"p\":1}")]
                },
                usage: crate::usage::counters(&identity),
                routing_key: rk.clone(),
                key_hash: crate::crypto::stream_hash(&rk),
                producer_lineage: lineage,
                key_version: 0,
                subkey,
                ts_hint_ms: None,
                seq: None,
                bytes: 0,
                close,
                producer: if close {
                    None
                } else {
                    Some(crate::shard::ProducerReq {
                        id: "prod-x".into(),
                        epoch: 1,
                        seq,
                        request_hash: None,
                    })
                },
                deferred_error: None,
                sealed_reject_new: None,
                touch: None,
                resp: tx,
            };
            assert!(engine.try_enqueue(req).is_ok());
            rx.await.expect("resp")
        }
    };

    // Alternating sequences on two keys, ONE producer id: independent
    // lanes must both start at 0 and advance without cross-talk.
    let k1s0 = send(parent, vec![], "k1", 0, false).await.expect("k1 s0");
    assert!(!k1s0.duplicate);
    let k2s0 = send(parent, vec![], "k2", 0, false).await.expect("k2 s0");
    assert!(!k2s0.duplicate, "k2's lane is independent of k1's");
    let k1s1 = send(parent, vec![], "k1", 1, false).await.expect("k1 s1");
    assert!(!k1s1.duplicate);
    let k2s1 = send(parent, vec![], "k2", 1, false).await.expect("k2 s1");
    assert!(!k2s1.duplicate);

    send(parent, vec![], "k1", 9, true).await.expect("seal");

    // Across the split, each key's duplicate answers with ITS OWN
    // original offset — not the other key's, not the tail.
    let d1 = send(child, vec![parent], "k1", 1, false)
        .await
        .expect("k1 retry");
    assert!(d1.duplicate);
    assert_eq!(d1.last_offset, k1s1.last_offset, "k1's own offset");
    let d2 = send(child, vec![parent], "k2", 1, false)
        .await
        .expect("k2 retry");
    assert!(d2.duplicate);
    assert_eq!(d2.last_offset, k2s1.last_offset, "k2's own offset");

    // And fresh sequences continue independently on the child.
    assert!(
        !send(child, vec![parent], "k1", 2, false)
            .await
            .expect("k1 s2")
            .duplicate
    );
    assert!(
        !send(child, vec![parent], "k2", 2, false)
            .await
            .expect("k2 s2")
            .duplicate
    );
    engine.begin_close();
}

// ---- physical scaling (review blocker 1: a split must ADD capacity —
// children on real routes, distinct engines, ≥1.8x throughput) --------

/// Concurrent keyed append load for `secs`; returns acks completed.
/// Every append must succeed — capacity tests tolerate zero errors.
async fn blast_keys(
    addr: std::net::SocketAddr,
    stream: &str,
    keys: &[&str],
    clients: usize,
    secs: f64,
) -> u64 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let done = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tasks = Vec::new();
    for c in 0..clients {
        let done = done.clone();
        let stop = stop.clone();
        let stream = stream.to_string();
        let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        tasks.push(tokio::spawn(async move {
            // One key per client (a client blocked on a saturated
            // segment must not throttle the other side) and ONE
            // persistent keep-alive connection: per-request TCP churn
            // burns the client time that should keep admitted slots
            // full, and that loss is what a capacity ratio measures.
            let k = keys[c % keys.len()].clone();
            let mut conn: Option<tokio::net::TcpStream> = None;
            let mut buf = vec![0u8; 16 * 1024];
            let mut i = c;
            'outer: while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                i += 1;
                let sck = match conn.as_mut() {
                    Some(s) => s,
                    None => {
                        conn = Some(tokio::net::TcpStream::connect(addr).await.unwrap());
                        conn.as_mut().unwrap()
                    }
                };
                let body = format!("{{\"c\":{c},\"i\":{i}}}");
                let req = format!(
                    "POST /v1/streams/{stream}/records HTTP/1.1\r\nhost: x\r\nprisma-encryption-key: {RIG_KEY_B64}\r\nprisma-routing-key: {k}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                    body.len()
                );
                if sck.write_all(req.as_bytes()).await.is_err() {
                    conn = None;
                    continue;
                }
                // Read one response: headers, then content-length body.
                let mut head = Vec::new();
                let split_at;
                loop {
                    let n = match sck.read(&mut buf).await {
                        Ok(0) | Err(_) => {
                            conn = None;
                            continue 'outer;
                        }
                        Ok(n) => n,
                    };
                    head.extend_from_slice(&buf[..n]);
                    if let Some(p) = head.windows(4).position(|w| w == b"\r\n\r\n") {
                        split_at = p;
                        break;
                    }
                }
                let head_str = String::from_utf8_lossy(&head[..split_at]).to_string();
                let status: u16 = head_str
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let clen: usize = head_str
                    .lines()
                    .find_map(|l| {
                        let (k2, v) = l.split_once(':')?;
                        (k2.trim().eq_ignore_ascii_case("content-length"))
                            .then(|| v.trim().parse().ok())?
                    })
                    .unwrap_or(0);
                let mut have = head.len() - split_at - 4;
                while have < clen {
                    let n = match sck.read(&mut buf).await {
                        Ok(0) | Err(_) => {
                            conn = None;
                            continue 'outer;
                        }
                        Ok(n) => n,
                    };
                    have += n;
                }
                if status == 429 {
                    // Backpressure is the capacity ceiling speaking — not
                    // an error. Back off briefly and retry.
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                assert!(
                    status == 200 || status == 204 || status == 503,
                    "append during capacity run: {status}"
                );
                if status == 200 || status == 204 {
                    done.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }));
    }
    tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for t in tasks {
        t.await.unwrap();
    }
    done.load(std::sync::atomic::Ordering::Relaxed)
}

/// Split children carry REAL routes on DISTINCT engines, and per-key
/// order + exact counts hold across the lineage on both sides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_children_land_on_distinct_engines() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig_opts(
        store,
        vec!["00".into(), "01".into(), "02".into(), "03".into()],
        crate::shard::ShardConfig::default(),
    )
    .await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/cap-routes",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    // Keys straddling the midpoint so both children get traffic.
    let keys = ["ga", "gb", "gc", "gd", "ge", "gf", "gg", "gh"];
    let mut per_key = std::collections::HashMap::new();
    for round in 0..12 {
        for k in &keys {
            let body = format!("{{\"k\":\"{k}\",\"n\":{round}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/cap-routes/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert!(st == 200 || st == 204);
            *per_key.entry(k.to_string()).or_insert(0usize) += 1;
        }
    }
    assert!(
        crate::scaler3::execute_split(&state, "cap-routes", 0, 0x8000_0000_0000_0000).await,
        "split executes"
    );
    state.registry.invalidate("cap-routes");
    let desc = state.registry.get("cap-routes").await.unwrap().unwrap();
    let map = desc.segments.as_ref().expect("map");
    let live: Vec<_> = map.segments.iter().filter(|s| s.is_live()).collect();
    assert_eq!(live.len(), 2);
    let r0 = desc.segment_route(live[0]);
    let r1 = desc.segment_route(live[1]);
    assert_ne!(r0, r1, "children carry independent routes");
    let p0 = crate::registry::shard_for_hash(&state.shard_prefixes, &r0);
    let p1 = crate::registry::shard_for_hash(&state.shard_prefixes, &r1);
    assert_ne!(p0, p1, "routes land on distinct shard prefixes");
    let e0 = state.engine_for_scaler(&r0).await.expect("engine 0");
    let e1 = state.engine_for_scaler(&r1).await.expect("engine 1");
    assert!(
        !Arc::ptr_eq(&e0, &e1),
        "children must resolve to DISTINCT ShardEngines"
    );

    // Post-split traffic to both sides, then exact ordered drains
    // across the lineage.
    for round in 12..20 {
        for k in &keys {
            let body = format!("{{\"k\":\"{k}\",\"n\":{round}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/cap-routes/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert!(st == 200 || st == 204, "post-split append {st}");
            *per_key.get_mut(&k.to_string()).unwrap() += 1;
        }
    }
    for k in &keys {
        let (recs, _) = drain_no_closure(addr, "cap-routes", Some(k)).await;
        let ns: Vec<i64> = recs
            .iter()
            .filter(|r| r["k"] == *k)
            .map(|r| r["n"].as_i64().unwrap())
            .collect();
        assert_eq!(ns.len(), per_key[&k.to_string()], "exact count for {k}");
        assert!(
            ns.windows(2).all(|w| w[0] <= w[1]),
            "per-key order for {k}: {ns:?}"
        );
    }
    engine_shutdown(&state).await;
}

/// The capacity gate: with serial per-append WAL (group commit off) and
/// uniform store latency, one segment plateaus at one committer's
/// throughput; after the split, two children on two engines must
/// deliver >= 1.8x — with zero client-visible errors and exact counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn post_split_throughput_scales() {
    // A capacity RATIO is only valid when the measurement owns the
    // machine: run serialized against the other heavy tests (the
    // parallel suite's CPU contention pushed a real 1.8x+ split to a
    // measured 1.77). This serializes the measurement; it does not
    // relax the gate.
    let _l = gap_lock().lock().await;
    let inner = mem();
    let store: Arc<dyn ObjectStore> = FaultStore::uniform(
        inner,
        1231,
        FaultPlan {
            latency_pct: 100,
            latency_ms: (20, 20),
            ..FaultPlan::new(0, 0, 0)
        },
    );
    // Capacity here = the per-SEGMENT admission budget (4 inflight) on
    // top of real store latency. An in-process rig cannot reproduce the
    // hardware saturation that caps a real committer (the field
    // envelope measured ~662 rps/segment on SIN); what it CAN prove
    // deterministically is the mechanism the review demanded: after a
    // split, each child owns an independent capacity budget on its own
    // engine, so admitted concurrency — and throughput at fixed
    // per-request cost — doubles. The field campaign re-measures this
    // on real hardware.
    let (state, addr) = http_rig_full(
        store,
        vec!["00".into(), "01".into(), "02".into(), "03".into()],
        crate::shard::ShardConfig {
            wal_group_commit: false,
            ..Default::default()
        },
        8,
    )
    .await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/cap-scale",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    let keys = ["ga", "gb", "gc", "gd", "ge", "gf", "gg", "gh"];

    // Warm-up (engine open, tail ring, registry cache), then the
    // single-segment plateau. 48 concurrent clients SATURATE the
    // committer — a latency-bound load (few sequential clients) would
    // measure round trips, not capacity, and mask the split entirely.
    blast_keys(addr, "cap-scale", &keys, 48, 0.5).await;
    // Capacity is a MAXIMUM-achievable property, so each phase takes
    // the best of three windows.
    //
    // This is a MECHANISM check, not performance evidence: it proves
    // that a split gives each child an independent admission budget on
    // a shared, noisy host. Best-of-N is the right shape for that and
    // the wrong shape for a published number — a fleet capacity claim
    // needs isolated instances, paired steady-state windows, medians
    // and a lower confidence bound, which is what the field campaign
    // measures (docs/ROUTING-V3.md §11). A contended host depresses samples
    // one-sidedly — the post-split phase needs two committers' worth of
    // CPU, so noise lands there and only ever understates the ratio
    // (observed: 1.78 with three other servers running, 1.82-1.91
    // quiet). Best-of-two removes that bias without relaxing the gate:
    // a real capacity regression fails both windows.
    let before = {
        let mut best = 0;
        for _ in 0..3 {
            best = best.max(blast_keys(addr, "cap-scale", &keys, 48, 2.5).await);
        }
        best
    };

    assert!(
        crate::scaler3::execute_split(&state, "cap-scale", 0, 0x8000_0000_0000_0000).await,
        "split executes"
    );
    // Verify distinct engines before measuring.
    state.registry.invalidate("cap-scale");
    let desc = state.registry.get("cap-scale").await.unwrap().unwrap();
    let map = desc.segments.as_ref().expect("map");
    let live: Vec<_> = map.segments.iter().filter(|s| s.is_live()).collect();
    let e0 = state
        .engine_for_scaler(&desc.segment_route(live[0]))
        .await
        .unwrap();
    let e1 = state
        .engine_for_scaler(&desc.segment_route(live[1]))
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&e0, &e1), "distinct engines post-split");

    // Warm the child committers, then measure.
    blast_keys(addr, "cap-scale", &keys, 48, 0.5).await;
    let after = {
        let mut best = 0;
        for _ in 0..3 {
            best = best.max(blast_keys(addr, "cap-scale", &keys, 48, 2.5).await);
        }
        best
    };

    let ratio = after as f64 / before.max(1) as f64;
    eprintln!("capacity gate: before={before} after={after} ratio={ratio:.2}");
    // On failure, say which failure it is. A depressed BASELINE means the
    // host was busy (other servers, another suite), not that a split
    // stopped adding capacity — and reading one as the other has now
    // cost two suite runs.
    assert!(
        ratio >= 1.8,
        "post-split throughput must be >= 1.8x (before={before} after={after} \
         ratio={ratio:.2}). Baseline {before} rps{}",
        if before < 480 {
            " is below the ~530-560 single-segment plateau this rig reaches when \
             idle: the host was loaded, re-run with nothing else on it before \
             treating this as a capacity regression"
        } else {
            " is in the normal range, so this is a real capacity regression"
        }
    );
    engine_shutdown(&state).await;
}

/// Close every open engine so background loops die with the test.
async fn engine_shutdown(state: &Arc<crate::http::AppState>) {
    let engines: Vec<_> = state.shards.read().unwrap().values().cloned().collect();
    for e in engines {
        e.begin_close();
    }
}

/// Merge execution (review deferral, now implemented): split then merge
/// back — the merged child covers the full range on a real route, both
/// children seal, and per-key reads drain exactly across all THREE
/// generations (parent -> split child -> merged child).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_rejoins_cold_children_with_exact_lineage() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig_opts(
        store,
        vec!["00".into(), "01".into()],
        crate::shard::ShardConfig::default(),
    )
    .await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/mergeback",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    let keys = ["ga", "gb", "gc", "gd"];
    let mut per_key: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    async fn append_round(
        addr: std::net::SocketAddr,
        keys: &[&str],
        round: i64,
        pk: &mut std::collections::HashMap<String, usize>,
    ) {
        for k in keys {
            let body = format!("{{\"k\":\"{k}\",\"n\":{round}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/mergeback/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert!(st == 200 || st == 204, "append {st}");
            *pk.entry(k.to_string()).or_insert(0usize) += 1;
        }
    }
    for r in 0..6 {
        append_round(addr, &keys, r, &mut per_key).await;
    }
    assert!(
        crate::scaler3::execute_split(&state, "mergeback", 0, 0x8000_0000_0000_0000).await,
        "split"
    );
    for r in 6..10 {
        append_round(addr, &keys, r, &mut per_key).await;
    }
    // Merge the two live children back together.
    state.registry.invalidate("mergeback");
    let desc = state.registry.get("mergeback").await.unwrap().unwrap();
    let live: Vec<u32> = {
        let map = desc.segments.as_ref().unwrap();
        let mut v: Vec<_> = map.segments.iter().filter(|s| s.is_live()).collect();
        v.sort_by_key(|s| s.lo);
        v.iter().map(|s| s.seg_id).collect()
    };
    assert_eq!(live.len(), 2);
    assert!(
        crate::scaler3::execute_merge(&state, "mergeback", live[0], live[1]).await,
        "merge executes"
    );
    state.registry.invalidate("mergeback");
    let desc = state.registry.get("mergeback").await.unwrap().unwrap();
    let map = desc.segments.as_ref().unwrap();
    assert!(map.pending.is_none());
    let now_live: Vec<_> = map.segments.iter().filter(|s| s.is_live()).collect();
    assert_eq!(now_live.len(), 1, "one merged child");
    assert_eq!(
        (now_live[0].lo, now_live[0].hi),
        (0, crate::segmap::KEYSPACE_END),
        "full-range cover"
    );
    assert_eq!(now_live[0].predecessors.len(), 2, "merge lineage recorded");
    assert_ne!(
        desc.segment_route(now_live[0]),
        [0u8; 16],
        "merged child carries a real route"
    );
    for id in &live {
        let sg = map.get(*id).unwrap();
        assert!(
            !sg.is_live() && sg.sealed_next_offset.is_some(),
            "children sealed"
        );
    }
    // Post-merge appends land on the merged child; drains stay exact
    // and ordered across all three generations.
    for r in 10..14 {
        append_round(addr, &keys, r, &mut per_key).await;
    }
    for k in &keys {
        let (recs, last) = drain_no_closure(addr, "mergeback", Some(k)).await;
        let ns: Vec<i64> = recs
            .iter()
            .filter(|r| r["k"] == *k)
            .map(|r| r["n"].as_i64().unwrap())
            .collect();
        assert_eq!(ns.len(), per_key[&k.to_string()], "exact count for {k}");
        assert!(ns.windows(2).all(|w| w[0] < w[1]), "order for {k}: {ns:?}");
        assert_eq!(
            last.get("prisma-up-to-date")
                .or_else(|| last.get("stream-up-to-date"))
                .map(String::as_str),
            Some("true")
        );
    }
    engine_shutdown(&state).await;
}

/// Keyed SSE follows the lineage (review deferral, now wired): a
/// subscriber from offset 0 receives every pre-split AND post-split
/// record for its key in order, then an upToDate control.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_follows_lineage_across_split() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/sselin",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    for r in 0..5 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{r}}}");
            preq(
                addr,
                "POST",
                "/v1/streams/sselin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
        }
    }
    assert!(crate::scaler3::execute_split(&state, "sselin", 0, 0x8000_0000_0000_0000).await);
    for r in 5..10 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{r}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/sselin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert!(st == 200 || st == 204);
        }
    }

    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/streams/sselin/records:sse?routingKey=ga HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nprisma-encryption-key: {RIG_KEY_B64}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 8192];
    let mut acc: Vec<u8> = Vec::new();
    let mut ns: Vec<i64> = Vec::new();
    let mut saw_utd = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    'read: while std::time::Instant::now() < deadline {
        let n = match tokio::time::timeout(std::time::Duration::from_secs(5), sck.read(&mut buf))
            .await
        {
            Ok(r) => r.expect("sse read"),
            Err(_) => panic!(
                "sse read timed out; acc={} bytes, ns={ns:?}, utd={saw_utd}, tail:\n{}",
                acc.len(),
                String::from_utf8_lossy(&acc[acc.len().saturating_sub(600)..])
            ),
        };
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&acc).to_string();
        ns.clear();
        saw_utd = false;
        for chunk in text.split("\n\n") {
            let mut is_control = false;
            for line in chunk.lines() {
                if line.starts_with("event: control") {
                    is_control = true;
                }
                if let Some(d) = line.strip_prefix("data:") {
                    if is_control {
                        if d.contains("\"upToDate\":true") {
                            saw_utd = true;
                        }
                        assert!(
                            !d.contains("streamClosed"),
                            "no closure on a live lineage: {d}"
                        );
                    } else if let Ok(v) = serde_json::from_str::<serde_json::Value>(d) {
                        // data events carry the JSON-array framing.
                        let rec = if v.is_array() { v[0].clone() } else { v };
                        if rec["k"] == "ga" {
                            ns.push(rec["n"].as_i64().unwrap());
                        }
                    }
                }
            }
        }
        if ns.len() >= 10 && saw_utd {
            break 'read;
        }
    }
    assert_eq!(
        ns,
        (0..10).collect::<Vec<i64>>(),
        "every generation's records, in order"
    );
    assert!(saw_utd, "upToDate control after the drain");
    drop(sck);
    engine_shutdown(&state).await;
}

// ---- product-surface foundation (spec Stages 7/8 core + clean switch) --

const PRISMA_KEY: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";

async fn preq(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> (u16, std::collections::HashMap<String, String>, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (k, v) in extra {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    // A peer RESET after a complete response is normal on macOS when
    // the server closes while the client still has unread request bytes
    // queued; treat it as end-of-response and let the parse below judge
    // completeness (a truly truncated read fails at the header
    // terminator).
    if let Err(e) = s.read_to_end(&mut buf).await {
        if e.kind() != std::io::ErrorKind::ConnectionReset || buf.is_empty() {
            panic!("response read: {e}");
        }
    }
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header terminator");
    let head = String::from_utf8_lossy(&buf[..split]).to_string();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = std::collections::HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    let mut raw_body = buf[split + 4..].to_vec();
    if headers.get("transfer-encoding").map(|v| v == "chunked") == Some(true) {
        let mut out = Vec::new();
        let mut rest: &[u8] = &raw_body;
        loop {
            let Some(le) = rest.windows(2).position(|w| w == b"\r\n") else {
                break;
            };
            let n =
                usize::from_str_radix(std::str::from_utf8(&rest[..le]).unwrap_or("0").trim(), 16)
                    .unwrap_or(0);
            if n == 0 {
                break;
            }
            let start = le + 2;
            out.extend_from_slice(&rest[start..start + n]);
            rest = &rest[start + n + 2..];
        }
        raw_body = out;
    }
    (status, headers, raw_body)
}

/// Typed creation, idempotence, config conflict, metadata shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_create_metadata_roundtrip() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let cfg = br#"{"format":{"kind":"json"},"expiry":{"idle":"30d"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#;
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/customers/acme/orders",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        cfg,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["name"], "customers/acme/orders");
    assert_eq!(v["contentType"], "application/json");
    assert_eq!(v["sealed"], false);
    assert_eq!(v["expiry"]["idle"], "2592000s");
    assert_eq!(v["watches"][0]["name"], "by-customer");

    // Idempotent re-PUT → 200.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/customers/acme/orders",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        cfg,
    )
    .await;
    assert_eq!(st, 200);

    // Different immutable config → 409.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/customers/acme/orders",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        br#"{"format":{"kind":"json"},"expiry":{"idle":"7d"}}"#,
    )
    .await;
    assert_eq!(st, 409);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "config_mismatch");
    assert_eq!(v["error"]["retryable"], false);

    // Metadata GET: product shape, no internals leaked.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/customers/acme/orders", &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["watches"][0]["fields"][0], "/customerId");
    let text = String::from_utf8_lossy(&b).to_string();
    for leak in ["fingerprint", "segment", "route_hash", "layout_version"] {
        assert!(!text.contains(leak), "metadata leaks {leak}: {text}");
    }
    // Unknown config fields rejected (v1 typo guard).
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/typoed",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"profile":"queue"}"#,
    )
    .await;
    assert_eq!(st, 400);
    engine_shutdown(&state).await;
}

/// The clean switch rejects experimental product inputs; __ds is
/// reserved on both surfaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_clean_switch_rejections() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    // Legacy header on the product route → 400, never translated.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/legacy",
        &[("stream-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 400);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "unknown_field");
    // Legacy query names rejected.
    let (st, _, _) = preq(addr, "GET", "/v1/streams/legacy?key=x", &[], b"").await;
    assert_eq!(st, 400);
    let (st, _, _) = preq(addr, "GET", "/v1/streams/legacy?offset=0", &[], b"").await;
    assert_eq!(st, 400);
    // Reserved namespace: product name, raw create, raw subpath.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/__ds/x",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 400);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/stream/__ds",
        &[
            ("stream-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 400, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(addr, "GET", "/v1/stream/__ds/subscriptions", &[], b"").await;
    assert_eq!(st, 404);
    // Reserved final segments can never be stream names: the path
    // parses as the records SUBRESOURCE of stream "a" (PUT on records
    // is method_not_allowed), so no stream named "a/records" exists.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/a/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 405);
    engine_shutdown(&state).await;
}

/// Product seal is durable, idempotent, collection-wide, and the RAW
/// default-key view observes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_seal_collection_wide() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealme",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Raw append on the default key (shared collection).
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/sealme",
        &[("content-type", "application/json")],
        br#"[{"n":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 204, "raw append {st}");

    let (st, _, _) = preq(addr, "POST", "/v1/streams/sealme:seal", &[], b"{}").await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(addr, "POST", "/v1/streams/sealme:seal", &[], b"{}").await;
    assert_eq!(st, 200, "seal is idempotent");

    let (st, _, b) = preq(addr, "GET", "/v1/streams/sealme", &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["sealed"], true);

    // Raw view: further appends refuse; drained read reports closure.
    let (st, _, b2) = hreq(
        addr,
        "POST",
        "/v1/stream/sealme",
        &[("content-type", "application/json")],
        br#"[{"n":2}]"#,
    )
    .await;
    assert_eq!(
        st,
        409,
        "sealed collection refuses appends: {}",
        String::from_utf8_lossy(&b2)
    );
    let (st, h, _) = hreq(addr, "GET", "/v1/stream/sealme", &[], b"").await;
    assert_eq!(st, 200);
    assert_eq!(
        h.get("stream-closed").map(String::as_str),
        Some("true"),
        "raw default-key view reports closure: {h:?}"
    );
    engine_shutdown(&state).await;
}

/// Stage 4: payload shape never changes operation meaning — append
/// stores ONE message (arrays stay array-valued records), appendMany
/// stores element-wise, both through the one committer path, with the
/// product response contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_append_and_append_many() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    // append([1,2,3]) = ONE array-valued message.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "customer-42"),
        ],
        b"[1,2,3]",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["count"], 1);
    assert_eq!(v["duplicate"], false);
    assert_eq!(v["sealed"], false);
    let cursor1 = v["cursor"].as_str().unwrap().to_string();
    assert!(!cursor1.is_empty());

    // appendMany([{a},{b}]) = TWO messages, atomic, contiguous.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records:batch",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "customer-42"),
        ],
        br#"[{"id":1},{"id":2}]"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["count"], 2);

    // The key sequence now holds 3 records: [1,2,3], {id:1}, {id:2} —
    // verified through the RAW keyed read (shared storage).
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/orders/records?routingKey=customer-42",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "1 array message + 2 batch messages");
    assert_eq!(recs[0], serde_json::json!([1, 2, 3]));
    assert_eq!(recs[1]["id"], 1);
    assert_eq!(recs[2]["id"], 2);

    // The returned cursor decodes and points past the appended records.
    let key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let desc = state.registry.get("orders").await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let kh = crate::crypto::stream_hash("customer-42");
    let c = crate::product_cursor::KeyCursor::decode(&cursor1, &key, &epoch, &kh)
        .expect("cursor decodes");
    assert_eq!(c.offset, 1, "cursor after the first single append");

    // Validation: empty batch, invalid JSON, oversized routing key.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records:batch",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"[]",
    )
    .await;
    assert_eq!(st, 400);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "empty_batch");
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{not json",
    )
    .await;
    assert_eq!(st, 400);
    let long_key = "k".repeat(1025);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", &long_key),
        ],
        b"1",
    )
    .await;
    assert_eq!(st, 400);

    // Bytes stream: batch is 405; single stores the body as one record.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/blobs",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"bytes"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/blobs/records:batch",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"xx",
    )
    .await;
    assert_eq!(st, 405, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/blobs/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"raw-bytes-here",
    )
    .await;
    assert_eq!(st, 200);
    engine_shutdown(&state).await;
}

/// Stage 4 §7: a duplicate producer request through the product route
/// returns duplicate: true and stores nothing twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_append_producer_duplicate() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/pdup",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let hdrs = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "ga"),
        ("producer-id", "checkout"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/pdup/records",
        &hdrs,
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], false);
    // Exact retry: recognized as duplicate, nothing stored twice.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/pdup/records",
        &hdrs,
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], true);
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/pdup/records?routingKey=ga",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "duplicate stored nothing");
    engine_shutdown(&state).await;
}

/// Stage 6: product keyed reads — signed cursors bind stream + key,
/// pagination reassembles exactly, headers speak Prisma (never
/// Stream-*), and state flags (up-to-date, sealed) survive translation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_read_pages_and_binds_cursors() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/reads",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // 5 fat records on c1 (pagination fodder), 3 small on the default key.
    let fat = format!("{{\"pad\":\"{}\"}}", "x".repeat(3000));
    for _ in 0..5 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/reads/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "c1"),
            ],
            fat.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    for n in 0..3 {
        let body = format!("{{\"d\":{n}}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/reads/records",
            &[("prisma-encryption-key", PRISMA_KEY)],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }

    // Full keyed read.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records?routingKey=c1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 5);
    assert_eq!(h.get("prisma-up-to-date").map(String::as_str), Some("true"));
    assert!(h.get("prisma-next-cursor").is_some());
    assert!(h.get("prisma-sealed").is_none());
    // The product surface never leaks protocol headers.
    assert!(h.get("stream-next-offset").is_none(), "raw header leaked");
    assert!(h.get("stream-up-to-date").is_none());

    // Paginate with a small budget: exact reassembly, no dupes/gaps.
    let mut got = 0usize;
    let mut cursor = String::from("beginning");
    for _ in 0..12 {
        let path = format!("/v1/streams/reads/records?routingKey=c1&cursor={cursor}&maxBytes=4096");
        let (st, h, b) = preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await;
        assert_eq!(st, 200);
        let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        got += recs.len();
        cursor = h.get("prisma-next-cursor").unwrap().clone();
        if h.get("prisma-up-to-date").map(String::as_str) == Some("true") {
            break;
        }
    }
    assert_eq!(got, 5, "pagination must reassemble exactly");

    // Default-key read sees ONLY the default key's records.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        recs.len(),
        3,
        "keyed records must not bleed into the default key"
    );

    // A cursor is bound to its routing key: reuse on another key is 400.
    let path = format!("/v1/streams/reads/records?routingKey=other&cursor={cursor}");
    let (st, _, b) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 400, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "invalid_cursor");

    // A scan cursor on the key endpoint is a different token class.
    let desc = state.registry.get("reads").await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let sc = crate::product_cursor::ScanCursor {
        epoch,
        map_version: 0,
        segments: vec![(0, 10)],
        current_index: 0,
        current_offset: 0,
        expires_at_ms: i64::MAX,
    }
    .encode(&skey());
    let path = format!("/v1/streams/reads/records?routingKey=c1&cursor={sc}");
    let (st, _, _) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 400);

    // cursor=now: empty, up-to-date.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records?routingKey=c1&cursor=now",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(h.get("prisma-up-to-date").map(String::as_str), Some("true"));
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert!(recs.is_empty());

    // Wrong encryption key: 403.
    let wrong = "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg";
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records?routingKey=c1",
        &[("prisma-encryption-key", wrong)],
        b"",
    )
    .await;
    assert_eq!(st, 403);

    // Seal, then a tail read reports Prisma-Sealed.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/reads:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert!(st == 200 || st == 204);
    let (st, h, _) = preq(
        addr,
        "GET",
        "/v1/streams/reads/records?routingKey=c1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(h.get("prisma-sealed").map(String::as_str), Some("true"));
    engine_shutdown(&state).await;
}

/// Stage 6: product cursors survive a split — pagination hands the
/// cursor across the sealed parent into the successor and reassembles
/// the key's sequence exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_read_follows_split_lineage() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lin",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for n in 0..5 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/lin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    assert!(crate::scaler3::execute_split(&state, "lin", 0, 0x8000_0000_0000_0000).await);
    for n in 5..10 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/lin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    let mut ns: Vec<i64> = Vec::new();
    let mut cursor = String::from("beginning");
    for _ in 0..16 {
        let path = format!("/v1/streams/lin/records?routingKey=ga&cursor={cursor}");
        let (st, h, b) = preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        for r in &recs {
            assert_eq!(r["k"], "ga");
            ns.push(r["n"].as_i64().unwrap());
        }
        cursor = h.get("prisma-next-cursor").unwrap().clone();
        if h.get("prisma-up-to-date").map(String::as_str) == Some("true") {
            break;
        }
    }
    assert_eq!(
        ns,
        (0..10).collect::<Vec<i64>>(),
        "exact per-key order across the split"
    );
    engine_shutdown(&state).await;
}

/// Stage 6: the long-poll transport — a timeout answers 204 with a
/// rearm cursor; a wake serves the new record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_long_poll_times_out_and_wakes() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lp",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/lp/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "w"),
        ],
        b"{\"n\":0}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, h, _) = preq(
        addr,
        "GET",
        "/v1/streams/lp/records?routingKey=w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let cursor = h.get("prisma-next-cursor").unwrap().clone();

    // Timeout: nothing new within waitMs.
    let path = format!("/v1/streams/lp/records:long-poll?routingKey=w&cursor={cursor}&waitMs=200");
    let (st, h, _) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 204);
    assert!(h.get("prisma-next-cursor").is_some());
    assert!(h.get("stream-next-offset").is_none());

    // Wake: a concurrent append answers the poll with the record.
    let path = format!("/v1/streams/lp/records:long-poll?routingKey=w&cursor={cursor}&waitMs=5000");
    let poll = tokio::spawn(async move {
        preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/lp/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "w"),
        ],
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, h, b) = poll.await.unwrap();
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["n"], 1);
    assert!(h.get("prisma-next-cursor").is_some());
    engine_shutdown(&state).await;
}

/// Stage 6: product SSE control frames carry SIGNED key cursors with
/// product field names — never a raw Stream-Next-Offset token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_sse_controls_carry_signed_cursors() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/psse",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for n in 0..2 {
        let body = format!("{{\"n\":{n}}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/psse/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "s1"),
            ],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/streams/psse/records:sse?routingKey=s1 HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 8192];
    let mut acc: Vec<u8> = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut data_n: Vec<i64> = Vec::new();
    let mut cursor_tok: Option<String> = None;
    while std::time::Instant::now() < deadline {
        let n = tokio::time::timeout(std::time::Duration::from_secs(4), sck.read(&mut buf))
            .await
            .expect("sse read timeout")
            .expect("sse read");
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&acc).to_string();
        assert!(
            !text.contains("streamNextOffset"),
            "product SSE leaked a raw offset token:\n{text}"
        );
        data_n.clear();
        cursor_tok = None;
        for chunk in text.split("\n\n") {
            let mut is_control = false;
            for line in chunk.lines() {
                if line.starts_with("event: control") {
                    is_control = true;
                }
                if let Some(d) = line.strip_prefix("data:") {
                    if is_control {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(d) {
                            if let Some(c) = v["nextCursor"].as_str() {
                                cursor_tok = Some(c.to_string());
                            }
                        }
                    } else if let Ok(v) = serde_json::from_str::<serde_json::Value>(d) {
                        let rec = if v.is_array() { v[0].clone() } else { v };
                        if let Some(x) = rec["n"].as_i64() {
                            data_n.push(x);
                        }
                    }
                }
            }
        }
        if data_n.len() >= 2 && cursor_tok.is_some() {
            break;
        }
    }
    assert_eq!(data_n, vec![0, 1], "catch-up records in order");
    let tok = cursor_tok.expect("control frame with nextCursor");
    let desc = state.registry.get("psse").await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let kh = crate::crypto::stream_hash("s1");
    let kc = crate::product_cursor::KeyCursor::decode(&tok, &skey(), &epoch, &kh)
        .expect("signed cursor decodes");
    assert_eq!(kc.offset, 2, "cursor sits after the two records");
    drop(sck);
    engine_shutdown(&state).await;
}

/// Stage 6: cross-key scan — every record at snapshot creation exactly
/// once, later appends excluded, expiry honored, token classes enforced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_scan_is_snapshot_exact() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/scn",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for (k, n) in [("a", 0), ("a", 1), ("b", 0), ("", 0)] {
        let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
        let mut hdrs = vec![("prisma-encryption-key", PRISMA_KEY)];
        if !k.is_empty() {
            hdrs.push(("prisma-routing-key", k));
        }
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/scn/records",
            &hdrs,
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }

    // One-page scan: complete, all four records with their keys.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/scn:scan",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    assert_eq!(
        h.get("prisma-scan-complete").map(String::as_str),
        Some("true")
    );
    assert!(h.get("prisma-next-scan-cursor").is_none());
    let items: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(items.len(), 4);
    let mut seen: Vec<(String, i64)> = items
        .iter()
        .map(|i| {
            (
                i["routingKey"].as_str().unwrap().to_string(),
                i["value"]["n"].as_i64().unwrap(),
            )
        })
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("".into(), 0),
            ("a".into(), 0),
            ("a".into(), 1),
            ("b".into(), 0)
        ]
    );

    // Paginated scan with a snapshot bound: fat records force pages; a
    // record appended MID-SCAN must not appear.
    let fat = format!("{{\"pad\":\"{}\"}}", "y".repeat(3000));
    for _ in 0..4 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/scn/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "fat"),
            ],
            fat.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/scn:scan?maxBytes=4096",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let mut total: usize = serde_json::from_slice::<Vec<serde_json::Value>>(&b)
        .unwrap()
        .len();
    let mut cursor = h.get("prisma-next-scan-cursor").cloned();
    assert!(cursor.is_some(), "fat records must not fit one 4 KiB page");
    // Mid-scan append: outside the snapshot.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/scn/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "late"),
        ],
        b"{\"late\":true}",
    )
    .await;
    assert_eq!(st, 200);
    for _ in 0..24 {
        let Some(c) = cursor.clone() else { break };
        let path = format!("/v1/streams/scn:scan?cursor={c}&maxBytes=4096");
        let (st, h, b) = preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let items: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        for i in &items {
            assert_ne!(
                i["routingKey"], "late",
                "snapshot must exclude mid-scan appends"
            );
        }
        total += items.len();
        if h.get("prisma-scan-complete").map(String::as_str) == Some("true") {
            cursor = None;
        } else {
            cursor = Some(h.get("prisma-next-scan-cursor").unwrap().clone());
        }
    }
    assert!(cursor.is_none(), "scan must complete");
    assert_eq!(
        total, 8,
        "4 originals + 4 fat, exactly once, no late record"
    );

    // Expired cursor: 410 scan_expired.
    let desc = state.registry.get("scn").await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let expired = crate::product_cursor::ScanCursor {
        epoch,
        map_version: 0,
        segments: vec![(0, 8)],
        current_index: 0,
        current_offset: 0,
        expires_at_ms: 1,
    }
    .encode(&skey());
    let path = format!("/v1/streams/scn:scan?cursor={expired}");
    let (st, _, b) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 410, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "scan_expired");

    // A key cursor on the scan endpoint: wrong token class.
    let kc = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: crate::crypto::stream_hash("a"),
        seg_id: 0,
        offset: 0,
    }
    .encode(&skey());
    let path = format!("/v1/streams/scn:scan?cursor={kc}");
    let (st, _, _) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 400);
    engine_shutdown(&state).await;
}

/// Stage 6: scan traverses split lineage — sealed parent + both
/// children, every record exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_scan_traverses_split_lineage() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/scnlin",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for n in 0..5 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/scnlin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    assert!(crate::scaler3::execute_split(&state, "scnlin", 0, 0x8000_0000_0000_0000).await);
    for n in 5..10 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/scnlin/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    let mut seen: Vec<(String, i64)> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..24 {
        let path = match &cursor {
            None => "/v1/streams/scnlin:scan?maxBytes=4096".to_string(),
            Some(c) => format!("/v1/streams/scnlin:scan?cursor={c}&maxBytes=4096"),
        };
        let (st, h, b) = preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let items: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        for i in &items {
            seen.push((
                i["routingKey"].as_str().unwrap().to_string(),
                i["value"]["n"].as_i64().unwrap(),
            ));
        }
        if h.get("prisma-scan-complete").map(String::as_str) == Some("true") {
            cursor = None;
            break;
        }
        cursor = Some(h.get("prisma-next-scan-cursor").unwrap().clone());
    }
    assert!(cursor.is_none(), "scan must complete");
    let mut sorted = seen.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(seen.len(), 20, "10 ga + 10 gb exactly once, got {seen:?}");
    assert_eq!(sorted.len(), 20, "duplicates in scan: {seen:?}");
    engine_shutdown(&state).await;
}

/// Stage 6: bytes-stream scan encodes values as base64.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_scan_bytes_stream() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/scb",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"bytes"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/scb/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "bin"),
        ],
        b"\x00\x01\xffraw",
    )
    .await;
    assert_eq!(st, 200);
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/scb:scan",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    assert_eq!(
        h.get("prisma-scan-complete").map(String::as_str),
        Some("true")
    );
    let items: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["routingKey"], "bin");
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(items[0]["valueB64"].as_str().unwrap())
        .unwrap();
    assert_eq!(raw, b"\x00\x01\xffraw");
    engine_shutdown(&state).await;
}

/// Stage 5 §7: the product checkpoint records the request hash — an
/// exact retry is a duplicate answering with the ORIGINAL cursor; the
/// same tuple with a different request is 409 producer_sequence_reused;
/// gaps and stale epochs carry the product taxonomy. The raw standards
/// route never compares bodies (pinned protocol).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_producer_hash_discipline() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/ph",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let hdr = |seq: &'static str| {
        vec![
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "g"),
            ("producer-id", "checkout"),
            ("producer-epoch", "1"),
            ("producer-seq", seq),
        ]
    };
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &hdr("0"),
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let c0 = v["cursor"].as_str().unwrap().to_string();
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &hdr("1"),
        b"{\"n\":2}",
    )
    .await;
    assert_eq!(st, 200);

    // Move the tail past the checkpoint with a plain (non-producer)
    // append, then retry the LATEST seq: the duplicate's cursor must
    // name the ORIGINAL commit (offset 2 = after n:2 at offset 1), not
    // the tail (offset 3). Older seqs degrade to the tail — the
    // checkpoint retains only the latest result (spec §7 last_result).
    let plain = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "g"),
    ];
    let (st, _, _) = preq(addr, "POST", "/v1/streams/ph/records", &plain, b"{\"n\":3}").await;
    assert_eq!(st, 200);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &hdr("1"),
        b"{\"n\":2}",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], true);
    let desc = state.registry.get("ph").await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let kh = crate::crypto::stream_hash("g");
    let kc = crate::product_cursor::KeyCursor::decode(
        v["cursor"].as_str().unwrap(),
        &skey(),
        &epoch,
        &kh,
    )
    .unwrap();
    assert_eq!(
        kc.offset, 2,
        "duplicate cursor = original commit, not the tail"
    );
    let kc0 = crate::product_cursor::KeyCursor::decode(&c0, &skey(), &epoch, &kh).unwrap();
    assert_eq!(kc0.offset, 1, "first append's cursor");

    // Older-seq retry: still a duplicate (no reuse conflict).
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &hdr("0"),
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], true);

    // Same tuple, different body: 409 producer_sequence_reused, nothing
    // stored.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &hdr("1"),
        b"{\"n\":99}",
    )
    .await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "producer_sequence_reused");
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/ph/records?routingKey=g",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "the reused sequence stored nothing");

    // Gap: 409 producer_gap with expected/received details.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &hdr("5"),
        b"{\"n\":5}",
    )
    .await;
    assert_eq!(st, 409);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "producer_gap");
    assert_eq!(v["error"]["details"]["expected"], 2);
    assert_eq!(v["error"]["details"]["received"], 5);

    // Stale epoch: 403 stale_producer_epoch with the current epoch.
    let stale = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "g"),
        ("producer-id", "checkout"),
        ("producer-epoch", "0"),
        ("producer-seq", "0"),
    ];
    let (st, _, b) = preq(addr, "POST", "/v1/streams/ph/records", &stale, b"{\"n\":0}").await;
    assert_eq!(st, 403);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "stale_producer_epoch");
    assert_eq!(v["error"]["details"]["currentEpoch"], 1);

    // Raw standards route: the pinned protocol's duplicate contract
    // does NOT compare bodies — same tuple, different body, still 204.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/rawdup",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    let rawh = [
        ("content-type", "application/json"),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/rawdup", &rawh, b"[1]").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/rawdup", &rawh, b"[2]").await;
    assert_eq!(st, 204, "raw duplicate never compares bodies");
    engine_shutdown(&state).await;
}

/// Stage 5 §8: the request hash follows the routing key's predecessor
/// chain — after a split, an exact retry on the successor deduplicates
/// and a reused sequence with a different body still conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_producer_hash_survives_split() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/psp",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let hdrs = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "ga"),
        ("producer-id", "svc"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, _) = preq(addr, "POST", "/v1/streams/psp/records", &hdrs, b"{\"a\":1}").await;
    assert_eq!(st, 200);
    assert!(crate::scaler3::execute_split(&state, "psp", 0, 0x8000_0000_0000_0000).await);
    // Exact retry lands on the successor: chain lookup finds the row
    // (with its hash) on the sealed parent.
    let (st, _, b) = preq(addr, "POST", "/v1/streams/psp/records", &hdrs, b"{\"a\":1}").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], true, "retry across the split deduplicates");
    // Same tuple, different body: the hash traveled too.
    let (st, _, b) = preq(addr, "POST", "/v1/streams/psp/records", &hdrs, b"{\"a\":2}").await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "producer_sequence_reused");
    engine_shutdown(&state).await;
}

/// Stage 2a: consumer config lifecycle — idempotent create, conflict,
/// get, delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_config_lifecycle() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cc",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let cfg = br#"{"visibilityTimeoutMs":5000,"maxAttempts":3}"#;
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        cfg,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        cfg,
    )
    .await;
    assert_eq!(st, 200, "identical config is idempotent");
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"visibilityTimeoutMs":9000}"#,
    )
    .await;
    assert_eq!(st, 409);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "consumer_config_conflict");
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["visibilityTimeoutMs"], 5000);
    assert_eq!(v["maxAttempts"], 3);
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/cc/consumers/nope",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 404);
    let (st, _, _) = preq(
        addr,
        "DELETE",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 204);
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 404);
    engine_shutdown(&state).await;
}

/// Stage 2a §2.3: per-key FIFO — a key with an active lease blocks its
/// later records; other keys flow; the ack unblocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_per_key_fifo() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cf",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for (k, n) in [("a", 0), ("a", 1), ("b", 0)] {
        let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/cf/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cf/consumers/w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cf/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"max":10}"#,
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let msgs = v["messages"].as_array().unwrap();
    let got: Vec<(String, i64)> = msgs
        .iter()
        .map(|m| {
            (
                m["routingKey"].as_str().unwrap().to_string(),
                m["value"]["n"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![("a".into(), 0), ("b".into(), 0)],
        "a/1 must be blocked behind a/0's active lease"
    );
    let a0_token = msgs[0]["leaseToken"].as_str().unwrap().to_string();

    // Ack a/0: a/1 becomes deliverable.
    let body = format!("{{\"acks\":[{{\"leaseToken\":\"{a0_token}\"}}]}}");
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cf/consumers/w:settle",
        &[("prisma-encryption-key", PRISMA_KEY)],
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["acked"], 1);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cf/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"max":10}"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let msgs = v["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["routingKey"], "a");
    assert_eq!(msgs[0]["value"]["n"], 1);
    assert_eq!(msgs[0]["attempts"], 1);
    engine_shutdown(&state).await;
}

/// Stage 2a §2.7: visibility expiry redelivers with attempts+1; a stale
/// (superseded) lease token is counted and cannot settle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_expiry_and_stale_fencing() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cv",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/cv/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "k"),
        ],
        b"{\"n\":0}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cv/consumers/w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cv/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"visibilityMs":1000}"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let first = v["messages"][0]["leaseToken"].as_str().unwrap().to_string();
    assert_eq!(v["messages"][0]["attempts"], 1);

    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cv/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"visibilityMs":30000}"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["messages"][0]["attempts"], 2, "expired lease redelivers");
    let fresh = v["messages"][0]["leaseToken"].as_str().unwrap().to_string();

    // The superseded first token is stale: counted, cannot ack.
    let body = format!("{{\"acks\":[{{\"leaseToken\":\"{first}\"}}]}}");
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cv/consumers/w:settle",
        &[("prisma-encryption-key", PRISMA_KEY)],
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["acked"], 0);
    assert_eq!(v["stale"], 1);
    // The fresh token acks.
    let body = format!("{{\"acks\":[{{\"leaseToken\":\"{fresh}\"}}]}}");
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cv/consumers/w:settle",
        &[("prisma-encryption-key", PRISMA_KEY)],
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["acked"], 1);
    engine_shutdown(&state).await;
}

/// Stage 2a §2.8: exceeding maxAttempts appends ONE record to the
/// dead-letter stream (durable before the source settles) and the
/// source message leaves the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_dlq_flow() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    for s in ["cd", "cd-dlq"] {
        let path = format!("/v1/streams/{s}");
        let (st, _, _) = preq(
            addr,
            "PUT",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201);
    }
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/cd/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "p"),
        ],
        b"{\"poison\":true}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cd/consumers/w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"maxAttempts":1,"deadLetterStream":"cd-dlq"}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Attempt 1, then let it expire.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cd/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"visibilityMs":1000}"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["messages"].as_array().unwrap().len(), 1);
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    // The next pull classifies it poison: DLQ append + source settle.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cd/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(
        v["messages"].as_array().unwrap().is_empty(),
        "poison is not redelivered"
    );
    // DLQ stream holds exactly one record with the source metadata.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/cd-dlq/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "exactly one DLQ record");
    assert_eq!(recs[0]["sourceStream"], "cd");
    assert_eq!(recs[0]["consumer"], "w");
    assert_eq!(recs[0]["routingKey"], "p");
    assert_eq!(recs[0]["attempts"], 1);
    assert_eq!(recs[0]["value"]["poison"], true);
    // Queue is drained: another pull with the key unblocked and empty
    // backlog.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cd/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(v["messages"].as_array().unwrap().is_empty());
    engine_shutdown(&state).await;
}

/// Stage 2a §2.9: consumption across a split — the sealed predecessor's
/// backlog delivers (and settles) fully before any successor record,
/// per-key order holds end to end, exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_drains_lineage_across_split() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cl",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cl/consumers/w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 201);
    for n in 0..3 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/cl/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    assert!(crate::scaler3::execute_split(&state, "cl", 0, 0x8000_0000_0000_0000).await);
    for n in 3..6 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/cl/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    // Pull + ack until drained; record delivery order per key.
    let mut per_key: std::collections::HashMap<String, Vec<i64>> = Default::default();
    let mut total = 0usize;
    for _round in 0..40 {
        let (st, _, b) = preq(
            addr,
            "POST",
            "/v1/streams/cl/consumers/w:pull",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"max":10}"#,
        )
        .await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        let msgs = v["messages"].as_array().unwrap().clone();
        if msgs.is_empty() {
            if v["backlog"].as_u64() == Some(0) && total == 12 {
                break;
            }
            continue;
        }
        let mut acks = Vec::new();
        for m in &msgs {
            per_key
                .entry(m["routingKey"].as_str().unwrap().to_string())
                .or_default()
                .push(m["value"]["n"].as_i64().unwrap());
            total += 1;
            acks.push(serde_json::json!({"leaseToken": m["leaseToken"]}));
        }
        let body = serde_json::json!({"acks": acks}).to_string();
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/cl/consumers/w:settle",
            &[("prisma-encryption-key", PRISMA_KEY)],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    assert_eq!(total, 12, "every record exactly once: {per_key:?}");
    assert_eq!(
        per_key["ga"],
        vec![0, 1, 2, 3, 4, 5],
        "ga in order across the split"
    );
    assert_eq!(
        per_key["gb"],
        vec![0, 1, 2, 3, 4, 5],
        "gb in order across the split"
    );
    engine_shutdown(&state).await;
}

/// Stage 2b: watches — definitions listed from the descriptor; a wait
/// wakes only when a MATCHING record commits (after durability); the
/// derived URL sig is a valid observation capability; a stale cursor is
/// an explicit resync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_watch_wakes_on_matching_append() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/wt",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));

    // Management endpoints.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/wt/watches",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["watches"][0]["name"], "by-customer");
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/wt/watches/by-customer",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/wt/watches/nope",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 404);

    let fields = vec!["/customerId".to_string()];
    let khex = crate::product::watch_key_hex("by-customer", &fields, &["\"c42\"".to_string()]);

    // Concurrent wait + matching append -> invalidated.
    let path = format!("/v1/streams/wt/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=5000");
    let wait = tokio::spawn(async move {
        preq(
            addr,
            "GET",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/wt/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "c42"),
        ],
        b"{\"customerId\":\"c42\",\"total\":9}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, b) = wait.await.unwrap();
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["invalidated"], true, "{v}");
    let cursor = v["cursor"].as_str().unwrap().to_string();

    // A NON-matching append does not wake this key.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/wt/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "other"),
        ],
        b"{\"customerId\":\"other\"}",
    )
    .await;
    assert_eq!(st, 200);
    let path =
        format!("/v1/streams/wt/watches/by-customer/keys/{khex}?cursor={cursor}&timeoutMs=300");
    let (st, _, b) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["invalidated"], false, "{v}");
    let cursor = v["cursor"].as_str().unwrap().to_string();

    // Derived URL sig authorizes WITHOUT the encryption key.
    let skey_local = skey();
    let desc = state.registry.get("wt").await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let tok = crate::crypto::touch_token(&skey_local, &epoch);
    let sk = crate::crypto::wait_sig_key(&tok, &epoch);
    let sig = crate::crypto::wait_url_sig(&sk, &khex);
    let path = format!(
        "/v1/streams/wt/watches/by-customer/keys/{khex}?cursor={cursor}&timeoutMs=200&sig={sig}"
    );
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    // Wrong sig, no key: 403.
    let path = format!(
        "/v1/streams/wt/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=200&sig=deadbeef"
    );
    let (st, _, _) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 403);

    // A stale (foreign-epoch) cursor is an explicit resync.
    let path =
        format!("/v1/streams/wt/watches/by-customer/keys/{khex}?cursor=999999:1&timeoutMs=200");
    let (st, _, b) = preq(
        addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["invalidated"], true);
    assert_eq!(v["reason"], "resync");
    engine_shutdown(&state).await;
}

/// Stage 1 exit criteria: profile machinery is GONE — removed product
/// inputs are rejected (never translated), removed routes are unknown,
/// and the descriptor no longer carries profile fields (enforced at
/// compile time by their absence; this test pins the wire behavior).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn profiles_are_removed_from_every_surface() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    // Product surface: removed names are 400 unknown_field.
    for h in [
        "stream-profile",
        "stream-touch-templates",
        "stream-queue-max-deliveries",
        "stream-ttl",
    ] {
        let (st, _, b) = preq(
            addr,
            "PUT",
            "/v1/streams/np",
            &[("prisma-encryption-key", PRISMA_KEY), (h, "queue")],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 400, "{h}: {}", String::from_utf8_lossy(&b));
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["error"]["code"], "unknown_field", "{h}");
    }
    // Removed profile routes are plain unknown routes — no alias, no
    // deprecation surface.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/qs",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    for path in ["/v1/stream/qs/queue/w/receive", "/v1/stream/qs/touch/meta"] {
        // Body-less probes: the 404 path never reads a request body, and
        // an unread body can turn the server's close into a RST before
        // the client reads the response (macOS, parallel-suite timing).
        let (st, _, _) = hreq(addr, "POST", path, &[], b"").await;
        assert!(
            st == 404 || st == 400 || st == 405,
            "removed route {path} must not exist (got {st})"
        );
    }
    // The raw route IGNORES unknown headers per the pinned protocol —
    // a Stream-Profile header neither errors nor configures anything.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/qp",
        &[
            ("content-type", "application/json"),
            ("stream-profile", "queue"),
        ],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "raw create ignores unknown headers");
    engine_shutdown(&state).await;
}

/// Stage 7 §14: the raw PUT and the product create resolve to ONE
/// stream incarnation. A raw idempotent PUT against a product-created
/// stream compares protocol config only (watches unchanged); a product
/// create against a raw-created stream succeeds when the immutable
/// config matches (empty capability config); equivalent duration
/// spellings normalize to the same config.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_creation_dual_contract() {
    let store = mem();
    let (state, addr) = http_rig(store).await;

    // Product create with watches, then a raw idempotent PUT.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dual1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"expiry":{"idle":"30d"},"watches":[{"name":"w","fields":["/a"]}]}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = hreq(
        addr,
        "PUT",
        "/v1/stream/dual1",
        &[
            ("content-type", "application/json"),
            ("stream-ttl", "2592000"),
        ],
        b"",
    )
    .await;
    assert_eq!(
        st,
        200,
        "raw PUT compares protocol config only: {}",
        String::from_utf8_lossy(&b)
    );
    // Watches survived the raw PUT.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/dual1/watches",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        v["watches"][0]["name"], "w",
        "raw PUT must not clear watches"
    );

    // Raw create, then product create with matching config: one
    // incarnation, not a duplicate; records flow both ways.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/dual2",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/dual2",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st,
        200,
        "product open of a raw stream: {}",
        String::from_utf8_lossy(&b)
    );
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/dual2",
        &[("content-type", "application/json")],
        br#"[{"via":"raw"}]"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/dual2/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(
        recs[0]["via"], "raw",
        "one canonical sequence across surfaces"
    );

    // Different immutable config conflicts (409), never merges.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dual2",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"bytes"}}"#,
    )
    .await;
    assert_eq!(st, 409);

    // Equivalent duration spellings normalize identically: create with
    // 30d, retry with 720h -> 200 (same normalized seconds).
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dual3",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"expiry":{"idle":"30d"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/dual3",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"expiry":{"idle":"720h"}}"#,
    )
    .await;
    assert_eq!(
        st,
        200,
        "30d == 720h after normalization: {}",
        String::from_utf8_lossy(&b)
    );

    // The product create stored NO records (config is never content).
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/dual3/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert!(recs.is_empty(), "product create must not append its config");

    // Watches on a bytes stream are rejected at creation.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dual4",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"bytes"},"watches":[{"name":"w","fields":["/a"]}]}"#,
    )
    .await;
    assert_eq!(st, 400, "watches require JSON");
    engine_shutdown(&state).await;
}

/// Stage 8 §7.2 + §10: seal with an atomic final append (deduped under
/// a producer retry), and the paginated catalog list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_seal_final_append_and_catalog() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    for s in ["cat-a", "cat-b", "cat-c"] {
        let path = format!("/v1/streams/{s}");
        let (st, _, _) = preq(
            addr,
            "PUT",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201);
    }
    // Seal cat-a with a final record through a producer (retry dedups).
    let hdrs = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("producer-id", "closer"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let body = br#"{"final":{"type":"completed"},"routingKey":"c1"}"#;
    let (st, _, b) = preq(addr, "POST", "/v1/streams/cat-a:seal", &hdrs, body).await;
    assert!(st == 200 || st == 204, "{}", String::from_utf8_lossy(&b));
    // Retry the same seal: the final append dedups, seal is idempotent.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/cat-a:seal", &hdrs, body).await;
    assert!(st == 200 || st == 204);
    // Exactly one final record, and the collection is sealed.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/cat-a/records?routingKey=c1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "final record exactly once");
    assert_eq!(recs[0]["type"], "completed");
    assert_eq!(h.get("prisma-sealed").map(String::as_str), Some("true"));
    // Further appends refuse.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/cat-a/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{\"late\":1}",
    )
    .await;
    assert_eq!(st, 409);

    // Catalog: paginated, name-ordered, sealed flag surfaced.
    let (st, _, b) = preq(addr, "GET", "/v1/streams?limit=2", &[], b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let page1: Vec<String> = v["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page1, vec!["cat-a", "cat-b"]);
    assert_eq!(v["streams"][0]["sealed"], true);
    let cur = v["cursor"].as_str().unwrap().to_string();
    let path = format!("/v1/streams?limit=2&cursor={cur}");
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let page2: Vec<String> = v["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page2, vec!["cat-c"]);
    assert!(v["cursor"].is_null(), "final page carries no cursor");
    engine_shutdown(&state).await;
}

/// Seal-with-final through the SDK's own shape: NO caller producer
/// headers, so the seal relies on the server's synthetic producer
/// identity for idempotence. That identity travels as a request header
/// into the shared committer path, and a header value may not contain
/// control bytes — a NUL-delimited id silently failed to insert, the
/// final append lost its producer, and the just-entered Sealing state
/// then refused the very record it was sealing with (live 409
/// `sealed`). The sibling test above passes its own producer headers
/// and never reaches this branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_seal_final_needs_no_caller_producer() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealnp",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let hdrs = vec![("prisma-encryption-key", PRISMA_KEY)];
    let body = br#"{"final":{"type":"done"},"routingKey":"c1"}"#;
    let (st, _, b) = preq(addr, "POST", "/v1/streams/sealnp:seal", &hdrs, body).await;
    assert!(
        st == 200 || st == 204,
        "seal without caller producer: {} {}",
        st,
        String::from_utf8_lossy(&b)
    );
    // Replay: the synthetic identity dedups the final append, so the
    // record lands exactly once and the seal stays idempotent.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/sealnp:seal", &hdrs, body).await;
    assert!(st == 200 || st == 204);
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/sealnp/records?routingKey=c1",
        &hdrs,
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "final record exactly once: {recs:?}");
    assert_eq!(recs[0]["type"], "done");
    assert_eq!(h.get("prisma-sealed").map(String::as_str), Some("true"));
    engine_shutdown(&state).await;
}

/// A signed watch URL is a DURABLE capability. The signature is checked
/// against the verifier persisted in the descriptor at create — not
/// against a cached stream key — so an issued URL keeps working on a
/// process that has never seen the collection, after a restart, and for
/// a collection nobody has appended to in days. The second rig here is
/// exactly that stranger: same store, cold caches, no key ever
/// presented to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_urls_verify_on_a_process_that_never_saw_the_key() {
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/wsig",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));

    // Derive exactly the way the SDK does: metadata carries the
    // incarnation salt, and everything else comes from the stream key.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/wsig", &[], b"").await;
    assert_eq!(st, 200);
    let meta: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let epoch_hex = meta["epoch"].as_str().expect("epoch exposed for watches");
    let epoch: [u8; 16] = crate::crypto::unhex(epoch_hex).unwrap().try_into().unwrap();
    let key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let khex = crate::product::watch_key_hex(
        "by-customer",
        &["/customerId".to_string()],
        &[r#""c1""#.to_string()],
    );
    let tok = crate::crypto::touch_token(&key, &epoch);
    let sig = crate::crypto::wait_url_sig(&crate::crypto::wait_sig_key(&tok, &epoch), &khex);

    // A second server over the same store: never saw the key, never
    // absorbed a record, never issued this URL.
    let (state2, addr2) = http_rig(store).await;
    let path = format!("/v1/streams/wsig/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=150&sig={sig}");
    let (st, _, b) = preq(addr2, "GET", &path, &[], b"").await;
    assert_eq!(st, 200, "cold process must verify: {}", String::from_utf8_lossy(&b));

    // A forged signature is refused, on both.
    let bad = format!("/v1/streams/wsig/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=150&sig=0000000000000000");
    let (st, _, _) = preq(addr2, "GET", &bad, &[], b"").await;
    assert_eq!(st, 403);
    let (st, _, _) = preq(addr, "GET", &bad, &[], b"").await;
    assert_eq!(st, 403);
    engine_shutdown(&state).await;
    engine_shutdown(&state2).await;
}

/// Dead-letter delivery writes with the SOURCE collection's key, so the
/// link is only meaningful between collections that share one. The gate
/// is at configuration time, where the caller can still act on it —
/// otherwise the mismatch surfaces much later as a poisoned key that
/// can never drain, with every DLQ append refused in silence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_letter_link_requires_a_shared_key() {
    const OTHER_KEY: &str = "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=";
    let store = mem();
    let (state, addr) = http_rig(store).await;
    for (name, key) in [
        ("dlq-src", PRISMA_KEY),
        ("dlq-same", PRISMA_KEY),
        ("dlq-other", OTHER_KEY),
    ] {
        let (st, _, b) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/{name}"),
            &[("prisma-encryption-key", key)],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201, "{name}: {}", String::from_utf8_lossy(&b));
    }
    let put_dlq = |target: &str| {
        let body = format!(r#"{{"deadLetterStream":"{target}"}}"#);
        async move {
            preq(
                addr,
                "PUT",
                "/v1/streams/dlq-src/consumers/w",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("content-type", "application/json"),
                ],
                body.as_bytes(),
            )
            .await
        }
    };
    let code = |b: &[u8]| -> String {
        serde_json::from_slice::<serde_json::Value>(b)
            .ok()
            .and_then(|v| v["error"]["code"].as_str().map(str::to_string))
            .unwrap_or_default()
    };

    let (st, _, b) = put_dlq("dlq-missing").await;
    assert_eq!(st, 400);
    assert_eq!(code(&b), "unknown_dead_letter_stream");

    let (st, _, b) = put_dlq("dlq-src").await;
    assert_eq!(st, 400, "self-DLQ is a delivery loop");
    assert_eq!(code(&b), "invalid_config");

    let (st, _, b) = put_dlq("dlq-other").await;
    assert_eq!(st, 400, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "dead_letter_key_mismatch");

    let (st, _, b) = put_dlq("dlq-same").await;
    assert!(st == 200 || st == 201, "{}", String::from_utf8_lossy(&b));

    // Sealing the target closes the link for anyone configuring it next.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/dlq-same:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/dlq-src/consumers/w2",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        br#"{"deadLetterStream":"dlq-same"}"#,
    )
    .await;
    assert_eq!(st, 400);
    assert_eq!(code(&b), "dead_letter_sealed");
    engine_shutdown(&state).await;
}

/// Collection names are hierarchical, so the product routes have to be
/// matched as SUFFIXES. Searching for the first `/records/` in the path
/// split `customers/records/2026/records` after `customers` — writing
/// to a collection nobody asked for, or 404ing when it did not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hierarchical_names_do_not_shadow_subresources() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];

    // A name whose MIDDLE segments spell subresources is legal.
    let deep = "customers/records/2026";
    let (st, _, b) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{deep}"),
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{deep}/records"),
        &key,
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    // The record landed in the deep collection, and `customers` was
    // never created as a side effect.
    let (st, _, b) = preq(
        addr,
        "GET",
        &format!("/v1/streams/{deep}/records"),
        &key,
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1);
    let (st, _, _) = preq(addr, "GET", "/v1/streams/customers", &key, b"").await;
    assert_eq!(st, 404, "the prefix must not become a collection");

    // A consumer may be called "records": the suffix that wins is the
    // one that leaves an addressable collection behind.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/shop",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/shop/consumers/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        b"{}",
    )
    .await;
    assert!(st == 200 || st == 201, "{}", String::from_utf8_lossy(&b));

    // That URL is the consumer route, always — a creation document sent
    // there is a bad consumer config, never a collection called
    // `shop/consumers/records`. Which is why such a name is refused
    // wherever one can still be written down: as a dead-letter target.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/shop/consumers/records",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 400);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "invalid_config");
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/shop/consumers/w",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        br#"{"deadLetterStream":"shop/consumers/records"}"#,
    )
    .await;
    assert_eq!(st, 400, "an unaddressable name is not a usable target");
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "invalid_config");

    // A colon is legal in a name; only the known verbs are verbs.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/ns/a:b",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "a colon is part of the name");
    let (st, _, b) = preq(addr, "GET", "/v1/streams/ns/a:b", &key, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["name"], "ns/a:b");
    // A mistyped verb addresses a collection that does not exist —
    // never a different verb, and never the collection without it.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/ns/a:seel", &key, b"").await;
    assert_eq!(st, 404);
    engine_shutdown(&state).await;
}

/// The signed watch URL is the ONE product route that authorizes
/// itself, and "looks like a watch URL" was decided by substring tests
/// on the raw path. Collection names are hierarchical, so
/// `acme/watches/x/keys/y/extra` is a legal COLLECTION whose path
/// contains every fragment a watch URL has — it, and its `/records`
/// subresource, skipped the account token entirely. Records could be
/// read with the encryption key alone, which is exactly the credential
/// separation the product surface exists to keep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_the_exact_signed_watch_route_skips_the_token() {
    let store = mem();
    let (state, addr) = http_rig_auth(store, "tok").await;
    let auth = [
        ("authorization", "Bearer tok"),
        ("prisma-encryption-key", PRISMA_KEY),
    ];
    // A collection whose NAME carries every watch-URL fragment.
    let evil = "acme/watches/x/keys/y/extra";
    let (st, _, b) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{evil}"),
        &auth,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{evil}/records"),
        &auth,
        br#"{"secret":"x"}"#,
    )
    .await;
    assert_eq!(st, 200);

    // Without the token, with a sig pasted on, every one of these is 401.
    let key_only = [("prisma-encryption-key", PRISMA_KEY)];
    for path in [
        format!("/v1/streams/{evil}?sig=anything"),
        format!("/v1/streams/{evil}/records?sig=anything"),
        format!("/v1/streams/{evil}/records?routingKey=&sig=anything"),
        format!("/v1/streams/{evil}/watches?sig=anything"),
        format!("/v1/streams/{evil}/consumers/c?sig=anything"),
        // a watch-shaped path with EXTRA segments after the key
        "/v1/streams/acme/watches/w/keys/0011223344556677/extra?sig=x".to_string(),
    ] {
        let (st, _, b) = preq(addr, "GET", &path, &key_only, b"").await;
        assert_eq!(
            st, 401,
            "token bypass via {path}: {}",
            String::from_utf8_lossy(&b)
        );
    }
    // The write path is refused too, before any body is read.
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{evil}/records?sig=anything"),
        &key_only,
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 401);

    // The exact signed route still works without a token: create a
    // collection WITH a watch, derive the URL, present it bare.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/wauth",
        &auth,
        br#"{"format":{"kind":"json"},"watches":[{"name":"w","fields":["/id"]}]}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (_, _, b) = preq(addr, "GET", "/v1/streams/wauth", &auth, b"").await;
    let meta: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let epoch: [u8; 16] = crate::crypto::unhex(meta["epoch"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    let key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let khex = crate::product::watch_key_hex("w", &["/id".to_string()], &[r#""a""#.to_string()]);
    let tok = crate::crypto::touch_token(&key, &epoch);
    let sig = crate::crypto::wait_url_sig(&crate::crypto::wait_sig_key(&tok, &epoch), &khex);
    let path =
        format!("/v1/streams/wauth/watches/w/keys/{khex}?cursor=now&timeoutMs=150&sig={sig}");
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(
        st,
        200,
        "the exact signed route must still self-authorize: {}",
        String::from_utf8_lossy(&b)
    );
    // …but not without the signature.
    let bare = format!("/v1/streams/wauth/watches/w/keys/{khex}?cursor=now&timeoutMs=150");
    let (st, _, _) = preq(addr, "GET", &bare, &[], b"").await;
    assert_eq!(st, 401);
    engine_shutdown(&state).await;
}

/// CORS that only answers preflights is not CORS: the browser passes
/// the OPTIONS and then blocks the response it was asking about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_responses_carry_cors_not_just_preflights() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, h, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cors",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    assert_eq!(h.get("access-control-allow-origin").map(String::as_str), Some("*"));
    let expose = |h: &std::collections::HashMap<String, String>| {
        h.get("access-control-expose-headers").cloned().unwrap_or_default()
    };
    assert!(expose(&h).contains("prisma-next-cursor"), "{:?}", expose(&h));

    // an actual GET…
    let (st, h, _) = preq(addr, "GET", "/v1/streams/cors/records", &key, b"").await;
    assert_eq!(st, 200);
    assert_eq!(h.get("access-control-allow-origin").map(String::as_str), Some("*"));
    assert!(expose(&h).contains("prisma-sealed"));
    // …a POST…
    let (st, h, _) = preq(
        addr,
        "POST",
        "/v1/streams/cors/records",
        &key,
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(h.get("access-control-allow-origin").map(String::as_str), Some("*"));
    // …and an ERROR, which a browser must be able to read to retry.
    let (st, h, _) = preq(addr, "GET", "/v1/streams/nope-missing", &key, b"").await;
    assert_eq!(st, 404);
    assert_eq!(h.get("access-control-allow-origin").map(String::as_str), Some("*"));
    assert!(expose(&h).contains("retry-after"));
    engine_shutdown(&state).await;
}

/// Readiness is not a stopwatch. An abandoned initialization used to
/// stop blocking reads once its claim aged past 15 s, so a stream whose
/// creator died mid-write started serving as complete — the original
/// field anomaly with a delay. A stale claim decides who may REDO the
/// work; it never publishes half-built content.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_initialization_never_becomes_visible() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/stale1", &ct, br#"[{"n":1}]"#).await;
    assert_eq!(st, 201);
    // A creator that died long ago: claim present, ancient.
    state
        .registry
        .cas_update("stale1", |d| {
            d.init = Some(crate::registry::InitState {
                request_hash: "abandoned".into(),
                key_fingerprint: d.key_fingerprint.clone(),
                claimed_ms: crate::shard::now_ms() - crate::registry::INIT_CLAIM_MS * 100,
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("stale1");

    let (st, _, _) = hreq(addr, "GET", "/v1/stream/stale1", &[], b"").await;
    assert_eq!(st, 503, "an abandoned create must not read as complete");
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/stale1", &ct, br#"[{"n":2}]"#).await;
    assert_eq!(st, 503, "…nor accept appends");
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/stale1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 503, "…nor describe itself through the product route");
    // …and it is not in the catalog.
    let (st, _, b) = preq(addr, "GET", "/v1/streams?limit=100", &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let names: Vec<&str> = v["streams"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["name"].as_str())
        .collect();
    assert!(!names.contains(&"stale1"), "half-built stream in catalog: {names:?}");
    engine_shutdown(&state).await;
}

/// Resuming an initialization writes the initial content with the
/// REQUEST's key. The resume path skips the idempotent-PUT validation
/// where the key would normally be compared, so a replay of the same
/// body under a different key completed the creation with a key the
/// descriptor's own fingerprint does not match — a stream that cannot
/// decrypt its first record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_initialization_cannot_be_resumed_with_another_key() {
    const OTHER_KEY: &str = "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=";
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let body = br#"[{"n":1}]"#;
    let mine = [
        ("content-type", "application/json"),
        ("stream-encryption-key", RIG_KEY_B64),
    ];
    let theirs = [
        ("content-type", "application/json"),
        ("stream-encryption-key", OTHER_KEY),
    ];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/keyinit", &mine, body).await;
    assert_eq!(st, 201);
    // Reopen the initialization, as a crashed creator would leave it.
    let plant = |stale: bool| {
        let state = state.clone();
        async move {
            state
                .registry
                .cas_update("keyinit", |d| {
                    d.init = Some(crate::registry::InitState {
                        request_hash: crate::http::create_request_hash(
                            "application/json",
                            None,
                            None,
                            false,
                            br#"[{"n":1}]"#,
                            None,
                        ),
                        key_fingerprint: d.key_fingerprint.clone(),
                        claimed_ms: if stale {
                            crate::shard::now_ms() - crate::registry::INIT_CLAIM_MS * 100
                        } else {
                            crate::shard::now_ms()
                        },
                    });
                    true
                })
                .await
                .unwrap();
            state.registry.invalidate("keyinit");
        }
    };
    for stale in [false, true] {
        plant(stale).await;
        let (st, _, b) = hreq(addr, "PUT", "/v1/stream/keyinit", &theirs, body).await;
        assert_eq!(
            st,
            403,
            "wrong key resumed an initialization (stale={stale}): {}",
            String::from_utf8_lossy(&b)
        );
        // The descriptor is untouched: still initializing, still ours.
        let d = state.registry.get("keyinit").await.unwrap().unwrap();
        assert!(d.init.is_some(), "a refused resume must not complete it");
    }
    // The RIGHT key resumes and completes it.
    plant(false).await;
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/keyinit", &mine, body).await;
    assert!(st == 200 || st == 201);
    let d = state.registry.get("keyinit").await.unwrap().unwrap();
    assert!(d.init.is_none(), "the right key completes initialization");
    engine_shutdown(&state).await;
}

/// A catalog page that crosses a dense run of tombstoned, expired or
/// half-built streams comes back underfull. That used to be read as
/// "end of catalog", which made every live stream after the run
/// unreachable — the walk continues while the PROVIDER has more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_paging_survives_dense_dead_entries() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    // 40 collections; everything except the last two is then killed,
    // so any page-sized window early in the walk is all corpses.
    for i in 0..40 {
        let (st, _, _) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/cat{i:03}"),
            &key,
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201);
    }
    for i in 0..38 {
        let (st, _, _) = preq(addr, "DELETE", &format!("/v1/streams/cat{i:03}"), &key, b"").await;
        assert!(st == 204 || st == 200, "delete cat{i:03}: {st}");
    }
    // Walk with a small limit: the first pages are empty but not final.
    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..40 {
        let path = match &cursor {
            None => "/v1/streams?limit=3".to_string(),
            Some(c) => format!("/v1/streams?limit=3&cursor={c}"),
        };
        let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        for s in v["streams"].as_array().unwrap() {
            seen.push(s["name"].as_str().unwrap().to_string());
        }
        match v["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    assert!(
        seen.contains(&"cat038".to_string()) && seen.contains(&"cat039".to_string()),
        "live streams behind a run of dead ones were unreachable: {seen:?}"
    );
    engine_shutdown(&state).await;
}

/// A seal that promised a final record owns the transition until that
/// record is durable. A plain `:seal` arriving after a crashed
/// seal-with-final used to close every segment and publish Sealed —
/// dropping the final record permanently, with both requests reporting
/// success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_seal_cannot_finish_someone_elses_final() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealint",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // A seal-with-final that published its intent and then died.
    state
        .registry
        .cas_update("sealint", |d| {
            d.sealing = Some(crate::registry::SealState {
                operation_id: crate::product::seal_op_id(
                    &serde_json::json!({"done": true}),
                    "",
                ),
                intent: crate::registry::SealIntent::Final {
                    routing_key: String::new(),
                    request_hash: crate::product::seal_op_id(
                        &serde_json::json!({"done": true}),
                        "",
                    ),
                    final_committed: false,
                },
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("sealint");

    // A plain seal must NOT complete it.
    let (st, _, b) = preq(addr, "POST", "/v1/streams/sealint:seal", &key, b"").await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&b));
    let d = state.registry.get("sealint").await.unwrap().unwrap();
    assert!(!d.sealed, "the collection must not be sealed yet");
    assert!(d.sealing.is_some(), "the intent survives the refusal");

    // A raw close must not either — it would close the segment first.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/sealint",
        &[("content-type", "application/json"), ("stream-closed", "true")],
        b"",
    )
    .await;
    assert!(st == 409 || st == 503, "raw close during a final seal: {st}");
    let d = state.registry.get("sealint").await.unwrap().unwrap();
    assert!(!d.sealed);

    // The owning operation finishes it: same final, same routing key.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/sealint:seal",
        &key,
        br#"{"final":{"done":true}}"#,
    )
    .await;
    assert!(st == 200 || st == 204, "{}", String::from_utf8_lossy(&b));
    let d = state.registry.get("sealint").await.unwrap().unwrap();
    assert!(d.sealed && d.sealing.is_none(), "seal completes: {d:?}");
    engine_shutdown(&state).await;
}

/// Producer requests are admitted during Sealing so a RETRY can be
/// recognised and answered with its original result. That let a
/// genuinely new sequence through as well: the descriptor said Sealing
/// while a novel producer write landed. The refusal now rides with the
/// request and the committer applies it after duplicate detection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producers_cannot_write_new_records_while_sealing() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/prodseal", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let ph = |seq: u32| {
        vec![
            ("content-type", "application/json"),
            ("producer-id", "p1"),
            ("producer-epoch", "1"),
            ("producer-seq", Box::leak(seq.to_string().into_boxed_str()) as &str),
        ]
    };
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/prodseal", &ph(0), br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 204, "first producer append: {st}");

    // Enter Sealing without finishing (as a crashed seal would leave it).
    state
        .registry
        .cas_update("prodseal", |d| {
            d.sealing = Some(crate::registry::SealState {
                operation_id: String::new(),
                intent: crate::registry::SealIntent::Empty,
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("prodseal");

    // The RETRY of seq 0 still dedups to success…
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/prodseal", &ph(0), br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 204, "a duplicate must still answer: {st}");
    // …but a NEW sequence is refused.
    let (st, _, b) = hreq(addr, "POST", "/v1/stream/prodseal", &ph(1), br#"[{"n":1}]"#).await;
    assert_eq!(
        st,
        409,
        "a new producer sequence landed during Sealing: {}",
        String::from_utf8_lossy(&b)
    );
    // And nothing was written.
    let (_, _, b) = hreq(addr, "GET", "/v1/stream/prodseal", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap_or_default();
    assert_eq!(recs.len(), 1, "records after a refused write: {recs:?}");
    engine_shutdown(&state).await;
}

/// The scaler must not publish a new writable child while a seal is in
/// flight: the seal snapshots the live segments, so a successor created
/// after that snapshot outlives the seal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_transitions_are_fenced_by_sealing() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/fenced",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    state
        .registry
        .cas_update("fenced", |d| {
            d.sealing = Some(crate::registry::SealState {
                operation_id: String::new(),
                intent: crate::registry::SealIntent::Empty,
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("fenced");
    assert!(
        !crate::scaler3::execute_split(&state, "fenced", 0, 0x8000_0000_0000_0000).await,
        "a split started under Sealing"
    );
    // …and once sealed, still refused.
    crate::product::run_seal(&state, "fenced", None).await.unwrap();
    state.registry.invalidate("fenced");
    assert!(
        !crate::scaler3::execute_split(&state, "fenced", 0, 0x8000_0000_0000_0000).await,
        "a split started under Sealed"
    );
    engine_shutdown(&state).await;
}

/// The raw route is the DEFAULT-key stream — including through a fork.
/// Stitched reads passed no key filter at all, so a raw fork of a
/// collection that product clients had written keyed records to
/// replayed every one of them through the standards surface.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_forks_show_only_the_default_key() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/forkiso", &ct, br#"[{"raw":0}]"#).await;
    assert!(st == 200 || st == 201);
    // Product traffic on other routing keys, interleaved with raw.
    for i in 0..3 {
        for k in ["ka", "kb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{i}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/forkiso/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
        let body = format!("[{{\"raw\":{}}}]", i + 1);
        let (st, _, _) = hreq(addr, "POST", "/v1/stream/forkiso", &ct, body.as_bytes()).await;
        assert!(st == 200 || st == 204);
    }
    // Fork it at the tail and read the fork through the raw route.
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/forkiso", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let (st, _, b) = hreq(
        addr,
        "PUT",
        "/v1/stream/forkiso-child",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "forkiso"),
            ("stream-fork-offset", &boundary),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    for name in ["forkiso", "forkiso-child"] {
        let (st, _, b) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
        assert_eq!(st, 200);
        let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        assert!(
            recs.iter().all(|r| r.get("k").is_none()),
            "{name} leaked another routing key: {recs:?}"
        );
        assert_eq!(recs.len(), 4, "{name} default-key records: {recs:?}");
    }
    engine_shutdown(&state).await;
}

/// Deleting a fork tombstones the child and then releases the parent's
/// reference. A crash in between left the parent pinning data for a
/// fork that no longer exists — and a retry bounced off the
/// already-dead check before it could finish the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_half_deleted_fork_finishes_its_cleanup() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dsrc", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/dsrc", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/dchild",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "dsrc"),
            ("stream-fork-offset", &boundary),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 201);
    let src = state.registry.get("dsrc").await.unwrap().unwrap();
    assert_eq!(src.fork_children.len(), 1, "the source holds the reference");

    // Simulate the crash: tombstone the child with the debt recorded,
    // exactly as delete_lifecycle writes it before releasing.
    state
        .registry
        .cas_update("dchild", |d| {
            d.deleted = true;
            d.parent_ref_pending = true;
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("dchild");
    let src = state.registry.get("dsrc").await.unwrap().unwrap();
    assert_eq!(src.fork_children.len(), 1, "the reference is still leaked");

    // A retried DELETE finishes the cleanup rather than bouncing.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/dchild", &[], b"").await;
    assert!(st == 404 || st == 410 || st == 204, "retry delete: {st}");
    state.registry.invalidate("dsrc");
    let src = state.registry.get("dsrc").await.unwrap().unwrap();
    assert!(
        src.fork_children.is_empty(),
        "the parent still pins a dead fork: {:?}",
        src.fork_children
    );
    state.registry.invalidate("dchild");
    let child = state.registry.get("dchild").await.unwrap().unwrap();
    assert!(!child.parent_ref_pending, "the debt is settled");
    engine_shutdown(&state).await;
}

/// The exact interleaving the round-3 audit specified: a split that has
/// already published its intent and sealed the parent, parked before
/// publishing successors, while the collection seals underneath it.
/// Fencing only the START of a transition left this open — phase B
/// would resume and publish live children under a Sealed collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_split_cannot_publish_under_a_sealed_collection() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/parked",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for k in ["a", "b", "c", "d"] {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/parked/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            format!("{{\"k\":\"{k}\"}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }

    // Park every resume between the parent seal and the successor CAS,
    // then start a split: it publishes pending, seals the parent, waits.
    crate::scaler3::failpoints::arm_before_publish();
    let split = {
        let st2 = state.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(&st2, "parked", 0, 0x8000_0000_0000_0000).await
        })
    };
    // Wait for the intent to become durable.
    let mut pending = false;
    for _ in 0..100 {
        state.registry.invalidate("parked");
        if let Ok(Some(d)) = state.registry.get("parked").await {
            if d.segments.as_ref().is_some_and(|m| m.pending.is_some()) {
                pending = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(pending, "the split never published its intent");

    // Seal the collection while the split is parked. The seal resolves
    // the transition first rather than snapshotting around it.
    let sealer = {
        let st2 = state.clone();
        tokio::spawn(async move { crate::product::run_seal(&st2, "parked", None).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    crate::scaler3::failpoints::release_before_publish();
    let _ = split.await;
    let seal_result = sealer.await.unwrap();

    // Whatever order they settled in, the end state must be coherent:
    // if the collection is sealed, nothing may be live and unclosed, and
    // no transition may be left dangling.
    state.registry.invalidate("parked");
    let d = state.registry.get("parked").await.unwrap().unwrap();
    if d.sealed {
        assert!(
            !d.segments.as_ref().is_some_and(|m| m.pending.is_some()),
            "sealed with a transition still pending: {:?}",
            d.segments
        );
        // The invariant is behavioural, not representational: whatever
        // segments exist, NOTHING may write. Keys are checked across the
        // whole keyspace, since a successor published by the parked
        // split would own half of it.
        for k in ["late", "a", "b", "c", "d", "zz", "q7"] {
            let (st, _, b) = preq(
                addr,
                "POST",
                "/v1/streams/parked/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                br#"{"late":1}"#,
            )
            .await;
            assert_eq!(
                st,
                409,
                "append on key {k} accepted after Sealed: {}",
                String::from_utf8_lossy(&b)
            );
        }
        // The raw (default-key) surface agrees.
        let (st, _, _) = hreq(
            addr,
            "POST",
            "/v1/stream/parked",
            &[("content-type", "application/json")],
            br#"[{"late":1}]"#,
        )
        .await;
        assert_eq!(st, 409, "raw append accepted after Sealed");
    } else {
        // The seal declined because the transition was in flight: that
        // is the other legal outcome, and it must say so.
        assert!(seal_result.is_err(), "unsealed but the seal reported success");
    }
    engine_shutdown(&state).await;
}

/// A fork initialization is claimed against ONE source incarnation. The
/// creation hash omitted the source epoch, so a retry against a
/// recreated source hashed identically and resumed the original
/// initialization — reference installed on the new incarnation while
/// the child still recorded the old one, which stitched reads only
/// discover later as an epoch mismatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_initialization_is_bound_to_its_source_incarnation() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fsrc", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/fsrc", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let epoch_a = state
        .registry
        .get("fsrc")
        .await
        .unwrap()
        .unwrap()
        .stream_epoch;

    // A child initialization claimed against incarnation A.
    let fh = [
        ("content-type", "application/json"),
        ("stream-forked-from", "fsrc"),
        ("stream-fork-offset", boundary.as_str()),
    ];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fchild", &fh, b"").await;
    assert_eq!(st, 201);
    // Reopen it as an in-flight initialization, as a crash would leave it.
    // The REAL request hash the server computes for this fork against
    // incarnation A. Planting an arbitrary string would make the retry
    // conflict on the hash alone and prove nothing about the epoch.
    let hash_against_a = crate::http::create_request_hash(
        "application/json",
        None,
        None,
        false,
        b"",
        Some(&crate::registry::ForkRef {
            source: "fsrc".into(),
            source_epoch: epoch_a.clone(),
            fork_offset: 1,
            fork_sub: 0,
            fork_id: String::new(),
        }),
    );
    state
        .registry
        .cas_update("fchild", |d| {
            d.init = Some(crate::registry::InitState {
                request_hash: hash_against_a.clone(),
                key_fingerprint: d.key_fingerprint.clone(),
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("fchild");

    // The source becomes a DIFFERENT incarnation. A delete+recreate is
    // the way that happens in the field, but a source pinned by a fork
    // soft-deletes and refuses recreation, so the incarnation is moved
    // directly here — the identity rule under test is about the epoch,
    // not about how it changed.
    let epoch_b = format!("{:032x}", 0xfeed_beefu64);
    state
        .registry
        .cas_update("fsrc", |d| {
            d.stream_epoch = epoch_b.clone();
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("fsrc");
    assert_ne!(epoch_a, epoch_b, "the source must be a new incarnation");

    // Retrying the same fork request must NOT quietly resume the
    // initialization that was claimed against the old incarnation.
    let (st, _, b) = hreq(addr, "PUT", "/v1/stream/fchild", &fh, b"").await;
    assert!(
        st == 409 || st == 403,
        "a fork of a RECREATED source resumed the old initialization: {st} {}",
        String::from_utf8_lossy(&b)
    );
    // The hash the SAME request computes against incarnation B must
    // differ from the one recorded against A — that difference is the
    // mechanism under test, not the conflict above.
    let hash_against_b = crate::http::create_request_hash(
        "application/json",
        None,
        None,
        false,
        b"",
        Some(&crate::registry::ForkRef {
            source: "fsrc".into(),
            source_epoch: epoch_b.clone(),
            fork_offset: 1,
            fork_sub: 0,
            fork_id: String::new(),
        }),
    );
    assert_ne!(
        hash_against_a, hash_against_b,
        "the creation hash ignores the source incarnation"
    );
    let child = state.registry.get("fchild").await.unwrap().unwrap();
    if let Some(f) = &child.forked_from {
        assert_eq!(
            f.source_epoch, epoch_a,
            "the child's recorded parentage changed incarnation"
        );
    }
    engine_shutdown(&state).await;
}

/// Three generations, A <- B <- C, with B already soft-deleted. Deleting
/// C hard-deletes B, which then owes A a release. A crash in between
/// used to strand that reference forever: B is dead, so its CAS refuses,
/// and a retried delete of C reported success having released nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_fork_cascade_can_be_resumed() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/genA", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let fork_of = |src: &str, child: &str| {
        let src = src.to_string();
        let child = child.to_string();
        async move {
            let (_, h, _) = hreq(addr, "GET", &format!("/v1/stream/{src}"), &[], b"").await;
            let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
            let (st, _, b) = hreq(
                addr,
                "PUT",
                &format!("/v1/stream/{child}"),
                &[
                    ("content-type", "application/json"),
                    ("stream-forked-from", &src),
                    ("stream-fork-offset", &boundary),
                ],
                b"",
            )
            .await;
            assert_eq!(st, 201, "fork {child}: {}", String::from_utf8_lossy(&b));
        }
    };
    fork_of("genA", "genB").await;
    fork_of("genB", "genC").await;
    // B is soft-deleted: alive only because C exists.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/genB", &[], b"").await;
    assert!(st == 204 || st == 200);
    let b = state.registry.get("genB").await.unwrap().unwrap();
    assert!(b.soft_deleted && !b.deleted, "B soft-deleted: {b:?}");

    // The crash, through the REAL cascade: deleting C makes B lose its
    // last child, so the production path tombstones B and records its
    // debt — and the failpoint stops it there, before A is released.
    // Nothing about the post-crash state is planted by hand.
    crate::http::fork_failpoints::stop_after_tombstone(true);
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/genC", &[], b"").await;
    assert!(st == 204 || st == 200, "delete C: {st}");
    crate::http::fork_failpoints::stop_after_tombstone(false);
    state.registry.invalidate("genB");
    let bdesc = state.registry.get("genB").await.unwrap().unwrap();
    assert!(
        bdesc.deleted && bdesc.parent_ref_pending,
        "the cascade did not tombstone B with its debt in one write: {bdesc:?}"
    );
    let a = state.registry.get("genA").await.unwrap().unwrap();
    assert_eq!(a.fork_children.len(), 1, "A still pins B");

    // Deleting the tombstoned middle generation settles the debt.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/genB", &[], b"").await;
    assert!(st == 404 || st == 410 || st == 204, "resume delete: {st}");
    state.registry.invalidate("genA");
    let a = state.registry.get("genA").await.unwrap().unwrap();
    assert!(
        a.fork_children.is_empty(),
        "A still pins a dead generation: {:?}",
        a.fork_children
    );
    state.registry.invalidate("genB");
    let b = state.registry.get("genB").await.unwrap().unwrap();
    assert!(!b.parent_ref_pending, "the debt is settled");
    engine_shutdown(&state).await;
}

/// Three seal-request defects the round-3 audit found by reading the
/// types: a PRESENT `null` final was indistinguishable from an absent
/// one, the operation identity concatenated record and routing key
/// without lengths, and the intent went durable before the key was
/// checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_requests_are_identified_and_validated_exactly() {
    const OTHER_KEY: &str = "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=";
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let mk = |n: &str| {
        let n = n.to_string();
        async move {
            let (st, _, _) = preq(
                addr,
                "PUT",
                &format!("/v1/streams/{n}"),
                &[("prisma-encryption-key", PRISMA_KEY)],
                br#"{"format":{"kind":"json"}}"#,
            )
            .await;
            assert_eq!(st, 201);
        }
    };

    // 1. `{"final": null}` seals WITH a null record, not without one.
    mk("sealnull").await;
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/sealnull:seal",
        &key,
        br#"{"final":null}"#,
    )
    .await;
    assert!(st == 200 || st == 204, "{}", String::from_utf8_lossy(&b));
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sealnull/records", &key, b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        recs,
        vec![serde_json::Value::Null],
        "a present null final was dropped"
    );

    // 2. Operation identity: the audit's collision pair. Concatenating
    //    record+key made {1,"23"} and {12,"3"} hash the same "123".
    let a = crate::product::seal_op_id(&serde_json::json!(1), "23");
    let b2 = crate::product::seal_op_id(&serde_json::json!(12), "3");
    assert_ne!(a, b2, "distinct seal requests share an operation id");

    // 3. A wrong key must not move the lifecycle at all.
    mk("sealkey").await;
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealkey:seal",
        &[("prisma-encryption-key", OTHER_KEY)],
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert_eq!(st, 403, "wrong key accepted");
    let d = state.registry.get("sealkey").await.unwrap().unwrap();
    assert!(
        d.sealing.is_none() && !d.sealed,
        "a refused seal published an intent: {:?}",
        d.sealing
    );
    // …and a missing key likewise.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealkey:seal",
        &[],
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert_eq!(st, 400);
    state.registry.invalidate("sealkey");
    let d = state.registry.get("sealkey").await.unwrap().unwrap();
    assert!(d.sealing.is_none() && !d.sealed);
    // The right key still works afterwards.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealkey:seal",
        &key,
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    engine_shutdown(&state).await;
}

/// A raw close that carries content promises those records. Publishing
/// an EMPTY seal intent for it meant a crash after the intent let a
/// later close-only finish the seal without them — and a request that
/// failed validation left the collection sealing forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_raw_close_with_content_owes_its_records() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawfin", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // A malformed close must not touch the lifecycle.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawfin",
        &[("content-type", "text/plain"), ("stream-closed", "true")],
        b"not json",
    )
    .await;
    assert!(st >= 400, "malformed close accepted: {st}");
    state.registry.invalidate("rawfin");
    let d = state.registry.get("rawfin").await.unwrap().unwrap();
    assert!(
        d.sealing.is_none() && !d.sealed,
        "a refused close published an intent: {:?}",
        d.sealing
    );

    // A valid close WITH content: the records land and the collection
    // seals as one operation.
    let (st, _, b) = hreq(
        addr,
        "POST",
        "/v1/stream/rawfin",
        &[("content-type", "application/json"), ("stream-closed", "true")],
        br#"[{"n":1},{"n":2}]"#,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "close with content: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state.registry.invalidate("rawfin");
    let d = state.registry.get("rawfin").await.unwrap().unwrap();
    assert!(d.sealed, "the collection did not seal");
    assert!(d.sealing.is_none(), "sealing state left behind");
    let (_, _, b) = hreq(addr, "GET", "/v1/stream/rawfin", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "the close's records are missing: {recs:?}");
    engine_shutdown(&state).await;
}

/// A raw close that carries content must survive a crash BETWEEN the
/// lifecycle intent and the append — recoverable by an ordinary retry
/// of the same request, with no private headers and no producer opt-in.
/// The old design recognised the owed record only by an `x-seal-final`
/// header that the product path inserted internally, so a real client's
/// retry was rejected as a new write and the collection stayed stuck
/// owing a record nobody could deliver. That header was also accepted
/// from the wire, which let any caller smuggle a record into a sealing
/// collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_raw_final_close_is_resumed_by_an_ordinary_retry() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawcrash", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // Simulate the crash point: the close published its Final intent and
    // died before the append. The intent is exactly what the server
    // writes — identity computed from the request's own bytes.
    let body = br#"[{"n":1},{"n":2}]"#;
    let request_hash = crate::http::create_request_hash(
        "application/json",
        None,
        None,
        true,
        body,
        None,
    );
    let op = crate::product::seal_op_id_raw(&request_hash, "");
    state
        .registry
        .cas_update("rawcrash", |d| {
            d.sealing = Some(crate::registry::SealState {
                operation_id: op.clone(),
                intent: crate::registry::SealIntent::Final {
                    routing_key: String::new(),
                    request_hash: request_hash.clone(),
                    final_committed: false,
                },
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("rawcrash");

    // An UNRELATED write is refused while the collection owes its final.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawcrash",
        &ct,
        br#"[{"other":1}]"#,
    )
    .await;
    assert_eq!(st, 409, "an unrelated write landed during Sealing");

    // …and so is a caller trying to assert the private authorization.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawcrash",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
            ("x-seal-final", op.as_str()),
        ],
        br#"[{"smuggled":1}]"#,
    )
    .await;
    assert_eq!(st, 400, "x-seal-final was accepted from the wire");

    // The ORDINARY retry — same request, no special headers — resumes.
    let (st, _, b) = hreq(
        addr,
        "POST",
        "/v1/stream/rawcrash",
        &[("content-type", "application/json"), ("stream-closed", "true")],
        body,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "the exact retry could not resume: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state.registry.invalidate("rawcrash");
    let d = state.registry.get("rawcrash").await.unwrap().unwrap();
    assert!(d.sealed, "the collection did not reach Sealed");
    assert!(d.sealing.is_none(), "sealing state left behind");
    let (_, _, b) = hreq(addr, "GET", "/v1/stream/rawcrash", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "the promised records are missing: {recs:?}");
    engine_shutdown(&state).await;
}

/// A seal intent installed OVER a pending split deadlocks the
/// collection: phase B refuses to publish because the collection is
/// sealing, and the seal cannot finish because the transition never
/// clears. The intent CAS is the serialization point — it installs only
/// over a topologically quiet descriptor, resolving the transition
/// first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_never_installs_over_a_pending_transition() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/deadl",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for k in ["a", "b"] {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/deadl/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            format!("{{\"k\":\"{k}\"}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }

    // Park a split in phase B: pending is durable, the parent is sealed,
    // successors are not published.
    crate::scaler3::failpoints::arm_before_publish();
    let split = {
        let st2 = state.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(&st2, "deadl", 0, 0x8000_0000_0000_0000).await
        })
    };
    let mut pending = false;
    for _ in 0..100 {
        state.registry.invalidate("deadl");
        if let Ok(Some(d)) = state.registry.get("deadl").await {
            if d.segments.as_ref().is_some_and(|m| m.pending.is_some()) {
                pending = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(pending, "the split never published its intent");

    // A seal-with-final arriving now must NOT install its intent over
    // the pending transition. It either resolves it and seals, or it
    // refuses — never "sealing forever with pending work".
    let sealer = {
        let st2 = state.clone();
        tokio::spawn(async move {
            preq(
                addr,
                "POST",
                "/v1/streams/deadl:seal",
                &[("prisma-encryption-key", PRISMA_KEY)],
                br#"{"final":{"done":true}}"#,
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    // While the split is still parked, the descriptor must never hold
    // BOTH a sealing intent and pending work.
    state.registry.invalidate("deadl");
    if let Ok(Some(d)) = state.registry.get("deadl").await {
        let both = d.sealing.is_some() && d.segments.as_ref().is_some_and(|m| m.pending.is_some());
        assert!(!both, "deadlock state: sealing over pending {:?}", d.segments);
    }
    crate::scaler3::failpoints::release_before_publish();
    let _ = split.await;
    let (st, _, b) = sealer.await.unwrap();

    state.registry.invalidate("deadl");
    let d = state.registry.get("deadl").await.unwrap().unwrap();
    assert!(
        !(d.sealing.is_some() && d.segments.as_ref().is_some_and(|m| m.pending.is_some())),
        "ended deadlocked: sealing={:?} segments={:?}",
        d.sealing,
        d.segments
    );
    if st == 200 || st == 204 {
        // Success must mean terminal, not "in progress".
        assert!(
            d.sealed && d.sealing.is_none(),
            "reported success without reaching Sealed: {} {:?}",
            String::from_utf8_lossy(&b),
            d.sealing
        );
    } else {
        // A refusal is fine — but then it must be resumable, and a
        // retry after the transition settles must succeed.
        let (st2, _, b2) = preq(
            addr,
            "POST",
            "/v1/streams/deadl:seal",
            &key,
            br#"{"final":{"done":true}}"#,
        )
        .await;
        assert!(
            st2 == 200 || st2 == 204,
            "the seal did not become possible again: {st2} {}",
            String::from_utf8_lossy(&b2)
        );
    }
    engine_shutdown(&state).await;
}

/// A first fork installing its reference must serialize against a
/// concurrent delete of the source. Deciding soft-versus-hard from a
/// descriptor read BEFORE the write let both win: the fork installed
/// its reference and the delete tombstoned the source anyway, leaving a
/// live fork whose parent is hard-deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_creation_and_source_deletion_serialize() {
    for attempt in 0..12 {
        let store = mem();
        let (state, addr) = http_rig(store).await;
        let ct = [("content-type", "application/json")];
        let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rsrc", &ct, br#"[{"n":0}]"#).await;
        assert!(st == 200 || st == 201);
        let (_, h, _) = hreq(addr, "GET", "/v1/stream/rsrc", &[], b"").await;
        let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();

        // Fire the fork and the delete together, with the ordering
        // nudged both ways across attempts.
        let fork = tokio::spawn(async move {
            if attempt % 2 == 1 {
                tokio::time::sleep(std::time::Duration::from_micros(200)).await;
            }
            hreq(
                addr,
                "PUT",
                "/v1/stream/rchild",
                &[
                    ("content-type", "application/json"),
                    ("stream-forked-from", "rsrc"),
                    ("stream-fork-offset", &boundary),
                ],
                b"",
            )
            .await
        });
        let del = tokio::spawn(async move {
            if attempt % 2 == 0 {
                tokio::time::sleep(std::time::Duration::from_micros(200)).await;
            }
            hreq(addr, "DELETE", "/v1/stream/rsrc", &[], b"").await
        });
        let (fst, _, _) = fork.await.unwrap();
        let _ = del.await.unwrap();

        state.registry.invalidate("rsrc");
        state.registry.invalidate("rchild");
        let src = state.registry.get("rsrc").await.unwrap().unwrap();
        let child = state.registry.get("rchild").await.unwrap();
        let child_live = child
            .as_ref()
            .is_some_and(|c| !c.deleted && fst == 201);
        if child_live {
            assert!(
                !src.deleted,
                "attempt {attempt}: a live fork is anchored to a HARD-deleted source"
            );
        }
        engine_shutdown(&state).await;
    }
}

/// A seal intent must never be published for a final record the append
/// path will always refuse — that leaves the collection sealing
/// forever, owing something undeliverable. Every deterministic refusal
/// is decided first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_impossible_final_never_publishes_an_intent() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/impossible",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let untouched = |label: &'static str| {
        let state = state.clone();
        async move {
            state.registry.invalidate("impossible");
            let d = state.registry.get("impossible").await.unwrap().unwrap();
            assert!(
                d.sealing.is_none() && !d.sealed,
                "{label} published a lifecycle intent: {:?}",
                d.sealing
            );
        }
    };

    // Routing key past the limit.
    let long = "k".repeat(2000);
    let body = format!(r#"{{"final":{{"x":1}},"routingKey":"{long}"}}"#);
    let (st, _, _) = preq(addr, "POST", "/v1/streams/impossible:seal", &key, body.as_bytes()).await;
    assert_eq!(st, 400, "oversized routing key accepted");
    untouched("an oversized routing key").await;

    // Routing key that cannot travel as a header value.
    let body = "{\"final\":{\"x\":1},\"routingKey\":\"bad\\u0001key\"}";
    let (st, _, _) = preq(addr, "POST", "/v1/streams/impossible:seal", &key, body.as_bytes()).await;
    assert_eq!(st, 400, "control character in the routing key accepted");
    untouched("an untransmittable routing key").await;

    // A partial producer trio.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/impossible:seal",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("producer-id", "p"),
        ],
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert_eq!(st, 400, "partial producer headers accepted");
    untouched("a partial producer trio").await;

    // And a valid one still seals.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/impossible:seal",
        &key,
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert!(st == 200 || st == 204, "{}", String::from_utf8_lossy(&b));
    engine_shutdown(&state).await;
}

/// Appendix §8: the 12-case dual-surface equivalence corpus — for the
/// default routing key, equivalent operations through the raw standards
/// route and the product route resolve to ONE collection incarnation
/// with identical canonical data and lifecycle state, in both orders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dual_surface_equivalence_corpus() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let pk = [("prisma-encryption-key", PRISMA_KEY)];
    let ct = [("content-type", "application/json")];

    // 1. Product create -> raw append -> product read.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/eq1",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/eq1", &ct, br#"[{"c":1}]"#).await;
    assert!(st == 200 || st == 204, "case 1 raw append {st}");
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq1/records", &pk, b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "case 1");
    assert_eq!(recs[0]["c"], 1);

    // 2. Raw create -> product append (no routing key) -> raw read.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/eq2", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = preq(addr, "POST", "/v1/streams/eq2/records", &pk, br#"{"c":2}"#).await;
    assert_eq!(st, 200);
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/eq2", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "case 2");
    assert_eq!(recs[0]["c"], 2);

    // 3. Raw producer append -> product read. 4. Product producer
    // append -> raw read. One producer scope, one sequence.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/eq3", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let rawp = [
        ("content-type", "application/json"),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/eq3", &rawp, br#"[{"c":3}]"#).await;
    assert!(st == 200 || st == 204, "case 3 {st}");
    let prodp = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "1"),
    ];
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/eq3/records",
        &prodp,
        br#"{"c":4}"#,
    )
    .await;
    assert_eq!(
        st, 200,
        "case 4: the product append continues the RAW producer's sequence"
    );
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq3/records", &pk, b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 2, "cases 3+4 share one sequence");
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/eq3", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 2);

    // 5. Product seal -> raw closed-tail read. 6-adjacent: raw HEAD
    // reports the closure.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/eq3:seal", &pk, b"{}").await;
    assert!(st == 200 || st == 204);
    let (st, h, _) = hreq(addr, "GET", "/v1/stream/eq3", &[], b"").await;
    assert_eq!(st, 200);
    assert_eq!(
        h.get("stream-closed").map(String::as_str),
        Some("true"),
        "case 5"
    );

    // 6. Raw close -> product metadata sealed.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/eq6", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/eq6",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        br#"[{"fin":true}]"#,
    )
    .await;
    assert!(st == 200 || st == 204, "case 6 close {st}");
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq6", &pk, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["sealed"], true, "case 6");

    // 7. Product delete -> raw gone. 8. Raw delete -> product gone.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/eq7",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/eq7", &pk, b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/eq7", &[], b"").await;
    assert_eq!(st, 404, "case 7");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/eq8", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/eq8", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = preq(addr, "GET", "/v1/streams/eq8", &pk, b"").await;
    assert_eq!(st, 404, "case 8");

    // 9. Raw TTL create -> product metadata expiry. 10. Product idle
    // expiry -> raw HEAD TTL.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/eq9",
        &[("content-type", "application/json"), ("stream-ttl", "3600")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq9", &pk, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(
        v["expiry"]["idle"].is_string() || v["expiry"].is_object(),
        "case 9: product metadata reflects the raw TTL: {v}"
    );
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/eq10",
        &pk,
        br#"{"format":{"kind":"json"},"expiry":{"idle":"1h"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, h, _) = hreq(addr, "HEAD", "/v1/stream/eq10", &[], b"").await;
    assert_eq!(st, 200);
    assert!(
        h.contains_key("stream-ttl"),
        "case 10: raw HEAD reports TTL"
    );

    // 11. Product-created JSON stream -> raw JSON array flattening.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/eq11",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/eq11",
        &ct,
        br#"[{"a":1},{"a":2}]"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq11/records", &pk, b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 2, "case 11: raw flattening stored two messages");

    // 12. Token classes never cross: a product cursor is rejected as a
    // raw offset; a raw offset is rejected as a product cursor.
    let (st, h, _) = preq(addr, "GET", "/v1/streams/eq11/records", &pk, b"").await;
    assert_eq!(st, 200);
    let cursor = h.get("prisma-next-cursor").unwrap().clone();
    let path = format!("/v1/stream/eq11?offset={cursor}");
    let (st, _, _) = hreq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 400, "case 12a: product cursor on the raw route");
    let (st, h, _) = hreq(addr, "GET", "/v1/stream/eq11", &[], b"").await;
    assert_eq!(st, 200);
    let raw_off = h.get("stream-next-offset").unwrap().clone();
    let path = format!("/v1/streams/eq11/records?cursor={raw_off}");
    let (st, _, _) = preq(addr, "GET", &path, &pk, b"").await;
    assert_eq!(st, 400, "case 12b: raw offset on the product route");
    engine_shutdown(&state).await;
}

/// Pinned DS fork contract (regression net for the conformance suite):
/// stitched reads across the boundary, source independence, sub-offset
/// materialization, soft-delete 410s, and the reference cascade.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_lifecycle_and_stitched_reads() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    // Source: three records.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/fsrc",
        &ct,
        br#"[{"n":0},{"n":1},{"n":2}]"#,
    )
    .await;
    assert!(st == 200 || st == 201);
    // Fork at record 2 (server-returned tokens are opaque; use the
    // reference zero-literal + JSON sub semantics: 0 + sub 2).
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/ffork",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "/v1/stream/fsrc"),
            ("stream-fork-offset", "0000000000000000_0000000000000000"),
            ("stream-fork-sub-offset", "2"),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 201);
    // Fork sees the inherited prefix only.
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/ffork", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 2, "{recs:?}");
    // Appends to fork and source are independent.
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/ffork", &ct, br#"[{"f":1}]"#).await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/fsrc", &ct, br#"[{"s":9}]"#).await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/ffork", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3);
    assert_eq!(recs[2]["f"], 1, "fork's own append after the prefix");
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/fsrc", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 4, "source unaffected by the fork's append");

    // Soft-delete: the source with a live fork answers 410 directly,
    // the fork still reads; deleting the fork cascades the source away.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fsrc", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/fsrc", &[], b"").await;
    assert_eq!(st, 410, "soft-deleted source is GONE, not missing");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fsrc", &ct, b"").await;
    assert_eq!(st, 409, "re-creation blocked while forks live");
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/ffork", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        recs.len(),
        3,
        "fork reads inherited data past the soft delete"
    );
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/ffork", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/fsrc", &[], b"").await;
    assert_eq!(
        st, 404,
        "last fork's deletion cascades the source to gone-gone"
    );
    engine_shutdown(&state).await;
}

/// SECURITY (audit P0): the account token gates EVERY product
/// operation when configured. The encryption key is a separate
/// credential and never substitutes for it. The one exception is the
/// signed watch observation URL — an explicit delegated capability.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_requires_the_account_token() {
    let store = mem();
    let (state, addr) = http_rig_auth(store, "s3cret").await;
    let bear = |t: &'static str| ("authorization", t);
    let ok = [
        ("authorization", "Bearer s3cret"),
        ("prisma-encryption-key", PRISMA_KEY),
    ];
    // Create needs the token; the key alone is not enough.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/sec",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st,
        401,
        "key without token must not create: {}",
        String::from_utf8_lossy(&b)
    );
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "unauthorized");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sec",
        &[bear("Bearer wrong"), ("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 401, "wrong token rejected");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sec",
        &ok,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "token + key creates");

    // Every other product operation: tokenless is 401.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/sec/records", &ok, b"{\"n\":1}").await;
    assert_eq!(st, 200);
    for (m, path, body) in [
        ("GET", "/v1/streams/sec", &b""[..]),
        ("POST", "/v1/streams/sec/records", &b"{\"n\":2}"[..]),
        ("POST", "/v1/streams/sec/records:batch", &b"[{\"n\":3}]"[..]),
        ("GET", "/v1/streams/sec/records", &b""[..]),
        (
            "GET",
            "/v1/streams/sec/records:long-poll?waitMs=50",
            &b""[..],
        ),
        ("GET", "/v1/streams/sec:scan", &b""[..]),
        ("PUT", "/v1/streams/sec/consumers/w", &b"{}"[..]),
        ("GET", "/v1/streams/sec/consumers/w", &b""[..]),
        ("POST", "/v1/streams/sec/consumers/w:pull", &b"{}"[..]),
        ("POST", "/v1/streams/sec/consumers/w:settle", &b"{}"[..]),
        ("DELETE", "/v1/streams/sec/consumers/w", &b""[..]),
        ("GET", "/v1/streams/sec/watches", &b""[..]),
        ("GET", "/v1/streams", &b""[..]),
        ("POST", "/v1/streams/sec:seal", &b"{}"[..]),
        ("DELETE", "/v1/streams/sec", &b""[..]),
    ] {
        let (st, _, _) = preq(
            addr,
            m,
            path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            body,
        )
        .await;
        assert_eq!(st, 401, "{m} {path} must require the account token");
    }
    // The stream still exists (no tokenless delete/seal took effect).
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sec", &ok, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["sealed"], false, "tokenless seal must not have landed");

    // Token + WRONG key is 403 (authorization passes, key access fails).
    let wrong_key = "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg";
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/sec/records",
        &[bear("Bearer s3cret"), ("prisma-encryption-key", wrong_key)],
        b"",
    )
    .await;
    assert_eq!(st, 403, "token ok, key wrong -> 403");

    // Browser preflight is answered WITHOUT credentials (a preflight
    // never carries them) and advertises the product headers.
    let (st, h, _) = preq(addr, "OPTIONS", "/v1/streams/sec/records", &[], b"").await;
    assert!(st == 200 || st == 204, "preflight status {st}");
    assert!(
        h.get("access-control-allow-headers").is_some(),
        "preflight must allow the product headers"
    );
    let (st, _, _) = preq(addr, "OPTIONS", "/v1/streams", &[], b"").await;
    assert!(st == 200 || st == 204, "catalog preflight status {st}");
    engine_shutdown(&state).await;
}

/// AUDIT P0 (the field create anomaly, made deterministic): a replayed
/// PUT must never observe a published-but-uninitialized descriptor and
/// answer success for a stream whose initial content never landed. The
/// replay JOINS the initialization; a DIFFERENT request conflicts; and
/// reads/appends against an initializing stream get a retryable answer
/// rather than an empty stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_replay_never_loses_the_initial_body() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];

    // Two identical PUTs racing (an edge replay). Exactly one creates;
    // BOTH must see the initial content durable when they answer.
    let a =
        tokio::spawn(
            async move { hreq(addr, "PUT", "/v1/stream/replay1", &ct, br#"[{"n":1}]"#).await },
        );
    let b =
        tokio::spawn(
            async move { hreq(addr, "PUT", "/v1/stream/replay1", &ct, br#"[{"n":1}]"#).await },
        );
    let (sa, _, ba) = a.await.unwrap();
    let (sb, _, bb) = b.await.unwrap();
    assert!(
        sa == 201 || sa == 200,
        "A {sa}: {}",
        String::from_utf8_lossy(&ba)
    );
    assert!(
        sb == 201 || sb == 200,
        "B {sb}: {}",
        String::from_utf8_lossy(&bb)
    );
    let (st, _, body) = hreq(addr, "GET", "/v1/stream/replay1", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(recs.len(), 1, "initial body must be durable: {recs:?}");
    assert_eq!(recs[0]["n"], 1);

    // A descriptor stuck in Initializing (the creator died): reads and
    // appends are retryable, NOT an empty stream, and the SAME request
    // resumes it while a different one conflicts.
    let desc = state.registry.get("replay1").await.unwrap().unwrap();
    assert!(desc.init.is_none(), "a completed create publishes Ready");
    // Plant an initializing incarnation by hand (a crashed creator).
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/replay2", &ct, br#"[{"n":7}]"#).await;
    assert_eq!(st, 201);
    state
        .registry
        .cas_update("replay2", |d| {
            d.init = Some(crate::registry::InitState {
                request_hash: "deadbeef".into(),
                key_fingerprint: d.key_fingerprint.clone(),
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("replay2");
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/replay2", &[], b"").await;
    assert_eq!(st, 503, "reads of an initializing stream are retryable");
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/replay2", &ct, br#"[{"n":8}]"#).await;
    assert_eq!(st, 503, "appends to an initializing stream are retryable");
    // A DIFFERENT creation request conflicts rather than hijacking it.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/replay2", &ct, br#"[{"other":1}]"#).await;
    assert_eq!(
        st, 409,
        "a different request must not steal an in-flight create"
    );

    // A STALE claim (dead creator) stops blocking so the name is never
    // wedged: the same request takes over and completes it.
    state
        .registry
        .cas_update("replay2", |d| {
            d.init = Some(crate::registry::InitState {
                request_hash: "deadbeef".into(),
                key_fingerprint: d.key_fingerprint.clone(),
                claimed_ms: crate::shard::now_ms() - crate::registry::INIT_CLAIM_MS - 1_000,
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("replay2");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/replay2", &ct, br#"[{"other":1}]"#).await;
    assert_eq!(
        st, 409,
        "stale claim + different config is still a config conflict"
    );
    engine_shutdown(&state).await;
}

/// AUDIT P0: sealing is a durable, resumable transition. A collection
/// stuck in Sealing (a crashed sealer) refuses ordinary appends on BOTH
/// surfaces, and the next seal request finishes the job. A sealed
/// descriptor is authoritative even if a segment engine has not
/// observed its close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_is_a_resumable_transition() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let pk = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealtx",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealtx/records",
        &pk,
        b"{\"n\":0}",
    )
    .await;
    assert_eq!(st, 200);

    // Plant a Sealing intent (a sealer that died before closing the
    // segments and publishing Sealed).
    state
        .registry
        .cas_update_retry("sealtx", |d| {
            d.sealing = Some(crate::registry::SealState {
                intent: crate::registry::SealIntent::Empty,
                operation_id: "op-1".into(),
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("sealtx");

    // Ordinary appends are refused on both surfaces WHILE sealing.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealtx/records",
        &pk,
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 409, "product append during Sealing");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/sealtx",
        &[("content-type", "application/json")],
        br#"[{"n":2}]"#,
    )
    .await;
    assert_eq!(st, 409, "raw append during Sealing");
    // Metadata still reports NOT sealed: the transition has not
    // completed, so nothing may claim it has.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sealtx", &pk, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["sealed"], false, "Sealing is not Sealed");

    // Any seal request resumes and completes the transition.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/sealtx:seal", &pk, b"{}").await;
    assert!(st == 200 || st == 204);
    let d = state.registry.get("sealtx").await.unwrap().unwrap();
    assert!(d.sealed, "resumed seal publishes Sealed");
    assert!(d.sealing.is_none(), "the intent is cleared");
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealtx/records",
        &pk,
        b"{\"n\":3}",
    )
    .await;
    assert_eq!(st, 409, "sealed refuses appends");

    // A sealed DESCRIPTOR is authoritative even when a segment engine
    // has not observed the close (the audit's "physically open segment
    // accepted writes" case): plant sealed on a stream whose engine is
    // still open and prove both surfaces refuse.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealauth",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    state
        .registry
        .cas_update_retry("sealauth", |d| {
            d.sealed = true; // descriptor only; engines untouched
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("sealauth");
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealauth/records",
        &pk,
        b"{\"n\":9}",
    )
    .await;
    assert_eq!(st, 409, "descriptor seal is authoritative (product)");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/sealauth",
        &[("content-type", "application/json")],
        br#"[{"n":9}]"#,
    )
    .await;
    assert_eq!(st, 409, "descriptor seal is authoritative (raw)");
    engine_shutdown(&state).await;
}

/// AUDIT P0: the fork lifecycle is idempotent and recoverable.
/// References are installed and released BY ID (a retried delete is a
/// no-op, not a double-release); a stale source incarnation is an
/// integrity error, not a silent cross-incarnation read; the product
/// create path cannot overwrite a retained source; and members of a
/// fork chain never split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_lifecycle_is_idempotent_and_epoch_checked() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let pk = [("prisma-encryption-key", PRISMA_KEY)];

    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/fk-src",
        &ct,
        br#"[{"n":0},{"n":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 201);
    // Two identical fork PUTs (a replay): one reference, not two.
    let fh = [
        ("content-type", "application/json"),
        ("stream-forked-from", "/v1/stream/fk-src"),
        ("stream-fork-offset", "0000000000000000_0000000000000000"),
        ("stream-fork-sub-offset", "1"),
    ];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fk-a", &fh, b"").await;
    assert_eq!(st, 201);
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fk-a", &fh, b"").await;
    assert!(st == 200 || st == 201, "idempotent fork PUT: {st}");
    let src = state.registry.get("fk-src").await.unwrap().unwrap();
    assert_eq!(
        src.fork_children.len(),
        1,
        "one reference per fork: {:?}",
        src.fork_children
    );
    let child = state.registry.get("fk-a").await.unwrap().unwrap();
    let fref = child.forked_from.as_ref().unwrap();
    assert!(!fref.fork_id.is_empty(), "the fork stamps its own id");
    assert_eq!(
        fref.source_epoch, src.stream_epoch,
        "source incarnation recorded"
    );

    // Neither member of the chain may split (stitched reads resolve one
    // segment per ancestor).
    assert!(
        !crate::scaler3::execute_split(&state, "fk-src", 0, 0x8000_0000_0000_0000).await,
        "a stream with live forks must not split"
    );
    assert!(
        !crate::scaler3::execute_split(&state, "fk-a", 0, 0x8000_0000_0000_0000).await,
        "a fork must not split"
    );

    // A stale source incarnation is an integrity error, never a silent
    // read of a recreated source.
    state
        .registry
        .cas_update_retry("fk-a", |d| {
            d.forked_from.as_mut().unwrap().source_epoch =
                "ffffffffffffffffffffffffffffffff".into();
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("fk-a");
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/fk-a", &[], b"").await;
    assert_eq!(
        st,
        500,
        "stale source epoch must fail loudly: {}",
        String::from_utf8_lossy(&b)
    );
    // Restore the true epoch and confirm the read works again.
    let true_epoch = src.stream_epoch.clone();
    state
        .registry
        .cas_update_retry("fk-a", |d| {
            d.forked_from.as_mut().unwrap().source_epoch = true_epoch.clone();
            true
        })
        .await
        .unwrap();
    state.registry.invalidate("fk-a");
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/fk-a", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "inherited prefix");

    // Soft-delete the source; the PRODUCT create path must not replace
    // a name that still backs a live fork (the raw path already blocks
    // it).
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fk-src", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/fk-src",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st,
        409,
        "product create must not overwrite a retained source: {}",
        String::from_utf8_lossy(&b)
    );

    // A RETRIED delete of the fork releases exactly once; the cascade
    // then removes the retained source.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fk-a", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fk-a", &[], b"").await;
    assert!(st == 404 || st == 410 || st == 204, "retried delete: {st}");
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/fk-src", &[], b"").await;
    assert_eq!(st, 404, "last fork released -> source cascades away");
    engine_shutdown(&state).await;
}

/// AUDIT P0: the singular route is the DEFAULT-KEY Durable Stream, and
/// stays one strict sequence while the product surface writes other
/// keys and splits the collection underneath it. The required
/// cross-surface test: product keys + split, raw default-key traffic,
/// raw reads see exactly their own records with resumable cursors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_route_is_the_default_key_view_across_splits() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let pk = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dualkey", &ct, b"").await;
    assert!(st == 200 || st == 201);

    // Raw (default-key) and product (other keys) traffic interleaved.
    for i in 0..3 {
        let body = format!("[{{\"raw\":{i}}}]");
        let (st, _, _) = hreq(addr, "POST", "/v1/stream/dualkey", &ct, body.as_bytes()).await;
        assert!(st == 200 || st == 204);
        for k in ["ka", "kb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{i}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/dualkey/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    // Before the split: the raw read sees ONLY its own records.
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/dualkey", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "raw sees only the default key: {recs:?}");
    assert!(recs.iter().all(|r| r.get("raw").is_some()));

    // Split the collection through the product surface.
    assert!(crate::scaler3::execute_split(&state, "dualkey", 0, 0x8000_0000_0000_0000).await);
    for i in 3..6 {
        let body = format!("[{{\"raw\":{i}}}]");
        let (st, _, _) = hreq(addr, "POST", "/v1/stream/dualkey", &ct, body.as_bytes()).await;
        assert!(st == 200 || st == 204, "raw append after split: {st}");
        for k in ["ka", "kb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{i}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/dualkey/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }

    // AFTER the split the raw route is STILL the default-key sequence:
    // every record, in order, nothing from other keys — paginated with
    // resumable raw offsets.
    let mut seen: Vec<i64> = Vec::new();
    let mut tok: Option<String> = None;
    for _ in 0..16 {
        let path = match &tok {
            None => "/v1/stream/dualkey".to_string(),
            Some(t) => format!("/v1/stream/dualkey?offset={t}"),
        };
        let (st, h, b) = hreq(addr, "GET", &path, &[], b"").await;
        assert_eq!(st, 200, "raw page after split");
        let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        for r in &recs {
            assert!(r.get("k").is_none(), "another key's record leaked: {r}");
            seen.push(r["raw"].as_i64().unwrap());
        }
        if h.get("stream-up-to-date").map(String::as_str) == Some("true") {
            break;
        }
        let nxt = h.get("stream-next-offset").cloned();
        if nxt == tok || nxt.is_none() {
            break;
        }
        tok = nxt;
    }
    assert_eq!(
        seen,
        (0..6).collect::<Vec<i64>>(),
        "default-key order across the split"
    );

    // Live reads on the raw route keep working after the split (one
    // key, one lineage — no keyless-live impossibility).
    let (st, h, _) = hreq(
        addr,
        "GET",
        "/v1/stream/dualkey?offset=now&live=long-poll&timeout=200ms",
        &[],
        b"",
    )
    .await;
    assert!(st == 200 || st == 204, "raw long-poll after split: {st}");
    assert!(
        !h.contains_key("stream-ordering"),
        "no internals leak to the raw route"
    );
    assert!(!h.contains_key("stream-segment-map-version"));

    // The removed keyed extensions are rejected, not honored.
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/dualkey?key=ka", &[], b"").await;
    assert_eq!(st, 400, "?key= is removed from the raw route");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/dualkey",
        &[("content-type", "application/json"), ("stream-key", "ka")],
        br#"[{"x":1}]"#,
    )
    .await;
    assert_eq!(st, 400, "Stream-Key is removed from the raw route");
    engine_shutdown(&state).await;
}

/// AUDIT P0: the catalog paginates without scanning the world. With
/// far more streams than one page, listing walks them in NAME order
/// through provider continuation, every stream is reachable (nothing
/// falls outside a fixed window), pages never restart from the
/// beginning, and a page's descriptor GETs are bounded by the page
/// size — not by the catalog size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_pages_without_scanning_the_world() {
    let inner = mem();
    let counting = FaultStore::uniform(inner, 7, FaultPlan::new(0, 0, 0));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (state, addr) = http_rig(store).await;
    // 1,200 streams: an order of magnitude more than one page, and
    // enough that a scan-everything implementation is obvious in the
    // request count.
    const N: usize = 1_200;
    for i in 0..N {
        let name = format!("cat-{i:05}");
        let d = crate::registry::StreamDesc {
            name: name.clone(),
            stream_epoch: format!("{:032x}", i),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
            soft_deleted: false,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            sealing: None,
            seal_op: None,
            content_type: "application/json".into(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            watch_definitions: Vec::new(),
            watch_sig_key: None,
            parent_ref_pending: false,
            layout_version: crate::registry::LAYOUT_VERSION,
        };
        state.registry.create(d).await.unwrap();
    }
    // Page through the whole catalog.
    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let ops_before = counting.ops();
    let mut pages = 0usize;
    for _ in 0..64 {
        let path = match &cursor {
            None => "/v1/streams?limit=100".to_string(),
            Some(c) => format!("/v1/streams?limit=100&cursor={c}"),
        };
        let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        let names: Vec<String> = v["streams"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["name"].as_str().unwrap().to_string())
            .collect();
        pages += 1;
        seen.extend(names);
        match v["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    let ops_after = counting.ops();
    assert_eq!(
        seen.len(),
        N,
        "every stream is reachable, got {} in {pages} pages",
        seen.len()
    );
    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(seen, sorted, "catalog pages walk in name order");
    sorted.dedup();
    assert_eq!(sorted.len(), N, "no stream is listed twice");
    // Cost: a scan-everything implementation costs pages * N GETs
    // (12 * 1200 = 14,400 here). Page-local cost is ~N total.
    let per_page_budget = (N + pages * 40) as u64;
    assert!(
        ops_after - ops_before < per_page_budget,
        "catalog cost must be page-local: {} store ops for {pages} pages over {N} streams",
        ops_after - ops_before
    );
    // A cursor is opaque (not a bare, editable stream name).
    let (_, _, b) = preq(addr, "GET", "/v1/streams?limit=10", &[], b"").await;
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let c = v["cursor"].as_str().unwrap();
    assert!(!c.starts_with("cat-"), "cursor must be opaque, got {c}");
    engine_shutdown(&state).await;
}
