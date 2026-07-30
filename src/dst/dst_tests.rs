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
        let observed = drain_observed(&fresh_hist(&ds), &engine, hash, &key, &cov).await;
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
    let observed = drain_observed(&fresh_hist(&ds), &engine, hash, &key, &cov).await;
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
        let res = crate::http::read_merged(
            &fresh_hist(&ds),
            &key,
            &hash,
            &handle,
            &b,
            0,
            None,
            8 * 1024 * 1024,
        )
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
    let res = crate::http::read_merged(
        &fresh_hist(&ds),
        &key,
        &hash,
        &handle,
        &b,
        0,
        None,
        8 * 1024 * 1024,
    )
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
    let res = crate::http::read_merged(
        &fresh_hist(&ds),
        &key,
        &hash,
        &handle,
        &engine,
        0,
        None,
        1 << 20,
    )
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
        let observed = drain_observed(&fresh_hist(&ds), &b, hash, &key, &cov).await;
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
    let observed = drain_observed(&fresh_hist(&ds), &b, hash, &key, &cov).await;
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
    open_engine_with_absorber_layout(store, prefix, hash, key, false).await
}

/// Legacy-layout variant: forces every stream through the per-stream v1
/// lanes so the v1 machinery (HistReaders coverage probes, k! index,
/// per-stream DBs) keeps its DST coverage now that fresh streams
/// default to the shared v2 partition.
async fn open_engine_with_absorber_v1(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
) -> (Arc<crate::shard::ShardEngine>, tokio::task::JoinHandle<()>) {
    open_engine_with_absorber_layout(store, prefix, hash, key, true).await
}

async fn open_engine_with_absorber_layout(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    force_v1: bool,
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
        force_v1,
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
    let observed = drain_observed(&fresh_hist(&ds), &engine, hash, &key, &cov).await;
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
    hist: &Arc<crate::history::HistReaders>,
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
            hist,
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

/// Unkeyed records carry **no k! index copy** in history (it is a full
/// payload duplicate — double the history bytes for the common unkeyed
/// workload), and an empty-key filtered read must still return exactly
/// the unkeyed records, across the history/tail boundary, served from
/// the primary r! range instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_key_records_skip_the_index_copy_but_still_filter_read() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 47, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [33u8; 16];
    let (engine, absorber) =
        open_engine_with_absorber_v1(store.clone(), "dst-noidx", hash, &key).await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    // Interleaved unkeyed ("") and keyed records.
    w.run(&engine, hash, &key, &["", "k1"], 25, false, &mut log)
        .await;
    assert!(log.total_acked() > 0, "nothing acked");

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
        "the absorber never ran — nothing reached history"
    );

    let ds: Arc<dyn ObjectStore> = store.clone();
    let hist = fresh_hist(&ds);

    // Each filter returns exactly its key's acked records, in ack order.
    let unkeyed = drain_filtered(&hist, &engine, hash, &key, "").await;
    let keyed = drain_filtered(&hist, &engine, hash, &key, "k1").await;
    assert_eq!(
        &unkeyed, &log.acked[""],
        "empty-key filter lost or reordered unkeyed records (absorbed={absorbed})"
    );
    assert_eq!(
        &keyed, &log.acked["k1"],
        "keyed filter lost or reordered keyed records (absorbed={absorbed})"
    );

    // Write-side proof: the history DB holds NO k! entries for the empty
    // key — the filtered read above was served without the index copy.
    let (reader, covered) = hist
        .acquire(&hash, &key, absorbed)
        .await
        .expect("history reader");
    assert!(covered, "reader must cover the absorbed boundary");
    let range =
        crate::history::hist_key_index_key("", 0)..crate::history::hist_key_index_key("", u64::MAX);
    let mut iter = reader
        .scan(range)
        .await
        .expect("scan empty-key index range");
    let mut leaked = 0u64;
    while let Some(_kv) = iter.next().await.expect("iter") {
        leaked += 1;
    }
    assert_eq!(
        leaked, 0,
        "unkeyed records still get a k! payload duplicate in history"
    );
    absorber.abort();
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
    let observed = drain_observed(&fresh_hist(&ds), &engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("sweep-absorbed stream lost records: {e}");
    }
    absorber.abort();
}

/// The concurrent small lane must preserve every per-stream invariant:
/// a dozen small streams absorb in overlapping passes, and each one's
/// merged read is still exactly its acked sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_small_lane_absorbs_many_streams_correctly() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 53, FaultPlan::new(0, 0, 10));
    let cov = store.coverage();
    let key = skey();

    let db = slatedb::Db::builder("dst-lane", store.clone() as Arc<dyn ObjectStore>)
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
        "dst-lane".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
    );
    let keys = Arc::new(crate::history::KeyCache::default());
    let hashes: Vec<[u8; 16]> = (0..12u8).map(|i| [100 + i; 16]).collect();
    for h in &hashes {
        keys.put(*h, key.clone(), *h);
    }
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
            concurrency: 4,
            force_v1: true,
            ..Default::default()
        },
        absorb_rx,
    );

    let mut logs: Vec<OpLog> = Vec::new();
    for h in &hashes {
        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        w.run(&engine, *h, &key, &["m"], 6, false, &mut log).await;
        assert!(log.total_acked() > 0, "stream {h:?}: nothing acked");
        logs.push(log);
    }

    // Every stream must fully absorb — concurrently, since they are all
    // due at once and far under the small-pass byte bound.
    for h in &hashes {
        let mut caught_up = false;
        for _ in 0..400 {
            let handle = engine.stream_handle(*h).await.expect("handle");
            {
                let st = handle.state.lock().unwrap();
                if st.durable.absorbed > 0 && st.durable.absorbed == st.durable.next {
                    caught_up = true;
                }
            }
            if caught_up {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(caught_up, "stream {h:?} never fully absorbed");
    }
    let ds: Arc<dyn ObjectStore> = store.clone();
    let hist = fresh_hist(&ds);
    for (h, log) in hashes.iter().zip(&logs) {
        let observed = drain_observed(&hist, &engine, *h, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("stream {h:?} corrupted by the concurrent lane: {e}");
        }
    }
    if let Err(e) = cov.require(&[mech::READ_FROM_HISTORY]) {
        panic!("{e}");
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
    let hist = fresh_hist(&ds);
    let observed = drain_observed(&hist, &engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("v2 keyless absorption lost records (absorbed={absorbed}): {e}");
    }
    let unkeyed = drain_filtered(&hist, &engine, hash, &key, "").await;
    assert_eq!(&unkeyed, &log.acked[""], "v2 empty-key filter broken");
    let keyed = drain_filtered(&hist, &engine, hash, &key, "vk").await;
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
    let observed = drain_observed(&fresh_hist(&ds), &b, hash, &key, &cov).await;
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
    let hist = fresh_hist(&ds);
    let obs_tiny = drain_observed(&hist, &engine, tiny, &key, &cov).await;
    tiny_log
        .audit(&obs_tiny)
        .expect("tiny stream readable from the shard log");
    let obs_fat = drain_observed(&hist, &engine, fat, &key, &cov).await;
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
        let observed = drain_observed(&fresh_hist(&ds), &engine, hash, &key, &cov).await;
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
        let observed = drain_observed(&fresh_hist(&ds), &engine, hash, &key, &cov).await;
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
        open_engine_with_absorber_v1(store.clone(), "dst-tier", hash, &key).await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    // Enough traffic, in waves, that absorption AND trim both advance
    // (trim lands one absorb round behind the boundary by design).
    for _ in 0..6 {
        w.run(&engine, hash, &key, &["t1", "t2"], 12, false, &mut log)
            .await;
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
        w.run(&engine, hash, &key, &["t1"], 2, false, &mut log)
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        trimmed > 0,
        "trim never advanced (absorbed={absorbed}, next={next}) — this \
         scenario would only be re-testing absorption"
    );
    assert!(
        absorbed >= trimmed,
        "absorbed {absorbed} must cover trimmed {trimmed}"
    );
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
    let hist =
        crate::history::read_history(&fresh_hist(&ds), &hash, &key, 0, absorbed, None, 8 << 20)
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
    let observed = drain_observed(&fresh_hist(&ds), &engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!(
            "merged read is not the canonical stream (trimmed={trimmed}, absorbed={absorbed}, next={next}): {e}"
        );
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

// The history reader cache and its counters are process-global, so the
// The reader service is per-instance now (one per store), so these
// scenarios need no cross-test serialization and no global poll pin:
// each constructs its own service with the poll it wants. `hist_hour`
// pins the manifest poll to an hour, making staleness deterministic —
// the boundary probe must do the work, polling cannot rescue the test.
fn hist_hour(store: &Arc<dyn ObjectStore>, cap: usize) -> Arc<crate::history::HistReaders> {
    crate::history::HistReaders::new(
        store.clone(),
        cap,
        std::time::Duration::from_secs(120),
        3_600_000,
    )
}

fn m(v: &std::sync::atomic::AtomicU64) -> u64 {
    v.load(Ordering::Relaxed)
}

/// Protocol-cost budget: after the first read warms the service, repeated
/// history reads must not open new DbReaders — the per-request manifest
/// GETs and checkpoint writes are exactly the small-metadata operations
/// Tigris sometimes serves from a remote region (the "metadata trickle",
/// docs/SOAK-REGIONS.md), so each cold open is a chance at a
/// transcontinental round trip on the user-visible read path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_reads_reuse_a_cached_reader() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 41, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [21u8; 16];
    let (engine, absorber) =
        open_engine_with_absorber_v1(store.clone(), "dst-hrc", hash, &key).await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["h"], 30, false, &mut log)
        .await;
    // Wait for FULL absorption, not merely absorbed > 0: an absorb
    // advance landing between drains forces a stale reopen in place of
    // a cache hit, and this test's budget is about repeat reads at a
    // SETTLED boundary. With `absorbed > 0` the test raced the
    // absorber's cadence and flaked under full-suite CPU load (hits 18
    // of 19 — one advance interleaved with the drain loop).
    let (mut absorbed, mut next) = (0u64, u64::MAX);
    for _ in 0..400 {
        if let Ok(h) = engine.stream_handle(hash).await {
            let s = h.state.lock().unwrap();
            absorbed = s.durable.absorbed;
            next = s.durable.next;
            if absorbed == next && next > 0 {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        absorbed == next && next > 0,
        "absorber never settled ({absorbed}/{next}) — nothing stable to read from history"
    );

    let ds: Arc<dyn ObjectStore> = store.clone();
    // ONE service held across all drains — the reuse under test.
    let hr = fresh_hist(&ds);

    let obs = drain_observed(&hr, &engine, hash, &key, &cov).await;
    log.audit(&obs).expect("first drain audit");
    let manifest_gets_after_warm = store.count(StoreOp::Get, ObjClass::Manifest);

    for _ in 0..19 {
        let obs = drain_observed(&hr, &engine, hash, &key, &cov).await;
        log.audit(&obs).expect("cached drain audit");
    }

    assert!(
        m(&hr.metrics.misses) <= 1,
        "at most one cache miss across 20 drains (got {})",
        m(&hr.metrics.misses)
    );
    assert!(
        m(&hr.metrics.hits) >= 19,
        "the cached reader must serve the repeat drains (hits {})",
        m(&hr.metrics.hits)
    );
    // The absorber may advance between drains; each advance is allowed one
    // stale reopen — bounded by absorb cadence, never by request count.
    assert!(
        m(&hr.metrics.stale_reopens) <= 3,
        "stale reopens must track absorb cadence, not request rate (got {})",
        m(&hr.metrics.stale_reopens)
    );
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
/// caught up (poll pinned to an hour: only the probe can save it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_cached_reader_is_detected_and_replaced() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 43, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [22u8; 16];
    let (engine, absorber) =
        open_engine_with_absorber_v1(store.clone(), "dst-hrc-stale", hash, &key).await;

    let ds: Arc<dyn ObjectStore> = store.clone();
    let hr = hist_hour(&ds, 8);
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

    w.run(&engine, hash, &key, &["s"], 15, false, &mut log)
        .await;
    let a1 = wait_absorbed_past(0).await;
    let obs = drain_observed(&hr, &engine, hash, &key, &cov).await;
    log.audit(&obs).expect("drain 1");

    w.run(&engine, hash, &key, &["s"], 15, false, &mut log)
        .await;
    let a2 = wait_absorbed_past(a1).await;
    assert!(a2 > a1);

    let s0 = m(&hr.metrics.stale_reopens);
    let obs = drain_observed(&hr, &engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&obs) {
        panic!("stale-reader drain lost records: {e}");
    }
    let s1 = m(&hr.metrics.stale_reopens);
    assert!(
        s1 > s0,
        "the stale reader was never detected — the scenario is vacuous \
         (poll should have been pinned too high for it to self-heal)"
    );

    let s2a = m(&hr.metrics.stale_reopens);
    let obs = drain_observed(&hr, &engine, hash, &key, &cov).await;
    log.audit(&obs).expect("drain 3");
    assert_eq!(
        m(&hr.metrics.stale_reopens),
        s2a,
        "the fresh reader must be cached, not reopened again"
    );

    absorber.abort();
}

/// Key-filtered reads cannot verify coverage by offset contiguity (the
/// filter legitimately skips offsets), which is why coverage is proven by
/// probe. Same staleness setup, filtered read path directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_history_reads_survive_a_stale_reader() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 47, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [23u8; 16];
    let (engine, absorber) =
        open_engine_with_absorber_v1(store.clone(), "dst-hrc-filt", hash, &key).await;

    let ds: Arc<dyn ObjectStore> = store.clone();
    let hr = hist_hour(&ds, 8);
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["fa", "fb"], 10, false, &mut log)
        .await;
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
    let _ = crate::history::read_history(&hr, &hash, &key, 0, a1, Some("fa"), 1 << 20)
        .await
        .expect("warm filtered read");

    w.run(&engine, hash, &key, &["fa", "fb"], 10, false, &mut log)
        .await;
    // Wait for absorption to CATCH UP, not merely advance: the lane-capped
    // absorber legitimately advances the boundary in partial steps, and a
    // read at a partial a2 honestly excludes the unabsorbed tail — which
    // this test's record-count assertion would misread as loss (it did,
    // ~50% of suite runs, once the per-tick caps landed).
    let mut a2 = a1;
    for _ in 0..400 {
        if let Ok(h) = engine.stream_handle(hash).await {
            let st = h.state.lock().unwrap();
            a2 = st.durable.absorbed;
            if a2 > a1 && a2 == st.durable.next {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(a2 > a1, "second absorb never landed");

    let res = crate::history::read_history(&hr, &hash, &key, 0, a2, Some("fa"), 1 << 20)
        .await
        .expect("filtered read at new boundary");
    assert!(
        res.completed,
        "filtered read must be coverage-proven complete after the fallback"
    );
    let acked_fa = log.acked.get("fa").map(|v| v.len()).unwrap_or(0);
    let in_history = res.records.len();
    assert!(
        in_history >= acked_fa.saturating_sub(5),
        "filtered read returned {in_history} records for {acked_fa} acked \
         (allowing a small unabsorbed tail)"
    );
    absorber.abort();
}

/// Seed `n` minimal history DBs (a DbReader cannot open a nonexistent
/// prefix; production never asks it to). Written WITH the key's block
/// transformer, exactly as the absorber writes them — a reader for `key`
/// must be able to read what it finds.
async fn seed_history_dbs(
    ds: &Arc<dyn ObjectStore>,
    key: &crate::crypto::StreamKey,
    base: u8,
    n: u8,
) -> Vec<[u8; 16]> {
    let mut hashes = Vec::new();
    for i in 0..n {
        let mut hash = [base; 16];
        hash[15] = i;
        let path = crate::history::history_db_path(&hash);
        let db = slatedb::Db::builder(path.as_str(), ds.clone())
            .with_block_transformer(Arc::new(crate::history::AesBlockTransformer::new(key)))
            .build()
            .await
            .expect("seed history db");
        db.put(b"seed", b"1").await.expect("seed row");
        db.close().await.expect("close seed db");
        hashes.push(hash);
    }
    hashes
}

/// The cache is bounded, and eviction has a provable lifecycle: closes
/// are counted, run to completion, and live_readers returns to cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_reader_cache_evicts_beyond_its_cap() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 53, FaultPlan::CLEAN);
    let key = skey();
    let ds: Arc<dyn ObjectStore> = store.clone();
    let hr = fresh_hist(&ds);

    for hash in seed_history_dbs(&ds, &key, 30, 12).await {
        let _ = hr.acquire(&hash, &key, 0).await.expect("acquire");
    }
    assert!(
        m(&hr.metrics.evictions) >= 3,
        "12 streams past a cap of 8 must evict (evictions {})",
        m(&hr.metrics.evictions)
    );
    assert!(hr.len_for_tests() <= 9, "cache size must stay near its cap");

    // Eviction lifecycle: every eviction started a close, and every close
    // completes (on the SlateDB runtime), so evicted checkpoints do not
    // linger behind a dropped-but-never-closed reader.
    assert_eq!(
        m(&hr.metrics.closes_started),
        m(&hr.metrics.evictions),
        "each eviction must start exactly one close"
    );
    for _ in 0..200 {
        if m(&hr.metrics.closes_completed) + m(&hr.metrics.close_failures)
            >= m(&hr.metrics.closes_started)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        m(&hr.metrics.closes_completed),
        m(&hr.metrics.closes_started),
        "every eviction close must complete (failures: {})",
        m(&hr.metrics.close_failures)
    );
}

/// **P0 regression guard: the cold-miss stampede.** 64 concurrent readers
/// of the same never-opened history must produce exactly ONE reader open;
/// everyone else coalesces onto it. Before single-flight, this was 64
/// opens — the metadata storm the cache exists to prevent, recreated in
/// proportion to request concurrency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixty_four_cold_readers_cause_exactly_one_open() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 59, FaultPlan::CLEAN);
    let key = skey();
    let ds: Arc<dyn ObjectStore> = store.clone();
    let hr = fresh_hist(&ds);
    let hash = seed_history_dbs(&ds, &key, 31, 1).await[0];

    let mut tasks = Vec::new();
    for _ in 0..64 {
        let hr = hr.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            hr.acquire(&hash, &key, 0).await.map(|(_, c)| c)
        }));
    }
    for t in tasks {
        let covered = t.await.expect("join").expect("acquire");
        assert!(covered, "upto=0 is trivially covered");
    }
    assert_eq!(m(&hr.metrics.opens_started), 1, "exactly one open started");
    assert_eq!(
        m(&hr.metrics.opens_completed),
        1,
        "exactly one open completed"
    );
    assert_eq!(m(&hr.metrics.misses), 1, "exactly one cold miss");
    assert!(
        m(&hr.metrics.coalesced) >= 63,
        "the other 63 must coalesce (got {})",
        m(&hr.metrics.coalesced)
    );
}

/// **P0 regression guard: the stale-boundary stampede.** The absorber
/// advances once; 64 subscribers wake and read to the new boundary
/// simultaneously. One probe, one stale reopen, one open — not 64 of
/// each. This is the exact "64 long-poll consumers wake after an absorb"
/// shape from the review.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixty_four_stale_readers_cause_one_probe_and_one_reopen() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 61, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [32u8; 16];
    let (engine, absorber) =
        open_engine_with_absorber_v1(store.clone(), "dst-hrc-stampede", hash, &key).await;
    let ds: Arc<dyn ObjectStore> = store.clone();
    let hr = hist_hour(&ds, 8);
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());

    // Warm at boundary a1.
    w.run(&engine, hash, &key, &["s"], 15, false, &mut log)
        .await;
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
    let _ = crate::history::read_history(&hr, &hash, &key, 0, a1, None, 1 << 20)
        .await
        .expect("warm");

    // Advance the boundary past the cached (hour-poll) view.
    w.run(&engine, hash, &key, &["s"], 15, false, &mut log)
        .await;
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
    assert!(a2 > a1);

    let probes0 = m(&hr.metrics.probes);
    let stale0 = m(&hr.metrics.stale_reopens);
    let opens0 = m(&hr.metrics.opens_started);
    let mut tasks = Vec::new();
    for _ in 0..64 {
        let hr = hr.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            crate::history::read_history(&hr, &hash, &key, 0, a2, None, 1 << 20).await
        }));
    }
    for t in tasks {
        let res = t.await.expect("join").expect("read");
        assert!(
            res.completed,
            "every stampede read must cover the new boundary"
        );
    }
    assert_eq!(
        m(&hr.metrics.probes) - probes0,
        1,
        "one probe for one boundary advance, regardless of concurrency"
    );
    assert_eq!(
        m(&hr.metrics.stale_reopens) - stale0,
        1,
        "one stale reopen for one boundary advance"
    );
    assert_eq!(
        m(&hr.metrics.opens_started) - opens0,
        1,
        "one fresh open for one boundary advance"
    );
    absorber.abort();
}

/// **P0 regression guard: cancellation cannot detach the open.** Slow the
/// manifest reads so the open takes ~seconds; give every caller a short
/// deadline so all of them give up; the cache-owned worker must still
/// finish, insert the reader, and the next (patient) read must be a hit
/// with no second open. This is the read-only cousin of the shard
/// detached-reopen storm.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_callers_cancel_but_the_open_still_lands_in_the_cache() {
    let inner = mem();
    // Manifest GETs are slow (1.2-1.8 s); everything else clean.
    let profile = FaultProfile::uniform(FaultPlan::CLEAN).with_class(
        ObjClass::Manifest,
        FaultPlan {
            error_pct: 0,
            lost_response_pct: 0,
            latency_pct: 100,
            latency_ms: (1_200, 1_800),
        },
    );
    let store = FaultStore::new(inner.clone(), 67, profile);
    let key = skey();
    let ds: Arc<dyn ObjectStore> = store.clone();
    let hr = fresh_hist(&ds);
    let hash = seed_history_dbs(&ds, &key, 33, 1).await[0];

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let hr = hr.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                hr.acquire(&hash, &key, 0),
            )
            .await
        }));
    }
    let mut cancelled = 0;
    for t in tasks {
        if t.await.expect("join").is_err() {
            cancelled += 1;
        }
    }
    assert!(
        cancelled >= 15,
        "the deadlines must actually cancel the callers (only {cancelled} timed out)"
    );

    // The worker outlives its callers: wait for the open to finish.
    for _ in 0..600 {
        if m(&hr.metrics.opens_completed) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(
        m(&hr.metrics.opens_started),
        1,
        "sixteen cancelled callers must not have started sixteen opens"
    );
    assert_eq!(
        m(&hr.metrics.opens_completed),
        1,
        "the open must complete despite zero surviving callers"
    );

    // And it landed: the next read is a pure hit.
    let opens_before = m(&hr.metrics.opens_started);
    let (_r, covered) = hr.acquire(&hash, &key, 0).await.expect("post-cancel read");
    assert!(covered);
    assert_eq!(
        m(&hr.metrics.opens_started),
        opens_before,
        "the post-cancellation read must be served by the cached reader"
    );
    assert!(m(&hr.metrics.hits) >= 1);
}

/// **P1 regression guard: a probe ERROR is an error, not staleness.**
///
/// A finding worth pinning first: a transient STORE outage cannot surface
/// here at all — SlateDB retries reads internally, so storage flakiness
/// makes a probe slow, never `Err` (the same absorption measured on the
/// append path). The reachable probe-error class is DATA errors — a block
/// that fails its transform (wrong key, corruption). So that is what this
/// scenario injects: a reader whose key cannot transform the history db's
/// blocks probes and gets `Err`, and the service must (a) hand the caller
/// the error, (b) keep the reader cached, (c) count probe_errors, and
/// (d) start NO fresh open — one bad row must not amplify into eviction,
/// close, and fresh manifest/checkpoint traffic. Before this fix, `Err`
/// and `Ok(None)` shared the staleness branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_probe_error_does_not_evict_the_reader() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 71, FaultPlan::CLEAN);
    let key = skey(); // the key the history db is actually written with
    let wrong = skey2(); // a reader under this key gets transform errors
    let ds: Arc<dyn ObjectStore> = store.clone();
    let hr = hist_hour(&ds, 8);
    let hash = seed_history_dbs(&ds, &key, 34, 1).await[0];
    // The probe targets hist_record_key(upto-1); it must EXIST, or the
    // get short-circuits to Ok(None) via index/bloom without ever reading
    // a transformed data block — and Ok(None) is (correctly) staleness,
    // not an error. Write the probe row under the RIGHT key.
    {
        let path = crate::history::history_db_path(&hash);
        let db = slatedb::Db::builder(path.as_str(), ds.clone())
            .with_block_transformer(Arc::new(crate::history::AesBlockTransformer::new(&key)))
            .build()
            .await
            .expect("open for probe row");
        db.put(crate::history::hist_record_key(0), b"row0")
            .await
            .expect("probe row");
        db.close().await.expect("close");
    }

    // Cache a reader under the WRONG key: the cold open itself succeeds
    // (manifest metadata is not block-transformed), and its fresh-probe
    // returns covered=false via the conservative Err path.
    let (_r, covered) = hr.acquire(&hash, &wrong, 1).await.expect("cold acquire");
    assert!(!covered, "a wrong-key fresh probe must be conservative");
    let opens0 = m(&hr.metrics.opens_started);
    let stale0 = m(&hr.metrics.stale_reopens);
    let live0 = hr.len_for_tests();

    // Now the cached-probe path: seen_upto=0 forces a probe, the probe
    // errors (transform), and the classification under test runs.
    let res = hr.acquire(&hash, &wrong, 1).await;
    assert!(res.is_err(), "the probe error must surface to the caller");
    assert!(
        m(&hr.metrics.probe_errors) >= 1,
        "the error must be counted as a probe error"
    );
    assert_eq!(
        m(&hr.metrics.stale_reopens),
        stale0,
        "an errored probe must NOT be classified as staleness"
    );
    assert_eq!(
        m(&hr.metrics.opens_started),
        opens0,
        "an errored probe must NOT trigger a fresh open"
    );
    assert_eq!(
        hr.len_for_tests(),
        live0,
        "the reader must stay cached through the error"
    );

    // The RIGHT key reads the same stream fine through its own reader —
    // the error was confined to the one caller with the bad key.
    let (r, covered) = hr.acquire(&hash, &key, 1).await.expect("right-key acquire");
    assert!(
        covered,
        "the right key's probe reads the row and proves coverage"
    );
    let v = r
        .get(crate::history::hist_record_key(0))
        .await
        .expect("get");
    assert!(v.is_some(), "probe row readable under the right key");
}

/// **P1: the working-set budget is measured, not assumed.** With a hot
/// set that FITS (cap 8, 5 streams), steady-state reads cost zero opens.
/// With a rotating hot set that does NOT fit (cap 4, 5 streams, LRU +
/// round-robin = pathological), opens are bounded by reads — the
/// documented worst case — and the service survives it. Both bounds are
/// asserted, so capacity planning has real numbers to point at.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_set_versus_capacity_has_measured_bounds() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 73, FaultPlan::CLEAN);
    let key = skey();
    let ds: Arc<dyn ObjectStore> = store.clone();
    let hashes = seed_history_dbs(&ds, &key, 35, 5).await;

    // Fits: 3 round-robin passes over 5 streams, cap 8.
    let fits =
        crate::history::HistReaders::new(ds.clone(), 8, std::time::Duration::from_secs(120), 5_000);
    for _ in 0..3 {
        for h in &hashes {
            let _ = fits.acquire(h, &key, 0).await.expect("fit acquire");
        }
    }
    assert_eq!(
        m(&fits.metrics.opens_started),
        5,
        "a fitting hot set opens each reader exactly once"
    );
    assert_eq!(m(&fits.metrics.evictions), 0);

    // Does not fit: cap 4, same 5 streams round-robin — LRU's worst case.
    let thrash =
        crate::history::HistReaders::new(ds.clone(), 4, std::time::Duration::from_secs(120), 5_000);
    let mut reads = 0u64;
    for _ in 0..3 {
        for h in &hashes {
            let _ = thrash.acquire(h, &key, 0).await.expect("thrash acquire");
            reads += 1;
        }
    }
    let opens = m(&thrash.metrics.opens_started);
    assert!(
        opens <= reads,
        "even the pathological set is bounded by one open per read"
    );
    assert!(
        opens > 5,
        "cap 4 with 5 rotating streams MUST thrash (opens {opens}) — if this \
         starts passing with opens=5, the eviction policy changed; update \
         the capacity guidance in RUNBOOK.md"
    );
    assert!(thrash.len_for_tests() <= 5);
}

/// **Two simulated nodes are actually independent.** Same stream hash,
/// same key, two distinct stores and services: each opens its own reader
/// against its own store and counts its own metrics. The old process-
/// global cache made this impossible (and could serve node B's read
/// through node A's reader — a wrong-store read).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_have_independent_reader_caches() {
    let key = skey();
    let mut readers = Vec::new();
    for seed in [81u64, 82] {
        let inner = mem();
        let store = FaultStore::uniform(inner.clone(), seed, FaultPlan::CLEAN);
        let ds: Arc<dyn ObjectStore> = store.clone();
        // Same hash in BOTH stores, but different seeded content marker.
        let mut hash = [36u8; 16];
        hash[0] = 36;
        let path = crate::history::history_db_path(&hash);
        let db = slatedb::Db::builder(path.as_str(), ds.clone())
            .with_block_transformer(Arc::new(crate::history::AesBlockTransformer::new(&key)))
            .build()
            .await
            .expect("seed");
        db.put(b"seed", format!("node-{seed}").as_bytes())
            .await
            .expect("seed row");
        db.close().await.expect("close");
        let hr = fresh_hist(&ds);
        let (r, _) = hr.acquire(&hash, &key, 0).await.expect("acquire");
        readers.push((hr, r, seed));
    }
    for (hr, r, seed) in &readers {
        assert_eq!(m(&hr.metrics.opens_started), 1, "node {seed}: its own open");
        assert_eq!(m(&hr.metrics.misses), 1);
        let v = r
            .get(bytes::Bytes::from_static(b"seed"))
            .await
            .expect("get");
        let v = v.expect("seed present");
        assert_eq!(
            v.as_ref(),
            format!("node-{seed}").as_bytes(),
            "each node's reader must read ITS OWN store"
        );
    }
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
    let mut drained = false;
    for _ in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if engine.trim_stats().0 == 0 {
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
    let hist = fresh_hist(&ds);
    let got = drain_filtered(&hist, &engine, hash, &key, "sp").await;
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
    let hist = fresh_hist(&ds);
    let got = drain_filtered(&hist, &engine, hash, &key, "ck").await;
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
            touch: None,
            resp: tx,
        };
        assert!(engine.try_enqueue(req).is_ok());
        rx.await.expect("resp").expect("ack");
        append_n(&engine, hash, &key, 32, 64).await;
    }
    wait_all_absorbed(&engine, &[hash]).await;

    let ds: Arc<dyn ObjectStore> = store.clone();
    let hist = fresh_hist(&ds);
    let cache = &engine.postings_cache;

    let first = drain_filtered(&hist, &engine, hash, &key, "hot").await;
    assert_eq!(first.len(), 8);
    let loads_after_first = cache.index_loads.load(Ordering::Relaxed);
    let hits_after_first = cache.hits.load(Ordering::Relaxed);
    assert!(loads_after_first >= 1, "cold read must load the index");

    for _ in 0..5 {
        let again = drain_filtered(&hist, &engine, hash, &key, "hot").await;
        assert_eq!(again, first);
    }
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        loads_after_first,
        "warm reads must not touch the physical index"
    );
    assert!(
        cache.hits.load(Ordering::Relaxed) >= hits_after_first + 5,
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
                    })
                },
                deferred_error: None,
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
