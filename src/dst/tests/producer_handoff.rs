//! Producer handoff.

use super::fixture_storage::{mem, open_engine, skey};
use crate::dst::{FaultPlan, FaultProfile, FaultStore, ObjClass, Outcome, Workload, mech};
use object_store::ObjectStore;
use std::sync::Arc;

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
#[expect(
    clippy::too_many_lines,
    reason = "producer handoff scenario; establishing producer state, handing off and retrying duplicates form one causal sequence; helper phases would hide which state the successor failed to inherit"
)]
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
        let _ds: Arc<dyn ObjectStore> = store.clone();
        let handle = b.stream_handle(hash).await.expect("handle");
        let before_next = handle.state.lock().unwrap().durable.next;
        let res = crate::http::read_merged(
            &key,
            &hash,
            &handle,
            &b,
            0,
            None,
            8 * 1024 * 1024,
            crate::shard::Deliver::Durable,
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
#[expect(
    clippy::too_many_lines,
    reason = "ambiguous commit scenario; the durable commit with a lost response, the handoff and the deduplicated retry form one causal sequence; helper phases would hide which boundary lost or duplicated the record"
)]
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
    let _ds: Arc<dyn ObjectStore> = store.clone();
    let handle = b.stream_handle(hash).await.expect("handle");
    let res = crate::http::read_merged(
        &key,
        &hash,
        &handle,
        &b,
        0,
        None,
        8 * 1024 * 1024,
        crate::shard::Deliver::Durable,
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

    let _ds: Arc<dyn ObjectStore> = store.clone();
    let handle = engine.stream_handle(hash).await.expect("handle");
    let res = crate::http::read_merged(
        &key,
        &hash,
        &handle,
        &engine,
        0,
        None,
        1 << 20,
        crate::shard::Deliver::Durable,
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
