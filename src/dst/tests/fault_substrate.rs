//! Fault substrate.

use crate::dst::{FaultPlan, FaultStore, ObjClass};
use object_store::path::Path as ObjPath;
use object_store::{GetOptions, ObjectStore, PutMultipartOptions, PutOptions, PutPayload};
use std::sync::Arc;

fn mem() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
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
#[expect(
    clippy::let_underscore_must_use,
    reason = "faults_actually_fire; the fixture issues puts only to make the fault plan fire and counts the injections afterwards; each put's own outcome is the fault under test"
)]
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
#[expect(
    clippy::let_underscore_must_use,
    reason = "lost_response_counter_tracks_applied_behaviour_only; the fixture issues a multipart put only to prove the lost-response fault cannot apply to it; the put's outcome is not the claim"
)]
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
