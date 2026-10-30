#![cfg(test)]
//! A consolidation job still applies only while its offsets resolve to
//! exactly its chunks: the same object, chunk offset, first and last
//! offsets, and as many of them.

use bytes::Bytes;
use rand::{SeedableRng, rngs::StdRng};

use super::super::gc_tests::{gc_rig, rounds};
use super::super::tests::Oracle;
use super::Job;
use crate::spill::directory::DirEntry;

/// One field of a chunk changed.
type Change = fn(&mut DirEntry);

fn job(hash: [u8; 16], chunks: &[DirEntry]) -> Job {
    let chunks: Vec<(DirEntry, Bytes)> = chunks.iter().map(|e| (e.clone(), Bytes::new())).collect();
    let permit = crate::spill::budget::consolidation_permits()
        .try_acquire_many(1)
        .expect("a permit");
    Job {
        hash,
        chunks,
        _permit: permit,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_job_still_applies_only_to_exactly_its_chunks() {
    let rig = gc_rig("picked").await;
    let hash = [0x46; 16];
    let mut rng = StdRng::seed_from_u64(0x6C30);
    rounds(
        &rig,
        &mut rng,
        &[hash],
        &std::sync::Mutex::new(Oracle::new()),
        2,
    )
    .await;
    let chunks = rig.engine.live_chunks(hash).await.unwrap();
    assert!(chunks.len() >= 2, "{chunks:?}");
    let picked = |chunks: Vec<DirEntry>| {
        let engine = rig.engine.clone();
        async move { engine.still_picked(&job(hash, &chunks)).await.unwrap() }
    };
    assert!(picked(chunks.clone()).await, "exactly its chunks");
    let changes: [(&str, Change); 4] = [
        ("object", |e| e.object.seq += 1_000),
        ("chunk offset", |e| e.chunk_off += 1),
        ("first offset", |e| e.first += 1),
        ("last offset", |e| e.last -= 1),
    ];
    for (what, change) in changes {
        let mut other = chunks.clone();
        change(&mut other[0]);
        assert!(!picked(other).await, "another {what}");
    }
    rig.engine.begin_close();
}
