//! One lock owns id/event/lifetime association.

#![allow(unused_imports)]
use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use super::super::{TraceEventKind, TraceOutcome, TraceStore};
use super::*;
use crate::dst::{ObjClass, StoreOp};

// ---- PR 3.2: one lock owns id/event/lifetime association ----------

/// Acceptance 1-3: N tasks race `begin` through one barrier, then
/// completions land in REVERSE id order. Every event must resolve
/// exactly once — completion locates events by id through the log's
/// map, never by arithmetic on insertion position. Under the
/// pre-3.2 shape (id allocated with an atomic BEFORE the vector
/// lock) this interleaving could push events out of id order,
/// completions then found the wrong slot, both events stayed
/// Pending, and `reset()` panicked forever.
#[test]
fn concurrent_begins_with_reverse_finishes_resolve_every_event_exactly_once() {
    let s = TraceStore::verbatim(Arc::new(object_store::memory::InMemory::new()));
    const N: usize = 8;
    for round in 0..50 {
        let barrier = std::sync::Barrier::new(N);
        let mut ops: Vec<super::super::TraceOperation> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..N)
                .map(|_| {
                    let st = s.st.clone();
                    let b = &barrier;
                    scope.spawn(move || {
                        b.wait(); // all N contend for the trace lock at once
                        st.begin_operation(StoreOp::Put, "shards/x/wal/c.sst", None, None, None)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let mut ids: Vec<u64> = ops.iter().map(|o| o.seq.unwrap()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), N, "round {round}: ids must be unique");
        // Finish in REVERSE id order.
        ops.sort_by_key(|o| o.seq.unwrap());
        for op in ops.into_iter().rev() {
            op.finish(TraceOutcome::Ok);
        }
        let evs = s.events();
        assert_eq!(evs.len(), N, "round {round}");
        for w in evs.windows(2) {
            assert!(
                w[0].seq < w[1].seq,
                "round {round}: vector must be in id order: {evs:?}"
            );
        }
        assert!(
            evs.iter().all(|e| e.outcome == TraceOutcome::Ok),
            "round {round}: every event resolves exactly once, none stay Pending: {evs:?}"
        );
        s.reset(); // acceptance 9: nothing leaked a lifetime
        assert!(s.events().is_empty());
    }
}

/// Acceptance 4: reset racing `begin` can neither clear an active
/// operation out from under its completion nor orphan one. All
/// interleavings serialize on the one trace lock: reset either wins
/// (clears a quiet window; the op then lands in the fresh window) or
/// observes the active lifetime and refuses. After joining, nothing
/// is active and nothing is Pending — deterministically, in every
/// round.
#[test]
fn reset_racing_with_begin_cannot_orphan_an_operation() {
    let s = TraceStore::verbatim(Arc::new(object_store::memory::InMemory::new()));
    for round in 0..200 {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let st = s.st.clone();
            let b = &barrier;
            scope.spawn(move || {
                b.wait();
                let op = st.begin_operation(StoreOp::Put, "shards/x/wal/r.sst", None, None, None);
                std::thread::yield_now();
                op.finish(TraceOutcome::Ok);
            });
            barrier.wait();
            // A refused reset is a legal outcome of the race; a
            // poisoned lock or a lost event is not.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| s.reset()));
        });
        let log = s.st.log.lock().unwrap();
        assert!(log.active.is_empty(), "round {round}: orphaned lifetime");
        assert!(
            log.events
                .iter()
                .all(|e| e.outcome != TraceOutcome::Pending),
            "round {round}: a completed operation may never stay Pending"
        );
        drop(log);
        s.reset(); // at quiescence reset must always work
    }
}
