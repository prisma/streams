#![cfg(test)]
//! The maintenance gate on one engine (S4-DESIGN §5.1, §5.3): an index batch outside the
//! committer waits while a split holds the gate exclusive, and lands once it is released; a purge
//! request answers `Busy` at once instead; while collection is off, the orphan sweep and the
//! purge pass stand down, and resume after.

use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use rand::{SeedableRng, rngs::StdRng};
use slatedb::WriteBatch;

use super::super::gc_tests::{gc_rig, rounds, stored};
use super::super::tests::Oracle;
use super::GcError;
use crate::spill::gc::{HeadState, ObjectKind};
use crate::spill::object::ObjectId;

const HELD: Duration = Duration::from_millis(200);
const BOUND: Duration = Duration::from_secs(5);

/// **An index batch waits out the split's hold.** With the gate held exclusive, a batch
/// through `write_index_batch` is still pending after 200 ms and its row is not stored; once
/// the hold is released, the same batch lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_index_batch_waits_while_the_gate_is_held_exclusive() {
    let rig = gc_rig("gate-batch").await;
    let gate = &rig.engine.gc_state().gate;
    let hold = gate.exclusive(BOUND).await.expect("the gate is free");
    let mut batch = WriteBatch::new();
    batch.put(b"\xffgate-probe", b"v");
    let mut write = Box::pin(rig.engine.write_index_batch(batch));
    assert!(
        tokio::time::timeout(HELD, &mut write).await.is_err(),
        "the batch waits for the gate"
    );
    assert_eq!(rig.engine.db.get(b"\xffgate-probe").await.unwrap(), None);
    drop(hold);
    tokio::time::timeout(BOUND, write)
        .await
        .expect("the batch lands once the gate is released")
        .unwrap();
    assert_eq!(
        rig.engine.db.get(b"\xffgate-probe").await.unwrap(),
        Some(Bytes::from_static(b"v"))
    );
    rig.engine.begin_close();
}

/// **A purge request is `Busy` at once while a split holds the gate.** With the gate held
/// exclusive and `refs` held (as by a writer waiting at the gate), `request_purge` answers
/// `Busy` within 1 s, waiting for neither, and writes no head row; once both are released, the
/// same request lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_purge_request_is_busy_at_once_while_a_split_holds_the_gate() {
    let rig = gc_rig("gate-purge-busy").await;
    let dead = [0x3C; 16];
    let gc = rig.engine.gc_state();
    let refs = gc.refs.lock().await;
    let hold = gc.gate.exclusive(BOUND).await.expect("the gate is free");
    let asked = tokio::time::timeout(Duration::from_secs(1), rig.engine.request_purge(dead, 0))
        .await
        .expect("the request waits for nothing");
    assert!(matches!(asked, Err(GcError::Busy)), "{asked:?}");
    assert_eq!(rig.engine.head_row(&dead).await.unwrap(), None);
    drop((hold, refs));
    rig.engine.request_purge(dead, 0).await.unwrap();
    let head = rig.engine.head_row(&dead).await.unwrap().expect("a head");
    assert_eq!(head.state, HeadState::Requested);
    rig.engine.begin_close();
}

/// **A final source takes no index batch, and a purge request on it is `Busy`** (#251 R3-d;
/// DESIGN-DECISION-c3-incomplete-rig-2026-11-11 Addendum 5). Once a topology operation wrote its
/// `'T'` and set the gate's final flag, a batch through `write_index_batch` is refused before it
/// writes, with the exact reason, and `request_purge` answers `Busy` and writes no head row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_final_source_refuses_an_index_batch_and_answers_a_purge_request_busy() {
    let rig = gc_rig("gate-final").await;
    let dead = [0x3D; 16];
    rig.engine.gc_state().gate.set_final();
    let mut batch = WriteBatch::new();
    batch.put(b"\xffgate-final", b"v");
    let written = rig
        .engine
        .write_index_batch(batch)
        .await
        .map_err(|e| e.to_string());
    let why = "the source is final: a topology operation wrote 'T'";
    assert!(
        written.as_ref().is_err_and(|e| e.contains(why)),
        "{written:?}"
    );
    assert_eq!(rig.engine.db.get(b"\xffgate-final").await.unwrap(), None);
    let asked = rig.engine.request_purge(dead, 0).await;
    assert!(matches!(asked, Err(GcError::Busy)), "{asked:?}");
    assert_eq!(rig.engine.head_row(&dead).await.unwrap(), None);
    rig.engine.begin_close();
}

/// **The orphan sweep stands down while collection is off.** An older owner's orphan is
/// neither listed nor deleted by a sweep with collection off; the next sweep, with it on,
/// deletes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_orphan_sweep_stands_down_while_collection_is_off() {
    let rig = gc_rig("gate-sweep").await;
    let orphan = ObjectId {
        home: 0,
        epoch: rig.engine.writer_epoch - 1,
        seq: 1,
    };
    let path = rig.engine.object_path_of(ObjectKind::Segment, orphan);
    object_store::ObjectStoreExt::put(&*rig.store, &path.into(), Bytes::from_static(b"o").into())
        .await
        .unwrap();
    let gate = &rig.engine.gc_state().gate;
    gate.set_collection_off(true);
    let report = rig.engine.sweep_orphans().await.unwrap();
    assert_eq!((report.listed, report.orphans), (0, 0), "{report:?}");
    assert!(stored(&rig, "hseg").await.contains_key(&orphan));
    gate.set_collection_off(false);
    let report = rig.engine.sweep_orphans().await.unwrap();
    assert_eq!((report.listed, report.orphans), (1, 1), "{report:?}");
    assert!(!stored(&rig, "hseg").await.contains_key(&orphan));
    rig.engine.begin_close();
}

/// **The purge pass stands down while collection is off.** A due purge request survives a
/// pass with collection off, still `Requested` with its signal up; the next pass, with it on,
/// purges and reclaims it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_purge_pass_stands_down_while_collection_is_off() {
    let rig = gc_rig("gate-purge").await;
    let dead = [0x3A; 16];
    let mut rng = StdRng::seed_from_u64(0x6A7E);
    rounds(&rig, &mut rng, &[dead], &Mutex::new(Oracle::new()), 1).await;
    let gc = rig.engine.gc_state();
    rig.engine.request_purge(dead, 0).await.unwrap();
    gc.gate.set_collection_off(true);
    rig.engine.gc_pass().await;
    let head = rig.engine.head_row(&dead).await.unwrap().expect("a head");
    assert_eq!(head.state, HeadState::Requested, "collection off: no purge");
    assert!(gc.heads_signal.load(std::sync::atomic::Ordering::SeqCst) > 0);
    gc.gate.set_collection_off(false);
    rig.engine.gc_pass().await;
    let head = rig.engine.head_row(&dead).await.unwrap().expect("a head");
    assert_eq!((head.state, head.owes()), (HeadState::Purged, false));
    rig.engine.begin_close();
}
