#![cfg(test)]
//! The release split on `home` (S6-DESIGN §3.2): an object's last reference
//! in this shard makes an own object `'G'` garbage and another home's (an
//! ancestor's) a pending release `'E'`, which only that home's lineage
//! deletes (L1). Either way a running read pins it.

use slatedb::WriteBatch;

use super::super::gc_tests::gc_rig;
use super::super::tests::Rig;
use crate::spill::gc::{self, ObjectKind, Refcount, Release};
use crate::spill::object::ObjectId;

/// Every `'E'` row, decoded.
async fn releases(rig: &Rig) -> Vec<(ObjectKind, ObjectId, Release)> {
    let prefix = crate::spill::runindex::sentinel_prefix(gc::RELEASE_TAG);
    let mut rows = rig.engine.db.scan_prefix(&prefix[..], ..).await.unwrap();
    let mut out = Vec::new();
    while let Some(kv) = rows.next().await.unwrap() {
        let (kind, id) = gc::parse_release_key(&kv.key).expect("an 'E' key");
        out.push((
            kind,
            id,
            gc::decode_release(&kv.value).expect("an 'E' value"),
        ));
    }
    out
}

/// An own object and another home's, each with one live chunk.
async fn own_and_ancestor(rig: &Rig) -> (ObjectId, ObjectId) {
    let own = ObjectId {
        home: rig.engine.home(),
        epoch: rig.engine.writer_epoch,
        seq: 1,
    };
    let ancestor = ObjectId { home: 7, ..own };
    let mut batch = WriteBatch::new();
    for id in [own, ancestor] {
        let r = Refcount {
            live_bytes: 5,
            live_chunks: 1,
            created_ms: 0,
            object_len: 40,
        };
        batch.put(gc::refcount_key(id), gc::encode_refcount(&r));
    }
    rig.engine.db.write(batch).await.unwrap();
    (own, ancestor)
}

/// A decrement (the purge's reclaim, a consolidation's or the cleaner's
/// commit) that zeroes both rows stages `'G'` for the own object and `'E'`
/// for the ancestor's, and reports both as zeroed, so reads pin both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_ancestors_last_reference_becomes_a_pending_release_never_garbage() {
    let rig = gc_rig("release-split").await;
    let (own, ancestor) = own_and_ancestor(&rig).await;
    let before = crate::shard::now_ms().unsigned_abs();
    let decrements: gc::Decrements = [(own, (5, 1)), (ancestor, (5, 1))].into();
    let mut batch = WriteBatch::new();
    let zeroed = rig.engine.stage_decrements(&mut batch, &decrements).await;
    assert_eq!(zeroed.unwrap(), [own, ancestor]);
    rig.engine.db.write(batch).await.unwrap();
    let garbage = rig.engine.garbage_rows().await.unwrap();
    assert_eq!(garbage.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [own]);
    let pending = releases(&rig).await;
    assert_eq!(pending.len(), 1, "{pending:?}");
    let (kind, id, release) = pending[0];
    assert_eq!(
        (kind, id, release.object_len),
        (ObjectKind::Segment, ancestor, 40)
    );
    let after = crate::shard::now_ms().unsigned_abs();
    assert!((before..=after).contains(&release.zero_ms), "{release:?}");
    assert_eq!(release.due_ms, release.zero_ms, "the rig's grace is zero");
    assert_eq!(rig.engine.refcount_rows().await.unwrap(), []);
    rig.engine.begin_close();
}

/// A merge's retirement of its inputs takes the same split: an own run-index
/// object becomes `'G'` of kind run-index, an inherited one an `'E'` of kind
/// run-index, with each input's length.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_retired_ancestors_run_index_becomes_a_pending_release() {
    let rig = gc_rig("release-ridx").await;
    let (own, ancestor) = own_and_ancestor(&rig).await;
    let mut batch = WriteBatch::new();
    rig.engine
        .stage_unreferenced(&mut batch, ObjectKind::RunIndex, own, 11);
    rig.engine
        .stage_unreferenced(&mut batch, ObjectKind::RunIndex, ancestor, 22);
    rig.engine.db.write(batch).await.unwrap();
    let garbage = rig.engine.garbage_rows().await.unwrap();
    assert_eq!(garbage.len(), 1);
    assert_eq!(
        (garbage[0].0, garbage[0].1.kind, garbage[0].1.object_len),
        (own, ObjectKind::RunIndex, 11)
    );
    let pending: Vec<_> = releases(&rig)
        .await
        .into_iter()
        .map(|(kind, id, r)| (kind, id, r.object_len))
        .collect();
    assert_eq!(pending, [(ObjectKind::RunIndex, ancestor, 22)]);
    rig.engine.begin_close();
}
