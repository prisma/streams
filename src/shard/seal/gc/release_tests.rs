#![cfg(test)]
//! The release split on `home` (S6-DESIGN §3.2): an object's last reference
//! in this shard makes an own object `'G'` garbage and another home's (an
//! ancestor's) a pending release `'E'`, which only that home's lineage
//! deletes (L1). Either way a running read pins it.
//!
//! Logging (§3.2 step 3): the collector writes the due, unpinned releases as
//! one log per home under that home's root, deletes their rows, and marks a
//! home it no longer references anything of as released-all.

use slatedb::WriteBatch;

use super::super::gc_tests::gc_rig;
use super::super::tests::Rig;
use crate::spill::gc::{self, ObjectKind, Refcount, Release};
use crate::spill::lineage;
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

/// Objects of `homes`, `n` of each (seq 1..=n), each with one live chunk.
async fn held(rig: &Rig, homes: &[u32], n: u64) -> Vec<ObjectId> {
    let mut batch = WriteBatch::new();
    let mut ids = Vec::new();
    for &home in homes {
        for seq in 1..=n {
            let id = ObjectId {
                home,
                epoch: 4,
                seq,
            };
            let r = Refcount {
                live_bytes: 5,
                live_chunks: 1,
                created_ms: 0,
                object_len: 40,
            };
            batch.put(gc::refcount_key(id), gc::encode_refcount(&r));
            ids.push(id);
        }
    }
    rig.engine.db.write(batch).await.unwrap();
    ids
}

/// Zero `ids`' rows through the decrement path, durably.
async fn zero(rig: &Rig, ids: &[ObjectId]) {
    let decrements: gc::Decrements = ids.iter().map(|id| (*id, (5, 1))).collect();
    let _refs = rig.engine.gc_state().refs.lock().await;
    let mut batch = WriteBatch::new();
    let zeroed = rig
        .engine
        .stage_decrements(&mut batch, &decrements)
        .await
        .unwrap();
    rig.engine.write_decrements(batch, &zeroed).await.unwrap();
}

/// The store object at `path`, if any.
async fn object(rig: &Rig, path: String) -> Option<bytes::Bytes> {
    match object_store::ObjectStoreExt::get(&*rig.store, &path.into()).await {
        Ok(got) => Some(got.bytes().await.unwrap()),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(e) => panic!("{e}"),
    }
}

/// The entries of release log `seq` of this opening under `home`'s root.
async fn logged(rig: &Rig, home: u32, seq: u64) -> Option<Vec<lineage::Entry>> {
    let root = rig.engine.object_root(home);
    let path = lineage::release_log_path(&root, rig.engine.home(), rig.engine.writer_epoch, seq);
    let body = object(rig, path).await?;
    Some(lineage::decode(&body).expect("a release log"))
}

async fn released_all(rig: &Rig, home: u32) -> bool {
    let root = rig.engine.object_root(home);
    let path = lineage::released_all_path(&root, rig.engine.home());
    object(rig, path).await.is_some()
}

/// A due release is logged under its home's root and its row goes; a home
/// this shard references nothing of then gets its released-all marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_due_release_is_logged_under_its_home_and_the_home_is_marked_released() {
    let rig = gc_rig("release-log").await;
    let ids = held(&rig, &[7], 2).await;
    zero(&rig, &ids).await;
    rig.engine.gc_pass().await;
    use ObjectKind::Segment;
    assert_eq!(
        logged(&rig, 7, 0).await,
        Some(vec![(Segment, 4, 1), (Segment, 4, 2)])
    );
    assert_eq!(logged(&rig, 7, 1).await, None, "one log per home and tick");
    assert_eq!(releases(&rig).await, []);
    assert!(released_all(&rig, 7).await);
    assert_eq!(rig.engine.garbage_rows().await.unwrap(), []);
    rig.engine.begin_close();
}

/// Each home gets its own log of its own objects, and a home with a row left
/// gets no released-all marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn each_home_gets_its_own_log_and_a_home_still_held_no_marker() {
    let rig = gc_rig("release-homes").await;
    let ids = held(&rig, &[7, 9], 2).await;
    let (kept, gone): (Vec<ObjectId>, Vec<ObjectId>) =
        ids.into_iter().partition(|id| id.home == 7 && id.seq == 2);
    zero(&rig, &gone).await;
    rig.engine.gc_pass().await;
    use ObjectKind::Segment;
    assert_eq!(logged(&rig, 7, 0).await, Some(vec![(Segment, 4, 1)]));
    assert_eq!(
        logged(&rig, 9, 1).await,
        Some(vec![(Segment, 4, 1), (Segment, 4, 2)])
    );
    assert!(
        !released_all(&rig, 7).await,
        "home 7's {kept:?} is still held"
    );
    assert!(released_all(&rig, 9).await);
    let rows: Vec<ObjectId> = rig
        .engine
        .refcount_rows()
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(rows, kept);
    rig.engine.begin_close();
}

/// A release waits while a read that started before its zeroing runs, and
/// while its grace lasts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_release_waits_for_its_readers_and_its_grace() {
    let rig = gc_rig("release-wait").await;
    let ids = held(&rig, &[7], 1).await;
    let pin = rig.engine.gc_state().pin();
    zero(&rig, &ids).await;
    assert_eq!(rig.engine.log_releases().await.unwrap(), (0, 1));
    drop(pin);
    assert_eq!(rig.engine.log_releases().await.unwrap(), (1, 0));
    assert_eq!(
        logged(&rig, 7, 0).await,
        Some(vec![(ObjectKind::Segment, 4, 1)])
    );

    let later = held(&rig, &[8], 1).await;
    rig.engine
        .gc_state()
        .configure(|s| s.grace = std::time::Duration::from_secs(3600));
    zero(&rig, &later).await;
    assert_eq!(rig.engine.log_releases().await.unwrap(), (0, 1));
    assert_eq!(logged(&rig, 8, 1).await, None);
    rig.engine.begin_close();
}
