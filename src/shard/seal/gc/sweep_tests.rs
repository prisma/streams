#![cfg(test)]
//! The orphan sweep's paging (layout 6, package 6): it resumes after its
//! cursor, starts over once its wrap is due and the listing ended with
//! every object decided, lists at most its pages, spares young objects and
//! a newer owner's, and reports what it listed and deleted.

use std::time::Duration;

use bytes::Bytes;
use slatedb::WriteBatch;

use super::super::gc_tests::{gc_rig, stored};
use super::super::tests::Rig;
use super::sweep::SweepReport;
use crate::spill::gc::{self, ObjectKind, Refcount};
use crate::spill::object::ObjectId;

/// Store an object of `len` bytes at `id`'s segment path.
async fn put_object(rig: &Rig, id: ObjectId, len: usize) {
    let path = rig.engine.object_path_of(ObjectKind::Segment, id);
    let body = Bytes::from(vec![7u8; len]);
    object_store::ObjectStoreExt::put(&*rig.store, &path.into(), body.into())
        .await
        .unwrap();
}

/// `'R'` rows naming `ids`, in one batch.
async fn name(rig: &Rig, ids: &[ObjectId]) {
    let mut batch = WriteBatch::new();
    for id in ids {
        let r = Refcount {
            live_bytes: 1,
            live_chunks: 1,
            created_ms: 0,
            object_len: 1,
        };
        batch.put(gc::refcount_key(*id), gc::encode_refcount(&r));
    }
    rig.engine.db.write(batch).await.unwrap();
}

async fn sweep(rig: &Rig) -> SweepReport {
    rig.engine.sweep_orphans().await.unwrap()
}

fn older(rig: &Rig, seq: u64) -> ObjectId {
    ObjectId {
        epoch: rig.engine.writer_epoch - 1,
        seq,
    }
}

/// A sweep lists after its cursor; once its wrap is due, a sweep whose
/// listing ended with every object decided drops the cursor, and the next
/// starts over. The report sums both prefixes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_sweep_resumes_after_its_cursor_and_starts_over_when_its_wrap_is_due() {
    let rig = gc_rig("sweep-cursor").await;
    assert!(rig.engine.writer_epoch >= 1);
    let named: Vec<ObjectId> = (1..=3).map(|s| older(&rig, s)).collect();
    for id in &named {
        put_object(&rig, *id, 10).await;
    }
    name(&rig, &named).await;
    put_object(&rig, older(&rig, 4), 37).await;
    let gc = rig.engine.gc_state();
    gc.configure(|s| s.wrap_every = Duration::from_secs(24 * 3600));
    let all = SweepReport {
        listed: 4,
        orphans: 1,
        orphan_bytes: 37,
    };
    assert_eq!(sweep(&rig).await, all, "the first lists everything");
    assert!(!stored(&rig, "hseg").await.contains_key(&older(&rig, 4)));
    let none = SweepReport::default();
    assert_eq!(sweep(&rig).await, none, "after its cursor: nothing");
    assert_eq!(sweep(&rig).await, none, "no wrap due: the cursor stays");
    gc.configure(|s| s.wrap_every = Duration::ZERO);
    assert_eq!(sweep(&rig).await, none, "the wrap drops the cursor");
    let three = SweepReport { listed: 3, ..none };
    assert_eq!(sweep(&rig).await, three, "and the next starts over");
    // A wrap every 200 ms: one sweep wraps, the next two start over and
    // then resume, the last finds the cursor kept.
    gc.configure(|s| s.wrap_every = Duration::from_millis(200));
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut listed = Vec::new();
    for _ in 0..4 {
        listed.push(sweep(&rig).await.listed);
    }
    assert_eq!(listed, [0, 3, 0, 0], "one wrap per 200 ms");
    rig.engine.begin_close();
}

/// A sweep spares an object younger than `orphan_age`, and stops at one a
/// newer owner wrote; an older owner's or its own orphan not in flight
/// goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_sweep_spares_young_objects_and_a_newer_owners() {
    let rig = gc_rig("sweep-age").await;
    let epoch = rig.engine.writer_epoch;
    let at = |epoch, seq| ObjectId { epoch, seq };
    put_object(&rig, at(epoch, 1 << 40), 5).await;
    rig.engine
        .gc_state()
        .configure(|s| s.orphan_age = Duration::from_secs(3600));
    assert_eq!(sweep(&rig).await.orphans, 0, "younger than an hour");
    rig.engine
        .gc_state()
        .configure(|s| s.orphan_age = Duration::ZERO);
    put_object(&rig, at(epoch + 1, 1), 5).await;
    let report = sweep(&rig).await;
    assert_eq!((report.listed, report.orphans), (2, 1), "{report:?}");
    let left: Vec<ObjectId> = stored(&rig, "hseg").await.into_keys().collect();
    assert_eq!(left, [at(epoch + 1, 1)], "a newer owner's object stays");
    rig.engine.begin_close();
}

/// A sweep lists at most `sweep_pages` pages of 1,000 objects.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_sweep_lists_at_most_its_pages() {
    let rig = gc_rig("sweep-pages").await;
    for seq in 0..1001 {
        put_object(&rig, older(&rig, seq), 1).await;
    }
    rig.engine.gc_state().configure(|s| s.sweep_pages = 1);
    assert_eq!(sweep(&rig).await.listed, 1000);
    assert_eq!(sweep(&rig).await.listed, 1);
    rig.engine.begin_close();
}
