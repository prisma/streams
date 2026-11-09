#![cfg(test)]
//! A split child's reads of a `'K'` row it inherited (K-SPLIT-DECISION-2026-11-09): the row is
//! copied byte for byte and names its sibling's chunks too, so the open and the fold each keep
//! only the shard's range, at the offsets the whole row gives.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;

use super::Trigger;
use super::tests::{Rig, append, directory_rows, manual, read_back, rig, settle};
use crate::keyspace::range::Range;
use crate::shard::Deliver;

/// A stream in range `"0"` and one outside it.
const INSIDE: [u8; 16] = [0x20; 16];
const OUTSIDE: [u8; 16] = [0xA0; 16];

/// Both streams sealed into one object (one `'K'` row of two entries), then the shard's range
/// narrowed to `"0"`, as a split child's is.
async fn inherited(label: &str) -> (Rig, Bytes) {
    let rig = rig(label, manual(), Duration::from_millis(2)).await;
    let record = Bytes::from_static(b"{\"in\":true}");
    append(&rig.engine, INSIDE, vec![record.clone()]).await;
    append(
        &rig.engine,
        OUTSIDE,
        vec![Bytes::from_static(b"{\"in\":false}")],
    )
    .await;
    settle(&rig.engine).await;
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    settle(&rig.engine).await;
    let rows = directory_rows(&rig).await;
    assert_eq!(
        rows.values().map(Vec::len).sum::<usize>(),
        2,
        "one row, two entries"
    );
    *rig.engine.gc_state().range_override.lock().unwrap() = Some(Range::of("0"));
    (rig, record)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_open_admits_only_its_range_from_an_inherited_directory_row() {
    let (rig, _) = inherited("range-open").await;
    let dir = rig.engine.rebuild_directory().await.unwrap();
    assert_eq!((dir.entries(), dir.streams()), (1, 1));
    assert!(
        dir.from(&OUTSIDE, 0, 8).is_empty(),
        "the sibling's chunk is not admitted"
    );
    let kept = dir.from(&INSIDE, 0, 8);
    let e = &kept[0];
    let path = crate::spill::object_path(&rig.engine.object_root(e.object.home), e.object);
    let range = e.chunk_off..e.chunk_off + u64::from(e.chunk_len);
    let raw = object_store::ObjectStoreExt::get_range(&*rig.store, &path.into(), range)
        .await
        .unwrap();
    let chunk = crate::spill::object::decode_chunk(&raw).unwrap();
    assert_eq!(
        chunk.hash, INSIDE,
        "the kept entry's offset names its own chunk"
    );
    rig.engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fold_of_an_inherited_row_keeps_its_range_and_deletes_the_row() {
    let (rig, record) = inherited("range-fold").await;
    let report = rig.engine.fold_once().await.unwrap();
    assert_eq!(report.entries, 1, "only the range's entry is folded");
    assert!(
        directory_rows(&rig).await.is_empty(),
        "the 'K' row is deleted"
    );
    let oracle = BTreeMap::from([(0, record)]);
    read_back(&rig.engine, INSIDE, &oracle, Deliver::Durable)
        .await
        .unwrap();
    rig.engine.begin_close();
}
