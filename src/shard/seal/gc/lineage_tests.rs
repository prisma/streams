#![cfg(test)]
//! The lineage sweep of a retired home (S6-DESIGN §4 steps 3-4): it deletes
//! exactly the objects under the home's root that no holder still holds, and
//! deletes nothing when any of its evidence is unreadable or missing.

use std::collections::BTreeSet;

use bytes::Bytes;
use object_store::ObjectStoreExt;
use object_store::path::Path;

use super::super::gc_tests::gc_rig;
use super::super::tests::Rig;
use super::lineage::LineageSweep;
use crate::spill::gc::ObjectKind::{RunIndex, Segment};
use crate::spill::lineage::{self, Entry};

const ROOT: &str = "shards/lineage-test-0";

async fn put(rig: &Rig, path: String, body: Vec<u8>) {
    rig.store
        .put(&Path::from(path), Bytes::from(body).into())
        .await
        .unwrap();
}

/// Store the retired home's objects `entries` under `ROOT`.
async fn objects(rig: &Rig, entries: &[Entry]) {
    for (kind, epoch, seq) in entries {
        let path = format!("{ROOT}/{}/{epoch:016x}/{seq:016x}", kind.dir());
        put(rig, path, vec![1; 8]).await;
    }
}

/// The objects left under `ROOT`'s two object prefixes.
async fn left(rig: &Rig) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for dir in ["hseg", "ridx"] {
        let prefix = Path::from(format!("{ROOT}/{dir}/"));
        let mut listing = rig.store.list(Some(&prefix));
        while let Some(meta) = futures_util::StreamExt::next(&mut listing).await {
            out.insert(meta.unwrap().location.to_string());
        }
    }
    out
}

fn path_of((kind, epoch, seq): Entry) -> String {
    format!("{ROOT}/{}/{epoch:016x}/{seq:016x}", kind.dir())
}

/// Holder 5 lists three objects and released one; holder 6 lists two and is
/// released-all. The sweep keeps exactly holder 5's two unreleased objects
/// and deletes the rest, including one no list ever named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_sweep_deletes_exactly_what_no_holder_still_holds() {
    let rig = gc_rig("lineage-sweep").await;
    let all: [Entry; 5] = [
        (Segment, 1, 1),
        (Segment, 1, 2),
        (Segment, 1, 3),
        (Segment, 2, 1),
        (RunIndex, 1, 9),
    ];
    objects(&rig, &all).await;
    let list5 = [(Segment, 1, 1), (Segment, 1, 2), (RunIndex, 1, 9)];
    put(
        &rig,
        lineage::reference_list_path(ROOT, 5),
        lineage::encode(&list5),
    )
    .await;
    let log = lineage::release_log_path(ROOT, 5, 3, 0);
    put(&rig, log, lineage::encode(&[(Segment, 1, 1)])).await;
    let list6 = [(Segment, 1, 2), (Segment, 1, 3)];
    put(
        &rig,
        lineage::reference_list_path(ROOT, 6),
        lineage::encode(&list6),
    )
    .await;
    put(
        &rig,
        lineage::released_all_path(ROOT, 6),
        lineage::encode(&[]),
    )
    .await;

    let report = rig.engine.sweep_lineage(ROOT, &[5, 6]).await.unwrap();
    assert_eq!(
        report,
        LineageSweep {
            listed: 5,
            held: 2,
            deleted: 3
        }
    );
    let kept: BTreeSet<String> = [(Segment, 1, 2), (RunIndex, 1, 9)].map(path_of).into();
    assert_eq!(left(&rig).await, kept);
    rig.engine.begin_close();
}

/// A log that does not decode, or a holder without a reference list, stops
/// the sweep before any DELETE: it never decides on evidence it cannot read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_sweep_with_unreadable_or_missing_evidence_deletes_nothing() {
    let rig = gc_rig("lineage-refuse").await;
    let all: [Entry; 2] = [(Segment, 1, 1), (Segment, 1, 2)];
    objects(&rig, &all).await;
    let list = [(Segment, 1, 1)];
    put(
        &rig,
        lineage::reference_list_path(ROOT, 5),
        lineage::encode(&list),
    )
    .await;
    let mut torn = lineage::encode(&[(Segment, 1, 1)]);
    torn.pop();
    put(&rig, lineage::release_log_path(ROOT, 5, 3, 0), torn).await;
    let every: BTreeSet<String> = all.map(path_of).into();

    assert!(rig.engine.sweep_lineage(ROOT, &[5]).await.is_err());
    assert_eq!(left(&rig).await, every, "a torn log decides nothing");
    put(
        &rig,
        lineage::release_log_path(ROOT, 5, 3, 0),
        lineage::encode(&[]),
    )
    .await;
    assert!(rig.engine.sweep_lineage(ROOT, &[5, 6]).await.is_err());
    assert_eq!(left(&rig).await, every, "holder 6 has no list");
    rig.engine.begin_close();
}
