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

/// While a topology operation holds collection off, the sweep deletes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_sweep_deletes_nothing_while_collection_is_off() {
    let rig = gc_rig("lineage-off").await;
    let all: [Entry; 1] = [(Segment, 1, 1)];
    objects(&rig, &all).await;
    put(
        &rig,
        lineage::reference_list_path(ROOT, 5),
        lineage::encode(&[]),
    )
    .await;
    rig.engine.gc_state().gate.set_collection_off(true);
    let report = rig.engine.sweep_lineage(ROOT, &[5]).await.unwrap();
    assert_eq!(report, LineageSweep::default());
    assert_eq!(left(&rig).await, all.map(path_of).into());
    rig.engine.gc_state().gate.set_collection_off(false);
    assert_eq!(
        rig.engine.sweep_lineage(ROOT, &[5]).await.unwrap().deleted,
        1
    );
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

/// Plant one sentinel row of `tag` naming home `home`'s object `seq`.
async fn row(rig: &Rig, tag: u8, home: u32, seq: u64) {
    let id = crate::spill::object::ObjectId {
        home,
        epoch: 1,
        seq,
    };
    let key = if tag == crate::spill::gc::RELEASE_TAG {
        crate::spill::gc::release_key(Segment, id)
    } else {
        crate::spill::directory::sentinel_key(tag, id)
    };
    rig.engine.db.put(&key, b"x").await.unwrap();
}

/// A shard that holds rows of another home without its reference list under
/// that home's root refuses to serve, naming the home; its own rows and the
/// homes it has lists under never refuse.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_shard_holding_an_ancestors_rows_without_its_list_refuses_to_serve() {
    use crate::spill::{directory::REFCOUNT_TAG, gc::RELEASE_TAG, runindex::POINTER_TAG};
    let rig = gc_rig("lineage-detect").await;
    let own = rig.engine.home();
    row(&rig, REFCOUNT_TAG, own, 1).await;
    assert_eq!(rig.engine.lineage_refusal().await.unwrap(), None);
    let list = |home| lineage::reference_list_path(&rig.engine.object_root(home), own);
    row(&rig, REFCOUNT_TAG, 7, 1).await;
    row(&rig, REFCOUNT_TAG, 7, 2).await;
    row(&rig, POINTER_TAG, 8, 1).await;
    row(&rig, RELEASE_TAG, 9, 1).await;
    for home in [7, 8] {
        put(&rig, list(home), lineage::encode(&[])).await;
    }
    let refusal = rig.engine.lineage_refusal().await.unwrap();
    let reason = refusal.expect("home 9 has no list");
    assert!(
        reason.contains("home 9") && reason.contains(&list(9)),
        "{reason}"
    );
    put(&rig, list(9), lineage::encode(&[])).await;
    assert_eq!(rig.engine.lineage_refusal().await.unwrap(), None);
    rig.store.delete(&Path::from(list(8))).await.unwrap();
    let reason = rig.engine.lineage_refusal().await.unwrap();
    assert!(
        reason.is_some_and(|r| r.contains("home 8")),
        "an 'I' row's home counts too"
    );
    rig.engine.begin_close();
}

/// The open-path fence fails an open whose shard holds an ancestor's rows
/// without its reference list, naming the home, and serves it once the list
/// exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_open_path_fence_fails_a_shard_whose_ancestor_list_is_missing() {
    use crate::topology::model::{ShardSet, Snapshot};
    use std::sync::Arc;
    let rig = gc_rig("lineage-fence").await;
    row(&rig, crate::spill::directory::REFCOUNT_TAG, 7, 1).await;
    let snapshot: crate::topology::fence::SnapshotFn = Arc::new(|| {
        Some(Arc::new(Snapshot {
            shards: ShardSet {
                version: 1,
                shards: Vec::new(),
                homes: std::collections::BTreeMap::new(),
                leaves: std::collections::BTreeMap::new(),
                next_home: 0,
            },
            etag: None,
            installed_at: None,
        }))
    });
    let held = crate::topology::fence::hold_until_served(&rig.engine, &snapshot).await;
    let error = held.expect_err("refused").to_string();
    assert!(
        error.contains("lineage_list_missing") && error.contains("home 7"),
        "{error}"
    );

    let rig = gc_rig("lineage-fence-ok").await;
    row(&rig, crate::spill::directory::REFCOUNT_TAG, 7, 1).await;
    let list = lineage::reference_list_path(&rig.engine.object_root(7), rig.engine.home());
    put(&rig, list, lineage::encode(&[])).await;
    crate::topology::fence::hold_until_served(&rig.engine, &snapshot)
        .await
        .expect("served");
    assert!(!rig.engine.gc_state().gate.collection_off());
    rig.engine.begin_close();
}

/// A holder's supplementary list (`refs/<h>-d`, the split's pause PUTs only the holdings its
/// Preparing list lacked) holds as its reference list does: the sweep keeps what either names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_holders_supplementary_list_holds_as_its_reference_list_does() {
    let rig = gc_rig("lineage-supplementary").await;
    let all: [Entry; 3] = [(Segment, 1, 1), (Segment, 1, 2), (Segment, 1, 3)];
    objects(&rig, &all).await;
    put(
        &rig,
        lineage::reference_list_path(ROOT, 5),
        lineage::encode(&[(Segment, 1, 1)]),
    )
    .await;
    put(
        &rig,
        lineage::supplementary_list_path(ROOT, 5),
        lineage::encode(&[(Segment, 1, 2)]),
    )
    .await;
    let report = rig.engine.sweep_lineage(ROOT, &[5]).await.unwrap();
    assert_eq!((report.held, report.deleted), (2, 1));
    let kept: BTreeSet<String> = [(Segment, 1, 1), (Segment, 1, 2)].map(path_of).into();
    assert_eq!(left(&rig).await, kept);
    rig.engine.begin_close();
}
