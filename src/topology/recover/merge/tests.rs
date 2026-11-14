#![cfg(test)]
//! The merge claimant's pieces over real logs in a memory store (S6 §7.4): which recovery is a
//! merge's, the merge a `'T'` names, the evidence it decides from, the roll forward's act and the
//! publish.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};
use slatedb::config::{CheckpointOptions, CheckpointScope};

use super::{Claimant, Evidence, Merge, Source, act, decide, is_merge, merge_of, publish};
use crate::topology::fence::{Retired, retired_key};
use crate::topology::model::{Snapshot, Topology, born_home_path};

const OP: &str = "op-m";

type Store = Arc<dyn ObjectStore>;

fn memory() -> Store {
    Arc::new(object_store::memory::InMemory::new())
}

/// A cell's four shards `00` `01` `10` `11` (homes 0 to 3) with home 4 reserved for `M`.
fn reserved() -> Topology {
    let (reserved, home) = crate::config::validation::InitialShards::new(4)
        .unwrap()
        .topology(1 << 20)
        .reserve_home();
    assert_eq!(home, 4);
    reserved
}

fn snapshot(topology: &Topology) -> Snapshot {
    Snapshot {
        shards: topology.shards.clone(),
        etag: None,
        installed_at: None,
    }
}

/// The merge of `00` and `01` into `0` at home 4.
fn the_merge() -> Merge {
    Merge {
        op_id: OP.to_string(),
        sources: [("00".to_string(), 0), ("01".to_string(), 1)],
        prefix: "0".to_string(),
        home: 4,
    }
}

/// `op_id`'s `'T'`, naming `M`.
fn t_of(op_id: &str) -> Retired {
    Retired {
        version: 2,
        op_id: op_id.to_string(),
        children: vec![("0".to_string(), 4)],
    }
}

async fn put_topology(store: &Store, topology: &Topology) {
    let raw = serde_json::to_vec(topology).unwrap();
    store
        .put(&"topology.json".into(), raw.into())
        .await
        .unwrap();
}

/// Both sources' logs, opened for maintenance as a claimant holds them.
async fn sources(store: &Store) -> Vec<Source<'static>> {
    let mut out = Vec::new();
    for path in ["shards/00", "shards/01"] {
        let db = crate::keyspace::db_builder(path, store.clone())
            .build()
            .await
            .unwrap();
        out.push(Source::Fenced {
            db,
            path: path.to_string(),
        });
    }
    out
}

async fn put_t(source: &Source<'_>, t: &Retired) {
    let row = serde_json::to_vec(t).unwrap();
    source.db().put(retired_key(), row).await.unwrap();
    source.db().flush().await.unwrap();
}

async fn checkpoint(source: &Source<'_>, name: &str) {
    let options = CheckpointOptions {
        lifetime: None,
        source: None,
        name: Some(name.to_string()),
    };
    source.db().flush().await.unwrap();
    source
        .db()
        .create_checkpoint(CheckpointScope::All, &options)
        .await
        .unwrap();
}

/// `M`'s log: with its identity row naming home 4 when `complete`, else another row only.
async fn merged_log(store: &Store, complete: bool) {
    let db = crate::keyspace::db_builder(born_home_path("0", 4).as_str(), store.clone())
        .build()
        .await
        .unwrap();
    let (key, value) = if complete {
        crate::shard::open_index::identity_row(4)
    } else {
        (b"not-the-identity".to_vec(), b"x".to_vec())
    };
    db.put(key, value).await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();
}

async fn close(sources: Vec<Source<'_>>) {
    for source in sources {
        if let Source::Fenced { db, .. } = source {
            db.close().await.unwrap();
        }
    }
}

/// A recovery is a merge's when its live intent is one, or its `'T'` names one child (a split's
/// names two).
#[test]
fn a_recovery_is_a_merges_by_its_intent_or_a_t_naming_one_child() {
    let mut split = t_of("op-s");
    split.children.push(("1".to_string(), 5));
    assert!(!is_merge(None, None));
    assert!(is_merge(Some(&t_of(OP)), None));
    assert!(!is_merge(Some(&split), None));
}

/// The merge a `'T'` alone names: its op, `M`, and both halves with their homes from the
/// snapshot.
#[test]
fn a_t_alone_names_its_merge_and_both_halves_homes() {
    let merge = merge_of(&snapshot(&reserved()), Some(&t_of(OP)), None).unwrap();
    assert_eq!(
        (merge.op_id.as_str(), merge.prefix.as_str(), merge.home),
        (OP, "0", 4)
    );
    assert_eq!(
        merge.sources,
        [("00".to_string(), 0), ("01".to_string(), 1)]
    );
}

/// **The evidence, witness by witness.** Nothing begun; a source's checkpoint alone; `M`'s files
/// alone; `M` complete; a published merge with `M` complete (whose `M` is not looked at); and a
/// source holding another operation's `'T'` beside one holding this one's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_claimant_decides_from_each_witness() {
    let at = |retired, copy_began, merged_complete, published| Evidence {
        retired,
        copy_began,
        merged_complete,
        published,
    };
    let merge = the_merge();
    let cases: [(&str, Evidence); 6] = [
        ("nothing", at([false, false], false, false, false)),
        ("a checkpoint", at([false, false], true, false, false)),
        ("M's files", at([false, false], true, false, false)),
        ("M complete", at([false, false], true, true, false)),
        ("published", at([false, false], true, false, true)),
        ("'T's", at([false, true], false, false, false)),
    ];
    for (what, want) in cases {
        let store = memory();
        let held = sources(&store).await;
        match what {
            "a checkpoint" => {
                checkpoint(&held[0], "topo-other").await;
                checkpoint(&held[0], &format!("topo-{OP}")).await;
            }
            "M's files" => merged_log(&store, false).await,
            "M complete" => merged_log(&store, true).await,
            "published" => {
                merged_log(&store, true).await;
                let published = reserved().merged("0", 4, OP).unwrap();
                put_topology(&store, &published).await;
            }
            "'T's" => {
                put_t(&held[0], &t_of("op-other")).await;
                put_t(&held[1], &t_of(OP)).await;
            }
            _ => {}
        }
        let (evidence, row) = decide(&store, &merge, &held).await.unwrap();
        assert_eq!(evidence, want, "{what}");
        assert_eq!(row.is_some(), what == "'T's", "{what}: the row kept");
        close(held).await;
    }
}

/// **A roll forward writes the `'T'` into the source that lacks it, publishes and ends the
/// claimed intent.** `00` holds the merge's `'T'`, `01` none, `M` is complete: after the act both
/// hold it and the stored topology names the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_roll_forward_writes_the_missing_t_and_publishes() {
    let store = memory();
    let topology = reserved();
    put_topology(&store, &topology).await;
    let held = sources(&store).await;
    put_t(&held[0], &t_of(OP)).await;
    let merge = the_merge();
    let snap = snapshot(&topology);
    let claimant = Claimant {
        store: &store,
        snapshot: &snap,
        merge: &merge,
        sources: &held,
        claimed: None,
    };
    let evidence = Evidence {
        retired: [true, false],
        copy_began: true,
        merged_complete: true,
        published: false,
    };
    act(&claimant, evidence, Some(t_of(OP))).await.unwrap();
    for source in &held {
        let raw = source.db().get(retired_key()).await.unwrap();
        let t: Retired = serde_json::from_slice(&raw.expect("'T' in both sources")).unwrap();
        assert_eq!(t.op_id, OP);
    }
    let (stored, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(stored.last_op.map(|op| op.op_id), Some(OP.to_string()));
    close(held).await;
}

/// The publish is done once: a second publish over the merged topology finds it done.
#[tokio::test(crate = "crate::test_rt")]
async fn a_publish_lands_once_and_finds_itself_done_again() {
    let store = memory();
    put_topology(&store, &reserved()).await;
    publish(&*store, &the_merge()).await.unwrap();
    let (once, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(once.last_op.as_ref().map(|op| op.op_id.as_str()), Some(OP));
    publish(&*store, &the_merge()).await.expect("done already");
    let (again, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(again, once);
}
