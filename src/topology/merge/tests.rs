#![cfg(test)]
//! The merge's topology change (S6 §7.2 step 4): one fresh home reserved; the two halves replaced
//! by their parent prefix at it, born at the next version with both halves' homes as parents, both
//! retired at it; refused for halves that are not both leaves or a home that is not fresh.

use std::sync::Arc;

use crate::topology::model::Topology;

/// A new cell's four shards `00` `01` `10` `11`, homes 0 to 3.
fn four() -> Topology {
    crate::config::validation::InitialShards::new(4)
        .expect("four shards")
        .topology(1 << 20)
}

#[test]
fn a_merge_replaces_two_halves_by_their_parent_at_a_fresh_home() {
    let (reserved, home) = four().reserve_home();
    assert_eq!(home, 4);
    assert_eq!(reserved.shards.next_home, 5);
    assert_eq!(reserved.version, 1, "a reservation routes nothing");
    let merged = reserved.merged("0", home, "op-m").unwrap();
    assert_eq!(merged.version, 2);
    let leaves: Vec<(&str, u32)> = merged
        .shards
        .leaves
        .iter()
        .map(|(p, h)| (p.as_str(), *h))
        .collect();
    assert_eq!(leaves, [("0", 4), ("10", 2), ("11", 3)]);
    assert_eq!(merged.shards.shards, ["0", "10", "11"]);
    let born = &merged.shards.homes[&4];
    assert_eq!(
        (born.prefix.as_str(), born.path.as_str()),
        ("0", "shards/0-4")
    );
    assert_eq!((born.parents.clone(), born.born_version), (vec![0, 1], 2));
    assert_eq!(merged.shards.homes[&0].retired_version, Some(2));
    assert_eq!(merged.shards.homes[&1].retired_version, Some(2));
    assert_eq!(merged.shards.homes[&2].retired_version, None);
    let op = merged.last_op.unwrap();
    assert_eq!((op.kind.as_str(), op.children), ("merge", vec![4]));
    assert_eq!(op.sources, [("00".to_string(), 0), ("01".to_string(), 1)]);
}

#[test]
fn a_merge_of_non_leaves_or_onto_a_used_home_is_refused() {
    let (reserved, home) = four().reserve_home();
    assert!(reserved.merged("1", 0, "op").is_err(), "home 0 is in use");
    assert!(
        reserved.merged("1", 9, "op").is_err(),
        "home 9 was never reserved"
    );
    let merged = reserved.merged("0", home, "op").unwrap();
    let (again, next) = merged.reserve_home();
    assert!(
        again.merged("0", next, "op").is_err(),
        "00 and 01 are no leaves now"
    );
}

/// S6 §7.4's recovery table, row by row: published reclaims; `'T'` in either source rolls forward
/// once `M` is complete (and is inconsistent otherwise); no `'T'` rolls back, with the takeover GC
/// when a copy began.
#[test]
fn a_claimant_decides_each_row_of_the_recovery_table() {
    use crate::topology::recover::merge::{Evidence, Recovery, recovery};
    let at = |retired: [bool; 2], copy_began, merged_complete, published| Evidence {
        retired,
        copy_began,
        merged_complete,
        published,
    };
    let rows = [
        (at([false, false], false, false, false), Recovery::RollBack),
        (
            at([false, false], true, false, false),
            Recovery::RollBackWithTakeoverGc,
        ),
        (
            at([false, false], true, true, false),
            Recovery::RollBackWithTakeoverGc,
        ),
        (at([true, false], true, true, false), Recovery::RollForward),
        (at([false, true], true, true, false), Recovery::RollForward),
        (at([true, true], true, true, false), Recovery::RollForward),
        (
            at([true, false], true, false, false),
            Recovery::Inconsistent,
        ),
        (at([true, true], true, true, true), Recovery::Reclaim),
        (at([false, false], false, false, true), Recovery::Reclaim),
    ];
    for (evidence, want) in rows {
        assert_eq!(recovery(evidence), want, "{evidence:?}");
    }
}

/// Each refusal of a merge reads as the route's code and its cause.
#[test]
fn a_merge_refusal_reads_as_its_code_and_cause() {
    use super::Refused;
    let read = [
        (Refused::UnknownPrefix, "unknown_prefix"),
        (Refused::Busy("intent"), "topology_busy: intent"),
        (
            Refused::NotMergeable("a retired source".to_string()),
            "not_mergeable: a retired source",
        ),
        (
            Refused::Unfinished("'T': fenced".to_string()),
            "past 'T', unpublished: 'T': fenced; the next open of a source decides it",
        ),
    ];
    for (refused, want) in read {
        assert_eq!(refused.to_string(), want);
    }
}

/// `M`'s counts are its merged `'R'` and `'O'` rows, each decoded, and nothing else: another
/// sentinel row is skipped.
#[test]
fn the_merged_counts_are_its_refcount_and_orphan_rows() {
    use crate::spill::gc::{Refcount, encode_refcount, refcount_key};
    use crate::spill::object::ObjectId;
    use crate::spill::runindex::{Orphans, encode_orphans, orphan_key};
    let id = ObjectId {
        home: 0,
        epoch: 1,
        seq: 2,
    };
    let refcount = Refcount {
        live_bytes: 300,
        live_chunks: 2,
        created_ms: 7,
        object_len: 420,
    };
    let orphans = Orphans {
        chunks: 1,
        bytes: 40,
    };
    let mut other = crate::keyspace::SENTINEL.to_vec();
    other.push(crate::keyspace::tag::RELEASE);
    let rows = [
        (refcount_key(id), encode_refcount(&refcount)),
        (orphan_key(id), encode_orphans(orphans)),
        (other, vec![1]),
    ];
    let counts = super::counts_of(&rows).unwrap();
    assert_eq!(counts.refcounts, [(id, refcount)]);
    assert_eq!(counts.orphans, [(id, orphans)]);
}

/// A directory over the four shards whose opener opens a real engine per prefix in `store`.
fn engines(store: &Arc<dyn object_store::ObjectStore>) -> crate::shard_directory::ShardDirectory {
    let store = store.clone();
    let opener: crate::sharddir::OpenFn = Box::new(move |prefix: String, _inc| {
        let store = store.clone();
        Box::pin(async move {
            let path = crate::sharddir::shard_db_path(&prefix);
            let db = crate::keyspace::db_builder(path.as_str(), store.clone())
                .build()
                .await?;
            let maintenance = crate::shard::load_or_rebuild_maintenance(&db).await?;
            Ok(crate::shard::ShardEngine::start(
                prefix,
                Arc::new(db),
                store,
                crate::shard::ShardConfig::default(),
                None,
                maintenance,
            ))
        })
    });
    crate::shard_directory::ShardDirectory::new(
        four().shards,
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: std::time::Duration::from_secs(10),
            open_wait: std::time::Duration::from_secs(10),
        },
        |_| opener,
    )
}

async fn opened(
    dir: &crate::shard_directory::ShardDirectory,
    prefix: &str,
) -> Arc<crate::shard::ShardEngine> {
    match dir
        .open_or_wait(prefix, std::time::Duration::from_secs(10))
        .await
    {
        crate::sharddir::OpenOutcome::Ready(engine) => engine,
        _ => panic!("{prefix} did not open"),
    }
}

fn an_intent(state: crate::topology::intent::State) -> crate::topology::intent::Intent {
    crate::topology::intent::Intent {
        kind: "merge".to_string(),
        op_id: "op-m".to_string(),
        sources: vec![("00".to_string(), 0), ("01".to_string(), 1)],
        children: vec![4],
        version_seen: 1,
        state,
        lease_owner: "inst-a".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    }
}

/// **The merge's refusals.** Sibling halves `00` and `01` merge into `0` from homes 0 and 1,
/// also beside a terminal intent; `00` and `10` are no sibling halves; a live intent holds the
/// cell; a source holding `'T'` is retired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_merge_is_refused_for_no_siblings_a_live_intent_or_a_retired_source() {
    use crate::topology::intent::{self, State};
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = engines(&store);
    let (left, right, far) = (
        opened(&dir, "00").await,
        opened(&dir, "01").await,
        opened(&dir, "10").await,
    );
    let ok = super::refuse(&dir, &left, &right)
        .await
        .map_err(|e| e.to_string());
    assert_eq!(ok, Ok(("0".to_string(), [0, 1])), "sibling halves");
    let apart = super::refuse(&dir, &left, &far)
        .await
        .map_err(|e| e.to_string());
    assert_eq!(
        apart,
        Err("not_mergeable: the two are not sibling halves".to_string())
    );
    let held = intent::create(&*store, &an_intent(State::Preparing))
        .await
        .unwrap();
    let busy = super::refuse(&dir, &left, &right)
        .await
        .map_err(|e| e.to_string());
    assert_eq!(
        busy,
        Err("topology_busy: intent".to_string()),
        "a live intent"
    );
    let mut ended = held.intent.clone();
    ended.state = State::RolledBack;
    intent::replace(&*store, &held, ended).await.unwrap();
    let after = super::refuse(&dir, &left, &right)
        .await
        .map_err(|e| e.to_string());
    assert_eq!(after, Ok(("0".to_string(), [0, 1])), "a terminal intent");
    right
        .db
        .put(crate::topology::fence::retired_key(), b"{}".to_vec())
        .await
        .unwrap();
    let retired = super::refuse(&dir, &left, &right)
        .await
        .map_err(|e| e.to_string());
    assert_eq!(retired, Err("not_mergeable: a retired source".to_string()));
    drop(dir.shutdown(std::time::Duration::from_secs(5)).await);
}

/// **Preparing stands both sources' collection down and starts their touched-queue marks; the
/// undo resumes collection and drops the marks; an idle pair drains at once; each source's `'T'`
/// names the next version.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn preparing_stands_collection_down_the_undo_resumes_it_an_idle_pair_drains_and_t_names_the_next_version()
 {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = engines(&store);
    let pair = super::Pair {
        sources: [(opened(&dir, "00").await, 0), (opened(&dir, "01").await, 1)],
        prefix: "0".to_string(),
        home: 4,
    };
    super::prepare(&dir, &*store, &pair, &super::Phases::new())
        .await
        .unwrap();
    for (source, _) in &pair.sources {
        let gate = &source.gc_state().gate;
        assert!(
            gate.collection_off(),
            "{}: collection stands down",
            source.prefix
        );
        gate.mark_touched(&[7; 16]);
        assert_eq!(gate.take_touched().len(), 1, "{}: marking", source.prefix);
        gate.start_marking();
    }
    super::undo(&dir, &*store, &pair).await;
    for (source, _) in &pair.sources {
        let gate = &source.gc_state().gate;
        assert!(
            !gate.collection_off(),
            "{}: collection resumes",
            source.prefix
        );
        gate.mark_touched(&[8; 16]);
        assert!(
            gate.take_touched().is_empty(),
            "{}: the mark is dropped",
            source.prefix
        );
    }
    let drained = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        super::drain(&pair, std::time::Duration::from_secs(60)),
    );
    drained.await.expect("an idle pair drains at once");
    let phases = super::Phases::new();
    let op = super::Op {
        dir: &dir,
        store: &*store,
        pair: &pair,
        op_id: "op-m",
        birth: None,
        phases: &phases,
    };
    assert!(super::retire_sources(&op, 5).await.is_ok());
    for (source, _) in &pair.sources {
        let raw = source
            .db
            .get(crate::topology::fence::retired_key())
            .await
            .unwrap();
        let t: crate::topology::fence::Retired = serde_json::from_slice(&raw.unwrap()).unwrap();
        assert_eq!(
            (t.version, t.op_id.as_str(), t.children),
            (6, "op-m", vec![("0".to_string(), 4)])
        );
    }
    drop(dir.shutdown(std::time::Duration::from_secs(5)).await);
}

/// `source`'s committer queue once it holds `n` operations, within 5 s.
async fn queued(source: &crate::shard::ShardEngine, n: usize) -> usize {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while source.queued_ops() != n && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    source.queued_ops()
}

/// **The release's drain waits while a source holds a queued operation, and returns once none
/// does or once its time is up** (`drain`). `00`'s committer is parked by a barrier with a second
/// barrier queued behind it: one operation queued (a parked committer holds its queue for
/// `HOLD`, 2 s). Resumed after 400 ms, a drain of 10 s returns after the resume, under the hold,
/// with nothing queued. Parked again with one queued and never resumed, a drain of 300 ms returns
/// at its time, under the hold, with the operation still queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_drain_waits_for_queued_operations_and_returns_once_none_is_or_at_its_time() {
    use std::time::{Duration, Instant};
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = engines(&store);
    let pair = super::Pair {
        sources: [(opened(&dir, "00").await, 0), (opened(&dir, "01").await, 1)],
        prefix: "0".to_string(),
        home: 4,
    };
    use futures_util::future::join;
    let source = pair.sources[0].0.clone();
    let parked = source.barrier().await.unwrap();
    let resumed_drain = async {
        assert_eq!(queued(&source, 1).await, 1, "one operation queued");
        let t0 = Instant::now();
        let resume = async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            parked.resume();
        };
        let drained = async {
            super::drain(&pair, Duration::from_secs(10)).await;
            (t0.elapsed(), source.queued_ops())
        };
        join(drained, resume).await.0
    };
    let (behind, (waited, left)) = join(source.barrier(), resumed_drain).await;
    assert_eq!(left, 0, "drained: {waited:?}");
    assert!(waited >= Duration::from_millis(400), "{waited:?}");
    assert!(waited < Duration::from_secs(2), "{waited:?}");
    let parked = behind.unwrap();
    let timed_drain = async {
        assert_eq!(queued(&source, 1).await, 1, "one operation queued again");
        let t0 = Instant::now();
        super::drain(&pair, Duration::from_millis(300)).await;
        let seen = (t0.elapsed(), source.queued_ops());
        parked.resume();
        seen
    };
    let (behind, (waited, left)) = join(source.barrier(), timed_drain).await;
    assert_eq!(left, 1, "still queued at the drain's time");
    assert!(waited >= Duration::from_millis(300), "{waited:?}");
    assert!(waited < Duration::from_secs(2), "{waited:?}");
    behind.unwrap().resume();
    drop(dir.shutdown(Duration::from_secs(5)).await);
}
