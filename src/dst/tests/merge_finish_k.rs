#![cfg(test)]
//! The merge's pause and its tail (S6-DESIGN rev 6 §7.2, §7.4): while it holds its two sources
//! parked, both answer as a split's parent does (edge #249 B3); once it attempted its `'T'` it only
//! rolls forward. A leaf that fails to open after the publish leaves the merge published, and a
//! publish that fails leaves the intent live for a claimant. Its rig is `merge_k`'s.

use super::billing_closure_debts::JSON;
use super::fixture_http::engine_shutdown;
use super::fixture_requests::hreq;
use super::merge_k::{Rig, State, halves_of_00, rig, split_00};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Pause the merge of `halves` at its park (the left half's gate), run `control` there, and let it
/// go: the merge's result.
pub(super) async fn merge_paused_at_park<F: std::future::Future<Output = ()>>(
    state: &State,
    halves: &[Arc<crate::shard::ShardEngine>; 2],
    op_id: &str,
    control: impl FnOnce() -> F,
) -> Result<crate::topology::merge::Published, crate::topology::merge::Refused> {
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    halves[0].gc_state().gate.pause_at_park(point);
    let clock = state.runtime.clock.clone();
    let merge = crate::topology::merge::merge(
        &state.shards,
        [&halves[0], &halves[1]],
        op_id,
        "inst-a",
        &*clock,
    );
    let controlled = async {
        reached_rx.await.expect("the merge paused at its park");
        control().await;
        assert!(go_tx.send(()).is_ok(), "the merge waits for go");
    };
    let (merged, ()) = tokio::join!(merge, controlled);
    merged
}

/// What a source's log holds of its WAL: its path, and each WAL object under it by id with the
/// sequence numbers of its records, in order. A quiesced source's mark does not move. The
/// writer's `next_wal_sst_id` is not in it: it catches up when an L0 flush publishes the manifest
/// (it went 1 -> 7 at a split's park with WAL objects 1-6 identical, suite-tip-142538), which is
/// no WAL write.
pub(super) async fn wal_mark(
    store: &Arc<dyn object_store::ObjectStore>,
    source: &crate::shard::ShardEngine,
) -> WalMark {
    let path = source.log_path();
    let reader = slatedb::WalReader::new(path.as_str(), store.clone());
    let mut objects = Vec::new();
    for file in reader.list(..).await.expect("the WAL lists") {
        let mut records = file.iterator().await.expect("the WAL object reads");
        let mut seqs = Vec::new();
        while let Some(record) = records.next().await.expect("a WAL record") {
            seqs.push(record.seq);
        }
        objects.push((file.id, seqs));
    }
    (path, objects)
}

/// A row the test owns in the sentinel's space. Tag `'D'` is reserved for #150's queue debt index
/// (`keyspace::tests::SENTINEL_TAGS`): nothing in the product writes or reads it yet.
pub(super) fn probe_row() -> Vec<u8> {
    let mut key = crate::keyspace::SENTINEL.to_vec();
    key.push(b'D');
    key.extend_from_slice(b"park-probe");
    key
}

/// An index batch of [`probe_row`], awaited once the operation ends.
pub(super) type Batch =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>;

type Answer = (u16, Option<String>, Option<String>);

/// A log's path and its WAL objects by id, each with its records' sequence numbers.
pub(super) type WalMark = (String, Vec<(u64, Vec<u64>)>);

/// What a quiesced operation's sources showed at its park: their WAL marks before and after a hold,
/// a requested seal's and a fold's answers on the first source, the answers to an append to a
/// stream of each, and whether the index batch was still waiting at the end of the hold.
#[derive(Debug)]
pub(super) struct AtPark {
    before: Vec<WalMark>,
    after: Vec<WalMark>,
    sealed: Result<usize, String>,
    folded: Result<(usize, bool), String>,
    appends: (Answer, Answer),
    batch_pending: bool,
}

/// At the park of an operation over `sources`: an index batch of [`probe_row`] on the first
/// through `write_index_batch` (the funnel every writer outside the committer takes, the gate held
/// shared), a requested seal and a fold of it, and an append to each of `names`, through a hold of
/// 3 s (past `barrier::HOLD`, 2 s); every source's WAL mark before and after. The batch comes back
/// unfinished for the caller to await once the operation ends.
pub(super) async fn probe_at_park(
    store: &Arc<dyn object_store::ObjectStore>,
    sources: &[Arc<crate::shard::ShardEngine>],
    addr: std::net::SocketAddr,
    names: [&str; 2],
) -> (AtPark, Batch) {
    let mut before = Vec::new();
    for source in sources {
        before.push(wal_mark(store, source).await);
    }
    let source = sources[0].clone();
    let mut batch: Batch = Box::pin(async move {
        let mut batch = slatedb::WriteBatch::new();
        batch.put(probe_row(), b"1");
        source
            .write_index_batch(batch)
            .await
            .map_err(|error| format!("{error:?}"))
    });
    let held = tokio::time::timeout(Duration::from_secs(3), &mut batch);
    let probes = async {
        let first = &sources[0];
        let sealed = first.seal_requested().await.map(|report| report.streams);
        let folded = first
            .fold_once()
            .await
            .map(|r| (r.rows, r.object.is_none()));
        let appends = futures_util::future::join(
            append_answer(addr, names[0], 30),
            append_answer(addr, names[1], 31),
        )
        .await;
        let sealed = sealed.map_err(|error| format!("{error:?}"));
        (
            sealed,
            folded.map_err(|error| format!("{error:?}")),
            appends,
        )
    };
    let (ended, (sealed, folded, appends)) = futures_util::future::join(held, probes).await;
    let mut after = Vec::new();
    for source in sources {
        after.push(wal_mark(store, source).await);
    }
    let at = AtPark {
        before,
        after,
        sealed,
        folded,
        appends,
        batch_pending: ended.is_err(),
    };
    (at, batch)
}

/// The park was quiet: every source's WAL objects and record seqs exactly unchanged through the
/// hold; the index batch still waiting on the gate; the seal refused by the parked committer; the
/// fold with nothing to fold (the operation's `prepare` folded each source first); both appends
/// held `503 shard_splitting`.
pub(super) fn assert_quiet(at: &AtPark) {
    for (path, objects) in &at.before {
        assert!(!objects.is_empty(), "{path}: the probe sees its WAL");
    }
    assert_eq!(at.after, at.before, "a parked source wrote to its WAL");
    assert!(
        at.batch_pending,
        "the index batch waits for the quiesce: {at:?}"
    );
    let refused = r#"Store("the seal's group was not applied")"#.to_string();
    assert_eq!(at.sealed, Err(refused), "a seal at the park");
    assert_eq!(at.folded, Ok((0, true)), "a fold at the park finds nothing");
    let splitting = (
        503,
        Some("1".to_string()),
        Some("shard_splitting".to_string()),
    );
    assert_eq!(
        at.appends,
        (splitting.clone(), splitting),
        "both appends held"
    );
}

type Probed = (
    Result<crate::topology::merge::Published, crate::topology::merge::Refused>,
    AtPark,
    Batch,
);

/// `topology.json` made unreadable, so a publish after `'T'` fails (`Unfinished`).
pub(super) async fn unreadable_topology(store: &Arc<dyn object_store::ObjectStore>) {
    let path = object_store::path::Path::from("topology.json");
    object_store::ObjectStoreExt::put(&**store, &path, b"{".to_vec().into())
        .await
        .unwrap();
}

/// The refusal a final source gives a queued index batch (`write_index_batch_held`).
pub(super) const FINAL: &str = r#"Batch("the source is final: a topology operation wrote 'T'")"#;

/// The queued batch, once the operation ended: refused as final, and the sources' WAL marks
/// exactly what they were when the operation returned (nothing lands after `'T'`).
pub(super) async fn refused_as_final(
    store: &Arc<dyn object_store::ObjectStore>,
    sources: &[Arc<crate::shard::ShardEngine>],
    batch: Batch,
) {
    let mut ended = Vec::new();
    for source in sources {
        ended.push(wal_mark(store, source).await);
    }
    let refused = tokio::time::timeout(Duration::from_secs(20), batch).await;
    assert_eq!(refused, Ok(Err(FINAL.to_string())), "the batch after 'T'");
    for (source, mark) in sources.iter().zip(&ended) {
        assert_eq!(&wal_mark(store, source).await, mark, "no write after 'T'");
    }
}

/// The merge of `00`'s halves paused at its park while [`probe_at_park`] runs there (and, when
/// `corrupt`, `topology.json` is then made unreadable): its result, the park's account and the
/// unfinished index batch.
async fn merge_probed(
    rig: &Rig,
    halves: &[Arc<crate::shard::ShardEngine>; 2],
    corrupt: bool,
) -> Probed {
    let seen = Arc::new(std::sync::Mutex::new(None));
    let (marks, sources, store) = (seen.clone(), halves.clone(), rig.state.data_store.clone());
    let (addr, names) = (rig.addr, [rig.low.clone(), rig.high.clone()]);
    let paused = move || async move {
        let probed = probe_at_park(&store, &sources, addr, [&names[0], &names[1]]).await;
        if corrupt {
            unreadable_topology(&store).await;
        }
        *marks.lock().unwrap() = Some(probed);
    };
    let merged = merge_paused_at_park(&rig.state, halves, "op-q", paused).await;
    let (at, batch) = seen.lock().unwrap().take().expect("the park was reached");
    (merged, at, batch)
}

/// `00` split, then a record appended to a stream of each half: the merge's sources, each with a
/// WAL of its own.
async fn halves_with_records(rig: &Rig) -> [Arc<crate::shard::ShardEngine>; 2] {
    split_00(&rig.state, &rig.parent).await;
    let halves = halves_of_00(&rig.state).await;
    for (name, n) in [(&rig.low, 20), (&rig.high, 21)] {
        assert_eq!(append_answer(rig.addr, name, n).await.0, 204, "{name}");
    }
    halves
}

/// **A quiesced source writes nothing until the merge publishes**
/// (DESIGN-DECISION-c3-incomplete-rig-2026-11-11, Addendum 4, step 2 as amended). At the merge's
/// park the probe runs (`probe_at_park`, `assert_quiet`). Released, the merge publishes; the index
/// batch is then refused as final (no write after `'T'`), and the merged leaf holds no probe row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_quiesced_source_writes_nothing_until_the_merge_publishes() {
    let rig = super::merge_k::cold_rig().await;
    let halves = halves_with_records(&rig).await;
    let (merged, at, batch) = merge_probed(&rig, &halves, false).await;
    assert_quiet(&at);
    merged.expect("the merge publishes");
    refused_as_final(&rig.state.data_store, &halves, batch).await;
    let leaf = super::merge_k::opened(&rig.state, "00").await;
    let row = leaf.db.get(probe_row()).await.unwrap();
    assert_eq!(row, None, "no probe row in the merged leaf");
    engine_shutdown(&rig.state).await;
}

/// **A quiesced source writes nothing until the merge rolls back** (the rollback variant of
/// `a_quiesced_source_writes_nothing_until_the_merge_publishes`). A manifest that does not decode
/// sits at `M`'s path, so `M`'s clone fails inside the pause. At the park the probe runs. Released,
/// the merge refuses `RolledBack` naming that path; the index batch then lands on the source,
/// which holds the probe row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_quiesced_source_writes_nothing_until_the_merge_rolls_back() {
    let rig = super::merge_k::cold_rig().await;
    let halves = halves_with_records(&rig).await;
    let home = rig.state.shards.prefixes().shards.next_home;
    let path = crate::topology::model::born_home_path("00", home);
    let junk = format!("{path}/manifest/00000000000000000001.manifest");
    let junk = object_store::path::Path::from(junk.as_str());
    object_store::ObjectStoreExt::put(&*rig.store, &junk, b"junk".to_vec().into())
        .await
        .unwrap();
    let (merged, at, batch) = merge_probed(&rig, &halves, false).await;
    assert_quiet(&at);
    let refused = merged.err().expect("the merge rolls back");
    assert!(
        matches!(&refused, crate::topology::merge::Refused::RolledBack(why) if why.contains(&path)),
        "{refused:?}"
    );
    let ended = tokio::time::timeout(Duration::from_secs(20), batch).await;
    assert_eq!(ended, Ok(Ok(())), "the batch lands once the merge ends");
    let row = halves[0].db.get(probe_row()).await.unwrap();
    assert_eq!(
        row,
        Some(bytes::Bytes::from_static(b"1")),
        "the source holds the probe row"
    );
    engine_shutdown(&rig.state).await;
}

/// **A quiesced source writes nothing after the merge's `'T'` when its publish fails** (the
/// `Unfinished` variant). At the park the probe runs, then `topology.json` is made unreadable, so
/// the publish fails after both `'T'`s: the merge refuses `Unfinished`, and the queued index batch
/// is refused as final, both sources' WALs unchanged since.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_quiesced_source_writes_nothing_after_the_merges_t_when_its_publish_fails() {
    let rig = super::merge_k::cold_rig().await;
    let halves = halves_with_records(&rig).await;
    let (merged, at, batch) = merge_probed(&rig, &halves, true).await;
    assert_quiet(&at);
    let refused = merged.err().expect("the publish fails");
    assert!(
        matches!(refused, crate::topology::merge::Refused::Unfinished(_)),
        "{refused:?}"
    );
    refused_as_final(&rig.state.data_store, &halves, batch).await;
    engine_shutdown(&rig.state).await;
}

/// **A merge whose leaf fails to open after the publish stays published** (S6 §7.2, as the split
/// does; edge #251's view). Junk WAL objects are planted under `M`'s path at the park, so `M`'s
/// clone passes and its open fails: the merge answers `Published` without `M`'s engine (it opens on
/// its next request), the intent ends `Reclaimed`, both sources are retired, and the stored
/// topology has `00` as a leaf again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_whose_leaf_fails_to_open_after_the_publish_stays_published() {
    let Rig {
        store,
        state,
        parent,
        ..
    } = rig().await;
    let version = split_00(&state, &parent).await;
    let halves = halves_of_00(&state).await;
    let home = state.shards.prefixes().shards.next_home;
    let merged_path = crate::topology::model::born_home_path("00", home);
    let planted = || async {
        for id in 1..=64u64 {
            let wal = format!("{merged_path}/wal/{id:020}.sst");
            let junk = b"not a wal table".to_vec();
            object_store::ObjectStoreExt::put(&*store, &wal.as_str().into(), junk.into())
                .await
                .unwrap();
        }
    };
    let merged = merge_paused_at_park(&state, &halves, "op-o", planted)
        .await
        .expect("published, M unopened");
    assert_eq!(merged.version, version + 1);
    assert_eq!((merged.merged.0.as_str(), merged.merged.1), ("00", home));
    assert!(merged.merged.2.is_none(), "M did not open");
    let ended = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(ended.state, crate::topology::intent::State::Reclaimed);
    assert!(
        halves.iter().all(|h| h.is_closed()),
        "both sources are retired"
    );
    let (topology, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(topology.version, version + 1);
    assert_eq!(topology.shards.leaves.get("00"), Some(&home));
    engine_shutdown(&state).await;
}

/// **A merge whose publish fails after its `'T'` is left live, never rolled back** (S6 §7.4; edge
/// #251's view). `topology.json` is made unreadable at the park, so the publish fails after both
/// `'T'`s: the merge refuses `Unfinished`, its intent stays live (`Preparing`, for the next opener
/// of a source to claim and roll forward), and nothing is undone: `M`'s reference list under the
/// parent's root is still there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_whose_publish_fails_after_t_is_left_live() {
    let Rig {
        store,
        state,
        parent,
        ..
    } = rig().await;
    let parent_root = state.shards.prefixes().shards.homes
        [&state.shards.prefixes().shards.leaves["00"]]
        .path
        .clone();
    split_00(&state, &parent).await;
    let halves = halves_of_00(&state).await;
    let home = state.shards.prefixes().shards.next_home;
    let corrupt = || async {
        let junk = b"{".to_vec();
        object_store::ObjectStoreExt::put(&*store, &"topology.json".into(), junk.into())
            .await
            .unwrap();
    };
    let refused = merge_paused_at_park(&state, &halves, "op-u", corrupt)
        .await
        .err()
        .expect("the publish fails");
    assert!(
        matches!(&refused, crate::topology::merge::Refused::Unfinished(_)),
        "{refused}"
    );
    let left = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(
        left.state,
        crate::topology::intent::State::Preparing,
        "live, for a claimant"
    );
    let list = crate::spill::lineage::reference_list_path(&parent_root, home);
    let kept = object_store::ObjectStoreExt::get(&*store, &list.as_str().into()).await;
    assert!(
        kept.is_ok(),
        "M's list under the parent's root is kept: {list}"
    );
    engine_shutdown(&state).await;
}

/// An append of `{"n": n}` to `name` through `addr`: its status, `Retry-After` and error code.
pub(super) async fn append_answer(
    addr: std::net::SocketAddr,
    name: &str,
    n: u32,
) -> (u16, Option<String>, Option<String>) {
    let body = format!(r#"[{{"n":{n}}}]"#);
    let path = format!("/v1/stream/{name}");
    let (status, headers, body) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let code = error["error"]["code"].as_str().map(str::to_string);
    (status, headers.get("retry-after").cloned(), code)
}

/// The records of `name` and the stream's next offset.
async fn records_and_next(
    addr: std::net::SocketAddr,
    name: &str,
) -> (Vec<serde_json::Value>, Option<String>) {
    let (status, headers, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 200, "{name}: {}", String::from_utf8_lossy(&body));
    let read: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let records = read.as_array().cloned().unwrap_or_default();
    (records, headers.get("stream-next-offset").cloned())
}

/// An append of `{"n": n}` to `name`: its status, `Stream-Next-Offset` and error code.
async fn append_at(
    addr: std::net::SocketAddr,
    name: &str,
    n: u32,
) -> (u16, Option<String>, Option<String>) {
    let body = format!(r#"[{{"n":{n}}}]"#);
    let path = format!("/v1/stream/{name}");
    let (status, headers, body) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let code = error["error"]["code"].as_str().map(str::to_string);
    (status, headers.get("stream-next-offset").cloned(), code)
}

/// One record appended to `records`.
fn with(records: &[serde_json::Value], n: u32) -> Vec<serde_json::Value> {
    let mut grown = records.to_vec();
    grown.push(serde_json::json!({ "n": n }));
    grown
}

/// A rig past a split of `00`, its two halves opened here, and each stream's records and next
/// offset.
struct Pair {
    rig: Rig,
    halves: [Arc<crate::shard::ShardEngine>; 2],
    before: [(Vec<serde_json::Value>, Option<String>); 2],
}

async fn pair(instance: Option<&str>) -> Pair {
    let rig = super::merge_k::rig_named(instance).await;
    split_00(&rig.state, &rig.parent).await;
    let halves = halves_of_00(&rig.state).await;
    let before = [
        records_and_next(rig.addr, &rig.low).await,
        records_and_next(rig.addr, &rig.high).await,
    ];
    Pair {
        rig,
        halves,
        before,
    }
}

/// **A merge pauses both its sources as a split pauses its parent** (S6 §7.2, edge #249 B3;
/// Fable's points 1, 2 and 4). At the merge's park both sources' committers are parked: an
/// append to a stream of each source, sent together, is answered no sooner than 2 s after, exactly
/// `503 shard_splitting` with `Retry-After: 1` and no `outcome_unknown`, and a later one to each at
/// once; a read of each stream is 200 meanwhile. Released, the merge publishes at the next
/// version, `00` its one leaf for the range, its intent `Reclaimed` (`published`); neither stream
/// holds a refused record, and a new append to each is acked by the merged leaf at the stream's
/// exact next offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_pauses_both_sources_with_503_shard_splitting_and_serves_after_its_publish() {
    let Pair {
        rig,
        halves,
        before,
    } = pair(None).await;
    let (addr, names) = (rig.addr, [rig.low.clone(), rig.high.clone()]);
    let splitting = (
        503,
        Some("1".to_string()),
        Some("shard_splitting".to_string()),
    );
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (answers, held_names) = (seen.clone(), names.clone());
    let paused = move || async move {
        let sent_at = Instant::now();
        let held = futures_util::future::join(
            append_answer(addr, &held_names[0], 10),
            append_answer(addr, &held_names[1], 11),
        )
        .await;
        let waited = sent_at.elapsed();
        for (name, held, n) in [(&held_names[0], held.0, 20), (&held_names[1], held.1, 21)] {
            let sent_at = Instant::now();
            let later = append_answer(addr, name, n).await;
            let at_once = sent_at.elapsed();
            let (read, _, _) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
            answers
                .lock()
                .unwrap()
                .push((name.clone(), held, waited, later, at_once, read));
        }
    };
    let merged = merge_paused_at_park(&rig.state, &halves, "op-p", paused).await;
    let published = merged.expect("the merge publishes");
    let answers = seen.lock().unwrap().clone();
    assert_eq!(answers.len(), 2, "both sources answered");
    for (name, held, waited, later, at_once, read) in &answers {
        assert_eq!(held, &splitting, "{name}: held past the hold");
        assert!(
            waited >= &Duration::from_secs(2),
            "{name} held for the hold: {waited:?}"
        );
        assert_eq!(later, &splitting, "{name}: a later append");
        assert!(
            at_once < &Duration::from_secs(1),
            "{name} answered at once: {at_once:?}"
        );
        assert_eq!(read, &200, "{name} is read during the pause");
    }
    let stored = crate::topology::store::read(&*rig.store).await.unwrap().0;
    assert_eq!(stored.version, published.version);
    assert_eq!(stored.shards.leaves.get("00"), Some(&published.merged.1));
    let ended = crate::topology::intent::read(&*rig.store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(ended.state, crate::topology::intent::State::Reclaimed);
    assert_eq!(
        ended.outcome.map(|o| o.result).as_deref(),
        Some("published")
    );
    for (i, name) in names.iter().enumerate() {
        let (records, next) = records_and_next(addr, name).await;
        assert_eq!(records, before[i].0, "{name}: nothing refused was written");
        assert_eq!(next, before[i].1, "{name}: the tail did not move");
        let n = 30 + u32::try_from(i).unwrap();
        let (status, acked_at, _) = append_at(addr, name, n).await;
        assert_eq!(status, 204, "{name} after the publish");
        let (records, next) = records_and_next(addr, name).await;
        assert_eq!(
            records,
            with(&before[i].0, n),
            "{name} holds the new record once"
        );
        assert_eq!(acked_at, next, "{name}: acked at its exact next offset");
    }
    engine_shutdown(&rig.state).await;
}

/// **A merge released within its hold acks the held appends from the merged leaf** (S6 §7.2,
/// edge #249 B3; Fable's point 3). An append to a stream of each source waits at the park; the
/// merge is let go at once, so it publishes before either hold ends: each held append is acked
/// (204) by `M` at its stream's exact next offset, and each stream reads back its records with the
/// held one, once, last.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_released_within_its_hold_acks_the_held_appends_from_the_merged_leaf() {
    let Pair {
        rig,
        halves,
        before,
    } = pair(None).await;
    let (addr, names) = (rig.addr, [rig.low.clone(), rig.high.clone()]);
    let (merge, parked, go) = paused_merge(&rig.state, &halves, "op-w");
    let appends = async {
        parked.notified().await;
        futures_util::future::join(
            append_at(addr, &names[0], 40),
            append_at(addr, &names[1], 41),
        )
        .await
    };
    // Let the merge go as soon as both appends wait in the parked committers, well within the hold.
    let release = async {
        parked.notified().await;
        let t0 = Instant::now();
        while halves.iter().any(|half| half.queued_ops() == 0) {
            assert!(
                t0.elapsed() < Duration::from_secs(1),
                "both appends wait at the park"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        go();
    };
    let (merged, (low, high), ()) = futures_util::future::join3(merge, appends, release).await;
    merged.expect("the merge publishes");
    for (i, (name, (status, acked_at, code))) in names.iter().zip([low, high]).enumerate() {
        assert_eq!(
            (status, code),
            (204, None),
            "{name}: the held append is acked by M"
        );
        let (records, next) = records_and_next(addr, name).await;
        let n = 40 + u32::try_from(i).unwrap();
        assert_eq!(
            records,
            with(&before[i].0, n),
            "{name}: the held record, once, last"
        );
        assert_eq!(acked_at, next, "{name}: acked at its exact next offset");
    }
    engine_shutdown(&rig.state).await;
}

/// The merge of `halves` as operation `op_id`, paused at its park; a notice each waiter gets once
/// the park is reached; and the release.
fn paused_merge<'a>(
    state: &'a State,
    halves: &'a [Arc<crate::shard::ShardEngine>; 2],
    op_id: &'a str,
) -> (
    impl std::future::Future<
        Output = Result<crate::topology::merge::Published, crate::topology::merge::Refused>,
    > + 'a,
    Arc<Parked>,
    impl FnOnce() + 'a,
) {
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    halves[0].gc_state().gate.pause_at_park(point);
    let parked = Arc::new(Parked::default());
    let notice = parked.clone();
    let clock = state.runtime.clock.clone();
    let merge = async move {
        let merge = crate::topology::merge::merge(
            &state.shards,
            [&halves[0], &halves[1]],
            op_id,
            "inst-a",
            &*clock,
        );
        let watch = async {
            reached_rx.await.expect("the merge paused at its park");
            notice.reach();
        };
        futures_util::future::join(merge, watch).await.0
    };
    let release = move || assert!(go_tx.send(()).is_ok(), "the merge waits for go");
    (merge, parked, release)
}

/// The park, once reached: every waiter, early or late, is let through.
#[derive(Default)]
struct Parked {
    reached: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

impl Parked {
    fn reach(&self) {
        self.reached
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    async fn notified(&self) {
        loop {
            let waiting = self.notify.notified();
            if self.reached.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            waiting.await;
        }
    }
}

/// **A held append whose merged leaf is another instance's is answered by that owner** (S6 §7.2,
/// edge #249 B3 and #250; Fable's point 5). The halves are this instance's (`inst-a`); the ring
/// gives the merged prefix `00` to `inst-b`. An append waits at the park; the merge publishes, `M`
/// does not open here, and the held append is re-resolved under the new topology: exactly `409
/// not_ring_owner` with `Streams-Replay-To: inst-b`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_held_append_whose_merged_leaf_is_another_instances_is_answered_by_its_owner() {
    let Pair { rig, halves, .. } = pair(Some("inst-a")).await;
    let (merge, parked, go) = paused_merge(&rig.state, &halves, "op-o");
    let held = async {
        parked.notified().await;
        replayed(rig.addr, &rig.low, 50).await
    };
    let release = async {
        parked.notified().await;
        let members = ["inst-a", "inst-b"].map(str::to_string).to_vec();
        let mut overrides = std::collections::HashMap::new();
        for half in ["000", "001"] {
            overrides.insert(half.to_string(), "inst-a".to_string());
        }
        overrides.insert("00".to_string(), "inst-b".to_string());
        rig.state.ownership.set_view(members, overrides);
        let t0 = Instant::now();
        while halves[0].queued_ops() == 0 {
            assert!(
                t0.elapsed() < Duration::from_secs(1),
                "the append waits at the park"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        go();
    };
    let (merged, held, ()) = futures_util::future::join3(merge, held, release).await;
    let merged = merged.expect("the merge publishes");
    assert!(merged.merged.2.is_none(), "M is inst-b's, not opened here");
    let moved = (
        409,
        Some("inst-b".to_string()),
        Some("not_ring_owner".to_string()),
    );
    assert_eq!(held, moved, "the held append");
    engine_shutdown(&rig.state).await;
}

/// An append of `{"n": n}` to `name`: its status, `Streams-Replay-To` and error code.
async fn replayed(
    addr: std::net::SocketAddr,
    name: &str,
    n: u32,
) -> (u16, Option<String>, Option<String>) {
    let body = format!(r#"[{{"n":{n}}}]"#);
    let path = format!("/v1/stream/{name}");
    let (status, headers, body) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let code = error["error"]["code"].as_str().map(str::to_string);
    (status, headers.get("streams-replay-to").cloned(), code)
}

/// **A claimant's completeness check leaves a merged leaf that serves serving** (S6 §7.4). A
/// merge publishes and its `M` serves an append here. A lapsed source's opener that rolls a
/// merge forward from `'T'` alone (no live intent, its instance still on the earlier topology)
/// checks `M`'s `'Y'` row. That check opened `M` with a writer, whose epoch bump fenced the `M`
/// that serves: its next append failed `Closed(Fenced)` and the stream answered 500
/// `shard_open_failed`. The check reads `M` without a writer, and the next append is acked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_claimants_completeness_check_leaves_a_serving_merged_leaf_serving() {
    let Rig {
        store,
        state,
        addr,
        low,
        parent,
        ..
    } = rig().await;
    split_00(&state, &parent).await;
    let halves = halves_of_00(&state).await;
    let clock = state.runtime.clock.clone();
    let published = crate::topology::merge::merge(
        &state.shards,
        [&halves[0], &halves[1]],
        "op-m",
        "inst-a",
        &*clock,
    )
    .await;
    let Ok(crate::topology::merge::Published {
        merged: (prefix, home, Some(_)),
        ..
    }) = published
    else {
        panic!("the merge publishes and M opens here");
    };
    assert_eq!(super::merge_k::append_to(addr, &low, 7).await.0, 204);
    let path = crate::topology::model::born_home_path(&prefix, home);
    assert!(
        crate::topology::recover::merge::merged_is_complete(&store, &path, home).await,
        "M is complete"
    );
    let after = super::merge_k::append_to(addr, &low, 8).await;
    assert_eq!(
        after,
        (204, String::new()),
        "M still serves after the check"
    );
    engine_shutdown(&state).await;
}

/// **A source's opener behind a published merge fences neither source** (S6 §7.4; edge #251
/// amendment B6). A merge publishes `00` and its intent ends. An opener of the left half whose
/// snapshot still predates the publish rolls the merge forward from `'T'` alone (no live intent),
/// at every recheck until its snapshot moves, while the right half's log is open again by a live
/// writer (the half's own fenced open, reopening). That roll forward opened the right half's log
/// for maintenance, whose epoch bump fenced the reopened writer: its next write failed
/// `Closed(Fenced)` and the open answered 500 `shard_open_failed`. The stored topology already
/// names the merge, so the roll forward opens nothing and the writer still writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_source_opener_behind_a_published_merge_fences_neither_source() {
    let Rig {
        store,
        state,
        parent,
        ..
    } = rig().await;
    split_00(&state, &parent).await;
    let halves = halves_of_00(&state).await;
    let behind = state.shards.prefixes();
    let clock = state.runtime.clock.clone();
    let published = crate::topology::merge::merge(
        &state.shards,
        [&halves[0], &halves[1]],
        "op-b",
        "inst-a",
        &*clock,
    )
    .await
    .expect("the merge publishes");
    let right = behind.shards.homes[&behind.shards.leaves["001"]]
        .path
        .clone();
    let reopened = crate::keyspace::db_builder(right.as_str(), store.clone())
        .build()
        .await
        .expect("the right half's log reopens");
    let retired = crate::topology::fence::Retired {
        version: published.version,
        op_id: "op-b".to_string(),
        children: vec![(published.merged.0.clone(), published.merged.1)],
    };
    let left = behind.shards.homes[&behind.shards.leaves["000"]]
        .path
        .clone();
    let opener = source_opener("000", &left, &store).await;
    let rolled =
        crate::topology::recover::merge::recover(&opener, &behind, Some(retired), None).await;
    assert_eq!(rolled, Ok(()), "the published merge needs nothing");
    let wrote = async {
        reopened.put(b"probe", b"1").await?;
        reopened.flush().await
    }
    .await
    .map_err(|e| e.to_string());
    assert_eq!(wrote, Ok(()), "the reopened right half still writes");
    drop(reopened.close().await);
    assert_eq!(
        opener.await_terminated(Duration::from_secs(10)).await,
        Ok(())
    );
    engine_shutdown(&state).await;
}

/// A fresh engine over the source `prefix`'s log at `path`, as that source's opener holds it.
async fn source_opener(
    prefix: &str,
    path: &str,
    store: &Arc<dyn object_store::ObjectStore>,
) -> Arc<crate::shard::ShardEngine> {
    let db = crate::keyspace::db_builder(path, store.clone())
        .build()
        .await
        .expect("the source's log opens");
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("its maintenance loads");
    crate::shard::ShardEngine::start(
        prefix.to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        None,
        maintenance,
    )
}
