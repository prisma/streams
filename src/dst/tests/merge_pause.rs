//! A merge's pause, measured (GATE-DECISION-s22-join-2026-11-20 Addenda 9 §1 and 10):
//! `split_pause`'s twin for the merge. A loaded parent on a store is split into `0` and `1`, left
//! quiet, and merged back while a paced writer appends to a stream of each half; each run prints
//! the merge's pause (its park to its release), each phase of its own line
//! (`topology::merge::shown_phases`, "the merge published, by phase"), the time from the merge's
//! start to its publish, and each writer's answers and longest gap between acks.
//!
//! Measured on 2026-11-21 (release, idle, under the machine's lock; ci-proof/s22-coloc 379af787f):
//!
//! | shape | pause | start to publish | clones | open | checkpoints | writers' longest gap |
//! |---|---|---|---|---|---|---|
//! | 2,500 streams, 20 ms away (3 runs) | 787-800 ms | 1,652-1,664 ms | 503-512 ms | 688-698 ms | 167-191 ms | 1.55 s, every write 204 |
//! | 16 streams, in memory (3 runs) | 2-3 ms | 5-11 ms | 0 | 1-3 ms | 0 | 208 ms, every write 204 |
//!
//! A merge of one instance's own pair is not seconds: C3's 4.0, 7.2 and 1.4 s came from what its
//! cell adds (three instances, halves just handed off), not from the merge's own work. The merged
//! leaf's open is not warmed, and at 2,500 streams the writers' longest gap is 0.45 s under `HOLD`
//! (NEXT-WORK §30). The 2,500-stream runs are an ignored measurement; the small pair's merge runs
//! in the suite and asserts only what a merge promises: every write acked (`204`), no
//! `outcome_unknown`, every acked record stored once, in order.
//!
//! A merge of halves whose streams were sealed, cold (Addenda 14 and 20): the store GETs of its
//! `prepare` and of its `lists`, and the `lists` phase's time. Measured on 2026-11-22 (release,
//! idle, under the machine's lock; merge-lists on ci-proof/s22-coloc 11d9aa4a0):
//!
//! | sealed streams | store | prepare GETs | lists GETs | lists | pause |
//! |---|---|---|---|---|---|
//! | 1,000 | in memory | 9 | 1 | - | - |
//! | 8,000 | in memory | 37-39 | 9 | - | - |
//! | 20,000 | 20 ms away | 77 | 11 | 131 ms | 818 ms |
//! | 40,000 | 20 ms away | 165 | 18 | 196 ms | 895 ms |
//!
//! `lists` is on the pause and stays within 64 GETs and under 250 ms at 40,000; `prepare` passes
//! 64 at 20,000 and 40,000, off the pause, holding no writer (NEXT-WORK §31). The two smaller
//! sizes run in the suite as a bound; the two larger are an ignored measurement.

use super::fixture_http::{HttpRig, engine_shutdown};
use super::fixture_requests::hreq;
use super::split_pause::{Ack, COMPACTION_SOURCES, flush, loaded, stream_of_child};
use crate::dst::StoreOp;
use crate::dst::fault_store::{FaultPlan, FaultStore};
use crate::dst::trace_store::{TraceEventKind, TraceStore};
use crate::topology::merge::{Published, shown_phases};
use futures_util::StreamExt;
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// A pair to merge: the loaded parent's streams, the store's latency each way, each writer's pace.
#[derive(Clone, Copy)]
struct Shape {
    name: &'static str,
    streams: usize,
    latency_ms: u64,
    pace: Duration,
    /// Every write must be acked (`204`): the suite's small case. The measurement counts 503s.
    all_acked: bool,
}

/// `split_pause`'s loaded parent at a quarter of its 10,000, on a store 20 ms away, a writer on
/// each half at 20 appends a second.
const LOADED: Shape = Shape {
    name: "2,500 streams, 20 ms away",
    streams: 2_500,
    latency_ms: 20,
    pace: Duration::from_millis(50),
    all_acked: false,
};

/// C3's pair (`topology_colocation_load`): a few streams a half on a store in memory with no
/// latency, a writer on each half at 5 appends a second.
const C3_SIZED: Shape = Shape {
    name: "16 streams, in memory",
    streams: 16,
    latency_ms: 0,
    pace: Duration::from_millis(200),
    all_acked: true,
};

/// How long the pair stays quiet after the split before the merge starts.
const QUIET: Duration = Duration::from_secs(5);

/// Appends to `stream` at `addr` every `pace`, one record at a time, until `done`: each answer
/// and its error code.
async fn paced(
    addr: std::net::SocketAddr,
    stream: &str,
    pace: Duration,
    done: &AtomicBool,
) -> Vec<(Ack, String)> {
    let (path, json) = (
        format!("/v1/stream/{stream}"),
        [("content-type", "application/json")],
    );
    let mut answers = Vec::new();
    let mut next = tokio::time::Instant::now();
    for k in 1u32.. {
        if done.load(Ordering::Relaxed) {
            break;
        }
        let body = format!(r#"[{{"n":{k}}}]"#);
        let (status, headers, answer) = hreq(addr, "POST", &path, &json, body.as_bytes()).await;
        let code = serde_json::from_slice::<serde_json::Value>(&answer)
            .ok()
            .and_then(|v| v["error"]["code"].as_str().map(str::to_owned))
            .unwrap_or_default();
        let offset = headers.get("stream-next-offset").cloned();
        answers.push((
            Ack {
                at: Instant::now(),
                status,
                offset,
            },
            code,
        ));
        next += pace;
        tokio::time::sleep_until(next.max(tokio::time::Instant::now())).await;
    }
    answers
}

/// One writer's tally: its answers by (status, code), its longest gap between acks, and whether
/// the stream holds exactly its acked records (its first, the load's, and one per ack, in order).
async fn tallied(
    addr: std::net::SocketAddr,
    stream: &str,
    answers: &[(Ack, String)],
    all_acked: bool,
) -> String {
    let mut by: std::collections::BTreeMap<(u16, &str), usize> = std::collections::BTreeMap::new();
    for (ack, code) in answers {
        *by.entry((ack.status, code.as_str())).or_insert(0) += 1;
    }
    assert!(
        !all_acked || answers.iter().all(|(ack, _)| ack.status == 204),
        "{stream}: every write through the merge acked: {by:?}"
    );
    assert!(
        answers.iter().all(|(_, code)| code != "outcome_unknown"),
        "{stream}: a merge answers no outcome_unknown: {by:?}"
    );
    let acked: Vec<u32> = (1u32..)
        .zip(answers)
        .filter(|(_, (ack, _))| ack.status == 204)
        .map(|(k, _)| k)
        .collect();
    let (records, _) = super::fixture_split::records_and_next(addr, stream).await;
    let read: Vec<u64> = records
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|record| record["n"].as_u64())
        .collect();
    let want: Vec<u64> = std::iter::once(0)
        .chain(acked.iter().map(|k| u64::from(*k)))
        .collect();
    assert_eq!(
        read, want,
        "{stream}: stored equals acked, each once, in order"
    );
    let at: Vec<Instant> = answers
        .iter()
        .filter(|(a, _)| a.status == 204)
        .map(|(a, _)| a.at)
        .collect();
    let gap = at
        .windows(2)
        .map(|w| w[1].duration_since(w[0]))
        .max()
        .unwrap_or_default();
    format!(
        "{stream}: {} sent, {by:?}, longest gap between acks {gap:?}",
        answers.len()
    )
}

/// One run: the loaded parent split into `0` and `1`, quiet for `QUIET`, then merged back under
/// the two paced writers. Its line: the pause, the phases, start to publish, each writer's tally.
async fn run(shape: Shape, round: u32) -> String {
    let plan = FaultPlan {
        latency_pct: if shape.latency_ms > 0 { 100 } else { 0 },
        latency_ms: (shape.latency_ms, shape.latency_ms),
        ..FaultPlan::CLEAN
    };
    let fault = FaultStore::uniform(
        super::fixture_storage::mem(),
        0x5490 + u64::from(round),
        plan,
    );
    let rig = loaded(fault, shape.streams).await;
    let clock = rig.state.runtime.clock.clone();
    let parent = rig.state.shards.open("").expect("the leaf is open");
    crate::topology::split::split(&rig.state.shards, &parent, "op-split", "inst-a", &*clock)
        .await
        .map_err(|refused| refused.to_string())
        .expect("the split publishes");
    let streams = [
        stream_of_child(&rig.state, 0).await,
        stream_of_child(&rig.state, 1).await,
    ];
    tokio::time::sleep(QUIET).await;
    let halves = ["0", "1"].map(|p| rig.state.shards.open(p).expect("the half is open"));
    let done = AtomicBool::new(false);
    let merged = async {
        let began = Instant::now();
        let started = crate::topology::merge::start(
            &rig.state.shards,
            [&halves[0], &halves[1]],
            "op-merge",
            "inst-a",
        )
        .await
        .map_err(|refused| refused.to_string())
        .expect("the merge starts");
        let published = started.finish(&*clock).await;
        let took = began.elapsed();
        tokio::time::sleep(Duration::from_secs(1)).await;
        done.store(true, Ordering::Relaxed);
        (published, took)
    };
    let written = futures_util::future::join(
        paced(rig.addr, &streams[0], shape.pace, &done),
        paced(rig.addr, &streams[1], shape.pace, &done),
    );
    let ((published, took), (left, right)) = futures_util::future::join(merged, written).await;
    let published = published
        .map_err(|refused| refused.to_string())
        .expect("the merge publishes");
    let lines = [
        tallied(rig.addr, &streams[0], &left, shape.all_acked).await,
        tallied(rig.addr, &streams[1], &right, shape.all_acked).await,
    ];
    engine_shutdown(&rig.state).await;
    format!(
        "MERGE_PAUSE {} run {round}: pause {} ms, start to publish {} ms, phases [{}]\nMERGE_PAUSE   {}\nMERGE_PAUSE   {}",
        shape.name,
        published.pause.as_millis(),
        took.as_millis(),
        shown_phases(&published),
        lines[0],
        lines[1]
    )
}

/// **A merge's pause and its phases, three runs** (the measurement of Addenda 9 §1 and 10 of the
/// s22 join ruling, run under the machine's lock; ignored in the suite): a quiet pair of 2,500
/// streams on a store 20 ms away, a paced writer on each half. Each run prints its pause, start
/// to publish, the merge's phases and each writer's answers; it asserts no `outcome_unknown` and
/// stored equal to acked, and counts 503s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a timing measurement, run under the machine's lock"]
async fn a_merges_pause_and_its_phases_three_runs() {
    for round in 1..=3 {
        println!("{}", run(LOADED, round).await);
    }
}

/// **Paced writers through a quiet pair's merge are every one acked and stored** (Addendum 10 of
/// the s22 join ruling): C3's size, a few streams a half in memory, one paced writer on each half
/// through one merge. Every write is answered `204`, none `outcome_unknown`, and each stream holds
/// exactly its acked records, once, in order. It asserts no time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn paced_writers_through_a_quiet_pairs_merge_are_every_one_acked_and_stored() {
    println!("{}", run(C3_SIZED, 1).await);
}

/// The most store GETs the merge's `lists` may make, and its `prepare` with them, whatever its
/// halves have sealed (Addendum 14 of the s22 join ruling; F5's bound for the split).
const LISTS_BOUND: usize = 64;

/// A JSON append's header.
const JSON: [(&str, &str); 1] = [("content-type", "application/json")];

/// Seals `engine` until a seal finds nothing left, then flushes its memtable to an L0 table.
async fn sealed_and_flushed(engine: &Arc<crate::shard::ShardEngine>) {
    for _ in 0..64 {
        let report = engine
            .seal_once(crate::shard::seal::Trigger::Requested)
            .await
            .map_err(|e| format!("{e:?}"))
            .expect("a requested seal");
        if report.object.is_none() {
            break;
        }
    }
    flush(engine).await;
}

/// C3's quiet pair split from its loaded parent, whose halves then hold `streams` streams more,
/// each created and appended once (`{"n":1}`) and sealed, in two rounds, each flushed to an L0
/// table; their directory rows folded; then both halves closed, reopened (an append to a stream of
/// each), settled under the compaction line and their blocks evicted: cold. Each stream's index
/// rows, put and deleted by its seal, are tombstones in its half's sentinel tables.
async fn sealed_pair(trace: Arc<TraceStore>, streams: usize) -> (HttpRig, Vec<String>) {
    let rig = loaded(trace, C3_SIZED.streams).await;
    let clock = rig.state.runtime.clock.clone();
    let parent = rig.state.shards.open("").expect("the leaf is open");
    crate::topology::split::split(&rig.state.shards, &parent, "op-split", "inst-a", &*clock)
        .await
        .map_err(|refused| refused.to_string())
        .expect("the split publishes");
    drop(parent);
    let names: Vec<String> = (0..streams).map(|n| format!("sealed-{n}")).collect();
    let addr = rig.addr;
    for round in names.chunks(streams.div_ceil(2)) {
        futures_util::stream::iter(round)
            .for_each_concurrent(32, |name| async move {
                let path = format!("/v1/stream/{name}");
                let (status, _, _) = hreq(addr, "PUT", &path, &JSON, b"").await;
                assert_eq!(status, 201, "create {path}");
                let (status, _, _) = hreq(addr, "POST", &path, &JSON, br#"[{"n":1}]"#).await;
                assert_eq!(status, 204, "append to {path}");
            })
            .await;
        for half in ["0", "1"] {
            sealed_and_flushed(&rig.state.shards.open(half).expect("the half is open")).await;
        }
    }
    let reopen = [
        stream_of_child(&rig.state, 0).await,
        stream_of_child(&rig.state, 1).await,
    ];
    for half in ["0", "1"] {
        let engine = rig.state.shards.open(half).expect("the half is open");
        for _ in 0..64 {
            if !engine
                .fold_once()
                .await
                .map_err(|e| format!("{e:?}"))
                .expect("a fold")
                .more
            {
                break;
            }
        }
    }
    engine_shutdown(&rig.state).await;
    for (half, stream) in ["0", "1"].into_iter().zip(&reopen) {
        let path = format!("/v1/stream/{stream}");
        let (status, _, _) = hreq(addr, "POST", &path, &JSON, br#"[{"n":2}]"#).await;
        assert_eq!(status, 204, "the half {half} reopens");
        cold(
            &rig.state.shards.open(half).expect("the half is open"),
            addr,
            stream,
        )
        .await;
    }
    (rig, names)
}

/// Waits, at most 30 s, until `leaf`'s log has room for two more L0 tables under the compaction
/// line (the merge's flushes), so no compaction starts in the merge's window. A log that sits under
/// the line without that room is pushed over it (one more table: `body` appended to `stream` at
/// `addr` and flushed) and left to compact. Then every cached block of its tables is evicted.
async fn cold(leaf: &Arc<crate::shard::ShardEngine>, addr: std::net::SocketAddr, stream: &str) {
    use slatedb::DbCacheManagerOps;
    let started = Instant::now();
    loop {
        let l0 = leaf.db.manifest().l0().len();
        if l0 + 2 < COMPACTION_SOURCES {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the half's L0 compacts: {l0}"
        );
        if l0 < COMPACTION_SOURCES {
            let path = format!("/v1/stream/{stream}");
            let (status, _, _) = hreq(addr, "POST", &path, &JSON, br#"[{"n":3}]"#).await;
            assert_eq!(
                status, 204,
                "an append to push {stream}'s half to compaction"
            );
            flush(leaf).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for view in leaf.db.manifest().all_sst_views() {
        let all = slatedb::CacheTarget::all();
        leaf.db
            .evict_cached_sst(&view.sst, &all)
            .await
            .expect("evict");
    }
}

/// The merge of `rig`'s pair, traced: the store GETs of its `prepare` (from its start to its first
/// reference list's PUT), those of its `lists` (from the pause's last checkpoint to its reference
/// lists' PUT), and the merge as published, with its phases.
async fn merged_gets(rig: &HttpRig, trace: &TraceStore) -> (usize, usize, Published) {
    let from = trace.events().last().map_or(0, |event| event.seq);
    let halves = ["0", "1"].map(|p| rig.state.shards.open(p).expect("the half is open"));
    let clock = rig.state.runtime.clock.clone();
    let started = crate::topology::merge::start(
        &rig.state.shards,
        [&halves[0], &halves[1]],
        "op-merge",
        "inst-a",
    )
    .await
    .map_err(|refused| refused.to_string())
    .expect("the merge starts");
    let published = started
        .finish(&*clock)
        .await
        .map_err(|refused| refused.to_string())
        .expect("the merge publishes");
    let events = trace.events();
    let window: Vec<_> = events
        .iter()
        .filter(|event| event.seq > from && event.kind == TraceEventKind::Operation)
        .collect();
    let put_to = |at: usize, part: &str| {
        window[at..]
            .iter()
            .position(|event| event.op == StoreOp::Put && event.path.contains(part))
            .map(|i| at + i)
    };
    let lists = put_to(0, "/lineage/refs/").expect("prepare's lists");
    let checkpoint = put_to(lists, "manifest").expect("a checkpoint");
    let pause_lists = put_to(checkpoint, "/lineage/refs/").expect("the pause's lists");
    let last_checkpoint = (checkpoint..pause_lists)
        .rev()
        .find(|i| window[*i].op == StoreOp::Put && window[*i].path.contains("manifest"))
        .unwrap_or(checkpoint);
    let gets = |part: &[&crate::dst::trace_store::StoreTraceEvent]| {
        part.iter().filter(|event| event.op == StoreOp::Get).count()
    };
    (
        gets(&window[..lists]),
        gets(&window[last_checkpoint..pause_lists]),
        published,
    )
}

/// One sealed pair of `streams` merged on a store `latency_ms` away (in memory at 0): its
/// `prepare`'s and `lists`' GETs and the merge as published, once the merge's promises are checked:
/// one leaf, and every sealed stream holding its one record.
async fn sealed_merge(streams: usize, latency_ms: u64) -> (usize, usize, Published) {
    let mem = super::fixture_storage::mem();
    let store: Arc<dyn ObjectStore> = if latency_ms == 0 {
        mem
    } else {
        let plan = FaultPlan {
            latency_pct: 100,
            latency_ms: (latency_ms, latency_ms),
            ..FaultPlan::CLEAN
        };
        FaultStore::uniform(mem, 0x5A20, plan)
    };
    let trace = TraceStore::verbatim(store);
    let (rig, names) = sealed_pair(trace.clone(), streams).await;
    let (prepare, lists, published) = merged_gets(&rig, &trace).await;
    let leaves: Vec<String> = rig
        .state
        .shards
        .prefixes()
        .shards
        .leaves
        .keys()
        .cloned()
        .collect();
    assert_eq!(leaves, [""], "one merge: the pair is one leaf");
    let addr = rig.addr;
    futures_util::stream::iter(&names)
        .for_each_concurrent(32, |name| async move {
            let (records, _) = super::fixture_split::records_and_next(addr, name).await;
            assert_eq!(
                records,
                serde_json::json!([{"n": 1}]),
                "{name}: stored equals acked"
            );
        })
        .await;
    engine_shutdown(&rig.state).await;
    (prepare, lists, published)
}

/// The time of a published merge's `lists` phase, in ms.
fn lists_ms(published: &Published) -> u128 {
    published
        .phases
        .iter()
        .find(|(name, _)| name == "lists")
        .map(|(_, took)| took.as_millis())
        .expect("the merge timed its lists")
}

/// **A merge's `prepare` and `lists` make at most 64 store GETs at 1,000 and 8,000 sealed
/// streams** (Addenda 14 and 20 of the s22 join ruling): halves whose 1,000 and then 8,000 streams
/// were sealed across L0 tables, their index rows tombstones in the sentinel, cold. The merge's
/// `prepare` and its `lists` each make at most 64 store GETs at both sizes, and its promises hold
/// (one leaf, every stream its one record). The count still rises with the halves' tables (9 and 1
/// at 1,000, 37-39 and 9 at 8,000). Red before the fix: the sentinel scans fetched one block a GET,
/// uncached, 164 and 140 at 8,000. A count, not a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merges_prepare_and_lists_make_at_most_64_gets_at_1000_and_8000_sealed_streams() {
    let (one_prepare, one_lists, one) = sealed_merge(1_000, 0).await;
    let (eight_prepare, eight_lists, eight) = sealed_merge(8_000, 0).await;
    let (one_phases, eight_phases) = (shown_phases(&one), shown_phases(&eight));
    println!(
        "MERGE_LISTS_GETS 1000: prepare {one_prepare} lists {one_lists} [{one_phases}]; 8000: \
         prepare {eight_prepare} lists {eight_lists} [{eight_phases}]"
    );
    assert!(
        [one_prepare, one_lists, eight_prepare, eight_lists]
            .iter()
            .all(|gets| *gets <= LISTS_BOUND),
        "GETs of the merge's prepare and lists: {one_prepare} and {one_lists} at 1,000 sealed \
         streams ({one_phases}), {eight_prepare} and {eight_lists} at 8,000 ({eight_phases})"
    );
}

/// **A merge's `prepare` and `lists` at 20,000 and 40,000 sealed streams, 20 ms away** (the
/// measurement of Addendum 20 of the s22 join ruling, run under the machine's lock; ignored in the
/// suite): the bound's pair at the 40,000 step's sizes on a store 20 ms away. Each size prints its
/// `prepare`'s and `lists`' GETs, the `lists` phase's time and the merge's phases; it asserts only
/// the merge's promises.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a measurement at the 40,000 step's sizes, run under the machine's lock"]
async fn a_merges_prepare_and_lists_at_20000_and_40000_sealed_streams_20_ms_away() {
    for streams in [20_000, 40_000] {
        let began = Instant::now();
        let (prepare, lists, published) = sealed_merge(streams, 20).await;
        println!(
            "MERGE_LISTS_SCALE {streams}: prepare {prepare} GETs, lists {lists} GETs in {} ms; \
             pause {} ms; phases [{}]; run {} s",
            lists_ms(&published),
            published.pause.as_millis(),
            shown_phases(&published),
            began.elapsed().as_secs()
        );
    }
}
