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

use super::fixture_http::engine_shutdown;
use super::fixture_requests::hreq;
use super::split_pause::{Ack, loaded, stream_of_child};
use crate::dst::fault_store::{FaultPlan, FaultStore};
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
        crate::topology::merge::shown_phases(&published),
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
