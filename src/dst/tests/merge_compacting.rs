#![cfg(test)]
//! A merge of a compacting source (GATE-DECISION-s22-join-2026-11-20 Addenda 11, 12 and 17; F5
//! part 1b's H1, GATE-DECISION-f5-checkpoint-compaction-2026-11-17, for the merge). The pause's
//! checkpoint forces each source's memtable to L0, and that upload waits while the source's log holds
//! `l0_max_ssts` L0 tables, until a compaction frees one (C3's solo merges: 1.2 to 5.2 s inside one
//! source's `checkpoint`). So the merge's Preparing settles both sources first (`topology::settle`,
//! the split's rule), and the merge does not park while a source is unsettled.
//!
//! The rig is `merge_k`'s over a fault store: `00` split into `000` and `001`, then `000`'s log at
//! `l0_max_ssts` with its compaction held (the compactor's write of its `compactions` file, as
//! `split_compacting` holds it).

use super::billing_closure_debts::JSON;
use super::fixture_http::{engine_shutdown, http_rig_two_bit};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::merge_k::{Rig, halves_of_00, rig_over, split_00};
use super::split_compacting::{Held, fast_worker, started, topped_up, while_running};
use super::split_pause::{COMPACTION_SOURCES, until};
use crate::dst::fault_store::{FaultPlan, FaultStore};
use crate::dst::{ObjClass, StoreOp};
use crate::topology::gate::PausePoint;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// SlateDB's `l0_max_ssts`, its default (the rig does not set it): a memtable upload waits while
/// the log holds this many L0 tables, until a compaction frees one.
const L0_MAX_SSTS: usize = 8;

/// The source's L0 line: its marks are then low 8 and high 12, so an L0 of `L0_MAX_SSTS` is not
/// settled (8 is not below 8) and the shed stays off (8 is below 12), as `split_compacting`'s
/// parent without room.
const UNSETTLED_LINE: usize = 16;

/// How long a test waits for a merge step, or for appends sent while the compaction is held: past
/// Preparing's settle bound (12 s under test) with room for a loaded host.
const HELD_WAIT: Duration = Duration::from_secs(30);

/// How long the first test holds the compaction once the merge's Preparing runs, appending to both
/// halves meanwhile.
const HELD_FOR: Duration = Duration::from_secs(1);

/// How long the bound test waits for its rollback: the settle bound under test (12 s) and 8 s for
/// the rollback; it moves with that bound.
const REFUSED_WITHIN: Duration = Duration::from_secs(20);

/// One append's answer: its status and, for a refusal, its code.
type Answer = (u16, Option<String>);

/// Appends `n` records to each of `rig`'s two streams (one in each half), numbered from `from`:
/// every answer.
async fn appended(rig: &Rig, from: u32, n: u32) -> Vec<Answer> {
    let mut answers = Vec::new();
    for k in from..from + n {
        for stream in [&rig.low, &rig.high] {
            let path = format!("/v1/stream/{stream}");
            let body = super::fixture_split::body(k);
            let (status, _, raw) = hreq(rig.addr, "POST", &path, &[JSON], body.as_bytes()).await;
            answers.push((status, super::fixture_cell::error_code(&raw)));
        }
    }
    answers
}

/// Whether every answer in `answers` is a `204`.
fn all_acked(answers: &[Answer]) -> bool {
    answers.iter().all(|(status, _)| *status == 204)
}

/// `merge_k`'s rig over a fault store, `00` split, and its half `000` at `L0_MAX_SSTS` with its
/// compaction held: the rig, the hold, the fault store and both halves.
async fn compacting_pair() -> (Rig, Held, [Arc<crate::shard::ShardEngine>; 2]) {
    let fault = FaultStore::uniform(super::fixture_storage::mem(), 0x5414, FaultPlan::CLEAN);
    let store: Arc<dyn object_store::ObjectStore> = fault.clone();
    let rigged = http_rig_two_bit(store.clone(), None, RigRuntime::first()).await;
    let rig = rig_over(store, rigged).await;
    split_00(&rig.state, &rig.parent).await;
    let halves = halves_of_00(&rig.state).await;
    let source = &halves[0];
    source.set_l0_line(UNSETTLED_LINE);
    let compactions = format!("{}/compactions", source.log_path());
    let engaged = fault.hold_class_under(StoreOp::Put, ObjClass::Other, &compactions, 1);
    let held = Held(fault);
    let mut k = 100;
    topped_up((rig.addr, &rig.low), source, COMPACTION_SOURCES, &mut k).await;
    until(
        || engaged.load(Ordering::Relaxed) >= 1,
        || {
            let l0 = source.db.manifest().l0().len();
            format!("the compactor's compactions write is held (l0 {l0})")
        },
    )
    .await;
    topped_up((rig.addr, &rig.low), source, L0_MAX_SSTS, &mut k).await;
    println!(
        "MERGE_COMPACTING_SETUP held, l0={}",
        source.db.manifest().l0().len()
    );
    (rig, held, halves)
}

/// The merge of `halves`, as `merge_k`'s tests start it.
async fn merged(
    rig: &Rig,
    halves: &[Arc<crate::shard::ShardEngine>; 2],
    op_id: &str,
) -> Result<crate::topology::merge::Published, crate::topology::merge::Refused> {
    let clock = rig.state.runtime.clock.clone();
    crate::topology::merge::merge(
        &rig.state.shards,
        [&halves[0], &halves[1]],
        op_id,
        "inst-a",
        &*clock,
    )
    .await
}

/// **A merge of a compacting source parks only after the compaction.** While the source's
/// compaction is held the merge's Preparing waits for the source to settle: the barrier does not
/// park and both halves' writers are acked. Released (a compaction worker of the test's own lands
/// it inside the bound), the merge parks and publishes; its checkpoint's time is printed, not
/// asserted. Red before the settle: the barrier parks at once with the compaction held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_of_a_compacting_source_parks_only_after_the_compaction() {
    let (rig, held, halves) = tokio::time::timeout(6 * HELD_WAIT, compacting_pair())
        .await
        .expect("the compacting pair is ready within 180 s");
    let (reached, park_rx) = tokio::sync::oneshot::channel();
    let (park_go, go) = tokio::sync::oneshot::channel();
    halves[0]
        .gc_state()
        .gate
        .pause_at_park(PausePoint { reached, go });
    let merge = merged(&rig, &halves, "op-held");
    let control = async {
        let mut park_rx = park_rx;
        started(&halves[0]).await;
        let answers = tokio::time::timeout(HELD_WAIT, appended(&rig, 200, 5)).await;
        tokio::time::sleep(HELD_FOR).await;
        assert!(
            park_rx.try_recv().is_err(),
            "the barrier parked with the compaction held"
        );
        let answers = answers.expect("appends answered while the compaction is held");
        assert!(
            all_acked(&answers),
            "both halves' appends acked while the compaction is held: {answers:?}"
        );
        let worker = fast_worker(&halves[0], held.0.clone()).await;
        let released = std::time::Instant::now();
        held.release();
        let parked = while_running(&worker, tokio::time::timeout(HELD_WAIT, park_rx)).await;
        let parked = parked.map(|reached| reached.is_ok());
        assert_eq!(
            parked,
            Ok(true),
            "the merge parks within 30 s of the release"
        );
        println!("MERGE_COMPACTING release to park {:?}", released.elapsed());
        assert!(park_go.send(()).is_ok(), "the merge waits for go");
    };
    let both = futures_util::future::join(merge, control);
    let (published, ()) = tokio::time::timeout(3 * HELD_WAIT, both)
        .await
        .expect("the merge publishes within 90 s of the start");
    let published = published
        .map_err(|refused| refused.to_string())
        .expect("the merge publishes");
    println!(
        "MERGE_COMPACTING pause_ms={} [{}]",
        published.pause.as_millis(),
        crate::topology::merge::shown_phases(&published)
    );
    tokio::time::timeout(HELD_WAIT, engine_shutdown(&rig.state))
        .await
        .expect("the cell shuts down within 30 s");
}

/// **A merge of a source compacting past the settle bound rolls back before the park.** The same
/// pair, the source's compaction held past Preparing's bound: the merge rolls back without parking
/// either source, every append sent meanwhile to both halves is acked, and the merge's intent ends
/// rolled back. Red before the settle: the barrier parks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_of_a_source_compacting_past_the_bound_rolls_back_before_the_park() {
    let (rig, held, halves) = tokio::time::timeout(6 * HELD_WAIT, compacting_pair())
        .await
        .expect("the compacting pair is ready within 180 s");
    let (reached, mut park_rx) = tokio::sync::oneshot::channel();
    let (park_go, go) = tokio::sync::oneshot::channel();
    halves[0]
        .gc_state()
        .gate
        .pause_at_park(PausePoint { reached, go });
    let merge = merged(&rig, &halves, "op-bound");
    let appending = appended(&rig, 300, 10);
    let both = futures_util::future::join(merge, appending);
    let ended = tokio::time::timeout(REFUSED_WITHIN, both).await;
    let parked = park_rx.try_recv().is_ok();
    drop(park_go);
    held.release();
    assert!(!parked, "the barrier parked with the compaction held");
    let (refused, answers) =
        ended.unwrap_or_else(|_| panic!("the merge ends within {REFUSED_WITHIN:?}"));
    match refused {
        Err(crate::topology::merge::Refused::RolledBack(why)) => {
            println!("ROLLED_BACK {why}");
            assert!(why.contains("did not settle"), "{why}");
        }
        other => panic!("the merge rolls back: {:?}", other.map(|p| p.version)),
    }
    assert!(all_acked(&answers), "every append acked: {answers:?}");
    let ended = crate::topology::intent::read(&**halves[0].data_store())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ended.intent.state,
        crate::topology::intent::State::RolledBack,
        "the merge's intent is terminal"
    );
    tokio::time::timeout(HELD_WAIT, engine_shutdown(&rig.state))
        .await
        .expect("the cell shuts down within 30 s");
}
